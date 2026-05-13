"""
strategy_ecosystem.py - 策略生态系统

V7次系统2核心：三大策略协同 + 资金路由 + 互斥规则

三大策略：
  1. 主策略(correct_trending): 跟随一致性共振方向做买方
  2. 反向策略(incorrect_reversal): 对错误信号做反向交易
  3. 震荡市策略(other): 箱体极值处的微利机会

资金路由(L-0.5):
  - 主策略基础占比60%，反向25%，震荡15%
  - 动态分配：激活状态对应策略获得优先资金

互斥规则：
  - 主策略和反向策略不允许同时持有同向仓位
  - 状态切换时先平旧策略仓位再开新策略仓位
  - other状态下主/反策略仅平仓模式

参数独立锁定：
  - 每个策略的参数在Walk-forward后冻结
  - 参数hash确保一致性

绝对期望值底线：
  - 任一策略绝对期望值<0 → 暂停该策略
  - 整体期望值<0 → 暂停实盘
"""
from __future__ import annotations

import copy
import json
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.box_detector import BoxDetector, BoxProfile, ExtremeState, BoxStrategyParams

logger = logging.getLogger(__name__)


@dataclass
class StrategySlot:
    """策略槽位"""
    strategy_id: str
    strategy_type: str
    state: str = 'inactive'
    capital_allocation: float = 0.0
    position_count: int = 0
    total_pnl: float = 0.0
    expected_value: float = 0.0
    sharpe: float = 0.0
    params_locked: bool = False
    params_hash: str = ''
    paused: bool = False
    pause_reason: str = ''
    _closed_pnl_sum: float = 0.0
    _closed_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CapitalRoute:
    """资金路由配置"""
    master_base: float = 0.60
    reverse_base: float = 0.25
    other_base: float = 0.15
    dynamic_enabled: bool = True
    master_active_boost: float = 0.15
    min_maintenance_ratio: float = 0.05

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def total_base(self) -> float:
        return self.master_base + self.reverse_base + self.other_base


@dataclass
class EcosystemTradeRecord:
    """生态系统交易记录"""
    trade_id: str
    strategy_id: str
    timestamp: str
    action: str = ''
    instrument_id: str = ''
    direction: str = ''
    price: float = 0.0
    quantity: int = 0
    open_reason: str = ''
    pnl: float = 0.0
    net_pnl: float = 0.0
    is_open: bool = True
    market_state: str = ''

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StrategyEcosystem:
    """策略生态系统

    三大策略协同：
    - correct_trending → 主策略
    - incorrect_reversal → 反向策略
    - other → 震荡市策略(箱体极值)
    """

    MIN_TRADES_FOR_EV = 5

    def __init__(
        self,
        capital_route: Optional[CapitalRoute] = None,
        box_params: Optional[BoxStrategyParams] = None,
        log_dir: Optional[str] = None,
    ):
        self._lock = threading.RLock()
        self._capital_route = capital_route or CapitalRoute()

        self._box_detector = BoxDetector(params=box_params)

        self._log_dir = log_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'ecosystem'
        )
        os.makedirs(self._log_dir, exist_ok=True)

        self._master = StrategySlot(
            strategy_id='master',
            strategy_type='correct_trending',
            state='active',
            capital_allocation=self._capital_route.master_base,
        )
        self._reverse = StrategySlot(
            strategy_id='reverse',
            strategy_type='incorrect_reversal',
            state='standby',
            capital_allocation=self._capital_route.reverse_base,
        )
        self._other = StrategySlot(
            strategy_id='other',
            strategy_type='box_extreme',
            state='standby',
            capital_allocation=self._capital_route.other_base,
        )

        self._active_strategy: str = 'master'
        self._prev_active_strategy: str = 'master'

        self._master_trades: deque = deque(maxlen=10000)
        self._reverse_trades: deque = deque(maxlen=10000)
        self._other_trades: deque = deque(maxlen=10000)

        self._master_equity: List[float] = []
        self._reverse_equity: List[float] = []
        self._other_equity: List[float] = []

        self._trade_id_counter: int = 0
        self._last_route_time: float = 0.0
        self._ev_cache: Optional[Dict[str, Any]] = None
        self._ev_cache_ts: float = 0.0
        self._ev_cache_ttl: float = 5.0

        self._stats = {
            'total_trades': 0,
            'master_trades': 0,
            'reverse_trades': 0,
            'other_trades': 0,
            'state_switches': 0,
            'mutual_exclusion_blocks': 0,
            'capital_rebalances': 0,
            'ev_breaches': 0,
        }

        logger.info("[StrategyEcosystem] 初始化完成")

    def on_state_switched(self, old_state: str, new_state: str) -> None:
        """状态切换回调：由StateParamManager触发

        当SPM检测到三态切换时，自动联动ecosystem切换活跃策略。
        """
        result = self.switch_active_strategy(new_state)
        if result.get('switched'):
            logger.info(
                "[StrategyEcosystem] SPM联动切换: %s → %s, result=%s",
                old_state, new_state, result,
            )

    @property
    def box_detector(self) -> BoxDetector:
        return self._box_detector

    def _generate_trade_id(self) -> str:
        self._trade_id_counter += 1
        ts = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ECO-{ts}-{self._trade_id_counter:06d}"

    def route_capital(self, active_state: str) -> Dict[str, float]:
        with self._lock:
            cr = self._capital_route

            if not cr.dynamic_enabled:
                return {
                    'master': cr.master_base,
                    'reverse': cr.reverse_base,
                    'other': cr.other_base,
                }

            allocations = {
                'master': cr.min_maintenance_ratio,
                'reverse': cr.min_maintenance_ratio,
                'other': cr.min_maintenance_ratio,
            }

            if active_state == 'correct_trending':
                allocations['master'] = cr.master_base + cr.master_active_boost
                remaining = 1.0 - allocations['master'] - 2 * cr.min_maintenance_ratio
                if remaining > 0:
                    allocations['reverse'] += remaining * 0.6
                    allocations['other'] += remaining * 0.4
            elif active_state == 'incorrect_reversal':
                allocations['reverse'] = cr.reverse_base + cr.master_active_boost
                remaining = 1.0 - allocations['reverse'] - 2 * cr.min_maintenance_ratio
                if remaining > 0:
                    allocations['master'] += remaining * 0.6
                    allocations['other'] += remaining * 0.4
            elif active_state == 'other':
                allocations['other'] = cr.other_base + cr.master_active_boost
                remaining = 1.0 - allocations['other'] - 2 * cr.min_maintenance_ratio
                if remaining > 0:
                    allocations['master'] += remaining * 0.5
                    allocations['reverse'] += remaining * 0.5
            else:
                allocations['master'] = cr.master_base
                allocations['reverse'] = cr.reverse_base
                allocations['other'] = cr.other_base

            total = sum(allocations.values())
            if abs(total - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= total

            self._master.capital_allocation = allocations['master']
            self._reverse.capital_allocation = allocations['reverse']
            self._other.capital_allocation = allocations['other']
            self._stats['capital_rebalances'] += 1

            return allocations

    def switch_active_strategy(self, new_state: str) -> Dict[str, Any]:
        with self._lock:
            state_map = {
                'correct_trending': 'master',
                'incorrect_reversal': 'reverse',
                'other': 'other',
            }
            new_active = state_map.get(new_state, 'master')

            if new_active == self._active_strategy:
                return {'switched': False, 'from': self._active_strategy, 'to': new_active}

            old_active = self._active_strategy
            self._prev_active_strategy = old_active
            self._active_strategy = new_active

            self._master.state = 'active' if new_active == 'master' else 'standby'
            self._reverse.state = 'active' if new_active == 'reverse' else 'standby'
            self._other.state = 'active' if new_active == 'other' else 'standby'

            self.route_capital(new_state)
            self._stats['state_switches'] += 1

            logger.info(
                "[StrategyEcosystem] 策略切换: %s → %s (state=%s)",
                old_active, new_active, new_state,
            )

            return {
                'switched': True,
                'from': old_active,
                'to': new_active,
                'market_state': new_state,
                'close_old_positions_first': True,
                'capital_allocations': {
                    'master': self._master.capital_allocation,
                    'reverse': self._reverse.capital_allocation,
                    'other': self._other.capital_allocation,
                },
            }

    def check_mutual_exclusion(
        self,
        strategy_id: str,
        direction: str,
        instrument_id: str = '',
    ) -> Tuple[bool, str]:
        with self._lock:
            if strategy_id == 'master' and self._reverse.position_count > 0:
                if self._reverse.state == 'active':
                    self._stats['mutual_exclusion_blocks'] += 1
                    return False, 'reverse策略已有同向持仓，主策略被互斥规则阻断'

            if strategy_id == 'reverse' and self._master.position_count > 0:
                if self._master.state == 'active':
                    self._stats['mutual_exclusion_blocks'] += 1
                    return False, '主策略已有同向持仓，反向策略被互斥规则阻断'

            if self._active_strategy == 'other' and strategy_id in ('master', 'reverse'):
                return False, 'other状态下主/反策略仅平仓模式，不允许新开仓'

            return True, ''

    def process_other_strategy_signal(
        self,
        current_price: float,
        resonance_direction: str = '',
        resonance_strength: float = 0.0,
        current_iv: float = 0.0,
        flow_imbalance: float = 0.0,
        cvd_slope: float = 0.0,
    ) -> Dict[str, Any]:
        if self._other.paused:
            return {'action': 'skip', 'reason': 'other策略已暂停'}

        extreme = self._box_detector.classify_extreme_state(
            current_price=current_price,
            resonance_direction=resonance_direction,
            resonance_strength=resonance_strength,
            current_iv=current_iv,
            flow_imbalance=flow_imbalance,
            cvd_slope=cvd_slope,
        )

        if not extreme.tradeable:
            return {
                'action': 'no_signal',
                'extreme_state': extreme.to_dict(),
                'reason': '极值不满足交易条件',
            }

        trade_dir = self._box_detector.determine_trade_direction(extreme)

        if not trade_dir:
            return {
                'action': 'no_signal',
                'extreme_state': extreme.to_dict(),
                'reason': '无明确交易方向',
            }

        allowed, reason = self.check_mutual_exclusion('other', trade_dir)
        if not allowed:
            return {
                'action': 'blocked',
                'extreme_state': extreme.to_dict(),
                'reason': reason,
            }

        with self._lock:
            record = EcosystemTradeRecord(
                trade_id=self._generate_trade_id(),
                strategy_id='other',
                timestamp=datetime.now().isoformat(),
                action='OPEN',
                direction=trade_dir,
                open_reason='OTHER_SCALP',
                market_state='other',
            )
            self._other_trades.append(record)
            self._other.position_count += 1
            self._stats['other_trades'] += 1
            self._stats['total_trades'] += 1

        return {
            'action': 'open',
            'direction': trade_dir,
            'strategy_id': 'other',
            'open_reason': 'OTHER_SCALP',
            'extreme_state': extreme.to_dict(),
            'params': self._box_detector.params.to_dict(),
            'trade_id': record.trade_id,
        }

    def record_strategy_pnl(
        self,
        strategy_id: str,
        pnl: float,
        commission: float = 0.0,
    ) -> None:
        with self._lock:
            net_pnl = pnl - commission
            slot = self._get_slot(strategy_id)
            if slot is None:
                return

            slot.total_pnl += net_pnl

            trades = self._get_trades(strategy_id)
            if trades:
                for trade in reversed(trades):
                    if trade.is_open:
                        trade.pnl = pnl
                        trade.net_pnl = net_pnl
                        trade.is_open = False
                        slot.position_count = max(0, slot.position_count - 1)
                        slot._closed_pnl_sum += net_pnl
                        slot._closed_count += 1
                        break

    def _get_slot(self, strategy_id: str) -> Optional[StrategySlot]:
        if strategy_id == 'master':
            return self._master
        elif strategy_id == 'reverse':
            return self._reverse
        elif strategy_id == 'other':
            return self._other
        return None

    def _get_trades(self, strategy_id: str) -> Optional[deque]:
        if strategy_id == 'master':
            return self._master_trades
        elif strategy_id == 'reverse':
            return self._reverse_trades
        elif strategy_id == 'other':
            return self._other_trades
        return None

    def compute_expected_value(self, strategy_id: str) -> float:
        trades = self._get_trades(strategy_id)
        if trades is None:
            return 0.0
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)

    def check_absolute_ev_bottomline(self) -> Dict[str, Any]:
        with self._lock:
            results = {}
            any_breached = False

            for sid in ('master', 'reverse', 'other'):
                slot = self._get_slot(sid)
                if slot._closed_count > 0:
                    ev = slot._closed_pnl_sum / slot._closed_count
                else:
                    ev = 0.0
                slot.expected_value = ev

                breached = ev < 0 and slot._closed_count >= self.MIN_TRADES_FOR_EV

                if breached and not slot.paused:
                    slot.paused = True
                    slot.pause_reason = f'期望值={ev:.4f}<0'
                    any_breached = True
                    self._stats['ev_breaches'] += 1
                    logger.critical(
                        "[StrategyEcosystem] 🚨 %s策略绝对期望值底线突破! EV=%.4f, 暂停!",
                        sid, ev,
                    )

                results[sid] = {
                    'ev': ev,
                    'breached': breached,
                    'paused': slot.paused,
                }

            return {
                'any_breached': any_breached,
                'strategies': results,
            }

    def get_active_strategy(self) -> str:
        with self._lock:
            return self._active_strategy

    def get_capital_allocations(self) -> Dict[str, float]:
        with self._lock:
            return {
                'master': self._master.capital_allocation,
                'reverse': self._reverse.capital_allocation,
                'other': self._other.capital_allocation,
            }

    def get_strategy_slots(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                'master': self._master.to_dict(),
                'reverse': self._reverse.to_dict(),
                'other': self._other.to_dict(),
            }

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            if self._ev_cache is not None and (now - self._ev_cache_ts) < self._ev_cache_ttl:
                ev_check = self._ev_cache
            else:
                ev_check = self.check_absolute_ev_bottomline()
                self._ev_cache = ev_check
                self._ev_cache_ts = now

            any_paused = self._master.paused or self._reverse.paused or self._other.paused

            return {
                'component': 'strategy_ecosystem',
                'status': 'CRITICAL' if any_paused else 'OK',
                'active_strategy': self._active_strategy,
                'strategies': {
                    'master': {'state': self._master.state, 'paused': self._master.paused},
                    'reverse': {'state': self._reverse.state, 'paused': self._reverse.paused},
                    'other': {'state': self._other.state, 'paused': self._other.paused},
                },
                'capital_allocations': self.get_capital_allocations(),
                'ev_check': ev_check,
                'box_detector': self._box_detector.get_health_status(),
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['active_strategy'] = self._active_strategy
            stats['master_pnl'] = self._master.total_pnl
            stats['reverse_pnl'] = self._reverse.total_pnl
            stats['other_pnl'] = self._other.total_pnl
            stats['master_ev'] = self._master.expected_value
            stats['reverse_ev'] = self._reverse.expected_value
            stats['other_ev'] = self._other.expected_value
            return stats

    def resume_strategy(self, strategy_id: str) -> bool:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot and slot.paused:
                slot.paused = False
                slot.pause_reason = ''
                logger.info("[StrategyEcosystem] %s策略已恢复", strategy_id)
                return True
            return False


_ecosystem: Optional[StrategyEcosystem] = None
_ecosystem_lock = threading.Lock()


def get_strategy_ecosystem(**kwargs) -> StrategyEcosystem:
    global _ecosystem
    if _ecosystem is None:
        with _ecosystem_lock:
            if _ecosystem is None:
                _ecosystem = StrategyEcosystem(**kwargs)
    return _ecosystem


__all__ = [
    'StrategyEcosystem',
    'StrategySlot',
    'CapitalRoute',
    'EcosystemTradeRecord',
    'get_strategy_ecosystem',
]
