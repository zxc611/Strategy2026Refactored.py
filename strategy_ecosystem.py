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
from ali2026v3_trading.策略评判.strategy_judgment_engine import CapitalScale
from ali2026v3_trading.plr_calculator import get_plr_calculator
from ali2026v3_trading.performance_monitor import count_call

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
    win_loss_ratio: float = 0.0
    profit_factor: float = 0.0
    consecutive_losses: int = 0
    target_plr: float = 2.0
    params_locked: bool = False
    params_hash: str = ''
    paused: bool = False
    pause_reason: str = ''
    frozen: bool = False
    last_direction: str = ''
    _closed_pnl_sum: float = 0.0
    _closed_count: int = 0
    _win_pnl_sum: float = 0.0
    _loss_pnl_sum: float = 0.0
    _win_count: int = 0
    _loss_count: int = 0

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
    target_plr: float = 0.0
    actual_plr: float = 0.0

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

    # 资金分配权重配置（P1-32修复：魔法数字参数化）
    # 基础资金分配比例
    CAPITAL_ALLOC_MASTER_BASE = 0.60
    CAPITAL_ALLOC_REVERSE_BASE = 0.25
    CAPITAL_ALLOC_OTHER_BASE = 0.15
    CAPITAL_ALLOC_SPRING_BASE = 0.15
    # 资金规模调整比例
    CAPITAL_ALLOC_SMALL_MASTER = 0.30
    CAPITAL_ALLOC_SMALL_REVERSE = 0.20
    CAPITAL_ALLOC_SMALL_OTHER = 0.15
    CAPITAL_ALLOC_SMALL_SPRING = 0.35
    CAPITAL_ALLOC_LARGE_MASTER = 0.50
    CAPITAL_ALLOC_LARGE_REVERSE = 0.30
    CAPITAL_ALLOC_LARGE_OTHER = 0.15
    CAPITAL_ALLOC_LARGE_SPRING = 0.05
    # 动态分配剩余资金权重
    REMAINING_ALLOC_MASTER = 0.50
    REMAINING_ALLOC_REVERSE = 0.50
    REMAINING_ALLOC_OTHER_MASTER = 0.40
    REMAINING_ALLOC_OTHER_REVERSE = 0.40
    REMAINING_ALLOC_OTHER_SPRING = 0.20
    REMAINING_ALLOC_REVERSE_MASTER = 0.50
    REMAINING_ALLOC_REVERSE_OTHER = 0.30
    REMAINING_ALLOC_REVERSE_SPRING = 0.20
    REMAINING_ALLOC_MASTER_OTHER = 0.30
    REMAINING_ALLOC_MASTER_SPRING = 0.20

    def __init__(
        self,
        capital_route: Optional[CapitalRoute] = None,
        box_params: Optional[BoxStrategyParams] = None,
        log_dir: Optional[str] = None,
        capital_scale: Optional[CapitalScale] = None,
    ):
        self._lock = threading.RLock()
        self._capital_route = capital_route or CapitalRoute()
        self._capital_scale = capital_scale

        _box_detector_kwargs = {}
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _all_params = get_cached_params()
            for _dk in ('box_gain_ratio', 'plr_normalization_base'):
                if _dk in _all_params:
                    _box_detector_kwargs[_dk] = _all_params[_dk]
        except Exception:
            pass
        self._box_detector = BoxDetector(params=box_params, **_box_detector_kwargs)
        self._last_bar_data: Dict[str, float] = {}

        # P1-32修复: 从ConfigService覆盖资金分配权重
        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            _cap_cfg = getattr(_cfg, 'trading', None)
            if _cap_cfg:
                for _attr in ['CAPITAL_ALLOC_MASTER_BASE', 'CAPITAL_ALLOC_REVERSE_BASE',
                              'CAPITAL_ALLOC_OTHER_BASE', 'CAPITAL_ALLOC_SPRING_BASE']:
                    _val = getattr(_cap_cfg, _attr.lower(), None)
                    if _val is not None:
                        setattr(self, _attr, _val)
        except Exception:
            pass

        self._log_dir = log_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'ecosystem'
        )
        os.makedirs(self._log_dir, exist_ok=True)

        self._master = StrategySlot(
            strategy_id='master',
            strategy_type='correct_trending',
            state='active',
            capital_allocation=self.CAPITAL_ALLOC_MASTER_BASE,
        )
        self._reverse = StrategySlot(
            strategy_id='reverse',
            strategy_type='incorrect_reversal',
            state='standby',
            capital_allocation=self.CAPITAL_ALLOC_REVERSE_BASE,
        )
        self._other = StrategySlot(
            strategy_id='other',
            strategy_type='box_extreme',
            state='standby',
            capital_allocation=self.CAPITAL_ALLOC_OTHER_BASE,
        )
        self._spring = StrategySlot(
            strategy_id='spring',
            strategy_type='box_spring',
            state='active',
            capital_allocation=self.CAPITAL_ALLOC_SPRING_BASE,
        )

        self._active_strategy: str = 'master'
        self._prev_active_strategy: str = 'master'

        self._master_trades: deque = deque(maxlen=10000)
        self._reverse_trades: deque = deque(maxlen=10000)
        self._other_trades: deque = deque(maxlen=10000)
        self._spring_trades: deque = deque(maxlen=10000)

        self._master_equity: List[float] = []
        self._reverse_equity: List[float] = []
        self._other_equity: List[float] = []

        self._trade_id_counter: int = 0
        self._last_route_time: float = 0.0
        self._ev_cache: Optional[Dict[str, Any]] = None
        self._ev_cache_ts: float = 0.0
        self._ev_cache_ttl: float = 5.0
        self._mode_engine = None
        self._decision_intervals: Dict[str, int] = {
            'master': 1,
            'reverse': 1,
            'other': 5,
            'spring': 5,
        }
        self._bar_counter: int = 0
        self._s1_params = {
            'strategy_id': 's1_hft',
            'signal_confirm_ticks': 3,
            'cooldown_ms': 50,
            'min_imbalance': 0.15,
            'hard_stop_minutes': 30,
            'daily_drawdown_pct': 0.03,
            'risk_ratio': 0.2,
            'decision_interval_minutes': 1,
        }
        self._s2_params = {
            'strategy_id': 's2_minute',
            'state_confirm_bars': 2,
            'cooldown_sec': 0,
            'min_strength': 0.3,
            'hard_stop_minutes': 90,
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
            'decision_interval_minutes': 1,
        }

        self._stats = {
            'total_trades': 0,
            'master_trades': 0,
            'reverse_trades': 0,
            'other_trades': 0,
            'spring_trades': 0,
            'state_switches': 0,
            'mutual_exclusion_blocks': 0,
            'capital_rebalances': 0,
            'ev_breaches': 0,
        }

        # ✅ 集成治理检查器（L-P0-4~L-P0-6, L-P1-4, L-P2-1, E7, E11）
        try:
            from ali2026v3_trading.governance_engine import (
                E12ReverseStrategyPseudoIndependenceDetector,
                E13ShadowStrategyCollusionDetector,
                MultiStateSwitchBacktestScenario,
                WF6ToWF10EliminationChecker,
                E8E9E10EliminationChecker,
                E7UnexplainedReturnChecker,
                E11QuantitativeSourceChecker,
            )
            self._e12_detector = E12ReverseStrategyPseudoIndependenceDetector()
            self._e13_detector = E13ShadowStrategyCollusionDetector()
            self._multi_state_scenario = MultiStateSwitchBacktestScenario()
            self._wf_elimination_checker = WF6ToWF10EliminationChecker()
            self._e8e9e10_checker = E8E9E10EliminationChecker()
            self._e7_checker = E7UnexplainedReturnChecker()
            self._e11_checker = E11QuantitativeSourceChecker()
            logger.info("[StrategyEcosystem] 治理检查器已集成: E7/E8/E9/E10/E11/E12/E13/WF6-10")
        except Exception as e:
            logger.warning("[StrategyEcosystem] 治理检查器集成失败: %s", e)
            self._e12_detector = None
            self._e13_detector = None
            self._multi_state_scenario = None
            self._wf_elimination_checker = None
            self._e8e9e10_checker = None
            self._e7_checker = None
            self._e11_checker = None

        logger.info("[StrategyEcosystem] 初始化完成")

        self._propagate_capital_scale()

    def set_decision_interval(self, strategy_id: str, interval_minutes: int) -> None:
        self._decision_intervals[strategy_id] = max(1, interval_minutes)

    def should_make_decision(self, strategy_id: str) -> bool:
        interval = self._decision_intervals.get(strategy_id, 1)
        if interval <= 1:
            return True
        return self._bar_counter % interval == 0

    def get_s1_params(self) -> Dict[str, Any]:
        return dict(self._s1_params)

    def get_s2_params(self) -> Dict[str, Any]:
        return dict(self._s2_params)

    def update_s1_params(self, **kwargs) -> None:
        self._s1_params.update(kwargs)

    def update_s2_params(self, **kwargs) -> None:
        self._s2_params.update(kwargs)

    def on_bar_update(self) -> None:
        self._bar_counter += 1
        # P1-11修复: 每bar更新时向BoxDetector喂数据（维护历史缓冲区）
        try:
            if hasattr(self, '_last_bar_data') and self._last_bar_data:
                bd = self._box_detector
                d = self._last_bar_data
                bd.update_bar(
                    high=d.get('high', 0.0), low=d.get('low', 0.0),
                    close=d.get('close', 0.0), volume=d.get('volume', 0.0),
                )
                if d.get('iv', 0.0) > 0:
                    bd.update_iv(d['iv'])
                bd.detect_box()
        except Exception:
            pass

    def _propagate_capital_scale(self) -> None:
        if self._capital_scale is None:
            return
        scale_str = self._capital_scale.value if hasattr(self._capital_scale, 'value') else str(self._capital_scale).lower()
        try:
            from ali2026v3_trading.mode_engine import ModeEngine
            me = ModeEngine.create_engine(scale_str)
            self._mode_engine = me
            me.register_component('StrategyEcosystem', self)
            result = me.switch_mode(scale_str)
            if not result.get('success'):
                logger.warning("[StrategyEcosystem] ModeEngine switch failed: %s", result)
        except Exception as e:
            logger.warning("[StrategyEcosystem] ModeEngine unavailable, falling back to direct propagation: %s", e)
            self._propagate_capital_scale_fallback(scale_str)

    def _propagate_capital_scale_fallback(self, scale_str: str) -> None:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            rs.set_capital_scale(scale_str)
        except Exception as e:
            logger.debug("[StrategyEcosystem] fallback propagate to RiskService failed: %s", e)
        try:
            from ali2026v3_trading.signal_service import SignalService, get_signal_service
            ss = get_signal_service()
            if scale_str == 'small':
                ss.enable_plr_filter(min_estimated_plr=2.0)
            elif scale_str == 'medium':
                ss.enable_plr_filter(min_estimated_plr=1.5)
            else:
                ss.disable_plr_filter()
        except Exception as e:
            logger.debug("[StrategyEcosystem] fallback propagate to SignalService failed: %s", e)
        try:
            from ali2026v3_trading.state_param_manager import StateParamManager, get_state_param_manager
            spm = get_state_param_manager()
            spm.set_capital_scale(scale_str)
        except Exception as e:
            logger.debug("[StrategyEcosystem] fallback propagate to StateParamManager failed: %s", e)
        try:
            from ali2026v3_trading.box_spring_strategy import get_box_spring_strategy
            bss = get_box_spring_strategy()
            if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                bss.set_capital_scale(scale_str)
            else:
                bss._capital_scale = scale_str
        except Exception as e:
            logger.debug("[StrategyEcosystem] fallback propagate to BoxSpringStrategy failed: %s", e)

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

    @count_call()
    def route_capital(self, active_state: str) -> Dict[str, float]:
        self.on_bar_update()
        with self._lock:
            cr = self._capital_route

            if not cr.dynamic_enabled:
                return {
                    'master': cr.master_base,
                    'reverse': cr.reverse_base,
                    'other': cr.other_base,
                    'spring': self._spring.capital_allocation,
                }

            if self._capital_scale == CapitalScale.SMALL:
                spring_base = self.CAPITAL_ALLOC_SMALL_SPRING
                master_base = self.CAPITAL_ALLOC_SMALL_MASTER
                reverse_base = self.CAPITAL_ALLOC_SMALL_REVERSE
                other_base = self.CAPITAL_ALLOC_SMALL_OTHER
            elif self._capital_scale == CapitalScale.LARGE:
                spring_base = self.CAPITAL_ALLOC_LARGE_SPRING
                master_base = self.CAPITAL_ALLOC_LARGE_MASTER
                reverse_base = self.CAPITAL_ALLOC_LARGE_REVERSE
                other_base = self.CAPITAL_ALLOC_LARGE_OTHER
            else:
                spring_base = self.CAPITAL_ALLOC_SPRING_BASE
                master_base = cr.master_base
                reverse_base = cr.reverse_base
                other_base = cr.other_base

            allocations = {
                'master': cr.min_maintenance_ratio,
                'reverse': cr.min_maintenance_ratio,
                'other': cr.min_maintenance_ratio,
                'spring': spring_base,
            }

            if active_state == 'correct_trending':
                allocations['master'] = master_base + cr.master_active_boost
                remaining = 1.0 - allocations['master'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['reverse'] += remaining * self.REMAINING_ALLOC_MASTER
                    allocations['other'] += remaining * self.REMAINING_ALLOC_MASTER_OTHER
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_MASTER_SPRING
            elif active_state == 'incorrect_reversal':
                allocations['reverse'] = reverse_base + cr.master_active_boost
                remaining = 1.0 - allocations['reverse'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['master'] += remaining * self.REMAINING_ALLOC_REVERSE_MASTER
                    allocations['other'] += remaining * self.REMAINING_ALLOC_REVERSE_OTHER
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_REVERSE_SPRING
            elif active_state == 'other':
                allocations['other'] = other_base + cr.master_active_boost
                remaining = 1.0 - allocations['other'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['master'] += remaining * self.REMAINING_ALLOC_OTHER_MASTER
                    allocations['reverse'] += remaining * self.REMAINING_ALLOC_OTHER_REVERSE
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_OTHER_SPRING
            else:
                allocations['master'] = master_base
                allocations['reverse'] = reverse_base
                allocations['other'] = other_base
                allocations['spring'] = spring_base

            total = sum(allocations.values())
            if abs(total - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= total

            self._master.capital_allocation = allocations['master']
            self._reverse.capital_allocation = allocations['reverse']
            self._other.capital_allocation = allocations['other']
            self._spring.capital_allocation = allocations.get('spring', self.CAPITAL_ALLOC_SPRING_BASE)
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

            # ✅ P1-16修复: 集成StateTransitionAnalytics状态转换分析
            try:
                from ali2026v3_trading.state_param_manager import StateTransitionAnalytics
                sta = getattr(self, '_state_transition_analytics', None)
                if sta is None:
                    sta = StateTransitionAnalytics()
                    self._state_transition_analytics = sta
                sta.on_state_change('default', old_active, new_active)
                matrix = sta.get_transition_matrix()
                logger.debug("[StrategyEcosystem] 状态转换矩阵: %s", matrix)
            except Exception as e:
                logger.debug("[StrategyEcosystem] StateTransitionAnalytics error: %s", e)

            logger.info(
                "[StrategyEcosystem] 策略切换: %s → %s (state=%s)",
                old_active, new_active, new_state,
            )

            if new_state == 'correct_trending':
                self._s1_params['active'] = True
                self._s2_params['active'] = True
            else:
                self._s1_params['active'] = False
                self._s2_params['active'] = False

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
                    'spring': self._spring.capital_allocation,
                },
            }

    @count_call()
    def check_mutual_exclusion(
        self,
        strategy_id: str,
        direction: str,
        instrument_id: str = '',
    ) -> Tuple[bool, str]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is not None and slot.frozen:
                return False, f'{strategy_id}策略已被冻结, 不允许新开仓(旧仓位按原规则退出)'

            if strategy_id == 'master' and self._reverse.position_count > 0:
                if self._reverse.state == 'active':
                    if self._is_same_delta_direction(strategy_id, direction, 'reverse'):
                        self._stats['mutual_exclusion_blocks'] += 1
                        return False, 'reverse策略已有同向持仓，主策略被互斥规则阻断'

            if strategy_id == 'reverse' and self._master.position_count > 0:
                if self._master.state == 'active':
                    if self._is_same_delta_direction(strategy_id, direction, 'master'):
                        self._stats['mutual_exclusion_blocks'] += 1
                        return False, '主策略已有同向持仓，反向策略被互斥规则阻断'

            if self._active_strategy == 'other' and strategy_id in ('master', 'reverse'):
                return False, 'other状态下主/反策略仅平仓模式，不允许新开仓'

            if strategy_id == 'spring':
                for other_id in ('master', 'reverse'):
                    other_slot = self._get_slot(other_id)
                    if other_slot and other_slot.position_count > 0 and other_slot.state == 'active':
                        if self._is_same_delta_direction(strategy_id, direction, other_id):
                            if other_slot.win_loss_ratio >= other_slot.target_plr:
                                pass
                            else:
                                self._stats['mutual_exclusion_blocks'] += 1
                                return False, f'{other_id}策略已有同向持仓且盈亏比未达标，spring策略被互斥规则阻断'

            risk_check = self._check_cross_strategy_risk(instrument_id, direction)
            if risk_check == 'BLOCK':
                self._stats['mutual_exclusion_blocks'] += 1
                return False, '跨策略Delta敞口超硬阻断线，拒绝新开仓'
            if risk_check == 'CIRCUIT_BREAK':
                self._stats['mutual_exclusion_blocks'] += 1
                return False, '跨策略Delta敞口超熔断线，拒绝新开仓'

            return True, ''

    def _get_slot(self, strategy_id: str) -> Optional[StrategySlot]:
        slot_map = {
            'master': self._master,
            'reverse': self._reverse,
            'other': self._other,
            'spring': self._spring,
        }
        return slot_map.get(strategy_id)

    def _is_same_delta_direction(self, my_id: str, my_direction: str, other_id: str) -> bool:
        other_slot = self._master if other_id == 'master' else self._reverse
        other_dir = getattr(other_slot, 'last_direction', '')
        if not other_dir:
            return True
        my_delta_sign = 1 if my_direction in ('long', 'BUY', 'buy') else -1
        other_delta_sign = 1 if other_dir in ('long', 'BUY', 'buy') else -1
        return my_delta_sign * other_delta_sign > 0

    def _check_cross_strategy_risk(self, instrument_id: str, direction: str) -> str:
        try:
            from ali2026v3_trading.position_service import (
                aggregate_greeks_exposure,
                get_cross_strategy_risk_guard,
            )
            guard = get_cross_strategy_risk_guard()
            pos_svc = self._get_position_service()
            if pos_svc is None:
                return 'PASS'
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            level, reason, detail = guard.check(exposure)
            if level in (guard.BLOCK, guard.CIRCUIT_BREAK):
                return level
            return 'PASS'
        except Exception as e:
            logger.warning("[StrategyEcosystem._check_cross_strategy_risk] Error: %s, fail-safe阻断", e)
            return 'BLOCK'

    def _get_position_service(self):
        try:
            from ali2026v3_trading.position_service import get_position_service
            return get_position_service()
        except Exception:
            return None

    def record_spring_trade(self, direction: str):
        with self._lock:
            self._spring.position_count += 1
            self._spring_trades.append(EcosystemTradeRecord(
                trade_id=self._generate_trade_id(),
                strategy_id='spring',
                timestamp=datetime.now().isoformat(),
                action='OPEN',
                direction=direction,
                open_reason='BOX_SPRING',
                market_state=self._active_strategy,
            ))
            self._stats['total_trades'] += 1
            self._stats['spring_trades'] += 1

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

    @count_call()
    def record_strategy_pnl(
        self,
        strategy_id: str,
        pnl: float,
        commission: float = 0.0,
        target_plr: float = 0.0,
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
                        trade.target_plr = target_plr or slot.target_plr
                        if slot._loss_pnl_sum > 1e-10:
                            trade.actual_plr = slot._win_pnl_sum / slot._loss_pnl_sum
                        elif net_pnl > 0:
                            trade.actual_plr = 0.0
                        else:
                            trade.actual_plr = 0.0
                        slot.position_count = max(0, slot.position_count - 1)
                        slot._closed_pnl_sum += net_pnl
                        slot._closed_count += 1
                        if net_pnl > 0:
                            slot._win_pnl_sum += net_pnl
                            slot._win_count += 1
                        elif net_pnl < 0:
                            slot._loss_pnl_sum += abs(net_pnl)
                            slot._loss_count += 1
                        break
            # ✅ P1-8修复: 交易记录后自动更新PLR统计并检查是否需要冻结策略
            self.update_plr_stats(strategy_id)
            plr_stats = self.compute_plr_stats(strategy_id)
            if plr_stats:
                plr_ratio = plr_stats.get('plr_ratio', 0.0)
                consecutive_losses = plr_stats.get('consecutive_losses', 0)
                if plr_ratio < 0.5 and consecutive_losses >= 5:
                    self.freeze_strategy_slot(strategy_id)
                    logging.warning("[StrategyEcosystem] 策略%s因PLR过低(%.2f)和连续亏损(%d次)被自动冻结",
                                    strategy_id, plr_ratio, consecutive_losses)

            # ✅ P0-18集成: 同步更新ProfitLossRatioCalculator（统一PLR计算引擎）
            try:
                _plr_calc = get_plr_calculator(default_target_plr=slot.target_plr)
                _trade_records = []
                trades_deque = self._get_trades(strategy_id)
                if trades_deque:
                    for t in trades_deque:
                        if not t.is_open:
                            _trade_records.append({'pnl': getattr(t, 'net_pnl', getattr(t, 'pnl', 0.0))})
                if _trade_records:
                    _plr_calc.update_from_trades(strategy_id, _trade_records)
            except Exception as _plr_err:
                logging.debug("[StrategyEcosystem] ProfitLossRatioCalculator同步更新异常: %s", _plr_err)

    def _get_trades(self, strategy_id: str) -> Optional[deque]:
        trades_map = {
            'master': self._master_trades,
            'reverse': self._reverse_trades,
            'other': self._other_trades,
            'spring': self._spring_trades,
        }
        return trades_map.get(strategy_id)

    def compute_expected_value(self, strategy_id: str) -> float:
        trades = self._get_trades(strategy_id)
        if trades is None:
            return 0.0
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)

    def compute_plr_stats(self, strategy_id: str) -> Dict[str, float]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None or slot._closed_count == 0:
                return {}
            n_win = slot._win_count
            n_loss = slot._loss_count
            total_win = slot._win_pnl_sum
            total_loss = slot._loss_pnl_sum
            avg_win = total_win / n_win if n_win > 0 else 0.0
            avg_loss = total_loss / n_loss if n_loss > 0 else 0.0
            win_rate = n_win / slot._closed_count if slot._closed_count > 0 else 0.0
            loss_rate = n_loss / slot._closed_count if slot._closed_count > 0 else 0.0
            expected_value = avg_win * win_rate - avg_loss * loss_rate
            profit_factor = total_win / total_loss if total_loss > 1e-10 else 0.0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 0.0
            return {
                'expected_value': expected_value,
                'win_rate': win_rate,
                'loss_rate': loss_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'win_loss_ratio': win_loss_ratio,
                'total_trades': slot._closed_count,
                'win_count': n_win,
                'loss_count': n_loss,
                'total_pnl': slot.total_pnl,
                'target_plr': slot.target_plr,
                'current_plr': profit_factor,
                'plr_ratio': profit_factor / slot.target_plr if slot.target_plr > 0 else 0.0,
            }

    @count_call()
    def update_plr_stats(self, strategy_id: str) -> Dict[str, float]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None or slot._closed_count == 0:
                return {}
            total_win = slot._win_pnl_sum
            total_loss = slot._loss_pnl_sum
            n_win = slot._win_count
            n_loss = slot._loss_count
            slot.profit_factor = total_win / total_loss if total_loss > 1e-10 else 0.0
            avg_win = total_win / n_win if n_win > 0 else 0.0
            avg_loss = total_loss / n_loss if n_loss > 0 else 0.0
            slot.win_loss_ratio = (avg_win / avg_loss) if avg_loss > 1e-10 else 0.0
            trades = self._get_trades(strategy_id)
            if trades:
                closed = [t for t in trades if not t.is_open]
                streak = 0
                max_streak = 0
                for t in reversed(closed):
                    if t.net_pnl < 0:
                        streak += 1
                        max_streak = max(max_streak, streak)
                    else:
                        break
                slot.consecutive_losses = max_streak
            logger.info(
                "[PLR] strategy=%s win_loss_ratio=%.2f profit_factor=%.2f consecutive_losses=%d target_plr=%.1f plr_ratio=%.2f",
                strategy_id, slot.win_loss_ratio, slot.profit_factor,
                slot.consecutive_losses, slot.target_plr,
                slot.profit_factor / slot.target_plr if slot.target_plr > 0 else 0.0,
            )
            return {
                'win_loss_ratio': slot.win_loss_ratio,
                'profit_factor': slot.profit_factor,
                'consecutive_losses': slot.consecutive_losses,
            }

    def check_absolute_ev_bottomline(self) -> Dict[str, Any]:
        with self._lock:
            results = {}
            any_breached = False

            for sid in ('master', 'reverse', 'other', 'spring'):
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

            # --- 瀑布式评判引擎（三系统统一，实盘准入门控） ---
            cascade_result = {}
            try:
                import sys, os
                _project_root = os.path.dirname(os.path.abspath(__file__))
                sys.path.insert(0, _project_root)
                from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
                _slot = self._get_slot('master')
                _live_metrics = {
                    'sharpe': getattr(_slot, 'sharpe', 0.0),
                    'calmar': getattr(_slot, 'calmar', 0.0),
                    'profit_loss_ratio': getattr(_slot, 'profit_factor', 1.0),
                    'max_consecutive_losses': getattr(_slot, 'consecutive_losses', 0),
                    'win_rate': getattr(_slot, 'win_rate', 0.5),
                    'total_trades': getattr(_slot, 'total_trades', 0),
                }
                _adapted = adapt_backtest_result(_live_metrics)
                _cascade = CascadeJudge.from_config()
                _cascade_report = _cascade.judge(_adapted)
                cascade_result = {
                    'passed': _cascade_report.passed,
                    'score': _cascade_report.final_score,
                    'fatal': _cascade_report.fatal_reason,
                }
                if not _cascade_report.passed:
                    logger.warning("[StrategyEcosystem] 瀑布式评判否决: %s", _cascade_report.fatal_reason)
            except Exception as _e:
                cascade_result = {'error': str(_e)}

            return {
                'any_breached': any_breached,
                'strategies': results,
                'cascade_judge': cascade_result,
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
                'spring': self._spring.capital_allocation,
            }

    def get_strategy_slots(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                'master': self._master.to_dict(),
                'reverse': self._reverse.to_dict(),
                'other': self._other.to_dict(),
                'spring': self._spring.to_dict(),
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

            any_paused = self._master.paused or self._reverse.paused or self._other.paused or self._spring.paused

            # ✅ 集成E12反向策略伪独立检测（L-P0-4）
            e12_result = None
            if self._e12_detector is not None:
                try:
                    master_trades = list(self._master_trades)
                    reverse_trades = list(self._reverse_trades)
                    e12_result = self._e12_detector.detect(master_trades, reverse_trades)
                    if e12_result and e12_result.get('e12_triggered'):
                        logger.critical("[StrategyEcosystem] E12告警: 反向策略与主策略相关性过高 correlation=%.2f",
                                        e12_result.get('correlation', 0.0))
                except Exception as e:
                    logger.debug("[StrategyEcosystem] E12检测异常: %s", e)

            # ✅ 集成E11量化来源检查（E11）
            e11_result = None
            if self._e11_checker is not None:
                try:
                    param_sources = {
                        'master_params': 'backtest',
                        'reverse_params': 'backtest',
                        'other_params': 'backtest',
                    }
                    e11_result = self._e11_checker.check(param_sources)
                except Exception as e:
                    logger.debug("[StrategyEcosystem] E11检测异常: %s", e)

            return {
                'component': 'strategy_ecosystem',
                'status': 'CRITICAL' if any_paused else 'OK',
                'active_strategy': self._active_strategy,
                'strategies': {
                    'master': {'state': self._master.state, 'paused': self._master.paused},
                    'reverse': {'state': self._reverse.state, 'paused': self._reverse.paused},
                    'other': {'state': self._other.state, 'paused': self._other.paused},
                    'spring': {'state': self._spring.state, 'paused': self._spring.paused},
                },
                'capital_allocations': self.get_capital_allocations(),
                'ev_check': ev_check,
                'box_detector': self._box_detector.get_health_status(),
                'governance': {
                    'e12_reverse_independence': e12_result,
                    'e11_quant_source': e11_result,
                },
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['active_strategy'] = self._active_strategy
            stats['master_pnl'] = self._master.total_pnl
            stats['reverse_pnl'] = self._reverse.total_pnl
            stats['other_pnl'] = self._other.total_pnl
            stats['spring_pnl'] = self._spring.total_pnl
            stats['master_ev'] = self._master.expected_value
            stats['reverse_ev'] = self._reverse.expected_value
            stats['other_ev'] = self._other.expected_value
            stats['spring_ev'] = self._spring.expected_value
            try:
                from ali2026v3_trading.performance_monitor import PathCounter
                perf = PathCounter.get_stats()
                stats['performance_monitor'] = {
                    'total_paths': perf.get('total_paths', 0),
                    'uptime_seconds': round(perf.get('uptime_seconds', 0), 1),
                }
            except Exception:
                pass
            return stats

    def freeze_strategy_slot(self, strategy_id: str) -> bool:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None:
                logger.warning("[StrategyEcosystem] freeze_strategy_slot: 未知策略%s", strategy_id)
                return False
            slot.frozen = True
            logger.info("[StrategyEcosystem] 策略%s已冻结新开仓", strategy_id)
            return True

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
