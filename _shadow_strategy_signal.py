"""
shadow_strategy_signal.py - signal generation mixin

Responsibilities:
- ShadowStrategySignalMixin: process_shadow_a/b_signal, record_master_trade, etc.

Source: split from _shadow_services.py ShadowPositionService per doc 12.3.4
"""
from __future__ import annotations

import copy
import json
import logging
import math
import numpy as np
import os
import pandas as pd
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from ali2026v3_trading.strategy._shadow_types import ShadowTradeRecord, AlphaMetrics, ShadowParamsSnapshot
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.resilience import (
    ShadowSyncBarrier, SharedStateRegistry, Watchdog, HeartbeatMonitor,
    CircuitBreakerHalfOpen, StrategyRestartGuard,
    stable_sum, stable_mean, stable_variance, stable_std,
    compute_sharpe_stable, KahanSummation, safe_divide,
    safe_normalize_weights, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
    get_strategy_registry, StrategyRegistry,
)
from ali2026v3_trading.config.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
)

try:
    import yaml
except ImportError:
    yaml = None


logger = get_logger(__name__)  # R9-5

class ShadowStrategySignalService:
    """Signal generation service (from ShadowStrategySignalMixin)"""

    def __init__(self):
        self._facade = None

    def __getattr__(self, name):
        if self._facade is not None:
            # 递归保护: 防止 facade 的 __getattr__ 又委托回 self 导致无限递归
            if '__getattr__recursing' in self.__dict__:
                raise AttributeError(name)
            self.__dict__['__getattr__recursing'] = True
            try:
                return getattr(self._facade, name)
            except AttributeError:
                pass
            finally:
                self.__dict__.pop('__getattr__recursing', None)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def process_shadow_a_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
        strategy_group: str = "s2_resonance",
    ) -> ShadowTradeRecord:
        # FR-P1-02修复: 记录影子引擎tick时间戳
        self._last_shadow_tick_timestamp = time.time()
        with self._lock:
            reverse_map = {
                'long': 'short',
                'short': 'long',
                'buy': 'sell',
                'sell': 'buy',
            }
            reversed_dir = reverse_map.get(signal_direction, signal_direction)

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('A'),
                shadow_type=f'{strategy_group}_shadow_a',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                instrument_id=instrument_id,
                direction=reversed_dir,
                price=price,
                quantity=quantity,
                open_reason='SHADOW_A_REVERSAL',
                market_state=market_state,
                signal_strength=signal_strength,
            )

            group = strategy_group if strategy_group in self.STRATEGY_GROUPS else 's2_resonance'
            self._group_trades[group]['shadow_a'].append(record)

            self._stats['shadow_a_trades'] += 1

            self._enqueue_trade_log(record)

            self.update_paper_account(instrument_id=instrument_id, direction=reversed_dir, quantity=quantity if quantity > 0 else 1, price=price if price > 0 else 0.0, commission=self._estimate_commission(price, quantity))

            if self._snapshot_collector is not None:
                try:
                    self._snapshot_collector.capture_signal_point(
                        timestamp=np.datetime64('now'),
                        trigger_detail=f"{group}_shadow_a {instrument_id} {reversed_dir}",
                    )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug("[ShadowEngine] shadow_a snapshot error: %s", e)

            return record
    def process_shadow_b_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
        strategy_group: str = "s2_resonance",
    ) -> ShadowTradeRecord:
        with self._lock:
            random_dir = self._rng.choice(['long', 'short'])

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('B'),
                shadow_type=f'{strategy_group}_shadow_b',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                instrument_id=instrument_id,
                direction=random_dir,
                price=price,
                quantity=quantity,
                open_reason='SHADOW_B_RANDOM',
                market_state=market_state,
                signal_strength=signal_strength,
            )

            group = strategy_group if strategy_group in self.STRATEGY_GROUPS else 's2_resonance'
            self._group_trades[group]['shadow_b'].append(record)

            self._stats['shadow_b_trades'] += 1

            self._enqueue_trade_log(record)

            self.update_paper_account(instrument_id=instrument_id, direction=random_dir, quantity=quantity if quantity > 0 else 1, price=price if price > 0 else 0.0, commission=self._estimate_commission(price, quantity))

            if self._snapshot_collector is not None:
                try:
                    self._snapshot_collector.capture_signal_point(
                        timestamp=np.datetime64('now'),
                        trigger_detail=f"{group}_shadow_b {instrument_id} {random_dir}",
                    )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.debug("[ShadowEngine] shadow_b snapshot error: %s", e)

            return record
    def record_master_trade(
        self,
        instrument_id: str = "",
        direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        pnl: float = 0.0,
        open_reason: str = "",
        market_state: str = "",
        strategy_group: str = "s2_resonance",
    ) -> ShadowTradeRecord:
        # FR-P1-02修复: 记录主引擎tick时间戳，监控主/影子引擎tick时间差
        _now_ts = time.time()
        self._last_master_tick_timestamp = _now_ts
        if self._last_shadow_tick_timestamp > 0:
            _tick_diff = abs(_now_ts - self._last_shadow_tick_timestamp)
            if _tick_diff > self._tick_timestamp_diff_threshold_sec:
                logger.warning("[FR-P1-02] 主引擎与影子引擎tick时间差=%.1fs > 阈值=%.1fs, "
                               "可能存在数据延迟或处理阻塞", _tick_diff, self._tick_timestamp_diff_threshold_sec)
        with self._lock:
            if math.isfinite(pnl):  # NP-P2-16: pnl累加添加isfinite检查
                self._master_pnl_sum += pnl

            group = strategy_group if strategy_group in self.STRATEGY_GROUPS else 's2_resonance'
            if math.isfinite(pnl):  # NP-P2-16: pnl累加添加isfinite检查
                self._group_pnl_sum[group]['master'] += pnl

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('MASTER'),
                shadow_type=f'{group}_master',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                instrument_id=instrument_id,
                direction=direction,
                price=price,
                quantity=quantity,
                pnl=pnl,
                net_pnl=pnl,
                open_reason=open_reason,
                market_state=market_state,
                is_open=False,
            )
            self._group_trades[group]['master'].append(record)
            self._stats['master_trades'] += 1

            self._enqueue_trade_log(record)

            if self._snapshot_collector is not None:
                try:
                    from ali2026v3_trading.strategy_judgment.market_snapshot_collector import SnapshotTrigger
                    self._snapshot_collector.capture_order_event(
                        timestamp=np.datetime64('now'),
                        event_type=SnapshotTrigger.ORDER_CLOSED,
                        trigger_detail=f"master {instrument_id} {direction} pnl={pnl:.2f}",
                    )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                    logging.debug("[ShadowEngine] master snapshot error: %s", e)

            return record
    def record_shadow_pnl(
        self,
        shadow_type: str,
        trade_id: str,
        pnl: float,
        close_reason: str = "",
        commission: float = 0.0,
    ) -> None:
        with self._lock:
            net_pnl = pnl - commission

            if shadow_type == 'shadow_a':
                trades = self._shadow_a_trades
                if math.isfinite(net_pnl):  # NP-P2-16: pnl累加添加isfinite检查
                    self._shadow_a_pnl_sum += net_pnl
            elif shadow_type == 'shadow_b':
                trades = self._shadow_b_trades
                if math.isfinite(net_pnl):  # NP-P2-16: pnl累加添加isfinite检查
                    self._shadow_b_pnl_sum += net_pnl
            else:
                return

            for trade in reversed(trades):
                if trade.trade_id == trade_id and trade.is_open:
                    trade.pnl = pnl
                    trade.commission = commission
                    trade.net_pnl = net_pnl
                    trade.close_reason = close_reason
                    trade.is_open = False
                    break
    def process_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
        strategy_group: str = "s2_resonance",  # R13-API-P1-01: add strategy_group param
    ) -> Dict[str, ShadowTradeRecord]:
        shadow_a = self.process_shadow_a_signal(
            market_state=market_state,
            instrument_id=instrument_id,
            signal_direction=signal_direction,
            price=price,
            quantity=quantity,
            signal_strength=signal_strength,
            strategy_group=strategy_group,  # R13-API-P1-01: pass strategy_group
        )
        shadow_b = self.process_shadow_b_signal(
            market_state=market_state,
            instrument_id=instrument_id,
            signal_direction=signal_direction,
            price=price,
            quantity=quantity,
            signal_strength=signal_strength,
            strategy_group=strategy_group,  # R13-API-P1-01: pass strategy_group
        )

        now = time.time()
        # R24-P2-CF-07修复: 将_last_summary_time更新放入锁内，防止并发双重汇总
        with self._lock:
            if now - self._last_summary_time > self._summary_interval_hours * 3600:
                self._last_summary_time = now
                _should_summary = True
            else:
                _should_summary = False
        if _should_summary:
            self.generate_daily_summary()

        return {'shadow_a': shadow_a, 'shadow_b': shadow_b}
    def process_signals_batch(
        self,
        signals: List[Dict[str, Any]],
    ) -> List[Dict[str, ShadowTradeRecord]]:
        """批量信号处理(优化：单次锁获取处理所有signals)"""
        results = []
        reverse_map = {'long': 'short', 'short': 'long', 'buy': 'sell', 'sell': 'buy'}
        with self._lock:
            for sig in signals:
                market_state = sig.get('market_state', '')
                instrument_id = sig.get('instrument_id', '')
                # P2-09: 信号字段统一为 signal_type/volume（与 SignalContext 规范对齐）
                signal_direction = sig.get('signal_type', '')
                price = sig.get('price', 0.0)
                quantity = sig.get('volume', 0)
                signal_strength = sig.get('signal_strength', 0.0)

                reversed_dir = reverse_map.get(signal_direction, signal_direction)
                record_a = ShadowTradeRecord(
                    trade_id=self._generate_trade_id('A'),
                    shadow_type='shadow_a',
                    timestamp=datetime.now(CHINA_TZ).isoformat(),
                    instrument_id=instrument_id,
                    direction=reversed_dir,
                    price=price, quantity=quantity,
                    open_reason='SHADOW_A_REVERSAL',
                    market_state=market_state,
                    signal_strength=signal_strength,
                )
                self._shadow_a_trades.append(record_a)
                self._stats['shadow_a_trades'] += 1
                self._enqueue_trade_log(record_a)
                # P0-裂缝23：批量接口也更新独立模拟账户（shadow_a）
                self.update_paper_account(
                    instrument_id=instrument_id,
                    direction=reversed_dir,
                    quantity=quantity if quantity > 0 else 1,
                    price=price if price > 0 else 0.0,
                    commission=self._estimate_commission(price, quantity),
                )

                random_dir = self._rng.choice(['long', 'short'])
                record_b = ShadowTradeRecord(
                    trade_id=self._generate_trade_id('B'),
                    shadow_type='shadow_b',
                    timestamp=datetime.now(CHINA_TZ).isoformat(),
                    instrument_id=instrument_id,
                    direction=random_dir,
                    price=price, quantity=quantity,
                    open_reason='SHADOW_B_RANDOM',
                    market_state=market_state,
                    signal_strength=signal_strength,
                )
                self._shadow_b_trades.append(record_b)
                self._stats['shadow_b_trades'] += 1
                self._enqueue_trade_log(record_b)
                # P0-裂缝23：批量接口也更新独立模拟账户
                self.update_paper_account(
                    instrument_id=instrument_id,
                    direction=random_dir,
                    quantity=quantity if quantity > 0 else 1,
                    price=price if price > 0 else 0.0,
                    commission=self._estimate_commission(price, quantity),
                )

                results.append({'shadow_a': record_a, 'shadow_b': record_b})

        now = time.time()
        # R24-P2-CF-07修复: 将_last_summary_time更新放入锁内，防止并发双重汇总
        with self._lock:
            if now - self._last_summary_time > self._summary_interval_hours * 3600:
                self._last_summary_time = now
                _should_summary = True
            else:
                _should_summary = False
        if _should_summary:
            self.generate_daily_summary()

        return results
    def get_paper_account(self) -> Dict[str, Any]:
        """P0-裂缝11：获取影子策略独立模拟账户状态"""
        with self._lock:
            return dict(self._paper_account)
    def update_paper_account(self, instrument_id: str, direction: str,
                              quantity: int, price: float,
                              commission: float = 0.0) -> Dict[str, Any]:
        """P0-裂缝11：更新影子策略模拟账户（不影响主策略）

        Args:
            instrument_id: 合约ID
            direction: 'long'/'short'/'close_long'/'close_short'
            quantity: 数量
            price: 成交价格
            commission: 手续费

        Returns:
            更新后的paper_account状态
        """
        with self._lock:
            acct = self._paper_account
            pos_size = acct['positions'].get(instrument_id, 0)

            if direction in ('long',):
                cost = price * quantity
                acct['current_equity'] -= cost + commission
                acct['positions'][instrument_id] = pos_size + quantity
                acct['entry_prices'][instrument_id] = price
            elif direction in ('short',):
                proceeds = price * quantity
                acct['current_equity'] += proceeds - commission
                acct['positions'][instrument_id] = pos_size - quantity
                acct['entry_prices'][instrument_id] = price
            elif direction in ('close_long',):
                entry_price = acct['entry_prices'].get(instrument_id, price)
                pnl = (price - entry_price) * quantity
                proceeds = price * quantity
                acct['current_equity'] += proceeds - commission
                acct['positions'][instrument_id] = pos_size - quantity
                acct['realized_pnl'] += pnl
            elif direction in ('close_short',):
                entry_price = acct['entry_prices'].get(instrument_id, price)
                pnl = (entry_price - price) * quantity
                cost = price * quantity
                acct['current_equity'] -= cost + commission
                acct['positions'][instrument_id] = pos_size + quantity
                acct['realized_pnl'] += pnl

            acct['commission_paid'] += commission
            acct['trade_count'] += 1

            # 记录到影子订单日志
            self._shadow_order_log.append({
                'instrument_id': instrument_id, 'direction': direction,
                'quantity': quantity, 'price': price, 'commission': commission,
                'equity_after': acct['current_equity'],
            })

            return dict(acct)
    def is_shadow_mode(self) -> bool:
        """P0-裂缝11：确认影子策略处于隔离模式

        R13-P0-DEAD-01修复: 不再直接返回True，而是验证隔离性：
        1. 影子订单日志中无真实order_id（不经过OrderService）
        2. paper_account持仓与主策略不重叠
        3. 所有检查通过则返回True，否则返回False并记录隔离性破坏
        """
        try:
            # 快速检查：影子订单日志中是否有真实order_id
            for log_entry in list(self._shadow_order_log)[-50:]:
                if log_entry.get('order_id', '').startswith('ORD-'):
                    logging.critical(
                        "[ShadowStrategyEngine] R13-P0-DEAD-01: 隔离性被破坏! "
                        "影子订单日志中发现真实order_id=%s",
                        log_entry.get('order_id'),
                    )
                    # 更新_is_shadow_mode状态
                    self._is_shadow_mode = False
                    return False

            # 检查paper_account持仓与主策略是否重叠
            try:
                from ali2026v3_trading.position.position_service import get_position_service
                ps = get_position_service()
                if ps:
                    shadow_positions = set(self._paper_account['positions'].keys())
                    with ps.global_lock:
                        for _inst_id, pos_dict in ps.positions.items():
                            for _pid, rec in pos_dict.items():
                                if getattr(rec, 'volume', 0) != 0 and rec.instrument_id in shadow_positions:
                                    if self._paper_account['positions'].get(rec.instrument_id, 0) != 0:
                                        logging.critical(
                                            "[ShadowStrategyEngine] R13-P0-DEAD-01: 隔离性被破坏! "
                                            "paper_account与主策略持仓重叠 instrument=%s",
                                            rec.instrument_id,
                                        )
                                        self._is_shadow_mode = False
                                        return False
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                pass  # PositionService不可用时默认通过

            # 所有检查通过，确认处于隔离模式
            self._is_shadow_mode = True
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[ShadowStrategyEngine] R13-P0-DEAD-01: is_shadow_mode检查异常: %s", e)
            # 异常情况下保守返回False
            self._is_shadow_mode = False
            return False
    def verify_shadow_isolation(self) -> Dict[str, Any]:
        """P0-裂缝11+R13-DEAD-01：验证影子策略与主策略完全隔离

        通过标准：
        1. _is_shadow_mode标志为True
        2. 影子策略不使用OrderService（检查shadow_order_log无真实order_id）
        3. 影子策略使用独立paper_account（检查不与主策略持仓重叠）
        """
        with self._lock:
            # R13-DEAD-01: 实际验证隔离性而非直接返回_is_shadow_mode
            uses_order_service = False
            try:
                from ali2026v3_trading.order.order_service import get_order_service
                osvc = get_order_service()
                if osvc:
                    # 检查影子订单日志中是否有真实order_id（不应有）
                    for log_entry in list(self._shadow_order_log)[-100:]:
                        if log_entry.get('order_id', '').startswith('ORD-'):
                            uses_order_service = True
                            break
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass

            # 检查paper_account持仓与主策略是否重叠
            position_overlap = False
            try:
                from ali2026v3_trading.position.position_service import get_position_service
                ps = get_position_service()
                if ps:
                    shadow_positions = set(self._paper_account['positions'].keys())
                    with ps.global_lock:
                        for _inst_id, pos_dict in ps.positions.items():
                            for _pid, rec in pos_dict.items():
                                if getattr(rec, 'volume', 0) != 0 and rec.instrument_id in shadow_positions:
                                    if self._paper_account['positions'].get(rec.instrument_id, 0) != 0:
                                        position_overlap = True
                                        break
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass

            isolation_passed = (
                not uses_order_service
                and not position_overlap
            )
            self._is_shadow_mode = isolation_passed
            if not isolation_passed:
                logger.critical(
                    "[ShadowStrategyEngine] 🚨 Isolation check FAILED! "
                    "uses_order_service=%s position_overlap=%s",
                    uses_order_service, position_overlap,
                )

            return {
                "is_shadow_mode": isolation_passed,
                "paper_account_independent": not position_overlap,
                "paper_account_equity": self._paper_account['current_equity'],
                "paper_account_positions": len(self._paper_account['positions']),
                "paper_account_trades": self._paper_account['trade_count'],
                "shadow_order_log_size": len(self._shadow_order_log),
                "uses_order_service": uses_order_service,
                "position_overlap": position_overlap,
                "isolation_passed": isolation_passed,
            }
    def get_recent_trades(self, shadow_type: str = 'all', limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            result = []
            if shadow_type in ('all', 'shadow_a'):
                result.extend([t.to_dict() for t in self._shadow_a_trades])
            if shadow_type in ('all', 'shadow_b'):
                result.extend([t.to_dict() for t in self._shadow_b_trades])
            if shadow_type in ('all', 'master'):
                result.extend([t.to_dict() for t in self._master_trades])
            result.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return result[:limit]


__all__ = ['ShadowStrategySignalService', 'ShadowStrategySignalMixin']
ShadowStrategySignalMixin = ShadowStrategySignalService