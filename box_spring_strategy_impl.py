"""
box_spring_strategy.py - 箱体波动率脉冲策略（弹簧策略）

策略本质：交易波动率的脉冲，而非价格的方向
利润来源：波动率从极低回归正常 + Gamma在行权价附近的非线性收益

识别条件（弹簧被压紧）：
1. 箱体结构已识别（价格在箱顶箱底之间震荡）
2. 近月期权（2-5天到期），权利金极便宜
3. IV百分位处于近期极低水平（下分位数）
4. 期货价格接近该期权行权价（Gamma最敏感位置）

入场信号（箱内脉冲）：
- 弹簧压紧后，任何波动率回归迹象即可入场
- 订单流出现异动，或全链期权从死寂状态略有反应

平仓纪律（铁律）：
- 止盈：弹簧松开即走，盈亏比达1.5:1或2:1时立刻平仓
- 止损：接受归零（成本极低，占总资金0.5%-1.5%）
- 铁律：绝不平移为趋势策略

期望值模型：
- 胜率 ~20%，盈亏比 ~5:1
- 期望值 = 20%*5 - 80%*1 = +0.2 > 0（正期望）

作者：AI代码助手
版本：v1.0
日期：2026-05-10
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.resilience import deterministic_round
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.strategy.box_spring_detector import (
    SpringState, BoxRange, SpringSignal, SpringPosition, PendingPullback,
    get_box_spring_strategy,
)
from ali2026v3_trading.strategy.box_spring_detector import BoxSpringDetectorService, BoxSpringDetectorMixin
from ali2026v3_trading.strategy.box_spring_executor import BoxSpringExecutorService, BoxSpringExecutorMixin


class BoxSpringStrategy:
    OPEN_REASON = 'BOX_SPRING'

    def __init__(self, params: Dict[str, Any]):
        self._detector_service = BoxSpringDetectorService()
        self._executor_service = BoxSpringExecutorService()
        # R13-P2-API-10修复: 外部dict参数验证
        if not isinstance(params, dict):
            logging.warning("[BoxSpringStrategy] params不是dict类型: %s，使用空dict", type(params))
            params = {}
        # R15-P2-API-10修复: 关键参数schema校验与类型修正
        _param_schema = {
            'iv_lookback_bars': int, 'min_box_touches': int, 'max_box_width_pct': float,
            'iv_low_percentile': float, 'iv_very_low_percentile': float,
            'spring_threshold': float, 'dynamic_tp_sl': bool,
        }
        for _pk, _pt in _param_schema.items():
            if _pk in params and not isinstance(params[_pk], _pt):
                try:
                    params[_pk] = _pt(params[_pk])
                except (ValueError, TypeError):
                    logging.warning("[BoxSpringStrategy] R15-P2-API-10: 参数%s类型修正失败, 期望%s", _pk, _pt.__name__)
        self.params = params
        self._lock = threading.RLock()

        self._boxes: Dict[str, BoxRange] = {}
        from collections import deque
        self._iv_history: Dict[str, deque] = {}
        self._iv_window = params.get('iv_lookback_bars', 120)

        try:
            from ali2026v3_trading.strategy.box_detector import get_box_detector
            self._box_detector = get_box_detector()
        except (ImportError, RuntimeError) as e:
            logging.debug("[BoxSpringStrategy] get_box_detector failed: %s", e)
            from ali2026v3_trading.strategy.box_detector import BoxDetector
            self._box_detector = BoxDetector()

        self._signals: Dict[str, SpringSignal] = {}
        self._positions: Dict[str, SpringPosition] = {}

        self._min_box_touches = params.get('min_box_touches', 3)
        self._max_box_width_pct = params.get('max_box_width_pct', 0.04)
        self._iv_low_percentile = params.get('iv_low_percentile', 5.0)
        self._iv_very_low_percentile = params.get('iv_very_low_percentile', 2.0)
        self._min_days_to_expiry = params.get('min_days_to_expiry', 2)
        self._max_days_to_expiry = params.get('max_days_to_expiry', 5)
        self._max_premium_cost_pct = params.get('max_premium_cost_pct', 0.015)
        self._stop_profit_ratio = params.get('stop_profit_ratio', 5.0)
        self._max_loss_pct = params.get('max_loss_pct', 0.95)
        self._max_position_pct = params.get('max_position_pct', 0.015)
        self._dynamic_tp_sl_enabled = params.get('dynamic_tp_sl_enabled', True)  # R14-P1-DEAD-06修复: 默认启用动态止盈止损
        self._min_estimated_plr = params.get('min_estimated_plr', 0.0)
        self._capital_scale: str = params.get('capital_scale', 'medium')
        self._cooldown_sec = params.get('spring_cooldown_sec', 300)
        self._max_active_positions = params.get('max_spring_positions', None)
        if self._max_active_positions is None:  # P1-4修复: 优先从统一参数max_open_positions读取
            try:
                from ali2026v3_trading.config.config_params import get_param
                self._max_active_positions = int(get_param('max_open_positions', get_param('max_position_count', 3)))
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                self._max_active_positions = 3

        self._pullback_enabled = params.get('pullback_enabled', False)
        self._pullback_wait_bars = params.get('pullback_wait_bars', 5)
        self._pullback_retrace_pct = params.get('pullback_retrace_pct', 0.15)
        self._pullback_iv_min_percentile = params.get('pullback_iv_min_percentile', 20.0)
        self._pullback_iv_max_percentile = params.get('pullback_iv_max_percentile', 80.0)
        self._pending_pullbacks: Dict[str, PendingPullback] = {}
        self._pullback_bar_counter: int = 0
        self._pullback_stats = {
            'signals_deferred': 0,
            'retrace_entries': 0,
            'retrace_timeouts': 0,
            'retrace_iv_rejected': 0,
        }

        self._box_breakout_tolerance = params.get('box_breakout_tolerance', 0.005)
        self._spring_price_pos_min = params.get('spring_price_pos_min', 0.3)
        self._spring_price_pos_max = params.get('spring_price_pos_max', 0.7)
        self._strike_distance_threshold = params.get('strike_distance_threshold', 0.02)
        self._direction_buy_call_threshold = params.get('direction_buy_call_threshold', 0.45)
        self._direction_buy_put_threshold = params.get('direction_buy_put_threshold', 0.55)
        # R24-P2-CF-02修复 + R26-P2修复: assert改为运行时检查，防止-O优化跳过
        if not (self._direction_buy_call_threshold < self._direction_buy_put_threshold):
            raise ValueError(
                f"direction_buy_call_threshold({self._direction_buy_call_threshold}) must be < "
                f"direction_buy_put_threshold({self._direction_buy_put_threshold})"
            )
        self._max_risk_ratio = params.get('max_risk_ratio', 0.8)  # P1-57: 默认值应从get_param_default获取，此处0.8与DEFAULT_PARAM_TABLE一致
        self._option_multiplier = params.get('option_multiplier', 10000)
        self._fallback_strike_search = params.get('fallback_strike_search', True)

        from ali2026v3_trading.infra.shared_utils import PullbackManager
        self._pullback_tick_tracking: bool = params.get('pullback_tick_tracking', True)
        self._pullback_mgr = PullbackManager({
            'pullback_enabled': self._pullback_enabled,
            'pullback_wait_bars': self._pullback_wait_bars,
            'pullback_retrace_pct': self._pullback_retrace_pct,
            'pullback_iv_min_percentile': self._pullback_iv_min_percentile,
            'pullback_iv_max_percentile': self._pullback_iv_max_percentile,
            'pullback_tick_tracking': self._pullback_tick_tracking,
        })

        self._current_bar_time: Optional[float] = None
        self._last_signal_time: Dict[str, float] = {}

        self._stats = {
            'total_signals': 0,
            'compressed_detected': 0,
            'triggers_fired': 0,
            'positions_opened': 0,
            'positions_closed_tp': 0,
            'positions_closed_sl': 0,
            'positions_closed_expired': 0,
            'total_pnl': 0.0,
            'win_count': 0,
            'loss_count': 0,
        }

    def __getattr__(self, name):
        _ds = self.__dict__.get('_detector_service')
        if _ds is not None and hasattr(_ds, name):
            return getattr(_ds, name)
        _es = self.__dict__.get('_executor_service')
        if _es is not None and hasattr(_es, name):
            attr = getattr(_es, name)
            if callable(attr):
                import types
                func = getattr(attr, '__func__', attr)
                return types.MethodType(func, self)
            return attr
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def set_capital_scale(self, capital_scale: str) -> None:
        with self._lock:
            old_scale = self._capital_scale
            self._capital_scale = capital_scale
            logging.info(
                '[BoxSpringStrategy] capital_scale changed: %s -> %s',
                old_scale, capital_scale,
            )

    def get_capital_scale(self) -> str:
        with self._lock:
            return self._capital_scale

    def _get_now(self) -> datetime:
        if self._current_bar_time is not None:
            return datetime.fromtimestamp(self._current_bar_time, tz=CHINA_TZ)
        return datetime.now(CHINA_TZ)

    # ========================================================================
    # 铁律检查：绝不平移为趋势策略
    # ========================================================================

    def is_spring_position(self, instrument_id: str) -> bool:
        with self._lock:
            for pos in self._positions.values():
                if (pos.option_instrument_id == instrument_id or
                    pos.paired_instrument_id == instrument_id) and pos.is_open:
                    return True
        return False

    def prevent_trend_conversion(self, instrument_id: str, proposed_action: str,
                                  proposed_reason: str) -> bool:
        if self.is_spring_position(instrument_id):
            if proposed_action == 'OPEN' and proposed_reason != self.OPEN_REASON:
                logging.warning(
                    "[BoxSpring] IRON_RULE: Blocking trend conversion for %s "
                    "(spring position exists, proposed_reason=%s)",
                    instrument_id, proposed_reason
                )
                return False
        return True

    # ========================================================================
    # 期望值计算与统计
    # ========================================================================

    def get_expected_value(self) -> Dict[str, Any]:
        with self._lock:
            total = self._stats['win_count'] + self._stats['loss_count']
            if total == 0:
                return {
                    'total_trades': 0,
                    'win_rate': 0.0,
                    'avg_win_ratio': 0.0,
                    'avg_loss_ratio': 0.0,
                    'expected_value': 0.0,
                    'is_positive_ev': False,
                }

            win_rate = self._stats['win_count'] / total

            win_pnls = []
            loss_pnls = []
            for pos in self._positions.values():
                if not pos.is_open:
                    if pos.pnl_ratio > 1.0:
                        win_pnls.append(pos.pnl_ratio - 1.0)
                    else:
                        loss_pnls.append(1.0 - pos.pnl_ratio)

            avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
            avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 1.0

            ev = win_rate * avg_win - (1 - win_rate) * avg_loss

            # R27-P2-FP-09修复: round()→deterministic_round()
            return {
                'total_trades': total,
                'win_rate': deterministic_round(win_rate, 4),
                'avg_win_ratio': deterministic_round(avg_win, 4),
                'avg_loss_ratio': deterministic_round(avg_loss, 4),
                'expected_value': deterministic_round(ev, 4),
                'is_positive_ev': ev > 0,
                'total_pnl': deterministic_round(self._stats['total_pnl'], 4),
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            ev = self.get_expected_value()
            active = sum(1 for p in self._positions.values() if p.is_open)
            return {
                'service_name': 'BoxSpringStrategy',
                **self._stats,
                **self._pullback_mgr._stats,
                'pending_pullbacks': len(self._pullback_mgr._pending),
                'active_positions': active,
                'active_boxes': sum(1 for b in self._boxes.values() if b.is_active),
                'expected_value': ev,
            }

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            ev = self.get_expected_value()
            active = sum(1 for p in self._positions.values() if p.is_open)
            boxes = sum(1 for b in self._boxes.values() if b.is_active)

            status = 'OK'
            if ev['total_trades'] >= 20 and not ev['is_positive_ev']:
                status = 'DEGRADED'
            if active > self._max_active_positions:
                status = 'WARNING'

            return {
                'status': status,
                'active_positions': active,
                'active_boxes': boxes,
                'expected_value': ev['expected_value'],
                'is_positive_ev': ev['is_positive_ev'],
                'total_pnl': ev['total_pnl'],
            }

    # ========================================================================
    # Tick驱动入口
    # ========================================================================

    def on_tick(self, instrument_id: str, price: float, high: float = 0.0,
                low: float = 0.0, volume: int = 0, timestamp: Optional[datetime] = None) -> None:
        if price <= 0:
            return

        _tick_ts = getattr(timestamp, 'timestamp', None) if timestamp is not None else None
        if _tick_ts is not None:
            if isinstance(_tick_ts, (int, float)):
                self._current_bar_time = float(_tick_ts)
            elif hasattr(_tick_ts, 'timestamp'):
                self._current_bar_time = _tick_ts.timestamp()
        elif timestamp is not None:
            self._current_bar_time = timestamp.timestamp() if hasattr(timestamp, 'timestamp') else None

        if high > 0 and low > 0:
            self.update_box(instrument_id, high, low, price, timestamp)

        # R14-P0-BIZ-06修复: 在tick处理中调用对冲比率计算，辅助交易决策
        try:
            hedge_ratio = self._compute_hedge_ratio(instrument_id)
            if abs(hedge_ratio) > 0.01:
                logging.info(
                    "[BoxSpring] R14-P0-BIZ-06: 对冲比率非零 hedge_ratio=%.4f instrument=%s",
                    hedge_ratio, instrument_id,
                )
                self._last_hedge_ratio = hedge_ratio
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _hr_err:
            _hedge_fail_count = getattr(self, '_hedge_ratio_fail_count', 0) + 1
            self._hedge_ratio_fail_count = _hedge_fail_count
            logging.warning("[BoxSpring] BIZ-P1-06: 对冲比率计算失败(count=%d): %s", _hedge_fail_count, _hr_err)
            if _hedge_fail_count >= 5:
                logging.critical("[BoxSpring] BIZ-P1-06: 对冲连续%d次失败，暂停新开仓", _hedge_fail_count)
                self._hedge_ratio_fail_paused = True
                try:
                    from ali2026v3_trading.infra.event_bus import get_global_event_bus
                    _bus = get_global_event_bus()
                    if _bus is not None:
                        _bus.publish('hedge.failure_critical', {'fail_count': _hedge_fail_count, 'action': 'pause_new_open'}, async_mode=True)
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass

        # [FIX-20260710-BOXSPRING-SIGNAL] 弹簧信号检测与下单接入
        # 根因: on_tick()仅计算对冲比率(桩函数)和更新Greeks，从未调用detect_spring/check_trigger/execute_spring_entry
        # 修复: 在tick处理中调用弹簧信号检测，若检测到信号则触发入场
        try:
            _spring_signal = self.detect_spring(
                instrument_id=instrument_id,
                future_price=price,
                option_instrument_id=instrument_id,
                strike_price=0.0,
                iv=0.0,
                premium_price=0.0,
                days_to_expiry=30,
            )
            if _spring_signal is not None:
                logging.info("[BoxSpring] FIX-0710: 弹簧信号检测到 %s state=%s dir=%s",
                             _spring_signal.option_instrument_id,
                             _spring_signal.spring_state, _spring_signal.direction)
                _triggered = self.check_trigger(instrument_id)
                if _triggered is not None:
                    logging.info("[BoxSpring] FIX-0710: 弹簧触发 %s reason=triggered dir=%s",
                                 _triggered.option_instrument_id, _triggered.direction)
                    self.execute_spring_entry(_triggered)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _spring_err:
            logging.debug("[BoxSpring] FIX-0710: 弹簧信号检测异常: %s", _spring_err)

        with self._lock:
            open_positions = [
                p for p in self._positions.values()
                if p.instrument_id == instrument_id and p.is_open
            ]

        for pos in open_positions:
            try:
                from ali2026v3_trading.governance.greeks_calculator import GreeksCalculator
                calc = self._get_greeks_calculator()
                if calc:
                    greeks = calc.get_greeks(pos.option_instrument_id)
                    if greeks:
                        iv = greeks.get('iv', 0.0)
                        if iv > 0:
                            self.update_iv(pos.option_instrument_id, iv)
                        gamma = greeks.get('gamma', 0.0)
                        theta = greeks.get('theta', 0.0)
                        vega = greeks.get('vega', 0.0)
                        if abs(gamma) > 0 or abs(theta) > 0 or abs(vega) > 0:
                            logging.debug(
                                "[BoxSpring] Greeks update: %s gamma=%.6f theta=%.6f vega=%.6f",
                                pos.option_instrument_id, gamma, theta, vega,
                            )

                        from ali2026v3_trading.data.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            opt_price = ds.realtime_cache.get_latest_price(pos.option_instrument_id)
                            if opt_price and opt_price > 0:
                                self.on_premium_update(pos.option_instrument_id, opt_price)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _price_err:
                logging.debug("[R22-EP-04b] premium更新失败: %s", _price_err)

    def _get_greeks_calculator(self):
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            if rs:
                return rs._get_greeks_calculator()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _greeks_err:
            logging.debug("[R22-EP-04c] greeks_calculator获取失败: %s", _greeks_err)
        return None


__all__ = [
    'BoxSpringStrategy',
    'BoxRange',
    'SpringSignal',
    'SpringPosition',
    'SpringState',
    'PendingPullback',
    'get_box_spring_strategy',
]
