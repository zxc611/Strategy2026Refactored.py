"""
HFT增强引擎 - 七大竞争优势统一入口(薄门面)

实际实现已分散到架构归属模块:
1. 信号时序滤波 → signal_service.py (KalmanFilter1D, EMASignalFilter, SignalTimingFilter)
2. 智能订单拆分 → order_service.py (SmartOrderSplitter, OrderSplitStrategy, SplitOrderResult)
3. 动态追击算法 → strategy_tick_handler.py (DynamicPursuitEngine, PursuitPosition)
4. 微观结构套利 → order_flow_bridge.py (MicrostructureArbitrageDetector, ArbitrageOpportunity)
5. 成交量加权订单流 → order_flow_analyzer.py (VolumeWeightedOrderFlow)
6. 状态转换捕捉 → state_param_manager.py (StateTransitionCapture, StateTransitionEvent, TransitionPoint)
7. 做市商防御 → order_flow_bridge.py (MarketMakerDefenseEngine, OrderDefenseType, DefensiveOrder)

本模块仅提供 HFTEnhancementEngine 统一编排入口。
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from signal.signal_service import (
    KalmanFilter1D,
    EMASignalFilter,
    SignalTimingFilter,
)
try:
    from order.order_service import (
        SmartOrderSplitter,
        OrderSplitStrategy,
        SplitOrderResult,
    )
except ImportError as _e:
    import logging as _log
    _log.warning("[hft_enhancements] SmartOrderSplitter导入失败(降级运行): %s", _e)
    SmartOrderSplitter = None  # type: ignore[assignment,misc]
    OrderSplitStrategy = None  # type: ignore[assignment,misc]
    SplitOrderResult = None  # type: ignore[assignment,misc]
from strategy.tick_hft import (
    DynamicPursuitEngine,
    PursuitPosition,
)
from order.order_flow_bridge import (
    MicrostructureArbitrageDetector,
    ArbitrageOpportunity,
    MarketMakerDefenseEngine,
    OrderDefenseType,
    DefensiveOrder,
)
from order.order_flow_analyzer import (
    VolumeWeightedOrderFlow,
)
from config.state_param import (
    StateTransitionCapture,
    StateTransitionEvent,
    TransitionPoint,
)


_MODULE_KEYS = [
    'signal_filter', 'pursuit_engine', 'arbitrage_detector',
    'volume_weighted_flow', 'transition_capture', 'defense_engine',
]

_DEFAULT_SAMPLE_RATES = {
    'signal_filter': 1,
    'pursuit_engine': 1,
    'arbitrage_detector': 5,
    'volume_weighted_flow': 1,
    'transition_capture': 1,
    'defense_engine': 10,
}

_DEFAULT_MIN_INTERVALS = {
    'signal_filter': 0.0,
    'pursuit_engine': 0.0,
    'arbitrage_detector': 0.05,
    'volume_weighted_flow': 0.0,
    'transition_capture': 0.0,
    'defense_engine': 0.1,
}


class HFTEnhancementEngine:
    """HFT增强引擎 - 七大竞争优势统一入口

    将7个增强模块统一管理, 提供一站式API。
    各模块实现分散在其架构归属模块中, 本类仅做编排调度。
    
    性能优化:
    - 模块开关: 每个模块可独立启用/禁用
    - 采样频率: 低优先级模块按采样率执行(如套利每5tick执行1次)
    - 最小间隔: 模块执行的最小时间间隔(秒)
    - 条件前置: 不满足前置条件直接跳过
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}

        self.signal_filter = SignalTimingFilter(
            threshold=cfg.get('signal_filter_threshold', 0.6),
            use_kalman=cfg.get('use_kalman', True),
        )

        if SmartOrderSplitter is not None:
            self.order_splitter = SmartOrderSplitter(
                max_depth_levels=cfg.get('max_depth_levels', 5),
                aggressive_signal_threshold=cfg.get('aggressive_threshold', 0.8),
                passive_signal_threshold=cfg.get('passive_threshold', 0.6),
            )
        else:
            self.order_splitter = None
            logging.warning("[HFTEnhancementEngine] SmartOrderSplitter不可用, 拆单功能降级")

        self.pursuit_engine = DynamicPursuitEngine(
            surge_threshold=cfg.get('surge_threshold', 0.3),
            max_add_positions=cfg.get('max_add_positions', 3),
        )

        self.arbitrage_detector = MicrostructureArbitrageDetector(
            deviation_threshold_bps=cfg.get('arbitrage_deviation_bps', 50.0),
        )

        self.volume_weighted_flow = VolumeWeightedOrderFlow(
            large_order_threshold=cfg.get('large_order_threshold', 50),
            smart_money_threshold=cfg.get('smart_money_threshold', 100),
        )

        self.transition_capture = StateTransitionCapture(
            tight_stop_loss_pct=cfg.get('tight_stop_loss_pct', 0.15),
            quick_take_profit_ratio=cfg.get('quick_take_profit_ratio', 1.2),
        )

        self.defense_engine = MarketMakerDefenseEngine(
            ioc_signal_threshold=cfg.get('ioc_signal_threshold', 0.8),
            offset_max_ticks=cfg.get('offset_max_ticks', 3),
        )

        self._module_enabled: Dict[str, bool] = {
            k: cfg.get(f'enable_{k}', True) for k in _MODULE_KEYS
        }
        self._sample_rates: Dict[str, int] = {
            k: cfg.get(f'{k}_sample_rate', _DEFAULT_SAMPLE_RATES.get(k, 1))
            for k in _MODULE_KEYS
        }
        self._min_intervals: Dict[str, float] = {
            k: cfg.get(f'{k}_min_interval', _DEFAULT_MIN_INTERVALS.get(k, 0.0))
            for k in _MODULE_KEYS
        }

        self._tick_counter: int = 0
        self._last_exec_time: Dict[str, float] = {k: 0.0 for k in _MODULE_KEYS}
        self._min_plr_for_signal: float = cfg.get('min_plr_for_signal', 0.0)
        self._plr_filter_enabled: bool = cfg.get('plr_filter_enabled', False)

        logging.info("[HFTEnhancementEngine] 七大竞争优势引擎初始化完成(分散架构)")

    def enable_module(self, module_name: str, enabled: bool = True) -> None:
        if module_name in self._module_enabled:
            self._module_enabled[module_name] = enabled
            logging.info("[HFTEnhancementEngine] %s %s", module_name, 'enabled' if enabled else 'disabled')

    def set_sample_rate(self, module_name: str, rate: int) -> None:
        if module_name in self._sample_rates:
            self._sample_rates[module_name] = max(1, rate)

    def set_min_interval(self, module_name: str, interval: float) -> None:
        if module_name in self._min_intervals:
            self._min_intervals[module_name] = max(0.0, interval)

    def _should_execute(self, module_name: str) -> bool:
        if not self._module_enabled.get(module_name, False):
            return False
        sample_rate = self._sample_rates.get(module_name, 1)
        if sample_rate > 1 and self._tick_counter % sample_rate != 0:
            return False
        min_interval = self._min_intervals.get(module_name, 0.0)
        if min_interval > 0:
            now = time.monotonic()
            if now - self._last_exec_time[module_name] < min_interval:
                return False
        return True

    def _mark_executed(self, module_name: str) -> None:
        min_interval = self._min_intervals.get(module_name, 0.0)
        if min_interval > 0:
            self._last_exec_time[module_name] = time.monotonic()

    def on_tick_enhanced(
        self, instrument_id: str, price: float, volume: int,
        direction: str, product: str = '',
        bid_price: float = 0.0, ask_price: float = 0.0,
        resonance_strength: float = 0.0, prev_resonance_strength: float = 0.0,
        current_state: str = 'other', prev_state: str = 'other',
        estimated_plr: float = 0.0,
    ) -> Dict[str, Any]:
        self._tick_counter += 1
        if self._plr_filter_enabled and self._min_plr_for_signal > 0:
            if estimated_plr > 0 and estimated_plr < self._min_plr_for_signal:
                return {
                    'signal_filter': None, 'pursuit_signal': None,
                    'pursuit_exit': None, 'arbitrage_signal': None,
                    'transition_signal': None, 'smart_money_signal': None,
                    'plr_filtered': True,
                }
        direction_lower = (direction or '').lower()
        direction_upper = direction_lower.upper()
        result = {
            'signal_filter': None,
            'pursuit_signal': None,
            'pursuit_exit': None,
            'arbitrage_signal': None,
            'transition_signal': None,
            'smart_money_signal': None,
            'defense_signal': None,
        }

        if self._should_execute('signal_filter'):
            try:
                if resonance_strength > 0:
                    result['signal_filter'] = self.signal_filter.filter_signal(
                        instrument_id, resonance_strength
                    )
                # P1-13修复: 使用order_splitter为大单信号生成拆单计划
                try:
                    if resonance_strength > 0 and volume > 1:
                        split_result = self.order_splitter.plan_order_split(
                            instrument_id, volume, direction_upper,
                            signal_strength=resonance_strength, estimated_plr=estimated_plr,
                        )
                        if split_result and getattr(split_result, 'sub_orders', None):
                            result['order_split'] = split_result
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
                self._mark_executed('signal_filter')
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] signal_filter critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] signal_filter error: %s", e)

        if self._should_execute('pursuit_engine'):
            try:
                # FIX-2 RC-3: 放宽pursuit_signal生成条件，允许resonance_strength=0时也评估
                # 根因: resonance_strength>0 and prev_resonance_strength>0条件过严，
                #       SPM未及时更新或初期resonance=0时pursuit_signal永不生成→S1 0信号
                # 修复: 移除>0前置条件，由pursuit_engine.evaluate_surge内部决定是否产出信号
                # FIX-26 RC-34: direction=''时跳过evaluate_surge调用，避免58273次WARNING日志洪水
                # 根因: tick_hft.py中direction_raw从tick_data.get('direction','')获取，
                #       期货tick_data中direction字段经常为空→direction_upper=''→evaluate_surge拒绝
                # 修复: direction_upper非BUY/SELL时跳过evaluate_surge，仅执行trailing_stop/exit
                if direction_upper in ('BUY', 'SELL'):
                    pursuit = self.pursuit_engine.evaluate_surge(
                        instrument_id, resonance_strength, prev_resonance_strength,
                        price, direction_upper
                    )
                    if pursuit:
                        result['pursuit_signal'] = pursuit
                self._mark_executed('pursuit_engine')
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] pursuit_engine critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] pursuit_engine error: %s", e)

            try:
                if price > 0:
                    if direction_upper in ('BUY', 'SELL'):
                        self.pursuit_engine.update_trailing_stop(instrument_id, price, direction_upper)
                    exit_sig = self.pursuit_engine.check_exit(instrument_id, price)
                    if exit_sig:
                        result['pursuit_exit'] = exit_sig
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] pursuit_engine trailing/exit critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] pursuit_engine trailing/exit error: %s", e)

        if self._should_execute('arbitrage_detector'):
            try:
                if price > 0:
                    self.arbitrage_detector.update_price(instrument_id, price, product)
                    if resonance_strength != 0:
                        self.arbitrage_detector.update_resonance_state(product, resonance_strength)

                    arb = self.arbitrage_detector.detect_arbitrage(instrument_id, price, product)
                    if arb:
                        result['arbitrage_signal'] = {
                            'opportunity_id': arb.opportunity_id,
                            'direction': arb.direction,
                            'deviation_bps': arb.deviation_bps,
                            'confidence': arb.confidence,
                            'entry_price': arb.entry_price,
                            'fair_value': arb.fair_value,
                        }
                self._mark_executed('arbitrage_detector')
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] arbitrage_detector critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] arbitrage_detector error: %s", e)

        if self._should_execute('volume_weighted_flow'):
            try:
                if product:
                    self.volume_weighted_flow.on_trade(product, price, volume, direction_lower)
                    smart_money = self.volume_weighted_flow.calc_smart_money_flow(product)
                    if smart_money['signal'] != 'neutral':
                        result['smart_money_signal'] = smart_money
                self._mark_executed('volume_weighted_flow')
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] volume_weighted_flow critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] volume_weighted_flow error: %s", e)

        if self._should_execute('transition_capture'):
            try:
                if current_state != prev_state:
                    transition = self.transition_capture.on_state_change(
                        prev_state, current_state, resonance_strength, price
                    )
                    if transition:
                        result['transition_signal'] = transition
                self._mark_executed('transition_capture')
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] transition_capture critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] transition_capture error: %s", e)

        if self._should_execute('defense_engine'):
            try:
                if price > 0 and direction_upper in ('BUY', 'SELL') and resonance_strength > 0:
                    defensive_orders = self.defense_engine.create_defensive_order(
                        instrument_id, direction_upper, 1, price,
                        signal_strength=resonance_strength,
                    )
                    if defensive_orders:
                        result['defense_signal'] = [
                            {
                                'order_id': o.order_id,
                                'instrument_id': o.instrument_id,
                                'direction': o.direction,
                                'volume': o.volume,
                                'price': o.price,
                                'defense_type': o.defense_type.value if hasattr(o.defense_type, 'value') else str(o.defense_type),
                            }
                            for o in defensive_orders
                        ]
                self._mark_executed('defense_engine')
            except (AttributeError, TypeError, ValueError, KeyError) as e:
                logging.error("[HFT] defense_engine critical error: %s", e, exc_info=True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[HFT] defense_engine error: %s", e)

        return result

    def get_all_stats(self) -> Dict[str, Any]:
        return {
            'signal_filter': self.signal_filter.get_stats(),
            'order_splitter': self.order_splitter.get_stats(),
            'pursuit_engine': self.pursuit_engine.get_stats(),
            'arbitrage_detector': self.arbitrage_detector.get_stats(),
            'volume_weighted_flow': self.volume_weighted_flow.get_stats(),
            'transition_capture': self.transition_capture.get_stats(),
            'defense_engine': self.defense_engine.get_stats(),
            'tick_counter': self._tick_counter,
            'module_enabled': dict(self._module_enabled),
            'sample_rates': dict(self._sample_rates),
        }


_hft_engine: Optional[HFTEnhancementEngine] = None
_hft_engine_lock = threading.Lock()


def get_hft_engine(config: Optional[Dict[str, Any]] = None) -> HFTEnhancementEngine:
    global _hft_engine
    if _hft_engine is None:
        with _hft_engine_lock:
            if _hft_engine is None:
                _hft_engine = HFTEnhancementEngine(config)
    return _hft_engine


__all__ = [
    'KalmanFilter1D',
    'EMASignalFilter',
    'SignalTimingFilter',
    'SmartOrderSplitter',
    'OrderSplitStrategy',
    'SplitOrderResult',
    'DynamicPursuitEngine',
    'PursuitPosition',
    'MicrostructureArbitrageDetector',
    'ArbitrageOpportunity',
    'VolumeWeightedOrderFlow',
    'StateTransitionCapture',
    'StateTransitionEvent',
    'TransitionPoint',
    'MarketMakerDefenseEngine',
    'OrderDefenseType',
    'DefensiveOrder',
    'HFTEnhancementEngine',
    'get_hft_engine',
]
