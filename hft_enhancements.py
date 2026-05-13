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
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.signal_service import (
    KalmanFilter1D,
    EMASignalFilter,
    SignalTimingFilter,
)
from ali2026v3_trading.order_service import (
    SmartOrderSplitter,
    OrderSplitStrategy,
    SplitOrderResult,
)
from ali2026v3_trading.strategy_tick_handler import (
    DynamicPursuitEngine,
    PursuitPosition,
)
from ali2026v3_trading.order_flow_bridge import (
    MicrostructureArbitrageDetector,
    ArbitrageOpportunity,
    MarketMakerDefenseEngine,
    OrderDefenseType,
    DefensiveOrder,
)
from ali2026v3_trading.order_flow_analyzer import (
    VolumeWeightedOrderFlow,
)
from ali2026v3_trading.state_param_manager import (
    StateTransitionCapture,
    StateTransitionEvent,
    TransitionPoint,
)


class HFTEnhancementEngine:
    """HFT增强引擎 - 七大竞争优势统一入口

    将7个增强模块统一管理, 提供一站式API。
    各模块实现分散在其架构归属模块中, 本类仅做编排调度。
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}

        self.signal_filter = SignalTimingFilter(
            threshold=cfg.get('signal_filter_threshold', 0.6),
            use_kalman=cfg.get('use_kalman', True),
        )

        self.order_splitter = SmartOrderSplitter(
            max_depth_levels=cfg.get('max_depth_levels', 5),
            aggressive_signal_threshold=cfg.get('aggressive_threshold', 0.8),
            passive_signal_threshold=cfg.get('passive_threshold', 0.6),
        )

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

        self._lock = threading.RLock()
        self._initialized = True

        logging.info("[HFTEnhancementEngine] 七大竞争优势引擎初始化完成(分散架构)")

    def on_tick_enhanced(
        self, instrument_id: str, price: float, volume: int,
        direction: str, product: str = '',
        bid_price: float = 0.0, ask_price: float = 0.0,
        resonance_strength: float = 0.0, prev_resonance_strength: float = 0.0,
        current_state: str = 'other', prev_state: str = 'other',
    ) -> Dict[str, Any]:
        direction_lower = (direction or '').lower()
        direction_upper = direction_lower.upper()
        result = {
            'signal_filter': None,
            'pursuit_signal': None,
            'pursuit_exit': None,
            'arbitrage_signal': None,
            'transition_signal': None,
            'smart_money_signal': None,
        }

        try:
            if resonance_strength > 0:
                result['signal_filter'] = self.signal_filter.filter_signal(
                    instrument_id, resonance_strength
                )
        except Exception as e:
            logging.debug("[HFT] signal_filter error: %s", e)

        try:
            if resonance_strength > 0 and prev_resonance_strength > 0:
                pursuit = self.pursuit_engine.evaluate_surge(
                    instrument_id, resonance_strength, prev_resonance_strength,
                    price, direction_upper
                )
                if pursuit:
                    result['pursuit_signal'] = pursuit
        except Exception as e:
            logging.debug("[HFT] pursuit_engine error: %s", e)

        try:
            if price > 0:
                if direction_upper in ('BUY', 'SELL'):
                    self.pursuit_engine.update_trailing_stop(instrument_id, price, direction_upper)
                exit_sig = self.pursuit_engine.check_exit(instrument_id, price)
                if exit_sig:
                    result['pursuit_exit'] = exit_sig
        except Exception as e:
            logging.debug("[HFT] pursuit_engine trailing/exit error: %s", e)

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
        except Exception as e:
            logging.debug("[HFT] arbitrage_detector error: %s", e)

        try:
            if product:
                self.volume_weighted_flow.on_trade(product, price, volume, direction_lower)
                smart_money = self.volume_weighted_flow.calc_smart_money_flow(product)
                if smart_money['signal'] != 'neutral':
                    result['smart_money_signal'] = smart_money
        except Exception as e:
            logging.debug("[HFT] volume_weighted_flow error: %s", e)

        try:
            if current_state != prev_state:
                transition = self.transition_capture.on_state_change(
                    prev_state, current_state, resonance_strength, price
                )
                if transition:
                    result['transition_signal'] = transition
        except Exception as e:
            logging.debug("[HFT] transition_capture error: %s", e)

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
