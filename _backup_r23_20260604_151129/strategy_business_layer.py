"""
StrategyCoreService 业务层 — 从strategy_core_service.py拆分
职责: 交易执行、订单/持仓服务惰性初始化、生态系统互斥、影子策略推送
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional


class StrategyBusinessLayer:
    def __init__(self, provider):
        self._provider = provider

    def ensure_order_service(self):
        provider = self._provider
        if getattr(provider, '_order_service', None) is not None:
            return provider._order_service
        with provider._service_lock:
            if getattr(provider, '_order_service', None) is not None:
                return provider._order_service
            try:
                from ali2026v3_trading.order_service import get_order_service
                from ali2026v3_trading.event_bus import get_event_bus
                _event_bus = get_event_bus()
                provider._order_service = get_order_service(event_bus=_event_bus)
                return provider._order_service
            except Exception as e:
                logging.error("[StrategyBusinessLayer] OrderService初始化失败: %s", e)
                return None

    def ensure_position_service(self):
        provider = self._provider
        if getattr(provider, '_position_service', None) is not None:
            return provider._position_service
        with provider._service_lock:
            if getattr(provider, '_position_service', None) is not None:
                return provider._position_service
            try:
                from ali2026v3_trading.position_service import get_position_service
                provider._position_service = get_position_service()
                return provider._position_service
            except Exception as e:
                logging.error("[StrategyBusinessLayer] PositionService初始化失败: %s", e)
                return None

    def ensure_hft_engine(self):
        provider = self._provider
        if getattr(provider, '_hft_engine', None) is not None:
            return provider._hft_engine
        try:
            from ali2026v3_trading.hft_enhancements import HFTEnhancementEngine
            provider._hft_engine = HFTEnhancementEngine(provider)
            return provider._hft_engine
        except Exception as e:
            logging.warning("[StrategyBusinessLayer] HFT引擎初始化失败: %s", e)
            return None

    def check_ecosystem_exclusion(self, strategy_name: str) -> bool:
        try:
            ecosystem = getattr(self._provider, '_strategy_ecosystem', None)
            if ecosystem and hasattr(ecosystem, 'is_excluded'):
                return ecosystem.is_excluded(strategy_name)
        except Exception:
            pass
        return False

    def feed_shadow_engine(self, signals: List[Dict[str, Any]]) -> None:
        try:
            shadow = getattr(self._provider, '_shadow_engine', None)
            if shadow and hasattr(shadow, 'on_signal'):
                for sig in signals:
                    shadow.on_signal(sig)
        except Exception as e:
            logging.warning("[StrategyBusinessLayer] 影子引擎推送失败: %s", e)