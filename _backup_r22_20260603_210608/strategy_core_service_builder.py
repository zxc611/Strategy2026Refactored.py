"""
Phase1-Sprint3: StrategyCoreServiceBuilder — Builder模式拆解__init__(339行)
每个build步骤≤50行，渐进式构建StrategyCoreService
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional


class StrategyCoreServiceBuilder:
    """渐进式构建StrategyCoreService

    原StrategyCoreService.__init__ 339行(圈复杂度>40)拆分为:
    - build_state_store: 状态存储初始化
    - build_managers: 管理器初始化(合约/持仓/风控/诊断)
    - build_thread_pools: 线程池初始化
    - build_platform_apis: 平台API绑定
    - build_monitoring: 监控/健康检查初始化
    - build: 组装并返回StrategyCoreService实例
    """

    def __init__(self):
        self._instance: Any = None
        self._config: Dict[str, Any] = {}
        self._event_bus: Any = None

    def set_config(self, config: Dict[str, Any]) -> 'StrategyCoreServiceBuilder':
        self._config = config
        return self

    def set_event_bus(self, event_bus: Any) -> 'StrategyCoreServiceBuilder':
        self._event_bus = event_bus
        return self

    def build_state_store(self) -> 'StrategyCoreServiceBuilder':
        """构建状态存储(state_store + 单例注册)"""
        try:
            from ali2026v3_trading.state_store import StateStore
            self._state_store = StateStore()
        except ImportError:
            self._state_store = None
            logging.debug("[Builder] StateStore不可用,使用dict替代")
        return self

    def build_managers(self) -> 'StrategyCoreServiceBuilder':
        """构建管理器(合约/持仓/风控/诊断)"""
        self._managers = {}
        try:
            from ali2026v3_trading.instrument_manager import InstrumentManager
            self._managers['instrument'] = InstrumentManager()
        except ImportError:
            pass
        try:
            from ali2026v3_trading.historical_data_manager import HistoricalDataManager
            self._managers['historical'] = HistoricalDataManager()
        except ImportError:
            pass
        try:
            from ali2026v3_trading.diagnosis_service import DiagnosisService
            self._managers['diagnosis'] = DiagnosisService()
        except ImportError:
            pass
        return self

    def build_thread_pools(self) -> 'StrategyCoreServiceBuilder':
        """构建线程池(订阅/诊断/心跳)"""
        from concurrent.futures import ThreadPoolExecutor
        self._thread_pools = {
            'subscription': ThreadPoolExecutor(max_workers=4, thread_name_prefix='sub'),
            'diagnosis': ThreadPoolExecutor(max_workers=2, thread_name_prefix='diag'),
            'heartbeat': ThreadPoolExecutor(max_workers=1, thread_name_prefix='hb'),
        }
        return self

    def build_platform_apis(self) -> 'StrategyCoreServiceBuilder':
        """构建平台API绑定(延迟,需运行时注入)"""
        self._platform_apis = {
            'insert_order': None,
            'cancel_order': None,
            'query_order': None,
        }
        return self

    def build_monitoring(self) -> 'StrategyCoreServiceBuilder':
        """构建监控/健康检查"""
        self._monitoring = {
            'health_check_interval': self._config.get('health_check_interval', 30.0),
            'performance_monitor': None,
            'health_aggregator': None,
        }
        try:
            from ali2026v3_trading.performance_monitor import PerformanceMonitor
            self._monitoring['performance_monitor'] = PerformanceMonitor()
        except ImportError:
            pass
        return self

    def build(self) -> Any:
        """组装并返回StrategyCoreService实例"""
        from ali2026v3_trading.strategy_core_service import StrategyCoreService
        self._instance = StrategyCoreService(event_bus=self._event_bus)
        if hasattr(self._instance, '_state_store') and self._state_store is not None:
            self._instance._state_store = self._state_store
        if hasattr(self._instance, '_managers'):
            self._instance._managers = self._managers
        if hasattr(self._instance, '_thread_pools'):
            self._instance._thread_pools = self._thread_pools
        if hasattr(self._instance, '_monitoring'):
            self._instance._monitoring = self._monitoring
        return self._instance