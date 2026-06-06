"""
Phase1-Sprint3: StrategyCoreServiceBuilder — Builder模式拆解__init__(339行)
每个build步骤≤50行，渐进式构建StrategyCoreService
修复: build()不再递归创建新实例，改为apply_to()注入到已有实例
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
    - apply_to: 将构建结果注入到已有StrategyCoreService实例(非递归)
    """

    def __init__(self):
        self._built_state_store: Any = None
        self._built_managers: Dict[str, Any] = {}
        self._built_thread_pools: Dict[str, Any] = {}
        self._built_platform_apis: Dict[str, Any] = {}
        self._built_monitoring: Dict[str, Any] = {}
        self._config: Dict[str, Any] = {}
        self._event_bus: Any = None

    def set_config(self, config: Dict[str, Any]) -> 'StrategyCoreServiceBuilder':
        self._config = config
        return self

    def set_event_bus(self, event_bus: Any) -> 'StrategyCoreServiceBuilder':
        self._event_bus = event_bus
        return self

    def build_state_store(self) -> 'StrategyCoreServiceBuilder':
        try:
            from ali2026v3_trading.state_store import StateStore
            self._built_state_store = StateStore()
        except ImportError:
            self._built_state_store = None
            logging.debug("[Builder] StateStore不可用,使用dict替代")
        return self

    def build_managers(self) -> 'StrategyCoreServiceBuilder':
        self._built_managers = {}
        try:
            from ali2026v3_trading.instrument_manager import InstrumentManager
            self._built_managers['instrument'] = InstrumentManager()
        except ImportError:
            pass
        try:
            from ali2026v3_trading.historical_data_manager import HistoricalDataManager
            self._built_managers['historical'] = HistoricalDataManager()
        except ImportError:
            pass
        try:
            from ali2026v3_trading.diagnosis_service import DiagnosisService
            self._built_managers['diagnosis'] = DiagnosisService()
        except ImportError:
            pass
        return self

    def build_thread_pools(self) -> 'StrategyCoreServiceBuilder':
        from concurrent.futures import ThreadPoolExecutor
        self._built_thread_pools = {
            'subscription': ThreadPoolExecutor(max_workers=4, thread_name_prefix='sub'),
            'diagnosis': ThreadPoolExecutor(max_workers=2, thread_name_prefix='diag'),
            'heartbeat': ThreadPoolExecutor(max_workers=1, thread_name_prefix='hb'),
        }
        return self

    def build_platform_apis(self) -> 'StrategyCoreServiceBuilder':
        self._built_platform_apis = {
            'insert_order': None,
            'cancel_order': None,
            'query_order': None,
        }
        return self

    def build_monitoring(self) -> 'StrategyCoreServiceBuilder':
        self._built_monitoring = {
            'health_check_interval': self._config.get('health_check_interval', 30.0),
            'performance_monitor': None,
            'health_aggregator': None,
        }
        try:
            from ali2026v3_trading.performance_monitor import PerformanceMonitor
            self._built_monitoring['performance_monitor'] = PerformanceMonitor()
        except ImportError:
            pass
        return self

    def apply_to(self, instance: Any) -> Any:
        """将构建结果注入到已有StrategyCoreService实例(非递归)"""
        if self._built_state_store is not None:
            instance._state_store = self._built_state_store
        if self._built_managers:
            instance._managers = self._built_managers
        if self._built_thread_pools:
            instance._thread_pools = self._built_thread_pools
        if self._built_platform_apis:
            instance._platform_apis = self._built_platform_apis
        if self._built_monitoring:
            instance._monitoring = self._built_monitoring
        return instance

    def build(self) -> Any:
        """创建新StrategyCoreService实例并注入构建结果(非递归: event_bus直传)"""
        from ali2026v3_trading.strategy_core_service import StrategyCoreService
        instance = StrategyCoreService.__new__(StrategyCoreService)
        instance.strategy_id = f"strategy_{int(time.time())}"
        instance._event_bus = self._event_bus
        self.apply_to(instance)
        return instance
