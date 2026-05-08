from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional


class ServiceContainer:
    """服务容器 - 完整的依赖管理

    层级规则:
      L0 基础设施: event_bus, config, params
      L1 数据层:   storage, market_data
      L2 业务层:   analytics, risk, order, signal, t_type, diagnosis, ui
      L3 策略层:   strategy_core

      L0 不依赖任何层, L1 仅依赖 L0, L2 仅依赖 L0+L1, L3 依赖 L0+L1+L2
      禁止反向依赖(高层→低层中跳过中间层)
    """

    _LAYERS = {
        'event_bus': 0, 'config': 0, 'params': 0,
        'storage': 1, 'market_data': 1,
        'analytics': 2, 'risk': 2, 'order': 2, 'signal': 2, 't_type': 2, 'diagnosis': 2, 'ui': 2,
        'strategy_core': 3,
    }

    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._dependencies: Dict[str, List[str]] = {
            'event_bus': [],
            'config': [],
            'params': [],
            'storage': ['event_bus'],
            'market_data': ['storage', 'event_bus'],
            'analytics': ['market_data'],
            'risk': ['market_data'],
            'order': ['risk'],
            'signal': ['analytics'],
            't_type': ['signal'],
            'diagnosis': ['t_type'],
            'ui': ['diagnosis'],
            'strategy_core': ['storage', 'event_bus', 'config', 'params']
        }
        self._initialization_order = [
            'event_bus', 'config', 'params', 'storage',
            'market_data', 'analytics', 'risk', 'order',
            'signal', 't_type', 'diagnosis', 'ui', 'strategy_core'
        ]

    def register(self, name: str, service: Any) -> None:
        """注册服务（带层级校验）"""
        self._services[name] = service
        svc_layer = self._LAYERS.get(name, 99)
        for dep in self._dependencies.get(name, []):
            dep_layer = self._LAYERS.get(dep, 99)
            if dep_layer >= svc_layer:
                logging.warning(
                    "[ServiceContainer] layer violation: %s(L%d) depends on %s(L%d)",
                    name, svc_layer, dep, dep_layer
                )
        logging.info("[ServiceContainer] registered: %s(L%d)", name, svc_layer)

    def get(self, name: str) -> Any:
        """获取服务"""
        return self._services.get(name)

    def initialize_all(self) -> bool:
        """按依赖顺序初始化所有服务"""
        logging.info("[ServiceContainer] initializing...")
        logging.info("[ServiceContainer] order: %s", self._initialization_order)

        if self._check_circular_dependencies():
            logging.error("[ServiceContainer] circular dependency detected!")
            return False

        for service_name in self._initialization_order:
            if service_name in self._services:
                deps = self._dependencies.get(service_name, [])
                missing_deps = [d for d in deps if d not in self._services]
                if missing_deps:
                    logging.error(
                        "[ServiceContainer] %s depends on %s, not registered",
                        service_name, missing_deps
                    )
                    return False
                logging.info("[ServiceContainer] initialized: %s", service_name)
        logging.info("[ServiceContainer] all services initialized")
        return True

    def _check_circular_dependencies(self) -> bool:
        """检查循环依赖"""
        visited = set()
        rec_stack = set()

        def visit(node):
            if node in rec_stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            rec_stack.add(node)
            for dep in self._dependencies.get(node, []):
                if visit(dep):
                    return True
            rec_stack.remove(node)
            return False

        for service_name in self._dependencies:
            if visit(service_name):
                return True
        return False

    def create_and_register_all_services(self) -> bool:
        """创建并注册所有服务（按依赖顺序）"""
        try:
            logging.info("[ServiceContainer] creating and registering all services...")

            from ali2026v3_trading.event_bus import get_global_event_bus
            event_bus = get_global_event_bus()
            self.register('event_bus', event_bus)

            from ali2026v3_trading.config_service import get_cached_params
            config = get_cached_params()
            self.register('config', config)
            self.register('params', config)

            from ali2026v3_trading import get_instrument_data_manager
            storage = get_instrument_data_manager()
            self.register('storage', storage)

            from ali2026v3_trading.signal_service import SignalService
            signal_service = SignalService(event_bus=event_bus)
            self.register('signal', signal_service)

            from ali2026v3_trading.order_service import OrderService
            order_service = OrderService(event_bus=event_bus)
            self.register('order', order_service)

            success = self.initialize_all()

            if success:
                logging.info("[ServiceContainer] all services created and initialized")
            else:
                logging.error("[ServiceContainer] service initialization failed")

            return success

        except Exception as e:
            logging.error("[ServiceContainer] failed to create services: %s", e)
            return False
