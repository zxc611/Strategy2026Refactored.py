# [M1-56] 服务容器
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

    R5-T-01修复: 循环依赖处理策略
      strategy_ecosystem → risk_service → signal_service → strategy_ecosystem
      解决: 使用延迟绑定(lazy binding)，L2层服务初始化时不立即绑定跨层依赖，
      而是在initialize_all()完成后通过bind_cross_layer_dependencies()统一绑定。

    R5-T-10修复: 初始化顺序文档
      1. event_bus (L0) - 无依赖
      2. config (L0) - 无依赖
      3. params (L0) - 无依赖
      4. storage (L1) - 依赖event_bus
      5. market_data (L1) - 依赖storage, event_bus
      6. analytics (L2) - 依赖market_data
      7. risk (L2) - 依赖market_data
      8. order (L2) - 依赖risk
      9. signal (L2) - 依赖analytics
      10. t_type (L2) - 依赖signal
      11. diagnosis (L2) - 依赖t_type
      12. ui (L2) - 依赖diagnosis
      13. strategy_core (L3) - 依赖storage, event_bus, config, params
    """

    UNKNOWN_LAYER = 99

    _LAYERS = {
        'event_bus': 0, 'config': 0, 'params': 0,
        'storage': 1, 'market_data': 1,
        'analytics': 2, 'risk': 2, 'order': 2, 'signal': 2, 't_type': 2, 'diagnosis': 2, 'ui': 2, 'operations_api': 2,
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
            'operations_api': ['risk', 'order'],
            'strategy_core': ['storage', 'event_bus', 'config', 'params']
        }
        self._initialization_order = [
            'event_bus', 'config', 'params', 'storage',
            'market_data', 'analytics', 'risk', 'order',
            'signal', 't_type', 'diagnosis', 'ui', 'operations_api', 'strategy_core'
        ]

    def register(self, name: str, service: Any) -> None:
        """注册服务（带层级校验）"""
        self._services[name] = service
        svc_layer = self._LAYERS.get(name, self.UNKNOWN_LAYER)
        for dep in self._dependencies.get(name, []):
            dep_layer = self._LAYERS.get(dep, self.UNKNOWN_LAYER)
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
        # R15-P1-RES-02修复: 拓扑排序启动(基于依赖图)
        self._dependency_graph = dict(self._dependencies)
        self._initialization_order = self._topological_sort()
        logging.info("[ServiceContainer] order: %s", self._initialization_order)

        if self._check_circular_dependencies():
            logging.error("[ServiceContainer] circular dependency detected!")
            return False

        # R15-P1-RES-11修复: 跟踪已启动服务，失败时回滚
        _started_services = []
        for service_name in self._initialization_order:
            if service_name in self._services:
                deps = self._dependencies.get(service_name, [])
                missing_deps = [d for d in deps if d not in self._services]
                if missing_deps:
                    logging.error(
                        "[ServiceContainer] %s depends on %s, not registered",
                        service_name, missing_deps
                    )
                    self._rollback_started_services(_started_services)
                    return False
                logging.info("[ServiceContainer] initialized: %s", service_name)
                _started_services.append(service_name)
        logging.info("[ServiceContainer] all services initialized")
        self.bind_cross_layer_dependencies()
        return True

    # R15-P1-RES-02修复: 拓扑排序
    def _topological_sort(self) -> List[str]:
        visited = set()
        order = []
        def visit(node):
            if node in visited:
                return
            visited.add(node)
            for dep in self._dependency_graph.get(node, []):
                visit(dep)
            order.append(node)
        for svc in self._dependency_graph:
            visit(svc)
        return order

    # R15-P1-RES-11修复: 启动失败时逆序停止已启动服务
    def _rollback_started_services(self, started: List[str]) -> None:
        for svc_name in reversed(started):
            svc = self._services.get(svc_name)
            if svc and hasattr(svc, 'stop'):
                try:
                    svc.stop()
                    logging.info("[ServiceContainer] rollback stopped: %s", svc_name)
                except Exception as e:
                    logging.warning("[ServiceContainer] rollback stop %s failed: %s", svc_name, e)
                    raise

    def bind_cross_layer_dependencies(self) -> None:
        """R5-T-01修复: 绑定跨层依赖（延迟绑定策略）

        L2层服务初始化时不立即绑定跨层依赖，
        在initialize_all()完成后统一绑定，解决循环依赖问题。
        """
        logging.info("[ServiceContainer] binding cross-layer dependencies...")

        risk = self._services.get('risk')
        signal = self._services.get('signal')
        strategy_core = self._services.get('strategy_core')

        if risk and hasattr(risk, '_signal_service') and signal:
            risk._signal_service = signal
            logging.info("[ServiceContainer] bound signal→risk")

        if signal and hasattr(signal, '_risk_service') and risk:
            signal._risk_service = risk
            logging.info("[ServiceContainer] bound risk→signal")

        if strategy_core:
            if hasattr(strategy_core, '_risk_service') and risk:
                strategy_core._risk_service = risk
                logging.info("[ServiceContainer] bound risk→strategy_core")
            if hasattr(strategy_core, '_signal_service') and signal:
                strategy_core._signal_service = signal
                logging.info("[ServiceContainer] bound signal→strategy_core")

        logging.info("[ServiceContainer] cross-layer dependencies bound")

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

            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            event_bus = get_global_event_bus()
            self.register('event_bus', event_bus)

            from ali2026v3_trading.config.config_service import get_config
            from ali2026v3_trading.config.config_params import get_cached_params
            config = get_config()
            params = get_cached_params()
            self.register('config', config)
            self.register('params', params)

            from ali2026v3_trading import get_instrument_data_manager
            storage = get_instrument_data_manager()
            self.register('storage', storage)

            # CORE-DEPENDENCY: QueryService是核心服务
            try:
                from ali2026v3_trading.query_service import QueryService
                market_data = QueryService(storage)
                self.register('market_data', market_data)
            except ImportError as _e:
                logging.error("[ServiceContainer] 核心服务QueryService加载失败: %s", _e)

            # CORE-DEPENDENCY: GreeksCalculator是核心服务
            try:
                from ali2026v3_trading.governance.greeks_calculator import GreeksCalculator
                analytics = GreeksCalculator()
                self.register('analytics', analytics)
            except ImportError as _e:
                logging.error("[ServiceContainer] 核心服务GreeksCalculator加载失败: %s", _e)

            # CORE-DEPENDENCY: RiskService是核心服务
            try:
                from ali2026v3_trading.risk.risk_service import RiskService, get_risk_service
                # R14-P1-API-06修复: 使用get_risk_service工厂函数而非直接RiskService()
                risk = get_risk_service(params={}, position_manager=None, strategy=None)
                self.register('risk', risk)
            except ImportError as _e:
                logging.error("[ServiceContainer] 核心服务RiskService加载失败: %s", _e)

            from ali2026v3_trading.order.order_service import OrderService
            order_service = OrderService(event_bus=event_bus)
            self.register('order', order_service)

            from ali2026v3_trading.signal.signal_service import SignalService
            signal_service = SignalService(event_bus=event_bus)
            self.register('signal', signal_service)

            # CORE-DEPENDENCY: TTypeService是核心服务
            try:
                from ali2026v3_trading.t_type_service import TTypeService
                t_type = TTypeService()
                self.register('t_type', t_type)
            except ImportError as _e:
                logging.error("[ServiceContainer] 核心服务TTypeService加载失败: %s", _e)

            # CORE-DEPENDENCY: DiagnosisService是核心服务
            try:
                from ali2026v3_trading.infra.diagnosis_service import DiagnosisService
                diagnosis = DiagnosisService()
                self.register('diagnosis', diagnosis)
            except ImportError as _e:
                logging.error("[ServiceContainer] 核心服务DiagnosisService加载失败: %s", _e)

            # OPTIONAL-DEPENDENCY: UIMixin是可选的UI服务
            try:
                from ali2026v3_trading.ui_service import UIMixin
                ui = UIMixin()
                self.register('ui', ui)
            except ImportError as _e:
                logging.info("[ServiceContainer] 可选服务UIMixin不可用: %s", _e)

            # CORE-DEPENDENCY: StrategyCoreService是核心服务
            try:
                from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
                strategy_core = StrategyCoreService()
                self.register('strategy_core', strategy_core)
            except ImportError as _e:
                logging.error("[ServiceContainer] 核心服务StrategyCoreService加载失败: %s", _e)

            # R21-OPS-18/20: 注册运维API（含容量压测+SOP）
            try:
                from ali2026v3_trading.infra.health_check_api import get_operations_api
                operations_api = get_operations_api()
                self.register('operations_api', operations_api)
            except Exception as _e:
                logging.info("[ServiceContainer] 可选服务OperationsAPI不可用: %s", _e)

            success = self.initialize_all()

            if success:
                logging.info("[ServiceContainer] all services created and initialized")
            else:
                logging.error("[ServiceContainer] service initialization failed")

            return success

        except Exception as e:
            logging.error("[ServiceContainer] failed to create services: %s", e, exc_info=True)
            raise
