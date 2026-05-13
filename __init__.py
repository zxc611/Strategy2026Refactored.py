"""
quant_trading_system_T型图 - 量化交易系统 T 型图架构包

核心模块:
- constants: 常量定义
- models: 数据模型
- shared_utils: 共享工具函数 (唯一实现源)
- strategy_core: 策略核心 (生命周期/状态机)
- t_type_service: T型图服务 (五态分类/排序)
- width_cache: 期权宽度缓存
- params: 参数管理
- config_service: 配置与参数表
- data_manager: 数据管理
- subscription: 订阅管理
- order: 下单执行
- position: 持仓管理
- risk: 风控检查
- signal: 信号生成
- event_bus: 事件总线
- greeks: 希腊字母计算
- storage: 持久化存储 (storage_core + storage_query)
- init_phase: 初始化阶段管理 (在 shared_utils 中)
- scheduler: 调度器
- query: 数据查询
- diagnosis: 诊断服务
- performance_monitor: 性能监控
- ui: UI 服务 (仅 Test/Dev 模式)
"""
from __future__ import annotations

from importlib import import_module
import threading
import logging

from ali2026v3_trading.storage_core import _StorageCoreMixin, _get_default_db_path
from ali2026v3_trading.storage_query import _StorageQueryMixin

__version__ = '1.2.0'
__author__ = 'Quant Trading System'

_EXPORTS = {
    # Core
    'StrategyCore': ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService'),
    'StrategyState': ('ali2026v3_trading.strategy_lifecycle_mixin', 'StrategyState'),
    # T-Type
    'TTypeService': ('ali2026v3_trading.t_type_service', 'TTypeService'),
    'get_t_type_service': ('ali2026v3_trading.t_type_service', 'get_t_type_service'),
    # Width Cache
    'WidthStrengthCache': ('ali2026v3_trading.width_cache', 'WidthStrengthCache'),
    'SortEntry': ('ali2026v3_trading.width_cache', 'SortEntry'),
    # Services
    'OrderService': ('ali2026v3_trading.order_service', 'OrderService'),
    'RiskService': ('ali2026v3_trading.risk_service', 'RiskService'),
    'ParamsService': ('ali2026v3_trading.params_service', 'ParamsService'),
    'QueryService': ('ali2026v3_trading.query_service', 'QueryService'),
    'ConfigService': ('ali2026v3_trading.config_service', 'ConfigService'),
    'DataService': ('ali2026v3_trading.data_service', 'DataService'),
    # Utils
    'is_market_open': ('ali2026v3_trading.scheduler_service', 'is_market_open'),
    'get_market_time_service': ('ali2026v3_trading.scheduler_service', 'get_market_time_service'),
    # Shard Router
    'ShardRouter': ('ali2026v3_trading.shared_utils', 'ShardRouter'),
    'get_shard_router': ('ali2026v3_trading.shared_utils', 'get_shard_router'),
    'extract_product_code': ('ali2026v3_trading.shared_utils', 'extract_product_code'),
    # Box Spring Strategy
    'BoxSpringStrategy': ('ali2026v3_trading.box_spring_strategy', 'BoxSpringStrategy'),
    'get_box_spring_strategy': ('ali2026v3_trading.box_spring_strategy', 'get_box_spring_strategy'),
    'BoxRange': ('ali2026v3_trading.box_spring_strategy', 'BoxRange'),
    'SpringState': ('ali2026v3_trading.box_spring_strategy', 'SpringState'),
}

__all__ = list(_EXPORTS.keys())


def __getattr__(name):
    export = _EXPORTS.get(name)
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = export
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(_EXPORTS.keys()) | {'__version__', '__author__', '__doc__'})


# ============================================================================
# InstrumentDataManager — Mixin组合类 (包入口直接装配)
# ============================================================================

class InstrumentDataManager(_StorageCoreMixin, _StorageQueryMixin):
    """期货/期权数据管理器 — Mixin组合类

    MRO: InstrumentDataManager -> _StorageCoreMixin -> _StorageQueryMixin -> object
    所有公共API通过Mixin自动继承，零功能损失。
    """
    pass


_instrument_data_manager_instance: InstrumentDataManager = None
_instrument_data_manager_lock = threading.Lock()


def get_instrument_data_manager() -> InstrumentDataManager:
    global _instrument_data_manager_instance

    with _instrument_data_manager_lock:
        if _instrument_data_manager_instance is None:
            db_path = _get_default_db_path()

            _instrument_data_manager_instance = InstrumentDataManager(
                db_path=db_path,
                max_retries=3,
                retry_delay=0.1,
                async_queue_size=500000,
                batch_size=5000,
                drop_on_full=True,
                cleanup_interval=3600,
                cleanup_config={'tick': 30, 'kline': 90}
            )

            logging.info(f"[Storage] Global InstrumentDataManager singleton initialized (db={db_path})")

        return _instrument_data_manager_instance
