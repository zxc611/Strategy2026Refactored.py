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

# NS-P1-02修复: 添加线程锁保护globals()动态注入
"""
from __future__ import annotations

import threading
import sys
import os as _os
_globals_lock = threading.Lock()

# R26-P0-CD-09修复: 中文路径兼容——param_pool与参数池双向映射
# Linux环境下中文路径可能失败, 此处将param_pool注册为参数池的别名
try:
    _pkg_dir = _os.path.dirname(_os.path.abspath(__file__))
    _param_pool_dir = _os.path.join(_pkg_dir, 'param_pool')
    _cn_pool_dir = _os.path.join(_pkg_dir, '参数池')
    if _os.path.isdir(_param_pool_dir) and 'ali2026v3_trading.param_pool' not in sys.modules:
        import importlib as _il
        _pm = _il.import_module('ali2026v3_trading.param_pool')
        sys.modules['ali2026v3_trading.param_pool'] = _pm
    if _os.path.isdir(_cn_pool_dir) and 'ali2026v3_trading.参数池' not in sys.modules:
        import importlib as _il
        _cn = _il.import_module('ali2026v3_trading.参数池')
        sys.modules['ali2026v3_trading.参数池'] = _cn
except Exception:
    pass
# storage: 持久化存储 (storage_core + storage_query)
# init_phase: 初始化阶段管理 (在 shared_utils 中)
# scheduler: 调度器
# query: 数据查询
# diagnosis: 诊断服务
# performance_monitor: 性能监控
# ui: UI 服务 (仅 Test/Dev 模式)
from importlib import import_module  # R21-CC-P2-03修复: 动态导入 — 用于延迟加载避免循环依赖，模块路径均为硬编码，安全风险可控
import threading
import logging
from typing import Optional

from ali2026v3_trading.storage_core import _StorageCoreMixin, _get_default_db_path
from ali2026v3_trading.storage_query import _StorageQueryMixin

__version__ = '1.2.0'
__author__ = 'Quant Trading System'

# UPG-11修复: 代码版本常量, 与手册V7.0对齐
CODE_VERSION = '7.0'

# P2-项14修复: 各模块版本常量(覆盖率提升至>5%)
MODULE_VERSIONS = {
    'risk_service': '1.2.0',
    'signal_service': '1.2.0',
    'event_bus': '1.2.0',
    'config_service': '1.2.0',
    'strategy_ecosystem': '1.2.0',
    'ds_schema_manager': '2.0',
    'maintenance_service': '1.2.0',
    'health_check_api': '1.2.0',
    'order_service': '1.2.0',
    'params_service': '1.2.0',
    'data_service': '1.2.0',
    'storage_core': '1.2.0',
    'storage_query': '1.2.0',
    'scheduler_service': '1.2.0',
    'shared_utils': '1.2.0',
    'box_spring_strategy': '1.2.0',
    'strategy_core_service': '1.2.0',
    'strategy_lifecycle_mixin': '1.2.0',
    't_type_service': '1.2.0',
    'width_cache': '1.2.0',
    'query_service': '1.2.0',
}

# P2-项21修复: 代码分支管理策略信息
BRANCH_INFO = {
    'main_branch': 'main',
    'release_prefix': 'release/',
    'hotfix_prefix': 'hotfix/',
    'feature_prefix': 'feature/',
    'current_branch': 'main',
    'branch_strategy': 'GitFlow简化版: main→release/*→hotfix/*, feature/*开发分支',
    'merge_strategy': 'Squash merge to main, Fast-forward to release',
}

# UPG-11修复: 版本对齐检查 - 确保代码版本与关键模块版本一致
def _check_version_alignment() -> None:
    """UPG-11修复: 检查代码版本与关键模块版本对齐

    在包导入时自动执行, 版本不对齐时输出WARNING日志.
    """
    try:
        from ali2026v3_trading.ds_schema_manager import SchemaManagerMixin
        schema_version = SchemaManagerMixin.SCHEMA_VERSION
        # 代码版本7.0应对应schema版本2.0
        expected_schema = {'7.0': '2.0', '1.2.0': '2.0'}
        if CODE_VERSION in expected_schema:
            if schema_version != expected_schema[CODE_VERSION]:
                logging.warning(
                    "UPG-11: 版本不对齐 CODE_VERSION=%s, SCHEMA_VERSION=%s (期望%s)",
                    CODE_VERSION, schema_version, expected_schema[CODE_VERSION],
                )
    except Exception as e:
        logging.debug("UPG-11: 版本对齐检查跳过: %s", e)


def _check_runtime_dependencies():
    """EC-P2-13: 检查运行时DLL依赖(duckdb需C++运行时, numpy需BLAS等)"""
    # TODO(R17-P2-DOC-02): 实现具体检查逻辑
    pass

_EXPORTS = {
    # UPG-11: 添加CODE_VERSION到导出
    'CODE_VERSION': (None, None),  # 特殊处理: 直接从本模块获取
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

__all__ = list(_EXPORTS.keys()) + ['InstrumentDataManager', 'get_instrument_data_manager']


def __getattr__(name):
    # UPG-11: CODE_VERSION直接从本模块获取
    if name == 'CODE_VERSION':
        return CODE_VERSION
    export = _EXPORTS.get(name)
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = export
    module = import_module(module_name)
    value = getattr(module, attr_name)
    # NS-P1-02修复: 加锁保护globals()动态注入
    with _globals_lock:
        globals()[name] = value
    return value


def __dir__():
    return sorted(set(_EXPORTS.keys()) | {'__version__', '__author__', '__doc__', 'CODE_VERSION', 'InstrumentDataManager', 'get_instrument_data_manager'})


# UPG-11修复: 包导入时自动执行版本对齐检查
_check_version_alignment()


# ============================================================================
# InstrumentDataManager - Mixin组合类 (包入口直接装配)
# ============================================================================

class InstrumentDataManager(_StorageCoreMixin, _StorageQueryMixin):
    """InstrumentDataManager - Mixin composition class

    MRO: InstrumentDataManager -> _StorageCoreMixin -> _StorageQueryMixin -> object
    All public APIs are automatically inherited via Mixin.
    """
    pass


_instrument_data_manager_instance: Optional[InstrumentDataManager] = None
_instrument_data_manager_lock = threading.Lock()


def get_instrument_data_manager(
    tick_retention_days: int = 1825,
    kline_retention_days: int = 1825,
    async_queue_size: int = 500000,
    batch_size: int = 5000,
) -> InstrumentDataManager:
    global _instrument_data_manager_instance

    with _instrument_data_manager_lock:
        if _instrument_data_manager_instance is None:
            db_path = _get_default_db_path()

            _instrument_data_manager_instance = InstrumentDataManager(
                db_path=db_path,
                max_retries=3,
                retry_delay=0.1,
                async_queue_size=async_queue_size,
                batch_size=batch_size,
                drop_on_full=True,
                cleanup_interval=3600,
                cleanup_config={'tick': tick_retention_days, 'kline': kline_retention_days}
            )

            logging.info(f"[Storage] Global InstrumentDataManager singleton initialized (db={db_path})")

        return _instrument_data_manager_instance
