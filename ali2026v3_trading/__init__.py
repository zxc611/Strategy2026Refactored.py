"""
quant_trading_system_T型图 - 量化交易系统 T 型图架构包

这个包包含了基于 T 型图架构的量化交易系统的核心模块:
- constants: 常量定义
- models: 数据模型
- utils: 工具函数
- cache: 缓存管理
- scheduler: 调度器
- params: 参数管理
- data_manager: 数据管理
- position: 持仓管理
- executor: 执行器
- gate: 网关层
- pause: 暂停服务
- trading_logic: 交易逻辑
- strategy_core: 策略核心
- diagnosis: 诊断服务
- ui: UI 服务
"""

from importlib import import_module

__version__ = '1.0.0'
__author__ = 'Quant Trading System'

_EXPORTS = {
    'InstrumentDataManager': ('ali2026v3_trading.storage', 'InstrumentDataManager'),
    'OrderService': ('ali2026v3_trading.order_service', 'OrderService'),
    'RiskService': ('ali2026v3_trading.risk_service', 'RiskService'),
    'ParamsService': ('ali2026v3_trading.params_service', 'ParamsService'),
    'QueryService': ('ali2026v3_trading.query_service', 'QueryService'),
    'ConfigService': ('ali2026v3_trading.config_service', 'ConfigService'),
    'DataService': ('ali2026v3_trading.data_service', 'DataService'),
    'is_market_open': ('ali2026v3_trading.scheduler_service', 'is_market_open'),
    'get_market_time_service': ('ali2026v3_trading.scheduler_service', 'get_market_time_service'),
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
