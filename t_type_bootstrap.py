"""
t_type_bootstrap - Strategy Bootstrap Module (Compatibility Layer)

This module serves as a thin compatibility layer for legacy code that
imports from t_type_bootstrap. All actual implementations have been
migrated to ali2026v3_trading service modules.

Migration History:
- Strategy2026 -> ali2026v3_trading.strategy_core_service
- T-Type Service -> ali2026v3_trading.t_type_service
- Other features -> various ali2026v3_trading.* modules

Note: This file is only a backward-compatible export layer and should
not contain actual business logic.
"""

from __future__ import annotations

# ============================================================================
# 懒加载导出 - 打破循环依赖
# ============================================================================

_LAZY_EXPORTS = {
    'Strategy2026': ('ali2026v3_trading.strategy_core_service', 'Strategy2026'),
    'ServiceContainer': ('ali2026v3_trading.config_service', 'ServiceContainer'),
    'InstrumentDataManager': ('ali2026v3_trading', 'InstrumentDataManager'),
    'OrderService': ('ali2026v3_trading.order_service', 'OrderService'),
    'RiskService': ('ali2026v3_trading.risk_service', 'RiskService'),
    'ParamsService': ('ali2026v3_trading.params_service', 'ParamsService'),
    'StrategyCore': ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService'),
    'StrategyState': ('ali2026v3_trading.strategy_lifecycle_mixin', 'StrategyState'),
    'ConfigService': ('ali2026v3_trading.config_service', 'ConfigService'),
    'DataService': ('ali2026v3_trading.data_service', 'DataService'),
    'TTypeService': ('ali2026v3_trading.t_type_service', 'TTypeService'),
}

__all__ = list(_LAZY_EXPORTS.keys()) + ['register_strategy_cache', 'get_strategy_cache', 't_type_bootstrap']


def __getattr__(name):
    """Lazy import to avoid circular dependencies."""
    if name in _LAZY_EXPORTS:
        import importlib
        module_name, attr_name = _LAZY_EXPORTS[name]
        module = importlib.import_module(module_name)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ============================================================================
# t_type_bootstrap 类（兼容层 - 仅作为模块级别的命名空间标识）
# ============================================================================

class t_type_bootstrap:
    """t_type_bootstrap compatibility class
    
    This class serves as a proxy to Strategy2026 when instantiated by the platform.
    The platform loads this file and tries to instantiate a class named 't_type_bootstrap'.
    We proxy all instantiation to Strategy2026.
    """
    
    def __new__(cls, *args, **kwargs):
        """Proxy instantiation to Strategy2026"""
        import os
        import sys
        
        _demo_dir = os.path.dirname(os.path.abspath(__file__))
        if _demo_dir not in sys.path:
            sys.path.insert(0, _demo_dir)
        
        from ali2026v3_trading.strategy_core_service import Strategy2026
        return Strategy2026(*args, **kwargs)
    
    def __init__(self, *args, **kwargs):
        pass
    
    @staticmethod
    def register_strategy_cache(strategy_instance):
        """Register strategy instance to global cache"""
        return register_strategy_cache(strategy_instance)
    
    @staticmethod
    def get_strategy_cache():
        """Get the registered strategy instance"""
        return get_strategy_cache()


# ============================================================================
# 策略缓存注册函数
# ============================================================================

_strategy_cache_instance = None


def register_strategy_cache(strategy_instance):
    """Register strategy instance to global cache (for legacy code compatibility)
    
    Args:
        strategy_instance: Strategy2026 instance
        
    Note:
        This is a compatibility function. Actual implementation has been
        migrated to strategy_core_service.
    """
    global _strategy_cache_instance
    _strategy_cache_instance = strategy_instance
    # 这里可以添加额外的注册逻辑


def get_strategy_cache():
    """Get the registered strategy instance"""
    return _strategy_cache_instance


# ============================================================================
# 版本信息
# ============================================================================

__version__ = '1.2.0'
__author__ = 'Quant Trading System'
