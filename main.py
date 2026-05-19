"""
InfiniTrader strategy compatibility entry.

This module is kept as a thin wrapper because some platform configurations
still load `main.py` directly.

Note: This is now a PURE EXPORT LAYER - it only re-exports symbols from
t_type_bootstrap for backward compatibility. All actual implementations
are in t_type_bootstrap.py and ali2026v3_trading service modules.

Important: Uses lazy imports via __getattr__ to avoid circular import issues.
The export list is defined in __all__ and _LAZY_IMPORTS must be consistent.
"""

from __future__ import annotations

# Lazy imports to break circular dependencies:
# main.py → t_type_bootstrap.py → ali2026v3_trading.* → (back to main.py)
# 
# By using __getattr__, we delay the import until the symbol is actually accessed,
# which breaks the circular dependency chain.

# Single source of truth for exports - used by both __getattr__ and __all__
_LAZY_IMPORTS = {
    'Strategy2026': ('ali2026v3_trading.strategy_core_service', 'Strategy2026'),
    'ServiceContainer': ('ali2026v3_trading.config_service', 'ServiceContainer'),
    'InstrumentDataManager': ('ali2026v3_trading', 'InstrumentDataManager'),
    'OrderService': ('ali2026v3_trading.order_service', 'OrderService'),
    'RiskService': ('ali2026v3_trading.risk_service', 'RiskService'),
    'ParamsService': ('ali2026v3_trading.params_service', 'ParamsService'),
    'StrategyCore': ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService'),
    'StrategyState': ('ali2026v3_trading.strategy_lifecycle_mixin', 'StrategyState'),
    'ConfigService': ('ali2026v3_trading.config_service', 'ConfigService'),
}

_IMPORT_CACHE: dict = {}

__all__ = list(_LAZY_IMPORTS.keys())


def __getattr__(name):
    """Lazy import to avoid circular imports.
    
    This delays importing from t_type_bootstrap until the symbol is actually
    accessed, breaking the circular dependency chain.
    
    Performance: Uses cache to avoid repeated imports.
    """
    if name in _IMPORT_CACHE:
        return _IMPORT_CACHE[name]
    
    if name in _LAZY_IMPORTS:
        import importlib
        entry = _LAZY_IMPORTS[name]
        if isinstance(entry, tuple):
            module_name, attr_name = entry
        else:
            module_name, attr_name = entry, name
        module = importlib.import_module(module_name)
        attr = getattr(module, attr_name)
        _IMPORT_CACHE[name] = attr
        return attr
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
