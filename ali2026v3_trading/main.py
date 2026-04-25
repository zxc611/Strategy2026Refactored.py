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
    't_type_bootstrap': 't_type_bootstrap',
    'Strategy2026': 'ali2026v3_trading.strategy_core_service',
    'ServiceContainer': 'ali2026v3_trading.config_service',
    'InstrumentDataManager': 'ali2026v3_trading.storage',
    'KLineData': 'ali2026v3_trading.storage',
    'TickData': 'ali2026v3_trading.storage',
    'OrderService': 'ali2026v3_trading.order_service',
    'RiskService': 'ali2026v3_trading.risk_service',
    'ParamsService': 'ali2026v3_trading.params_service',
    'StrategyCore': 'ali2026v3_trading.strategy_core_service',
    'StrategyState': 'ali2026v3_trading.strategy_core_service',
    'ConfigService': 'ali2026v3_trading.config_service',
}

__all__ = list(_LAZY_IMPORTS.keys())


def __getattr__(name):
    """Lazy import to avoid circular imports.
    
    This delays importing from t_type_bootstrap until the symbol is actually
    accessed, breaking the circular dependency chain.
    """
    if name in _LAZY_IMPORTS:
        import importlib
        module = importlib.import_module(_LAZY_IMPORTS[name])
        return getattr(module, name)
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
