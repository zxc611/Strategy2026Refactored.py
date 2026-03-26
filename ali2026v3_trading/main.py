"""
InfiniTrader strategy compatibility entry.

This module is kept as a thin wrapper because some platform configurations
still load `main.py` directly.

Note: This is now a PURE EXPORT LAYER - it only re-exports symbols from
t_type_bootstrap for backward compatibility. All actual implementations
are in t_type_bootstrap.py and ali2026v3_trading service modules.

Important: Uses lazy imports via __getattr__ to avoid circular import issues.
"""

from __future__ import annotations

# Lazy imports to break circular dependencies:
# main.py → t_type_bootstrap.py → ali2026v3_trading.* → (back to main.py)
# 
# By using __getattr__, we delay the import until the symbol is actually accessed,
# which breaks the circular dependency chain.

def __getattr__(name):
    """Lazy import to avoid circular imports.
    
    This delays importing from t_type_bootstrap until the symbol is actually
    accessed, breaking the circular dependency chain.
    """
    # Map of exported names to their import paths
    _lazy_imports = {
        't_type_bootstrap': 't_type_bootstrap',
        'Strategy2026': 'ali2026v3_trading.strategy_core_service',
        'ServiceContainer': 'ali2026v3_trading.config_service',
        'MarketDataService': 'ali2026v3_trading.market_data_service',
        'KLineData': 'ali2026v3_trading.market_data_service',
        'TickData': 'ali2026v3_trading.market_data_service',
        'OrderService': 'ali2026v3_trading.order_service',
        'RiskService': 'ali2026v3_trading.risk_service',
        'ParamsService': 'ali2026v3_trading.params_service',
        'StrategyCore': 'ali2026v3_trading.strategy_core_service',
        'StrategyState': 'ali2026v3_trading.strategy_core_service',
        'ConfigService': 'ali2026v3_trading.config_service',
    }
    
    if name in _lazy_imports:
        import importlib
        module = importlib.import_module(_lazy_imports[name])
        return getattr(module, name)
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# For static analysis tools and IDE autocomplete
if False:  # pragma: no cover
    from ali2026v3_trading.strategy_core_service import (
        Strategy2026,
        StrategyCoreService as StrategyCore,
        StrategyState,
    )
    from ali2026v3_trading.config_service import (
        ServiceContainer,
        ConfigService,
    )
    from ali2026v3_trading.market_data_service import (
        MarketDataService,
        KLineData,
        TickData,
    )
    from ali2026v3_trading.order_service import OrderService
    from ali2026v3_trading.risk_service import RiskService
    from ali2026v3_trading.params_service import ParamsService



__all__ = [
    "t_type_bootstrap",
    "Strategy2026",
    "ServiceContainer",
    "MarketDataService",
    "KLineData",
    "TickData",
    "OrderService",
    "RiskService",
    "ParamsService",
    "StrategyCore",
    "StrategyState",
    "ConfigService",
]
