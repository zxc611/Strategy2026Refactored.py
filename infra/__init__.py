"""infra/子系统(P2-S1)"""


def __getattr__(name: str):
    _DEPRECATED_MODULES = {
        "_backup_restore": ".storage_service",
        "_deprecated_reexports": ".trading_utils",
        "_disk_monitor": ".storage_service",
        "_helpers": ".storage_service",
        "_ops_framework": ".ops_service",
        "callback_registry": ".registry_service",
        "commission_utils": ".trading_utils",
        "cross_system": ".trading_utils",
        "event_publisher": ".event_bus",
        "market_time_service": ".scheduler_service",
        "risk_audit_utils": ".security_service",
        "risk_rules": ".security_service",
        "security": ".security_service",
        "service_contracts": ".contracts_service",
        "singleton_registry": ".registry_service",
        "subscription_manager": ".subscription_service",
    }
    if name in _DEPRECATED_MODULES:
        import importlib
        return importlib.import_module(_DEPRECATED_MODULES[name], __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
