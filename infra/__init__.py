"""infra/子系统(P2-S1)"""


def __getattr__(name: str):
    _DEPRECATED_MODULES = {
        "_backup_restore": ".storage_service",
        "_deprecated_reexports": ".trading_utils",
        "_disk_monitor": "._helpers",
        "_ops_automation": "..tests.infra_archived._ops_automation",
        "_ops_framework": "..tests.infra_archived._ops_framework",
        "_ops_knowledge_metrics": "._helpers",
        "_storage": "..tests.infra_archived._storage",
        "callback_registry": ".registry_service",
        "commission_utils": ".trading_utils",
        "contracts_service": ".service_contracts",
        "cross_system": ".trading_utils",
        "event_publisher": ".concurrent_utils",
        "exception_layers": ".exceptions",
        "logging_utils": "._helpers",
        "market_time_service": ".scheduler_service",
        "module_load_status": ".metrics_registry",
        "ops_service": "..tests.infra_archived.ops_service",
        "performance_monitor": ".metrics_registry",
        "phase_feature_flag": ".metrics_registry",
        "risk_audit_utils": ".security_service",
        "risk_rules": ".security_service",
        "security": ".security_service",
        "service_contracts": ".service_contracts",
        "shared_providers": ".service_contracts",
        "shared_trading_constants": ".commission_utils",
        "singleton_registry": ".registry_service",
        "state_machine": ".service_contracts",
        "state_store": ".registry_service",
        "subscription_manager": ".subscription_service",
    }
    if name in _DEPRECATED_MODULES:
        import importlib
        return importlib.import_module(_DEPRECATED_MODULES[name], __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
