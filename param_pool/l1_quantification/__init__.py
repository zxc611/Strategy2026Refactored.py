# MODULE_ID: M1-167
"""param_pool/l1_quantification/ — moved to tests/precompute_fixtures/"""
from __future__ import annotations

__all__ = [
    "MetaAuditEngine",
    "audit_backtest_engine_integrity",
    "ExternalSourceConfig",
    "TierConfig",
    "PerformanceTierManager",
    "get_tier_config",
    "STATES",
]


def __getattr__(name: str):
    if name == "MetaAuditEngine":
        from ali2026v3_trading.precompute.meta_audit_engine import MetaAuditEngine
        return MetaAuditEngine
    if name == "audit_backtest_engine_integrity":
        from ali2026v3_trading.precompute.meta_audit_engine import MetaAuditEngine
        return MetaAuditEngine.audit_backtest_engine_integrity
    if name == "ExternalSourceConfig":
        from ali2026v3_trading.precompute._data_validation import ExternalSourceConfig
        return ExternalSourceConfig
    if name in ("TierConfig", "PerformanceTierManager"):
        from ali2026v3_trading.precompute._quantification_core import TierConfig, PerformanceTierManager
        return locals()[name]
    if name == "get_tier_config":
        from ali2026v3_trading.precompute._quantification_core import PerformanceTierManager
        return PerformanceTierManager.get_tier_config
    if name == "STATES":
        from ali2026v3_trading.precompute._quantification_core import TripleTruthAnchor
        return TripleTruthAnchor.STATES
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
