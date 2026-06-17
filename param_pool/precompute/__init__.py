"""V5 precompute subpackage — moved to tests/precompute_fixtures/."""
from __future__ import annotations

__all__: list[str] = [
    "PrecomputeEngine",
    "sync_db_schema_v5",
    "SCHEMA_REQUIRED_COLUMNS_V5",
]


def __getattr__(name: str):
    _MOVED = {
        "PrecomputeEngine": "._engine",
        "sync_db_schema_v5": "._schema",
        "SCHEMA_REQUIRED_COLUMNS_V5": "._schema",
        "_cache_ttl": "._cache_ttl",
        "_calendar": "._calendar",
        "_cycle_resonance_vec": "._cycle_resonance_vec",
        "_daily_pivot": "._daily_pivot",
        "_engine": "._engine",
        "_hmm": "._hmm",
        "_kl_rpd": "._kl_rpd",
        "_l0_state": "._l0_state",
        "_multiscale": "._multiscale",
        "_obos": "._obos",
        "_params": "._params",
        "_position_decision": "._position_decision",
        "_pullback": "._pullback",
        "_registry": "._registry",
        "_schema": "._schema",
        "_signals": "._signals",
        "_signal_decay": "._signal_decay",
        "_trend_scores": "._trend_scores",
    }
    if name in _MOVED:
        from ali2026v3_trading.tests import precompute_fixtures as _dst
        return getattr(_dst, _MOVED[name].lstrip('.'))
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
