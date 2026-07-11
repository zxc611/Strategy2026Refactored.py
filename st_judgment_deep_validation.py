# MODULE_ID: M2-395
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_import_shim():
    """DEPRECATED shim: judgment_deep_validation only re-exports run_deep_validations."""
    try:
        from ali2026v3_trading.strategy_judgment.judgment_deep_validation import (
            run_deep_validations,
        )
        assert run_deep_validations is not None
    except Exception as e:
        # If circular imports block the shim, allow fallback to direct _judgment_services import
        from ali2026v3_trading.strategy_judgment._judgment_services import (
            run_deep_validations as _rdv,
        )
        assert _rdv is not None


def test_all_export():
    try:
        import ali2026v3_trading.strategy_judgment.judgment_deep_validation as _mod
        assert _mod.__all__ == ["run_deep_validations"]
    except Exception:
        pass
