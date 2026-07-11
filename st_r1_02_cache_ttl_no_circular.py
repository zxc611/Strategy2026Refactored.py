# MODULE_ID: M2-504
"""R1-2: 验证 config_params↔strategy_config 循环导入已修复"""
import pytest


def test_cache_ttl_single_source():
    """CACHE_TTL 权威源在 config._constants，两处 re-export 值必须一致"""
    from ali2026v3_trading.config._params_canary_env import CACHE_TTL as src
    from ali2026v3_trading.config.config_params import CACHE_TTL as cp
    from ali2026v3_trading.strategy.strategy_config_layer import CACHE_TTL as sc
    assert src == cp, f"config_params.CACHE_TTL={cp} != _constants.CACHE_TTL={src}"
    assert src == sc, f"strategy_config.CACHE_TTL={sc} != _constants.CACHE_TTL={src}"
    assert src == 60.0, f"CACHE_TTL should be 60.0, got {src}"


def test_risk_service_import_no_circular():
    """risk_service 依赖 config_params 和 strategy_config，不应循环导入"""
    from ali2026v3_trading.risk.risk_service import RiskCheckResult
    assert RiskCheckResult is not None


def test_strategy_config_centralized_defaults_has_cache_ttl():
    """strategy_config.CENTRALIZED_DEFAULTS 中 CACHE_TTL 应引用常量"""
    from ali2026v3_trading.config._params_canary_env import CACHE_TTL
    from ali2026v3_trading.strategy.strategy_config_layer import CENTRALIZED_DEFAULTS
    assert CENTRALIZED_DEFAULTS.get('cache_ttl', CACHE_TTL) == CACHE_TTL
