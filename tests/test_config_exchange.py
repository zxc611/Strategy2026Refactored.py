# MODULE_ID: M2-313
"""Tests for config.config_exchange module."""
import sys
from datetime import date
import pytest
from unittest.mock import MagicMock, patch


def _ensure_imports():
    for _mod_name in [
        'ali2026v3_trading.config.config_exchange',
        'ali2026v3_trading.infra.subscription_service',
    ]:
        if _mod_name in sys.modules:
            del sys.modules[_mod_name]
    from ali2026v3_trading.config.config_exchange import (
        ExchangeConfig,
        build_exchange_mapping,
        resolve_product_exchange,
        make_platform_future_id,
        _get_option_underlying_product,
        ensure_products_with_retry,
        month_mapping,
        get_runtime_scoring_months,
        delivery_month_rules,
        tick_size_by_product,
        exchange_trading_sessions,
        rollover_days,
    )
    return (
        ExchangeConfig,
        build_exchange_mapping,
        resolve_product_exchange,
        make_platform_future_id,
        _get_option_underlying_product,
        ensure_products_with_retry,
        month_mapping,
        get_runtime_scoring_months,
        delivery_month_rules,
        tick_size_by_product,
        exchange_trading_sessions,
        rollover_days,
    )


class TestExchangeConfig:
    def test_defaults(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        assert cfg.depth_levels == 5
        assert "CFFEX" in cfg.exchanges
        assert cfg.enable_auto_generation is True

    def test_get_instruments_for_exchange(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        insts = cfg.get_instruments_for_exchange("CFFEX")
        assert isinstance(insts, list)
        assert cfg.get_instruments_for_exchange("UNKNOWN") == []

    def test_get_all_simulated_instruments(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        all_insts = cfg.get_all_simulated_instruments()
        assert isinstance(all_insts, list)
        assert len(all_insts) > 0

    def test_future_switches(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        cfg.set_future_switch("IF", False)
        assert "IF" in cfg.get_disabled_futures()
        assert "IF" not in cfg.get_enabled_futures()
        assert cfg.is_future_enabled("IF") is False
        cfg.set_future_switch("IF", True)
        assert cfg.is_future_enabled("IF") is True

    def test_is_future_enabled_global_disable(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        cfg.enable_futures = False
        assert cfg.is_future_enabled("IF") is False

    def test_should_generate_option(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        assert cfg.should_generate_option("IO", "IF") is True
        cfg.enable_options = False
        assert cfg.should_generate_option("IO", "IF") is False
        cfg.enable_options = True
        cfg.set_future_switch("IF", False)
        assert cfg.should_generate_option("IO", "IF") is False

    def test_get_enabled_disabled_options(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        enabled = cfg.get_enabled_options()
        disabled = cfg.get_disabled_options()
        assert isinstance(enabled, list)
        assert isinstance(disabled, list)

    def test_get_generation_config(self):
        ExchangeConfig, *_ = _ensure_imports()
        cfg = ExchangeConfig()
        gc = cfg.get_generation_config()
        assert "enable_auto_generation" in gc
        assert "futures_count" in gc
        assert "options_count" in gc


class TestBuildExchangeMapping:
    def test_default(self):
        _, build_exchange_mapping, *_ = _ensure_imports()
        mapping = build_exchange_mapping()
        assert mapping["IF"] == "CFFEX"
        assert mapping["CU"] == "SHFE"

    def test_custom_merge(self):
        _, build_exchange_mapping, *_ = _ensure_imports()
        mapping = build_exchange_mapping({"XX": "TEST", "IF": "OVERRIDE"})
        assert mapping["XX"] == "TEST"
        assert mapping["IF"] == "OVERRIDE"

    def test_skip_empty(self):
        _, build_exchange_mapping, *_ = _ensure_imports()
        mapping = build_exchange_mapping({"": "A", "ZZ_EMPTY": ""})
        assert "" not in mapping
        assert "ZZ_EMPTY" not in mapping


class TestResolveProductExchange:
    def test_future_product(self):
        _, _, resolve_product_exchange, *_ = _ensure_imports()
        with patch("ali2026v3_trading.config.config_exchange.SubscriptionManager") as MockSub:
            MockSub.is_option.return_value = False
            MockSub.parse_future.return_value = {"product": "RB"}
            assert resolve_product_exchange("RB2410") == "SHFE"

    def test_option_product(self):
        _, _, resolve_product_exchange, *_ = _ensure_imports()
        with patch("ali2026v3_trading.config.config_exchange.SubscriptionManager") as MockSub:
            MockSub.is_option.return_value = True
            MockSub.parse_option.return_value = {"product": "IO"}
            assert resolve_product_exchange("IO2406C4000") == "CFFEX"

    def test_fallback_default(self):
        _, _, resolve_product_exchange, *_ = _ensure_imports()
        with patch("ali2026v3_trading.config.config_exchange.SubscriptionManager") as MockSub:
            MockSub.is_option.side_effect = ValueError("err")
            assert resolve_product_exchange("UNKNOWN") == "CFFEX"

    def test_custom_default(self):
        _, _, resolve_product_exchange, *_ = _ensure_imports()
        with patch("ali2026v3_trading.config.config_exchange.SubscriptionManager") as MockSub:
            MockSub.is_option.return_value = False
            MockSub.parse_future.return_value = {"product": "ZZ"}
            assert resolve_product_exchange("ZZ", default_exchange="DCE") == "DCE"


class TestMakePlatformFutureId:
    def test_make(self):
        _, _, _, make_platform_future_id, *_ = _ensure_imports()
        assert make_platform_future_id("IF", "2606") == "IF2606"

    def test_empty(self):
        _, _, _, make_platform_future_id, *_ = _ensure_imports()
        assert make_platform_future_id("", "2606") == ""
        assert make_platform_future_id("IF", "") == ""


class TestHelpers:
    def test_get_option_underlying_product_fallback(self):
        _, _, _, _, _get_option_underlying_product, *_ = _ensure_imports()
        with patch("ali2026v3_trading.lifecycle.product_initializer._get_option_underlying_product") as mock_impl:
            mock_impl.side_effect = ImportError("mock")
            assert _get_option_underlying_product("IO2406C4000") == "IO"
        assert isinstance(_get_option_underlying_product("IO2406C4000"), str)

    def test_ensure_products_with_retry_fallback(self):
        _, _, _, _, _, ensure_products_with_retry, *_ = _ensure_imports()
        with patch("ali2026v3_trading.lifecycle.product_initializer.ensure_products_with_retry") as mock_impl:
            mock_impl.side_effect = ImportError("mock")
            assert ensure_products_with_retry(None) == {}


class TestConstants:
    def test_month_mapping(self):
        _, _, _, _, _, _, month_mapping, *_ = _ensure_imports()
        assert "IF" in month_mapping
        assert isinstance(month_mapping["IF"], list)

    def test_runtime_scoring_months_uses_july_as_current_and_august_as_next(self):
        _, _, _, _, _, _, _, get_runtime_scoring_months, *_ = _ensure_imports()

        months = get_runtime_scoring_months(date(2026, 6, 26), count=5)

        assert months[:5] == ['2607', '2608', '2609', '2612', '2703']

    def test_delivery_rules(self):
        _, _, _, _, _, _, _, delivery_month_rules, *_ = _ensure_imports()
        assert "CFFEX" in delivery_month_rules

    def test_tick_size(self):
        _, _, _, _, _, _, _, _, tick_size_by_product, *_ = _ensure_imports()
        assert "IF" in tick_size_by_product

    def test_sessions(self):
        _, _, _, _, _, _, _, _, _, exchange_trading_sessions, *_ = _ensure_imports()
        assert "CFFEX" in exchange_trading_sessions
        assert "day_start" in exchange_trading_sessions["CFFEX"]

    def test_rollover(self):
        _, _, _, _, _, _, _, _, _, _, rollover_days = _ensure_imports()
        assert isinstance(rollover_days, dict)
        assert "CFFEX" in rollover_days
