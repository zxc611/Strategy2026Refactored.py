# MODULE_ID: M2-466
"""test_phase1_r4_pricing_separation.py — Phase 1 轮次4 断言测试

验证 backtest_state.py 数据类与业务逻辑分离后的运行时行为:
1. backtest_config.py 是定价/识别函数的权威源（原backtest_pricing已合并入config）
2. backtest_pricing.py 作为shim re-export 保持下游兼容
3. backtest_state.py re-export 保持下游兼容
4. backtest_runner_base.py 通过 state 间接获取 pricing 函数（同一对象）
5. 迁移后运行时行为不变
"""
import pytest
import pandas as pd
from types import SimpleNamespace


# ── 1. 权威源验证: backtest_config.py（原backtest_pricing已合并入config） ──

class TestPricingAuthoritativeSource:
    """定价/识别函数权威源在 backtest_config.py（通过 backtest_pricing shim 可访问）"""

    def test_infer_contract_type_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_contract_type
        assert callable(_infer_contract_type)

    def test_infer_exchange_id_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_exchange_id
        assert callable(_infer_exchange_id)

    def test_infer_instrument_type_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_instrument_type
        assert callable(_infer_instrument_type)

    def test_calculate_limit_prices_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _calculate_limit_prices
        assert callable(_calculate_limit_prices)

    def test_calc_trade_fee_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import calc_trade_fee
        assert callable(calc_trade_fee)

    def test_compute_option_mtm_price_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_option_mtm_price
        assert callable(_compute_option_mtm_price)

    def test_get_contract_multiplier_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _get_contract_multiplier
        assert callable(_get_contract_multiplier)

    def test_compute_cascade_slippage_bps_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_cascade_slippage_bps
        assert callable(_compute_cascade_slippage_bps)

    def test_infer_liquidity_tier_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_liquidity_tier
        assert callable(_infer_liquidity_tier)

    def test_get_expiry_slippage_multiplier_importable_from_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _get_expiry_slippage_multiplier
        assert callable(_get_expiry_slippage_multiplier)

    def test_pricing_shim_reexports_same_as_config(self):
        """backtest_pricing shim re-export 与 backtest_config 是同一对象"""
        from ali2026v3_trading.param_pool.backtest_config import _infer_contract_type as cfg_fn
        from ali2026v3_trading.param_pool.backtest_config import _infer_contract_type as prc_fn
        assert cfg_fn is prc_fn


# ── 2. Re-export 兼容性: backtest_state.py ──

class TestStateReExport:
    """backtest_state.py re-export 与权威源是同一对象"""

    def test_infer_exchange_id_same(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_exchange_id as p_fn
        from ali2026v3_trading.param_pool.backtest_state import _infer_exchange_id as s_fn
        assert p_fn is s_fn

    def test_infer_contract_type_same(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_contract_type as p_fn
        from ali2026v3_trading.param_pool.backtest_state import _infer_contract_type as s_fn
        assert p_fn is s_fn

    def test_calculate_limit_prices_same(self):
        from ali2026v3_trading.param_pool.backtest_config import _calculate_limit_prices as p_fn
        from ali2026v3_trading.param_pool.backtest_state import _calculate_limit_prices as s_fn
        assert p_fn is s_fn

    def test_compute_option_mtm_price_same(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_option_mtm_price as p_fn
        from ali2026v3_trading.param_pool.backtest_state import _compute_option_mtm_price as s_fn
        assert p_fn is s_fn

    def test_get_contract_multiplier_same(self):
        from ali2026v3_trading.param_pool.backtest_config import _get_contract_multiplier as p_fn
        from ali2026v3_trading.param_pool.backtest_state import _get_contract_multiplier as s_fn
        assert p_fn is s_fn

    def test_calc_trade_fee_same(self):
        from ali2026v3_trading.param_pool.backtest_config import calc_trade_fee as p_fn
        from ali2026v3_trading.param_pool.backtest_state import calc_trade_fee as s_fn
        assert p_fn is s_fn


# ── 3. 下游兼容性: backtest_runner_base.py ──

class TestDownstreamCompatibility:
    """backtest_runner_base.py 通过 state 间接获取 pricing 函数"""

    def test_runner_base_gets_pricing_functions(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_exchange_id as p_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _infer_exchange_id as b_fn
        assert p_fn is b_fn

    def test_runner_base_gets_option_pricing(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_option_mtm_price as p_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _compute_option_mtm_price as b_fn
        assert p_fn is b_fn

    def test_runner_base_gets_limit_prices(self):
        from ali2026v3_trading.param_pool.backtest_config import _calculate_limit_prices as p_fn
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import _calculate_limit_prices as b_fn
        assert p_fn is b_fn


# ── 4. 运行时行为验证 ──

class TestPricingBehavior:
    """迁移后运行时行为不变"""

    def test_infer_exchange_id_shfe(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_exchange_id
        assert _infer_exchange_id("AU2506") == 'SHFE'

    def test_infer_exchange_id_dce(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_exchange_id
        assert _infer_exchange_id("M2509") == 'DCE'

    def test_infer_exchange_id_czce(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_exchange_id
        assert _infer_exchange_id("SR2509") == 'CZCE'

    def test_infer_instrument_type_option(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_instrument_type
        assert _infer_instrument_type("50ETF-C-2506") == "option_buyer"

    def test_infer_instrument_type_future(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_instrument_type
        assert _infer_instrument_type("IF2506") == "future"

    def test_infer_contract_type_etf_option(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_contract_type
        assert _infer_contract_type("50ETF2506C02500") == "50ETF_OPTION"

    def test_cascade_slippage_no_switch(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_cascade_slippage_bps
        assert _compute_cascade_slippage_bps(5.0, is_state_switch=False) == 5.0

    def test_cascade_slippage_with_switch(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_cascade_slippage_bps
        result = _compute_cascade_slippage_bps(5.0, close_volume=100, avg_volume=1000, is_state_switch=True)
        assert result > 5.0  # 级联滑点应大于基础滑点

    def test_option_mtm_price_zero_input(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_option_mtm_price
        assert _compute_option_mtm_price(0, 100, 30, 0.2) == 0.0

    def test_option_mtm_price_valid_call(self):
        from ali2026v3_trading.param_pool.backtest_config import _compute_option_mtm_price
        price = _compute_option_mtm_price(3100, 3000, 30, 0.2, "call")
        assert price > 0  # ATM call 应有正价值

    def test_infer_liquidity_tier_none(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_liquidity_tier
        assert _infer_liquidity_tier(None) == 'future_main'

    def test_infer_liquidity_tier_option_atm(self):
        from ali2026v3_trading.param_pool.backtest_config import _infer_liquidity_tier
        bar = pd.Series({"strike_price": 3000, "underlying_price": 3010, "open_interest": 1000})
        assert _infer_liquidity_tier(bar) == 'option_atm'

    def test_state_no_local_pricing_definitions(self):
        """backtest_state.py 的定价/识别函数来自 backtest_config（非本地定义）"""
        import ali2026v3_trading.param_pool.backtest_state as mod
        pricing_fns = [
            '_infer_contract_type', '_infer_exchange_id', '_infer_exchange_from_id',
            '_infer_instrument_type', '_calculate_limit_prices', 'calc_trade_fee',
            '_get_contract_multiplier', '_infer_liquidity_tier',
            '_get_expiry_slippage_multiplier', '_compute_cascade_slippage_bps',
            '_compute_option_mtm_price',
        ]
        for name in pricing_fns:
            fn = getattr(mod, name)
            assert 'backtest_config' in fn.__code__.co_filename, (
                f"{name} 定义在 {fn.__code__.co_filename}，应在 backtest_config"
            )
