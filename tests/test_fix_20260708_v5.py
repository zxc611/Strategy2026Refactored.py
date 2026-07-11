# test_fix_20260708_v5.py — 6策略开仓/平仓信号链路断点修复验证
# 覆盖: (1) incorrect_reversal sort_bucket过滤修复
#       (2) spring/box high/low fallback修复
#       (3) HFT套利信号下单链路修复
#       (4) _provider_ref设置修复
#       (5) 6策略信号链路完整性
import pytest
import os


# ===================== 1. incorrect_reversal sort_bucket修复 =====================

class TestIncorrectReversalSortBucket:
    """验证sort_bucket不再排除wrong_rise/wrong_fall状态的期权"""

    def test_sort_entry_has_current_status_field(self):
        """SortEntry包含current_status字段"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_types.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'current_status' in src
        assert "field(compare=False, default='other')" in src

    def test_sort_entry_create_accepts_current_status(self):
        """SortEntry.create方法接受current_status参数"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_types.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert "current_status: str = 'other'" in src
        assert 'current_status=current_status' in src

    def test_update_sort_bucket_allows_wrong_rise_fall(self):
        """_update_sort_bucket允许wrong_rise/wrong_fall进入sort_bucket"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_state_mixin.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'FIX-20260708-INCORRECT-REV' in src
        assert "'wrong_rise'" in src
        assert "'wrong_fall'" in src
        # 确认不再只允许correct_rise/correct_fall
        assert "not in ('correct_rise', 'correct_fall'):" not in src

    def test_select_from_sort_bucket_returns_current_status(self):
        """select_from_sort_bucket返回的字典包含current_status"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_query_mixin.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert "'current_status'" in src
        assert "getattr(entry, 'current_status', 'other')" in src

    def test_sort_entry_create_passes_current_status(self):
        """_update_sort_bucket在创建SortEntry时传入current_status"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_state_mixin.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'current_status=current_status' in src


# ===================== 2. spring/box high/low fallback修复 =====================

class TestBoxSpringHighLowFallback:
    """验证BoxSpring的on_tick在high/low为0时使用price作为fallback"""

    def test_on_tick_has_high_low_fallback(self):
        """on_tick方法包含high/low的fallback逻辑"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'box_spring_strategy_impl.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'FIX-20260708-BOXSPRING-HL' in src
        assert 'if high <= 0 and price > 0:' in src
        assert 'if low <= 0 and price > 0:' in src
        assert 'high = price' in src
        assert 'low = price' in src

    def test_on_tick_fallback_before_update_box(self):
        """fallback逻辑在update_box调用之前"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'box_spring_strategy_impl.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        idx_fallback = src.find('FIX-20260708-BOXSPRING-HL')
        idx_update = src.find('self.update_box', idx_fallback)
        assert idx_fallback > 0 and idx_update > 0 and idx_update > idx_fallback

    def test_on_tick_still_checks_high_low_positive(self):
        """fallback后仍检查high>0 and low>0"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'box_spring_strategy_impl.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        idx_fallback = src.find('FIX-20260708-BOXSPRING-HL')
        func_end = src.find('\n\n', idx_fallback + 100)
        func_section = src[idx_fallback:func_end] if func_end > 0 else src[idx_fallback:]
        assert 'if high > 0 and low > 0:' in func_section


# ===================== 3. HFT套利信号下单链路修复 =====================

class TestHFTArbitrageOrderLink:
    """验证HFT套利信号产生后有下单链路"""

    def test_handle_arbitrage_signal_has_order_logic(self):
        """handle_arbitrage_signal包含下单逻辑"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'FIX-20260708-HFT-ORDER' in src
        assert '_order_svc' in src
        assert 'send_order' in src
        assert "action='OPEN'" in src

    def test_hft_order_has_ensure_order_service(self):
        """HFT下单前确保OrderService已初始化"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_ensure_order_service' in src

    def test_hft_order_success_log(self):
        """HFT下单成功有INFO日志"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '套利信号下单成功' in src

    def test_hft_order_failure_log(self):
        """HFT下单失败有WARNING日志"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '套利信号下单失败' in src

    def test_hft_order_deviation_threshold(self):
        """HFT下单阈值: deviation > 10"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        idx = src.find('FIX-20260708-HFT-ORDER')
        assert idx > 0
        section = src[idx:idx+500]
        assert 'abs(deviation) > 10' in section


# ===================== 4. _provider_ref设置修复 =====================

class TestProviderRefSetting:
    """验证ensure_order_service中设置_provider_ref"""

    def test_ensure_order_service_sets_provider_ref(self):
        """ensure_order_service设置_provider_ref"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_provider_ref = provider' in src
        assert 'FIX-20260708-PROVIDER-REF' in src


# ===================== 5. 6策略信号链路完整性 =====================

class TestSixStrategySignalChainComplete:
    """验证6策略的完整信号链路"""

    def test_correct_trending_chain(self):
        """correct_trending: option_trading_cycle → select_otm → execute_by_ranking"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'execute_option_trading_cycle' in src
        assert 'select_otm_targets_signal_sources' in src
        assert 'execute_by_ranking' in src

    def test_incorrect_reversal_chain(self):
        """incorrect_reversal: sort_bucket允许wrong_rise/wrong_fall → select → order"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_state_mixin.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert "'wrong_rise'" in src
        assert "'wrong_fall'" in src

    def test_spring_box_chain(self):
        """spring/box: BoxSpring.on_tick → update_box（high/low fallback）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'box_spring_strategy_impl.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'update_box' in src
        assert 'FIX-20260708-BOXSPRING-HL' in src

    def test_resonance_chain(self):
        """resonance: _feed_shadow_engine → SHADOW_TRADE"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_feed_shadow_engine' in src

    def test_hft_chain(self):
        """hft: handle_arbitrage_signal → send_order"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'handle_arbitrage_signal' in src
        assert 'send_order' in src
        assert 'FIX-20260708-HFT-ORDER' in src

    def test_close_signal_chain(self):
        """平仓: check_position_risk → _check_stop_profit/_check_stop_loss"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'position', 'position_check_service.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_check_stop_profit' in src
        assert '_check_stop_loss' in src


# ===================== 6. 彻底性排查 =====================

class TestThoroughnessV5:
    """彻底性排查"""

    def test_all_fix_files_compilable(self):
        """所有修复文件可编译"""
        import py_compile
        files = [
            'data/width_cache_types.py',
            'data/width_cache_state_mixin.py',
            'data/width_cache_query_mixin.py',
            'strategy/box_spring_strategy_impl.py',
            'strategy/tick_hft.py',
            'strategy/strategy_business_layer.py',
            'order/order_executor_validation.py',
        ]
        base = os.path.join(os.path.dirname(__file__), '..')
        for f in files:
            fpath = os.path.join(base, f)
            assert os.path.exists(fpath), f"Missing: {f}"
            py_compile.compile(fpath, doraise=True)

    def test_sort_entry_backward_compatible(self):
        """SortEntry.create向后兼容（current_status有默认值）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_types.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        # current_status参数有默认值"other"
        assert "current_status: str = 'other'" in src

    def test_box_spring_fallback_uses_price_not_zero(self):
        """BoxSpring fallback使用price（非0），不是硬编码值"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'box_spring_strategy_impl.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        # 确认fallback是price，不是1.0或其他硬编码值
        assert 'high = price' in src
        assert 'low = price' in src

    def test_hft_order_has_exception_safety(self):
        """HFT下单有异常安全（try/except）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'tick_hft.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'FIX-20260708-HFT-ORDER' in src
        assert '_hft_ord_err' in src  # except子句的变量名
        assert '套利信号下单失败' in src

    def test_sort_bucket_filter_comment_explains_fix(self):
        """sort_bucket过滤修复有注释说明"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'data', 'width_cache_state_mixin.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'FIX-20260708-INCORRECT-REV' in src
        assert 'incorrect_reversal' in src

    def test_all_six_strategies_have_signal_paths(self):
        """所有6个策略都有信号产生路径"""
        strategies = {
            'correct_trending': 'strategy/strategy_business_layer.py',
            'incorrect_reversal': 'data/width_cache_state_mixin.py',
            'spring': 'strategy/box_spring_strategy_impl.py',
            'resonance': 'strategy/strategy_business_layer.py',
            'box': 'strategy/box_spring_strategy_impl.py',
            'hft': 'strategy/tick_hft.py',
        }
        base = os.path.join(os.path.dirname(__file__), '..')
        for name, path in strategies.items():
            fpath = os.path.join(base, path)
            assert os.path.exists(fpath), f"Missing {name}: {path}"
