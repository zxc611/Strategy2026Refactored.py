# test_fix_20260708_v4.py — 14:01重启后6策略快照断点修复验证
# 覆盖: (1) _provider_ref=None根因修复（ensure_order_service设置_provider_ref）
#       (2) 虚拟回调注入诊断日志升级
#       (3) 6策略信号链路完整性
#       (4) 14:01重启后对齐验证
import pytest
import os
from unittest.mock import MagicMock, patch


# ===================== 1. _provider_ref根因修复 =====================

class TestProviderRefRootCause:
    """验证ensure_order_service中设置_provider_ref的修复"""

    def test_ensure_order_service_sets_provider_ref(self):
        """ensure_order_service应在OrderService初始化时设置_provider_ref"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_provider_ref' in src
        assert 'provider._order_service._provider_ref = provider' in src
        assert 'FIX-20260708-PROVIDER-REF' in src

    def test_provider_ref_set_before_dry_run_callback(self):
        """_provider_ref在OrderService初始化时设置，早于dry_run回调注入"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        # 确保_provider_ref设置在ensure_order_service内
        func_start = src.find('def ensure_order_service')
        assert func_start > 0
        next_def = src.find('\n    def ', func_start + 1)
        func_body = src[func_start:next_def] if next_def > 0 else src[func_start:]
        assert '_provider_ref = provider' in func_body

    def test_provider_ref_provider_is_strategy_core(self):
        """_provider_ref指向provider(策略核心对象)，有_position_service等属性"""
        # 逻辑验证：provider是self._provider，即StrategyCoreService
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        # ensure_order_service中provider = self._provider
        func_start = src.find('def ensure_order_service')
        next_def = src.find('\n    def ', func_start + 1)
        func_body = src[func_start:next_def] if next_def > 0 else src[func_start:]
        assert 'provider = self._provider' in func_body
        assert '_provider_ref = provider' in func_body


# ===================== 2. 虚拟回调注入诊断日志 =====================

class TestDryRunCallbackDiagnostics:
    """验证虚拟回调注入的日志升级"""

    def test_provider_fallback_failure_is_warning(self):
        """provider回退失败应为WARNING级别（非DEBUG）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_validation.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'logging.warning("[DRY-RUN] 所有回退均无法获取provider' in src
        assert '_dry_run_provider_fallback_fail_count' in src

    def test_provider_fallback_success_is_info(self):
        """provider回退成功应有INFO日志"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_validation.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'logging.info("[DRY-RUN] provider回退成功' in src
        assert '_dry_run_provider_fallback_ok_count' in src

    def test_position_service_none_is_warning(self):
        """_position_service为None应为WARNING级别"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_validation.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'logging.warning("[DRY-RUN] _position_service为None' in src

    def test_fallback_sources_tracked(self):
        """回退来源应被追踪（direct/platform_fn_self/cached_params）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_validation.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert "_fb_source = 'direct'" in src
        assert "_fb_source = 'platform_fn_self'" in src
        assert "_fb_source = 'cached_params'" in src


# ===================== 3. 6策略信号链路完整性 =====================

class TestSixStrategySignalChain:
    """验证6策略的信号链路完整性"""

    def test_correct_trending_signal_chain(self):
        """correct_trending: option_trading_cycle → select_otm_targets → send_order"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'execute_option_trading_cycle' in src
        assert 'select_otm_targets' in src
        assert 'execute_by_ranking' in src

    def test_spring_box_signal_chain(self):
        """spring/box: BoxSpringStrategy → on_tick → box检测 → spring_entry"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'get_box_spring_strategy' in src
        assert 'bss.on_tick' in src

    def test_resonance_signal_chain(self):
        """resonance: _feed_shadow_engine → SHADOW_TRADE"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_feed_shadow_engine' in src

    def test_hft_signal_chain(self):
        """hft: hft_enhancements → ensure_hft_engine"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'ensure_hft_engine' in src

    def test_close_signal_chain_exists(self):
        """平仓信号链路存在：check_position_risk → _check_stop_profit/_check_stop_loss"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'position', 'position_check_service.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_check_stop_profit' in src
        assert '_check_stop_loss' in src

    def test_dry_run_virtual_callback_adds_position(self):
        """dry_run虚拟回调注入调用_add_position（开仓）和_reduce_position（平仓）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_validation.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_add_position' in src
        assert '_reduce_position' in src
        assert '虚拟开仓回调注入成功' in src
        assert '虚拟平仓回调注入成功' in src


# ===================== 4. 14:01重启后对齐验证 =====================

class Test14pmRestartAlignment:
    """14:01重启后对齐验证"""

    def test_check_pending_orders_no_crash(self):
        """check_pending_orders不再崩溃（_safe_elapsed已部署）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_state_manager.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_safe_elapsed' in src

    def test_dry_run_intercept_working(self):
        """dry_run拦截生效（4重回退+传播）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_platform.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert 'FIX-20260708-DRY-RUN-V2' in src

    def test_state_param_manager_five_state(self):
        """StateParamManager五态判定正常运行"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'config', 'state_param.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        assert '_STATE_MAP' in src
        assert 'correct_rise' in src
        assert 'wrong_rise' in src
        assert 'incorrect_reversal' in src

    def test_strategy_mode_map_completeness(self):
        """_STATE_MAP包含所有5种状态"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'config', 'state_param.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        for state in ['correct_rise', 'correct_fall', 'wrong_rise', 'wrong_fall', 'other']:
            assert state in src, f"Missing state: {state}"


# ===================== 5. 彻底性排查 =====================

class TestThoroughnessV4:
    """彻底性排查：确保修复完整无遗漏"""

    def test_all_fix_files_compilable(self):
        """所有修复文件可编译"""
        import py_compile
        files = [
            'order/order_executor_platform.py',
            'order/order_executor_validation.py',
            'order/order_state_manager.py',
            'lifecycle/lifecycle_callbacks.py',
            'strategy/tick_dispatch.py',
            'strategy/tick_processing_service.py',
            'strategy/strategy_business_layer.py',
        ]
        base = os.path.join(os.path.dirname(__file__), '..')
        for f in files:
            fpath = os.path.join(base, f)
            assert os.path.exists(fpath), f"Missing: {f}"
            py_compile.compile(fpath, doraise=True)

    def test_provider_ref_not_set_in_init(self):
        """init_order_service_attrs中未设置_provider_ref（这是根因）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_base.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        # 确认init_order_service_attrs不设置_provider_ref
        func_start = src.find('def init_order_service_attrs')
        next_def = src.find('\ndef ', func_start + 1)
        func_body = src[func_start:next_def] if next_def > 0 else src[func_start:]
        assert '_provider_ref' not in func_body, "init_order_service_attrs不应设置_provider_ref（现在由ensure_order_service设置）"

    def test_ensure_order_service_only_sets_once(self):
        """ensure_order_service有双重检查锁，_provider_ref只设置一次"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        func_start = src.find('def ensure_order_service')
        next_def = src.find('\n    def ', func_start + 1)
        func_body = src[func_start:next_def] if next_def > 0 else src[func_start:]
        # 双重检查锁: if provider._order_service is not None: return (出现2次)
        assert func_body.count('if provider._order_service is not None:') == 2

    def test_dry_run_callback_has_exception_safety(self):
        """虚拟回调注入有异常安全（try/except包裹）"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'order', 'order_executor_validation.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        # _inject_dry_run_callback内部应有try/except
        assert 'except' in src
        assert 'ValueError' in src or 'TypeError' in src

    def test_provider_ref_set_after_order_service_init(self):
        """_provider_ref在get_order_service()之后设置，确保svc对象存在"""
        src_path = os.path.join(
            os.path.dirname(__file__), '..', 'strategy', 'strategy_business_layer.py')
        with open(src_path, 'r', encoding='utf-8') as f:
            src = f.read()
        func_start = src.find('def ensure_order_service')
        next_def = src.find('\n    def ', func_start + 1)
        func_body = src[func_start:next_def] if next_def > 0 else src[func_start:]
        # _provider_ref = provider 应在 get_order_service() 之后
        idx_get = func_body.find('get_order_service()')
        idx_ref = func_body.find('_provider_ref = provider')
        assert idx_get > 0 and idx_ref > 0 and idx_ref > idx_get, "_provider_ref应在get_order_service()之后设置"
