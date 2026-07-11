# -*- coding: utf-8 -*-
"""
test_fix_20260708_v2.py — 断言验证与回归测试

修复1: dry_run拦截V2（order_executor_platform.py）
  - _execute_platform_insert中dry_run检查使用4重回退
  - 任一为True即拦截，不用None判断
  - lifecycle_callbacks.py中_dry_run_active传播到OrderService._dry_run_mode

修复2: tick背压降级（tick_dispatch.py + tick_processing_service.py）
  - tick_preflight_check中不再return False丢弃tick，改为设置svc._tick_degraded_mode=True
  - tick_dispatch_layer中_degraded=True时走dispatch_tick_degraded轻量路径
  - 阈值变更: THRESHOLD 200→500ms, DROP_WINDOW 5→2s, EXTENDED_DROP 30→10s
  - 过载窗口结束后_tick_degraded_mode重置为False
"""

import sys
import os
import time
import threading
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from types import SimpleNamespace

# 确保项目根路径在sys.path中
_project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

_ali_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if _ali_root not in sys.path:
    sys.path.insert(0, _ali_root)


# ============================================================================
# 编译/导入检查
# ============================================================================

class TestImportCheck(unittest.TestCase):
    """验证源文件可正确导入（编译检查）"""

    def test_import_order_executor_platform(self):
        """order_executor_platform.py可导入"""
        from ali2026v3_trading.order import order_executor_platform
        self.assertTrue(hasattr(order_executor_platform, '_OrderExecutorPlatformMethods'))

    def test_import_tick_dispatch(self):
        """strategy/tick_dispatch.py可导入"""
        from ali2026v3_trading.strategy import tick_dispatch
        self.assertTrue(hasattr(tick_dispatch, 'tick_dispatch_layer'))
        self.assertTrue(hasattr(tick_dispatch, 'tick_preflight_check'))
        self.assertTrue(hasattr(tick_dispatch, 'dispatch_tick_degraded'))

    def test_import_tick_processing_service(self):
        """strategy/tick_processing_service.py可导入"""
        from ali2026v3_trading.strategy import tick_processing_service
        self.assertTrue(hasattr(tick_processing_service, 'TickProcessingService'))

    def test_import_lifecycle_callbacks_e(self):
        """e/lifecycle_callbacks.py可导入"""
        from ali2026v3_trading.e import lifecycle_callbacks as lc_e
        self.assertTrue(hasattr(lc_e, 'LifecycleCallbacks'))

    def test_import_lifecycle_callbacks(self):
        """lifecycle/lifecycle_callbacks.py可导入"""
        from ali2026v3_trading.lifecycle import lifecycle_callbacks as lc
        self.assertTrue(hasattr(lc, 'LifecycleCallbacks'))


# ============================================================================
# 修复1: dry_run多重回退逻辑验证
# ============================================================================

class _OrderSvcMock:
    """真实的mock对象（非MagicMock），避免自动属性创建导致的比较异常。

    MagicMock会在getattr时自动创建子mock，导致 time.time()-MagicMock 的比较失败。
    使用真实对象+手动方法mock来避免此问题。
    """
    def __init__(self):
        # dry_run相关
        self._dry_run_mode = False
        self._provider_ref = None
        self._platform_insert_order = None

        # 订单生成
        self._generate_order_id = MagicMock(return_value='ORD_TEST_001')

        # 滑点估算
        self._estimate_slippage = MagicMock(return_value=0.5)

        # 线程锁
        self._lock = threading.Lock()

        # 幂等集合
        self._order_idempotent_set = set()

        # 订单追踪
        self._orders_by_id = {}
        self._MAX_ORDERS_TRACKED = 1000
        self._persist_idempotent_key = MagicMock()

        # 统计
        self._stats = {'total_orders': 0, 'failed_orders': 0}

        # 平台ID映射
        self._platform_id_to_order_id = {}

        # WAL
        self._wal_write = MagicMock()
        self._append_order_state = MagicMock()

        # 连续失败
        self._consecutive_failures = 0

        # 认证器（签名请求）- 透传order
        self.authenticator = MagicMock()
        self.authenticator.generate_signed_request.side_effect = lambda order: dict(order)

        # 平台下单参数
        self._platform_insert_order_params = set()
        self._platform_api_ready = True

        # 平台下单辅助方法
        self._build_platform_insert_params = MagicMock(return_value={})
        self._invoke_platform_insert_with_timeout = MagicMock(return_value=12345)
        self._normalize_platform_result = MagicMock(return_value={'order_id': 12345})

        # 断路器
        self._circuit_breaker_threshold = 10
        self._platform_submit_timeout_seconds = 5.0
        self._remove_order_and_idempotent_key = MagicMock()


def _make_full_svc():
    """构造完整的OrderService mock"""
    return _OrderSvcMock()


def _make_ctx(**overrides):
    """构造最小OrderContext - 使用SimpleNamespace确保属性赋值正常工作"""
    defaults = dict(
        instrument_id='ag2607',
        exchange='SHFE',
        volume=1,
        price=5000.0,
        direction='BUY',
        action='OPEN',
        signal_id='sig_001',
        open_reason='test',
        decision_score=0.8,
        position_scale=1.0,
        decision_action='OPEN',
        dimension_scores={},
        dimension_weights={},
        idempotent_key='test_idempotent_key_001',
        order_id=None,
        order={},
        rejected=False,
        reject_code='',
        reject_message='',
        _order_submit_start_ts=time.perf_counter(),
        _cyclic_guard=MagicMock(),
    )
    defaults.update(overrides)

    ctx = SimpleNamespace(**defaults)
    return ctx


class TestDryRunMultiFallback(unittest.TestCase):
    """验证_execute_platform_insert中dry_run 4重回退逻辑"""

    def _make_executor(self, svc):
        """构造_OrderExecutorPlatformMethods实例"""
        from ali2026v3_trading.order.order_executor_platform import _OrderExecutorPlatformMethods
        executor = _OrderExecutorPlatformMethods()
        executor._svc = svc
        executor._simulate_dry_run_callbacks = MagicMock()
        return executor

    def test_fallback1_svc_dry_run_mode_true(self):
        """检查1: svc._dry_run_mode=True时拦截"""
        svc = _make_full_svc()
        svc._dry_run_mode = True
        ctx = _make_ctx()
        executor = self._make_executor(svc)
        executor._execute_platform_insert(ctx)
        self.assertEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_fallback2_provider_dry_run_active(self):
        """检查2: svc._dry_run_mode=False但_provider_ref._dry_run_active=True时拦截"""
        svc = _make_full_svc()
        svc._dry_run_mode = False
        provider = MagicMock()
        provider._dry_run_active = True
        svc._provider_ref = provider
        ctx = _make_ctx()
        executor = self._make_executor(svc)
        executor._execute_platform_insert(ctx)
        self.assertEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_fallback3_platform_self_dry_run_active(self):
        """检查3: svc._dry_run_mode=False, provider=None, 但_platform_insert_order.__self__._dry_run_active=True"""
        svc = _make_full_svc()
        svc._dry_run_mode = False
        svc._provider_ref = None

        # 构造一个绑定方法的__self__
        host_obj = MagicMock()
        host_obj._dry_run_active = True
        platform_fn = MagicMock()
        platform_fn.__self__ = host_obj
        svc._platform_insert_order = platform_fn

        ctx = _make_ctx()
        executor = self._make_executor(svc)
        executor._execute_platform_insert(ctx)
        self.assertEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_fallback4_get_cached_params(self):
        """检查4: 前3项均False, get_cached_params().dry_run_mode=True时拦截"""
        svc = _make_full_svc()
        svc._dry_run_mode = False
        svc._provider_ref = None
        host_obj = MagicMock()
        host_obj._dry_run_active = False
        platform_fn = MagicMock()
        platform_fn.__self__ = host_obj
        svc._platform_insert_order = platform_fn

        ctx = _make_ctx()
        executor = self._make_executor(svc)

        # 由于lazy import在函数内部: from ali2026v3_trading.config.config_params import get_cached_params
        # 我们需要patch sys.modules使得该import返回我们的mock
        mock_params_module = MagicMock()
        mock_params_module.get_cached_params.return_value = {'dry_run_mode': True}

        with patch.dict('sys.modules', {
            'ali2026v3_trading.config.config_params': mock_params_module,
        }):
            executor._execute_platform_insert(ctx)
            self.assertEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_no_fallback_when_all_false(self):
        """所有4项均为False时不拦截（正常下单路径）"""
        svc = _make_full_svc()
        svc._dry_run_mode = False
        svc._provider_ref = MagicMock()
        svc._provider_ref._dry_run_active = False

        host_obj = MagicMock()
        host_obj._dry_run_active = False
        platform_fn = MagicMock()
        platform_fn.__self__ = host_obj
        platform_fn.__name__ = 'insert_order'
        svc._platform_insert_order = platform_fn

        ctx = _make_ctx()
        executor = self._make_executor(svc)

        # 必须patch get_cached_params: params.yaml中dry_run_mode默认为true，
        # 不patch则检查4会把所有dry_run=False的场景都拦截
        mock_params_module = MagicMock()
        mock_params_module.get_cached_params.return_value = {'dry_run_mode': False}
        with patch.dict('sys.modules', {
            'ali2026v3_trading.config.config_params': mock_params_module,
        }):
            executor._execute_platform_insert(ctx)
        # 不应被dry_run拦截
        self.assertNotEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_svc_dry_run_mode_false_does_not_skip_remaining_fallbacks(self):
        """关键回归: svc._dry_run_mode=False时不应跳过后续回退检查
        这是之前bug的核心：svc._dry_run_mode=False时不应短路"""
        svc = _make_full_svc()
        svc._dry_run_mode = False  # 显式False
        provider = MagicMock()
        provider._dry_run_active = True  # 但provider上有dry_run
        svc._provider_ref = provider

        ctx = _make_ctx()
        executor = self._make_executor(svc)
        executor._execute_platform_insert(ctx)
        # 必须被拦截（因为provider._dry_run_active=True）
        self.assertEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_dry_run_intercept_uses_true_check_not_none(self):
        """验证dry_run用bool(True)判断而非is None判断"""
        svc = _make_full_svc()
        # _dry_run_mode = False (not None), 应不拦截
        svc._dry_run_mode = False
        svc._provider_ref = MagicMock()
        svc._provider_ref._dry_run_active = False

        host_obj = MagicMock()
        host_obj._dry_run_active = False
        platform_fn = MagicMock()
        platform_fn.__self__ = host_obj
        svc._platform_insert_order = platform_fn

        ctx = _make_ctx()
        executor = self._make_executor(svc)

        # patch get_cached_params: params.yaml中dry_run_mode默认true
        mock_params_module = MagicMock()
        mock_params_module.get_cached_params.return_value = {'dry_run_mode': False}
        with patch.dict('sys.modules', {
            'ali2026v3_trading.config.config_params': mock_params_module,
        }):
            executor._execute_platform_insert(ctx)
        # _dry_run_mode=False不应被当作None而误拦截
        self.assertNotEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')

    def test_lifecycle_callbacks_propagates_dry_run_active(self):
        """验证lifecycle_callbacks.py源码中包含_dry_run_active传播逻辑"""
        import inspect
        from ali2026v3_trading.lifecycle import lifecycle_callbacks as lc_module
        source = inspect.getsource(lc_module.LifecycleCallbacks.on_start)
        self.assertIn("_dry_run_active", source, "lifecycle_callbacks.on_start必须包含_dry_run_active设置")
        self.assertIn("OrderService", source, "lifecycle_callbacks.on_start必须包含OrderService._dry_run_mode同步")

    def test_lifecycle_callbacks_dry_run_mode_sync_source(self):
        """验证lifecycle_callbacks.py源码中包含_dry_run_mode同步到OrderService的逻辑"""
        import inspect
        from ali2026v3_trading.lifecycle import lifecycle_callbacks as lc_module
        source = inspect.getsource(lc_module.LifecycleCallbacks.on_start)
        # 关键代码: _osvc._dry_run_mode = True
        self.assertIn("_dry_run_mode", source,
                      "lifecycle_callbacks.on_start必须包含_dry_run_mode同步逻辑")

    def test_e_lifecycle_callbacks_dry_run_active_source(self):
        """验证e/lifecycle_callbacks.py源码中也包含_dry_run_active"""
        import inspect
        from ali2026v3_trading.e import lifecycle_callbacks as lc_e_module
        source = inspect.getsource(lc_e_module.LifecycleCallbacks.on_start)
        self.assertIn("_dry_run_active", source,
                      "e/lifecycle_callbacks.on_start必须包含_dry_run_active设置")


# ============================================================================
# 修复2: tick背压降级逻辑验证
# ============================================================================

class TestTickBackpressureDegradation(unittest.TestCase):
    """验证tick背压降级逻辑"""

    def _make_svc(self):
        """构造最小TickProcessingService mock"""
        svc = MagicMock()
        svc._tick_overload_until = 0.0
        svc._tick_degraded_mode = False
        svc._tick_dedup_cache = {}
        svc._tick_dedup_window_ms = 100.0
        svc._tick_last_data_time = {}
        svc._tick_seq_lock = threading.Lock()
        svc._tick_last_seq = {}
        svc._tick_seq_error_count = 0
        svc._state_store = MagicMock()
        svc._state_store.set = MagicMock()
        svc._state_store.get_ref.return_value = None
        svc._state_store.get.return_value = None
        svc._backpressure_degrade_count = 0
        return svc

    def _make_tick(self, instrument_id='ag2607', last_price=5000.0, volume=10,
                   datetime_val=None, exchange='SHFE'):
        """构造最小tick mock"""
        tick = MagicMock()
        tick.instrument_id = instrument_id
        tick.last_price = last_price
        tick.volume = volume
        tick.exchange = exchange
        tick.datetime = datetime_val or time.time()
        tick.update_time = None
        tick.sequence_no = None
        tick.bid_price1 = 4999.0
        tick.ask_price1 = 5001.0
        return tick

    def test_preflight_returns_true_not_false_during_overload(self):
        """tick_preflight_check在过载期间返回True（不丢弃tick），改为设置降级标记"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check

        svc = self._make_svc()
        # 设置过载窗口为未来10秒
        svc._tick_overload_until = time.time() + 10.0

        tick = self._make_tick()
        result = tick_preflight_check(svc, tick)

        # 关键：过载期间返回True（不丢弃），不是False
        self.assertTrue(result, "tick_preflight_check在过载期间应返回True（降级处理而非丢弃）")

    def test_preflight_sets_degraded_mode_during_overload(self):
        """tick_preflight_check在过载期间设置_tick_degraded_mode=True"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check

        svc = self._make_svc()
        svc._tick_overload_until = time.time() + 10.0
        svc._tick_degraded_mode = False

        tick = self._make_tick()
        tick_preflight_check(svc, tick)

        self.assertTrue(svc._tick_degraded_mode,
                        "过载期间_tick_degraded_mode应被设为True")

    def test_preflight_resets_degraded_mode_after_overload(self):
        """过载窗口结束后_tick_degraded_mode重置为False"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check

        svc = self._make_svc()
        # 过载窗口已过期
        svc._tick_overload_until = time.time() - 1.0
        svc._tick_degraded_mode = True  # 之前处于降级模式

        tick = self._make_tick()
        tick_preflight_check(svc, tick)

        self.assertFalse(svc._tick_degraded_mode,
                         "过载窗口结束后_tick_degraded_mode应重置为False")

    def test_preflight_normal_path_when_no_overload(self):
        """无过载时_tick_degraded_mode保持False"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check

        svc = self._make_svc()
        svc._tick_overload_until = 0.0  # 从未过载
        svc._tick_degraded_mode = False

        tick = self._make_tick()
        result = tick_preflight_check(svc, tick)

        self.assertTrue(result, "正常时应返回True")
        self.assertFalse(svc._tick_degraded_mode, "正常时_tick_degraded_mode应保持False")

    def test_dispatch_layer_uses_degraded_path(self):
        """tick_dispatch_layer在_tick_degraded_mode=True时走dispatch_tick_degraded轻量路径"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_dispatch_layer

        svc = MagicMock()
        svc._tick_degraded_mode = True
        svc._normalize_tick.return_value = {
            'instrument_id': 'ag2607',
            'exchange': 'SHFE',
            'last_price': 5000.0,
            'volume': 10,
        }
        svc._state_store = MagicMock()
        svc._state_store.get_ref.return_value = None
        svc._tick_stats_lock = threading.Lock()
        svc._tick_count = 0
        svc._last_tick_log_time = time.time()
        svc._tick_log_interval = 180.0

        tick = self._make_tick()

        with patch('ali2026v3_trading.strategy.tick_dispatch.dispatch_tick_degraded') as mock_degraded, \
             patch('ali2026v3_trading.strategy.tick_dispatch.process_tick_unified_path') as mock_unified, \
             patch('ali2026v3_trading.strategy.tick_dispatch.check_hard_time_stop_live'), \
             patch('ali2026v3_trading.infra.subscription_service.SubscriptionManager') as mock_sm:
            mock_sm.is_option.return_value = False
            tick_dispatch_layer(svc, tick, 'ag2607')

            # 降级模式下应调用dispatch_tick_degraded，而非process_tick_unified_path
            mock_degraded.assert_called_once()
            mock_unified.assert_not_called()

    def test_dispatch_layer_uses_unified_path_when_not_degraded(self):
        """tick_dispatch_layer在_tick_degraded_mode=False时走process_tick_unified_path"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_dispatch_layer

        svc = MagicMock()
        svc._tick_degraded_mode = False
        svc._normalize_tick.return_value = {
            'instrument_id': 'ag2607',
            'exchange': 'SHFE',
            'last_price': 5000.0,
            'volume': 10,
        }
        svc._state_store = MagicMock()
        svc._state_store.get_ref.return_value = None
        svc._tick_stats_lock = threading.Lock()
        svc._tick_count = 0
        svc._last_tick_log_time = time.time()
        svc._tick_log_interval = 180.0

        tick = self._make_tick()

        with patch('ali2026v3_trading.strategy.tick_dispatch.dispatch_tick_degraded') as mock_degraded, \
             patch('ali2026v3_trading.strategy.tick_dispatch.process_tick_unified_path') as mock_unified, \
             patch('ali2026v3_trading.strategy.tick_dispatch.check_hard_time_stop_live'), \
             patch('ali2026v3_trading.infra.subscription_service.SubscriptionManager') as mock_sm:
            mock_sm.is_option.return_value = False
            tick_dispatch_layer(svc, tick, 'ag2607')

            # 正常模式下应调用process_tick_unified_path
            mock_unified.assert_called_once()
            mock_degraded.assert_not_called()


# ============================================================================
# 阈值变更验证
# ============================================================================

class TestThresholdChanges(unittest.TestCase):
    """验证tick背压阈值变更"""

    def test_overload_threshold_500ms(self):
        """_TICK_OVERLOAD_THRESHOLD_MS应为500.0ms（原200ms）"""
        from ali2026v3_trading.strategy.tick_processing_service import TickProcessingService
        self.assertEqual(TickProcessingService._TICK_OVERLOAD_THRESHOLD_MS, 500.0,
                         "_TICK_OVERLOAD_THRESHOLD_MS应为500.0ms")

    def test_overload_drop_window_2s(self):
        """_TICK_OVERLOAD_DROP_WINDOW_SEC应为2.0s（原5s）"""
        from ali2026v3_trading.strategy.tick_processing_service import TickProcessingService
        self.assertEqual(TickProcessingService._TICK_OVERLOAD_DROP_WINDOW_SEC, 2.0,
                         "_TICK_OVERLOAD_DROP_WINDOW_SEC应为2.0s")

    def test_overload_extended_drop_10s(self):
        """_TICK_OVERLOAD_EXTENDED_DROP_SEC应为10.0s（原30s）"""
        from ali2026v3_trading.strategy.tick_processing_service import TickProcessingService
        self.assertEqual(TickProcessingService._TICK_OVERLOAD_EXTENDED_DROP_SEC, 10.0,
                         "_TICK_OVERLOAD_EXTENDED_DROP_SEC应为10.0s")


# ============================================================================
# 源码断言验证
# ============================================================================

class TestSourceCodeAssertions(unittest.TestCase):
    """验证源码中包含修复的关键模式"""

    def test_dry_run_four_fallback_checks_in_source(self):
        """验证order_executor_platform.py源码中包含4重回退检查"""
        import inspect
        from ali2026v3_trading.order.order_executor_platform import _OrderExecutorPlatformMethods
        source = inspect.getsource(_OrderExecutorPlatformMethods._execute_platform_insert)

        # 检查4重回退都存在
        self.assertIn("_dry_run_mode", source, "必须包含svc._dry_run_mode检查")
        self.assertIn("_provider_ref", source, "必须包含_provider_ref._dry_run_active检查")
        self.assertIn("__self__", source, "必须包含_platform_insert_order.__self__._dry_run_active检查")
        self.assertIn("get_cached_params", source, "必须包含get_cached_params兜底检查")

    def test_dry_run_check_uses_true_not_none(self):
        """验证dry_run检查用True/False判断而非is None"""
        import inspect
        from ali2026v3_trading.order.order_executor_platform import _OrderExecutorPlatformMethods
        source = inspect.getsource(_OrderExecutorPlatformMethods._execute_platform_insert)

        # 关键: getattr(svc, '_dry_run_mode', False) - 用False默认值
        # 不是getattr(svc, '_dry_run_mode', None) is not None
        self.assertIn("getattr(svc, '_dry_run_mode', False)", source,
                      "dry_run检查应使用getattr默认值False而非None")

    def test_tick_no_drop_pattern_in_source(self):
        """验证tick_dispatch.py源码中背压降级不丢弃tick的标记"""
        import inspect
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check
        source = inspect.getsource(tick_preflight_check)

        # 验证降级模式设置
        self.assertIn("_tick_degraded_mode", source,
                      "tick_preflight_check必须设置_tick_degraded_mode")

    def test_dispatch_layer_degraded_check_in_source(self):
        """验证tick_dispatch_layer源码中包含degraded模式分支"""
        import inspect
        from ali2026v3_trading.strategy.tick_dispatch import tick_dispatch_layer
        source = inspect.getsource(tick_dispatch_layer)

        self.assertIn("_tick_degraded_mode", source,
                      "tick_dispatch_layer必须检查_tick_degraded_mode")
        self.assertIn("dispatch_tick_degraded", source,
                      "tick_dispatch_layer必须在degraded时调用dispatch_tick_degraded")

    def test_lifecycle_callbacks_dry_run_active_propagation(self):
        """验证lifecycle_callbacks.py源码中_dry_run_active传播到OrderService"""
        import inspect
        from ali2026v3_trading.lifecycle import lifecycle_callbacks as lc_module
        source = inspect.getsource(lc_module.LifecycleCallbacks.on_start)

        self.assertIn("_dry_run_active", source,
                      "lifecycle_callbacks.on_start必须设置_dry_run_active")
        self.assertIn("_dry_run_mode", source,
                      "lifecycle_callbacks.on_start必须同步_dry_run_mode到OrderService")

    def test_tick_overload_reset_degraded_mode_in_source(self):
        """验证tick_preflight_check源码中包含降级模式重置逻辑"""
        import inspect
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check
        source = inspect.getsource(tick_preflight_check)

        # 验证存在重置逻辑: _tick_degraded_mode = False
        self.assertIn("_tick_degraded_mode = False", source,
                      "tick_preflight_check必须在过载窗口结束时重置_tick_degraded_mode为False")


# ============================================================================
# 回归测试：确保修复不破坏现有功能
# ============================================================================

class TestRegression(unittest.TestCase):
    """回归测试：确保修复不破坏现有功能"""

    def test_dry_run_all_false_proceeds_to_platform(self):
        """回归: 所有dry_run检查均为False时应正常下单"""
        svc = _make_full_svc()
        svc._dry_run_mode = False
        svc._provider_ref = MagicMock()
        svc._provider_ref._dry_run_active = False

        host_obj = MagicMock()
        host_obj._dry_run_active = False
        host_obj.trading = True
        platform_fn = MagicMock()
        platform_fn.__self__ = host_obj
        platform_fn.__name__ = 'insert_order'
        svc._platform_insert_order = platform_fn

        ctx = _make_ctx()

        from ali2026v3_trading.order.order_executor_platform import _OrderExecutorPlatformMethods
        executor = _OrderExecutorPlatformMethods()
        executor._svc = svc
        executor._simulate_dry_run_callbacks = MagicMock()

        # patch get_cached_params: params.yaml中dry_run_mode默认true
        mock_params_module = MagicMock()
        mock_params_module.get_cached_params.return_value = {'dry_run_mode': False}
        with patch.dict('sys.modules', {
            'ali2026v3_trading.config.config_params': mock_params_module,
        }):
            executor._execute_platform_insert(ctx)

        # 不应被dry_run拦截
        self.assertNotEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')
        # 平台下单应被调用
        svc._invoke_platform_insert_with_timeout.assert_called_once()

    def test_tick_normal_processing_not_affected(self):
        """回归: 正常情况下tick_preflight_check返回True"""
        from ali2026v3_trading.strategy.tick_dispatch import tick_preflight_check

        svc = MagicMock()
        svc._tick_overload_until = 0.0
        svc._tick_degraded_mode = False
        svc._tick_dedup_cache = {}
        svc._tick_dedup_window_ms = 100.0
        svc._tick_last_data_time = {}
        svc._tick_seq_lock = threading.Lock()
        svc._tick_last_seq = {}
        svc._tick_seq_error_count = 0
        svc._state_store = MagicMock()
        svc._state_store.set = MagicMock()
        svc._state_store.get_ref.return_value = None
        svc._state_store.get.return_value = None
        svc._get_tick_field = MagicMock(side_effect=lambda t, f, d=None: d)

        tick = MagicMock()
        tick.instrument_id = 'ag2607'
        tick.datetime = time.time()
        tick.sequence_no = None

        result = tick_preflight_check(svc, tick)
        self.assertTrue(result, "正常情况tick_preflight_check应返回True")

    def test_dry_run_first_check_hits(self):
        """回归: 检查1命中时正确拦截"""
        svc = _make_full_svc()
        svc._dry_run_mode = True

        ctx = _make_ctx()

        from ali2026v3_trading.order.order_executor_platform import _OrderExecutorPlatformMethods
        executor = _OrderExecutorPlatformMethods()
        executor._svc = svc
        executor._simulate_dry_run_callbacks = MagicMock()

        executor._execute_platform_insert(ctx)
        self.assertEqual(ctx.order.get('status'), 'DRY_RUN_ACCEPTED')


if __name__ == '__main__':
    unittest.main(verbosity=2)
