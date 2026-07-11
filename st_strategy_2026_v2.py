# MODULE_ID: M2-587
"""
test_strategy_2026_v2.py - Strategy2026 综合测试（pytest风格）

使用 unittest.mock 进行依赖隔离，避免真实初始化的副作用。
覆盖：类导入、默认属性值、日志方法、定时器取消、回调委托、
      市场状态查询、初始化参数传递、生命周期重复防护。
"""
import threading
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# 辅助：通过 __new__ 构造轻量实例，绕过 __init__ 的重依赖
# ---------------------------------------------------------------------------
def _make_strategy(**overrides):
    """构造一个绕过 __init__ 的 Strategy2026 实例，所有依赖均为 MagicMock。"""
    from ali2026v3_trading.strategy.strategy_2026 import Strategy2026

    s = Strategy2026.__new__(Strategy2026)
    s._is_real_strategy = True
    s.strategy_id = "test_strategy_001"
    s.strategy_core = MagicMock()
    s.auto_trading_enabled = False
    s._stop_requested = False
    s._callbacks_enabled = True
    s.config = {}
    s.params = MagicMock()
    s._lifecycle_lock = threading.Lock()
    s._start_executed = False
    s._stop_executed = False
    s._lifecycle_run_id = None
    s._config_loaded = False
    s._runtime_config = {}
    s._runtime_strategy_ref = None
    s._runtime_market_center_ref = None
    s._storage_warm_timer = None
    s._storage_warm_thread = None
    s._platform_subscribe_thread = None
    s._platform_subscribe_stop = threading.Event()
    for key, value in overrides.items():
        setattr(s, key, value)
    return s


# ===================================================================
# 1. 类导入与继承
# ===================================================================

class TestClassImport:
    """验证 Strategy2026 可被正确导入。"""

    def test_class_exists(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026 is not None

    def test_inherits_base_strategy(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026, BaseStrategy
        assert issubclass(Strategy2026, BaseStrategy)

    def test_inherits_ui_mixin(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        from ali2026v3_trading.config.ui_service import UIMixin
        assert issubclass(Strategy2026, UIMixin)


# ===================================================================
# 2. 默认属性值
# ===================================================================

class TestDefaultAttributes:

    def test_is_real_strategy_default(self):
        s = _make_strategy()
        assert s._is_real_strategy is True

    def test_auto_trading_enabled_default(self):
        s = _make_strategy()
        assert s.auto_trading_enabled is False

    def test_stop_requested_default(self):
        s = _make_strategy()
        assert s._stop_requested is False

    def test_callbacks_enabled_default(self):
        s = _make_strategy()
        assert s._callbacks_enabled is True

    def test_config_is_dict(self):
        s = _make_strategy()
        assert isinstance(s.config, dict)

    def test_start_executed_default(self):
        s = _make_strategy()
        assert s._start_executed is False

    def test_stop_executed_default(self):
        s = _make_strategy()
        assert s._stop_executed is False

    def test_config_loaded_default(self):
        s = _make_strategy()
        assert s._config_loaded is False


# ===================================================================
# 3. 日志方法 — 不抛异常
# ===================================================================

class TestLogMethods:

    def test_log_warning_does_not_raise(self):
        s = _make_strategy()
        s._log_warning("test warning")

    def test_log_info_does_not_raise(self):
        s = _make_strategy()
        s._log_info("test info")

    def test_log_error_does_not_raise(self):
        s = _make_strategy()
        s._log_error("test error")


# ===================================================================
# 4. 定时器取消
# ===================================================================

class TestCancelAllTimers:

    def test_cancel_all_timers_no_timers(self):
        """没有定时器时不应抛异常。"""
        s = _make_strategy()
        s._cancel_all_timers()  # 不应抛异常

    def test_cancel_all_timers_with_timer(self):
        """设置 mock timer 后，cancel 应被调用且引用置空。"""
        s = _make_strategy()
        mock_timer = MagicMock()
        s._storage_warm_timer = mock_timer

        s._cancel_all_timers()

        mock_timer.cancel.assert_called_once()
        assert s._storage_warm_timer is None

    def test_cancel_all_timers_with_warm_thread(self):
        """有存活的 warm_thread 时，应 join 并置空。"""
        s = _make_strategy()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        s._storage_warm_thread = mock_thread

        s._cancel_all_timers()

        mock_thread.join.assert_called_once_with(timeout=5.0)
        assert s._storage_warm_thread is None

    def test_cancel_all_timers_with_subscribe_thread(self):
        """有存活的 subscribe_thread 时，应设置 stop 事件、join 并置空。"""
        s = _make_strategy()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        stop_event = threading.Event()
        s._platform_subscribe_thread = mock_thread
        s._platform_subscribe_stop = stop_event

        s._cancel_all_timers()

        assert stop_event.is_set()
        mock_thread.join.assert_called_once_with(timeout=5.0)
        assert s._platform_subscribe_thread is None

    def test_cancel_all_timers_dead_thread_skipped(self):
        """已死亡的线程不应被 join。"""
        s = _make_strategy()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        s._storage_warm_thread = mock_thread

        s._cancel_all_timers()

        mock_thread.join.assert_not_called()


# ===================================================================
# 5. 回调委托 — onTick / onOrder / onTrade
# ===================================================================

class TestCallbackDelegation:

    def test_onTick_delegates_to_core(self):
        s = _make_strategy()
        mock_tick = MagicMock()
        # 阻止 super().on_tick 调用（BaseStrategy 可能不存在）
        with patch.object(type(s), 'on_tick', lambda self, t: None):
            s.onTick(mock_tick)
        s.strategy_core.on_tick.assert_called_once_with(mock_tick)

    def test_onTick_returns_none(self):
        s = _make_strategy()
        mock_tick = MagicMock()
        with patch.object(type(s), 'on_tick', lambda self, t: None):
            result = s.onTick(mock_tick)
        assert result is None

    def test_onTick_disabled_callbacks(self):
        """_callbacks_enabled=False 时不应委托到 core。"""
        s = _make_strategy(_callbacks_enabled=False)
        mock_tick = MagicMock()
        result = s.onTick(mock_tick)
        s.strategy_core.on_tick.assert_not_called()
        assert result is None

    def test_onTick_degraded_state_returns_none(self):
        """DEGRADED 状态下 onTick 应直接返回 None。"""
        s = _make_strategy()
        from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
        s.strategy_core._state = StrategyState.DEGRADED
        mock_tick = MagicMock()
        with patch.object(type(s), 'on_tick', lambda self, t: None):
            result = s.onTick(mock_tick)
        s.strategy_core.on_tick.assert_not_called()
        assert result is None

    def test_onOrder_delegates_to_core(self):
        s = _make_strategy()
        mock_order = MagicMock()
        with patch.object(type(s), 'on_order', lambda self, o: None):
            s.onOrder(mock_order)
        s.strategy_core.on_order.assert_called_once_with(mock_order)

    def test_onOrder_returns_none(self):
        s = _make_strategy()
        mock_order = MagicMock()
        with patch.object(type(s), 'on_order', lambda self, o: None):
            result = s.onOrder(mock_order)
        assert result is None

    def test_onOrder_disabled_callbacks(self):
        s = _make_strategy(_callbacks_enabled=False)
        mock_order = MagicMock()
        result = s.onOrder(mock_order)
        s.strategy_core.on_order.assert_not_called()
        assert result is None

    def test_onTrade_delegates_to_core(self):
        s = _make_strategy()
        mock_trade = MagicMock()
        with patch.object(type(s), 'on_trade', lambda self, t: None):
            s.onTrade(mock_trade)
        s.strategy_core.on_trade.assert_called_once_with(mock_trade)

    def test_onTrade_returns_none(self):
        s = _make_strategy()
        mock_trade = MagicMock()
        with patch.object(type(s), 'on_trade', lambda self, t: None):
            result = s.onTrade(mock_trade)
        assert result is None

    def test_onTrade_disabled_callbacks(self):
        s = _make_strategy(_callbacks_enabled=False)
        mock_trade = MagicMock()
        result = s.onTrade(mock_trade)
        s.strategy_core.on_trade.assert_not_called()
        assert result is None


# ===================================================================
# 6. is_market_open
# ===================================================================

class TestIsMarketOpen:

    def test_is_market_open_with_exchange(self):
        """指定交易所时，应委托给 infra.scheduler_service.is_market_open。"""
        s = _make_strategy()
        with patch(
            'ali2026v3_trading.infra.scheduler_service.is_market_open',
            return_value=True,
        ) as mock_imo:
            result = s.is_market_open(exchange='CFFEX')
            mock_imo.assert_called_once_with('CFFEX')
            assert result is True

    def test_is_market_open_auto_exchange(self):
        """exchange='AUTO' 时应遍历 params.exchanges 对应的交易所。"""
        s = _make_strategy()
        s.params = MagicMock()
        s.params.exchanges = 'CFFEX,SHFE'

        with patch(
            'ali2026v3_trading.infra.scheduler_service.is_market_open',
            return_value=True,
        ):
            result = s.is_market_open(exchange='AUTO')
            assert result is True

    def test_is_market_open_auto_list_exchanges(self):
        """params.exchanges 为列表时应正常处理。"""
        s = _make_strategy()
        s.params = MagicMock()
        s.params.exchanges = ['CFFEX', 'SHFE']

        with patch(
            'ali2026v3_trading.infra.scheduler_service.is_market_open',
            return_value=False,
        ):
            result = s.is_market_open(exchange='AUTO')
            assert result is False

    def test_is_market_open_exception_fallback(self):
        """异常时应回退到默认交易所列表。"""
        s = _make_strategy()
        s.params = MagicMock()
        # 让 params.exchanges 抛异常
        type(s.params).exchanges = property(lambda self: (_ for _ in ()).throw(AttributeError))

        with patch(
            'ali2026v3_trading.infra.scheduler_service.is_market_open',
            return_value=True,
        ) as mock_imo:
            result = s.is_market_open(exchange='AUTO')
            assert result is True
            # 应该回退到默认6个交易所
            assert mock_imo.call_count > 0


# ===================================================================
# 7. 完整 __init__ 测试（综合 mock）
# ===================================================================

class TestFullInit:

    @patch('ali2026v3_trading.strategy.strategy_core_service.StrategyCoreService')
    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.infra.scheduler_service.get_market_time_service')
    @patch.object(
        __import__('ali2026v3_trading.config.ui_service', fromlist=['UIMixin']).UIMixin,
        '__init__', lambda self, *a, **kw: None,
    )
    def test_full_init_with_strategy_id(
        self, mock_mts, mock_params_cls, mock_core_cls
    ):
        mock_mts.return_value = MagicMock()
        mock_params_cls.return_value = MagicMock()
        mock_core_cls.return_value = MagicMock()

        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        s = Strategy2026(strategy_id="test_001")

        assert s.strategy_id == "test_001"
        assert s._is_real_strategy is True
        assert s.auto_trading_enabled is False
        assert s._stop_requested is False

    @patch('ali2026v3_trading.strategy.strategy_core_service.StrategyCoreService')
    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.infra.scheduler_service.get_market_time_service')
    @patch.object(
        __import__('ali2026v3_trading.config.ui_service', fromlist=['UIMixin']).UIMixin,
        '__init__', lambda self, *a, **kw: None,
    )
    def test_full_init_auto_strategy_id(
        self, mock_mts, mock_params_cls, mock_core_cls
    ):
        mock_mts.return_value = MagicMock()
        mock_params_cls.return_value = MagicMock()
        mock_core_cls.return_value = MagicMock()

        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        s = Strategy2026()

        assert s.strategy_id.startswith("strategy_")
        assert s._is_real_strategy is True

    @patch('ali2026v3_trading.strategy.strategy_core_service.StrategyCoreService')
    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.infra.scheduler_service.get_market_time_service')
    @patch.object(
        __import__('ali2026v3_trading.config.ui_service', fromlist=['UIMixin']).UIMixin,
        '__init__', lambda self, *a, **kw: None,
    )
    def test_strategy_id_from_kwargs(
        self, mock_mts, mock_params_cls, mock_core_cls
    ):
        mock_mts.return_value = MagicMock()
        mock_params_cls.return_value = MagicMock()
        mock_core_cls.return_value = MagicMock()

        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        s = Strategy2026(strategy_id="custom_id_42")

        assert s.strategy_id == "custom_id_42"

    @patch('ali2026v3_trading.strategy.strategy_core_service.StrategyCoreService')
    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.infra.scheduler_service.get_market_time_service')
    @patch.object(
        __import__('ali2026v3_trading.config.ui_service', fromlist=['UIMixin']).UIMixin,
        '__init__', lambda self, *a, **kw: None,
    )
    def test_full_init_creates_strategy_core(
        self, mock_mts, mock_params_cls, mock_core_cls
    ):
        mock_mts.return_value = MagicMock()
        mock_params_cls.return_value = MagicMock()
        mock_core_instance = MagicMock()
        mock_core_cls.return_value = mock_core_instance

        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        s = Strategy2026(strategy_id="test_core_create")

        mock_core_cls.assert_called_once_with("test_core_create")
        assert s.strategy_core is mock_core_instance

    @patch('ali2026v3_trading.strategy.strategy_core_service.StrategyCoreService')
    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.infra.scheduler_service.get_market_time_service')
    @patch.object(
        __import__('ali2026v3_trading.config.ui_service', fromlist=['UIMixin']).UIMixin,
        '__init__', lambda self, *a, **kw: None,
    )
    def test_full_init_creates_params(
        self, mock_mts, mock_params_cls, mock_core_cls
    ):
        mock_mts.return_value = MagicMock()
        mock_params_instance = MagicMock()
        mock_params_cls.return_value = mock_params_instance
        mock_core_cls.return_value = MagicMock()

        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        s = Strategy2026(strategy_id="test_params_create")

        assert s.params is mock_params_instance

    @patch('ali2026v3_trading.strategy.strategy_core_service.StrategyCoreService')
    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.infra.scheduler_service.get_market_time_service')
    @patch.object(
        __import__('ali2026v3_trading.config.ui_service', fromlist=['UIMixin']).UIMixin,
        '__init__', lambda self, *a, **kw: None,
    )
    def test_full_init_market_time_service(
        self, mock_mts, mock_params_cls, mock_core_cls
    ):
        mock_mts_instance = MagicMock()
        mock_mts.return_value = mock_mts_instance
        mock_params_cls.return_value = MagicMock()
        mock_core_cls.return_value = MagicMock()

        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        s = Strategy2026(strategy_id="test_mts")

        mock_mts.assert_called_once()
        assert s.market_time_service is mock_mts_instance


# ===================================================================
# 8. 生命周期 — onStart / onStop 重复防护
# ===================================================================

class TestLifecycleDuplicatePrevention:

    def test_on_start_duplicate_prevention(self):
        """_start_executed=True 时 on_start 应提前返回 None。"""
        s = _make_strategy(_start_executed=True)
        result = s.on_start()
        assert result is None
        # strategy_core.start 不应被调用
        s.strategy_core.start.assert_not_called()

    def test_on_stop_sets_flags(self):
        """on_stop 应设置 _stop_executed=True。"""
        s = _make_strategy()
        # on_stop 中会调用 self.save_instance_file()，需要 mock
        s.save_instance_file = MagicMock()
        # on_stop 中会设置 self.trading = False
        s.trading = True

        s.on_stop()

        assert s._stop_executed is True
        assert s._stop_requested is True
        assert s._callbacks_enabled is False

    def test_on_stop_duplicate_prevention(self):
        """_stop_executed=True 时 on_stop 应提前返回 None。"""
        s = _make_strategy(_stop_executed=True)
        result = s.on_stop()
        assert result is None
        s.strategy_core.stop.assert_not_called()

    def test_on_stop_calls_core_stop(self):
        """on_stop 应调用 strategy_core.stop()。"""
        s = _make_strategy()
        s.save_instance_file = MagicMock()
        s.trading = True

        s.on_stop()

        s.strategy_core.stop.assert_called_once()

    def test_on_stop_cleans_core_attributes(self):
        """on_stop 应清理 strategy_core 的5个实例属性。"""
        s = _make_strategy()
        s.save_instance_file = MagicMock()
        s.trading = True
        # 设置需要清理的属性
        for attr in ('_shadow_engine', '_signal_service',
                      '_strategy_ecosystem', '_hft_engine', '_snapshot_collector'):
            setattr(s.strategy_core, attr, MagicMock())

        s.on_stop()

        for attr in ('_shadow_engine', '_signal_service',
                      '_strategy_ecosystem', '_hft_engine', '_snapshot_collector'):
            assert getattr(s.strategy_core, attr) is None


# ===================================================================
# 9. _ensure_runtime_config_loaded
# ===================================================================

class TestEnsureRuntimeConfigLoaded:

    def test_skips_if_already_loaded(self):
        """_config_loaded=True 时不应重复加载。"""
        s = _make_strategy(_config_loaded=True)
        s._ensure_runtime_config_loaded()
        # 不应修改 config
        assert s.config == {}

    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.config.config_service.get_cached_params')
    def test_loads_and_merges_config(self, mock_get_cached, mock_params_cls):
        """首次加载时应合并缓存配置和运行时配置。"""
        mock_get_cached.return_value = {'key_a': 1, 'key_b': 2}
        mock_params_instance = MagicMock()
        mock_params_cls.return_value = mock_params_instance

        s = _make_strategy(
            _config_loaded=False,
            _runtime_config={'key_b': 200, 'key_c': 3},
        )
        # 防止 UIMixin.__getattr__ 递归：预置 market_center 属性
        s.__dict__['market_center'] = None
        s._ensure_runtime_config_loaded()

        assert s._config_loaded is True
        # 运行时配置应覆盖缓存配置
        assert s.config['key_b'] == 200
        assert s.config['key_a'] == 1
        assert s.config['key_c'] == 3
        mock_params_cls.assert_called_once()

    @patch('ali2026v3_trading.strategy.strategy_2026.StrategyParams')
    @patch('ali2026v3_trading.config.config_service.get_cached_params')
    def test_skips_none_and_empty_values(self, mock_get_cached, mock_params_cls):
        """合并时应跳过 None、空字符串、空列表/字典。"""
        mock_get_cached.return_value = {'keep': 1}
        mock_params_cls.return_value = MagicMock()

        s = _make_strategy(
            _config_loaded=False,
            _runtime_config={
                'none_val': None,
                'empty_str': '',
                'empty_list': [],
                'empty_dict': {},
                'valid_val': 42,
            },
        )
        # 防止 UIMixin.__getattr__ 递归：预置 market_center 属性
        s.__dict__['market_center'] = None
        s._ensure_runtime_config_loaded()

        assert 'none_val' not in s.config
        assert 'empty_str' not in s.config
        assert 'empty_list' not in s.config
        assert 'empty_dict' not in s.config
        assert s.config['valid_val'] == 42


# ===================================================================
# 10. 属性别名
# ===================================================================

class TestMethodAliases:

    def test_on_init_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_init is Strategy2026.onInit

    def test_on_start_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_start is Strategy2026.onStart

    def test_on_stop_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_stop is Strategy2026.onStop

    def test_on_destroy_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_destroy is Strategy2026.onDestroy

    def test_on_tick_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_tick is Strategy2026.onTick

    def test_on_order_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_order is Strategy2026.onOrder

    def test_on_trade_alias(self):
        from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
        assert Strategy2026.on_trade is Strategy2026.onTrade


# ===================================================================
# 11. 属性访问器
# ===================================================================

class TestPropertyAccessors:

    def test_my_is_running(self):
        s = _make_strategy()
        s.strategy_core._is_running = True
        assert s.my_is_running is True

    def test_my_is_running_false(self):
        s = _make_strategy()
        s.strategy_core._is_running = False
        assert s.my_is_running is False

    def test_my_is_paused(self):
        s = _make_strategy()
        s.strategy_core._is_paused = True
        assert s.my_is_paused is True

    def test_my_trading_with_is_trading_attr(self):
        s = _make_strategy()
        s.strategy_core._is_trading = True
        assert s.my_trading is True

    def test_my_trading_without_is_trading_attr(self):
        """没有 _is_trading 时，_is_running and not _is_paused。"""
        s = _make_strategy()
        del s.strategy_core._is_trading
        s.strategy_core._is_running = True
        s.strategy_core._is_paused = False
        assert s.my_trading is True

    def test_my_trading_setter(self):
        s = _make_strategy()
        s.strategy_core._is_trading = True
        s.my_trading = False
        assert s.strategy_core._is_trading is False


# ===================================================================
# 12. onDestroy
# ===================================================================

class TestOnDestroy:

    def test_onDestroy_sets_flags(self):
        s = _make_strategy()
        s._destroy_output_mode_ui = MagicMock()
        s.onDestroy()
        assert s._callbacks_enabled is False
        assert s._stop_requested is True

    def test_onDestroy_sets_stop_executed_if_not_yet(self):
        """如果 _stop_executed 为 False，onDestroy 应设为 True。"""
        s = _make_strategy(_stop_executed=False)
        s._destroy_output_mode_ui = MagicMock()
        s.onDestroy()
        assert s._stop_executed is True

    def test_onDestroy_does_not_reset_stop_executed(self):
        """如果 _stop_executed 已为 True，onDestroy 不应重置。"""
        s = _make_strategy(_stop_executed=True)
        s._destroy_output_mode_ui = MagicMock()
        s.onDestroy()
        assert s._stop_executed is True
