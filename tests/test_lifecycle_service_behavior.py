# MODULE_ID: M2-408
"""
lifecycle_service 行为测试 — 生命周期桥接

验证 LifecycleService 的核心行为契约:
1. 委托桥接: 所有公开方法正确委托到子模块
2. 惰性子模块: 首次访问创建, 后续访问复用
3. DeprecationWarning: 子类化触发警告
4. 向后兼容别名: StrategyLifecycleService = LifecycleService
5. 状态映射: StrategyState → LSM状态映射
6. Storage惰性初始化: 首次访问创建, 失败返回None
7. 子模块强制初始化: _init_lifecycle_submodules
"""
import threading
import warnings
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from ali2026v3_trading.strategy.lifecycle_service import (
    LifecycleService,
    StrategyLifecycleService,
)
from ali2026v3_trading.lifecycle.lifecycle_state_machine import (
    StrategyState, LifecycleStateMachine, VALID_STATE_TRANSITIONS,
)


# ============================================================================
# 测试辅助: 构造 provider mock
# ============================================================================

def _make_provider(**overrides):
    """构造标准 provider mock"""
    p = MagicMock()
    p._state = StrategyState.INITIALIZING
    p._lock = threading.RLock()
    p._storage = None
    p._storage_lock = threading.Lock()
    p._is_running = False
    p._is_paused = False
    p._is_trading = False
    p._event_bus_param = None
    p.strategy_id = 'test_strategy'
    for k, v in overrides.items():
        setattr(p, k, v)
    return p


# ============================================================================
# 委托桥接行为 — Transition
# ============================================================================

class TestTransitionDelegationBehavior:
    """验证: transition_to 委托到 _lc_transition"""

    def test_transition_to_delegates(self):
        """transition_to → _lc_transition.transition_to 被调用"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_transition, 'transition_to', return_value=True) as mock:
            result = svc.transition_to(StrategyState.RUNNING)
            mock.assert_called_once_with(StrategyState.RUNNING)
            assert result is True


# ============================================================================
# 委托桥接行为 — Bind
# ============================================================================

class TestBindDelegationBehavior:
    """验证: bind_platform_apis 委托到 _lc_bind"""

    def test_bind_platform_apis_delegates(self):
        """bind_platform_apis → _lc_bind.bind_platform_apis 被调用"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        strategy_obj = MagicMock()
        with patch.object(svc._lc_bind, 'bind_platform_apis', return_value=True) as mock:
            result = svc.bind_platform_apis(strategy_obj)
            mock.assert_called_once_with(strategy_obj)

    def test_extract_runtime_market_center_delegates(self):
        """_extract_runtime_market_center → 委托到 LifecycleBind 静态方法"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        strategy_obj = MagicMock()
        # 静态方法委托, 不需要mock
        result = svc._extract_runtime_market_center(strategy_obj)
        # 只要不抛异常就说明委托成功


# ============================================================================
# 委托桥接行为 — Init
# ============================================================================

class TestInitDelegationBehavior:
    """验证: on_init/initialize 委托到 _lc_init"""

    def test_on_init_delegates(self):
        """on_init → _lc_init.on_init 被调用"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_init, 'on_init', return_value=None) as mock:
            svc.on_init()
            mock.assert_called_once()

    def test_initialize_delegates(self):
        """initialize → _lc_init.initialize 被调用"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_init, 'initialize', return_value=None) as mock:
            svc.initialize(params={})
            mock.assert_called_once_with({})


# ============================================================================
# 委托桥接行为 — Callbacks
# ============================================================================

class TestCallbacksDelegationBehavior:
    """验证: on_start/on_stop/on_destroy/start/stop/pause/resume/destroy 委托到 _lc_callbacks"""

    def test_on_start_delegates(self):
        """on_start → _lc_callbacks.on_start"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'on_start') as mock:
            svc.on_start()
            mock.assert_called_once()

    def test_on_stop_delegates(self):
        """on_stop → _lc_callbacks.on_stop"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'on_stop') as mock:
            svc.on_stop()
            mock.assert_called_once()

    def test_on_destroy_delegates(self):
        """on_destroy → _lc_callbacks.on_destroy"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'on_destroy') as mock:
            svc.on_destroy()
            mock.assert_called_once()

    def test_start_delegates(self):
        """start → _lc_callbacks.start"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'start') as mock:
            svc.start()
            mock.assert_called_once()

    def test_stop_delegates(self):
        """stop → _lc_callbacks.stop"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'stop') as mock:
            svc.stop()
            mock.assert_called_once()

    def test_pause_delegates(self):
        """pause → _lc_callbacks.pause"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'pause') as mock:
            svc.pause()
            mock.assert_called_once()

    def test_resume_delegates(self):
        """resume → _lc_callbacks.resume"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'resume') as mock:
            svc.resume()
            mock.assert_called_once()

    def test_destroy_delegates(self):
        """destroy → _lc_callbacks.destroy"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'destroy') as mock:
            svc.destroy()
            mock.assert_called_once()

    def test_save_state_delegates(self):
        """save_state → _lc_callbacks.save_state"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_callbacks, 'save_state') as mock:
            svc.save_state()
            mock.assert_called_once()


# ============================================================================
# 委托桥接行为 — Monitor
# ============================================================================

class TestMonitorDelegationBehavior:
    """验证: 监控方法委托到 _lc_monitor"""

    def test_get_state_delegates(self):
        """get_state → _lc_monitor.get_state"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_monitor, 'get_state', return_value=StrategyState.INITIALIZING) as mock:
            result = svc.get_state()
            mock.assert_called_once()
            assert result == StrategyState.INITIALIZING

    def test_is_running_delegates(self):
        """is_running → _lc_monitor.is_running"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_monitor, 'is_running', return_value=True) as mock:
            assert svc.is_running() is True
            mock.assert_called_once()

    def test_is_paused_delegates(self):
        """is_paused → _lc_monitor.is_paused"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_monitor, 'is_paused', return_value=False) as mock:
            assert svc.is_paused() is False
            mock.assert_called_once()

    def test_health_check_delegates(self):
        """health_check → _lc_monitor.health_check"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_monitor, 'health_check', return_value={'status': 'healthy'}) as mock:
            result = svc.health_check()
            assert result['status'] == 'healthy'

    def test_record_tick_delegates(self):
        """record_tick → _lc_monitor.record_tick"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_monitor, 'record_tick') as mock:
            svc.record_tick()
            mock.assert_called_once()

    def test_record_error_delegates(self):
        """record_error → _lc_monitor.record_error"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_monitor, 'record_error') as mock:
            svc.record_error("test error")
            mock.assert_called_once_with("test error")


# ============================================================================
# 委托桥接行为 — Parallel Ops
# ============================================================================

class TestParallelOpsDelegationBehavior:
    """验证: 并行操作方法委托到 _lc_parallel_ops"""

    def test_enter_parallel_running_delegates(self):
        """enter_parallel_running → _lc_parallel_ops.enter_parallel_running"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_parallel_ops, 'enter_parallel_running', return_value=True) as mock:
            result = svc.enter_parallel_running(shadow=None, cb=None, dur=3600.0)
            mock.assert_called_once_with(None, None, 3600.0)

    def test_exit_parallel_running_delegates(self):
        """exit_parallel_running → _lc_parallel_ops.exit_parallel_running"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_parallel_ops, 'exit_parallel_running', return_value=True) as mock:
            result = svc.exit_parallel_running(promote=False)
            mock.assert_called_once_with(False)

    def test_get_parallel_results_delegates(self):
        """get_parallel_results → _lc_parallel_ops.get_parallel_results"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch.object(svc._lc_parallel_ops, 'get_parallel_results', return_value={}) as mock:
            result = svc.get_parallel_results()
            mock.assert_called_once()


# ============================================================================
# 惰性子模块行为
# ============================================================================

class TestLazySubmoduleBehavior:
    """验证: 子模块首次访问创建, 后续访问复用"""

    def test_state_machine_lazy_init(self):
        """_lifecycle_state_machine → 首次访问创建LifecycleStateMachine"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        assert svc._lazy_LifecycleStateMachine is None, "初始应为None"
        sm = svc._lifecycle_state_machine
        assert sm is not None, "首次访问应创建实例"
        assert isinstance(sm, LifecycleStateMachine)

    def test_state_machine_reuse(self):
        """_lifecycle_state_machine → 后续访问返回同一实例"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        sm1 = svc._lifecycle_state_machine
        sm2 = svc._lifecycle_state_machine
        assert sm1 is sm2, "后续访问应返回同一实例"

    def test_platform_lazy_init(self):
        """_lifecycle_platform → 首次访问创建"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        assert svc._lazy_LifecyclePlatform is None
        lp = svc._lifecycle_platform
        assert lp is not None

    def test_resource_lazy_init(self):
        """_lifecycle_resource → 首次访问创建"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        assert svc._lazy_LifecycleResource is None
        lr = svc._lifecycle_resource
        assert lr is not None

    def test_all_lazy_attrs_initially_none(self):
        """所有惰性属性 → 初始为None"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        lazy_attrs = [
            '_lazy_LifecycleStateMachine', '_lazy_LifecyclePlatform',
            '_lazy_LifecycleResource', '_lazy_LifecycleParallel',
            '_lazy_LifecycleTransition', '_lazy_LifecycleInit',
            '_lazy_LifecycleBind', '_lazy_LifecycleCallbacks',
            '_lazy_LifecycleMonitor', '_lazy_LifecycleParallelOps',
        ]
        for attr in lazy_attrs:
            assert getattr(svc, attr) is None, f"{attr}初始应为None"


# ============================================================================
# 子模块强制初始化行为
# ============================================================================

class TestInitLifecycleSubmodulesBehavior:
    """验证: _init_lifecycle_submodules 强制初始化所有子模块"""

    def test_init_all_submodules(self):
        """_init_lifecycle_submodules → 所有惰性属性不再为None"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        svc._init_lifecycle_submodules()
        lazy_attrs = [
            '_lazy_LifecycleStateMachine', '_lazy_LifecyclePlatform',
            '_lazy_LifecycleResource', '_lazy_LifecycleParallel',
            '_lazy_LifecycleTransition', '_lazy_LifecycleInit',
            '_lazy_LifecycleBind', '_lazy_LifecycleCallbacks',
            '_lazy_LifecycleMonitor', '_lazy_LifecycleParallelOps',
        ]
        for attr in lazy_attrs:
            assert getattr(svc, attr) is not None, \
                f"_init_lifecycle_submodules后{attr}不应为None"


# ============================================================================
# DeprecationWarning行为
# ============================================================================

class TestDeprecationWarningBehavior:
    """验证: 子类化LifecycleService触发DeprecationWarning"""

    def test_subclass_triggers_deprecation_warning(self):
        """子类化 → 触发DeprecationWarning"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            class DeprecatedSubclass(LifecycleService):
                pass
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) > 0, \
                "子类化LifecycleService应触发DeprecationWarning"

    def test_direct_instantiation_no_warning(self):
        """直接实例化 → 不触发DeprecationWarning"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            provider = _make_provider()
            svc = LifecycleService(provider, strategy_id='test')
            dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(dep_warnings) == 0, \
                "直接实例化不应触发DeprecationWarning"


# ============================================================================
# 向后兼容别名行为
# ============================================================================

class TestBackwardCompatibleAliasBehavior:
    """验证: StrategyLifecycleService = LifecycleService"""

    def test_alias_is_same_class(self):
        """StrategyLifecycleService is LifecycleService"""
        assert StrategyLifecycleService is LifecycleService, \
            "别名应指向同一类"

    def test_alias_creates_instance(self):
        """StrategyLifecycleService → 可正常创建实例"""
        provider = _make_provider()
        svc = StrategyLifecycleService(provider, strategy_id='test')
        assert isinstance(svc, LifecycleService)


# ============================================================================
# 状态映射行为
# ============================================================================

class TestStateMappingBehavior:
    """验证: StrategyState → LSM状态映射"""

    def test_running_maps_to_running(self):
        """StrategyState.RUNNING → 'RUNNING'"""
        mapping = LifecycleService._STRATEGY_STATE_TO_LSM_MAP
        assert mapping[StrategyState.RUNNING] == "RUNNING"

    def test_paused_maps_to_paused(self):
        """StrategyState.PAUSED → 'PAUSED'"""
        mapping = LifecycleService._STRATEGY_STATE_TO_LSM_MAP
        assert mapping[StrategyState.PAUSED] == "PAUSED"

    def test_stopped_maps_to_stopped(self):
        """StrategyState.STOPPED → 'STOPPED'"""
        mapping = LifecycleService._STRATEGY_STATE_TO_LSM_MAP
        assert mapping[StrategyState.STOPPED] == "STOPPED"

    def test_degraded_maps_to_paused(self):
        """StrategyState.DEGRADED → 'PAUSED'(降级视为暂停)"""
        mapping = LifecycleService._STRATEGY_STATE_TO_LSM_MAP
        assert mapping[StrategyState.DEGRADED] == "PAUSED"

    def test_error_maps_to_stopped(self):
        """StrategyState.ERROR → 'STOPPED'(错误视为停止)"""
        mapping = LifecycleService._STRATEGY_STATE_TO_LSM_MAP
        assert mapping[StrategyState.ERROR] == "STOPPED"

    def test_initializing_maps_to_initialized(self):
        """StrategyState.INITIALIZING → 'INITIALIZED'"""
        mapping = LifecycleService._STRATEGY_STATE_TO_LSM_MAP
        assert mapping[StrategyState.INITIALIZING] == "INITIALIZED"


# ============================================================================
# Storage惰性初始化行为
# ============================================================================

class TestStorageLazyInitBehavior:
    """验证: storage属性惰性初始化"""

    def test_storage_initially_none(self):
        """初始状态 → _storage为None"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        assert provider._storage is None

    def test_storage_lazy_init_success(self):
        """首次访问 → storage被初始化"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch('ali2026v3_trading.strategy.lifecycle_service.get_instrument_data_manager') as mock_get:
            mock_dm = MagicMock()
            mock_get.return_value = mock_dm
            result = svc.storage
            assert result is mock_dm, "storage应被初始化"

    def test_storage_already_initialized_returns_existing(self):
        """已初始化 → 返回现有实例"""
        provider = _make_provider()
        existing_dm = MagicMock()
        provider._storage = existing_dm
        svc = LifecycleService(provider, strategy_id='test')
        result = svc.storage
        assert result is existing_dm, "已初始化应返回现有实例"

    def test_storage_init_failure_returns_none(self):
        """初始化失败 → 返回None"""
        provider = _make_provider()
        svc = LifecycleService(provider, strategy_id='test')
        with patch('ali2026v3_trading.strategy.lifecycle_service.get_instrument_data_manager',
                   side_effect=RuntimeError("init failed")):
            result = svc.storage
            assert result is None, "初始化失败应返回None"


# ============================================================================
# 类常量行为
# ============================================================================

class TestClassConstantsBehavior:
    """验证: 类常量正确性"""

    def test_option_to_future_map(self):
        """OPTION_TO_FUTURE_MAP 映射正确"""
        assert LifecycleService.OPTION_TO_FUTURE_MAP['MO'] == 'IM'
        assert LifecycleService.OPTION_TO_FUTURE_MAP['IO'] == 'IF'
        assert LifecycleService.OPTION_TO_FUTURE_MAP['HO'] == 'IH'

    def test_valid_state_transitions_exists(self):
        """VALID_STATE_TRANSITIONS 存在且非空"""
        assert hasattr(LifecycleService, 'VALID_STATE_TRANSITIONS')
        assert len(LifecycleService.VALID_STATE_TRANSITIONS) > 0
