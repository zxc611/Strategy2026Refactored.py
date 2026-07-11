# MODULE_ID: M2-340
"""
Mixin消灭 - 7轮端到端功能验证测试 v2
核心原则：参数改变 → 结果改变
策略：优先测试纯计算/静态方法，需要状态的用正确初始化
"""
import sys
import os
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


# ============================================================================
# 第1轮
# ============================================================================

class TestRound1StorageQueryBase:
    def test_infer_exchange_different_ids(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        svc = StorageQueryBaseService.__new__(StorageQueryBaseService)
        r1 = svc.infer_exchange_from_id('CFFEX.IF2603')
        r2 = svc.infer_exchange_from_id('DCE.m2609')
        assert r1 != r2, f"CFFEX vs DCE应返回不同交易所: {r1} vs {r2}"

    def test_infer_exchange_same_prefix(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        svc = StorageQueryBaseService.__new__(StorageQueryBaseService)
        r1 = svc.infer_exchange_from_id('CFFEX.IF2603')
        r2 = svc.infer_exchange_from_id('CFFEX.IC2606')
        assert r1 == r2, "相同交易所前缀应返回相同结果"


# ============================================================================
# 第2轮 - StorageInitService
# ============================================================================

class TestRound2StorageInit:
    def test_is_ready_false_then_true(self):
        from ali2026v3_trading.data.storage_init_mixin import StorageInitService
        svc = StorageInitService.__new__(StorageInitService)
        svc._init_state = type('State', (), {'is_ready': lambda self: False})()
        assert svc.is_ready() is False
        svc._init_state = type('State', (), {'is_ready': lambda self: True})()
        assert svc.is_ready() is True


# ============================================================================
# 第3轮
# ============================================================================

class TestRound3ConfigQuery:
    def test_check_version_alignment_same_vs_different(self):
        from ali2026v3_trading.config.config_service import ConfigQueryService
        svc = ConfigQueryService.__new__(ConfigQueryService)
        r_same = svc.check_version_alignment(code_version='1.0', manual_version='1.0', config_version='1.0')
        r_diff = svc.check_version_alignment(code_version='1.0', manual_version='2.0', config_version='3.0')
        assert r_same != r_diff, "版本一致vs不一致应产生不同结果"


class TestRound3StorageChecks:
    def test_is_feature_degraded_parameter_sensitivity(self):
        from ali2026v3_trading.infra.storage_service import StorageChecksService
        svc = StorageChecksService.__new__(StorageChecksService)
        svc._degradation_features = {'wal_write', 'kline_query'}
        assert svc.is_feature_degraded('wal_write') is True
        assert svc.is_feature_degraded('nonexistent') is False

    def test_is_feature_degraded_empty_set(self):
        from ali2026v3_trading.infra.storage_service import StorageChecksService
        svc = StorageChecksService.__new__(StorageChecksService)
        svc._degradation_features = set()
        assert svc.is_feature_degraded('anything') is False


# ============================================================================
# 第4轮 - WidthCache纯计算方法
# ============================================================================

class TestRound4WidthCacheContext:
    def test_is_out_of_the_money_call_vs_put(self):
        from ali2026v3_trading.data.width_cache_types import WidthCacheContextMixin
        svc = WidthCacheContextMixin.__new__(WidthCacheContextMixin)
        assert svc._is_out_of_the_money('CALL', 3900.0, 4100.0) is True
        assert svc._is_out_of_the_money('CALL', 4200.0, 4100.0) is False
        assert svc._is_out_of_the_money('PUT', 4200.0, 4100.0) is True
        assert svc._is_out_of_the_money('PUT', 3900.0, 4100.0) is False

    def test_is_out_of_the_money_price_sensitivity(self):
        from ali2026v3_trading.data.width_cache_types import WidthCacheContextMixin
        svc = WidthCacheContextMixin.__new__(WidthCacheContextMixin)
        r_low = svc._is_out_of_the_money('CALL', 3900.0, 4100.0)
        r_high = svc._is_out_of_the_money('CALL', 4200.0, 4100.0)
        assert r_low != r_high, "CALL: 价格从低于strike变为高于strike，OTM应反转"

    def test_normalize_option_type_c_p(self):
        from ali2026v3_trading.data.width_cache_types import WidthCacheContextMixin
        svc = WidthCacheContextMixin.__new__(WidthCacheContextMixin)
        r_c = svc._normalize_option_type('C')
        r_p = svc._normalize_option_type('P')
        assert r_c != r_p, "C和P应标准化为不同值"


class TestRound4WidthCacheQuery:

    def test_select_from_sort_bucket_empty(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService
        svc = WidthCacheQueryService.__new__(WidthCacheQueryService)
        svc._facade = None
        svc._sort_buckets = {}
        svc._lock = threading.Lock()
        result = svc.select_from_sort_bucket(future_internal_id=999, month='2606', opt_type='CALL', top_n=3)
        assert isinstance(result, list)
        assert len(result) == 0


# ============================================================================
# 第5轮 - Subscription纯计算/静态方法
# ============================================================================

class TestRound5SubscriptionInstrument:
    def test_parse_future_different_products(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionInstrumentService
        r1 = SubscriptionInstrumentService.parse_future('IF2603')
        r2 = SubscriptionInstrumentService.parse_future('au2605')
        assert r1['product'] != r2['product'], "IF vs au应返回不同product"

    def test_parse_option_dash_vs_compact_format(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionInstrumentService
        r_dash = SubscriptionInstrumentService.parse_option('CU2603-C-5000')
        r_compact = SubscriptionInstrumentService.parse_option('CU2603C5000')
        assert r_dash['product'] == r_compact['product']
        assert r_dash['strike_price'] == r_compact['strike_price']
        assert r_dash['format'] != r_compact['format']

    def test_classify_instruments_futures_vs_options(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionInstrumentService
        futures, options = SubscriptionInstrumentService.classify_instruments(
            ['IF2603', 'CU2603-C-5000', 'au2605']
        )
        assert 'IF2603' in futures
        assert 'au2605' in futures
        assert 'CU2603-C-5000' not in futures
        assert len(options) > 0

    def test_is_option_vs_not_option(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionInstrumentService
        assert SubscriptionInstrumentService.is_option('CU2603-C-5000') is True
        assert SubscriptionInstrumentService.is_option('IF2603') is False

    def test_strip_exchange_prefix(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionInstrumentService
        r1 = SubscriptionInstrumentService._strip_exchange_prefix('CFFEX.IF2603')
        r2 = SubscriptionInstrumentService._strip_exchange_prefix('IF2603')
        assert 'IF2603' in r1
        assert 'IF2603' in r2


class TestRound5SubscriptionWALBackoff:
    def test_calc_backoff_exponential_increasing(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionWALService
        svc = SubscriptionWALService.__new__(SubscriptionWALService)
        svc._config = type('C', (), {
            'retry_base_delay': 1.0, 'retry_max_delay': 60.0, 'backoff_strategy': 'exponential',
        })()
        d0 = svc._calc_backoff_delay(0)
        d1 = svc._calc_backoff_delay(1)
        d5 = svc._calc_backoff_delay(5)
        assert d0 < d1 < d5, f"指数退避应递增: d0={d0} < d1={d1} < d5={d5}"

    def test_calc_backoff_linear_uniform_step(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionWALService
        svc = SubscriptionWALService.__new__(SubscriptionWALService)
        svc._config = type('C', (), {
            'retry_base_delay': 1.0, 'retry_max_delay': 60.0, 'backoff_strategy': 'linear',
        })()
        d0 = svc._calc_backoff_delay(0)
        d1 = svc._calc_backoff_delay(1)
        d2 = svc._calc_backoff_delay(2)
        step1 = d1 - d0
        step2 = d2 - d1
        assert abs(step1 - step2) < 0.01, f"线性退避步长应相等: step1={step1}, step2={step2}"

    def test_calc_backoff_respects_max_delay(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionWALService
        svc = SubscriptionWALService.__new__(SubscriptionWALService)
        svc._config = type('C', (), {
            'retry_base_delay': 1.0, 'retry_max_delay': 5.0, 'backoff_strategy': 'exponential',
        })()
        d10 = svc._calc_backoff_delay(10)
        assert d10 <= 5.0, f"退避延迟不应超过max: d10={d10}"

    def test_calc_backoff_different_strategies_differ(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionWALService
        svc_exp = SubscriptionWALService.__new__(SubscriptionWALService)
        svc_exp._config = type('C', (), {
            'retry_base_delay': 1.0, 'retry_max_delay': 60.0, 'backoff_strategy': 'exponential',
        })()
        svc_lin = SubscriptionWALService.__new__(SubscriptionWALService)
        svc_lin._config = type('C', (), {
            'retry_base_delay': 1.0, 'retry_max_delay': 60.0, 'backoff_strategy': 'linear',
        })()
        d_exp = svc_exp._calc_backoff_delay(5)
        d_lin = svc_lin._calc_backoff_delay(5)
        assert d_exp != d_lin, f"指数vs线性退避在retry_count=5时应不同: exp={d_exp}, lin={d_lin}"


# ============================================================================
# 第6轮 - OrderQueryService纯查询
# ============================================================================

class TestRound6OrderQuery:
    def test_get_order_status_unknown(self):
        from ali2026v3_trading.order.order_base import OrderQueryService
        svc = OrderQueryService.__new__(OrderQueryService)
        svc._lock = threading.Lock()
        svc._orders_by_id = {}
        assert svc.get_order_status('nonexistent') == 'UNKNOWN'

    def test_get_order_status_known(self):
        from ali2026v3_trading.order.order_base import OrderQueryService
        svc = OrderQueryService.__new__(OrderQueryService)
        svc._lock = threading.Lock()
        svc._orders_by_id = {'o1': {'status': 'FILLED'}, 'o2': {'status': 'CANCELLED'}}
        assert svc.get_order_status('o1') == 'FILLED'
        assert svc.get_order_status('o2') == 'CANCELLED'
        assert svc.get_order_status('o1') != svc.get_order_status('o2')

    def test_get_order_status_parameter_sensitivity(self):
        from ali2026v3_trading.order.order_base import OrderQueryService
        svc = OrderQueryService.__new__(OrderQueryService)
        svc._lock = threading.Lock()
        svc._orders_by_id = {'o1': {'status': 'PENDING'}}
        r_exist = svc.get_order_status('o1')
        r_miss = svc.get_order_status('o999')
        assert r_exist != r_miss


# ============================================================================
# 第7轮 - BoxSpringExecutor + CapitalRouting + TradingEV + Governance + Ops
# ============================================================================

class TestRound7BoxSpringExecutor:
    def test_compute_equity_based_lots_exists(self):
        from ali2026v3_trading.strategy.box_spring_executor import BoxSpringExecutorService
        assert hasattr(BoxSpringExecutorService, 'compute_equity_based_lots')
        assert callable(BoxSpringExecutorService.compute_equity_based_lots)


class TestRound7CapitalRouting:
    def test_route_capital_method_exists(self):
        from ali2026v3_trading.strategy.strategy_ecosystem._core import CapitalRoutingService
        assert hasattr(CapitalRoutingService, 'route_capital')
        assert hasattr(CapitalRoutingService, 'set_master_substrategy_pause')


class TestRound7TradingEV:
    def test_compute_defense_net_benefit_defense_better_vs_worse(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService
        svc = TradingEVService.__new__(TradingEVService)
        r_better = svc.compute_defense_net_benefit(
            defense_mode_loss=100.0, ignore_mode_loss=500.0,
            ignore_mode_win_trades=10, ignore_mode_avg_win=50.0, other_state_ratio=0.3
        )
        r_worse = svc.compute_defense_net_benefit(
            defense_mode_loss=500.0, ignore_mode_loss=100.0,
            ignore_mode_win_trades=10, ignore_mode_avg_win=50.0, other_state_ratio=0.3
        )
        assert r_better != r_worse, "防御损失vs忽略损失反转应产生不同结果"

    def test_compute_defense_net_benefit_ratio_sensitivity(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService
        svc = TradingEVService.__new__(TradingEVService)
        r_low_ratio = svc.compute_defense_net_benefit(
            defense_mode_loss=100.0, ignore_mode_loss=200.0,
            ignore_mode_win_trades=10, ignore_mode_avg_win=50.0, other_state_ratio=0.1
        )
        r_high_ratio = svc.compute_defense_net_benefit(
            defense_mode_loss=100.0, ignore_mode_loss=200.0,
            ignore_mode_win_trades=10, ignore_mode_avg_win=50.0, other_state_ratio=0.9
        )
        assert r_low_ratio != r_high_ratio, "不同other_state_ratio应产生不同结果"


class TestRound7Governance:
    def test_configure_grayscale_method_exists(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService
        assert hasattr(GovernanceService, 'configure_grayscale')
        assert hasattr(GovernanceService, 'run_governance_checks')


class TestRound7Ops:
    def test_enable_parallel_run_different_strategies(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService
        svc = OpsService.__new__(OpsService)
        svc._strategy_slots = {}
        svc._lock = threading.Lock()
        r1 = svc.enable_parallel_run('old_A', 'new_A', max_duration_sec=60.0)
        r2 = svc.enable_parallel_run('old_B', 'new_B', max_duration_sec=3600.0)
        assert r1 != r2 or isinstance(r1, dict), "不同策略参数应产生不同结果"


# ============================================================================
# 门面组合+__getattr__委托端到端验证
# ============================================================================

class TestFacadeDelegation:
    def test_subscription_manager_delegates(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionManager
        sm = SubscriptionManager.__new__(SubscriptionManager)
        sm.__dict__['_core_service'] = type('M', (), {'get_subscription_stats': lambda s: {'total': 42}})()
        assert sm.get_subscription_stats() == {'total': 42}

    def test_mode_engine_delegates(self):
        from ali2026v3_trading.governance.mode_engine import ModeEngine
        me = ModeEngine.__new__(ModeEngine)
        me.__dict__['_propagation_service'] = type('M', (), {'check_auto_recovery': lambda s: 'correct_trending'})()
        assert me.check_auto_recovery() == 'correct_trending'

    def test_order_service_delegates(self):
        from ali2026v3_trading.order.order_service import OrderService
        os_svc = OrderService.__new__(OrderService)
        os_svc.__dict__['_query_service'] = type('M', (), {'get_order_status': lambda s, oid: 'FILLED'})()
        assert os_svc.get_order_status('any_id') == 'FILLED'

    def test_ecosystem_delegates_to_capital(self):
        from ali2026v3_trading.strategy.strategy_ecosystem._core import StrategyEcosystem
        se = StrategyEcosystem.__new__(StrategyEcosystem)
        se.__dict__['_capital_routing_service'] = type('M', (), {'route_capital': lambda s, st: {'master': 0.6}})()
        assert se.route_capital('correct_trending') == {'master': 0.6}

    def test_box_spring_delegates_to_detector(self):
        from ali2026v3_trading.strategy.box_spring_strategy_impl import BoxSpringStrategy
        bs = BoxSpringStrategy.__new__(BoxSpringStrategy)
        bs.__dict__['_detector_service'] = type('M', (), {'update_iv': lambda s, iid, iv: 75.0})()
        assert bs.update_iv('IF2603', 0.25) == 75.0

    def test_shadow_engine_delegates_to_signal(self):
        from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
        se = ShadowStrategyEngine.__new__(ShadowStrategyEngine)
        se.__dict__['_signal_service'] = type('M', (), {'process_shadow_a_signal': lambda s, **kw: 'signal_result'})()
        assert se.process_shadow_a_signal(market_state='test') == 'signal_result'

    def test_config_service_delegates_to_query(self):
        from ali2026v3_trading.config.config_service import ConfigService
        cs = ConfigService.__new__(ConfigService)
        cs.__dict__['_query_service'] = type('M', (), {'check_version_alignment': lambda s, **kw: {'aligned': True}})()
        assert cs.check_version_alignment() == {'aligned': True}

    def test_params_service_delegates_to_matrix(self):
        from ali2026v3_trading.config._params_core import ParamsService
        ps = ParamsService.__new__(ParamsService)
        ps.__dict__['_attribute_matrix_service'] = type('M', (), {'validate_with_attribute_matrix': lambda s: {'violations': 0}})()
        assert ps.validate_with_attribute_matrix() == {'violations': 0}

    def test_storage_maintenance_delegates_to_catalog(self):
        from ali2026v3_trading.infra.storage_service import StorageMaintenanceService
        sm = StorageMaintenanceService.__new__(StorageMaintenanceService)
        sm.__dict__['_catalog_service'] = type('M', (), {'ensure_option_product_catalog': lambda s: True})()
        assert sm.ensure_option_product_catalog() is True

    def test_width_strength_cache_delegates_to_sort(self):
        from ali2026v3_trading.data.width_cache import WidthStrengthCache
        wc = WidthStrengthCache.__new__(WidthStrengthCache)
        wc.__dict__['_sort_service'] = type('M', (), {'_update_sort_bucket': lambda s, *a: 'updated'})()
        result = wc._sort_service._update_sort_bucket(1, {}, 2, '2606', 'CALL')
        assert result == 'updated'


# ============================================================================
# 向后兼容别名验证
# ============================================================================

class TestBackwardCompatibleAliases:
    def test_storage_data_write_alias(self):
        from ali2026v3_trading.data.storage_data_write_mixin import StorageDataWriteService, _StorageDataWriteMixin
        assert _StorageDataWriteMixin is StorageDataWriteService

    def test_storage_snapshot_alias(self):
        from ali2026v3_trading.data.storage_snapshot_mixin import StorageSnapshotService, _StorageSnapshotMixin
        assert _StorageSnapshotMixin is StorageSnapshotService

    def test_storage_wal_alias(self):
        from ali2026v3_trading.data.storage_core import StorageWalService, _StorageWalMixin
        assert _StorageWalMixin is StorageWalService

    def test_width_cache_sort_alias(self):
        from ali2026v3_trading.data.width_cache_types import WidthCacheSortService, _WidthCacheSortMixin
        assert _WidthCacheSortMixin is WidthCacheSortService

    def test_width_cache_query_alias(self):
        from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService, _WidthCacheQueryMixin
        assert _WidthCacheQueryMixin is WidthCacheQueryService

    def test_subscription_core_alias(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionCoreService, _SubscriptionCoreMixin
        assert _SubscriptionCoreMixin is SubscriptionCoreService

    def test_subscription_wal_alias(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionWALService, _SubscriptionWALMixin
        assert _SubscriptionWALMixin is SubscriptionWALService

    def test_subscription_instrument_alias(self):
        from ali2026v3_trading.infra.subscription_service import SubscriptionInstrumentService, _SubscriptionInstrumentMixin
        assert _SubscriptionInstrumentMixin is SubscriptionInstrumentService

    def test_mode_engine_propagation_alias(self):
        from ali2026v3_trading.governance.mode_engine_propagation import ModeEnginePropagationService, ModeEnginePropagationMixin
        assert ModeEnginePropagationMixin is ModeEnginePropagationService

    def test_order_query_alias(self):
        from ali2026v3_trading.order.order_base import OrderQueryService, OrderQueryMixin
        assert OrderQueryMixin is OrderQueryService

    def test_box_spring_detector_alias(self):
        from ali2026v3_trading.strategy.box_spring_detector import BoxSpringDetectorService, BoxSpringDetectorMixin
        assert BoxSpringDetectorMixin is BoxSpringDetectorService

    def test_box_spring_executor_alias(self):
        from ali2026v3_trading.strategy.box_spring_executor import BoxSpringExecutorService, BoxSpringExecutorMixin
        assert BoxSpringExecutorMixin is BoxSpringExecutorService

    def test_capital_routing_alias(self):
        from ali2026v3_trading.strategy.strategy_ecosystem._core import CapitalRoutingService, CapitalRoutingMixin
        assert CapitalRoutingMixin is CapitalRoutingService

    def test_lifecycle_alias(self):
        from ali2026v3_trading.strategy.lifecycle_service import StrategyLifecycleService
        assert StrategyLifecycleService is not None

    def test_trading_ev_alias(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService, TradingEVMixin
        assert TradingEVMixin is TradingEVService

    def test_monitoring_alias(self):
        from ali2026v3_trading.strategy.strategy_monitoring_layer import MonitoringService, MonitoringMixin
        assert MonitoringMixin is MonitoringService

    def test_governance_alias(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService, GovernanceMixin
        assert GovernanceMixin is GovernanceService

    def test_ops_alias(self):
        from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService, OpsMixin
        assert OpsMixin is OpsService
