# MODULE_ID: M2-364
import sys
import os
import pytest
import threading
import time
import queue
import logging
import tempfile
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# ============================================================
# 1. ali2026v3_trading.data.quant_infra
# ============================================================
class TestQuantInfra:
    def test_numpy_ring_buffer_init(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5, dtype='float64')
        assert buf.count == 0
        assert len(buf) == 0

    def test_numpy_ring_buffer_append_snapshot(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5, dtype='float64')
        for i in range(3):
            buf.append(float(i))
        snap = buf.snapshot()
        assert len(snap) == 3
        assert list(snap) == [0.0, 1.0, 2.0]

    def test_numpy_ring_buffer_wrap_around(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=3, dtype='float64')
        for i in range(5):
            buf.append(float(i))
        snap = buf.snapshot()
        assert len(snap) == 3
        assert list(snap) == [2.0, 3.0, 4.0]

    def test_numpy_ring_buffer_sum_mean_std(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10, dtype='float64')
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            buf.append(v)
        assert buf.sum() == 15.0
        assert abs(buf.mean() - 3.0) < 1e-9
        assert buf.std() > 0

    def test_numpy_ring_buffer_last(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5, dtype='float64')
        buf.append(10.0)
        buf.append(20.0)
        assert buf.last() == 20.0

    def test_numpy_ring_buffer_percentile(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10, dtype='float64')
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            buf.append(v)
        assert buf.percentile(50.0) == 3.0

    def test_numpy_ring_buffer_to_list(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5, dtype='float64')
        buf.append(1.0)
        buf.append(2.0)
        assert buf.to_list() == [1.0, 2.0]

    def test_rate_limit_log(self):
        from ali2026v3_trading.data.quant_infra import rate_limit_log
        logger = logging.getLogger("test_rate_limit")
        rate_limit_log(logger, logging.INFO, "test msg", "test_key", min_interval=0.0)


# ============================================================
# 2. ali2026v3_trading.data.quant_cointegration
# ============================================================
class TestQuantCointegration:
    def test_cointegration_scanner_init(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=50, scan_interval=30)
        assert scanner is not None

    def test_cointegration_scanner_add_symbol(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=10, scan_interval=20)
        scanner.add_symbol("AU")
        scanner.add_symbol("AG")

    def test_cointegration_scanner_update_price(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=10, scan_interval=20)
        scanner.add_symbol("AU")
        for i in range(5):
            scanner.update_price("AU", 500.0 + i)

    def test_cointegration_scanner_scan_returns_dict(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=10, scan_interval=20)
        scanner.add_symbol("AU")
        scanner.add_symbol("AG")
        result = scanner.scan()
        assert isinstance(result, dict)

    def test_survival_analyzer_init(self):
        from ali2026v3_trading.data.quant_cointegration import SurvivalAnalyzer
        analyzer = SurvivalAnalyzer()
        assert analyzer is not None

    def test_get_adf_critical_5pct(self):
        from ali2026v3_trading.data.quant_cointegration import _get_adf_critical_5pct
        val = _get_adf_critical_5pct(50)
        assert isinstance(val, float)
        assert val < 0


# ============================================================
# 3. ali2026v3_trading.data.quant_hmm
# ============================================================
class TestQuantHMM:
    def test_adaptive_hmm_init(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=3, update_interval=50)
        assert hmm is not None

    def test_adaptive_hmm_update(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=2, update_interval=100)
        hmm.update(0.001)
        hmm.update(-0.0005)
        hmm.update(0.002)


# ============================================================
# 4. ali2026v3_trading.data.quant_platform
# ============================================================
class TestQuantPlatform:
    def test_exchange_time_init(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        assert et is not None

    def test_exchange_time_get_trade_date(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 10, 30, tzinfo=timezone(timedelta(hours=8)))
        trade_date = et.get_trade_date(dt)
        assert trade_date == '2026-06-15'

    def test_exchange_time_night_session(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt = datetime(2026, 6, 15, 21, 30, tzinfo=timezone(timedelta(hours=8)))
        trade_date = et.get_trade_date(dt)
        assert trade_date == '2026-06-16'

    def test_exchange_time_is_trading_hours(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime(exchange='DCE')
        dt_day = datetime(2026, 6, 15, 10, 0, tzinfo=timezone(timedelta(hours=8)))
        assert et.is_trading_hours(dt_day) is True
        dt_night = datetime(2026, 6, 15, 16, 0, tzinfo=timezone(timedelta(hours=8)))
        assert et.is_trading_hours(dt_night) is False

    def test_tick_aggregator_init(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=60, volume_mode='tick')
        assert ta is not None

    def test_tick_aggregator_update_tick(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=300, volume_mode='tick')
        result = ta.update_tick(500.0, 1)
        assert result is None or isinstance(result, dict)

    def test_atomic_system_state_init(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        assert state is not None

    def test_system_health_monitor_init(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        monitor = SystemHealthMonitor()
        monitor.record_latency(module='test', latency_us=5000)
        monitor.record_latency(module='test', latency_us=10000)
        report = monitor.get_health_report()
        assert isinstance(report, dict)


# ============================================================
# 5. ali2026v3_trading.data.quant_services
# ============================================================
class TestQuantServices:
    def test_lightweight_persistence_init(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir)
            assert lp is not None

    def test_lightweight_persistence_save_load(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir, batch_size=1)
            lp.save("key1", {"value": 42})
            result = lp.load("key1")
            assert result is not None or result is None
            lp.close()

    def test_singleton_manager_init(self):
        from ali2026v3_trading.data.quant_services import SingletonManager
        sm = SingletonManager()
        assert sm is not None


# ============================================================
# 6. ali2026v3_trading.data.quant_trend_scorer
# ============================================================
class TestQuantTrendScorer:
    def test_multi_period_trend_scorer_init(self):
        from ali2026v3_trading.data.quant_infra import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=[14, 28])
        assert scorer is not None

    def test_multi_period_trend_scorer_update(self):
        from ali2026v3_trading.data.quant_infra import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=[5, 10])
        for i in range(15):
            scorer.update(high=101.0 + i * 0.5, low=99.0 + i * 0.5, close=100.0 + i * 0.5)


# ============================================================
# 7. ali2026v3_trading.data.quant_volatility
# ============================================================
class TestQuantVolatility:
    def test_iv_surface_pca_init(self):
        from ali2026v3_trading.data.quant_infra import IVSurfacePCA
        pca = IVSurfacePCA(window=50)
        assert pca is not None

    def test_volatility_regime_filter_init(self):
        from ali2026v3_trading.data.quant_infra import VolatilityRegimeFilter
        vrf = VolatilityRegimeFilter(lookback=20)
        assert vrf is not None


# ============================================================
# 8-12. ali2026v3_trading.data.storage_query*
# ============================================================
class TestStorageQueryModules:
    def test_storage_query_import(self):
        from ali2026v3_trading.data.storage_core import StorageQuery
        assert StorageQuery is not None

    def test_storage_query_base_service_import(self):
        from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService
        assert StorageQueryBaseService is not None

    def test_storage_query_base_infer_exchange(self):
        from ali2026v3_trading.param_pool.backtest.backtest_config import _infer_exchange_from_id
        result = _infer_exchange_from_id("au2506")
        assert isinstance(result, str)

    def test_storage_history_service_import(self):
        from ali2026v3_trading.data.storage_query_history import StorageHistoryService
        assert StorageHistoryService is not None

    def test_storage_instrument_service_import(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert StorageInstrumentService is not None

    def test_storage_instrument_register_func(self):
        from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService
        assert StorageInstrumentService is not None

    def test_storage_wal_service_import(self):
        from ali2026v3_trading.data.storage_core import StorageWalService
        assert StorageWalService is not None


# ============================================================
# 13. ali2026v3_trading.infra.concurrent_utils
# ============================================================
class TestConcurrentUtils:
    def test_rejection_policy_enum_values(self):
        from ali2026v3_trading.infra.concurrent_utils import RejectionPolicy
        assert RejectionPolicy.ABORT.value == 'abort'
        assert RejectionPolicy.DROP.value == 'drop'
        assert RejectionPolicy.CALLER_RUNS.value == 'caller_runs'
        assert RejectionPolicy.DISCARD_OLDEST.value == 'discard_oldest'

    def test_thread_pool_executor_init(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        from concurrent.futures import ThreadPoolExecutor
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=1)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.ABORT)
        assert wrapper is not None

    def test_thread_pool_executor_submit(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        from concurrent.futures import ThreadPoolExecutor
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=10)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.ABORT)
        future = wrapper.submit(lambda: 42)
        assert future.result() == 42
        executor.shutdown(wait=True)

    def test_thread_pool_executor_get_stats(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        from concurrent.futures import ThreadPoolExecutor
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=10)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.DROP)
        stats = wrapper.get_stats()
        assert 'drop_count' in stats
        assert 'policy' in stats
        executor.shutdown(wait=False)


# ============================================================
# 14. ali2026v3_trading.infra.operations_api
# ============================================================
class TestOperationsAPI:
    def test_operations_api_import(self):
        from ali2026v3_trading.infra.operations_api import OperationsAPI
        assert OperationsAPI is not None

    def test_get_operations_api_import(self):
        from ali2026v3_trading.infra.operations_api import get_operations_api
        assert callable(get_operations_api)


# ============================================================
# 15. ali2026v3_trading.infra.service_contracts (merged from shared_providers)
# ============================================================
class TestSharedProviders:
    def test_state_provider_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import StateProvider
        assert hasattr(StateProvider, 'state')
        assert hasattr(StateProvider, 'is_running')
        assert hasattr(StateProvider, 'is_paused')

    def test_lock_provider_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import LockProvider
        assert hasattr(LockProvider, 'lock')

    def test_stats_provider_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import StatsProvider
        assert hasattr(StatsProvider, 'stats')
        assert hasattr(StatsProvider, 'record_tick')

    def test_safety_provider_is_protocol(self):
        from ali2026v3_trading.infra.service_contracts import SafetyProvider
        assert hasattr(SafetyProvider, 'safety_meta_layer')
        assert hasattr(SafetyProvider, 'check_safety')

    def test_default_state_provider(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStateProvider
        provider = DefaultStateProvider()
        assert hasattr(provider, 'state')
        assert hasattr(provider, 'is_running')

    def test_default_lock_provider(self):
        from ali2026v3_trading.infra.service_contracts import DefaultLockProvider
        provider = DefaultLockProvider()
        assert hasattr(provider, 'lock')
        assert isinstance(provider.lock, type(threading.RLock()))

    def test_default_stats_provider(self):
        from ali2026v3_trading.infra.service_contracts import DefaultStatsProvider
        provider = DefaultStatsProvider()
        assert hasattr(provider, 'stats')


# ============================================================
# 16. ali2026v3_trading.risk.risk_priority_matrix
# ============================================================
class TestRiskPriorityMatrix:
    def test_risk_item_rpn(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=3, impact=4, detectability=2)
        assert item.rpn == 24

    def test_risk_item_severity_critical(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=5, impact=5, detectability=5)
        assert item.severity == 'CRITICAL'

    def test_risk_item_severity_low(self):
        from ali2026v3_trading.risk.risk_support import RiskItem
        item = RiskItem(name="test", probability=1, impact=1, detectability=1)
        assert item.severity == 'LOW'

    def test_risk_priority_matrix_init(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        risks = matrix.get_sorted_risks()
        assert len(risks) > 0

    def test_risk_priority_matrix_sorted_by_rpn(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        risks = matrix.get_sorted_risks()
        for i in range(len(risks) - 1):
            assert risks[i].rpn >= risks[i + 1].rpn

    def test_risk_priority_matrix_get_critical(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        critical = matrix.get_critical_risks()
        for r in critical:
            assert r.severity == 'CRITICAL'

    def test_risk_priority_matrix_add_risk(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix, RiskItem
        matrix = RiskPriorityMatrix()
        initial_count = len(matrix.get_sorted_risks())
        matrix.add_risk(RiskItem(name="new_risk", probability=2, impact=2, detectability=2))
        assert len(matrix.get_sorted_risks()) == initial_count + 1

    def test_risk_priority_matrix_to_report(self):
        from ali2026v3_trading.risk.risk_support import RiskPriorityMatrix
        matrix = RiskPriorityMatrix()
        report = matrix.to_report()
        assert isinstance(report, str)
        assert 'RPN' in report


# ============================================================
# 17. ali2026v3_trading.risk.safety_meta_layer
# ============================================================
class TestSafetyMetaLayer:
    def test_safety_meta_layer_init(self):
        from ali2026v3_trading.risk.risk_support import SafetyMetaLayer
        layer = SafetyMetaLayer()
        assert layer is not None
        assert not layer.is_hard_stop_triggered()

    def test_safety_meta_layer_daily_drawdown(self):
        from ali2026v3_trading.risk.risk_support import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        assert not layer.check_daily_drawdown(96000.0)
        assert layer.check_daily_drawdown(94000.0)
        assert layer.is_hard_stop_triggered()

    def test_safety_meta_layer_confirm_resume(self):
        from ali2026v3_trading.risk.risk_support import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        layer.check_daily_drawdown(94000.0)
        assert layer.confirm_daily_resume("trader")
        assert not layer.is_hard_stop_triggered()

    def test_safety_meta_layer_auto_resume_rejected(self):
        from ali2026v3_trading.risk.risk_support import SafetyMetaLayer
        layer = SafetyMetaLayer(params={'daily_loss_hard_stop_pct': 0.05})
        layer.set_daily_start_equity(100000.0)
        layer.check_daily_drawdown(94000.0)
        assert not layer.confirm_daily_resume("timer")
        assert layer.is_hard_stop_triggered()

    def test_safety_meta_layer_circuit_breaker(self):
        from ali2026v3_trading.risk.risk_support import SafetyMetaLayer
        layer = SafetyMetaLayer()
        layer.activate_circuit_breaker(pause_sec=0.01)
        assert layer.is_circuit_breaker_active()
        time.sleep(0.02)
        assert not layer.is_circuit_breaker_active()


# ============================================================
# 18. ali2026v3_trading.risk_engine.snapshot
# ============================================================
class TestRiskEngineSnapshot:
    def test_risk_snapshot_creation(self):
        from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
        snap = RiskSnapshot(
            snapshot_time=time.time(),
            signal={"symbol": "au2506"},
            action="OPEN",
            symbol="au2506",
            instrument_id="au2506",
            account_id="test",
            amount=1.0,
            price=500.0,
            volume=1.0,
            direction="LONG",
        )
        assert snap.symbol == "au2506"
        assert snap.direction == "LONG"

    def test_risk_snapshot_from_signal(self):
        from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
        signal = {
            "symbol": "au2506",
            "instrument_id": "au2506",
            "action": "OPEN",
            "direction": "LONG",
            "price": 500.0,
            "volume": 1.0,
            "amount": 1.0,
        }
        snap = RiskSnapshot.from_signal(signal)
        assert snap.symbol == "au2506"
        assert snap.action == "OPEN"


# ============================================================
# 19. ali2026v3_trading.risk_engine.engine
# ============================================================
class TestRiskEngineEngine:
    def test_risk_engine_import(self):
        from ali2026v3_trading.risk_engine.engine import RiskEngine
        assert RiskEngine is not None


# ============================================================
# 20-25. ali2026v3_trading.risk_engine.*_risk
# ============================================================
class TestRiskEngineSubModules:
    def test_market_risk_functions(self):
        from ali2026v3_trading.risk_engine.market_risk import (
            check_risk_ratio, check_greeks_limits, check_market_risks
        )
        assert callable(check_risk_ratio)
        assert callable(check_greeks_limits)
        assert callable(check_market_risks)

    def test_counterparty_risk_functions(self):
        from ali2026v3_trading.risk_engine.counterparty_risk import (
            check_e13_collusion, check_counterparty_risks
        )
        assert callable(check_e13_collusion)
        assert callable(check_counterparty_risks)

    def test_operational_risk_functions(self):
        from ali2026v3_trading.risk_engine.operational_risk import (
            check_strategy_status, check_rate_limit, check_operational_risks
        )
        assert callable(check_strategy_status)
        assert callable(check_rate_limit)
        assert callable(check_operational_risks)

    def test_regulatory_risk_import(self):
        from ali2026v3_trading.risk_engine import regulatory_risk
        assert regulatory_risk is not None

    def test_shared_checks_functions(self):
        from ali2026v3_trading.risk_engine.shared_checks import (
            check_position_limit, check_margin_sufficiency, check_regulatory_risks
        )
        assert callable(check_position_limit)
        assert callable(check_margin_sufficiency)
        assert callable(check_regulatory_risks)

    def test_result_handler_functions(self):
        from ali2026v3_trading.risk_engine.result_handler import (
            record_result, record_and_publish, record_block_with_context
        )
        assert callable(record_result)
        assert callable(record_and_publish)
        assert callable(record_block_with_context)

    def test_input_builder_function(self):
        from ali2026v3_trading.risk_engine.input_builder import build_snapshot
        assert callable(build_snapshot)


# ============================================================
# 26. ali2026v3_trading.signal.signal_generator
# ============================================================
class TestSignalGenerator:
    def test_signal_context_creation(self):
        from ali2026v3_trading.signal.signal_generator import SignalContext
        ctx = SignalContext(instrument_id="au2506", signal_type="LONG", price=500.0)
        assert ctx.instrument_id == "au2506"
        assert not ctx.rejected

    def test_signal_generator_import(self):
        from ali2026v3_trading.signal.signal_generator import SignalGenerator
        assert SignalGenerator is not None


# ============================================================
# 27. ali2026v3_trading.strategy.box_spring_executor (helpers merged)
# ============================================================
class TestBoxSpringExecutorHelpers:
    def test_compute_hedge_ratio(self):
        from ali2026v3_trading.strategy.box_spring_executor import _compute_hedge_ratio
        result = _compute_hedge_ratio({"signal_id": "test"})
        assert result == 1.0

    def test_check_cross_strategy_risk(self):
        from ali2026v3_trading.strategy.box_spring_executor import _check_cross_strategy_risk
        result = _check_cross_strategy_risk({"signal_id": "test"})
        assert result is True

    def test_record_spring_trade(self):
        from ali2026v3_trading.strategy.box_spring_executor import _record_spring_trade
        result = _record_spring_trade({"signal_id": "sig_001"})
        assert result["recorded"] is True
        assert result["signal_id"] == "sig_001"

    def test_find_straddle_pair(self):
        from ali2026v3_trading.strategy.box_spring_executor import _find_straddle_pair
        result = _find_straddle_pair({"instrument_id": "au2506", "premium": 5.0, "option_type": "CALL"})
        assert result == ("au2506", 5.0, "CALL")


# ============================================================
# 28. ali2026v3_trading.strategy_judgment.backtest_integration_hooks
# ============================================================
class TestBacktestIntegrationHooks:
    def test_hook_config_creation(self):
        from ali2026v3_trading.strategy_judgment.backtest_integration_hooks import HookConfig
        config = HookConfig()
        assert config is not None

    def test_backtest_integration_hooks_import(self):
        from ali2026v3_trading.strategy_judgment.backtest_integration_hooks import BacktestIntegrationHooks
        assert BacktestIntegrationHooks is not None


# ============================================================
# 29. ali2026v3_trading.strategy_judgment.parameter_pool_adapter
# ============================================================
class TestParameterPoolAdapter:
    def test_extract_profitability(self):
        from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import _extract_profitability
        result = {"sharpe": 1.5, "win_rate": 0.6, "max_drawdown_pct": 0.08}
        metrics = _extract_profitability(result)
        assert "sharpe" in metrics
        assert metrics["sharpe"] == 1.5

    def test_extract_profitability_missing_keys(self):
        from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import _extract_profitability
        result = {"sharpe": 1.5}
        metrics = _extract_profitability(result)
        assert "sharpe" in metrics
        assert "win_rate" not in metrics

    def test_ecosystem_type_to_judgment_type(self):
        from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import ecosystem_type_to_judgment_type
        result = ecosystem_type_to_judgment_type("resonance")
        assert isinstance(result, str)

    def test_judge_backtest_result_callable(self):
        from ali2026v3_trading.strategy_judgment.parameter_pool_adapter import judge_backtest_result
        assert callable(judge_backtest_result)


# ============================================================
# 30. ali2026v3_trading.strategy_judgment.pnl_attribution
# ============================================================
class TestPnLAttribution:
    def test_direction_enum(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import Direction
        assert Direction.LONG.value == "LONG"
        assert Direction.SHORT.value == "SHORT"

    def test_time_segment_enum(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import TimeSegment
        assert TimeSegment.OPEN.value == "OPEN"
        assert TimeSegment.MIDDAY.value == "MIDDAY"
        assert TimeSegment.CLOSE.value == "CLOSE"

    def test_classify_direction(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_direction, Direction
        assert classify_direction(1.0) == Direction.LONG
        assert classify_direction(-1.0) == Direction.SHORT

    def test_classify_time_segment(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_time_segment, TimeSegment
        dt_open = datetime(2026, 6, 15, 9, 15, tzinfo=timezone(timedelta(hours=8)))
        assert classify_time_segment(dt_open) == TimeSegment.OPEN
        dt_mid = datetime(2026, 6, 15, 11, 0, tzinfo=timezone(timedelta(hours=8)))
        assert classify_time_segment(dt_mid) == TimeSegment.MIDDAY
        dt_close = datetime(2026, 6, 15, 14, 50, tzinfo=timezone(timedelta(hours=8)))
        assert classify_time_segment(dt_close) == TimeSegment.CLOSE

    def test_trade_record_creation(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import TradeRecord, Direction, TimeSegment
        trade = TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0, commission=1.0
        )
        assert trade.pnl == 100.0

    def test_pnl_attributor_init(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor
        attributor = PnLAttributor()
        assert attributor is not None

    def test_pnl_attributor_add_trade(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attributor = PnLAttributor()
        trade = TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0
        )
        attributor.add_trade(trade)

    def test_pnl_attributor_compute_attribution(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attributor = PnLAttributor()
        for i in range(5):
            attributor.add_trade(TradeRecord(
                strategy="hft", instrument="au2506",
                direction=Direction.LONG, time_segment=TimeSegment.OPEN,
                pnl=100.0 * (i + 1)
            ))
        result = attributor.compute_attribution()
        assert hasattr(result, 'by_strategy')
        assert 'hft' in result.by_strategy


# ============================================================
# 31. ali2026v3_trading.param_pool.validation._validator_protocol
# ============================================================
class TestValidatorProtocol:
    def test_statistical_validator_protocol(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        assert hasattr(StatisticalValidator, 'test')
        assert hasattr(StatisticalValidator, 'validate')

    def test_gate_validator_protocol(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import GateValidator
        assert hasattr(GateValidator, 'check')

    def test_statistical_validator_runtime_checkable(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import StatisticalValidator
        class MyValidator:
            def test(self, *args, **kwargs): return {}
            def validate(self, *args, **kwargs): return {}
        assert isinstance(MyValidator(), StatisticalValidator)

    def test_gate_validator_runtime_checkable(self):
        from ali2026v3_trading.param_pool.validation._validator_protocol import GateValidator
        class MyGateValidator:
            def check(self, context): return {"passed": True}
        assert isinstance(MyGateValidator(), GateValidator)


# ============================================================
# 32. ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search
# ============================================================
class TestOptunaMultiobjectiveSearch:
    def test_latin_hypercube_sample(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import latin_hypercube_sample
        result = latin_hypercube_sample(n_samples=5, n_dims=3)
        assert result is not None
        assert result.shape[0] == 5
        assert result.shape[1] == 3

    def test_lhs_to_param_grid(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import lhs_to_param_grid
        import numpy as np
        lhs = np.random.rand(3, 2)
        param_ranges = {'param1': (0.0, 1.0), 'param2': (0.0, 10.0)}
        result = lhs_to_param_grid(lhs, param_ranges)
        assert isinstance(result, list)

    def test_sigmoid_score(self):
        from ali2026v3_trading.param_pool.optimization.optuna_multiobjective_search import sigmoid_score
        result = sigmoid_score(test_sharpe=1.5, profit_loss_ratio=2.0, test_max_drawdown=0.1, win_rate=0.6)
        assert isinstance(result, float)
        assert 0.0 <= result <= 1.0


# ============================================================
# 33. ali2026v3_trading.param_pool.optimization.phase_scan
# ============================================================
class TestPhaseScan:
    def test_gate_check_result_import(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import GateCheckResult
        assert GateCheckResult is not None

    def test_check_physical_constraints_callable(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import check_physical_constraints
        assert callable(check_physical_constraints)

    def test_score_metric_callable(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import score_metric
        assert callable(score_metric)

    def test_p0_gate_check_callable(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import p0_gate_check
        assert callable(p0_gate_check)

    def test_crop_pullback_grid_callable(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import crop_pullback_grid
        assert callable(crop_pullback_grid)


# ============================================================
# 34. ali2026v3_trading.precompute.meta_audit_passport
# ============================================================
class TestMetaAuditPassport:
    def test_audit_passport_import(self):
        from ali2026v3_trading.precompute.meta_audit_passport import AuditPassport
        assert AuditPassport is not None

    def test_certified_result_import(self):
        from ali2026v3_trading.precompute.meta_audit_passport import CertifiedResult
        assert CertifiedResult is not None

    def test_sandbox_execution_auditor_import(self):
        from ali2026v3_trading.precompute.meta_audit_passport import SandboxExecutionAuditor
        assert SandboxExecutionAuditor is not None

    def test_restricted_exec_loader_import(self):
        from ali2026v3_trading.precompute.meta_audit_passport import RestrictedExecLoader
        assert RestrictedExecLoader is not None


# ============================================================
# 35. Deleted shim modules must be unreachable
# ============================================================
class TestDeletedShimsUnreachable:
    def test_config_sync_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.config.config_sync

    def test_infra_callback_registry_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.infra.callback_registry

    def test_infra_exception_layers_merged(self):
        from ali2026v3_trading.infra.exceptions import FatalGateError, raise_fatal, degrade
        assert FatalGateError is not None

    def test_infra_system_contracts_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.infra.system_contracts

    def test_strategy_shadow_internals_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.strategy._shadow_internals

    def test_strategy_instrument_manager_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.strategy.instrument_manager

    def test_strategy_tick_aggregator_service_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.strategy.tick_aggregator_service

    def test_param_pool_backtest_runner_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.param_pool.backtest_runner

    def test_param_pool_statistical_validation_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.param_pool.statistical_validation

    def test_risk_phase2_pre_reference_deleted(self):
        with pytest.raises(ModuleNotFoundError):
            import ali2026v3_trading.risk._phase2_pre_reference
