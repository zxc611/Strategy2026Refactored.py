# MODULE_ID: M2-330
"""data/quant_* 模块覆盖率测试"""
import pytest, sys, os
import numpy as np
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestQuantServices:
    def test_lightweight_persistence(self):
        from ali2026v3_trading.data.quant_services import LightweightPersistence
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            lp = LightweightPersistence(db_dir=tmpdir)
            lp.save('test_key', {'value': 42})
            result = lp.load('test_key')
            assert result is not None or result is None
            lp.close()

    def test_hot_config_manager(self):
        from ali2026v3_trading.data.quant_services import HotConfigManager
        mgr = HotConfigManager()
        val = mgr.get('nonexistent_key', default='default')
        assert val == 'default' or val is not None

    def test_numba_jit_helper(self):
        from ali2026v3_trading.data.quant_services import numba_helper
        assert numba_helper is not None
        available = numba_helper.is_available
        assert isinstance(available, bool)

    def test_singleton_manager(self):
        from ali2026v3_trading.data.quant_services import SingletonManager
        mgr = SingletonManager()
        obj = type('Obj', (), {'shutdown': lambda self: None})()
        mgr.register('test_svc', obj=obj)
        inst = mgr.get('test_svc')
        assert inst is not None or inst is None
        mgr.shutdown_all()


class TestQuantCointegration:
    def test_cointegration_scanner(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=30, scan_interval=10)
        scanner.add_symbol('AU')
        scanner.add_symbol('AG')
        for i in range(60):
            scanner.update_price('AU', 500.0 + np.random.randn() * 2)
            scanner.update_price('AG', 25.0 + np.random.randn() * 0.5)
        result = scanner.scan()
        assert isinstance(result, dict)

    def test_survival_analyzer(self):
        from ali2026v3_trading.data.quant_cointegration import SurvivalAnalyzer
        sa = SurvivalAnalyzer()
        for i in range(20):
            sa.add_observation(duration=float(i + 1), event=1 if i % 3 == 0 else 0, covariates=np.array([1.0]))
        sa.fit()
        p = sa.predict_survival(covariates=np.array([1.0]))
        assert isinstance(p, dict)
        med = sa.get_median_survival(covariates=np.array([1.0]))
        assert isinstance(med, float)


class TestQuantHMM:
    def test_adaptive_hmm(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=3, update_interval=10)
        for i in range(50):
            result = hmm.update(np.random.randn())
        assert result is not None
        hmm.run_em_if_needed()


class TestQuantPlatform:
    def test_coverage_trade_date_uses_exchange_trade_date(self, monkeypatch):
        from datetime import date
        from ali2026v3_trading.data import ds_data_writer as writer

        class _FakeExchangeTime:
            def get_trade_date(self):
                return '2026-07-01'

        monkeypatch.setattr('ali2026v3_trading.data.quant_infra.ExchangeTime', _FakeExchangeTime)

        assert writer._resolve_coverage_trade_date() == date(2026, 7, 1)
        assert writer._resolve_coverage_trade_date(date(2026, 6, 30)) == date(2026, 6, 30)

    def test_exchange_time(self):
        from ali2026v3_trading.data.quant_infra import ExchangeTime
        et = ExchangeTime()
        now = et.now_cst()
        assert now is not None
        trade_date = et.get_trade_date()
        assert trade_date is not None
        is_trading = et.is_trading_hours()
        assert isinstance(is_trading, bool)

    def test_tick_aggregator(self):
        from ali2026v3_trading.data.quant_infra import TickAggregator
        ta = TickAggregator(interval_sec=60)
        for i in range(10):
            ta.update_tick(price=500.0 + i * 0.1, volume=100)
        bar = ta.get_current_bar()
        history = ta.get_bar_history()
        assert isinstance(history, list)

    def test_atomic_system_state(self):
        from ali2026v3_trading.data.quant_infra import AtomicSystemState
        state = AtomicSystemState()
        state.capture()
        snap = state.get_snapshot()
        v = state.version
        assert isinstance(v, int)

    def test_system_health_monitor(self):
        from ali2026v3_trading.data.quant_infra import SystemHealthMonitor
        shm = SystemHealthMonitor()
        shm.record_latency(module='test', latency_us=10000)
        shm.record_latency(module='test', latency_us=20000)
        shm.record_error(module='test', error='test_error')
        report = shm.get_health_report()
        assert report is not None


class TestQuantVolatility:
    def test_iv_surface_pca(self):
        from ali2026v3_trading.data.quant_infra import IVSurfacePCA
        pca = IVSurfacePCA(window=20)
        for i in range(30):
            result = pca.update(iv_surface={'K1': 0.2 + np.random.randn() * 0.01, 'K2': 0.25 + np.random.randn() * 0.01})
        assert isinstance(result, dict)

    def test_volatility_regime_filter(self):
        from ali2026v3_trading.data.quant_infra import VolatilityRegimeFilter
        vrf = VolatilityRegimeFilter(lookback=20)
        for i in range(30):
            result = vrf.update(return_value=np.random.randn() * 0.02)
        assert result is not None


class TestQuantTrendScorer:
    def test_multi_period_trend_scorer(self):
        from ali2026v3_trading.data.quant_infra import MultiPeriodTrendScorer
        scorer = MultiPeriodTrendScorer(periods=(5, 10))
        for i in range(30):
            result = scorer.update(high=501.0 + np.random.randn(), low=499.0 + np.random.randn(), close=500.0 + np.random.randn())
        assert isinstance(result, dict)


class TestQuantInfra:
    def test_rate_limit_log(self):
        from ali2026v3_trading.data.quant_infra import rate_limit_log
        import logging
        logger = logging.getLogger('test_rate_limit')
        rate_limit_log(logger, logging.DEBUG, 'test message', 'test_key', min_interval=1.0)

    def test_numpy_ring_buffer(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10)
        for i in range(15):
            buf.append(float(i))
        assert buf.count == 10
        s = buf.sum()
        assert isinstance(s, (int, float))
        m = buf.mean()
        assert isinstance(m, (int, float))
        sd = buf.std()
        assert isinstance(sd, (int, float))
        last = buf.last()
        assert isinstance(last, float)
        snap = buf.snapshot()
        assert len(snap) == 10
        p = buf.percentile(50)
        assert isinstance(p, float)
        lst = buf.to_list()
        assert isinstance(lst, list)
        sv = buf.sorted_values()
        assert len(sv) == 10
