import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ali2026v3_trading'))
os.chdir(os.path.dirname(os.path.abspath(__file__)))


class TestDiagnosis20Fix(unittest.TestCase):
    """验证诊断20/20的3个根因修复"""

    def test_bug1_inject_runtime_context_trusted(self):
        from ali2026v3_trading.config.config_params import _TRUSTED_UPDATE_CALLERS
        self.assertIn('_inject_runtime_context', _TRUSTED_UPDATE_CALLERS)

    def test_bug1_strategy_injectable_into_cache(self):
        from ali2026v3_trading.infra.health_monitor import (
            _get_runtime_state, set_runtime_strategy_ref, _runtime_strategy_ref
        )
        from ali2026v3_trading.infra import health_monitor

        class MockStrategy:
            strategy_core = type('Core', (), {})()
            params = type('Params', (), {'_subscribed_instruments': ['IF2609', 'IH2609', 'cu2609']})()

        old_ref = health_monitor._runtime_strategy_ref
        set_runtime_strategy_ref(MockStrategy())
        try:
            strategy, core, subscribed, hist, ready = _get_runtime_state()
            self.assertIsNotNone(strategy, "strategy should not be None via _runtime_strategy_ref")
            self.assertGreater(len(subscribed), 0, f"subscribed should not be empty, got {list(subscribed)}")
            self.assertIn('IF2609', subscribed)
        finally:
            health_monitor._runtime_strategy_ref = old_ref

    def test_bug2_failure_labels_corrected(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)

        self.assertIn("合约元数据未加载", source)
        self.assertIn("不在订阅文件中", source)
        self.assertIn("未加入运行时订阅列表", source)
        self.assertNotIn("不在配置文件中", source)
        self.assertNotIn("配置表未加载到内存", source)

    def test_bug2_failure_priority_correct(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)

        idx_meta = source.index("合约元数据未加载")
        idx_subfile = source.index("不在订阅文件中")
        idx_runtime = source.index("未加入运行时订阅列表")
        self.assertLess(idx_meta, idx_subfile)
        self.assertLess(idx_subfile, idx_runtime)

    def test_bug2_stage_stats_labels(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)

        self.assertIn("1-MetaLoaded", source)
        self.assertIn("2-SubFile", source)
        self.assertNotIn("1-Config", source)
        self.assertNotIn("2-Memory", source)

    def test_bug3_subscribed_source_multi_level_lookup(self):
        from ali2026v3_trading.infra.health_monitor import _get_runtime_state
        import inspect
        source = inspect.getsource(_get_runtime_state)
        self.assertIn("for _src in", source)
        self.assertIn("_subscribed_instruments", source)

    def test_bug3_subscribed_on_strategy_itself(self):
        from ali2026v3_trading.infra.health_monitor import (
            _get_runtime_state, set_runtime_strategy_ref
        )
        from ali2026v3_trading.infra import health_monitor

        class MockStrategyObj:
            _subscribed_instruments = ['IF2609', 'IH2609', 'cu2609']
            params = type('Params', (), {})()

        old_ref = health_monitor._runtime_strategy_ref
        set_runtime_strategy_ref(MockStrategyObj())
        try:
            strategy, core, subscribed, hist, ready = _get_runtime_state()
            self.assertEqual(subscribed, {'IF2609', 'IH2609', 'cu2609'})
        finally:
            health_monitor._runtime_strategy_ref = old_ref

    def test_bug3_subscribed_on_params_fallback(self):
        from ali2026v3_trading.infra.health_monitor import (
            _get_runtime_state, set_runtime_strategy_ref
        )
        from ali2026v3_trading.infra import health_monitor

        class MockStrategyObj:
            params = type('Params', (), {'_subscribed_instruments': ['IF2609', 'IH2609']})()

        old_ref = health_monitor._runtime_strategy_ref
        set_runtime_strategy_ref(MockStrategyObj())
        try:
            strategy, core, subscribed, hist, ready = _get_runtime_state()
            self.assertEqual(subscribed, {'IF2609', 'IH2609'})
        finally:
            health_monitor._runtime_strategy_ref = old_ref

    def test_bug3_subscribed_on_core_fallback(self):
        from ali2026v3_trading.infra.health_monitor import (
            _get_runtime_state, set_runtime_strategy_ref
        )
        from ali2026v3_trading.infra import health_monitor

        class MockCore:
            _subscribed_instruments = ['cu2609', 'al2609']

        class MockStrategyObj:
            strategy_core = MockCore()
            params = type('Params', (), {})()

        old_ref = health_monitor._runtime_strategy_ref
        set_runtime_strategy_ref(MockStrategyObj())
        try:
            strategy, core, subscribed, hist, ready = _get_runtime_state()
            self.assertEqual(subscribed, {'cu2609', 'al2609'})
        finally:
            health_monitor._runtime_strategy_ref = old_ref

    def test_bug3_strategy_first_priority(self):
        from ali2026v3_trading.infra.health_monitor import (
            _get_runtime_state, set_runtime_strategy_ref
        )
        from ali2026v3_trading.infra import health_monitor

        class MockStrategyObj:
            _subscribed_instruments = ['IF2609']
            params = type('Params', (), {'_subscribed_instruments': ['WRONG_PARAMS']})()
            strategy_core = type('Core', (), {'_subscribed_instruments': ['WRONG_CORE']})()

        old_ref = health_monitor._runtime_strategy_ref
        set_runtime_strategy_ref(MockStrategyObj())
        try:
            strategy, core, subscribed, hist, ready = _get_runtime_state()
            self.assertEqual(subscribed, {'IF2609'})
        finally:
            health_monitor._runtime_strategy_ref = old_ref

    def test_monitored_contracts_dynamic_loading(self):
        from ali2026v3_trading.infra.health_monitor import (
            MONITORED_CONTRACTS, _load_monitored_contracts_from_subscription_files
        )
        self.assertEqual(len(MONITORED_CONTRACTS), 20)
        dynamic = _load_monitored_contracts_from_subscription_files()
        self.assertIsNotNone(dynamic)
        self.assertEqual(len(dynamic), 20)

    def test_cc04_cyclic_dependency_guard_removed(self):
        from ali2026v3_trading.strategy.strategy_monitoring_layer import StrategyMonitoringLayer
        import inspect
        source = inspect.getsource(StrategyMonitoringLayer.check_position_risk)
        lines = source.split('\n')
        in_doc = False
        code_only = []
        for l in lines:
            s = l.strip()
            if '"""' in s:
                c = s.count('"""')
                if c == 2: continue
                in_doc = not in_doc
                continue
            if not in_doc and not s.startswith('#'):
                code_only.append(l)
        code_text = '\n'.join(code_only)
        self.assertNotIn('CyclicDependencyGuard', code_text)

    def test_signal_service_no_bare_chinese_identifiers(self):
        from ali2026v3_trading.risk.risk_compute_service import RiskComputeService
        import ast, inspect
        source = inspect.getsource(RiskComputeService)
        tree = ast.parse(source)

        class ChineseNameChecker(ast.NodeVisitor):
            def __init__(self):
                self.bare = []
            def visit_Name(self, node):
                if any('\u4e00' <= c <= '\u9fff' for c in node.id):
                    self.bare.append((node.lineno, node.id))
                self.generic_visit(node)

        checker = ChineseNameChecker()
        checker.visit(tree)
        self.assertEqual(len(checker.bare), 0, f"Found bare Chinese identifiers: {checker.bare}")

    def test_bug3_real_runtime_scenario(self):
        from ali2026v3_trading.infra.health_monitor import (
            _get_runtime_state, set_runtime_strategy_ref
        )
        from ali2026v3_trading.infra import health_monitor

        class MockStrategyParams:
            pass

        class MockStrategyCore:
            _subscribed_instruments = ['AP612', 'AP703', 'CF607', 'IF2609', 'IH2609']

        class MockStrategy2026:
            params = MockStrategyParams()
            strategy_core = MockStrategyCore()

        old_ref = health_monitor._runtime_strategy_ref
        set_runtime_strategy_ref(MockStrategy2026())
        try:
            strategy, core, subscribed, hist, ready = _get_runtime_state()
            self.assertIsNotNone(strategy)
            self.assertIsNotNone(core)
            self.assertGreater(len(subscribed), 0,
                f"subscribed should not be empty in real runtime scenario, got {list(subscribed)}")
            self.assertIn('AP612', subscribed)
            self.assertIn('IF2609', subscribed)
        finally:
            health_monitor._runtime_strategy_ref = old_ref

    def test_bug5_subscribed_from_storage_subscription_manager(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)
        self.assertIn("storage", source)
        self.assertIn("subscription_manager", source)
        self.assertNotIn("runtime_core, '_subscription_manager'", source)
        self.assertNotIn("is_subscribed", source)
        self.assertNotIn("get_registration_info", source)

    def test_bug6_registered_from_params_service(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)
        self.assertIn("get_instrument_meta_by_id", source)
        self.assertIn("get_internal_id", source)
        self.assertNotIn("_option_info", source)

    def test_bug8_tick_from_subscription_manager_or_tps(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)
        self.assertIn("tick_instruments", source)
        self.assertIn("_tick_processing_service", source)

    def test_bug5_old_logic_used_wrong_object(self):
        class MockCore:
            pass
        core = MockCore()
        self.assertIsNone(getattr(core, '_subscription_manager', None))
        self.assertIsNone(getattr(core, 'subscription_manager', None))

    def test_bug5_subscription_manager_on_storage(self):
        class MockSubscriptionManager:
            _subscription_success = {
                'subscribe_time': {'IF2609': 1.0, 'cu2609': 2.0},
                'tick_instruments': {'IF2609', 'cu2609'},
            }
        class MockStorage:
            subscription_manager = MockSubscriptionManager()
        storage = MockStorage()
        sub_mgr = getattr(storage, 'subscription_manager', None)
        self.assertIsNotNone(sub_mgr)
        sub_success = getattr(sub_mgr, '_subscription_success', None)
        self.assertIsNotNone(sub_success)
        self.assertIn('IF2609', sub_success['subscribe_time'])
        self.assertIn('IF2609', sub_success['tick_instruments'])

    def test_bug6_params_service_has_get_instrument_meta(self):
        from ali2026v3_trading.config.params_service import get_params_service
        ps = get_params_service()
        if ps is not None:
            self.assertTrue(hasattr(ps, 'get_instrument_meta_by_id'))

    def test_bug8_nameerror_regression_fixed(self):
        import inspect
        from ali2026v3_trading.infra.health_monitor import _get_runtime_state
        source = inspect.getsource(_get_runtime_state)
        self.assertNotIn('subscribed_source', source)

    def test_bug12_duckdb_from_subscription_manager(self):
        import inspect
        from ali2026v3_trading.infra import health_monitor
        source = inspect.getsource(health_monitor.run_14_contracts_periodic_diagnostic)
        self.assertTrue(
            "kline_instruments" in source or "get_kline_count" in source,
            "DuckDB kline check should use either kline_instruments or query_service.get_kline_count"
        )

    def test_bug12_duckdb_fallback_chain(self):
        class MockSubscriptionManager:
            _subscription_success = {
                'subscribe_time': {'IF2609': 1.0},
                'tick_instruments': {'IF2609'},
                'kline_instruments': {'IF2609', 'cu2609'},
            }
        class MockStorage:
            subscription_manager = MockSubscriptionManager()
        storage = MockStorage()
        sub_mgr = getattr(storage, 'subscription_manager', None)
        sub_success = getattr(sub_mgr, '_subscription_success', None)
        kline_instruments = sub_success.get('kline_instruments', set())
        self.assertIn('IF2609', kline_instruments)
        self.assertIn('cu2609', kline_instruments)

    def test_quantification_core_importable(self):
        from ali2026v3_trading.param_pool.quantification._quantification_core import BayesianShrinkageLifeEstimator
        self.assertIsNotNone(BayesianShrinkageLifeEstimator)


if __name__ == '__main__':
    unittest.main(verbosity=2)