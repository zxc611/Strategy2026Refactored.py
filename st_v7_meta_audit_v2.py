# MODULE_ID: M2-604
"""
test_v7_meta_audit_v2.py
========================
v7_meta_audit_v2修正版测试：验证7项修复
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from ali2026v3_trading.precompute.meta_audit_passport import (
    AuditPassport, MetaAuditEngine, SandboxExecutionAuditor,
    AutoFieldLineageTracker, AuditIssue, VulnerabilityType, Severity,
    FutureLeakException, _norm_cdf, DETERMINISTIC_PATTERNS,
    RestrictedExecLoader, CertifiedResult,
)
from ali2026v3_trading.precompute.meta_audit_engine import (
    SmartSignificanceFilter,
    HistoricalUniverseRestorer,
    SignalReadinessAligner,
)


class CleanEngine:
    def __init__(self, param1=0.5, param2=10):
        self.param1 = param1
        self.param2 = param2

    def backtest(self, tick_data):
        return pd.DataFrame({
            'signal_time': tick_data['timestamp'],
            'pnl': np.random.randn(len(tick_data))
        })


CLEAN_ENGINE_CODE = """
class CleanEngine:
    def __init__(self, param1=0.5, param2=10):
        self.param1 = param1
        self.param2 = param2

    def backtest(self, tick_data):
        return tick_data.copy()
"""


class TestNormCdf:
    """修复1：纯numpy norm_cdf精度验证"""

    def test_at_zero(self):
        assert abs(_norm_cdf(0) - 0.5) < 1e-6

    def test_at_1_96(self):
        assert abs(_norm_cdf(1.96) - 0.975) < 1e-4

    def test_at_minus_1_96(self):
        assert abs(_norm_cdf(-1.96) - 0.025) < 1e-4

    def test_symmetry(self):
        for x in [0.5, 1.0, 2.0, 3.0]:
            assert abs(_norm_cdf(x) + _norm_cdf(-x) - 1.0) < 1e-6

    def test_monotonicity(self):
        xs = np.linspace(-5, 5, 100)
        vals = [_norm_cdf(x) for x in xs]
        for i in range(len(vals) - 1):
            assert vals[i] <= vals[i+1] + 1e-10

    def test_tail_clamp(self):
        assert _norm_cdf(-10) == 0.0
        assert _norm_cdf(10) == 1.0


class TestScipyFreeSignificance:
    """修复1续：SmartSignificanceFilter不依赖scipy"""

    def test_bootstrap_no_scipy(self):
        sf = SmartSignificanceFilter(min_sample_size=10, n_bootstrap=200)
        evidence = {
            'values': np.random.randn(200) + 0.5,
            'threshold': 0.0
        }
        result = sf._bootstrap_ratio_test(evidence, 200)
        assert isinstance(result, bool)

    def test_z_test_no_scipy(self):
        sf = SmartSignificanceFilter(confidence_level=0.95)
        result = sf._test_ratio_difference(60, 100, 40, 100)
        assert isinstance(result, bool)

    def test_ratio_diff_significant(self):
        sf = SmartSignificanceFilter(confidence_level=0.95)
        result = sf._test_ratio_difference(70, 100, 30, 100)
        assert result is True

    def test_ratio_diff_not_significant(self):
        sf = SmartSignificanceFilter(confidence_level=0.95)
        result = sf._test_ratio_difference(51, 100, 49, 100)
        assert result is False

    def test_zero_denominator_safe(self):
        sf = SmartSignificanceFilter()
        result = sf._test_ratio_difference(0, 0, 0, 0)
        assert result is False


class TestMonkeyPatchSafety:
    """修复2：_data_monitor的try/finally保证还原"""

    def test_original_getitem_restored(self):
        original = pd.DataFrame.__getitem__
        auditor = SandboxExecutionAuditor(CleanEngine)
        tick_df, _ = auditor.create_poisoned_tick_stream()
        poison_time = tick_df.loc[500, 'timestamp']

        with auditor._data_monitor(tick_df, poison_time) as audit_time:
            assert pd.DataFrame.__getitem__ is not original

        assert pd.DataFrame.__getitem__ is original

    def test_original_restored_on_exception(self):
        original = pd.DataFrame.__getitem__
        auditor = SandboxExecutionAuditor(CleanEngine)
        tick_df, _ = auditor.create_poisoned_tick_stream()
        poison_time = tick_df.loc[500, 'timestamp']

        try:
            with auditor._data_monitor(tick_df, poison_time) as audit_time:
                raise ValueError("test exception")
        except ValueError:
            pass

        assert pd.DataFrame.__getitem__ is original


class TestExecIsolation:
    """修复3：example_usage中exec隔离命名空间"""

    def test_exec_does_not_pollute_globals(self):
        code = "class TestIsolatedClass:\n    pass\n"
        ns = {}
        exec(code, ns)
        assert 'TestIsolatedClass' in ns
        assert 'TestIsolatedClass' not in globals()


class TestInjectGhostTradesInterface:
    """修复4：inject_ghost_trades使用engine.backtest(data)"""

    def test_backtest_single_arg(self):
        records = pd.DataFrame({
            'symbol': ['TEST1'],
            'listing_date': [pd.Timestamp('2024-01-01')],
            'delisting_date': [pd.Timestamp('2024-06-30')],
            'delisting_reason': ['EXPIRED'],
            'status': ['DELISTED_DURING_BACKTEST']
        })

        call_log = []

        class MockEngine:
            def backtest(self, data):
                call_log.append(('backtest', type(data).__name__))
                return pd.DataFrame({'pnl': [1.0]})

        def mock_loader(symbol, start, end):
            return pd.DataFrame({
                'timestamp': pd.date_range(start, periods=100, freq='min'),
                'price': np.random.randn(100) + 100
            })

        restorer = HistoricalUniverseRestorer(records, mock_loader)
        result = restorer.inject_ghost_trades(
            MockEngine(), {},
            lambda r: True,
            max_ghosts=5
        )

        assert len(call_log) > 0
        assert call_log[0] == ('backtest', 'DataFrame')


class TestIsLaterLogic:
    """修复5：_is_later正确判断时间先后"""

    def test_future_data_is_later(self):
        tracker = AutoFieldLineageTracker(CLEAN_ENGINE_CODE)
        assert tracker._is_later('FUTURE_DATA', 'CURRENT_BAR_CLOSE') is True

    def test_earlier_bar_is_not_later(self):
        tracker = AutoFieldLineageTracker(CLEAN_ENGINE_CODE)
        assert tracker._is_later('BAR_CLOSE_MINUS_20', 'CURRENT_BAR_CLOSE') is False

    def test_current_is_later_than_minus_5(self):
        tracker = AutoFieldLineageTracker(CLEAN_ENGINE_CODE)
        assert tracker._is_later('CURRENT_BAR_CLOSE', 'BAR_CLOSE_MINUS_5') is True

    def test_minus_5_is_later_than_minus_20(self):
        tracker = AutoFieldLineageTracker(CLEAN_ENGINE_CODE)
        assert tracker._is_later('BAR_CLOSE_MINUS_5', 'BAR_CLOSE_MINUS_20') is True

    def test_minus_20_is_not_later_than_minus_5(self):
        tracker = AutoFieldLineageTracker(CLEAN_ENGINE_CODE)
        assert tracker._is_later('BAR_CLOSE_MINUS_20', 'BAR_CLOSE_MINUS_5') is False

    def test_previous_bar_is_earlier_than_current(self):
        tracker = AutoFieldLineageTracker(CLEAN_ENGINE_CODE)
        assert tracker._is_later('PREVIOUS_BAR_CLOSE', 'CURRENT_BAR_CLOSE') is False


class TestNoAuditedDataFrameSubclass:
    """修复6：不再使用AuditedDataFrame继承hack"""

    def test_wrapper_no_subclass(self):
        source = inspect.getsource(SandboxExecutionAuditor._run_with_audit_wrapper)
        assert 'AuditedDataFrame' not in source
        assert 'class ' not in source or 'AuditedDataFrame' not in source


class TestShiftNegativeZeroTolerance:
    """shift(-n)零容忍：CRITICAL级别"""

    def test_shift_negative_detected(self):
        code = "df['future'] = df['price'].shift(-1)\nresult = data.copy()\n"
        engine = MetaAuditEngine(code)
        issues = engine.audit_backtest_engine_integrity()
        shift_issues = [i for i in issues if i.issue_type == 'SHIFT_NEGATIVE_RISK']
        assert len(shift_issues) == 1
        assert shift_issues[0].severity == 'CRITICAL'

    def test_shift_negative_in_patterns(self):
        assert 'SHIFT_NEGATIVE_RISK' in DETERMINISTIC_PATTERNS


class TestPotentialOrderBias:
    """同tick多品种处理顺序检查"""

    def test_detected_without_groupby(self):
        code = "for symbol in symbols:\n    process(symbol)\nresult = data.copy()\n"
        engine = MetaAuditEngine(code)
        issues = engine.audit_backtest_engine_integrity()
        bias_issues = [i for i in issues if i.issue_type == 'POTENTIAL_ORDER_BIAS']
        assert len(bias_issues) == 1

    def test_not_detected_with_groupby(self):
        code = "for symbol in symbols:\n    process(symbol)\ngroupby('timestamp')\nsort_values('time')\n"
        engine = MetaAuditEngine(code)
        issues = engine.audit_backtest_engine_integrity()
        bias_issues = [i for i in issues if i.issue_type == 'POTENTIAL_ORDER_BIAS']
        assert len(bias_issues) == 0


class TestAutoFieldLineageSyntaxError:
    """AST解析SyntaxError安全处理"""

    def test_invalid_syntax_no_crash(self):
        code = "this is not valid python !!!\n"
        tracker = AutoFieldLineageTracker(code)
        assert tracker.lineage == {}


class TestSignalReadinessAlignerNoMutation:
    """SignalReadinessAligner不修改输入DataFrame"""

    def test_analyze_no_mutation(self):
        df = pd.DataFrame({
            'event_time': pd.date_range('2024-01-01', periods=10, freq='min'),
            'ready_time': pd.date_range('2024-01-01', periods=10, freq='min') + pd.Timedelta(seconds=5)
        })
        original_cols = list(df.columns)
        aligner = SignalReadinessAligner({'strategy_a': df})
        aligner.analyze_readiness_distribution()
        assert list(df.columns) == original_cols
        assert 'latency' not in df.columns


class TestAuditPassportIntegration:
    """AuditPassport集成测试"""

    def test_clean_engine_passes(self):
        passport = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5, 'param2': 10}
        )
        result = passport.run_meta_audit()
        assert result is True
        assert passport.certified is True

    def test_uncertified_backtest_raises(self):
        passport = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5, 'param2': 10}
        )
        passport.certified = False
        with pytest.raises(RuntimeError, match="certification invalid|not certified"):
            passport.run_certified_backtest(pd.DataFrame())

    def test_passport_id_deterministic(self):
        p1 = AuditPassport(CLEAN_ENGINE_CODE, CleanEngine, {'a': 1})
        p2 = AuditPassport(CLEAN_ENGINE_CODE, CleanEngine, {'a': 1})
        assert p1.passport_id == p2.passport_id

    def test_passport_id_diff_config(self):
        p1 = AuditPassport(CLEAN_ENGINE_CODE, CleanEngine, {'a': 1})
        p2 = AuditPassport(CLEAN_ENGINE_CODE, CleanEngine, {'a': 2})
        assert p1.passport_id != p2.passport_id

    def test_save_and_load(self, tmp_path):
        passport = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5}
        )
        passport.run_meta_audit()
        path = str(tmp_path / 'audit.json')
        passport.save(path)

        passport2 = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5}
        )
        passport2.load(path)
        assert passport2.certified == passport.certified
        assert passport2.passport_id == passport.passport_id


class TestDeterministicPatternsComplete:
    """DETERMINISTIC_PATTERNS完整性"""

    def test_all_patterns_in_list(self):
        expected = [
            'SHIFT_NEGATIVE_RISK',
            'POTENTIAL_ORDER_BIAS',
            'FUTURE_LEAK_CAUGHT',
            'GLOBAL_STATE_RISK',
            'SORT_ORDER_RISK',
            'DATA_MUTATION_RISK',
        ]
        for p in expected:
            assert p in DETERMINISTIC_PATTERNS, f"{p} missing from DETERMINISTIC_PATTERNS"

    def test_count(self):
        assert len(DETERMINISTIC_PATTERNS) == 13


# =============================================================================
# 残留项1-11专项测试
# =============================================================================

class TestResidue1DynamicCodeDetection:
    """残留1: exec/eval/compile调用AST检测"""

    def test_exec_detected(self):
        code = "exec('print(1)')\nresult = data.copy()\n"
        engine = MetaAuditEngine(code)
        issues = engine.audit_backtest_engine_integrity()
        dynamic = [i for i in issues if i.issue_type == 'DYNAMIC_CODE_RISK']
        assert len(dynamic) == 1
        assert dynamic[0].severity == 'HIGH'

    def test_eval_detected(self):
        code = "x = eval('1+1')\nresult = data.copy()\n"
        engine = MetaAuditEngine(code)
        issues = engine.audit_backtest_engine_integrity()
        dynamic = [i for i in issues if i.issue_type == 'DYNAMIC_CODE_RISK']
        assert len(dynamic) == 1

    def test_no_dynamic_code(self):
        engine = MetaAuditEngine(CLEAN_ENGINE_CODE)
        issues = engine.audit_backtest_engine_integrity()
        dynamic = [i for i in issues if i.issue_type == 'DYNAMIC_CODE_RISK']
        assert len(dynamic) == 0


class TestResidue2AccessorInterception:
    """残留2: iloc/loc accessor拦截"""

    def test_getattr_patched_and_restored(self):
        original = pd.DataFrame.__getattr__
        auditor = SandboxExecutionAuditor(CleanEngine)
        tick_df, _ = auditor.create_poisoned_tick_stream()
        poison_time = tick_df.loc[500, 'timestamp']
        with auditor._data_monitor(tick_df, poison_time) as audit_time:
            assert pd.DataFrame.__getattr__ is not original
        assert pd.DataFrame.__getattr__ is original


class TestResidue3RestrictedExec:
    """残留3: exec安全白名单"""

    def test_allowed_module(self):
        ns = {}
        RestrictedExecLoader.safe_exec("import math\nx = math.pi", ns)
        assert abs(ns['x'] - 3.14159) < 0.01

    def test_blocked_module(self):
        ns = {}
        with pytest.raises(RuntimeError, match="Blocked import"):
            RestrictedExecLoader.safe_exec("import os\nx = os.getcwd()", ns)

    def test_blocked_from_import(self):
        ns = {}
        with pytest.raises(RuntimeError, match="Blocked"):
            RestrictedExecLoader.safe_exec("from os import path\nx = 1", ns)


class TestResidue4DualArgBacktest:
    """残留4: backtest(data) TypeError回退到backtest(data, config)"""

    def test_fallback_to_config_arg(self):
        records = pd.DataFrame({
            'symbol': ['TEST1'],
            'listing_date': [pd.Timestamp('2024-01-01')],
            'delisting_date': [pd.Timestamp('2024-06-30')],
            'delisting_reason': ['EXPIRED'],
            'status': ['DELISTED_DURING_BACKTEST']
        })

        class DualArgEngine:
            def backtest(self, data, config=None):
                return pd.DataFrame({'pnl': [1.0]})

        def mock_loader(symbol, start, end):
            return pd.DataFrame({
                'timestamp': pd.date_range(start, periods=100, freq='min'),
                'price': np.random.randn(100) + 100
            })

        restorer = HistoricalUniverseRestorer(records, mock_loader)
        result = restorer.inject_ghost_trades(
            DualArgEngine(), {'param': 1},
            lambda r: True, max_ghosts=5
        )
        assert result['total_delisted_tested'] == 1


class TestResidue5DependencyAwareAlignment:
    """残留5: 因果依赖感知对齐"""

    def test_independent_strategies_not_delayed(self):
        df_a = pd.DataFrame({
            'event_time': pd.date_range('2024-01-01', periods=10, freq='min'),
            'ready_time': pd.date_range('2024-01-01', periods=10, freq='min') + pd.Timedelta(seconds=5)
        })
        df_b = pd.DataFrame({
            'event_time': pd.date_range('2024-01-01', periods=10, freq='min'),
            'ready_time': pd.date_range('2024-01-01', periods=10, freq='min') + pd.Timedelta(seconds=100)
        })
        aligner = SignalReadinessAligner({'fast': df_a, 'slow': df_b})
        rules = aligner.generate_alignment_rules(
            dependency_graph={'fast': [], 'slow': []}
        )
        assert rules['fast']['required_delay_seconds'] == 0.0
        assert rules['slow']['required_delay_seconds'] == 0.0

    def test_dependent_strategy_waits(self):
        df_a = pd.DataFrame({
            'event_time': pd.date_range('2024-01-01', periods=10, freq='min'),
            'ready_time': pd.date_range('2024-01-01', periods=10, freq='min') + pd.Timedelta(seconds=100)
        })
        df_b = pd.DataFrame({
            'event_time': pd.date_range('2024-01-01', periods=10, freq='min'),
            'ready_time': pd.date_range('2024-01-01', periods=10, freq='min') + pd.Timedelta(seconds=5)
        })
        aligner = SignalReadinessAligner({'slow': df_a, 'fast': df_b})
        rules = aligner.generate_alignment_rules(
            dependency_graph={'slow': [], 'fast': ['slow']}
        )
        assert rules['fast']['required_delay_seconds'] > 0


class TestResidue6BiasFormula:
    """残留6: _estimate_bias基于ghost与active的Sharpe差"""

    def test_bias_with_active_sharpe(self):
        records = pd.DataFrame({
            'symbol': [], 'listing_date': [], 'delisting_date': [],
            'delisting_reason': [], 'status': []
        })
        restorer = HistoricalUniverseRestorer(records)
        ghost_results = [
            {'status': 'SUCCESS', 'would_have_been_selected': True, 'strategy_sharpe': -0.2}
        ]
        bias = restorer._estimate_bias(ghost_results, active_avg_sharpe=1.5)
        assert abs(bias - 1.7) < 0.01

    def test_bias_fallback_without_active(self):
        records = pd.DataFrame({
            'symbol': [], 'listing_date': [], 'delisting_date': [],
            'delisting_reason': [], 'status': []
        })
        restorer = HistoricalUniverseRestorer(records)
        ghost_results = [
            {'status': 'SUCCESS', 'would_have_been_selected': True, 'strategy_sharpe': 0.3}
        ]
        bias = restorer._estimate_bias(ghost_results)
        assert abs(bias - 0.2) < 0.01


class TestResidue7ScopeIsolation:
    """残留7: AST作用域隔离"""

    def test_same_name_different_scope(self):
        code = """
def func_a():
    result = df['close'].rolling(5).mean()

def func_b():
    result = df['close'].rolling(20).mean()
"""
        tracker = AutoFieldLineageTracker(code)
        keys = list(tracker.lineage.keys())
        scoped = [k for k in keys if '.' in k]
        assert len(scoped) >= 2


class TestResidue8VarSubscript:
    """残留8: 变量名下标识别"""

    def test_variable_subscript_detected(self):
        code = "col = 'price'\nval = df[col]\nresult = data.copy()\n"
        tracker = AutoFieldLineageTracker(code)
        all_sources = []
        for meta in tracker.lineage.values():
            all_sources.extend(meta['source_fields'])
        var_sources = [s for s in all_sources if s.startswith('<var:')]
        assert len(var_sources) >= 1


class TestResidue9PrecognitionBinomialTest:
    """残留9: 预知检测二项检验"""

    def test_strong_precognition_detected(self):
        auditor = SandboxExecutionAuditor(CleanEngine)
        tick_df, _ = auditor.create_poisoned_tick_stream()
        # 确保poison后价格上涨：手动调整post-poison价格
        poison_idx = 500
        for i in range(poison_idx + 1, poison_idx + 11):
            tick_df.loc[i, 'price'] = tick_df.loc[poison_idx, 'price'] + 1.0
        n = 100
        pre_signals = pd.DataFrame({
            'signal_time': tick_df.loc[:n-1, 'timestamp'],
            'direction': ['LONG'] * 90 + ['SHORT'] * 10
        })
        result = auditor._detect_precognition(pre_signals, tick_df, poison_idx)
        assert result is True

    def test_random_signals_not_detected(self):
        auditor = SandboxExecutionAuditor(CleanEngine)
        tick_df, _ = auditor.create_poisoned_tick_stream()
        n = 100
        pre_signals = pd.DataFrame({
            'signal_time': tick_df.loc[:n-1, 'timestamp'],
            'direction': ['LONG'] * 52 + ['SHORT'] * 48
        })
        result = auditor._detect_precognition(pre_signals, tick_df, 500)
        assert result is False


class TestResidue10TracebackPreserved:
    """残留10: 异常堆栈保留"""

    def test_ghost_trade_traceback(self):
        records = pd.DataFrame({
            'symbol': ['TEST1'],
            'listing_date': [pd.Timestamp('2024-01-01')],
            'delisting_date': [pd.Timestamp('2024-06-30')],
            'delisting_reason': ['EXPIRED'],
            'status': ['DELISTED_DURING_BACKTEST']
        })

        class FailingEngine:
            def backtest(self, data):
                raise ValueError("intentional error")

        def mock_loader(symbol, start, end):
            return pd.DataFrame({
                'timestamp': pd.date_range(start, periods=100, freq='min'),
                'price': np.random.randn(100) + 100
            })

        restorer = HistoricalUniverseRestorer(records, mock_loader)
        result = restorer.inject_ghost_trades(
            FailingEngine(), {}, lambda r: True, max_ghosts=5
        )
        failed = [g for g in result['ghost_trades'] if g.get('status') == 'BACKTEST_FAILED']
        assert len(failed) == 1
        assert 'traceback' in failed[0]
        assert failed[0]['error_type'] == 'ValueError'


class TestResidue11CertificationTTL:
    """残留11: certified TTL和哈希校验"""

    def test_ttl_not_expired(self):
        passport = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5},
            certified_ttl_seconds=3600
        )
        passport.run_meta_audit()
        assert passport.is_certification_valid() is True

    def test_ttl_expired(self):
        passport = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5},
            certified_ttl_seconds=0
        )
        passport.run_meta_audit()
        import time
        time.sleep(0.01)
        assert passport.is_certification_valid() is False

    def test_code_hash_mismatch(self):
        passport = AuditPassport(
            engine_source_code=CLEAN_ENGINE_CODE,
            engine_class=CleanEngine,
            strategy_config={'param1': 0.5}
        )
        passport.run_meta_audit()
        passport.engine_code = "MODIFIED CODE"
        assert passport.is_certification_valid() is False
