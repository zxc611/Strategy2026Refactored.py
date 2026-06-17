# MODULE_ID: M2-368
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestGateCheckResult:
    def test_import(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import GateCheckResult
        assert GateCheckResult is not None

    def test_creation(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import GateCheckResult
        result = GateCheckResult(passed=True, failures=[], warnings=["minor"])
        assert result.passed is True
        assert result.failures == []
        assert result.warnings == ["minor"]


class TestNullValidator:
    def test_import(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        assert NullValidator is not None

    def test_bool_false(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        nv = NullValidator()
        assert not nv

    def test_repr(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import NullValidator
        nv = NullValidator(name="test")
        assert "test" in repr(nv)


class TestPhaseScanFunctions:
    def test_check_physical_constraints(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import check_physical_constraints
        assert callable(check_physical_constraints)

    def test_score_metric(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import score_metric
        assert callable(score_metric)

    def test_p0_gate_check(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import p0_gate_check
        assert callable(p0_gate_check)

    def test_crop_pullback_grid(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import crop_pullback_grid
        assert callable(crop_pullback_grid)

    def test_meets_hard_constraints(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import meets_hard_constraints
        assert callable(meets_hard_constraints)

    def test_phase1_scan(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import phase1_scan
        assert callable(phase1_scan)

    def test_phase2_scan(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import phase2_scan
        assert callable(phase2_scan)

    def test_coupling_verification(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import coupling_verification
        assert callable(coupling_verification)

    def test_integrate_judgment(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import integrate_judgment
        assert callable(integrate_judgment)

    def test_save_results(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import save_results
        assert callable(save_results)

    def test_run_backtest_full(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import run_backtest_full
        assert callable(run_backtest_full)


class TestEnhancedPhaseScanOptimizer:
    def test_import(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import EnhancedPhaseScanOptimizer
        assert EnhancedPhaseScanOptimizer is not None

    def test_init(self):
        from ali2026v3_trading.param_pool.optimization.phase_scan import EnhancedPhaseScanOptimizer
        opt = EnhancedPhaseScanOptimizer(symbol="rb", strategy_type="main")
        assert opt is not None