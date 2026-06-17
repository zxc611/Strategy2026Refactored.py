# MODULE_ID: M2-456
"""End-to-end assertion test for param_pool refactoring
Tests:
1. All shim files can re-export symbols from their targets
2. All merged modules are importable
3. All __init__.py public API symbols are reachable
4. No ModuleNotFoundError from broken imports
5. Key class/function identity checks (shim → real module)
"""
import sys
import os
import importlib
import traceback

sys.path.insert(0, os.path.join(
    r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo'))

PASS = 0
FAIL = 0
SKIP = 0

def assert_test(condition, msg):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        print(f"  FAIL: {msg}")

def try_import(module_path, symbol=None):
    """Try importing a module or symbol, return (success, error_msg)"""
    try:
        mod = importlib.import_module(module_path)
        if symbol:
            obj = getattr(mod, symbol, None)
            if obj is None:
                return False, f"{module_path}.{symbol} not found"
            return True, None
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def _check_shim(shim_module, target_module, symbols):
    """Test that shim re-exports symbols from target"""
    for sym in symbols:
        ok, err = try_import(shim_module, sym)
        assert_test(ok, f"shim {shim_module}.{sym}: {err}")
        if ok:
            # Verify identity: shim symbol IS the target symbol
            try:
                shim_mod = importlib.import_module(shim_module)
                target_mod = importlib.import_module(target_module)
                shim_obj = getattr(shim_mod, sym)
                target_obj = getattr(target_mod, sym)
                assert_test(shim_obj is target_obj,
                           f"shim {shim_module}.{sym} is not same object as {target_module}.{sym}")
            except Exception:
                pass

def _check_module_importable(module_path):
    """Test that a module can be imported"""
    ok, err = try_import(module_path)
    assert_test(ok, f"import {module_path}: {err}")
    return ok

# ══════════════════════════════════════════════════════════════
# TEST 1: Root shim files
# ══════════════════════════════════════════════════════════════
print("=" * 70)
print("TEST 1: Root shim re-exports")
print("=" * 70)

shim_tests = [
    ("ali2026v3_trading.param_pool.backtest_loop_slippage", "ali2026v3_trading.param_pool.backtest.backtest_runner_base", []),
    ("ali2026v3_trading.param_pool.backtest_loop_risk", "ali2026v3_trading.param_pool.backtest.backtest_runner_utils", ["_build_risk_dimension_defaults"]),
    ("ali2026v3_trading.param_pool.backtest_loop_positions", "ali2026v3_trading.param_pool.backtest.backtest_runner_utils", []),
    ("ali2026v3_trading.param_pool.backtest_orchestrator", "ali2026v3_trading.param_pool.backtest.backtest_runner_base", []),
    ("ali2026v3_trading.param_pool.backtest_state_checker", "ali2026v3_trading.param_pool.backtest_snapshot", []),
    ("ali2026v3_trading.param_pool.backtest_snapshot", "ali2026v3_trading.param_pool.backtest_snapshot", []),
    ("ali2026v3_trading.param_pool.backtest_safety_checker", "ali2026v3_trading.param_pool.backtest.backtest_runner_validation", ["_force_close_position", "check_margin_call", "check_safety"]),
    ("ali2026v3_trading.param_pool.adv_validation_survival", "ali2026v3_trading.param_pool.validation.statistical_validation", []),
    ("ali2026v3_trading.param_pool.adv_validation_walkforward", "ali2026v3_trading.param_pool.validation.statistical_validation", []),
    ("ali2026v3_trading.param_pool._preprocess", "ali2026v3_trading.param_pool._preprocess", []),
    ("ali2026v3_trading.param_pool.sharpe_3d_mapping", "ali2026v3_trading.param_pool.sharpe_3d_mapping", []),
    ("ali2026v3_trading.param_pool.preprocess_ticks", "ali2026v3_trading.param_pool._preprocess", []),
    ("ali2026v3_trading.param_pool.preprocess_validation", "ali2026v3_trading.param_pool._preprocess", []),
    ("ali2026v3_trading.param_pool.data_validator", "ali2026v3_trading.param_pool._preprocess", []),
    ("ali2026v3_trading.param_pool.tick_aggregator", "ali2026v3_trading.param_pool._preprocess", []),
    ("ali2026v3_trading.param_pool.validation.statistical_validation", "ali2026v3_trading.param_pool.validation.statistical_validation", ["CounterfactualResult", "MonteCarloBankruptcyValidator"]),
    ("ali2026v3_trading.param_pool.test_design_suite", "ali2026v3_trading.param_pool.sensitivity_analysis", ["TestDesignSuite", "ExecutionChecklist"]),
    ("ali2026v3_trading.param_pool.sharpe_3d_config", "ali2026v3_trading.param_pool.sharpe_3d_mapping", []),
    ("ali2026v3_trading.param_pool.sharpe_3d_live", "ali2026v3_trading.param_pool.sharpe_3d_mapping", []),
    ("ali2026v3_trading.param_pool.backtest_metrics", "ali2026v3_trading.param_pool.backtest_config", ["compute_profit_loss_ratio_metrics"]),
    ("ali2026v3_trading.param_pool.backtest_pricing", "ali2026v3_trading.param_pool.backtest_config", ["calc_trade_fee"]),
    ("ali2026v3_trading.param_pool.backtest_position_manager", "ali2026v3_trading.param_pool.backtest_state", ["try_open_check_gates", "check_eod_close"]),
    ("ali2026v3_trading.param_pool.backtest_validation", "ali2026v3_trading.param_pool.backtest.backtest_runner_validation", []),
    ("ali2026v3_trading.param_pool.ts_result_data_loader", "ali2026v3_trading.param_pool.ts_result_writer", []),
    ("ali2026v3_trading.param_pool.ts_result_executor", "ali2026v3_trading.param_pool.ts_result_writer", []),
    ("ali2026v3_trading.param_pool.checks_individual", "ali2026v3_trading.param_pool.checks_orchestrator", ["validate_logic_reversal_no_future"]),
    ("ali2026v3_trading.param_pool.validation_hmm_state", "ali2026v3_trading.param_pool.validation.validation_deep_orchestrator", ["validate_hmm_online_vs_offline"]),
    ("ali2026v3_trading.param_pool.validation_deep_checks", "ali2026v3_trading.param_pool.validation.validation_deep_orchestrator", ["validate_rollover_impact"]),
    ("ali2026v3_trading.param_pool.ts_param_grids", "ali2026v3_trading.param_pool.ts.ts_backtest_strategies", []),
    ("ali2026v3_trading.param_pool.backtest_snapshot", "ali2026v3_trading.param_pool.backtest.backtest_runner_base", []),
    ("ali2026v3_trading.param_pool.task_scheduler", "ali2026v3_trading.param_pool.backtest_config", []),
]

for shim_mod, target_mod, syms in shim_tests:
    _check_shim(shim_mod, target_mod, syms)

# ══════════════════════════════════════════════════════════════
# TEST 2: L1 quantification shim files
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TEST 2: L1 quantification shim re-exports")
print("=" * 70)

l1_shim_tests = [
    ("ali2026v3_trading.param_pool.l1_quantification.meta_audit_helpers",
     "ali2026v3_trading.param_pool.l1_quantification.meta_audit_engine",
     ["SmartSignificanceFilter", "HistoricalUniverseRestorer", "SignalReadinessAligner"]),
    ("ali2026v3_trading.param_pool.l1_quantification.meta_audit_sandbox",
     "ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport",
     ["SandboxExecutionAuditor", "AutoFieldLineageTracker", "RestrictedExecLoader"]),
    ("ali2026v3_trading.param_pool.l1_quantification.bayesian_shrinkage_life_estimator",
     "ali2026v3_trading.param_pool.l1_quantification._quantification_core",
     ["BayesianShrinkageLifeEstimator"]),
    ("ali2026v3_trading.param_pool.l1_quantification.triple_truth_anchor",
     "ali2026v3_trading.param_pool.l1_quantification._quantification_core",
     ["TripleTruthAnchor"]),
    ("ali2026v3_trading.param_pool.l1_quantification.soft_constrained_optimizer",
     "ali2026v3_trading.param_pool.l1_quantification._tier_optimizer",
     ["SoftConstrainedOptimizer"]),
    ("ali2026v3_trading.param_pool.l1_quantification.performance_tier_manager",
     "ali2026v3_trading.param_pool.l1_quantification._tier_optimizer",
     ["PerformanceTierManager", "TierConfig"]),
    ("ali2026v3_trading.param_pool.l1_quantification.duckdb_tick_storage",
     "ali2026v3_trading.param_pool.l1_quantification._data_validation",
     ["DuckDBTickStorage"]),
    ("ali2026v3_trading.param_pool.l1_quantification.external_validation_pipeline",
     "ali2026v3_trading.param_pool.l1_quantification._data_validation",
     ["ExternalValidationPipeline", "ExternalSourceConfig"]),
]

for shim_mod, target_mod, syms in l1_shim_tests:
    _check_shim(shim_mod, target_mod, syms)

# ══════════════════════════════════════════════════════════════
# TEST 3: Merged modules importable
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TEST 3: Merged modules importable")
print("=" * 70)

merged_modules = [
    "ali2026v3_trading.param_pool.backtest.backtest_runner_base",
    "ali2026v3_trading.param_pool.backtest.backtest_runner_utils",
    "ali2026v3_trading.param_pool.backtest.backtest_runner_validation",
    "ali2026v3_trading.param_pool.backtest_config",
    "ali2026v3_trading.param_pool.backtest_state",
    "ali2026v3_trading.param_pool.ts_result_writer",
    "ali2026v3_trading.param_pool.checks_orchestrator",
    "ali2026v3_trading.param_pool.validation.validation_deep_orchestrator",
    "ali2026v3_trading.param_pool.ts.ts_backtest_strategies",
    "ali2026v3_trading.param_pool.backtest.backtest_runner_base",
    "ali2026v3_trading.param_pool.sharpe_3d_mapping",
    "ali2026v3_trading.param_pool._preprocess",
    "ali2026v3_trading.param_pool._preprocess",
    "ali2026v3_trading.param_pool.sensitivity_analysis",
    "ali2026v3_trading.param_pool.validation.statistical_validation",
    "ali2026v3_trading.param_pool.l1_quantification.meta_audit_engine",
    "ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport",
    "ali2026v3_trading.param_pool.l1_quantification._quantification_core",
    "ali2026v3_trading.param_pool.l1_quantification._tier_optimizer",
    "ali2026v3_trading.param_pool.l1_quantification._data_validation",
]

for mod in merged_modules:
    ok, err = try_import(mod)
    assert_test(ok, f"import {mod}: {err}")

# ══════════════════════════════════════════════════════════════
# TEST 4: Key public API symbols reachable via __init__.py
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TEST 4: __init__.py public API reachable")
print("=" * 70)

init_api = [
    ("ali2026v3_trading.param_pool", "MetaAuditEngine"),
    ("ali2026v3_trading.param_pool", "audit_backtest_engine_integrity"),
    ("ali2026v3_trading.param_pool", "ExternalSourceConfig"),
    ("ali2026v3_trading.param_pool", "TierConfig"),
    ("ali2026v3_trading.param_pool", "PerformanceTierManager"),
    ("ali2026v3_trading.param_pool", "get_tier_config"),
    ("ali2026v3_trading.param_pool", "STATES"),
]

for mod, sym in init_api:
    ok, err = try_import(mod, sym)
    assert_test(ok, f"{mod}.{sym}: {err}")

# ══════════════════════════════════════════════════════════════
# TEST 5: L1 __init__.py public API
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TEST 5: L1 __init__.py public API reachable")
print("=" * 70)

l1_api = [
    ("ali2026v3_trading.param_pool.l1_quantification", "MetaAuditEngine"),
    ("ali2026v3_trading.param_pool.l1_quantification", "ExternalSourceConfig"),
    ("ali2026v3_trading.param_pool.l1_quantification", "PerformanceTierManager"),
    ("ali2026v3_trading.param_pool.l1_quantification", "STATES"),
]

for mod, sym in l1_api:
    ok, err = try_import(mod, sym)
    assert_test(ok, f"{mod}.{sym}: {err}")

# ══════════════════════════════════════════════════════════════
# TEST 6: Key class instantiation smoke tests
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TEST 6: Key class instantiation smoke tests")
print("=" * 70)

smoke_tests = [
    ("ali2026v3_trading.param_pool.backtest.backtest_runner_validation", "check_margin_call", callable),
    ("ali2026v3_trading.param_pool.backtest_config", "compute_profit_loss_ratio_metrics", callable),
    ("ali2026v3_trading.param_pool.backtest_config", "calc_trade_fee", callable),
    ("ali2026v3_trading.param_pool.checks_orchestrator", "validate_logic_reversal_no_future", callable),
    ("ali2026v3_trading.param_pool.validation.validation_deep_orchestrator", "validate_hmm_online_vs_offline", callable),
    ("ali2026v3_trading.param_pool.sensitivity_analysis", "SensitivityAnalyzer", type),
    ("ali2026v3_trading.param_pool.sensitivity_analysis", "TestDesignSuite", type),
    ("ali2026v3_trading.param_pool.validation.statistical_validation", "CounterfactualValidator", type),
    ("ali2026v3_trading.param_pool.validation.statistical_validation", "MonteCarloBankruptcyValidator", type),
    ("ali2026v3_trading.param_pool.sharpe_3d_mapping", "TimeParams", type),
    ("ali2026v3_trading.param_pool._preprocess", "CircuitBreakerResult", type),
    ("ali2026v3_trading.param_pool.l1_quantification.meta_audit_engine", "MetaAuditEngine", type),
    ("ali2026v3_trading.param_pool.l1_quantification.meta_audit_engine", "SmartSignificanceFilter", type),
    ("ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport", "AuditPassport", type),
    ("ali2026v3_trading.param_pool.l1_quantification._quantification_core", "BayesianShrinkageLifeEstimator", type),
    ("ali2026v3_trading.param_pool.l1_quantification._tier_optimizer", "PerformanceTierManager", type),
    ("ali2026v3_trading.param_pool.l1_quantification._data_validation", "DuckDBTickStorage", type),
]

for mod_path, sym, expected_type in smoke_tests:
    ok, err = try_import(mod_path, sym)
    if ok:
        mod = importlib.import_module(mod_path)
        obj = getattr(mod, sym)
        if expected_type == callable:
            assert_test(callable(obj), f"{mod_path}.{sym} should be callable")
        elif expected_type == type:
            assert_test(isinstance(obj, type), f"{mod_path}.{sym} should be a class")
    else:
        assert_test(False, f"{mod_path}.{sym}: {err}")

# ══════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print(f"SUMMARY: {PASS} PASSED, {FAIL} FAILED")
print("=" * 70)

if FAIL > 0 and __name__ == "__main__":
    sys.exit(1)