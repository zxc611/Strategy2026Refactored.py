# MODULE_ID: M2-291
"""运行选定的测试以验证修复"""
import subprocess
import sys

tests_to_run = [
    "test_coverage_boost_25modules.py::TestSortEntry::test_sort_order_sync_flag_priority",
    "test_coverage_boost_25modules.py::TestCircuitBreakerServiceDeep::test_is_trading_paused_during_pause",
    "test_coverage_boost_batch2.py::TestMetricsRegistry::test_observe_histogram",
    "test_coverage_boost_lifecycle.py::TestLifecycleManager::test_lock_property",
    "test_safety_meta_circuit.py::TestCircuitBreakerServiceDeep::test_is_trading_paused_during_pause",
    "test_coverage_boost_risk_infra.py::TestDrawdownMonitorService::test_daily_drawdown_triggers_hard_stop",
    "test_param_pool_low_cov.py::TestOptimizationPhaseScan::test_gate_check_result",
]

for test in tests_to_run:
    print(f"\n{'='*60}")
    print(f"Running: {test}")
    print('='*60)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", f"ali2026v3_trading/tests/{test}", "-xvs", "--tb=short"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode == 0:
        print("✓ PASSED")
    else:
        print("✗ FAILED")
        print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
        print(result.stderr[-500:] if len(result.stderr) > 500 else result.stderr)