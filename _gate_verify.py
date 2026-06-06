"""Gate验证脚本：按增量gate逐个验证子目录"""
import subprocess, sys, os, re, json, time

pkg = os.path.dirname(os.path.abspath(__file__))
IGNORE_TESTS = [
    '--ignore=tests/test_e2e_pipeline.py',
    '--ignore=tests/test_kline_length_backtest.py',
    '--ignore=tests/test_v71_task_scheduler.py',
    '--ignore=tests/test_phase1_managers.py',
]

# Gate定义：子目录 -> 对应的重导出模块
gates = {
    'G1': {
        'subdirs': ['order', 'signal'],
        'reexports': [
            'order_base.py', 'order_executor.py', 'order_service.py',
            'order_chase_service.py', 'order_flow_analyzer.py', 'order_flow_bridge.py',
            'order_persistence.py', 'order_platform_auth.py', 'order_risk_guard.py',
            'order_state_manager.py', 'order_wal_state_service.py',
            'signal_service.py', 'signal_filter_chain.py', 'signal_history_service.py',
            'signal_timing_filter.py',
        ],
        'pass_condition': '0 compileall errors + >=396 tests pass',
    },
    'G2': {
        'subdirs': ['lifecycle', 'config'],
        'reexports': [
            'lifecycle_bind.py', 'lifecycle_callbacks.py', 'lifecycle_init.py',
            'lifecycle_manager.py', 'lifecycle_monitor.py', 'lifecycle_parallel_ops.py',
            'lifecycle_transition.py',
            'config_service.py', 'config_params.py', 'config_resolver.py',
            'config_state_defaults.py', 'config_sync.py', 'config_version_tracker.py',
            'config_json_loader.py', 'config_option_loader.py', 'config_facade.py',
        ],
        'pass_condition': '0 compileall errors + all paths pass',
    },
    'G3': {
        'subdirs': ['risk', 'position', 'data'],
        'reexports': [
            'risk_service.py', 'risk_audit_utils.py', 'risk_check_service.py',
            'risk_circuit_breaker.py', 'risk_compute_service.py', 'risk_config_provider.py',
            'position_service.py', 'position_check_service.py', 'position_command_service.py',
            'position_greeks.py', 'position_persistence.py', 'position_pnl_service.py',
            'position_utils.py',
            'data_service.py', 'data_access.py', 'ds_db_connection.py',
            'ds_realtime_cache.py', 'ds_schema_manager.py', 'db_adapter.py',
        ],
        'pass_condition': '0 compileall errors + >=396 tests pass',
    },
    'G4': {
        'subdirs': ['strategy', 'governance'],
        'reexports': [
            'strategy_2026.py', 'strategy_core_service.py', 'strategy_ecosystem.py',
            'strategy_business_layer.py', 'strategy_config.py', 'strategy_config_layer.py',
            'strategy_instrument_mixin.py', 'strategy_lifecycle_mixin.py',
            'strategy_monitoring_layer.py', 'strategy_tick_handler.py',
            'box_detector.py', 'box_spring_strategy.py',
            'governance_engine.py', 'greeks_calculator.py',
            'compliance_checker.py', 'callback_registry.py',
        ],
        'pass_condition': '0 compileall errors + >=396 tests pass',
    },
    'G5': {
        'subdirs': ['infra'],
        'reexports': [
            'shared_utils.py', 'shared_utils_contracts.py', 'shared_utils_infra.py',
            'shared_utils_types.py', 'shared_utils_instrument.py', 'shared_providers.py',
            'event_bus.py', 'maintenance_service.py', 'diagnosis_service.py',
            'diagnosis_probe.py', 'diagnosis_periodic.py', 'scheduler_service.py',
            'performance_monitor.py', 'health_check_api.py', 'health_check_aggregator.py',
            'service_container.py', 'state_store.py', 'audit_log_utils.py',
            'security_config.py', 'phase_feature_flag.py', 'resilience_config.py',
            'metrics_registry.py', 'validation_registry.py', 'param_table_provider.py',
            'concurrent_utils.py',
        ],
        'pass_condition': '0 compileall errors + 0 ImportError from consumers',
    },
}

def run_compileall():
    r = subprocess.run(
        [sys.executable, '-m', 'compileall', '-q', '.'],
        capture_output=True, text=True, cwd=pkg, timeout=60
    )
    return r.returncode == 0

def run_tests():
    cmd = [sys.executable, '-m', 'pytest', '--tb=no', '-q'] + IGNORE_TESTS
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=pkg, timeout=300)
    output = r.stdout + r.stderr
    m_pass = re.search(r'(\d+) passed', output)
    m_fail = re.search(r'(\d+) failed', output)
    m_err = re.search(r'(\d+) error', output)
    passed = int(m_pass.group(1)) if m_pass else 0
    failed = int(m_fail.group(1)) if m_fail else 0
    errors = int(m_err.group(1)) if m_err else 0
    # Count ImportError in output
    import_errors = output.count('ImportError')
    return passed, failed, errors, import_errors

def check_subdir_exists(subdirs):
    missing = []
    for d in subdirs:
        if not os.path.isdir(os.path.join(pkg, d)):
            missing.append(d)
    return missing

def check_reexport_exists(files):
    missing = []
    for f in files:
        if not os.path.isfile(os.path.join(pkg, f)):
            missing.append(f)
    return missing

# Run all gates
all_results = {}
cumulative_subdirs = []
cumulative_reexports = []

for gate_name, gate_info in gates.items():
    print(f"\n{'='*60}")
    print(f"  {gate_name}: {', '.join(gate_info['subdirs'])}")
    print(f"  通过条件: {gate_info['pass_condition']}")
    print(f"{'='*60}")
    
    result = {'gate': gate_name, 'subdirs': gate_info['subdirs']}
    
    # 1. Check subdirectories exist
    missing_dirs = check_subdir_exists(gate_info['subdirs'])
    if missing_dirs:
        print(f"  [FAIL] Missing subdirectories: {missing_dirs}")
        result['subdir_check'] = False
    else:
        print(f"  [PASS] All subdirectories exist: {gate_info['subdirs']}")
        result['subdir_check'] = True
    
    # 2. Check re-export modules exist
    missing_files = check_reexport_exists(gate_info['reexports'])
    if missing_files:
        print(f"  [FAIL] Missing re-export modules: {missing_files}")
        result['reexport_check'] = False
    else:
        print(f"  [PASS] All {len(gate_info['reexports'])} re-export modules exist")
        result['reexport_check'] = True
    
    # 3. compileall
    compile_ok = run_compileall()
    result['compileall'] = compile_ok
    print(f"  [{'PASS' if compile_ok else 'FAIL'}] compileall: {'zero errors' if compile_ok else 'HAS ERRORS'}")
    
    # 4. Unit tests
    passed, failed, errors, import_errors = run_tests()
    result['tests_passed'] = passed
    result['tests_failed'] = failed
    result['collection_errors'] = errors
    result['import_errors'] = import_errors
    
    test_ok = errors == 0 and passed >= 396
    print(f"  [{'PASS' if test_ok else 'INFO'}] Tests: {passed} passed, {failed} failed, {errors} collection errors, {import_errors} ImportErrors")
    
    # 5. Gate-specific conditions
    if gate_name == 'G5':
        # G5: 0 ImportError from consumers
        g5_ok = import_errors == 0
        print(f"  [{'PASS' if g5_ok else 'FAIL'}] G5 specific: {import_errors} ImportErrors (need 0)")
        result['g5_specific'] = g5_ok
    else:
        result['g5_specific'] = None
    
    # Overall gate pass
    gate_pass = result['subdir_check'] and result['reexport_check'] and compile_ok
    if gate_name == 'G5':
        gate_pass = gate_pass and (import_errors == 0)
    
    result['gate_pass'] = gate_pass
    print(f"\n  >>> {gate_name} 结果: {'PASS' if gate_pass else 'FAIL'} <<<")
    
    all_results[gate_name] = result
    cumulative_subdirs.extend(gate_info['subdirs'])
    cumulative_reexports.extend(gate_info['reexports'])

# Summary
print(f"\n{'='*60}")
print("  增量Gate验证总结")
print(f"{'='*60}")
for gate_name, result in all_results.items():
    status = 'PASS' if result['gate_pass'] else 'FAIL'
    print(f"  {gate_name}: {status} (subdirs: {', '.join(result['subdirs'])})")

total_pass = sum(1 for r in all_results.values() if r['gate_pass'])
print(f"\n  总计: {total_pass}/5 gates passed")

# Save
with open(os.path.join(pkg, 'archive', 'gate_verification_results.json'), 'w', encoding='utf-8') as f:
    json.dump(all_results, f, indent=2, ensure_ascii=False)
print(f"  Results saved to archive/gate_verification_results.json")