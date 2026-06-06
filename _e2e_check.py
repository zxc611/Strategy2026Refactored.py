import sys, subprocess
sys.path.insert(0, '..')
modules = [
    "ali2026v3_trading.maintenance_service",
    "ali2026v3_trading.lifecycle_manager",
    "ali2026v3_trading.lifecycle_bind",
    "ali2026v3_trading.lifecycle_callbacks",
    "ali2026v3_trading.diagnosis_probe",
    "ali2026v3_trading.risk_circuit_breaker",
    "ali2026v3_trading.signal_filter_chain",
]
for mod in modules:
    r = subprocess.run(
        [sys.executable, '-c', f'import sys; sys.path.insert(0,".."); import {mod}; print("OK")'],
        capture_output=True, text=True, timeout=30
    )
    ok = 'OK' in r.stdout
    print(f'{mod}: {"PASS" if ok else "FAIL"}')
    if not ok:
        err_lines = [l for l in (r.stderr + r.stdout).split('\n') if 'ImportError' in l or 'ModuleNotFoundError' in l or 'NameError' in l]
        if err_lines:
            print(f'  Error: {err_lines[0][:150]}')