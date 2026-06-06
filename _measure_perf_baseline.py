"""P0-S0d: 性能基线测量 - 系统模块导入时间100次取P95"""
import time
import statistics
import json
import os

CORE_MODULES = [
    'ali2026v3_trading.signal_service',
    'ali2026v3_trading.signal_generator',
    'ali2026v3_trading.config_service',
    'ali2026v3_trading.lifecycle_manager',
    'ali2026v3_trading.lifecycle_state_machine',
    'ali2026v3_trading.lifecycle_init',
    'ali2026v3_trading.lifecycle_callbacks',
    'ali2026v3_trading.order_service',
    'ali2026v3_trading.order_executor',
    'ali2026v3_trading.strategy_core_service',
    'ali2026v3_trading.risk_service_core',
    'ali2026v3_trading.maintenance_service',
    'ali2026v3_trading.shared_utils',
]

N = 100
results = {}

for mod_name in CORE_MODULES:
    times = []
    for i in range(N):
        if mod_name in __import__('sys').modules:
            del __import__('sys').modules[mod_name]
        t0 = time.perf_counter()
        try:
            __import__(mod_name)
        except Exception:
            pass
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)
    p50 = statistics.median(times)
    p95 = sorted(times)[int(N * 0.95)]
    p99 = sorted(times)[int(N * 0.99)]
    mean = statistics.mean(times)
    results[mod_name] = {
        'mean_ms': round(mean, 3),
        'p50_ms': round(p50, 3),
        'p95_ms': round(p95, 3),
        'p99_ms': round(p99, 3),
        'n': N,
    }
    print(f"{mod_name}: mean={mean:.3f}ms p50={p50:.3f}ms p95={p95:.3f}ms p99={p99:.3f}ms")

total_import_times = []
for i in range(N):
    for mod_name in CORE_MODULES:
        if mod_name in __import__('sys').modules:
            del __import__('sys').modules[mod_name]
    t0 = time.perf_counter()
    for mod_name in CORE_MODULES:
        try:
            __import__(mod_name)
        except Exception:
            pass
    t1 = time.perf_counter()
    total_import_times.append((t1 - t0) * 1000)

total_p50 = statistics.median(total_import_times)
total_p95 = sorted(total_import_times)[int(N * 0.95)]
total_mean = statistics.mean(total_import_times)
results['_total_core_import'] = {
    'mean_ms': round(total_mean, 3),
    'p50_ms': round(total_p50, 3),
    'p95_ms': round(total_p95, 3),
    'n': N,
}
print(f"\nTotal core import: mean={total_mean:.3f}ms p50={total_p50:.3f}ms p95={total_p95:.3f}ms")

baseline = {
    'date': '2026-06-06',
    'measurement': 'module_import_time_ms',
    'n_runs': N,
    'modules': results,
}

out_dir = os.path.join('archive', 'perf_baseline')
os.makedirs(out_dir, exist_ok=True)
with open(os.path.join(out_dir, 'perf_baseline_20260606.json'), 'w', encoding='utf-8') as f:
    json.dump(baseline, f, indent=2, ensure_ascii=False)

print(f"\nBaseline saved to {out_dir}/perf_baseline_20260606.json")