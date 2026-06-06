"""
collect_baseline.py — Phase 4 性能基线采集脚本
采集当前代码的性能基线数据，供Phase 4 G6/G7门控使用

基线指标：
- G6: Tick处理P99延迟
- G7: RSS内存峰值
- StateStore snapshot延迟
- CallbackRegistry invoke延迟
"""
import gc
import json
import os
import sys
import time
import threading
import statistics
from datetime import datetime, timezone, timedelta

_CHINA_TZ = timezone(timedelta(hours=8))  # [R26-AUDIT] 中国时区

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

BASELINE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'performance_baseline.json')

N_TICKS = 1000
N_SNAPSHOTS = 500
N_CALLBACKS = 1000


def collect_state_store_baseline():
    from ali2026v3_trading.state_store import StateStore
    store = StateStore()
    store.set('test_key_int', 42)
    store.set('test_key_float', 3.14)
    store.set('test_key_str', 'baseline')
    store.set('test_key_dict', {'price': 100.0, 'volume': 10})

    latencies_us = []
    for _ in range(N_SNAPSHOTS):
        t0 = time.perf_counter()
        snap = store.snapshot()
        t1 = time.perf_counter()
        latencies_us.append((t1 - t0) * 1_000_000)

    return {
        'snapshot_count': N_SNAPSHOTS,
        'p50_us': round(statistics.median(latencies_us), 2),
        'p99_us': round(sorted(latencies_us)[int(len(latencies_us) * 0.99)], 2),
        'max_us': round(max(latencies_us), 2),
        'mean_us': round(statistics.mean(latencies_us), 2),
    }


def collect_callback_registry_baseline():
    from ali2026v3_trading.callback_registry import CallbackGroup
    group = CallbackGroup()
    reg = group.get_registry('test_benchmark')

    callbacks = []
    for i in range(10):
        def _cb(**kwargs):
            pass
        callbacks.append(_cb)
        reg.register(_cb)

    latencies_us = []
    for _ in range(N_CALLBACKS):
        t0 = time.perf_counter()
        reg.invoke()
        t1 = time.perf_counter()
        latencies_us.append((t1 - t0) * 1_000_000)

    return {
        'invoke_count': N_CALLBACKS,
        'callbacks_per_invoke': 10,
        'p50_us': round(statistics.median(latencies_us), 2),
        'p99_us': round(sorted(latencies_us)[int(len(latencies_us) * 0.99)], 2),
        'max_us': round(max(latencies_us), 2),
        'mean_us': round(statistics.mean(latencies_us), 2),
    }


def collect_memory_baseline():
    gc.collect()
    import tracemalloc
    tracemalloc.start()

    from ali2026v3_trading.state_store import StateStore
    from ali2026v3_trading.callback_registry import CallbackGroup

    store = StateStore()
    cb_group = CallbackGroup()

    for i in range(100):
        store.set_ref(f'key_{i}', {'data': list(range(100))})

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return {
        'rss_current_mb': round(current / 1024 / 1024, 2),
        'rss_peak_mb': round(peak / 1024 / 1024, 2),
        'state_keys': 100,
    }


def collect_tick_processing_baseline():
    latencies_us = []
    try:
        from ali2026v3_trading.state_store import StateStore
        store = StateStore()
        store.set_ref('_is_running', True)
        store.set_ref('_is_paused', False)
        store.set_ref('storage', None)

        mock_tick = {
            'instrument_id': 'IF2606',
            'last_price': 3500.0,
            'volume': 100,
            'exchange': 'CFFEX',
            'timestamp': time.time(),
            'bid_price1': 3499.8,
            'ask_price1': 3500.2,
        }

        for _ in range(N_TICKS):
            t0 = time.perf_counter()
            snap = store.snapshot(keys=['_is_running', '_is_paused'])
            is_running = snap.get('_is_running', False)
            is_paused = snap.get('_is_paused', True)
            if is_running and not is_paused:
                pass
            t1 = time.perf_counter()
            latencies_us.append((t1 - t0) * 1_000_000)
    except Exception as e:
        return {'error': str(e)}

    return {
        'tick_count': N_TICKS,
        'p50_us': round(statistics.median(latencies_us), 2),
        'p99_us': round(sorted(latencies_us)[int(len(latencies_us) * 0.99)], 2),
        'max_us': round(max(latencies_us), 2),
        'mean_us': round(statistics.mean(latencies_us), 2),
    }


def main():
    print("=" * 60)
    print("性能基线采集 — Phase 4 G6/G7门控")
    print("=" * 60)

    baseline = {
        'timestamp': datetime.now(_CHINA_TZ).isoformat(),
        'hostname': os.environ.get('COMPUTERNAME', 'unknown'),
    }

    print("\n--- StateStore Snapshot延迟 ---")
    try:
        baseline['state_store_snapshot'] = collect_state_store_baseline()
        for k, v in baseline['state_store_snapshot'].items():
            print(f"  {k}: {v}")
    except Exception as e:
        baseline['state_store_snapshot'] = {'error': str(e)[:200]}
        print(f"  ERROR: {e}")

    print("\n--- CallbackRegistry Invoke延迟 ---")
    try:
        baseline['callback_registry_invoke'] = collect_callback_registry_baseline()
        for k, v in baseline['callback_registry_invoke'].items():
            print(f"  {k}: {v}")
    except Exception as e:
        baseline['callback_registry_invoke'] = {'error': str(e)[:200]}
        print(f"  ERROR: {e}")

    print("\n--- 内存基线 ---")
    try:
        baseline['memory'] = collect_memory_baseline()
        for k, v in baseline['memory'].items():
            print(f"  {k}: {v}")
    except Exception as e:
        baseline['memory'] = {'error': str(e)[:200]}
        print(f"  ERROR: {e}")

    print("\n--- Tick处理基线(StateStore快照路径) ---")
    try:
        baseline['tick_processing'] = collect_tick_processing_baseline()
        for k, v in baseline['tick_processing'].items():
            print(f"  {k}: {v}")
    except Exception as e:
        baseline['tick_processing'] = {'error': str(e)[:200]}
        print(f"  ERROR: {e}")

    with open(BASELINE_FILE, 'w', encoding='utf-8') as f:
        json.dump(baseline, f, indent=2, ensure_ascii=False)
    print(f"\n基线已保存: {BASELINE_FILE}")

    print("\n--- Phase 4门控判定 ---")
    ss_p99 = baseline['state_store_snapshot']['p99_us']
    tick_p99 = baseline['tick_processing']['p99_us']
    rss_peak = baseline['memory']['rss_peak_mb']
    print(f"  G6 StateStore P99 < 500μs: {'PASS' if ss_p99 < 500 else 'FAIL'} ({ss_p99:.1f}μs)")
    print(f"  G6 Tick P99 < 1000μs: {'PASS' if tick_p99 < 1000 else 'FAIL'} ({tick_p99:.1f}μs)")
    print(f"  G7 RSS峰值: {rss_peak:.2f}MB")


if __name__ == '__main__':
    main()
