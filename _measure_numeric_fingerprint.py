"""P0-S0e: 数值指纹基线 - 核心策略回测指标截断6位有效数字"""
import json
import os
import math
import hashlib
from datetime import datetime

def truncate_sig(x, sig=6):
    if x == 0:
        return 0.0
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)

def file_hash(filepath):
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()[:16]

baseline = {
    'date': '2026-06-06',
    'method': 'truncate_6_sig_digits',
    'note': 'Production backtest requires InfiniTrader platform; this baseline captures module fingerprints and config hashes',
}

core_files = [
    'signal_service.py', 'signal_generator.py', 'config_service.py',
    'lifecycle_manager.py', 'lifecycle_state_machine.py', 'lifecycle_init.py',
    'lifecycle_callbacks.py', 'order_service.py', 'order_executor.py',
    'strategy_core_service.py', 'risk_service_core.py', 'maintenance_service.py',
    'strategy_2026.py', 'lifecycle_state.py',
]

file_fingerprints = {}
for f in core_files:
    if os.path.exists(f):
        file_fingerprints[f] = {
            'sha256_prefix': file_hash(f),
            'size_bytes': os.path.getsize(f),
        }

baseline['file_fingerprints'] = file_fingerprints

config_files = []
config_dir = 'config'
if os.path.isdir(config_dir):
    for f in os.listdir(config_dir):
        fp = os.path.join(config_dir, f)
        if os.path.isfile(fp):
            config_files.append(fp)
            baseline.setdefault('config_hashes', {})[f] = file_hash(fp)

baseline['config_files_count'] = len(config_files)

placeholder_metrics = {
    'sharpe': truncate_sig(1.234567),
    'max_drawdown': truncate_sig(-0.0854321),
    'win_rate': truncate_sig(0.543210),
    'daily_returns_corr': truncate_sig(0.99995),
    'note': 'PLACEHOLDER - requires live backtest run on InfiniTrader platform to populate real values',
}
baseline['backtest_metrics'] = placeholder_metrics

out_dir = os.path.join('archive', 'numeric_fingerprint')
os.makedirs(out_dir, exist_ok=True)
with open(os.path.join(out_dir, 'numeric_fingerprint_baseline_20260606.json'), 'w', encoding='utf-8') as f:
    json.dump(baseline, f, indent=2, ensure_ascii=False)

print(f"Numeric fingerprint baseline saved to {out_dir}/numeric_fingerprint_baseline_20260606.json")
print(f"Core files fingerprinted: {len(file_fingerprints)}")
print(f"Config files hashed: {len(config_files)}")
print("NOTE: Backtest metrics are PLACEHOLDER - real values require InfiniTrader platform backtest")