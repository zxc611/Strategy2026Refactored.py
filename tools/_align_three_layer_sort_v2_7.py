"""SIM→LIVE 同步并校验三层期权五态排序 v2.5-v2.8 核心文件。"""
import hashlib
import os
import shutil
import sys

SIM_BASE = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo'
LIVE_BASE = r'C:\Users\xu\AppData\Roaming\InfiniTrader_QhZijintianfengPythonX64\pyStrategy\demo'

FILES = [
    'ali2026v3_trading/config/tvf_param_loader.py',
    'ali2026v3_trading/config/params.yaml',
    'ali2026v3_trading/config/params_default.json',
    'ali2026v3_trading/data/three_layer_sort/__init__.py',
    'ali2026v3_trading/data/three_layer_sort/signal_source_a.py',
    'ali2026v3_trading/data/three_layer_sort/resonance_engine.py',
    'ali2026v3_trading/data/ds_schema_manager.py',
    'ali2026v3_trading/data/width_cache_query_mixin.py',
    'ali2026v3_trading/data/width_cache_state_mixin.py',
    'ali2026v3_trading/param_pool/_param_grids.py',
    'ali2026v3_trading/strategy/strategy_scheduler.py',
    'ali2026v3_trading/tests/test_three_layer_sort_v2_7.py',
    'ali2026v3_trading/docs/audit/三层期权五态排序方案_最终落地方案V2_20260624.md',
]


def md5(path: str) -> str:
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def main() -> int:
    all_ok = True
    for f in FILES:
        sim_path = os.path.join(SIM_BASE, f)
        live_path = os.path.join(LIVE_BASE, f)
        if not os.path.exists(sim_path):
            print(f'MISSING_SIM {f}')
            all_ok = False
            continue
        os.makedirs(os.path.dirname(live_path), exist_ok=True)
        shutil.copy2(sim_path, live_path)
        sim_md5 = md5(sim_path)
        live_md5 = md5(live_path)
        match = sim_md5 == live_md5
        if not match:
            all_ok = False
        status = 'OK' if match else 'MISMATCH'
        print(f'{status} {sim_md5} {live_md5} {f}')
    if all_ok:
        print('ALL_MATCH')
        return 0
    print('SOME_MISMATCH')
    return 1


if __name__ == '__main__':
    sys.exit(main())
