
import os, shutil

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
bk = os.path.join(base, "backup_r13_fix_20260524")
os.makedirs(bk, exist_ok=True)

files = [
    "box_spring_strategy.py", "strategy_core_service.py", "risk_service.py",
    "order_service.py", "position_service.py", "shadow_strategy_engine.py",
    "mode_engine.py", "greeks_calculator.py", "signal_service.py",
    "strategy_ecosystem.py", "service_container.py", "config_service.py",
    "event_bus.py", "health_check_api.py", "plr_calculator.py",
    "data_service.py", "config_params.py", "subscription_manager.py",
    "governance_engine.py", "strategy_lifecycle_mixin.py", "state_param_manager.py"
]

total_lines = 0
for f in files:
    src = os.path.join(base, f)
    dst = os.path.join(bk, f)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        with open(src, 'r', encoding='utf-8', errors='ignore') as fh:
            lines = len(fh.readlines())
        total_lines += lines
        print(f"  Backed up: {f} ({lines} lines)")
    else:
        print(f"  NOT FOUND: {f}")

print(f"\nBackup complete: {len(files)} files, {total_lines} total lines")
