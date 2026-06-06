"""P1-R1: order/子系统目录重组PoC"""
import os
import shutil

root = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'
order_dir = os.path.join(root, 'order')

moved = []
for f in os.listdir(root):
    if f.startswith('order_') and f.endswith('.py'):
        src = os.path.join(root, f)
        dst = os.path.join(order_dir, f)
        shutil.move(src, dst)
        moved.append(f)

print(f"Moved {len(moved)} order files to order/:")
for f in sorted(moved):
    print(f"  {f}")