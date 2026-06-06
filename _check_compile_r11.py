import ast
import sys
try:
    with open(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\参数池\task_scheduler.py', 'r', encoding='utf-8') as f:
        source = f.read()
    ast.parse(source)
    print("ast.parse PASSED - syntax OK")
except SyntaxError as e:
    print(f"SyntaxError: {e}")
    sys.exit(1)
