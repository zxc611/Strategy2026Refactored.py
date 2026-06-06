"""P1-R1: 创建order模块根目录重导出"""
import os

root = r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading'

order_modules = [
    'order_base', 'order_chase_service', 'order_compliance', 'order_executor',
    'order_flow_analyzer', 'order_flow_bridge', 'order_market_impact', 'order_persistence',
    'order_platform_auth', 'order_risk_guard', 'order_service', 'order_split_models',
    'order_state_manager', 'order_sync', 'order_wal_state_service',
]

created = []
for mod in order_modules:
    filepath = os.path.join(root, f'{mod}.py')
    content = f'"""{mod}.py — 重导出模块(P1-R1目录重组后)"""\nfrom ali2026v3_trading.order.{mod} import *\n'
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    created.append(mod)

print(f"Created {len(created)} re-export modules in root:")
for mod in sorted(created):
    print(f"  {mod}.py")