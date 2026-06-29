#!/usr/bin/env python3
"""导入链完整性检查"""
import sys, os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

CRITICAL_MODULES = [
    'ali2026v3_trading.infra.shared_utils',
    'ali2026v3_trading.infra.resilience',
    'ali2026v3_trading.infra.health_monitor',
    'ali2026v3_trading.infra.event_bus',
    'ali2026v3_trading.config.config_service',
    'ali2026v3_trading.lifecycle.lifecycle_state_machine',
    'ali2026v3_trading.strategy_judgment.judgment_scoring_helpers',
    'ali2026v3_trading.evaluation.cascade_judge',
    'ali2026v3_trading.governance.governance_engine',
    'ali2026v3_trading.risk.risk_service',
    'ali2026v3_trading.order.order_service',
    'ali2026v3_trading.position.position_service',
    'ali2026v3_trading.strategy.shadow_strategy_facade',
]

def check():
    errors = []
    for mod in CRITICAL_MODULES:
        try:
            __import__(mod)
        except ImportError as e:
            errors.append(f"{mod}: {e}")
    return errors

def main():
    errors = check()
    if errors:
        print(f"发现 {len(errors)} 个导入失败:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print(f"全部 {len(CRITICAL_MODULES)} 个关键模块导入成功")
    return 0

if __name__ == '__main__':
    sys.exit(main())
