#!/usr/bin/env python3
"""导入链完整性检查"""
import sys, os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

CRITICAL_MODULES = [
    'infra.shared_utils',
    'infra.resilience',
    'infra.health_monitor',
    'infra.event_bus',
    'config.config_service',
    'lifecycle.lifecycle_state_machine',
    'strategy_judgment.judgment_scoring_helpers',
    'evaluation.cascade_judge',
    'governance.governance_engine',
    'risk.risk_service',
    'order.order_service',
    'position.position_service',
    'strategy.shadow_strategy_facade',
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
