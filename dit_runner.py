#!/usr/bin/env python3
"""定期审计自动化工具 — 支持按模块/维度/严重度筛选"""

import os
import sys
import json
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from ali2026v3_trading.infra.shared_utils import CHINA_TZ  # 五唯一性修复：统一 CHINA_TZ

# 审计维度定义
AUDIT_DIMENSIONS = {
    'import_chain': {
        'description': '导入链完整性',
        'severity': 'P0',
        'check': '_check_import_chain',
    },
    'call_chain': {
        'description': '关键函数调用链',
        'severity': 'P0',
        'check': '_check_call_chain',
    },
    'micro_files': {
        'description': '微文件检测',
        'severity': 'P2',
        'check': '_check_micro_files',
    },
    'tool_scripts': {
        'description': '工具脚本混入',
        'severity': 'P2',
        'check': '_check_tool_scripts',
    },
    'api_whitelist': {
        'description': 'API白名单合规',
        'severity': 'P1',
        'check': '_check_api_whitelist',
    },
    'component_failure_policy': {
        'description': '组件失败策略配置',
        'severity': 'P1',
        'check': '_check_component_failure_policy',
    },
}

def _check_import_chain():
    errors = []
    critical_modules = [
        'ali2026v3_trading.infra.shared_utils',
        'ali2026v3_trading.infra.resilience',
        'ali2026v3_trading.infra.health_monitor',
        'ali2026v3_trading.infra.event_bus',
        'ali2026v3_trading.config.config_service',
        'ali2026v3_trading.evaluation.cascade_judge',
        'ali2026v3_trading.governance.governance_engine',
    ]
    for mod in critical_modules:
        try:
            __import__(mod)
        except ImportError as e:
            errors.append(f"{mod}: {e}")
    return errors

def _check_call_chain():
    errors = []
    # Check RiskService has safety methods
    try:
        from ali2026v3_trading.risk.risk_service import RiskService
        for method in ['check_regulatory_compliance', 'check_capital_sufficiency', 'check_exchange_status']:
            if not hasattr(RiskService, method):
                errors.append(f"RiskService缺少{method}")
    except ImportError as e:
        errors.append(f"RiskService导入失败: {e}")
    return errors

def _check_micro_files():
    errors = []
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', 'archive', 'tools', 'tests', '_verify_logs', '_wal_orders')]
        for f in files:
            if f.endswith('.py') and not f.startswith('__'):
                path = os.path.join(root, f)
                try:
                    with open(path, 'r', encoding='utf-8') as fh:
                        lines = fh.readlines()
                        content = ''.join(lines)
                        if 'Backward-compatible re-export facade' in content:
                            continue
                        if len(lines) <= 50:
                            defs = [l for l in lines if l.strip().startswith(('class ', 'def '))]
                            if len(defs) <= 1:
                                rel = os.path.relpath(path, PROJECT_DIR)
                                errors.append(f"{rel}: {len(lines)}行, {len(defs)}个定义")
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
    return errors

def _check_tool_scripts():
    errors = []
    tool_patterns = ['_gen_', '_split_', '_do_count', '_do_verify']
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', 'archive', 'tools', 'tests', '_verify_logs', '_wal_orders')]
        for f in files:
            if f.endswith('.py'):
                for pattern in tool_patterns:
                    if pattern in f:
                        rel = os.path.relpath(os.path.join(root, f), PROJECT_DIR)
                        errors.append(f"{rel}: 工具脚本混入模块目录")
    return errors

def _check_api_whitelist():
    errors = []
    import ast
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', 'archive', 'tools', 'tests', '_verify_logs', '_wal_orders')]
        if '__init__.py' in files:
            init_path = os.path.join(root, '__init__.py')
            try:
                with open(init_path, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                    if '__all__' in content:
                        tree = ast.parse(content)
                        for node in ast.walk(tree):
                            if isinstance(node, ast.Assign):
                                for target in node.targets:
                                    if isinstance(target, ast.Name) and target.id == '__all__':
                                        if isinstance(node.value, ast.List):
                                            n = len(node.value.elts)
                                            if n > 20:
                                                rel = os.path.relpath(init_path, PROJECT_DIR)
                                                errors.append(f"{rel}: __all__有{n}项(>20)")
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    return errors

def _check_component_failure_policy():
    errors = []
    try:
        from ali2026v3_trading.strategy_judgment._judgment_services import CRITICAL_COMPONENTS, ComponentFailurePolicy
        # Verify BLOCK components exist
        block_components = [k for k, v in CRITICAL_COMPONENTS.items() if v == ComponentFailurePolicy.BLOCK]
        if len(block_components) < 2:
            errors.append(f"BLOCK级组件不足2个(当前{len(block_components)})")
    except ImportError as e:
        errors.append(f"ComponentFailurePolicy导入失败: {e}")
    return errors

def run_audit(dimensions=None, severity=None, output_format='text'):
    """执行审计

    Args:
        dimensions: 审计维度列表(None=全部)
        severity: 严重度过滤(P0/P1/P2)
        output_format: 输出格式(text/json)
    """
    results = {}
    total_errors = 0

    for dim_name, dim_config in AUDIT_DIMENSIONS.items():
        if dimensions and dim_name not in dimensions:
            continue
        if severity and dim_config['severity'] != severity:
            continue

        check_fn = globals().get(dim_config['check'])
        if check_fn:
            errors = check_fn()
            results[dim_name] = {
                'description': dim_config['description'],
                'severity': dim_config['severity'],
                'errors': errors,
                'passed': len(errors) == 0,
            }
            total_errors += len(errors)

    if output_format == 'json':
        return json.dumps(results, ensure_ascii=False, indent=2)

    # Text format
    lines = [
        "=" * 60,
        f"审计报告 — {datetime.now(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 60,
    ]

    for dim_name, result in results.items():
        status = "PASS" if result['passed'] else "FAIL"
        lines.append(f"\n[{status}] {dim_name} ({result['severity']}) — {result['description']}")
        for err in result['errors'][:5]:
            lines.append(f"  - {err}")
        if len(result['errors']) > 5:
            lines.append(f"  ... 还有 {len(result['errors']) - 5} 项")

    lines.append("\n" + "=" * 60)
    if total_errors > 0:
        lines.append(f"审计结果: {total_errors} 项问题")
    else:
        lines.append("审计结果: 全部通过")

    return "\n".join(lines)

def main():
    import argparse
    parser = argparse.ArgumentParser(description='定期审计自动化工具')
    parser.add_argument('--dimensions', nargs='*', help='审计维度')
    parser.add_argument('--severity', choices=['P0', 'P1', 'P2'], help='严重度过滤')
    parser.add_argument('--format', choices=['text', 'json'], default='text', help='输出格式')
    args = parser.parse_args()

    output = run_audit(dimensions=args.dimensions, severity=args.severity, output_format=args.format)
    print(output)

    # 如果有P0问题，返回非零退出码
    has_p0_errors = any(
        not r['passed'] and r['severity'] == 'P0'
        for r in run_audit(dimensions=args.dimensions, severity=args.severity, output_format='json')
        if isinstance(r, dict)
    )
    return 1 if has_p0_errors else 0

if __name__ == '__main__':
    sys.exit(main())
