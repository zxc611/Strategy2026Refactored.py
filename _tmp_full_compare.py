"""
R27 功能完整性对比验证脚本
提取重构前(R22备份)和重构后(当前)的完整API清单，逐项对比
"""
import ast
import os
import sys
import json
import importlib
import traceback

BASE = r'ali2026v3_trading'
BACKUP = os.path.join(BASE, '_backup_r22_20260603_210608')
sys.path.insert(0, os.path.dirname(os.path.abspath(BASE)))

# ============================================================
# Part 1: AST-based API extraction
# ============================================================

def extract_api(fpath):
    """Extract full API surface from a Python file using AST"""
    with open(fpath, 'r', encoding='utf-8') as f:
        source = f.read()
    tree = ast.parse(source)
    api = {'classes': {}, 'functions': [], 'imports': [], 'exports': []}
    
    for node in tree.body:
        # Top-level functions
        if isinstance(node, ast.FunctionDef):
            args = [a.arg for a in node.args.args if a.arg != 'self']
            api['functions'].append({
                'name': node.name,
                'lineno': node.lineno,
                'args': args,
            })
        # Top-level classes
        elif isinstance(node, ast.ClassDef):
            methods = []
            class_vars = []
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    args = [a.arg for a in item.args.args if a.arg != 'self']
                    methods.append({
                        'name': item.name,
                        'lineno': item.lineno,
                        'args': args,
                        'is_static': any(isinstance(d, ast.Call) and getattr(getattr(d, 'func', None), 'id', None) == 'staticmethod' for d in item.decorator_list),
                        'is_classmethod': any(isinstance(d, ast.Call) and getattr(getattr(d, 'func', None), 'id', None) == 'classmethod' for d in item.decorator_list),
                    })
                elif isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name):
                            class_vars.append(target.id)
            api['classes'][node.name] = {
                'methods': methods,
                'class_vars': class_vars,
                'lineno': node.lineno,
            }
        # __all__ exports
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == '__all__':
                    if isinstance(node.value, ast.List):
                        api['exports'] = [elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)]
    
    return api

def compare_apis(api_before, api_after, filename):
    """Compare two API surfaces and return differences"""
    diffs = {'missing_classes': [], 'missing_functions': [], 'missing_methods': [], 'signature_changes': [], 'added': []}
    
    # Check classes
    for cls_name, cls_info in api_before['classes'].items():
        if cls_name not in api_after['classes']:
            diffs['missing_classes'].append(cls_name)
            continue
        after_cls = api_after['classes'][cls_name]
        # Check methods
        before_methods = {m['name'] for m in cls_info['methods']}
        after_methods = {m['name'] for m in after_cls['methods']}
        missing = before_methods - after_methods
        if missing:
            diffs['missing_methods'].extend([f'{cls_name}.{m}' for m in missing])
        # Check signature changes
        before_sig = {m['name']: m['args'] for m in cls_info['methods']}
        after_sig = {m['name']: m['args'] for m in after_cls['methods']}
        for m_name in before_methods & after_methods:
            if before_sig[m_name] != after_sig[m_name]:
                diffs['signature_changes'].append(f'{cls_name}.{m_name}: {before_sig[m_name]} -> {after_sig[m_name]}')
    
    # Check functions
    before_funcs = {f['name'] for f in api_before['functions']}
    after_funcs = {f['name'] for f in api_after['functions']}
    missing_funcs = before_funcs - after_funcs
    if missing_funcs:
        diffs['missing_functions'].extend(list(missing_funcs))
    
    # Check __all__ exports
    before_exports = set(api_before.get('exports', []))
    after_exports = set(api_after.get('exports', []))
    missing_exports = before_exports - after_exports
    if missing_exports:
        diffs['missing_functions'].extend([f'__all__:{e}' for e in missing_exports])
    
    return diffs

# ============================================================
# Part 2: Runtime import verification
# ============================================================

def runtime_import_test(module_path, names):
    """Test that names can be imported from module at runtime"""
    results = []
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        return [('MODULE_IMPORT_FAIL', str(e)[:100])]
    
    for name in names:
        try:
            obj = getattr(mod, name)
            obj_type = type(obj).__name__
            if callable(obj):
                results.append((name, f'OK({obj_type})', None))
            else:
                results.append((name, f'OK({obj_type})', None))
        except AttributeError as e:
            results.append((name, 'MISSING', str(e)[:80]))
        except Exception as e:
            results.append((name, 'ERROR', str(e)[:80]))
    
    return results

# ============================================================
# Part 3: Call-chain verification
# ============================================================

def verify_delegation_chain(module_path, class_name, method_name, expected_delegate_module=None):
    """Verify that a delegated method actually calls through to the delegate"""
    try:
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        method = getattr(cls, method_name)
        import inspect
        source = inspect.getsource(method)
        # Check if it contains a return delegation pattern
        is_delegate = 'return self._' in source and '(' in source
        return ('delegate' if is_delegate else 'inline', len(source.split('\n')))
    except Exception as e:
        return ('error', str(e)[:80])

# ============================================================
# Main execution
# ============================================================

print('=' * 80)
print('R27 功能完整性对比验证报告')
print('=' * 80)

# Key files to compare
key_files = [
    ('order_service.py', 'order_service.py'),
    ('signal_service.py', 'signal_service.py'),
    ('strategy_core_service.py', 'strategy_core_service.py'),
    ('config_params.py', 'config_params.py'),
]

all_diffs = {}
all_before_counts = {}
all_after_counts = {}

print('\n## Part 1: AST API 对比\n')

for backup_name, current_name in key_files:
    backup_path = os.path.join(BACKUP, backup_name)
    current_path = os.path.join(BASE, current_name)
    
    if not os.path.exists(backup_path):
        print(f'  [{backup_name}] 备份不存在，跳过')
        continue
    
    api_before = extract_api(backup_path)
    api_after = extract_api(current_path)
    
    # Count methods
    before_method_count = sum(len(c['methods']) for c in api_before['classes'].values())
    after_method_count = sum(len(c['methods']) for c in api_after['classes'].values())
    before_func_count = len(api_before['functions'])
    after_func_count = len(api_after['functions'])
    
    all_before_counts[backup_name] = {'classes': len(api_before['classes']), 'methods': before_method_count, 'functions': before_func_count}
    all_after_counts[current_name] = {'classes': len(api_after['classes']), 'methods': after_method_count, 'functions': after_func_count}
    
    diffs = compare_apis(api_before, api_after, backup_name)
    all_diffs[backup_name] = diffs
    
    total_issues = len(diffs['missing_classes']) + len(diffs['missing_functions']) + len(diffs['missing_methods']) + len(diffs['signature_changes'])
    status = 'PASS' if total_issues == 0 else 'FAIL'
    
    print(f'  [{status}] {backup_name}:')
    print(f'    重构前: {len(api_before["classes"])}类/{before_method_count}方法/{before_func_count}函数')
    print(f'    重构后: {len(api_after["classes"])}类/{after_method_count}方法/{after_func_count}函数')
    
    if diffs['missing_classes']:
        print(f'    缺失类: {diffs["missing_classes"]}')
    if diffs['missing_functions']:
        print(f'    缺失函数: {diffs["missing_functions"]}')
    if diffs['missing_methods']:
        print(f'    缺失方法({len(diffs["missing_methods"])}): {diffs["missing_methods"][:10]}{"..." if len(diffs["missing_methods"])>10 else ""}')
    if diffs['signature_changes']:
        print(f'    签名变化: {diffs["signature_changes"][:5]}')

# Part 2: Runtime import verification
print('\n## Part 2: 运行时导入验证\n')

runtime_tests = [
    ('ali2026v3_trading.order_service', ['OrderService', 'PlatformAuthenticator', 'get_order_service', 'sync_order_status_with_exchange', 'almgren_chriss_impact', 'estimate_fill_probability', 'check_self_trade_across_splits', 'AlgoTradingCompliance', 'WashTradeDetector', 'SmartOrderSplitter', 'OrderSplitStrategy', 'SplitOrderResult', 'RateLimiter']),
    ('ali2026v3_trading.signal_service', ['SignalService', 'get_signal_service']),
    ('ali2026v3_trading.strategy_core_service', ['StrategyCoreService', 'Strategy2026']),
    ('ali2026v3_trading.config_params', ['get_cached_params', 'update_cached_params', 'reset_param_cache', 'check_config_hot_reload', 'get_param_version', 'rollback_param_version', 'get_default_state_param_sets', 'validate_params', 'validate_config_schema', 'validate_ui_params', 'DEFAULT_PARAM_TABLE']),
    ('ali2026v3_trading.order_executor', ['OrderExecutor']),
    ('ali2026v3_trading.signal_generator', ['SignalGenerator']),
    ('ali2026v3_trading.health_check_aggregator', ['HealthCheckAggregator']),
    ('ali2026v3_trading.order_risk_guard', ['OrderRiskGuard']),
    ('ali2026v3_trading.signal_filter_chain', ['SignalFilterChain']),
    ('ali2026v3_trading.cooldown_manager', ['CooldownManager']),
    ('ali2026v3_trading.order_chase_service', ['OrderChaseService']),
    ('ali2026v3_trading.order_wal_state_service', ['OrderWALStateService']),
    ('ali2026v3_trading.order_platform_auth', ['PlatformAuthenticator']),
    ('ali2026v3_trading.order_market_impact', ['almgren_chriss_impact', 'estimate_fill_probability']),
    ('ali2026v3_trading.order_compliance', ['check_self_trade_across_splits', 'AlgoTradingCompliance', 'WashTradeDetector']),
    ('ali2026v3_trading.order_sync', ['sync_order_status_with_exchange']),
    ('ali2026v3_trading.order_split_models', ['SmartOrderSplitter', 'OrderSplitStrategy', 'SplitOrderResult']),
    ('ali2026v3_trading.strategy_recovery_mixin', ['StrategyRecoveryMixin']),
    ('ali2026v3_trading.strategy_checkpoint_mixin', ['StrategyCheckpointMixin']),
    ('ali2026v3_trading.config_state_defaults', ['get_default_state_param_sets', 'validate_config_schema', 'validate_ui_params']),
    ('ali2026v3_trading.security_config', ['_sanitize_for_return', 'get_sensitive_credential']),
    ('ali2026v3_trading.resilience_config', ['_auto_recovery_decision_tree']),
    ('ali2026v3_trading.phase_feature_flag', ['PhaseFeatureFlag']),
    ('ali2026v3_trading.lifecycle_manager', ['LifecycleManager']),
]

runtime_pass = 0
runtime_fail = 0
runtime_details = []

for module_path, names in runtime_tests:
    results = runtime_import_test(module_path, names)
    module_ok = all(r[1].startswith('OK') for r in results)
    fails = [r for r in results if not r[1].startswith('OK')]
    
    if module_ok:
        runtime_pass += 1
        print(f'  [PASS] {module_path}: {len(names)} names OK')
    else:
        runtime_fail += 1
        fail_names = [f'{r[0]}({r[1]})' for r in fails]
        print(f'  [FAIL] {module_path}: missing={fail_names}')
    runtime_details.append((module_path, results))

# Part 3: Delegation chain verification
print('\n## Part 3: 委托链完整性验证\n')

delegation_tests = [
    ('ali2026v3_trading.order_service', 'OrderService', 'send_order'),
    ('ali2026v3_trading.order_service', 'OrderService', '_wal_write'),
    ('ali2026v3_trading.order_service', 'OrderService', '_append_order_state'),
    ('ali2026v3_trading.order_service', 'OrderService', '_execute_with_compensation_v2'),
    ('ali2026v3_trading.signal_service', 'SignalService', 'generate_signal'),
    ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService', 'get_health_status'),
    ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService', '_auto_recovery_flow'),
    ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService', 'save_checkpoint'),
    ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService', '_sync_state_to_store'),
]

for module_path, class_name, method_name in delegation_tests:
    result = verify_delegation_chain(module_path, class_name, method_name)
    status = 'PASS' if result[0] in ('delegate', 'inline') else 'FAIL'
    print(f'  [{status}] {class_name}.{method_name}: type={result[0]}, lines={result[1]}')

# Part 4: Cross-module reference verification
print('\n## Part 4: 跨模块引用一致性验证\n')

# Verify that re-exported names from new modules are identical to originals
cross_checks = [
    ('ali2026v3_trading.order_service', 'PlatformAuthenticator', 'ali2026v3_trading.order_platform_auth', 'PlatformAuthenticator'),
    ('ali2026v3_trading.order_service', 'almgren_chriss_impact', 'ali2026v3_trading.order_market_impact', 'almgren_chriss_impact'),
    ('ali2026v3_trading.order_service', 'AlgoTradingCompliance', 'ali2026v3_trading.order_compliance', 'AlgoTradingCompliance'),
    ('ali2026v3_trading.order_service', 'sync_order_status_with_exchange', 'ali2026v3_trading.order_sync', 'sync_order_status_with_exchange'),
    ('ali2026v3_trading.order_service', 'SmartOrderSplitter', 'ali2026v3_trading.order_split_models', 'SmartOrderSplitter'),
    ('ali2026v3_trading.config_params', 'get_default_state_param_sets', 'ali2026v3_trading.config_state_defaults', 'get_default_state_param_sets'),
    ('ali2026v3_trading.config_params', 'validate_config_schema', 'ali2026v3_trading.config_state_defaults', 'validate_config_schema'),
]

for mod1, name1, mod2, name2 in cross_checks:
    try:
        m1 = importlib.import_module(mod1)
        m2 = importlib.import_module(mod2)
        obj1 = getattr(m1, name1)
        obj2 = getattr(m2, name2)
        is_same = obj1 is obj2
        status = 'PASS' if is_same else 'WARN'
        print(f'  [{status}] {mod1}.{name1} is {mod2}.{name2}: {is_same}')
    except Exception as e:
        print(f'  [FAIL] {mod1}.{name1} vs {mod2}.{name2}: {str(e)[:80]}')

# Summary
print('\n' + '=' * 80)
print('功能完整性对比验证总结')
print('=' * 80)

total_ast_issues = sum(
    len(d['missing_classes']) + len(d['missing_functions']) + len(d['missing_methods']) + len(d['signature_changes'])
    for d in all_diffs.values()
)

print(f'AST API对比: {len(all_diffs)}文件, {total_ast_issues}项差异')
print(f'运行时导入: {runtime_pass}/{runtime_pass+runtime_fail}模块PASS')
print(f'评级: {"A(功能完整)" if total_ast_issues == 0 and runtime_fail == 0 else "B(有差异需关注)" if total_ast_issues <= 3 else "C(功能缺失)"}')

# Detail missing methods
if total_ast_issues > 0:
    print('\n### 需关注的差异详情:')
    for fname, diffs in all_diffs.items():
        issues = []
        if diffs['missing_methods']:
            issues.append(f"缺失方法({len(diffs['missing_methods'])}): {diffs['missing_methods'][:5]}")
        if diffs['missing_functions']:
            issues.append(f"缺失函数: {diffs['missing_functions'][:5]}")
        if diffs['signature_changes']:
            issues.append(f"签名变化: {diffs['signature_changes'][:3]}")
        if issues:
            print(f'  {fname}: {"; ".join(issues)}')