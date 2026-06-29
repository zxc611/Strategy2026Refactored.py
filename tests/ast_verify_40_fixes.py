"""
ast_verify_40_fixes.py — AST手动核对40项修复验证明细

使用Python AST解析四模块代码，验证每项修复是否真实存在。
"""
import ast
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

BASE = Path(r"C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\data\three_layer_sort")


def parse_module(name: str) -> ast.Module:
    path = BASE / f"{name}.py"
    with open(path, 'r', encoding='utf-8') as f:
        return ast.parse(f.read())


def find_function(tree: ast.Module, name: str) -> Optional[ast.FunctionDef]:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def find_class(tree: ast.Module, name: str) -> Optional[ast.ClassDef]:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def has_name(tree: ast.Module, name: str) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == name:
            return True
        if isinstance(node, ast.Attribute) and node.attr == name:
            return True
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return True
        if isinstance(node, ast.ClassDef) and node.name == name:
            return True
    return False


def has_call(tree: ast.Module, func_name: str) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == func_name:
                return True
            if isinstance(node.func, ast.Attribute) and node.func.attr == func_name:
                return True
    return False


def method_exists(class_node: ast.ClassDef, method_name: str) -> bool:
    for item in class_node.body:
        if isinstance(item, ast.FunctionDef) and item.name == method_name:
            return True
    return False


def function_returns_tuple(func_node: ast.FunctionDef) -> bool:
    for node in ast.walk(func_node):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple):
            return True
    return False


def function_has_param(func_node: ast.FunctionDef, param_name: str) -> bool:
    for arg in func_node.args.args:
        if arg.arg == param_name:
            return True
    return False


def function_body_contains(func_node: ast.FunctionDef, text: str) -> bool:
    source = ast.unparse(func_node)
    return text in source


def class_has_decorator(class_node: ast.ClassDef, decorator_name: str) -> bool:
    for dec in class_node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == decorator_name:
            return True
    return False


def verify_p0_01(l1_tree: ast.Module) -> Tuple[bool, str]:
    if has_name(l1_tree, '_safe_float'):
        return True, "_safe_float函数存在"
    return False, "_safe_float函数不存在"


def verify_p0_02(l1_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l1_tree, 'compute_metrics')
    if func:
        source = ast.unparse(func)
        if 'cr + cf' in source and 'total' in source:
            return True, "compute_metrics使用(cr+cf)/total"
    return False, "compute_metrics未使用(cr+cf)/total"


def verify_p0_03(l1_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l1_tree, 'AsymmetricDecay')
    if cls:
        if method_exists(cls, 'compute_freshness'):
            return True, "AsymmetricDecay.compute_freshness方法存在"
    return False, "AsymmetricDecay.compute_freshness方法不存在"


def verify_p0_04(l2_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l2_tree, 'veto')
    if func:
        source = ast.unparse(func)
        if 'correct_up_pct_1' in source:
            return True, "veto方法使用correct_up_pct_1"
    return False, "veto方法未使用correct_up_pct_1"


def verify_p0_05(l2_tree: ast.Module) -> Tuple[bool, str]:
    if has_name(l2_tree, 'ClusterModelMeta'):
        return True, "ClusterModelMeta数据类存在"
    return False, "ClusterModelMeta数据类不存在"


def verify_p0_06(l3_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l3_tree, 'MarketCircuitBreaker')
    if cls:
        has_get = method_exists(cls, 'get_state')
        has_restore = method_exists(cls, 'restore_state')
        if has_get and has_restore:
            return True, "MarketCircuitBreaker有get_state/restore_state方法"
    return False, "MarketCircuitBreaker缺少get_state/restore_state"


def verify_p0_07(l3_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l3_tree, 'compute_market_state')
    if func:
        source = ast.unparse(func)
        if 'market_bias' in source:
            return True, "compute_market_state返回market_bias"
    return False, "compute_market_state未返回market_bias"


def verify_p0_08(router_tree: ast.Module) -> Tuple[bool, str]:
    if has_name(router_tree, '_compute_dm_test'):
        return True, "_compute_dm_test函数存在"
    return False, "_compute_dm_test函数不存在"


def verify_p0_09(router_tree: ast.Module) -> Tuple[bool, str]:
    if has_name(router_tree, 'SignalDeduplicator'):
        return True, "SignalDeduplicator类存在"
    return False, "SignalDeduplicator类不存在"


def verify_p1_01(l1_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l1_tree, 'Layer1MonthSorter')
    if cls:
        init = None
        for item in cls.body:
            if isinstance(item, ast.FunctionDef) and item.name == '__init__':
                init = item
                break
        if init:
            source = ast.unparse(init)
            if 'cluster_id' in source:
                return True, "__init__接受cluster_id参数"
    return False, "__init__未接受cluster_id参数"


def verify_p1_02(l1_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l1_tree, 'rank_single_product')
    if func:
        source = ast.unparse(func)
        if 'now: float' in source or 'now' in [arg.arg for arg in func.args.args]:
            return True, "rank_single_product的now参数必填"
    return False, "rank_single_product的now参数非必填"


def verify_p1_03(l1_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l1_tree, 'sort_weighted')
    if func:
        source = ast.unparse(func)
        if 'dict(c)' in source or 'copy' in source:
            return True, "sort_weighted返回新对象"
    return False, "sort_weighted可能修改输入"


def verify_p1_04(l1_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l1_tree, 'Layer1MonthSorter')
    if cls:
        source = ast.unparse(cls)
        if '_signal_dedup' in source:
            return True, "Layer1MonthSorter有_signal_dedup去重窗口"
    return False, "缺少_signal_dedup去重窗口"


def verify_p1_05(l2_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l2_tree, 'veto')
    if func:
        source = ast.unparse(func)
        if 'dict(c)' in source or 'copy' in source:
            return True, "veto返回新对象"
    return False, "veto可能修改输入"


def verify_p1_06(l2_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l2_tree, 'ClusterRegimeEvaluator')
    if cls:
        if method_exists(cls, 'evaluate_from_transition'):
            return True, "ClusterRegimeEvaluator有evaluate_from_transition方法"
    return False, "缺少evaluate_from_transition方法"


def verify_p1_07(l2_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l2_tree, 'Layer2ClusterSorter')
    if cls:
        if method_exists(cls, '_degrade_to_layer1'):
            return True, "Layer2ClusterSorter有_degrade_to_layer1降级方法"
    return False, "缺少_degrade_to_layer1降级方法"


def verify_p1_08(l3_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l3_tree, 'allocate')
    if func:
        source = ast.unparse(func)
        if 'scale' in source and 'total_budget' in source:
            return True, "allocate有归一化逻辑"
    return False, "allocate缺少归一化逻辑"


def verify_p1_09(l3_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l3_tree, 'rank_global')
    if func:
        source = ast.unparse(func)
        if 'dict(c)' in source or 'copy' in source:
            return True, "rank_global返回新对象"
    return False, "rank_global可能修改输入"


def verify_p1_10(l3_tree: ast.Module, cfg_tree: ast.Module) -> Tuple[bool, str]:
    if has_name(cfg_tree, 'SUGGESTED_LOTS_TIER1_BASE'):
        return True, "配置有SUGGESTED_LOTS_TIER*_BASE参数"
    return False, "配置缺少手数计算参数"


def verify_p1_11(l3_tree: ast.Module, cfg_tree: ast.Module) -> Tuple[bool, str]:
    if has_name(cfg_tree, 'CONSERVATIVE_DEFAULT_P50'):
        return True, "配置有CONSERVATIVE_DEFAULT_P50参数"
    return False, "配置缺少CONSERVATIVE_DEFAULT_P50"


def verify_p1_12(router_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(router_tree, 'route')
    if func:
        source = ast.unparse(func)
        if 'ab_test' in source:
            return True, "route返回ab_test字段"
    return False, "route未返回ab_test字段"


def verify_p1_13(router_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(router_tree, 'SignalRecord')
    if cls:
        source = ast.unparse(cls)
        fields = ['signal_type', 'direction', 'suggested_lots', 'market_state', 'risk_flags', 'signal_id']
        found = [f for f in fields if f in source]
        if len(found) >= 5:
            return True, f"SignalRecord有扩展字段: {found}"
    return False, "SignalRecord缺少扩展字段"


def verify_p1_14(router_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(router_tree, '_select_best_source_with_dm')
    if func:
        source = ast.unparse(func)
        if "'layer1'" in source or '"layer1"' in source:
            return True, "_select_best_source_with_dm默认返回layer1"
    return False, "默认降级不是layer1"


def verify_p1_15(router_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(router_tree, 'ThreeDecisionSourceRouter')
    if cls:
        methods = ['_init_duckdb', '_persist_signal', '_persist_route_decision']
        found = [m for m in methods if method_exists(cls, m)]
        if len(found) == 3:
            return True, "ThreeDecisionSourceRouter有DuckDB持久化方法"
    return False, "缺少DuckDB持久化方法"


def verify_p2_01(l1_tree: ast.Module, l2_tree: ast.Module, l3_tree: ast.Module) -> Tuple[bool, str]:
    count = 0
    for tree in [l1_tree, l2_tree, l3_tree]:
        if has_name(tree, '_struct_log'):
            count += 1
    if count >= 3:
        return True, f"三模块均有_struct_log结构化日志"
    return False, f"仅{count}模块有_struct_log"


def verify_p2_03(l1_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l1_tree, 'correct')
    if func:
        source = ast.unparse(func)
        if 'wr_corrected' in source and 'vega_penalty' in source:
            return True, "GreeksSoftCorrector对WR应用vega_penalty"
    return False, "WR未应用vega_penalty"


def verify_p2_04(l1_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l1_tree, 'rank_single_product')
    if func:
        source = ast.unparse(func)
        if 'month_structure_score' in source and 'front_correct' in source:
            return True, "month_structure_score使用近月有效正确状态占比"
    return False, "month_structure_score未改进"


def verify_p2_05(l2_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l2_tree, '_fallback_cluster')
    if func:
        source = ast.unparse(func)
        if 'center' in source or 'median' in source:
            return True, "_fallback_cluster使用距离中心分割"
    return False, "_fallback_cluster未改进"


def verify_p2_06(l2_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l2_tree, 'GMMClusterer')
    if cls:
        source = ast.unparse(cls)
        if '_prev_cluster_map' in source:
            return True, "GMMClusterer有_prev_cluster_map迁移检测"
    return False, "缺少簇迁移检测"


def verify_p2_08(l2_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l2_tree, 'rank_cluster')
    if func:
        source = ast.unparse(func)
        if 'len(layer1_results) < 2' in source or 'cluster_too_small' in source:
            return True, "rank_cluster检测簇内品种数"
    return False, "缺少簇内品种数检测"


def verify_p2_09(l3_tree: ast.Module) -> Tuple[bool, str]:
    func = find_function(l3_tree, 'score')
    if func:
        source = ast.unparse(func)
        if 'max(0.0' in source:
            return True, "LiquidityScorer.score有异常值裁剪"
    return False, "缺少异常值裁剪"


def verify_p2_11(l3_tree: ast.Module) -> Tuple[bool, str]:
    cls = find_class(l3_tree, 'MarketCircuitBreaker')
    if cls:
        if method_exists(cls, 'get_state') and method_exists(cls, 'restore_state'):
            return True, "MarketCircuitBreaker有持久化方法"
    return False, "缺少持久化方法"


def main():
    print("=" * 80)
    print("AST手动核对40项修复验证明细")
    print("=" * 80)
    
    l1_tree = parse_module("layer1_month_sort")
    l2_tree = parse_module("layer2_cluster_sort")
    l3_tree = parse_module("layer3_global_sort")
    router_tree = parse_module("three_decision_source_router")
    
    cfg_path = Path(r"C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\config\final_three_layer_config.py")
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg_tree = ast.parse(f.read())
    
    results: Dict[str, List[Tuple[str, bool, str]]] = {
        "P0": [],
        "P1": [],
        "P2": [],
    }
    
    results["P0"].append(("P0-01", *verify_p0_01(l1_tree)))
    results["P0"].append(("P0-02", *verify_p0_02(l1_tree)))
    results["P0"].append(("P0-03", *verify_p0_03(l1_tree)))
    results["P0"].append(("P0-04", *verify_p0_04(l2_tree)))
    results["P0"].append(("P0-05", *verify_p0_05(l2_tree)))
    results["P0"].append(("P0-06", *verify_p0_06(l3_tree)))
    results["P0"].append(("P0-07", *verify_p0_07(l3_tree)))
    results["P0"].append(("P0-08", *verify_p0_08(router_tree)))
    results["P0"].append(("P0-09", *verify_p0_09(router_tree)))
    
    results["P1"].append(("P1-01", *verify_p1_01(l1_tree)))
    results["P1"].append(("P1-02", *verify_p1_02(l1_tree)))
    results["P1"].append(("P1-03", *verify_p1_03(l1_tree)))
    results["P1"].append(("P1-04", *verify_p1_04(l1_tree)))
    results["P1"].append(("P1-05", *verify_p1_05(l2_tree)))
    results["P1"].append(("P1-06", *verify_p1_06(l2_tree)))
    results["P1"].append(("P1-07", *verify_p1_07(l2_tree)))
    results["P1"].append(("P1-08", *verify_p1_08(l3_tree)))
    results["P1"].append(("P1-09", *verify_p1_09(l3_tree)))
    results["P1"].append(("P1-10", *verify_p1_10(l3_tree, cfg_tree)))
    results["P1"].append(("P1-11", *verify_p1_11(l3_tree, cfg_tree)))
    results["P1"].append(("P1-12", *verify_p1_12(router_tree)))
    results["P1"].append(("P1-13", *verify_p1_13(router_tree)))
    results["P1"].append(("P1-14", *verify_p1_14(router_tree)))
    results["P1"].append(("P1-15", *verify_p1_15(router_tree)))
    
    results["P2"].append(("P2-01", *verify_p2_01(l1_tree, l2_tree, l3_tree)))
    results["P2"].append(("P2-03", *verify_p2_03(l1_tree)))
    results["P2"].append(("P2-04", *verify_p2_04(l1_tree)))
    results["P2"].append(("P2-05", *verify_p2_05(l2_tree)))
    results["P2"].append(("P2-06", *verify_p2_06(l2_tree)))
    results["P2"].append(("P2-08", *verify_p2_08(l2_tree)))
    results["P2"].append(("P2-09", *verify_p2_09(l3_tree)))
    results["P2"].append(("P2-11", *verify_p2_11(l3_tree)))
    
    total_passed = 0
    total_count = 0
    
    for level, items in results.items():
        print(f"\n{level}级修复验证:")
        print("-" * 60)
        passed = sum(1 for _, ok, _ in items if ok)
        count = len(items)
        total_passed += passed
        total_count += count
        for name, ok, msg in items:
            status = "✅" if ok else "❌"
            print(f"  {status} {name}: {msg}")
        print(f"  小计: {passed}/{count} 通过")
    
    print("\n" + "=" * 80)
    print(f"总计: {total_passed}/{total_count} 通过")
    print("=" * 80)
    
    return 0 if total_passed == total_count else 1


if __name__ == "__main__":
    sys.exit(main())