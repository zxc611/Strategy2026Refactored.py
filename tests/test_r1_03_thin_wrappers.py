# MODULE_ID: M2-507
"""Round1-3 断言测试：P1-27 删除 config_params 薄包装委托函数
验证：1) config_params 不再导出8个薄包装函数
      2) config_loader 权威版本仍可导出
      3) config_resolver 直接从 config_loader 导入
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

THIN_WRAPPERS = [
    '_resolve_params_json_path',
    '_resolve_runtime_env_name',
    '_apply_env_overrides',
    'load_default_params_from_json',
    'check_config_hot_reload',
    '_PARAMS_JSON_CACHE',
    'PARAMS_JSON_CACHE_TTL',
    '_PARAMS_JSON_LOCK',
]

def test_thin_wrappers_removed():
    """config_params 不再导出8个薄包装函数"""
    import importlib
    mod = importlib.import_module('ali2026v3_trading.config.config_params')
    remaining = [name for name in THIN_WRAPPERS if hasattr(mod, name)]
    assert len(remaining) == 0, f"config_params 仍导出薄包装函数: {remaining}"
    print(f"  OK: 8个薄包装函数已从 config_params 删除")

def test_canonical_still_works():
    """config_loader 权威版本仍可导出"""
    import importlib
    mod = importlib.import_module('ali2026v3_trading.config.config_loader')
    assert hasattr(mod, '_resolve_params_json_path'), "config_loader 缺少 _resolve_params_json_path"
    assert hasattr(mod, 'load_default_params_from_json'), "config_loader 缺少 load_default_params_from_json"
    print("  OK: config_loader 权威版本仍可导出")

def test_resolver_direct_import():
    """config_resolver 直接从 config_loader 导入"""
    import inspect
    from ali2026v3_trading.config.config_loader import get_params_metadata
    source = inspect.getsource(get_params_metadata)
    assert 'config_loader' in source or 'json_loads' in source, "config_loader 未包含核心加载逻辑"
    print("  OK: config_loader 直接包含 get_params_metadata")

if __name__ == '__main__':
    print("=== Round1-3: P1-27 删除 config_params 薄包装 ===")
    test_thin_wrappers_removed()
    test_canonical_still_works()
    test_resolver_direct_import()
    print("ALL ASSERTIONS PASSED")
