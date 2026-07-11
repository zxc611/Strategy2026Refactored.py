# MODULE_ID: M2-502
"""Round1-1 断言测试：P0-09/P0-26 删除中文子目录 L1参数量化/
验证：1) 英文路径 l1_quantification 所有关键符号可达 2) 中文路径 L1参数量化 不可达
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_l1_quantification_symbols():
    """英文路径 l1_quantification 所有关键符号可达"""
    import importlib
    # 逐模块导入，避免 __init__.py 延迟加载问题
    mae = importlib.import_module('ali2026v3_trading.param_pool.l1_quantification.meta_audit_engine')
    mas = importlib.import_module('ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport')
    assert hasattr(mae, 'FutureLeakException'), "meta_audit_engine 缺少 FutureLeakException"
    assert hasattr(mas, 'SandboxExecutionAuditor'), "meta_audit_sandbox 缺少 SandboxExecutionAuditor"
    print("  OK: l1_quantification 关键符号全部可达")

def test_chinese_dir_removed():
    """中文路径 L1参数量化 不可达（目录已删除）"""
    base = os.path.join(os.path.dirname(__file__), '..', 'param_pool')
    chinese_dir = os.path.join(base, 'L1参数量化')
    assert not os.path.isdir(chinese_dir), f"中文目录仍存在: {chinese_dir}"
    print("  OK: L1参数量化 目录已删除")

if __name__ == '__main__':
    print("=== Round1-1: P0-09/P0-26 中文目录删除 ===")
    test_l1_quantification_symbols()
    test_chinese_dir_removed()
    print("ALL ASSERTIONS PASSED")
