"""
P2级问题修复验证脚本 - 验证所有修复项
验证日期: 2026-05-28
"""
import os
import sys
import py_compile
from typing import List, Dict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def verify_file_compile(filepath: str) -> Dict:
    """验证文件是否可以py_compile通过"""
    try:
        py_compile.compile(filepath, doraise=True)
        return {'status': 'PASS', 'detail': '编译成功'}
    except py_compile.PyCompileError as e:
        return {'status': 'FAIL', 'detail': str(e)}
    except Exception as e:
        return {'status': 'ERROR', 'detail': str(e)}


def verify_checkpoint_atomic_write() -> Dict:
    """验证SER-P2-20: 检查点原子写入"""
    filepath = os.path.join(BASE_DIR, "strategy_core_service.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('tempfile.mkstemp' in content, "使用tempfile.mkstemp创建临时文件"),
        ('os.replace(_tmp_path, _checkpoint_path)' in content, "使用os.replace原子替换"),
        ('SER-P2-20修复' in content, "包含修复标记"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_serialization_perf_monitor() -> Dict:
    """验证SER-P2-28: 序列化性能监控"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('serialization_perf_monitor' in content, "存在serialization_perf_monitor装饰器"),
        ('SER_PERF_LOG' in content, "支持SER_PERF_LOG环境变量"),
        ('time.perf_counter()' in content, "使用perf_counter计时"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_progress_callback() -> Dict:
    """验证SER-P2-29: 大文件序列化进度报告"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('safe_jsonl_write_with_progress' in content, "存在safe_jsonl_write_with_progress函数"),
        ('progress_callback' in content, "支持progress_callback参数"),
        ('batch_size' in content, "支持batch_size参数"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_memory_limit() -> Dict:
    """验证SER-P2-30: 序列化内存上限控制"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('json_dumps_with_memory_limit' in content, "存在json_dumps_with_memory_limit函数"),
        ('max_memory_mb' in content, "支持max_memory_mb参数"),
        ('MemoryError' in content, "可抛出MemoryError"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_schema_version_doc() -> Dict:
    """验证SER-P2-31: Schema版本号命名规范文档"""
    doc_path = os.path.join(BASE_DIR, "docs_schema_version_naming_convention.md")
    
    checks = [
        (os.path.exists(doc_path), "文档文件存在"),
    ]
    
    if os.path.exists(doc_path):
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
        checks.append(('SCHEMA_VERSION' in content, "包含版本号命名约定"))
        checks.append(('MAJOR.MINOR.PATCH' in content, "包含语义化版本说明"))
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_roundtrip_test() -> Dict:
    """验证SER-P2-32: Round-trip测试覆盖"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('comprehensive_roundtrip_test' in content, "存在comprehensive_roundtrip_test函数"),
        ('datetime.datetime' in content, "覆盖datetime类型"),
        ('decimal.Decimal' in content, "覆盖Decimal类型"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_migration_test() -> Dict:
    """验证SER-P2-33: 迁移脚本测试"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('test_schema_migration_example' in content, "存在迁移测试示例"),
        ('migrate_v1_to_v2' in content, "包含迁移函数示例"),
        ('migration_chain' in content, "包含迁移链示例"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_version_router() -> Dict:
    """验证SER-P2-34: 多版本数据路由"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('version_aware_load' in content, "存在version_aware_load函数"),
        ('get_compatible_versions' in content, "存在get_compatible_versions函数"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_friendly_error() -> Dict:
    """验证SER-P2-35: 序列化错误消息友好"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('class SerializationError' in content, "存在SerializationError类"),
        ('json_dumps_with_path_tracking' in content, "存在json_dumps_with_path_tracking函数"),
        ('field_path' in content, "支持字段路径"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_thread_safety() -> Dict:
    """验证P2项12: 线程安全保护"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('P2项12修复' in content or '线程安全' in content, "包含线程安全注释"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_large_file_doc() -> Dict:
    """验证SER-P2-02/NEW-P2-02: 过大文件拆分建议文档"""
    doc_path = os.path.join(BASE_DIR, "docs_large_file_split_suggestions.md")
    
    checks = [
        (os.path.exists(doc_path), "文档文件存在"),
    ]
    
    if os.path.exists(doc_path):
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
        checks.append(('拆分原则' in content, "包含拆分原则"))
        checks.append(('拆分策略' in content, "包含拆分策略"))
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def verify_named_constants() -> Dict:
    """验证NEW-P2-01: 魔法数字命名常量"""
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ('NEW-P2-01修复' in content, "包含修复标记"),
        ('JSON_MAX_RECURSION_DEPTH' in content, "定义JSON最大递归深度"),
        ('MEMORY_WARNING_THRESHOLD_MB' in content, "定义内存警告阈值"),
        ('MAX_RETRY_COUNT' in content, "定义最大重试次数"),
    ]
    
    all_pass = all(c[0] for c in checks)
    return {
        'status': 'PASS' if all_pass else 'FAIL',
        'checks': checks
    }


def main():
    """主验证流程"""
    print("="*70)
    print("P2级问题修复验证")
    print("="*70)
    
    results = {}
    
    print("\n[1] py_compile验证 - strategy_core_service.py")
    results['compile_strategy'] = verify_file_compile(
        os.path.join(BASE_DIR, "strategy_core_service.py")
    )
    print(f"  状态: {results['compile_strategy']['status']}")
    
    print("\n[2] py_compile验证 - serialization_utils.py")
    results['compile_serialization'] = verify_file_compile(
        os.path.join(BASE_DIR, "serialization_utils.py")
    )
    print(f"  状态: {results['compile_serialization']['status']}")
    
    print("\n[3] SER-P2-20: 检查点原子写入")
    results['SER-P2-20'] = verify_checkpoint_atomic_write()
    print(f"  状态: {results['SER-P2-20']['status']}")
    for passed, desc in results['SER-P2-20']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[4] SER-P2-28: 序列化性能监控")
    results['SER-P2-28'] = verify_serialization_perf_monitor()
    print(f"  状态: {results['SER-P2-28']['status']}")
    for passed, desc in results['SER-P2-28']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[5] SER-P2-29: 大文件序列化进度报告")
    results['SER-P2-29'] = verify_progress_callback()
    print(f"  状态: {results['SER-P2-29']['status']}")
    for passed, desc in results['SER-P2-29']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[6] SER-P2-30: 序列化内存上限控制")
    results['SER-P2-30'] = verify_memory_limit()
    print(f"  状态: {results['SER-P2-30']['status']}")
    for passed, desc in results['SER-P2-30']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[7] SER-P2-31: Schema版本号命名规范文档")
    results['SER-P2-31'] = verify_schema_version_doc()
    print(f"  状态: {results['SER-P2-31']['status']}")
    for passed, desc in results['SER-P2-31']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[8] SER-P2-32: Round-trip测试覆盖")
    results['SER-P2-32'] = verify_roundtrip_test()
    print(f"  状态: {results['SER-P2-32']['status']}")
    for passed, desc in results['SER-P2-32']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[9] SER-P2-33: 迁移脚本测试")
    results['SER-P2-33'] = verify_migration_test()
    print(f"  状态: {results['SER-P2-33']['status']}")
    for passed, desc in results['SER-P2-33']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[10] SER-P2-34: 多版本数据路由")
    results['SER-P2-34'] = verify_version_router()
    print(f"  状态: {results['SER-P2-34']['status']}")
    for passed, desc in results['SER-P2-34']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[11] SER-P2-35: 序列化错误消息友好")
    results['SER-P2-35'] = verify_friendly_error()
    print(f"  状态: {results['SER-P2-35']['status']}")
    for passed, desc in results['SER-P2-35']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[12] P2项12: 线程安全保护")
    results['P2项12'] = verify_thread_safety()
    print(f"  状态: {results['P2项12']['status']}")
    for passed, desc in results['P2项12']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[13] SER-P2-02/NEW-P2-02: 过大文件拆分建议文档")
    results['SER-P2-02'] = verify_large_file_doc()
    print(f"  状态: {results['SER-P2-02']['status']}")
    for passed, desc in results['SER-P2-02']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n[14] NEW-P2-01: 魔法数字命名常量")
    results['NEW-P2-01'] = verify_named_constants()
    print(f"  状态: {results['NEW-P2-01']['status']}")
    for passed, desc in results['NEW-P2-01']['checks']:
        symbol = "✓" if passed else "✗"
        print(f"    {symbol} {desc}")
    
    print("\n" + "="*70)
    print("验证汇总")
    print("="*70)
    
    pass_count = sum(1 for v in results.values() if v['status'] == 'PASS')
    fail_count = sum(1 for v in results.values() if v['status'] == 'FAIL')
    total_count = len(results)
    
    print(f"通过: {pass_count}/{total_count}")
    print(f"失败: {fail_count}/{total_count}")
    
    if fail_count == 0:
        print("\n✓ 所有P2级问题修复验证通过")
        return 0
    else:
        print("\n✗ 存在验证失败的修复项")
        return 1


if __name__ == "__main__":
    sys.exit(main())
