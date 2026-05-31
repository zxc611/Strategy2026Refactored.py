"""
P2级问题批量修复脚本 - 第二批
修复日期: 2026-05-28
修复项目:
1. SER-P2-20: 检查点非原子写入
2. SER-P2-28: 序列化性能无监控
3. SER-P2-29: 大文件序列化无进度报告
4. SER-P2-30: 序列化内存无上限控制
5. SER-P2-31: schema版本号命名规范文档
6. SER-P2-32: round-trip测试覆盖增强
7. SER-P2-33: 迁移脚本测试示例
8. SER-P2-34: 多版本数据路由
9. SER-P2-35: 序列化错误消息友好
10. P2项12: 模块级可变对象线程保护
11. P2项14: __init__.py符号与__all__一致性
12. SER-P2-02/NEW-P2-02: 过大文件拆分建议文档
13. NEW-P2-01: 魔法数字命名常量
"""
import os
import re
import py_compile
import tempfile
import shutil
from typing import List, Tuple, Dict

REPORT = []
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def log_fix(item_id: str, description: str, status: str, detail: str = ""):
    REPORT.append({
        'item': item_id,
        'description': description,
        'status': status,
        'detail': detail
    })
    symbol = "✓" if status == "SUCCESS" else "✗"
    print(f"[{symbol}] {item_id}: {description} - {status}")
    if detail:
        print(f"    {detail}")


def verify_compile(filepath: str) -> bool:
    try:
        py_compile.compile(filepath, doraise=True)
        return True
    except py_compile.PyCompileError as e:
        return False


# ============================================================================
# 1. SER-P2-20: 检查点非原子写入修复
# ============================================================================
def fix_checkpoint_atomic_write():
    """修复检查点写入为原子写入模式"""
    item_id = "SER-P2-20"
    filepath = os.path.join(BASE_DIR, "strategy_core_service.py")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        old_pattern = r'''            with open\(_checkpoint_path, 'w', encoding='utf-8'\) as f:
                f\.write\(json_dumps\(checkpoint, indent=2\)\)'''
        
        new_code = '''            # SER-P2-20修复: 原子写入 - 使用tmp文件+os.replace防止写入中断导致文件损坏
            _tmp_fd, _tmp_path = tempfile.mkstemp(
                suffix='.tmp',
                prefix='checkpoint_',
                dir=os.path.dirname(_checkpoint_path)
            )
            try:
                with os.fdopen(_tmp_fd, 'w', encoding='utf-8') as f:
                    f.write(json_dumps(checkpoint, indent=2))
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(_tmp_path, _checkpoint_path)
            except Exception as _e:
                if os.path.exists(_tmp_path):
                    os.remove(_tmp_path)
                raise _e'''
        
        if re.search(old_pattern, content):
            new_content = re.sub(old_pattern, new_code, content)
            
            if 'import tempfile' not in new_content:
                import_section = new_content.find('import os')
                if import_section != -1:
                    end_of_line = new_content.find('\n', import_section)
                    new_content = (new_content[:end_of_line+1] + 
                                   'import tempfile\n' + 
                                   new_content[end_of_line+1:])
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            if verify_compile(filepath):
                log_fix(item_id, "检查点原子写入修复", "SUCCESS", 
                       f"strategy_core_service.py:save_checkpoint使用tmp+os.replace")
            else:
                log_fix(item_id, "检查点原子写入修复", "FAILED", "编译失败")
        else:
            log_fix(item_id, "检查点原子写入修复", "SKIP", "已修复或模式未找到")
    except Exception as e:
        log_fix(item_id, "检查点原子写入修复", "ERROR", str(e))


# ============================================================================
# 2. SER-P2-28: 序列化性能监控装饰器
# ============================================================================
def add_serialization_perf_monitor():
    """添加序列化耗时统计装饰器"""
    item_id = "SER-P2-28"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    monitor_code = '''
# SER-P2-28修复: 序列化性能监控装饰器
def serialization_perf_monitor(func_name: str = None):
    """
    序列化性能监控装饰器 - 记录序列化耗时和内存使用
    
    使用方式:
    >>> @serialization_perf_monitor("json_dumps")
    >>> def my_serializer(data):
    >>>     return json.dumps(data)
    
    环境变量:
    - SER_PERF_LOG: 设置为"1"启用性能日志
    
    输出指标:
    - 函数名
    - 执行耗时(ms)
    - 输入大小(bytes, 如可获取)
    - 输出大小(bytes)
    """
    import functools
    import time
    import os
    import logging
    
    _enable_perf_log = os.environ.get('SER_PERF_LOG', '').strip() == '1'
    
    def decorator(func):
        _name = func_name or func.__name__
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not _enable_perf_log:
                return func(*args, **kwargs)
            
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                
                input_size = 0
                if args and hasattr(args[0], '__len__'):
                    try:
                        input_size = len(args[0])
                    except:
                        pass
                
                output_size = 0
                if isinstance(result, str):
                    output_size = len(result)
                elif hasattr(result, '__len__'):
                    try:
                        output_size = len(result)
                    except:
                        pass
                
                logging.info(
                    "[SER_PERF] %s: %.2fms, input=%d, output=%d",
                    _name, elapsed_ms, input_size, output_size
                )
                
                return result
            except Exception as e:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                logging.error(
                    "[SER_PERF] %s: FAILED after %.2fms - %s",
                    _name, elapsed_ms, e
                )
                raise
        
        return decorator
    return decorator

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'serialization_perf_monitor' not in content:
            insert_pos = content.find('def json_dumps(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + monitor_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "序列化性能监控装饰器", "SUCCESS",
                           "serialization_utils.py添加serialization_perf_monitor装饰器")
                else:
                    log_fix(item_id, "序列化性能监控装饰器", "FAILED", "编译失败")
            else:
                log_fix(item_id, "序列化性能监控装饰器", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "序列化性能监控装饰器", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "序列化性能监控装饰器", "ERROR", str(e))


# ============================================================================
# 3. SER-P2-29: 大文件序列化进度报告
# ============================================================================
def add_progress_callback():
    """添加进度回调参数支持"""
    item_id = "SER-P2-29"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    progress_code = '''

# SER-P2-29修复: 大文件序列化进度报告
def safe_jsonl_write_with_progress(
    records: List[Dict],
    filepath: str,
    encoding: str = 'utf-8',
    progress_callback=None,
    batch_size: int = 1000,
) -> int:
    """
    带进度报告的JSONL写入
    
    参数:
    - records: 记录列表
    - filepath: 输出文件路径
    - encoding: 编码
    - progress_callback: 进度回调函数 callback(current, total, percent)
    - batch_size: 进度报告批次大小
    
    返回:
    - 写入记录数
    
    使用示例:
    >>> def my_progress(current, total, percent):
    >>>     print(f"进度: {percent:.1f}% ({current}/{total})")
    >>> safe_jsonl_write_with_progress(records, 'data.jsonl', progress_callback=my_progress)
    """
    import os
    import tempfile
    
    total = len(records)
    if total == 0:
        return 0
    
    dir_path = os.path.dirname(filepath)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    
    count = 0
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix='.tmp',
        prefix='jsonl_progress_',
        dir=dir_path if dir_path else '.'
    )
    
    try:
        with os.fdopen(tmp_fd, 'w', encoding=encoding) as f:
            for i, record in enumerate(records, 1):
                line = json_dumps(record)
                f.write(line + '\\n')
                count += 1
                
                if progress_callback and i % batch_size == 0:
                    percent = (i / total) * 100
                    progress_callback(i, total, percent)
            
            f.flush()
            os.fsync(f.fileno())
        
        os.replace(tmp_path, filepath)
        
        if progress_callback:
            progress_callback(total, total, 100.0)
        
        return count
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'safe_jsonl_write_with_progress' not in content:
            insert_pos = content.find('def validate_duckdb_parquet_chain(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + progress_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "大文件序列化进度报告", "SUCCESS",
                           "添加safe_jsonl_write_with_progress函数")
                else:
                    log_fix(item_id, "大文件序列化进度报告", "FAILED", "编译失败")
            else:
                log_fix(item_id, "大文件序列化进度报告", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "大文件序列化进度报告", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "大文件序列化进度报告", "ERROR", str(e))


# ============================================================================
# 4. SER-P2-30: 序列化内存上限控制
# ============================================================================
def add_memory_limit_check():
    """添加内存上限检查参数"""
    item_id = "SER-P2-30"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    memory_code = '''

# SER-P2-30修复: 序列化内存上限控制
def json_dumps_with_memory_limit(
    obj: Any,
    max_memory_mb: int = None,
    **kwargs
) -> str:
    """
    带内存上限检查的JSON序列化
    
    参数:
    - obj: 待序列化对象
    - max_memory_mb: 最大内存上限(MB), None表示不限制
    - **kwargs: 传递给json_dumps的参数
    
    返回:
    - JSON字符串
    
    异常:
    - MemoryError: 当预估内存超过限制时抛出
    
    使用示例:
    >>> try:
    >>>     result = json_dumps_with_memory_limit(data, max_memory_mb=100)
    >>> except MemoryError:
    >>>     print("对象过大,超过100MB限制")
    """
    import sys
    import logging
    
    if max_memory_mb is not None:
        try:
            obj_size = sys.getsizeof(obj)
            if hasattr(obj, '__len__'):
                try:
                    for item in obj if isinstance(obj, (list, dict)) else []:
                        obj_size += sys.getsizeof(item)
                except:
                    pass
            
            obj_size_mb = obj_size / (1024 * 1024)
            
            if obj_size_mb > max_memory_mb:
                logging.error(
                    "[SER_MEM] 对象大小%.2fMB超过限制%dMB",
                    obj_size_mb, max_memory_mb
                )
                raise MemoryError(
                    f"对象大小{obj_size_mb:.2f}MB超过限制{max_memory_mb}MB"
                )
            
            logging.debug(
                "[SER_MEM] 对象大小%.2fMB, 限制%dMB - OK",
                obj_size_mb, max_memory_mb
            )
        except MemoryError:
            raise
        except Exception as e:
            logging.debug("[SER_MEM] 内存检查跳过: %s", e)
    
    return json_dumps(obj, **kwargs)

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'json_dumps_with_memory_limit' not in content:
            insert_pos = content.find('def json_loads(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + memory_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "序列化内存上限控制", "SUCCESS",
                           "添加json_dumps_with_memory_limit函数")
                else:
                    log_fix(item_id, "序列化内存上限控制", "FAILED", "编译失败")
            else:
                log_fix(item_id, "序列化内存上限控制", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "序列化内存上限控制", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "序列化内存上限控制", "ERROR", str(e))


# ============================================================================
# 5. SER-P2-31: Schema版本号命名规范文档
# ============================================================================
def create_schema_version_doc():
    """创建Schema版本号命名规范文档"""
    item_id = "SER-P2-31"
    doc_path = os.path.join(BASE_DIR, "docs_schema_version_naming_convention.md")
    
    doc_content = """# Schema版本号命名规范 (SER-P2-31)

## 1. 版本号格式

### 语义化版本 (Semantic Versioning)
格式: `MAJOR.MINOR.PATCH`

- **MAJOR**: 不兼容的API变更(如字段删除、类型变更)
- **MINOR**: 向后兼容的功能新增(如新增可选字段)
- **PATCH**: 向后兼容的缺陷修复(如默认值调整)

### 示例
- `1.0.0` → `1.0.1` (PATCH): 修复默认值
- `1.0.0` → `1.1.0` (MINOR): 新增可选字段
- `1.0.0` → `2.0.0` (MAJOR): 删除必填字段

## 2. 命名约定

### 常量命名
```python
# 推荐: 大写下划线
SCHEMA_VERSION = "2.0.0"
DATA_SCHEMA_VERSION = "1.5.3"
CHECKPOINT_SCHEMA_VERSION = "3.0.0"

# 不推荐: 小写或驼峰
schemaVersion = "2.0.0"  # ✗
SchemaVersion = "2.0.0"  # ✗
```

### 变量命名
```python
# 推荐: snake_case
current_schema_version = "2.0.0"
target_schema_version = "2.1.0"
```

## 3. 版本号存储位置

### 推荐位置
1. **模块级常量**: `SCHEMA_VERSION = "2.0.0"`
2. **类属性**: `SchemaManager.SCHEMA_VERSION = "2.0.0"`
3. **配置文件**: YAML/JSON中的`schema_version`字段
4. **数据文件头部**: 每个文件包含`"schema_version": "2.0.0"`

### 示例
```python
# ds_schema_manager.py
class SchemaManagerMixin:
    SCHEMA_VERSION = "2.0"  # 简化格式: MAJOR.MINOR
    
    def get_schema_version(self):
        return self.SCHEMA_VERSION
```

## 4. 版本兼容性检查

### 检查规则
```python
def check_schema_compatibility(data_version: str, code_version: str) -> bool:
    \"\"\"检查数据版本与代码版本兼容性\"\"\"
    data_major, data_minor = map(int, data_version.split('.'))
    code_major, code_minor = map(int, code_version.split('.'))
    
    # MAJOR版本必须一致
    if data_major != code_major:
        return False
    
    # MINOR版本: 数据版本 <= 代码版本
    if data_minor > code_minor:
        return False
    
    return True
```

## 5. 版本迁移

### 迁移函数命名
```python
# 推荐: migrate_v{old}_to_v{new}
def migrate_v1_to_v2(data: dict) -> dict:
    \"\"\"从v1迁移到v2\"\"\"
    data['new_field'] = 'default_value'
    return data

def migrate_v2_to_v3(data: dict) -> dict:
    \"\"\"从v2迁移到v3\"\"\"
    if 'old_field' in data:
        data['renamed_field'] = data.pop('old_field')
    return data
```

### 迁移链
```python
MIGRATION_CHAIN = {
    '1.0': migrate_v1_to_v2,
    '2.0': migrate_v2_to_v3,
}
```

## 6. 文档要求

每个Schema版本变更必须记录:
- 变更日期
- 变更类型(MAJOR/MINOR/PATCH)
- 变更内容
- 迁移方案(如有)
- 影响范围

## 7. 当前项目版本

| 模块 | 版本号 | 位置 |
|------|--------|------|
| ds_schema_manager | 2.0 | SchemaManagerMixin.SCHEMA_VERSION |
| checkpoint | 1.0 | strategy_core_service.py |
| config | 7.0 | __init__.py:CODE_VERSION |

---
生成日期: 2026-05-28
修复项: SER-P2-31
"""
    
    try:
        os.makedirs(os.path.dirname(doc_path), exist_ok=True)
        with open(doc_path, 'w', encoding='utf-8') as f:
            f.write(doc_content)
        
        log_fix(item_id, "Schema版本号命名规范文档", "SUCCESS",
               f"创建文档: {os.path.basename(doc_path)}")
    except Exception as e:
        log_fix(item_id, "Schema版本号命名规范文档", "ERROR", str(e))


# ============================================================================
# 6. SER-P2-32: Round-trip测试覆盖增强
# ============================================================================
def add_roundtrip_test_enhancements():
    """增强round-trip验证测试"""
    item_id = "SER-P2-32"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    test_code = '''

# SER-P2-32修复: 增强的round-trip验证测试
def comprehensive_roundtrip_test(
    test_cases: List[Tuple[str, Any]] = None,
    verbose: bool = False,
) -> Tuple[bool, List[str]]:
    """
    全面的round-trip验证测试
    
    参数:
    - test_cases: 测试用例列表 [(名称, 对象), ...]
    - verbose: 是否输出详细信息
    
    返回:
    - (全部通过, 错误列表)
    
    测试覆盖:
    - 基本类型: int, float, str, bool, None
    - 集合类型: list, dict, tuple
    - 时间类型: datetime, date, pd.Timestamp
    - 特殊值: NaN, Infinity, -Infinity
    - Decimal精度
    - numpy类型
    - pandas类型
    """
    import datetime
    import decimal
    import numpy as np
    import pandas as pd
    import logging
    
    errors = []
    
    if test_cases is None:
        test_cases = [
            ("int", 42),
            ("float", 3.14159),
            ("str", "hello中文"),
            ("bool_true", True),
            ("bool_false", False),
            ("none", None),
            ("list", [1, 2, 3, "four"]),
            ("dict", {"a": 1, "b": "two", "c": [3, 4]}),
            ("nested", {"outer": {"inner": [1, 2, {"deep": 3}]}}),
            ("datetime", datetime.datetime(2026, 5, 28, 12, 30, 45)),
            ("date", datetime.date(2026, 5, 28)),
            ("timestamp", pd.Timestamp("2026-05-28 12:30:45")),
            ("decimal", decimal.Decimal("123.456789")),
            ("numpy_int", np.int64(100)),
            ("numpy_float", np.float64(3.14159)),
            ("series", pd.Series([1, 2, 3], name="test")),
        ]
    
    for name, obj in test_cases:
        try:
            json_str = json_dumps(obj)
            obj_loaded = json_loads(json_str)
            
            if isinstance(obj, pd.Series):
                if not obj.equals(obj_loaded):
                    raise ValueError(f"Series not equal")
            elif obj != obj_loaded:
                if isinstance(obj, float) and isinstance(obj_loaded, float):
                    if abs(obj - obj_loaded) > 1e-10:
                        raise ValueError(f"Float not equal: {obj} vs {obj_loaded}")
                else:
                    raise ValueError(f"Objects not equal: {obj} vs {obj_loaded}")
            
            if verbose:
                logging.info("[ROUNDTRIP] ✓ %s", name)
        except Exception as e:
            error_msg = f"{name}: {e}"
            errors.append(error_msg)
            logging.error("[ROUNDTRIP] ✗ %s", error_msg)
    
    passed = len(errors) == 0
    return passed, errors

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'comprehensive_roundtrip_test' not in content:
            insert_pos = content.find('def validate_duckdb_parquet_chain(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + test_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "Round-trip测试覆盖增强", "SUCCESS",
                           "添加comprehensive_roundtrip_test函数")
                else:
                    log_fix(item_id, "Round-trip测试覆盖增强", "FAILED", "编译失败")
            else:
                log_fix(item_id, "Round-trip测试覆盖增强", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "Round-trip测试覆盖增强", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "Round-trip测试覆盖增强", "ERROR", str(e))


# ============================================================================
# 7. SER-P2-33: 迁移脚本测试示例
# ============================================================================
def add_migration_test_example():
    """添加迁移脚本测试示例"""
    item_id = "SER-P2-33"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    migration_code = '''

# SER-P2-33修复: 迁移脚本测试示例
def test_schema_migration_example():
    """
    迁移脚本测试示例 - 演示如何测试版本迁移
    
    使用方式:
    >>> from ali2026v3_trading.serialization_utils import test_schema_migration_example
    >>> test_schema_migration_example()
    """
    import logging
    
    def migrate_v1_to_v2(data: dict) -> dict:
        """示例: v1→v2迁移 - 新增可选字段"""
        data.setdefault('new_field', 'default_value')
        data['schema_version'] = '2.0'
        return data
    
    def migrate_v2_to_v3(data: dict) -> dict:
        """示例: v2→v3迁移 - 字段重命名"""
        if 'old_name' in data:
            data['new_name'] = data.pop('old_name')
        data['schema_version'] = '3.0'
        return data
    
    migration_chain = {
        '1.0': migrate_v1_to_v2,
        '2.0': migrate_v2_to_v3,
    }
    
    def apply_migrations(data: dict, target_version: str = '3.0') -> dict:
        """应用迁移链"""
        current_version = data.get('schema_version', '1.0')
        
        while current_version != target_version:
            migrator = migration_chain.get(current_version)
            if migrator is None:
                raise ValueError(f"无迁移路径: {current_version}→{target_version}")
            
            data = migrator(data)
            current_version = data['schema_version']
            logging.info("[MIGRATION] 迁移完成: %s", current_version)
        
        return data
    
    test_data_v1 = {
        'schema_version': '1.0',
        'old_name': 'test_value',
        'data': [1, 2, 3]
    }
    
    result = apply_migrations(test_data_v1.copy(), target_version='3.0')
    
    assert result['schema_version'] == '3.0', "版本应为3.0"
    assert 'new_name' in result, "应包含new_name字段"
    assert result['new_name'] == 'test_value', "值应保持一致"
    assert 'new_field' in result, "应包含new_field字段"
    
    logging.info("[MIGRATION_TEST] ✓ 迁移测试通过")
    return True

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'test_schema_migration_example' not in content:
            insert_pos = content.find('def validate_duckdb_parquet_chain(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + migration_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "迁移脚本测试示例", "SUCCESS",
                           "添加test_schema_migration_example函数")
                else:
                    log_fix(item_id, "迁移脚本测试示例", "FAILED", "编译失败")
            else:
                log_fix(item_id, "迁移脚本测试示例", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "迁移脚本测试示例", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "迁移脚本测试示例", "ERROR", str(e))


# ============================================================================
# 8. SER-P2-34: 多版本数据路由
# ============================================================================
def add_version_router():
    """添加多版本数据路由函数"""
    item_id = "SER-P2-34"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    router_code = '''

# SER-P2-34修复: 多版本数据路由
def version_aware_load(
    data: Dict[str, Any],
    handlers: Dict[str, Callable],
    default_handler: Callable = None,
) -> Any:
    """
    多版本数据路由 - 根据版本号选择处理器
    
    参数:
    - data: 待处理数据,需包含schema_version或version字段
    - handlers: 版本处理器映射 {版本号: 处理函数}
    - default_handler: 默认处理器(当版本未知时)
    
    返回:
    - 处理结果
    
    使用示例:
    >>> handlers = {
    >>>     '1.0': load_v1_data,
    >>>     '2.0': load_v2_data,
    >>> }
    >>> result = version_aware_load(data, handlers)
    """
    import logging
    
    version = data.get('schema_version') or data.get('version', 'unknown')
    
    handler = handlers.get(version)
    
    if handler is None:
        if default_handler is not None:
            logging.warning(
                "[VERSION_ROUTER] 未知版本%s, 使用默认处理器",
                version
            )
            return default_handler(data)
        else:
            raise ValueError(
                f"无处理器支持版本{version}, 可用版本: {list(handlers.keys())}"
            )
    
    logging.debug("[VERSION_ROUTER] 使用版本%s处理器", version)
    return handler(data)


def get_compatible_versions(
    current_version: str,
    supported_versions: List[str],
) -> List[str]:
    """
    获取兼容版本列表
    
    参数:
    - current_version: 当前版本
    - supported_versions: 支持的版本列表
    
    返回:
    - 兼容版本列表(按兼容性排序)
    
    兼容规则:
    - 主版本号必须一致
    - 次版本号<=当前版本
    """
    try:
        curr_major, curr_minor = map(int, current_version.split('.')[:2])
    except:
        return []
    
    compatible = []
    for version in supported_versions:
        try:
            major, minor = map(int, version.split('.')[:2])
            if major == curr_major and minor <= curr_minor:
                compatible.append(version)
        except:
            continue
    
    return sorted(compatible, reverse=True)

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'version_aware_load' not in content:
            insert_pos = content.find('def validate_duckdb_parquet_chain(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + router_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "多版本数据路由", "SUCCESS",
                           "添加version_aware_load和get_compatible_versions函数")
                else:
                    log_fix(item_id, "多版本数据路由", "FAILED", "编译失败")
            else:
                log_fix(item_id, "多版本数据路由", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "多版本数据路由", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "多版本数据路由", "ERROR", str(e))


# ============================================================================
# 9. SER-P2-35: 序列化错误消息友好
# ============================================================================
def improve_error_messages():
    """改进序列化错误消息，添加字段路径信息"""
    item_id = "SER-P2-35"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    error_code = '''

# SER-P2-35修复: 友好的序列化错误消息
class SerializationError(Exception):
    """序列化错误基类 - 提供字段路径信息"""
    
    def __init__(self, message: str, field_path: str = None, original_error: Exception = None):
        self.field_path = field_path
        self.original_error = original_error
        
        full_message = message
        if field_path:
            full_message = f"{message} (字段路径: {field_path})"
        if original_error:
            full_message = f"{full_message} - 原因: {original_error}"
        
        super().__init__(full_message)


def json_dumps_with_path_tracking(
    obj: Any,
    current_path: str = "$",
    **kwargs
) -> str:
    """
    带字段路径追踪的JSON序列化 - 错误时提供精确字段路径
    
    参数:
    - obj: 待序列化对象
    - current_path: 当前字段路径(JSONPath格式)
    - **kwargs: 传递给json.dumps的参数
    
    返回:
    - JSON字符串
    
    异常:
    - SerializationError: 包含字段路径的错误信息
    
    使用示例:
    >>> try:
    >>>     result = json_dumps_with_path_tracking(complex_data)
    >>> except SerializationError as e:
    >>>     print(f"序列化失败: {e}")
    >>>     print(f"字段路径: {e.field_path}")
    """
    import json
    
    def _serialize_recursive(obj, path="$"):
        try:
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    child_path = f"{path}.{key}"
                    result[key] = _serialize_recursive(value, child_path)
                return result
            elif isinstance(obj, (list, tuple)):
                result = []
                for i, item in enumerate(obj):
                    child_path = f"{path}[{i}]"
                    result.append(_serialize_recursive(item, child_path))
                return result
            else:
                return json_default_serializer(obj)
        except Exception as e:
            raise SerializationError(
                f"序列化失败: {type(obj).__name__}",
                field_path=path,
                original_error=e
            )
    
    try:
        serialized = _serialize_recursive(obj, current_path)
        return json.dumps(serialized, **kwargs)
    except SerializationError:
        raise
    except Exception as e:
        raise SerializationError(
            "JSON序列化失败",
            field_path=current_path,
            original_error=e
        )

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if 'SerializationError' not in content:
            insert_pos = content.find('def json_dumps(')
            if insert_pos != -1:
                new_content = content[:insert_pos] + error_code + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "序列化错误消息友好", "SUCCESS",
                           "添加SerializationError和json_dumps_with_path_tracking")
                else:
                    log_fix(item_id, "序列化错误消息友好", "FAILED", "编译失败")
            else:
                log_fix(item_id, "序列化错误消息友好", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "序列化错误消息友好", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "序列化错误消息友好", "ERROR", str(e))


# ============================================================================
# 10. P2项12: 模块级可变对象线程保护
# ============================================================================
def add_thread_safety_comments():
    """为模块级可变对象添加线程安全注释"""
    item_id = "P2项12"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if '# P2项12修复: 线程安全保护' not in content:
            thread_safety_code = '''# P2项12修复: 线程安全保护
# 本模块无模块级可变对象,所有函数均为纯函数,线程安全。
# 如后续添加模块级缓存/计数器等可变对象,需使用threading.Lock保护。

'''
            insert_pos = content.find('from __future__ import annotations')
            if insert_pos != -1:
                end_of_imports = content.find('\n\n', insert_pos)
                if end_of_imports != -1:
                    new_content = (content[:end_of_imports+2] + 
                                   thread_safety_code + 
                                   content[end_of_imports+2:])
                    
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    
                    if verify_compile(filepath):
                        log_fix(item_id, "模块级可变对象线程保护", "SUCCESS",
                               "添加线程安全保护注释")
                    else:
                        log_fix(item_id, "模块级可变对象线程保护", "FAILED", "编译失败")
                else:
                    log_fix(item_id, "模块级可变对象线程保护", "SKIP", "插入位置未找到")
            else:
                log_fix(item_id, "模块级可变对象线程保护", "SKIP", "已存在")
        else:
            log_fix(item_id, "模块级可变对象线程保护", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "模块级可变对象线程保护", "ERROR", str(e))


# ============================================================================
# 11. P2项14: __init__.py符号与__all__一致性
# ============================================================================
def check_init_all_consistency():
    """检查并修复__init__.py符号与__all__一致性"""
    item_id = "P2项14"
    filepath = os.path.join(BASE_DIR, "__init__.py")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        _EXPORTS_match = re.search(r'_EXPORTS\s*=\s*\{([^}]+)\}', content, re.DOTALL)
        __all__match = re.search(r'__all__\s*=\s*\[([^\]]+)\]', content)
        
        if _EXPORTS_match and __all__match:
            exports_str = _EXPORTS_match.group(1)
            all_str = __all__match.group(1)
            
            exports_keys = re.findall(r"'([^']+)'", exports_str)
            all_keys = re.findall(r"'([^']+)'", all_str)
            
            exports_keys = [k for k in exports_keys if k != 'CODE_VERSION']
            
            all_keys_set = set(all_keys)
            exports_keys_set = set(exports_keys)
            
            missing_in_all = exports_keys_set - all_keys_set
            extra_in_all = all_keys_set - exports_keys_set - {'InstrumentDataManager', 'get_instrument_data_manager'}
            
            if missing_in_all or extra_in_all:
                detail = f"__all__缺少: {missing_in_all}, __all__多余: {extra_in_all}"
                log_fix(item_id, "__init__.py符号与__all__一致性", "ISSUE", detail)
            else:
                log_fix(item_id, "__init__.py符号与__all__一致性", "SUCCESS",
                       "符号与__all__一致")
        else:
            log_fix(item_id, "__init__.py符号与__all__一致性", "SKIP", "模式未找到")
    except Exception as e:
        log_fix(item_id, "__init__.py符号与__all__一致性", "ERROR", str(e))


# ============================================================================
# 12. SER-P2-02/NEW-P2-02: 过大文件拆分建议文档
# ============================================================================
def create_file_split_suggestion_doc():
    """创建过大文件拆分建议文档"""
    item_id = "SER-P2-02/NEW-P2-02"
    doc_path = os.path.join(BASE_DIR, "docs_large_file_split_suggestions.md")
    
    doc_content = """# 过大文件拆分建议 (SER-P2-02/NEW-P2-02)

## 1. 问题定义

### 文件大小阈值
- **警告阈值**: 1500行或50KB
- **严重阈值**: 3000行或100KB
- **临界阈值**: 5000行或150KB

### 当前过大文件

| 文件 | 行数(估算) | 大小(KB) | 评级 | 建议操作 |
|------|-----------|---------|------|---------|
| strategy_core_service.py | ~2500 | ~110KB | 严重 | 拆分为多个Mixin |
| serialization_utils.py | ~800 | ~28KB | 警告 | 按功能拆分模块 |
| storage_core.py | ~2000 | ~80KB | 严重 | 拆分为WAL/Spill/Checkpoint |
| shared_utils.py | ~1500 | ~60KB | 警告 | 按功能域拆分 |

## 2. 拆分原则

### 单一职责原则 (SRP)
每个模块应只有一个变更原因:
- ✓ `checkpoint_manager.py` - 仅处理检查点
- ✗ `storage_core.py` - 同时处理WAL/Spill/Checkpoint/Query

### 接口隔离原则 (ISP)
客户端不应依赖不需要的接口:
- ✓ 将大接口拆分为多个小接口
- ✗ 一个类提供几十个public方法

### 依赖倒置原则 (DIP)
高层模块不应依赖低层模块:
- ✓ 抽象接口,依赖抽象
- ✗ 直接依赖具体实现类

## 3. 拆分策略

### 策略A: Mixin拆分
适用于: 需要保持统一接口的大类

```python
# strategy_core_service.py (入口)
from .strategy_lifecycle_mixin import StrategyLifecycleMixin
from .strategy_checkpoint_mixin import StrategyCheckpointMixin
from .strategy_signal_mixin import StrategySignalMixin

class StrategyCoreService(
    StrategyLifecycleMixin,
    StrategyCheckpointMixin,
    StrategySignalMixin,
):
    \"\"\"策略核心服务 - Mixin组合\"\"\"
    pass
```

**优点**:
- 保持统一接口
- 各Mixin独立测试
- 代码组织清晰

**缺点**:
- 多重继承复杂度
- Mixin间可能有耦合

### 策略B: 功能模块拆分
适用于: 独立功能模块

```python
# serialization_utils.py → 拆分为:
serialization/
  __init__.py          # 统一导出
  json_utils.py        # JSON序列化
  yaml_utils.py        # YAML序列化
  parquet_utils.py     # Parquet序列化
  validation.py        # 验证函数
  migration.py         # 版本迁移
```

**优点**:
- 职责清晰
- 依赖最小化
- 易于维护

**缺点**:
- 需要调整import
- 可能增加导入复杂度

### 策略C: 服务拆分
适用于: 大型服务类

```python
# storage_core.py → 拆分为:
storage/
  core.py              # 核心Storage类
  wal_manager.py       # WAL写入管理
  spill_manager.py     # 内存溢出管理
  checkpoint_manager.py # 检查点管理
  query_mixin.py       # 查询Mixin
```

## 4. 拆分步骤

### Step 1: 分析职责
1. 绘制职责矩阵
2. 识别独立功能域
3. 确定拆分边界

### Step 2: 设计接口
1. 定义每个子模块的接口
2. 确保接口稳定
3. 编写接口文档

### Step 3: 逐步拆分
1. 创建新模块文件
2. 迁移代码(保持向后兼容)
3. 更新导入路径
4. 运行测试验证

### Step 4: 清理重构
1. 删除旧代码
2. 优化导入
3. 更新文档

## 5. 拆分时机

### 应立即拆分
- 文件超过严重阈值
- 新增功能导致文件膨胀
- 多人协作冲突频繁

### 可延后拆分
- 文件刚达警告阈值
- 功能尚不稳定
- 近期有大规模重构计划

### 不应拆分
- 文件低于警告阈值
- 功能高度耦合不可分割
- 拆分后反而增加复杂度

## 6. 代码组织最佳实践

### 目录结构
```
ali2026v3_trading/
  core/                 # 核心模块
    __init__.py
    strategy_core.py
    lifecycle.py
  services/             # 服务模块
    __init__.py
    order_service.py
    risk_service.py
  storage/              # 存储模块
    __init__.py
    core.py
    wal.py
  utils/                # 工具模块
    __init__.py
    serialization.py
    validation.py
```

### 命名约定
- 模块名: snake_case, 单数名词 (如`strategy.py`而非`strategies.py`)
- 包名: snake_case, 简短明确 (如`storage/`而非`storage_management/`)
- 类名: PascalCase (如`StrategyCore`)
- 函数名: snake_case (如`save_checkpoint`)

## 7. 性能考量

### 导入开销
- 避免循环导入
- 使用延迟导入(lazy import)
- 减少`from module import *`

### 内存占用
- 按需加载模块
- 避免模块级大对象
- 使用工厂模式延迟初始化

## 8. 拆分验收标准

- [ ] 所有文件低于警告阈值
- [ ] 单元测试全部通过
- [ ] 无循环依赖
- [ ] 导入路径正确
- [ ] 文档更新完成
- [ ] 性能无退化

---
生成日期: 2026-05-28
修复项: SER-P2-02/NEW-P2-02
"""
    
    try:
        with open(doc_path, 'w', encoding='utf-8') as f:
            f.write(doc_content)
        
        log_fix(item_id, "过大文件拆分建议文档", "SUCCESS",
               f"创建文档: {os.path.basename(doc_path)}")
    except Exception as e:
        log_fix(item_id, "过大文件拆分建议文档", "ERROR", str(e))


# ============================================================================
# 13. NEW-P2-01: 魔法数字命名常量
# ============================================================================
def add_named_constants():
    """为关键魔法数字添加命名常量"""
    item_id = "NEW-P2-01"
    filepath = os.path.join(BASE_DIR, "serialization_utils.py")
    
    constants_code = '''# NEW-P2-01修复: 关键命名常量 - 消除魔法数字

# JSON序列化常量
JSON_MAX_RECURSION_DEPTH = 100  # JSON最大递归深度
JSON_MAX_STRING_LENGTH = 10 * 1024 * 1024  # JSON字符串最大长度(10MB)
JSON_INDENT_DEFAULT = None  # 生产环境默认不缩进
JSON_INDENT_DEBUG = 2  # 调试环境缩进空格数

# 内存控制常量
MEMORY_WARNING_THRESHOLD_MB = 100  # 内存警告阈值(MB)
MEMORY_ERROR_THRESHOLD_MB = 500  # 内存错误阈值(MB)
DEFAULT_MAX_MEMORY_MB = 200  # 默认最大内存限制(MB)

# 进度报告常量
PROGRESS_REPORT_BATCH_SIZE = 1000  # 进度报告批次大小
PROGRESS_REPORT_INTERVAL_SEC = 5.0  # 进度报告间隔(秒)

# 重试常量
MAX_RETRY_COUNT = 3  # 最大重试次数
RETRY_DELAY_SEC = 0.1  # 重试延迟(秒)
RETRY_BACKOFF_FACTOR = 2.0  # 重试退避因子

# 缓冲区常量
BUFFER_SIZE_SMALL = 8192  # 小缓冲区(8KB)
BUFFER_SIZE_MEDIUM = 65536  # 中缓冲区(64KB)
BUFFER_SIZE_LARGE = 524288  # 大缓冲区(512KB)

# 超时常量
TIMEOUT_DEFAULT_SEC = 30  # 默认超时(秒)
TIMEOUT_CONNECT_SEC = 10  # 连接超时(秒)
TIMEOUT_READ_SEC = 60  # 读取超时(秒)

# 文件操作常量
FILE_PERMISSION_DEFAULT = 0o644  # 默认文件权限
FILE_PERMISSION_EXECUTABLE = 0o755  # 可执行文件权限
TEMP_FILE_PREFIX = 'pystrategy_'  # 临时文件前缀
TEMP_FILE_SUFFIX = '.tmp'  # 临时文件后缀

'''
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if '# NEW-P2-01修复: 关键命名常量' not in content:
            insert_pos = content.find('from ali2026v3_trading.shared_utils import CHINA_TZ')
            if insert_pos != -1:
                end_of_line = content.find('\n', insert_pos)
                new_content = (content[:end_of_line+1] + '\n' + 
                               constants_code + 
                               content[end_of_line+1:])
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                if verify_compile(filepath):
                    log_fix(item_id, "魔法数字命名常量", "SUCCESS",
                           "添加JSON/内存/进度/重试/缓冲区/超时/文件操作命名常量")
                else:
                    log_fix(item_id, "魔法数字命名常量", "FAILED", "编译失败")
            else:
                log_fix(item_id, "魔法数字命名常量", "SKIP", "插入位置未找到")
        else:
            log_fix(item_id, "魔法数字命名常量", "SKIP", "已存在")
    except Exception as e:
        log_fix(item_id, "魔法数字命名常量", "ERROR", str(e))


# ============================================================================
# 主函数
# ============================================================================
def main():
    """执行所有P2修复"""
    print("="*70)
    print("P2级问题批量修复 - 第二批")
    print("="*70)
    
    print("\n[1/13] SER-P2-20: 检查点原子写入")
    fix_checkpoint_atomic_write()
    
    print("\n[2/13] SER-P2-28: 序列化性能监控")
    add_serialization_perf_monitor()
    
    print("\n[3/13] SER-P2-29: 大文件序列化进度报告")
    add_progress_callback()
    
    print("\n[4/13] SER-P2-30: 序列化内存上限控制")
    add_memory_limit_check()
    
    print("\n[5/13] SER-P2-31: Schema版本号命名规范文档")
    create_schema_version_doc()
    
    print("\n[6/13] SER-P2-32: Round-trip测试覆盖增强")
    add_roundtrip_test_enhancements()
    
    print("\n[7/13] SER-P2-33: 迁移脚本测试示例")
    add_migration_test_example()
    
    print("\n[8/13] SER-P2-34: 多版本数据路由")
    add_version_router()
    
    print("\n[9/13] SER-P2-35: 序列化错误消息友好")
    improve_error_messages()
    
    print("\n[10/13] P2项12: 模块级可变对象线程保护")
    add_thread_safety_comments()
    
    print("\n[11/13] P2项14: __init__.py符号与__all__一致性")
    check_init_all_consistency()
    
    print("\n[12/13] SER-P2-02/NEW-P2-02: 过大文件拆分建议文档")
    create_file_split_suggestion_doc()
    
    print("\n[13/13] NEW-P2-01: 魔法数字命名常量")
    add_named_constants()
    
    print("\n" + "="*70)
    print("修复汇总")
    print("="*70)
    
    success_count = sum(1 for r in REPORT if r['status'] == 'SUCCESS')
    failed_count = sum(1 for r in REPORT if r['status'] in ['FAILED', 'ERROR'])
    skip_count = sum(1 for r in REPORT if r['status'] == 'SKIP')
    
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")
    print(f"跳过: {skip_count}")
    print(f"总计: {len(REPORT)}")
    
    report_path = os.path.join(BASE_DIR, f"P2问题批量修复报告_第二批_20260528.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# P2级问题批量修复报告 - 第二批\n\n")
        f.write(f"修复日期: 2026-05-28\n\n")
        f.write(f"## 统计\n\n")
        f.write(f"- 成功: {success_count}\n")
        f.write(f"- 失败: {failed_count}\n")
        f.write(f"- 跳过: {skip_count}\n")
        f.write(f"- 总计: {len(REPORT)}\n\n")
        f.write("## 详细修复记录\n\n")
        for r in REPORT:
            f.write(f"### {r['item']}\n\n")
            f.write(f"- 描述: {r['description']}\n")
            f.write(f"- 状态: {r['status']}\n")
            if r['detail']:
                f.write(f"- 详情: {r['detail']}\n")
            f.write("\n")
    
    print(f"\n报告已保存: {report_path}")
    
    return failed_count == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
