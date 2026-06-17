# Schema版本号命名规范 (SER-P2-31)

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
    """检查数据版本与代码版本兼容性"""
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
    """从v1迁移到v2"""
    data['new_field'] = 'default_value'
    return data

def migrate_v2_to_v3(data: dict) -> dict:
    """从v2迁移到v3"""
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
