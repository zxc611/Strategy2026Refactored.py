# 过大文件拆分建议 (SER-P2-02/NEW-P2-02)

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
    """策略核心服务 - Mixin组合"""
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
