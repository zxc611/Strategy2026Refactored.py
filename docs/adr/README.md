# ADR Template — Architecture Decision Records

## ADR-NNN: [决策标题]

### 状态
[提议 | 已接受 | 已废弃 | 已替代]

### 日期
YYYY-MM-DD

### 上下文
[触发此决策的问题/约束/需求]

### 决策
[做出的具体决策]

### 理由
[为什么选择此方案，替代方案及排除原因]

### 后果
[此决策带来的正面和负面影响]

---

## ADR-001: DuckDB引用通过db_adapter统一隔离

### 状态
已接受

### 日期
2026-05-31

### 上下文
P0.1审计发现全项目30处`import duckdb`，其中21处散布在业务文件中，违反信息隐藏原则。业务代码直接调用duckdb API，导致DB迁移时需修改所有业务文件。

### 决策
1. 所有DuckDB连接通过`db_adapter.py`统一隔离
2. 新增`data_access.py`提供5类领域接口(Protocol)
3. 业务文件`import duckdb`全部迁移为`from ali2026v3_trading.db_adapter import ...`
4. 基础设施层(db_adapter.py/ds_db_connection.py)保留直接import

### 理由
- 替代方案A(全项目零import)：基础设施层(db_adapter自身)不可能零import
- 替代方案B(保持现状)：每次DB迁移需改21处，风险高
- 当前方案：业务层零直引+基础设施层封装，迁移时仅改db_adapter.py

### 后果
- 正面：DB迁移成本从21文件降至1文件，业务代码领域意图清晰
- 负面：增加1层间接调用(微秒级延迟)，需维护db_adapter接口兼容性

---

## ADR-002: Mixin分层策略(4核心+数据层+UI)

### 状态
已接受

### 日期
2026-05-31

### 上下文
StrategyCoreService继承4个核心Mixin(Lifecycle/Instrument/Historical/Tick)，数据层8个Mixin(ds_*/storage_*)由DataService组合使用。

### 决策
1. 核心Mixin按解耦难度分3组：(1)可独立化 (2)需状态重构 (3)渐进迁移
2. 数据层Mixin通过DataService组合而非继承
3. UIMixin独立于核心，UI重构阶段处理

### 理由
- Mixin方法数差异大(Lifecycle 60 vs Instrument 8)，统一策略不可行
- 数据层Mixin(ds_*)已有清晰边界，无需额外解耦
- 隐式self._属性数(TickHandler 103)决定重构顺序

### 后果
- 正面：解耦优先级明确，高风险Mixin(103隐式属性)后处理
- 负面：_LifecycleMixin短期保留继承，渐进迁移周期长
