# storage.py InstrumentDataManager.__init__ 57步初始化分析报告

> 分析日期: 2026-05-03
> 分析对象: ali2026v3_trading/storage.py InstrumentDataManager.__init__
> 代码范围: 第55行 ~ 第149行
> 分析结论: **57步初始化，17步含副作用，14步含失败模式，错误恢复能力=NONE，整体风险=HIGH**

---

## 一、逐项初始化步骤清单

| # | 行号 | 代码/逻辑块 | 副作用 | 失败模式 | 依赖前置步骤 | 异常安全 |
|---|------|-------------|--------|----------|-------------|---------|
| 1 | 61 | `self.db_path = db_path or _get_default_db_path()` | 文件系统路径计算(间接调用config_service) | config_service导入失败(有fallback), 路径生成失败 | 无 | 有fallback |
| 2 | 62 | `self.max_retries = max_retries` | 无 | 无 | 无 | N/A |
| 3 | 63 | `self.retry_delay = retry_delay` | 无 | 无 | 无 | N/A |
| 4 | 64 | `self.batch_size = batch_size` | 无 | 无 | 无 | N/A |
| 5 | 65 | `self._writer_batch_tasks = max(5, min(20, int(batch_size or 1)))` | 无 | batch_size=0时int(1)不会失败 | #4 | 安全 |
| 6 | 66 | `self.drop_on_full = drop_on_full` | 无 | 无 | 无 | N/A |
| 7 | 68-70 | `db_dir`计算 + `os.makedirs(db_dir, exist_ok=True)` | **文件I/O**: 创建目录 | 权限不足、磁盘满、路径非法 | #1 | 无保护 |
| 8 | 72 | `self._data_service = get_data_service()` | **外部调用**: 获取DuckDB DataService单例 | DuckDB连接失败、模块导入失败 | #1(db_path) | 无保护 |
| 9 | 76 | `self._TICK_SHARD_COUNT = 16` | 无 | 无 | 无 | N/A |
| 10 | 77 | `self._TICK_WRITER_COUNT = 6` | 无 | 无 | 无 | N/A |
| 11 | 78-79 | `from shared_utils import get_shard_router` + `self._shard_router = get_shard_router(...)` | **外部调用**: 导入模块+创建ShardRouter | shared_utils模块不存在 | #9 | 无保护 |
| 12 | 80-81 | `_shard_cap`计算 + `self._tick_shard_queues = [queue.Queue(...) for _ in range(16)]` | 无(内存分配) | 内存不足(OOM) | #9 | 无保护 |
| 13 | 82 | `self._tick_shard_writers: list = []` | 无 | 无 | 无 | N/A |
| 14 | 83 | `self._kline_queue = queue.Queue(maxsize=100000)` | 无 | 内存不足 | 无 | N/A |
| 15 | 84 | `self._kline_writer_thread = None` | 无 | 无 | 无 | N/A |
| 16 | 85 | `self._maintenance_queue = queue.Queue(maxsize=50000)` | 无 | 内存不足 | 无 | N/A |
| 17 | 86 | `self._maintenance_writer_thread = None` | 无 | 无 | 无 | N/A |
| 18 | 87 | `self._stop_event = threading.Event()` | 无 | 无 | 无 | N/A |
| 19 | 88 | `self._pending_on_stop_data: list = []` | 无 | 无 | 无 | N/A |
| 20 | 89 | `self._pending_data_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 21 | 90 | `self._spill_wal_path = os.path.join(db_dir, '_spill_wal.jsonl')` | 无 | 无 | #7(db_dir) | N/A |
| 22 | 91 | `self._spill_wal_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 23 | 92 | `self._spill_wal_max_entries = 100000` | 无 | 无 | 无 | N/A |
| 24 | 93 | `self._restore_spill_wal()` | **文件I/O**: 读取`_spill_wal.jsonl` | 文件读取异常 | #19, #21, #20 | **有try/except，不传播** |
| 25 | 95-100 | `self._queue_stats = {...}` | 无 | 无 | 无 | N/A |
| 26 | 101 | `self._queue_stats_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 27 | 103 | `self._column_cache: Dict = {}` | 无 | 无 | 无 | N/A |
| 28 | 104 | `self._column_cache_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 29 | 106 | `self._lock = threading.RLock()` | 无 | 无 | 无 | N/A |
| 30 | 107 | `self._ext_kline_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 31 | 108 | `self._agg_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 32 | 109 | `self._db_tick_write_locks = [threading.Lock() for _ in range(6)]` | 无 | 无 | #10 | N/A |
| 33 | 110 | `self._db_kline_write_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 34 | 111 | `self._db_maintenance_write_lock = threading.Lock()` | 无 | 无 | 无 | N/A |
| 35 | 112 | `self._runtime_missing_warned = set()` | 无 | 无 | 无 | N/A |
| 36 | 113 | `self._platform_subscribe = None` | 无 | 无 | 无 | N/A |
| 37 | 114 | `self._platform_unsubscribe = None` | 无 | 无 | 无 | N/A |
| 38 | 116 | `self._last_ext_kline: Dict = {}` | 无 | 无 | 无 | N/A |
| 39 | 117 | `self._ext_kline_load_in_progress = False` | 无 | 无 | 无 | N/A |
| 40 | 118 | `self._aggregators: Dict = {}` | 无 | 无 | 无 | N/A |
| 41 | 120-123 | `self.subscription_manager = SubscriptionManager(self, SubscriptionConfig(...))` | **外部调用**: 创建SubscriptionManager,传入self | SubscriptionManager.__init__可能访问self的未就绪属性 | #8 | **⚠循环依赖: self._params_service未设置** |
| 42 | 125-126 | `from params_service import get_params_service` + `self._params_service = get_params_service()` | **外部调用**: 导入+获取ParamsService单例 | 模块导入失败、单例初始化失败 | 无 | 无保护 |
| 43 | 127-130 | `self._params_service.init_instrument_cache()` (try/except包裹) | **外部调用**: 初始化合约缓存 | 缓存初始化异常 | #42 | **有try/except，仅warning** |
| 44 | 131 | `self._maintenance_service = StorageMaintenanceService(self)` | **外部调用**: 创建维护服务,传入self | StorageMaintenanceService.__init__可能访问self未就绪属性 | #8, #42 | **⚠循环依赖: self._assigned_ids未设置** |
| 45 | 133 | `self._assigned_ids = set()` | 无 | 无 | 无 | N/A |
| 46 | 135 | `self._cleanup_interval = cleanup_interval` | 无 | 无 | 无 | N/A |
| 47 | 136 | `self._cleanup_config = cleanup_config or {}` | 无 | 无 | 无 | N/A |
| 48 | 137 | `self._cleanup_thread = None` | 无 | 无 | 无 | N/A |
| 49 | 138 | `self._closed = False` | 无 | 无 | 无 | N/A |
| 50 | 140 | `self._migrate_legacy_schema()` | **数据库I/O**: 多次DDL/DML操作 | DuckDB查询失败 | #8 | **❌直接raise，无清理** |
| 51 | 141 | `self._create_indexes()` | **数据库I/O**: 4条CREATE INDEX语句 | DuckDB查询失败(raise_on_error=True) | #8 | **❌直接raise，无清理** |
| 52 | 142 | `self._init_kv_store()` | **数据库I/O**: CREATE TABLE + CREATE INDEX | DuckDB查询失败(raise_on_error=True) | #8 | **❌直接raise，无清理** |
| 53 | 143 | `self._maintenance_service.run_startup_checks()` | **数据库I/O**: 多次查询+修复操作 | 整体有try/except(仅warning) | #44, #8 | **有try/except，仅warning** |
| 54 | 144 | `self._load_caches()` | **数据库I/O**: load_caches_from_db | 缓存加载失败 + assigned_ids加载失败(warning) | #42, #8 | **❌部分路径直接raise，无清理** |
| 55 | 145 | `self._restore_aggregator_states()` | **数据库I/O**: KV store读取 | 有try/except包裹 | #40, #52, #8 | **有try/except，仅warning** |
| 56 | 146 | `self._start_async_writer()` | **线程创建**: 6 TickWriter + 1 KlineWriter + 1 MaintenanceWriter + atexit注册 | 线程创建/启动失败; WAL数据恢复到队列 | #12-17, #19, #24, #87 | 无保护 |
| 57 | 147-148 | `if self._cleanup_interval: self._start_cleanup_thread()` | **线程创建**: daemon cleanup线程 | 线程创建/启动失败 | #18, #46, #47 | 无保护 |

---

## 二、汇总统计

| 指标 | 值 |
|------|-----|
| 总步骤数 | **57** |
| 含副作用步骤数 | **17** (#1,#7,#8,#11,#24,#41,#42,#43,#44,#50,#51,#52,#53,#54,#55,#56,#57) |
| 含失败模式步骤数 | **14** (#7,#8,#11,#24,#41,#42,#44,#50,#51,#52,#54,#55,#56,#57) |
| 异常安全步骤 | **4** (#24,#43,#53,#55) — 有try/except，异常不传播 |
| 异常不安全步骤(直接raise) | **4** (#50,#51,#52,#54) — 失败时已创建资源不清理 |
| 循环依赖风险步骤 | **2** (#41,#44) — 传入self时依赖属性未设置 |

---

## 三、错误恢复能力分析

**当前状态: NONE (无)**

### 具体失败场景分析

| 场景 | 失败步骤 | 已创建资源 | 后果 |
|------|---------|-----------|------|
| DuckDB schema迁移失败 | #50 | 步骤1-49全部完成(含SubscriptionManager、StorageMaintenanceService、_data_service连接) | raise传播，调用方捕获异常但8个writer线程尚未启动(步骤56未到)，SubscriptionManager持有半初始化self |
| CREATE INDEX失败 | #51 | 同上 + 步骤50已完成 | 同上 |
| KV Store初始化失败 | #52 | 同上 + 步骤50-51已完成 | 同上 |
| _load_caches失败 | #54 | 同上 + 步骤50-53已完成 | 同上 |
| _start_async_writer失败 | #56 | 同上 + 步骤50-55已完成 | **8个线程可能部分启动**，atexit已注册，无法干净退出 |
| _start_cleanup_thread失败 | #57 | 同上 + 步骤56已完成 | **8个writer线程已在运行**，cleanup线程未启动(可降级运行) |

### 资源泄漏清单

| 资源 | 创建步骤 | 构造失败时清理 | 泄漏风险 |
|------|---------|--------------|---------|
| DuckDB连接 | #8 | ❌ 不关闭 | 文件锁残留 |
| SubscriptionManager(self) | #41 | ❌ 不析构 | 持有半初始化self引用 |
| StorageMaintenanceService(self) | #44 | ❌ 不析构 | 持有半初始化self引用 |
| Writer线程(8个) | #56 | ❌ 不停止 | 非daemon线程阻止进程退出 |
| atexit注册 | #56 | ❌ 不注销 | 可能重复执行关闭逻辑 |
| _stop_event | #18 | ❌ 未设置 | 线程无法感知应停止 |

---

## 四、循环依赖风险分析

### 高风险1: 步骤41 - SubscriptionManager(self)

```
步骤41: self.subscription_manager = SubscriptionManager(self, SubscriptionConfig(...))
步骤42: self._params_service = get_params_service()  ← 还未执行!
```

如果 `SubscriptionManager.__init__` 内部访问 `self._params_service`:
- 触发 `AttributeError: 'InstrumentDataManager' object has no attribute '_params_service'`
- 构造函数异常退出，步骤42-57全部跳过

### 高风险2: 步骤44 - StorageMaintenanceService(self)

```
步骤44: self._maintenance_service = StorageMaintenanceService(self)
步骤45: self._assigned_ids = set()  ← 还未执行!
```

如果 `StorageMaintenanceService.__init__` 内部访问 `self._assigned_ids`:
- 触发 `AttributeError: 'InstrumentDataManager' object has no attribute '_assigned_ids'`

---

## 五、阻塞时间分析

| 阻塞点 | 步骤 | 预估耗时 | 原因 |
|--------|------|---------|------|
| os.makedirs | #7 | 毫秒级 | 仅首次目录不存在时 |
| get_data_service | #8 | 百毫秒~秒级 | DuckDB连接建立 |
| _restore_spill_wal | #24 | 毫秒~秒级 | 读取JSONL(最多10万条) |
| _migrate_legacy_schema | #50 | **秒级~十秒级** | DDL+DML，大表迁移慢 |
| _create_indexes | #51 | 秒级 | CREATE INDEX，大表索引构建 |
| _init_kv_store | #52 | 百毫秒级 | CREATE TABLE |
| run_startup_checks | #53 | 秒级~十秒级 | orphan修复、metadata回填 |
| _load_caches | #54 | 百毫秒~秒级 | 全表扫描加载缓存 |
| _restore_aggregator_states | #55 | 百毫秒级 | KV store读取 |
| _start_async_writer | #56 | 百毫秒级 | WAL replay + 8线程start |
| _start_cleanup_thread | #57 | 毫秒级 | 1线程start |

**总体预估**: 空库1-3秒；有数据的库3-15秒；需schema迁移可能>15秒

---

## 六、修复建议

### 最小修复(低风险，优先)

1. **异常安全**: 在步骤50/51/52/54外包裹try/except，失败时执行`_emergency_cleanup()`
2. **循环依赖**: 调整步骤顺序，将#42(_params_service)移至#41(SubscriptionManager)之前，将#45(_assigned_ids)移至#44(StorageMaintenanceService)之前
3. **紧急清理方法**: `_emergency_cleanup()`停止已启动线程+关闭DuckDB连接+设置_stop_event

### 推荐修复(中等风险)

将__init__拆为3阶段:
- Phase1: 纯内存初始化(步骤1-40)
- Phase2: DB连接+schema(步骤41-55)
- Phase3: 缓存+线程启动(步骤56-57)

各阶段失败可独立回滚到上一阶段结束状态。

### 完整重构(高风险)

57步全部分阶段化+依赖图+状态机，工作量大，适合大版本重构。
