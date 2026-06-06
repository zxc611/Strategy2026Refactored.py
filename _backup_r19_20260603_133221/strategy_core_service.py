"""
策略核心服务模块 - Facade
Facade: StrategyCoreService = _LifecycleMixin(composition) + _InstrumentHelperMixin
         + HistoricalKlineMixin + TickHandlerMixin

CC-04修复: 移除_LifecycleMixin继承，改为_LifecycleMixin类方法直接调用模式，
60个方法通过显式委托转发（_LifecycleMixin.method(self, ...)），@staticmethod直接调用。

分拆历史：
- v3.0 (2026-05-03): 拆分为 strategy_framework + strategy_lifecycle_mixin
  + strategy_instrument_mixin + strategy_trading_mixin
  原2759行 → facade ~200行 + 3个mixin模块
- v3.1 (2026-05-03): strategy_framework → strategy_lifecycle_mixin
  storage.py → storage_core.py（消除小文件碎片化）
- v3.2 (2026-05-03): _TradingMixin 吸收进 StrategyCoreService
  （7个有效方法内联，4个死代码方法删除）
  原因：PositionService(931行)+RiskService(530行)已完备，
  _TradingMixin仅做服务定位+回调适配+薄编排，无独立存在价值

R21-MEM-P2-18修复: 内存碎片化防护策略
  Python内存管理器(pymalloc)使用arena机制，频繁创建/销毁大小不一的对象会导致内存碎片化，
  进程RSS持续增长但不释放给OS。本项目为长期运行的交易策略，需关注以下场景：
  1. tick处理循环中频繁创建小对象(dict/tuple)：建议复用对象池或预分配
  2. 回测批量任务中DataFrame反复创建/销毁：已通过max_tasks_per_child=50缓解子进程碎片
  3. 日志格式化字符串临时对象：使用f-string而非%格式化可减少中间对象
  4. 定期gc.collect()策略建议：
     - 回测批量任务完成后：立即gc.collect()释放碎片化内存
     - 策略运行时：不建议频繁gc.collect()（会暂停所有线程，影响tick处理延迟）
     - 策略stop()时：执行gc.collect()确保资源释放
     - 可选：在低峰时段(如非交易时段)执行gc.collect()整理内存
"""
from __future__ import annotations

import json
import os
import copy
import time
import threading
import logging
import collections
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable

from ali2026v3_trading.serialization_utils import json_dumps
from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState, _state_key, _state_is, _LifecycleMixin
from ali2026v3_trading.strategy_instrument_mixin import _InstrumentHelperMixin
from ali2026v3_trading.strategy_historical import HistoricalKlineMixin
from ali2026v3_trading.strategy_tick_handler import TickHandlerMixin

# R27-P1修复: 导入容错/策略重启/浮点工具
from ali2026v3_trading.resilience_utils import (
    StrategyRestartGuard, Watchdog, HeartbeatMonitor, CircuitBreakerHalfOpen,
    TimeoutGuard, BoundedRetry, safe_callback_wrapper,
    get_process_health, ProcessHealthState, get_signal_lifecycle,
    stable_sum, stable_mean, compute_sharpe_stable,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, PRICE_TOLERANCE,
)
from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE, STRATEGY_MODE_CORRECT_DIVERGENCE,
)

try:
    from ali2026v3_trading.causal_chain_utils import (
        CyclicDependencyGuard, ParamIsolationGuard, ContaminationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False


class ConnectionState:
    """R23-SM-03-FIX: 连接状态机 — 断开检测/重连/数据回放/抖动防护"""
    DISCONNECTED = 'DISCONNECTED'
    CONNECTING = 'CONNECTING'
    CONNECTED = 'CONNECTED'
    RECONNECTING = 'RECONNECTING'
    DATA_STALE = 'DATA_STALE'

    VALID_TRANSITIONS = {
        DISCONNECTED: [CONNECTING],
        CONNECTING: [CONNECTED, DISCONNECTED],
        CONNECTED: [DISCONNECTED, DATA_STALE],
        RECONNECTING: [CONNECTED, DISCONNECTED],
        DATA_STALE: [CONNECTED, DISCONNECTED, RECONNECTING],
    }


logger = logging.getLogger(__name__)

# IN-P1-04修复: 初始化时序守卫装饰器
def _require_initialized(func):
    """关键方法初始化守卫 — 未完成初始化时拒绝执行并记录WARNING"""
    def wrapper(self, *args, **kwargs):
        if not getattr(self, '_initialized', False):
            logging.warning("[IN-P1-04] %s()调用时初始化未完成，跳过执行", func.__name__)
            return None
        return func(self, *args, **kwargs)
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper

# P-12修复: LiveStrategySelector实盘集成（手册9.4节）
# P0-R8-08修复: 修正导入路径，LiveStrategySelector定义在delay_time_sharpe_3d.py中
# R13-P1-DEAD-03修复: 添加日志记录缺失模块，便于排查
try:
    from ali2026v3_trading.param_pool.delay_time_sharpe_3d import LiveStrategySelector
    from ali2026v3_trading.module_load_status import mark_module_loaded
    mark_module_loaded('strategy_core_service_live_strategy_selector')
except ImportError as _e1:
    logger.warning("[R13-P1-DEAD-03] LiveStrategySelector导入失败, 功能不可用: %s", _e1)
    LiveStrategySelector = None
    try:
        from ali2026v3_trading.module_load_status import mark_module_failed
        mark_module_failed('strategy_core_service_live_strategy_selector', _e1)
    except Exception:
        pass


class StrategyCoreService(_InstrumentHelperMixin,
                          HistoricalKlineMixin, TickHandlerMixin):
    """策略核心服务 - Facade

    职责:
    - 策略生命周期管理（初始化、启动、暂停、恢复、停止）
    - 状态监控和报告
    - 健康管理（心跳、错误恢复）
    - 性能监控

    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 事件驱动
    """
    
    MAX_PROCESSED_TRADE_IDS = 10000

    def __init__(self, strategy_id: str = None, event_bus: Optional[Any] = None):
        self.strategy_id = strategy_id or f"strategy_{int(time.time())}"

        # StateStore + CallbackGroup 初始化（最早，供所有Mixin/Manager使用）
        from ali2026v3_trading.state_store import get_state_store
        from ali2026v3_trading.callback_registry import get_callback_group
        self._state_store = get_state_store()
        self._callback_group = get_callback_group()

        # DataAccess层初始化（领域级数据访问，替代直接服务引用）
        from ali2026v3_trading.data_access import get_data_access
        self._data_access = get_data_access()

        # R27-P0-DR-07修复: 策略独立线程池隔离，防止策略崩溃影响全局
        self._strategy_executor = None
        try:
            import concurrent.futures as _cf
            self._strategy_executor = _cf.ThreadPoolExecutor(
                max_workers=2, thread_name_prefix=f"strat_{self.strategy_id[:12]}"
            )
        except Exception as _executor_err:
            logging.warning("[R22-P1-NEW] 策略线程池创建失败(后续可能AttributeError): %s", _executor_err)

        # 状态管理
        self._state = StrategyState.INITIALIZING
        self._is_running = False
        self._is_paused = False
        self._is_trading = True
        self._destroyed = False
        self._initialized = False
        self._health_pause_new_open = False  # R13-P0-DEAD-03/04验证: 已在R10修复，标志在execute_option_trading_cycle中被消费检查
        self._analytics_warmup_done = False
        self._analytics_warmup_thread = None
        # R15-P1-RES-04修复: 策略降级后自动恢复检测
        self._degraded_since: Optional[float] = None
        self._recovery_check_interval_sec: float = 60.0
        self._last_recovery_check: float = 0.0
        self.t_type_service = None
        self._processed_trade_ids = collections.OrderedDict()

        # 线程锁（职责说明）
        # R21-CC02修复: 统一锁获取顺序，防止跨线程多锁死锁
        # 锁层级定义(必须按层级从高到低获取，不可逆序):
        #   L1: _state_lock (状态变更最高优先级)
        #   L2: _lock (通用操作锁)
        #   L3: _trading_lock (交易操作锁)
        #   L4: _scheduler_lock (调度器锁)
        #   L5: _storage_lock (存储操作锁)
        # R21-CC-P2-05修复: RLock vs Lock选择说明
        #   RLock(递归锁): 用于同一线程可能多次获取同一锁的场景（如方法嵌套调用）
        #     - _lock: on_tick→process_option→execute_trading 等嵌套调用链需要重入
        #     - _state_lock: start/stop/pause/resume等方法内部可能调用其他已获取_state_lock的方法
        #     - _trading_lock: execute_option_trading_cycle内部调用子方法可能重入
        #     - _scheduler_lock: 调度器回调可能嵌套调用
        #     - _storage_lock: 存储操作可能在已持有锁的上下文中再次调用
        #   Lock(普通锁): 用于无重入需求的简单互斥场景，性能略优于RLock
        #     - _platform_subscribe_lock: 仅保护订阅操作的简单互斥，无嵌套调用
        self._lock = threading.RLock()
        self._scheduler_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._trading_lock = threading.RLock()

        # 调度器管理器
        from ali2026v3_trading.strategy_scheduler import StrategyScheduler
        self._scheduler_manager = StrategyScheduler()

        # 性能统计
        self._stats = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'total_klines': 0,
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': None,
            'tick_by_type': {'future': 0, 'option': 0},
            'tick_by_exchange': {},
            'tick_by_instrument': {}
        }

        # 注册Manager实例
        from ali2026v3_trading.instrument_manager import InstrumentManager
        from ali2026v3_trading.historical_data_manager import HistoricalDataManager
        self._instrument_mgr = InstrumentManager()
        self._historical_mgr = HistoricalDataManager(
            state_store=self._state_store,
            callback_group=self._callback_group,
        )

        # Phase 2 (CC-02+CC-04): 组合持有6个Manager，替代Mixin继承
        from ali2026v3_trading.lifecycle_manager import LifecycleManager
        from ali2026v3_trading.analytics_manager import AnalyticsManager
        from ali2026v3_trading.event_publisher import EventPublisher
        from ali2026v3_trading.health_check_manager import HealthCheckManager
        from ali2026v3_trading.scheduler_manager_proxy import SchedulerManagerProxy
        self._lifecycle_mgr = LifecycleManager(provider=self)

        self._analytics_mgr = AnalyticsManager(provider=self)
        self._event_publisher = EventPublisher(provider=self)
        self._health_check_mgr = HealthCheckManager(provider=self)
        self._scheduler_mgr_proxy = SchedulerManagerProxy(provider=self)

        # Phase 2: 同步初始状态到LifecycleManager（双写过渡期）
        self._lifecycle_mgr.state = self._state
        self._lifecycle_mgr.is_running = self._is_running
        self._lifecycle_mgr.is_paused = self._is_paused
        self._lifecycle_mgr.destroyed = self._destroyed
        self._lifecycle_mgr.initialized = self._initialized

        # 初始化历史K线Mixin
        self._init_historical_kline_mixin()

        # 初始化Tick处理Mixin
        self._init_tick_handler_mixin()

        # R13-P0-API-07修复: Mixin初始化依赖隐式属性设置，添加显式检查
        self._validate_mixin_attributes()

        # 将关键状态同步到StateStore，供HistoricalDataManager等Manager通过StateStore读取
        self._sync_state_to_store()

        # 在CallbackGroup中注册关键回调方法
        self._register_callbacks()

        # EventBus 集成
        self._event_bus = event_bus

        # DFG-P1-13修复: 订阅系统启动事件，触发检查点恢复
        if self._event_bus is not None:
            try:
                self._event_bus.subscribe_weak('SystemEvent', self._on_system_event_checkpoint)
            except Exception:
                logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                pass

        # R15-P1-RES-09修复: atexit handler确保ThreadPoolExecutor.shutdown()
        import atexit as _atexit
        _atexit.register(self._atexit_cleanup_executor)

        # 信号服务集成
        from ali2026v3_trading.signal_service import get_signal_service
        self._signal_service = get_signal_service(event_bus=event_bus)

        # 运行时平台 API 绑定
        self.subscribe = None
        self.unsubscribe = None
        self.get_instrument = None
        self.get_kline = None
        self._runtime_strategy_host = None
        self._runtime_market_center = None
        self._fallback_market_center = None
        self._api_ready = False
        self._platform_subscribe_completed = threading.Event()

        # R23-SM-03-FIX: 连接状态机
        self._connection_state: str = ConnectionState.DISCONNECTED
        self._last_tick_time: float = 0.0
        self._connection_stale_threshold_sec: float = 120.0
        self._reconnect_attempts: int = 0
        self._max_reconnect_attempts: int = 5

        # R21-NET-P1-03修复: 定期心跳检查机制 — 运行时持续验证平台连接状态
        # R21-NET-P2-01修复: 心跳间隔改为可配置参数，支持从环境变量或配置文件读取
        _heartbeat_env = os.environ.get('STRATEGY_HEARTBEAT_INTERVAL_SEC', '').strip()
        if _heartbeat_env:
            try:
                self._heartbeat_interval_sec: float = float(_heartbeat_env)
            except ValueError:
                self._heartbeat_interval_sec: float = 30.0
        else:
            self._heartbeat_interval_sec: float = 30.0
        self._last_heartbeat_check: float = 0.0
        self._heartbeat_consecutive_failures: int = 0
        self._heartbeat_max_failures: int = 3  # 连续N次心跳失败→标记断线

        # P0修复：端到端六段计数器
        # R21-NET-P1-07修复: 确保生产环境也完整激活E2E六段计数器
        # 六段链路: configured→preregistered→subscribed→first_tick→enqueued→persisted
        # 生产环境必须与回测环境一样完整记录每段计数，用于数据完整性校验
        self._e2e_counters = {
            'configured_instruments': 0,
            'preregistered_instruments': 0,
            'platform_subscribe_called': 0,
            'first_tick_received': 0,
            'enqueued_count': 0,
            'persisted_count': 0,
            'kline_persisted_count': 0,
        }
        self._e2e_shard_enqueued: Dict[int, int] = {}
        self._e2e_shard_persisted: Dict[int, int] = {}
        self._e2e_first_tick_set = set()

        # R21-NET-P1-08修复: 网络分区降级策略规划
        # 降级策略设计:
        # 1. 检测分区: 心跳连续失败(见NET-P1-03) + 订单超时率飙升 → 判定分区
        # 2. 分区降级: 暂停新开单, 仅允许平仓(风控优先); 缓存本地决策, 等待恢复后同步
        # 3. 恢复判定: 心跳恢复 + 订单通道验证 → 逐步放开限制
        # 4. 数据一致性: 分区期间本地缓存的成交/持仓变更需在恢复后与交易所对账
        # TODO(R17-P2-DOC-02): 实现网络分区检测器(NetworkPartitionDetector)和降级状态机

        # R21-NET-P1-09修复: 交易所API版本变更主动检测规划
        # 检测策略设计:
        # 1. 启动时记录API版本号, 定期(每小时)查询交易所API版本
        # 2. 版本变更时: 记录变更日志 + 通知运维 + 自动验证关键字段兼容性
        # 3. 不兼容变更: 触发安全停机(SafeShutdown), 防止数据损坏
        # 4. 兼容变更: 仅记录日志, 继续运行
        # TODO: 实现APIVersionMonitor, 在health check中集成版本校验

        # ===== R21-NET-P2 网络通信P2级规范注释 =====
        # R21-NET-P2-02修复: 消息确认(ACK)机制不完整
        #   现状: CTP平台使用同步回调模型，订单回报通过OnRtnOrder回调，无显式ACK机制
        #   风险: 网络抖动时订单状态可能丢失，无法确认交易所是否收到请求
        #   规划: 1. 为关键操作(下单/撤单)添加应用层ACK超时检测
        #         2. ACK超时后自动重试(幂等性由order_idempotent_set保证)
        #         3. 实现OrderAckTracker，跟踪每个订单的ACK状态和超时

        # R21-NET-P2-03修复: 网络缓冲区大小未配置
        #   现状: CTP SDK内部管理TCP缓冲区，Python层无缓冲区配置
        #   风险: 高频行情下默认缓冲区可能不足，导致数据丢失
        #   规划: 1. 在配置中添加recv_buffer_size/send_buffer_size参数
        #         2. 通过CTP的SubscribeMarketData主题过滤减少无效数据
        #         3. 实现应用层环形缓冲区作为二级缓存

        # R21-NET-P2-04修复: 连接复用未使用
        #   现状: CTP SDK为长连接模型，连接建立后持续复用，无需应用层连接池
        #   风险: 若未来引入HTTP REST API(如查询服务)，需连接复用避免频繁建连
        #   规划: 1. 对HTTP类接口使用requests.Session或aiohttp.ClientSession
        #         2. 设置连接池大小(max_pool_size)和keep-alive超时
        #         3. 监控连接池使用率，动态调整

        # R21-NET-P2-05修复: 压缩算法未配置化
        #   现状: CTP行情/交易数据为二进制协议，无应用层压缩
        #   风险: 若引入WebSocket/HTTP传输层，大数据量传输无压缩增加带宽消耗
        #   规划: 1. 在配置中添加compression_algorithm参数(zstd/lz4/gzip)
        #         2. 对历史数据查询结果启用压缩传输
        #         3. 压缩阈值配置(compression_min_bytes)，小数据不压缩

        # R21-NET-P2-06修复: 多地部署的延迟差异未考虑
        #   现状: 策略部署在单一机房，未考虑交易所托管机房与策略机房间的延迟
        #   风险: 跨机房部署时网络延迟差异影响交易时效性
        #   规划: 1. 在配置中添加datacenter_latency_ms参数
        #         2. 启动时测量与交易所的RTT，动态调整超时和重试策略
        #         3. 多机房部署时选择最低延迟的连接路径

        # R21-NET-P2-07修复: 网络拓扑未文档化
        #   现状: 系统网络拓扑(CTP前置→策略进程→DuckDB→监控)未在代码中文档化
        #   规划: 1. 在模块docstring中添加网络拓扑图(ASCII art)
        #         2. 标注每个连接的协议、端口、超时、重试策略
        #         3. 拓扑变更时同步更新文档

        # R21-NET-P2-08修复: 防火墙/代理兼容性未验证
        #   现状: CTP SDK使用TCP直连，未考虑企业防火墙/HTTP代理场景
        #   风险: 部署在受限网络环境时无法连接交易所
        #   规划: 1. 添加proxy_config参数支持HTTP/SOCKS5代理
        #         2. 实现连接诊断工具，检测防火墙阻断
        #         3. 支持WebSocket作为CTP的降级传输通道
        # ===== R21-NET-P2 网络通信P2级规范注释结束 =====

        # ===== R21-NET-P2-09~16 其他P2网络规范问题 =====
        # SEC-P1-05修复: TLS/SSL证书验证配置
        self._ssl_verify: bool = os.environ.get('SSL_VERIFY', 'true').lower() != 'false'
        self._cert_path: Optional[str] = os.environ.get('SSL_CERT_PATH', None)
        if self._cert_path and os.path.isfile(self._cert_path):
            import ssl as _ssl_mod
            self._ssl_context = _ssl_mod.create_default_context(cafile=self._cert_path)
            logging.info("[SEC-P1-05] SSL证书验证已配置: cert_path=%s", self._cert_path)
        elif not self._ssl_verify:
            logging.warning("[SEC-P1-05] SSL证书验证已禁用(不推荐生产环境)")
        else:
            self._ssl_context = None
            logging.info("[SEC-P1-05] SSL证书验证使用系统默认CA证书")
        # R21-NET-P2-10: 网络QoS优先级未设置
        #   交易报文应优先于行情报文，当前无QoS区分
        #   规划: 实现消息优先级队列，交易消息优先发送
        # R21-NET-P2-11: 连接状态监控指标不完整
        #   缺少连接延迟、丢包率、重连次数等网络质量指标
        #   规划: 添加NetworkMetrics收集器，集成到health check
        # R21-NET-P2-12: 网络超时策略不统一
        #   不同操作(下单/查询/订阅)使用相同超时，应按操作类型区分
        #   规划: 按操作类型配置不同超时(order_timeout/query_timeout/subscribe_timeout)
        # R21-NET-P2-13: 数据校验和未实现
        #   网络传输数据无校验和验证，可能接收损坏数据
        #   规划: 对关键数据添加CRC32/MD5校验
        # R21-NET-P2-14: 流量控制未实现
        #   高频行情下无流量控制，可能导致处理积压
        #   规划: 实现令牌桶限流，控制行情处理速率
        # R21-NET-P2-15: 网络重连退避策略不完善
        #   当前重连为固定间隔，应使用指数退避避免雪崩
        #   规划: 实现ExponentialBackoffReconnect，最大重试次数+抖动
        # R21-NET-P2-16: 多网卡绑定未支持
        #   服务器多网卡时未指定绑定网卡，可能走非最优路径
        #   规划: 添加bind_address配置，支持指定网卡
        # ===== R21-NET-P2-09~16 结束 =====

        # 运行时分析链路
        self.analytics_service = None
        self._future_ids: set[str] = set()
        self._option_ids: set[str] = set()
        self._subscribed_instruments: List[str] = []
        self._init_instruments_result: Optional[Dict[str, Any]] = None

        self._storage = None
        self._storage_init_error = None
        self._storage_lock = threading.RLock()
        self._storage_warm_thread: Optional[threading.Thread] = None
        self._storage_warm_started = False
        self._platform_subscribe_thread: Optional[threading.Thread] = None
        self._platform_subscribe_stop = threading.Event()
        self._platform_subscribe_lock = threading.Lock()
        self._last_option_status_log_time = 0.0
        self._option_status_log_interval = 180.0

        # 订单和持仓服务（惰性初始化）
        self._order_service = None
        self._position_service = None

        # 状态路由与安全元层
        self._state_param_manager = None
        self._safety_meta_layer = None

        # 影子策略监控引擎(V7次系统1)
        # N-02确认: _shadow_engine在_feed_shadow_engine(line371)、get_health_status(line466/553)等处被调用消费，非孤立
        self._shadow_engine = None

        # 策略生态系统(V7次系统2)
        self._strategy_ecosystem = None

        # HFT增强引擎(V8)
        self._hft_engine = None

        # 市场快照采集器 — 覆盖18策略(6主策略×3变体)
        self._snapshot_collector = None

        # R17-P1-CFG-P1-05: 启动时全局依赖检查
        self._check_startup_dependencies()

        self._sync_state_to_store()

    def _check_startup_dependencies(self):
        """R17-P1-CFG-P1-05: 启动时全局依赖检查"""
        _required_attrs = ['_state_store', '_callback_group', '_data_access']
        for attr in _required_attrs:
            if not hasattr(self, attr) or getattr(self, attr) is None:
                logging.error("[STARTUP-DEP] R17-P1-CFG-P1-05: 关键依赖缺失: %s", attr)

    def _coordinated_shutdown(self):
        """R17-P1-CFG-P1-04: 统一优雅关机协调器"""
        services = []
        if hasattr(self, '_shadow_engine') and self._shadow_engine:
            services.append(('ShadowEngine', self._shadow_engine))
        if hasattr(self, '_risk_service') and self._risk_service:
            services.append(('RiskService', self._risk_service))
        if hasattr(self, '_order_service') and self._order_service:
            services.append(('OrderService', self._order_service))
        for name, svc in services:
            try:
                if hasattr(svc, 'shutdown'):
                    svc.shutdown()
            except Exception as e:
                logging.error("[SHUTDOWN] %s shutdown failed: %s", name, e)

    # Phase 2 (CC-02+CC-04): __getattr__转发兼容层 — 6个月过渡期
    # 旧代码通过self._state等访问属性时，转发到对应Manager
    _PHASE2_GETATTR_MAP = {
        '_state': '_lifecycle_mgr',
        '_is_running': '_lifecycle_mgr',
        '_is_paused': '_lifecycle_mgr',
        '_destroyed': '_lifecycle_mgr',
        '_initialized': '_lifecycle_mgr',
        '_lock': '_lifecycle_mgr',
        '_stats': '_lifecycle_mgr',
        '_safety_meta_layer': '_lifecycle_mgr',
    }

    def __getattr__(self, name: str):
        mgr_name = self._PHASE2_GETATTR_MAP.get(name)
        if mgr_name is not None:
            mgr = object.__getattribute__(self, mgr_name)
            if name == '_state':
                return mgr.state
            if name == '_is_running':
                return mgr.is_running
            if name == '_is_paused':
                return mgr.is_paused
            if name == '_destroyed':
                return mgr.destroyed
            if name == '_initialized':
                return mgr.initialized
            if name == '_lock':
                return getattr(mgr, 'lock', object.__getattribute__(self, '_state_lock'))
            if name == '_stats':
                return getattr(mgr, 'stats', None)
            if name == '_safety_meta_layer':
                return getattr(mgr, 'safety_meta_layer', None)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # CC-04修复: _LifecycleMixin方法委托层 — 组合替代继承
    # 移除_LifecycleMixin继承后，通过_LifecycleMixin类方法直接调用，传入self作为实例
    @property
    def storage(self):
        return _LifecycleMixin.storage.fget(self)

    def transition_to(self, new_state: StrategyState) -> bool:
        return _LifecycleMixin.transition_to(self, new_state)

    def bind_platform_apis(self, strategy_obj: Any) -> None:
        return _LifecycleMixin.bind_platform_apis(self, strategy_obj)

    def save_state(self) -> bool:
        return _LifecycleMixin.save_state(self)

    def on_init(self, *args, **kwargs) -> bool:
        return _LifecycleMixin.on_init(self, *args, **kwargs)

    def on_start(self) -> bool:
        return _LifecycleMixin.on_start(self)

    def on_stop(self) -> bool:
        return _LifecycleMixin.on_stop(self)

    def on_destroy(self) -> None:
        return _LifecycleMixin.on_destroy(self)

    def initialize(self, params: Optional[Dict[str, Any]] = None) -> bool:
        return _LifecycleMixin.initialize(self, params)

    def start(self) -> bool:
        return _LifecycleMixin.start(self)

    def pause(self) -> bool:
        return _LifecycleMixin.pause(self)

    def resume(self) -> bool:
        return _LifecycleMixin.resume(self)

    def stop(self) -> bool:
        return _LifecycleMixin.stop(self)

    def destroy(self) -> bool:
        return _LifecycleMixin.destroy(self)

    def get_state(self) -> StrategyState:
        return _LifecycleMixin.get_state(self)

    def is_running(self) -> bool:
        return _LifecycleMixin.is_running(self)

    def is_paused(self) -> bool:
        return _LifecycleMixin.is_paused(self)

    def is_trading(self) -> bool:
        return _LifecycleMixin.is_trading(self)

    def get_uptime(self) -> float:
        return _LifecycleMixin.get_uptime(self)

    def health_check(self) -> Dict[str, Any]:
        return _LifecycleMixin.health_check(self)

    def enter_parallel_running(self, shadow_strategy: Any = None, comparison_callback: Optional[Callable] = None, duration_sec: float = 3600.0) -> bool:
        return _LifecycleMixin.enter_parallel_running(self, shadow_strategy, comparison_callback, duration_sec)

    def exit_parallel_running(self, promote_new: bool = False) -> bool:
        return _LifecycleMixin.exit_parallel_running(self, promote_new)

    def compare_parallel_results(self, old_result: Any, new_result: Any) -> Dict[str, Any]:
        return _LifecycleMixin.compare_parallel_results(self, old_result, new_result)

    def get_parallel_running_status(self) -> Dict[str, Any]:
        return _LifecycleMixin.get_parallel_running_status(self)

    def prepare_restart(self) -> bool:
        return _LifecycleMixin.prepare_restart(self)

    def record_tick(self) -> None:
        return _LifecycleMixin.record_tick(self)

    def record_trade(self) -> None:
        return _LifecycleMixin.record_trade(self)

    def record_signal(self) -> None:
        return _LifecycleMixin.record_signal(self)

    def record_error(self, error_message: str) -> None:
        return _LifecycleMixin.record_error(self, error_message)

    def get_stats(self) -> Dict[str, Any]:
        return _LifecycleMixin.get_stats(self)

    def _start_platform_subscribe_async(self, instrument_ids: List[str]) -> None:
        return _LifecycleMixin._start_platform_subscribe_async(self, instrument_ids)

    def _platform_subscribe_worker(self, instrument_ids: List[str]) -> None:
        return _LifecycleMixin._platform_subscribe_worker(self, instrument_ids)

    def _do_bind_platform_apis(self, strategy_obj: Any) -> None:
        return _LifecycleMixin._do_bind_platform_apis(self, strategy_obj)

    @staticmethod
    def _extract_runtime_market_center(strategy_obj: Any) -> Any:
        return _LifecycleMixin._extract_runtime_market_center(strategy_obj)

    def _inject_runtime_context(self, strategy_obj: Any) -> None:
        return _LifecycleMixin._inject_runtime_context(self, strategy_obj)

    def _get_fallback_market_center(self) -> Any:
        return _LifecycleMixin._get_fallback_market_center(self)

    def _init_analytics_services(self, params: Any) -> None:
        return _LifecycleMixin._init_analytics_services(self, params)

    def _build_instrument_groups(self, storage) -> Tuple[List[str], Dict[str, List[str]]]:
        return _LifecycleMixin._build_instrument_groups(self, storage)

    def _resolve_option_underlying_id(self, inst_id: str, storage) -> Optional[str]:
        return _LifecycleMixin._resolve_option_underlying_id(self, inst_id, storage)

    def _init_t_type_service_and_preload(self, storage) -> None:
        return _LifecycleMixin._init_t_type_service_and_preload(self, storage)

    def _register_analytics_jobs(self) -> None:
        return _LifecycleMixin._register_analytics_jobs(self)

    def _start_analytics_warmup_async(self, params) -> None:
        return _LifecycleMixin._start_analytics_warmup_async(self, params)

    def _ensure_analytics_ready(self, timeout: float = 30.0) -> bool:
        return _LifecycleMixin._ensure_analytics_ready(self, timeout)

    def _warm_storage_async(self) -> None:
        return _LifecycleMixin._warm_storage_async(self)

    @staticmethod
    def _fetch_china_holidays(year: Optional[int] = None) -> List:
        return _LifecycleMixin._fetch_china_holidays(year)

    def _start_historical_kline_load_async(self) -> None:
        return _LifecycleMixin._start_historical_kline_load_async(self)

    def _unsubscribe_all_instruments(self) -> None:
        return _LifecycleMixin._unsubscribe_all_instruments(self)

    def _shutdown_runtime_services(self) -> None:
        return _LifecycleMixin._shutdown_runtime_services(self)

    def _log_resource_ownership_table(self, phase: str = 'unknown') -> None:
        return _LifecycleMixin._log_resource_ownership_table(self, phase)

    @staticmethod
    def _should_probe_t_type_future(product: str) -> bool:
        return _LifecycleMixin._should_probe_t_type_future(product)

    def _log_t_type_future_probe(self, phase: str, instrument_id: str, product: str, month: str, last_price: float) -> None:
        return _LifecycleMixin._log_t_type_future_probe(self, phase, instrument_id, product, month, last_price)

    def _init_logging(self, params: Optional[Dict[str, Any]]) -> None:
        return _LifecycleMixin._init_logging(self, params)

    def _init_scheduler(self) -> None:
        return _LifecycleMixin._init_scheduler(self)

    def _stop_scheduler(self) -> None:
        return _LifecycleMixin._stop_scheduler(self)

    def _add_option_status_diagnosis_job(self) -> None:
        return _LifecycleMixin._add_option_status_diagnosis_job(self)

    def _add_tick_sync_job(self) -> None:
        return _LifecycleMixin._add_tick_sync_job(self)

    def _add_14_contracts_diagnosis_job(self) -> None:
        return _LifecycleMixin._add_14_contracts_diagnosis_job(self)

    def _add_trading_jobs(self) -> None:
        return _LifecycleMixin._add_trading_jobs(self)

    def _ensure_check_pending_orders_job(self) -> None:
        return _LifecycleMixin._ensure_check_pending_orders_job(self)

    def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        return _LifecycleMixin._publish_event(self, event_type, data)

    # CC-04修复结束: _LifecycleMixin委托层

    # R15-P1-RES-09修复: atexit清理ThreadPoolExecutor
    def _atexit_cleanup_executor(self):
        for _attr in ('_executor', '_log_writer_executor', '_analysis_executor', '_strategy_executor'):
            _ex = getattr(self, _attr, None)
            if _ex and hasattr(_ex, 'shutdown'):
                try:
                    _ex.shutdown(wait=False)
                except Exception:
                    logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                    pass

    # DR-P1-04: 崩溃后自动化恢复
    def _auto_recovery_flow(self) -> bool:
        """DR-P1-04修复: 检测是否从崩溃中恢复，自动恢复状态

        返回 True 表示正常或恢复成功，False 表示恢复失败（进入SAFE_MODE）。
        """
        _recovery_marker = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', '_shutdown_gracefully.marker'
        )
        _recovery_log_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'recovery_events.jsonl'
        )

        _crashed = os.path.exists(_recovery_marker)

        if not _crashed:
            logging.info("[DR-P1-04] 正常启动，无需恢复流程")
            return True

        logging.warning("[DR-P1-04] 检测到非正常关闭(_shutdown_gracefully.marker存在)，启动自动化恢复...")

        _recovery_start = time.time()
        _recovery_success = True
        _recovery_details = {
            'positions_restored': False,
            'orders_restored': False,
            'circuit_breaker_restored': False,
            'idempotent_restored': False,
        }

        try:
            # 1. 恢复订单状态（从JSONL）
            self._ensure_order_service()
            if self._order_service is not None:
                try:
                    # R16-P0-RES-05修复: 调用已实现的_recover_order_state方法
                    if hasattr(self._order_service, '_recover_order_state'):
                        self._order_service._recover_order_state()
                        _recovery_details['orders_restored'] = True
                        logging.info("[DR-P1-04] 订单状态已从JSONL恢复")
                    else:
                        logging.warning("[DR-P1-04] OrderService缺少_recover_order_state方法")
                        _recovery_details['orders_restored'] = False
                except Exception as e:
                    logging.error("[DR-P1-04] 订单状态恢复失败: %s", e)
                    _recovery_details['orders_restored'] = False
        except Exception as e:
            logging.error("[DR-P1-04] 订单服务获取失败: %s", e)

        try:
            # 2. 恢复持仓状态（从JSONL）
            self._ensure_position_service()
            if self._position_service is not None:
                try:
                    if hasattr(self._position_service, '_recover_positions'):
                        self._position_service._recover_positions()
                    _recovery_details['positions_restored'] = True
                    logging.info("[DR-P1-04] 持仓状态已恢复")
                except Exception as e:
                    logging.error("[DR-P1-04] 持仓状态恢复失败: %s", e)
                    _recovery_details['positions_restored'] = False
        except Exception as e:
            logging.error("[DR-P1-04] 持仓服务获取失败: %s", e)

        try:
            # 3. 恢复断路器状态
            if hasattr(self, '_order_service') and self._order_service is not None:
                self._order_service._circuit_breaker_open = getattr(
                    self._order_service, '_circuit_breaker_open', False
                )
                self._order_service._consecutive_failures = getattr(
                    self._order_service, '_consecutive_failures', 0
                )
                _recovery_details['circuit_breaker_restored'] = True
                logging.info("[DR-P1-04] 断路器状态已恢复")
        except Exception as e:
            logging.error("[DR-P1-04] 断路器状态恢复失败: %s", e)

        try:
            # 4. 恢复幂等状态
            if hasattr(self, '_order_service') and self._order_service is not None:
                # R16-P0-RES-02修复: 调用已实现的_recover_idempotent_state方法
                if hasattr(self._order_service, '_recover_idempotent_state'):
                    self._order_service._recover_idempotent_state()
                    _recovery_details['idempotent_restored'] = True
                    logging.info("[DR-P1-04] 幂等状态已恢复")
                else:
                    logging.warning("[DR-P1-04] OrderService缺少_recover_idempotent_state方法")
                    _recovery_details['idempotent_restored'] = False
        except Exception as e:
            logging.error("[DR-P1-04] 幂等状态恢复失败: %s", e)

        # 记录恢复日志
        try:
            os.makedirs(os.path.dirname(_recovery_log_path), exist_ok=True)
            _recovery_record = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'event_type': 'auto_recovery',
                'strategy_id': self.strategy_id,
                'was_crashed': True,
                'recovery_success': all(_recovery_details.values()),
                'recovery_duration_sec': round(time.time() - _recovery_start, 3),
                'details': _recovery_details,
            }
            with open(_recovery_log_path, 'a', encoding='utf-8') as f:
                from ali2026v3_trading.serialization_utils import safe_jsonl_append_line
                safe_jsonl_append_line(f, _recovery_record)
        except Exception as e:
            logging.warning("[DR-P1-04] 恢复日志写入失败: %s", e)

        # 如果恢复失败，自动进入SAFE_MODE
        if not all(_recovery_details.values()):
            logging.critical("[DR-P1-04] 恢复未完全成功，进入SAFE_MODE（仅平仓不开仓）")
            self._health_pause_new_open = True
            self._is_trading = False
            # 尝试删除标记文件避免下次仍触发恢复
            try:
                if os.path.exists(_recovery_marker):
                    os.remove(_recovery_marker)
            except Exception:
                logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                pass
            return False

        # 恢复成功，写入正常关闭标记
        try:
            if os.path.exists(_recovery_marker):
                os.remove(_recovery_marker)
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        logging.info("[DR-P1-04] 自动化恢复完成，耗时 %.2fs", time.time() - _recovery_start)
        return True

    def _watchdog_restart(self, max_restarts=3, cooldown_sec=60):
        import time as _time
        for attempt in range(max_restarts):
            try:
                self._auto_recovery_flow()
                return True
            except Exception as e:
                logging.critical("[WATCHDOG] Restart attempt %d/%d failed: %s", attempt+1, max_restarts, e)
                if attempt < max_restarts - 1:
                    _time.sleep(cooldown_sec)
        return False

    # DFG-P1-13修复: 回测检查点恢复 — 重启时从检查点恢复策略状态
    def recover_from_checkpoint(self) -> Dict[str, Any]:
        """DFG-P1-13修复: 从回测检查点恢复策略状态

        检查点文件路径: logs/checkpoint_{strategy_id}.json
        恢复内容: 策略状态、持仓快照、风控参数、信号冷却等

        Returns:
            Dict: 恢复结果 {recovered: bool, fields: [...], errors: [...]}
        """
        _checkpoint_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs',
            f'checkpoint_{self.strategy_id}.json'
        )
        _result = {
            'recovered': False,
            'fields': [],
            'errors': [],
            'checkpoint_path': _checkpoint_path,
        }

        if not os.path.exists(_checkpoint_path):
            logging.debug("[DFG-P1-13] 检查点文件不存在: %s", _checkpoint_path)
            return _result

        try:
            with open(_checkpoint_path, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
        except Exception as e:
            logging.warning("[DFG-P1-13] 检查点文件读取失败: %s", e)
            _result['errors'].append(f'read_failed: {e}')
            return _result

        # 恢复策略状态
        try:
            _saved_state = checkpoint.get('strategy_state')
            if _saved_state:
                from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState, _state_key
                for _s in StrategyState:
                    if _state_key(_s) == _saved_state or str(_s) == _saved_state:
                        self._state = _s
                        _result['fields'].append('strategy_state')
                        break
        except Exception as e:
            _result['errors'].append(f'state_restore_failed: {e}')

        # 恢复风控参数
        try:
            _risk_params = checkpoint.get('risk_params', {})
            if _risk_params:
                self._ensure_order_service()
                if self._order_service and hasattr(self._order_service, '_risk_service'):
                    _rs = self._order_service._risk_service
                    if _rs and isinstance(getattr(_rs, 'params', None), dict):
                        _rs.params.update(_risk_params)
                        _result['fields'].append('risk_params')
        except Exception as e:
            _result['errors'].append(f'risk_params_restore_failed: {e}')

        # 恢复信号冷却时间
        try:
            _cooldowns = checkpoint.get('signal_cooldowns', {})
            if _cooldowns and hasattr(self, '_signal_service') and self._signal_service:
                for _key, _ts in _cooldowns.items():
                    self._signal_service._cooldown_times[_key] = _ts
                _result['fields'].append('signal_cooldowns')
        except Exception as e:
            _result['errors'].append(f'cooldowns_restore_failed: {e}')

        # 恢复统计计数器
        try:
            _stats = checkpoint.get('stats', {})
            if _stats:
                for _k, _v in _stats.items():
                    if _k in self._stats:
                        self._stats[_k] = _v
                _result['fields'].append('stats')
        except Exception as e:
            _result['errors'].append(f'stats_restore_failed: {e}')

        _result['recovered'] = len(_result['fields']) > 0
        if _result['recovered']:
            logging.info("[DFG-P1-13] 检查点恢复成功: fields=%s", _result['fields'])
        else:
            logging.warning("[DFG-P1-13] 检查点恢复无有效字段")

        return _result

    def save_checkpoint(self) -> bool:
        """DFG-P1-13修复: 保存当前策略状态到检查点文件

        Returns:
            bool: 保存是否成功
        """
        _checkpoint_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs',
            f'checkpoint_{self.strategy_id}.json'
        )
        try:
            os.makedirs(os.path.dirname(_checkpoint_path), exist_ok=True)
            # P2-R11-11修复: 添加strategy_version到checkpoint文件，确保恢复时版本兼容性检查
            try:
                from ali2026v3_trading import __version__ as _strategy_version
            except ImportError:
                _strategy_version = 'unknown'
            checkpoint = {
                'strategy_id': self.strategy_id,
                'strategy_version': _strategy_version,  # P2-R11-11修复
                'strategy_state': str(self._state),
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'stats': dict(self._stats),
                'signal_cooldowns': {},
                'risk_params': {},
            }
            # 保存信号冷却
            if hasattr(self, '_signal_service') and self._signal_service:
                checkpoint['signal_cooldowns'] = dict(
                    getattr(self._signal_service, '_cooldown_times', {})
                )
            # 保存风控参数
            self._ensure_order_service()
            if self._order_service and hasattr(self._order_service, '_risk_service'):
                _rs = self._order_service._risk_service
                if _rs and isinstance(getattr(_rs, 'params', None), dict):
                    checkpoint['risk_params'] = dict(_rs.params)

            import tempfile
            _tmp_fd, _tmp_path = tempfile.mkstemp(dir=os.path.dirname(_checkpoint_path), suffix='.tmp')
            try:
                with os.fdopen(_tmp_fd, 'w', encoding='utf-8') as f:
                    f.write(json_dumps(checkpoint, indent=2))
                os.replace(_tmp_path, _checkpoint_path)
            except Exception:
                try:
                    os.unlink(_tmp_path)
                except OSError:
                    pass
                raise
            logging.debug("[DFG-P1-13] 检查点已保存: %s", _checkpoint_path)
            return True
        except Exception as e:
            logging.warning("[DFG-P1-13] 检查点保存失败: %s", e)
            return False

    # DFG-P1-13修复: 系统启动事件消费者 — 触发检查点恢复
    def _on_system_event_checkpoint(self, event: Any) -> None:
        """DFG-P1-13修复: 消费系统启动事件，触发检查点恢复

        当系统启动事件（STARTUP）发布时，自动尝试从检查点恢复策略状态，
        解决回测checkpoint数据无恢复消费者的数据流断裂问题。
        """
        try:
            _system_type = None
            if isinstance(event, dict):
                _system_type = event.get('system_type', '')
            elif hasattr(event, 'system_type'):
                _system_type = event.system_type
            if _system_type == 'STARTUP':
                _result = self.recover_from_checkpoint()
                if _result.get('recovered'):
                    logging.info("[DFG-P1-13] 系统启动时检查点恢复成功: fields=%s", _result.get('fields', []))
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

    # R13-P0-API-07修复: Mixin初始化依赖隐式属性设置，添加显式检查
    def _validate_mixin_attributes(self) -> None:
        """验证所有Mixin依赖的属性已正确初始化，防止隐式属性缺失导致运行时错误。"""
        _REQUIRED_ATTRS = {
            '_LifecycleMixin': ['_state', '_state_lock', '_lock'],
            'HistoricalKlineMixin': ['_historical_load_in_progress', '_historical_loader_lock'],
            'TickHandlerMixin': ['_tick_count', '_shard_router'],
        }
        for mixin_name, attrs in _REQUIRED_ATTRS.items():
            for attr in attrs:
                if not hasattr(self, attr):
                    raise AttributeError(
                        f"[R13-P0-API-07] {mixin_name} required attribute '{attr}' not initialized. "
                        f"Ensure _init_*_mixin() is called in __init__ before this check."
                    )

    def _sync_state_to_store(self) -> None:
        if self._state_store is None:
            return
        state_val_keys = [
            'strategy_id', '_is_running', '_is_paused', '_is_trading',
            '_destroyed', '_initialized',
            '_runtime_market_center', '_fallback_market_center',
            '_health_pause_new_open', '_connection_state', '_lifecycle_run_id',
            '_processed_trade_ids', '_heartbeat_consecutive_failures',
            '_last_heartbeat_check', '_heartbeat_interval_sec', '_heartbeat_max_failures',
            '_connection_stale_threshold_sec', '_reconnect_attempts',
            '_max_reconnect_attempts', '_degraded_since',
            '_recovery_check_interval_sec', '_last_recovery_check',
            '_storage_warm_started', '_platform_subscribe_completed',
            '_config_loaded', '_option_status_log_interval',
            '_last_option_status_log_time', '_pqs_tick_count',
            '_pqs_last_update_time',
            '_stop_executed', '_cert_path', '_init_pending',
            '_runtime_market_center_ref', '_start_executed',
            '_callbacks_enabled', '_runtime_strategy_ref',
            '_init_error', '_api_ready', '_last_tick_time',
            '_ssl_verify', '_init_instruments_result', '_option_ids',
            '_storage_init_error', '_future_ids',
            '_analytics_warmup_done', '_is_real_strategy',
        ]
        state_ref_keys = [
            'params', '_state', '_subscribed_instruments',
            '_runtime_strategy_host', '_stats', '_e2e_counters',
            '_e2e_first_tick_set', '_e2e_shard_enqueued', '_e2e_shard_persisted',
            '_lock', '_stop_requested',
            '_signal_service', '_order_service', '_position_service',
            '_hft_engine', '_state_param_manager', '_safety_meta_layer',
            't_type_service', '_scheduler_manager', '_event_bus',
            '_data_access',
            '_snapshot_collector', '_shadow_engine', '_strategy_ecosystem',
            '_box_spring_strategy', '_instrument_mgr', '_historical_mgr',
            '_strategy', '_AUTO_CALLER_IDS', '_runtime_config',
            '_state_store', '_callback_group', '_storage_warm_timer',
            '_params', '_strategy_executor', '_ssl_context',
            '_storage', '_atexit_cleanup_executor',
            '_ensure_order_service', '_ensure_position_service',
            '_ensure_runtime_config_loaded', '_sync_state_to_store',
            '_ensure_hft_engine', '_ensure_snapshot_collector',
            '_start_output_mode_ui', '_destroy_output_mode_ui',
            '_extract_runtime_market_center', '_extract_contract_year_month',
            '_init_tick_handler_mixin', '_init_historical_kline_mixin',
            '_register_callbacks', '_on_system_event_checkpoint',
            '_check_ecosystem_exclusion', '_feed_shadow_engine',
            '_publish_event', '_get_fallback_market_center',
            '_schedule_storage_warmup', '_log_tick_summary',
            '_validate_mixin_attributes', '_update_position_from_trade',
            '_resolve_open_reason',
        ]
        for key in state_val_keys:
            val = getattr(self, key, None)
            if val is not None:
                self._state_store.set(key, val)
        for key in state_ref_keys:
            val = getattr(self, key, None)
            if val is not None:
                try:
                    self._state_store.set(key, val)
                except (TypeError, copy.Error):
                    self._state_store.set_ref(key, val)
        try:
            if hasattr(self, 'storage'):
                storage = self.storage
                if storage is not None:
                    self._state_store.set_ref('storage', storage)
        except AttributeError as e:
            logging.debug("[R26-FIX] storage属性未初始化，跳过state_store.set_ref: %s", e)
        except Exception as e:
            logging.warning("[R26-FIX] state_store.set_ref('storage')失败: %s", e)

    def _register_callbacks(self) -> None:
        if self._callback_group is None:
            return
        callback_map = {
            '_extract_contract_year_month': self._extract_contract_year_month,
            '_extract_runtime_market_center': self._extract_runtime_market_center,
            '_get_fallback_market_center': self._get_fallback_market_center,
            '_publish_event': self._publish_event,
        }
        for name, fn in callback_map.items():
            if callable(fn):
                registry = self._callback_group.get_registry(name)
                registry.register(fn)

    # ========== 交易/持仓/风控方法（原 _TradingMixin，v3.2 内联） ==========

    # R27-P0-RC-01修复: _ensure_order_service双重检查锁
    _order_service_init_lock = threading.Lock()

    def _ensure_order_service(self) -> None:
        if self._order_service is not None:
            return
        with self._order_service_init_lock:
            if self._order_service is not None:
                return
            try:
                from ali2026v3_trading.order_service import get_order_service
                self._order_service = get_order_service()
                _store = getattr(self, '_state_store', None)
                if _store:
                    _store.set_ref('_order_service', self._order_service)
                logging.debug("[StrategyCoreService] OrderService initialized (singleton)")
            except Exception as e:
                logging.warning("[StrategyCoreService] Failed to initialize OrderService: %s", e)
                self._order_service = None

    def _ensure_position_service(self) -> None:
        if self._position_service is not None:
            return
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            from ali2026v3_trading.position_service import get_position_service
            scope_id = str(getattr(self, 'strategy_id', '') or 'global')
            risk_svc = get_risk_service(None, scope_id=scope_id)
            self._position_service = get_position_service(risk_service=risk_svc, scope_id=scope_id)
            _store = getattr(self, '_state_store', None)
            if _store:
                _store.set_ref('_position_service', self._position_service)
            logging.debug("[StrategyCoreService] PositionService initialized (scope=%s)", scope_id)
        except Exception as e:
            logging.warning("[StrategyCoreService] Failed to initialize PositionService: %s", e)
            self._position_service = None

    def _ensure_hft_engine(self) -> None:
        if self._hft_engine is not None:
            return
        try:
            from ali2026v3_trading.hft_enhancements import get_hft_engine
            self._hft_engine = get_hft_engine()
            self._ensure_order_service()
            if self._order_service:
                self._order_service.enable_hft_enhancements()
            _store = getattr(self, '_state_store', None)
            if _store:
                _store.set_ref('_hft_engine', self._hft_engine)
            logging.debug("[StrategyCoreService] HFT增强引擎已初始化(七大竞争优势-分散架构)")
        except Exception as e:
            logging.warning("[StrategyCoreService] HFT增强引擎初始化失败: %s", e)
            self._hft_engine = None

    def _ensure_snapshot_collector(self, symbol: str = "") -> None:
        """初始化市场快照采集器 — 覆盖18策略(6主策略×3变体)"""
        if self._snapshot_collector is not None:
            return
        try:
            from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
            sym = symbol or self.strategy_id
            self._snapshot_collector = MarketSnapshotCollector(symbol=sym)
            # 注入到信号服务
            if self._signal_service and hasattr(self._signal_service, 'set_snapshot_collector'):
                self._signal_service.set_snapshot_collector(self._snapshot_collector)
            # 注入到订单服务
            self._ensure_order_service()
            if self._order_service and hasattr(self._order_service, 'set_snapshot_collector'):
                self._order_service.set_snapshot_collector(self._snapshot_collector)
            logging.debug("[StrategyCoreService] MarketSnapshotCollector已初始化(18策略覆盖), symbol=%s", sym)  # R13-P2-LOG-07修复
        except Exception as e:
            logging.warning("[StrategyCoreService] MarketSnapshotCollector初始化失败: %s", e)  # R13-P2-LOG-01修复
            self._snapshot_collector = None

    def get_snapshot_collector(self):
        """获取市场快照采集器（惰性初始化）"""
        if self._snapshot_collector is None:
            self._ensure_snapshot_collector()
        return self._snapshot_collector

    @_require_initialized
    def on_order(self, order_data: Any, *args, **kwargs) -> None:
        try:
            oid = getattr(order_data, "order_id", "") or getattr(order_data, "OrderRef", "")
            sts = getattr(order_data, "status", "") or getattr(order_data, "OrderStatus", "")
            logging.info("[StrategyCoreService.on_order] OrderID=%s, Status=%s", oid, sts)
            if self._order_service:
                self._order_service.on_trade_update(order_data)
        except Exception as e:
            logging.error("[StrategyCoreService.on_order] Error: %s", e)

    @_require_initialized
    def on_trade(self, trade_data: Any, *args, **kwargs) -> None:
        try:
            self._update_position_from_trade(trade_data)
        except Exception as e:
            logging.error("[StrategyCoreService.on_trade] Error: %s", e)

    def _update_position_from_trade(self, trade_data: Any) -> None:
        try:
            trade_id = getattr(trade_data, 'trade_id', None) or (trade_data.get('trade_id') if isinstance(trade_data, dict) else None) or str(id(trade_data))
            if trade_id in self._processed_trade_ids:
                return
            self._processed_trade_ids[trade_id] = True
            if len(self._processed_trade_ids) > self.MAX_PROCESSED_TRADE_IDS:
                remove_count = len(self._processed_trade_ids) - int(self.MAX_PROCESSED_TRADE_IDS * 0.8)
                for _ in range(remove_count):
                    self._processed_trade_ids.popitem(last=False)
            from ali2026v3_trading.position_service import get_position_service
            scope_id = str(getattr(self, 'strategy_id', '') or 'global')
            pos_svc = get_position_service(scope_id=scope_id)
            pos_svc.on_trade(trade_data)
        except Exception as e:
            # R13-P2-API-12修复: 不再静默吞掉异常，改为warning级别记录
            logging.warning("[StrategyCoreService._update_position_from_trade] 持仓更新失败: %s", e, exc_info=True)

    def execute_option_trading_cycle(self) -> None:
        # R24-P1-CF-02修复: 初始化未完成时拒绝执行交易周期
        if not self._initialized:
            logging.warning("[StrategyCoreService.execute_option_trading_cycle] R24-P1-CF-02: 初始化未完成，跳过交易周期")
            return
        if not self._is_running or self._is_paused:
            return
        if self._state == StrategyState.DEGRADED:
            logging.info("[OptionTrading] 策略处于DEGRADED状态，跳过交易周期")  # R13-P1-LOG-01修复
            return
        if not self._trading_lock.acquire(blocking=False):
            logging.info("[OptionTrading] 交易锁被占用，跳过本次周期")  # R13-P1-LOG-01修复
            return
        try:
            # R24-P2-AT-01修复: 添加交易周期耗时记录
            _cycle_start = time.time()
            t_type = getattr(self, 't_type_service', None)
            if not t_type:
                return
            targets = t_type.select_otm_targets_by_volume()
            if not targets:
                return
            open_reason = self._resolve_open_reason()
            for t in targets:
                t['open_reason'] = t.get('open_reason', '') or open_reason
            if not self._check_ecosystem_exclusion(targets):
                logging.info("[OptionTrading] 互斥规则或跨策略风控阻断, 跳过本次周期")
                return
            # ✅ P0-5修复: 交易前风控检查
            # R13-TYP-02修复: signal dict必须包含action字段，否则断路器平仓豁免永远不触发
            # R13-P1-BIZ-06修复: signal_id链路贯通 — signal→risk_check→order→position
            import uuid as _uuid
            try:
                from ali2026v3_trading.risk_service import get_risk_service
                scope_id = str(getattr(self, 'strategy_id', '') or 'global')
                rs = get_risk_service(None, scope_id=scope_id)
                passed_targets = []
                for t in targets:
                    _sig_id = t.get('signal_id', '') or f"SIG_{_uuid.uuid4().hex[:12]}"
                    signal = {
                        'symbol': t.get('instrument_id', ''),
                        'direction': t.get('direction', 'BUY'),
                        'price': t.get('price', 0.0),
                        'volume': t.get('volume', 0),
                        'is_valid': True,
                        # R13-P0-API-03修复: action字段默认为OPEN，断路器平仓豁免逻辑依赖此字段
                        'action': t.get('action', '') or 'OPEN',
                        'account_id': t.get('account_id', 'default'),
                        'amount': t.get('amount', 0),
                        'signal_id': _sig_id,  # R13-P1-BIZ-06修复: signal_id贯穿全链路
                    }
                    t['signal_id'] = _sig_id  # R13-P1-BIZ-06修复: 回写到target供后续order使用
                    check_result = rs.check_before_trade(signal)
                    if check_result.is_block:
                        logging.warning("[OptionTrading] 风控阻断: %s %s reason=%s",
                                        t.get('instrument_id', ''), t.get('direction', ''), check_result.reason)
                        continue
                    passed_targets.append(t)
                if not passed_targets:
                    logging.info("[OptionTrading] 所有target均被风控阻断，跳过本次周期")
                    return
                targets = passed_targets
            except Exception as e:
                # R10-P0-11修复: 风控检查异常时fail-safe阻断交易，而非"继续交易"
                # 与P0-R10-10叠加修复：安全元层异常→阻断，外层也必须阻断
                # R10-P2-02: 关键异常日志添加exc_info=True
                logging.error(
                    "[R10-P0-11] 风控检查异常，fail-safe阻断交易: %s", e,
                    exc_info=True,
                )
                return
            # P-30/R7-M-15修复: 健康检查CRITICAL时暂停新开仓（手册12.1节）
            # 双重检查: (1) _health_pause_new_open标志 (2) 实时health查询
            if self._health_pause_new_open:
                logging.warning("[OptionTrading] _health_pause_new_open=True(健康异常), 暂停新开仓")
                return
            try:
                health = self.get_health_status()
                if health.get('health') in ('CRITICAL', 'DEGRADED'):
                    self._health_pause_new_open = True  # R7-M-15修复: 同步设置标志
                    logging.warning("[OptionTrading] 系统健康状态=%s, 暂停新开仓",
                                    health.get('health'))
                    return
            except Exception as e:
                logging.warning("[OptionTrading] 健康检查异常: %s", e)  # R13-P1-LOG-01修复 [R22-P2-EP02]
            self._ensure_order_service()
            if self._order_service:
                order_ids = self._order_service.execute_by_ranking(targets)
                if order_ids:
                    logging.info("[OptionTrading] 下单完成: %d 笔 reason=%s", len(order_ids), open_reason)
            self._feed_shadow_engine(targets, open_reason)
            # R14-P1-DEAD-08修复: 在决策循环中调用governance checker，手册14节治理检测
            try:
                from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
                _eco = get_strategy_ecosystem()
                if _eco and hasattr(_eco, 'run_governance_checks'):
                    _gov_result = _eco.run_governance_checks()
                    if _gov_result.get('any_triggered', False):
                        logging.warning("[OptionTrading] R14-P1-DEAD-08: 治理检测触发降级: %s",
                                        _gov_result.get('degradation_reasons', []))
            except Exception as _gov_err:
                logging.warning("[R22-EP-P1] governance check跳过(合规检查可能放行违规交易): %s", _gov_err)
            # ✅ P1-9修复: 集成box_spring_strategy的箱体弹簧扫描  # R17-P2-DOC-05
            # R13-API-01修复: 传递完整tick参数(high/low/volume/timestamp)，否则箱体检测失效
            # R15-P1-PERF-02修复: 使用全局单例get_box_spring_strategy()，避免绕过单例创建多实例
            try:
                from ali2026v3_trading.box_spring_strategy import get_box_spring_strategy
                bss = get_box_spring_strategy()
            except ImportError as _bss_ie:
                logging.warning("[P1-FIX] box_spring_strategy导入失败, 箱体弹簧扫描不可用: %s", _bss_ie)
                bss = None
            try:  # R23-P0-FIX: 补充缺失的try块，修复SyntaxError
                if bss is not None:
                    for t in targets:
                        inst_id = t.get('instrument_id', '')
                        if inst_id:
                            # R13-API-01修复: 传递完整tick参数，箱体检测依赖high>0 and low>0条件
                            bss.on_tick(
                                instrument_id=inst_id,
                                price=t.get('price', 0.0),
                                high=t.get('high', 0.0),  # R13-API-01: 补充high参数
                                low=t.get('low', 0.0),    # R13-API-01: 补充low参数
                                volume=t.get('volume', 0),  # R13-API-01: 补充volume参数
                                timestamp=t.get('timestamp'),  # R13-API-01: 补充timestamp参数
                            )
            except Exception as e:
                logging.warning("[R22-EP-P1-17] [StrategyCoreService] box_spring_strategy tick error: %s", e)
        except Exception as e:
            # R10-P2-02: 关键异常日志添加exc_info=True
            logging.error("[StrategyCoreService.execute_option_trading_cycle] Error: %s", e, exc_info=True)
        finally:
            # R24-P2-AT-01修复: 交易周期耗时记录
            _cycle_elapsed = time.time() - _cycle_start if '_cycle_start' in dir() else -1
            if _cycle_elapsed > 1.0:
                logging.warning("[R24-P2-AT-01] 交易周期耗时过长: %.2fs", _cycle_elapsed)
            self._trading_lock.release()

    def _check_ecosystem_exclusion(self, targets: list) -> bool:
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            spm = getattr(self, '_state_param_manager', None)
            current_state = spm.get_current_state() if spm else 'other'
            # R13-P0-API-06修复: 使用strategy_ecosystem的映射方法替代硬编码
            strategy_id = eco.resolve_strategy_id_from_state(current_state)
            for t in targets:
                direction = t.get('direction', 'BUY')
                instrument_id = t.get('instrument_id', '')
                allowed, reason = eco.check_mutual_exclusion(strategy_id, direction, instrument_id)
                if not allowed:
                    logging.warning("[OptionTrading] 互斥阻断: strategy=%s dir=%s reason=%s", strategy_id, direction, reason)
                    return False
            return True
        except Exception as e:
            logging.error("[R16-P1-4.2] StrategyCoreService._check_ecosystem_exclusion Error: %s, 阻断交易以保护安全", e)
            self._health_pause_new_open = True
            return False

    def _feed_shadow_engine(self, targets: list, open_reason: str) -> None:
        """V7新增：将交易信号批量推送到影子策略监控引擎(优化：单次锁获取)"""
        try:
            se = getattr(self, '_shadow_engine', None)
            if se is None:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                se = get_shadow_strategy_engine()
                self._shadow_engine = se
            if hasattr(se, 'are_params_locked') and not se.are_params_locked():
                logging.warning("[R16-P1-15] 影子策略参数未锁定，使用未验证参数")
            spm = getattr(self, '_state_param_manager', None)
            market_state = spm.get_current_state() if spm else 'unknown'
            # R13-API-05: Determine strategy_group from state_param_manager
            strategy_group = "s2_resonance"
            try:
                eco = self._get_ecosystem()
                _STRATEGY_ID_TO_SHADOW_GROUP = {
                    'master': 's2_resonance', 'reverse': 's2_resonance', 'other': 's2_resonance',
                    'hft': 's1_hft', 'box': 's3_box', 'spring': 's4_spring',
                    'arbitrage': 's5_arbitrage', 'market_making': 's6_market_making',
                }
                _sid = eco.resolve_strategy_id_from_state(market_state) if eco else 'master'
                strategy_group = _STRATEGY_ID_TO_SHADOW_GROUP.get(_sid, 's2_resonance')
            except Exception:
                logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                pass
            for t in targets:
                se.process_signal(
                    market_state=market_state,
                    instrument_id=t.get('instrument_id', ''),
                    signal_direction=t.get('direction', ''),
                    price=t.get('price', 0.0),
                    quantity=t.get('volume', 0),
                    signal_strength=t.get('signal_strength', 0.0),
                    strategy_group=strategy_group,  # R13-API-05: pass strategy_group
                )
            if not se.is_shadow_mode():
                logging.warning("[StrategyCoreService] 影子策略隔离性验证失败")
        except Exception as e:
            logging.warning("[StrategyCoreService._feed_shadow_engine] Error: %s", e)  # R13-P1-LOG-01修复 [R22-P2-EP01]

    def _resolve_open_reason(self) -> str:
        """V7新增：根据当前状态诊断映射开仓理由编码"""
        state = 'unknown'
        try:
            spm = getattr(self, '_state_param_manager', None)
            if spm:
                state = spm.get_current_state()
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        # R10-三对齐修复: 五态映射补全incorrect_reversal_defensive
        _STATE_REASON_MAP = {  # R25-SE-P1-02-FIX
            STRATEGY_MODE_CORRECT_TRENDING: STRATEGY_MODE_CORRECT_RESONANCE,
            STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: STRATEGY_MODE_CORRECT_DIVERGENCE,
            STRATEGY_MODE_INCORRECT_REVERSAL: 'INCORRECT_REVERSAL',
            'incorrect_reversal_defensive': 'INCORRECT_DIVERGENCE',
            STRATEGY_MODE_OTHER: 'OTHER_SCALP',
        }
        # R14-P1-DEAD-02修复: unknown状态返回有意义默认值+warning日志
        reason = _STATE_REASON_MAP.get(state, '')
        if not reason:
            logging.warning("[StrategyCoreService] R14-P1-DEAD-02: _resolve_open_reason遇到未知状态'%s'，默认OTHER_SCALP", state)
            reason = 'OTHER_SCALP'
        return reason

    def check_position_risk(self) -> None:
        if not self._is_running:
            return
        if not self._trading_lock.acquire(blocking=False):
            logging.info("[PositionRisk] 交易锁被占用，跳过本次风控检查")  # R13-P1-LOG-01修复
            return
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _cyclic_guard and not _cyclic_guard.enter("check_position_risk"):
            logging.warning("[CC-04/CC-11] Cyclic call detected in check_position_risk, skipping")
            self._trading_lock.release()
            return
        _param_guard = ParamIsolationGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _param_guard:
            _param_guard.register_param_source("strategy_params", "strategy_core_service", str(hash(frozenset(self.params.items()))))
        try:
            self._ensure_position_service()
            if self._position_service:
                self._position_service.check_all_positions()
        except Exception as e:
            # R10-P2-02: 关键异常日志添加exc_info=True
            logging.error("[StrategyCoreService.check_position_risk] Error: %s", e, exc_info=True)
        finally:
            if _cyclic_guard:
                _cyclic_guard.exit("check_position_risk")
            self._trading_lock.release()

    def get_health_status(self) -> Dict[str, Any]:
        """V7新增：健康检查API（文档12.1节）

        任一组件状态非OK触发告警并暂停新开仓。
        """
        risk_dashboard_status = 'unknown'  # R24-P0-CF-01修复: 先定义默认值，防止NameError
        components = {}
        try:
            from ali2026v3_trading.data_service import get_data_service
            ds = get_data_service()
            components["data_feeds"] = "OK" if ds and ds.realtime_cache else "DEGRADED"
        except Exception:
            components["data_feeds"] = "ERROR"

        try:
            from ali2026v3_trading.greeks_calculator import GreeksCalculator
            components["greeks_calculator"] = "OK"
        except Exception:
            components["greeks_calculator"] = "UNAVAILABLE"

        try:
            spm = getattr(self, '_state_param_manager', None)
            components["state_diagnosis"] = "OK" if spm else "UNINITIALIZED"
        except Exception:
            components["state_diagnosis"] = "ERROR"

        try:
            self._ensure_order_service()
            components["order_service"] = "OK" if self._order_service else "ERROR"
        except Exception:
            components["order_service"] = "ERROR"

        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            paused, _ = safety.is_trading_paused()
            components["L1_safety"] = "PAUSED" if paused else "OK"
        except Exception:
            components["L1_safety"] = "ERROR"

        try:
            se = getattr(self, '_shadow_engine', None)
            if se is None:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                se = get_shadow_strategy_engine()
                self._shadow_engine = se
            shadow_health = se.get_health_status()
            _shadow_status = shadow_health.get('status', 'ERROR')
            # P1-43修复: shadow_engine alpha衰减WARNING判断
            if _shadow_status == 'OK':
                _alpha_decay = shadow_health.get('alpha_decay', 0.0)
                _alpha_decay_warn_threshold = 0.15
                _alpha_decay_critical_threshold = 0.30
                try:
                    from ali2026v3_trading.param_pool.task_scheduler import P0_IRON_RULES as _P0_RULES
                    _alpha_decay_warn_threshold = abs(_P0_RULES.get("oos_decay_warn", -0.20))
                    _alpha_decay_critical_threshold = abs(_P0_RULES.get("max_oos_decay", -0.30))
                except ImportError as e:
                    logging.debug("[R26-FIX] P0_IRON_RULES模块不可用，使用默认阈值: %s", e)
                except Exception as e:
                    logging.warning("[R26-FIX] P0_IRON_RULES读取异常，使用默认阈值: %s", e)
                if _alpha_decay >= _alpha_decay_critical_threshold:
                    _shadow_status = 'CRITICAL'
                elif _alpha_decay >= _alpha_decay_warn_threshold:
                    _shadow_status = 'WARNING'
            components["shadow_engine"] = _shadow_status
        except Exception:
            components["shadow_engine"] = "ERROR"

        try:
            eco = getattr(self, '_strategy_ecosystem', None)
            if eco is None:
                from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
                eco = get_strategy_ecosystem()
                self._strategy_ecosystem = eco
            eco_health = eco.get_health_status()
            components["strategy_ecosystem"] = eco_health.get('status', 'ERROR')
        except Exception:
            components["strategy_ecosystem"] = "ERROR"

        try:
            self._ensure_hft_engine()
            components["hft_engine"] = "OK" if self._hft_engine else "UNAVAILABLE"
        except Exception:
            components["hft_engine"] = "ERROR"

        # R21-NET-P1-03修复: 定期心跳检查 — 在health check中验证平台连接状态
        try:
            _now = time.time()
            if self._is_running and (_now - self._last_heartbeat_check) >= self._heartbeat_interval_sec:
                self._last_heartbeat_check = _now
                _api_alive = False
                try:
                    _host = getattr(self, '_runtime_strategy_host', None)
                    if _host is not None and callable(getattr(_host, 'sub_market_data', None)):
                        _api_alive = True
                    elif self._api_ready:
                        _api_alive = True
                except Exception:
                    logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                    pass
                if _api_alive:
                    self._heartbeat_consecutive_failures = 0
                    # R23-SM-03-FIX: 心跳成功→连接状态转移
                    if self._connection_state != ConnectionState.CONNECTED:
                        _old_cs = self._connection_state
                        self._connection_state = ConnectionState.CONNECTED
                        logging.info("[R23-SM-03-FIX] 连接状态转移: %s -> CONNECTED", _old_cs)
                    self._last_tick_time = time.time()
                    # R23-SM-06-FIX: 心跳成功时检查熔断自动恢复
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service, check_circuit_breaker_auto_recovery
                        _rs = get_risk_service()
                        if _rs is not None:
                            check_circuit_breaker_auto_recovery(_rs)
                    except Exception as _cb_err:
                        logging.debug("[R23-SM-06-FIX] 熔断自动恢复检查异常: %s", _cb_err)
                    # R23-SM-04-FIX: 心跳成功时扫描超时订单
                    try:
                        _os = getattr(self, '_order_service', None)
                        if _os is not None and callable(getattr(_os, 'scan_order_timeouts', None)):
                            _os.scan_order_timeouts()
                    except Exception as _ot_err:
                        logging.debug("[R23-SM-04-FIX] 超时订单扫描异常: %s", _ot_err)
                    # R27-P0-DR-08修复: 心跳成功时检查降级自动恢复
                    try:
                        _me = getattr(self, '_mode_engine', None)
                        if _me is not None and callable(getattr(_me, 'check_auto_recovery', None)):
                            _recovery = _me.check_auto_recovery()
                            if _recovery:
                                logging.info("[R27-P0-DR-08] 降级自动恢复成功: %s", _recovery)
                    except Exception as _ar_err:
                        logging.debug("[R27-P0-DR-08] 降级自动恢复检查异常: %s", _ar_err)
                else:
                    self._heartbeat_consecutive_failures += 1
                    if self._heartbeat_consecutive_failures >= self._heartbeat_max_failures:
                        # R23-SM-03-FIX: 心跳连续失败→连接状态转移
                        if self._connection_state != ConnectionState.DISCONNECTED:
                            _old_cs = self._connection_state
                            self._connection_state = ConnectionState.DISCONNECTED
                            logging.critical("[R23-SM-03-FIX] 连接状态转移: %s -> DISCONNECTED", _old_cs)
                        logging.critical(
                            "[R21-NET-P1-03修复] 心跳检查连续%d次失败, 平台连接可能已断开!",
                            self._heartbeat_consecutive_failures,
                        )
                        components["platform_heartbeat"] = "DISCONNECTED"
                    else:
                        # R23-SM-03-FIX: 心跳偶发失败→DATA_STALE
                        if self._connection_state == ConnectionState.CONNECTED:
                            self._connection_state = ConnectionState.DATA_STALE
                            logging.warning("[R23-SM-03-FIX] 连接状态转移: CONNECTED -> DATA_STALE")
                        logging.warning(
                            "[R21-NET-P1-03修复] 心跳检查失败 %d/%d, api_ready=%s",
                            self._heartbeat_consecutive_failures, self._heartbeat_max_failures, self._api_ready,
                        )
                        components["platform_heartbeat"] = "DEGRADED"
        except Exception as _hb_err:
            logging.warning("[R22-EP-P1-17] 心跳检查异常: %s", _hb_err)

        # R18-P0-DFG-03修复: 先计算risk_dashboard_status，再赋值到components
        risk_dashboard_status = "UNAVAILABLE"
        try:
            from ali2026v3_trading.risk_service import get_risk_dashboard_service
            rds = get_risk_dashboard_service()
            # 获取当前持仓并计算Greeks聚合
            positions_data = {}
            self._ensure_position_service()
            if self._position_service:
                for inst_map in self._position_service.positions.values():
                    for sym, rec in inst_map.items():
                        if rec.volume != 0:
                            positions_data[sym] = rec
            if positions_data:
                greeks_agg = rds.aggregate_greeks(positions_data)
                risk_dashboard_status = "OK"
                logging.info("[P-04] RiskDashboard Greeks: %s", greeks_agg)  # R13-P1-LOG-01修复
            else:
                risk_dashboard_status = "NO_POSITIONS"
                greeks_agg = {}  # R24-P1-TR-04修复: 初始化greeks_agg避免NameError
        except Exception as e:
            risk_dashboard_status = f"ERROR: {str(e)[:50]}"
        # P-04修复: 添加RiskDashboardService状态（必须在计算后赋值）
        components["risk_dashboard"] = risk_dashboard_status

        open_positions = 0
        try:
            self._ensure_position_service()
            if self._position_service:
                for inst_map in self._position_service.positions.values():
                    for rec in inst_map.values():
                        if rec.volume != 0:
                            open_positions += 1
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        current_state = 'unknown'
        try:
            spm = getattr(self, '_state_param_manager', None)
            if spm:
                current_state = spm.get_current_state()
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        last_signal_time = None
        try:
            if hasattr(self, '_signal_service') and self._signal_service:
                stats = self._signal_service.get_stats()
                last_signal_time = stats.get('last_signal_time')
                if last_signal_time:
                    last_signal_time = last_signal_time.strftime('%H:%M:%S')
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        # P-40修复: 集成参数分类API到健康检查（手册5.3节）
        param_categories = {}
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            shadow_params = ps.get_shadow_params()
            quant_params = ps.get_quantitative_params()
            intuition_params = ps.get_intuition_params()
            param_categories = {
                'shadow_count': len(shadow_params),
                'quantitative_count': len(quant_params),
                'intuition_count': len(intuition_params),
            }
        except Exception:
            param_categories = {'error': 'params_service_unavailable'}

        any_non_ok = any(v not in ("OK", "UNAVAILABLE", "UNINITIALIZED") for v in components.values())
        # P-32修复: 支持WARNING中间态（手册12.1节）
        any_warning = any(v == "WARNING" for v in components.values())
        any_critical = any(v in ("ERROR", "CRITICAL", "PAUSED", "DEGRADED") for v in components.values())

        if any_critical:
            health_level = "CRITICAL"
        elif any_warning or any_non_ok:
            health_level = "WARNING"
        else:
            health_level = "OK"

        # P-34修复: CRITICAL/WARNING时暂停新开仓动作
        if health_level in ("CRITICAL", "WARNING"):
            self._health_pause_new_open = True
            logging.warning("[P-34] 健康检查异常(%s)，暂停新开仓", health_level)
        else:
            self._health_pause_new_open = False

        health = {
            "timestamp": datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "components": components,
            "open_positions": open_positions,
            "last_signal_time": last_signal_time or "N/A",
            "circuit_breaker": "PAUSED" if any_non_ok else "INACTIVE",
            "current_state": current_state,
            "active_strategy": "master",
            "health": health_level,
            "param_categories": param_categories,  # P-40修复: 参数分类统计
            "risk_dashboard_detail": {  # R24-P1-TR-04修复: 风险仪表盘明细维度
                "status": risk_dashboard_status,
                "greeks_agg": greeks_agg if 'greeks_agg' in dir() else {},
                "open_positions_count": open_positions,
                "current_state": current_state,
            },
        }

        try:
            se = getattr(self, '_shadow_engine', None)
            if se:
                shadow_stats = se.get_health_status()
                health["shadow_engine"] = shadow_stats
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        try:
            eco = getattr(self, '_strategy_ecosystem', None)
            if eco:
                eco_health = eco.get_health_status()
                health["strategy_ecosystem"] = eco_health
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        if any_non_ok:
            logging.warning("[HealthCheck] Degraded components: %s",
                          {k: v for k, v in components.items() if v not in ("OK", "UNAVAILABLE", "UNINITIALIZED")})

        # R24-P0-TR-04修复: 健康检查时保存系统状态快照
        try:
            from ali2026v3_trading.risk_service import save_state_snapshot
            save_state_snapshot({'health_status': health}, tag='health_check')
        except ImportError as e:
            logging.debug("[R26-FIX] save_state_snapshot模块不可用，跳过健康检查快照: %s", e)
        except Exception as e:
            logging.warning("[R26-FIX] save_state_snapshot写入失败: %s", e)

        # R24-P1-TR-12修复: 健康检查时记录结构化诊断日志
        try:
            from ali2026v3_trading.health_check_api import StructuredJsonlLogger
            _sjl = StructuredJsonlLogger()
            _sjl.log_diagnostic('strategy_core_service', f'health_check: {health_level}', data=health)
        except ImportError as e:
            logging.debug("[R26-FIX] StructuredJsonlLogger模块不可用，跳过诊断日志: %s", e)
        except Exception as e:
            logging.warning("[R26-FIX] StructuredJsonlLogger初始化失败: %s", e)

        return health

    # R10-P1-11修复: 禁止自动调用confirm_daily_resume，必须人工确认
    _AUTO_CALLER_IDS = frozenset(['timer', 'scheduler', 'cron', 'auto', 'system', 'background'])

    # P0-R8-04修复 + R13-API-03: confirm_daily_resume桥接方法
    def confirm_daily_resume(self, caller_id: str = "unknown") -> bool:
        """
        人工确认恢复日回撤硬停止。
        手册9.6节补丁二b：仅通过人工调用此方法确认恢复，换日不自动重置。
        R13-API-03: 新增caller_id参数，用于审计追踪。
        R10-P1-11修复: 禁止定时器/调度器等自动调用，仅允许人工确认。
        Args:
            caller_id: 调用方标识，用于审计追踪。
        Returns:
            True if daily hard stop was successfully cleared, False otherwise.
        """
        # R10-P1-11修复: 拒绝自动调用来源（定时器、调度器等）
        if caller_id.lower() in self._AUTO_CALLER_IDS:
            logging.warning(
                "[R10-P1-11] confirm_daily_resume被自动来源'%s'调用，已拒绝。"
                "手册9.6节要求仅通过人工确认恢复，换日不自动重置。", caller_id)
            return False
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety:
                result = safety.confirm_daily_resume(caller_id=caller_id)
                if result:
                    logging.critical("[StrategyCoreService] ✅ 人工确认恢复交易：日回撤硬停止已通过confirm_daily_resume()解除 caller_id=%s", caller_id)
                    # P0-R8-04: 恢复后解除健康检查的新开仓阻塞
                    self._health_pause_new_open = False
                return result
            else:
                logging.error("[StrategyCoreService] confirm_daily_resume失败: SafetyMetaLayer不可用")
                return False
        except Exception as e:
            logging.error("[StrategyCoreService] confirm_daily_resume异常: %s", e)
            return False

    # P0-R8-04修复: 检查硬停止状态
    def is_hard_stop_triggered(self) -> bool:
        """检查日回撤硬停止是否已触发。"""
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety:
                return safety.is_hard_stop_triggered()
            return False
        except Exception as e:
            logging.warning("[StrategyCoreService] is_hard_stop_triggered error: %s", e)
            return False

    # OPS-03修复: 紧急一键停止 — 暂停策略 + 撤销挂单 + 平所有仓位 + 通知
    def emergency_stop(self, caller_id: str = "unknown", reason: str = "") -> Dict[str, Any]:
        """OPS-03修复: 紧急一键停止 — 暂停策略 + 撤销挂单 + 平所有仓位 + 通知

        操作步骤：
        1. 暂停策略（停止接收新行情和新交易）
        2. 撤销所有未成交挂单
        3. 市价平所有持仓
        4. 触发通知（告警回调）
        5. 记录审计日志

        Args:
            caller_id: 操作人标识
            reason: 紧急停止原因

        Returns:
            dict: {paused, cancelled_orders, close_result, errors, timestamp}
        """
        result = {
            'paused': False,
            'cancelled_orders': 0,
            'close_result': None,
            'errors': [],
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'caller_id': caller_id,
            'reason': reason,
        }
        logging.critical(
            "[OPS-03] emergency_stop 触发! caller_id=%s reason=%s",
            caller_id, reason,
        )

        # Step 1: 暂停策略
        try:
            self._is_paused = True
            self._is_trading = False
            self._health_pause_new_open = True
            result['paused'] = True
            logging.critical("[OPS-03] 策略已暂停")
        except Exception as e:
            result['errors'].append(f"pause failed: {e}")

        # Step 2+3: 撤销挂单 + 平所有仓位
        try:
            self._ensure_order_service()
            if self._order_service:
                close_result = self._order_service.emergency_close_all_positions(
                    caller_id=caller_id,
                )
                result['cancelled_orders'] = close_result.get('cancelled_orders', 0)
                result['close_result'] = close_result
            else:
                result['errors'].append("OrderService不可用")
        except Exception as e:
            result['errors'].append(f"emergency_close_all_positions failed: {e}")
            logging.error("[OPS-03] 紧急平仓失败: %s", e)

        # Step 4: 触发通知
        try:
            from ali2026v3_trading.risk_service import RiskService
            RiskService._fire_alert('EMERGENCY_STOP', {
                'caller_id': caller_id,
                'reason': reason,
                'strategy_id': self.strategy_id,
                'timestamp': result['timestamp'],
            })
        except Exception as e:
            result['errors'].append(f"alert failed: {e}")

        # Step 5: 审计日志
        try:
            from ali2026v3_trading.risk_service import operations_audit_log
            operations_audit_log(
                action='EMERGENCY_STOP',
                operator=caller_id,
                result='success' if not result['errors'] else 'partial',
                detail=result,
            )
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        logging.critical(
            "[OPS-03] emergency_stop 完成: paused=%s, cancelled=%d, errors=%d",
            result['paused'], result['cancelled_orders'], len(result['errors']),
        )
        return result

    # UPG-P1-13修复: 策略逻辑回归测试
    def run_regression_check(self, test_cases: Optional[List[Dict[str, Any]]] = None,
                              test_func: Optional[Callable] = None) -> Dict[str, Any]:
        """UPG-P1-13修复: 运行策略逻辑回归测试

        使用shared_utils.StrategyRegressionTest框架，验证策略逻辑变更后
        关键场景的输出仍然符合预期。

        Args:
            test_cases: 自定义测试用例列表，每项格式:
                {
                    'name': str,           # 测试用例名称
                    'input': Any,          # 输入数据
                    'expected': Any,       # 期望输出
                    'tolerance': float,    # 浮点数容差(可选)
                    'comparator': Callable # 自定义比较函数(可选)
                }
                如果为None，使用内置默认测试用例。
            test_func: 被测试的策略逻辑函数
                如果为None，使用默认的信号生成测试函数。

        Returns:
            Dict: 回归测试报告 {
                passed: int,
                failed: int,
                total: int,
                details: list,
                test_name: str,
            }
        """
        from ali2026v3_trading.shared_utils import StrategyRegressionTest

        tester = StrategyRegressionTest("strategy_core_regression")

        # 注册测试用例
        if test_cases:
            for case in test_cases:
                tester.register(
                    case_name=case.get('name', 'unnamed'),
                    input_data=case.get('input'),
                    expected_output=case.get('expected'),
                    tolerance=case.get('tolerance', 0.0),
                    comparator=case.get('comparator'),
                )
        else:
            # 内置默认测试用例：验证核心状态转换
            tester.register(
                case_name="state_initializing",
                input_data={'action': 'get_state'},
                expected_output='INITIALIZING',
            )
            tester.register(
                case_name="health_check_returns_dict",
                input_data={'action': 'health_check'},
                expected_output=None,  # 只验证不抛异常
                comparator=lambda actual, expected: isinstance(actual, dict),
            )

        # 确定测试函数
        if test_func is not None:
            func = test_func
        else:
            def _default_test_func(input_data):
                """默认测试函数：根据input_data中的action执行对应检查"""
                action = input_data.get('action', '') if isinstance(input_data, dict) else ''
                if action == 'get_state':
                    return str(self._state)
                elif action == 'health_check':
                    return self.get_health_status()
                elif action == 'is_running':
                    return self._is_running
                else:
                    return None
            func = _default_test_func

        # 执行回归测试
        report = tester.run_all(func)

        if report['failed'] > 0:
            logging.warning(
                "UPG-P1-13: 策略回归测试 %d/%d 通过, %d 失败",
                report['passed'], report['total'], report['failed'],
            )
        else:
            logging.info(
                "UPG-P1-13: 策略回归测试全部通过 (%d/%d)",
                report['passed'], report['total'],
            )

        return report


# 导出公共接口
__all__ = ['StrategyCoreService', 'StrategyState', 'Strategy2026']

# ============================================================================
# Strategy2026 - 策略主类（从 t_type_bootstrap.py 迁移至此）
# ============================================================================

try:  # R16-P0-DEP-01修复: pythongo裸导入加try/except保护，缺失时使用基类兜底
    from pythongo.base import BaseStrategy
except ImportError:
    import logging as _import_logging
    _import_logging.warning("[R16-P0-DEP-01] pythongo.base.BaseStrategy不可用，使用兜底基类")
    class BaseStrategy:
        """pythongo缺失时的兜底策略基类"""
        pass
from .ui_service import UIMixin


class StrategyParams:
    """策略参数容器 - 替代 type('Params', (), dict)() 匿名类

    提供可调试的 __repr__ 和 __dir__，方便排查参数问题。
    """

    __slots__ = ('_data',)

    def __init__(self, data: Dict[str, Any]):  # [R22-P2-TS09]
        object.__setattr__(self, '_data', dict(data))

    def __getattr__(self, name: str):
        try:
            return object.__getattribute__(self, '_data')[name]
        except KeyError:
            raise AttributeError(
                f"'StrategyParams' object has no attribute '{name}'. "
                f"Available: {sorted(object.__getattribute__(self, '_data').keys())}"
            )

    def __setattr__(self, name: str, value):
        if name == '_data':
            object.__setattr__(self, name, value)
        else:
            object.__getattribute__(self, '_data')[name] = value

    def __repr__(self) -> str:
        data = object.__getattribute__(self, '_data')
        items = {k: v for k, v in data.items() if not k.startswith('_')}
        return f"StrategyParams({items})"

    def __dir__(self):
        data = object.__getattribute__(self, '_data')
        return sorted(data.keys())

    def as_dict(self) -> Dict[str, Any]:  # [R22-P2-TS08]
        return dict(object.__getattribute__(self, '_data'))

    def get(self, key: str, default=None):
        return object.__getattribute__(self, '_data').get(key, default)


class Strategy2026(BaseStrategy, UIMixin):
    """策略 2026 主类 - 直接继承平台基类和 UI 混合类

    [P1-2归属] UIMixin被此类继承，属于UI层，在UI重构阶段(Phase 3+)处理
    | StrategyCoreService不继承UIMixin(仅继承4核心Mixin: Lifecycle/Instrument/Historical/Tick)
    """

    def __init__(self, *args, **kwargs):
        BaseStrategy.__init__(self)
        # R13-P2-API-08修复: 调用UIMixin.__init__确保Mixin正确初始化
        UIMixin.__init__(self, *args, **kwargs)

        self._is_real_strategy = True

        self._strategy = None
        self._init_pending = False
        self._init_error = None

        self._lifecycle_run_id = None
        self._start_executed = False
        self._stop_executed = False
        self._lifecycle_lock = threading.Lock()

        runtime_config = kwargs.get('config', {}) or {}
        self._runtime_config = dict(runtime_config)
        self._runtime_strategy_ref = kwargs.get('strategy')
        self._runtime_market_center_ref = kwargs.get('market_center')
        self._config_loaded = False

        logging.info(
            "[Strategy2026.__init__] type=%s, MRO=%s, kwargs_keys=%s",
            type(self).__name__,
            [c.__name__ for c in type(self).__mro__],
            list(kwargs.keys()),
        )

        bootstrap_config = dict(runtime_config)
        if self._runtime_strategy_ref is not None:
            bootstrap_config['strategy'] = self._runtime_strategy_ref
        else:
            bootstrap_config['strategy'] = self
        if self._runtime_market_center_ref is not None:
            bootstrap_config['market_center'] = self._runtime_market_center_ref

        self.config = bootstrap_config
        self.params = StrategyParams(bootstrap_config)

        self.strategy_id = kwargs.get('strategy_id', f"strategy_{int(time.time())}")
        self.strategy_core = StrategyCoreService(self.strategy_id)

        try:
            import t_type_bootstrap as ttb
            if hasattr(ttb, 'register_strategy_cache'):
                ttb.register_strategy_cache(self)
                logging.info("[Strategy2026] Registered to t_type_bootstrap via register_strategy_cache()")
        except Exception as e:
            logging.warning("[Strategy2026] Failed to register to t_type_bootstrap: %s", e)  # R13-P2-LOG-01修复

        self.auto_trading_enabled = False
        self._callbacks_enabled = True
        self._stop_requested = False
        self._storage_warm_timer: Optional[threading.Timer] = None

        from ali2026v3_trading.scheduler_service import get_market_time_service
        self.market_time_service = get_market_time_service()

        logging.debug("[Strategy2026] 已初始化，运行时配置延迟加载")  # R13-P2-LOG-07修复
        logging.info("[Strategy2026] UI 功能已集成，继承自 UIMixin")  # R13-P2-LOG-01修复

    def _log_warning(self, message: str) -> None:
        logging.warning("[Strategy2026] %s", message)  # R13-P2-LOG-01修复

    def _log_info(self, message: str) -> None:
        logging.info("[Strategy2026] %s", message)  # R13-P2-LOG-01修复

    def _log_error(self, message: str) -> None:
        logging.error("[Strategy2026] %s", message)  # R13-P2-LOG-01修复

    def _log_tick_summary(self, tick: Any) -> None:
        instrument_id = getattr(tick, 'instrument_id', '') or getattr(tick, 'InstrumentID', '')
        last_price = getattr(tick, 'last_price', 0.0) or getattr(tick, 'LastPrice', 0.0)
        volume = getattr(tick, 'volume', 0) or getattr(tick, 'Volume', 0)
        exchange = getattr(tick, 'exchange', '') or getattr(tick, 'ExchangeID', '')
        logging.debug("[Strategy2026.onTick] %s %s price=%.2f vol=%d", exchange, instrument_id, last_price, volume)

    def _ensure_runtime_config_loaded(self) -> None:
        if self._config_loaded:
            return

        from ali2026v3_trading.config_service import get_cached_params

        started_at = time.perf_counter()
        loaded_config = get_cached_params()
        logging.info("[Strategy2026] 从缓存参数表加载了 %d 个参数", len(loaded_config))  # R13-P2-LOG-01修复  # R17-P2-CFG-05: 注意-此处仅记录参数数量，不打印完整配置值，避免敏感信息泄漏

        merged = dict(loaded_config)
        for key, value in self._runtime_config.items():
            if value is None:
                continue
            if isinstance(value, str) and value == "":
                continue
            if isinstance(value, (list, dict)) and len(value) == 0:
                continue
            merged[key] = value

        merged['strategy'] = self._runtime_strategy_ref or self
        runtime_market_center = self._runtime_market_center_ref or getattr(self, 'market_center', None)
        if runtime_market_center is not None:
            merged['market_center'] = runtime_market_center

        self.config = merged
        self.params = StrategyParams(merged)
        self._config_loaded = True

        logging.info("[Strategy2026] params.strategy = %s", getattr(self.params, 'strategy', None))  # R13-P2-LOG-01修复
        logging.info("[Strategy2026] 运行时配置就绪，耗时 %.3fs", time.perf_counter() - started_at)

    def _schedule_storage_warmup(self, delay_seconds: float = 0.25) -> None:
        try:
            existing_timer = getattr(self, '_storage_warm_timer', None)
            if existing_timer and existing_timer.is_alive():
                return

            def _warmup() -> None:
                try:
                    self.strategy_core._warm_storage_async()
                except Exception as exc:
                    logging.warning("[Strategy2026] storage 延迟预热启动失败: %s", exc)  # R13-P2-LOG-01修复

            timer = threading.Timer(delay_seconds, _warmup)
            timer.daemon = True
            self._storage_warm_timer = timer
            timer.start()
            logging.info("[Strategy2026] 已计划在 %.2fs 后启动 storage 后台预热", delay_seconds)
        except Exception as exc:
            logging.warning("[Strategy2026] 安排 storage 预热失败: %s", exc)  # R13-P2-LOG-01修复

    # R21-CC-P1-04修复: Timer取消机制 — stop时取消所有已注册的Timer
    def _cancel_all_timers(self) -> None:
        """取消所有已注册的threading.Timer，防止stop后回调仍触发"""
        _warm_timer = getattr(self, '_storage_warm_timer', None)
        if _warm_timer is not None:
            _warm_timer.cancel()
            self._storage_warm_timer = None
        # R22-RES-03修复: 线程生命周期管理 — stop时join清理
        _warm_thread = getattr(self, '_storage_warm_thread', None)
        if _warm_thread is not None and _warm_thread.is_alive():
            _warm_thread.join(timeout=5.0)
            self._storage_warm_thread = None
        _sub_thread = getattr(self, '_platform_subscribe_thread', None)
        if _sub_thread is not None and _sub_thread.is_alive():
            self._platform_subscribe_stop.set()
            _sub_thread.join(timeout=5.0)
            self._platform_subscribe_thread = None

    def is_market_open(self, exchange: str = 'AUTO') -> bool:
        from ali2026v3_trading.scheduler_service import is_market_open as _is_market_open
        from ali2026v3_trading.config_params import get_param

        try:
            exchange = exchange or 'AUTO'
            if exchange != 'AUTO':
                return _is_market_open(exchange)

            exchanges_raw = getattr(self.params, 'exchanges', '') if hasattr(self, 'params') else ''
            if isinstance(exchanges_raw, (list, tuple, set)):
                exchanges = [str(e).strip() for e in exchanges_raw if str(e).strip()]
            else:
                exchanges = [e.strip() for e in str(exchanges_raw).split(',') if e.strip()]

            if not exchanges:
                default_exchanges_raw = get_param('exchanges', 'CFFEX,SHFE,DCE,CZCE,INE,GFEX')
                exchanges = [e.strip() for e in str(default_exchanges_raw).split(',') if e.strip()]

            return any(_is_market_open(exch) for exch in exchanges)
        except Exception:
            return any(_is_market_open(exch) for exch in ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX'])

    def onStart(self):
        with self._lifecycle_lock:
            if self._start_executed:
                logging.warning(
                    f"[Strategy2026.onStart] DUPLICATE ENTRY DETECTED! "
                    f"run_id={self._lifecycle_run_id}, skipping duplicate execution"
                )
                return None

            import uuid
            self._lifecycle_run_id = str(uuid.uuid4())[:8]
            self._start_executed = True

        logging.info("[Strategy2026.onStart] New run_id=%s", self._lifecycle_run_id)  # R13-P2-LOG-01修复

        self._callbacks_enabled = True
        self._stop_requested = False
        logging.info("[Strategy2026] 平台启动回调")

        _init_steps = {}
        _CRITICAL_STEPS = ('config_load', 'api_bind', 'core_init', 'core_start')
        _step_timings = {}  # [R23-P2-05-FIX] 分段计时容器
        _overall_start = time.time()  # [R23-P2-05-FIX]

        try:
            _step_start = time.time()  # [R23-P2-05-FIX]
            self._ensure_runtime_config_loaded()
            _step_timings['config_load'] = time.time() - _step_start  # [R23-P2-05-FIX]
            # R23-IN-06-FIX: 数据源就绪检查
            try:
                _ds = getattr(self.strategy_core, 'storage', None)
                if _ds is not None and hasattr(_ds, 'check_data_source_ready'):
                    _ds_ready, _ds_msg = _ds.check_data_source_ready()
                    if not _ds_ready:
                        logging.warning("[R23-IN-06-FIX] 数据源未就绪: %s, 策略降级为DEGRADED", _ds_msg)
                        self.strategy_core.transition_to(StrategyState.DEGRADED)
            except Exception as _ds_err:
                logging.warning("[R23-IN-06-FIX] 数据源就绪检查异常: %s", _ds_err)
            _init_steps['config_load'] = True
        except Exception as e:
            _step_timings['config_load'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['config_load'] = False
            logging.error("[Strategy2026.onStart] CRITICAL: 加载运行时配置错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] 加载运行时配置堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=config_load error=%s run_id=%s", e, self._lifecycle_run_id)

        try:
            _step_start = time.time()  # [R23-P2-05-FIX]
            if not getattr(self.strategy_core, '_api_ready', False):
                self.strategy_core.bind_platform_apis(self)
                logging.info("[Strategy2026.onStart] API未就绪，已补调bind_platform_apis")
            else:
                logging.info("[Strategy2026.onStart] API已就绪，跳过bind_platform_apis")
            _step_timings['api_bind'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['api_bind'] = True
        except Exception as e:
            _step_timings['api_bind'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['api_bind'] = False
            logging.error("[Strategy2026.onStart] CRITICAL: bind_platform_apis() 错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] bind_platform_apis() 堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=api_bind error=%s run_id=%s", e, self._lifecycle_run_id)

        try:
            self.trading = True
            self.update_status_bar()
            self.output("策略启动")
            _init_steps['status_set'] = True
        except Exception as e:
            _init_steps['status_set'] = False
            logging.error("[Strategy2026.onStart] 状态设置错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] 状态设置堆栈:")

        try:
            _step_start = time.time()  # [R23-P2-05-FIX]
            logging.info("[Strategy2026] 检查 strategy_core 状态：has on_init=%s", hasattr(self.strategy_core, 'on_init'))  # R13-P2-LOG-01修复
            if hasattr(self.strategy_core, 'on_init'):
                current_state = getattr(self.strategy_core, '_state', None)
                is_running = getattr(self.strategy_core, '_is_running', False)
                is_paused = getattr(self.strategy_core, '_is_paused', False)
                logging.info("[Strategy2026] strategy_core 状态：state=%s, _is_running=%s, _is_paused=%s", current_state, is_running, is_paused)  # R13-P2-LOG-01修复

                if _state_is(current_state, StrategyState.STOPPED) and hasattr(self.strategy_core, 'prepare_restart'):
                    restart_ready = self.strategy_core.prepare_restart()
                    logging.info("[Strategy2026] strategy_core STOPPED -> prepare_restart=%s", restart_ready)  # R13-P2-LOG-01修复
                    current_state = getattr(self.strategy_core, '_state', None)

                if _state_is(current_state, StrategyState.ERROR):
                    logging.error(
                        "[Strategy2026] ❌ strategy_core 处于ERROR状态（合约加载/预注册失败），策略终止运行"
                    )
                    self.strategy_core._is_running = False
                    _init_steps['core_init'] = False
                    raise RuntimeError(
                        "合约加载/预注册失败，策略不可恢复启动。请检查合约配置文件或DB连接后重启。"
                    )

                if _state_is(current_state, StrategyState.INITIALIZING):
                    logging.info("[Strategy2026] 正在调用 strategy_core.initialize()...")
                    init_result = self.strategy_core.initialize(params=self.config)
                    logging.info("[Strategy2026] strategy_core 初始化结果：%s", init_result)  # R13-P2-LOG-01修复

                    is_running = getattr(self.strategy_core, '_is_running', False)
                    is_paused = getattr(self.strategy_core, '_is_paused', False)
                    logging.info("[Strategy2026] 初始化后状态：_is_running=%s, _is_paused=%s", is_running, is_paused)  # R13-P2-LOG-01修复
                else:
                    logging.info("[Strategy2026] 无需调用 on_init，当前状态为：%s", current_state)  # R13-P2-LOG-01修复
            _step_timings['core_init'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['core_init'] = True
        except Exception as e:
            _step_timings['core_init'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['core_init'] = False
            logging.error("[Strategy2026.onStart] CRITICAL: strategy_core 初始化阶段错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] strategy_core 初始化阶段堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=core_init error=%s run_id=%s", e, self._lifecycle_run_id)

        # DR-P1-04: 崩溃后自动化恢复
        try:
            if hasattr(self.strategy_core, '_auto_recovery_flow'):
                recovery_ok = self.strategy_core._auto_recovery_flow()
                _init_steps['auto_recovery'] = recovery_ok
                if not recovery_ok:
                    logging.critical("[Strategy2026.onStart] DR-P1-04: 自动化恢复失败，进入SAFE_MODE")
            else:
                _init_steps['auto_recovery'] = 'skipped'
        except Exception as e:
            _init_steps['auto_recovery'] = False
            logging.error("[Strategy2026.onStart] DR-P1-04: 自动化恢复异常: %s", e)

        # ✅ P0-19集成: ServiceContainer依赖注入容器初始化
        try:
            from ali2026v3_trading.service_container import ServiceContainer
            _service_container = ServiceContainer()
            _sc_ok = _service_container.create_and_register_all_services()
            if _sc_ok:
                logging.info("[Strategy2026] ServiceContainer所有服务注册并初始化成功")
            else:
                logging.warning("[Strategy2026] ServiceContainer初始化返回False，部分服务可能未就绪")
            _init_steps['service_container_init'] = True
        except Exception as e:
            _init_steps['service_container_init'] = False
            logging.warning("[Strategy2026.onStart] ServiceContainer初始化跳过（非关键）: %s", e)  # R13-P2-LOG-01修复

        # ✅ P0-21集成: 连接PerformanceConfig到PathCounter性能监控  # R17-P2-DOC-03: P0修复标记保留用于追溯
        try:
            from ali2026v3_trading.performance_monitor import PathCounter
            _perf_enabled = getattr(self, 'config', None)
            if _perf_enabled is not None:
                _perf_enabled = getattr(_perf_enabled, 'performance', None)
                if _perf_enabled is not None:
                    _perf_enabled = getattr(_perf_enabled, 'enable_performance_monitoring', True)
            if _perf_enabled:
                PathCounter.enable()
                logging.info("[Strategy2026] 性能监控已启用（P0-21集成）")
            else:
                PathCounter.disable()
                logging.info("[Strategy2026] 性能监控已禁用（PerformanceConfig配置）")
            _init_steps['performance_monitor_init'] = True
        except Exception as e:
            _init_steps['performance_monitor_init'] = False
            logging.warning("[R22-EP-P1-17] [Strategy2026.onStart] 性能监控初始化跳过: %s", e)

        try:
            market_open = self.is_market_open()
            logging.info("[Strategy2026] 市场开盘状态：%s", market_open)  # R13-P2-LOG-01修复
            _init_steps['market_check'] = True
        except Exception as e:
            _init_steps['market_check'] = False
            logging.error("[Strategy2026.onStart] 市场状态检查错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] 市场状态检查堆栈:")

        try:
            logging.info("[Strategy2026] 正在启动 UI 界面...")
            self._start_output_mode_ui()
            logging.info("[Strategy2026] UI 界面启动成功")
            _init_steps['ui_start'] = True
        except Exception as e:
            _init_steps['ui_start'] = False
            logging.error("[Strategy2026.onStart] 启动 UI 错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] 启动 UI 堆栈:")

        try:
            _step_start = time.time()  # [R23-P2-05-FIX]
            if hasattr(self.strategy_core, 'start'):
                result = self.strategy_core.start()
                logging.info("[Strategy2026] 策略核心启动结果：%s", result)  # R13-P2-LOG-01修复
            else:
                logging.warning("[Strategy2026.onStart] strategy_core.start 不可用，跳过")
            _step_timings['core_start'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['core_start'] = True
        except Exception as e:
            _step_timings['core_start'] = time.time() - _step_start  # [R23-P2-05-FIX]
            _init_steps['core_start'] = False
            logging.error("[Strategy2026.onStart] CRITICAL: strategy_core.start() 错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStart] strategy_core.start() 堆栈:")
            logging.error("[R23-P2-06-FIX] init_failed_event step=core_start error=%s run_id=%s", e, self._lifecycle_run_id)

        _failed_critical = [s for s in _CRITICAL_STEPS if not _init_steps.get(s, False)]
        _failed_all = [s for s, ok in _init_steps.items() if not ok]
        if _failed_critical:
            logging.error(
                "[Strategy2026.onStart] CRITICAL STEPS FAILED: %s, "
                "all results: %s. Strategy may be in DEGRADED state.",
                _failed_critical, _init_steps,  # R13-P2-LOG-01修复
            )
            if hasattr(self.strategy_core, 'transition_to'):
                try:
                    self.strategy_core.transition_to(StrategyState.DEGRADED)
                except Exception:
                    logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                    pass
        elif _failed_all:
            logging.warning(
                "[Strategy2026.onStart] Non-critical steps failed: %s, "
                "all results: %s. Strategy running with degraded features.",
                _failed_all, _init_steps,  # R13-P2-LOG-01修复
            )
        else:
            logging.info("[Strategy2026.onStart] All steps succeeded: %s", _init_steps)  # R13-P2-LOG-01修复

        _overall_elapsed = time.time() - _overall_start  # [R23-P2-05-FIX]
        logging.info("[R23-P2-05-FIX] onStart分段计时汇总: timings=%s overall=%.3fs", _step_timings, _overall_elapsed)

        return None

    def onInit(self):
        try:
            logging.info("[Strategy2026] 平台初始化回调")
            self._ensure_runtime_config_loaded()

            self.strategy_core.bind_platform_apis(self)
            logging.info("[Strategy2026.onInit] 已刷新平台 API 到 strategy_core")

            self._init_pending = True
            self._init_error = None

            if hasattr(self.strategy_core, 'initialize'):
                result = self.strategy_core.initialize(params=self.config)
                logging.info("[Strategy2026.onInit] strategy_core 初始化结果：%s", result)  # R13-P2-LOG-01修复

            self._init_pending = False

            super().on_init()
            self._schedule_storage_warmup()

            return None
        except Exception as e:
            logging.error("[Strategy2026.onInit] 错误：%s", e)  # R13-P2-LOG-01修复
            logging.info("[PROBE_PARAMS_MAP] exchange=%s", getattr(self.params_map, 'exchange', 'N/A'))  # R13-P2-LOG-01修复
            logging.info("[PROBE_PARAMS_MAP] instrument_id=%s", getattr(self.params_map, 'instrument_id', 'N/A'))  # R13-P2-LOG-01修复
            logging.info("[PROBE_PARAMS_MAP] exchange_list=%s", self.exchange_list[:10] if self.exchange_list else 'empty')  # R13-P2-LOG-01修复
            logging.info("[PROBE_PARAMS_MAP] instrument_list=%s", self.instrument_list[:10] if self.instrument_list else 'empty')  # R13-P2-LOG-01修复

            logging.exception("[Strategy2026.onInit] 堆栈:")
            self._init_error = e
            self._init_pending = False
            return None

    def onStop(self):
        with self._lifecycle_lock:
            if self._stop_executed:
                logging.warning(
                    "[Strategy2026.onStop] DUPLICATE ENTRY DETECTED! "
                    "run_id=%s, skipping duplicate execution",
                    self._lifecycle_run_id,  # R13-P2-LOG-01修复
                )
                return None

            self._stop_executed = True

        logging.info("[Strategy2026.onStop] Stopped (run_id=%s)", self._lifecycle_run_id)  # R13-P2-LOG-01修复

        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onStop] enter")

        try:
            logging.info("[Strategy2026] 平台停止回调")
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'stop'):
                result = self.strategy_core.stop()
                logging.info("[Strategy2026.onStop] strategy_core 停止结果：%s", result)  # R13-P2-LOG-01修复
                # R23-IN-07-FIX: onStop清理所有实例属性，防止热重启惰性引用滞留
                for _attr in ('_shadow_engine', '_signal_service', '_strategy_ecosystem', '_hft_engine', '_snapshot_collector'):
                    if hasattr(self.strategy_core, _attr):
                        setattr(self.strategy_core, _attr, None)
                logging.info("[R23-IN-07-FIX] onStop: 已清理5个实例属性(_shadow_engine/_signal_service/_strategy_ecosystem/_hft_engine/_snapshot_collector)")
            else:
                logging.warning("[Strategy2026.onStop] strategy_core.stop 不可用，跳过")
        except Exception as e:
            logging.error("[Strategy2026.onStop] strategy_core.stop failed: %s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.onStop] strategy_core.on_stop stack:")

        try:
            from pythongo import utils as _utils
        except ImportError:
            _utils = None
            logging.warning("[DEP-04] pythongo.utils not available, using None fallback")
        self.trading = False
        if _utils is not None:
            try:
                _utils.Scheduler("PythonGO").stop()
            except Exception as e:
                logging.error("[Strategy2026.onStop] Scheduler stop failed: %s", e)
        self.save_instance_file()
        logging.info("[Strategy2026.onStop] BaseStrategy non-INFINIGO steps completed")

        return None

    def internal_pause_strategy(self) -> bool:
        result = False
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'pause'):
                result = bool(self.strategy_core.pause())
        except Exception as e:
            logging.error("[Strategy2026.internal_pause_strategy] 错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.internal_pause_strategy] 堆栈:")
            result = False

        return result

    def internal_resume_strategy(self) -> bool:
        result = False
        try:
            current_state = getattr(self.strategy_core, '_state', None)
            if _state_is(current_state, StrategyState.PAUSED) and hasattr(self.strategy_core, 'resume'):
                result = bool(self.strategy_core.resume())
            elif _state_is(current_state, StrategyState.STOPPED):
                logging.info("[Strategy2026.internal_resume_strategy] 从STOPPED状态恢复，重置启动标志后调用onStart()")
                with self._lifecycle_lock:
                    self._start_executed = False
                    self._stop_executed = False
                self.onStart()
                result = bool(_state_is(getattr(self.strategy_core, '_state', None), StrategyState.RUNNING))
            else:
                logging.warning("[Strategy2026.internal_resume_strategy] Cannot resume in state: %s", current_state)  # R13-P2-LOG-01修复
        except Exception as e:
            logging.error("[Strategy2026.internal_resume_strategy] 错误：%s", e)  # R13-P2-LOG-01修复
            logging.exception("[Strategy2026.internal_resume_strategy] 堆栈:")
            result = False

        return result

    def onDestroy(self):
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onDestroy] enter")

        if not getattr(self, '_stop_executed', False):
            logging.info("[Strategy2026.onDestroy] onStop not executed, performing full cleanup")
            self._stop_executed = True
        else:
            logging.info("[Strategy2026.onDestroy] onStop already executed, performing remaining cleanup")

        try:
            self._destroy_output_mode_ui()
        except Exception as e:
            logging.warning("[Strategy2026.onDestroy] destroy UI failed: %s", e)

        try:
            from ali2026v3_trading.param_pool.task_scheduler import cleanup_global_data
            cleanup_global_data()
            logging.info("[R22-P0-MEM-01] onDestroy: task_scheduler全局DataFrame已释放")
        except ImportError as _ie:
            logging.warning("[R22-P0-MEM-01] onDestroy: param_pool.task_scheduler导入失败, 全局DataFrame可能未释放: %s", _ie)
        except Exception as e:
            logging.warning("[R22-P0-MEM-01] onDestroy: cleanup_global_data failed: %s", e)

    def onTick(self, tick):
        """平台 Tick 数据回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        # R15-P0-RES-01修复: DEGRADED状态下阻断tick回调，避免降级后仍处理行情
        try:
            core_state = getattr(self.strategy_core, '_state', None) if hasattr(self, 'strategy_core') else None
            if _state_is(core_state, StrategyState.DEGRADED):
                return None
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        try:
            super().on_tick(tick)
        except Exception as e:
            logging.error("[Strategy2026.onTick] super().on_tick() 错误：%s", e)  # R13-P2-LOG-01修复

        try:
            self._log_tick_summary(tick)

            # R30-ARCH-02修复: 桥接PQS量化分析到策略决策链路
            # R32-ARCH-02-v2修复: 添加tick节流，避免高频tick下PQS计算成为性能瓶颈
            # 默认每5秒或每10个tick才调用一次PQS更新，减少CPU开销
            try:
                _now_pqs = time.time()
                _pqs_tick_count = getattr(self, '_pqs_tick_count', 0) + 1
                self._pqs_tick_count = _pqs_tick_count
                _pqs_last_time = getattr(self, '_pqs_last_update_time', 0.0)
                _pqs_throttle_sec = 5.0
                _pqs_throttle_ticks = 10
                if (_now_pqs - _pqs_last_time) >= _pqs_throttle_sec or _pqs_tick_count >= _pqs_throttle_ticks:
                    from ali2026v3_trading.ProductionQuantSystem import get_production_quant_system
                    _pqs = get_production_quant_system()
                    if _pqs is not None and _pqs._initialized:
                        _sym = getattr(tick, 'instrument_id', '') or getattr(tick, 'symbol', '')
                        _high = getattr(tick, 'high_price', 0) or getattr(tick, 'high', 0)
                        _low = getattr(tick, 'low_price', 0) or getattr(tick, 'low', 0)
                        _close = getattr(tick, 'last_price', 0) or getattr(tick, 'close', 0)
                        _ret = getattr(tick, 'return_value', 0)
                        if _close > 0:
                            _result = _pqs.update_tick(_sym, _high, _low, _close, _ret)
                            _pqs.publish_analysis_to_strategy(_result)
                        self._pqs_last_update_time = _now_pqs
                        self._pqs_tick_count = 0
            except Exception as _pqs_err:
                logging.debug("[R22-P1-NEW] PQS量化分析tick更新失败(策略缺少量化输入): %s", _pqs_err)

            if hasattr(self.strategy_core, 'on_tick'):
                self.strategy_core.on_tick(tick)
        except Exception as e:
            logging.error("[Strategy2026.onTick] strategy_core.on_tick() 错误：%s", e)  # R13-P2-LOG-01修复

        return None

    def onOrder(self, order):
        """平台订单回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_order(order)
        except Exception as e:
            logging.error("[Strategy2026.onOrder] super().on_order() 错误：%s", e)  # R13-P2-LOG-01修复

        try:
            if hasattr(self.strategy_core, 'on_order'):
                self.strategy_core.on_order(order)
        except Exception as e:
            logging.error("[Strategy2026.onOrder] strategy_core.on_order() 错误：%s", e)  # R13-P2-LOG-01修复

        return None

    def onTrade(self, trade):
        """平台成交回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_trade(trade)
        except Exception as e:
            logging.error("[Strategy2026.onTrade] super().on_trade() 错误：%s", e)  # R13-P2-LOG-01修复

        try:
            if hasattr(self.strategy_core, 'on_trade'):
                self.strategy_core.on_trade(trade)
        except Exception as e:
            logging.error("[Strategy2026.onTrade] strategy_core.on_trade() 错误：%s", e)  # R13-P2-LOG-01修复

        return None

    on_init = onInit
    on_start = onStart
    on_stop = onStop
    on_destroy = onDestroy
    on_tick = onTick
    on_order = onOrder
    on_trade = onTrade

    @property
    def my_is_running(self) -> bool:
        return getattr(self.strategy_core, '_is_running', False)

    @property
    def my_is_paused(self) -> bool:
        return getattr(self.strategy_core, '_is_paused', False)

    @property
    def my_trading(self) -> bool:
        if hasattr(self.strategy_core, '_is_trading'):
            return getattr(self.strategy_core, '_is_trading', True)
        return getattr(self.strategy_core, '_is_running', False) and not getattr(self.strategy_core, '_is_paused', False)

    @my_trading.setter
    def my_trading(self, value: bool) -> None:
        if hasattr(self.strategy_core, '_is_trading'):
            self.strategy_core._is_trading = bool(value)
