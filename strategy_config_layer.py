"""
StrategyCoreService 配置层 — 从strategy_core_service.py拆分
职责: 配置初始化、参数解析、依赖检查、优雅关机协调
"""
from __future__ import annotations

import collections
import logging
import os
import threading
from typing import Any, Dict, Optional

from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_DIVERGENCE,
)
from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState


class StrategyConfigLayer:
    def __init__(self, provider):
        self._provider = provider

    def init_state(self) -> None:
        """状态初始化 — 从StrategyCoreService._init_state迁移"""
        provider = self._provider
        # 状态管理
        provider._state = StrategyState.INITIALIZING
        provider._is_running = False
        provider._is_paused = False
        provider._is_trading = True
        provider._destroyed = False
        provider._initialized = False
        provider._health_pause_new_open = False  # R13-P0-DEAD-03/04验证: 已在R10修复，标志在execute_option_trading_cycle中被消费检查
        provider._analytics_warmup_done = False
        provider._analytics_warmup_thread = None
        # R15-P1-RES-04修复: 策略降级后自动恢复检测
        provider._degraded_since = None
        provider._recovery_check_interval_sec = 60.0
        provider._last_recovery_check = 0.0
        provider.t_type_service = None
        provider._processed_trade_ids = collections.OrderedDict()

    def init_locks(self) -> None:
        """锁与Manager初始化 — 从StrategyCoreService._init_locks迁移"""
        provider = self._provider
        # 线程锁（职责说明）
        # R21-CC02修复: 统一锁获取顺序，防止跨线程多锁死锁
        # 锁层级定义(必须按层级从高到低获取，不可逆序):
        #   L1: _state_lock (状态变更最高优先级)
        #   L2: _lock (通用操作锁)
        #   L3: _trading_lock (交易操作锁)
        #   L4: _scheduler_lock (调度器锁)
        #   L5: _storage_lock (存储操作锁)
        provider._lock = threading.RLock()
        provider._scheduler_lock = threading.RLock()
        provider._state_lock = threading.RLock()
        provider._trading_lock = threading.RLock()

        # 调度器管理器
        from ali2026v3_trading.strategy_scheduler import StrategyScheduler
        provider._scheduler_manager = StrategyScheduler()

        # 性能统计
        provider._stats = {
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
        provider._instrument_mgr = InstrumentManager()
        provider._historical_mgr = HistoricalDataManager(
            state_store=provider._state_store,
            callback_group=provider._callback_group,
        )

        # Phase 2 (CC-02+CC-04): 组合持有6个Manager，替代Mixin继承
        from ali2026v3_trading.lifecycle_manager import LifecycleManager
        from ali2026v3_trading.analytics_manager import AnalyticsManager
        from ali2026v3_trading.event_publisher import EventPublisher
        from ali2026v3_trading.health_check_manager import HealthCheckManager
        from ali2026v3_trading.scheduler_manager_proxy import SchedulerManagerProxy
        provider._lifecycle_mgr = LifecycleManager(provider=provider)

        provider._analytics_mgr = AnalyticsManager(provider=provider)
        provider._event_publisher = EventPublisher(provider=provider)
        provider._health_check_mgr = HealthCheckManager(provider=provider)
        provider._scheduler_mgr_proxy = SchedulerManagerProxy(provider=provider)

        provider._config_layer = self
        provider._business_layer = None  # 由_init_attributes后续设置
        provider._monitoring_layer = None  # 由_init_attributes后续设置

        # Phase 2: 同步初始状态到LifecycleManager（双写过渡期）
        provider._lifecycle_mgr.state = provider._state
        provider._lifecycle_mgr.is_running = provider._is_running
        provider._lifecycle_mgr.is_paused = provider._is_paused
        provider._lifecycle_mgr.destroyed = provider._destroyed
        provider._lifecycle_mgr.initialized = provider._initialized

        # 初始化历史K线Mixin
        provider._init_historical_kline_mixin()

        # 初始化Tick处理Mixin
        provider._init_tick_handler_mixin()

        # R13-P0-API-07修复: Mixin初始化依赖隐式属性设置，添加显式检查
        from ali2026v3_trading.strategy_checkpoint_mixin import StrategyCheckpointMixin
        StrategyCheckpointMixin._validate_mixin_attributes(provider)

        # 将关键状态同步到StateStore，供HistoricalDataManager等Manager通过StateStore读取
        provider._sync_state_to_store()

        # 在CallbackGroup中注册关键回调方法
        provider._register_callbacks()

    def resolve_open_reason(self) -> str:
        state = 'unknown'
        try:
            spm = getattr(self._provider, '_state_param_manager', None)
            if spm:
                state = spm.get_current_state()
        except Exception:
            logging.warning("[R22-EP-P1] StrategyConfigLayer exception swallowed")
            pass

        _STATE_REASON_MAP = {
            STRATEGY_MODE_CORRECT_TRENDING: STRATEGY_MODE_CORRECT_RESONANCE,
            STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: STRATEGY_MODE_CORRECT_DIVERGENCE,
            STRATEGY_MODE_INCORRECT_REVERSAL: 'INCORRECT_REVERSAL',
            'incorrect_reversal_defensive': 'INCORRECT_DIVERGENCE',
            STRATEGY_MODE_OTHER: 'OTHER_SCALP',
        }
        reason = _STATE_REASON_MAP.get(state, '')
        if not reason:
            logging.warning("[StrategyConfigLayer] R14-P1-DEAD-02: _resolve_open_reason遇到未知状态'%s'，默认OTHER_SCALP", state)
            reason = 'OTHER_SCALP'
        return reason

    def init_services(self, event_bus: Optional[Any] = None) -> None:
        """服务初始化 — 从StrategyCoreService._init_services迁移"""
        provider = self._provider

        # EventBus 集成
        provider._event_bus = event_bus

        # DFG-P1-13修复: 订阅系统启动事件，触发检查点恢复
        if event_bus is not None:
            try:
                event_bus.subscribe_weak('SystemEvent', provider._on_system_event_checkpoint)
            except Exception:
                logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                pass

        # R15-P1-RES-09修复: atexit handler确保ThreadPoolExecutor.shutdown()
        import atexit as _atexit
        _atexit.register(provider._atexit_cleanup_executor)

        # 信号服务集成
        from ali2026v3_trading.signal_service import get_signal_service
        provider._signal_service = get_signal_service(event_bus=event_bus)

        # 运行时平台 API 绑定
        provider.subscribe = None
        provider.unsubscribe = None
        provider.get_instrument = None
        provider.get_kline = None
        provider._runtime_strategy_host = None
        provider._runtime_market_center = None
        provider._fallback_market_center = None
        provider._api_ready = False
        provider._platform_subscribe_completed = threading.Event()

        # R23-SM-03-FIX: 连接状态机
        provider._connection_state = 'DISCONNECTED'
        provider._last_tick_time = 0.0
        provider._connection_stale_threshold_sec = 120.0
        provider._reconnect_attempts = 0
        provider._max_reconnect_attempts = 5

        # R21-NET-P1-03修复: 定期心跳检查机制 — 运行时持续验证平台连接状态
        # R21-NET-P2-01修复: 心跳间隔改为可配置参数，支持从环境变量或配置文件读取
        _heartbeat_env = os.environ.get('STRATEGY_HEARTBEAT_INTERVAL_SEC', '').strip()
        if _heartbeat_env:
            try:
                provider._heartbeat_interval_sec = float(_heartbeat_env)
            except ValueError:
                provider._heartbeat_interval_sec = 30.0
        else:
            provider._heartbeat_interval_sec = 30.0
        provider._last_heartbeat_check = 0.0
        provider._heartbeat_consecutive_failures = 0
        provider._heartbeat_max_failures = 3  # 连续N次心跳失败→标记断线

        # P0修复：端到端六段计数器
        # R21-NET-P1-07修复: 确保生产环境也完整激活E2E六段计数器
        # 六段链路: configured→preregistered→subscribed→first_tick→enqueued→persisted
        # 生产环境必须与回测环境一样完整记录每段计数，用于数据完整性校验
        provider._e2e_counters = {
            'configured_instruments': 0,
            'preregistered_instruments': 0,
            'platform_subscribe_called': 0,
            'first_tick_received': 0,
            'enqueued_count': 0,
            'persisted_count': 0,
            'kline_persisted_count': 0,
        }
        provider._e2e_shard_enqueued = {}
        provider._e2e_shard_persisted = {}
        provider._e2e_first_tick_set = set()

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
        provider._ssl_verify = os.environ.get('SSL_VERIFY', 'true').lower() != 'false'
        provider._cert_path = os.environ.get('SSL_CERT_PATH', None)
        if provider._cert_path and os.path.isfile(provider._cert_path):
            import ssl as _ssl_mod
            provider._ssl_context = _ssl_mod.create_default_context(cafile=provider._cert_path)
            logging.info("[SEC-P1-05] SSL证书验证已配置: cert_path=%s", provider._cert_path)
        elif not provider._ssl_verify:
            logging.warning("[SEC-P1-05] SSL证书验证已禁用(不推荐生产环境)")
        else:
            provider._ssl_context = None
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
        provider.analytics_service = None
        provider._future_ids = set()
        provider._option_ids = set()
        provider._subscribed_instruments = []
        provider._init_instruments_result = None

        provider._storage = None
        provider._storage_init_error = None
        provider._storage_lock = threading.RLock()
        provider._storage_warm_thread = None
        provider._storage_warm_started = False
        provider._platform_subscribe_thread = None
        provider._platform_subscribe_stop = threading.Event()
        provider._platform_subscribe_lock = threading.Lock()
        provider._last_option_status_log_time = 0.0
        provider._option_status_log_interval = 180.0

        # 订单和持仓服务（惰性初始化）
        provider._order_service = None
        provider._position_service = None

        # 状态路由与安全元层
        provider._state_param_manager = None
        provider._safety_meta_layer = None

        # 影子策略监控引擎(V7次系统1)
        # N-02确认: _shadow_engine在_feed_shadow_engine、get_health_status等处被调用消费，非孤立
        provider._shadow_engine = None

        # 策略生态系统(V7次系统2)
        provider._strategy_ecosystem = None

        # HFT增强引擎(V8)
        provider._hft_engine = None

        # 市场快照采集器 — 覆盖18策略(6主策略×3变体)
        provider._snapshot_collector = None

        # R17-P1-CFG-P1-05: 启动时全局依赖检查

    def check_startup_dependencies(self) -> None:
        """R17-P1-CFG-P1-05: 启动时全局依赖检查"""
        provider = self._provider
        _required_attrs = ['_state_store', '_callback_group', '_data_access']
        for attr in _required_attrs:
            if not hasattr(provider, attr) or getattr(provider, attr) is None:
                logging.error("[STARTUP-DEP] R17-P1-CFG-P1-05: 关键依赖缺失: %s", attr)

    def coordinated_shutdown(self) -> None:
        """R17-P1-CFG-P1-04: 统一优雅关机协调器"""
        provider = self._provider
        services = []
        if hasattr(provider, '_shadow_engine') and provider._shadow_engine:
            services.append(('ShadowEngine', provider._shadow_engine))
        # P3-6-FIX: 使用get_risk_service()替代不存在的_risk_service属性
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            _risk_svc = get_risk_service()
            if _risk_svc is not None:
                services.append(('RiskService', _risk_svc))
        except Exception:
            pass
        if hasattr(provider, '_order_service') and provider._order_service:
            services.append(('OrderService', provider._order_service))
        for name, svc in services:
            try:
                if hasattr(svc, 'shutdown'):
                    svc.shutdown()
            except Exception as e:
                logging.error("[SHUTDOWN] %s shutdown failed: %s", name, e)

    def init_logging(self, params: Optional[Dict[str, Any]] = None) -> None:
        logging.info("[StrategyConfigLayer] 日志初始化完成")

    def init_scheduler(self) -> None:
        pass

    def stop_scheduler(self) -> None:
        pass
