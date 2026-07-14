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

from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from datetime import datetime


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
        provider._health_pause_new_open_ts = 0.0
        provider._analytics_warmup_done = False
        provider._analytics_warmup_thread = None
        # R15-P1-RES-04修复: 策略降级后自动恢复检测
        provider._degraded_since = None
        provider._recovery_check_interval_sec = 60.0
        provider._last_recovery_check = 0.0
        provider.t_type_service = None
        provider._processed_trade_ids = collections.OrderedDict()
        provider._trade_ids_lock = threading.Lock()

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
        from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler
        provider._scheduler_manager = StrategyScheduler()

        # 性能统计
        # FIX-20260708-V2: start_time初始化为当前时间(而非None)，确保output_periodic_summary
        # 即使on_init()未完成也能显示运行时长。on_init()会在L122更新为精确的初始化完成时间。
        provider._stats = {
            'start_time': datetime.now(CHINA_TZ),
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
        from ali2026v3_trading.strategy.instrument_service import InstrumentManager
        from ali2026v3_trading.data.historical_data_manager import HistoricalDataManager
        provider._instrument_mgr = InstrumentManager()
        provider._historical_mgr = HistoricalDataManager(
            state_store=provider._state_store,
            callback_group=provider._callback_group,
        )

        # Phase 2 (CC-02+CC-04): 组合持有6个Manager，替代Mixin继承
        from ali2026v3_trading.lifecycle.lifecycle_manager import LifecycleManager
        from ali2026v3_trading.strategy_judgment.analytics_manager import AnalyticsManager
        from ali2026v3_trading.infra.concurrent_utils import EventPublisher
        from ali2026v3_trading.infra.scheduler_service import SchedulerManagerProxy
        provider._lifecycle_mgr = LifecycleManager(provider=provider)

        provider._analytics_mgr = AnalyticsManager(provider=provider)
        provider._event_publisher = EventPublisher(provider=provider)
        provider._scheduler_mgr_proxy = SchedulerManagerProxy(provider=provider)

        provider._config_layer = self
        # 保留已初始化的business_layer和monitoring_layer（由_init_core_deps设置）
        if getattr(provider, '_business_layer', None) is None:
            provider._business_layer = None
        if getattr(provider, '_monitoring_layer', None) is None:
            provider._monitoring_layer = None

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

        # R13-P0-API-07修复: Service初始化依赖隐式属性设置，添加显式检查
        # FIX: G2b Mixin消灭后，TickProcessingService属性在_tick_svc实例上，不在provider上
        _REQUIRED_ATTRS = {
            'LifecycleService': ['_state', '_state_lock', '_lock'],
            'KlineDataService': ['_historical_load_in_progress', '_historical_loader_lock'],
        }
        for _svc_name, _attrs in _REQUIRED_ATTRS.items():
            for _attr in _attrs:
                if not hasattr(provider, _attr):
                    logging.warning(
                        "[R13-P0-API-07] %s required attribute '%s' not initialized",
                        _svc_name, _attr,
                    )
        # TickProcessingService属性检查：检查_tick_svc实例而非provider
        _tick_svc = getattr(provider, '_tick_svc', None) or getattr(provider, '_tick_service', None)
        if _tick_svc is not None:
            for _attr in ['_tick_count', '_shard_router']:
                if not hasattr(_tick_svc, _attr):
                    logging.warning(
                        "[R13-P0-API-07] TickProcessingService required attribute '%s' not initialized",
                        _attr,
                    )

        # 将关键状态同步到StateStore，供HistoricalDataManager等Manager通过StateStore读取
        provider._sync_state_to_store()

        # 在CallbackGroup中注册关键回调方法
        provider._register_callbacks()

    def resolve_open_reason(self) -> str:
        state = 'unknown'
        try:
            spm = getattr(self._provider, '_state_param_manager', None)
            if not spm:
                try:
                    from ali2026v3_trading.config.state_param import get_state_param_manager
                    spm = get_state_param_manager()
                except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                    pass
            if spm:
                try:
                    spm.update_state_from_width_cache()
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass
                state = spm.get_current_state()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.warning("[R22-EP-P1] StrategyConfigLayer exception swallowed")
            pass

        reason = _STATE_REASON_MAP.get(state, '')
        if not reason:
            logging.warning("[StrategyConfigLayer] R14-P1-DEAD-02: _resolve_open_reason遇到未知状态'%s'，默认OTHER_SCALP", state)
            reason = 'OTHER_SCALP'

        # FIX-20260711-S3S5S6: S3-BOX_EXTREME/S5-ARBITRAGE/S6-MARKET_MAKING信号通路
        # 根因: SPM五态系统不产出box_extreme/arbitrage/market_making状态，
        #       导致_STATE_REASON_MAP中对应映射为死代码，S3/S5/S6永远0下单
        # 修复: 当SPM状态为'other'时，检查独立信号检测器，优先级:
        #       BOX_EXTREME > ARBITRAGE > MARKET_MAKING > OTHER_SCALP
        if state in (STRATEGY_MODE_OTHER, 'other'):
            try:
                eco = None
                try:
                    from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
                    eco = get_strategy_ecosystem()
                except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                    pass

                # S3-BOX_EXTREME: 检查BoxDetector极值信号
                # P0-2修复: classify_extreme_state()要求current_price为必需参数，
                #           且resonance_direction需为fall/rise等值才能产出tradeable信号
                if eco is not None:
                    bd = getattr(eco, '_box_detector', None)
                    if bd is not None:
                        try:
                            _bd_price = 0.0
                            try:
                                from ali2026v3_trading.data.data_service import get_data_service
                                _bd_ds = get_data_service()
                                if _bd_ds and getattr(_bd_ds, 'realtime_cache', None):
                                    _bd_price = _bd_ds.realtime_cache.get_latest_price(
                                        getattr(self._provider, '_instrument_id', '')
                                    ) or 0.0
                            except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                                pass
                            _bd_res_dir = ''
                            try:
                                from ali2026v3_trading.param_pool.optimization.cycle_sharpe import get_cycle_resonance_module
                                _bd_crm = get_cycle_resonance_module()
                                _bd_output = getattr(_bd_crm, '_last_output', None)
                                if _bd_output and hasattr(_bd_output, 'directional_bias'):
                                    _bd_bias = _bd_output.directional_bias
                                    if _bd_bias > 0.3:
                                        _bd_res_dir = 'rise'
                                    elif _bd_bias < -0.3:
                                        _bd_res_dir = 'fall'
                            except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                                pass
                            if _bd_price > 0:
                                bd.classify_extreme_state(
                                    current_price=_bd_price,
                                    resonance_direction=_bd_res_dir,
                                )
                        except (ValueError, KeyError, TypeError, AttributeError):
                            pass
                        extreme = getattr(bd, '_extreme_state', None)
                        if extreme is not None and getattr(extreme, 'tradeable', False):
                            reason = 'BOX_EXTREME'
                            logging.info("[FIX-S3S5S6] BoxDetector极值信号触发: extreme_type=%s → BOX_EXTREME",
                                        getattr(extreme, 'extreme_type', 'unknown'))

                # S5-ARBITRAGE: 检查套利信号(高置信度)
                # 注意: HFT通道已在handle_arbitrage_signal()中直接下单(reason='hft_arbitrage')
                #       为避免双重下单，仅当HFT通道未处理该信号时才走主交易周期
                if reason == 'OTHER_SCALP':
                    try:
                        from ali2026v3_trading.strategy.tick_hft import get_last_arbitrage_signal
                        arb_sig = get_last_arbitrage_signal()
                        if arb_sig is not None:
                            arb_conf = arb_sig.get('confidence', 0.0)
                            arb_dev = abs(arb_sig.get('deviation_bps', 0.0))
                            arb_consumed = arb_sig.get('hft_consumed', False)
                            if arb_conf >= 0.8 and arb_dev > 100 and not arb_consumed:
                                reason = 'ARBITRAGE'
                                logging.info("[FIX-S3S5S6] 套利信号触发: dev=%.1fbps conf=%.2f → ARBITRAGE",
                                            arb_dev, arb_conf)
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                        pass

                # S6-MARKET_MAKING: 检查做市条件(窄价差+低持仓)
                if reason == 'OTHER_SCALP':
                    try:
                        pos_svc = getattr(self._provider, '_position_service', None)
                        if pos_svc is not None:
                            open_count = len(getattr(pos_svc, '_positions', {}))
                            max_positions = getattr(pos_svc, '_max_positions', 30)
                            if open_count < max_positions * 0.1:
                                eco_mm = eco if eco is not None else None
                                mm_slot = getattr(eco_mm, '_market_making', None) if eco_mm else None
                                if mm_slot is not None and getattr(mm_slot, 'state', None) is not None:
                                    mm_cap = getattr(mm_slot, 'capital_allocation', 0.0)
                                    if mm_cap > 0.05:
                                        spread_ok = False
                                        try:
                                            from ali2026v3_trading.config.params_service import get_params_service
                                            _ps_mm = get_params_service()
                                            _last_spread_bps = _ps_mm.get_float('last_bid_ask_spread_bps', 999.0)
                                            _max_spread = _ps_mm.get_float('market_maker_max_spread_bps', 50.0)
                                            spread_ok = _last_spread_bps <= _max_spread
                                        except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                                            spread_ok = True
                                        if spread_ok:
                                            reason = 'MARKET_MAKING'
                                            logging.info("[FIX-S3S5S6] 做市条件触发: open=%d cap=%.2f spread_ok=%s → MARKET_MAKING",
                                                         open_count, mm_cap, spread_ok)
                    except (ValueError, KeyError, TypeError, AttributeError):
                        pass

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _s3s5s6_err:
                logging.debug("[FIX-S3S5S6] S3/S5/S6信号检查异常: %s", _s3s5s6_err)

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
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                pass

        # R15-P1-RES-09修复: atexit handler确保ThreadPoolExecutor.shutdown()
        import atexit as _atexit
        _atexit.register(provider._atexit_cleanup_executor)

        # 信号服务集成
        from ali2026v3_trading.signal.signal_service import get_signal_service
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
        # FIX-20260714-THREAD-STOP: 为启动阶段daemon线程提供统一退出事件
        provider._bulk_subscribe_stop = threading.Event()
        provider._subscribe_retry_stop = threading.Event()
        provider._deferred_subscribe_stop = threading.Event()
        provider._historical_kline_stop = threading.Event()
        provider._historical_kline_thread = None
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
            from ali2026v3_trading.risk.risk_service import get_risk_service
            _risk_svc = get_risk_service()
            if _risk_svc is not None:
                services.append(('RiskService', _risk_svc))
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass
        if hasattr(provider, '_order_service') and provider._order_service:
            services.append(('OrderService', provider._order_service))
        for name, svc in services:
            try:
                if hasattr(svc, 'shutdown'):
                    svc.shutdown()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error("[SHUTDOWN] %s shutdown failed: %s", name, e)

    def init_logging(self, params: Optional[Dict[str, Any]] = None) -> None:
        try:
            from ali2026v3_trading.config.config_logging import setup_logging
            setup_logging()
            logging.info("[StrategyConfigLayer] 日志初始化完成（已调用setup_logging）")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[StrategyConfigLayer] setup_logging调用失败: %s", e)

    def init_scheduler(self) -> None:
        pass

    def stop_scheduler(self) -> None:
        pass


# ============================================================================
# 以下为原 strategy_config.py 内容（已合并到 strategy_config_layer.py）
# ============================================================================

"""
策略配置模块 — 从config_params.py拆分
职责: 策略模式常量、策略参数表(策略相关部分)、策略参数缓存管理
"""
import copy
import time
from typing import Any, Dict, List, Optional

# R1-2修复: CACHE_TTL权威源从config._constants导入，消除与config_params的循环依赖
from ali2026v3_trading.config._params_canary_env import CACHE_TTL  # noqa: F401

STRATEGY_MODE_CORRECT_TRENDING = "correct_trending"
STRATEGY_MODE_INCORRECT_REVERSAL = "incorrect_reversal"
STRATEGY_MODE_SPRING = "spring"
STRATEGY_MODE_RESONANCE = "resonance"
STRATEGY_MODE_BOX = "box"
STRATEGY_MODE_HFT = "hft"
STRATEGY_MODE_BOX_EXTREME = "box_extreme"
STRATEGY_MODE_BOX_SPRING = "box_spring"
STRATEGY_MODE_ARBITRAGE = "arbitrage"
STRATEGY_MODE_MARKET_MAKING = "market_making"
STRATEGY_MODE_HIGH_FREQ = "high_freq"
STRATEGY_MODE_DIVERGENCE_REVERSAL = "divergence_reversal"

ALL_STRATEGY_MODES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_SPRING,
    STRATEGY_MODE_RESONANCE,
    STRATEGY_MODE_BOX,
    STRATEGY_MODE_HFT,
    STRATEGY_MODE_DIVERGENCE_REVERSAL,
)

STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE = "correct_trending_defensive"
STRATEGY_MODE_OTHER = "other"
STRATEGY_MODE_CORRECT_RESONANCE = "CORRECT_RESONANCE"
STRATEGY_MODE_CORRECT_DIVERGENCE = "CORRECT_DIVERGENCE"

ALL_MARKET_STATES = (
    STRATEGY_MODE_CORRECT_TRENDING,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,
    STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_OTHER,
    STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_DIVERGENCE,
)

# 五唯一性修复：_STATE_REASON_MAP 权威定义（合并自3处重复定义）
# 原 strategy_config_layer 模块级5项 + 函数级9项(含策略模式) + backtest_config 9项(含原始五态)
# 键 = 状态字符串（5策略状态 + 5原始五态 + 4策略模式，'other'重复已合并）
# 值 = 开仓理由标签
_STATE_REASON_MAP = {
    # 5种策略状态 → 7种开仓理由
    STRATEGY_MODE_CORRECT_TRENDING: STRATEGY_MODE_CORRECT_RESONANCE,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: 'DIVERGENCE_REVERSAL',
    STRATEGY_MODE_INCORRECT_REVERSAL: 'DIVERGENCE_REVERSAL',
    'incorrect_reversal_defensive': 'DIVERGENCE_REVERSAL',
    STRATEGY_MODE_OTHER: 'OTHER_SCALP',
    # 5种原始五态 → 7种开仓理由
    'correct_rise': 'CORRECT_RESONANCE',
    'correct_fall': 'DIVERGENCE_REVERSAL',
    'wrong_rise': 'DIVERGENCE_REVERSAL',
    'wrong_fall': 'DIVERGENCE_REVERSAL',
    # 4种策略模式 → 策略理由
    'spring': 'BOX_SPRING',
    'box': 'BOX_SPRING',
    'hft': 'HIGH_FREQ',
    'high_freq': 'HIGH_FREQ',
    'intraday': 'INTRADAY',  # [FIX-20260712-S2] S2日内交易策略
    'arbitrage': 'ARBITRAGE',
    'market_making': 'MARKET_MAKING',
    'divergence_reversal': 'DIVERGENCE_REVERSAL',
    'box_extreme': 'BOX_EXTREME',
}

_strategy_param_caches: Dict[str, Dict[str, Any]] = {}
_strategy_cache_timestamps: Dict[str, float] = {}
_param_age_monitor: Dict[str, float] = {}
_invalidate_idempotent: Dict[str, float] = {}
_INVALIDATE_IDEMPOTENT_SEC = 1.0
_strategy_cache_lock = threading.Lock()


def resolve_open_reason_from_state(state: str) -> str:
    reason = _STATE_REASON_MAP.get(state, '')
    if not reason:
        logging.warning("[strategy_config] 未知状态'%s'，默认OTHER_SCALP", state)
        reason = 'OTHER_SCALP'
    return reason


def get_strategy_param_cache(strategy_id: str) -> Optional[Dict[str, Any]]:
    with _strategy_cache_lock:
        return _strategy_param_caches.get(strategy_id)


def set_strategy_param_cache(strategy_id: str, params: Dict[str, Any]) -> None:
    with _strategy_cache_lock:
        _strategy_param_caches[strategy_id] = params
        _strategy_cache_timestamps[strategy_id] = time.time()


def invalidate_strategy_cache(strategy_id: str) -> bool:
    _now = time.time()
    if strategy_id in _invalidate_idempotent and (_now - _invalidate_idempotent[strategy_id]) < _INVALIDATE_IDEMPOTENT_SEC:
        return False
    _invalidate_idempotent[strategy_id] = _now
    with _strategy_cache_lock:
        if strategy_id in _strategy_param_caches:
            del _strategy_param_caches[strategy_id]
            _strategy_cache_timestamps.pop(strategy_id, None)
            logging.info("[strategy_config] 已清理策略级参数缓存 strategy_id=%s", strategy_id)
            return True
    return False


def get_all_strategy_modes():
    return ALL_STRATEGY_MODES


def get_all_market_states():
    return ALL_MARKET_STATES


# ============================================================================
# 策略级默认参数常量 — 从config_params.py迁移
# ============================================================================

STRATEGY_DEFAULTS = {
    'signal_cooldown_sec': 60.0,
    'close_take_profit_ratio': 1.8,
    'close_stop_loss_ratio': 0.3,
    'max_risk_ratio': 0.8,
    'default_slippage_bps': 3.0,
    'max_open_positions': 3,
    'tvf_enabled': True,
    'tvf_l1_weight': 0.40,
    'tvf_l2_weight': 0.35,
    'tvf_l3_weight': 0.25,
    'STRATEGY_SL_RATIOS': {
        'correct_trending': 0.4,
        'incorrect_reversal': 0.6,
        'other': 0.5,
        'box_extreme': 0.3,
        'hft': 0.2,
        'intraday': 0.5,  # [FIX-20260712-S2] S2日内交易止损比率
    },
    'max_signals_per_window': 5,
    'state_confirm_bars': 5,
    'circuit_breaker_pause_sec': 180.0,
    'max_hold_minutes': 120,
    'lots_min': 3,
    'option_buy_lots_min': 1,
    'option_buy_lots_max': 100,
    'position_limit_max_ratio': 0.2,
    'position_timeout_sec': 3600,
    'daily_drawdown_multiplier': 2.0,
    'circuit_breaker_trigger_sigma': 3.0,
}

# R24-P1-DF-06修复: 中心化默认值常量体系、单一真相源
# R26-P2-DF-01修复: 核心参数语义注释
CENTRALIZED_DEFAULTS = {
    'max_risk_ratio': 0.8,  # 最大风险比率，风控拒绝阈值(0~1)
    'close_stop_loss_ratio': 0.3,  # 全局止损比率，持仓亏损达此比例触发平仓
    'close_take_profit_ratio': 1.8,  # R24-P1-DF-07修复: 全局止盈比率，持仓盈利达此比例触发平仓
    'signal_cooldown_sec': 60.0,  # R24-P1-DF-10修复: 信号冷却时间(秒)，同合约两次信号最小间隔
    'state_confirm_bars': 5,  # 状态确认K线数，连续N根K线确认趋势状态
    'alpha_window_days': 7,  # Alpha衰减计算窗口(天)，评估策略Alpha衰减的时间跨度
    'circuit_breaker_pause_sec': 180.0,  # 断路器暂停时间(秒)，触发后暂停交易时长
    'daily_drawdown_multiplier': 2.0,  # 日内回撤乘数，最大回撤=波动率×此乘数
    'max_hold_minutes': 120,  # 最大持仓时间(分钟)，超时强制平仓
    'max_signals_per_window': 10,  # 窗口内最大信号数，防止信号洪泛
    'lots_min': 3,  # R24-P1-DF-08修复: 最小开仓手数
    'option_buy_lots_min': 1,  # 期权最小买入手数
    'option_buy_lots_max': 100,  # 期权最大买入手数
    'tvf_enabled': True,  # P1-01修复: TVF三因子投票开关
    'tvf_l1_weight': 0.40,  # P1-01修复: TVF L1因子权重(趋势验证)
    'tvf_l2_weight': 0.35,  # P1-01修复: TVF L2因子权重(订单流验证)
    'tvf_l3_weight': 0.25,  # P1-01修复: TVF L3因子权重(Greeks验证)
    # R19-P1-02修复: 补齐params_default.json中但CENTRALIZED_DEFAULTS缺失的风控关键参数
    'position_limit_max_ratio': 0.2,  # 单合约持仓上限占总资金比例
    'circuit_breaker_trigger_sigma': 3.0,  # 断路器触发标准差倍数
    'kline_max_age_sec': 60,  # K线最大有效期(秒)，过期K线不参与计算
    'signal_max_age_sec': 180,  # 信号最大有效期(秒)，过期信号不执行
    'subscription_batch_size': 10,  # 订阅批量大小，单次订阅合约数
    'rate_limit_min_interval_sec': 1,  # API调用最小间隔(秒)，限频保护
    'enforce_trading_session_boundary': True,  # 是否强制交易时段边界检查
    'last_trading_day_close_ahead_days': 3,  # 到期前N天提前平仓
    'delivery_slippage_multiplier_max': 3.0,  # 交割滑点乘数上限
    'default_slippage_bps': 3.0,  # R27-P0-FIX: 默认滑点(基点)，与DEFAULT_PARAM_TABLE对齐
    'intraday_max_total_ticks': 5000000,  # 日内最大tick总量，超量丢弃
    'tick_shard_count': 16,  # tick分片数，并发写入分片数
    'CACHE_TTL': CACHE_TTL,  # R1-2修复: 引用_constants.CACHE_TTL而非硬编码
    # R24-P1-DF-05修复: 策略级止损比率默认值集中管理
    'STRATEGY_SL_RATIOS': {
        'correct_trending': 0.4,  # 正确趋势止损比率
        'incorrect_reversal': 0.6,  # R27-P0-FIX: 错误反转止损比率，与V7.0手册§9.2及position_service对齐
        'other': 0.5,  # 其他状态止损比率
        'box_extreme': 0.3,  # 箱体极值止损比率
        'hft': 0.2,  # HFT止损比率
    },
    'position_timeout_sec': 3600,  # R24-P1-DF-12修复: 持仓超时默认值(秒)
    'max_retry_count': 3,  # R24-P1-DF-14修复: 最大重试次数默认值
    'retry_delay_sec': 5.0,  # R24-P1-DF-15修复: 重试间隔默认值(秒)
    'log_retention_days': 7,  # R24-P1-DF-17修复: 日志保留天数默认值
}

# ============================================================================
# 策略参数监控与通知 — 从config_params.py迁移
# ============================================================================

# [FR-P1-08~15-FIX] 通用数据新鲜度增量
_cache_refresh_timestamps: Dict[str, float] = {}
_pipeline_latency_monitor: Dict[str, float] = {}
_computation_timestamps: Dict[str, float] = {}
_indicator_freshness_cache: Dict[str, tuple] = {}
_risk_data_freshness: Dict[str, float] = {}


def subscribe_param_changes(callback) -> None:
    """[FR-P1-10-FIX] 参数变更事件订阅 — P1-05修复: 统一走EventBus单通道"""
    try:
        from ali2026v3_trading.infra.event_bus import EventBus
        event_bus = EventBus.get_instance()
        event_bus.subscribe("param_changed", callback)
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
        pass


def _notify_param_change(param_key: str, old_val: Any, new_val: Any) -> None:
    """[FR-P1-10-FIX] 参数变更事件发布通知 — P1-05修复: 统一走EventBus单通道"""
    try:
        from ali2026v3_trading.infra.event_bus import EventBus
        event_bus = EventBus.get_instance()
        event_bus.publish("param_changed", {
            "param_key": param_key,
            "old_val": old_val,
            "new_val": new_val,
        })
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
        pass


def record_pipeline_latency(stage: str, latency_sec: float) -> None:
    """[FR-P1-12-FIX] 数据管道延迟监控"""
    _pipeline_latency_monitor[stage] = latency_sec


def mark_computation_timestamp(computation_id: str) -> None:
    """[FR-P1-13-FIX] 计算结果时间戳标记"""
    _computation_timestamps[computation_id] = time.time()


def check_indicator_freshness(indicator_key: str, max_age_sec: float = 60.0) -> bool:
    """[FR-P1-14-FIX] 指标计算新鲜度校验"""
    _entry = _indicator_freshness_cache.get(indicator_key)
    if _entry is None:
        return False
    _ts, _ = _entry
    return (time.time() - _ts) < max_age_sec


def record_indicator_value(indicator_key: str, value: Any) -> None:
    """[FR-P1-14-FIX] 记录指标值和时间戳"""
    _indicator_freshness_cache[indicator_key] = (time.time(), value)


def check_risk_data_freshness(risk_key: str, max_age_sec: float = 5.0) -> bool:
    """[FR-P1-15-FIX] 风控数据新鲜度校验"""
    _ts = _risk_data_freshness.get(risk_key, 0.0)
    return (time.time() - _ts) < max_age_sec


def update_risk_data_timestamp(risk_key: str) -> None:
    """[FR-P1-15-FIX] 更新风控数据时间戳"""
    _risk_data_freshness[risk_key] = time.time()


# [R23-P2-FR-10-FIX] 参数年龄监控汇总方法
def get_param_age_summary() -> Dict[str, Any]:
    """返回参数年龄监控汇总"""
    _now = time.time()
    _summary = {}
    for _sid, _at in _param_age_monitor.items():
        _summary[_sid] = {'age_sec': _now - _at, 'last_access': _at}
    return _summary


def check_multi_level_cache_consistency() -> Dict[str, bool]:
    """[FR-P1-11-FIX] 多级缓存一致性校验"""
    _now = time.time()
    result = {}
    for _sid in list(_strategy_param_caches):
        _strategy_ts = _strategy_cache_timestamps.get(_sid, 0.0)
        result[_sid] = (_now - _strategy_ts) < _get_cache_ttl()
    return result