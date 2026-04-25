"""
策略核心服务模块 - CQRS 架构 Core 层
来源：13_strategy_core.py + 15_diagnosis.py (部分)
功能：策略生命周期管理 + 状态监控 + 诊断服务
行数：~800 行 (-43% vs 原 1400 行)

优化 v1.1 (2026-03-16):
- ✅ 集成 EventBus 发布策略事件
- ✅ 添加健康检查接口
- ✅ 性能监控仪表板支持

优化 v2.0 (2026-04-10):
- ✅ PathCounter 已迁移至 performance_monitor.py
- ✅ 减少 ~157 行代码，提升模块化程度
"""
from __future__ import annotations

import os
import sys
import inspect
import re
import importlib.util
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable
from functools import wraps
from enum import Enum

# ============================================================================
# 枚举定义
# ============================================================================

class StrategyState(Enum):
    """策略完整生命周期状态"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    DEGRADED = "degraded"      # 降级态：API未就绪/队列丢弃过多/订阅部分失败
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"
    DEGRADED_STOP = "degraded_stop"  # 停止但未完全收敛

from ali2026v3_trading.scheduler_service import get_market_time_service, is_market_open
from ali2026v3_trading.storage import get_instrument_data_manager
from ali2026v3_trading.params_service import get_params_service
from ali2026v3_trading.config_service import get_cached_params, update_cached_params
from ali2026v3_trading.signal_service import SignalService
from ali2026v3_trading.performance_monitor import PathCounter, count_call
from ali2026v3_trading.strategy_scheduler import StrategyScheduler
from ali2026v3_trading.strategy_historical import HistoricalKlineMixin
from ali2026v3_trading.strategy_tick_handler import TickHandlerMixin
from ali2026v3_trading.subscription_manager import SubscriptionManager
from ali2026v3_trading.t_type_service import TTypeService


# 从 params_service 导入，解决 strategy_core_service ↔ strategy_historical 循环导入
from ali2026v3_trading.params_service import _read_param, get_param_value


# ✅ P0-3: StrategyState 已迁移至 enums.py，从此处导入


def _state_key(state: Any) -> str:
    """将状态标准化为稳定的字符串键，兼容热重载后的 Enum 实例比较。"""
    if state is None:
        return ""

    value = getattr(state, 'value', None)
    if value is not None:
        return str(value)

    name = getattr(state, 'name', None)
    if name is not None:
        return str(name)

    return str(state).rsplit('.', 1)[-1]


def _state_is(state: Any, *targets: Any) -> bool:
    state_key = _state_key(state)
    return any(state_key == _state_key(target) for target in targets)


class StrategyCoreService(HistoricalKlineMixin, TickHandlerMixin):
    """策略核心服务 - Core 层
    
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
    
    def __init__(self, strategy_id: str = None, event_bus: Optional[Any] = None):
        """初始化策略核心服务
        
        Args:
            strategy_id: 策略 ID
            event_bus: 事件总线实例（可选）
        """
        self.strategy_id = strategy_id or f"strategy_{int(time.time())}"
        
        # 状态管理
        self._state = StrategyState.INITIALIZING
        self._is_running = False
        self._is_paused = False
        self._is_trading = True
        self._destroyed = False
        self._initialized = False  # 幂等标志：防止on_init被重复调用
        self._analytics_warmup_done = False  # P1-1: analytics warmup 完成标志
        self._analytics_warmup_thread = None  # P1-1: analytics warmup 后台线程
        self.t_type_service = None  # P1-1: warmup完成前为None，调用方必须检查
        
        # 线程锁
        self._lock = threading.RLock()
        self._scheduler_lock = threading.RLock()
        self._state_lock = threading.RLock()
        
        # 调度器管理器（P0重构：提取到 strategy_scheduler.py）
        self._scheduler_manager = StrategyScheduler()
        
        # 性能统计
        self._stats = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'total_klines': 0,  # 新增：K线统计
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': None,
            'tick_by_type': {'future': 0, 'option': 0},
            'tick_by_exchange': {},
            'tick_by_instrument': {}
        }
                
        # 初始化历史K线Mixin
        self._init_historical_kline_mixin()
                
        # 初始化Tick处理Mixin
        self._init_tick_handler_mixin()
        
        # EventBus 集成
        self._event_bus = event_bus
        
        # 信号服务集成
        self._signal_service = SignalService(event_bus=event_bus)

        # 运行时平台 API 绑定（在 Strategy2026.onInit/onStart 中刷新）
        self.subscribe = None
        self.unsubscribe = None
        self.get_instrument = None
        self.get_kline = None
        self._runtime_strategy_host = None
        self._runtime_market_center = None
        self._fallback_market_center = None
        self._api_ready = False  # 断点A修复：API就绪标志
        self._platform_subscribe_completed = threading.Event()  # 断点B修复：平台订阅完成事件

        # P0修复：端到端六段计数器
        self._e2e_counters = {
            'configured_instruments': 0,     # 段1: 已配置合约数
            'preregistered_instruments': 0,  # 段2: 已预注册合约数
            'platform_subscribe_called': 0,  # 段3: 已调用平台订阅数
            'first_tick_received': 0,        # 段4: 已收到首笔Tick的合约数
            'enqueued_count': 0,             # 段5: 已入队Tick/K线数
            'persisted_count': 0,            # 段6: 已落盘Tick/K线数
        }
        self._e2e_first_tick_set = set()  # 记录已收到首笔Tick的合约ID

        # 运行时分析链路
        self.analytics_service = None
        # 保留 _future_ids 和 _option_ids 用于快速判断合约类型
        self._future_ids: set[str] = set()
        self._option_ids: set[str] = set()
        self._subscribed_instruments: List[str] = []
        
        self._storage = None
        self._storage_init_error = None
        self._storage_lock = threading.RLock()
        self._storage_warm_thread: Optional[threading.Thread] = None
        self._storage_warm_started = False
        self._platform_subscribe_thread: Optional[threading.Thread] = None
        self._platform_subscribe_stop = threading.Event()
        self._platform_subscribe_lock = threading.Lock()
        self._last_option_status_log_time = 0.0
        self._option_status_log_interval = 180.0  # ✅ 3分钟输出一次
        
        # 订单和持仓服务（惰性初始化）
        self._order_service = None
        self._position_service = None
    
    # ========== 生命周期管理 ==========

    @property
    def storage(self):
        """惰性初始化storage"""
        if self._storage is None:
            with self._storage_lock:
                if self._storage is None:
                    try:
                        self._storage = get_instrument_data_manager()
                    except Exception as e:
                        logging.error(f"[Storage] Init failed: {e}")
                        raise RuntimeError(f"Storage init failed: {e}")
        return self._storage

    def transition_to(self, new_state: StrategyState) -> bool:
        """
        线程安全的状态转换方法。
        
        Args:
            new_state: 目标状态
            
        Returns:
            bool: 状态转换是否成功
        """
        with self._state_lock:
            old_state = self._state
            self._state = new_state
            logging.debug(f"[StrategyCoreService] State transition: {old_state.value} -> {new_state.value}")
            return True

    def _start_platform_subscribe_async(self, instrument_ids: List[str]) -> None:
        """异步平台订阅"""
        targets = [str(x).strip() for x in (instrument_ids or []) if str(x).strip()]
        if not targets:
            return

        with self._platform_subscribe_lock:
            if self._platform_subscribe_thread and self._platform_subscribe_thread.is_alive():
                return
            self._platform_subscribe_stop.clear()
            thread = threading.Thread(
                target=self._platform_subscribe_worker, 
                args=(targets,), 
                name=f"PlatformSubscribe[strategy:{self.strategy_id}]",
                daemon=True
            )
            self._platform_subscribe_thread = thread
            thread.start()

    def _platform_subscribe_worker(self, instrument_ids: List[str]) -> None:
        """方法唯一修复：统一使用self.subscribe单一入口，order_service降级为内部实现"""
        success = failed = 0
        total = len(instrument_ids)
        
        subscribe_fn = None
        if callable(getattr(self, 'subscribe', None)):
            subscribe_fn = self.subscribe
        else:
            logging.error("[Subscribe] self.subscribe不可用，无法订阅")
        
        for i, inst in enumerate(instrument_ids, 1):
            if self._platform_subscribe_stop.is_set():
                break
            try:
                if subscribe_fn:
                    subscribe_fn(inst)
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logging.warning(f"[Subscribe] Failed {inst}: {e}")
            
            if i % 500 == 0 or i == total:
                logging.info(f"[Subscribe] Progress {i}/{total}, ok={success}, fail={failed}")
        
        logging.info(f"[Subscribe] Done: ok={success}, fail={failed}, total={total}")
        self._platform_subscribe_completed.set()
        if failed > 0 and failed == total:
            logging.error("[Subscribe] ❌ 全部订阅失败，策略进入DEGRADED状态")
            with self._state_lock:
                self._state = StrategyState.DEGRADED

    def bind_platform_apis(self, strategy_obj: Any) -> None:
        """绑定平台API"""
        self._runtime_strategy_host = strategy_obj
        
        from ali2026v3_trading.config_service import resolve_product_exchange
        
        sub = getattr(strategy_obj, 'sub_market_data', None)
        unsub = getattr(strategy_obj, 'unsub_market_data', None)
        
        # 根据PythonGO文档，sub_market_data(exchange, instrument_id) 使用位置参数
        if callable(sub):
            def _subscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
                sub(exchange, instrument_id)
            self.subscribe = _subscribe
        else:
            self.subscribe = None
        
        if callable(unsub):
            def _unsubscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
                unsub(exchange, instrument_id)
            self.unsubscribe = _unsubscribe
        else:
            self.unsubscribe = None
        
        self.get_instrument = getattr(strategy_obj, 'get_instrument', None)
        self._platform_insert_order = _read_param(strategy_obj, 'insert_order')
        if self._platform_insert_order is None:
            self._platform_insert_order = _read_param(strategy_obj, 'send_order')
            if self._platform_insert_order is not None:
                logging.info("[bind_platform_apis] insert_order未找到，使用send_order降级")
        self._platform_cancel_order = _read_param(strategy_obj, 'cancel_order')
        if self._platform_cancel_order is None:
            self._platform_cancel_order = _read_param(strategy_obj, 'cancel_order_ref')
            if self._platform_cancel_order is not None:
                logging.info("[bind_platform_apis] cancel_order未找到，使用cancel_order_ref降级")
        self._platform_get_position = getattr(strategy_obj, 'get_position', None)
        self._platform_get_orders = getattr(strategy_obj, 'get_orders', None)
        # 历史K线API通过 MarketCenter.get_kline_data 获取，不是 strategy.get_kline
        self._runtime_market_center = self._extract_runtime_market_center(strategy_obj) or self._get_fallback_market_center()
        self.get_kline = None
        if self._runtime_market_center and callable(getattr(self._runtime_market_center, 'get_kline_data', None)):
            self.get_kline = self._runtime_market_center.get_kline_data
        self._inject_runtime_context(strategy_obj)

        # 断点A修复：设置API就绪标志，不可运行时升级为DEGRADED
        self._api_ready = callable(self.subscribe) and callable(self.unsubscribe)
        self._kline_ready = callable(self.get_kline)
        
        # 将平台订阅/退订API注入到storage，使SubscriptionManager可以调用
        if hasattr(self, 'storage') and self.storage is not None:
            self.storage.bind_platform_subscribe_api(self.subscribe, self.unsubscribe)
        
        # 将平台下单/撤单API注入到OrderService
        if self._platform_insert_order:
            self._ensure_order_service()
            if self._order_service:
                self._order_service.bind_platform_apis(self._platform_insert_order, self._platform_cancel_order)
        
        if not self._api_ready:
            logging.warning(
                "[bind_platform_apis] 平台API未完全就绪: "
                f"subscribe={callable(self.subscribe)}, unsubscribe={callable(self.unsubscribe)}, "
                "策略将以DEGRADED状态运行"
            )
            self._state = StrategyState.DEGRADED
        if not self._kline_ready:
            logging.warning(
                "[bind_platform_apis] 历史K线API不可用: "
                f"get_kline={callable(self.get_kline)}, 历史K线加载将跳过"
            )

    @staticmethod
    def _extract_runtime_market_center(strategy_obj: Any) -> Any:
        """提取market_center（方法唯一修复：减少到2种路径：直接访问+infini链式）"""
        if not strategy_obj:
            return None
        mc = getattr(strategy_obj, 'market_center', None)
        if mc:
            return mc
        infini = getattr(strategy_obj, 'infini', None)
        if infini:
            return getattr(infini, 'market_center', None)
        return None

    def _inject_runtime_context(self, strategy_obj: Any) -> None:
        """注入运行时上下文"""
        if not strategy_obj:
            return
        
        params = getattr(self, 'params', None)
        if not params:
            return
        
        mc = self._runtime_market_center or self._get_fallback_market_center()
        try:
            # 传递渠道唯一修复：strategy为唯一键名，strategy_instance为属性代理
            if isinstance(params, dict):
                params['strategy'] = strategy_obj
                if mc:
                    params['market_center'] = mc
            else:
                setattr(params, 'strategy', strategy_obj)
                if mc:
                    setattr(params, 'market_center', mc)
            # strategy_instance代理到strategy
            if isinstance(params, dict):
                params['strategy_instance'] = params['strategy']
            else:
                setattr(params, 'strategy_instance', params.strategy)
        except Exception as e:
            logging.warning(f"[Context] Inject failed: {e}")

    def _get_fallback_market_center(self) -> Any:
        """获取fallback market_center"""
        if self._fallback_market_center:
            return self._fallback_market_center
        try:
            from pythongo.core import MarketCenter
            self._fallback_market_center = MarketCenter()
            return self._fallback_market_center
        except Exception as e:
            logging.warning(f"[Fallback] Create failed: {e}")
            return None

    def _init_analytics_services(self, params: Any) -> None:
        """初始化运行时行情和分析服务。"""
        try:
            # ✅ 修复：使用属性访问器触发惰性初始化
            storage = self.storage
            logging.info(f"[StrategyCoreService._init_analytics_services] storage = {storage}")
            
            # 获取合约数据（从 storage 查询，替代本地缓存）
            # 注意：在 on_init 阶段，合约可能尚未注册（注册在 on_start 中进行）
            # 所以这里获取的可能是空列表，实际使用时由 t_type_service 动态解析
            futures_instruments = []
            option_instruments = {}
            try:
                if storage:
                    from ali2026v3_trading.query_service import QueryService
                    from ali2026v3_trading.subscription_manager import SubscriptionManager
                    qs = QueryService(storage)
                    # 获取已注册的合约
                    registered_ids = storage.get_registered_instrument_ids()
                    logging.debug(f"[InitServices] 已注册合约数量: {len(registered_ids)}")
                    
                    # 分类期货和期权
                    for inst_id in registered_ids:
                        if SubscriptionManager.is_option(inst_id):
                            # 期权：按 underlying 分组
                            try:
                                # ✅ ID直通：优先从 ParamsService 获取真实的 underlying_future
                                from ali2026v3_trading.params_service import get_params_service
                                ps = get_params_service()
                                meta = ps.get_instrument_meta_by_id(inst_id) if ps else None
                                
                                if meta and meta.get('underlying_future_id'):
                                    underlying = meta['underlying_future_id']  # 使用数据库中的原始值
                                else:
                                    # ✅ ID直通：降级路径从DB查询，不自行构造
                                    try:
                                        from ali2026v3_trading.data_service import get_data_service
                                        parsed = SubscriptionManager.parse_option(inst_id)
                                        rows = get_data_service().query(
                                            "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                            [parsed['product'], parsed['year_month']]
                                        ).to_pylist()
                                        if rows:
                                            underlying = rows[0]['instrument_id']
                                        else:
                                            logging.warning(f"[InitServices] 标的期货未注册: {parsed['product']}{parsed['year_month']}，跳过期权{inst_id}")
                                            continue
                                    except Exception as db_err:
                                        logging.warning(f"[InitServices] underlying_future_id缺失且DB查询失败: {inst_id} - {db_err}")
                                        continue
                                
                                if underlying not in option_instruments:
                                    option_instruments[underlying] = []
                                option_instruments[underlying].append(inst_id)
                            except Exception as parse_e:
                                logging.debug(f"[InitServices] 期权解析失败: {inst_id} - {parse_e}")
                        else:
                            # 期货
                            futures_instruments.append(inst_id)
                else:
                    logging.debug("[InitServices] storage 未初始化，跳过合约查询")
            except Exception as e:
                logging.warning(f"[InitServices] 从 storage 查询合约失败: {e}")
            
            # ID唯一修复：strategy为唯一键名
            runtime_strategy_instance = _read_param(params, 'strategy')
            
            logging.info(f"[InitServices] futures_instruments count: {len(futures_instruments)}")
            logging.info(f"[InitServices] option_instruments groups: {len(option_instruments)}")
            logging.info(f"[InitServices] strategy_instance: {runtime_strategy_instance is not None}")
            
            # ✅ analytics_service 已移除 (2026-03-28)
            self.analytics_service = None
            
            # ✅ 初始化 TTypeService（期权5种状态诊断）
            # ✅ 传递渠道唯一：使用get_t_type_service()工厂函数获取单例
            try:
                from ali2026v3_trading.t_type_service import get_t_type_service
                self.t_type_service = get_t_type_service()
                logging.info("[AnalyticsInit] ✅ t_type_service initialized")
                
                # ✅ 将 TTypeService 注入到 SubscriptionManager
                if hasattr(self, 'storage') and self.storage and hasattr(self.storage, 'subscription_manager'):
                    self.storage.subscription_manager.set_t_type_service(self.t_type_service)
                    logging.info("[AnalyticsInit] ✅ TTypeService injected into SubscriptionManager")
                
                # ✅ 从数据库加载期货最新价格并初始化TTypeService（解决商品期货无实时tick问题）
                if storage and self.t_type_service:
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        
                        # 从latest_prices表获取期货价格（不依赖futures_instruments表）
                        futures_df = ds.query("""
                            SELECT lp.instrument_id, lp.last_price, fi.internal_id
                            FROM latest_prices lp
                            LEFT JOIN futures_instruments fi ON fi.instrument_id = lp.instrument_id
                            WHERE lp.last_price > 0
                              AND lp.instrument_id NOT LIKE '%-%'
                              AND LENGTH(lp.instrument_id) >= 6
                            ORDER BY lp.instrument_id
                        """, arrow=False, raise_on_error=False)
                        
                        init_count = 0
                        if hasattr(futures_df, 'iterrows'):
                            import re
                            for _, row in futures_df.iterrows():
                                try:
                                    instrument_id = str(row['instrument_id']).strip()
                                    # 解析期货合约ID（复用_extract_contract_year_month统一入口）
                                    clean_id = instrument_id
                                    if '.' in clean_id:
                                        clean_id = clean_id.split('.', 1)[1]
                                    
                                    year_month = self._extract_contract_year_month(clean_id)
                                    if not year_month:
                                        continue
                                    
                                    price = float(row['last_price']) if row['last_price'] is not None else 0.0
                                    
                                    if price <= 0:
                                        continue
                                    
                                    # Bug 6修复：使用 internal_id（整数）而非 instrument_id（字符串）
                                    internal_id = row.get('internal_id')
                                    if internal_id is None:
                                        continue  # 未在futures_instruments表中注册，跳过
                                    
                                    self.t_type_service.on_future_instrument_tick(int(internal_id), price)
                                    init_count += 1
                                except Exception as fut_e:
                                    logging.debug(f"[InitFutures] 初始化期货失败: {fut_e}")
                        
                        if init_count > 0:
                            logging.info(f"[InitFutures] ✅ 从数据库初始化了 {init_count} 个期货合约")
                        else:
                            logging.warning("[InitFutures] 数据库中没有期货价格数据")
                    except Exception as fallback_e:
                        logging.warning(f"[InitFutures] Fallback初始化失败: {fallback_e}")
                
                # ✅ 从数据库预加载期权合约（解决期权Tick在注册前到达的问题）
                if storage and self.t_type_service and hasattr(self.t_type_service, '_width_cache') and self.t_type_service._width_cache:
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        options_df = ds.query("""
                            WITH latest_option_meta AS (
                                SELECT instrument_id, option_type, strike_price, future_instrument_id
                                FROM (
                                    SELECT
                                        instrument_id,
                                        option_type,
                                        strike_price,
                                        future_instrument_id,
                                        ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY timestamp DESC) AS rn
                                    FROM ticks_raw
                                    WHERE option_type IS NOT NULL
                                      AND strike_price IS NOT NULL
                                      AND future_instrument_id IS NOT NULL
                                ) t
                                WHERE rn = 1
                            )
                            SELECT
                                lp.instrument_id,
                                lp.last_price,
                                meta.option_type,
                                meta.strike_price,
                                meta.future_instrument_id
                            FROM latest_prices lp
                            INNER JOIN latest_option_meta meta
                                ON lp.instrument_id = meta.instrument_id
                            WHERE lp.last_price > 0
                            ORDER BY lp.instrument_id
                        """, arrow=False, raise_on_error=False)
                        
                        options_loaded = 0
                        if hasattr(options_df, 'iterrows'):
                            import re
                            for _, row in options_df.iterrows():
                                try:
                                    instrument_id = str(row['instrument_id']).strip()
                                    # ✅ 方法唯一修复：统一使用SubscriptionManager.parse_option，不再内联正则/字符串拆分
                                    try:
                                        parsed_opt = SubscriptionManager.parse_option(instrument_id)
                                        option_product = parsed_opt['product']
                                        month = parsed_opt['year_month']
                                        opt_type = 'CALL' if parsed_opt['option_type'].upper() in ('C', 'CALL') else 'PUT'
                                        strike = float(parsed_opt['strike_price'])
                                    except (ValueError, KeyError, TypeError):
                                        continue
                                    
                                    price = float(row['last_price']) if row['last_price'] is not None else 0.0
                                    opt_type = str(row['option_type'] or opt_type)
                                    strike = float(row['strike_price']) if row['strike_price'] is not None else strike
                                    
                                    if not option_product or not month or strike <= 0 or price <= 0:
                                        continue
                                    
                                    self.t_type_service._width_cache.register_option(
                                        instrument_id,
                                        option_product,
                                        month,
                                        strike,
                                        opt_type,
                                        price,
                                        str(row['future_instrument_id'] or ''),
                                    )
                                    options_loaded += 1
                                except Exception as opt_e:
                                    logging.debug(f"[InitOptions] 注册期权失败: {opt_e}")
                        
                        if options_loaded > 0:
                            logging.info(f"[InitOptions] ✅ 从数据库预加载了 {options_loaded} 个期权合约")
                        else:
                            logging.warning("[InitOptions] No option prices found for preload")
                    except Exception as opt_preload_e:
                        logging.warning(f"[InitOptions] 期权预加载失败: {opt_preload_e}")
                
                if getattr(self, '_scheduler_manager', None) is not None:
                    self._add_option_status_diagnosis_job()
                    self._add_14_contracts_diagnosis_job()
                    logging.info("[AnalyticsInit] ✅ 期权5种状态诊断任务已重新注册（t_type_service就绪后）")
            except Exception as tte:
                logging.error(f"[AnalyticsInit] Failed to initialize t_type_service: {tte}", exc_info=True)
                self.t_type_service = None

            # ✅ _future_ids 和 _option_ids 简化为空集合
            # 实际使用时由 analytics_service 动态解析
            self._future_ids = set()
            self._option_ids = set()

            logging.info(
                "[AnalyticsInit] "
                f"analytics_service initialized, futures={len(self._future_ids)}, options={len(self._option_ids)}"
            )
        except Exception as e:
            logging.error(f"[StrategyCoreService._init_analytics_services] Error: {e}", exc_info=True)
            self.analytics_service = None

    def _start_analytics_warmup_async(self, params) -> None:
        """P1-1修复：将 analytics 初始化拆为异步后台 warmup，不阻塞 onInit 主链。
        
        设计约束（修改必看十原则）：
        1. warmup 线程为 daemon，stop/destroy 时不会因 warmup 卡住
        2. warmup 被中断后可恢复（_analytics_warmup_done 标志）
        3. warmup 期间 t_type_service 不可用是合法的，调用方必须检查 None
        4. 禁止后续将任何 preload/catalog/诊断注册重新塞回 onInit 主链
        5. 防重入：已有 warmup 线程在运行时跳过，避免 ERROR 恢复/restart 路径下并发 warmup
        """
        # 防重入：如果已有 warmup 线程在运行，跳过
        if self._analytics_warmup_thread and self._analytics_warmup_thread.is_alive():
            logging.info("[AnalyticsWarmup] warmup 线程已在运行，跳过重复启动")
            return
        
        def _warmup_worker():
            try:
                warmup_start = time.perf_counter()
                logging.info("[AnalyticsWarmup] 后台 warmup 开始...")
                self._init_analytics_services(params)
                self._analytics_warmup_done = True
                elapsed = time.perf_counter() - warmup_start
                logging.info(f"[AnalyticsWarmup] 后台 warmup 完成 (耗时={elapsed:.3f}s)")
            except Exception as e:
                logging.error(f"[AnalyticsWarmup] 后台 warmup 失败: {e}", exc_info=True)
                self._analytics_warmup_done = False

        self._analytics_warmup_thread = threading.Thread(
            target=_warmup_worker,
            name=f"analytics-warmup[strategy:{self.strategy_id}]",
            daemon=True,
        )
        self._analytics_warmup_thread.start()
        logging.info("[AnalyticsWarmup] analytics warmup 已调度到后台线程，onInit 不再阻塞")

    def _ensure_analytics_ready(self, timeout: float = 30.0) -> bool:
        """等待 analytics warmup 完成（可选），供需要 analytics 就绪的调用方使用。
        
        Args:
            timeout: 最大等待秒数
            
        Returns:
            True 表示 warmup 已完成，False 表示超时或失败
        """
        if self._analytics_warmup_done:
            return True
        if self._analytics_warmup_thread and self._analytics_warmup_thread.is_alive():
            self._analytics_warmup_thread.join(timeout=timeout)
        return self._analytics_warmup_done

    def _warm_storage_async(self) -> None:
        """后台预热 storage 服务（异步加载合约列表等）"""
        try:
            # 触发 storage 惰性初始化
            storage = self.storage
            if storage:
                logging.info("[WarmStorage] storage 后台预热完成")
            else:
                logging.warning("[WarmStorage] storage 未能初始化")
        except Exception as e:
            logging.warning(f"[WarmStorage] 后台预热失败：{e}")

    def save_state(self) -> bool:
        """保存策略状态到存储"""
        try:
            # 检查_storage属性是否存在且有save方法
            if not hasattr(self, '_storage') or not self._storage or not hasattr(self._storage, 'save'):
                logging.error("[save_state] Storage not available or missing save method")
                return False
                
            state_data = {
                'strategy_id': self.strategy_id,
                'state': self._state.value,
                'stats': self._stats,
                'saved_at': datetime.now().isoformat()
            }
            save_result = self._storage.save(f'strategy_state_{self.strategy_id}', state_data)
            if not save_result:
                raise RuntimeError("Storage save returned False")
            # 验证数据是否真的被保存
            loaded_data = self._storage.load(f'strategy_state_{self.strategy_id}')
            if not loaded_data:
                raise RuntimeError("Data verification failed: cannot load saved state")
            if loaded_data.get('strategy_id') != self.strategy_id:
                raise RuntimeError("Data verification failed: strategy_id mismatch")
            logging.info("[save_state] State saved and verified")
            return True
        except Exception as e:
            logging.error(f"[save_state] Failed: {e}", exc_info=True)
            return False
    
    def on_init(self, *args, **kwargs) -> bool:
        """策略初始化回调 - 对应旧 StrategyCore.on_init (L746)
            
        Args:
            *args: 可变参数
            **kwargs: 可变关键字参数
                
        Returns:
            bool: 初始化是否成功
        """
        with self._lock:
            # 幂等保护：已初始化则跳过（防止onInit+onStart场景下双重调用）
            if self._initialized:
                logging.info("[StrategyCoreService.on_init] Already initialized, skipping")
                return True
            
            # 允许从 INITIALIZING 或 ERROR 状态进行初始化
            if self._state not in (StrategyState.INITIALIZING, StrategyState.ERROR):
                logging.warning(f"[StrategyCoreService.on_init] Cannot initialize in state: {self._state}")
                return False
                
            try:
                # ✅ 确保日志级别为INFO（防止平台覆盖）
                root_logger = logging.getLogger()
                if root_logger.level > logging.INFO:
                    root_logger.setLevel(logging.INFO)
                    logging.info("[StrategyCoreService.on_init] 🔧 日志级别已修正为INFO")
                
                logging.info("[StrategyCoreService.on_init] Initializing...")
                init_started_at = time.perf_counter()
                    
                # ========== 步骤1: 确保品种表不为空（阻塞式，失败则终止） ==========
                from ali2026v3_trading.config_service import ensure_products_with_retry
                from ali2026v3_trading.data_service import get_data_service
                ds = get_data_service()
                logging.info("[Init-Step1] 加载品种配置...")
                try:
                    product_result = ensure_products_with_retry(ds)
                    logging.info(
                        f"[Init-Step1] ✅ 品种加载成功: "
                        f"期货新增={product_result['future_added']}(已有={product_result['future_existing']}), "
                        f"期权新增={product_result['option_added']}(已有={product_result['option_existing']})"
                    )
                except Exception as e:
                    logging.error(f"[Init-Step1] ❌ 品种加载失败: {e}")
                    raise RuntimeError(f"品种加载失败，策略无法继续初始化: {e}")
                # ============================================================
                    
                # 保存初始化参数，供后续使用
                self._init_kwargs = kwargs
                # ✅ 传递渠道唯一：self.params为平台运行时参数对象，get_params_service()为合约缓存单例，两者职责不同不冲突
                self.params = kwargs.get('params')
                    
                # 初始化日志
                self._init_logging(kwargs.get('params'))
                self._init_scheduler()

                # ========== K 线合成功能说明 (2026-03-28) ==========
                # K 线合成已集成在 storage.py 的 process_tick() 方法中
                # storage.py 使用 _KlineAggregator 自动处理多合约 K 线合成
                # 支持周期: 1min, 5min, 15min, 30min, 60min
                # 当外部 K 线缺失时自动保存合成的 K 线
                # 无需额外初始化 kline_generator
                # ===========================================================

                # _futures_instruments 和 _option_instruments 已删除
                logging.info("[Instruments] 使用 storage.py 原生订阅模式，无需预先生成合约列表")
                
                if self.params is not None:
                    if isinstance(self.params, dict):
                        self.params.setdefault('future_instruments', [])
                        self.params.setdefault('option_instruments', {})
                    else:
                        setattr(self.params, 'future_instruments', getattr(self.params, 'future_instruments', []))
                        setattr(self.params, 'option_instruments', getattr(self.params, 'option_instruments', {}))
                
                logging.info(
                    f"[Instruments] 使用 storage.py 原生模式 - "
                    f"合约数量由 storage 管理"
                )
                
                # P1-1修复：将 _init_analytics_services 从同步主链拆出，改为异步后台warmup
                # 目标：onInit 总耗时压到 5 秒以内，analytics warmup 不阻塞 execution ready
                self._analytics_warmup_done = False
                self._analytics_warmup_thread = None
                self._start_analytics_warmup_async(self.params)
                # ======================================
                
                # 更新状态：on_init完成后保持INITIALIZING，on_start才设RUNNING
                self._stats['start_time'] = datetime.now()
                
                logging.info(
                    "[StrategyCoreService.on_init] Initialized: %s (total=%.3fs)",
                    self.strategy_id,
                    time.perf_counter() - init_started_at,
                )
                
                # 发布事件
                self._publish_event('StrategyInitialized', {
                    'strategy_id': self.strategy_id,
                    'timestamp': datetime.now().isoformat()
                })
                
                self._initialized = True  # 幂等标志：标记初始化已完成
                return True
                
            except Exception as e:
                logging.error(f"[StrategyCoreService.on_init] Failed: {e}")
                self._state = StrategyState.ERROR
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now()
                self._stats['last_error_message'] = str(e)
                return False

    # 合约加载功能已迁移至 params_service.ParamsService
    # 使用: params_service.load_instrument_list(), cache_instrument_list() 等方法

    @staticmethod
    def _extract_contract_year_month(instrument_id: str) -> Optional[str]:
        """提取合约中的四位年月（yyMM），无法提取时返回 None。

        使用 SubscriptionManager.parse_option() 正确解析 SHFE 紧凑格式期权。
        """
        normalized = str(instrument_id or '').strip()
        if not normalized:
            return None

        from ali2026v3_trading.subscription_manager import SubscriptionManager

        if SubscriptionManager.is_option(instrument_id):
            try:
                parsed = SubscriptionManager.parse_option(instrument_id)
                return parsed.get('year_month')
            except (ValueError, KeyError):
                return None

        base = normalized.split('-', 1)[0]
        match = re.search(r'(\d{4})$', base)
        if not match:
            return None

        return match.group(1)

    # ========================================================================
    # 订单和持仓服务惰性初始化
    # ========================================================================

    def _ensure_order_service(self) -> None:
        """
        惰性初始化OrderService（使用全局单例）
        """
        if self._order_service is not None:
            return
        
        try:
            from ali2026v3_trading.order_service import get_order_service
            self._order_service = get_order_service()
            logging.info("[StrategyCoreService] OrderService initialized (singleton)")
        except Exception as e:
            logging.warning(f"[StrategyCoreService] Failed to initialize OrderService: {e}")
            self._order_service = None

    def _ensure_position_service(self) -> None:
        """
        惰性初始化PositionService（使用全局单例）
        """
        if self._position_service is not None:
            return
        
        try:
            from ali2026v3_trading.position_service import get_position_service
            self._position_service = get_position_service()
            logging.info("[StrategyCoreService] PositionService initialized (singleton)")
        except Exception as e:
            logging.warning(f"[StrategyCoreService] Failed to initialize PositionService: {e}")
            self._position_service = None

    # ========================================================================
    # Helper 方法簇 - 合约加载与处理（利用现有解析器）
    # ========================================================================

    def _load_instruments_from_param_cache(self, params: Any) -> Optional[Dict[str, Any]]:
        """
        从参数缓存加载合约列表
        
        Args:
            params: 参数对象
            
        Returns:
            Dict: {'futures_list': [...], 'options_dict': {...}} 或 None
        """
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_service = get_params_service()
            result = params_service.load_instrument_list(params, source='param_cache')
            if result:
                logging.info(f"[Helper] 从参数缓存加载: {len(result['futures_list'])} 期货, {sum(len(v) for v in result['options_dict'].values())} 期权")
            return result
        except Exception as e:
            logging.warning(f"[Helper._load_instruments_from_param_cache] Error: {e}")
            return None

    def _cache_instruments_to_params(self, params: Any, futures_list: List[str],
                                     options_dict: Dict[str, List[str]], source: str) -> None:
        """
        将合约列表缓存到参数对象
        
        Args:
            params: 参数对象
            futures_list: 期货合约列表
            options_dict: 期权合约字典
            source: 缓存来源标识
        """
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_service = get_params_service()
            params_service.cache_instrument_list(params, futures_list, options_dict, source)
            logging.debug(f"[Helper._cache_instruments_to_params] Cached to {source}")
        except Exception as e:
            logging.warning(f"[Helper._cache_instruments_to_params] Error: {e}")

    def _load_instruments_from_output_files(self) -> Optional[Dict[str, Any]]:
        """
        从输出文件加载合约列表
        
        Returns:
            Dict: {'futures_list': [...], 'options_dict': {...}} 或 None
        """
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_service = get_params_service()
            # 创建一个临时的空params对象用于加载文件
            class TempParams:
                future_instruments = []
                option_instruments = {}
            
            temp_params = TempParams()
            result = params_service.load_instrument_list(temp_params, source='output_files')
            if result:
                logging.info(f"[Helper] 从输出文件加载: {len(result['futures_list'])} 期货, {sum(len(v) for v in result['options_dict'].values())} 期权")
            return result
        except Exception as e:
            logging.warning(f"[Helper._load_instruments_from_output_files] Error: {e}")
            return None

    def _normalize_cached_futures(self, futures_list: List[str]) -> List[str]:
        """
        规范化期货合约列表
        
        Args:
            futures_list: 原始期货合约列表
            
        Returns:
            List[str]: 规范化后的期货合约列表（去重、验证格式）
        """
        normalized = []
        seen = set()
        
        for contract in futures_list:
            try:
                original_id = str(contract or '').strip()
                parsed = SubscriptionManager.parse_future(contract)
                canonical_key = f"{parsed['product']}{parsed['year_month']}"
                if canonical_key not in seen:
                    normalized.append(original_id)
                    seen.add(canonical_key)
                try:
                    from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                    DiagnosisProbeManager.on_parse_transform(
                        'core.normalize_future_cache',
                        original_id,
                        canonical_key,
                        detail='dedupe_by_canonical_key,preserve_source_id',
                        level='DEBUG',
                    )
                except Exception:
                    pass
            except (ValueError, Exception) as e:
                logging.warning(f"[Helper] 跳过无效期货合约: {contract}, error: {e}")
                try:
                    from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                    DiagnosisProbeManager.on_parse_failure('core.normalize_future_cache', str(contract or '').strip(), str(e))
                except Exception:
                    pass
        
        logging.debug(f"[Helper._normalize_cached_futures] {len(futures_list)} -> {len(normalized)}")
        return normalized

    def _normalize_cached_options(self, options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        规范化期权合约字典
        
        Args:
            options_dict: 原始期权合约字典 {product: [contracts]}
            
        Returns:
            Dict[str, List[str]]: 规范化后的期权合约字典
        """
        normalized = {}
        
        for product, contracts in options_dict.items():
            normalized_contracts = []
            seen = set()
            
            for contract in contracts:
                try:
                    original_id = str(contract or '').strip()
                    parsed = SubscriptionManager.parse_option(contract)
                    canonical_key = f"{parsed['product']}{parsed['year_month']}-{parsed['option_type']}-{int(parsed['strike_price'])}"
                    if canonical_key not in seen:
                        normalized_contracts.append(original_id)
                        seen.add(canonical_key)
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_transform(
                            'core.normalize_option_cache',
                            original_id,
                            canonical_key,
                            detail=f"source_format={parsed.get('format')},preserve_source_id",
                            level='INFO' if original_id != canonical_key else 'DEBUG',
                        )
                    except Exception:
                        pass
                except (ValueError, Exception) as e:
                    logging.warning(f"[Helper] 跳过无效期权合约: {contract}, error: {e}")
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_failure('core.normalize_option_cache', str(contract or '').strip(), str(e))
                    except Exception:
                        pass
            
            if normalized_contracts:
                normalized[product] = normalized_contracts
        
        total_before = sum(len(v) for v in options_dict.values())
        total_after = sum(len(v) for v in normalized.values())
        logging.debug(f"[Helper._normalize_cached_options] {total_before} -> {total_after}")
        return normalized

    def _count_option_contracts(self, options_dict: Dict[str, List[str]]) -> int:
        """
        统计期权合约总数
        
        Args:
            options_dict: 期权合约字典
            
        Returns:
            int: 期权合约总数
        """
        return sum(len(contracts) for contracts in options_dict.values())

    def _derive_underlying_futures_from_options(self, options_dict: Dict[str, List[str]]) -> List[str]:
        """
        从期权字典推导标的期货列表
        
        Args:
            options_dict: 期权合约字典 {product: [contracts]}
            
        Returns:
            List[str]: 标的期货列表（去重）
        """
        underlying_set = set()
        
        for product, contracts in options_dict.items():
            for contract in contracts:
                try:
                    # ✅ ID直通：从DB查询标的期货instrument_id，不自行构造
                    parsed = SubscriptionManager.parse_option(contract)
                    from ali2026v3_trading.data_service import get_data_service
                    rows = get_data_service().query(
                        "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                        [parsed['product'], parsed['year_month']]
                    ).to_pylist()
                    if rows:
                        underlying = rows[0]['instrument_id']
                    else:
                        underlying = f"{parsed['product']}{parsed['year_month']}"
                        logging.debug(f"[Helper] 标的期货未注册，使用解析值: {underlying}")
                    underlying_set.add(underlying)
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_transform(
                            'core.derive_underlying_future',
                            str(contract or '').strip(),
                            underlying,
                            detail=f"option_format={parsed.get('format')}",
                            level='INFO',
                        )
                    except Exception:
                        pass
                except Exception as e:
                    logging.warning(f"[Helper] 无法提取标的期货: {contract}, error: {e}")
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_failure('core.derive_underlying_future', str(contract or '').strip(), str(e))
                    except Exception:
                        pass
        
        result = sorted(underlying_set)
        if result:
            logging.debug(f"[Helper._derive_underlying_futures_from_options] Derived {len(result)} underlying futures: {result[:5]}...")
        return result

    def on_start(self) -> bool:
        """策略启动回调 - 对应旧 StrategyCore.on_start (L946)
        
        Returns:
            bool: 启动是否成功
        """
        # ✅ 诊断日志配置状态
        root_logger = logging.getLogger()
        logging.info(
            f"[StrategyCoreService.on_start] 📊 日志配置诊断: "
            f"Level={logging.getLevelName(root_logger.level)}, "
            f"Handlers={len(root_logger.handlers)}"
        )
        
        logging.info("[StrategyCoreService.on_start] ========== START ==========")
        
        # 预热 DataService（确保 RealTimeCache 可用）
        try:
            from ali2026v3_trading.data_service import get_data_service
            ds = get_data_service()
            logging.info(f"[StrategyCoreService.on_start] DataService预热完成: {ds is not None}")
        except Exception as ds_e:
            logging.warning(f"[StrategyCoreService.on_start] DataService预热失败: {ds_e}")
        
        with self._lock:
            if self._state not in (StrategyState.INITIALIZING, StrategyState.RUNNING, StrategyState.PAUSED, StrategyState.DEGRADED):
                logging.warning(f"[StrategyCoreService.on_start] Cannot start in state: {self._state}")
                return False
            
            if self._is_paused:
                self._is_paused = False
            
            self._is_running = True
            self._state = StrategyState.RUNNING
            
            logging.info(f"[StrategyCoreService.on_start] Started: {self.strategy_id}")
                        
            # ========== ✅ 新增：完整订阅流程 (2026-03-28) ==========
            # 从配置文件读取品种，调用 storage 查询接口获取合约，完成订阅
            # 不依赖 platform 的 instrument_list，直接从配置驱动
            # ============================================================
                        
            # P0修复：从 _runtime_strategy_host 获取 params，而不是 self.params
            # StrategyCoreService 实例没有 params 属性，必须从 Strategy2026 实例获取
            params = None
            if hasattr(self, '_runtime_strategy_host') and self._runtime_strategy_host:
                params = getattr(self._runtime_strategy_host, 'params', None)
            
            if params is None:
                logging.warning("[Subscribe] ⚠️ 无法获取 params 对象，跳过订阅")
                return True
                        
            # 1. 读取配置
            future_products_str = get_param_value(params, 'future_products', '')
            option_products_str = get_param_value(params, 'option_products', '')
                        
            # 2. 解析品种列表
            future_products = [p.strip() for p in future_products_str.split(',') if p.strip()]
            option_products = [p.strip() for p in option_products_str.split(',') if p.strip()]
                        
            logging.info(f"[Subscribe] 配置品种：期货={len(future_products)} 个，期权={len(option_products)} 个")
            if future_products:
                logging.debug(f"[Subscribe] 期货品种：{future_products[:5]}...")
            if option_products:
                logging.debug(f"[Subscribe] 期权品种：{option_products[:5]}...")
                        
            cached_result = self._load_instruments_from_param_cache(params)
            should_prefer_output_files = bool(get_param_value(params, 'load_all_products', False)) or \
                str(get_param_value(params, 'run_profile', '')) == 'full'

            selected_futures_list: List[str] = []
            selected_options_dict: Dict[str, List[str]] = {}
            selected_source = 'none'

            if cached_result:
                # ✅ 规范化期货和期权合约
                selected_futures_list = self._normalize_cached_futures(cached_result['futures_list'])
                selected_options_dict = self._normalize_cached_options(cached_result['options_dict'])
                selected_source = 'params_cache'
                logging.info(f"[Subscribe] 从参数缓存加载并规范化: {len(selected_futures_list)} 期货, {self._count_option_contracts(selected_options_dict)} 期权")
            elif should_prefer_output_files:
                file_result = self._load_instruments_from_output_files()
                if file_result:
                    # ✅ 规范化期货和期权合约
                    selected_futures_list = self._normalize_cached_futures(file_result['futures_list'])
                    selected_options_dict = self._normalize_cached_options(file_result['options_dict'])
                    selected_source = 'output_files'
                    logging.info(f"[Subscribe] 从输出文件加载并规范化: {len(selected_futures_list)} 期货, {self._count_option_contracts(selected_options_dict)} 期权")
                    self._cache_instruments_to_params(
                        params,
                        selected_futures_list,
                        selected_options_dict,
                        selected_source,
                    )

            derived_futures = self._derive_underlying_futures_from_options(selected_options_dict)
            if derived_futures:
                existing_futures = set(selected_futures_list)
                added_futures = [item for item in derived_futures if item not in existing_futures]
                if added_futures:
                    selected_futures_list.extend(added_futures)
                    logging.info("[Subscribe] 从期权清单补齐 %d 个标的期货", len(added_futures))

            # 3. 调用 storage 查询接口获取合约（最后回退）
            if not selected_futures_list and not selected_options_dict and self.storage and (future_products or option_products):
                try:
                    all_instrument_ids = []
                    selected_source = 'storage'
                                
                    # 获取期货合约
                    if future_products:
                        futures_from_storage = self.storage.get_active_instruments_by_products(future_products)
                        all_instrument_ids.extend(futures_from_storage)
                        logging.info(f"[Subscribe] 从 storage 获取到 {len(futures_from_storage)} 个期货合约")
                                
                    # 获取期权合约
                    if option_products:
                        options_from_storage = self.storage.get_active_instruments_by_products(option_products)
                        all_instrument_ids.extend(options_from_storage)
                        logging.info(f"[Subscribe] 从 storage 获取到 {len(options_from_storage)} 个期权合约")
                                
                    # 4. 分类
                    if all_instrument_ids:
                        from ali2026v3_trading.query_service import get_query_service
                        qs = get_query_service()
                        futures_list, options_dict = qs.classify_instruments(all_instrument_ids)
                        
                        # ✅ 规范化期货和期权合约
                        selected_futures_list = self._normalize_cached_futures(futures_list)
                        selected_options_dict = self._normalize_cached_options(options_dict)
                        
                        total_options = self._count_option_contracts(selected_options_dict)
                        logging.info(f"[Subscribe] 从 storage 加载并规范化: {len(selected_futures_list)} 期货，{total_options} 期权")
                except Exception as e:
                    logging.error(f"[Subscribe] 失败：{e}")
                    logging.exception("[Subscribe] Stack:")

            if selected_futures_list or selected_options_dict:
                # _futures_instruments 和 _option_instruments 已删除
                subscribe_list = list(selected_futures_list)
                for option_ids in selected_options_dict.values():
                    subscribe_list.extend(option_ids or [])

                seen = set()
                self._subscribed_instruments = [
                    inst for inst in subscribe_list if not (inst in seen or seen.add(inst))
                ]
                # P0修复：e2e段1 - 配置合约数
                self._e2e_counters['configured_instruments'] = len(self._subscribed_instruments)

                # P1-2修复：预注册拆到后台任务，不阻塞 onStart 主链
                # 目标：onStart 总耗时预算 < 3s，预注册/历史补齐异步补完
                if self.storage and self._subscribed_instruments:
                    self._start_preregister_async(self._subscribed_instruments)

                # 5. 调用 SubscriptionManager 统一订阅
                # 显式确保 storage 已初始化
                try:
                    _ = self.storage  # 触发初始化
                    logging.info(f"[Subscribe] storage 已就绪: {self.storage is not None}")
                except Exception as storage_e:
                    logging.error(f"[Subscribe] storage 初始化失败: {storage_e}")
                
                if self.storage and hasattr(self.storage, 'subscription_manager'):
                    db_count = self.storage.subscription_manager.subscribe_all_instruments(
                        selected_futures_list,
                        selected_options_dict,
                    )
                    logging.info(f"[Subscribe] ✅ 数据库登记完成：{db_count} 个合约")
                    self._e2e_counters['preregistered_instruments'] = db_count  # P0: 六段计数器段2
                    
                    # 断点A+B修复：平台订阅不可用时重试+DEGRADED
                    if callable(self.subscribe):
                        self._start_platform_subscribe_async(self._subscribed_instruments)
                        logging.info("[Subscribe] 🚀 平台订阅已在后台启动")
                        self._e2e_counters['platform_subscribe_called'] = len(self._subscribed_instruments)  # P0: 六段计数器段3
                    else:
                        logging.warning("[Subscribe] ⚠️ self.subscribe 不可调用，平台API未就绪，安排延迟重试")
                        with self._state_lock:
                            self._state = StrategyState.DEGRADED
                        def _retry_platform_subscribe():
                            for attempt in range(1, 4):  # 最多重试3次：5s, 10s, 15s
                                time.sleep(5.0 * attempt)
                                # 先尝试重新绑定平台API（根因可能是bind未成功）
                                if not getattr(self, '_api_ready', False) and hasattr(self, '_runtime_strategy_host') and self._runtime_strategy_host:
                                    try:
                                        self.bind_platform_apis(self._runtime_strategy_host)
                                        logging.info("[Subscribe] 🔄 延迟重试bind_platform_apis成功（第%d次）", attempt)
                                    except Exception as bind_e:
                                        logging.warning("[Subscribe] ⚠️ 延迟重试bind_platform_apis失败: %s", bind_e)
                                if callable(self.subscribe):
                                    self._start_platform_subscribe_async(self._subscribed_instruments)
                                    self._e2e_counters['platform_subscribe_called'] = len(self._subscribed_instruments)
                                    logging.info("[Subscribe] 🔄 延迟重试平台订阅成功（第%d次）", attempt)
                                    with self._state_lock:
                                        if self._state == StrategyState.DEGRADED:
                                            self._state = StrategyState.RUNNING
                                    return
                                logging.warning("[Subscribe] ⚠️ 第%d次重试失败，API仍未就绪", attempt)
                            logging.error("[Subscribe] ❌ 平台API经3次重试始终未就绪，策略保持DEGRADED状态运行")
                        threading.Thread(
                            target=_retry_platform_subscribe, 
                            name=f"subscribe-retry[strategy:{self.strategy_id}]",
                            daemon=True
                        ).start()
                    
                    # ✅ 同步分表数据到 ticks_raw（用于状态计算）
                    # 注释掉初始同步，避免阻塞启动流程
                    # 定时任务会每分钟增量同步，无需启动时全量同步
                    logging.info(f"[SyncTicks] ⏭️ 跳过初始全量同步，依赖定时任务增量同步")
                    # try:
                    #     from ali2026v3_trading.data_service import DataService
                    #     ds = DataService()
                    #     storage_db_path = getattr(self.storage, 'db_path', None) if hasattr(self, 'storage') and self.storage else None
                    #     synced_count = ds.sync_tick_tables_to_ticks_raw(storage_db_path=storage_db_path)
                    #     logging.info(f"[SyncTicks] ✅ Synced {synced_count:,} tick records to ticks_raw")
                    # except Exception as e:
                    #     logging.error(f"[SyncTicks] Failed to sync tick tables: {e}", exc_info=True)
                else:
                    logging.warning("[Subscribe] 无 subscription_manager")
            else:
                self._subscribed_instruments = []
                logging.warning("[Subscribe] 新三步链路未获取到任何合约，跳过订阅")

            # P1-2修复：历史K线加载拆到后台任务，不阻塞 onStart 主链
            logging.info("[StrategyCoreService.on_start] 历史K线加载将异步执行...")
            self._start_historical_kline_load_async()
            
            # 发布事件
            self._publish_event('StrategyStarted', {
                'strategy_id': self.strategy_id
            })
            
            # ✅ P2: 启动完成后输出资源所有权表
            self._log_resource_ownership_table(phase='start')
            
            return True
    
    def _start_preregister_async(self, instrument_ids: List[str]) -> None:
        """P1-2修复：预注册拆到后台任务，不阻塞 onStart 主链。
        
        设计约束（修改必看十原则）：
        1. 预注册是 non-critical 操作，允许延迟完成
        2. daemon 线程确保 stop/destroy 不会因预注册卡住
        3. 预注册失败不影响策略运行态（合约会在 tick 到达时惰性注册）
        4. 后续任何"为了启动成功顺便做"的动作都必须标注 critical/non-critical
        """
        def _preregister_worker():
            try:
                prereg_start = time.perf_counter()
                logging.info("[PreRegisterAsync] 后台预注册开始: %d 个合约", len(instrument_ids))
                prereg_stats = self.storage.ensure_registered_instruments(instrument_ids)
                self._last_preregister_stats = prereg_stats
                elapsed = time.perf_counter() - prereg_start
                logging.info(
                    "[PreRegisterAsync] 后台预注册完成 (耗时=%.3fs): "
                    "配置=%d, 已注册=%d, 缺失=%d, 新建=%d, 失败=%d",
                    elapsed,
                    prereg_stats['configured_count'],
                    prereg_stats['registered_count'],
                    prereg_stats['missing_count'],
                    prereg_stats['created_count'],
                    prereg_stats['failed_count'],
                )
            except Exception as e:
                logging.error(f"[PreRegisterAsync] 后台预注册失败: {e}", exc_info=True)

        threading.Thread(
            target=_preregister_worker, 
            name=f"preregister-async[strategy:{self.strategy_id}]",
            daemon=True
        ).start()
        logging.info("[PreRegisterAsync] 预注册已调度到后台线程，onStart 不再阻塞")

    def _start_historical_kline_load_async(self) -> None:
        """P1-2修复：历史K线加载拆到后台任务，不阻塞 onStart 主链。
        
        设计约束（修改必看十原则）：
        1. 历史K线加载是 non-critical 操作，允许延迟完成
        2. daemon 线程确保 stop/destroy 不会因K线加载卡住
        3. K线加载期间 stop barrier 可中断
        """
        def _kline_worker():
            try:
                logging.info("[KlineLoadAsync] 后台历史K线加载开始...")
                self._start_historical_kline_load()
                logging.info("[KlineLoadAsync] 后台历史K线加载完成")
            except Exception as e:
                logging.error(f"[KlineLoadAsync] 后台历史K线加载失败: {e}", exc_info=True)

        threading.Thread(
            target=_kline_worker, 
            name=f"kline-load-async[strategy:{self.strategy_id}]",
            daemon=True
        ).start()
        logging.info("[KlineLoadAsync] 历史K线加载已调度到后台线程，onStart 不再阻塞")

    def _unsubscribe_all_instruments(self) -> None:
        """停止时取消全部已订阅合约，避免平台继续向本实例推送回调。"""
        try:
            self._platform_subscribe_stop.set()
            if not callable(self.unsubscribe):
                return

            subscribed = list(getattr(self, '_subscribed_instruments', []) or [])
            if not subscribed:
                return

            success_count = 0
            failed_count = 0
            for inst in subscribed:
                try:
                    self.unsubscribe(inst)
                    success_count += 1
                except Exception:
                    failed_count += 1

            logging.info(
                f"[Unsubscribe] Summary: total={len(subscribed)}, success={success_count}, failed={failed_count}"
            )
        except Exception as e:
            logging.error(f"[StrategyCoreService._unsubscribe_all_instruments] Error: {e}", exc_info=True)

    def _shutdown_runtime_services(self) -> None:
        """停止运行时后台服务，避免卸载后仍有后台输出。"""
        # 1. 先停止历史K线加载（阻止新任务入队）
        self._shutdown_historical_services()
        
        # 2. 再停止 storage 异步写入线程（清空队列余下任务）
        if self._storage is not None and hasattr(self._storage, '_stop_async_writer'):
            try:
                self._storage._stop_async_writer()
            except Exception as e:
                logging.warning(f"[StrategyCoreService] Storage async writer stop error: {e}")
    
    def on_stop(self) -> bool:
        """策略停止回调 - 接口唯一修复：内部保证save_state+停止逻辑
        
        ✅ P0-2: 两阶段停止 - 先冻结新任务，再等待归零，最后shutdown
        """
        run_id = getattr(self, '_lifecycle_run_id', 'N/A')
        with self._lock:
            if self._state == StrategyState.STOPPED:
                logging.warning(
                    f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Already stopped"
                )
                return True
            
            logging.info(
                f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"Stopping"
            )

            # ✅ P0-2: Phase 1 - 拒绝新任务
            self._is_running = False
            self._is_paused = True
        
        # ✅ P0-2: Phase 2 - 冻结scheduler + 等待job归零（锁外执行）
        jobs_zero = True
        try:
            # 先冻结scheduler
            if hasattr(self._scheduler_manager, 'pause_scheduler'):
                self._scheduler_manager.pause_scheduler()
            
            # 根因2修复：主动卸载该策略注册的所有job，消除时间窗口
            if hasattr(self._scheduler_manager, 'remove_jobs_by_owner'):
                removed = self._scheduler_manager.remove_jobs_by_owner(self.strategy_id)
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Removed {removed} strategy jobs"
                )
            
            # 等待job归零
            if hasattr(self._scheduler_manager, 'wait_for_jobs_zero'):
                jobs_zero = self._scheduler_manager.wait_for_jobs_zero(timeout=10.0)
                if not jobs_zero:
                    logging.warning(
                        f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                        f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                        f"⚠️ Jobs not zero after 10s, entering DEGRADED_STOP"
                    )
        except Exception as e:
            logging.error(
                f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"Phase 2 error: {e}"
            )
            jobs_zero = False
        
        # ✅ P0-2: Phase 3 - 完成shutdown
        try:
            self._stop_scheduler()
        except Exception as e:
            logging.error(
                f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"_stop_scheduler error: {e}"
            )

        try:
            self._unsubscribe_all_instruments()
        except Exception as e:
            logging.error(
                f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"_unsubscribe error: {e}"
            )

        try:
            self._shutdown_runtime_services()
        except Exception as e:
            logging.error(
                f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"_shutdown_runtime error: {e}"
            )

        if hasattr(self, '_flush_tick_buffer'):
            try:
                self._flush_tick_buffer()
            except Exception as e:
                logging.error(
                    f"[on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Failed to flush tick buffer: {e}"
                )

        # ✅ P0-2: 根据归零结果设置状态
        with self._lock:
            if jobs_zero:
                self._state = StrategyState.STOPPED
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"✅ Stopped"
                )
            else:
                self._state = StrategyState.DEGRADED_STOP
                logging.warning(
                    f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"⚠️ DEGRADED_STOP (jobs not zero)"
                )
        
        try:
            self._publish_event('StrategyStopped', {
                'strategy_id': self.strategy_id,
                'state': self._state.value
            })
        except Exception as e:
            logging.error(
                f"[on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"Failed to publish event: {e}"
            )
        
        # ✅ P2: stop后资源所有权扫描与断言
        self._log_resource_ownership_table(phase='stop')
        
        # 接口唯一修复：save_state在on_stop内部完成，确保任何入口都保存状态
        try:
            self.save_state()
        except Exception as e:
            logging.warning(f"[on_stop] save_state failed: {e}")
        
        return True

    def _log_resource_ownership_table(self, phase: str = 'unknown') -> None:
        """✅ P2: 输出资源所有权表，扫描线程列表并断言strategy-instance级线程已消失"""
        import threading as _threading
        
        strategy_id = getattr(self, 'strategy_id', 'unknown')
        run_id = getattr(self, '_lifecycle_run_id', 'N/A')
        
        ALLOWED_PREFIXES = (
            'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
            'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
            'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
            'TTypeService-Preload[shared-service]', 'onStop-worker',
        )
        
        threads = _threading.enumerate()
        strategy_threads = []
        shared_threads = []
        system_threads = []
        
        for t in threads:
            name = t.name or ''
            if '[shared-service]' in name:
                shared_threads.append(name)
            elif any(name.startswith(p) for p in ALLOWED_PREFIXES):
                system_threads.append(name)
            elif 'strategy' in name.lower() or strategy_id in name:
                strategy_threads.append(name)
            elif name and not name.startswith('Main'):
                system_threads.append(name)
        
        logging.info(
            f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
            f"[source_type=resource-ownership] "
            f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
            f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
        )
        
        if shared_threads:
            for name in shared_threads:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"Thread alive: {name} (expected: continues after strategy stop)"
                )
        
        if strategy_threads:
            for name in strategy_threads:
                logging.warning(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"⚠️ LEAKED thread: {name} (expected: should be gone after strategy stop)"
                )
        else:
            if phase == 'stop':
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"✅ No strategy-instance threads leaked"
                )
        
        # ✅ P2-2: 扩展诊断 - scheduler jobs, storage queue, event_bus pending events
        # 1. Scheduler jobs
        scheduler_mgr = getattr(self, '_scheduler_manager', None)
        if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
            try:
                remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
                if remaining_jobs:
                    job_ids = [j['job_id'] for j in remaining_jobs]
                    logging.warning(
                        f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=strategy-job] "
                        f"⚠️ LEAKED scheduler jobs: {job_ids}"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=strategy-job] "
                            f"✅ No strategy-instance scheduler jobs leaked"
                        )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Scheduler diagnosis error: {e}"
                )
        
        # 2. Storage queue stats
        storage = getattr(self, '_storage', None)
        if storage and hasattr(storage, 'get_queue_stats'):
            try:
                qstats = storage.get_queue_stats()
                qsize = qstats.get('current_queue_size', 0)
                if qsize > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"Storage queue backlog: {qsize} tasks (expected: drain continues)"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=shared-queue-drain] "
                            f"✅ Storage queue empty"
                        )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Storage queue diagnosis error: {e}"
                )
        
        # 3. Event bus pending callbacks
        event_bus = getattr(self, '_event_bus', None)
        if event_bus and hasattr(event_bus, '_pending_events'):
            try:
                pending = getattr(event_bus, '_pending_events', 0)
                if pending > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"EventBus pending callbacks: {pending} (expected: drain in progress)"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=event-tail] "
                            f"✅ EventBus pending callbacks empty"
                        )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] EventBus diagnosis error: {e}"
                )
    
    def prepare_restart(self) -> bool:
        """将已停止的策略重新置回可初始化状态。"""
        run_id = getattr(self, '_lifecycle_run_id', 'N/A')
        with self._lock:
            if self._destroyed:
                logging.warning(
                    f"[StrategyCoreService.prepare_restart][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Cannot restart destroyed strategy"
                )
                return False

            if self._state != StrategyState.STOPPED:
                logging.info(
                    f"[StrategyCoreService.prepare_restart][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Skip restart prep in state: {self._state}"
                )
                return self._state in (StrategyState.INITIALIZING, StrategyState.RUNNING, StrategyState.PAUSED)

            self._state = StrategyState.INITIALIZING
            self._is_running = False
            self._is_paused = False
            self._initialized = False  # 重置幂等标志，允许重新初始化
            self._analytics_warmup_done = False  # P1-1: 重置 warmup 状态，允许重新 warmup
            self._analytics_warmup_thread = None  # P1-1: 清除旧 warmup 线程引用
            
            # 重置历史K线状态以支持重启
            self._reset_historical_state_for_restart()
            
            self._platform_subscribe_thread = None
            self._platform_subscribe_stop.clear()

            logging.info(
                f"[StrategyCoreService.prepare_restart][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"Rearmed from STOPPED (rearm_reason=user-requested)"
            )
            return True
    
    @staticmethod
    def _should_probe_t_type_future(product: str) -> bool:
        return str(product or '') in {'AL', 'IH'}

    def _log_t_type_future_probe(self, phase: str, instrument_id: str, product: str, month: str, last_price: float) -> None:
        if not self._should_probe_t_type_future(product):
            return

        logging.info(
            "[TTypeFutureProbe] phase=%s instrument=%s product=%s month=%s price=%s",
            phase,
            instrument_id,
            product,
            month,
            last_price,
        )

    def on_order(self, order_data: Any, *args, **kwargs) -> None:
        try:
            oid = getattr(order_data, "order_id", "") or getattr(order_data, "OrderRef", "")
            sts = getattr(order_data, "status", "") or getattr(order_data, "OrderStatus", "")
            logging.info("[StrategyCoreService.on_order] OrderID=%s, Status=%s", oid, sts)
            if self._order_service:
                self._order_service.on_trade_update(order_data)
        except Exception as e:
            logging.error("[StrategyCoreService.on_order] Error: %s", e)

    def on_order_trade(self, trade_data: Any, *args, **kwargs) -> None:
        try:
            logging.info("[StrategyCoreService.on_order_trade] Trade executed")
            self._update_position_from_trade(trade_data)
        except Exception as e:
            logging.error("[StrategyCoreService.on_order_trade] Error: %s", e)

    def on_trade(self, trade_data: Any, *args, **kwargs) -> None:
        try:
            self._update_position_from_trade(trade_data)
        except Exception as e:
            logging.error("[StrategyCoreService.on_trade] Error: %s", e)

    def _update_position_from_trade(self, trade_data: Any) -> None:
        try:
            from ali2026v3_trading.position_service import get_position_service
            pos_svc = get_position_service()
            pos_svc.on_trade(trade_data)
        except Exception as e:
            logging.error("[StrategyCoreService._update_position_from_trade] Error: %s", e)

    def execute_option_trading_cycle(self) -> None:
        if not self._is_running or self._is_paused:
            return
        try:
            t_type = getattr(self, 't_type_service', None)
            if not t_type:
                return
            targets = t_type.select_otm_targets_by_volume()
            if not targets:
                return
            now = time.time()
            filtered = []
            for t in targets:
                iid = t.get('instrument_id', '')
                self._ensure_position_service()
                if self._position_service and hasattr(self._position_service, 'has_position'):
                    if self._position_service.has_position(iid):
                        continue
                filtered.append(t)
            if not filtered:
                return
            self._ensure_order_service()
            if self._order_service:
                order_ids = self._order_service.execute_by_ranking(filtered)
                if order_ids:
                    logging.info("[OptionTrading] 下单完成: %d 笔", len(order_ids))
        except Exception as e:
            logging.error("[StrategyCoreService.execute_option_trading_cycle] Error: %s", e)

    def check_position_risk(self) -> None:
        if not self._is_running:
            return
        try:
            self._ensure_position_service()
            if self._position_service:
                self._position_service.check_all_positions()
        except Exception as e:
            logging.error("[StrategyCoreService.check_position_risk] Error: %s", e)
    
    def on_destroy(self) -> None:
        """策略销毁"""
        try:
            self.destroy()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_destroy] Error: {e}", exc_info=True)
    
    def _is_instrument_allowed(self, instrument_id: str, exchange: str = "") -> bool:
        """检查合约是否允许"""
        return True
    
    def _is_backtest_context(self) -> bool:
        """检查是否为回测环境"""
        return False
    
    def _is_trade_context(self) -> bool:
        """检查是否为交易环境"""
        try:
            return str(getattr(self.params, "run_mode", "")) in ["trade", "paper"]
        except Exception as e:
            return False
    
    # ========== 诊断功能已移至 diagnosis_service.py ==========
    # 使用方式:
    #   from ali2026v3_trading.diagnosis_service import StrategyDiagnoser
    #   diagnoser = StrategyDiagnoser(strategy_instance=self)
    #   report = diagnoser.generate_full_link_diagnosis()
    # =======================================================
    
    def initialize(self, params: Optional[Dict[str, Any]] = None) -> bool:
        """接口唯一修复：initialize为唯一初始化入口，on_init为钩子回调"""
        return self.on_init(params=params)
    
    def start(self) -> bool:
        """接口唯一修复：start为唯一启动入口，on_start为钩子回调"""
        return self.on_start()
    
    def pause(self) -> bool:
        """暂停策略
        
        Returns:
            bool: 暂停是否成功
        """
        with self._lock:
            if self._state != StrategyState.RUNNING:
                logging.warning(f"[StrategyCoreService] Cannot pause in state: {self._state}")
                return False
            
            self._is_paused = True
            self._state = StrategyState.PAUSED
            
            logging.info(f"[StrategyCoreService] Paused: {self.strategy_id}")
            
            # 发布事件
            self._publish_event('StrategyPaused', {
                'strategy_id': self.strategy_id
            })
            
            return True
    
    def resume(self) -> bool:
        """恢复策略
        
        Returns:
            bool: 恢复是否成功
        """
        with self._lock:
            if self._state != StrategyState.PAUSED:
                logging.warning(f"[StrategyCoreService] Cannot resume in state: {self._state}")
                return False
            
            self._is_paused = False
            self._state = StrategyState.RUNNING
            
            logging.info(f"[StrategyCoreService] Resumed: {self.strategy_id}")
            
            # 发布事件
            self._publish_event('StrategyResumed', {
                'strategy_id': self.strategy_id
            })
            
            return True
    
    def stop(self) -> bool:
        """接口唯一修复：stop为唯一停止入口，内部保证save_state+on_stop"""
        return self.on_stop()
    
    def destroy(self) -> bool:
        """销毁策略
        
        Returns:
            bool: 销毁是否成功
        """
        run_id = getattr(self, '_lifecycle_run_id', 'N/A')
        with self._lock:
            if self._destroyed:
                logging.info(
                    f"[StrategyCoreService.destroy][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Already destroyed"
                )
                return True
            
            try:
                logging.info(
                    f"[StrategyCoreService.destroy][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Destroying"
                )
                
                # 1. 先停止（统一走 on_stop 主链，确保取消订阅和后台服务关闭）
                self.on_stop()
                
                # 2. 清理调度器引用
                self._scheduler = None
                
                # 3. 清理事件总线引用
                self._event_bus = None
                
                # 4. 标记为已销毁
                self._destroyed = True
                
                # 5. 清空统计数据
                self._stats = {
                    'start_time': None,
                    'total_ticks': 0,
                    'total_trades': 0,
                    'total_signals': 0,
                    'errors_count': 0,
                    'last_error_time': None,
                    'last_error_message': None,
                    # 详细分类统计
                    'tick_by_type': {'future': 0, 'option': 0},
                    'tick_by_exchange': {},
                    'tick_by_instrument': {},
                    'kline_stats': {
                        'total_requested': 0,
                        'success': 0,
                        'failed': 0,
                        'total_klines_loaded': 0,
                        'by_instrument': {}
                    }
                }
                
                logging.info(
                    f"[StrategyCoreService.destroy][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Destroyed"
                )
                
                # 发布事件
                self._publish_event('StrategyDestroyed', {
                    'strategy_id': self.strategy_id
                })
                
                return True
                
            except Exception as e:
                logging.error(
                    f"[StrategyCoreService.destroy][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Failed: {e}"
                )
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now()
                self._stats['last_error_message'] = str(e)
                return False
    
    # ========== 状态查询 ==========
    
    def get_state(self) -> StrategyState:
        """获取当前状态
        
        Returns:
            StrategyState: 当前状态
        """
        return self._state
    
    def is_running(self) -> bool:
        """检查是否运行中
        
        Returns:
            bool: 是否运行中
        """
        return self._is_running and not self._is_paused
    
    def is_paused(self) -> bool:
        """检查是否暂停
        
        Returns:
            bool: 是否暂停
        """
        return self._is_paused
    
    def is_trading(self) -> bool:
        """检查是否交易中
        
        Returns:
            bool: 是否交易中
        """
        # 开盘时间内强制返回True
        if is_market_open():
            return True
        # 收盘时间内返回原有的_is_trading值
        return self._is_trading
    
    def get_uptime(self) -> float:
        """获取运行时长（秒）
        
        Returns:
            float: 运行时长
        """
        if not self._stats['start_time']:
            return 0.0
        
        elapsed = (datetime.now() - self._stats['start_time']).total_seconds()
        return elapsed
    
    # ========== 性能监控 ==========
    
    def record_tick(self) -> None:
        """记录 Tick 数据"""
        with self._lock:
            self._stats['total_ticks'] += 1
    
    def record_trade(self) -> None:
        """记录成交"""
        with self._lock:
            self._stats['total_trades'] += 1
    
    def record_signal(self) -> None:
        """记录信号"""
        with self._lock:
            self._stats['total_signals'] += 1
    
    def record_error(self, error_message: str) -> None:
        """记录错误
        
        Args:
            error_message: 错误信息
        """
        with self._lock:
            self._stats['errors_count'] += 1
            self._stats['last_error_time'] = datetime.now()
            self._stats['last_error_message'] = error_message
    
    # ✅ ID唯一：get_stats统一接口，返回值含service_name="StrategyCoreService"
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息
        
        Returns:
            Dict: 统计数据
        """
        with self._lock:
            uptime = self.get_uptime()
            ticks_per_second = self._stats['total_ticks'] / uptime if uptime > 0 else 0
            
            return {
                'service_name': 'StrategyCoreService',  # ✅ ID唯一：统一标识服务来源
                **self._stats,
                'uptime_seconds': uptime,
                'ticks_per_second': ticks_per_second,
                'state': self._state.value,
                'is_running': self._is_running,
                'is_paused': self._is_paused
            }
    
    # ========== 健康检查 ==========
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查
        
        Returns:
            Dict: 健康状态 {status, details}
        """
        issues = []
        status = 'healthy'
        
        # 检查状态
        if self._state == StrategyState.ERROR:
            issues.append("Strategy in ERROR state")
            status = 'unhealthy'
        
        # P0修复：检查DEGRADED状态
        if self._state == StrategyState.DEGRADED:
            issues.append("Strategy in DEGRADED state: API未就绪或订阅部分失败")
            status = 'degraded' if status == 'healthy' else status
        
        # 检查错误率
        uptime = self.get_uptime()
        if uptime > 60:  # 运行超过 1 分钟
            error_rate = self._stats['errors_count'] / uptime
            if error_rate > 0.1:  # 每分钟错误率>0.1
                issues.append(f"High error rate: {error_rate*60:.2f}/min")
                status = 'warning' if status == 'healthy' else 'unhealthy'
        
        # P0修复：端到端六段计数器检查
        e2e = self._e2e_counters
        if uptime > 30 and e2e['configured_instruments'] > 0:
            if e2e['first_tick_received'] == 0:
                issues.append("E2E: 运行30秒后仍未收到任何Tick")
                status = 'warning' if status == 'healthy' else status
            if e2e['preregistered_instruments'] == 0:
                issues.append("E2E: 未完成任何合约预注册")
                status = 'warning' if status == 'healthy' else status
        
        return {
            'status': status,
            'issues': issues,
            'strategy_id': self.strategy_id,
            'state': self._state.value,
            'uptime_seconds': uptime,
            'e2e_counters': e2e,  # P0: 输出六段计数器
            'timestamp': datetime.now().isoformat()
        }
    
    # ========== 内部方法 ==========
    
    def _init_logging(self, params: Optional[Dict[str, Any]]) -> None:
        """初始化日志配置（仅当 config_service 未初始化时）
        
        Args:
            params: 策略参数
        """
        import sys
        import os
        from logging import FileHandler, StreamHandler
        from logging.handlers import RotatingFileHandler
        
        # 检查是否已有 FileHandler，避免重复添加
        root_logger = logging.getLogger()
        has_file_handler = any(
            isinstance(h, (FileHandler, RotatingFileHandler))
            for h in root_logger.handlers
        )
        
        # 如果已经有文件处理器（来自 config_service.setup_logging），则跳过
        if has_file_handler:
            logging.debug("[StrategyCoreService._init_logging] Logging already initialized by config_service, skipping")
            return
        
        # 否则，继续初始化（向后兼容）
        log_file = params.get('log_file', 'auto_logs/strategy.log') if params else 'auto_logs/strategy.log'
        log_level_name = str((params or {}).get('log_level') or os.getenv('LOG_LEVEL', 'INFO'))
        log_level = getattr(logging, log_level_name, logging.INFO)
        
        # 确保目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 不再清空处理器：依赖 config_service 预先注入
        root_logger.setLevel(log_level)
        
        # 标准格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        abs_log_file = os.path.abspath(log_file)

        # 文件处理器（按绝对路径去重）
        has_target_file_handler = any(
            isinstance(h, (FileHandler, RotatingFileHandler))
            and os.path.abspath(getattr(h, 'baseFilename', '')) == abs_log_file
            for h in root_logger.handlers
        )
        if not has_target_file_handler:
            file_handler = FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

        # 若不存在任何 stream handler，才补充 stdout/stderr handler
        has_stream_handler = any(isinstance(h, StreamHandler) for h in root_logger.handlers)
        if not has_stream_handler:
            console_handler = StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

            error_handler = StreamHandler(sys.stderr)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            root_logger.addHandler(error_handler)
    
    def _init_scheduler(self) -> None:
        """初始化调度器（委托给 StrategyScheduler）
        
        注意：仅初始化调度器实例和注册不依赖t_type_service的任务。
        依赖t_type_service的诊断任务在_init_analytics_services完成后注册。
        """
        self._scheduler_manager.initialize()
        
        # 仅注册不依赖t_type_service的任务
        self._add_tick_sync_job()
        self._add_14_contracts_diagnosis_job()
        self._add_trading_jobs()
    
    def _stop_scheduler(self) -> None:
        """停止调度器（委托给 StrategyScheduler）
        
        P0-3/P0-2交叉修复：同时清理 strategy 级和 GLOBAL 级 job，
        避免诊断 job 在策略停止后继续运行。
        """
        self._scheduler_manager.stop_strategy_jobs(self.strategy_id)
        # 清理 GLOBAL owner 的诊断 job（option_status_diagnosis, 14_contracts_diagnosis, tick_data_sync）
        self._scheduler_manager.remove_jobs_by_owner('GLOBAL')
        self._scheduler_manager.shutdown()
    
    def _add_option_status_diagnosis_job(self) -> None:
        """添加期权5种状态诊断定时任务（委托给 StrategyScheduler）"""
        t_type_service = getattr(self, 't_type_service', None)
        self._scheduler_manager.register_option_diagnosis_task(t_type_service)
    
    def _add_tick_sync_job(self) -> None:
        """添加缓存刷写定时任务（委托给 StrategyScheduler）"""
        data_service = getattr(self, '_data_service', None) or getattr(self, 'data_service', None)
        self._scheduler_manager.register_cache_flush_task(data_service)
    
    def _add_14_contracts_diagnosis_job(self) -> None:
        """添加14个监控合约诊断定时任务（委托给 StrategyScheduler）"""
        storage = getattr(self, 'storage', None)
        query_service = getattr(self, 'query_service', None)
        self._scheduler_manager.register_14_contracts_diagnosis_task(storage=storage, query_service=query_service)

    def _add_trading_jobs(self) -> None:
        """注册交易定时任务（委托给 StrategyScheduler）"""
        run_id = getattr(self, '_lifecycle_run_id', None)
        self._scheduler_manager.register_trading_jobs(
            strategy_id=self.strategy_id,
            run_id=run_id,
            execute_option_trading_cycle=self.execute_option_trading_cycle,
            check_position_risk=self.check_position_risk,
            order_service=self._order_service
        )
    
    def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
        """
        run_id = getattr(self, '_lifecycle_run_id', 'N/A')
        if self._event_bus:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'strategy_id': self.strategy_id,
                    'run_id': run_id,
                    'source_type': 'event-tail',
                    **data
                })()
                self._event_bus.publish(event, async_mode=True)
            except Exception as e:
                logging.debug(
                    f"[StrategyCoreService][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=event-tail] "
                    f"Failed to publish {event_type}: {e}"
                )



# 导出公共接口
__all__ = ['StrategyCoreService', 'StrategyState', 'Strategy2026']

# ============================================================================
# Strategy2026 - 策略主类（从 t_type_bootstrap.py 迁移至此）
# ============================================================================

from pythongo.base import BaseStrategy
from .ui_service import UIMixin

class Strategy2026(BaseStrategy, UIMixin):
    """策略 2026 主类 - 直接继承平台基类和 UI 混合类"""
    
    def __init__(self, *args, **kwargs):
        # 调用 BaseStrategy 的 __init__
        BaseStrategy.__init__(self)
        
        # 标记这是一个真实的策略实例
        self._is_real_strategy = True
        
        # 添加关键属性（从 TTypeBootstrap 迁移）
        self._strategy = None  # 策略实例缓存
        self._init_pending = False  # 初始化中标志
        self._init_error = None  # 初始化错误信息
        
        # ✅ P0-1: 生命周期入口收口 - run_id用于追踪重复入口
        # 注意：run_id在每次onStart时重新生成，以区分不同的启动周期
        self._lifecycle_run_id = None  # 将在onStart时生成
        self._start_executed = False  # P0-1: 防止重复进入onStart
        self._stop_executed = False   # P0-1: 防止重复进入onStop
        
        # PROOF: Write to file immediately when Strategy2026 is instantiated
        from datetime import datetime as _dt
        try:
            with open('auto_logs/strategy2026_init_proof.log', 'a', encoding='utf-8') as _f:
                _f.write(f"[{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}] Strategy2026.__init__ CALLED\n")
                _f.write(f"  - args: {args}\n")
                _f.write(f"  - kwargs keys: {list(kwargs.keys())}\n")
                _f.write(f"  - self type: {type(self).__name__}\n")
                _f.write(f"  - self MRO: {[c.__name__ for c in type(self).__mro__]}\n")
        except Exception as _e:
            print(f"Failed to write init proof: {_e}")
        
        # Load params from cached table (lazy loaded, thread-safe)
        runtime_config = kwargs.get('config', {}) or {}
        self._runtime_config = dict(runtime_config)
        self._runtime_strategy_ref = kwargs.get('strategy')
        self._runtime_market_center_ref = kwargs.get('market_center')
        self._config_loaded = False

        # 注入运行时上下文，供 MarketDataService 获取市场中心和平台对象
        bootstrap_config = dict(runtime_config)
        if self._runtime_strategy_ref is not None:
            bootstrap_config['strategy'] = self._runtime_strategy_ref
        else:
            bootstrap_config['strategy'] = self  # 将策略实例本身注入到params中
        if self._runtime_market_center_ref is not None:
            bootstrap_config['market_center'] = self._runtime_market_center_ref

        self.config = bootstrap_config
        self.params = type('Params', (), bootstrap_config)()  # 先创建轻量参数对象，真正参数在 onInit 延迟加载
                    
        self.strategy_id = kwargs.get('strategy_id', f"strategy_{int(time.time())}")
        self.strategy_core = StrategyCoreService(self.strategy_id)
        
        # ========== 注入平台 API 到 strategy_core ==========
        # 注意：不在 __init__ 中注入，因为此时平台还没提供这些方法
        # 而是在 onInit 中首次调用前注入
        # ================================================
        
        # ========== 注册到 t_type_bootstrap 缓存 ==========
        # 让 TTypeBootstrap 的回调能委托给本实例
        # Bug 7修复：通过t_type_bootstrap提供的register_strategy_cache函数注册
        try:
            import t_type_bootstrap as ttb
            if hasattr(ttb, 'register_strategy_cache'):
                ttb.register_strategy_cache(self)
                logging.info("[Strategy2026] Registered to t_type_bootstrap via register_strategy_cache()")
        except Exception as e:
            logging.warning(f"[Strategy2026] Failed to register to t_type_bootstrap: {e}")
        # ================================================
        
        # 初始化 UI 相关属性
        self.auto_trading_enabled = False
        # ✅ P1-冲突6: 消除UI层状态副本，改为属性代理直接读取核心层
        # self.my_is_running, self.my_is_paused, self.my_trading 已改为@property
        self._callbacks_enabled = True
        self._stop_requested = False
        self._storage_warm_timer: Optional[threading.Timer] = None
        
        # 初始化市场时间服务
        self.market_time_service = get_market_time_service()
        
        logging.info("[Strategy2026] 已初始化，运行时配置延迟加载")
        logging.info(f"[Strategy2026] UI 功能已集成，继承自 UIMixin")

    def _ensure_runtime_config_loaded(self) -> None:
        """延迟加载参数表和运行时配置，缩短实例创建耗时。"""
        if self._config_loaded:
            return

        started_at = time.perf_counter()
        loaded_config = get_cached_params()
        logging.info(f"[Strategy2026] 从缓存参数表加载了 {len(loaded_config)} 个参数")

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
        self.params = type('Params', (), merged)()
        self._config_loaded = True

        logging.info(f"[Strategy2026] params.strategy = {getattr(self.params, 'strategy', None)}")
        logging.info("[Strategy2026] 运行时配置就绪，耗时 %.3fs", time.perf_counter() - started_at)

    def _schedule_storage_warmup(self, delay_seconds: float = 0.25) -> None:
        """在 onInit 返回后延迟预热 storage，避免阻塞平台初始化回调。"""
        try:
            existing_timer = getattr(self, '_storage_warm_timer', None)
            if existing_timer and existing_timer.is_alive():
                return

            def _warmup() -> None:
                try:
                    self.strategy_core._warm_storage_async()
                except Exception as exc:
                    logging.warning(f"[Strategy2026] storage 延迟预热启动失败: {exc}")

            timer = threading.Timer(delay_seconds, _warmup)
            timer.daemon = True
            self._storage_warm_timer = timer
            timer.start()
            logging.info("[Strategy2026] 已计划在 %.2fs 后启动 storage 后台预热", delay_seconds)
        except Exception as exc:
            logging.warning(f"[Strategy2026] 安排 storage 预热失败: {exc}")

    # ✅ ID唯一：委托scheduler_service.is_market_open()，本方法仅处理AUTO多交易所逻辑
    def is_market_open(self, exchange: str = 'AUTO') -> bool:
        """
        检查市场是否开盘
        
        Args:
            exchange: 交易所代码，AUTO 表示按参数中的 exchanges 任一开盘即视为开盘
            
        Returns:
            bool: 市场是否开盘
        """
        try:
            exchange = exchange or 'AUTO'
            if exchange != 'AUTO':
                return is_market_open(exchange)

            exchanges_raw = getattr(self.params, 'exchanges', '') if hasattr(self, 'params') else ''
            if isinstance(exchanges_raw, (list, tuple, set)):
                exchanges = [str(e).strip() for e in exchanges_raw if str(e).strip()]
            else:
                exchanges = [e.strip() for e in str(exchanges_raw).split(',') if e.strip()]

            if not exchanges:
                default_exchanges_raw = get_cached_params().get('exchanges', 'CFFEX,SHFE,DCE,CZCE,INE,GFEX')
                exchanges = [e.strip() for e in str(default_exchanges_raw).split(',') if e.strip()]

            return any(is_market_open(exch) for exch in exchanges)
        except Exception:
            # 保底不影响启动链路
            return any(is_market_open(exch) for exch in ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX'])
    
    def onStart(self):
        """平台启动回调（必须返回 None）
        
        ✅ P0-1: 生命周期入口收口 - 添加幂等保护和run_id打点
        """
        # ✅ P0-1: 幂等保护 - 防止重复进入
        if self._start_executed:
            logging.warning(
                f"[Strategy2026.onStart] ⚠️ DUPLICATE ENTRY DETECTED! "
                f"run_id={self._lifecycle_run_id}, skipping duplicate execution"
            )
            return None
        
        # ✅ P0-1: 每次启动生成新的run_id，区分不同的启动周期
        import uuid
        self._lifecycle_run_id = str(uuid.uuid4())[:8]
        self._start_executed = True
        
        logging.info(f"[Strategy2026.onStart] 🆔 New run_id={self._lifecycle_run_id}")
        
        self._callbacks_enabled = True
        self._stop_requested = False
        logging.info("[Strategy2026] 平台启动回调")

        try:
            self._ensure_runtime_config_loaded()
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] 加载运行时配置错误：{e}")
            logging.exception("[Strategy2026.onStart] 加载运行时配置堆栈:")

        try:
            # 仅在API未就绪时补调bind_platform_apis（避免与onInit重复）
            if not getattr(self.strategy_core, '_api_ready', False):
                self.strategy_core.bind_platform_apis(self)
                logging.info("[Strategy2026.onStart] API未就绪，已补调bind_platform_apis")
            else:
                logging.info("[Strategy2026.onStart] API已就绪，跳过bind_platform_apis")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] bind_platform_apis() 错误：{e}")
            logging.exception("[Strategy2026.onStart] bind_platform_apis() 堆栈:")

        try:
            # 跳过父类 BaseStrategy 的 on_start 方法中的订阅逻辑
            # 订阅逻辑统一在 on_start() 中直接处理（简化实现，48 行）
            # 只执行必要的状态设置和 UI 更新
            self.trading = True
            self.update_status_bar()
            self.output("策略启动")
            # 不调用 super().on_start()，避免重复订阅
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] 状态设置错误：{e}")
            logging.exception("[Strategy2026.onStart] 状态设置堆栈:")

        try:
            # 先确保 strategy_core 已初始化
            logging.info(f"[Strategy2026] 检查 strategy_core 状态：has on_init={hasattr(self.strategy_core, 'on_init')}")
            if hasattr(self.strategy_core, 'on_init'):
                current_state = getattr(self.strategy_core, '_state', None)
                is_running = getattr(self.strategy_core, '_is_running', False)
                is_paused = getattr(self.strategy_core, '_is_paused', False)
                logging.info(f"[Strategy2026] strategy_core 状态：state={current_state}, _is_running={is_running}, _is_paused={is_paused}")

                if _state_is(current_state, StrategyState.STOPPED) and hasattr(self.strategy_core, 'prepare_restart'):
                    restart_ready = self.strategy_core.prepare_restart()
                    logging.info(f"[Strategy2026] strategy_core STOPPED -> prepare_restart={restart_ready}")
                    current_state = getattr(self.strategy_core, '_state', None)

                # ERROR状态恢复：重置为INITIALIZING以允许重新初始化
                if _state_is(current_state, StrategyState.ERROR):
                    logging.warning(f"[Strategy2026] strategy_core 处于ERROR状态，尝试恢复")
                    self.strategy_core.transition_to(StrategyState.INITIALIZING)
                    with self.strategy_core._state_lock:
                        self.strategy_core._initialized = False
                    current_state = StrategyState.INITIALIZING

                if _state_is(current_state, StrategyState.INITIALIZING):
                    logging.info("[Strategy2026] 正在调用 strategy_core.initialize()...")
                    init_result = self.strategy_core.initialize(params=self.config)
                    logging.info(f"[Strategy2026] strategy_core 初始化结果：{init_result}")

                    # 验证初始化后的状态
                    is_running = getattr(self.strategy_core, '_is_running', False)
                    is_paused = getattr(self.strategy_core, '_is_paused', False)
                    logging.info(f"[Strategy2026] 初始化后状态：_is_running={is_running}, _is_paused={is_paused}")
                else:
                    logging.info(f"[Strategy2026] 无需调用 on_init，当前状态为：{current_state}")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] strategy_core 初始化阶段错误：{e}")
            logging.exception("[Strategy2026.onStart] strategy_core 初始化阶段堆栈:")

        try:
            # 检查市场开盘状态
            market_open = self.is_market_open()
            logging.info(f"[Strategy2026] 市场开盘状态：{market_open}")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] 市场状态检查错误：{e}")
            logging.exception("[Strategy2026.onStart] 市场状态检查堆栈:")

        try:
            # 启动 UI 界面
            logging.info("[Strategy2026] 正在启动 UI 界面...")
            self._start_output_mode_ui()
            logging.info("[Strategy2026] UI 界面启动成功")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] 启动 UI 错误：{e}")
            logging.exception("[Strategy2026.onStart] 启动 UI 堆栈:")

        try:
            # 调用策略核心的启动方法
            if hasattr(self.strategy_core, 'start'):
                result = self.strategy_core.start()
                logging.info(f"[Strategy2026] 策略核心启动结果：{result}")
            else:
                logging.warning("[Strategy2026.onStart] strategy_core.start 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] strategy_core.start() 错误：{e}")
            logging.exception("[Strategy2026.onStart] strategy_core.start() 堆栈:")

        return None
    
    def onInit(self):
        """平台初始化回调（必须返回 None）"""
        try:
            logging.info("[Strategy2026] 平台初始化回调")
            self._ensure_runtime_config_loaded()
            
            self.strategy_core.bind_platform_apis(self)
            logging.info("[Strategy2026.onInit] 已刷新平台 API 到 strategy_core")
            
            # 设置初始化标志
            self._init_pending = True
            self._init_error = None
            
            # 调用 strategy_core 的初始化
            if hasattr(self.strategy_core, 'initialize'):
                result = self.strategy_core.initialize(params=self.config)
                logging.info(f"[Strategy2026.onInit] strategy_core 初始化结果：{result}")
            
            # 清除初始化标志
            self._init_pending = False
            
            # 调用父类 BaseStrategy 的 on_init 方法（官方规范要求）
            super().on_init()
            # 注：DuckDB 架构下 storage 在 on_init 中已初始化，无需额外预热
            self._schedule_storage_warmup()
            
            return None
        except Exception as e:
            logging.error(f"[Strategy2026.onInit] 错误：{e}")
            logging.exception("[Strategy2026.onInit] 堆栈:")
            self._init_error = e
            self._init_pending = False
            return None
    
    def onStop(self):
        """平台停止回调
        
        ✅ P0-1: 生命周期入口收口 - 添加幂等保护和run_id打点
        """
        # ✅ P0-1: 幂等保护 - 防止重复进入
        if self._stop_executed:
            logging.warning(
                f"[Strategy2026.onStop] ⚠️ DUPLICATE ENTRY DETECTED! "
                f"run_id={self._lifecycle_run_id}, skipping duplicate execution"
            )
            return None
        
        self._stop_executed = True
        logging.info(f"[Strategy2026.onStop] 🛑 Stopped (run_id={self._lifecycle_run_id})")
        
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onStop] enter")

        # P0-fix: 先停工作点，确保第一时间收到停止信号
        try:
            logging.info("[Strategy2026] 平台停止回调")
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'stop'):
                result = self.strategy_core.stop()
                logging.info(f"[Strategy2026.onStop] strategy_core 停止结果：{result}")
            else:
                logging.warning("[Strategy2026.onStop] strategy_core.stop 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onStop] strategy_core.stop failed: {e}")
            logging.exception("[Strategy2026.onStop] strategy_core.on_stop stack:")

        # 不调 super().on_stop()（其中 unsub_market_data() 和 update_status_bar()
        # 调 INFINIGO.* 会与平台回调线程死锁）。
        # strategy_core.on_stop() 已通过 _unsubscribe_all_instruments() 完成取消订阅。
        # 只执行 BaseStrategy.on_stop() 中不调 INFINIGO 的步骤：
        try:
            from pythongo import utils as _utils
            self.trading = False
            _utils.Scheduler("PythonGO").stop()
            self.save_instance_file()
            logging.info("[Strategy2026.onStop] BaseStrategy non-INFINIGO steps completed")
        except Exception as e:
            logging.error(f"[Strategy2026.onStop] BaseStrategy cleanup failed: {e}")

        # 最后销毁UI（已在上层调用过，此处跳过）

        return None

    def internal_pause_strategy(self) -> bool:
        """供 UI 调用的内部暂停入口。"""
        result = False
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'pause'):
                result = bool(self.strategy_core.pause())
        except Exception as e:
            logging.error(f"[Strategy2026.internal_pause_strategy] 错误：{e}")
            logging.exception("[Strategy2026.internal_pause_strategy] 堆栈:")
            result = False

        # ✅ P1-冲突6: 已删除单向同步代码，my_is_running/my_is_paused现在是@property
        return result

    def internal_resume_strategy(self) -> bool:
        """供 UI 调用的内部恢复入口。"""
        result = False
        try:
            current_state = getattr(self.strategy_core, '_state', None)
            if _state_is(current_state, StrategyState.PAUSED) and hasattr(self.strategy_core, 'resume'):
                result = bool(self.strategy_core.resume())
            elif _state_is(current_state, StrategyState.STOPPED):
                self.onStart()
                result = bool(_state_is(getattr(self.strategy_core, '_state', None), StrategyState.RUNNING))
            else:
                logging.warning(f"[Strategy2026.internal_resume_strategy] Cannot resume in state: {current_state}")
        except Exception as e:
            logging.error(f"[Strategy2026.internal_resume_strategy] 错误：{e}")
            logging.exception("[Strategy2026.internal_resume_strategy] 堆栈:")
            result = False

        # ✅ P1-冲突6: 已删除单向同步代码，my_is_running/my_is_paused现在是@property
        return result

    def onDestroy(self):
        """平台卸载/销毁回调（必须返回 None）。
        
        ✅ P0-1: 移除对隐式补救路径的依赖，不再调用onStop()
        改为直接执行必要的清理步骤
        """
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onDestroy] enter")

        # ✅ P0-1: 如果尚未执行onStop，直接执行核心清理逻辑
        # 而不是调用self.onStop()避免重复进入和幂等保护警告
        if not getattr(self, '_stop_executed', False):
            logging.info("[Strategy2026.onDestroy] onStop not executed, performing cleanup directly")
            self._stop_executed = True
            
            # 执行UI销毁
            try:
                self._destroy_output_mode_ui()
            except Exception as e:
                logging.warning(f"[Strategy2026.onDestroy] destroy UI failed: {e}")
            
            # 执行策略核心停止
            try:
                if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'stop'):
                    result = self.strategy_core.stop()
                    logging.info(f"[Strategy2026.onDestroy] strategy_core.stop() result: {result}")
            except Exception as e:
                logging.error(f"[Strategy2026.onDestroy] strategy_core.stop() error: {e}")
        else:
            # onStop已执行，只需销毁UI
            logging.info("[Strategy2026.onDestroy] onStop already executed, skipping duplicate cleanup")
            try:
                self._destroy_output_mode_ui()
            except Exception as e:
                logging.warning(f"[Strategy2026.onDestroy] destroy UI failed: {e}")

        # 销毁策略核心
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'destroy'):
                result = self.strategy_core.destroy()
                logging.info(f"[Strategy2026.onDestroy] strategy_core.destroy() 结果：{result}")
        except Exception as e:
            logging.error(f"[Strategy2026.onDestroy] strategy_core.destroy() 错误：{e}")
            logging.exception("[Strategy2026.onDestroy] strategy_core.destroy() 堆栈:")

        # 释放运行时缓存
        try:
            self._release_runtime_caches()
        except Exception as e:
            logging.warning(f"[Strategy2026.onDestroy] release caches failed: {e}")

        # 调用父类on_destroy
        try:
            super_destroy = getattr(super(), 'on_destroy', None)
            if callable(super_destroy):
                super_destroy()
        except Exception as e:
            logging.error(f"[Strategy2026.onDestroy] super().on_destroy() 错误：{e}")

        logging.info(f"[Strategy2026.onDestroy] completed (run_id={getattr(self, '_lifecycle_run_id', 'N/A')})")
        
        # ✅ P0-1: 重置标志，允许下次启动时生成新run_id
        self._start_executed = False
        self._stop_executed = False
        self._lifecycle_run_id = None
        
        return None
    
    def onTick(self, tick):
        """平台 Tick 数据回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            # 调用父类 BaseStrategy 的 on_tick 方法（官方规范要求）
            super().on_tick(tick)
        except Exception as e:
            logging.error(f"[Strategy2026.onTick] super().on_tick() 错误：{e}")

        try:
            # 记录 Tick 汇总日志（委托给 UIMixin）
            self._log_tick_summary(tick)

            # 委托给 strategy_core 处理
            if hasattr(self.strategy_core, 'on_tick'):
                self.strategy_core.on_tick(tick)
        except Exception as e:
            logging.error(f"[Strategy2026.onTick] strategy_core.on_tick() 错误：{e}")

        return None
    
    def onOrder(self, order):
        """平台订单回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_order(order)
        except Exception as e:
            logging.error(f"[Strategy2026.onOrder] super().on_order() 错误：{e}")

        try:
            if hasattr(self.strategy_core, 'on_order'):
                self.strategy_core.on_order(order)
        except Exception as e:
            logging.error(f"[Strategy2026.onOrder] strategy_core.on_order() 错误：{e}")

        return None

    def onTrade(self, trade):
        """平台成交回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_trade(trade)
        except Exception as e:
            logging.error(f"[Strategy2026.onTrade] super().on_trade() 错误：{e}")

        try:
            if hasattr(self.strategy_core, 'on_trade'):
                self.strategy_core.on_trade(trade)
        except Exception as e:
            logging.error(f"[Strategy2026.onTrade] strategy_core.on_trade() 错误：{e}")

        return None

    # ========== 蛇形命名兼容方法（供平台调用） ==========
    # InfiniTrader 平台可能使用蛇形命名调用回调方法
    on_init = onInit
    on_start = onStart
    on_stop = onStop
    on_destroy = onDestroy
    on_tick = onTick
    on_order = onOrder
    on_trade = onTrade
    # ================================================
    
    # ✅ P1-冲突6: UI层状态属性代理，直接读取核心层状态
    @property
    def my_is_running(self) -> bool:
        """运行状态：直接读取核心层，无副本"""
        return getattr(self.strategy_core, '_is_running', False)
    
    @property
    def my_is_paused(self) -> bool:
        """暂停状态：直接读取核心层，无副本"""
        return getattr(self.strategy_core, '_is_paused', False)
    
    @property
    def my_trading(self) -> bool:
        """交易状态：直接读取核心层，无副本"""
        # 如果核心层没有_is_trading，则根据_is_running和_is_paused推断
        if hasattr(self.strategy_core, '_is_trading'):
            return getattr(self.strategy_core, '_is_trading', True)
        return getattr(self.strategy_core, '_is_running', False) and not getattr(self.strategy_core, '_is_paused', False)
