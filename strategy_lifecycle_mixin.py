"""
strategy_lifecycle_mixin.py — StrategyState + _LifecycleMixin
策略生命周期状态定义、生命周期管理、平台API绑定、Analytics初始化、调度器、日志/事件/资源诊断。

包含内容：
- StrategyState（Enum）：策略完整生命周期状态
- _state_key / _state_is：状态比较工具函数
- _LifecycleMixin：生命周期核心逻辑

设计原则：
- 不依赖 _InstrumentHelperMixin 的方法
- 其他 mixin 可从此模块导入 StrategyState（不构成循环依赖）
"""

from enum import Enum

import os
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ali2026v3_trading.params_service import _read_param, get_param_value


# ============================================================================
# StrategyState — 策略完整生命周期状态（供跨模块导入）
# ============================================================================

class StrategyState(Enum):
    """策略完整生命周期状态"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    DEGRADED = "degraded"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"
    DEGRADED_STOP = "degraded_stop"


def _state_key(state) -> str:
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


def _state_is(state, *targets) -> bool:
    state_key = _state_key(state)
    return any(state_key == _state_key(target) for target in targets)


# ============================================================================
# _LifecycleMixin
# ============================================================================


class _LifecycleMixin:

    @property
    def storage(self):
        """惰性初始化storage"""
        if self._storage is None:
            with self._storage_lock:
                if self._storage is None:
                    try:
                        from ali2026v3_trading import get_instrument_data_manager
                        self._storage = get_instrument_data_manager()
                    except Exception as e:
                        logging.error(f"[Storage] Init failed: {e}")
                        raise RuntimeError(f"Storage init failed: {e}")
        return self._storage

    def transition_to(self, new_state: StrategyState) -> bool:
        """线程安全的状态转换方法。"""
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
            self.transition_to(StrategyState.DEGRADED)

    def bind_platform_apis(self, strategy_obj: Any) -> None:
        """绑定平台API（P0-3.3修复：加锁保护，防止与on_stop等并发竞争）"""
        with self._lock:
            self._do_bind_platform_apis(strategy_obj)

    def _do_bind_platform_apis(self, strategy_obj: Any) -> None:
        """绑定平台API实际逻辑"""
        self._runtime_strategy_host = strategy_obj

        from ali2026v3_trading.config_service import resolve_product_exchange

        sub = getattr(strategy_obj, 'sub_market_data', None)
        unsub = getattr(strategy_obj, 'unsub_market_data', None)

        if callable(sub):
            _sub_call_counter = [0]
            def _subscribe(instrument_id: str, data_type: str = 'tick') -> None:
                exchange = resolve_product_exchange(instrument_id)
                _sub_call_counter[0] += 1
                if _sub_call_counter[0] <= 10 or (exchange == 'SHFE' and 'C' in instrument_id[6:] or 'P' in instrument_id[6:]):
                    logging.info(f"[PROBE_SUB] #{_sub_call_counter[0]} exchange={exchange} instrument_id={instrument_id}")
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
        self._platform_insert_order = _read_param(strategy_obj, 'insert_order') or _read_param(strategy_obj, 'send_order')
        self._platform_cancel_order = _read_param(strategy_obj, 'cancel_order') or _read_param(strategy_obj, 'cancel_order_ref')
        self._platform_get_position = getattr(strategy_obj, 'get_position', None)
        self._platform_get_orders = getattr(strategy_obj, 'get_orders', None)
        self._runtime_market_center = self._extract_runtime_market_center(strategy_obj) or self._get_fallback_market_center()
        self.get_kline = None
        if self._runtime_market_center and callable(getattr(self._runtime_market_center, 'get_kline_data', None)):
            self.get_kline = self._runtime_market_center.get_kline_data
        self._inject_runtime_context(strategy_obj)

        self._api_ready = callable(self.subscribe) and callable(self.unsubscribe)
        self._kline_ready = callable(self.get_kline)

        if hasattr(self, 'storage') and self.storage is not None:
            self.storage.bind_platform_subscribe_api(self.subscribe, self.unsubscribe)

        if self._platform_insert_order:
            self._ensure_order_service()
            if self._order_service:
                self._order_service.bind_platform_apis(self._platform_insert_order, self._platform_cancel_order)
                self._ensure_check_pending_orders_job()

        if not self._api_ready:
            logging.warning(
                "[bind_platform_apis] 平台API未完全就绪: "
                f"subscribe={callable(self.subscribe)}, unsubscribe={callable(self.unsubscribe)}, "
                "策略将以DEGRADED状态运行"
            )
            self.transition_to(StrategyState.DEGRADED)
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
            if isinstance(params, dict):
                params['strategy'] = strategy_obj
                if mc:
                    params['market_center'] = mc
            else:
                setattr(params, 'strategy', strategy_obj)
                if mc:
                    setattr(params, 'market_center', mc)
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
        """初始化运行时行情和分析服务（P1-3.3修复：拆分子方法，消除3层嵌套try-except）。"""
        try:
            storage = self.storage
            logging.info(f"[StrategyCoreService._init_analytics_services] storage = {storage}")

            futures_instruments, option_instruments = self._build_instrument_groups(storage)

            runtime_strategy_instance = _read_param(params, 'strategy')
            logging.info(f"[InitServices] futures_instruments count: {len(futures_instruments)}")
            logging.info(f"[InitServices] option_instruments groups: {len(option_instruments)}")
            logging.info(f"[InitServices] strategy_instance: {runtime_strategy_instance is not None}")

            self.analytics_service = None
            self._init_t_type_service_and_preload(storage)
            self._register_analytics_jobs()

            self._future_ids = set()
            self._option_ids = set()

            logging.info(
                "[AnalyticsInit] "
                f"analytics_service initialized, futures={len(self._future_ids)}, options={len(self._option_ids)}"
            )
        except Exception as e:
            logging.error(f"[StrategyCoreService._init_analytics_services] Error: {e}", exc_info=True)
            self.analytics_service = None

    def _build_instrument_groups(self, storage) -> tuple:
        """构建期货/期权合约分组（从_storage查询已注册合约并分类）。"""
        futures_instruments: List[str] = []
        option_instruments: Dict[str, List[str]] = {}
        if not storage:
            logging.debug("[InitServices] storage 未初始化，跳过合约查询")
            return futures_instruments, option_instruments

        try:
            from ali2026v3_trading.query_service import QueryService
            from ali2026v3_trading.subscription_manager import SubscriptionManager
            qs = QueryService(storage)
            registered_ids = storage.get_registered_instrument_ids()
            logging.debug(f"[InitServices] 已注册合约数量: {len(registered_ids)}")

            for inst_id in registered_ids:
                if SubscriptionManager.is_option(inst_id):
                    underlying = self._resolve_option_underlying_id(inst_id, storage)
                    if underlying:
                        option_instruments.setdefault(underlying, []).append(inst_id)
                else:
                    futures_instruments.append(inst_id)
        except Exception as e:
            logging.warning(f"[InitServices] 从 storage 查询合约失败: {e}")

        return futures_instruments, option_instruments

    def _resolve_option_underlying_id(self, inst_id: str, storage) -> Optional[str]:
        """解析期权的标的期货ID（先查meta，失败再查DB）。"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            meta = ps.get_instrument_meta_by_id(inst_id) if ps else None
            if meta and meta.get('underlying_future_id'):
                return meta['underlying_future_id']
        except Exception:
            pass

        try:
            from ali2026v3_trading.subscription_manager import SubscriptionManager
            from ali2026v3_trading.data_service import get_data_service
            parsed = SubscriptionManager.parse_option(inst_id)
            option_product = parsed['product']
            year_month = parsed['year_month']

            OPTION_TO_FUTURE_MAP = {'MO': 'IM', 'IO': 'IF', 'HO': 'IH'}
            future_product = OPTION_TO_FUTURE_MAP.get(option_product, option_product)

            rows = get_data_service().query(
                "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                [future_product, year_month]
            ).to_pylist()
            if rows:
                return rows[0]['instrument_id']
            else:
                logging.warning(
                    f"[InitServices] 标的期货未注册: {future_product}{year_month}"
                    f"（期权{option_product}{year_month}），跳过期权{inst_id}"
                )
                return None
        except Exception as db_err:
            logging.warning(f"[InitServices] underlying_future_id缺失且DB查询失败: {inst_id} - {db_err}")
            return None

    def _init_t_type_service_and_preload(self, storage) -> None:
        """初始化TTypeService并从配置文件metadata同步预加载期货/期权数据。

        设计约束：
        1. 合约配置文件是订阅的唯一来源（无DB回退）
        2. 预加载同步阻塞执行，失败则raise RuntimeError中断初始化
        3. internal_id/underlying_future_id直接从配置文件|分隔列读取
        """
        from ali2026v3_trading.t_type_service import get_t_type_service
        self.t_type_service = get_t_type_service()
        logging.info("[AnalyticsInit] t_type_service initialized")

        if hasattr(self, 'storage') and self.storage and hasattr(self.storage, 'subscription_manager'):
            self.storage.subscription_manager.set_t_type_service(self.t_type_service)
            logging.info("[AnalyticsInit] TTypeService injected into SubscriptionManager")

        if not self.t_type_service:
            raise RuntimeError("[AnalyticsInit] t_type_service 初始化失败，策略无法继续")

        instruments_result = getattr(self, '_init_instruments_result', None)
        if not instruments_result:
            raise RuntimeError("[AnalyticsInit] _init_instruments_result 未就绪，无法预加载")

        futures_metadata = instruments_result.get('futures_metadata', {})
        options_metadata = instruments_result.get('options_metadata', {})
        futures_list = instruments_result.get('futures_list', [])

        # 步骤1：注册期货合约（从配置文件metadata获取internal_id）
        from ali2026v3_trading.data_service import get_latest_price
        futures_registered = 0
        for inst_id in futures_list:
            try:
                meta = futures_metadata.get(inst_id, {})
                internal_id = meta.get('internal_id')
                if internal_id is None:
                    logging.warning("[AnalyticsInit] 期货 %s 无 internal_id，跳过", inst_id)
                    continue
                price = get_latest_price(inst_id) or 0.0
                if price <= 0:
                    price = 0.0
                future_internal_id = int(internal_id)
                year_month = meta.get('year_month') or self._extract_contract_year_month(inst_id) or ''
                self.t_type_service._width_cache.register_future(future_internal_id, float(price), month=year_month)
                futures_registered += 1
            except Exception as e:
                logging.error("[AnalyticsInit] 注册期货 %s 失败: %s", inst_id, e)

        if futures_registered == 0:
            raise RuntimeError(
                f"[AnalyticsInit] 期货预加载注册0个合约（配置中{len(futures_list)}个），"
                f"策略初始化终止。请检查配置文件internal_id列。"
            )
        logging.info("[AnalyticsInit] 注册期货 %d/%d", futures_registered, len(futures_list))

        # 步骤2：注册期权合约（从配置文件metadata直接读取所有字段，零正则解析）
        options_dict = instruments_result.get('options_dict', {})
        options_registered = 0
        options_total = sum(len(v) for v in options_dict.values())
        for product, option_ids in options_dict.items():
            for opt_id in option_ids:
                try:
                    meta = options_metadata.get(opt_id, {})
                    internal_id = meta.get('internal_id')
                    underlying_future_id = meta.get('underlying_future_id')
                    option_product = meta.get('product')
                    month = meta.get('year_month')
                    opt_type = meta.get('option_type')
                    strike = meta.get('strike_price')
                    if internal_id is None or underlying_future_id is None:
                        logging.warning("[AnalyticsInit] 期权 %s metadata缺失(internal_id=%s, underlying_future_id=%s)，跳过",
                                        opt_id, internal_id, underlying_future_id)
                        continue
                    if not option_product or not month or not opt_type or not strike or strike <= 0:
                        logging.warning("[AnalyticsInit] 期权 %s 字段无效(product=%s, month=%s, type=%s, strike=%s)，跳过",
                                        opt_id, option_product, month, opt_type, strike)
                        continue
                    price = get_latest_price(opt_id) or 0.0
                    self.t_type_service.register_option_contract(
                        instrument_id=opt_id,
                        underlying_product=option_product,
                        month=month,
                        strike_price=float(strike),
                        option_type=opt_type,
                        initial_price=float(price),
                        underlying_future_id=int(underlying_future_id),
                        internal_id=int(internal_id),
                    )
                    options_registered += 1
                except Exception as e:
                    logging.error("[AnalyticsInit] 注册期权 %s 失败: %s", opt_id, e)

        if options_registered == 0 and options_total > 0:
            raise RuntimeError(
                f"[AnalyticsInit] 期权预加载注册0个合约（配置中{options_total}个），"
                f"策略初始化终止。请检查配置文件internal_id/underlying_future_id列。"
            )
        logging.info("[AnalyticsInit] 注册期权 %d/%d", options_registered, options_total)

        # 步骤3：标记预加载完成
        self.t_type_service.mark_preload_complete()
        stats = self.t_type_service._width_cache.get_cache_stats()
        logging.info(
            "[AnalyticsInit] 预加载完成 - Futures: %d, Options: %d",
            stats['total_futures'], stats['total_options'],
        )

    def _register_analytics_jobs(self) -> None:
        """注册分析诊断定时任务（t_type_service就绪后）。"""
        if getattr(self, '_scheduler_manager', None) is not None:
            self._add_option_status_diagnosis_job()
            self._add_14_contracts_diagnosis_job()
            logging.info("[AnalyticsInit] ✅ 期权5种状态诊断任务已重新注册（t_type_service就绪后）")

    def _start_analytics_warmup_async(self, params) -> None:
        """同步执行 analytics 初始化（含TType预加载）。

        设计约束：预加载失败必须中断on_init，不允许带病运行。
        RuntimeError会直接传播到on_init的except块。
        """
        if self._analytics_warmup_thread and self._analytics_warmup_thread.is_alive():
            logging.info("[AnalyticsWarmup] warmup 线程已在运行，跳过重复启动")
            return

        warmup_start = time.perf_counter()
        logging.info("[AnalyticsWarmup] 同步初始化开始...")
        self._init_analytics_services(params)
        self._analytics_warmup_done = True
        elapsed = time.perf_counter() - warmup_start
        logging.info("[AnalyticsWarmup] 同步初始化完成 (耗时=%.3fs)", elapsed)

    def _ensure_analytics_ready(self, timeout: float = 30.0) -> bool:
        """等待 analytics warmup 完成（可选），供需要 analytics 就绪的调用方使用。"""
        if self._analytics_warmup_done:
            return True
        if self._analytics_warmup_thread and self._analytics_warmup_thread.is_alive():
            self._analytics_warmup_thread.join(timeout=timeout)
        return self._analytics_warmup_done

    def _warm_storage_async(self) -> None:
        """后台预热 storage 服务（异步加载合约列表等）"""
        try:
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
        """策略初始化回调"""
        with self._lock:
            if self._initialized:
                logging.info("[StrategyCoreService.on_init] Already initialized, skipping")
                return True

            if self._state not in (StrategyState.INITIALIZING, StrategyState.ERROR):
                logging.warning(f"[StrategyCoreService.on_init] Cannot initialize in state: {self._state}")
                return False

            try:
                root_logger = logging.getLogger()
                if root_logger.level > logging.INFO:
                    root_logger.setLevel(logging.INFO)
                    logging.info("[StrategyCoreService.on_init] 🔧 日志级别已修正为INFO")

                logging.info("[StrategyCoreService.on_init] Initializing...")
                init_started_at = time.perf_counter()

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

                self._init_kwargs = kwargs
                self.params = kwargs.get('params')

                self._init_logging(kwargs.get('params'))
                self._init_scheduler()

                logging.info("[Init-Step2] 从合约配置文件加载合约列表+预注册...")
                from ali2026v3_trading.query_service import QueryService
                _qs = QueryService(self.storage)
                self._init_instruments_result = _qs.load_and_preregister_instruments(self.storage, self.params)
                total_f = len(self._init_instruments_result['futures_list'])
                total_o = _qs._count_option_contracts(self._init_instruments_result['options_dict'])
                logging.info(
                    f"[Init-Step2] ✅ 合约加载+预注册完成: 期货=%d, 期权=%d, 共=%d",
                    total_f, total_o, len(self._init_instruments_result['subscribed_instruments']),
                )

                self._analytics_warmup_done = False
                self._analytics_warmup_thread = None
                self._start_analytics_warmup_async(self.params)

                self._stats['start_time'] = datetime.now()

                logging.info(
                    "[StrategyCoreService.on_init] Initialized: %s (total=%.3fs)",
                    self.strategy_id,
                    time.perf_counter() - init_started_at,
                )

                self._publish_event('StrategyInitialized', {
                    'strategy_id': self.strategy_id,
                    'timestamp': datetime.now().isoformat()
                })

                self._initialized = True
                return True

            except Exception as e:
                logging.error(f"[StrategyCoreService.on_init] Failed: {e}")
                self.transition_to(StrategyState.ERROR)
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now()
                self._stats['last_error_message'] = str(e)
                return False

    def on_start(self) -> bool:
        """策略启动回调"""
        root_logger = logging.getLogger()
        logging.info(
            f"[StrategyCoreService.on_start] 📊 日志配置诊断: "
            f"Level={logging.getLevelName(root_logger.level)}, "
            f"Handlers={len(root_logger.handlers)}"
        )

        logging.info("[StrategyCoreService.on_start] ========== START ==========")

        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            if hasattr(self, 't_type_service') and self.t_type_service:
                wc = getattr(self.t_type_service, '_width_cache', None)
                if wc:
                    spm.bind_width_cache(wc)
            self._state_param_manager = spm
            logging.info("[StrategyCoreService.on_start] StateParamManager initialized, state=%s",
                         spm.get_current_state())

            try:
                from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
                eco = get_strategy_ecosystem()
                spm.register_on_state_switch(eco.on_state_switched)
                logging.info("[StrategyCoreService.on_start] SPM↔Ecosystem联动已绑定")
            except Exception as eco_e:
                logging.warning("[StrategyCoreService.on_start] Ecosystem联动绑定失败: %s", eco_e)
        except Exception as spm_e:
            logging.warning("[StrategyCoreService.on_start] StateParamManager init failed: %s", spm_e)

        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            self._safety_meta_layer = get_safety_meta_layer(params=ps)
            logging.info("[StrategyCoreService.on_start] SafetyMetaLayer initialized")
        except Exception as safety_e:
            logging.warning("[StrategyCoreService.on_start] SafetyMetaLayer init failed: %s", safety_e)

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
            self.transition_to(StrategyState.RUNNING)

            logging.info(f"[StrategyCoreService.on_start] Started: {self.strategy_id}")

            params = None
            if hasattr(self, '_runtime_strategy_host') and self._runtime_strategy_host:
                params = getattr(self._runtime_strategy_host, 'params', None)

            if params is None:
                logging.warning("[Subscribe] ⚠️ 无法获取 params 对象，跳过订阅")
                return True

            selected_futures_list = self._init_instruments_result['futures_list']
            selected_options_dict = self._init_instruments_result['options_dict']
            self._subscribed_instruments = self._init_instruments_result['subscribed_instruments']
            logging.info(
                f"[Subscribe] 使用on_init结果: "
                f"{len(selected_futures_list)} 期货, "
                f"{self._count_option_contracts(selected_options_dict)} 期权, "
                f"共 {len(self._subscribed_instruments)} 个合约"
            )

            if selected_futures_list or selected_options_dict:
                try:
                    from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                    DiagnosisProbeManager.start_contract_watch(self._subscribed_instruments)
                except Exception as contract_watch_e:
                    logging.warning("[ContractWatch] 启动失败: %s", contract_watch_e)
                self._e2e_counters['configured_instruments'] = len(self._subscribed_instruments)

                try:
                    _ = self.storage
                    logging.info(f"[Subscribe] storage 已就绪: {self.storage is not None}")
                except Exception as storage_e:
                    logging.error(f"[Subscribe] storage 初始化失败: {storage_e}")

                if self.storage and hasattr(self.storage, 'subscription_manager'):
                    db_count = self.storage.subscription_manager.subscribe_all_instruments(
                        selected_futures_list,
                        selected_options_dict,
                    )
                    logging.info(f"[Subscribe] ✅ 数据库登记完成：{db_count} 个合约")
                    self._e2e_counters['preregistered_instruments'] = db_count

                    if callable(self.subscribe):
                        self._start_platform_subscribe_async(self._subscribed_instruments)
                        logging.info("[Subscribe] 🚀 平台订阅已在后台启动")
                        self._e2e_counters['platform_subscribe_called'] = len(self._subscribed_instruments)
                    else:
                        logging.warning("[Subscribe] ⚠️ self.subscribe 不可调用，平台API未就绪，安排延迟重试")
                        self.transition_to(StrategyState.DEGRADED)
                        def _retry_platform_subscribe():
                            for attempt in range(1, 4):
                                time.sleep(5.0 * attempt)
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
                                    if _state_is(self._state, StrategyState.DEGRADED):
                                        self.transition_to(StrategyState.RUNNING)
                                    return
                                logging.warning("[Subscribe] ⚠️ 第%d次重试失败，API仍未就绪", attempt)
                            logging.error("[Subscribe] ❌ 平台API经3次重试始终未就绪，策略保持DEGRADED状态运行")
                        threading.Thread(
                            target=_retry_platform_subscribe,
                            name=f"subscribe-retry[strategy:{self.strategy_id}]",
                            daemon=True
                        ).start()

                    logging.info(f"[SyncTicks] ⏭️ 跳过初始全量同步，依赖定时任务增量同步")
                else:
                    logging.warning("[Subscribe] 无 subscription_manager")
            else:
                logging.warning("[Subscribe] 无合约可订阅")

            auto_load = bool(get_param_value(params, 'auto_load_history', False))
            if auto_load:
                logging.info("[StrategyCoreService.on_start] 历史K线加载启动（异步，不阻塞）...")
                self._start_historical_kline_load_async()
            else:
                logging.info("[StrategyCoreService.on_start] auto_load_history=False，跳过历史K线加载")

            self._publish_event('StrategyStarted', {
                'strategy_id': self.strategy_id
            })

            self._log_resource_ownership_table(phase='start')

            return True

    def _start_historical_kline_load_async(self) -> None:
        """P1-2修复：历史K线加载拆到后台任务，不阻塞 onStart 主链。"""
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
        self._shutdown_historical_services()

        if self._storage is not None and hasattr(self._storage, '_stop_async_writer'):
            try:
                self._storage._stop_async_writer()
            except Exception as e:
                logging.warning(f"[StrategyCoreService] Storage async writer stop error: {e}")

    def on_stop(self) -> bool:
        """策略停止回调 - 接口唯一修复：内部保证save_state+停止逻辑"""
        run_id = getattr(self, '_lifecycle_run_id', 'N/A')
        with self._lock:
            if self._state == StrategyState.STOPPED:
                logging.debug(
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

            self._is_running = False
            self._is_paused = True

        jobs_zero = True
        try:
            if hasattr(self._scheduler_manager, 'pause_scheduler'):
                self._scheduler_manager.pause_scheduler()

            if hasattr(self._scheduler_manager, 'remove_jobs_by_owner'):
                removed = self._scheduler_manager.remove_jobs_by_owner(self.strategy_id)
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Removed {removed} strategy jobs"
                )

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

        try:
            from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager, reset_diagnosis_grace_period
            DiagnosisProbeManager.stop_contract_watch(reason='strategy_stop')
            reset_diagnosis_grace_period()
        except Exception as contract_watch_e:
            logging.warning(
                f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"contract_watch stop error: {contract_watch_e}"
            )

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

        with self._lock:
            if jobs_zero:
                self.transition_to(StrategyState.STOPPED)
                logging.info(
                    f"[StrategyCoreService.on_stop][strategy_id={self.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"✅ Stopped"
                )
            else:
                self.transition_to(StrategyState.DEGRADED_STOP)
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

        self._log_resource_ownership_table(phase='stop')

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

            self.transition_to(StrategyState.INITIALIZING)
            self._is_running = False
            self._is_paused = False
            self._initialized = False
            self._analytics_warmup_done = False
            self._analytics_warmup_thread = None

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

    def on_destroy(self) -> None:
        """策略销毁"""
        try:
            self.destroy()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_destroy] Error: {e}", exc_info=True)

    def initialize(self, params: Optional[Dict[str, Any]] = None) -> bool:
        """接口唯一修复：initialize为唯一初始化入口，on_init为钩子回调"""
        return self.on_init(params=params)

    def start(self) -> bool:
        """接口唯一修复：start为唯一启动入口，on_start为钩子回调"""
        return self.on_start()

    def pause(self) -> bool:
        """暂停策略（含多通道drain：flush shard buffer + drain所有队列）"""
        with self._lock:
            if self._state != StrategyState.RUNNING:
                logging.warning(f"[StrategyCoreService] Cannot pause in state: {self._state}")
                return False

            self._is_paused = True
            self.transition_to(StrategyState.PAUSED)

            logging.info(f"[StrategyCoreService] Paused: {self.strategy_id}")

            self._publish_event('StrategyPaused', {
                'strategy_id': self.strategy_id
            })

        tick_handler = getattr(self, '_tick_handler', None)
        if tick_handler and hasattr(tick_handler, '_flush_tick_buffer'):
            try:
                tick_handler._flush_tick_buffer()
                logging.info("[StrategyCoreService] pause: shard buffer已flush")
            except Exception as e:
                logging.warning("[StrategyCoreService] pause: shard buffer flush失败: %s", e)

        storage = getattr(self, 'storage', None)
        if storage and hasattr(storage, 'drain_all_queues'):
            try:
                drain_result = storage.drain_all_queues(timeout_per_queue=2.0)
                total_drained = sum(drain_result.values()) if drain_result else 0
                if total_drained > 0:
                    logging.info("[StrategyCoreService] pause: drain完成 %s", drain_result)
            except Exception as e:
                logging.warning("[StrategyCoreService] pause: drain失败: %s", e)

        return True

    def resume(self) -> bool:
        """恢复策略"""
        with self._lock:
            if self._state != StrategyState.PAUSED:
                logging.warning(f"[StrategyCoreService] Cannot resume in state: {self._state}")
                return False

            self._is_paused = False
            self.transition_to(StrategyState.RUNNING)

            logging.info(f"[StrategyCoreService] Resumed: {self.strategy_id}")

            self._publish_event('StrategyResumed', {
                'strategy_id': self.strategy_id
            })

            return True

    def stop(self) -> bool:
        """接口唯一修复：stop为唯一停止入口，内部保证save_state+on_stop"""
        return self.on_stop()

    def destroy(self) -> bool:
        """销毁策略"""
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

                self.on_stop()

                try:
                    self._shutdown_runtime_services()
                except Exception as e:
                    logging.warning(
                        f"[StrategyCoreService.destroy][strategy_id={self.strategy_id}]"
                        f"[run_id={run_id}] _shutdown_runtime_services error: {e}"
                    )

                self._scheduler = None
                self._event_bus = None
                self._destroyed = True

                self._stats = {
                    'start_time': None,
                    'total_ticks': 0,
                    'total_trades': 0,
                    'total_signals': 0,
                    'errors_count': 0,
                    'last_error_time': None,
                    'last_error_message': None,
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
        return self._state

    def is_running(self) -> bool:
        return self._is_running and not self._is_paused

    def is_paused(self) -> bool:
        return self._is_paused

    def is_trading(self) -> bool:
        from ali2026v3_trading.scheduler_service import is_market_open
        if is_market_open():
            return True
        return self._is_trading

    def get_uptime(self) -> float:
        if not self._stats['start_time']:
            return 0.0
        elapsed = (datetime.now() - self._stats['start_time']).total_seconds()
        return elapsed

    # ========== 性能监控 ==========

    def record_tick(self) -> None:
        with self._lock:
            self._stats['total_ticks'] += 1

    def record_trade(self) -> None:
        with self._lock:
            self._stats['total_trades'] += 1

    def record_signal(self) -> None:
        with self._lock:
            self._stats['total_signals'] += 1

    def record_error(self, error_message: str) -> None:
        with self._lock:
            self._stats['errors_count'] += 1
            self._stats['last_error_time'] = datetime.now()
            self._stats['last_error_message'] = error_message

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            uptime = self.get_uptime()
            ticks_per_second = self._stats['total_ticks'] / uptime if uptime > 0 else 0

            return {
                'service_name': 'StrategyCoreService',
                **self._stats,
                'uptime_seconds': uptime,
                'ticks_per_second': ticks_per_second,
                'state': self._state.value,
                'is_running': self._is_running,
                'is_paused': self._is_paused
            }

    # ========== 健康检查 ==========

    def health_check(self) -> Dict[str, Any]:
        issues = []
        status = 'healthy'

        if self._state == StrategyState.ERROR:
            issues.append("Strategy in ERROR state")
            status = 'unhealthy'

        if self._state == StrategyState.DEGRADED:
            issues.append("Strategy in DEGRADED state: API未就绪或订阅部分失败")
            status = 'degraded' if status == 'healthy' else status

        uptime = self.get_uptime()
        if uptime > 60:
            error_rate = self._stats['errors_count'] / uptime
            if error_rate > 0.1:
                issues.append(f"High error rate: {error_rate*60:.2f}/min")
                status = 'warning' if status == 'healthy' else 'unhealthy'

        e2e = self._e2e_counters
        if uptime > 30 and e2e['configured_instruments'] > 0:
            if e2e['first_tick_received'] == 0:
                issues.append("E2E: 运行30秒后仍未收到任何Tick")
                status = 'warning' if status == 'healthy' else status
            if e2e['preregistered_instruments'] == 0:
                issues.append("E2E: 未完成任何合约预注册")
                status = 'warning' if status == 'healthy' else status
            if e2e['kline_persisted_count'] == 0 and uptime > 60:
                issues.append("E2E: 运行60秒后仍无K线数据落盘")
                status = 'warning' if status == 'healthy' else status

        return {
            'status': status,
            'issues': issues,
            'strategy_id': self.strategy_id,
            'state': self._state.value,
            'uptime_seconds': uptime,
            'e2e_counters': e2e,
            'e2e_shard_enqueued': dict(self._e2e_shard_enqueued),
            'e2e_shard_persisted': dict(self._e2e_shard_persisted),
            'timestamp': datetime.now().isoformat()
        }

    # ========== 内部方法 ==========

    def _init_logging(self, params: Optional[Dict[str, Any]]) -> None:
        """初始化日志配置（仅当 config_service 未初始化时）"""
        import sys
        from logging import FileHandler, StreamHandler
        from logging.handlers import RotatingFileHandler

        root_logger = logging.getLogger()
        has_file_handler = any(
            isinstance(h, (FileHandler, RotatingFileHandler))
            for h in root_logger.handlers
        )

        if has_file_handler:
            logging.debug("[StrategyCoreService._init_logging] Logging already initialized by config_service, skipping")
            return

        log_file = params.get('log_file', 'auto_logs/strategy.log') if params else 'auto_logs/strategy.log'
        log_level_name = str((params or {}).get('log_level') or os.getenv('LOG_LEVEL', 'INFO'))
        log_level = getattr(logging, log_level_name, logging.INFO)

        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        root_logger.setLevel(log_level)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        abs_log_file = os.path.abspath(log_file)

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
        """初始化调度器（委托给 StrategyScheduler）"""
        self._scheduler_manager.initialize()

        self._add_tick_sync_job()
        self._add_14_contracts_diagnosis_job()
        self._add_trading_jobs()

    def _stop_scheduler(self) -> None:
        """停止调度器（委托给 StrategyScheduler）"""
        self._scheduler_manager.stop_strategy_jobs(self.strategy_id)
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
        """添加重点监控合约诊断定时任务（委托给 StrategyScheduler）"""
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

    def _ensure_check_pending_orders_job(self) -> None:
        if not self._order_service or not hasattr(self._order_service, 'check_pending_orders'):
            return
        if not self._scheduler_manager or not self._scheduler_manager.scheduler:
            return
        job_id = f'{self.strategy_id}_check_pending_orders'
        try:
            existing = self._scheduler_manager.scheduler.get_job(job_id)
            if existing is not None:
                return
        except Exception:
            pass
        try:
            self._scheduler_manager.add_job_with_owner(
                func=self._order_service.check_pending_orders,
                trigger='interval',
                job_id=job_id,
                strategy_id=self.strategy_id,
                run_id=getattr(self, '_lifecycle_run_id', None),
                owner_scope='strategy',
                seconds=3
            )
            logging.info("[StrategyCoreService] 补偿注册check_pending_orders job成功")
        except Exception as e:
            logging.error("[StrategyCoreService] 补偿注册check_pending_orders job失败: %s", e)

    def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """发布事件"""
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
