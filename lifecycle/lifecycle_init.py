"""lifecycle_init.py — 初始化逻辑（从strategy_lifecycle_mixin.py拆分）
职责: on_init, analytics初始化, 合约加载, 调度器初始化, 日志初始化, prepare_restart
"""
from __future__ import annotations

import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.params_service import _read_param, get_param_value
from ali2026v3_trading.lifecycle_state import StrategyState, _state_is


class LifecycleInit:
    def __init__(self, provider):
        self.p = provider

    def on_init(self, *args, **kwargs) -> bool:
        p = self.p
        with p._lock:
            if p._initialized:
                logging.info("[StrategyCoreService.on_init] Already initialized, skipping")
                return True
            if not _state_is(p._state, StrategyState.INITIALIZING) and not _state_is(p._state, StrategyState.ERROR):
                logging.warning(f"[StrategyCoreService.on_init] Cannot initialize in state: {p._state}")
                return False
            try:
                root_logger = logging.getLogger()
                if root_logger.level > logging.INFO:
                    root_logger.setLevel(logging.INFO)
                    logging.info("[StrategyCoreService.on_init] 日志级别已修正为INFO")
                logging.info("[StrategyCoreService.on_init] Initializing...")
                init_started_at = time.perf_counter()
                from ali2026v3_trading.config_service import ensure_products_with_retry
                from ali2026v3_trading.data_service import get_data_service
                ds = get_data_service()
                logging.info("[Init-Step1] 加载品种配置...")
                try:
                    product_result = ensure_products_with_retry(ds)
                    logging.info(
                        f"[Init-Step1] 品种加载成功: "
                        f"期货新增={product_result['future_added']}(已有={product_result['future_existing']}), "
                        f"期权新增={product_result['option_added']}(已有={product_result['option_existing']})"
                    )
                except Exception as e:
                    logging.error(f"[Init-Step1] 品种加载失败: {e}")
                    raise RuntimeError(f"品种加载失败，策略无法继续初始化: {e}") from e
                p._init_kwargs = kwargs
                p.params = kwargs.get('params')
                p._init_lifecycle_submodules()
                p._init_logging(kwargs.get('params'))
                p._init_scheduler()
                logging.info("[Init-Step2] 从合约配置文件加载合约列表+预注册...")
                if p.storage is None:
                    raise RuntimeError("[Init-Step2] storage初始化失败，无法加载合约列表")
                from ali2026v3_trading.query_service import QueryService
                _qs = QueryService(p.storage)
                p._init_instruments_result = _qs.load_and_preregister_instruments(p.storage, p.params)
                total_f = len(p._init_instruments_result['futures_list'])
                total_o = _qs._count_option_contracts(p._init_instruments_result['options_dict'])
                logging.info(
                    f"[Init-Step2] 合约加载+预注册完成: 期货=%d, 期权=%d, 共=%d",
                    total_f, total_o, len(p._init_instruments_result['subscribed_instruments']),
                )
                p._analytics_warmup_done = False
                p._analytics_warmup_thread = None
                p._start_analytics_warmup_async(p.params)
                p._stats['start_time'] = datetime.now(CHINA_TZ)
                logging.info(
                    "[StrategyCoreService.on_init] Initialized: %s (total=%.3fs)",
                    p.strategy_id,
                    time.perf_counter() - init_started_at,
                )
                p._publish_event('StrategyInitialized', {
                    'strategy_id': p.strategy_id,
                    'timestamp': datetime.now(CHINA_TZ).isoformat()
                })
                p._initialized = True
                _lm = getattr(p, '_lifecycle_mgr', None)
                if _lm is not None:
                    _lm.initialized = True
                return True
            except Exception as e:
                logging.error(f"[StrategyCoreService.on_init] Failed: {e}")
                p.transition_to(StrategyState.ERROR)
                p._initialized = False
                _lm = getattr(p, '_lifecycle_mgr', None)
                if _lm is not None:
                    _lm.initialized = False
                p._stats['errors_count'] += 1
                p._stats['last_error_time'] = datetime.now(CHINA_TZ)
                p._stats['last_error_message'] = str(e)
                return False

    def initialize(self, params: Optional[Dict[str, Any]] = None) -> bool:
        return self.on_init(params=params)

    def prepare_restart(self) -> bool:
        p = self.p
        run_id = getattr(p, '_lifecycle_run_id', 'N/A')
        with p._lock:
            if p._destroyed:
                logging.warning(
                    f"[StrategyCoreService.prepare_restart][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Cannot restart destroyed strategy"
                )
                return False
            if p._state != StrategyState.STOPPED:
                logging.info(
                    f"[StrategyCoreService.prepare_restart][strategy_id={p.strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                    f"Skip restart prep in state: {p._state}"
                )
                return p._state in (StrategyState.INITIALIZING, StrategyState.RUNNING, StrategyState.PAUSED)
            p.transition_to(StrategyState.INITIALIZING)
            p._is_running = False
            p._is_paused = False
            p._initialized = False
            _lm = getattr(p, '_lifecycle_mgr', None)
            if _lm is not None:
                _lm.is_running = False
                _lm.is_paused = False
                _lm.initialized = False
            p._analytics_warmup_done = False
            p._analytics_warmup_thread = None
            p._reset_historical_state_for_restart()
            p._platform_subscribe_thread = None
            p._platform_subscribe_stop.clear()
            logging.info(
                f"[StrategyCoreService.prepare_restart][strategy_id={p.strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"Rearmed from STOPPED (rearm_reason=user-requested)"
            )
            return True

    def _init_analytics_services(self, params: Any) -> None:
        p = self.p
        try:
            storage = p.storage
            logging.info(f"[StrategyCoreService._init_analytics_services] storage = {storage}")
            futures_instruments, option_instruments = p._build_instrument_groups(storage)
            runtime_strategy_instance = _read_param(params, 'strategy')
            logging.info(f"[InitServices] futures_instruments count: {len(futures_instruments)}")
            logging.info(f"[InitServices] option_instruments groups: {len(option_instruments)}")
            logging.info(f"[InitServices] strategy_instance: {runtime_strategy_instance is not None}")
            p.analytics_service = None
            p._init_t_type_service_and_preload(storage)
            p._register_analytics_jobs()
            p._future_ids = set()
            p._option_ids = set()
            logging.info(
                "[AnalyticsInit] "
                f"analytics_service initialized, futures={len(p._future_ids)}, options={len(p._option_ids)}"
            )
        except Exception as e:
            logging.error(f"[StrategyCoreService._init_analytics_services] Error: {e}", exc_info=True)
            p.analytics_service = None

    def _build_instrument_groups(self, storage) -> Tuple[List[str], Dict[str, List[str]]]:
        p = self.p
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
                    underlying = p._resolve_option_underlying_id(inst_id, storage)
                    if underlying:
                        option_instruments.setdefault(underlying, []).append(inst_id)
                else:
                    futures_instruments.append(inst_id)
        except Exception as e:
            logging.warning(f"[InitServices] 从 storage 查询合约失败: {e}")
        return futures_instruments, option_instruments

    def _resolve_option_underlying_id(self, inst_id: str, storage) -> Optional[str]:
        p = self.p
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
                "SELECT instrument_id FROM futures_instrument WHERE product=? AND year_month=?",
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
        p = self.p
        from ali2026v3_trading.t_type_service import get_t_type_service
        p.t_type_service = get_t_type_service()
        logging.info("[AnalyticsInit] t_type_service initialized")
        if hasattr(p, 'storage') and p.storage and hasattr(p.storage, 'subscription_manager'):
            p.storage.subscription_manager.set_t_type_service(p.t_type_service)
            logging.info("[AnalyticsInit] TTypeService injected into SubscriptionManager")
        if not p.t_type_service:
            raise RuntimeError("[AnalyticsInit] t_type_service 初始化失败，策略无法继续")
        instruments_result = getattr(p, '_init_instruments_result', None)
        if not instruments_result:
            raise RuntimeError("[AnalyticsInit] _init_instruments_result 未就绪，无法预加载")
        futures_metadata = instruments_result.get('futures_metadata', {})
        options_metadata = instruments_result.get('options_metadata', {})
        futures_list = instruments_result.get('futures_list', [])
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
                year_month = meta.get('year_month') or p._extract_contract_year_month(inst_id) or ''
                p.t_type_service._width_cache.register_future(future_internal_id, float(price), month=year_month)
                futures_registered += 1
            except Exception as e:
                logging.error("[AnalyticsInit] 注册期货 %s 失败: %s", inst_id, e)
        if futures_registered == 0:
            raise RuntimeError(
                f"[AnalyticsInit] 期货预加载注册0个合约（配置中{len(futures_list)}个），策略初始化终止。请检查配置文件internal_id列。"
            )
        logging.info("[AnalyticsInit] 注册期货 %d/%d", futures_registered, len(futures_list))
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
                    p.t_type_service.register_option_contract(
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
                f"[AnalyticsInit] 期权预加载注册0个合约（配置中{options_total}个），策略初始化终止。请检查配置文件internal_id/underlying_future_id列。"
            )
        logging.info("[AnalyticsInit] 注册期权 %d/%d", options_registered, options_total)
        p.t_type_service.mark_preload_complete()
        stats = p.t_type_service._width_cache.get_cache_stats()
        logging.info(
            "[AnalyticsInit] 预加载完成 - Futures: %d, Options: %d",
            stats['total_futures'], stats['total_options'],
        )

    def _register_analytics_jobs(self) -> None:
        p = self.p
        if getattr(p, '_scheduler_manager', None) is not None:
            p._add_option_status_diagnosis_job()
            p._add_14_contracts_diagnosis_job()
            logging.info("[AnalyticsInit] 期权5种状态诊断任务已重新注册（t_type_service就绪后）")

    def _start_analytics_warmup_async(self, params) -> None:
        p = self.p
        if p._analytics_warmup_thread and p._analytics_warmup_thread.is_alive():
            logging.info("[AnalyticsWarmup] warmup 线程已在运行，跳过重复启动")
            return
        warmup_start = time.perf_counter()
        logging.info("[AnalyticsWarmup] 同步初始化开始...")
        p._init_analytics_services(params)
        p._analytics_warmup_done = True
        elapsed = time.perf_counter() - warmup_start
        logging.info("[AnalyticsWarmup] 同步初始化完成 (耗时=%.3fs)", elapsed)

    def _ensure_analytics_ready(self, timeout: float = 30.0) -> bool:
        p = self.p
        if p._analytics_warmup_done:
            return True
        if p._analytics_warmup_thread and p._analytics_warmup_thread.is_alive():
            p._analytics_warmup_thread.join(timeout=timeout)
        return p._analytics_warmup_done

    def _warm_storage_async(self) -> None:
        p = self.p
        try:
            storage = p.storage
            if storage:
                logging.info("[WarmStorage] storage 后台预热完成")
            else:
                logging.warning("[WarmStorage] storage 未能初始化")
        except Exception as e:
            logging.warning(f"[WarmStorage] 后台预热失败：{e}")

    @staticmethod
    def _fetch_china_holidays(year: Optional[int] = None) -> List:
        from datetime import date as _date, timedelta as _td, datetime as _dt
        import urllib.request
        import json as _json
        if year is None:
            year = _dt.now(CHINA_TZ).year
        _BUILTIN_HOLIDAYS = {
            (2026, 1, 1), (2026, 1, 2),
            (2026, 2, 14), (2026, 2, 15),
            (2026, 4, 3), (2026, 4, 6), (2026, 4, 7),
        }
        def _builtin_as_dates(target_year: int) -> List[_date]:
            result = [_date(y, m, d) for y, m, d in _BUILTIN_HOLIDAYS if y == target_year]
            logging.info("[P1-R11-13] 使用内置节假日列表 (year=%d, count=%d)", target_year, len(result))
            return result
        try:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/CN"
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'ali2026v3_trading/1.0')
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = _json.loads(resp.read().decode('utf-8'))
                holidays = [_date.fromisoformat(item['date']) for item in data if item.get('date')]
                logging.info("[P1-R11-13] 在线获取中国%d年节假日成功 (count=%d)", year, len(holidays))
                return holidays
        except Exception as _e:
            logging.warning("[P1-R11-13] 在线获取节假日失败 (%s)，回退到内置列表", _e)
        return _builtin_as_dates(year)

    def _init_logging(self, params: Optional[Dict[str, Any]]) -> None:
        p = self.p
        import sys
        from logging import FileHandler, StreamHandler
        from logging.handlers import RotatingFileHandler
        root_logger = logging.getLogger()
        log_file = params.get('log_file', 'logs/strategy.log') if params else 'logs/strategy.log'
        abs_log_file = os.path.abspath(log_file)
        has_valid_file_handler = False
        stale_handlers = []
        for h in root_logger.handlers[:]:
            if isinstance(h, (FileHandler, RotatingFileHandler)):
                handler_file = os.path.abspath(getattr(h, 'baseFilename', ''))
                if handler_file == abs_log_file:
                    try:
                        if h.stream and not h.stream.closed:
                            has_valid_file_handler = True
                        else:
                            stale_handlers.append(h)
                    except Exception:
                        stale_handlers.append(h)
        for h in stale_handlers:
            try:
                root_logger.removeHandler(h)
                h.close()
            except Exception:
                pass
        if has_valid_file_handler:
            logging.debug("[StrategyCoreService._init_logging] Logging already initialized, skipping")
            return
        log_level_name = str((params or {}).get('log_level') or os.getenv('LOG_LEVEL', 'INFO'))
        log_level = getattr(logging, log_level_name, logging.INFO)
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        root_logger.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        has_target_file_handler = any(
            isinstance(h, (FileHandler, RotatingFileHandler))
            and os.path.abspath(getattr(h, 'baseFilename', '')) == abs_log_file
            for h in root_logger.handlers
        )
        if not has_target_file_handler:
            max_bytes = int((params or {}).get('log_max_bytes', 100 * 1024 * 1024))
            backup_count = int((params or {}).get('log_backup_count', 3))
            file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
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
        p = self.p
        p._scheduler_manager.initialize()
        p._add_tick_sync_job()
        p._add_14_contracts_diagnosis_job()
        p._add_trading_jobs()

    def _stop_scheduler(self) -> None:
        p = self.p
        p._scheduler_manager.stop_strategy_jobs(p.strategy_id)
        p._scheduler_manager.remove_jobs_by_owner('GLOBAL')
        p._scheduler_manager.shutdown()
        import threading as _th
        _alive_threads = [t for t in _th.enumerate() if t.name.startswith('scheduler_') and t.is_alive()]
        if _alive_threads:
            _deadline = time.time() + 10.0
            for t in _alive_threads:
                t.join(timeout=max(0, _deadline - time.time()))
            _still_alive = [t for t in _alive_threads if t.is_alive()]
            if _still_alive:
                logging.critical("R15-P1-RES-06: 调度器线程超时未退出，强制终止: %s", [t.name for t in _still_alive])
                os._exit(1)

    def _add_option_status_diagnosis_job(self) -> None:
        p = self.p
        t_type_service = getattr(p, 't_type_service', None)
        p._scheduler_manager.register_option_diagnosis_task(t_type_service)

    def _add_tick_sync_job(self) -> None:
        p = self.p
        data_service = getattr(p, '_data_service', None) or getattr(p, 'data_service', None)
        p._scheduler_manager.register_cache_flush_task(data_service)

    def _add_14_contracts_diagnosis_job(self) -> None:
        p = self.p
        storage = getattr(p, 'storage', None)
        query_service = getattr(p, 'query_service', None)
        p._scheduler_manager.register_14_contracts_diagnosis_task(storage=storage, query_service=query_service)

    def _add_trading_jobs(self) -> None:
        p = self.p
        run_id = getattr(p, '_lifecycle_run_id', None)
        p._scheduler_manager.register_trading_jobs(
            strategy_id=p.strategy_id,
            run_id=run_id,
            execute_option_trading_cycle=p.execute_option_trading_cycle,
            check_position_risk=p.check_position_risk,
            order_service=p._order_service
        )

    def _ensure_check_pending_orders_job(self) -> None:
        p = self.p
        if not p._order_service or not hasattr(p._order_service, 'check_pending_orders'):
            return
        if not p._scheduler_manager or not p._scheduler_manager.scheduler:
            return
        job_id = f'{p.strategy_id}_check_pending_orders'
        try:
            existing = p._scheduler_manager.scheduler.get_job(job_id)
            if existing is not None:
                return
        except Exception:
            pass
        try:
            p._scheduler_manager.add_job_with_owner(
                func=p._order_service.check_pending_orders,
                trigger='interval',
                job_id=job_id,
                strategy_id=p.strategy_id,
                run_id=getattr(p, '_lifecycle_run_id', None),
                owner_scope='strategy',
                seconds=3
            )
            logging.info("[StrategyCoreService] 补偿注册check_pending_orders job成功")
        except Exception as e:
            logging.error("[StrategyCoreService] 补偿注册check_pending_orders job失败: %s", e)