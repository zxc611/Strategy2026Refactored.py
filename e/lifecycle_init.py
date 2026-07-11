# MODULE_ID: M1-122
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""lifecycle_init.py — 初始化逻辑（从strategy_lifecycle_mixin.py拆分）
职责: on_init, analytics初始化, 合约加载, 调度器初始化, 日志初始化, prepare_restart
"""
from __future__ import annotations

import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.config.params_service import _read_param, get_param_value
from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState, _state_is


def _force_log_flush() -> None:
    """强制刷新所有日志handler到磁盘 (os.fsync)"""
    try:
        _root = logging.getLogger()
        for handler in _root.handlers:
            try:
                handler.flush()
                stream = getattr(handler, 'stream', None)
                if stream is not None and hasattr(stream, 'fileno'):
                    try:
                        os.fsync(stream.fileno())
                    except (OSError, ValueError):
                        pass
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError):
                pass
    except Exception:
        pass


class LifecycleInit:
    def __init__(self, provider):
        self.p = provider

    def on_init(self, *args, **kwargs) -> bool:
        p = self.p
        with p._lock:
            if p._initialized:
                logging.info("[StrategyCoreService.on_init] Already initialized, ensuring ParamsService cache loaded")
                try:
                    from ali2026v3_trading.config.params_service import get_params_service
                    _ps = get_params_service()
                    if _ps is not None and hasattr(_ps, '_instrument_id_to_internal_id'):
                        with _ps._lock:
                            if not _ps._instrument_id_to_internal_id:
                                from ali2026v3_trading.data.data_service import get_existing_data_service
                                _ds = get_existing_data_service()
                                if _ds is not None:
                                    _ps.load_caches_from_db(_ds)
                                    logging.info("[StrategyCoreService.on_init] ParamsService缓存已从DB加载: %d 个合约", len(_ps._instrument_id_to_internal_id))
                                else:
                                    logging.warning("[StrategyCoreService.on_init] DataService不可用，无法加载ParamsService缓存")
                            else:
                                logging.info("[StrategyCoreService.on_init] ParamsService缓存已有 %d 个合约，无需重新加载", len(_ps._instrument_id_to_internal_id))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _cache_err:
                    logging.warning("[StrategyCoreService.on_init] ParamsService缓存加载失败: %s", _cache_err)
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
                # Step0a: 最先初始化日志系统，确保后续所有步骤（含品种加载失败路径）都有日志落地
                # 此前 _init_logging 位于品种加载之后，品种加载失败/阻塞时日志永不初始化，形成诊断盲区
                p._init_kwargs = kwargs
                p.params = kwargs.get('params')
                p._init_logging(kwargs.get('params'))
                logging.info("[Init-Step0a] 日志系统已初始化")
                # Step0b: 初始化生命周期子模块
                p._init_lifecycle_submodules()
                logging.info("[Init-Step0b] 生命周期子模块已初始化")
                # Step1: 品种加载
                from ali2026v3_trading.config.config_service import ensure_products_with_retry
                from ali2026v3_trading.data.data_service import get_data_service
                ds = get_data_service()
                logging.info("[Init-Step1] 加载品种配置...")
                try:
                    product_result = ensure_products_with_retry(ds)
                    logging.info(
                        f"[Init-Step1] 品种加载成功: "
                        f"期货新增={product_result['future_added']}(已有={product_result['future_existing']}), "
                        f"期权新增={product_result['option_added']}(已有={product_result['option_existing']})"
                    )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.error(f"[Init-Step1] 品种加载失败: {e}")
                    raise RuntimeError(f"品种加载失败，策略无法继续初始化: {e}") from e

                logging.info("[Init-Step2] 从合约配置文件加载合约列表+预注册...")
                if p.storage is None:
                    raise RuntimeError("[Init-Step2] storage初始化失败，无法加载合约列表")
                from ali2026v3_trading.data.query_service import QueryService
                _qs = QueryService(p.storage)
                _instr_ret = _qs.load_and_preregister_instruments(p.storage, p.params)
                p._init_instruments_result = _instr_ret
                del _instr_ret
                total_f = p._init_instruments_result.get('total_futures', len(p._init_instruments_result.get('futures_list', [])))
                total_o = p._init_instruments_result.get('total_options', _qs._count_option_contracts(p._init_instruments_result.get('options_dict', {})))
                total_all = p._init_instruments_result.get('total_instruments', len(p._init_instruments_result.get('subscribed_instruments', [])))
                _deferred_count = p._init_instruments_result.get('deferred_count', 0)
                logging.info(
                    f"[Init-Step2] 合约加载+预注册完成: 期货=%d, 期权=%d, 共=%d (延迟=%d)",
                    total_f, total_o, total_all, _deferred_count,
                )
                p._init_scheduler()
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
            logging.info("[StrategyCoreService._init_analytics_services] 开始初始化 analytics services")
            futures_instruments, option_instruments = p._build_instrument_groups(storage)
            runtime_strategy_instance = _read_param(params, 'strategy')
            logging.info("[InitServices] futures_instruments count: %d", len(futures_instruments))
            logging.info("[InitServices] option_instruments groups: %d", len(option_instruments))
            logging.info("[InitServices] strategy_instance: %s", runtime_strategy_instance is not None)
            p.analytics_service = None
            logging.info("[InitServices] 即将调用 _init_t_type_service_and_preload")
            p._init_t_type_service_and_preload(storage)
            logging.info("[InitServices] _init_t_type_service_and_preload 已完成")
            p._register_analytics_jobs()
            p._future_ids = set()
            p._option_ids = set()
            logging.info(
                "[AnalyticsInit] "
                "analytics_service initialized, futures=%d, options=%d",
                len(p._future_ids), len(p._option_ids)
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StrategyCoreService._init_analytics_services] Error: %s", e, exc_info=True)
            p.analytics_service = None

    def _build_instrument_groups(self, storage) -> Tuple[List[str], Dict[str, List[str]]]:
        p = self.p
        futures_instruments: List[str] = []
        option_instruments: Dict[str, List[str]] = {}
        if not storage:
            logging.debug("[InitServices] storage 未初始化，跳过合约查询")
            return futures_instruments, option_instruments
        try:
            from ali2026v3_trading.data.query_service import QueryService
            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning(f"[InitServices] 从 storage 查询合约失败: {e}")
        return futures_instruments, option_instruments

    def _resolve_option_underlying_id(self, inst_id: str, storage) -> Optional[str]:
        p = self.p
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            ps = get_params_service()
            meta = ps.get_instrument_meta_by_id(inst_id) if ps else None
            if meta and meta.get('underlying_future_id'):
                return meta['underlying_future_id']
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
            from ali2026v3_trading.data.data_service import get_data_service
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as db_err:
            logging.warning(f"[InitServices] underlying_future_id缺失且DB查询失败: {inst_id} - {db_err}")
            return None

    def _init_t_type_service_and_preload(self, storage) -> None:
        p = self.p
        from ali2026v3_trading.data.t_type_service import get_t_type_service
        logging.info("[AnalyticsInit] Step 1/7: 获取 t_type_service 单例")
        p.t_type_service = get_t_type_service()
        logging.info("[AnalyticsInit] Step 2/7: t_type_service 实例已获取: %s", p.t_type_service is not None)
        _storage = getattr(p, 'storage', None) or storage
        _sm = None
        if _storage is not None:
            _sm = getattr(_storage, 'subscription_manager', None)
            if callable(_sm):
                _sm = _sm()
        if _sm is None:
            logging.warning("[AnalyticsInit] storage.subscription_manager is None, trying DataService fallback")
            try:
                from ali2026v3_trading.data.data_service import get_data_service
                _ds = get_data_service()
                if _ds is not None:
                    _sm = _ds.subscription_manager
            except Exception as _sm_fb_err:
                logging.warning("[AnalyticsInit] DataService fallback for SubscriptionManager failed: %s", _sm_fb_err)
        if _sm is not None and hasattr(_sm, 'set_t_type_service'):
            _sm.set_t_type_service(p.t_type_service)
            logging.info("[AnalyticsInit] TTypeService injected into SubscriptionManager")
        else:
            logging.warning("[AnalyticsInit] SubscriptionManager not available, TTypeService not injected")
        if not p.t_type_service:
            raise RuntimeError("[AnalyticsInit] t_type_service 初始化失败，策略无法继续")
        instruments_result = getattr(p, '_init_instruments_result', None)
        if not instruments_result:
            raise RuntimeError("[AnalyticsInit] _init_instruments_result 未就绪，无法预加载")
        futures_metadata = instruments_result.get('futures_metadata', {})
        options_metadata = instruments_result.get('options_metadata', {})
        futures_list = instruments_result.get('futures_list', [])
        logging.info("[AnalyticsInit] Step 3/7: 准备注册期货 %d 个", len(futures_list))
        from ali2026v3_trading.data.data_service import get_latest_price
        from ali2026v3_trading.config.params_service import get_params_service as _get_ps
        _ps = _get_ps()
        futures_registered = 0
        _futures_total = len(futures_list)
        for inst_id in futures_list:
            try:
                meta = futures_metadata.get(inst_id, {})
                db_meta = _ps.get_instrument_meta_by_id(inst_id)
                internal_id = db_meta.get('internal_id') if db_meta else meta.get('internal_id')
                if internal_id is None:
                    logging.warning("[AnalyticsInit] 期货 %s 无 internal_id（TXT=%s, DB=%s），跳过",
                                    inst_id, meta.get('internal_id'), db_meta)
                    continue
                price = get_latest_price(inst_id) or 0.0
                if price <= 0:
                    price = 0.0
                future_internal_id = int(internal_id)
                year_month = (db_meta.get('year_month') if db_meta else None) or meta.get('year_month') or p._extract_contract_year_month(inst_id) or ''
                p.t_type_service._width_cache.register_future(future_internal_id, float(price), month=year_month)
                futures_registered += 1
                if futures_registered % 50 == 0 or futures_registered == _futures_total:
                    logging.info("[AnalyticsInit] 期货注册进度: %d/%d (last=%s)", futures_registered, _futures_total, inst_id)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error("[AnalyticsInit] 注册期货 %s 失败: %s", inst_id, e)
        if futures_registered == 0:
            raise RuntimeError(
                f"[AnalyticsInit] 期货预加载注册0个合约（配置中{len(futures_list)}个），策略初始化终止。请检查配置文件internal_id列。"
            )
        logging.info("[AnalyticsInit] Step 4/7: 期货注册完成 %d/%d", futures_registered, len(futures_list))

        options_dict = instruments_result.get('options_dict', {})
        options_total = sum(len(v) for v in options_dict.values())
        logging.info("[AnalyticsInit] Step 5/7: 期权 %d 个延迟到on_start后异步注册（DLL安全）", options_total)
        p._deferred_t_type_options = {
            'options_dict': {k: list(v) for k, v in options_dict.items()},
            'options_metadata': dict(options_metadata),
        }
        logging.info("[AnalyticsInit] Step 6/7: 期权延迟注册数据已存储 (待注册=%d)", options_total)
        logging.info("[AnalyticsInit] Step 7/7: 标记预加载完成（仅期货）")
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
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
        stale_handlers = []
        for h in root_logger.handlers[:]:
            if isinstance(h, (FileHandler, RotatingFileHandler)):
                handler_file = os.path.abspath(getattr(h, 'baseFilename', ''))
                if handler_file == abs_log_file:
                    try:
                        file_exists = os.path.exists(handler_file)
                        if h.stream and not h.stream.closed and file_exists:
                            return
                        else:
                            stale_handlers.append(h)
                    except (ValueError, KeyError, TypeError, AttributeError):
                        stale_handlers.append(h)
        for h in stale_handlers:
            try:
                root_logger.removeHandler(h)
                h.close()
            except (ValueError, KeyError, TypeError, AttributeError):
                pass
        log_level_name = str((params or {}).get('log_level') or os.getenv('LOG_LEVEL', 'INFO'))
        log_level = getattr(logging, log_level_name, logging.INFO)
        log_dir = os.path.dirname(abs_log_file)
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
            try:
                file_handler = RotatingFileHandler(abs_log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
                file_handler.setLevel(log_level)
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
                file_handler.flush()
                if os.path.exists(abs_log_file):
                    logging.info("[_init_logging] Created RotatingFileHandler: %s (verified)", abs_log_file)
                else:
                    logging.warning("[_init_logging] RotatingFileHandler created but file not found: %s", abs_log_file)
            except (OSError, IOError, PermissionError) as _fh_err:
                logging.warning("[_init_logging] Failed to create RotatingFileHandler for %s: %s", abs_log_file, _fh_err)
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
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StrategyCoreService] 补偿注册check_pending_orders job失败: %s", e)