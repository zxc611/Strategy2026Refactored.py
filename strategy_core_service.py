"""
策略核心服务模块 - Facade
Facade: StrategyCoreService = _LifecycleMixin + _InstrumentHelperMixin
         + HistoricalKlineMixin + TickHandlerMixin

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
"""
from __future__ import annotations

import time
import threading
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState, _state_key, _state_is, _LifecycleMixin
from ali2026v3_trading.strategy_instrument_mixin import _InstrumentHelperMixin
from ali2026v3_trading.strategy_historical import HistoricalKlineMixin
from ali2026v3_trading.strategy_tick_handler import TickHandlerMixin


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class StrategyCoreService(_LifecycleMixin, _InstrumentHelperMixin,
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

    def __init__(self, strategy_id: str = None, event_bus: Optional[Any] = None):
        self.strategy_id = strategy_id or f"strategy_{int(time.time())}"

        # 状态管理
        self._state = StrategyState.INITIALIZING
        self._is_running = False
        self._is_paused = False
        self._is_trading = True
        self._destroyed = False
        self._initialized = False
        self._analytics_warmup_done = False
        self._analytics_warmup_thread = None
        self.t_type_service = None
        self._processed_trade_ids = set()

        # 线程锁（职责说明）
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

        # 初始化历史K线Mixin
        self._init_historical_kline_mixin()

        # 初始化Tick处理Mixin
        self._init_tick_handler_mixin()

        # EventBus 集成
        self._event_bus = event_bus

        # 信号服务集成
        from ali2026v3_trading.signal_service import SignalService
        self._signal_service = SignalService(event_bus=event_bus)

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

        # P0修复：端到端六段计数器
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
        self._shadow_engine = None

        # 策略生态系统(V7次系统2)
        self._strategy_ecosystem = None

        # HFT增强引擎(V8)
        self._hft_engine = None

    # ========== 交易/持仓/风控方法（原 _TradingMixin，v3.2 内联） ==========

    def _ensure_order_service(self) -> None:
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
        if self._position_service is not None:
            return
        try:
            from ali2026v3_trading.position_service import get_position_service
            self._position_service = get_position_service()
            logging.info("[StrategyCoreService] PositionService initialized (singleton)")
        except Exception as e:
            logging.warning(f"[StrategyCoreService] Failed to initialize PositionService: {e}")
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
            logging.info("[StrategyCoreService] HFT增强引擎已初始化(七大竞争优势-分散架构)")
        except Exception as e:
            logging.warning(f"[StrategyCoreService] HFT增强引擎初始化失败: {e}")
            self._hft_engine = None

    def on_order(self, order_data: Any, *args, **kwargs) -> None:
        try:
            oid = getattr(order_data, "order_id", "") or getattr(order_data, "OrderRef", "")
            sts = getattr(order_data, "status", "") or getattr(order_data, "OrderStatus", "")
            logging.info("[StrategyCoreService.on_order] OrderID=%s, Status=%s", oid, sts)
            if self._order_service:
                self._order_service.on_trade_update(order_data)
        except Exception as e:
            logging.error("[StrategyCoreService.on_order] Error: %s", e)

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
            self._processed_trade_ids.add(trade_id)
            if len(self._processed_trade_ids) > 10000:
                self._processed_trade_ids.clear()
            from ali2026v3_trading.position_service import get_position_service
            pos_svc = get_position_service()
            pos_svc.on_trade(trade_data)
        except Exception as e:
            logging.error("[StrategyCoreService._update_position_from_trade] Error: %s", e)

    def execute_option_trading_cycle(self) -> None:
        if not self._is_running or self._is_paused:
            return
        if self._state == StrategyState.DEGRADED:
            logging.debug("[OptionTrading] 策略处于DEGRADED状态，跳过交易周期")
            return
        if not self._trading_lock.acquire(blocking=False):
            logging.debug("[OptionTrading] 交易锁被占用，跳过本次周期")
            return
        try:
            t_type = getattr(self, 't_type_service', None)
            if not t_type:
                return
            targets = t_type.select_otm_targets_by_volume()
            if not targets:
                return
            open_reason = self._resolve_open_reason()
            for t in targets:
                t['open_reason'] = t.get('open_reason', '') or open_reason
            self._ensure_order_service()
            if self._order_service:
                order_ids = self._order_service.execute_by_ranking(targets)
                if order_ids:
                    logging.info("[OptionTrading] 下单完成: %d 笔 reason=%s", len(order_ids), open_reason)
            self._feed_shadow_engine(targets, open_reason)
        except Exception as e:
            logging.error("[StrategyCoreService.execute_option_trading_cycle] Error: %s", e)
        finally:
            self._trading_lock.release()

    def _feed_shadow_engine(self, targets: list, open_reason: str) -> None:
        """V7新增：将交易信号批量推送到影子策略监控引擎(优化：单次锁获取)"""
        try:
            se = getattr(self, '_shadow_engine', None)
            if se is None:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                se = get_shadow_strategy_engine()
                self._shadow_engine = se
            spm = getattr(self, '_state_param_manager', None)
            market_state = spm.get_current_state() if spm else 'unknown'
            for t in targets:
                se.process_signal(
                    market_state=market_state,
                    instrument_id=t.get('instrument_id', ''),
                    signal_direction=t.get('direction', ''),
                    price=t.get('price', 0.0),
                    quantity=t.get('volume', 0),
                    signal_strength=t.get('signal_strength', 0.0),
                )
        except Exception as e:
            logging.debug("[StrategyCoreService._feed_shadow_engine] Error: %s", e)

    def _resolve_open_reason(self) -> str:
        """V7新增：根据当前状态诊断映射开仓理由编码"""
        state = 'unknown'
        try:
            spm = getattr(self, '_state_param_manager', None)
            if spm:
                state = spm.get_current_state()
        except Exception:
            pass

        _STATE_REASON_MAP = {
            'correct_trending': 'CORRECT_RESONANCE',
            'correct_trending_defensive': 'CORRECT_DIVERGENCE',
            'incorrect_reversal': 'INCORRECT_REVERSAL',
            'other': 'OTHER_SCALP',
        }
        return _STATE_REASON_MAP.get(state, '')

    def check_position_risk(self) -> None:
        if not self._is_running:
            return
        if not self._trading_lock.acquire(blocking=False):
            logging.debug("[PositionRisk] 交易锁被占用，跳过本次风控检查")
            return
        try:
            self._ensure_position_service()
            if self._position_service:
                self._position_service.check_all_positions()
        except Exception as e:
            logging.error("[StrategyCoreService.check_position_risk] Error: %s", e)
        finally:
            self._trading_lock.release()

    def get_health_status(self) -> Dict[str, Any]:
        """V7新增：健康检查API（文档12.1节）

        任一组件状态非OK触发告警并暂停新开仓。
        """
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
            safety = get_safety_meta_layer()
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
            components["shadow_engine"] = shadow_health.get('status', 'ERROR')
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

        open_positions = 0
        try:
            self._ensure_position_service()
            if self._position_service:
                for inst_map in self._position_service.positions.values():
                    for rec in inst_map.values():
                        if rec.volume != 0:
                            open_positions += 1
        except Exception:
            pass

        current_state = 'unknown'
        try:
            spm = getattr(self, '_state_param_manager', None)
            if spm:
                current_state = spm.get_current_state()
        except Exception:
            pass

        last_signal_time = None
        try:
            if hasattr(self, '_signal_service') and self._signal_service:
                stats = self._signal_service.get_stats()
                last_signal_time = stats.get('last_signal_time')
                if last_signal_time:
                    last_signal_time = last_signal_time.strftime('%H:%M:%S')
        except Exception:
            pass

        any_non_ok = any(v not in ("OK", "UNAVAILABLE", "UNINITIALIZED") for v in components.values())

        health = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "components": components,
            "open_positions": open_positions,
            "last_signal_time": last_signal_time or "N/A",
            "circuit_breaker": "PAUSED" if any_non_ok else "INACTIVE",
            "current_state": current_state,
            "active_strategy": "master",
            "health": "DEGRADED" if any_non_ok else "OK",
        }

        try:
            se = getattr(self, '_shadow_engine', None)
            if se:
                shadow_stats = se.get_health_status()
                health["shadow_engine"] = shadow_stats
        except Exception:
            pass

        try:
            eco = getattr(self, '_strategy_ecosystem', None)
            if eco:
                eco_health = eco.get_health_status()
                health["strategy_ecosystem"] = eco_health
        except Exception:
            pass

        if any_non_ok:
            logging.warning("[HealthCheck] Degraded components: %s",
                          {k: v for k, v in components.items() if v not in ("OK", "UNAVAILABLE", "UNINITIALIZED")})

        return health


# 导出公共接口
__all__ = ['StrategyCoreService', 'StrategyState', 'Strategy2026']

# ============================================================================
# Strategy2026 - 策略主类（从 t_type_bootstrap.py 迁移至此）
# ============================================================================

from pythongo.base import BaseStrategy
from .ui_service import UIMixin


class StrategyParams:
    """策略参数容器 - 替代 type('Params', (), dict)() 匿名类

    提供可调试的 __repr__ 和 __dir__，方便排查参数问题。
    """

    __slots__ = ('_data',)

    def __init__(self, data: dict):
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

    def as_dict(self) -> dict:
        return dict(object.__getattribute__(self, '_data'))

    def get(self, key: str, default=None):
        return object.__getattribute__(self, '_data').get(key, default)


class Strategy2026(BaseStrategy, UIMixin):
    """策略 2026 主类 - 直接继承平台基类和 UI 混合类"""

    def __init__(self, *args, **kwargs):
        BaseStrategy.__init__(self)

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
            logging.warning(f"[Strategy2026] Failed to register to t_type_bootstrap: {e}")

        self.auto_trading_enabled = False
        self._callbacks_enabled = True
        self._stop_requested = False
        self._storage_warm_timer: Optional[threading.Timer] = None

        from ali2026v3_trading.scheduler_service import get_market_time_service
        self.market_time_service = get_market_time_service()

        logging.info("[Strategy2026] 已初始化，运行时配置延迟加载")
        logging.info(f"[Strategy2026] UI 功能已集成，继承自 UIMixin")

    def _log_warning(self, message: str) -> None:
        logging.warning(f"[Strategy2026] {message}")

    def _log_info(self, message: str) -> None:
        logging.info(f"[Strategy2026] {message}")

    def _log_error(self, message: str) -> None:
        logging.error(f"[Strategy2026] {message}")

    def _ensure_runtime_config_loaded(self) -> None:
        if self._config_loaded:
            return

        from ali2026v3_trading.config_service import get_cached_params

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
        self.params = StrategyParams(merged)
        self._config_loaded = True

        logging.info(f"[Strategy2026] params.strategy = {getattr(self.params, 'strategy', None)}")
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
                    logging.warning(f"[Strategy2026] storage 延迟预热启动失败: {exc}")

            timer = threading.Timer(delay_seconds, _warmup)
            timer.daemon = True
            self._storage_warm_timer = timer
            timer.start()
            logging.info("[Strategy2026] 已计划在 %.2fs 后启动 storage 后台预热", delay_seconds)
        except Exception as exc:
            logging.warning(f"[Strategy2026] 安排 storage 预热失败: {exc}")

    def is_market_open(self, exchange: str = 'AUTO') -> bool:
        from ali2026v3_trading.scheduler_service import is_market_open as _is_market_open
        from ali2026v3_trading.config_service import get_cached_params

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
                default_exchanges_raw = get_cached_params().get('exchanges', 'CFFEX,SHFE,DCE,CZCE,INE,GFEX')
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

        logging.info(f"[Strategy2026.onStart] New run_id={self._lifecycle_run_id}")

        self._callbacks_enabled = True
        self._stop_requested = False
        logging.info("[Strategy2026] 平台启动回调")

        _init_steps = {}
        _CRITICAL_STEPS = ('config_load', 'api_bind', 'core_init', 'core_start')

        try:
            self._ensure_runtime_config_loaded()
            _init_steps['config_load'] = True
        except Exception as e:
            _init_steps['config_load'] = False
            logging.error(f"[Strategy2026.onStart] CRITICAL: 加载运行时配置错误：{e}")
            logging.exception("[Strategy2026.onStart] 加载运行时配置堆栈:")

        try:
            if not getattr(self.strategy_core, '_api_ready', False):
                self.strategy_core.bind_platform_apis(self)
                logging.info("[Strategy2026.onStart] API未就绪，已补调bind_platform_apis")
            else:
                logging.info("[Strategy2026.onStart] API已就绪，跳过bind_platform_apis")
            _init_steps['api_bind'] = True
        except Exception as e:
            _init_steps['api_bind'] = False
            logging.error(f"[Strategy2026.onStart] CRITICAL: bind_platform_apis() 错误：{e}")
            logging.exception("[Strategy2026.onStart] bind_platform_apis() 堆栈:")

        try:
            self.trading = True
            self.update_status_bar()
            self.output("策略启动")
            _init_steps['status_set'] = True
        except Exception as e:
            _init_steps['status_set'] = False
            logging.error(f"[Strategy2026.onStart] 状态设置错误：{e}")
            logging.exception("[Strategy2026.onStart] 状态设置堆栈:")

        try:
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
                    logging.info(f"[Strategy2026] strategy_core 初始化结果：{init_result}")

                    is_running = getattr(self.strategy_core, '_is_running', False)
                    is_paused = getattr(self.strategy_core, '_is_paused', False)
                    logging.info(f"[Strategy2026] 初始化后状态：_is_running={is_running}, _is_paused={is_paused}")
                else:
                    logging.info(f"[Strategy2026] 无需调用 on_init，当前状态为：{current_state}")
            _init_steps['core_init'] = True
        except Exception as e:
            _init_steps['core_init'] = False
            logging.error(f"[Strategy2026.onStart] CRITICAL: strategy_core 初始化阶段错误：{e}")
            logging.exception("[Strategy2026.onStart] strategy_core 初始化阶段堆栈:")

        try:
            market_open = self.is_market_open()
            logging.info(f"[Strategy2026] 市场开盘状态：{market_open}")
            _init_steps['market_check'] = True
        except Exception as e:
            _init_steps['market_check'] = False
            logging.error(f"[Strategy2026.onStart] 市场状态检查错误：{e}")
            logging.exception("[Strategy2026.onStart] 市场状态检查堆栈:")

        try:
            logging.info("[Strategy2026] 正在启动 UI 界面...")
            self._start_output_mode_ui()
            logging.info("[Strategy2026] UI 界面启动成功")
            _init_steps['ui_start'] = True
        except Exception as e:
            _init_steps['ui_start'] = False
            logging.error(f"[Strategy2026.onStart] 启动 UI 错误：{e}")
            logging.exception("[Strategy2026.onStart] 启动 UI 堆栈:")

        try:
            if hasattr(self.strategy_core, 'start'):
                result = self.strategy_core.start()
                logging.info(f"[Strategy2026] 策略核心启动结果：{result}")
            else:
                logging.warning("[Strategy2026.onStart] strategy_core.start 不可用，跳过")
            _init_steps['core_start'] = True
        except Exception as e:
            _init_steps['core_start'] = False
            logging.error(f"[Strategy2026.onStart] CRITICAL: strategy_core.start() 错误：{e}")
            logging.exception("[Strategy2026.onStart] strategy_core.start() 堆栈:")

        _failed_critical = [s for s in _CRITICAL_STEPS if not _init_steps.get(s, False)]
        _failed_all = [s for s, ok in _init_steps.items() if not ok]
        if _failed_critical:
            logging.error(
                f"[Strategy2026.onStart] CRITICAL STEPS FAILED: {_failed_critical}, "
                f"all results: {_init_steps}. Strategy may be in DEGRADED state."
            )
            if hasattr(self.strategy_core, 'transition_to'):
                try:
                    self.strategy_core.transition_to(StrategyState.DEGRADED)
                except Exception:
                    pass
        elif _failed_all:
            logging.warning(
                f"[Strategy2026.onStart] Non-critical steps failed: {_failed_all}, "
                f"all results: {_init_steps}. Strategy running with degraded features."
            )
        else:
            logging.info(f"[Strategy2026.onStart] All steps succeeded: {_init_steps}")

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
                logging.info(f"[Strategy2026.onInit] strategy_core 初始化结果：{result}")

            self._init_pending = False

            super().on_init()
            self._schedule_storage_warmup()

            return None
        except Exception as e:
            logging.error(f"[Strategy2026.onInit] 错误：{e}")
            logging.info(f"[PROBE_PARAMS_MAP] exchange={getattr(self.params_map, 'exchange', 'N/A')}")
            logging.info(f"[PROBE_PARAMS_MAP] instrument_id={getattr(self.params_map, 'instrument_id', 'N/A')}")
            logging.info(f"[PROBE_PARAMS_MAP] exchange_list={self.exchange_list[:10] if self.exchange_list else 'empty'}")
            logging.info(f"[PROBE_PARAMS_MAP] instrument_list={self.instrument_list[:10] if self.instrument_list else 'empty'}")

            logging.exception("[Strategy2026.onInit] 堆栈:")
            self._init_error = e
            self._init_pending = False
            return None

    def onStop(self):
        with self._lifecycle_lock:
            if self._stop_executed:
                logging.warning(
                    f"[Strategy2026.onStop] DUPLICATE ENTRY DETECTED! "
                    f"run_id={self._lifecycle_run_id}, skipping duplicate execution"
                )
                return None

            self._stop_executed = True

        logging.info(f"[Strategy2026.onStop] Stopped (run_id={self._lifecycle_run_id})")

        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onStop] enter")

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

        try:
            from pythongo import utils as _utils
            self.trading = False
            _utils.Scheduler("PythonGO").stop()
            self.save_instance_file()
            logging.info("[Strategy2026.onStop] BaseStrategy non-INFINIGO steps completed")
        except Exception as e:
            logging.error(f"[Strategy2026.onStop] BaseStrategy cleanup failed: {e}")

        return None

    def internal_pause_strategy(self) -> bool:
        result = False
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'pause'):
                result = bool(self.strategy_core.pause())
        except Exception as e:
            logging.error(f"[Strategy2026.internal_pause_strategy] 错误：{e}")
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
                logging.warning(f"[Strategy2026.internal_resume_strategy] Cannot resume in state: {current_state}")
        except Exception as e:
            logging.error(f"[Strategy2026.internal_resume_strategy] 错误：{e}")
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
            logging.warning(f"[Strategy2026.onDestroy] destroy UI failed: {e}")

    def onTick(self, tick):
        """平台 Tick 数据回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            super().on_tick(tick)
        except Exception as e:
            logging.error(f"[Strategy2026.onTick] super().on_tick() 错误：{e}")

        try:
            self._log_tick_summary(tick)

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
