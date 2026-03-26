"""
策略核心服务模块 - CQRS 架构 Core 层
来源：13_strategy_core.py + 15_diagnosis.py (部分)
功能：策略生命周期管理 + 状态监控 + 诊断服务
行数：~800 行 (-43% vs 原 1400 行)

优化 v1.1 (2026-03-16):
- ✅ 集成 EventBus 发布策略事件
- ✅ 添加健康检查接口
- ✅ 性能监控仪表板支持
"""
from __future__ import annotations

import os
import sys
import inspect
import importlib.util

# Add demo directory to sys.path to ensure ali2026v3_trading can be imported
demo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if demo_dir not in sys.path:
    sys.path.insert(0, demo_dir)

import threading
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from functools import wraps

from ali2026v3_trading.market_data_service import is_market_open, MarketDataService
from ali2026v3_trading.analytics_service import AnalyticsService


_STORAGE_MANAGER_CLASS = None


def _get_storage_manager_class():
    """Resolve InstrumentDataManager from the package-local storage.py deterministically."""
    global _STORAGE_MANAGER_CLASS

    if _STORAGE_MANAGER_CLASS is not None:
        return _STORAGE_MANAGER_CLASS

    storage_path = os.path.join(os.path.dirname(__file__), 'storage.py')
    module_name = 'ali2026v3_trading._storage_runtime'

    importlib.invalidate_caches()
    module = sys.modules.get(module_name)

    if module is None or os.path.abspath(getattr(module, '__file__', '')) != os.path.abspath(storage_path):
        spec = importlib.util.spec_from_file_location(module_name, storage_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load storage module spec from {storage_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

    storage_manager_class = getattr(module, 'InstrumentDataManager', None)
    if storage_manager_class is None:
        raise ImportError(f"InstrumentDataManager not found in {storage_path}")

    init_signature = inspect.signature(storage_manager_class.__init__)
    if 'db_path' not in init_signature.parameters:
        raise TypeError(
            f"Resolved InstrumentDataManager from {storage_path} has unexpected signature {init_signature}"
        )

    _STORAGE_MANAGER_CLASS = storage_manager_class
    return _STORAGE_MANAGER_CLASS


# ============================================================================
# P2 功能恢复：PathCounter（从 utils_service.py 整合至此）
# ============================================================================

class PathCounter:
    """路径计数器（整合自 utils_service.py）"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._counters: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._enabled = True
        self._start_time = time.time()
        self._initialized = True
    
    @classmethod
    def enable(cls) -> None:
        counter = cls()
        counter._enabled = True
    
    @classmethod
    def disable(cls) -> None:
        counter = cls()
        counter._enabled = False
    
    @classmethod
    def is_enabled(cls) -> bool:
        counter = cls()
        return counter._enabled
    
    @classmethod
    def record_call(cls, path: str, execution_time: float = 0.0, exception: Optional[Exception] = None) -> None:
        counter = cls()
        if not counter._enabled:
            return
        
        with counter._lock:
            if path not in counter._counters:
                counter._counters[path] = {
                    'count': 0,
                    'total_time': 0.0,
                    'errors': 0,
                    'last_error': None
                }
            
            data = counter._counters[path]
            data['count'] += 1
            data['total_time'] += execution_time
            
            if exception is not None:
                data['errors'] += 1
                data['last_error'] = str(exception)
    
    @classmethod
    def export_to_json(cls, filepath: str) -> None:
        counter = cls()
        with counter._lock:
            data = {
                'start_time': datetime.fromtimestamp(counter._start_time).isoformat(),
                'uptime_seconds': time.time() - counter._start_time,
                'counters': {}
            }
            
            for path, stats in counter._counters.items():
                avg_time = (stats['total_time'] / stats['count']) if stats['count'] > 0 else 0
                data['counters'][path] = {
                    'count': stats['count'],
                    'total_time': stats['total_time'],
                    'avg_time': avg_time,
                    'errors': stats['errors'],
                    'last_error': stats['last_error']
                }
            
            import json
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
    
    @classmethod
    def print_summary(cls) -> None:
        counter = cls()
        with counter._lock:
            print(f"\n{'='*60}")
            print(f"Path Counter Summary (Uptime: {time.time() - counter._start_time:.1f}s)")
            print(f"{'='*60}")
            
            sorted_paths = sorted(
                counter._counters.items(),
                key=lambda x: x[1]['count'],
                reverse=True
            )[:20]
            
            for path, stats in sorted_paths:
                avg_time = (stats['total_time'] / stats['count']) if stats['count'] > 0 else 0
                error_flag = f" ({stats['errors']} errors)" if stats['errors'] > 0 else ""
                print(f"{stats['count']:5d} calls | {avg_time*1000:7.3f}ms avg | {path}{error_flag}")
            
            print(f"{'='*60}\n")


def count_path(path: Optional[str] = None):
    """装饰器：记录函数调用（整合自 utils_service.py）"""
    def decorator(func: Callable):
        call_path = path or f"{func.__module__}.{func.__name__}"
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not PathCounter.is_enabled():
                return func(*args, **kwargs)
            
            start_time = time.time()
            exception = None
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                exception = e
                raise
            finally:
                execution_time = time.time() - start_time
                PathCounter.record_call(call_path, execution_time, exception)
            
            return result
        return wrapper
    return decorator


def count_method(path: Optional[str] = None):
    """装饰器：记录方法调用（整合自 utils_service.py）"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not PathCounter.is_enabled():
                return func(self, *args, **kwargs)
            
            call_path = path or f"{type(self).__name__}.{func.__name__}"
            start_time = time.time()
            exception = None
            try:
                result = func(self, *args, **kwargs)
            except Exception as e:
                exception = e
                raise
            finally:
                execution_time = time.time() - start_time
                PathCounter.record_call(call_path, execution_time, exception)
            
            return result
        return wrapper
    return decorator


class StrategyState(Enum):
    """策略状态枚举"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class StrategyCoreService:
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
        
        # 线程锁
        self._lock = threading.RLock()
        self._scheduler_lock = threading.RLock()
        
        # 调度器
        self._scheduler = None
        
        # 性能统计
        self._stats = {
            'start_time': None,
            'total_ticks': 0,
            'total_trades': 0,
            'total_signals': 0,
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': None
        }
        
        # Tick 计数器（用于 on_tick 方法）
        self._tick_count = 0
        
        # ✅ Tick 日志时间控制（30 秒汇总输出）
        self._last_tick_log_time = time.time()
        self._tick_log_interval = 30.0  # 30 秒
        
        # EventBus 集成
        self._event_bus = event_bus

        # 运行时平台 API 绑定（在 Strategy2026.onInit/onStart 中刷新）
        self.subscribe = None
        self.unsubscribe = None
        self.get_instrument = None
        self.get_kline_data = None

        # 运行时分析链路
        self.market_data_service = None
        self.analytics_service = None
        self.option_width_results: Dict[str, Dict[str, Any]] = {}
        self._latest_prices: Dict[str, float] = {}
        self._futures_instruments: List[Any] = []
        self._option_instruments: Dict[str, List[Any]] = {}
        self._future_ids: set[str] = set()
        self._option_ids: set[str] = set()
        self._historical_load_started = False
        self._subscribed_instruments: List[str] = []
        self._hkl_diag_emitted = False
    
        # 初始化存储服务
        self._init_storage()
    
    # ========== 生命周期管理 ==========

    def _init_storage(self) -> None:
        """初始化存储服务（延迟初始化，避免启动阻塞）"""
        max_retries = 3
        retry_delay = 1.0  # 秒
        
        for attempt in range(max_retries):
            try:
                database_manager_class = _get_storage_manager_class()
                
                # Get database path - use module-level default (avoid self dependency during __init__)
                import os
                db_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    'trading_data.db'
                )
                
                # 确保 db_path 存在
                os.makedirs(os.path.dirname(db_path), exist_ok=True)

                logging.info(
                    "[StrategyCoreService] Storage manager resolved from: %s",
                    getattr(database_manager_class, '__module__', '<unknown>')
                )
                logging.info(
                    "[StrategyCoreService] Storage manager init signature: %s",
                    inspect.signature(database_manager_class.__init__)
                )
                
                # 直接传递位置参数而不是关键字参数
                self._storage = database_manager_class(db_path)
                # 验证storage是否可用
                test_key = f"storage_test_{self.strategy_id}"
                test_data = {"test": True, "timestamp": time.time()}
                success = self._storage.save(test_key, test_data)
                if not success:
                    raise RuntimeError("Storage save test failed")
                # 测试数据会自动覆盖，无需清理
                logging.info(f"[StrategyCoreService] Storage service initialized (attempt {attempt + 1}/{max_retries})")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"[StrategyCoreService] Storage init failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logging.error(f"[StrategyCoreService] Failed to initialize storage after {max_retries} attempts: {e}")
                    raise RuntimeError(f"Storage initialization failed: {e}")

    @staticmethod
    def _get_instrument_value(instrument: Any, *keys: str) -> str:
        """兼容 dict/对象 两种合约结构读取字段。"""
        for key in keys:
            if isinstance(instrument, dict):
                value = instrument.get(key, '')
            else:
                value = getattr(instrument, key, '')

            if value:
                return str(value).strip()
        return ''

    @staticmethod
    def _split_exchange_instrument(instrument_code: str) -> tuple[str, str]:
        """把 EXCHANGE.INSTRUMENT 形式拆成交易所和合约代码。"""
        if '.' not in instrument_code:
            raise ValueError(f"Invalid instrument code: {instrument_code}")

        exchange, instrument_id = instrument_code.split('.', 1)
        exchange = str(exchange).strip().upper()
        instrument_id = str(instrument_id).strip()
        if not exchange or not instrument_id:
            raise ValueError(f"Invalid instrument code: {instrument_code}")
        return exchange, instrument_id

    def bind_platform_apis(self, strategy_obj: Any) -> None:
        """把平台运行时 API 重新绑定到 StrategyCoreService。"""
        runtime_sub_market_data = getattr(strategy_obj, 'sub_market_data', None)
        runtime_unsub_market_data = getattr(strategy_obj, 'unsub_market_data', None)

        if callable(runtime_sub_market_data):
            def _subscribe(instrument_code: str) -> None:
                exchange, instrument_id = self._split_exchange_instrument(instrument_code)
                runtime_sub_market_data(exchange=exchange, instrument_id=instrument_id)

            self.subscribe = _subscribe
        else:
            self.subscribe = None

        if callable(runtime_unsub_market_data):
            def _unsubscribe(instrument_code: str) -> None:
                exchange, instrument_id = self._split_exchange_instrument(instrument_code)
                runtime_unsub_market_data(exchange=exchange, instrument_id=instrument_id)

            self.unsubscribe = _unsubscribe
        else:
            self.unsubscribe = None

        self.get_instrument = getattr(strategy_obj, 'get_instrument', None)
        self.get_kline_data = getattr(strategy_obj, 'get_kline_data', None)
        self.get_kline = getattr(strategy_obj, 'get_kline', None)

        logging.info(
            "[StrategyCoreService.bind_platform_apis] "
            f"subscribe={callable(self.subscribe)}, "
            f"unsubscribe={callable(self.unsubscribe)}, "
            f"get_instrument={callable(self.get_instrument)}, "
            f"get_kline_data={callable(self.get_kline_data)}, "
            f"get_kline={callable(self.get_kline)}"
        )

        self._inject_runtime_context(strategy_obj)

        if self.market_data_service is not None:
            try:
                runtime_market_center = getattr(strategy_obj, 'market_center', None)
                if runtime_market_center is not None:
                    self.market_data_service.set_market_center(runtime_market_center)
                else:
                    self.market_data_service.set_market_center(strategy_obj)
            except Exception:
                pass

    def _inject_runtime_context(self, strategy_obj: Any) -> None:
        """把运行时 strategy / market_center 注回参数对象，供 MarketDataService 取数。"""
        if strategy_obj is None:
            return

        params = getattr(self, 'params', None)
        market_center = getattr(strategy_obj, 'market_center', None)

        try:
            if isinstance(params, dict):
                params['strategy'] = strategy_obj
                if market_center is not None:
                    params['market_center'] = market_center
            elif params is not None:
                setattr(params, 'strategy', strategy_obj)
                if market_center is not None:
                    setattr(params, 'market_center', market_center)
        except Exception as e:
            logging.debug(f"[StrategyCoreService._inject_runtime_context] Failed to update params: {e}")

    def _build_future_to_option_map(self,
                                    futures_list: List[Any],
                                    options_dict: Dict[str, List[Any]]) -> Dict[str, str]:
        """基于已加载的月份合约推导期货品种到期权品种映射。"""
        mapping: Dict[str, str] = {
            'IF': 'IO',
            'IH': 'HO',
            'IM': 'MO',
        }

        option_keys = [str(key).strip().upper() for key in (options_dict or {}).keys() if str(key).strip()]
        option_by_suffix: Dict[str, set[str]] = {}
        for opt_key in option_keys:
            try:
                product, suffix = self._split_symbol_product_suffix(opt_key)
            except ValueError:
                continue
            option_by_suffix.setdefault(suffix, set()).add(product)

        for inst in futures_list or []:
            future_id = self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id').upper()
            if not future_id:
                continue
            try:
                product, suffix = self._split_symbol_product_suffix(future_id)
            except ValueError:
                continue

            if product in mapping:
                continue

            if any(key.startswith(product) for key in option_keys):
                mapping[product] = product
                continue

            candidates = option_by_suffix.get(suffix, set())
            if len(candidates) == 1:
                mapping[product] = next(iter(candidates))

        return mapping

    @staticmethod
    def _split_symbol_product_suffix(symbol: str) -> tuple[str, str]:
        """拆分品种前缀和月份后缀，如 IF2604 -> (IF, 2604)。"""
        import re

        normalized = str(symbol or '').strip().upper()
        match = re.match(r'^([A-Z]+)(\d{3,4})$', normalized)
        if not match:
            raise ValueError(f'Invalid month symbol: {symbol}')
        return match.group(1), match.group(2)

    def _init_analytics_services(self, params: Any) -> None:
        """初始化运行时行情和分析服务。"""
        try:
            self.storage = getattr(self, '_storage', None)
            logging.info(f"[StrategyCoreService._init_analytics_services] storage = {self.storage}")
            
            # 获取合约数据（已加载）
            futures_instruments = getattr(self, '_futures_instruments', [])
            option_instruments = getattr(self, '_option_instruments', {})
            
            # 获取策略实例
            runtime_strategy_instance = None
            if isinstance(params, dict):
                runtime_strategy_instance = params.get('strategy') or params.get('strategy_instance')
            else:
                runtime_strategy_instance = getattr(params, 'strategy', None) or getattr(params, 'strategy_instance', None)
            
            logging.info(f"[InitServices] futures_instruments count: {len(futures_instruments)}")
            logging.info(f"[InitServices] option_instruments groups: {len(option_instruments)}")
            logging.info(f"[InitServices] strategy_instance: {runtime_strategy_instance is not None}")
            
            # 显式传递合约数据和策略实例
            self.market_data_service = MarketDataService(
                params, 
                self.storage, 
                logging.info,
                futures_instruments=futures_instruments,
                option_instruments=option_instruments,
                strategy_instance=runtime_strategy_instance
            )
            logging.info(f"[StrategyCoreService._init_analytics_services] market_data_service = {self.market_data_service}")
            self._historical_load_started = False

            runtime_market_center = None
            if isinstance(params, dict):
                runtime_market_center = params.get('market_center') or params.get('strategy')
            else:
                runtime_market_center = getattr(params, 'market_center', None) or getattr(params, 'strategy', None)

            if runtime_market_center is not None:
                self.market_data_service.set_market_center(runtime_market_center)
            elif callable(self.get_kline_data):
                self.market_data_service.set_market_center(self)

            self.analytics_service = AnalyticsService(params, self.market_data_service, logging.info)
            self.analytics_service.set_option_instruments(getattr(self, '_option_instruments', {}) or {})
            self.analytics_service.set_future_to_option_map(
                self._build_future_to_option_map(
                    getattr(self, '_futures_instruments', []) or [],
                    getattr(self, '_option_instruments', {}) or {},
                )
            )

            self._future_ids = {
                self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id').upper()
                for inst in (getattr(self, '_futures_instruments', []) or [])
                if self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id')
            }
            self._option_ids = {
                self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id').upper()
                for contracts in (getattr(self, '_option_instruments', {}) or {}).values()
                for inst in (contracts or [])
                if self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id')
            }

            logging.info(
                "[AnalyticsInit] "
                f"futures={len(self._future_ids)}, option_groups={len(self._option_instruments)}, "
                f"option_contracts={len(self._option_ids)}, mapping={self.analytics_service.future_to_option_map}"
            )
        except Exception as e:
            logging.error(f"[StrategyCoreService._init_analytics_services] Error: {e}", exc_info=True)
            self.market_data_service = None
            self.analytics_service = None

    def _start_historical_kline_load(self) -> None:
        """在真实订阅完成后启动后台历史K线加载。"""
        try:
            if self._historical_load_started:
                logging.info("[HKL] Historical loader already started, skip duplicate start")
                return

            if self.market_data_service is None:
                logging.warning("[StrategyCoreService] MarketDataService unavailable, skipping historical kline load")
                logging.warning("[HKL] Historical loader blocked: market_data_service is None")
                return

            mc = self.market_data_service._get_market_center()
            if mc is None:
                logging.warning("[StrategyCoreService] Market center unavailable, skipping historical kline load")
                logging.warning("[HKL] Historical loader blocked: market_center is None")
                return

            threading.Thread(
                target=self.market_data_service._async_load_historical_klines,
                daemon=True,
                name="history-kline-loader",
            ).start()
            self._historical_load_started = True
            logging.info("[StrategyCoreService] Started background historical kline load")
            logging.info("[HKL] Historical loader thread started")
        except Exception as e:
            logging.error(f"[StrategyCoreService._start_historical_kline_load] Error: {e}", exc_info=True)

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
            # 允许从 INITIALIZING 或 ERROR 状态进行初始化
            if self._state not in (StrategyState.INITIALIZING, StrategyState.ERROR):
                logging.warning(f"[StrategyCoreService.on_init] Cannot initialize in state: {self._state}")
                return False
                
            try:
                logging.info("[StrategyCoreService.on_init] Initializing...")
                    
                # 保存初始化参数，供后续使用
                self._init_kwargs = kwargs
                self.params = kwargs.get('params')
                    
                # 初始化日志
                self._init_logging(kwargs.get('params'))
                self._init_scheduler()
                
                # ========== 加载期货和期权合约 ==========
                try:
                    database_manager_class = _get_storage_manager_class()
                    
                    # 获取 params 对象
                    params = kwargs.get('params')
                    
                    # 修复：确保开启全量模式，加载所有品种
                    if params is not None:
                        if isinstance(params, dict):
                            params['load_all_products'] = True
                            params['enable_all_products'] = True
                            logging.info("[InitParams] Set load_all_products=True in dict params")
                        else:
                            setattr(params, 'load_all_products', True)
                            setattr(params, 'enable_all_products', True)
                            logging.info("[InitParams] Set load_all_products=True in object params")

                    # 尽量传入运行时策略实例，便于 load_all_instruments 在 OptionChain 不可用时走 market_center 兜底。
                    runtime_strategy_instance = None
                    if isinstance(params, dict):
                        runtime_strategy_instance = params.get('strategy') or params.get('strategy_instance')
                    else:
                        runtime_strategy_instance = getattr(params, 'strategy', None) or getattr(params, 'strategy_instance', None)
                    
                    # 加载所有合约信息
                    futures_list, options_dict = database_manager_class.load_all_instruments(
                        strategy_instance=runtime_strategy_instance,
                        params=params,
                        logger=logging.getLogger(__name__)
                    )
                    
                    # 存储到实例属性，供 MarketDataService 使用
                    self._futures_instruments = futures_list
                    self._option_instruments = options_dict

                    # 同时注入到 params 对象，确保 MarketDataService 可以访问
                    # 修复：根据 params 类型选择正确的注入方式
                    if params is not None:
                        if isinstance(params, dict):
                            # 字典使用键值赋值
                            params['future_instruments'] = futures_list
                            params['option_instruments'] = options_dict
                            logging.info(f"[Instruments] Injected into dict params: {len(futures_list)} futures, {sum(len(opts) for opts in options_dict.values())} options")
                            # 验证注入成功
                            assert 'future_instruments' in params, "future_instruments not in params after injection!"
                            assert 'option_instruments' in params, "option_instruments not in params after injection!"
                        else:
                            # 对象使用 setattr
                            setattr(params, 'future_instruments', futures_list)
                            setattr(params, 'option_instruments', options_dict)
                            logging.info(f"[Instruments] Injected into object params: {len(futures_list)} futures, {sum(len(opts) for opts in options_dict.values())} options")
                            # 验证注入成功
                            assert hasattr(params, 'future_instruments'), "future_instruments not in params after injection!"
                            assert hasattr(params, 'option_instruments'), "option_instruments not in params after injection!"
                    
                    # 输出加载结果
                    total_options = sum(len(opts) for opts in options_dict.values()) if options_dict else 0
                    logging.info(
                        f"[Instruments] Loaded {len(futures_list)} futures and {total_options} options "
                        f"across {len(options_dict)} underlying symbols"
                    )
                    
                except Exception as e:
                    logging.error(f"[Instruments] Failed to load instruments: {e}", exc_info=True)
                    self._futures_instruments = []
                    self._option_instruments = {}

                self._init_analytics_services(self.params)
                # ======================================
                
                # 更新状态
                self._state = StrategyState.RUNNING
                self._is_running = True
                self._stats['start_time'] = datetime.now()
                
                logging.info(f"[StrategyCoreService.on_init] Initialized: {self.strategy_id}")
                
                # 发布事件
                self._publish_event('StrategyInitialized', {
                    'strategy_id': self.strategy_id,
                    'timestamp': datetime.now().isoformat()
                })
                
                return True
                
            except Exception as e:
                logging.error(f"[StrategyCoreService.on_init] Failed: {e}")
                self._state = StrategyState.ERROR
                self._stats['errors_count'] += 1
                self._stats['last_error_time'] = datetime.now()
                self._stats['last_error_message'] = str(e)
                return False
    
    def on_start(self) -> bool:
        """策略启动回调 - 对应旧 StrategyCore.on_start (L946)
        
        Returns:
            bool: 启动是否成功
        """
        with self._lock:
            if self._state not in (StrategyState.RUNNING, StrategyState.PAUSED):
                logging.warning(f"[StrategyCoreService.on_start] Cannot start in state: {self._state}")
                return False
            
            if self._is_paused:
                self._is_paused = False
                self._state = StrategyState.RUNNING
            
            self._is_running = True
            
            logging.info(f"[StrategyCoreService.on_start] Started: {self.strategy_id}")
            
            # ========== 订阅合约数据 ==========
            try:
                self._subscribe_instruments()
                self._start_historical_kline_load()
            except Exception as e:
                logging.error(f"[StrategyCoreService.on_start] Subscription failed: {e}")
            # ==================================
            
            # 发布事件
            self._publish_event('StrategyStarted', {
                'strategy_id': self.strategy_id
            })
            
            return True
    
    def _subscribe_instruments(self) -> None:
        """订阅合约数据（强制全量订阅，无回退逻辑）"""
        try:
            # 尝试从多个来源获取参数
            params = None

            # 1. 首先尝试 self.params（如果存在）
            if hasattr(self, 'params'):
                params = self.params
            # 2. 尝试从 kwargs 或 config 中获取
            elif hasattr(self, '_init_kwargs') and 'params' in self._init_kwargs:
                params = self._init_kwargs['params']
            # 3. 使用默认配置
            else:
                logging.debug("[StrategyCoreService._subscribe_instruments] Using default config")
                # 创建一个临时的配置对象
                from ali2026v3_trading.config_service import get_cached_params
                params = type('Params', (), get_cached_params())()

            subscribe_list = []

            # 获取 on_init 阶段已加载的真实合约清单
            futures_instruments = getattr(self, '_futures_instruments', []) or []
            option_instruments = getattr(self, '_option_instruments', {}) or {}

            # 强制要求必须加载到真实合约数据
            if not futures_instruments and not option_instruments:
                logging.error("[Subscribe] 严重错误：未能加载到任何合约数据！")
                logging.error("[Subscribe] 可能原因：")
                logging.error("  1. DatabaseManager.load_all_instruments() 失败")
                logging.error("  2. 数据库为空或连接失败")
                logging.error("  3. 合约数据注入失败")
                logging.error("[Subscribe] 策略将无法正常运行，请检查日志！")

                # 尝试重新加载
                logging.warning("[Subscribe] 尝试重新加载合约数据...")
                try:
                    database_manager_class = _get_storage_manager_class()
                    runtime_strategy_instance = None
                    if isinstance(params, dict):
                        runtime_strategy_instance = params.get('strategy') or params.get('strategy_instance')
                    else:
                        runtime_strategy_instance = getattr(params, 'strategy', None) or getattr(params, 'strategy_instance', None)

                    futures_list, options_dict = database_manager_class.load_all_instruments(
                        strategy_instance=runtime_strategy_instance,
                        params=params,
                        logger=logging.getLogger(__name__)
                    )

                    if futures_list:
                        self._futures_instruments = futures_list
                        self._option_instruments = options_dict
                        futures_instruments = futures_list
                        option_instruments = options_dict
                        logging.info(f"[Subscribe] 重新加载成功：{len(futures_list)} 期货，{sum(len(opts) for opts in options_dict.values())} 期权")
                    else:
                        logging.error("[Subscribe] 重新加载失败，合约列表仍为空！")
                        return
                except Exception as e:
                    logging.error(f"[Subscribe] 重新加载异常：{e}")
                    return

            # 订阅期货合约
            for inst in futures_instruments:
                exchange = self._get_instrument_value(inst, 'ExchangeID', 'exchange', '_exchange').upper()
                instrument_id = self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id')
                if exchange and instrument_id:
                    subscribe_list.append(f"{exchange}.{instrument_id}")

            # 订阅期权合约（如果启用）
            if getattr(params, 'subscribe_options', True):
                for contracts in option_instruments.values():
                    for inst in contracts or []:
                        exchange = self._get_instrument_value(inst, 'ExchangeID', 'exchange', '_exchange').upper()
                        instrument_id = self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id')
                        if exchange and instrument_id:
                            subscribe_list.append(f"{exchange}.{instrument_id}")

            # 去重，保持顺序
            seen = set()
            subscribe_list = [inst for inst in subscribe_list if not (inst in seen or seen.add(inst))]

            # 获取交易所过滤配置
            exchanges_raw = getattr(params, 'exchanges', '')
            allowed_exchanges = set()
            if isinstance(exchanges_raw, (list, tuple, set)):
                allowed_exchanges = {str(e).strip().upper() for e in exchanges_raw if str(e).strip()}
            elif exchanges_raw:
                allowed_exchanges = {e.strip().upper() for e in str(exchanges_raw).split(',') if e.strip()}

            # 如果没有指定交易所，使用已加载合约的交易所
            if not allowed_exchanges:
                loaded_exchanges = {
                    inst.split('.', 1)[0].strip().upper()
                    for inst in subscribe_list
                    if '.' in inst and inst.split('.', 1)[0].strip()
                }
                allowed_exchanges = loaded_exchanges

            # 应用交易所过滤
            if allowed_exchanges:
                subscribe_list = [inst for inst in subscribe_list if inst.split('.', 1)[0] in allowed_exchanges]

            self._subscribed_instruments = list(subscribe_list)

            # 统计信息
            futures_count = len(futures_instruments)
            options_count = sum(len(opts) for opts in option_instruments.values()) if option_instruments else 0
            total_options_groups = len(option_instruments)

            logging.info(
                f"[Subscribe] 全量订阅模式："
                f"期货={futures_count} 个，"
                f"期权={options_count} 个（{total_options_groups} 组），"
                f"总计={len(subscribe_list)} 个合约"
            )
            logging.info(f"[Subscribe] 交易所过滤：{sorted(allowed_exchanges)}")
            if subscribe_list:
                logging.info(f"[Subscribe] 示例合约：{', '.join(subscribe_list[:10])}{'...' if len(subscribe_list) > 10 else ''}")

            # 验证平台 API 绑定
            if not callable(self.subscribe):
                raise RuntimeError("StrategyCoreService.subscribe is not callable; platform API binding is missing")

            # 调用平台订阅接口
            success_count = 0
            failed_count = 0
            failed_instruments = []

            logging.info(f"[Subscribe] 开始订阅 {len(subscribe_list)} 个合约...")
            for idx, inst in enumerate(subscribe_list, 1):
                try:
                    self.subscribe(inst)
                    success_count += 1
                    # 每 50 个合约输出一次进度
                    if idx % 50 == 0 or idx == len(subscribe_list):
                        logging.info(f"[Subscribe] 进度：{idx}/{len(subscribe_list)} - {inst}")
                    logging.debug(f"[Subscribe] 已订阅：{inst}")
                except Exception as e:
                    failed_count += 1
                    failed_instruments.append(inst)
                    logging.warning(f"[Subscribe] 订阅失败 {inst}: {e}")

            # 按交易所统计
            by_exchange = {}
            for inst in subscribe_list:
                exch = inst.split('.', 1)[0] if '.' in inst else 'UNKNOWN'
                by_exchange[exch] = by_exchange.get(exch, 0) + 1

            logging.info(
                f"[Subscribe] 订阅完成：总计={len(subscribe_list)}，成功={success_count}，失败={failed_count}，"
                f"交易所分布={by_exchange}"
            )
            if failed_instruments:
                logging.warning(
                    f"[Subscribe] 失败合约示例：{', '.join(failed_instruments[:20])}"
                    f"{'...' if len(failed_instruments) > 20 else ''}"
                )

            # 验证订阅成功率
            if failed_count > len(subscribe_list) * 0.1:  # 失败率超过10%
                logging.error(f"[Subscribe] 警告：订阅失败率过高 ({failed_count/len(subscribe_list)*100:.1f}%)，可能影响数据接收！")

        except Exception as e:
            logging.error(f"[StrategyCoreService._subscribe_instruments] 订阅过程异常：{e}")
            logging.exception("[StrategyCoreService._subscribe_instruments] 堆栈：")

    def _unsubscribe_all_instruments(self) -> None:
        """停止时取消全部已订阅合约，避免平台继续向本实例推送回调。"""
        try:
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
        try:
            storage_obj = getattr(self, '_storage', None)
            if storage_obj and hasattr(storage_obj, 'shutdown'):
                storage_obj.shutdown(flush=False)
        except Exception as e:
            logging.warning(f"[StrategyCoreService._shutdown_runtime_services] Storage shutdown failed: {e}")

        self._historical_load_started = False
    
    def on_stop(self) -> bool:
        """策略停止回调 - 对应旧 StrategyCore.on_stop (L1357)
        
        Returns:
            bool: 停止是否成功
        """
        with self._lock:
            if self._state == StrategyState.STOPPED:
                logging.warning(f"[StrategyCoreService.on_stop] Already stopped: {self.strategy_id}")
                return True
            
            try:
                logging.info(f"[StrategyCoreService.on_stop] Stopping: {self.strategy_id}")

                # 先落状态，尽快让 on_tick 入口短路
                self._is_running = False
                self._is_paused = True
                
                # 停止调度器
                self._stop_scheduler()

                # 取消订阅，避免平台继续推送回调
                self._unsubscribe_all_instruments()

                # 停止运行时服务
                self._shutdown_runtime_services()
                
                # 更新状态
                self._state = StrategyState.STOPPED
                
                logging.info(f"[StrategyCoreService.on_stop] Stopped: {self.strategy_id}")
                
                # 发布事件
                self._publish_event('StrategyStopped', {
                    'strategy_id': self.strategy_id
                })
                
                return True
                
            except Exception as e:
                logging.error(f"[StrategyCoreService.on_stop] Failed: {e}")
                return False
    
    def on_tick(self, tick: Any) -> None:
        """Tick 数据回调 - 对应旧 StrategyCore.on_tick (L1421)
        
        Args:
            tick: Tick 数据对象
        """
        # 添加状态检查日志
        if not self._is_running:
            logging.warning(f"[StrategyCoreService.on_tick] 策略未运行(_is_running=False)，丢弃Tick数据")
            return

        if self._is_paused:
            logging.warning(f"[StrategyCoreService.on_tick] 策略已暂停(_is_paused=True)，丢弃Tick数据")
            return

        if not self._hkl_diag_emitted:
            self._hkl_diag_emitted = True
            
            # 获取 K 线统计
            kline_stats = {}
            if self.market_data_service:
                try:
                    kline_stats = self.market_data_service.get_kline_stats()
                except Exception as e:
                    logging.warning(f"[HKL] Failed to get kline stats: {e}")
            
            logging.info(
                "[HKL] First tick diagnostics: "
                f"state={self._state}, is_running={self._is_running}, is_paused={self._is_paused}, "
                f"historical_started={self._historical_load_started}, "
                f"subscribed_count={len(self._subscribed_instruments)}, "
                f"has_market_data_service={self.market_data_service is not None}, "
                f"kline_requested={kline_stats.get('total_requested', 0)}, "
                f"kline_success={kline_stats.get('success', 0)}, "
                f"kline_failed={kline_stats.get('failed', 0)}, "
                f"kline_loaded={kline_stats.get('total_klines_loaded', 0)}"
            )
        
        try:
            # 提取关键信息用于统计
            exchange = getattr(tick, 'exchange', getattr(tick, 'ExchangeID', ''))
            instrument_id = getattr(tick, 'instrument_id', getattr(tick, 'InstrumentID', ''))
            
            # 处理 Tick 数据
            self._process_tick(tick)
            
            # 统计
            self._stats['total_ticks'] += 1
            self._tick_count += 1
            
            # 详细分类统计
            instrument_type = 'option' if '-' in instrument_id else 'future'
            self._stats['tick_by_type'][instrument_type] += 1
            
            # 按交易所统计
            if exchange:
                if exchange not in self._stats['tick_by_exchange']:
                    self._stats['tick_by_exchange'][exchange] = 0
                self._stats['tick_by_exchange'][exchange] += 1
            
            # 按合约统计
            if instrument_id:
                if instrument_id not in self._stats['tick_by_instrument']:
                    self._stats['tick_by_instrument'][instrument_id] = 0
                self._stats['tick_by_instrument'][instrument_id] += 1

            # ✅ 改为 30 秒汇总输出一次
            current_time = time.time()
            if current_time - self._last_tick_log_time >= self._tick_log_interval:
                # 计算运行时长（秒）
                start_time = self._stats.get('start_time')
                if start_time:
                    if isinstance(start_time, datetime):
                        elapsed_seconds = (datetime.now() - start_time).total_seconds()
                    else:
                        elapsed_seconds = current_time - start_time
                else:
                    elapsed_seconds = 1.0

                logging.info(
                    f"[StrategyCoreService.on_tick] 📊 过去 30 秒处理 Tick 汇总："
                    f"总计={self._tick_count:,} 个 | "
                    f"当前速率={self._tick_count / max(elapsed_seconds, 1):.1f} ticks/s"
                )
                self._last_tick_log_time = current_time
                        
            # 每 3 分钟（约 18000 个 Tick，假设每秒 100 个）输出详细汇总
            if self._tick_count % 18000 == 0 and self._tick_count > 0:
                self._output_periodic_summary()
            
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_tick] Error: {e}")
            self._stats['errors_count'] += 1
            self._stats['last_error_time'] = datetime.now()
    
    def _output_periodic_summary(self) -> None:
        """输出 3 分钟定期状态汇总"""
        try:
            uptime = datetime.now() - self._stats['start_time'] if self._stats['start_time'] else timedelta(0)
            uptime_str = str(uptime).split('.')[0]  # 去掉微秒
            
            # 获取 K 线统计
            kline_stats = {}
            if self.market_data_service:
                try:
                    kline_stats = self.market_data_service.get_kline_stats()
                except Exception as e:
                    logging.warning(f"[Periodic Summary] Failed to get kline stats: {e}")
            
            # 准备详细统计信息
            tick_by_type = self._stats.get('tick_by_type', {'future': 0, 'option': 0})
            tick_by_exchange = self._stats.get('tick_by_exchange', {})
            tick_by_instrument = self._stats.get('tick_by_instrument', {})
            
            # 按交易所排序的统计
            sorted_exchanges = sorted(tick_by_exchange.items(), key=lambda x: x[1], reverse=True)[:5]  # 只显示前5个
            
            # 按合约排序的统计
            sorted_instruments = sorted(tick_by_instrument.items(), key=lambda x: x[1], reverse=True)[:10]  # 只显示前10个
            
            summary = f"""
{'='*80}
【3 分钟状态汇总】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}
运行时长：{uptime_str}
Tick 数据：总计 {self._stats['total_ticks']:,} 个
  - 期货：{tick_by_type.get('future', 0):,} 个
  - 期权：{tick_by_type.get('option', 0):,} 个
交易数量：总计 {self._stats['total_trades']:,} 笔
信号数量：总计 {self._stats['total_signals']:,} 个
错误统计：{self._stats['errors_count']} 次
最后错误：{self._stats['last_error_time'].strftime('%H:%M:%S') if self._stats['last_error_time'] else '无'}
策略状态：{self._state.value}
是否运行：{self._is_running}
是否暂停：{self._is_paused}
K 线加载：成功 {kline_stats.get('success', 0):,} / 失败 {kline_stats.get('failed', 0):,} / 总计 {kline_stats.get('total_klines_loaded', 0):,} 根

【交易所分布】
{''.join([f'  - {exchange}: {count:,} 个\n' for exchange, count in sorted_exchanges])}

【合约活跃度 Top 10】
{''.join([f'  - {inst}: {count:,} 个\n' for inst, count in sorted_instruments])}
{'='*80}
"""
            logging.info(summary)
        except Exception as e:
            logging.error(f"[Periodic Summary] 输出失败：{e}")
    
    def _process_tick(self, tick: Any) -> None:
        """处理 Tick 数据（核心方法）
        
        Args:
            tick: Tick 数据对象
        """
        try:
            # 提取关键信息
            exchange = getattr(tick, 'exchange', getattr(tick, 'ExchangeID', ''))
            instrument_id = getattr(tick, 'instrument_id', getattr(tick, 'InstrumentID', ''))
            last_price = getattr(tick, 'last_price', getattr(tick, 'LastPrice', 0.0))
            volume = getattr(tick, 'volume', getattr(tick, 'Volume', 0))
            
            # ========== 详细日志输出（恢复原有功能） ==========
            # 1. 每 10 秒输出一次 Tick 诊断信息
            now_ts = time.time()
            last_tick_diag_ts = getattr(self, '_tick_debug_last_ts', 0.0)
            
            if (now_ts - last_tick_diag_ts >= 10.0) and instrument_id:
                logging.info(f"[TickDiag] inst={instrument_id} exch={exchange} price={last_price} vol={volume}")
                self._tick_debug_last_ts = now_ts
            
            # 2. 如果开启 debug_output，每条 Tick 都记录
            params_dict = getattr(self, 'params', {})
            if isinstance(params_dict, dict):
                debug_output = params_dict.get('debug_output', False)
            else:
                debug_output = getattr(params_dict, 'debug_output', False)
            
            if debug_output and instrument_id:
                logging.debug(f"[Tick] {exchange}.{instrument_id} price={last_price} vol={volume}")
            # ===============================================
            
            # 更新最新价格
            if hasattr(self, '_latest_prices') and instrument_id:
                self._latest_prices[instrument_id] = last_price
            
            # K 线合成（如果有 kline_generator）
            if hasattr(self, 'kline_generator') and self.kline_generator:
                self.kline_generator.tick_to_kline(tick)
            
            # ✅ 保存 Tick 数据到数据库并合成 K 线
            if hasattr(self, 'storage') and self.storage:
                try:
                    tick_data = {
                        'ts': time.time(),
                        'instrument_id': instrument_id,
                        'exchange': exchange,
                        'last_price': last_price,
                        'volume': volume,
                        'amount': getattr(tick, 'amount', 0.0),
                        'open_interest': getattr(tick, 'open_interest', 0.0)
                    }
                    # 保存 Tick 数据到数据库
                    self.storage.save_tick(tick_data)
                except Exception as e:
                    logging.error(f"[_process_tick] Failed to process tick: {e}")
            
            # 触发信号计算
            self._on_tick_signal(tick)
            
        except Exception as e:
            logging.error(f"[StrategyCoreService._process_tick] Error: {e}")
            logging.exception("[StrategyCoreService._process_tick] Stack:")
    
    def _on_tick_signal(self, tick: Any) -> None:
        """Tick 数据触发的信号计算
        
        Args:
            tick: Tick 数据对象
        """
        try:
            if not self.analytics_service:
                return

            exchange = str(getattr(tick, 'exchange', '') or getattr(tick, 'ExchangeID', '') or '').upper()
            instrument_id = str(getattr(tick, 'instrument_id', '') or getattr(tick, 'InstrumentID', '') or '').upper()
            if not exchange or not instrument_id:
                return

            if instrument_id in getattr(self, '_option_ids', set()):
                self._update_option_tick_snapshot(instrument_id, tick)
                return

            if instrument_id not in getattr(self, '_future_ids', set()):
                return

            result = self.analytics_service.calculate_option_width(exchange, instrument_id)
            if result is None:
                return

            result_dict = dict(result.__dict__) if hasattr(result, '__dict__') else dict(result)
            self.option_width_results[instrument_id] = result_dict
            self._check_width_ranking_output()
        except Exception as e:
            logging.error(f"[StrategyCoreService._on_tick_signal] Error: {e}")

    def _update_option_tick_snapshot(self, instrument_id: str, tick: Any) -> None:
        """把最新期权 Tick 价格回写到已加载期权对象，供分析层读取。"""
        last_price = getattr(tick, 'last_price', getattr(tick, 'LastPrice', 0.0))
        if not isinstance(last_price, (int, float)) or last_price <= 0:
            return

        for contracts in (getattr(self, '_option_instruments', {}) or {}).values():
            for option in contracts or []:
                current_id = self._get_instrument_value(option, 'InstrumentID', 'instrument_id', '_instrument_id').upper()
                if current_id != instrument_id:
                    continue

                if isinstance(option, dict):
                    option['last_price'] = last_price
                    option['last'] = last_price
                    option['price'] = last_price
                else:
                    setattr(option, 'last_price', last_price)
                    setattr(option, 'last', last_price)
                    setattr(option, 'price', last_price)
                return
    
    # ========== P2 功能恢复：策略核心辅助方法 ==========
    
    def _check_width_ranking_output(self) -> None:
        """检查并执行宽度排名输出（对应旧 L1450）"""
        try:
            current_time = time.time()
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
            
            interval = 180  # 默认间隔
            if mode == "open_debug":
                interval = getattr(self.params, "open_debug_output_interval", 60)
            elif mode == "trade":
                interval = getattr(self.params, "trade_output_interval", 900)
            else:
                interval = getattr(self.params, "debug_output_interval", 180)
            
            if hasattr(self, 'last_width_output_time'):
                if current_time - self.last_width_output_time >= interval:
                    self._perform_width_output()
                    self.last_width_output_time = current_time
        except Exception as e:
            logging.error(f"[StrategyCoreService._check_width_ranking_output] Error: {e}")
    
    def _perform_width_output(self) -> None:
        """执行宽度输出（对应旧 L1464）"""
        try:
            if not hasattr(self, "option_width_results"):
                return

            mode = str(getattr(self.params, "output_mode", "debug")).lower()
            mode_str = "OPEN_DEBUG" if mode == "open_debug" else ("CLOSE_DEBUG" if mode == "close_debug" else "TRADE")
            
            logging.info(f"\n[Periodic] Option Width Ranking Report ({mode_str})")
            logging.info("-" * 60)
            
            items = []
            for instr, res in self.option_width_results.items():
                if not isinstance(res, dict):
                    continue
                w = res.get('option_width', 0)
                items.append((instr, w, res))

            items.sort(key=lambda x: x[1], reverse=True)

            top_n = 10
            for i, (instr, w, res) in enumerate(items[:top_n]):
                logging.info(f"Rank #{i+1:02d} {instr}: Width={w} (Sync={res.get('all_sync')})")

            if len(self.option_width_results) > top_n:
                logging.info(f"... and {len(self.option_width_results)-top_n} more instruments.")
            elif len(self.option_width_results) == 0:
                logging.info("No valid width data available yet.")
                
            logging.info("-" * 60 + "\n")
            
        except Exception as e:
            logging.error(f"[StrategyCoreService._perform_width_output] Error: {e}")
    
    def on_order(self, order_data: Any, *args, **kwargs) -> None:
        """订单回调（对应旧 L1504）"""
        try:
            oid = getattr(order_data, "order_id", "") or getattr(order_data, "OrderRef", "")
            sts = getattr(order_data, "status", "") or getattr(order_data, "OrderStatus", "")
            logging.info(f"[StrategyCoreService.on_order] OrderID={oid}, Status={sts}")
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_order] Error: {e}")
    
    def on_order_trade(self, trade_data: Any, *args, **kwargs) -> None:
        """订单成交通告（对应旧 L1523）"""
        try:
            logging.info(f"[StrategyCoreService.on_order_trade] Trade executed")
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_order_trade] Error: {e}")
    
    def on_trade(self, trade_data: Any, *args, **kwargs) -> None:
        """交易回调（对应旧 L1542）"""
        try:
            logging.info(f"[StrategyCoreService.on_trade] Trade notification received")
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_trade] Error: {e}")
    
    def on_destroy(self) -> None:
        """策略销毁（对应旧 L1554）"""
        try:
            self.destroy()
        except Exception as e:
            logging.error(f"[StrategyCoreService.on_destroy] Error: {e}", exc_info=True)
    
    def _is_instrument_allowed(self, instrument_id: str, exchange: str = "") -> bool:
        """检查合约是否允许（对应旧 L1583）"""
        return True
    
    def _is_symbol_specified_or_next(self, symbol: str) -> bool:
        """检查合约是否为指定月或下月合约（对应旧 L1589）"""
        try:
            month_mapping = getattr(self.params, 'month_mapping', {})
            if not month_mapping:
                return True
            
            import re
            match = re.match(r'^([A-Z]+)', symbol.upper())
            if not match:
                return True
            
            product = match.group(1)
            if product in month_mapping:
                specified_months = month_mapping[product]
                if isinstance(specified_months, (list, tuple)) and len(specified_months) >= 2:
                    return symbol in [specified_months[0], specified_months[1]]
            return True
        except Exception as e:
            return True
    
    def _is_symbol_current_or_next(self, symbol: str) -> bool:
        """兼容旧命名（对应旧 L1614）"""
        return self._is_symbol_specified_or_next(symbol)
    
    def _is_backtest_context(self) -> bool:
        """检查是否为回测环境（对应旧 L1618）"""
        return False
    
    def _is_trade_context(self) -> bool:
        """检查是否为交易环境（对应旧 L1622）"""
        try:
            return str(getattr(self.params, "run_mode", "")).lower() in ["trade", "paper"]
        except Exception as e:
            return False
    
    # ========== P2 功能恢复：诊断工具方法 ==========
    
    def diagnose(self, component: str) -> Dict[str, Any]:
        """诊断指定组件（对应旧 15_diagnosis.py）"""
        try:
            result = {
                'component': component,
                'status': 'OK',
                'issues': [],
                'timestamp': time.time()
            }
            
            # 执行诊断逻辑
            if component == "data_link":
                result['issues'] = self._diagnose_data_link()
            elif component == "strategy_core":
                result['issues'] = self._diagnose_strategy_core()
            elif component == "event_bus":
                result['issues'] = self._diagnose_event_bus()
            
            if result['issues']:
                result['status'] = 'WARNING'
            
            return result
            
        except Exception as e:
            logging.error(f"[StrategyCoreService.diagnose] Error: {e}")
            return {'error': str(e)}
    
    def _diagnose_data_link(self) -> List[str]:
        """诊断数据链路"""
        issues = []
        # TODO: 实际的数据链路检查
        return issues
    
    def _diagnose_strategy_core(self) -> List[str]:
        """诊断策略核心"""
        issues = []
        # TODO: 实际的策略核心检查
        return issues
    
    def _diagnose_event_bus(self) -> List[str]:
        """诊断事件总线"""
        issues = []
        # TODO: 实际的事件总线检查
        return issues
    
    def get_health_report(self) -> Dict[str, Any]:
        """获取健康报告"""
        return {
            'overall_status': 'OK',
            'health_score': 100.0,
            'timestamp': time.time()
        }
    
    def clear_diagnosis(self) -> None:
        """清空诊断结果（对应旧 15_diagnosis.py）"""
        # 简化实现
        pass
    
    def export_diagnosis_report(self) -> str:
        """导出诊断报告（对应旧 15_diagnosis.py）"""
        return "Diagnosis report placeholder"
    
    def initialize(self, params: Optional[Dict[str, Any]] = None) -> bool:
        """初始化策略
        
        Args:
            params: 策略参数
            
        Returns:
            bool: 初始化是否成功
        """
        return self.on_init(params=params)
    
    def start(self) -> bool:
        """启动策略
        
        Returns:
            bool: 启动是否成功
        """
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
        """停止策略
        
        Returns:
            bool: 停止是否成功
        """
        # 兼容旧调用入口：统一转发到 on_stop，避免两套停止逻辑分叉。
        self.save_state()
        return self.on_stop()
    
    def destroy(self) -> bool:
        """销毁策略
        
        Returns:
            bool: 销毁是否成功
        """
        with self._lock:
            if self._destroyed:
                logging.info(f"[StrategyCoreService.destroy] Already destroyed: {self.strategy_id}")
                return True
            
            try:
                logging.info(f"[StrategyCoreService.destroy] Destroying: {self.strategy_id}")
                
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
                
                logging.info(f"[StrategyCoreService.destroy] Destroyed: {self.strategy_id}")
                
                # 发布事件
                self._publish_event('StrategyDestroyed', {
                    'strategy_id': self.strategy_id
                })
                
                return True
                
            except Exception as e:
                logging.error(f"[StrategyCoreService.destroy] Failed: {e}")
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
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息
        
        Returns:
            Dict: 统计数据
        """
        with self._lock:
            uptime = self.get_uptime()
            ticks_per_second = self._stats['total_ticks'] / uptime if uptime > 0 else 0
            
            return {
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
        
        # 检查错误率
        uptime = self.get_uptime()
        if uptime > 60:  # 运行超过 1 分钟
            error_rate = self._stats['errors_count'] / uptime
            if error_rate > 0.1:  # 每分钟错误率>0.1
                issues.append(f"High error rate: {error_rate*60:.2f}/min")
                status = 'warning' if status == 'healthy' else 'unhealthy'
        
        return {
            'status': status,
            'issues': issues,
            'strategy_id': self.strategy_id,
            'state': self._state.value,
            'uptime_seconds': uptime,
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
        log_level_name = str((params or {}).get('log_level') or os.getenv('LOG_LEVEL', 'INFO')).upper()
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
        """初始化调度器"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            self._scheduler = BackgroundScheduler()
            self._scheduler.start()
            logging.info("[StrategyCoreService] APScheduler initialized")
        except ImportError:
            # 使用简单调度器
            self._scheduler = SimpleScheduler()
            self._scheduler.start()
            logging.info("[StrategyCoreService] SimpleScheduler initialized")
    
    def _stop_scheduler(self) -> None:
        """停止调度器"""
        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=True)  # 等待任务完成
            except Exception as e:
                logging.error(f"[StrategyCoreService] Stop scheduler error: {e}")
    
    def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
        """
        if self._event_bus:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'strategy_id': self.strategy_id,
                    **data
                })()
                self._event_bus.publish(event, async_mode=True)
            except Exception as e:
                logging.debug(f"[StrategyCoreService] Failed to publish {event_type}: {e}")


# ============================================================================
# 简单调度器实现（备用）
# ============================================================================

class SimpleScheduler:
    """简单调度器 - 当 APScheduler 不可用时使用"""
    
    def __init__(self):
        self.jobs = {}
        self._running = False
        self._lock = threading.RLock()
    
    def add_job(self, func, trigger, **kwargs):
        """添加任务"""
        try:
            job_id = kwargs.get('id', f"job_{len(self.jobs)}")
            interval = kwargs.get('seconds', 60)
            
            def _job_wrapper():
                while self._running:
                    try:
                        func()
                    except Exception as e:
                        logging.error(f"[SimpleScheduler] Job {job_id} error: {e}")
                    time.sleep(interval)
            
            thread = threading.Thread(target=_job_wrapper, daemon=True, name=f"Job_{job_id}")
            self.jobs[job_id] = thread
            if self._running:
                thread.start()
            return job_id
        except Exception as e:
            logging.error(f"[SimpleScheduler] Failed to add job: {e}")
            return None
    
    def remove_job(self, job_id):
        """移除任务"""
        with self._lock:
            if job_id in self.jobs:
                del self.jobs[job_id]
                return True
            return False
    
    def start(self):
        """启动调度器"""
        with self._lock:
            if not self._running:
                self._running = True
                for job_id, thread in self.jobs.items():
                    if not thread.is_alive():
                        thread.start()
    
    def shutdown(self):
        """关闭调度器"""
        with self._lock:
            self._running = False
    
    @property
    def running(self):
        """运行状态"""
        return self._running


# 导出公共接口
__all__ = ['StrategyCoreService', 'StrategyState', 'Strategy2026']

# ============================================================================
# Strategy2026 - 策略主类（从 t_type_bootstrap.py 迁移至此）
# ============================================================================

from pythongo.base import BaseStrategy
from .ui_service import UIMixin
from .market_data_service import is_market_open, get_market_time_service
from ali2026v3_trading.config_service import get_cached_params

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
                    
        # 使用缓存参数表，避免重复导入和复制
        loaded_config = get_cached_params()
        logging.info(f"[Strategy2026] 从缓存参数表加载了 {len(loaded_config)} 个参数")

        merged = dict(loaded_config)
        for k, v in runtime_config.items():
            if v is None:
                continue
            if isinstance(v, str) and v == "":
                continue
            if isinstance(v, (list, dict)) and len(v) == 0:
                continue
            merged[k] = v

        # 注入运行时上下文，供 MarketDataService 获取市场中心和平台对象
        runtime_strategy = kwargs.get('strategy')
        runtime_market_center = kwargs.get('market_center')
        if runtime_strategy is not None:
            merged['strategy'] = runtime_strategy
        else:
            merged['strategy'] = self  # 将策略实例本身注入到params中
        if runtime_market_center is not None:
            merged['market_center'] = runtime_market_center

        self.config = merged
        self.params = type('Params', (), merged)()  # 创建参数对象，供 UI 使用
        logging.info(f"[Strategy2026] params.strategy = {getattr(self.params, 'strategy', None)}")  # 添加诊断日志
                    
        self.strategy_id = kwargs.get('strategy_id', f"strategy_{int(time.time())}")
        self.strategy_core = StrategyCoreService(self.strategy_id)
        
        # ========== 注入平台 API 到 strategy_core ==========
        # 注意：不在 __init__ 中注入，因为此时平台还没提供这些方法
        # 而是在 onInit 中首次调用前注入
        # ================================================
        
        # ========== 注册到 t_type_bootstrap 缓存 ==========
        # 让 TTypeBootstrap 的回调能委托给本实例
        # 避免循环导入，直接修改 sys.modules 中的变量
        try:
            import sys
            if 't_type_bootstrap' in sys.modules:
                sys.modules['t_type_bootstrap']._strategy_cache = self
                logging.info("[Strategy2026] Registered to t_type_bootstrap._strategy_cache")
        except Exception as e:
            logging.warning(f"[Strategy2026] Failed to register: {e}")
        # ================================================
        
        # 初始化 UI 相关属性
        self.auto_trading_enabled = False
        self.my_is_running = False
        self.my_is_paused = False
        self.my_trading = False
        self._callbacks_enabled = True
        self._stop_requested = False
        
        # 初始化市场时间服务
        self.market_time_service = get_market_time_service()
        
        logging.info(f"[Strategy2026] 已初始化，配置：{self.config}")
        logging.info(f"[Strategy2026] UI 功能已集成，继承自 UIMixin")

    def _destroy_output_mode_ui(self) -> None:
        """显式销毁输出模式UI，避免卸载后仍持有窗口与实例引用。"""
        try:
            if hasattr(self, "_ui_queue") and self._ui_queue:
                try:
                    self._ui_queue.put_nowait({"action": "destroy"})
                except Exception:
                    self._ui_queue.put({"action": "destroy"})
        except Exception as e:
            logging.warning(f"[Strategy2026._destroy_output_mode_ui] Queue destroy failed: {e}")

        try:
            cls = self.__class__
            setattr(cls, "_ui_global_running", False)
            setattr(cls, "_ui_global_root", None)
            self._ui_running = False
        except Exception as e:
            logging.warning(f"[Strategy2026._destroy_output_mode_ui] UI state cleanup failed: {e}")

    def _release_runtime_caches(self) -> None:
        """释放模块级缓存，避免平台删除实例时仍被强引用持有。"""
        try:
            import sys
            bootstrap_mod = sys.modules.get('t_type_bootstrap')
            if bootstrap_mod is not None and getattr(bootstrap_mod, '_strategy_cache', None) is self:
                bootstrap_mod._strategy_cache = None
                logging.info("[Strategy2026] Cleared t_type_bootstrap._strategy_cache")
        except Exception as e:
            logging.warning(f"[Strategy2026._release_runtime_caches] Failed to clear bootstrap cache: {e}")

        try:
            import strategy_manager
            cached = getattr(strategy_manager, '_strategy_cache', None)
            if cached is self:
                strategy_manager.reset_strategy_cache()
                logging.info("[Strategy2026] Cleared strategy_manager._strategy_cache")
        except Exception:
            pass

    def is_market_open(self, exchange: str = 'AUTO') -> bool:
        """
        检查市场是否开盘
        
        Args:
            exchange: 交易所代码，AUTO 表示按参数中的 exchanges 任一开盘即视为开盘
            
        Returns:
            bool: 市场是否开盘
        """
        try:
            exchange = (exchange or 'AUTO').upper()
            if exchange != 'AUTO':
                return is_market_open(exchange)

            exchanges_raw = getattr(self.params, 'exchanges', '') if hasattr(self, 'params') else ''
            if isinstance(exchanges_raw, (list, tuple, set)):
                exchanges = [str(e).strip().upper() for e in exchanges_raw if str(e).strip()]
            else:
                exchanges = [e.strip().upper() for e in str(exchanges_raw).split(',') if e.strip()]

            if not exchanges:
                from ali2026v3_trading.config_service import get_cached_params
                default_exchanges_raw = get_cached_params().get('exchanges', 'CFFEX,SHFE,DCE,CZCE,INE,GFEX')
                exchanges = [e.strip().upper() for e in str(default_exchanges_raw).split(',') if e.strip()]

            return any(is_market_open(exch) for exch in exchanges)
        except Exception:
            # 保底不影响启动链路
            return any(is_market_open(exch) for exch in ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX'])
    
    def onStart(self):
        """平台启动回调（必须返回 None）"""
        self._callbacks_enabled = True
        self._stop_requested = False
        logging.info("[Strategy2026] 平台启动回调")

        try:
            self.strategy_core.bind_platform_apis(self)
            logging.info("[Strategy2026.onStart] 已刷新平台 API 到 strategy_core")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] bind_platform_apis() 错误：{e}")
            logging.exception("[Strategy2026.onStart] bind_platform_apis() 堆栈:")

        try:
            # 跳过父类 BaseStrategy 的 on_start 方法中的订阅逻辑
            # 订阅逻辑统一在 StrategyCoreService._subscribe_instruments() 中处理
            # 只执行必要的状态设置和UI更新
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

                if current_state == StrategyState.INITIALIZING:
                    logging.info("[Strategy2026] 正在调用 strategy_core.on_init()...")
                    init_result = self.strategy_core.on_init(params=self.config)
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
            if hasattr(self.strategy_core, 'on_start'):
                result = self.strategy_core.on_start()
                logging.info(f"[Strategy2026] 策略核心启动结果：{result}")
            else:
                logging.warning("[Strategy2026.onStart] strategy_core.on_start 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onStart] strategy_core.on_start() 错误：{e}")
            logging.exception("[Strategy2026.onStart] strategy_core.on_start() 堆栈:")

        return None
    
    def onInit(self):
        """平台初始化回调（必须返回 None）"""
        try:
            logging.info("[Strategy2026] 平台初始化回调")
            
            self.strategy_core.bind_platform_apis(self)
            logging.info("[Strategy2026.onInit] 已刷新平台 API 到 strategy_core")
            
            # 设置初始化标志
            self._init_pending = True
            self._init_error = None
            
            # 调用 strategy_core 的初始化
            if hasattr(self.strategy_core, 'on_init'):
                result = self.strategy_core.on_init(params=self.config)
                logging.info(f"[Strategy2026.onInit] strategy_core 初始化结果：{result}")
            
            # 清除初始化标志
            self._init_pending = False
            
            # 调用父类 BaseStrategy 的 on_init 方法（官方规范要求）
            super().on_init()
            
            return None
        except Exception as e:
            logging.error(f"[Strategy2026.onInit] 错误：{e}")
            logging.exception("[Strategy2026.onInit] 堆栈:")
            self._init_error = e
            self._init_pending = False
            return None
    
    def onStop(self):
        """平台停止回调"""
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onStop] enter")

        try:
            self._destroy_output_mode_ui()
        except Exception as e:
            logging.warning(f"[Strategy2026.onStop] destroy UI failed: {e}")

        try:
            super().on_stop()
            logging.info("[Strategy2026.onStop] super().on_stop() completed")
        except Exception as e:
            logging.error(f"[Strategy2026.onStop] super().on_stop() failed: {e}")
            logging.exception("[Strategy2026.onStop] super().on_stop() stack:")

        try:
            logging.info("[Strategy2026] 平台停止回调")
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'on_stop'):
                result = self.strategy_core.on_stop()
                logging.info(f"[Strategy2026.onStop] strategy_core 停止结果：{result}")
            else:
                logging.warning("[Strategy2026.onStop] strategy_core.on_stop 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onStop] strategy_core.on_stop failed: {e}")
            logging.exception("[Strategy2026.onStop] strategy_core.on_stop stack:")

        return None

    def onDestroy(self):
        """平台卸载/销毁回调（必须返回 None）。"""
        self._callbacks_enabled = False
        self._stop_requested = True
        logging.info("[Strategy2026.onDestroy] enter")

        try:
            self._destroy_output_mode_ui()
        except Exception as e:
            logging.warning(f"[Strategy2026.onDestroy] destroy UI failed: {e}")

        try:
            # 复用停机逻辑，确保先反订阅与停止后台服务
            self.onStop()
        except Exception as e:
            logging.error(f"[Strategy2026.onDestroy] onStop() 错误：{e}")

        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'destroy'):
                result = self.strategy_core.destroy()
                logging.info(f"[Strategy2026.onDestroy] strategy_core.destroy() 结果：{result}")
        except Exception as e:
            logging.error(f"[Strategy2026.onDestroy] strategy_core.destroy() 错误：{e}")
            logging.exception("[Strategy2026.onDestroy] strategy_core.destroy() 堆栈:")

        try:
            self._release_runtime_caches()
        except Exception as e:
            logging.warning(f"[Strategy2026.onDestroy] release caches failed: {e}")

        try:
            super_destroy = getattr(super(), 'on_destroy', None)
            if callable(super_destroy):
                super_destroy()
        except Exception as e:
            logging.error(f"[Strategy2026.onDestroy] super().on_destroy() 错误：{e}")

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
            logging.exception("[Strategy2026.onTick] super().on_tick() 堆栈:")

        try:
            # 添加入口日志，记录每个接收到的 Tick
            if tick:
                instrument_id = getattr(tick, 'InstrumentID', '') or getattr(tick, 'instrument_id', '')
                last_price = getattr(tick, 'LastPrice', 0) or getattr(tick, 'last_price', 0)
                volume = getattr(tick, 'Volume', 0) or getattr(tick, 'volume', 0)
                
                # 使用 30 秒汇总输出，避免高频日志刷屏
                current_time = datetime.now()
                if not hasattr(self, '_tick_log_last_time'):
                    self._tick_log_last_time = current_time
                    self._tick_log_count = 0
                    self._tick_log_samples = []
                
                time_diff = (current_time - self._tick_log_last_time).total_seconds()
                self._tick_log_count += 1
                
                # 每秒采样一次代表性数据
                if len(self._tick_log_samples) < 5 and time_diff >= 1.0:
                    self._tick_log_samples.append({
                        'instrument_id': instrument_id,
                        'last_price': last_price,
                        'volume': volume,
                        'time': current_time.strftime('%H:%M:%S')
                    })
                
                # 每 30 秒汇总输出一次
                if time_diff >= 30.0:
                    sample_str = ', '.join([f"{s['instrument_id']}={s['last_price']}" for s in self._tick_log_samples[:5]])
                    logging.info(f"[Strategy2026.onTick] 过去 30 秒共接收 {self._tick_log_count} 个 Tick | 采样：{sample_str}")
                    self._tick_log_last_time = current_time
                    self._tick_log_count = 0
                    self._tick_log_samples = []

            # 委托给 strategy_core 处理
            if hasattr(self.strategy_core, 'on_tick'):
                self.strategy_core.on_tick(tick)
            else:
                logging.warning("[Strategy2026.onTick] strategy_core.on_tick 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onTick] strategy_core.on_tick() 错误：{e}")
            logging.exception("[Strategy2026.onTick] strategy_core.on_tick() 堆栈:")

        return None
    
    def onOrder(self, order):
        """平台订单回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            # 调用父类 BaseStrategy 的 on_order 方法（官方规范要求）
            super().on_order(order)
        except Exception as e:
            logging.error(f"[Strategy2026.onOrder] super().on_order() 错误：{e}")
            logging.exception("[Strategy2026.onOrder] super().on_order() 堆栈:")

        try:
            logging.info(f"[Strategy2026.onOrder] 订单更新：{order}")

            # 委托给 strategy_core 处理
            if hasattr(self.strategy_core, 'on_order'):
                self.strategy_core.on_order(order)
            else:
                logging.warning("[Strategy2026.onOrder] strategy_core.on_order 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onOrder] strategy_core.on_order() 错误：{e}")
            logging.exception("[Strategy2026.onOrder] strategy_core.on_order() 堆栈:")

        return None
    
    def onTrade(self, trade):
        """平台成交回调（必须返回 None）"""
        if not getattr(self, '_callbacks_enabled', True):
            return None

        try:
            # 调用父类 BaseStrategy 的 on_trade 方法（官方规范要求）
            super().on_trade(trade)
        except Exception as e:
            logging.error(f"[Strategy2026.onTrade] super().on_trade() 错误：{e}")
            logging.exception("[Strategy2026.onTrade] super().on_trade() 堆栈:")

        try:
            logging.info(f"[Strategy2026.onTrade] 成交：{trade}")

            # 委托给 strategy_core 处理
            if hasattr(self.strategy_core, 'on_trade'):
                self.strategy_core.on_trade(trade)
            else:
                logging.warning("[Strategy2026.onTrade] strategy_core.on_trade 不可用，跳过")
        except Exception as e:
            logging.error(f"[Strategy2026.onTrade] strategy_core.on_trade() 错误：{e}")
            logging.exception("[Strategy2026.onTrade] strategy_core.on_trade() 堆栈:")

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
