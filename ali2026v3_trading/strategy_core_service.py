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
import re
import importlib.util
import time
from typing import Any, Dict, List, Optional

import threading
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from functools import wraps

from ali2026v3_trading.storage import get_instrument_data_manager, is_market_open, get_market_time_service
from ali2026v3_trading.config_service import get_cached_params


def get_param_value(params: Any, key: str, default: Any = None, alt_keys: Optional[List[str]] = None) -> Any:
    """兼容 dict/对象 两种参数结构读取配置值。"""
    if params is None:
        return default

    if isinstance(params, dict):
        value = params.get(key)
    else:
        value = getattr(params, key, None)

    if value is not None:
        return value

    for alt_key in alt_keys or []:
        if isinstance(params, dict):
            value = params.get(alt_key)
        else:
            value = getattr(params, alt_key, None)
        if value is not None:
            return value

    return default


# ========== 已删除：品种映射工具函数 (2026-03-27) ==========
# 原因：这些功能与 storage.py 重复，storage.py 已完美实现 ID 三原样订阅
# 如需合约解析功能，应使用 instrument_mapper.py 或直接使用 storage.py
# ================================================================


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
            'total_klines': 0,  # 新增：K 线统计
            'errors_count': 0,
            'last_error_time': None,
            'last_error_message': None,
            'tick_by_type': {'future': 0, 'option': 0},
            'tick_by_exchange': {},
            'tick_by_instrument': {}
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
        self.get_kline = None

        # 运行时分析链路
        self.analytics_service = None
        self.option_width_results: Dict[str, Dict[str, Any]] = {}
        self._latest_prices: Dict[str, float] = {}
        
        # 合约数据缓存（从 storage.py 派生的临时副本，用于性能优化）
        # 注意：这些不是独立的数据源，而是 storage.classify_instruments() 结果的缓存
        # 真正的数据持久化在 storage.py 中，这里只是避免重复查询的性能优化
        self._futures_instruments: List[Any] = []
        self._option_instruments: Dict[str, List[Any]] = {}
        self._future_ids: set[str] = set()
        self._option_ids: set[str] = set()
        self._subscribed_instruments: List[str] = []
        self._hkl_diag_emitted = False
        self._historical_load_started = False  # ✅ 修复：在 __init__ 中初始化
    
        # 初始化存储服务
        self._init_storage()
    
    # ========== 生命周期管理 ==========

    def _init_storage(self) -> None:
        """初始化存储服务（延迟初始化，避免启动阻塞）"""
        try:
            self._storage = get_instrument_data_manager()
        except Exception as e:
            logging.error(f"[StrategyCoreService] Failed to initialize global storage singleton: {e}")
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
        """把 EXCHANGE.INSTRUMENT 形式拆分成交易所和合约代码。"""
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
            def _subscribe(instrument_id: str) -> None:
                # ✅ wrapper 负责推断交易所（靠近 platform，符合单一职责）
                exchange = self.storage.infer_exchange_from_id(instrument_id)
                runtime_sub_market_data(exchange=exchange, instrument_id=instrument_id)

            self.subscribe = _subscribe
        else:
            self.subscribe = None

        if callable(runtime_unsub_market_data):
            def _unsubscribe(instrument_id: str) -> None:
                # ✅ wrapper 负责推断交易所
                exchange = self.storage.infer_exchange_from_id(instrument_id)
                runtime_unsub_market_data(exchange=exchange, instrument_id=instrument_id)

            self.unsubscribe = _unsubscribe
        else:
            self.unsubscribe = None

        self.get_instrument = getattr(strategy_obj, 'get_instrument', None)
        self.get_kline = getattr(strategy_obj, 'get_kline', None)

        logging.info(
            "[StrategyCoreService.bind_platform_apis] "
            f"subscribe={callable(self.subscribe)}, "
            f"unsubscribe={callable(self.unsubscribe)}, "
            f"get_instrument={callable(self.get_instrument)}, "
            f"get_kline={callable(self.get_kline)}"
        )

        # ✅ market_data_service 已删除 (2026-03-27)
        # 不再需要注入 runtime context

    def _inject_runtime_context(self, strategy_obj: Any) -> None:
        """把运行时 strategy / market_center 注回参数对象。"""
        # ✅ market_data_service 已删除 (2026-03-27)
        # 此方法不再需要
        pass

    # ========== 已删除：_build_future_to_option_map (2026-03-27) ==========
    # 原因：此业务逻辑应由上层应用负责，storage.py 只提供基础注册/订阅功能
    # 如需推导期货 - 期权映射，应在 config_service 或独立服务中实现
    # =====================================================================

    def _init_analytics_services(self, params: Any) -> None:
        """初始化运行时行情和分析服务。"""
        try:
            self.storage = getattr(self, '_storage', None)
            logging.info(f"[StrategyCoreService._init_analytics_services] storage = {self.storage}")
            
            # 获取合约数据（已加载）
            futures_instruments = getattr(self, '_futures_instruments', [])
            option_instruments = getattr(self, '_option_instruments', {})
            
            # 获取策略实例
            # 获取策略实例
            runtime_strategy_instance = None
            if isinstance(params, dict):
                runtime_strategy_instance = params.get('strategy') or params.get('strategy_instance')
            else:
                runtime_strategy_instance = getattr(params, 'strategy', None) or getattr(params, 'strategy_instance', None)
            
            logging.info(f"[InitServices] futures_instruments count: {len(futures_instruments)}")
            logging.info(f"[InitServices] option_instruments groups: {len(option_instruments)}")
            logging.info(f"[InitServices] strategy_instance: {runtime_strategy_instance is not None}")
            
            # ✅ analytics_service 已移除 (2026-03-28)
            # 所有分析功能已迁移至 t_type_service.py
            # self.analytics_service = AnalyticsService(params, self.storage, logging.info)
            self.analytics_service = None  # 已移除，如需分析功能请使用 t_type_service

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

                # ========== K 线合成功能说明 (2026-03-28) ==========
                # K 线合成已集成在 storage.py 的 process_tick() 方法中
                # storage.py 使用 _KlineAggregator 自动处理多合约 K 线合成
                # 支持周期: 1min, 5min, 15min, 30min, 60min
                # 当外部 K 线缺失时自动保存合成的 K 线
                # 无需额外初始化 kline_generator
                # ===========================================================

                # ========== 已删除：合约自动生成逻辑 (2026-03-27) ==========
                # 原因：与 storage.py 功能重复
                # storage.py 已通过 register_instrument() 实现自动注册
                # 新方案：直接使用 storage.py 的订阅接口
                # ===========================================================
                self._futures_instruments = []
                self._option_instruments = {}
                logging.info("[Instruments] 使用 storage.py 原生订阅模式，无需预先生成合约列表")
                
                # ========== 注入到 params (已删除 - 2026-03-27) ==========
                # 原因：storage.py 不需要预先注入合约列表，直接通过 ID 订阅
                # 保持向后兼容，但不再实际使用
                if self.params is not None:
                    if isinstance(self.params, dict):
                        self.params.setdefault('future_instruments', [])
                        self.params.setdefault('option_instruments', {})
                    else:
                        setattr(self.params, 'future_instruments', getattr(self.params, 'future_instruments', []))
                        setattr(self.params, 'option_instruments', getattr(self.params, 'option_instruments', {}))
                # ===============================================================
                
                logging.info(
                    f"[Instruments] 使用 storage.py 原生模式 - "
                    f"期货={len(self._futures_instruments)}, "
                    f"期权={sum(len(opts) for opts in self._option_instruments.values()) if self._option_instruments else 0}"
                )
                
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
                        
            # ========== ✅ 新增：完整订阅流程 (2026-03-28) ==========
            # 从配置文件读取品种，调用 storage 查询接口获取合约，完成订阅
            # 不依赖 platform 的 instrument_list，直接从配置驱动
            # ============================================================
                        
            params = getattr(self, 'params', None)
                        
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
                        
            # 3. 调用 storage 查询接口获取合约（优先）
            if self.storage and (future_products or option_products):
                try:
                    all_instrument_ids = []
                                
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
                        futures_list, options_dict = self.storage.classify_instruments(all_instrument_ids)
                        total_options = sum(len(v) for v in options_dict.values())
                        logging.info(f"[Subscribe] 分类结果：{len(futures_list)} 期货，{total_options} 期权")
                                    
                        # 存储到实例属性（覆盖之前的空列表）
                        self._futures_instruments = futures_list
                        self._option_instruments = options_dict
                                    
                        # 5. 调用 SubscriptionManager 统一订阅
                        if hasattr(self.storage, 'subscription_manager'):
                            self.storage.subscription_manager.subscribe_all_instruments(futures_list, options_dict)
                            logging.info(f"[Subscribe] ✅ 完成数据库订阅！")
                            
                            # ✅ 新增：立即调用 platform 真实订阅（解决第二层根因）
                            if callable(self.subscribe):
                                success_count = 0
                                failed_count = 0
                                
                                # 订阅期货 - ✅ 直接传合约 ID，wrapper 会推断交易所
                                for future_id in futures_list:
                                    try:
                                        self.subscribe(future_id)  # 直接传 "IF2603"
                                        success_count += 1
                                    except Exception as e:
                                        logging.warning(f"[Platform Subscribe] 期货失败 {future_id}: {e}")
                                        failed_count += 1
                                
                                # 订阅期权 - ✅ 直接传合约 ID，wrapper 会推断交易所
                                for underlying, option_ids in options_dict.items():
                                    for option_id in option_ids:
                                        try:
                                            self.subscribe(option_id)  # 直接传 "IO2603-C-4500"
                                            success_count += 1
                                        except Exception as e:
                                            logging.warning(f"[Platform Subscribe] 期权失败 {option_id}: {e}")
                                            failed_count += 1
                                
                                logging.info(f"[Platform Subscribe] ✅ 完成！成功={success_count}, 失败={failed_count}")
                        else:
                            logging.warning("[Subscribe] 无 subscription_manager")
                except Exception as e:
                    logging.error(f"[Subscribe] 失败：{e}")
                    logging.exception("[Subscribe] Stack:")
                        
            # ========== 恢复订阅预处理逻辑 (2026-03-27) ==========
            # 场景：策略刚建立，storage.py 自动注册机制还未触发 (无 tick)
            # 必须通过这 90 行代码完成首次全量订阅，引进数据
            # ================================================
            
            try:
                # 1. 延迟加载合约数据 (如果 on_init 阶段为空)
                if not hasattr(self, '_futures_instruments') or len(getattr(self, '_futures_instruments', [])) == 0:
                    logging.info("[Instruments] on_start 检测到合约数据为空，尝试加载...")

                    # get_param_value 已在本文件顶部定义，无需额外导入
                    params = getattr(self, 'params', None)
                    runtime_strategy_instance = get_param_value(params, 'strategy', alt_keys=['strategy_instance'])
                    
                    # ✅ 优先从 platform 获取已订阅合约
                    if runtime_strategy_instance is not None:
                        try:
                            logging.info("[Instruments] 从 strategy_instance 获取已订阅合约...")
                            
                            # 获取平台已订阅的合约列表
                            instrument_list = getattr(runtime_strategy_instance, 'instrument_list', [])
                            exchange_list = getattr(runtime_strategy_instance, 'exchange_list', [])
                            
                            # ✅ 验证数据有效性：过滤空字符串
                            if instrument_list:
                                instrument_list = [x for x in instrument_list if x and isinstance(x, str) and x.strip()]
                            if exchange_list:
                                exchange_list = [x for x in exchange_list if x and isinstance(x, str) and x.strip()]
                            
                            logging.info(f"[Instruments] instrument_list count: {len(instrument_list) if instrument_list else 'None'}")
                            logging.info(f"[Instruments] exchange_list count: {len(exchange_list) if exchange_list else 'None'}")
                            
                            if instrument_list and len(instrument_list) > 0:
                                logging.info(f"[Instruments] First 3 instruments: {instrument_list[:3]}")
                                
                                # 直接使用 storage.py 的分类方法，完全不自己判断
                                try:
                                    if self.storage:
                                        futures_list, options_dict = self.storage.classify_instruments(instrument_list)
                                        total_options = sum(len(v) for v in options_dict.values())
                                        logging.info(f"[Instruments] ✅ storage 分类结果：{len(futures_list)} 期货，{total_options} 期权")
                                        
                                        # 缓存到实例属性（仅作为临时副本，避免重复查询 storage）
                                        # 数据来源：storage.classify_instruments()
                                        self._futures_instruments = futures_list
                                        self._option_instruments = options_dict
                                        
                                except Exception as e:
                                    logging.warning(f"[Instruments] storage 分类失败：{e}")
                                    self._futures_instruments = []
                                    self._option_instruments = {}
                                    
                        except Exception as e:
                            logging.warning(f"[Instruments] on_start 加载失败：{e}")
                            self._futures_instruments = []
                            self._option_instruments = {}
                    
                    # ✅ Fallback: 如果 platform 无合约，从 storage 查询缓存（非交易时间启动时）
                    if (not self._futures_instruments or not self._option_instruments) and runtime_strategy_instance is not None:
                        logging.info("[Instruments] platform 无有效合约，尝试从 storage 加载缓存...")
                        # ✅ Fallback: 从 storage 查询缓存的合约列表（非交易时间启动时）
                        logging.info("[Instruments] strategy_instance 无有效合约，尝试从 storage 加载缓存...")
                        
                        try:
                            if self.storage and hasattr(self.storage, 'get_all_instruments_summary'):
                                # 从 storage 获取所有缓存合约
                                all_instruments = self.storage.get_all_instruments_summary()
                                logging.info(f"[Instruments] Storage 缓存：{len(all_instruments)} 个合约")
                                
                                if all_instruments and len(all_instruments) > 0:
                                    # 提取 instrument_id 列表
                                    instrument_list = [inst['instrument_id'] for inst in all_instruments if inst.get('instrument_id')]
                                    logging.info(f"[Instruments] 从 storage 获取到 {len(instrument_list)} 个合约")
                                    
                                    # 使用 storage 分类
                                    futures_list, options_dict = self.storage.classify_instruments(instrument_list)
                                    total_options = sum(len(v) for v in options_dict.values())
                                    logging.info(f"[Instruments] ✅ storage 分类结果：{len(futures_list)} 期货，{total_options} 期权")
                                    
                                    # 存储到实例属性
                                    self._futures_instruments = futures_list
                                    self._option_instruments = options_dict
                                    
                                    # 使用 SubscriptionManager 进行全量订阅
                                    if hasattr(self.storage, 'subscription_manager'):
                                        logging.info("[Subscribe] 使用 SubscriptionManager 统一订阅...")
                                        self.storage.subscription_manager.subscribe_all_instruments(
                                            futures_list,
                                            options_dict
                                        )
                                    else:
                                        logging.warning("[Subscribe] 无 subscription_manager，跳过订阅")
                                else:
                                    logging.warning("[Instruments] Storage 缓存为空")
                                    self._futures_instruments = []
                                    self._option_instruments = {}
                            else:
                                logging.warning("[Instruments] storage 不可用或无 get_all_instruments_summary")
                                self._futures_instruments = []
                                self._option_instruments = {}
                        except Exception as e:
                            logging.warning(f"[Instruments] 从 storage 加载失败：{e}")
                            self._futures_instruments = []
                            self._option_instruments = {}
                if not self._futures_instruments and not self._option_instruments:
                    logging.info("[Instruments] strategy_instance 无有效合约")
                    # 注意：不再从配置文件加载示例数据
                    # 实际运行时合约数据应由平台注入
                
                # 2. 执行订阅 - ✅ 直接传合约 ID，wrapper 会推断交易所
                futures_instruments = getattr(self, '_futures_instruments', []) or []
                option_instruments = getattr(self, '_option_instruments', {}) or {}
                
                subscribe_list = []
                
                # 订阅期货合约
                for inst in futures_instruments:
                    # ✅ 如果是字符串，直接传；如果是对象，提取 instrument_id
                    if isinstance(inst, str):
                        subscribe_list.append(inst)
                    else:
                        instrument_id = self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id')
                        if instrument_id:
                            subscribe_list.append(instrument_id)
                
                # 订阅期权合约（如果启用）
                params = getattr(self, 'params', None)
                if get_param_value(params, 'subscribe_options', True):
                    for contracts in option_instruments.values():
                        for inst in contracts or []:
                            # ✅ 如果是字符串，直接传；如果是对象，提取 instrument_id
                            if isinstance(inst, str):
                                subscribe_list.append(inst)
                            else:
                                instrument_id = self._get_instrument_value(inst, 'InstrumentID', 'instrument_id', '_instrument_id')
                                if instrument_id:
                                    subscribe_list.append(instrument_id)
                
                # 去重，保持顺序
                seen = set()
                subscribe_list = [inst for inst in subscribe_list if not (inst in seen or seen.add(inst))]
                
                self._subscribed_instruments = list(subscribe_list)
                
                logging.info(
                    f"[Subscribe] 全量订阅："
                    f"期货={len(futures_instruments)} 个，"
                    f"期权={sum(len(opts) for opts in option_instruments.values()) if option_instruments else 0} 个，"
                    f"总计={len(subscribe_list)} 个合约"
                )
                
                # 调用平台订阅接口
                if callable(self.subscribe):
                    for inst in subscribe_list:
                        try:
                            self.subscribe(inst)
                        except Exception as e:
                            logging.warning(f"[Subscribe] 订阅失败 {inst}: {e}")
                    
            except Exception as e:
                logging.error(f"[StrategyCoreService.on_start] Subscription failed: {e}")
                logging.exception("[StrategyCoreService.on_start] Subscription stack:")
            # ========================================================
            
            # 发布事件
            self._publish_event('StrategyStarted', {
                'strategy_id': self.strategy_id
            })
            
            return True
        
    # ========== 已删除：_subscribe_instruments() ==========
    # 原因：与 storage.subscribe() 功能重复
    # 新实现：直接在 on_start() 中调用 storage.load_all_instruments() 和 platform.subscribe()
    # 参考：storage.py L936 subscribe() 方法
    # ===============================================
        
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
        # ✅ 不再 shutdown 全局 storage 单例（因为它会被其他策略复用）
        # ❌ 如果 shutdown 了全局单例，下次重启时写入线程无法恢复
        # self._storage 会在策略实例销毁时自动释放，但全局单例继续运行
        
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
            historical_load_started = getattr(self, '_historical_load_started', False)
            
            # 获取 K 线统计
            kline_stats = {}
            # market_data_service 已移除，使用 storage.py 直接管理
            
            logging.info(
                "[HKL] First tick diagnostics: "
                f"state={self._state}, is_running={self._is_running}, is_paused={self._is_paused}, "
                f"historical_started={historical_load_started}, "
                f"subscribed_count={len(self._subscribed_instruments)}, "
                f"has_market_data_service=False"
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
            # market_data_service 已移除，使用 storage.py 直接管理
            
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
K 线数据：总计 {self._stats['total_klines']:,} 条
交易数量：总计 {self._stats['total_trades']:,} 笔
信号数量：总计 {self._stats['total_signals']:,} 个
错误统计：{self._stats['errors_count']} 次
最后错误：{self._stats['last_error_time'].strftime('%H:%M:%S') if self._stats['last_error_time'] else '无'}
策略状态：{self._state.value}
是否运行：{self._is_running}
是否暂停：{self._is_paused}

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

            # ✅ 保存 Tick 数据到数据库并合成 K 线
            if hasattr(self, 'storage') and self.storage:
                try:
                    tick_data = {
                        'timestamp': time.time(),  # ✅ 修复：使用 timestamp 而不是 ts
                        'instrument_id': instrument_id,
                        'exchange': exchange,
                        'last_price': last_price,
                        'volume': volume,
                        'amount': getattr(tick, 'amount', 0.0),
                        'open_interest': getattr(tick, 'open_interest', 0.0)
                    }
                    # 使用 process_tick 代替 save_tick，自动进行 K 线合成
                    self.storage.process_tick(tick_data)
                except Exception as e:
                    logging.error(f"[_process_tick] Failed to process tick: {e}")
                    logging.exception(f"[_process_tick] Stack trace for instrument: {instrument_id}")
            
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
        """检查合约是否为指定月或下月合约（已废弃 - 全量模式，不过滤）"""
        # ✅ 已删除 month_mapping 过滤逻辑 (2026-03-27)
        # 原因: 推翻原始报告的"全量模式"错误结论,采用真正的全量模式
        # 影响: 不再根据 month_mapping 过滤合约,所有合约都返回 True

        try:
            # ✅ 全量模式：直接返回 True,不过滤任何合约
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
    
    # ========== 诊断功能已移至 diagnosis_service.py ==========
    # 使用方式:
    #   from ali2026v3_trading.diagnosis_service import StrategyDiagnoser
    #   diagnoser = StrategyDiagnoser(strategy_instance=self)
    #   report = diagnoser.generate_full_link_diagnosis()
    # =======================================================
    
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
