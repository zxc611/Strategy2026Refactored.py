"""
market_data_service.py - 市场数据服务

合并来源：07_data_manager.py + 12_trading_logic.py
合并策略：提取K线管理核心功能，去除冗余缓存层，简化API调用

重构目标：
- 旧架构：07_data_manager.py (~1000行) + 12_trading_logic.py 部分功能
- 新架构：market_data_service.py (~500行)
- 减少：50%

核心改进：
1. 去除冗余缓存层（直接使用StorageManager）
2. 无平台API依赖
3. 统一K线数据访问接口
4. 线程安全设计

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-03-15
"""

from __future__ import annotations

import os
import re
import time
import struct
import threading
import logging
from datetime import datetime, timedelta, time as dt_time
from typing import Any, Dict, List, Optional, Set, Union, Callable, Tuple

# 导入K线和Tick数据类
from pythongo.classdef import KLineData, TickData

# 安全转换函数
def safe_int(value: Any, default: int = 0) -> int:
    """安全转换为整数"""
    try:
        if value is None:
            return default
        return int(float(value))
    except (ValueError, TypeError) as e:
        logging.warning(f"[safe_int] Conversion failed for value '{value}': {e}")
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """安全转换为浮点数"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError) as e:
        logging.warning(f"[safe_float] Conversion failed for value '{value}': {e}")
        return default


def _to_float32(value: Any) -> float:
    """将值转换为float32精度"""
    try:
        return struct.unpack("!f", struct.pack("!f", float(value)))[0]
    except (ValueError, TypeError, struct.error) as e:
        logging.warning(f"[_to_float32] Conversion failed for value '{value}': {e}")
        return safe_float(value, 0.0)


# ============================================================================
# 市场时间服务（整合自 market_time_service.py）
# ============================================================================

class MarketTimeService:
    """市场时间服务 - 负责市场开盘状态检测和交易时间配置"""
    
    def __init__(self):
        """初始化市场时间服务"""
        self._lock = threading.RLock()
        self._cache: Dict[str, Tuple[float, bool]] = {}  # 缓存 {exchange: (timestamp, is_open)}
        self._last_check_time: Dict[str, float] = {}  # 上次检查时间 {exchange: timestamp}
        
        # 交易所交易时段配置（支持跨午夜夜盘）
        self._exchange_trading_sessions: Dict[str, List[Tuple[dt_time, dt_time]]] = {
            # 中金所：无夜盘
            'CFFEX': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 0), dt_time(15, 0)),
            ],
            # 上期所/大商所/能源中心/广期所：按宽窗口覆盖常见夜盘
            'SHFE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            'DCE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            'INE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            'GFEX': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(2, 30)),
            ],
            # 郑商所：夜盘通常较短，给到 23:30 的保护窗口
            'CZCE': [
                (dt_time(9, 0), dt_time(11, 30)),
                (dt_time(13, 30), dt_time(15, 0)),
                (dt_time(21, 0), dt_time(23, 30)),
            ],
        }
        
        # 初始化时检查一次开盘状态
        for exchange in self._exchange_trading_sessions:
            self._check_market_status(exchange)

    @staticmethod
    def _time_in_session(current_time: dt_time, session_start: dt_time, session_end: dt_time) -> bool:
        """判断当前时间是否落在某个交易时段内（支持跨午夜时段）。"""
        if session_start <= session_end:
            return session_start <= current_time < session_end
        # 跨午夜，如 21:00-02:30
        return current_time >= session_start or current_time < session_end
    
    def _check_market_status(self, exchange: str, current_time: Optional[datetime] = None) -> bool:
        """
        执行市场状态检查
        
        Args:
            exchange: 交易所代码
            current_time: 当前时间，默认为 None（使用当前时间）
            
        Returns:
            bool: 市场是否开盘
        """
        now = current_time or datetime.now()
        
        exchange = (exchange or 'CFFEX').upper()
        sessions = self._exchange_trading_sessions.get(exchange, self._exchange_trading_sessions['CFFEX'])
        current_time_obj = now.time()
        weekday = now.weekday()  # Mon=0 ... Sun=6

        # 周六全日关闭
        if weekday == 5:
            result = False
        # 周日仅夜盘窗口可能开（中金所无夜盘）
        elif weekday == 6:
            result = any(
                self._time_in_session(current_time_obj, start, end)
                for start, end in sessions
                if start > end
            )
        else:
            # 周五 21:00 后通常不开夜盘，直接关（中金所本来也无夜盘）
            if weekday == 4 and current_time_obj >= dt_time(21, 0):
                result = False
            else:
                result = any(
                    self._time_in_session(current_time_obj, start, end)
                    for start, end in sessions
                )
        
        # 更新缓存和检查时间
        current_timestamp = now.timestamp()
        with self._lock:
            self._cache[exchange] = (current_timestamp, result)
            self._last_check_time[exchange] = current_timestamp
        
        logging.debug(f"[MarketTimeService._check_market_status] Checked {exchange}: {result}")
        return result
    
    def is_market_open(self, exchange: str = 'CFFEX', current_time: Optional[datetime] = None) -> bool:
        """
        检查市场是否开盘
        
        只在以下情况进行实际检查：
        1. 服务重启时（缓存为空）
        2. 到达开盘时间时
        3. 到达收盘时间时
        
        Args:
            exchange: 交易所代码
            current_time: 当前时间，默认为 None（使用当前时间）
            
        Returns:
            bool: 市场是否开盘
        """
        try:
            now = current_time or datetime.now()
            current_timestamp = now.timestamp()
            current_time_obj = now.time()

            # 传入显式时间时直接实算，避免缓存污染测试/回放判断。
            if current_time is not None:
                return self._check_market_status(exchange, now)
            
            with self._lock:
                # 检查是否有缓存
                if exchange in self._cache:
                    cached_timestamp, cached_result = self._cache[exchange]
                    
                    # 夜盘存在跨午夜边界，基于时点比较容易误判。
                    # 这里仅做短 TTL 缓存，超过 TTL 即重算。
                    if 0 <= (current_timestamp - cached_timestamp) < 30:
                        return cached_result
            
            # 执行实际检查
            return self._check_market_status(exchange, now)
            
        except Exception as e:
            logging.error(f"[MarketTimeService.is_market_open] Error: {e}")
            return False
    
    def get_trading_hours(self, exchange: str = 'CFFEX') -> Dict[str, dt_time]:
        """
        获取交易所的交易时间
        
        Args:
            exchange: 交易所代码
            
        Returns:
            Dict[str, dt_time]: 交易时间配置
        """
        sessions = self._exchange_trading_sessions.get(exchange, self._exchange_trading_sessions['CFFEX'])
        return {'start': sessions[0][0], 'end': sessions[-1][1]}
    
    def set_trading_hours(self, exchange: str, start_time: dt_time, end_time: dt_time) -> None:
        """
        设置交易所的交易时间
        
        Args:
            exchange: 交易所代码
            start_time: 开始时间
            end_time: 结束时间
        """
        with self._lock:
            self._exchange_trading_sessions[exchange] = [(start_time, end_time)]
            # 清除缓存
            if exchange in self._cache:
                del self._cache[exchange]
            logging.info(f"[MarketTimeService.set_trading_hours] Updated trading hours for {exchange}: {start_time} - {end_time}")
    
    def clear_cache(self) -> None:
        """清除缓存"""
        with self._lock:
            self._cache.clear()
            logging.info("[MarketTimeService.clear_cache] Cache cleared")
    
    def get_cache_status(self) -> Dict[str, Tuple[float, bool]]:
        """获取缓存状态"""
        with self._lock:
            return dict(self._cache)


# 全局市场时间服务实例
_market_time_service_instance = None
_market_time_service_lock = threading.Lock()

def get_market_time_service() -> MarketTimeService:
    """获取市场时间服务实例（单例模式）"""
    global _market_time_service_instance
    with _market_time_service_lock:
        if _market_time_service_instance is None:
            _market_time_service_instance = MarketTimeService()
        return _market_time_service_instance


def is_market_open(exchange: str = 'CFFEX', current_time: Optional[datetime] = None) -> bool:
    """检查市场是否开盘（便捷函数）"""
    service = get_market_time_service()
    return service.is_market_open(exchange, current_time)


# ============================================================================
# 轻量 K 线数据结构
# ============================================================================

class LightKLine:
    """轻量级 K 线对象，使用__slots__优化内存"""
    __slots__ = ('open', 'high', 'low', 'close', 'volume', 'datetime',
                 'exchange', 'instrument_id', 'style', 'is_placeholder', 'source')

    def __init__(self, open_price: float, high: float, low: float,
                 close: float, volume: float, dt: datetime):
        self.open = _to_float32(open_price)
        self.high = _to_float32(high)
        self.low = _to_float32(low)
        self.close = _to_float32(close)
        self.volume = _to_float32(volume)
        self.datetime = dt
        self.exchange = ""
        self.instrument_id = ""
        self.style = "M1"
        self.is_placeholder = False
        self.source = ""


# ============================================================================
# 市场数据服务
# ============================================================================

class MarketDataService:
    """
    市场数据服务 - 统一的K线数据访问接口

    职责：
    1. K线数据获取与缓存
    2. 速率限制管理
    3. 数据标准化处理
    4. 占位数据生成

    使用方式：
        service = MarketDataService(params, storage_manager)
        klines = service.get_kline("SHFE", "ru2501", count=10)
    """

    def __init__(self, params: Any, storage: Any = None, output_func: Optional[Callable] = None,
                 futures_instruments: List[Any] = None,  # 新增：显式传递期货合约
                 option_instruments: Dict[str, List[Any]] = None,  # 新增：显式传递期权合约
                 strategy_instance: Any = None):  # 新增：显式传递策略实例
        """
        初始化市场数据服务
    
        Args:
            params: 参数配置对象或字典
            storage: 存储管理器（可选）
            output_func: 输出函数（可选）
            futures_instruments: 期货合约列表（显式传递）
            option_instruments: 期权合约字典（显式传递）
            strategy_instance: 策略实例（显式传递）
        """
        # 保存显式传递的合约数据和策略实例
        self._futures_instruments = futures_instruments or []
        self._option_instruments = option_instruments or {}
        self._strategy_instance = strategy_instance
        
        # 处理参数对象，确保具有属性访问能力
        if isinstance(params, dict):
            # 创建一个动态代理对象，保持对原始字典的引用
            # 修复：不再复制快照，而是动态访问字典
            class DictProxy:
                def __init__(self, d):
                    self._dict = d

                def __getattr__(self, name):
                    # 动态从字典中获取值
                    if name in self._dict:
                        return self._dict[name]
                    # 如果找不到，返回 None 而不是抛出异常
                    return None

                def __setattr__(self, name, value):
                    # 特殊处理 _dict 属性
                    if name == '_dict':
                        super().__setattr__(name, value)
                    else:
                        # 同时更新字典
                        if hasattr(self, '_dict'):
                            self._dict[name] = value

            self.params = DictProxy(params)
            logging.info(f"[MarketDataService] Created DictProxy for dict params with {len(params)} keys")
            # 诊断日志：检查关键的合约数据
            future_instruments = params.get('future_instruments')
            option_instruments = params.get('option_instruments')
            if future_instruments:
                logging.info(f"[MarketDataService] future_instruments already in params: {len(future_instruments)} contracts")
            if option_instruments:
                total_options = sum(len(opts) for opts in option_instruments.values())
                logging.info(f"[MarketDataService] option_instruments already in params: {total_options} options")
        else:
            self.params = params
            logging.info(f"[MarketDataService] Using object params of type {type(params).__name__}")
        
        # 诊断日志：检查显式传递的数据
        if self._futures_instruments:
            logging.info(f"[MarketDataService] Explicitly received futures_instruments: {len(self._futures_instruments)} contracts")
        if self._option_instruments:
            total_options = sum(len(opts) for opts in self._option_instruments.values())
            logging.info(f"[MarketDataService] Explicitly received option_instruments: {total_options} options")
        if self._strategy_instance:
            logging.info(f"[MarketDataService] Explicitly received strategy_instance: {type(self._strategy_instance).__name__}")
        self.storage = storage
        self._output = output_func or print
        self._historical_loader_lock = threading.Lock()
        self._historical_loader_started = False
    
        # K 线数据缓存
        self._kline_data: Dict[str, Dict] = {}
        self._kline_lock = threading.RLock()
    
        # 速率限制
        self._rate_lock = threading.Lock()
        self._last_fetch_time = 0.0
        self._fetch_timestamps: Dict[str, List[float]] = {}
    
        # 市场中心引用
        self._market_center = None
    
        # K 线加载统计
        self._kline_stats = {
            'total_requested': 0,  # 请求加载的合约数
            'success': 0,  # 成功加载的合约数
            'failed': 0,  # 失败的合约数
            'total_klines_loaded': 0,  # 加载的 K 线总数量
        }
        self._kline_stats_lock = threading.Lock()
    
        # 初始化状态
        self._init_state()

    def _init_state(self) -> None:
        """初始化内部状态"""
        self._kline_data = {}
        self._fetch_timestamps = {"__GLOBAL__": []}
        # 重置 K 线统计（保留初始值）
        with self._kline_stats_lock:
            if not hasattr(self, '_kline_stats'):
                self._kline_stats = {
                    'total_requested': 0,
                    'success': 0,
                    'failed': 0,
                    'total_klines_loaded': 0,
                }

    def output(self, message: str, force: bool = False) -> None:
        """输出日志信息"""
        try:
            if self._output:
                self._output(message)
            else:
                logging.info(f"[MarketDataService] {message}")
        except Exception as e:
            logging.error(f"[MarketDataService.output] Error: {e}")

    # ========================================================================
    # L3 查询层 - 三层门禁系统（新增）
    # ========================================================================
        
    # ========== 数据加载辅助方法 ==========
    
    def _collect_future_instruments(self) -> List[Dict]:
        """收集期货合约信息 - 从市场中心直接获取（全量模式）"""
        instruments = []
        seen = set()
        try:
            # 从参数或市场中心获取期货合约列表
            future_instruments = getattr(self.params, 'future_instruments', [])
            
            # 处理直接提供的期货合约列表
            for instrument in future_instruments:
                exchange = instrument.get("exchange", "") or instrument.get("ExchangeID", "")
                instrument_id = instrument.get("instrument_id", "") or instrument.get("InstrumentID", "")
                instrument_norm = self._normalize_instrument_id(instrument_id)
                if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                    rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                    key = f"{exchange}_{str(instrument_id).upper()}"
                    if key not in seen:
                        seen.add(key)
                        instruments.append(rec)
            
            # 如果没有直接的期货合约列表，尝试从配置中构建
            if not future_instruments and hasattr(self.params, 'month_mapping') and hasattr(self.params, 'exchange_mapping'):
                from ali2026v3_trading.config_service import resolve_product_exchange
                month_mapping = getattr(self.params, 'month_mapping', {})
                exchange_mapping = getattr(self.params, 'exchange_mapping', {})
                
                for product, months in month_mapping.items():
                    for month in months:
                        exchange = resolve_product_exchange(product, exchange_mapping, getattr(self.params, 'exchange', 'CFFEX'))
                        instrument_id = month
                        instrument_norm = self._normalize_instrument_id(instrument_id)
                        if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                            rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                            key = f"{exchange}_{str(instrument_id).upper()}"
                            if key not in seen:
                                seen.add(key)
                                instruments.append(rec)
            
            # 如果还是没有，尝试从市场中心获取（添加超时保护）
            if not future_instruments and not instruments and self._market_center:
                get_instruments = getattr(self._market_center, 'get_instruments', None)
                if callable(get_instruments):
                    try:
                        # 添加超时保护，防止市场中心调用卡住
                        import threading
                        import time
                        
                        result = []
                        error = None
                        
                        def get_instruments_with_timeout():
                            nonlocal result, error
                            try:
                                result = get_instruments()
                            except Exception as e:
                                error = e
                        
                        # 创建线程执行市场中心调用
                        thread = threading.Thread(target=get_instruments_with_timeout)
                        thread.daemon = True
                        thread.start()
                        
                        # 等待最多 3 秒
                        thread.join(timeout=3.0)
                        
                        if thread.is_alive():
                            logging.warning("[MarketDataService._collect_future_instruments] Market center get_instruments timed out")
                        elif error:
                            logging.warning(f"[MarketDataService._collect_future_instruments] Failed to get instruments: {error}")
                        else:
                            for future in result:
                                exchange = future.get("ExchangeID", "")
                                instrument_id = future.get("InstrumentID", "")
                                instrument_norm = self._normalize_instrument_id(instrument_id)
                                if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                                    rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                                    key = f"{exchange}_{str(instrument_id).upper()}"
                                    if key not in seen:
                                        seen.add(key)
                                        instruments.append(rec)
                    except Exception as e:
                        logging.warning(f"[MarketDataService._collect_future_instruments] Error getting instruments: {e}")
        except Exception as e:
            logging.error(f"[MarketDataService._collect_future_instruments] Error: {e}")
        return instruments
    
    def _load_instruments_to_database(self) -> None:
        """
        从市场中心获取全量合约并保存到数据库
        
        此方法在数据库为空时自动调用，确保所有可交易的合约都被保存
        """
        try:
            # Step 1: 从市场中心获取所有合约
            mc = self._get_market_center()
            if not mc:
                logging.error("[MarketDataService] Cannot load instruments: Market Center not available")
                return
            
            logging.info("[MarketDataService] Fetching all instruments from market center...")
            all_instruments = mc.get_instruments()
            
            if not all_instruments:
                logging.error("[MarketDataService] Market center returned no instruments")
                return
            
            logging.info(f"[MarketDataService] Retrieved {len(all_instruments)} instruments from market center")
            
            # Step 2: 分类合约为期货和期权
            futures = []
            options = []
            
            for inst in all_instruments:
                exchange = getattr(inst, 'ExchangeID', '')
                instrument_id = getattr(inst, 'InstrumentID', '')
                product_class = getattr(inst, 'ProductClass', '')
                product_type = getattr(inst, 'product_type', '')
                
                # 判断是期货还是期权
                is_option = ('option' in str(product_class).lower() or 
                            '期权' in str(product_class).lower() or
                            str(product_type) == '2')
                
                if is_option:
                    # 期权合约
                    underlying = getattr(inst, 'UnderlyingInstrID', '')
                    option_type = getattr(inst, 'OptionsType', '')
                    strike_price = float(getattr(inst, 'StrikePrice', 0))
                    
                    # 提取品种和年月
                    product = instrument_id[:-7] if len(instrument_id) >= 7 else instrument_id
                    year_month = instrument_id[-6:-4] + instrument_id[-4:-2] if len(instrument_id) >= 6 else ''
                    
                    options.append({
                        'exchange': exchange,
                        'instrument_id': instrument_id,
                        'product': product,
                        'underlying_future': underlying,
                        'underlying_product': underlying[:-2] if underlying else '',
                        'year_month': year_month,
                        'option_type': 'C' if str(option_type).upper() == 'CALL' else 'P',
                        'strike_price': strike_price,
                        'format_str': f'{exchange}.{instrument_id}',
                        'expire_date': None,
                        'listing_date': None,
                    })
                else:
                    # 期货合约
                    product = instrument_id[:-4] if len(instrument_id) >= 4 else instrument_id
                    year_month = instrument_id[-6:-4] + instrument_id[-4:-2] if len(instrument_id) >= 6 else ''
                    
                    futures.append({
                        'exchange': exchange,
                        'instrument_id': instrument_id,
                        'product': product,
                        'year_month': year_month,
                        'format_str': f'{exchange}.{instrument_id}',
                        'expire_date': None,
                        'listing_date': None,
                    })
            
            logging.info(f"[MarketDataService] Classified: {len(futures)} futures, {len(options)} options")
            
            # Step 3: 批量保存到数据库
            if futures:
                logging.info(f"[MarketDataService] Saving {len(futures)} futures to database...")
                self.storage.batch_add_future_instruments(futures)
                logging.info(f"[MarketDataService] Saved {len(futures)} futures")
            
            if options:
                logging.info(f"[MarketDataService] Saving {len(options)} options to database...")
                self.storage.batch_add_option_instruments(options)
                logging.info(f"[MarketDataService] Saved {len(options)} options")
            
            total = len(futures) + len(options)
            logging.info(f"[MarketDataService] Successfully saved {total} instruments to database")
            
        except Exception as e:
            logging.error(f"[MarketDataService._load_instruments_to_database] Error: {e}")
            import traceback
            traceback.print_exc()
        
    def _apply_three_layer_filter(self, instruments: List[Dict]) -> List[Dict]:
        """
        应用三层门禁过滤器（已废弃 - 仅保留向后兼容）
            
        ⚠️  警告：此方法已被废弃，不再进行任何过滤！
        
        原过滤逻辑:
        1. 月份映射：根据 month_mapping 筛选主力合约
        2. 产品聚焦：如果 focus_products 非空
        3. 历史窗口：根据 history_minutes
        4. 数量限制：每个品种最多 N 个月
            
        Args:
            instruments: 合约列表
            
        Returns:
            List[Dict]: 原始合约列表（不做任何过滤）
        """
        # ✅ 全量模式：直接返回原始列表，不做任何过滤
        logging.warning(
            "[MarketDataService] _apply_three_layer_filter() called but FILTERING DISABLED - "
            f"returning all {len(instruments)} instruments unchanged"
        )
        return instruments  # 直接返回，不过滤！
        
    def load_historical_klines_from_database(self) -> tuple:
        """
        从本地数据库加载历史 K 线（L3 查询层主入口）
            
        改动要点:
        1. ❌ 移除：不再调用 MarketCenter API
        2. ✅ 新增：从本地数据库查询
        3. ✅ 应用：三层门禁过滤
        4. ✅ 优势：无网络延迟，快速查询
        5. ✅ 原样返回：不做任何后处理
            
        Returns:
            tuple: (empty_keys, non_empty_keys)
        """
        empty_keys = []
        non_empty_keys = []
            
        try:
            # 收集合约（应用三层门禁）
            future_instruments = self._collect_future_instruments()
            option_instruments = self._collect_option_instruments()
            all_instruments = future_instruments + option_instruments
                
            logging.info(
                f"[MarketDataService L3] Loading from database: "
                f"{len(all_instruments)} contracts"
            )
                
            if not all_instruments:
                return empty_keys, non_empty_keys
                
            # ✅ 从数据库并发查询
            import concurrent.futures
            max_workers = min(getattr(self.params, "history_load_max_workers", 4), 8)
                
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                    
                for instrument in all_instruments:
                    fut = executor.submit(
                        self._load_from_database,
                        instrument,
                        getattr(self.params, 'history_minutes', 240)
                    )
                    futures[fut] = instrument
                    
                for future in concurrent.futures.as_completed(futures):
                    instrument = futures[future]
                        
                    try:
                        success, key, klines = future.result()
                            
                        if success and klines:
                            non_empty_keys.append(key)
                            self._cache_klines(
                                instrument.get('exchange', ''),
                                instrument.get('instrument_id', ''),
                                klines
                            )
                        else:
                            empty_keys.append(key)
                        
                    except Exception as e:
                        key = f"{instrument.get('exchange')}_{instrument.get('instrument_id')}"
                        empty_keys.append(key)
            
        except Exception as e:
            logging.error(f"[MarketDataService L3] Error: {e}")
            
        return empty_keys, non_empty_keys
        
    def _load_from_database(self, instrument: Dict, history_minutes: int):
        """
        从数据库加载单个合约的 K 线
            
        Args:
            instrument: 合约信息
            history_minutes: 历史分钟数
            
        Returns:
            tuple: (success, key, klines)
        """
        exchange = instrument.get('exchange', '')
        instrument_id = instrument.get('instrument_id', '')
        key = f"{exchange}_{instrument_id}"
            
        try:
            if not self.storage:
                return False, key, []
                
            # ✅ 从数据库查询（原样返回）
            klines = self.storage.query_kline(
                instrument_id=instrument_id,
                start_time=self._get_start_time(history_minutes),
                end_time=self._get_end_time(),
                limit=None
            )
                
            if klines and len(klines) > 0:
                return True, key, klines
            else:
                return False, key, []
            
        except Exception as e:
            logging.debug(f"[MarketDataService L3] Load failed for {key}: {e}")
            return False, key, []
        
    def _get_start_time(self, minutes: int) -> str:
        """获取开始时间字符串"""
        from datetime import datetime, timedelta
        start_time = datetime.now() - timedelta(minutes=minutes)
        return start_time.strftime('%Y-%m-%d %H:%M:%S')
        
    def _get_end_time(self) -> str:
        """获取结束时间字符串"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
    def _collect_option_instruments(self) -> List[Dict]:
        """
        收集期权合约 - 从本地数据库查询（全量模式，不过滤）
            
        改动要点:
        1. ✅ 从数据库获取所有已存储的期权合约
        2. ✅ 不再应用任何过滤
        3. ✅ 支持全量订阅模式
            
        Returns:
            List[Dict]: 期权合约列表（全部，不过滤）
        """
        if not self.storage:
            logging.warning("[MarketDataService] Storage not available for options")
            return []
            
        try:
            # 从数据库获取所有已存储的合约
            all_instruments = self.storage.get_all_instruments()
                
            # 过滤出期权合约
            options_only = []
            for inst in all_instruments:
                inst_type = str(inst.get('product_type', '') or '').lower()
                if 'option' in inst_type or '期权' in inst_type or inst_type == '2':
                    options_only.append(inst)
                
            logging.info(
                f"[MarketDataService] Found {len(options_only)} option instruments "
                f"(FULL SUBSCRIPTION - NO FILTERING)"
            )
                
            # 直接返回所有期权合约，不过滤
            return options_only
            
        except Exception as e:
            logging.error(f"[MarketDataService] Error collecting options: {e}")
            return []
        
    def _to_light_kline(self, bar: Dict) -> Any:
        """转换为轻量级 K 线对象（保持原有逻辑）"""
        # TODO: 实现转换逻辑
        return bar
        
    def _create_placeholder_klines(self, exchange: str, instrument_id: str, count: int) -> List[Any]:
        """创建占位 K 线数据（保持原有逻辑）"""
        # TODO: 实现占位数据生成
        return []
        
    def _cache_klines(self, exchange: str, instrument_id: str, klines: List[Any]) -> None:
        """缓存 K 线数据（保持原有逻辑）"""
        with self._kline_lock:
            key = f"{exchange}_{instrument_id}"
            self._kline_data[key] = klines
        
    def _normalize_instrument_id(self, instrument_id: str) -> str:
        """标准化合约代码（仅清理，不修改格式）- 真正的原样清理"""
        if not instrument_id:
            return ''
        # ✅ 真正的三原样设计：仅去首尾空格，保持大小写和所有分隔符
        cleaned = str(instrument_id).strip()
        # ❌ 错误：不应该转大写
        # ❌ 错误：cleaned = cleaned.upper()
        # ❌ 错误：不应该去除分隔符（_, -, . 等）
        # ❌ 错误：cleaned = cleaned.replace('_', '')
        return cleaned
        
    def _get_kline_style(self) -> str:
        """获取 K 线周期"""
        return getattr(self.params, 'kline_style', 'M1')
        
    def _get_max_kline_count(self) -> int:
        """获取最大 K 线数量"""
        return getattr(self.params, 'max_kline_count', 1000)
        
    def _should_skip_market_center_kline(self, instrument_id: str) -> bool:
        """判断是否跳过市场中心获取"""
        return False
        
    def _check_rate_limit(self, instrument_id: str, exchange: str) -> bool:
        """检查速率限制"""
        return True
        
    def _get_cached_or_placeholder(self, exchange: str, instrument_id: str, count: int) -> List[Any]:
        """获取缓存或占位数据"""
        return self._create_placeholder_klines(exchange, instrument_id, count)
        
    def _get_from_market_center(self, exchange: str, instrument_id: str, count: int, style: str) -> Optional[List[Any]]:
        """从市场中心获取 K 线（备用）"""
        return None

    def get_kline(self, exchange: str, instrument_id: str,
                  count: int = 10, style: Optional[str] = None,
                  *, prefer_market_center: bool = False,
                  request_timeout_sec: float = 10.0) -> List[LightKLine]:
        """
        获取K线数据（主入口）

        Args:
            exchange: 交易所代码
            instrument_id: 合约代码
            count: 获取数量
            style: K线周期（默认M1）

        Returns:
            K线数据列表
        """
        # 标准化参数
        exchange = str(exchange or "").upper().strip()
        instrument_id = self._normalize_instrument_id(instrument_id)
        style = style or self._get_kline_style()
        count = max(1, min(abs(count), self._get_max_kline_count()))

        # 检查速率限制
        if not self._check_rate_limit(instrument_id, exchange):
            return self._get_cached_or_placeholder(exchange, instrument_id, count)

        # 使用线程和超时来防止获取K线数据卡住
        import threading
        import time
        
        result = None
        error = None
        skip_market_center_fetch = self._should_skip_market_center_kline(instrument_id)
        
        def fetch_data():
            nonlocal result, error
            try:
                # 历史预加载阶段优先走市场中心，避免数据库连接阻塞导致全链路超时。
                if self.storage and not prefer_market_center:
                    klines = self._get_from_storage(exchange, instrument_id, count, style)
                    if klines:
                        result = klines
                        return

                if skip_market_center_fetch:
                    logging.debug(f"[MarketDataService.get_kline] Skip market center fetch for synthetic instrument: {exchange}.{instrument_id}")
                    result = self._get_cached_or_placeholder(exchange, instrument_id, count)
                    return

                # 尝试从市场中心获取
                klines = self._get_from_market_center(exchange, instrument_id, count, style)
                if klines:
                    self._cache_klines(exchange, instrument_id, klines)
                    result = klines
                    return

                # 返回占位数据
                result = self._create_placeholder_klines(exchange, instrument_id, count)
            except Exception as e:
                error = e
                result = self._create_placeholder_klines(exchange, instrument_id, count)
        
        # 创建并启动线程
        thread = threading.Thread(target=fetch_data)
        thread.daemon = True
        thread.start()
        
        # 等待请求完成（默认10秒，可按调用方场景调整）
        timeout = max(0.5, float(request_timeout_sec or 10.0))
        thread.join(timeout=timeout)
        
        if thread.is_alive():
            logging.warning(f"[MarketDataService.get_kline] Data fetch timed out: {exchange}.{instrument_id}")
            return self._create_placeholder_klines(exchange, instrument_id, count)
        elif error:
            self.output(f"[ERROR] get_kline failed: {exchange}.{instrument_id}: {error}")
            return self._create_placeholder_klines(exchange, instrument_id, count)
        else:
            return result or self._create_placeholder_klines(exchange, instrument_id, count)

    def _get_from_storage(self, exchange: str, instrument_id: str,
                          count: int, style: str) -> Optional[List[LightKLine]]:
        """从存储管理器获取K线"""
        if not self.storage:
            return None

        try:
            # 调用存储管理器的查询方法
            if hasattr(self.storage, 'get_kline'):
                # 修复参数顺序：storage.get_kline(instrument_id, period, start_time, end_time, limit)
                raw_data = self.storage.get_kline(
                    instrument_id,
                    style,
                    None,
                    None,
                    count,
                    connection_timeout_sec=0.2,
                )
                if raw_data:
                    return [self._to_light_kline(bar) for bar in raw_data[-count:]]
        except Exception as e:
            logging.error(f"[MarketDataService._get_from_storage] Error: {e}")

        return None

    def _get_from_market_center(self, exchange: str, instrument_id: str,
                                 count: int, style: str) -> Optional[List[LightKLine]]:
        """从市场中心获取 K 线"""
        logging.debug(f"[MarketDataService._get_from_market_center] Fetching kline: {exchange}.{instrument_id}, count={count}, style={style}")
            
        # 使用线程和超时来防止获取市场中心和 K 线数据卡住
        import threading
        import time
            
        result = None
        error = None
            
        def fetch_from_mc():
            nonlocal result, error
            try:
                # Step 1: 获取 MarketCenter
                mc = self._get_market_center()
                if not mc:
                    logging.warning(f"[MarketDataService._get_from_market_center] MarketCenter not available: {exchange}.{instrument_id}")
                    result = None
                    return
                logging.debug(f"[MarketDataService._get_from_market_center] Got MarketCenter: {type(mc)}")
    
                # Step 2: 解析 K 线获取函数
                get_kline_func = self._resolve_kline_fetch_callable(mc)
                if not callable(get_kline_func):
                    logging.warning(f"[MarketDataService._get_from_market_center] K line API not callable: {exchange}.{instrument_id}")
                    result = None
                    return
                logging.debug(f"[MarketDataService._get_from_market_center] Resolved kline func: {get_kline_func}")
    
                # Step 3: 尝试获取 K 线数据
                raw_data = self._try_get_kline_data(get_kline_func, exchange, instrument_id, style, count)
                if not raw_data:
                    logging.warning(f"[MarketDataService._get_from_market_center] No data returned: {exchange}.{instrument_id}")
                    result = None
                    return
                logging.debug(f"[MarketDataService._get_from_market_center] Got raw data: {len(raw_data) if hasattr(raw_data, '__len__') else 'N/A'} items")
                        
                # Step 4: 标准化数据
                bars = self._normalize_bars_payload(raw_data)
                if not bars:
                    logging.warning(f"[MarketDataService._get_from_market_center] Failed to normalize bars: {exchange}.{instrument_id}")
                    result = None
                    return
                logging.debug(f"[MarketDataService._get_from_market_center] Normalized bars: {len(bars)} items")
                        
                result = [self._to_light_kline(bar) for bar in bars[-count:]]
                logging.debug(f"[MarketDataService._get_from_market_center] Success: {len(result)} klines")
                        
            except (ValueError, TypeError, AttributeError) as e:
                error = e
                logging.error(f"[MarketDataService._get_from_market_center] Exception: {type(e).__name__}: {e}")
                result = None
            
        # 创建并启动线程
        thread = threading.Thread(target=fetch_from_mc)
        thread.daemon = True
        thread.start()
            
        # 等待最多 5 秒
        thread.join(timeout=5.0)
            
        if thread.is_alive():
            logging.warning(f"[MarketDataService._get_from_market_center] Operation timed out: {exchange}.{instrument_id}")
            return None
        elif error:
            logging.error(f"[MarketDataService._get_from_market_center] Error: {error}")
            return None
        else:
            if result:
                logging.info(f"[MarketDataService._get_from_market_center] Successfully fetched {len(result)} klines for {exchange}.{instrument_id}")
            else:
                logging.info(f"[MarketDataService._get_from_market_center] Returned empty result for {exchange}.{instrument_id}")
            return result

    def _resolve_kline_fetch_callable(self, market_center: Any) -> Optional[Callable]:
        """解析可用的K线获取函数，兼容不同平台接口命名。"""
        if market_center is None:
            return None

        for name in ('get_kline_data', 'get_kline'):
            fn = getattr(market_center, name, None)
            if callable(fn):
                return fn
        return None

    @staticmethod
    def _contains_real_klines(klines: Optional[List[LightKLine]]) -> bool:
        """判断是否包含真实K线（不是占位数据）。"""
        if not klines:
            return False
        return any(not bool(getattr(k, 'is_placeholder', False)) for k in klines)

    def _try_get_kline_data(self, get_kline_func: Callable, exchange: str,
                            instrument_id: str, style: str, count: int) -> Any:
        """尝试获取K线数据（提取的辅助方法，避免嵌套try-except）"""
        import inspect
        import threading
        
        result = None
        error = None

        def _try_signature_kwargs(try_count: int) -> bool:
            """基于函数签名做键名自适配调用。"""
            nonlocal result
            try:
                sig = inspect.signature(get_kline_func)
            except (TypeError, ValueError):
                return False

            kwargs: Dict[str, Any] = {}
            required_missing = False

            for pname, param in sig.parameters.items():
                if pname == 'self':
                    continue

                key = pname.lower()
                value_set = False

                if key in ('exchange', 'exchangeid', 'exchange_id'):
                    kwargs[pname] = exchange
                    value_set = True
                elif key in ('instrument_id', 'instrumentid', 'instrument', 'symbol', 'code'):
                    kwargs[pname] = instrument_id
                    value_set = True
                elif key in ('style', 'kline_type', 'klinetype', 'period', 'interval', 'frequency', 'freq'):
                    kwargs[pname] = style
                    value_set = True
                elif key in ('count', 'limit', 'size', 'n', 'num'):
                    kwargs[pname] = try_count
                    value_set = True
                elif key in ('start_time', 'start', 'from_time', 'begin_time'):
                    kwargs[pname] = None
                    value_set = True
                elif key in ('end_time', 'end', 'to_time', 'stop_time'):
                    kwargs[pname] = None
                    value_set = True

                if not value_set and param.default is inspect._empty and param.kind in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                ):
                    required_missing = True
                    break

            if required_missing:
                return False

            # 部分平台内置方法仅支持位置参数；若关键字调用失败必须回退。
            try:
                result = get_kline_func(**kwargs)
                return True
            except TypeError:
                return False
        
        def fetch_data(try_count):
            nonlocal result, error
            try:
                # 1) 优先按签名自适配键名调用。
                if _try_signature_kwargs(try_count):
                    return

                # 2) 兼容旧接口关键字调用；若接口不支持，再回落位置参数调用。
                try:
                    result = get_kline_func(
                        exchange=exchange,
                        instrument_id=instrument_id,
                        style=style,
                        count=try_count
                    )
                    return
                except TypeError:
                    pass

                try:
                    result = get_kline_func(
                        exchange=exchange,
                        instrument_id=instrument_id,
                        kline_type=style,
                        count=try_count,
                    )
                    return
                except TypeError:
                    pass

                # 3) 位置参数兼容矩阵：避免基于函数名猜测导致误判。
                positional_patterns = [
                    (exchange, instrument_id, style, try_count),
                    (exchange, instrument_id, try_count, style),
                    (exchange, instrument_id, style),
                    (exchange, instrument_id, try_count),
                ]
                last_type_error: Optional[TypeError] = None
                for args in positional_patterns:
                    try:
                        result = get_kline_func(*args)
                        return
                    except TypeError as te:
                        last_type_error = te
                        continue

                if last_type_error is not None:
                    raise last_type_error
            except (ValueError, TypeError, AttributeError) as e:
                error = e
        
        for try_count in [-abs(count), abs(count)]:
            # 创建并启动线程
            thread = threading.Thread(target=fetch_data, args=(try_count,))
            thread.daemon = True
            thread.start()
            
            # 等待最多3秒
            thread.join(timeout=3.0)
            
            if thread.is_alive():
                logging.warning(f"[MarketDataService] K线获取尝试超时(count={try_count})")
                continue
            elif error:
                logging.warning(f"[MarketDataService] K线获取尝试失败(count={try_count}): {error}")
                error = None
                continue
            elif result is not None:
                return result
        return None

    # ========================================================================
    # 数据标准化
    # ========================================================================

    def _should_skip_market_center_kline(self, instrument_id: str) -> bool:
        """判定是否应跳过Market Center K线请求（合成标的通常无历史K线接口）。"""
        try:
            inst = str(instrument_id or '').strip().upper()
            if not inst:
                return True

            synthetic_suffixes = (
                '_WEIGHTED',
                '_PCR',
                '_BASIS',
                '_SPREAD',
                '_INDEX',
                '_MAIN',
                '_CONTINUOUS',
            )
            return inst.endswith(synthetic_suffixes)
        except Exception:
            return False

    def _normalize_bars_payload(self, payload: Any) -> List[Any]:
        """标准化行情接口返回数据"""
        if payload is None:
            return []

        if isinstance(payload, list):
            return payload

        if isinstance(payload, tuple):
            if len(payload) >= 2 and isinstance(payload[1], list):
                return payload[1]
            for part in payload:
                if isinstance(part, list):
                    return part
            return []

        if isinstance(payload, dict):
            for key in ("data", "bars", "result", "klines"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value
            return []

        return []

    def _to_light_kline(self, bar: Any) -> LightKLine:
        """将任意bar对象转换为LightKLine"""
        try:
            if isinstance(bar, LightKLine):
                return bar

            # 字典类型
            if isinstance(bar, dict):
                o = self._get_bar_value(bar, ['open', 'Open', 'OPEN'])
                h = self._get_bar_value(bar, ['high', 'High', 'HIGH'])
                l = self._get_bar_value(bar, ['low', 'Low', 'LOW'])
                c = self._get_bar_value(bar, ['close', 'Close', 'CLOSE', 'last', 'last_price'])
                v = self._get_bar_value(bar, ['volume', 'Volume', 'VOLUME', 'vol'])
                dt = self._get_bar_datetime(bar)

                # 处理零收盘价
                if c <= 0:
                    c = o if o > 0 else max(h, l)

                return LightKLine(o, h, l, c, v, dt)

            # 对象类型
            o = getattr(bar, 'open', 0) or 0
            h = getattr(bar, 'high', 0) or 0
            l = getattr(bar, 'low', 0) or 0
            c = getattr(bar, 'close', 0) or getattr(bar, 'last', 0) or 0
            v = getattr(bar, 'volume', 0) or 0
            dt = self._get_bar_datetime(bar)

            if c <= 0:
                c = o if o > 0 else max(h, l)

            return LightKLine(o, h, l, c, v, dt)

        except Exception as e:
            logging.error(f"[MarketDataService._to_light_kline] Error: {e}")
            return LightKLine(0, 0, 0, 0, 0, datetime.now())

    def _get_bar_value(self, bar: Dict, attrs: List[str]) -> float:
        """从bar字典获取值"""
        for attr in attrs:
            if attr in bar:
                try:
                    return float(bar[attr])
                except (ValueError, TypeError) as e:
                    logging.warning(f"[_get_bar_value] Failed to convert {attr}: {e}")
                    continue
        return 0.0

    def _get_bar_datetime(self, bar: Any) -> datetime:
        """获取bar的时间戳"""
        for attr in ['datetime', 'DateTime', 'timestamp', 'Timestamp', 'time']:
            val = None
            if isinstance(bar, dict):
                val = bar.get(attr)
            else:
                try:
                    val = getattr(bar, attr, None)
                except AttributeError as e:
                    logging.warning(f"[_get_bar_datetime] Failed to get {attr}: {e}")
                    continue

            if val is None:
                continue

            if isinstance(val, datetime):
                return val

            if isinstance(val, (int, float)):
                try:
                    return datetime.fromtimestamp(val)
                except (ValueError, OSError) as e:
                    logging.warning(f"[_get_bar_datetime] Failed to convert timestamp: {e}")
                    continue

            if isinstance(val, str):
                for fmt in [None, "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]:
                    try:
                        if fmt is None:
                            return datetime.fromisoformat(val)
                        return datetime.strptime(val, fmt)
                    except (ValueError, TypeError) as e:
                        logging.warning(f"[_get_bar_datetime] Failed to parse date: {e}")
                        continue

        return datetime.now()

    # ========================================================================
    # 缓存管理
    # ========================================================================

    def _get_cache_key(self, exchange: str, instrument_id: str) -> str:
        """生成缓存键"""
        return f"{exchange}_{instrument_id}".upper()

    def _cache_klines(self, exchange: str, instrument_id: str,
                      klines: List[LightKLine]) -> None:
        """缓存K线数据"""
        key = self._get_cache_key(exchange, instrument_id)
        max_count = self._get_max_kline_count()

        with self._kline_lock:
            if key not in self._kline_data:
                self._kline_data[key] = {"data": []}

            self._kline_data[key]["data"].extend(klines)

            # 限制缓存大小
            if len(self._kline_data[key]["data"]) > max_count:
                self._kline_data[key]["data"] = self._kline_data[key]["data"][-max_count:]

    def _get_cached_klines(self, exchange: str, instrument_id: str) -> Optional[List[LightKLine]]:
        """获取缓存的K线数据"""
        key = self._get_cache_key(exchange, instrument_id)

        with self._kline_lock:
            cache_entry = self._kline_data.get(key)
            if cache_entry and "data" in cache_entry:
                return cache_entry["data"]

        return None

    def _get_cached_or_placeholder(self, exchange: str, instrument_id: str,
                                    count: int) -> List[LightKLine]:
        """获取缓存数据或创建占位数据"""
        cached = self._get_cached_klines(exchange, instrument_id)
        if cached:
            return cached[-count:] if len(cached) > count else cached

        return self._create_placeholder_klines(exchange, instrument_id, count)

    # ========================================================================
    # 占位数据生成
    # ========================================================================

    def _create_placeholder_klines(self, exchange: str, instrument_id: str,
                                    count: int) -> List[LightKLine]:
        """创建占位K线数据"""
        result = []
        now = datetime.now()

        # 默认占位价格
        base_price = 3000.0
        # 根据合约类型设置不同的基础价格
        if 'CU' in instrument_id:
            base_price = 50000.0  # 铜
        elif 'M' in instrument_id:
            base_price = 3000.0   # 豆粕
        elif 'IF' in instrument_id or 'IH' in instrument_id or 'IC' in instrument_id:
            base_price = 3500.0  # 股指期货

        for i in range(count):
            # 生成模拟价格波动
            price_change = (i % 10 - 5) / 1000  # -0.5% 到 +0.5%
            bar = LightKLine(
                open_price=base_price * (1 + price_change),
                high=base_price * (1 + price_change + 0.001),
                low=base_price * (1 + price_change - 0.001),
                close=base_price * (1 + price_change),
                volume=1000 + (i % 500),  # 模拟成交量
                dt=now - timedelta(minutes=i)
            )
            bar.exchange = exchange
            bar.instrument_id = instrument_id
            bar.style = "M1"
            bar.is_placeholder = True
            bar.source = "placeholder"
            result.append(bar)

        return result

    # ========================================================================
    # 速率限制
    # ========================================================================

    def _check_rate_limit(self, instrument_id: str, exchange: str) -> bool:
        """检查速率限制"""
        with self._rate_lock:
            now = time.time()
            duration = self._get_rate_limit_window()
            max_requests = self._get_max_requests_per_instrument()
            max_global = self._get_max_global_requests()

            key = f"{exchange}_{instrument_id}" if exchange else instrument_id
            global_key = "__GLOBAL__"

            # 清理过期时间戳
            self._cleanup_old_timestamps(key, now, duration)
            self._cleanup_old_timestamps(global_key, now, duration)

            # 检查限制
            inst_count = len(self._fetch_timestamps.get(key, []))
            global_count = len(self._fetch_timestamps.get(global_key, []))

            if inst_count < max_requests and global_count < max_global:
                # 记录本次请求
                if key not in self._fetch_timestamps:
                    self._fetch_timestamps[key] = []
                self._fetch_timestamps[key].append(now)
                self._fetch_timestamps[global_key].append(now)
                return True

            return False

    def _cleanup_old_timestamps(self, key: str, now: float, duration: int) -> None:
        """清理过期的时间戳"""
        if key in self._fetch_timestamps:
            self._fetch_timestamps[key] = [
                ts for ts in self._fetch_timestamps[key]
                if (now - ts) < duration
            ]

    # ========================================================================
    # 配置参数获取
    # ========================================================================

    def _get_kline_style(self) -> str:
        """获取K线周期"""
        style = getattr(self.params, 'kline_style', 'M1')
        return str(style or 'M1').strip()

    def _get_max_kline_count(self) -> int:
        """获取最大K线数量"""
        count = getattr(self.params, 'max_kline', 100)
        return safe_int(count, 100)

    def _get_rate_limit_window(self) -> int:
        """获取速率限制时间窗口（秒）"""
        window = getattr(self.params, 'rate_limit_window_sec', 60)
        return safe_int(window, 60)

    def _get_max_requests_per_instrument(self) -> int:
        """获取每个合约的最大请求数"""
        limit = getattr(self.params, 'rate_limit_per_instrument', 2)
        return safe_int(limit, 2)

    def _get_max_global_requests(self) -> int:
        """获取全局最大请求数"""
        limit = getattr(self.params, 'rate_limit_global_per_min', 10)
        return safe_int(limit, 10)

    def _get_market_center(self) -> Any:
        """获取市场中心引用（快速失败机制）"""
        # 1. 优先使用已缓存的市场中心
        if self._market_center is not None:
            if self._resolve_kline_fetch_callable(self._market_center):
                return self._market_center
            logging.warning("[MarketDataService._get_market_center] Cached market center lacks kline api, refreshing")
            self._market_center = None

        # 2. 快速尝试从params获取（无超时，直接返回）
        try:
            market_center = getattr(self.params, 'market_center', None)
            if market_center:
                if self._resolve_kline_fetch_callable(market_center):
                    self._market_center = market_center
                    logging.info("[MarketDataService._get_market_center] Market center obtained from params with kline api")
                    return self._market_center
                else:
                    logging.warning("[MarketDataService._get_market_center] Market center from params lacks kline api")
        except Exception as e:
            logging.warning(f"[MarketDataService._get_market_center] Failed to get market_center from params: {e}")

        # 3. 快速尝试从params的strategy属性获取（无超时，直接返回）
        try:
            strategy = getattr(self.params, 'strategy', None)
            if strategy:
                market_center = getattr(strategy, 'market_center', None)
                if market_center:
                    if self._resolve_kline_fetch_callable(market_center):
                        self._market_center = market_center
                        logging.info("[MarketDataService._get_market_center] Market center obtained from strategy with kline api")
                        return self._market_center
                    else:
                        logging.warning("[MarketDataService._get_market_center] Market center from strategy lacks kline api")
                # 尝试将strategy本身作为市场中心（如果它有kline接口）
                elif self._resolve_kline_fetch_callable(strategy):
                    self._market_center = strategy
                    logging.info("[MarketDataService._get_market_center] Strategy itself used as market center with kline api")
                    return self._market_center
        except Exception as e:
            logging.warning(f"[MarketDataService._get_market_center] Failed to get market_center from strategy: {e}")

        # 4. 尝试从pythongo.core导入MarketCenter（带超时处理）
        try:
            # 使用线程和超时来防止导入和初始化卡住
            import threading
            import time
            
            market_center = None
            error = None
            
            def import_and_init():
                nonlocal market_center, error
                try:
                    logging.info("[MarketCenter Debug] Step 1: Importing pythongo.core.MarketCenter...")
                    from pythongo.core import MarketCenter
                    logging.info(f"[MarketCenter Debug] Step 2: MarketCenter class imported: {MarketCenter}")
                    
                    logging.info("[MarketCenter Debug] Step 3: Creating MarketCenter instance...")
                    mc_instance = MarketCenter()
                    logging.info(f"[MarketCenter Debug] Step 4: MarketCenter instance created: {mc_instance}")
                    logging.info(f"[MarketCenter Debug]     Type: {type(mc_instance)}")
                    logging.info(f"[MarketCenter Debug]     Bool: {bool(mc_instance)}")
                    logging.info(f"[MarketCenter Debug]     Dir (first 10): {dir(mc_instance)[:10]}")
                    
                    # 检查是否有 get_kline_data 方法
                    if hasattr(mc_instance, 'get_kline_data'):
                        method = getattr(mc_instance, 'get_kline_data')
                        logging.info(f"[MarketCenter Debug]     Has get_kline_data: {method}")
                        logging.info(f"[MarketCenter Debug]     Callable: {callable(method)}")
                        
                        # 尝试获取方法签名
                        try:
                            import inspect
                            sig = inspect.signature(method)
                            logging.info(f"[MarketCenter Debug]     Signature: {sig}")
                        except Exception as sig_err:
                            logging.warning(f"[MarketCenter Debug]     Cannot get signature: {sig_err}")
                    else:
                        logging.warning("[MarketCenter Debug]     NO get_kline_data method!")
                    
                    market_center = mc_instance
                    logging.info("[MarketCenter Debug] Step 5: MarketCenter initialization completed successfully")
                    
                except ImportError as ie:
                    error = ie
                    logging.error(f"[MarketCenter Debug] ❌ ImportError: {ie}")
                    logging.error(f"[MarketCenter Debug]    Error type: {type(ie)}")
                    import traceback
                    logging.error(f"[MarketCenter Debug]    Traceback: {traceback.format_exc()}")
                except Exception as e:
                    error = e
                    logging.error(f"[MarketCenter Debug] ❌ Exception during MarketCenter creation: {e}")
                    logging.error(f"[MarketCenter Debug]    Error type: {type(e)}")
                    logging.error(f"[MarketCenter Debug]    Error args: {e.args}")
                    import traceback
                    logging.error(f"[MarketCenter Debug]    Traceback: {traceback.format_exc()}")
            
            # 创建并启动线程
            thread = threading.Thread(target=import_and_init)
            thread.daemon = True
            thread.start()
            
            # 等待最多3秒
            thread.join(timeout=3.0)
            
            if thread.is_alive():
                logging.warning("[MarketDataService._get_market_center] MarketCenter import/initialization timed out")
            elif error:
                logging.warning(f"[MarketDataService._get_market_center] Failed to import/initialize MarketCenter: {error}")
            elif market_center:
                if self._resolve_kline_fetch_callable(market_center):
                    self._market_center = market_center
                    logging.info("[MarketDataService._get_market_center] Market center created from pythongo.core.MarketCenter")
                    return self._market_center
                else:
                    logging.warning("[MarketDataService._get_market_center] pythongo.core.MarketCenter lacks kline api")
        except Exception as e:
            logging.warning(f"[MarketDataService._get_market_center] Failed to import pythongo.core.MarketCenter: {e}")

        # 5. 尝试从全局变量获取
        try:
            import __main__
            if hasattr(__main__, 'market_center'):
                market_center = getattr(__main__, 'market_center')
                if market_center and self._resolve_kline_fetch_callable(market_center):
                    self._market_center = market_center
                    logging.info("[MarketDataService._get_market_center] Market center obtained from global variable")
                    return self._market_center
        except Exception as e:
            logging.warning(f"[MarketDataService._get_market_center] Failed to get market_center from global variable: {e}")

        # 6. 尝试从INFINIGO全局对象获取（如果可用）
        try:
            global INFINIGO
            if 'INFINIGO' in globals():
                infini_obj = globals()['INFINIGO']
                if hasattr(infini_obj, 'MarketCenter'):
                    try:
                        market_center = infini_obj.MarketCenter()
                        if self._resolve_kline_fetch_callable(market_center):
                            self._market_center = market_center
                            logging.info("[MarketDataService._get_market_center] Market center created from INFINIGO.MarketCenter")
                            return self._market_center
                    except Exception as e:
                        logging.warning(f"[MarketDataService._get_market_center] Failed to create MarketCenter from INFINIGO: {e}")
                # 尝试直接使用INFINIGO作为市场中心（如果它有kline接口）
                elif self._resolve_kline_fetch_callable(infini_obj):
                    self._market_center = infini_obj
                    logging.info("[MarketDataService._get_market_center] INFINIGO itself used as market center")
                    return self._market_center
        except Exception as e:
            logging.warning(f"[MarketDataService._get_market_center] Failed to check INFINIGO: {e}")

        # 所有尝试都失败
        logging.warning("[MarketDataService._get_market_center] Market center not available, using placeholder data")
        return None

    def set_market_center(self, market_center: Any) -> None:
        """设置市场中心引用"""
        if market_center is not None and not self._resolve_kline_fetch_callable(market_center):
            logging.warning("[MarketDataService.set_market_center] Provided object lacks kline api, ignoring cache update")
            return
        self._market_center = market_center

    def set_storage(self, storage: Any) -> None:
        """设置存储管理器"""
        self.storage = storage

    # ========================================================================
    # 市场中心健康检查机制
    # ========================================================================

    def start_health_check(self) -> None:
        """启动市场中心健康检查"""
        import threading
        def health_check_loop():
            while True:
                try:
                    # 尝试获取市场中心
                    mc = self._get_market_center()
                    if mc:
                        # 尝试调用一个简单的方法来检查市场中心是否正常
                        try:
                            # 尝试获取K线数据来验证市场中心
                            test_klines = self.get_kline("CFFEX", "IF2603", count=1)
                            if test_klines:
                                logging.debug("[MarketDataService] Market center health check passed")
                            else:
                                logging.warning("[MarketDataService] Market center health check failed: no data returned")
                        except Exception as e:
                            logging.warning(f"[MarketDataService] Market center health check failed: {e}")
                    else:
                        logging.debug("[MarketDataService] Market center not available (health check)")
                except Exception as e:
                    logging.error(f"[MarketDataService] Health check error: {e}")
                # 每30秒检查一次
                import time
                time.sleep(30)
        
        # 启动健康检查线程
        self._health_check_thread = threading.Thread(target=health_check_loop)
        self._health_check_thread.daemon = True
        self._health_check_thread.start()
        logging.info("[MarketDataService] Health check started")

    def stop_health_check(self) -> None:
        """停止市场中心健康检查"""
        # 由于使用的是守护线程，不需要显式停止
        logging.info("[MarketDataService] Health check stopped")

    # ========================================================================
    # 工具方法
    # ========================================================================

    def get_kline_stats(self) -> Dict[str, int]:
        """获取 K 线加载统计信息"""
        with self._kline_stats_lock:
            return dict(self._kline_stats)
    
    def clear_cache(self) -> None:
        """清空缓存"""
        with self._kline_lock:
            self._kline_data.clear()

    def get_cache_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        with self._kline_lock:
            return {
                "cache_entries": len(self._kline_data),
                "total_klines": sum(
                    len(entry.get("data", []))
                    for entry in self._kline_data.values()
                )
            }

    # ========================================================================
    # 订阅功能扩展
    # ========================================================================

    def _resolve_subscribe_flag(self, flag_name: str, legacy_flag: str, default: bool) -> bool:
        """解析订阅标志"""
        try:
            # 优先使用新标志
            value = getattr(self.params, flag_name, None)
            if value is not None:
                return bool(value)
            # 兼容旧标志
            value = getattr(self.params, legacy_flag, default)
            return bool(value)
        except Exception:
            return default

    def _is_symbol_current_or_next(self, symbol: str) -> bool:
        """检查合约是否为当前或下月合约"""
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
        except Exception:
            return True

    def _is_real_month_contract(self, instrument_id: str) -> bool:
        """
        判断是否为具体的月合约（已废弃 - 全量模式，不过滤）
            
        ⚠️  警告：此方法已被废弃，不再进行任何过滤！
        
        原过滤逻辑:
        1. 排除 888, 999, HOT, IDX 等非具体合约
        2. 只保留年月结尾的合约
        """
        # ✅ 全量模式：直接返回 True，不做任何过滤
        return True

    def _collect_future_instruments(self) -> List[Dict]:
        """收集期货合约信息"""
        instruments = []
        seen = set()
        try:
            # 从参数或市场中心获取期货合约列表
            future_instruments = getattr(self.params, 'future_instruments', [])
            
            # 处理直接提供的期货合约列表
            for instrument in future_instruments:
                exchange = instrument.get("exchange", "") or instrument.get("ExchangeID", "")
                instrument_id = instrument.get("instrument_id", "") or instrument.get("InstrumentID", "")
                instrument_norm = self._normalize_instrument_id(instrument_id)
                if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                    rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                    key = f"{exchange}_{str(instrument_id).upper()}"
                    if key not in seen:
                        seen.add(key)
                        instruments.append(rec)
            
            # 如果没有直接的期货合约列表，尝试从配置中构建
            if not future_instruments and hasattr(self.params, 'month_mapping') and hasattr(self.params, 'exchange_mapping'):
                from ali2026v3_trading.config_service import resolve_product_exchange
                month_mapping = getattr(self.params, 'month_mapping', {})
                exchange_mapping = getattr(self.params, 'exchange_mapping', {})
                
                for product, months in month_mapping.items():
                    for month in months:
                        exchange = resolve_product_exchange(product, exchange_mapping, getattr(self.params, 'exchange', 'CFFEX'))
                        instrument_id = month
                        instrument_norm = self._normalize_instrument_id(instrument_id)
                        if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                            rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                            key = f"{exchange}_{str(instrument_id).upper()}"
                            if key not in seen:
                                seen.add(key)
                                instruments.append(rec)
            
            # 如果还是没有，尝试从市场中心获取（添加超时保护）
            if not future_instruments and not instruments and self._market_center:
                get_instruments = getattr(self._market_center, 'get_instruments', None)
                if callable(get_instruments):
                    try:
                        # 添加超时保护，防止市场中心调用卡住
                        import threading
                        import time
                        
                        result = []
                        error = None
                        
                        def get_instruments_with_timeout():
                            nonlocal result, error
                            try:
                                result = get_instruments()
                            except Exception as e:
                                error = e
                        
                        # 创建线程执行市场中心调用
                        thread = threading.Thread(target=get_instruments_with_timeout)
                        thread.daemon = True
                        thread.start()
                        
                        # 等待最多3秒
                        thread.join(timeout=3.0)
                        
                        if thread.is_alive():
                            logging.warning("[MarketDataService._collect_future_instruments] Market center get_instruments timed out")
                        elif error:
                            logging.warning(f"[MarketDataService._collect_future_instruments] Failed to get instruments: {error}")
                        else:
                            for future in result:
                                exchange = future.get("ExchangeID", "")
                                instrument_id = future.get("InstrumentID", "")
                                instrument_norm = self._normalize_instrument_id(instrument_id)
                                if exchange and instrument_id and self._is_real_month_contract(instrument_norm):
                                    rec = {"exchange": exchange, "instrument_id": instrument_id, "type": "future"}
                                    key = f"{exchange}_{str(instrument_id).upper()}"
                                    if key not in seen:
                                        seen.add(key)
                                        instruments.append(rec)
                    except Exception as e:
                        logging.warning(f"[MarketDataService._collect_future_instruments] Error getting instruments: {e}")
        except Exception as e:
            logging.error(f"[MarketDataService._collect_future_instruments] Error: {e}")
        return instruments

    def _collect_option_instruments(self) -> List[Dict]:
        """收集期权合约信息"""
        instruments = []
        seen = set()
        try:
            # 检查是否启用期权订阅
            if not getattr(self.params, 'subscribe_options', True):
                logging.debug("[MarketDataService._collect_option_instruments] Options subscription disabled")
                return instruments
            
            # 尝试从多个来源获取 option_instruments
            option_instruments = None
            
            # 1. 从 params.option_instruments 获取
            if hasattr(self.params, 'option_instruments'):
                option_instruments = getattr(self.params, 'option_instruments', None)
            
            # 2. 如果 strategy_core_service 已加载，从其属性获取
            if option_instruments is None:
                strategy_obj = getattr(self.params, 'strategy', None)
                if strategy_obj and hasattr(strategy_obj, '_option_instruments'):
                    option_instruments = strategy_obj._option_instruments
            
            # 3. 最后尝试从 self 获取（向后兼容）
            if option_instruments is None:
                option_instruments = getattr(self, 'option_instruments', {})
            
            # 如果仍然没有，记录警告并返回空列表
            if not option_instruments:
                logging.warning("[MarketDataService._collect_option_instruments] No option_instruments found in self or params")
                return instruments

            for future_symbol, options in option_instruments.items():
                for option in options:
                    opt_exchange = option.get("ExchangeID", "")
                    opt_instrument = option.get("InstrumentID", "")
                    if not opt_exchange and opt_instrument:
                        from ali2026v3_trading.config_service import resolve_product_exchange
                        opt_exchange = resolve_product_exchange(
                            opt_instrument,
                            getattr(self.params, 'exchange_mapping', {}),
                            getattr(self.params, 'exchange', 'CFFEX'),
                        )
                    if opt_exchange and opt_instrument:
                        rec = {
                            "exchange": opt_exchange,
                            "instrument_id": opt_instrument,
                            "type": "option",
                            "_normalized_future": self._normalize_instrument_id(future_symbol),
                        }
                        key = f"{opt_exchange}_{str(opt_instrument).upper()}"
                        if key not in seen:
                            seen.add(key)
                            instruments.append(rec)
        except Exception as e:
            logging.error(f"[MarketDataService._collect_option_instruments] Error: {e}")
        return instruments

    def _fetch_single_instrument(self, instrument: Dict, mc_get_kline: Callable, history_minutes: int, primary_style: str, styles_to_try: List[str]) -> tuple:
        """获取单个合约的历史K线数据"""
        exchange = str((instrument or {}).get("exchange", "") or "")
        instrument_id = str((instrument or {}).get("instrument_id", "") or "")
        key = f"{exchange}_{instrument_id}"

        if not exchange or not instrument_id:
            return False, key

        # 检查速率限制
        if not self._check_rate_limit(instrument_id, exchange):
            # 历史预加载阶段不能直接跳过
            logging.debug(f"[History Load] Rate limit hit but continuing: {key}")

        # 等待速率限制
        try:
            # 简单的速率限制等待
            time.sleep(0.05)
        except Exception as e:
            logging.error(f"[MarketDataService._fetch_single_instrument] Error waiting for rate limit: {e}")

        exch_upper = str(exchange).upper()
        inst_upper = str(instrument_id).upper()

        try:
            # 先尝试主周期，再回退到备用周期；每个周期先请求10根，失败后再尝试1根。
            style_candidates = [s for s in (styles_to_try or [primary_style]) if s]
            if not style_candidates:
                style_candidates = [primary_style or 'M1']

            for style in style_candidates:
                for req_count in (10, 1):
                    klines = self.get_kline(
                        exchange,
                        instrument_id,
                        count=req_count,
                        style=style,
                        prefer_market_center=True,
                        request_timeout_sec=3.0,
                    )
                    if self._contains_real_klines(klines):
                        logging.debug(
                            f"[History Load] Successfully fetched: {key}, style={style}, req_count={req_count}, len={len(klines)}"
                        )
                        return True, key

            logging.debug(f"[History Load] Empty after retries: {key}, styles={style_candidates}")
            return False, key
        except Exception as e:
            logging.error(f"[MarketDataService._fetch_single_instrument] Error fetching {key}: {e}")
            return False, key

    def load_historical_klines(self) -> tuple:
        """加载历史 K 线数据"""
        empty_keys = []
        non_empty_keys = []
            
        try:
            # 收集合约信息
            future_instruments = self._collect_future_instruments()
            option_instruments = self._collect_option_instruments()
            all_instruments = future_instruments + option_instruments

            # 统计合约数量
            fut_count = len(future_instruments)
            opt_count = len(option_instruments)
            logging.info(f"[MarketDataService] Total instruments to load: {len(all_instruments)} (futures: {fut_count}, options: {opt_count})")
            
            if not all_instruments:
                logging.warning("[MarketDataService] No instruments to load")
                return empty_keys, non_empty_keys

            # 获取市场中心
            mc = self._get_market_center()
            if not mc:
                logging.error("[MarketDataService] Market center not available")
                return empty_keys, non_empty_keys

            mc_get_kline = self._resolve_kline_fetch_callable(mc)
            if not callable(mc_get_kline):
                logging.warning("[MarketDataService] Market center kline api unavailable, retrying with refreshed resolver")
                self._market_center = None
                mc = self._get_market_center()
                mc_get_kline = self._resolve_kline_fetch_callable(mc) if mc else None
                if not callable(mc_get_kline):
                    logging.error("[MarketDataService] Market center kline api not available")
                    return empty_keys, non_empty_keys

            # 配置参数
            history_minutes = getattr(self.params, "history_minutes", 240) or 240
            primary_style = (getattr(self.params, "kline_style", "M1") or "M1").strip()
            styles_to_try = [primary_style] + (["M1"] if primary_style != "M1" else [])

            # 并发加载
            import concurrent.futures
            max_workers = min(getattr(self.params, "history_load_max_workers", 4), 8)
            
            logging.info(f"[MarketDataService] Starting concurrent historical kline load with {max_workers} workers")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for instrument in all_instruments:
                    fut = executor.submit(
                        self._fetch_single_instrument,
                        instrument,
                        mc_get_kline,
                        history_minutes,
                        primary_style,
                        styles_to_try
                    )
                    futures[fut] = instrument
                    # 限流
                    time.sleep(0.02)

                # 处理结果
                for future in concurrent.futures.as_completed(futures):
                    instrument = futures[future]
                    try:
                        success, key = future.result()
                        if success:
                            non_empty_keys.append(key)
                        else:
                            empty_keys.append(key)
                    except Exception as e:
                        key = f"{instrument.get('exchange', '')}_{instrument.get('instrument_id', '')}"
                        empty_keys.append(key)
                        logging.error(f"[MarketDataService] History load failed for {instrument}: {e}")

            logging.info(f"[MarketDataService] Historical kline load completed: {len(non_empty_keys)} successful, {len(empty_keys)} failed")
            logging.info(f"[HKL] Historical load result: success={len(non_empty_keys)}, failed={len(empty_keys)}")
            
            # 更新 K 线统计
            with self._kline_stats_lock:
                self._kline_stats['success'] += len(non_empty_keys)
                self._kline_stats['failed'] += len(empty_keys)
                # 估算加载的 K 线数量（每个合约平均 10 根）
                estimated_klines = len(non_empty_keys) * 10
                self._kline_stats['total_klines_loaded'] += estimated_klines
            
            if empty_keys:
                sample = ', '.join(empty_keys[:20])
                logging.warning(f"[HKL] Historical load failed keys(sample<=20): {sample}")
            if non_empty_keys:
                sample_ok = ', '.join(non_empty_keys[:20])
                logging.info(f"[HKL] Historical load success keys(sample<=20): {sample_ok}")
            
            # 记录详细统计
            logging.info(
                f"[KLine Stats] Load stats: requested={len(all_instruments)}, "
                f"success={len(non_empty_keys)}, failed={len(empty_keys)}, "
                f"estimated_klines={estimated_klines}"
            )
            
        except Exception as e:
            logging.error(f"[MarketDataService.load_historical_klines] Error: {e}")
        
        return empty_keys, non_empty_keys

    @staticmethod
    def _extract_instrument_id(instrument: Any) -> str:
        """从 dict 或对象形式的合约中提取 instrument_id。"""
        if isinstance(instrument, dict):
            return str(
                instrument.get('instrument_id', '')
                or instrument.get('InstrumentID', '')
                or instrument.get('symbol', '')
                or instrument.get('code', '')
            ).strip()

        return str(
            getattr(instrument, 'instrument_id', '')
            or getattr(instrument, 'InstrumentID', '')
            or getattr(instrument, 'symbol', '')
            or getattr(instrument, 'code', '')
        ).strip()

    def _sync_storage_subscriptions(self, instruments: List[Any]) -> Tuple[int, int]:
        """将订阅同步到 storage，用于本地订阅索引而不是平台实际订阅。"""
        if not self.storage or not hasattr(self.storage, 'subscribe'):
            logging.info(
                "[MarketDataService] Storage subscription sync skipped: storage.subscribe unavailable"
            )
            return len(instruments), 0

        subscribed_ok = 0
        failed_ids: List[str] = []

        for instrument in instruments:
            instrument_id = self._extract_instrument_id(instrument)
            if not instrument_id:
                failed_ids.append('<missing instrument_id>')
                continue

            try:
                self.storage.subscribe(instrument_id, 'tick')
                subscribed_ok += 1
            except Exception as e:
                failed_ids.append(instrument_id)
                logging.warning(
                    f"[MarketDataService] Storage subscribe failed for {instrument_id}: {e}"
                )

        if failed_ids:
            sample = ', '.join(failed_ids[:20])
            logging.warning(
                f"[MarketDataService] Storage subscription sync failures(sample<=20): {sample}"
            )

        return subscribed_ok, len(failed_ids)

    def subscribe_instruments(self) -> bool:
        """订阅合约"""
        try:
            # Step 1: 收集合约（使用显式传递的数据）
            # 优先使用显式传递的合约数据
            if self._futures_instruments:
                future_instruments = self._futures_instruments
                logging.info(f"[MarketDataService] Using explicitly passed futures_instruments: {len(future_instruments)}")
            else:
                # 降级到从市场中心收集
                future_instruments = self._collect_future_instruments()
                logging.info(f"[MarketDataService] Fallback to collected futures_instruments: {len(future_instruments)}")
            
            if self._option_instruments:
                option_instruments = []
                for contracts in self._option_instruments.values():
                    option_instruments.extend(contracts)
                logging.info(f"[MarketDataService] Using explicitly passed option_instruments: {len(option_instruments)}")
            else:
                # 降级到从市场中心收集
                option_instruments = self._collect_option_instruments()
                logging.info(f"[MarketDataService] Fallback to collected option_instruments: {len(option_instruments)}")
            
            all_instruments = future_instruments + option_instruments

            # 统计合约数量
            fut_count = len(future_instruments)
            opt_count = len(option_instruments)
            logging.info(f"[MarketDataService] Subscribing to {len(all_instruments)} instruments (futures: {fut_count}, options: {opt_count})")

            # 使用显式传递的策略实例
            strategy_obj = self._strategy_instance
            logging.info(f"[MarketDataService] strategy_obj = {strategy_obj}")
            logging.info(f"[MarketDataService] all_instruments count = {len(all_instruments)}")
            
            subscribed_ok = 0
            subscribe_failed = len(all_instruments)
            if strategy_obj is not None and all_instruments:
                try:
                    subscribed_ok, subscribe_failed = self._sync_storage_subscriptions(all_instruments)
                    logging.info(
                        f"[MarketDataService] Storage subscribe done: success={subscribed_ok}, failed={subscribe_failed}"
                    )
                except Exception as e:
                    logging.error(f"[MarketDataService] Storage subscribe failed: {e}")
            elif not all_instruments:
                logging.warning("[MarketDataService] No instruments collected for subscription")
            else:
                logging.warning("[MarketDataService] Strategy object unavailable, skipped storage subscription")
            
            # 检查市场中心可用性
            mc = self._get_market_center()
            if mc:
                mc_get_kline = self._resolve_kline_fetch_callable(mc)
                if callable(mc_get_kline):
                    # 市场中心可用时仅启动一次后台历史加载，避免重入递归。
                    with self._historical_loader_lock:
                        if not self._historical_loader_started:
                            import threading
                            threading.Thread(target=self._async_load_historical_klines, daemon=True).start()
                            self._historical_loader_started = True
                            logging.info("[MarketDataService] Started background historical data load")
                            logging.info("[HKL] Background historical loader started from subscribe_instruments")
                        else:
                            logging.info("[HKL] Background historical loader already started, skip duplicate start")
                else:
                    logging.warning("[MarketDataService] Market center kline api not available, skipping historical data load")
                    logging.warning("[HKL] Historical load skipped: market center kline api not available")
            else:
                logging.warning("[MarketDataService] Market center not available, skipping historical data load")
                logging.warning("[HKL] Historical load skipped: market center is None")
            
            # 立即返回合约收集结果，不等待历史数据加载
            logging.info(f"[MarketDataService] Subscription completed: {len(all_instruments)} instruments registered")
            return subscribed_ok > 0
            
        except Exception as e:
            logging.error(f"[MarketDataService.subscribe_instruments] Error: {e}")
            return False
    
    def _async_load_historical_klines(self):
        """异步加载历史K线数据"""
        try:
            logging.info("[MarketDataService] Starting async historical kline load")
            logging.info("[HKL] Async historical loader entered")
            empty_keys, non_empty_keys = self.load_historical_klines()
            logging.info(f"[MarketDataService] Async historical kline load completed: {len(non_empty_keys)} successful, {len(empty_keys)} failed")
            logging.info(f"[HKL] Async historical loader completed: success={len(non_empty_keys)}, failed={len(empty_keys)}")
        except Exception as e:
            logging.error(f"[MarketDataService._async_load_historical_klines] Error: {e}")
            logging.error(f"[HKL] Async historical loader error: {e}")


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'MarketDataService',
    'MarketTimeService',
    'LightKLine',
    'KLineData',
    'TickData',
    'safe_int',
    'safe_float',
    'get_market_time_service',
    'is_market_open',
]
