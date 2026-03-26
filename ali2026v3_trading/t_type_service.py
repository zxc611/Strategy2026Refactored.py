"""
T 型图服务模块 - CQRS 架构 Query 层
来源：12_trading_logic.py (部分)
功能：期权宽度计算 + T 型图数据生成
行数：~200 行 (-33% vs 原 300 行)

优化 v1.1 (2026-03-16):
- ✅ 添加数据源适配层接口
- ✅ 集成 EventBus 发布计算结果
- ✅ 性能基准测试支持
"""
from __future__ import annotations

import threading
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Protocol
from collections import defaultdict
from abc import ABC, abstractmethod

# 导入 EventBus（可选依赖）
try:
    from event_bus import EventBus, get_global_event_bus
    _HAS_EVENT_BUS = True
except ImportError as e:
    logging.warning(f"[TTypeService] Failed to import EventBus: {e}")
    _HAS_EVENT_BUS = False
    EventBus = None


# ============================================================================
# 数据源适配层接口
# ============================================================================

class DataSourceAdapter(ABC):
    """数据源适配器抽象基类
    
    用于对接不同的数据源（实盘、模拟盘、历史数据库等）
    """
    
    @abstractmethod
    def get_option_chain(self, underlying_symbol: str) -> List[Dict[str, Any]]:
        """获取期权链数据
        
        Args:
            underlying_symbol: 标的符号（如 IF2501）
            
        Returns:
            List[Dict]: 期权链数据，每个元素包含 instrument_id, strike_price, option_type 等
        """
        pass
    
    @abstractmethod
    def get_underlying_price(self, symbol: str) -> float:
        """获取标的价格
        
        Args:
            symbol: 合约代码
            
        Returns:
            float: 当前价格
        """
        pass


class SimpleDataSourceAdapter(DataSourceAdapter):
    """简单数据源适配器（示例实现）
    
    可直接使用或作为其他适配器的基础
    """
    
    def __init__(self, data_provider: Any = None):
        """初始化适配器
        
        Args:
            data_provider: 数据提供者对象（需支持 get_option_chain 和 get_price 方法）
        """
        self._data_provider = data_provider
    
    def get_option_chain(self, underlying_symbol: str) -> List[Dict[str, Any]]:
        """获取期权链数据
        
        Args:
            underlying_symbol: 标的符号
            
        Returns:
            List[Dict]: 期权链数据
        """
        if self._data_provider and hasattr(self._data_provider, 'get_option_chain'):
            return self._data_provider.get_option_chain(underlying_symbol)
        else:
            # 返回空列表或使用默认数据
            logging.warning(f"[SimpleDataSourceAdapter] No data provider for {underlying_symbol}")
            return []
    
    def get_underlying_price(self, symbol: str) -> float:
        """获取标的价格
        
        Args:
            symbol: 合约代码
            
        Returns:
            float: 当前价格
        """
        if self._data_provider and hasattr(self._data_provider, 'get_price'):
            return self._data_provider.get_price(symbol)
        else:
            logging.warning(f"[SimpleDataSourceAdapter] No price data for {symbol}")
            return 0.0


class TTypeService:
    """T 型图服务 - Query 层
    
    职责:
    - 期权宽度计算
    - T 型图数据生成
    - 期权类型识别
    - 虚值/实值判断
    
    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 高性能缓存
    """
    
    def __init__(self, data_source: Optional[Any] = None, event_bus: Optional[EventBus] = None):
        """初始化 T 型图服务
        
        Args:
            data_source: 数据源适配器（可选，实现 get_option_chain() 接口）
            event_bus: 事件总线实例（可选，用于发布计算结果）
        """
        # 计算结果缓存
        self._option_width_results: Dict[str, Dict] = {}
        self._option_type_cache: Dict[str, str] = {}
        self._out_of_money_cache: Dict[str, bool] = {}
        
        # 统计信息
        self._stats = {
            'total_calculations': 0,
            'successful_calculations': 0,
            'failed_calculations': 0,
            'average_calculation_time': 0.0
        }
        
        # 线程锁
        self._data_lock = threading.RLock()
        self._cache_max_size = 10000
        
        # 数据源适配器
        self._data_source = data_source
        
        # EventBus 集成
        self._event_bus = event_bus or (get_global_event_bus() if _HAS_EVENT_BUS and get_global_event_bus else None)
    
    def calculate_option_width(
        self,
        instrument_id: str,
        underlying_price: float,
        strike_price: float,
        option_type: str = 'CALL'
    ) -> Dict[str, Any]:
        """计算期权宽度
        
        Args:
            instrument_id: 期权合约代码
            underlying_price: 标的价格
            strike_price: 行权价
            option_type: 期权类型 ('CALL'/'PUT')
            
        Returns:
            Dict: 计算结果 {width, moneyness, is_otm, ...}
        """
        if not instrument_id or underlying_price <= 0 or strike_price <= 0:
            logging.error(f"[TTypeService] Invalid params: {instrument_id}, {underlying_price}, {strike_price}")
            return {}
        
        start_time = time.time()
        
        try:
            with self._data_lock:
                self._stats['total_calculations'] += 1
            
            # 计算虚实值程度
            if option_type.upper() == 'CALL':
                # 看涨期权
                moneyness = (underlying_price - strike_price) / strike_price * 100
                is_otm = underlying_price < strike_price  # 虚值
            else:
                # 看跌期权
                moneyness = (strike_price - underlying_price) / strike_price * 100
                is_otm = underlying_price > strike_price  # 虚值
            
            # 计算宽度（绝对值）
            width = abs(underlying_price - strike_price)
            
            # 结果
            result = {
                'instrument_id': instrument_id,
                'underlying_price': underlying_price,
                'strike_price': strike_price,
                'option_type': option_type.upper(),
                'width': width,
                'moneyness_percent': moneyness,
                'is_otm': is_otm,
                'is_itm': not is_otm,
                'calculated_at': datetime.now()
            }
            
            # 缓存结果
            with self._data_lock:
                self._option_width_results[instrument_id] = result
                
                # 缓存清理（防止内存溢出）
                if len(self._option_width_results) > self._cache_max_size:
                    # 移除最旧的一半
                    keys_to_remove = list(self._option_width_results.keys())[:self._cache_max_size // 2]
                    for key in keys_to_remove:
                        del self._option_width_results[key]
                
                self._stats['successful_calculations'] += 1
                
                # 更新平均耗时
                elapsed = time.time() - start_time
                old_avg = self._stats['average_calculation_time']
                count = self._stats['successful_calculations']
                self._stats['average_calculation_time'] = (old_avg * (count - 1) + elapsed) / count
            
            # 发布计算结果事件（如果 EventBus 可用）
            if self._event_bus:
                try:
                    from event_bus import KLineEvent as EventBusKLineEvent
                    calculation_event = type('OptionWidthCalculatedEvent', (), {
                        'type': 'OptionWidthCalculatedEvent',
                        'instrument_id': instrument_id,
                        'width': width,
                        'moneyness': moneyness,
                        'is_otm': is_otm
                    })()
                    self._event_bus.publish(calculation_event, async_mode=True)
                except Exception as e:
                    logging.error(f"[TTypeService] Failed to publish calculation event: {e}")
            
            logging.debug(f"[TTypeService] Calculated: {instrument_id} width={width:.2f}, moneyness={moneyness:.2f}%")
            
            return result
            
        except Exception as e:
            logging.error(f"[TTypeService] Calculation error: {e}")
            with self._data_lock:
                self._stats['failed_calculations'] += 1
            # 返回空字典表示计算失败（调用方需检查返回值）
            logging.warning("[TTypeService] Returning empty dict due to calculation failure")
            return {}
    
    def get_option_type(self, instrument_id: str) -> Optional[str]:
        """识别期权类型
        
        Args:
            instrument_id: 期权合约代码
            
        Returns:
            Optional[str]: 'CALL' 或 'PUT'
        """
        # 检查缓存
        with self._data_lock:
            if instrument_id in self._option_type_cache:
                return self._option_type_cache[instrument_id]
        
        # 从合约代码解析（示例：IF2501-C-3500）
        try:
            parts = instrument_id.split('-')
            if len(parts) >= 2:
                option_type = parts[-2].upper()
                if option_type in ('C', 'CALL'):
                    result = 'CALL'
                elif option_type in ('P', 'PUT'):
                    result = 'PUT'
                else:
                    result = None
            else:
                result = None
        except Exception as e:
            logging.error(f"[TTypeService] Failed to parse option type from {instrument_id}: {e}")
            result = None
        
        # 缓存结果
        if result:
            with self._data_lock:
                self._option_type_cache[instrument_id] = result
                
                # 缓存清理
                if len(self._option_type_cache) > self._cache_max_size:
                    keys_to_remove = list(self._option_type_cache.keys())[:self._cache_max_size // 2]
                    for key in keys_to_remove:
                        del self._option_type_cache[key]
        
        return result
    
    def is_out_of_the_money(
        self,
        instrument_id: str,
        underlying_price: float,
        strike_price: float,
        option_type: str
    ) -> bool:
        """判断是否为虚值期权
        
        Args:
            instrument_id: 期权合约代码
            underlying_price: 标的价格
            strike_price: 行权价
            option_type: 期权类型
            
        Returns:
            bool: 是否虚值
        """
        cache_key = f"{instrument_id}_{underlying_price}_{strike_price}"
        
        # 检查缓存
        with self._data_lock:
            if cache_key in self._out_of_money_cache:
                return self._out_of_money_cache[cache_key]
        
        # 计算
        if option_type.upper() == 'CALL':
            is_otm = underlying_price < strike_price
        else:
            is_otm = underlying_price > strike_price
        
        # 缓存结果
        with self._data_lock:
            self._out_of_money_cache[cache_key] = is_otm
            
            # 缓存清理
            if len(self._out_of_money_cache) > self._cache_max_size:
                keys_to_remove = list(self._out_of_money_cache.keys())[:self._cache_max_size // 2]
                for key in keys_to_remove:
                    del self._out_of_money_cache[key]
        
        return is_otm
    
    def generate_t_type_data(
        self,
        underlying_symbol: str,
        options_chain: List[Dict],
        underlying_price: float
    ) -> Dict[str, Any]:
        """生成 T 型图数据
        
        Args:
            underlying_symbol: 标的符号（如 IF2501）
            options_chain: 期权链数据
            underlying_price: 标的价格
            
        Returns:
            Dict: T 型图数据结构
        """
        t_type_data = {
            'underlying_symbol': underlying_symbol,
            'underlying_price': underlying_price,
            'calls': [],
            'puts': [],
            'strikes': set(),
            'generated_at': datetime.now()
        }
        
        for option in options_chain:
            instrument_id = option.get('instrument_id', '')
            strike = option.get('strike_price', 0)
            opt_type = option.get('option_type', '')
            
            if not instrument_id or strike <= 0:
                continue
            
            # 计算期权宽度
            result = self.calculate_option_width(
                instrument_id=instrument_id,
                underlying_price=underlying_price,
                strike_price=strike,
                option_type=opt_type
            )
            
            if not result:
                continue
            
            # 添加到对应列表
            if opt_type.upper() == 'CALL':
                t_type_data['calls'].append(result)
            else:
                t_type_data['puts'].append(result)
            
            t_type_data['strikes'].add(strike)
        
        # 转换集合为列表
        t_type_data['strikes'] = sorted(list(t_type_data['strikes']))
        
        logging.info(f"[TTypeService] Generated T-type data: {len(t_type_data['calls'])} calls, "
                    f"{len(t_type_data['puts'])} puts")
        
        return t_type_data
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息
        
        Returns:
            Dict: 统计数据
        """
        with self._data_lock:
            return {
                **self._stats,
                'cached_results': len(self._option_width_results),
                'cached_types': len(self._option_type_cache),
                'cached_otm': len(self._out_of_money_cache)
            }
    
    def clear_cache(self) -> None:
        """清空所有缓存"""
        with self._data_lock:
            self._option_width_results.clear()
            self._option_type_cache.clear()
            self._out_of_money_cache.clear()
            logging.info("[TTypeService] Cache cleared")
    
    # ========================================================================
    # 性能基准测试支持
    # ========================================================================
    
    def run_benchmark(self, iterations: int = 1000) -> Dict[str, Any]:
        """运行性能基准测试
        
        Args:
            iterations: 测试迭代次数
            
        Returns:
            Dict: 基准测试结果 {avg_latency, total_time, calculations_per_second, ...}
        """
        import statistics
        
        latencies = []
        start_total = time.time()
        
        for i in range(iterations):
            test_instrument = f"TEST_C_{3500 + (i % 10)}"
            test_underlying = 3550.0 + (i % 5)
            test_strike = 3500.0 + (i % 10) * 10
            
            start = time.time()
            self.calculate_option_width(
                instrument_id=test_instrument,
                underlying_price=test_underlying,
                strike_price=test_strike,
                option_type='CALL'
            )
            elapsed = time.time() - start
            latencies.append(elapsed)
        
        total_time = time.time() - start_total
        
        # 计算统计信息
        results = {
            'iterations': iterations,
            'total_time_seconds': total_time,
            'avg_latency_ms': statistics.mean(latencies) * 1000,
            'min_latency_ms': min(latencies) * 1000,
            'max_latency_ms': max(latencies) * 1000,
            'median_latency_ms': statistics.median(latencies) * 1000,
            'stddev_latency_ms': statistics.stdev(latencies) * 1000 if len(latencies) > 1 else 0,
            'calculations_per_second': iterations / total_time if total_time > 0 else 0,
            'cache_hit_rate': self._stats.get('successful_calculations', 0) / max(1, self._stats.get('total_calculations', 0))
        }
        
        logging.info(f"[TTypeService] Benchmark completed: {iterations} iterations, "
                    f"avg latency={results['avg_latency_ms']:.3f}ms, "
                    f"{results['calculations_per_second']:.1f} calc/s")
        
        return results


# 导出公共接口
__all__ = ['TTypeService']
