"""
T 型图服务模块 - CQRS 架构 Query 层
来源：12_trading_logic.py (部分) + analytics_service.py (价格分析功能)
功能：期权宽度计算 + T 型图数据生成 + 价格分析 + 方向判断
行数：~200 行 (-33% vs 原 300 行)

优化 v1.1 (2026-03-16):
- ✅ 添加数据源适配层接口
- ✅ 集成 EventBus 发布计算结果
- ✅ 性能基准测试支持

优化 v1.2 (2026-03-28):
- ✅ 迁移价格分析功能 (从 analytics_service.py)
- ✅ 迁移方向判断功能 (从 analytics_service.py)
- ✅ 添加 K线数据结构
"""
from __future__ import annotations

import threading
import logging
import time
import struct
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Protocol
from collections import defaultdict
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

# 导入 EventBus（可选依赖）
try:
    from event_bus import EventBus, get_global_event_bus
    _HAS_EVENT_BUS = True
except ImportError as e:
    logging.warning(f"[TTypeService] Failed to import EventBus: {e}")
    _HAS_EVENT_BUS = False
    EventBus = None


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class PriceAnalysis:
    """价格分析结果"""
    current_price: float = 0.0
    previous_price: float = 0.0
    price_change: float = 0.0
    price_change_pct: float = 0.0
    is_rising: bool = False
    is_falling: bool = False
    volatility: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class DirectionSignal:
    """方向信号"""
    direction: str = "neutral"  # "up", "down", "neutral"
    strength: float = 0.0
    confidence: float = 0.0
    reason: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


# ============================================================================
# 工具函数
# ============================================================================

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
    - 价格趋势分析
    - 方向信号判断

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
        # 线程锁
        self._data_lock = threading.RLock()

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
        # 计算
        if option_type.upper() == 'CALL':
            is_otm = underlying_price < strike_price
        else:
            is_otm = underlying_price > strike_price

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
        """获取统计信息（已移除缓存统计，保留接口兼容性）"""
        return {
            'total_calculations': 0,
            'successful_calculations': 0,
            'failed_calculations': 0,
            'average_calculation_time': 0.0,
            'cached_results': 0,
            'cached_types': 0,
            'cached_otm': 0
        }
    
    def clear_cache(self) -> None:
        """清空所有缓存（已移除独立缓存，保留接口兼容性）"""
        pass
    
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
            'cache_hit_rate': 0.0
        }
        
        logging.info(f"[TTypeService] Benchmark completed: {iterations} iterations, "
                    f"avg latency={results['avg_latency_ms']:.3f}ms, "
                    f"{results['calculations_per_second']:.1f} calc/s")

        return results

    # ========================================================================
    # 价格分析
    # ========================================================================

    def analyze_price(self, klines: List[Any]) -> PriceAnalysis:
        """
        分析价格趋势

        Args:
            klines: K线数据列表

        Returns:
            PriceAnalysis: 价格分析结果
        """
        try:
            if not klines or len(klines) < 2:
                return PriceAnalysis()

            # 获取当前和前一根K线
            current_bar = klines[-1]
            previous_bar = klines[-2]

            current_price = self._get_bar_close(current_bar)
            previous_price = self._get_bar_close(previous_bar)

            if current_price <= 0 or previous_price <= 0:
                return PriceAnalysis()

            # 计算变化
            price_change = current_price - previous_price
            price_change_pct = (price_change / previous_price) * 100 if previous_price > 0 else 0.0

            # 计算波动率
            volatility = self._calculate_volatility(klines)

            return PriceAnalysis(
                current_price=_to_float32(current_price),
                previous_price=_to_float32(previous_price),
                price_change=_to_float32(price_change),
                price_change_pct=_to_float32(price_change_pct),
                is_rising=price_change > 0,
                is_falling=price_change < 0,
                volatility=_to_float32(volatility),
            )

        except Exception as e:
            logging.error(f"[TTypeService.analyze_price] Error: {e}")
            return PriceAnalysis()

    def _calculate_volatility(self, klines: List[Any]) -> float:
        """计算波动率"""
        try:
            if len(klines) < 3:
                return 0.0

            closes = [self._get_bar_close(bar) for bar in klines[-10:]]
            closes = [c for c in closes if c > 0]

            if len(closes) < 2:
                return 0.0

            # 简单波动率：最高价与最低价的差除以均值
            high_price = max(closes)
            low_price = min(closes)
            avg_price = sum(closes) / len(closes)

            if avg_price <= 0:
                return 0.0

            volatility = (high_price - low_price) / avg_price
            return volatility

        except Exception as e:
            logging.error(f"[TTypeService._calculate_volatility] Error: {e}")
            raise RuntimeError(f"Volatility calculation failed: {e}") from e

    def _get_bar_close(self, bar: Any) -> float:
        """获取K线收盘价"""
        try:
            if isinstance(bar, dict):
                for key in ['close', 'Close', 'CLOSE', 'last', 'last_price']:
                    if key in bar:
                        return safe_float(bar[key], 0.0)
            return safe_float(getattr(bar, 'close', 0) or getattr(bar, 'last', 0), 0.0)
        except (AttributeError, KeyError, TypeError, ValueError) as e:
            logging.error(f"[TTypeService._get_bar_close] Error: {e}")
            raise RuntimeError(f"Bar close price extraction failed: {e}") from e

    # ========================================================================
    # 方向信号
    # ========================================================================

    def detect_direction(self, klines: List[Any],
                         threshold: float = 0.001) -> DirectionSignal:
        """
        检测价格方向信号

        Args:
            klines: K线数据列表
            threshold: 变化阈值（默认0.1%）

        Returns:
            DirectionSignal: 方向信号
        """
        try:
            price_analysis = self.analyze_price(klines)

            if price_analysis.current_price <= 0:
                return DirectionSignal(direction="neutral", reason="无效价格")

            change_pct = abs(price_analysis.price_change_pct)

            if change_pct < threshold * 100:
                return DirectionSignal(
                    direction="neutral",
                    strength=0.0,
                    confidence=0.0,
                    reason="变化幅度不足"
                )

            direction = "up" if price_analysis.is_rising else "down"
            strength = min(change_pct / 5.0, 1.0)  # 归一化到0-1
            confidence = min(price_analysis.volatility * 10, 1.0)

            return DirectionSignal(
                direction=direction,
                strength=_to_float32(strength),
                confidence=_to_float32(confidence),
                reason=f"价格变化{price_analysis.price_change_pct:.2f}%"
            )

        except Exception as e:
            logging.error(f"[TTypeService.detect_direction] Error: {e}")
            return DirectionSignal(direction="neutral", reason=f"计算错误: {e}")


# 导出公共接口
__all__ = [
    'TTypeService',
    'PriceAnalysis',
    'DirectionSignal',
    'DataSourceAdapter',
    'SimpleDataSourceAdapter',
]
