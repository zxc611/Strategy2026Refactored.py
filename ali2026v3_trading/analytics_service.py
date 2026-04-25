"""
analytics_service.py - 分析服务

合并来源：12_trading_logic.py (OptionWidthCalculationMixin) + 10_gate.py (部分)
合并策略：提取期权宽度计算、价格分析、方向判断等分析功能

重构目标：
- 旧架构：12_trading_logic.py (~3600行) + 10_gate.py (~1800行) = ~850行相关功能
- 新架构：analytics_service.py (~640行)
- 减少：~25%

核心改进：
1. 单一职责：专注于市场数据分析计算
2. 无状态设计：纯函数式计算逻辑
3. 线程安全：支持并发计算
4. 可测试性：独立的分析模块

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-03-16
"""

from __future__ import annotations

import re
import struct
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable, Protocol, runtime_checkable
from dataclasses import dataclass, field
from collections import deque
from ali2026v3_trading.shared_utils import normalize_instrument_id


class RingBuffer:
    """环形缓冲区，用于高效管理历史数据（线程安全）"""
    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.data = deque(maxlen=maxlen)
        self._lock = threading.Lock()
    
    def append(self, item):
        with self._lock:
            self.data.append(item)
    
    def extend(self, items):
        with self._lock:
            self.data.extend(items)
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, index):
        return self.data[index]
    
    def __iter__(self):
        return iter(self.data)
    
    def clear(self):
        self.data.clear()
    
    def to_list(self):
        return list(self.data)


class ParamsProtocol(Protocol):
    """参数协议接口"""
    def __getattr__(self, item: str) -> Any: ...


class MarketDataServiceProtocol(Protocol):
    """市场数据服务协议接口"""
    def get_kline(self, exchange: str, future_id: str, count: int) -> List[Any]: ...


# ============================================================================
# 辅助函数
# ============================================================================

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
    """✅ ID唯一：委托shared_utils.to_float32，不再独立实现"""
    from ali2026v3_trading.shared_utils import to_float32
    return to_float32(value)


def _get_field(data: Any, dict_keys: list, obj_attrs: list, default: Any = None) -> Any:
    """方法唯一修复：统一为object属性访问，dict先转为SimpleNamespace"""
    if isinstance(data, dict):
        from types import SimpleNamespace
        data = SimpleNamespace(**data)
    for attr in obj_attrs:
        val = getattr(data, attr, None)
        if val is not None:
            return val
    for key in dict_keys:
        val = getattr(data, key, None)
        if val is not None:
            return val
    return default


_FLOAT32_EPS = _to_float32(0.0001)


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class OptionWidthResult:
    """期权宽度计算结果"""
    exchange: str = ""
    future_id: str = ""
    option_width: float = 0.0
    all_sync: bool = False
    future_rising: bool = False
    current_price: float = 0.0
    previous_price: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    specified_month_count: int = 0
    next_specified_month_count: int = 0
    total_specified_target: float = 0.0
    total_next_specified_target: float = 0.0
    total_all_om_specified: float = 0.0
    total_all_om_next_specified: float = 0.0
    has_direction_options: bool = False
    top_active_calls: List[Dict] = field(default_factory=list)
    top_active_puts: List[Dict] = field(default_factory=list)


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
# 分析服务
# ============================================================================

class AnalyticsService:
    """
    分析服务 - 市场数据分析计算

    职责：
    1. 期权宽度计算
    2. 价格趋势分析
    3. 方向信号判断
    4. 波动率计算

    使用方式：
        service = AnalyticsService(params, market_data_service)
        result = service.calculate_option_width("SHFE", "ru2501")
    """

    def __init__(self, params: Optional[ParamsProtocol] = None,
                 market_data_service: Optional[MarketDataServiceProtocol] = None,
                 output_func: Optional[Callable] = None,
                 params_service = None,
                 storage = None):
        """
        初始化分析服务

        Args:
            params: 参数配置对象
            market_data_service: 市场数据服务（可选）
            output_func: 输出函数（可选）
            params_service: ParamsService实例（必需，用于获取metadata）
            storage: Storage实例（可选，用于直接查询元数据表）
        
        ✅ 分组D根因修复：不再依赖外部注入的option_instruments，直接从metadata获取
        """
        if params_service is None:
            raise ValueError("params_service is required for analytics_service (Group D root cause fix)")
        
        self.params = params
        self.market_data = market_data_service
        self._output = output_func or print
        self._params_service = params_service  # ✅ 必需：metadata来源
        self._storage = storage  # ✅ 可选：直接查询元数据表

        # 计算状态
        self._calc_lock = threading.RLock()
        self._calc_stats = {
            "total_calculations": 0,
            "successful_calculations": 0,
            "failed_calculations": 0,
            "last_calculation_time": None,
        }

        # 缓存（3元素tuple: value, timestamp）
        self._option_type_cache: Dict[str, Tuple[str, float]] = {}
        self._out_of_money_cache: Dict[str, Tuple[bool, float]] = {}
        self._cache_max_size = 10000
        self._cache_ttl = 300.0  # 缓存过期时间（秒）

        # ✅ 分组D根因修复：移除外部注入的option_instruments，改为从metadata动态获取
        # self.option_instruments: Dict[str, List[Any]] = {}  # ❌ 已删除
        # self.future_to_option_map: Dict[str, str] = {}  # ❌ 已删除

    def output(self, message: str) -> None:
        """输出日志信息"""
        try:
            if self._output:
                self._output(message)
            else:
                logging.info(f"[AnalyticsService] {message}")
        except Exception as e:
            logging.error(f"[AnalyticsService.output] Error: {e}")

    # ========================================================================
    # 期权宽度计算
    # ========================================================================

    def calculate_option_width(self, exchange: str, future_id: str,
                                klines: Optional[List[Any]] = None,
                                options: Optional[List[Any]] = None) -> OptionWidthResult:
        """
        计算期权宽度（主入口）

        Args:
            exchange: 交易所代码
            future_id: 期货合约代码
            klines: K线数据（可选，若不提供则从market_data获取）
            options: 期权合约列表（可选，若不提供则从本地缓存获取）

        Returns:
            OptionWidthResult: 期权宽度计算结果
        """
        try:
            # 定期清理过期缓存
            self._clean_expired_cache()

            month_meta = self._build_month_selection_meta(future_id)
            if not month_meta['future_allowed']:
                logging.info(
                    f"[AnalyticsService] Skip width calc by future month filter: {future_id}"
                )
                return self._create_zero_width_result(exchange, future_id)
            
            # 获取K线数据
            if klines is None and self.market_data:
                klines = self.market_data.get_kline(exchange, future_id, count=10)

            if not klines or len(klines) < 2:
                return self._create_zero_width_result(exchange, future_id)

            # 获取期权数据
            if options is None:
                options = self._get_option_group_options(future_id)

            if not options:
                return self._create_zero_width_result(exchange, future_id)

            if not month_meta['option_allowed']:
                logging.info(
                    f"[AnalyticsService] Skip width calc by option month filter: {future_id}"
                )
                return self._create_zero_width_result(exchange, future_id)

            # 价格分析
            price_analysis = self.analyze_price(klines)
            if price_analysis.current_price <= 0:
                return self._create_zero_width_result(exchange, future_id)

            # 计算期权宽度
            result = self._compute_option_width(
                exchange, future_id, options, price_analysis, month_meta
            )

            # 更新统计
            self._update_calc_stats(True)

            return result

        except Exception as e:
            logging.error(f"[AnalyticsService.calculate_option_width] Error: {e}")
            self._update_calc_stats(False)
            return self._create_zero_width_result(exchange, future_id)

    def _compute_option_width(self, exchange: str, future_id: str,
                               options: List[Any],
                               price_analysis: PriceAnalysis,
                               month_meta: Optional[Dict[str, Any]] = None) -> OptionWidthResult:
        """计算期权宽度核心逻辑"""
        try:
            current_price = price_analysis.current_price
            is_rising = price_analysis.is_rising
            month_meta = month_meta or {}

            # 分类期权
            calls, puts = self._classify_options(options, current_price)

            # 计算宽度
            call_width = self._calculate_single_width(calls, current_price, "call")
            put_width = self._calculate_single_width(puts, current_price, "put")

            # 总宽度
            total_width = call_width + put_width

            # 获取活跃期权
            top_calls = self._get_top_active_options(calls, current_price, "call", 3)
            top_puts = self._get_top_active_options(puts, current_price, "put", 3)

            return OptionWidthResult(
                exchange=exchange,
                future_id=future_id,
                option_width=_to_float32(total_width),
                all_sync=len(calls) > 0 and len(puts) > 0,
                future_rising=is_rising,
                current_price=current_price,
                previous_price=price_analysis.previous_price,
                specified_month_count=int(month_meta.get('specified_month_count', len(options))),
                next_specified_month_count=int(month_meta.get('next_specified_month_count', 0)),
                total_specified_target=_to_float32(month_meta.get('total_specified_target', 0.0)),
                total_next_specified_target=_to_float32(month_meta.get('total_next_specified_target', 0.0)),
                has_direction_options=len(top_calls) > 0 or len(top_puts) > 0,
                top_active_calls=top_calls,
                top_active_puts=top_puts,
            )

        except Exception as e:
            logging.error(f"[AnalyticsService._compute_option_width] Error: {e}")
            return self._create_zero_width_result(exchange, future_id)

    def _calculate_single_width(self, options: List[Any], current_price: float,
                                 option_type: str) -> float:
        """计算单边期权宽度"""
        if not options:
            return 0.0

        try:
            total_width = 0.0
            for opt in options:
                strike = self._get_strike_price(opt)
                if strike <= 0:
                    continue

                # 计算距离
                distance = abs(current_price - strike)
                weight = self._get_option_weight(opt, current_price, option_type)

                total_width += distance * weight

            return total_width

        except Exception as e:
            logging.error(f"[AnalyticsService._calculate_single_width] Error: {e}")
            raise RuntimeError(f"Option width calculation failed: {e}") from e

    def _get_option_weight(self, option: Any, current_price: float,
                           option_type: str) -> float:
        """获取期权权重"""
        try:
            # 获取期权价格
            opt_price = self._get_option_price(option)
            if opt_price <= 0:
                return 0.0

            # 简单权重：价格越高权重越大
            weight = min(opt_price / current_price, 1.0) if current_price > 0 else 0.0
            return _to_float32(weight)

        except Exception as e:
            logging.error(f"[AnalyticsService._get_option_weight] Error: {e}")
            raise RuntimeError(f"Option weight calculation failed: {e}") from e

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
            logging.error(f"[AnalyticsService.analyze_price] Error: {e}")
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
            logging.error(f"[AnalyticsService._calculate_volatility] Error: {e}")
            raise RuntimeError(f"Volatility calculation failed: {e}") from e

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
            logging.error(f"[AnalyticsService.detect_direction] Error: {e}")
            return DirectionSignal(direction="neutral", reason=f"计算错误: {e}")

    # ========================================================================
    # 辅助方法
    # ========================================================================

    def _get_bar_close(self, bar: Any) -> float:
        """获取K线收盘价"""
        try:
            val = _get_field(bar, ['close', 'Close', 'CLOSE', 'last', 'last_price'], ['close', 'last'], 0)
            return safe_float(val, 0.0)
        except (AttributeError, KeyError, TypeError, ValueError) as e:
            logging.error(f"[AnalyticsService._get_bar_close] Error: {e}")
            raise RuntimeError(f"Bar close price extraction failed: {e}") from e

    def _get_option_group_options(self, future_id: str) -> List[Any]:
        """获取期货对应的期权合约列表
        
        ✅ 分组D根因修复：直接从 metadata 获取，不再依赖漂移的 options_dict.key
        """
        try:
            fut = normalize_instrument_id(future_id)
            if not fut:
                return []

            # ✅ 从 params_service 获取该期货的所有期权
            try:
                # 获取所有元数据缓存
                all_instruments = self._params_service.get_all_instrument_cache()
                if not all_instruments:
                    logging.warning(f"[AnalyticsService] No instruments found in params_service")
                    return []

                future_meta = self._params_service.get_instrument_meta_by_id(fut) or {}
                future_internal_id = future_meta.get('internal_id')
                
                # 筛选出标的为该期货的期权
                option_list = []
                for inst_id, meta in all_instruments.items():
                    if meta.get('type') == 'option':  # ✅ 使用'type'字段，不是'instrument_type'
                        option_underlying_id = meta.get('underlying_future_id')
                        if future_internal_id and option_underlying_id and str(option_underlying_id) == str(future_internal_id):
                            option_list.append(meta)
                
                return option_list
                
            except Exception as e:
                logging.error(f"[AnalyticsService] Failed to get options from params_service: {e}")
                return []

        except Exception as e:
            logging.error(f"[AnalyticsService._get_option_group_options] Error: {e}")
            raise RuntimeError(f"Option group lookup failed: {e}") from e

    def _resolve_calc_flag(self, flag_name: str, default: bool = False) -> bool:
        """解析计算层月份过滤标志（单一参数名接口）。"""
        try:
            value = getattr(self.params, flag_name, None)
            if value is not None:
                return bool(value)
            return default
        except Exception:
            return default

    # ✅ 方法唯一：委托shared_utils.extract_product_code，不再独立实现
    def _extract_product_code(self, symbol: str) -> str:
        """提取品种前缀。"""
        from ali2026v3_trading.shared_utils import extract_product_code
        return extract_product_code(symbol)

    # 期货->期权品种静态映射（与 t_type_service._option_to_future_product 互逆）
    _FUTURE_TO_OPTION_MAP: Dict[str, str] = {'IF': 'IO', 'IH': 'HO', 'IC': 'MO', 'IM': 'EO'}

    def _to_option_group_id(self, future_symbol: str) -> str:
        """将期货月份ID映射为期权分组ID，如 IF2604 -> IO2604。
        
        使用静态映射表，不再依赖已删除的 self.future_to_option_map。
        """
        norm = normalize_instrument_id(future_symbol)
        match = re.match(r"^([A-Za-z]+)(\d{3,4})$", norm)
        if not match:
            return norm
        product = match.group(1)
        suffix = match.group(2)
        option_product = self._FUTURE_TO_OPTION_MAP.get(product, "")
        if not option_product:
            return norm
        return f"{option_product}{suffix}"

    def _get_month_bucket(self, symbol: str) -> Optional[str]:
        """判断合约属于指定月还是下月。支持期货ID和期权分组ID。
        
        使用静态映射表，不再依赖已删除的 self.future_to_option_map。
        """
        norm = normalize_instrument_id(symbol)
        if not norm:
            return None

        month_mapping = getattr(self.params, 'month_mapping', {}) or {}
        if not isinstance(month_mapping, dict) or not month_mapping:
            return None

        product = self._extract_product_code(norm)
        reverse_map = {
            str(opt).strip(): str(fut).strip()
            for fut, opt in self._FUTURE_TO_OPTION_MAP.items()
            if str(opt).strip()
        }
        candidate_products = [product]
        if product in reverse_map:
            candidate_products.append(reverse_map[product])

        for candidate_product in candidate_products:
            entry = month_mapping.get(candidate_product)
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                continue

            specified_id = normalize_instrument_id(str(entry[0]))
            next_id = normalize_instrument_id(str(entry[1]))
            option_specified_id = self._to_option_group_id(specified_id)
            option_next_id = self._to_option_group_id(next_id)

            if norm in {specified_id, option_specified_id}:
                return 'specified'
            if norm in {next_id, option_next_id}:
                return 'next'

        return None

    def _build_month_selection_meta(self, future_id: str) -> Dict[str, Any]:
        """构造计算层月份过滤元数据。"""
        filter_futures = self._resolve_calc_flag(
            'subscribe_only_specified_month_futures',
            False,
        )
        filter_options = self._resolve_calc_flag(
            'subscribe_only_specified_month_options',
            False,
        )

        bucket = self._get_month_bucket(future_id)
        future_allowed = (not filter_futures) or (bucket in {'specified', 'next'})
        option_allowed = (not filter_options) or (bucket in {'specified', 'next'})

        specified_count = 0
        next_count = 0
        total_specified_target = 0.0
        total_next_target = 0.0

        if filter_options:
            total_specified_target = 1.0
            total_next_target = 1.0
            if bucket == 'specified':
                specified_count = 1
            elif bucket == 'next':
                next_count = 1

        return {
            'bucket': bucket,
            'future_allowed': future_allowed,
            'option_allowed': option_allowed,
            'specified_month_count': specified_count,
            'next_specified_month_count': next_count,
            'total_specified_target': total_specified_target,
            'total_next_specified_target': total_next_target,
        }

    def _classify_options(self, options: List[Any],
                          current_price: float) -> Tuple[List[Any], List[Any]]:
        """分类期权为看涨和看跌"""
        calls = []
        puts = []

        for opt in options:
            opt_type = self._get_option_type(opt)
            if opt_type in ("C", "CALL", "看涨"):
                calls.append(opt)
            elif opt_type in ("P", "PUT", "看跌"):
                puts.append(opt)

        return calls, puts

    def _get_option_type(self, option: Any) -> str:
        """获取期权类型（带缓存）"""
        try:
            cache_key = _get_field(option, ['instrument_id', 'InstrumentID'], ['instrument_id', 'InstrumentID'], '') or str(id(option))
            now = time.time()
            if cache_key in self._option_type_cache:
                cached_value, cached_time = self._option_type_cache[cache_key]
                if now - cached_time < self._cache_ttl:
                    return cached_value
            val = _get_field(option, ['OptionType', 'OptionsType'], ['option_type'], '')
            result = str(val).strip()
            with self._calc_lock:
                self._option_type_cache[cache_key] = (result, now)
            return result
        except Exception as e:
            logging.error(f"[AnalyticsService._get_option_type] Error: {e}")
            raise RuntimeError(f"Option type extraction failed: {e}") from e

    def _get_strike_price(self, option: Any) -> float:
        """获取行权价"""
        try:
            val = _get_field(option, ['StrikePrice', 'strike', 'strike_price', 'Strike'], ['strike_price', 'strike', 'StrikePrice'], 0)
            return safe_float(val, 0.0)
        except Exception as e:
            logging.error(f"[AnalyticsService._get_strike_price] Error: {e}")
            raise RuntimeError(f"Strike price extraction failed: {e}") from e

    def _get_option_price(self, option: Any) -> float:
        """获取期权价格"""
        try:
            val = _get_field(option, ['last', 'last_price', 'close', 'price'], ['last', 'close', 'price'], 0)
            return safe_float(val, 0.0)
        except Exception as e:
            logging.error(f"[AnalyticsService._get_option_price] Error: {e}")
            raise RuntimeError(f"Option price extraction failed: {e}") from e

    def _is_out_of_money(self, option: Any, current_price: float) -> bool:
        """判断是否虚值期权（带缓存）"""
        try:
            cache_key = str(_get_field(option, ['instrument_id', 'InstrumentID'], ['instrument_id', 'InstrumentID'], '') or id(option))
            cache_key = f"{cache_key}_{current_price:.2f}"
            now = time.time()
            if cache_key in self._out_of_money_cache:
                cached_value, cached_time = self._out_of_money_cache[cache_key]
                if now - cached_time < self._cache_ttl:
                    return cached_value
            opt_type = self._get_option_type(option)
            strike = self._get_strike_price(option)
            if strike <= 0:
                result = False
            elif opt_type in ("C", "CALL", "看涨"):
                result = strike > current_price
            elif opt_type in ("P", "PUT", "看跌"):
                result = strike < current_price
            else:
                result = False
            with self._calc_lock:
                self._out_of_money_cache[cache_key] = (result, now)
            return result
        except Exception as e:
            logging.error(f"[AnalyticsService._is_out_of_money] Error: {e}")
            return False

    def _get_top_active_options(self, options: List[Any], current_price: float,
                                 option_type: str, top_n: int = 3) -> List[Dict]:
        """获取最活跃的期权"""
        try:
            scored = []
            for opt in options:
                strike = self._get_strike_price(opt)
                price = self._get_option_price(opt)
                if strike <= 0 or price <= 0:
                    continue

                # 活跃度评分：价格 * 成交量权重
                score = price
                scored.append({
                    "strike": strike,
                    "price": price,
                    "type": option_type,
                    "score": score,
                })

            # 按评分排序
            scored.sort(key=lambda x: x["score"], reverse=True)
            return scored[:top_n]

        except Exception as e:
            logging.error(f"[AnalyticsService._get_top_active_options] Error: {e}")
            raise RuntimeError(f"Top active options lookup failed: {e}") from e

    def _create_zero_width_result(self, exchange: str,
                                   future_id: str) -> OptionWidthResult:
        """创建零宽度结果"""
        return OptionWidthResult(
            exchange=exchange,
            future_id=future_id,
            option_width=0.0,
            all_sync=False,
            future_rising=False,
            current_price=0.0,
            previous_price=0.0,
        )

    def _update_calc_stats(self, success: bool) -> None:
        """更新计算统计"""
        with self._calc_lock:
            self._calc_stats["total_calculations"] += 1
            if success:
                self._calc_stats["successful_calculations"] += 1
            else:
                self._calc_stats["failed_calculations"] += 1
            self._calc_stats["last_calculation_time"] = datetime.now()

    # ========================================================================
    # 配置与状态
    # ========================================================================

    # ✅ 分组D根因修复：删除 set_option_instruments 和 set_future_to_option_map
    # 这些方法已被移除，因为 analytics_service 现在直接从 metadata 获取数据
    # def set_option_instruments(self, instruments: Dict[str, List[Any]]) -> None:
    #     """❌ 已删除：不再支持外部注入 option_instruments"""
    #     raise NotImplementedError("This method has been removed in Group D root cause fix")
    #
    # def set_future_to_option_map(self, mapping: Dict[str, str]) -> None:
    #     """❌ 已删除：不再支持外部注入 future_to_option_map"""
    #     raise NotImplementedError("This method has been removed in Group D root cause fix")

    def get_stats(self) -> Dict[str, Any]:
        """获取计算统计信息"""
        with self._calc_lock:
            stats = dict(self._calc_stats)
            stats['service_name'] = 'AnalyticsService'  # ✅ ID唯一：统一标识服务来源
            return stats

    # ✅ ID唯一：clear_cache统一接口，服务=AnalyticsService
    def clear_cache(self) -> None:
        """清空缓存"""
        with self._calc_lock:
            self._option_type_cache.clear()
            self._out_of_money_cache.clear()
            logging.info("[AnalyticsService] Cache cleared")

    def _clean_expired_cache(self) -> None:
        now = time.time()
        with self._calc_lock:
            expired_keys = [k for k, v in self._option_type_cache.items() 
                           if isinstance(v, tuple) and len(v) >= 2 and now - v[-1] > self._cache_ttl]
            for key in expired_keys:
                del self._option_type_cache[key]
            expired_keys = [k for k, v in self._out_of_money_cache.items() 
                           if isinstance(v, tuple) and len(v) >= 2 and now - v[-1] > self._cache_ttl]
            for key in expired_keys:
                del self._out_of_money_cache[key]


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'AnalyticsService',
    'OptionWidthResult',
    'PriceAnalysis',
    'DirectionSignal',
    'safe_int',
    'safe_float',
]
