"""
T型图服务模块 - TTypeService
从 t_type_service.py 拆分 (原 2059 行  width_cache ~1382 行 + t_type_service ~680 行)
只包含 TTypeService 类 + get_t_type_service 单例工厂
"""
from __future__ import annotations

import threading
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ali2026v3_trading.width_cache import WidthStrengthCache, _NoOpDiagnosisProbeManager
from ali2026v3_trading.params_service import get_params_service
from ali2026v3_trading.shared_utils import to_float32
# ✅ 新增：导入 DataService（顶级性能数据服务）- P1 修复：增强健壮性
try:
    from ali2026v3_trading.data_service import DataService
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning(f"[TTypeService] Failed to import DataService: {e}")
    _HAS_DATA_SERVICE = False
    DataService = None

# ============================================================================
# 工具函数
# ============================================================================

def _to_float32(value: Any) -> float:
    """✅ ID唯一：委托shared_utils.to_float32，不再独立实现"""
    from ali2026v3_trading.shared_utils import to_float32
    return to_float32(value)



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
    - 高性能缓存（复用 DataService）
    
    优化 v2.0:
    - 集成 DataService 获取标的价格和 K 线数据
    - 移除重复的数据查询逻辑
    - 利用 DataService 的索引和缓存（5-20x 性能提升）
    """
    
    def __init__(self, data_source: Optional[Any] = None, use_data_service: bool = True):
        """初始化 T 型图服务
        
        Args:
            data_source: 数据源（已弃用，直接使用 DataService）
            use_data_service: 是否使用 DataService（默认 True，推荐）
        """
        # ✅ #89修复：DiagnosisProbeManager导入失败时不阻断TTypeService初始化
        try:
            from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
        except ImportError:
            DiagnosisProbeManager = _NoOpDiagnosisProbeManager

        # 线程锁
        self._data_lock = threading.RLock()

        # ✅ 新增：DataService 集成
        self._data_service: Optional[Any] = None
        if use_data_service and _HAS_DATA_SERVICE:
            try:
                with DiagnosisProbeManager.startup_step("TTypeService.__init__.DataService"):
                    # ✅ 传递渠道唯一：统一使用get_data_service()工厂函数获取单例，禁止直接实例化
                    from ali2026v3_trading.data_service import get_data_service
                    self._data_service = get_data_service()
                logging.info("[TTypeService] DataService integration enabled")
            except Exception as e:
                logging.warning(f"[TTypeService] Failed to initialize DataService: {e}")
                self._data_service = None
        
        # ✅ 新增：WidthStrengthCache 内存缓存（已内嵌）
        try:
            with DiagnosisProbeManager.startup_step("TTypeService.__init__.WidthStrengthCache"):
                self._width_cache = WidthStrengthCache()
            logging.info("[TTypeService] WidthStrengthCache integration enabled (O(1) query)")
            self._preload_complete = False
        except Exception as e:
            logging.warning(f"[TTypeService] Failed to initialize WidthStrengthCache: {e}")
            self._width_cache = None
    
    def initialize(self):
        """显式初始化（预加载已由strategy_lifecycle_mixin统一执行）"""
        pass

    def mark_preload_complete(self):
        """标记预加载完成（由外部统一调用）"""
        self._preload_complete = True
    
    # 使用 WidthStrengthCache 的统一方法（避免重复定义）
    _normalize_option_type = staticmethod(WidthStrengthCache._normalize_option_type)

    # ✅ 方法唯一：委托shared_utils.extract_strike_price，不再独立实现
    @staticmethod
    def _extract_strike_price(instrument_id: str) -> float:
        """
        从期权合约代码提取行权价
        """
        from ali2026v3_trading.shared_utils import extract_strike_price
        result = extract_strike_price(instrument_id)
        return result if result is not None else 0.0
    
    def get_underlying_price_from_service(self, symbol: str) -> Optional[float]:
        """
        从 DataService 获取标的价格（优先使用）- P1 修复：懒加载
        
        Args:
            symbol: 合约代码
            
        Returns:
            Optional[float]: 当前价格，如果 DataService 不可用则返回 None
        """
        ds = self._get_data_service()
        if ds:
            try:
                return ds.get_latest_price(symbol)
            except Exception as e:
                logging.error(f"[TTypeService] Failed to get price from DataService: {e}")
        return None
    
    def _get_data_service(self):
        """P1 修复：获取 DataService 实例（懒加载模式）
        ✅ 传递渠道唯一：统一使用get_data_service()工厂函数，禁止直接实例化
        """
        if self._data_service is None:
            try:
                from ali2026v3_trading.data_service import get_data_service
                self._data_service = get_data_service()
                logging.info("[TTypeService] DataService lazily initialized via get_data_service().")
            except Exception as e:
                logging.error(f"[TTypeService] Failed to initialize DataService: {e}")
                return None
        return self._data_service

    # ======================== WidthStrengthCache 公共 API ========================
    # 便捷入口：提供空值检查和降级处理。调用方也可直接通过 .width_cache 属性访问。

    @property
    def width_cache(self):
        return self._width_cache
    
    def calculate_option_width(
        self,
        instrument_id: str,
        underlying_price: Optional[float] = None,
        strike_price: float = 0.0,
        option_type: str = 'CALL'
    ) -> Dict[str, Any]:
        """✅ P2说明：提供空值检查和降级处理，非简单薄代理"""
        if self._width_cache:
            return self._width_cache.calculate_option_width(instrument_id, underlying_price, strike_price, option_type)
        return {}
    def calculate_option_width_strength(
        self,
        specified_months: Optional[List[str]] = None,
        underlying_future_id: int = None,
        option_type_filter: Optional[str] = None,
        min_width_threshold: float = 4.0
    ) -> Dict[str, Any]:
        if self._width_cache:
            return self._width_cache.calculate_option_width_strength(specified_months, underlying_future_id, option_type_filter, min_width_threshold)
        return {}
    def _empty_width_result(self, product: str, months: List[str]) -> Dict[str, Any]:
        if self._width_cache:
            return self._width_cache._empty_width_result(product, months)
        return {}
    def select_trading_targets(
        self,
        width_strength_results: Dict[str, Dict[str, Any]],
        top_n: int = 5,
        min_width_threshold: float = 4.0
    ) -> List[Dict[str, Any]]:
        if self._width_cache:
            return self._width_cache.select_trading_targets(width_strength_results, top_n, min_width_threshold)
        return []
    def clear_cache(self) -> None:
        """清空所有缓存（已移除独立缓存，保留接口兼容性）"""
        logging.info('[TTypeService] Cache cleared')
    

    # ======================== WidthStrengthCache 公共 API ========================
    
    def on_future_tick(self, future_internal_id: int, price: float, month: str = None):
        """外部行情推送时调用，更新期货价格并自动重算宽度强度
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            price: 期货最新价
            month: 可选，合约月份 (如 '2605')
        """
        if self._width_cache:
            self._width_cache.on_future_tick(future_internal_id, price, month)

    def on_future_instrument_tick(self, future_internal_id: int, price: float):
        """外部行情推送时按期货 internal_id 更新 TType（ID语义）。"""
        if self._width_cache:
            self._width_cache.on_future_instrument_tick(future_internal_id, price)
    
    def on_option_tick(self, instrument_id: str, price: float, volume: float = 0):
        if self._width_cache:
            self._width_cache.on_option_tick(instrument_id, price, volume)
    
    # ✅ 接口唯一：register_future_contract委托给WidthStrengthCache.register_future
    def register_future_contract(self, future_internal_id: int, initial_price: float):
        """注册期货合约到缓存
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            initial_price: 初始价格
        """
        if self._width_cache:
            self._width_cache.register_future(future_internal_id, initial_price)
    
    # ✅ 接口唯一：register_option_contract委托给WidthStrengthCache.register_option
    def register_option_contract(self, instrument_id: str, underlying_product: str,
                                 month: str, strike_price: float, option_type: str,
                                 initial_price: float,
                                 underlying_future_id: Optional[int] = None,
                                 internal_id: Optional[int] = None):
        """注册期权合约到缓存
        
        Args:
            instrument_id: 期权合约代码
            underlying_product: 期权产品代码（兼容展示入参，不参与关系推导）
            month: 月份 (如 '2603')
            strike_price: 行权价
            option_type: 期权类型 ('CALL'/'PUT')
            initial_price: 初始价格
            underlying_future_id: 标的期货 internal_id，主链来源（必须为int，禁止传入合约代码字符串）
            internal_id: 系统内部代理键（优先使用）
        """
        if underlying_future_id is not None and not isinstance(underlying_future_id, int):
            raise TypeError(
                f"register_option_contract: underlying_future_id must be int (internal_id), "
                f"got {type(underlying_future_id).__name__} '{underlying_future_id}' for {instrument_id}. "
                f"Use params_service.get_internal_id(future_instrument_id) to resolve."
            )
        if self._width_cache:
            self._width_cache.register_option(
                instrument_id, underlying_product, month, 
                strike_price, option_type, initial_price,
                underlying_future_id=underlying_future_id,
                internal_id=internal_id,
            )

    def register_option(self, instrument_id: str, underlying_product: str,
                        month: str, strike_price: float, option_type: str,
                        initial_price: float,
                        underlying_future_id: Optional[int] = None,
                        internal_id: Optional[int] = None):
        """兼容旧调用路径，转发到 register_option_contract。"""
        self.register_option_contract(
            instrument_id=instrument_id,
            underlying_product=underlying_product,
            month=month,
            strike_price=strike_price,
            option_type=option_type,
            initial_price=initial_price,
            underlying_future_id=underlying_future_id,
            internal_id=internal_id,
        )
    
    def get_width_strength_from_cache(self, future_internal_id: int, months: List[str], 
                                      option_type: str = None) -> int:
        """从内存缓存获取宽度强度 - O(1) 查询
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            months: 月份列表
            option_type: 可选，'CALL' 或 'PUT'
            
        Returns:
            int: 宽度强度（同步虚值期权计数）
        """
        if self._width_cache:
            return self._width_cache.get_width_strength(future_internal_id, months, option_type)
        return 0
    
    def get_width_cache_stats(self) -> Dict[str, Any]:
        """获取宽度缓存统计信息（含内存占用估算）"""
        if self._width_cache:
            return self._width_cache.get_cache_stats()
        return {
            'enabled': False,
            'total_futures': 0,
            'total_options': 0,
            'query_count': 0
        }
    
    def print_option_status_diagnosis(self, future_internal_id: int = None, top_n: int = 10):
        """打印期权状态诊断报告
        
        Args:
            future_internal_id: 期货合约 internal_id（主键），None 表示所有期货
            top_n: 每个状态下显示前 N 个合约
        """
        if self._width_cache:
            self._width_cache.print_status_diagnosis(future_internal_id, top_n)
    
    def get_all_options(self) -> List[Dict[str, Any]]:
        """获取所有已注册的期权合约列表"""
        if self._width_cache:
            return self._width_cache.get_all_options()
        return []
    
    def select_otm_targets_by_volume(self, product: str = None) -> List[Dict[str, Any]]:
        """代理方法：委托给 WidthStrengthCache.select_otm_targets_by_volume"""
        if self._width_cache:
            return self._width_cache.select_otm_targets_by_volume(product)
        return []

    def compute_decision_score(self, future_internal_id: int = None,
                                order_flow_imbalance: float = None,
                                product: str = None) -> Dict[str, Any]:
        """决策得分协同计算：期权状态强度 × 订单流方向一致性

        Args:
            future_internal_id: 期货合约ID，None=全品种聚合
            order_flow_imbalance: 订单流方向一致性 [-1, 1]，正值=买方主导
                                  None=自动从OrderFlowBridge实时获取
            product: 指定品种查询订单流（如'IF'），None=全品种聚合

        Returns:
            dict: {
                'score': float,           # 综合决策得分 [-1, 1]
                'state_strength': float,  # 期权状态强度 [0, 1]
                'flow_consistency': float,# 订单流一致性 [-1, 1]
                'action': str,            # 建议动作
                'position_scale': float,  # 仓位缩放因子 [0, 1]
                'flow_source': str,       # 'auto'或'manual'
            }
        """
        state_strength = self._calc_option_state_strength(future_internal_id)

        if order_flow_imbalance is not None:
            flow_consistency = max(-1.0, min(1.0, order_flow_imbalance))
            flow_source = 'manual'
        else:
            flow_consistency = self._auto_get_flow_consistency(product)
            flow_source = 'auto'

        w1 = 0.6
        w2 = 0.4
        score = state_strength * w1 + flow_consistency * w2

        if state_strength > 0.7 and flow_consistency > 0.3:
            action = "normal_open"
            position_scale = 1.0
        elif state_strength > 0.7 and flow_consistency < -0.3:
            action = "divergence_warning"
            position_scale = 0.5
        elif 0.4 <= state_strength <= 0.7 and flow_consistency > 0:
            action = "small_open_tight_stop"
            position_scale = 0.3
        else:
            action = "no_open_wait"
            position_scale = 0.0

        return {
            'score': round(score, 4),
            'state_strength': round(state_strength, 4),
            'flow_consistency': round(flow_consistency, 4),
            'action': action,
            'position_scale': position_scale,
            'flow_source': flow_source,
        }

    def _auto_get_flow_consistency(self, product: str = None) -> float:
        """自动从OrderFlowBridge获取订单流方向一致性"""
        try:
            from ali2026v3_trading.order_flow_bridge import get_order_flow_bridge
            bridge = get_order_flow_bridge()
            return bridge.get_flow_consistency(product)
        except Exception as e:
            logging.debug(f"[TTypeService._auto_get_flow_consistency] Error: {e}")
            return 0.0

    def _calc_option_state_strength(self, future_internal_id: int = None) -> float:
        """计算期权状态强度 [0, 1]

        基于 width_cache 的 _status_counts 统计各状态占比，
        correct_trending 占比越高 → 强度越高
        """
        if not self._width_cache:
            return 0.0

        status_counts = getattr(self._width_cache, '_status_counts', {})
        if not status_counts:
            return 0.0

        total = 0
        correct_count = 0
        for fid, months_data in status_counts.items():
            if future_internal_id is not None and fid != future_internal_id:
                continue
            if not isinstance(months_data, dict):
                continue
            for month, types_data in months_data.items():
                if not isinstance(types_data, dict):
                    continue
                for opt_type, counts in types_data.items():
                    if not isinstance(counts, dict):
                        continue
                    for status, count in counts.items():
                        if not isinstance(count, (int, float)):
                            continue
                        total += count
                        if status in ('correct_rise', 'correct_fall'):
                            correct_count += count

        if total == 0:
            return 0.0

        return correct_count / total


# ✅ 传递渠道唯一：TTypeService单例工厂函数，禁止直接实例化
_t_type_service_instance: Optional['TTypeService'] = None
_t_type_service_lock = threading.Lock()


def get_t_type_service() -> 'TTypeService':
    """获取TTypeService单例（线程安全）"""
    global _t_type_service_instance
    if _t_type_service_instance is None:
        with _t_type_service_lock:
            if _t_type_service_instance is None:
                _t_type_service_instance = TTypeService()
    return _t_type_service_instance
