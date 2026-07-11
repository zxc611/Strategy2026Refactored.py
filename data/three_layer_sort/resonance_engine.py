# MODULE_ID: M1-056-RE
"""
resonance_engine.py — v2.6/v2.7 共振模块（ResonanceEngine）

依据文档：三层期权五态排序方案_最终落地方案_v2.7_Final_Approved.md
  §16 (v2.6): 共振模块作为可插拔组件，默认关闭
  §17 (v2.7): D_term_structure 合并 D1+D2, D_momentum 当日, D_type_balance 新增,
              方向一致性检查, 冷启动回退, resonance_weight=0.5 等权

核心原则：
  - v2.5 冻结状态不变，共振作为扩展模块，通过配置接口接入
  - 默认关闭（resonance_enabled=False），配置关闭时完全退化为 v2.5
  - v2.7 强制方向一致性：product_score_ts 与 resonance_score 异号 → final_score=0
  - v2.7 冷启动回退：缺失维度权重重新分配，不直接归零

维度定义（v2.7 定稿）：
  D_term_structure: 近月与远月 net_score 同向性（合并 v2.6 的 D1+D2）
  D_momentum:       当日动量（period=1，统一即时周期）
  D_type_balance:   Call/Put 类型共振（利用五态数据自身）
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


# ============================================================================
# 维度信号计算函数
# ============================================================================

def compute_term_structure_direction(month_data_list: List[Dict]) -> int:
    """
    v2.7 D_term_structure: 五态期限结构方向。
    近月与远月方向信号的同向性。

    v2.8.1 fix: 优先使用 D（v2.8 正交分解方向强度），回退 net_score（向后兼容）。
    这样在 scheme_2/3/all 下 D_term_structure 与 product_score 基础一致；
    在 scheme_1 下回退到 net_score，保持 v2.7 行为。

    返回:
        +1: 同向看多（近月>0 且 远月>0）
        -1: 同向看空（近月<0 且 远月<0）
         0: 反向或数据不足
    """
    if not month_data_list or len(month_data_list) < 2:
        return 0
    try:
        # v2.8.1: 优先 D（v2.8 方向强度），回退 net_score（v2.7 兼容）
        near_score = _safe_float(
            month_data_list[0].get('D', month_data_list[0].get('net_score', 0.0))
        )
        far_score = _safe_float(
            month_data_list[-1].get('D', month_data_list[-1].get('net_score', 0.0))
        )
    except (IndexError, TypeError):
        return 0

    near_sign = 1 if near_score > 1e-9 else -1 if near_score < -1e-9 else 0
    far_sign = 1 if far_score > 1e-9 else -1 if far_score < -1e-9 else 0

    if near_sign == far_sign and near_sign != 0:
        return near_sign
    return 0


def compute_type_resonance_direction(data: Dict, threshold: float = 0.50) -> int:
    """
    v2.7 D_type_balance: Call/Put 类型共振方向。

    核心逻辑：
        Call 的 CR+CF 高 AND Put 的 WR+WF 高 → 看多 (+1)
        Call 的 WR+WF 高 AND Put 的 CR+CF 高 → 看空 (-1)
        否则 → 中性 (0)

    Args:
        data: 包含 'call_counts' 和 'put_counts' 的字典
              call_counts/put_counts: {'CR': int, 'CF': int, 'WR': int, 'WF': int, 'Other': int}
        threshold: 占比阈值（默认 0.50）

    返回:
        +1: 看多共振
        -1: 看空共振
         0: 中性
    """
    call = data.get('call_counts', {}) or {}
    put = data.get('put_counts', {}) or {}

    # Call 多头 = CR + CF（认购正确上涨 + 认购正确下跌=看涨方向正确）
    call_long = _safe_float(call.get('CR', 0)) + _safe_float(call.get('CF', 0))
    # Call 空头 = WR + WF
    call_short = _safe_float(call.get('WR', 0)) + _safe_float(call.get('WF', 0))
    # Put 多头 = CF + WF（认沽正确下跌=看跌方向正确，对应标的下跌）
    put_long = _safe_float(put.get('CF', 0)) + _safe_float(put.get('WF', 0))
    # Put 空头 = CR + WR（认沽错误方向，对应标的上涨）
    put_short = _safe_float(put.get('CR', 0)) + _safe_float(put.get('WR', 0))

    call_total = call_long + call_short + _safe_float(call.get('Other', 0))
    put_total = put_long + put_short + _safe_float(put.get('Other', 0))

    if call_total <= 0 or put_total <= 0:
        return 0

    call_long_pct = call_long / call_total
    call_short_pct = call_short / call_total
    put_long_pct = put_long / put_total
    put_short_pct = put_short / put_total

    # 看多共振：Call 多头强 AND Put 空头强
    if call_long_pct > threshold and put_short_pct > threshold:
        return 1
    # 看空共振：Call 空头强 AND Put 多头强
    if call_short_pct > threshold and put_long_pct > threshold:
        return -1
    return 0


def compute_momentum_direction(
    current_price: float,
    prev_price: float,
    period: int = 1,
) -> int:
    """
    v2.7 D_momentum: 当日动量方向（period=1）。

    Args:
        current_price: 当前价格
        prev_price: period 日前价格
        period: 动量周期（默认 1=当日）

    返回:
        +1: 上涨动量
        -1: 下跌动量
         0: 无数据或持平
    """
    if current_price is None or prev_price is None or prev_price <= 0:
        return 0
    current_price = _safe_float(current_price)
    prev_price = _safe_float(prev_price)
    if prev_price <= 0:
        return 0
    returns = (current_price - prev_price) / prev_price
    if returns > 1e-9:
        return 1
    elif returns < -1e-9:
        return -1
    return 0


# ============================================================================
# 冷启动回退：缺失维度权重重新分配
# ============================================================================

def compute_resonance_with_fallback(
    dimensions: Dict[str, int],
    weights: Dict[str, float],
) -> Dict[str, Any]:
    """
    v2.7 §17.1 批判三: 冷启动回退。
    若某维度数据缺失（返回 0），不直接令 resonance_score=0，
    而是将缺失维度的权重重新分配至其余活跃维度。

    Args:
        dimensions: {'D_term_structure': ±1/0, 'D_momentum': ±1/0, 'D_type_balance': ±1/0}
        weights: {'D_term_structure': 1.0, 'D_momentum': 1.0, 'D_type_balance': 1.0}

    Returns:
        {
            'resonance_score': float [-1, 1],
            'direction': int (+1/-1/0),
            'active_count': int,
            'fallback_applied': bool,
            'redistributed_weights': Dict,
        }
    """
    active = {k: v for k, v in dimensions.items() if v != 0}
    inactive = {k: v for k, v in dimensions.items() if v == 0}

    if not active:
        return {
            'resonance_score': 0.0,
            'direction': 0,
            'active_count': 0,
            'fallback_applied': False,
            'redistributed_weights': {},
        }

    # 重新分配权重
    total_active_weight = sum(_safe_float(weights.get(k, 1.0)) for k in active)
    if total_active_weight <= 0:
        total_active_weight = float(len(active))
        redistributed = {k: 1.0 / len(active) for k in active}
    else:
        redistributed = {k: _safe_float(weights.get(k, 1.0)) / total_active_weight for k in active}

    # 加权共振计算
    pos_weight = sum(redistributed[k] for k, v in active.items() if v > 0)
    neg_weight = sum(redistributed[k] for k, v in active.items() if v < 0)
    total = pos_weight + neg_weight

    if total <= 0:
        return {
            'resonance_score': 0.0,
            'direction': 0,
            'active_count': len(active),
            'fallback_applied': len(inactive) > 0,
            'redistributed_weights': redistributed,
        }

    if pos_weight >= neg_weight:
        score = (pos_weight - neg_weight) / total
        direction = 1
    else:
        score = -(neg_weight - pos_weight) / total
        direction = -1

    return {
        'resonance_score': round(score, 6),
        'direction': direction,
        'active_count': len(active),
        'fallback_applied': len(inactive) > 0,
        'redistributed_weights': redistributed,
    }


# ============================================================================
# v2.7 §17.1 批判二: 方向一致性检查
# ============================================================================

def compute_final_score(
    product_score_ts: float,
    resonance_score: float,
    resonance_weight: float,
    config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    v2.7 §17.1: 融合 product_score_ts 与 resonance_score，强制方向一致性检查。

    方向相反 → final_score = 0.0，标记 direction_conflict = True
    方向一致或任一为零 → 按 resonance_weight 加权融合

    Args:
        product_score_ts: 历史分位标准化得分 [-3, 3]
        resonance_score: 共振强度 [-1, 1]
        resonance_weight: 共振权重 [0, 1]
        config: SORTER_CONFIG（可选，用于读取 direction_conflict_zero 开关）

    Returns:
        {
            'final_score': float,
            'direction_conflict': bool,
            'conflict_reason': str,
            'product_score_ts': float,
            'resonance_score': float,
        }
    """
    product_score_ts = _safe_float(product_score_ts)
    resonance_score = _safe_float(resonance_score)
    resonance_weight = max(0.0, min(1.0, _safe_float(resonance_weight, 0.5)))

    # 检查是否启用方向冲突归零
    direction_conflict_zero = True
    if config is not None:
        direction_conflict_zero = config.get('resonance_direction_conflict_zero', True)

    # Step 1: 方向一致性检查
    ts_sign = 0 if abs(product_score_ts) < 1e-6 else (1 if product_score_ts > 0 else -1)
    rs_sign = 0 if abs(resonance_score) < 1e-6 else (1 if resonance_score > 0 else -1)

    # Step 2: 异号处理
    if direction_conflict_zero and ts_sign != 0 and rs_sign != 0 and ts_sign != rs_sign:
        return {
            'final_score': 0.0,
            'direction_conflict': True,
            'conflict_reason': f'product_score_ts={product_score_ts:+.4f} vs resonance_score={resonance_score:+.4f}',
            'product_score_ts': product_score_ts,
            'resonance_score': resonance_score,
        }

    # Step 3: 同号或任一为零：按权重融合
    w = resonance_weight
    final = (1.0 - w) * product_score_ts + w * resonance_score

    return {
        'final_score': round(final, 6),
        'direction_conflict': False,
        'conflict_reason': '',
        'product_score_ts': product_score_ts,
        'resonance_score': resonance_score,
    }


# ============================================================================
# ResonanceEngine 主类
# ============================================================================

class ResonanceEngine:
    """
    v2.6/v2.7 共振引擎（可插拔，默认关闭）。

    使用方式：
        engine = ResonanceEngine(config=SORTER_CONFIG)
        result = engine.compute(dimensions_data)
        # result 包含 resonance_score, resonance_direction, final_score 等

    配置关闭时（resonance_enabled=False）：
        返回 {'resonance_enabled': False, 'resonance_score': None, ...}
        AlphaEngine 退化为 v2.5 行为：final_score = product_score_ts
    """

    # v2.7 维度默认权重（等权）
    DEFAULT_DIMENSION_WEIGHTS = {
        'D_term_structure': 1.0,
        'D_momentum': 1.0,
        'D_type_balance': 1.0,
    }

    def __init__(self, config: Optional[Dict] = None):
        self._config = config or {}
        self._enabled = bool(self._config.get('resonance_enabled', False))
        self._binary_mode = bool(self._config.get('resonance_binary_mode', False))
        self._min_active = int(self._config.get('resonance_min_active', 2))
        self._min_score = _safe_float(self._config.get('resonance_min_score', 0.30))
        self._resonance_weight = _safe_float(self._config.get('resonance_weight', 0.50))
        self._cold_start_fallback = bool(self._config.get('resonance_cold_start_fallback', True))
        self._type_balance_threshold = _safe_float(self._config.get('type_balance_threshold', 0.50))
        self._momentum_period = int(self._config.get('momentum_period', 1))
        self._dimensions_list = self._config.get(
            'resonance_dimensions',
            ['D_term_structure', 'D_momentum', 'D_type_balance'],
        )

    @property
    def enabled(self) -> bool:
        return self._enabled

    def compute(
        self,
        month_data_list: List[Dict],
        call_counts: Optional[Dict] = None,
        put_counts: Optional[Dict] = None,
        current_price: Optional[float] = None,
        prev_price: Optional[float] = None,
        product_score_ts: float = 0.0,
    ) -> Dict[str, Any]:
        """
        计算共振得分。

        Args:
            month_data_list: 月份候选列表（含 net_score）
            call_counts: Call 五态计数 {'CR':, 'CF':, 'WR':, 'WF':, 'Other':}
            put_counts: Put 五态计数
            current_price: 标的当前价格
            prev_price: 标的 period 日前价格
            product_score_ts: AlphaEngine 的历史分位标准化得分

        Returns:
            {
                'resonance_enabled': bool,
                'resonance_score': float or None,
                'resonance_direction': int or None,
                'resonance_active_dims': int or None,
                'resonance_fallback_applied': bool or None,
                'direction_conflict': bool or None,
                'final_score': float,
                'historical_percentile': float or None,
            }
        """
        if not self._enabled:
            return {
                'resonance_enabled': False,
                'resonance_score': None,
                'resonance_direction': None,
                'resonance_active_dims': None,
                'resonance_fallback_applied': None,
                'direction_conflict': None,
                'final_score': product_score_ts,  # v2.5 退化：final = product_score_ts
                'historical_percentile': None,
            }

        # 计算各维度信号
        dimensions = {}
        weights = {}

        for dim_name in self._dimensions_list:
            weights[dim_name] = self.DEFAULT_DIMENSION_WEIGHTS.get(dim_name, 1.0)

            if dim_name == 'D_term_structure':
                dimensions[dim_name] = compute_term_structure_direction(month_data_list)
            elif dim_name == 'D_momentum':
                dimensions[dim_name] = compute_momentum_direction(
                    current_price, prev_price, self._momentum_period,
                )
            elif dim_name == 'D_type_balance':
                type_data = {
                    'call_counts': call_counts or {},
                    'put_counts': put_counts or {},
                }
                dimensions[dim_name] = compute_type_resonance_direction(
                    type_data, self._type_balance_threshold,
                )
            else:
                logger.warning(f"[ResonanceEngine] Unknown dimension: {dim_name}")
                dimensions[dim_name] = 0

        # v2.7 冷启动回退
        if self._cold_start_fallback:
            resonance_result = compute_resonance_with_fallback(dimensions, weights)
        else:
            # v2.6 原始逻辑：active < min_active → score=0
            active = {k: v for k, v in dimensions.items() if v != 0}
            if len(active) < self._min_active:
                resonance_result = {
                    'resonance_score': 0.0,
                    'direction': 0,
                    'active_count': len(active),
                    'fallback_applied': False,
                    'redistributed_weights': {},
                }
            else:
                resonance_result = compute_resonance_with_fallback(dimensions, weights)

        resonance_score = resonance_result['resonance_score']
        resonance_direction = resonance_result['direction']
        active_count = resonance_result['active_count']
        fallback_applied = resonance_result['fallback_applied']

        # 共振强度阈值检查
        if abs(resonance_score) < self._min_score:
            resonance_score = 0.0
            resonance_direction = 0

        # v2.7 二值化模式（可选）
        if self._binary_mode and resonance_score != 0.0:
            resonance_score = float(resonance_direction)

        # v2.7 方向一致性检查 + 融合
        final_result = compute_final_score(
            product_score_ts=product_score_ts,
            resonance_score=resonance_score,
            resonance_weight=self._resonance_weight,
            config=self._config,
        )

        # 计算 historical_percentile（基于 product_score_ts 的 z-score 转换）
        # product_score_ts 已经是 z-score（[-3,3]），转换为百分位
        historical_percentile = None
        try:
            from statistics import NormalDist
            _nd = NormalDist()
            if abs(product_score_ts) <= 3.0:
                historical_percentile = round(_nd.cdf(product_score_ts), 4)
        except Exception:
            pass

        return {
            'resonance_enabled': True,
            'resonance_score': round(resonance_score, 6),
            'resonance_direction': resonance_direction,
            'resonance_active_dims': active_count,
            'resonance_fallback_applied': fallback_applied,
            'direction_conflict': final_result['direction_conflict'],
            'conflict_reason': final_result.get('conflict_reason', ''),
            'final_score': final_result['final_score'],
            'historical_percentile': historical_percentile,
        }
