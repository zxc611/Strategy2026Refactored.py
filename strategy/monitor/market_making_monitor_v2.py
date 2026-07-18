# MODULE_ID: S6-MM-MONITOR-V2
"""S6做市商策略实盘模拟监控模块 V2（顶级基金做市商标准 — 2026版）

V2 核心升级（对比 V1）
----------------------
1. 微观价格模型：加权bid/ask不平衡，替代简单mid-price
2. 动态风险厌恶：波动率体制调整γ，替代静态risk_aversion
3. 日内成交量模式：U型曲线订单规模调整
4. 多层毒流分类：OFT + 知情交易者检测，替代简单VPIN
5. 库存相关性对冲：跨品种相关性库存风险对冲
6. GLFT多资产扩展：含alpha信号的reservation price
7. PnL归因分解：价差捕获 vs 库存重估 vs 费率净额
8. 订单簿不平衡分析：L2数据驱动的微观价格
9. 最优订单规模：波动率+库存+成交量三维联合优化
10. 报价取消概率模型：队列位置+市场冲击联合估计

顶级基金对标
------------
- Citadel Securities: 微观价格模型 + 多层毒流检测
- Jane Street: GLFT多资产扩展 + PnL归因
- Jump Trading: 日内成交量模式 + 最优订单规模
- Virtu Financial: 库存相关性对冲 + 报价取消概率
- IMC: 动态风险厌恶 + 订单簿不平衡

约束
----
- 不参与资金分配，不调用 OrderService.send_order。
- 仅生成模拟信号 + JSON 快照，供其他策略参考方向。
"""
from __future__ import annotations

import json
import logging
import math
import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

try:
    from infra.serialization_utils import json_dumps as _json_dumps
except Exception:
    _json_dumps = None

try:
    from infra._helpers import get_logger as _get_logger
except Exception:
    def _get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
        _lg = logging.getLogger(name)
        if level is not None:
            _lg.setLevel(level)
        return _lg

try:
    from infra.shared_utils import CHINA_TZ as _CHINA_TZ
except Exception:
    from datetime import timezone, timedelta as _td
    _CHINA_TZ = timezone(_td(hours=8))

__all__ = ['MarketMakingMonitorV2', 'MarketMakingSignalV2', 'get_market_making_monitor_v2']

logger = _get_logger(__name__)

# ── 默认参数 ────────────────────────────────────────────────────────────────
_DEFAULT_MAX_INVENTORY = 10
_DEFAULT_MAX_INVENTORY_NOTIONAL = 0.0
_DEFAULT_BASE_SPREAD_BPS = 8.0
_DEFAULT_VOL_LOOKBACK = 60
_DEFAULT_VOL_TARGET_BPS = 12.0
_DEFAULT_COMPETITION_FACTOR = 1.0
_DEFAULT_INVENTORY_FACTOR_BPS = 24.0
_DEFAULT_ADVERSE_WINDOW = 20
_DEFAULT_ADVERSE_THRESHOLD_BPS = 6.0
_DEFAULT_SNAPSHOT_DIR = "logs/market_making_monitor_v2"
_DEFAULT_PRICE_EPS = 1e-9
_MIN_VOL_SAMPLE = 5
_DEFAULT_SIGNAL_INTERVAL_TICKS = 10
_DEFAULT_SNAPSHOT_INTERVAL_SEC = 5.0
_DEFAULT_VOL_SCALE = 1.0

# A-S 模型参数
_DEFAULT_RISK_AVERSION = 0.1
_DEFAULT_TIME_HORIZON_SEC = 300.0
_DEFAULT_ORDER_ARRIVAL_KAPPA = 1.5
_DEFAULT_QUOTE_JITTER_BPS = 0.5
_DEFAULT_TARGET_INVENTORY = 0.0
_DEFAULT_UNWIND_THRESHOLD = 0.8

# 费率与风控
_DEFAULT_MAKER_FEE_BPS = 0.2
_DEFAULT_TAKER_FEE_BPS = 0.5
_DEFAULT_REBATE_BPS = 0.1
_DEFAULT_MAX_DAILY_LOSS_BPS = 50.0
_DEFAULT_MIN_QUOTE_REFRESH_SEC = 0.05

# VPIN
_DEFAULT_VPIN_WINDOW = 60
_DEFAULT_VPIN_BUCKET_SEC = 5.0

# V2: 微观价格模型
_DEFAULT_IMBALANCE_LOOKBACK = 20
_DEFAULT_IMBALANCE_WEIGHT = 0.3

# V2: 动态Gamma
_DEFAULT_GAMMA_VOL_SCALE = 1.5
_DEFAULT_GAMMA_INV_SCALE = 0.5

# V2: OFT毒流
_DEFAULT_OFT_WINDOW = 50
_DEFAULT_OFT_THRESHOLD = 0.6

# V2: 相关性对冲
_DEFAULT_CORR_LOOKBACK = 100
_DEFAULT_MAX_CORR_HEDGE_RATIO = 0.5

# V2: 最优订单规模
_DEFAULT_MIN_ORDER_SIZE = 1.0
_DEFAULT_MAX_ORDER_SIZE = 100.0
_DEFAULT_SIZE_VOL_SCALE = 0.5


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class MarketMakingSignalV2:
    """V2做市商模拟信号快照（顶级基金标准）。"""

    instrument_id: str = ""
    bid_price: float = 0.0
    ask_price: float = 0.0
    theo_price: float = 0.0
    mid_price: float = 0.0
    spread: float = 0.0
    spread_bps: float = 0.0
    inventory: float = 0.0
    inventory_skew: float = 0.0
    adverse_selection_flag: bool = False
    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp: float = 0.0
    half_spread: float = 0.0
    volatility_bps: float = 0.0
    competition_factor: float = 1.0
    preferred_side: str = "neutral"

    # V1保留字段
    reservation_price: float = 0.0
    bid_half_spread: float = 0.0
    ask_half_spread: float = 0.0
    vpin: float = 0.0
    optimal_spread_as: float = 0.0
    target_inventory_delta: float = 0.0
    sigma: float = 0.0
    regime: str = "normal"
    expected_hit_prob: float = 0.0
    net_capture_bps: float = 0.0
    quote_refresh_due: bool = True

    # V2新增字段
    micro_price: float = 0.0               # 微观价格（加权bid/ask）
    dynamic_gamma: float = 0.1             # 动态风险厌恶
    volume_profile_multiplier: float = 1.0  # 日内成交量模式乘子
    order_flow_toxicity: float = 0.0       # OFT毒流指标
    informed_trader_prob: float = 0.0      # 知情交易者概率
    inventory_correlation_hedge: float = 0.0  # 相关性对冲建议
    alpha_adjustment: float = 0.0          # alpha信号调整
    pnl_attribution: Dict[str, float] = field(default_factory=dict)  # PnL归因
    order_book_imbalance: float = 0.0      # 订单簿不平衡
    bid_ask_bounce_prob: float = 0.0       # 买卖反弹概率
    cancellation_probability: float = 0.0  # 报价取消概率
    optimal_order_size: float = 0.0        # 最优订单规模
    intraday_volume_percentile: float = 0.5  # 日内成交量百分位
    quote_skew_ratio: float = 0.0          # bid/ask偏斜比
    inventory_risk_bps: float = 0.0        # 库存风险（bps）
    market_regime_v2: str = "normal"       # V2市场体制

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# 微观价格模型
# ============================================================================

class _MicroPriceModel:
    """基于订单簿不平衡的微观价格模型。

    对标: Citadel Securities micro-price estimation。
    微价 = weighted_mid + imbalance * adjustment
    其中 weighted_mid = (bid*ask_vol + ask*bid_vol) / (bid_vol + ask_vol)
    """

    def __init__(self, imbalance_lookback: int = _DEFAULT_IMBALANCE_LOOKBACK,
                 imbalance_weight: float = _DEFAULT_IMBALANCE_WEIGHT) -> None:
        self.lookback = imbalance_lookback
        self.weight = imbalance_weight
        self._imbalance_history: Deque[float] = deque(maxlen=imbalance_lookback)
        self._spread_history: Deque[float] = deque(maxlen=imbalance_lookback)

    def compute(self, bid: float, ask: float, bid_vol: float = 0.0,
                ask_vol: float = 0.0, last_price: float = 0.0) -> Tuple[float, float]:
        """计算微观价格和订单簿不平衡。

        Returns:
            (micro_price, order_book_imbalance)
        """
        if bid <= 0 or ask <= 0 or ask <= bid:
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last_price
            return mid, 0.0

        mid = (bid + ask) / 2.0

        # 加权中间价
        total_vol = bid_vol + ask_vol
        if total_vol > 0:
            weighted_mid = (bid * ask_vol + ask * bid_vol) / total_vol
        else:
            weighted_mid = mid

        # 订单簿不平衡: [-1, 1]，正=买方压力
        if total_vol > 0:
            imb = (bid_vol - ask_vol) / total_vol
        else:
            # 基于价格的简化不平衡
            if last_price > 0:
                imb = (last_price - mid) / (ask - bid) * 2.0
                imb = max(-1.0, min(1.0, imb))
            else:
                imb = 0.0

        self._imbalance_history.append(imb)

        # 指数加权平均不平衡
        ewma_imb = self._compute_ewma_imbalance()

        # 微价 = 加权中间价 + 不平衡调整
        half_spread = (ask - bid) / 2.0
        micro_price = weighted_mid + ewma_imb * half_spread * self.weight

        return micro_price, ewma_imb

    def _compute_ewma_imbalance(self) -> float:
        """指数加权移动平均不平衡。"""
        if not self._imbalance_history:
            return 0.0
        alpha = 2.0 / (len(self._imbalance_history) + 1)
        ewma = self._imbalance_history[0]
        for v in list(self._imbalance_history)[1:]:
            ewma = alpha * v + (1 - alpha) * ewma
        return ewma

    def compute_imbalance(self) -> float:
        return self._compute_ewma_imbalance()


# ============================================================================
# 动态风险厌恶估算器
# ============================================================================

class _DynamicGammaEstimator:
    """波动率+库存驱动的动态风险厌恶。

    对标: IMC dynamic risk aversion。
    γ_dynamic = γ_base * (1 + vol_scale * (σ/σ_target - 1)) * (1 + inv_scale * |q/q_max|)
    """

    def __init__(self, base_gamma: float = _DEFAULT_RISK_AVERSION,
                 vol_scale: float = _DEFAULT_GAMMA_VOL_SCALE,
                 inv_scale: float = _DEFAULT_GAMMA_INV_SCALE) -> None:
        self.base_gamma = base_gamma
        self.vol_scale = vol_scale
        self.inv_scale = inv_scale

    def compute(self, current_vol_bps: float, target_vol_bps: float,
                inventory: float, max_inventory: float) -> float:
        """计算动态gamma。

        Args:
            current_vol_bps: 当前波动率(bps)
            target_vol_bps: 目标波动率(bps)
            inventory: 当前库存
            max_inventory: 最大库存

        Returns:
            动态gamma值
        """
        # 波动率调整
        if target_vol_bps > 0:
            vol_ratio = current_vol_bps / target_vol_bps
            vol_adj = 1.0 + self.vol_scale * (vol_ratio - 1.0)
        else:
            vol_adj = 1.0

        # 库存调整
        if max_inventory > 0:
            inv_ratio = abs(inventory) / max_inventory
            inv_adj = 1.0 + self.inv_scale * inv_ratio
        else:
            inv_adj = 1.0

        gamma = self.base_gamma * vol_adj * inv_adj
        return max(0.01, min(1.0, gamma))


# ============================================================================
# 日内成交量模式
# ============================================================================

class _IntradayVolumeProfile:
    """U型日内成交量模式。

    对标: Jump Trading intraday volume profile。
    中国期货市场特点:
    - 开盘(9:00-9:30): 高成交量 (1.5x)
    - 上午中段(9:30-11:00): 正常 (1.0x)
    - 上午尾段(11:00-11:30): 略高 (1.1x)
    - 下午开盘(13:00-13:30): 高 (1.3x)
    - 下午中段(13:30-14:30): 正常 (1.0x)
    - 收盘(14:30-15:00): 最高 (1.6x)
    - 夜盘(21:00-23:00): 分散
    """

    # 30分钟时段乘子 (中国时间)
    _PERIOD_MULTIPLIERS = {
        # 上午
        (9, 0): 1.5, (9, 30): 1.3, (10, 0): 1.0, (10, 30): 1.0,
        (11, 0): 1.1,
        # 下午
        (13, 0): 1.3, (13, 30): 1.0, (14, 0): 1.0, (14, 30): 1.6,
        # 夜盘
        (21, 0): 1.2, (21, 30): 1.0, (22, 0): 1.0, (22, 30): 0.8,
        (23, 0): 0.6,
    }

    def get_multiplier(self, hour: int, minute: int) -> float:
        """获取当前时段成交量乘子。"""
        # 对齐到30分钟区间
        aligned_minute = (minute // 30) * 30
        key = (hour, aligned_minute)
        return self._PERIOD_MULTIPLIERS.get(key, 1.0)

    def get_percentile(self, hour: int, minute: int) -> float:
        """获取当前时段成交量百分位。"""
        mult = self.get_multiplier(hour, minute)
        return min(1.0, max(0.0, mult / 2.0))


# ============================================================================
# 多层毒流分类器
# ============================================================================

class _ToxicFlowClassifier:
    """多层毒流分类器：OFT + 知情交易者检测。

    对标: Citadel Securities multi-layer toxic flow detection。

    层1 (OFT - Order Flow Toxicity):
        VPIN的改进版，基于订单流不平衡的持续性。
        OFT = |累计买方量 - 累计卖方量| / 总成交量
        高OFT → 方向性毒流

    层2 (知情交易者概率 - PIN):
        简化版Easley et al. (1996) PIN模型。
        基于买卖单到达率的不对称性。
    """

    def __init__(self, oft_window: int = _DEFAULT_OFT_WINDOW,
                 oft_threshold: float = _DEFAULT_OFT_THRESHOLD) -> None:
        self.oft_window = oft_window
        self.oft_threshold = oft_threshold
        self._buy_volume: Deque[float] = deque(maxlen=oft_window)
        self._sell_volume: Deque[float] = deque(maxlen=oft_window)
        self._trade_directions: Deque[int] = deque(maxlen=oft_window)
        self._arrival_times: Deque[float] = deque(maxlen=oft_window)

    def update(self, direction: int, volume: float, timestamp: float) -> None:
        """更新毒流分类器。"""
        self._trade_directions.append(direction)
        self._arrival_times.append(timestamp)
        if direction > 0:
            self._buy_volume.append(volume)
            self._sell_volume.append(0.0)
        elif direction < 0:
            self._buy_volume.append(0.0)
            self._sell_volume.append(volume)

    def compute_oft(self) -> float:
        """计算OFT (Order Flow Toxicity)。"""
        total_buy = sum(self._buy_volume)
        total_sell = sum(self._sell_volume)
        total = total_buy + total_sell
        if total <= 0:
            return 0.0
        return abs(total_buy - total_sell) / total

    def compute_informed_trader_prob(self) -> float:
        """简化PIN模型：知情交易者概率。

        基于买卖单到达率的不对称性。
        PIN ≈ |buy_rate - sell_rate| / (buy_rate + sell_rate)
        """
        n = len(self._arrival_times)
        if n < 10:
            return 0.0

        buys = sum(1 for d in self._trade_directions if d > 0)
        sells = sum(1 for d in self._trade_directions if d < 0)

        if buys + sells == 0:
            return 0.0

        # 时间跨度
        time_span = self._arrival_times[-1] - self._arrival_times[0]
        if time_span <= 0:
            time_span = 1.0

        buy_rate = buys / time_span
        sell_rate = sells / time_span
        total_rate = buy_rate + sell_rate

        if total_rate <= 0:
            return 0.0

        pin = abs(buy_rate - sell_rate) / total_rate
        return min(1.0, max(0.0, pin))

    def is_toxic(self) -> bool:
        """综合毒性判断。"""
        oft = self.compute_oft()
        pin = self.compute_informed_trader_prob()
        return oft > self.oft_threshold or pin > 0.7


# ============================================================================
# 库存相关性对冲器
# ============================================================================

class _CorrelationHedger:
    """跨品种相关性库存风险对冲。

    对标: Virtu Financial correlation-based inventory hedging。

    原理:
    - 计算库存品种与对冲品种的滚动相关性
    - 当相关性>阈值时，建议在对冲品种开反向仓位
    - 对冲比率 = corr * min(库存风险, max_hedge_ratio)
    """

    def __init__(self, corr_lookback: int = _DEFAULT_CORR_LOOKBACK,
                 max_hedge_ratio: float = _DEFAULT_MAX_CORR_HEDGE_RATIO) -> None:
        self.corr_lookback = corr_lookback
        self.max_hedge_ratio = max_hedge_ratio
        self._price_history: Dict[str, Deque[float]] = {}
        self._correlations: Dict[str, float] = {}

    def register_instrument(self, instrument_id: str) -> None:
        if instrument_id not in self._price_history:
            self._price_history[instrument_id] = deque(maxlen=self.corr_lookback)

    def update_price(self, instrument_id: str, price: float) -> None:
        if instrument_id not in self._price_history:
            self.register_instrument(instrument_id)
        self._price_history[instrument_id].append(price)

    def compute_hedge_ratio(self, primary_id: str, hedge_id: str) -> float:
        """计算对冲比率。"""
        prices_primary = list(self._price_history.get(primary_id, []))
        prices_hedge = list(self._price_history.get(hedge_id, []))
        n = min(len(prices_primary), len(prices_hedge))
        if n < 30:
            return 0.0

        # 计算收益率
        rets_p = [(prices_primary[i] - prices_primary[i - 1]) / prices_primary[i - 1]
                  for i in range(1, n) if prices_primary[i - 1] > 0]
        rets_h = [(prices_hedge[i] - prices_hedge[i - 1]) / prices_hedge[i - 1]
                  for i in range(1, n) if prices_hedge[i - 1] > 0]
        m = min(len(rets_p), len(rets_h))
        if m < 10:
            return 0.0

        # Pearson相关系数
        mean_p = sum(rets_p[:m]) / m
        mean_h = sum(rets_h[:m]) / m
        num = sum((rets_p[i] - mean_p) * (rets_h[i] - mean_h) for i in range(m))
        den_p = sum((r - mean_p) ** 2 for r in rets_p[:m])
        den_h = sum((r - mean_h) ** 2 for r in rets_h[:m])
        den = math.sqrt(den_p * den_h)
        if den <= 0:
            return 0.0

        corr = num / den
        self._correlations[f"{primary_id}:{hedge_id}"] = corr

        # FIX-20260713-S6-V2: 正相关 → 反向对冲；负相关 → 同向对冲
        hedge_ratio = -corr * self.max_hedge_ratio
        return max(-self.max_hedge_ratio, min(self.max_hedge_ratio, hedge_ratio))

    def get_correlation(self, primary_id: str, hedge_id: str) -> float:
        return self._correlations.get(f"{primary_id}:{hedge_id}", 0.0)


# ============================================================================
# PnL归因分解
# ============================================================================

class _PnLAttribution:
    """PnL归因分解器。

    对标: Jane Street PnL attribution。

    分解:
    - spread_capture: 买卖价差捕获
    - inventory_revaluation: 库存重估（mark-to-market）
    - fee_net: 手续费净额（手续费 - 返佣）
    - adverse_cost: 毒单损失
    - total: 总PnL

    公式:
    total = spread_capture + inventory_revaluation + fee_net + adverse_cost
    """

    def __init__(self) -> None:
        self.spread_capture: float = 0.0
        self.inventory_revaluation: float = 0.0
        self.fee_net: float = 0.0
        self.adverse_cost: float = 0.0
        self._last_mark_price: float = 0.0
        self._trade_count: int = 0

    def record_trade(self, direction: int, fill_price: float, mid_price: float,
                     half_spread: float, maker_fee_bps: float, rebate_bps: float,
                     is_adverse: bool = False) -> None:
        """记录一笔交易。"""
        # 价差捕获: 方向 * half_spread (maker收入)
        spread_pnl = half_spread
        self.spread_capture += spread_pnl
        self._trade_count += 1

        # 费率: -(maker_fee) + rebate
        fee_pnl = (-maker_fee_bps + rebate_bps) * mid_price / 10000.0
        self.fee_net += fee_pnl

        # 毒单损失
        if is_adverse:
            adverse = -abs(half_spread) * 2.0  # 毒单损失约2倍价差
            self.adverse_cost += adverse

    def mark_to_market(self, inventory: float, current_mid: float) -> None:
        """库存重估。"""
        if self._last_mark_price > 0 and inventory != 0:
            price_change = current_mid - self._last_mark_price
            self.inventory_revaluation += inventory * price_change
        self._last_mark_price = current_mid

    def get_attribution(self) -> Dict[str, float]:
        total = self.spread_capture + self.inventory_revaluation + self.fee_net + self.adverse_cost
        return {
            'spread_capture': round(self.spread_capture, 6),
            'inventory_revaluation': round(self.inventory_revaluation, 6),
            'fee_net': round(self.fee_net, 6),
            'adverse_cost': round(self.adverse_cost, 6),
            'total': round(total, 6),
            'trade_count': self._trade_count,
        }


# ============================================================================
# 最优订单规模优化器
# ============================================================================

class _OrderSizeOptimizer:
    """GLFT最优订单规模优化器。

    对标: Jump Trading optimal order size。

    三维联合优化:
    - 波动率维度: 高波动 → 小订单
    - 库存维度: 高库存 → 偏斜订单
    - 成交量维度: 高成交量 → 大订单

    optimal_size = base_size * vol_adj * inv_adj * vol_adj
    """

    def __init__(self, min_size: float = _DEFAULT_MIN_ORDER_SIZE,
                 max_size: float = _DEFAULT_MAX_ORDER_SIZE,
                 vol_scale: float = _DEFAULT_SIZE_VOL_SCALE) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self.vol_scale = vol_scale

    def compute(self, base_size: float, volatility_bps: float, target_vol_bps: float,
                inventory_skew: float, volume_multiplier: float,
                toxicity: float) -> Tuple[float, float, float]:
        """计算最优bid/ask订单规模。

        Returns:
            (optimal_size, bid_size, ask_size)
        """
        # 波动率调整: 高波动减仓
        if target_vol_bps > 0:
            vol_ratio = volatility_bps / target_vol_bps
            vol_adj = 1.0 / (1.0 + self.vol_scale * (vol_ratio - 1.0))
        else:
            vol_adj = 1.0

        # 毒流调整: 高毒流减仓
        toxicity_adj = 1.0 - toxicity * 0.5

        # 基础规模
        optimal = base_size * vol_adj * volume_multiplier * toxicity_adj
        optimal = max(self.min_size, min(self.max_size, optimal))

        # 库存偏斜: 多库存→小bid大ask, 空库存→大bid小ask
        bid_size = optimal * (1.0 - inventory_skew * 0.3)
        ask_size = optimal * (1.0 + inventory_skew * 0.3)
        bid_size = max(self.min_size, min(self.max_size, bid_size))
        ask_size = max(self.min_size, min(self.max_size, ask_size))

        return optimal, bid_size, ask_size


# ============================================================================
# 买卖反弹检测
# ============================================================================

class _BidAskBounceDetector:
    """买卖反弹检测器。

    检测价格在bid/ask之间快速反弹的模式。
    高反弹概率 → 不适合做市（假突破风险高）。
    """

    def __init__(self, lookback: int = 20) -> None:
        self.lookback = lookback
        self._bounces: Deque[int] = deque(maxlen=lookback)
        self._crosses: Deque[int] = deque(maxlen=lookback)

    def update(self, last_price: float, bid: float, ask: float,
               prev_last_price: float = 0.0) -> None:
        """更新反弹检测。"""
        if prev_last_price <= 0 or bid <= 0 or ask <= 0:
            return

        # 检测是否穿越bid/ask
        crossed_bid = prev_last_price > bid and last_price <= bid
        crossed_ask = prev_last_price < ask and last_price >= ask
        bounced_bid = prev_last_price <= bid and last_price > bid
        bounced_ask = prev_last_price >= ask and last_price < ask

        if crossed_bid or crossed_ask:
            self._crosses.append(1)
        else:
            self._crosses.append(0)

        if bounced_bid or bounced_ask:
            self._bounces.append(1)
        else:
            self._bounces.append(0)

    def compute_bounce_prob(self) -> float:
        """计算反弹概率。"""
        total_bounces = sum(self._bounces)
        total_crosses = sum(self._crosses)
        if total_crosses == 0:
            return 0.0
        return total_bounces / total_crosses


# ============================================================================
# MarketMakingMonitorV2
# ============================================================================

class MarketMakingMonitorV2:
    """S6做市商策略实盘模拟监控模块 V2（顶级基金做市商标准）。"""

    STRATEGY_ID = 's6_market_making_v2'
    STRATEGY_TYPE = 's6_market_making'
    PARTICIPATES_CAPITAL_ALLOCATION: bool = False
    CAPITAL_ALLOCATION: float = 0.0
    SENDS_REAL_ORDERS: bool = False
    SNAPSHOT_SUBDIR = _DEFAULT_SNAPSHOT_DIR

    def __init__(
        self,
        instrument_id: str = "",
        *,
        max_inventory: float = _DEFAULT_MAX_INVENTORY,
        max_inventory_notional: float = _DEFAULT_MAX_INVENTORY_NOTIONAL,
        base_spread_bps: float = _DEFAULT_BASE_SPREAD_BPS,
        vol_lookback: int = _DEFAULT_VOL_LOOKBACK,
        vol_target_bps: float = _DEFAULT_VOL_TARGET_BPS,
        competition_factor: float = _DEFAULT_COMPETITION_FACTOR,
        inventory_factor_bps: float = _DEFAULT_INVENTORY_FACTOR_BPS,
        adverse_window: int = _DEFAULT_ADVERSE_WINDOW,
        adverse_threshold_bps: float = _DEFAULT_ADVERSE_THRESHOLD_BPS,
        snapshot_dir: str = _DEFAULT_SNAPSHOT_DIR,
        price_tick: float = 0.0,
        signal_interval_ticks: int = _DEFAULT_SIGNAL_INTERVAL_TICKS,
        snapshot_interval_sec: float = _DEFAULT_SNAPSHOT_INTERVAL_SEC,
        vol_scale: float = _DEFAULT_VOL_SCALE,
        risk_aversion: float = _DEFAULT_RISK_AVERSION,
        time_horizon_sec: float = _DEFAULT_TIME_HORIZON_SEC,
        order_arrival_kappa: float = _DEFAULT_ORDER_ARRIVAL_KAPPA,
        quote_jitter_bps: float = _DEFAULT_QUOTE_JITTER_BPS,
        target_inventory: float = _DEFAULT_TARGET_INVENTORY,
        unwind_threshold: float = _DEFAULT_UNWIND_THRESHOLD,
        vpin_window: int = _DEFAULT_VPIN_WINDOW,
        maker_fee_bps: float = _DEFAULT_MAKER_FEE_BPS,
        taker_fee_bps: float = _DEFAULT_TAKER_FEE_BPS,
        rebate_bps: float = _DEFAULT_REBATE_BPS,
        max_daily_loss_bps: float = _DEFAULT_MAX_DAILY_LOSS_BPS,
        min_quote_refresh_sec: float = _DEFAULT_MIN_QUOTE_REFRESH_SEC,
        # V2新增参数
        imbalance_lookback: int = _DEFAULT_IMBALANCE_LOOKBACK,
        imbalance_weight: float = _DEFAULT_IMBALANCE_WEIGHT,
        gamma_vol_scale: float = _DEFAULT_GAMMA_VOL_SCALE,
        gamma_inv_scale: float = _DEFAULT_GAMMA_INV_SCALE,
        oft_window: int = _DEFAULT_OFT_WINDOW,
        oft_threshold: float = _DEFAULT_OFT_THRESHOLD,
        corr_lookback: int = _DEFAULT_CORR_LOOKBACK,
        max_corr_hedge_ratio: float = _DEFAULT_MAX_CORR_HEDGE_RATIO,
        min_order_size: float = _DEFAULT_MIN_ORDER_SIZE,
        max_order_size: float = _DEFAULT_MAX_ORDER_SIZE,
        size_vol_scale: float = _DEFAULT_SIZE_VOL_SCALE,
    ) -> None:
        # 参数校验
        if max_inventory <= 0 and max_inventory_notional <= 0:
            raise ValueError("max_inventory 与 max_inventory_notional 不能同时 <=0")
        if vol_lookback < _MIN_VOL_SAMPLE:
            raise ValueError(f"vol_lookback 必须 >= {_MIN_VOL_SAMPLE}")
        if base_spread_bps < 0:
            raise ValueError("base_spread_bps 不能为负")
        if competition_factor <= 0:
            raise ValueError("competition_factor 必须 >0")
        if signal_interval_ticks < 1:
            raise ValueError("signal_interval_ticks 必须 >=1")

        self.instrument_id: str = instrument_id
        self.max_inventory: float = float(max_inventory)
        self.max_inventory_notional: float = float(max_inventory_notional)
        self.base_spread_bps: float = float(base_spread_bps)
        self.vol_lookback: int = int(vol_lookback)
        self.vol_target_bps: float = float(vol_target_bps)
        self.competition_factor: float = float(competition_factor)
        self.inventory_factor_bps: float = float(inventory_factor_bps)
        self.adverse_window: int = int(adverse_window)
        self.adverse_threshold_bps: float = float(adverse_threshold_bps)
        self.snapshot_dir: str = snapshot_dir
        self.price_tick: float = float(price_tick)
        self.signal_interval_ticks: int = int(signal_interval_ticks)
        self.snapshot_interval_sec: float = float(snapshot_interval_sec)
        self.vol_scale: float = float(vol_scale)

        # A-S 参数
        self.risk_aversion: float = float(risk_aversion)
        self.time_horizon_sec: float = float(time_horizon_sec)
        self.order_arrival_kappa: float = float(order_arrival_kappa)
        self.quote_jitter_bps: float = float(quote_jitter_bps)
        self.target_inventory: float = float(target_inventory)
        self.unwind_threshold: float = float(unwind_threshold)
        self.vpin_window: int = int(vpin_window)

        # 费率
        self.maker_fee_bps: float = float(maker_fee_bps)
        self.taker_fee_bps: float = float(taker_fee_bps)
        self.rebate_bps: float = float(rebate_bps)
        self.max_daily_loss_bps: float = float(max_daily_loss_bps)
        self.min_quote_refresh_sec: float = float(min_quote_refresh_sec)

        # 运行时状态
        self._lock = threading.RLock()
        self._inventory: float = 0.0
        self._last_mid_price: float = 0.0
        self._last_bid: float = 0.0
        self._last_ask: float = 0.0
        self._last_last_price: float = 0.0
        self._last_bid_vol: float = 0.0
        self._last_ask_vol: float = 0.0

        self._price_history: Deque[float] = deque(maxlen=self.vol_lookback)
        self._fill_events: Deque[Tuple[int, float, float]] = deque(maxlen=self.adverse_window)
        self._vpin_buckets: Deque[Tuple[float, float, float]] = deque(maxlen=self.vpin_window)
        self._vpin_current_bucket_ts: float = 0.0
        self._vpin_current_buy: float = 0.0
        self._vpin_current_sell: float = 0.0

        # V2: 新增组件
        self._micro_price_model = _MicroPriceModel(imbalance_lookback, imbalance_weight)
        self._gamma_estimator = _DynamicGammaEstimator(risk_aversion, gamma_vol_scale, gamma_inv_scale)
        self._volume_profile = _IntradayVolumeProfile()
        self._toxic_classifier = _ToxicFlowClassifier(oft_window, oft_threshold)
        self._correlation_hedger = _CorrelationHedger(corr_lookback, max_corr_hedge_ratio)
        self._pnl_attribution = _PnLAttribution()
        self._order_sizer = _OrderSizeOptimizer(min_order_size, max_order_size, size_vol_scale)
        self._bounce_detector = _BidAskBounceDetector()

        # 统计
        self._quote_count: int = 0
        self._quote_hit_count: int = 0
        self._spread_captured_bps: float = 0.0
        self._spread_offered_bps: float = 0.0
        self._adverse_count: int = 0
        self._inventory_peak: float = 0.0
        self._inventory_turnover_sum: float = 0.0
        self._realized_pnl: float = 0.0
        self._pnl_samples: Deque[float] = deque(maxlen=128)
        self._daily_loss_bps: float = 0.0

        self._last_signal: Optional[MarketMakingSignalV2] = None
        self._snapshot_count: int = 0
        self._tick_count: int = 0
        self._last_signal_time: float = 0.0
        self._last_quote_time: float = 0.0
        self._regime: str = "normal"

        # V2: 诊断统计
        self._diag_stats: Dict[str, int] = {
            'micro_price_updates': 0,
            'gamma_adjustments': 0,
            'volume_profile_queries': 0,
            'toxicity_classifications': 0,
            'correlation_hedges': 0,
            'pnl_attributions': 0,
            'order_size_optimizations': 0,
            'bounce_detections': 0,
            'signals_generated': 0,
            'snapshots_saved': 0,
            'quote_hits_simulated': 0,
            'toxic_events': 0,
        }

        logger.info(
            "[S6-MMM-V2] 初始化完成 inst=%s max_inv=%.2f base_spread=%.1fbps "
            "γ=%.3f κ=%.2f | V2微价模型+动态Gamma+OFT+PnL归因+最优规模",
            self.instrument_id, self.max_inventory, self.base_spread_bps,
            self.risk_aversion, self.order_arrival_kappa,
        )

    # ========================================================================
    # 1. 微观价格计算
    # ========================================================================

    def compute_micro_price(self) -> float:
        """V2: 计算微观价格（替代简单mid-price）。"""
        micro_price, imb = self._micro_price_model.compute(
            self._last_bid, self._last_ask,
            self._last_bid_vol, self._last_ask_vol,
            self._last_last_price)
        self._diag_stats['micro_price_updates'] += 1
        return micro_price

    def get_order_book_imbalance(self) -> float:
        return self._micro_price_model.compute_imbalance()

    # ========================================================================
    # 2. A-S报价（含V2增强）
    # ========================================================================

    def compute_reservation_price(self, mid_price: float,
                                   sigma: Optional[float] = None,
                                   dynamic_gamma: Optional[float] = None,
                                   alpha_signal: float = 0.0) -> float:
        """V2: A-S reservation price with dynamic gamma + alpha signal。

        r = s - q * γ_dynamic * σ² * (T-t) + alpha_adjustment
        """
        if mid_price <= 0:
            return 0.0
        if sigma is None:
            vol_bps = self._estimate_volatility_bps()
            sigma = vol_bps * mid_price / 10000.0
        if sigma <= 0:
            return self.compute_theoretical_price(mid_price)
        if dynamic_gamma is None:
            dynamic_gamma = self.risk_aversion

        with self._lock:
            q = self._inventory - self.target_inventory
            T_minus_t = self.time_horizon_sec
            reservation = mid_price - q * dynamic_gamma * (sigma ** 2) * T_minus_t
            # V2: alpha信号调整
            reservation += alpha_signal * mid_price / 10000.0
            return self._align_to_tick(reservation)

    def compute_theoretical_price(self, mid_price: float) -> float:
        """简单 skew 理论价（兼容性）。

        FIX-20260713-S6-V2: 与 A-S reservation price 符号一致，多头(skew>0)时拉低理论价以鼓励卖出。
        """
        if mid_price <= 0:
            return 0.0
        skew = self.get_inventory_skew()
        inventory_factor = self.inventory_factor_bps * mid_price / 10000.0
        theo_price = mid_price - skew * inventory_factor
        return self._align_to_tick(theo_price)

    def compute_optimal_spread_as(self, mid_price: float,
                                   sigma: Optional[float] = None,
                                   dynamic_gamma: Optional[float] = None) -> float:
        """V2: A-S最优半价差 with dynamic gamma。

        δ* = γ_dynamic * σ² * (T-t) / 2 + ln(1 + γ_dynamic/κ) / γ_dynamic
        """
        if mid_price <= 0:
            return 0.0
        if sigma is None:
            vol_bps = self._estimate_volatility_bps()
            sigma = vol_bps * mid_price / 10000.0
        if sigma <= 0:
            return self.base_spread_bps * mid_price / 10000.0
        if dynamic_gamma is None:
            dynamic_gamma = self.risk_aversion

        T_minus_t = self.time_horizon_sec
        risk_component = dynamic_gamma * (sigma ** 2) * T_minus_t / 2.0
        adverse_component = math.log(1.0 + dynamic_gamma / self.order_arrival_kappa) / dynamic_gamma
        # FIX-20260713-S6-V2: adverse_component 已是价格单位，直接相加
        half_spread = risk_component + adverse_component
        base_half = self.base_spread_bps * mid_price / 10000.0
        return max(half_spread, base_half)

    def compute_bid_ask_quotes(
        self, mid_price: float, *, volatility_bps: Optional[float] = None
    ) -> Tuple[float, float, float]:
        """对称双边报价（兼容旧版）。"""
        if mid_price <= 0:
            return 0.0, 0.0, 0.0
        if volatility_bps is None:
            volatility_bps = self._estimate_volatility_bps()
        vol_multiplier = max(1.0, volatility_bps / max(self.vol_target_bps, _DEFAULT_PRICE_EPS))
        effective_spread_bps = self.base_spread_bps * vol_multiplier / max(self.competition_factor, _DEFAULT_PRICE_EPS)
        # FIX-20260713-S6-V2: effective_spread_bps 是全价差，需折半作为 half_spread_bps
        half_spread_bps = max(self.base_spread_bps, effective_spread_bps) / 2.0
        half_spread = half_spread_bps * mid_price / 10000.0
        theo_price = self.compute_theoretical_price(mid_price)
        bid = self._align_to_tick(theo_price - half_spread)
        ask = self._align_to_tick(theo_price + half_spread)
        if ask <= bid:
            ask = self._align_to_tick(bid + max(half_spread, self.price_tick if self.price_tick > 0 else _DEFAULT_PRICE_EPS))
        return bid, ask, half_spread

    def compute_asymmetric_quotes_v2(
        self, mid_price: float, *, volatility_bps: Optional[float] = None,
        sigma: Optional[float] = None, dynamic_gamma: Optional[float] = None,
        alpha_signal: float = 0.0, toxicity: float = 0.0
    ) -> Tuple[float, float, float, float, float, float, float]:
        """V2: 不对称双边报价（顶级基金标准）。

        Returns:
            (bid, ask, bid_half, ask_half, reservation, optimal_size, bid_size, ask_size)
        """
        if mid_price <= 0:
            return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

        if volatility_bps is None:
            volatility_bps = self._estimate_volatility_bps()
        if sigma is None:
            sigma = volatility_bps * mid_price / 10000.0
        if dynamic_gamma is None:
            dynamic_gamma = self._gamma_estimator.compute(
                volatility_bps, self.vol_target_bps,
                self._inventory, self._effective_max_inventory(mid_price))

        # V2: 最优价差
        base_half = self.compute_optimal_spread_as(mid_price, sigma=sigma, dynamic_gamma=dynamic_gamma)
        min_half = self.base_spread_bps * mid_price / 10000.0
        base_half = max(base_half, min_half)

        # 毒流调整: 扩大价差
        toxicity_spread_mult = 1.0 + toxicity * 0.5
        base_half *= toxicity_spread_mult

        # 库存偏斜
        skew = self.get_inventory_skew()
        asym_factor = skew * 0.5
        bid_half = base_half * (1.0 + asym_factor)
        ask_half = base_half * (1.0 - asym_factor)
        bid_half = max(bid_half, min_half * 0.3)
        ask_half = max(ask_half, min_half * 0.3)

        # V2: 微观价格参与报价（替代简单mid-price作为reservation基准）
        micro_price = self.compute_micro_price()
        # V2: reservation price with dynamic gamma + alpha
        reservation = self.compute_reservation_price(
            micro_price, sigma=sigma, dynamic_gamma=dynamic_gamma, alpha_signal=alpha_signal)

        # 波动率机制调整
        if volatility_bps > self.vol_target_bps * 2.0:
            bid_half *= 1.3
            ask_half *= 1.3
            self._regime = "high_vol"
        elif volatility_bps < self.vol_target_bps * 0.5:
            self._regime = "low_vol"
        else:
            self._regime = "normal"

        # V2: 日内成交量模式调整
        now = datetime.now(_CHINA_TZ)
        vol_mult = self._volume_profile.get_multiplier(now.hour, now.minute)
        bid_half /= math.sqrt(vol_mult)  # 高成交量→窄价差
        ask_half /= math.sqrt(vol_mult)
        self._diag_stats['volume_profile_queries'] += 1

        bid = self._align_to_tick(reservation - bid_half)
        ask = self._align_to_tick(reservation + ask_half)

        # 防 sniffing 随机抖动
        if self.quote_jitter_bps > 0:
            jitter_bid = random.uniform(-self.quote_jitter_bps, self.quote_jitter_bps) * mid_price / 10000.0
            jitter_ask = random.uniform(-self.quote_jitter_bps, self.quote_jitter_bps) * mid_price / 10000.0
            bid = self._align_to_tick(bid + jitter_bid)
            ask = self._align_to_tick(ask + jitter_ask)

        if ask <= bid:
            ask = self._align_to_tick(bid + (self.price_tick if self.price_tick > 0 else _DEFAULT_PRICE_EPS))

        # V2: 最优订单规模
        optimal_size, bid_size, ask_size = self._order_sizer.compute(
            self.max_inventory, volatility_bps, self.vol_target_bps,
            skew, vol_mult, toxicity)
        self._diag_stats['order_size_optimizations'] += 1

        return bid, ask, bid_half, ask_half, reservation, optimal_size, bid_size, ask_size

    # ========================================================================
    # 3. Inventory 管理
    # ========================================================================

    def update_inventory(self, delta: float, *, fill_price: Optional[float] = None) -> float:
        """更新模拟持仓。"""
        with self._lock:
            prev_inv = self._inventory
            new_inv = prev_inv + delta
            cap = self._effective_max_inventory(fill_price)
            if cap > 0:
                new_inv = max(-cap, min(cap, new_inv))
            actual_delta = new_inv - prev_inv
            self._inventory = new_inv
            self._inventory_turnover_sum += abs(actual_delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            if fill_price is not None and fill_price > 0 and actual_delta != 0.0:
                direction = 1 if actual_delta > 0 else -1
                self._fill_events.append((direction, float(fill_price), time.time()))
            return self._inventory

    def get_inventory_skew(self) -> float:
        with self._lock:
            cap = self._effective_max_inventory(self._last_mid_price)
            if cap <= 0:
                return 0.0
            return max(-1.0, min(1.0, self._inventory / cap))

    def get_inventory(self) -> float:
        with self._lock:
            return self._inventory

    def check_inventory_unwind(self) -> float:
        with self._lock:
            cap = self._effective_max_inventory(self._last_mid_price)
            if cap <= 0:
                return 0.0
            ratio = abs(self._inventory) / cap
            if ratio < self.unwind_threshold:
                return 0.0
            if self._inventory > 0:
                return -abs(self._inventory) * 0.3
            return abs(self._inventory) * 0.3

    # ========================================================================
    # 4. 毒单检测（V2增强）
    # ========================================================================

    def detect_adverse_selection(self) -> bool:
        """基于自身 fill 事件检测毒单。"""
        with self._lock:
            if not self._fill_events or self._last_last_price <= 0:
                return False
            current_price = self._last_last_price
            for direction, fill_price, _ts in self._fill_events:
                if fill_price <= 0:
                    continue
                price_move_bps = (current_price - fill_price) / fill_price * 10000.0
                adverse_move_bps = -direction * price_move_bps
                if adverse_move_bps >= self.adverse_threshold_bps:
                    self._adverse_count += 1
                    return True
            return False

    def compute_vpin(self) -> float:
        with self._lock:
            total_buy = self._vpin_current_buy
            total_sell = self._vpin_current_sell
            for _ts, v_buy, v_sell in self._vpin_buckets:
                total_buy += v_buy
                total_sell += v_sell
            total = total_buy + total_sell
            if total <= 0:
                return 0.0
            return max(0.0, min(1.0, abs(total_buy - total_sell) / total))

    def compute_toxicity_v2(self) -> Tuple[float, float, bool]:
        """V2: 多层毒流分类。"""
        oft = self._toxic_classifier.compute_oft()
        pin = self._toxic_classifier.compute_informed_trader_prob()
        is_toxic = self._toxic_classifier.is_toxic()
        self._diag_stats['toxicity_classifications'] += 1
        if is_toxic:
            self._diag_stats['toxic_events'] += 1
        return oft, pin, is_toxic

    def _update_vpin_bucket(self, direction: int, volume: float) -> None:
        now = time.time()
        bucket_ts = (now // _DEFAULT_VPIN_BUCKET_SEC) * _DEFAULT_VPIN_BUCKET_SEC
        if self._vpin_current_bucket_ts == 0.0:
            self._vpin_current_bucket_ts = bucket_ts
        if bucket_ts != self._vpin_current_bucket_ts:
            self._vpin_buckets.append((
                self._vpin_current_bucket_ts,
                self._vpin_current_buy,
                self._vpin_current_sell,
            ))
            self._vpin_current_bucket_ts = bucket_ts
            self._vpin_current_buy = 0.0
            self._vpin_current_sell = 0.0
        if direction > 0:
            self._vpin_current_buy += abs(volume)
        else:
            self._vpin_current_sell += abs(volume)

    def _estimate_hit_probability(self, spread_bps: float) -> float:
        """报价命中概率：spread 越宽，被穿越概率越低。"""
        if spread_bps <= 0:
            return 0.8
        # FIX-20260713-S6-V2: 命中概率随 spread 增加而衰减
        prob = math.exp(-spread_bps / self.base_spread_bps)
        return max(0.0, min(0.8, prob))

    # V2: 报价取消概率
    def _estimate_cancellation_probability(self, spread_bps: float, queue_pos: float,
                                            volatility_bps: float) -> float:
        """估计报价被取消/未成交的概率。

        基于: 队列位置 + 价差宽度 + 波动率
        """
        # 队列位置越靠后，取消概率越高
        queue_factor = 1.0 - queue_pos  # queue_pos∈[0,1], 1=队首
        # 价差越宽，越容易被穿越
        spread_factor = min(1.0, spread_bps / (self.base_spread_bps * 2))
        # 波动率越高，价格变动越快
        vol_factor = min(1.0, volatility_bps / (self.vol_target_bps * 2))

        cancel_prob = queue_factor * 0.4 + spread_factor * 0.3 + vol_factor * 0.3
        return max(0.0, min(1.0, cancel_prob))

    # ========================================================================
    # 5. 指标
    # ========================================================================

    def compute_metrics(self) -> Dict[str, float]:
        with self._lock:
            quote_hit_rate = self._quote_hit_count / self._quote_count if self._quote_count > 0 else 0.0
            spread_capture_rate = self._spread_captured_bps / self._spread_offered_bps if self._spread_offered_bps > 0 else 0.0
            inventory_turnover = self._inventory_turnover_sum / (2.0 * max(self._inventory_peak, _DEFAULT_PRICE_EPS)) if self._inventory_peak > 0 else 0.0
            adverse_selection_rate = self._adverse_count / self._quote_count if self._quote_count > 0 else 0.0
            sharpe_ratio = self._compute_sharpe()
            vpin = self.compute_vpin()
            target_delta = self.check_inventory_unwind()
            oft, pin, is_toxic = self.compute_toxicity_v2()

            return {
                "quote_hit_rate": round(quote_hit_rate, 6),
                "spread_capture_rate": round(spread_capture_rate, 6),
                "inventory_turnover": round(inventory_turnover, 6),
                "adverse_selection_rate": round(adverse_selection_rate, 6),
                "sharpe_ratio": round(sharpe_ratio, 6),
                "inventory": round(self._inventory, 6),
                "inventory_skew": round(self.get_inventory_skew(), 6),
                "realized_pnl": round(self._realized_pnl, 6),
                "quote_count": float(self._quote_count),
                "quote_hit_count": float(self._quote_hit_count),
                "adverse_count": float(self._adverse_count),
                "vpin": round(vpin, 6),
                "target_inventory_delta": round(target_delta, 6),
                "daily_loss_bps": round(self._daily_loss_bps, 6),
                # V2新增
                "oft": round(oft, 6),
                "informed_trader_prob": round(pin, 6),
                "is_toxic": float(is_toxic),
            }

    # ========================================================================
    # 6. 信号生成 + 快照（V2增强）
    # ========================================================================

    def generate_simulated_signal_v2(
        self, *, tick_data: Optional[Dict[str, Any]] = None,
        hedge_instruments: Optional[Dict[str, float]] = None
    ) -> MarketMakingSignalV2:
        """V2: 生成模拟做市信号（顶级基金标准）。"""
        with self._lock:
            if tick_data is not None:
                self._ingest_tick(tick_data)

            mid_price = self._last_mid_price
            if mid_price <= 0:
                signal = MarketMakingSignalV2(
                    instrument_id=self.instrument_id,
                    timestamp=time.time(),
                    metrics=self.compute_metrics(),
                )
                self._last_signal = signal
                return signal

            volatility_bps = self._estimate_volatility_bps()
            sigma = volatility_bps * mid_price / 10000.0

            # V2: 微观价格
            micro_price = self.compute_micro_price()
            imb = self.get_order_book_imbalance()

            # V2: 动态gamma
            dynamic_gamma = self._gamma_estimator.compute(
                volatility_bps, self.vol_target_bps,
                self._inventory, self._effective_max_inventory(mid_price))
            self._diag_stats['gamma_adjustments'] += 1

            # V2: 毒流分类
            oft, pin, is_toxic = self.compute_toxicity_v2()

            # V2: 相关性对冲
            corr_hedge = 0.0
            if hedge_instruments:
                for hedge_id, hedge_weight in hedge_instruments.items():
                    hr = self._correlation_hedger.compute_hedge_ratio(
                        self.instrument_id, hedge_id)
                    corr_hedge += hr * hedge_weight
                self._diag_stats['correlation_hedges'] += 1

            # V2: alpha信号（基于订单簿不平衡）
            alpha_signal = imb * 0.5  # 简化alpha

            # V2: 不对称报价
            try:
                bid, ask, bid_half, ask_half, reservation, opt_size, bid_size, ask_size = \
                    self.compute_asymmetric_quotes_v2(
                        mid_price, volatility_bps=volatility_bps, sigma=sigma,
                        dynamic_gamma=dynamic_gamma, alpha_signal=alpha_signal,
                        toxicity=oft)
                theo_price = reservation
                half_spread = (bid_half + ask_half) / 2.0
            except Exception as _as_err:
                logger.debug("[S6-MMM-V2] 不对称报价降级: %s", _as_err)
                bid, ask, half_spread = self.compute_bid_ask_quotes(mid_price, volatility_bps=volatility_bps)
                theo_price = self.compute_theoretical_price(mid_price)
                bid_half = half_spread
                ask_half = half_spread
                reservation = theo_price
                opt_size = self.max_inventory
                bid_size = opt_size
                ask_size = opt_size

            optimal_spread_as = self.compute_optimal_spread_as(
                mid_price, sigma=sigma, dynamic_gamma=dynamic_gamma)
            spread = ask - bid
            spread_bps = (spread / mid_price * 10000.0) if mid_price > 0 else 0.0
            skew = self.get_inventory_skew()
            adverse_flag = self.detect_adverse_selection()
            vpin = self.compute_vpin()
            target_delta = self.check_inventory_unwind()
            hit_prob = self._estimate_hit_probability(spread_bps)

            # V2: 反弹概率
            bounce_prob = self._bounce_detector.compute_bounce_prob()
            self._diag_stats['bounce_detections'] += 1

            # V2: 取消概率
            queue_pos = 0.85 if abs(spread_bps) < self.base_spread_bps else 0.6
            cancel_prob = self._estimate_cancellation_probability(
                spread_bps, queue_pos, volatility_bps)

            # 净价差
            net_capture_bps = spread_bps - 2.0 * self.maker_fee_bps + 2.0 * self.rebate_bps

            # 方向建议（统一阈值0.15）
            if skew > 0.15:
                preferred_side = "sell"
            elif skew < -0.15:
                preferred_side = "buy"
            else:
                preferred_side = "neutral"

            # 日亏损检查
            stop_quoting = self._daily_loss_bps >= self.max_daily_loss_bps

            # V2: 日内成交量百分位
            now = datetime.now(_CHINA_TZ)
            vol_percentile = self._volume_profile.get_percentile(now.hour, now.minute)

            # V2: PnL归因
            self._pnl_attribution.mark_to_market(self._inventory, mid_price)
            pnl_attr = self._pnl_attribution.get_attribution()
            self._diag_stats['pnl_attributions'] += 1

            # V2: 库存风险（bps）
            inventory_risk = abs(skew) * volatility_bps * self._inventory / max(abs(self._inventory), 1)

            # V2: 报价偏斜比
            if bid_half + ask_half > 0:
                quote_skew_ratio = (bid_half - ask_half) / (bid_half + ask_half)
            else:
                quote_skew_ratio = 0.0

            signal = MarketMakingSignalV2(
                instrument_id=self.instrument_id,
                bid_price=bid,
                ask_price=ask,
                theo_price=theo_price,
                mid_price=mid_price,
                spread=spread,
                spread_bps=spread_bps,
                inventory=self._inventory,
                inventory_skew=skew,
                adverse_selection_flag=adverse_flag,
                metrics=self.compute_metrics(),
                timestamp=time.time(),
                half_spread=half_spread,
                volatility_bps=volatility_bps,
                competition_factor=self.competition_factor,
                preferred_side=preferred_side,
                reservation_price=reservation,
                bid_half_spread=bid_half,
                ask_half_spread=ask_half,
                vpin=vpin,
                optimal_spread_as=optimal_spread_as,
                target_inventory_delta=target_delta,
                sigma=sigma,
                regime=self._regime,
                expected_hit_prob=round(hit_prob, 6),
                net_capture_bps=round(net_capture_bps, 6),
                quote_refresh_due=not stop_quoting,
                # V2新增
                micro_price=round(micro_price, 6),
                dynamic_gamma=round(dynamic_gamma, 6),
                volume_profile_multiplier=round(
                    self._volume_profile.get_multiplier(now.hour, now.minute), 4),
                order_flow_toxicity=round(oft, 6),
                informed_trader_prob=round(pin, 6),
                inventory_correlation_hedge=round(corr_hedge, 6),
                alpha_adjustment=round(alpha_signal, 6),
                pnl_attribution=pnl_attr,
                order_book_imbalance=round(imb, 6),
                bid_ask_bounce_prob=round(bounce_prob, 6),
                cancellation_probability=round(cancel_prob, 6),
                optimal_order_size=round(opt_size, 4),
                intraday_volume_percentile=round(vol_percentile, 4),
                quote_skew_ratio=round(quote_skew_ratio, 6),
                inventory_risk_bps=round(inventory_risk, 6),
                market_regime_v2=self._regime,
            )

            # 断言验证
            assert bid - _DEFAULT_PRICE_EPS <= theo_price <= ask + _DEFAULT_PRICE_EPS, (
                f"理论价格 {theo_price} 不在 bid {bid} 与 ask {ask} 之间")
            cap = self._effective_max_inventory(mid_price)
            assert abs(self._inventory) <= cap + _DEFAULT_PRICE_EPS, (
                f"inventory {self._inventory} 超过 max_inventory {cap}")

            # V2: 额外断言
            assert 0.0 <= dynamic_gamma <= 1.0, f"dynamic_gamma={dynamic_gamma} 超出[0,1]"
            assert 0.0 <= oft <= 1.0, f"oft={oft} 超出[0,1]"
            assert 0.0 <= cancel_prob <= 1.0, f"cancel_prob={cancel_prob} 超出[0,1]"

            self._quote_count += 1
            self._spread_offered_bps += spread_bps
            self._last_signal = signal
            self._diag_stats['signals_generated'] += 1

            logger.info(
                "[S6-MMM-V2] 信号生成: inst=%s micro=%.6f r=%.6f bid=%.6f ask=%.6f "
                "spread=%.2fbps net=%.2fbps inv=%.4f skew=%.4f γ=%.4f "
                "oft=%.4f pin=%.4f toxic=%s cancel=%.2f size=%.2f | ⚠不下单",
                self.instrument_id, micro_price, reservation, bid, ask,
                spread_bps, net_capture_bps, self._inventory, skew,
                dynamic_gamma, oft, pin, is_toxic, cancel_prob, opt_size,
            )
            return signal

    def save_snapshot(self, signal: Optional[MarketMakingSignalV2] = None) -> str:
        if signal is None:
            signal = self._last_signal
        if signal is None:
            logger.warning("[S6-MMM-V2] save_snapshot: 无可用信号")
            return ""

        os.makedirs(self.snapshot_dir, exist_ok=True)
        safe_inst = "".join(c for c in (self.instrument_id or "unknown") if c.isalnum() or c in ("_", "-"))
        ts = int(signal.timestamp) if signal.timestamp > 0 else int(time.time())
        fname = f"S6MMV2_{safe_inst}_{ts}_{self._snapshot_count}.json"
        fpath = os.path.join(self.snapshot_dir, fname)

        payload = {
            "module": "S6-MMM-V2",
            "version": "2.0",
            "instrument_id": self.instrument_id,
            "participates_capital_allocation": self.PARTICIPATES_CAPITAL_ALLOCATION,
            "sends_real_orders": self.SENDS_REAL_ORDERS,
            "signal": signal.to_dict(),
            "saved_at": time.time(),
            "v2_features": {
                "micro_price_model": True,
                "dynamic_gamma": True,
                "intraday_volume_profile": True,
                "multi_layer_toxic_flow": True,
                "correlation_hedging": True,
                "pnl_attribution": True,
                "order_size_optimization": True,
                "bid_ask_bounce_detection": True,
                "cancellation_probability": True,
            },
            "strategy_meta": {
                "strategy_id": self.STRATEGY_ID,
                "strategy_type": self.STRATEGY_TYPE,
                "capital_allocation": self.CAPITAL_ALLOCATION,
                "simulated": True,
                "note": "S6做市商策略V2实盘模拟，顶级基金做市商标准，不参与资金分配",
            },
        }

        if _json_dumps is not None:
            content = _json_dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
        else:
            content = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True, default=str)

        try:
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(content)
        except OSError as e:
            logger.error("[S6-MMM-V2] 快照写入失败 path=%s err=%s", fpath, e)
            return ""

        self._snapshot_count += 1
        self._diag_stats['snapshots_saved'] += 1
        assert os.path.exists(fpath), f"快照文件保存后不存在: {fpath}"
        logger.info("[S6-MMM-V2] 快照已保存: %s (size=%d bytes)", fpath, os.path.getsize(fpath))
        return fpath

    # ========================================================================
    # 7. 集成接口
    # ========================================================================

    def on_tick(self, tick_data: Dict[str, Any]) -> None:
        """tick 回调：节流 + 命中模拟 + 快照。"""
        if not isinstance(tick_data, dict):
            return

        with self._lock:
            prev_last_price = self._last_last_price
            self._ingest_tick(tick_data)
            self._tick_count += 1

            # V2: 更新毒流分类器
            volume = float(tick_data.get('volume', 0.0) or 0.0)
            if volume > 0 and self._last_last_price > 0:
                direction = self._infer_trade_direction(tick_data)
                if direction != 0:
                    self._update_vpin_bucket(direction, volume)
                    self._toxic_classifier.update(direction, volume, time.time())

            # V2: 更新买卖反弹检测
            if self._last_bid > 0 and self._last_ask > 0:
                self._bounce_detector.update(
                    self._last_last_price, self._last_bid, self._last_ask, prev_last_price)

            # V2: 更新相关性对冲器
            if self._last_last_price > 0:
                self._correlation_hedger.update_price(self.instrument_id, self._last_last_price)

            # 命中模拟（使用新tick价格，修复时序错误）
            if self._last_signal is not None:
                try:
                    self._simulate_quote_hit(self._last_signal)
                except Exception as e:
                    logger.debug("[S6-MMM-V2] _simulate_quote_hit 异常: %s", e)

        # 节流判断
        _now = time.time()
        _ticks_ok = self._tick_count >= self.signal_interval_ticks
        _time_ok = (_now - self._last_signal_time) >= self.snapshot_interval_sec
        _refresh_ok = (_now - self._last_quote_time) >= self.min_quote_refresh_sec
        if not (_ticks_ok or _time_ok) or not _refresh_ok:
            return

        try:
            signal = self.generate_simulated_signal_v2()
            self.save_snapshot(signal)
            with self._lock:
                self._tick_count = 0
                self._last_signal_time = _now
                self._last_quote_time = _now
        except AssertionError as e:
            logger.error("[S6-MMM-V2] on_tick 断言失败: %s", e)
        except Exception as e:
            logger.exception("[S6-MMM-V2] on_tick 处理异常: %s", e)

    def on_bar(self, bar_data: Dict[str, Any]) -> None:
        if not isinstance(bar_data, dict):
            return
        try:
            close = float(bar_data.get("close", 0.0) or 0.0)
            high = float(bar_data.get("high", close) or close)
            low = float(bar_data.get("low", close) or close)
            if close <= 0:
                return
            mid = (high + low) / 2.0
            with self._lock:
                if self.instrument_id == "":
                    self.instrument_id = str(bar_data.get("instrument_id", ""))
                self._last_mid_price = mid
                self._last_last_price = close
                self._last_bid = low
                self._last_ask = high
                self._price_history.append(close)
                self._correlation_hedger.update_price(self.instrument_id, close)
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM-V2] on_bar 解析异常: %s", e)

    def get_monitoring_report(self) -> Dict[str, Any]:
        with self._lock:
            metrics = self.compute_metrics()
            last_signal_dict = self._last_signal.to_dict() if self._last_signal else None
            return {
                "module": "S6-MMM-V2",
                "version": "2.0",
                "instrument_id": self.instrument_id,
                "participates_capital_allocation": self.PARTICIPATES_CAPITAL_ALLOCATION,
                "sends_real_orders": self.SENDS_REAL_ORDERS,
                "config": {
                    "max_inventory": self.max_inventory,
                    "max_inventory_notional": self.max_inventory_notional,
                    "base_spread_bps": self.base_spread_bps,
                    "vol_lookback": self.vol_lookback,
                    "vol_target_bps": self.vol_target_bps,
                    "competition_factor": self.competition_factor,
                    "inventory_factor_bps": self.inventory_factor_bps,
                    "risk_aversion": self.risk_aversion,
                    "time_horizon_sec": self.time_horizon_sec,
                    "order_arrival_kappa": self.order_arrival_kappa,
                },
                "state": {
                    "inventory": self._inventory,
                    "inventory_skew": self.get_inventory_skew(),
                    "last_mid_price": self._last_mid_price,
                    "last_bid": self._last_bid,
                    "last_ask": self._last_ask,
                    "micro_price": self.compute_micro_price(),
                    "order_book_imbalance": self.get_order_book_imbalance(),
                    "snapshot_count": self._snapshot_count,
                    "tick_count": self._tick_count,
                    "regime": self._regime,
                    "daily_loss_bps": self._daily_loss_bps,
                },
                "metrics": metrics,
                "v2_components": {
                    "dynamic_gamma": self._gamma_estimator.compute(
                        self._estimate_volatility_bps(), self.vol_target_bps,
                        self._inventory, self._effective_max_inventory(self._last_mid_price)),
                    "oft": self._toxic_classifier.compute_oft(),
                    "informed_trader_prob": self._toxic_classifier.compute_informed_trader_prob(),
                    "pnl_attribution": self._pnl_attribution.get_attribution(),
                    "bounce_prob": self._bounce_detector.compute_bounce_prob(),
                },
                "last_signal": last_signal_dict,
                "diag_stats": dict(self._diag_stats),
                "report_time": time.time(),
            }

    # ========================================================================
    # 8. 内部辅助
    # ========================================================================

    def _ingest_tick(self, tick_data: Dict[str, Any]) -> None:
        try:
            inst = tick_data.get("instrument_id")
            if inst and not self.instrument_id:
                self.instrument_id = str(inst)

            last_price = float(tick_data.get("last_price", 0.0) or 0.0)
            bid = float(tick_data.get("bid_price", tick_data.get("bid_price1", 0.0)) or 0.0)
            ask = float(tick_data.get("ask_price", tick_data.get("ask_price1", 0.0)) or 0.0)
            # V2: 读取买卖量
            bid_vol = float(tick_data.get("bid_volume", tick_data.get("bid_volume1", 0.0)) or 0.0)
            ask_vol = float(tick_data.get("ask_volume", tick_data.get("ask_volume1", 0.0)) or 0.0)

            if last_price > 0:
                self._last_last_price = last_price
                self._price_history.append(last_price)

            if bid > 0 and ask > 0:
                self._last_bid = bid
                self._last_ask = ask
                self._last_mid_price = (bid + ask) / 2.0
                if bid_vol > 0 or ask_vol > 0:
                    self._last_bid_vol = bid_vol
                    self._last_ask_vol = ask_vol
            elif last_price > 0 and self._last_mid_price <= 0:
                self._last_mid_price = last_price
                self._last_bid = last_price
                self._last_ask = last_price
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM-V2] _ingest_tick 解析异常: %s", e)

    def _infer_trade_direction(self, tick_data: Dict[str, Any]) -> int:
        last_price = self._last_last_price
        bid = self._last_bid
        ask = self._last_ask
        if last_price <= 0 or bid <= 0 or ask <= 0:
            return 0
        if last_price >= ask:
            return 1
        if last_price <= bid:
            return -1
        mid = (bid + ask) / 2.0
        return 1 if last_price >= mid else -1

    def _estimate_volatility_bps(self) -> float:
        prices = list(self._price_history)
        if len(prices) < _MIN_VOL_SAMPLE:
            return self.base_spread_bps
        rets = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices)) if prices[i - 1] > 0]
        if len(rets) < _MIN_VOL_SAMPLE - 1:
            return self.base_spread_bps
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        std = math.sqrt(var)
        vol_bps = std * 10000.0 * self.vol_scale
        return vol_bps if vol_bps > 0 else self.base_spread_bps

    def _simulate_quote_hit(self, signal: MarketMakingSignalV2) -> None:
        """模拟报价命中与成交。调用方需持锁。"""
        market_price = self._last_last_price
        if market_price <= 0:
            return

        hit_prob = self._estimate_hit_probability(signal.spread_bps)
        if random.random() > hit_prob:
            return

        if signal.bid_price > 0 and market_price <= signal.bid_price:
            self._quote_hit_count += 1
            fill_price = signal.bid_price
            delta = 1.0  # FIX-20260713-S6-V2: bid 被命中，我们作为买方成交，库存增加
            self._inventory = self._clamp_inventory(self._inventory + delta, market_price)
            self._inventory_turnover_sum += abs(delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            captured_bps = signal.half_spread / signal.mid_price * 10000.0 if signal.mid_price > 0 else 0.0
            self._spread_captured_bps += captured_bps
            net_bps = captured_bps - self.maker_fee_bps + self.rebate_bps
            incremental_pnl = net_bps * signal.mid_price / 10000.0
            self._realized_pnl += incremental_pnl
            self._daily_loss_bps = max(0.0, self._daily_loss_bps - net_bps)
            self._pnl_samples.append(incremental_pnl)  # V2: 增量值
            self._fill_events.append((1, fill_price, time.time()))
            self._update_vpin_bucket(1, abs(delta))
            self._diag_stats['quote_hits_simulated'] += 1
            # V2: PnL归因记录
            self._pnl_attribution.record_trade(
                1, fill_price, signal.mid_price, signal.half_spread,
                self.maker_fee_bps, self.rebate_bps, signal.adverse_selection_flag)

        elif signal.ask_price > 0 and market_price >= signal.ask_price:
            self._quote_hit_count += 1
            fill_price = signal.ask_price
            delta = -1.0  # FIX-20260713-S6-V2: ask 被命中，我们作为卖方成交，库存减少
            self._inventory = self._clamp_inventory(self._inventory + delta, market_price)
            self._inventory_turnover_sum += abs(delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            captured_bps = signal.half_spread / signal.mid_price * 10000.0 if signal.mid_price > 0 else 0.0
            self._spread_captured_bps += captured_bps
            net_bps = captured_bps - self.maker_fee_bps + self.rebate_bps
            incremental_pnl = net_bps * signal.mid_price / 10000.0
            self._realized_pnl += incremental_pnl
            self._daily_loss_bps = max(0.0, self._daily_loss_bps - net_bps)
            self._pnl_samples.append(incremental_pnl)  # V2: 增量值
            self._fill_events.append((-1, fill_price, time.time()))
            self._update_vpin_bucket(-1, abs(delta))
            self._diag_stats['quote_hits_simulated'] += 1
            # V2: PnL归因记录
            self._pnl_attribution.record_trade(
                -1, fill_price, signal.mid_price, signal.half_spread,
                self.maker_fee_bps, self.rebate_bps, signal.adverse_selection_flag)

    def _clamp_inventory(self, inv: float, price: float) -> float:
        cap = self._effective_max_inventory(price)
        if cap <= 0:
            return inv
        return max(-cap, min(cap, inv))

    def _effective_max_inventory(self, price: Optional[float]) -> float:
        if self.max_inventory_notional > 0 and price is not None and price > 0:
            return self.max_inventory_notional / price
        return self.max_inventory

    def _align_to_tick(self, price: float) -> float:
        if self.price_tick <= 0:
            return price
        return round(price / self.price_tick) * self.price_tick

    def _compute_sharpe(self) -> float:
        if len(self._pnl_samples) < 2:
            return 0.0
        samples = list(self._pnl_samples)
        mean = sum(samples) / len(samples)
        var = sum((s - mean) ** 2 for s in samples) / len(samples)
        std = math.sqrt(var)
        if std <= _DEFAULT_PRICE_EPS:
            return 0.0
        return mean / std

    # ========================================================================
    # 9. 查询接口
    # ========================================================================

    def get_preferred_side(self) -> str:
        with self._lock:
            skew = self.get_inventory_skew()
            if skew > 0.15:
                return 'sell'
            elif skew < -0.15:
                return 'buy'
            return 'neutral'

    def get_last_signal(self) -> Optional['MarketMakingSignalV2']:
        with self._lock:
            return self._last_signal

    def get_diag_stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._diag_stats)

    @classmethod
    def get_instance(cls, instrument_id: str = '', **kwargs) -> 'MarketMakingMonitorV2':
        global _mmm_v2_instances
        with _mmm_v2_lock:
            _key = instrument_id or 'DEFAULT'
            if _key not in _mmm_v2_instances:
                _mmm_v2_instances[_key] = MarketMakingMonitorV2(instrument_id=_key, **kwargs)
            return _mmm_v2_instances[_key]


# ============================================================================
# 模块级多合约 registry
# ============================================================================

_mmm_v2_instances: Dict[str, 'MarketMakingMonitorV2'] = {}
_mmm_v2_lock = threading.Lock()


def get_market_making_monitor_v2(instrument_id: str = '', **kwargs) -> MarketMakingMonitorV2:
    return MarketMakingMonitorV2.get_instance(instrument_id=instrument_id, **kwargs)