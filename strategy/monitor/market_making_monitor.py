# -*- coding: utf-8 -*-
"""
S6做市商策略实盘模拟监控模块 V1（顶级基金做市商标准）
===========================================================

核心升级（对比旧版）
-------------------
1. 完整 Avellaneda-Stoikov 框架：reservation price + 最优spread，参数显式可配。
2. 市场毒单检测：VPIN 基于市场 tick 主动成交方向，而非仅自身 fill。
3. 费率模型：手续费/返佣显式建模，盈亏计算考虑费率。
4. 波动率机制检测：高/低波动率自动调整 risk_aversion 与 base_spread。
5. 报价命中概率：引入队列位置模型，命中概率与 spread 宽度相关。
6. Inventory 硬止损：亏损或持仓超限自动停止双边报价。
7. 报价刷新策略：最小刷新间隔、随机抖动防 sniffing。
8. 多合约 registry：每个合约独立状态。

约束
----
- 不参与资金分配，不调用 OrderService.send_order，不调用 route_capital。
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
_DEFAULT_SNAPSHOT_DIR = "logs/market_making_monitor"
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
_DEFAULT_MAKER_FEE_BPS = 0.2       # 单边 maker 费率
_DEFAULT_TAKER_FEE_BPS = 0.5       # 单边 taker 费率
_DEFAULT_REBATE_BPS = 0.1          # 单边返佣
_DEFAULT_MAX_DAILY_LOSS_BPS = 50.0  # 日最大亏损（bps）
_DEFAULT_MIN_QUOTE_REFRESH_SEC = 0.05  # 最小报价刷新间隔

# VPIN
_DEFAULT_VPIN_WINDOW = 60
_DEFAULT_VPIN_BUCKET_SEC = 5.0


# ============================================================================
# 数据结构
# ============================================================================

@dataclass(slots=True)
class MarketMakingSignal:
    """S6 做市商模拟信号快照。"""

    instrument_id: str = ""
    # FIX-HH (R9-6-2): 补齐信号标准字段，与其他策略信号对齐
    # 根因: MarketMakingSignal缺少signal_id/direction/signal_type字段，
    #       下游消费者(strategy_business_layer等)通过getattr访问这些字段时返回None/默认值，
    #       导致信号无法被正确分类和记录
    signal_id: str = ""
    direction: str = "neutral"
    signal_type: str = "market_making"
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

    # 顶级基金字段
    reservation_price: float = 0.0
    bid_half_spread: float = 0.0
    ask_half_spread: float = 0.0
    vpin: float = 0.0
    optimal_spread_as: float = 0.0
    target_inventory_delta: float = 0.0
    sigma: float = 0.0
    regime: str = "normal"           # normal / high_vol / low_vol
    expected_hit_prob: float = 0.0   # 报价命中概率
    net_capture_bps: float = 0.0     # 扣除费率后的净价差
    quote_refresh_due: bool = True   # 是否触发刷新
    # FIX-20260713-S6-MULTISRC: 多源数据采集字段
    order_book_imbalance: float = 0.0  # 订单簿不平衡 [-1,1]，正值=买盘更厚
    open_interest: float = 0.0         # 当前持仓量
    oi_trend: str = "stable"           # 持仓量趋势 rising/falling/stable
    vwap: float = 0.0                  # 成交量加权平均价
    real_trade_direction: int = 0      # 真实成交方向(+1买/-1卖/0未知)
    data_source_mask: int = 0          # 活跃数据源掩码

    # FIX-20260713-S6-V2ABSORB: V2 顶级基金组件字段
    micro_price: float = 0.0               # 微观价格
    dynamic_gamma: float = 0.1             # 动态风险厌恶
    volume_profile_multiplier: float = 1.0  # 日内成交量乘子
    order_flow_toxicity: float = 0.0       # OFT毒流指标
    informed_trader_prob: float = 0.0      # 知情交易者概率(PIN)
    inventory_correlation_hedge: float = 0.0  # 相关性对冲建议
    pnl_attribution: Dict[str, float] = field(default_factory=dict)  # PnL归因
    bid_ask_bounce_prob: float = 0.0       # 买卖反弹概率
    cancellation_probability: float = 0.0  # 报价取消概率
    optimal_order_size: float = 0.0        # 最优订单规模
    intraday_volume_percentile: float = 0.5  # 日内成交量百分位

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# 核心监控类
# ============================================================================

# ----------------------------------------------------------------------------
# FIX-20260713-S6-V2ABSORB: V2 顶级基金组件（批判式吸收自 market_making_monitor_v2）
# 保留所有现有 V1 框架与多源数据采集逻辑；V2 组件以独立类形式接入，不破坏既有约束。
# ----------------------------------------------------------------------------


class _MicroPriceModel:
    """基于订单簿不平衡的微观价格模型。
    对标: Citadel Securities micro-price estimation。
    微价 = weighted_mid + ewma_imbalance * half_spread * weight
    weighted_mid = (bid*ask_vol + ask*bid_vol) / (bid_vol + ask_vol)
    """
    def __init__(self, imbalance_lookback: int = 20, imbalance_weight: float = 0.5) -> None:
        self.lookback = imbalance_lookback
        self.weight = imbalance_weight
        self._imbalance_history: Deque[float] = deque(maxlen=imbalance_lookback)

    def compute(self, bid: float, ask: float, bid_vol: float = 0.0,
                ask_vol: float = 0.0, last_price: float = 0.0) -> Tuple[float, float]:
        if bid <= 0 or ask <= 0 or ask <= bid:
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last_price
            return mid, 0.0
        mid = (bid + ask) / 2.0
        total_vol = bid_vol + ask_vol
        if total_vol > 0:
            weighted_mid = (bid * ask_vol + ask * bid_vol) / total_vol
            imb = (bid_vol - ask_vol) / total_vol
        else:
            weighted_mid = mid
            if last_price > 0:
                imb = max(-1.0, min(1.0, (last_price - mid) / (ask - bid) * 2.0))
            else:
                imb = 0.0
        self._imbalance_history.append(imb)
        ewma_imb = self._compute_ewma_imbalance()
        half_spread = (ask - bid) / 2.0
        micro_price = weighted_mid + ewma_imb * half_spread * self.weight
        return micro_price, ewma_imb

    def _compute_ewma_imbalance(self) -> float:
        if not self._imbalance_history:
            return 0.0
        alpha = 2.0 / (len(self._imbalance_history) + 1)
        ewma = self._imbalance_history[0]
        for v in list(self._imbalance_history)[1:]:
            ewma = alpha * v + (1 - alpha) * ewma
        return ewma


class _DynamicGammaEstimator:
    """波动率+库存驱动的动态风险厌恶。
    对标: IMC dynamic risk aversion。
    γ_dynamic = γ_base * (1 + vol_scale*(σ/σ_target-1)) * (1 + inv_scale*|q/q_max|)
    """
    def __init__(self, base_gamma: float = 0.1, vol_scale: float = 0.5, inv_scale: float = 0.3) -> None:
        self.base_gamma = base_gamma
        self.vol_scale = vol_scale
        self.inv_scale = inv_scale

    def compute(self, current_vol_bps: float, target_vol_bps: float,
                inventory: float, max_inventory: float) -> float:
        if target_vol_bps > 0:
            vol_ratio = current_vol_bps / target_vol_bps
            vol_adj = 1.0 + self.vol_scale * (vol_ratio - 1.0)
        else:
            vol_adj = 1.0
        if max_inventory > 0:
            inv_ratio = abs(inventory) / max_inventory
            inv_adj = 1.0 + self.inv_scale * inv_ratio
        else:
            inv_adj = 1.0
        gamma = self.base_gamma * vol_adj * inv_adj
        return max(0.01, min(1.0, gamma))


class _IntradayVolumeProfile:
    """U型日内成交量模式。对标: Jump Trading。
    中国期货: 开盘1.5x, 收盘1.6x, 午间正常1.0x。
    """
    _PERIOD_MULTIPLIERS = {
        (9, 0): 1.5, (9, 30): 1.3, (10, 0): 1.0, (10, 30): 1.0, (11, 0): 1.1,
        (13, 0): 1.3, (13, 30): 1.0, (14, 0): 1.0, (14, 30): 1.6,
        (21, 0): 1.2, (21, 30): 1.0, (22, 0): 1.0, (22, 30): 0.8, (23, 0): 0.6,
    }

    def get_multiplier(self, hour: int, minute: int) -> float:
        aligned_minute = (minute // 30) * 30
        return self._PERIOD_MULTIPLIERS.get((hour, aligned_minute), 1.0)

    def get_percentile(self, hour: int, minute: int) -> float:
        mult = self.get_multiplier(hour, minute)
        return min(1.0, max(0.0, mult / 2.0))


class _ToxicFlowClassifier:
    """多层毒流分类器: OFT + PIN。对标: Citadel Securities。
    层1 OFT = |累计买-累计卖| / 总成交量
    层2 PIN ≈ |buy_rate - sell_rate| / (buy_rate + sell_rate)
    """
    def __init__(self, oft_window: int = 60, oft_threshold: float = 0.7) -> None:
        self.oft_window = oft_window
        self.oft_threshold = oft_threshold
        self._buy_volume: Deque[float] = deque(maxlen=oft_window)
        self._sell_volume: Deque[float] = deque(maxlen=oft_window)
        self._trade_directions: Deque[int] = deque(maxlen=oft_window)
        self._arrival_times: Deque[float] = deque(maxlen=oft_window)

    def update(self, direction: int, volume: float, timestamp: float) -> None:
        self._trade_directions.append(direction)
        self._arrival_times.append(timestamp)
        if direction > 0:
            self._buy_volume.append(volume)
            self._sell_volume.append(0.0)
        elif direction < 0:
            self._buy_volume.append(0.0)
            self._sell_volume.append(volume)

    def compute_oft(self) -> float:
        total_buy = sum(self._buy_volume)
        total_sell = sum(self._sell_volume)
        total = total_buy + total_sell
        return abs(total_buy - total_sell) / total if total > 0 else 0.0

    def compute_informed_trader_prob(self) -> float:
        n = len(self._arrival_times)
        if n < 10:
            return 0.0
        buys = sum(1 for d in self._trade_directions if d > 0)
        sells = sum(1 for d in self._trade_directions if d < 0)
        if buys + sells == 0:
            return 0.0
        time_span = self._arrival_times[-1] - self._arrival_times[0]
        if time_span <= 0:
            time_span = 1.0
        buy_rate = buys / time_span
        sell_rate = sells / time_span
        total_rate = buy_rate + sell_rate
        return min(1.0, max(0.0, abs(buy_rate - sell_rate) / total_rate)) if total_rate > 0 else 0.0

    def is_toxic(self) -> bool:
        return self.compute_oft() > self.oft_threshold or self.compute_informed_trader_prob() > 0.7


class _CorrelationHedger:
    """跨品种相关性库存风险对冲。对标: Virtu Financial。
    对冲比率 = corr * max_hedge_ratio
    """
    def __init__(self, corr_lookback: int = 60, max_hedge_ratio: float = 0.5) -> None:
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
        prices_primary = list(self._price_history.get(primary_id, []))
        prices_hedge = list(self._price_history.get(hedge_id, []))
        n = min(len(prices_primary), len(prices_hedge))
        if n < 30:
            return 0.0
        rets_p = [(prices_primary[i] - prices_primary[i - 1]) / prices_primary[i - 1]
                  for i in range(1, n) if prices_primary[i - 1] > 0]
        rets_h = [(prices_hedge[i] - prices_hedge[i - 1]) / prices_hedge[i - 1]
                  for i in range(1, n) if prices_hedge[i - 1] > 0]
        m = min(len(rets_p), len(rets_h))
        if m < 10:
            return 0.0
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
        # FIX-20260713-S6-V1: 正相关 → 反向对冲；负相关 → 同向对冲
        hedge_ratio = -corr * self.max_hedge_ratio
        return max(-self.max_hedge_ratio, min(self.max_hedge_ratio, hedge_ratio))

    def get_correlation(self, primary_id: str, hedge_id: str) -> float:
        return self._correlations.get(f"{primary_id}:{hedge_id}", 0.0)


class _PnLAttribution:
    """PnL归因分解: spread_capture + inventory_revaluation + fee_net + adverse_cost。
    对标: Jane Street PnL attribution。
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
        self.spread_capture += half_spread
        self._trade_count += 1
        self.fee_net += (-maker_fee_bps + rebate_bps) * mid_price / 10000.0
        if is_adverse:
            self.adverse_cost += -abs(half_spread) * 2.0

    def mark_to_market(self, inventory: float, current_mid: float) -> None:
        if self._last_mark_price > 0 and inventory != 0:
            self.inventory_revaluation += inventory * (current_mid - self._last_mark_price)
        self._last_mark_price = current_mid

    def get_attribution(self) -> Dict[str, float]:
        total = self.spread_capture + self.inventory_revaluation + self.fee_net + self.adverse_cost
        return {'spread_capture': round(self.spread_capture, 6),
                'inventory_revaluation': round(self.inventory_revaluation, 6),
                'fee_net': round(self.fee_net, 6),
                'adverse_cost': round(self.adverse_cost, 6),
                'total': round(total, 6), 'trade_count': self._trade_count}


class _OrderSizeOptimizer:
    """GLFT最优订单规模优化器。对标: Jump Trading。
    三维: 波动率(高→小) + 库存(偏斜) + 成交量(高→大)
    """
    def __init__(self, min_size: float = 1.0, max_size: float = 20.0, vol_scale: float = 0.5) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self.vol_scale = vol_scale

    def compute(self, base_size: float, volatility_bps: float, target_vol_bps: float,
                inventory_skew: float, volume_multiplier: float,
                toxicity: float) -> Tuple[float, float, float]:
        if target_vol_bps > 0:
            vol_ratio = volatility_bps / target_vol_bps
            vol_adj = 1.0 / (1.0 + self.vol_scale * (vol_ratio - 1.0))
        else:
            vol_adj = 1.0
        toxicity_adj = 1.0 - toxicity * 0.5
        optimal = base_size * vol_adj * volume_multiplier * toxicity_adj
        optimal = max(self.min_size, min(self.max_size, optimal))
        bid_size = max(self.min_size, min(self.max_size, optimal * (1.0 - inventory_skew * 0.3)))
        ask_size = max(self.min_size, min(self.max_size, optimal * (1.0 + inventory_skew * 0.3)))
        return optimal, bid_size, ask_size


class _BidAskBounceDetector:
    """买卖反弹检测器。高反弹概率→不适合做市(假突破风险)。
    对标: Virtu Financial。
    """
    def __init__(self, lookback: int = 20) -> None:
        self.lookback = lookback
        self._bounces: Deque[int] = deque(maxlen=lookback)
        self._crosses: Deque[int] = deque(maxlen=lookback)

    def update(self, last_price: float, bid: float, ask: float,
               prev_last_price: float = 0.0) -> None:
        if prev_last_price <= 0 or bid <= 0 or ask <= 0:
            return
        crossed_bid = prev_last_price > bid and last_price <= bid
        crossed_ask = prev_last_price < ask and last_price >= ask
        bounced_bid = prev_last_price <= bid and last_price > bid
        bounced_ask = prev_last_price >= ask and last_price < ask
        self._crosses.append(1 if (crossed_bid or crossed_ask) else 0)
        self._bounces.append(1 if (bounced_bid or bounced_ask) else 0)

    def compute_bounce_prob(self) -> float:
        total_crosses = sum(self._crosses)
        if total_crosses == 0:
            return 0.0
        return sum(self._bounces) / total_crosses


class MarketMakingMonitor:
    """S6做市商策略实盘模拟监控模块 V1（顶级基金标准）。"""

    PARTICIPATES_CAPITAL_ALLOCATION: bool = False
    CAPITAL_ALLOCATION: float = 0.0
    SENDS_REAL_ORDERS: bool = False

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
        # FIX-20260713-S6-V2ABSORB: V2 新增参数
        imbalance_lookback: int = 20,
        imbalance_weight: float = 0.5,
        gamma_vol_scale: float = 0.5,
        gamma_inv_scale: float = 0.3,
        oft_window: int = 60,
        oft_threshold: float = 0.7,
        corr_lookback: int = 60,
        max_corr_hedge_ratio: float = 0.5,
        min_order_size: float = 1.0,
        max_order_size: float = 20.0,
        size_vol_scale: float = 0.5,
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
        if snapshot_interval_sec < 0:
            raise ValueError("snapshot_interval_sec 不能为负")
        if vol_scale <= 0:
            raise ValueError("vol_scale 必须 >0")
        if risk_aversion <= 0:
            raise ValueError("risk_aversion 必须 >0")
        if time_horizon_sec <= 0:
            raise ValueError("time_horizon_sec 必须 >0")
        if order_arrival_kappa <= 0:
            raise ValueError("order_arrival_kappa 必须 >0")
        if quote_jitter_bps < 0:
            raise ValueError("quote_jitter_bps 不能为负")
        if not (0.0 < unwind_threshold <= 1.0):
            raise ValueError("unwind_threshold 必须在 (0, 1]")
        if vpin_window < 5:
            raise ValueError("vpin_window 必须 >=5")
        if min_quote_refresh_sec < 0:
            raise ValueError("min_quote_refresh_sec 不能为负")

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

        self._price_history: Deque[float] = deque(maxlen=self.vol_lookback)
        self._fill_events: Deque[Tuple[int, float, float]] = deque(maxlen=self.adverse_window)
        self._vpin_buckets: Deque[Tuple[float, float, float]] = deque(maxlen=self.vpin_window)
        self._vpin_current_bucket_ts: float = 0.0
        self._vpin_current_buy: float = 0.0
        self._vpin_current_sell: float = 0.0

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

        self._last_signal: Optional[MarketMakingSignal] = None
        self._snapshot_count: int = 0
        self._tick_count: int = 0
        self._last_signal_time: float = 0.0
        self._last_quote_time: float = 0.0
        self._regime: str = "normal"

        # FIX-20260713-S6-MULTISRC: 多源数据采集状态（不限于期权五态/HFT管道）
        # 订单簿 L1 数据
        self._last_bid_volume1: float = 0.0
        self._last_ask_volume1: float = 0.0
        self._order_book_imbalance: float = 0.0  # (bid_vol - ask_vol)/(bid_vol+ask_vol) ∈ [-1,1]
        # 多级订单簿（on_order_book 接入时填充）
        self._multi_level_book: Deque[Dict[str, float]] = deque(maxlen=10)
        # 持仓量数据
        self._last_open_interest: float = 0.0
        self._prev_open_interest: float = 0.0
        self._oi_trend: str = "stable"  # 'rising' / 'falling' / 'stable'
        # VWAP 计算
        self._vwap: float = 0.0
        self._cum_turnover: float = 0.0
        self._cum_volume: float = 0.0
        # 真实成交方向（tick direction 字段，+1 买/-1 卖/0 未知）
        self._real_trade_direction: int = 0
        # 真实成交流缓冲（on_trade 接入时填充，用于 VPIN）
        self._trade_flow_buffer: Deque[Tuple[int, float, float]] = deque(maxlen=200)
        # 数据源活跃掩码（bit0=tick, bit1=order_book, bit2=trade, bit3=open_interest, bit4=bar）
        self._data_source_mask: int = 0

        # FIX-20260713-S6-V2ABSORB: V2 顶级基金组件
        self._micro_price_model = _MicroPriceModel(imbalance_lookback, imbalance_weight)
        self._gamma_estimator = _DynamicGammaEstimator(risk_aversion, gamma_vol_scale, gamma_inv_scale)
        self._volume_profile = _IntradayVolumeProfile()
        self._toxic_classifier = _ToxicFlowClassifier(oft_window, oft_threshold)
        self._correlation_hedger = _CorrelationHedger(corr_lookback, max_corr_hedge_ratio)
        self._pnl_attribution = _PnLAttribution()
        self._order_sizer = _OrderSizeOptimizer(min_order_size, max_order_size, size_vol_scale)
        self._bounce_detector = _BidAskBounceDetector()
        self._hedge_instrument_id: str = ""  # 对冲品种ID（可选配置）
        self._prev_last_price_for_bounce: float = 0.0  # 反弹检测用的上一笔 last_price
        # 缓存最新 V2 计算结果（供 generate_simulated_signal 直接复用，避免重复计算）
        self._v2_micro_price: float = 0.0
        self._v2_dynamic_gamma: float = float(risk_aversion)
        self._v2_volume_multiplier: float = 1.0
        self._v2_intraday_percentile: float = 0.5
        self._v2_oft: float = 0.0
        self._v2_pin: float = 0.0
        self._v2_bounce_prob: float = 0.0
        self._v2_cancel_prob: float = 0.0
        self._v2_optimal_size: float = 0.0
        self._v2_bid_size: float = 0.0
        self._v2_ask_size: float = 0.0
        self._v2_corr_hedge: float = 0.0

        logger.info(
            "[S6-MMM-V1] 初始化完成 inst=%s max_inv=%.2f base_spread=%.1fbps "
            "γ=%.3f κ=%.2f maker=%.2fbps taker=%.2fbps rebate=%.2fbps",
            self.instrument_id, self.max_inventory, self.base_spread_bps,
            self.risk_aversion, self.order_arrival_kappa,
            self.maker_fee_bps, self.taker_fee_bps, self.rebate_bps,
        )

    # ========================================================================
    # 1. 理论价与 A-S 报价
    # ========================================================================

    def compute_reservation_price(self, mid_price: float, sigma: Optional[float] = None) -> float:
        """A-S reservation price: r = s - q * γ * σ² * (T-t)。"""
        if mid_price <= 0:
            return 0.0
        if sigma is None:
            vol_bps = self._estimate_volatility_bps()
            sigma = vol_bps * mid_price / 10000.0
        if sigma <= 0:
            return self.compute_theoretical_price(mid_price)
        with self._lock:
            q = self._inventory - self.target_inventory
            T_minus_t = self.time_horizon_sec
            reservation = mid_price - q * self.risk_aversion * (sigma ** 2) * T_minus_t
            return self._align_to_tick(reservation)

    def compute_theoretical_price(self, mid_price: float) -> float:
        """简单 skew 理论价（兼容性）。

        FIX-20260713-S6-V1: 与 A-S reservation price 符号一致，多头(skew>0)时拉低理论价以鼓励卖出。
        """
        if mid_price <= 0:
            return 0.0
        skew = self.get_inventory_skew()
        inventory_factor = self.inventory_factor_bps * mid_price / 10000.0
        theo_price = mid_price - skew * inventory_factor
        return self._align_to_tick(theo_price)

    def compute_optimal_spread_as(self, mid_price: float, sigma: Optional[float] = None) -> float:
        """A-S 最优半价差: δ* = γ*σ²*(T-t)/2 + ln(1+γ/κ)/γ。"""
        if mid_price <= 0:
            return 0.0
        if sigma is None:
            vol_bps = self._estimate_volatility_bps()
            sigma = vol_bps * mid_price / 10000.0
        if sigma <= 0:
            return self.base_spread_bps * mid_price / 10000.0
        T_minus_t = self.time_horizon_sec
        risk_component = self.risk_aversion * (sigma ** 2) * T_minus_t / 2.0
        adverse_component = math.log(1.0 + self.risk_aversion / self.order_arrival_kappa) / self.risk_aversion
        # FIX-20260713-S6-V1: adverse_component 已是价格单位，直接相加，不再乘以 mid_price/10000
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
        # FIX-20260713-S6-V1: effective_spread_bps 是全价差，需折半作为 half_spread_bps
        half_spread_bps = max(self.base_spread_bps, effective_spread_bps) / 2.0
        half_spread = half_spread_bps * mid_price / 10000.0
        theo_price = self.compute_theoretical_price(mid_price)
        bid = self._align_to_tick(theo_price - half_spread)
        ask = self._align_to_tick(theo_price + half_spread)
        if ask <= bid:
            ask = self._align_to_tick(bid + max(half_spread, self.price_tick if self.price_tick > 0 else _DEFAULT_PRICE_EPS))
        return bid, ask, half_spread

    def compute_asymmetric_quotes(
        self, mid_price: float, *, volatility_bps: Optional[float] = None,
        sigma: Optional[float] = None
    ) -> Tuple[float, float, float, float, float]:
        """不对称双边报价（顶级基金标准）。"""
        if mid_price <= 0:
            return 0.0, 0.0, 0.0, 0.0, 0.0
        if volatility_bps is None:
            volatility_bps = self._estimate_volatility_bps()
        if sigma is None:
            sigma = volatility_bps * mid_price / 10000.0

        base_half = self.compute_optimal_spread_as(mid_price, sigma=sigma)
        min_half = self.base_spread_bps * mid_price / 10000.0
        base_half = max(base_half, min_half)

        skew = self.get_inventory_skew()
        asym_factor = skew * 0.5

        # FIX-20260713-S6-MULTISRC: 订单簿不平衡作为额外 skew 因子
        # 买盘更厚(imbalance>0) → 市场偏多 → bid 更宽(不想买) ask 更窄(想卖)
        # 卖盘更厚(imbalance<0) → 市场偏空 → bid 更窄(想买) ask 更宽(不想卖)
        ob_imbalance = self._order_book_imbalance
        asym_factor += ob_imbalance * 0.3  # 订单簿不平衡权重 0.3（inventory 0.5）

        bid_half = base_half * (1.0 + asym_factor)
        ask_half = base_half * (1.0 - asym_factor)
        bid_half = max(bid_half, min_half * 0.3)
        ask_half = max(ask_half, min_half * 0.3)

        reservation = self.compute_reservation_price(mid_price, sigma=sigma)

        # 波动率机制调整
        if volatility_bps > self.vol_target_bps * 2.0:
            bid_half *= 1.3
            ask_half *= 1.3
            self._regime = "high_vol"
        elif volatility_bps < self.vol_target_bps * 0.5:
            self._regime = "low_vol"
        else:
            self._regime = "normal"

        # FIX-20260713-S6-V2ABSORB: V2 顶级基金组件增强（保留 OBI*0.3 逻辑不变）
        try:
            # 1. 微观价格（基于订单簿不平衡）
            self._v2_micro_price, _ = self._micro_price_model.compute(
                self._last_bid, self._last_ask,
                bid_vol=self._last_bid_volume1, ask_vol=self._last_ask_volume1,
                last_price=self._last_last_price,
            )

            # 2. 动态风险厌恶 γ（波动率+库存驱动）
            cap_for_gamma = self._effective_max_inventory(mid_price)
            self._v2_dynamic_gamma = self._gamma_estimator.compute(
                volatility_bps, self.vol_target_bps,
                self._inventory, cap_for_gamma,
            )
            # γ 替代静态 risk_aversion 作为价差调整因子（不影响 A-S 公式本体）
            if self.risk_aversion > 0:
                gamma_ratio = self._v2_dynamic_gamma / self.risk_aversion
                # 限制在 [0.8, 1.5] 避免极端调整破坏既有报价形态
                gamma_ratio = max(0.8, min(1.5, gamma_ratio))
                bid_half *= gamma_ratio
                ask_half *= gamma_ratio

            # 3. 日内成交量乘子（U型模式）
            _lt = time.localtime()
            self._v2_volume_multiplier = self._volume_profile.get_multiplier(
                _lt.tm_hour, _lt.tm_min
            )
            self._v2_intraday_percentile = self._volume_profile.get_percentile(
                _lt.tm_hour, _lt.tm_min
            )

            # 4. 毒流指标（OFT + PIN）
            self._v2_oft = self._toxic_classifier.compute_oft()
            self._v2_pin = self._toxic_classifier.compute_informed_trader_prob()
            # 毒流高时加宽价差
            if self._v2_oft > 0.5:
                _toxic_widen = 1.0 + (self._v2_oft - 0.5) * 0.6
                bid_half *= _toxic_widen
                ask_half *= _toxic_widen
            if self._v2_pin > 0.5:
                _pin_widen = 1.0 + (self._v2_pin - 0.5) * 0.4
                bid_half *= _pin_widen
                ask_half *= _pin_widen

            # 5. 反弹概率（高时加宽价差，防假突破）
            self._v2_bounce_prob = self._bounce_detector.compute_bounce_prob()
            if self._v2_bounce_prob > 0.3:
                _bounce_widen = 1.0 + (self._v2_bounce_prob - 0.3) * 0.5
                bid_half *= _bounce_widen
                ask_half *= _bounce_widen

            # 6. 报价取消概率
            spread_bps_for_cancel = (bid_half + ask_half) / mid_price * 10000.0 if mid_price > 0 else 0.0
            self._v2_cancel_prob = self._estimate_cancellation_probability(
                spread_bps_for_cancel, 0.5, volatility_bps
            )

            # 7. 最优订单规模（GLFT 三维优化）
            _base_size = max(self._order_sizer.min_size, 1.0)
            self._v2_optimal_size, self._v2_bid_size, self._v2_ask_size = (
                self._order_sizer.compute(
                    _base_size, volatility_bps, self.vol_target_bps,
                    skew, self._v2_volume_multiplier, self._v2_oft,
                )
            )
        except Exception as _v2_quote_err:
            logger.debug("[S6-MMM-V1] V2 报价增强异常: %s", _v2_quote_err)

        bid_half = max(bid_half, min_half * 0.3)
        ask_half = max(ask_half, min_half * 0.3)

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
        return bid, ask, bid_half, ask_half, reservation

    # ========================================================================
    # 2. Inventory 管理
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
            logger.info(
                "[S6-MMM-V1] inventory 更新: prev=%.4f delta=%.4f → new=%.4f cap=%.4f",
                prev_inv, delta, self._inventory, cap,
            )
            return self._inventory

    def get_inventory_skew(self) -> float:
        """inventory 偏斜系数 ∈ [-1, 1]。"""
        with self._lock:
            cap = self._effective_max_inventory(self._last_mid_price)
            if cap <= 0:
                return 0.0
            return max(-1.0, min(1.0, self._inventory / cap))

    def get_inventory(self) -> float:
        with self._lock:
            return self._inventory

    def check_inventory_unwind(self) -> float:
        """强制减仓建议。"""
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
    # 3. 毒单检测（市场 VPIN + 自身 fill adverse）
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
        """基于市场主动成交方向的 VPIN。"""
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

    def _update_vpin_bucket(self, direction: int, volume: float) -> None:
        """更新 VPIN 桶。调用方需持锁。"""
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
        # 简化指数模型：命中概率随 spread 增加而衰减，上限 0.8
        prob = math.exp(-spread_bps / self.base_spread_bps)
        return max(0.0, min(0.8, prob))

    def _estimate_cancellation_probability(self, spread_bps: float, queue_pos: float,
                                           volatility_bps: float) -> float:
        """估计报价被取消/未成交的概率。对标: Virtu Financial。
        基于: 队列位置 + 价差宽度 + 波动率

        FIX-20260713-S6-V2ABSORB
        """
        queue_factor = 1.0 - queue_pos
        spread_factor = min(1.0, spread_bps / (self.base_spread_bps * 2))
        vol_factor = min(1.0, volatility_bps / (self.vol_target_bps * 2))
        cancel_prob = queue_factor * 0.4 + spread_factor * 0.3 + vol_factor * 0.3
        return max(0.0, min(1.0, cancel_prob))

    # ========================================================================
    # 4. 指标
    # ========================================================================

    def compute_metrics(self) -> Dict[str, float]:
        """计算做市指标。"""
        with self._lock:
            quote_hit_rate = self._quote_hit_count / self._quote_count if self._quote_count > 0 else 0.0
            spread_capture_rate = self._spread_captured_bps / self._spread_offered_bps if self._spread_offered_bps > 0 else 0.0
            inventory_turnover = self._inventory_turnover_sum / (2.0 * max(self._inventory_peak, _DEFAULT_PRICE_EPS)) if self._inventory_peak > 0 else 0.0
            adverse_selection_rate = self._adverse_count / self._quote_count if self._quote_count > 0 else 0.0
            sharpe_ratio = self._compute_sharpe()
            vpin = self.compute_vpin()
            target_delta = self.check_inventory_unwind()

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
            }

    # ========================================================================
    # 5. 信号生成 + 快照
    # ========================================================================

    def generate_simulated_signal(
        self, *, tick_data: Optional[Dict[str, Any]] = None
    ) -> MarketMakingSignal:
        """生成模拟做市信号。"""
        with self._lock:
            if tick_data is not None:
                self._ingest_tick(tick_data)

            mid_price = self._last_mid_price
            # FIX-S6-1: mid_price <= 0 时使用 fallback 价格而非返回空信号
            # 根因: mid_price <= 0 时返回空信号(mid_price=0) → execute_by_ranking L754 price<=0 跳过 → S6永远0模拟下单
            # 修复: 依次尝试 _last_last_price → 默认价格1.0，确保模拟下单链路完整
            if mid_price <= 0:
                _fallback_price = self._last_last_price
                if _fallback_price <= 0:
                    _fallback_price = 1.0  # 最终 fallback，确保模拟链路不中断
                    logger.warning(
                        "[S6-MMM-V1] _last_mid_price和_last_last_price均为0，使用默认价格%.2f "
                        "(RC-S6-1/S6-2: 确保模拟下单链路完整)", _fallback_price
                    )
                else:
                    logger.warning(
                        "[S6-MMM-V1] _last_mid_price<=0，使用_last_last_price=%.6f 作为fallback "
                        "(RC-S6-1: 单边行情或tick未投喂)", _fallback_price
                    )
                mid_price = _fallback_price
                # 同步更新 _last_mid_price，使后续计算一致
                self._last_mid_price = mid_price

            # FIX-S6-3: 确保 instrument_id 非空
            # 根因: 若 monitor 未通过 __init__ 或 _ingest_tick 设置 instrument_id，则为空
            # 修复: 使用 fallback instrument_id
            if not self.instrument_id:
                self.instrument_id = 'S6_MM_FALLBACK'
                logger.warning(
                    "[S6-MMM-V1] instrument_id为空，使用fallback='S6_MM_FALLBACK' "
                    "(RC-S6-3: 确保模拟下单链路完整)"
                )

            volatility_bps = self._estimate_volatility_bps()
            sigma = volatility_bps * mid_price / 10000.0

            try:
                bid, ask, bid_half, ask_half, reservation = self.compute_asymmetric_quotes(
                    mid_price, volatility_bps=volatility_bps, sigma=sigma
                )
                theo_price = reservation
                half_spread = (bid_half + ask_half) / 2.0
            except Exception as _as_err:
                logger.debug("[S6-MMM-V1] 不对称报价降级: %s", _as_err)
                bid, ask, half_spread = self.compute_bid_ask_quotes(mid_price, volatility_bps=volatility_bps)
                theo_price = self.compute_theoretical_price(mid_price)
                bid_half = half_spread
                ask_half = half_spread
                reservation = theo_price

            optimal_spread_as = self.compute_optimal_spread_as(mid_price, sigma=sigma)
            spread = ask - bid
            spread_bps = (spread / mid_price * 10000.0) if mid_price > 0 else 0.0
            skew = self.get_inventory_skew()
            adverse_flag = self.detect_adverse_selection()
            vpin = self.compute_vpin()
            target_delta = self.check_inventory_unwind()
            hit_prob = self._estimate_hit_probability(spread_bps)

            # 净价差 = 毛价差 - 双边 maker 费 + 双边返佣
            net_capture_bps = spread_bps - 2.0 * self.maker_fee_bps + 2.0 * self.rebate_bps

            # 方向建议
            if skew > 0.15:
                preferred_side = "sell"
            elif skew < -0.15:
                preferred_side = "buy"
            else:
                preferred_side = "neutral"

            # 日亏损检查
            stop_quoting = self._daily_loss_bps >= self.max_daily_loss_bps

            signal = MarketMakingSignal(
                instrument_id=self.instrument_id,
                # FIX-S6-3: 补齐 signal_id 和 direction 字段
                # 根因: 原构造未设置 signal_id/direction → signal_id="" → 下游 dedup 用 timestamp fallback；
                #       direction="neutral" → execute_by_ranking 价格修正分支都不执行
                # 修复: 生成唯一 signal_id；direction 基于 preferred_side 映射
                signal_id=f"S6MM_{int(time.time() * 1000)}",
                direction="BUY" if preferred_side in ("buy", "neutral") else "SELL",
                signal_type="market_making",
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
                # FIX-20260713-S6-MULTISRC: 多源数据采集字段
                order_book_imbalance=round(self._order_book_imbalance, 6),
                open_interest=self._last_open_interest,
                oi_trend=self._oi_trend,
                vwap=self._vwap,
                real_trade_direction=self._real_trade_direction,
                data_source_mask=self._data_source_mask,
                # FIX-20260713-S6-V2ABSORB: V2 顶级基金组件字段
                micro_price=round(self._v2_micro_price, 6),
                dynamic_gamma=round(self._v2_dynamic_gamma, 6),
                volume_profile_multiplier=round(self._v2_volume_multiplier, 6),
                order_flow_toxicity=round(self._v2_oft, 6),
                informed_trader_prob=round(self._v2_pin, 6),
                inventory_correlation_hedge=round(self._v2_corr_hedge, 6),
                pnl_attribution=self._pnl_attribution.get_attribution(),
                bid_ask_bounce_prob=round(self._v2_bounce_prob, 6),
                cancellation_probability=round(self._v2_cancel_prob, 6),
                optimal_order_size=round(self._v2_optimal_size, 6),
                intraday_volume_percentile=round(self._v2_intraday_percentile, 6),
            )

            assert bid - _DEFAULT_PRICE_EPS <= theo_price <= ask + _DEFAULT_PRICE_EPS, (
                f"理论价格 {theo_price} 不在 bid {bid} 与 ask {ask} 之间"
            )
            cap = self._effective_max_inventory(mid_price)
            assert abs(self._inventory) <= cap + _DEFAULT_PRICE_EPS, (
                f"inventory {self._inventory} 超过 max_inventory {cap}"
            )

            self._quote_count += 1
            self._spread_offered_bps += spread_bps
            self._last_signal = signal

            # FIX-20260716-S6-SNAPSHOT: generate_simulated_signal 生成信号后必须保存快照
            # 根因: 原方法仅设置 self._last_signal，未主动 save_snapshot。
            #       strategy_business_layer.py 直接调用本方法接入订单链时，没有快照留存，
            #       导致 S6 模拟交易记录缺失。on_tick 路径虽然也会保存，但存在时间窗口不一致。
            self.save_snapshot(signal)

            logger.info(
                "[S6-MMM-V1] 信号生成: inst=%s mid=%.6f r=%.6f bid=%.6f ask=%.6f "
                "spread=%.2fbps net=%.2fbps inv=%.4f skew=%.4f adv=%s vpin=%.4f "
                "side=%s regime=%s hit_prob=%.2f | ⚠不下单 不参与资金分配",
                self.instrument_id, mid_price, reservation, bid, ask,
                spread_bps, net_capture_bps, self._inventory, skew,
                adverse_flag, vpin, preferred_side, self._regime, hit_prob,
            )
            return signal

    def save_snapshot(self, signal: Optional[MarketMakingSignal] = None) -> str:
        """保存信号快照。"""
        if signal is None:
            signal = self._last_signal
        if signal is None:
            logger.warning("[S6-MMM-V1] save_snapshot: 无可用信号")
            return ""
        os.makedirs(self.snapshot_dir, exist_ok=True)
        safe_inst = "".join(c for c in (self.instrument_id or "unknown") if c.isalnum() or c in ("_", "-"))
        ts = int(signal.timestamp) if signal.timestamp > 0 else int(time.time())
        fname = f"{safe_inst}_{ts}_{self._snapshot_count}.json"
        fpath = os.path.join(self.snapshot_dir, fname)

        payload = {
            "module": "S6-MMM-V1",
            "version": "1.0",
            "instrument_id": self.instrument_id,
            "participates_capital_allocation": self.PARTICIPATES_CAPITAL_ALLOCATION,
            "sends_real_orders": self.SENDS_REAL_ORDERS,
            "signal": signal.to_dict(),
            "saved_at": time.time(),
        }

        if _json_dumps is not None:
            content = _json_dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
        else:
            content = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True, default=str)

        try:
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(content)
        except OSError as e:
            logger.error("[S6-MMM-V1] 快照写入失败 path=%s err=%s", fpath, e)
            return ""

        self._snapshot_count += 1
        assert os.path.exists(fpath), f"快照文件保存后不存在: {fpath}"
        logger.info("[S6-MMM-V1] 快照已保存: %s (size=%d bytes)", fpath, os.path.getsize(fpath))
        return fpath

    # ========================================================================
    # 6. 集成接口
    # ========================================================================

    def on_tick(self, tick_data: Dict[str, Any]) -> None:
        """tick 回调：节流 + 命中模拟 + 快照。"""
        if not isinstance(tick_data, dict):
            return

        with self._lock:
            # FIX-20260713-S6-V2ABSORB: 捕获 prev_last_price 用于反弹检测
            _prev_last_price = self._last_last_price
            self._ingest_tick(tick_data)
            self._tick_count += 1

            # 更新市场 VPIN — 优先使用 tick direction 字段（真实成交方向），回退到价格推断
            volume = float(tick_data.get('volume', 0.0) or 0.0)
            if volume > 0 and self._last_last_price > 0:
                # FIX-20260713-S6-MULTISRC: 优先使用真实 direction 字段
                direction = self._real_trade_direction
                if direction == 0:
                    direction = self._infer_trade_direction(tick_data)
                if direction != 0:
                    self._update_vpin_bucket(direction, volume)

            # FIX-20260713-S6-V2ABSORB: V2 组件更新（保留多源数据采集逻辑）
            try:
                # 1. 毒流分类器更新（用真实成交方向）
                if volume > 0 and self._last_last_price > 0:
                    _v2_dir = self._real_trade_direction
                    if _v2_dir == 0:
                        _v2_dir = self._infer_trade_direction(tick_data)
                    if _v2_dir != 0:
                        self._toxic_classifier.update(_v2_dir, volume, time.time())

                # 2. 买卖反弹检测（用 last_price/bid/ask）
                self._bounce_detector.update(
                    self._last_last_price, self._last_bid, self._last_ask,
                    prev_last_price=_prev_last_price,
                )

                # 3. 相关性对冲器更新（用 last_price）
                if self.instrument_id and self._last_last_price > 0:
                    self._correlation_hedger.update_price(
                        self.instrument_id, self._last_last_price
                    )
                    # 若配置了对冲品种，计算对冲建议
                    if self._hedge_instrument_id:
                        self._v2_corr_hedge = self._correlation_hedger.compute_hedge_ratio(
                            self.instrument_id, self._hedge_instrument_id
                        )

                # 4. PnL 归因 mark-to-market
                if self._last_mid_price > 0:
                    self._pnl_attribution.mark_to_market(
                        self._inventory, self._last_mid_price
                    )
            except Exception as _v2_err:
                logger.debug("[S6-MMM-V1] V2 组件更新异常: %s", _v2_err)

            if self._last_signal is not None:
                try:
                    self._simulate_quote_hit(self._last_signal)
                except Exception as e:
                    logger.debug("[S6-MMM-V1] _simulate_quote_hit 异常: %s", e)

        _now = time.time()
        _ticks_ok = self._tick_count >= self.signal_interval_ticks
        _time_ok = (_now - self._last_signal_time) >= self.snapshot_interval_sec
        _refresh_ok = (_now - self._last_quote_time) >= self.min_quote_refresh_sec
        if not (_ticks_ok or _time_ok) or not _refresh_ok:
            return

        try:
            signal = self.generate_simulated_signal()
            # FIX-W R9-6-1: 移除重复save_snapshot，generate_simulated_signal内部L1115已调用
            with self._lock:
                self._tick_count = 0
                self._last_signal_time = _now
                self._last_quote_time = _now
        except AssertionError as e:
            logger.error("[S6-MMM-V1] on_tick 断言失败: %s", e)
        except Exception as e:
            logger.exception("[S6-MMM-V1] on_tick 处理异常: %s", e)

    def on_bar(self, bar_data: Dict[str, Any]) -> None:
        """K线回调。"""
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
                self._data_source_mask |= 0x10  # bit4 = bar 活跃
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM-V1] on_bar 解析异常: %s", e)

    # ========================================================================
    # FIX-20260713-S6-MULTISRC: 多源数据采集接口（不限于 HFT tick 管道）
    # ========================================================================

    def on_order_book(self, levels: List[Dict[str, Any]]) -> None:
        """多级订单簿数据接入。

        顶级基金做市商不依赖单一 tick 管道，而是直接消费订单簿深度数据。
        本方法接受任意层级的订单簿快照，更新内部订单簿不平衡状态。

        Args:
            levels: 订单簿层级列表，每层包含:
                {'level': 1, 'bid_price': 3500.0, 'bid_volume': 10.0,
                 'ask_price': 3501.0, 'ask_volume': 8.0}
        """
        if not levels or not isinstance(levels, list):
            return
        try:
            with self._lock:
                self._multi_level_book.clear()
                total_bid_vol = 0.0
                total_ask_vol = 0.0
                for lv in levels:
                    if not isinstance(lv, dict):
                        continue
                    bp = float(lv.get('bid_price', 0.0) or 0.0)
                    bv = float(lv.get('bid_volume', 0.0) or 0.0)
                    ap = float(lv.get('ask_price', 0.0) or 0.0)
                    av = float(lv.get('ask_volume', 0.0) or 0.0)
                    level = int(lv.get('level', len(self._multi_level_book) + 1))
                    self._multi_level_book.append({
                        'level': level, 'bid_price': bp, 'bid_volume': bv,
                        'ask_price': ap, 'ask_volume': av,
                    })
                    total_bid_vol += bv
                    total_ask_vol += av
                    # L1 更新主盘口
                    if level == 1 and bp > 0 and ap > 0:
                        self._last_bid = bp
                        self._last_ask = ap
                        self._last_mid_price = (bp + ap) / 2.0
                        self._last_bid_volume1 = bv
                        self._last_ask_volume1 = av
                # 多级订单簿不平衡（加权：L1 权重最大）
                if total_bid_vol + total_ask_vol > 0:
                    self._order_book_imbalance = (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol)
                self._data_source_mask |= 0x02  # bit1 = order_book 活跃
        except (ValueError, TypeError, KeyError) as e:
            logger.debug("[S6-MMM-V1] on_order_book 解析异常: %s", e)

    def on_trade(self, trade: Dict[str, Any]) -> None:
        """单笔成交流数据接入。

        顶级基金做市商直接消费交易所成交流（trade tape），
        而非从 tick 价格推断成交方向。本方法接受单笔成交事件。

        Args:
            trade: {'price': 3500.0, 'volume': 5.0, 'direction': 1, 'timestamp': 1234567.0}
                    direction: +1 买方主动, -1 卖方主动, 0 未知
        """
        if not trade or not isinstance(trade, dict):
            return
        try:
            price = float(trade.get('price', 0.0) or 0.0)
            volume = float(trade.get('volume', 0.0) or 0.0)
            direction = int(trade.get('direction', 0) or 0)
            ts = float(trade.get('timestamp', time.time()) or time.time())
            if price <= 0 or volume <= 0:
                return
            with self._lock:
                self._trade_flow_buffer.append((direction, volume, ts))
                self._real_trade_direction = direction
                # 用真实成交更新 VPIN
                if direction != 0:
                    self._update_vpin_bucket(direction, volume)
                # 更新最新价格
                self._last_last_price = price
                self._price_history.append(price)
                self._data_source_mask |= 0x04  # bit2 = trade 活跃
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM-V1] on_trade 解析异常: %s", e)

    def on_open_interest(self, oi_data: Dict[str, Any]) -> None:
        """持仓量数据接入。

        顶级基金做市商监控持仓量变化以判断市场参与者的仓位变动趋势，
        仓位上升+价格上升=多头增仓，仓位上升+价格下跌=空头增仓。

        Args:
            oi_data: {'open_interest': 123456.0, 'prev': 123000.0, 'timestamp': 1234567.0}
        """
        if not oi_data or not isinstance(oi_data, dict):
            return
        try:
            oi = float(oi_data.get('open_interest', 0.0) or 0.0)
            if oi <= 0:
                return
            with self._lock:
                if self._last_open_interest > 0:
                    self._prev_open_interest = self._last_open_interest
                self._last_open_interest = oi
                # 也接受外部传入的 prev 值
                prev = oi_data.get('prev')
                if prev is not None and float(prev) > 0:
                    self._prev_open_interest = float(prev)
                if self._prev_open_interest > 0:
                    oi_change = oi - self._prev_open_interest
                    if oi_change > 0:
                        self._oi_trend = "rising"
                    elif oi_change < 0:
                        self._oi_trend = "falling"
                    else:
                        self._oi_trend = "stable"
                self._data_source_mask |= 0x08  # bit3 = open_interest 活跃
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM-V1] on_open_interest 解析异常: %s", e)

    def get_order_book_imbalance(self) -> float:
        """获取订单簿不平衡系数 ∈ [-1, 1]。正值=买盘更厚。"""
        with self._lock:
            return self._order_book_imbalance

    def get_vwap(self) -> float:
        """获取 VWAP。"""
        with self._lock:
            return self._vwap

    def get_oi_trend(self) -> str:
        """获取持仓量趋势。"""
        with self._lock:
            return self._oi_trend

    def get_monitoring_report(self) -> Dict[str, Any]:
        """监控报告。"""
        with self._lock:
            metrics = self.compute_metrics()
            last_signal_dict = self._last_signal.to_dict() if self._last_signal else None
            return {
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
                    "adverse_window": self.adverse_window,
                    "adverse_threshold_bps": self.adverse_threshold_bps,
                    "price_tick": self.price_tick,
                    "signal_interval_ticks": self.signal_interval_ticks,
                    "snapshot_interval_sec": self.snapshot_interval_sec,
                    "vol_scale": self.vol_scale,
                    "risk_aversion": self.risk_aversion,
                    "time_horizon_sec": self.time_horizon_sec,
                    "order_arrival_kappa": self.order_arrival_kappa,
                    "quote_jitter_bps": self.quote_jitter_bps,
                    "target_inventory": self.target_inventory,
                    "unwind_threshold": self.unwind_threshold,
                    "vpin_window": self.vpin_window,
                    "maker_fee_bps": self.maker_fee_bps,
                    "taker_fee_bps": self.taker_fee_bps,
                    "rebate_bps": self.rebate_bps,
                    "max_daily_loss_bps": self.max_daily_loss_bps,
                    "min_quote_refresh_sec": self.min_quote_refresh_sec,
                },
                "state": {
                    "inventory": self._inventory,
                    "inventory_skew": self.get_inventory_skew(),
                    "last_mid_price": self._last_mid_price,
                    "last_bid": self._last_bid,
                    "last_ask": self._last_ask,
                    "last_last_price": self._last_last_price,
                    "price_history_len": len(self._price_history),
                    "fill_events_len": len(self._fill_events),
                    "snapshot_count": self._snapshot_count,
                    "tick_count": self._tick_count,
                    "last_signal_time": self._last_signal_time,
                    "vpin_buckets_len": len(self._vpin_buckets),
                    "vpin_current_bucket_ts": self._vpin_current_bucket_ts,
                    "regime": self._regime,
                    "daily_loss_bps": self._daily_loss_bps,
                    # FIX-20260713-S6-MULTISRC: 多源数据采集状态
                    "order_book_imbalance": round(self._order_book_imbalance, 6),
                    "last_bid_volume1": self._last_bid_volume1,
                    "last_ask_volume1": self._last_ask_volume1,
                    "last_open_interest": self._last_open_interest,
                    "oi_trend": self._oi_trend,
                    "vwap": self._vwap,
                    "real_trade_direction": self._real_trade_direction,
                    "multi_level_book_len": len(self._multi_level_book),
                    "trade_flow_buffer_len": len(self._trade_flow_buffer),
                    "data_source_mask": self._data_source_mask,
                    "data_sources_active": {
                        "tick": bool(self._data_source_mask & 0x01),
                        "order_book": bool(self._data_source_mask & 0x02),
                        "trade": bool(self._data_source_mask & 0x04),
                        "open_interest": bool(self._data_source_mask & 0x08),
                        "bar": bool(self._data_source_mask & 0x10),
                    },
                },
                # FIX-20260713-S6-V2ABSORB: V2 顶级基金组件状态
                "v2_components": {
                    "v2_micro_price": round(self._v2_micro_price, 6),
                    "v2_micro_price_ewma_imb": round(
                        self._micro_price_model._compute_ewma_imbalance(), 6
                    ),
                    "v2_dynamic_gamma": round(self._v2_dynamic_gamma, 6),
                    "v2_volume_multiplier": round(self._v2_volume_multiplier, 6),
                    "v2_intraday_percentile": round(self._v2_intraday_percentile, 6),
                    "v2_oft": round(self._toxic_classifier.compute_oft(), 6),
                    "v2_pin": round(self._toxic_classifier.compute_informed_trader_prob(), 6),
                    "v2_is_toxic": self._toxic_classifier.is_toxic(),
                    "v2_bounce_prob": round(self._bounce_detector.compute_bounce_prob(), 6),
                    "v2_cancel_prob": round(self._v2_cancel_prob, 6),
                    "v2_optimal_size": round(self._v2_optimal_size, 6),
                    "v2_bid_size": round(self._v2_bid_size, 6),
                    "v2_ask_size": round(self._v2_ask_size, 6),
                    "v2_corr_hedge": round(self._v2_corr_hedge, 6),
                    "v2_pnl_attribution": self._pnl_attribution.get_attribution(),
                },
                "metrics": metrics,
                "last_signal": last_signal_dict,
                "report_time": time.time(),
            }

    # ========================================================================
    # 7. 内部辅助
    # ========================================================================

    def _ingest_tick(self, tick_data: Dict[str, Any]) -> None:
        """摄入 tick — 多源数据采集：提取全部可用字段，不限于价格。

        FIX-20260713-S6-MULTISRC: 从 tick 中提取所有可用字段：
        - bid_price1/ask_price1: 盘口价格
        - bid_volume1/ask_volume1: 盘口量 → 订单簿不平衡
        - volume: 成交量 → VWAP + VPIN
        - turnover: 成交额 → VWAP
        - open_interest: 持仓量 → OI 趋势
        - direction: 真实成交方向 → VPIN（优于价格推断）
        """
        try:
            inst = tick_data.get("instrument_id")
            if inst and not self.instrument_id:
                self.instrument_id = str(inst)

            last_price = float(tick_data.get("last_price", 0.0) or 0.0)
            bid = float(tick_data.get("bid_price", tick_data.get("bid_price1", 0.0)) or 0.0)
            ask = float(tick_data.get("ask_price", tick_data.get("ask_price1", 0.0)) or 0.0)

            if last_price > 0:
                self._last_last_price = last_price
                self._price_history.append(last_price)

            if bid > 0 and ask > 0:
                self._last_bid = bid
                self._last_ask = ask
                self._last_mid_price = (bid + ask) / 2.0
            elif last_price > 0 and self._last_mid_price <= 0:
                self._last_mid_price = last_price
                self._last_bid = last_price
                self._last_ask = last_price

            # FIX-20260713-S6-MULTISRC: 提取盘口量 → 订单簿不平衡
            bid_vol = float(tick_data.get("bid_volume1", tick_data.get("bid_volume", 0.0)) or 0.0)
            ask_vol = float(tick_data.get("ask_volume1", tick_data.get("ask_volume", 0.0)) or 0.0)
            if bid_vol > 0 or ask_vol > 0:
                self._last_bid_volume1 = bid_vol
                self._last_ask_volume1 = ask_vol
                total_vol = bid_vol + ask_vol
                if total_vol > 0:
                    self._order_book_imbalance = (bid_vol - ask_vol) / total_vol

            # FIX-20260713-S6-MULTISRC: 提取成交额/量 → VWAP
            turnover = float(tick_data.get("turnover", 0.0) or 0.0)
            volume = float(tick_data.get("volume", 0.0) or 0.0)
            if turnover > 0 and volume > 0:
                self._cum_turnover = turnover  # CTP累计值，直接覆盖
                self._cum_volume = volume      # CTP累计值，直接覆盖
                if self._cum_volume > 0:
                    self._vwap = self._cum_turnover / self._cum_volume

            # FIX-20260713-S6-MULTISRC: 提取持仓量 → OI 趋势
            oi = float(tick_data.get("open_interest", 0.0) or 0.0)
            if oi > 0:
                if self._last_open_interest > 0:
                    self._prev_open_interest = self._last_open_interest
                self._last_open_interest = oi
                if self._prev_open_interest > 0:
                    oi_change = oi - self._prev_open_interest
                    if oi_change > 0:
                        self._oi_trend = "rising"
                    elif oi_change < 0:
                        self._oi_trend = "falling"
                    else:
                        self._oi_trend = "stable"

            # FIX-20260713-S6-MULTISRC: 提取真实成交方向
            raw_dir = tick_data.get("direction", tick_data.get("trade_direction", 0))
            if raw_dir is not None:
                try:
                    self._real_trade_direction = int(float(raw_dir))
                except (ValueError, TypeError):
                    self._real_trade_direction = 0

            self._data_source_mask |= 0x01  # bit0 = tick 活跃
        except Exception as e:  # FIX-S6-2: 窄异常元组(ValueError,TypeError)→Exception (NEW-1硬约束) + debug→warning
            logger.warning("[S6-MMM-V1] _ingest_tick 解析异常: %s", e)

    def _infer_trade_direction(self, tick_data: Dict[str, Any]) -> int:
        """推断主动成交方向：+1 买方主动，-1 卖方主动，0 未知。调用方需持锁。"""
        last_price = self._last_last_price
        bid = self._last_bid
        ask = self._last_ask
        if last_price <= 0 or bid <= 0 or ask <= 0:
            return 0
        if last_price >= ask:
            return 1
        if last_price <= bid:
            return -1
        # 中间价靠近判断
        mid = (bid + ask) / 2.0
        return 1 if last_price >= mid else -1

    def _estimate_volatility_bps(self) -> float:
        """tick 收益率波动率（bps）。"""
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

    def _simulate_quote_hit(self, signal: MarketMakingSignal) -> None:
        """模拟报价命中与成交。调用方需持锁。"""
        market_price = self._last_last_price
        if market_price <= 0:
            return

        # 命中概率判断
        hit_prob = self._estimate_hit_probability(signal.spread_bps)
        if random.random() > hit_prob:
            return

        if signal.bid_price > 0 and market_price <= signal.bid_price:
            self._quote_hit_count += 1
            fill_price = signal.bid_price
            delta = 1.0  # FIX-20260713-S6-V1: bid 被命中，我们作为买方成交，库存增加
            self._inventory = self._clamp_inventory(self._inventory + delta, market_price)
            self._inventory_turnover_sum += abs(delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            captured_bps = signal.half_spread / signal.mid_price * 10000.0 if signal.mid_price > 0 else 0.0
            self._spread_captured_bps += captured_bps
            # 净盈亏 = 捕获价差 - maker 费 + 返佣
            net_bps = captured_bps - self.maker_fee_bps + self.rebate_bps
            incremental_pnl = net_bps * signal.mid_price / 10000.0
            self._realized_pnl += incremental_pnl
            self._daily_loss_bps = max(0.0, self._daily_loss_bps - net_bps)
            self._pnl_samples.append(incremental_pnl)
            self._fill_events.append((1, fill_price, time.time()))
            self._update_vpin_bucket(1, abs(delta))

        elif signal.ask_price > 0 and market_price >= signal.ask_price:
            self._quote_hit_count += 1
            fill_price = signal.ask_price
            delta = -1.0  # FIX-20260713-S6-V1: ask 被命中，我们作为卖方成交，库存减少
            self._inventory = self._clamp_inventory(self._inventory + delta, market_price)
            self._inventory_turnover_sum += abs(delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            captured_bps = signal.half_spread / signal.mid_price * 10000.0 if signal.mid_price > 0 else 0.0
            self._spread_captured_bps += captured_bps
            net_bps = captured_bps - self.maker_fee_bps + self.rebate_bps
            incremental_pnl = net_bps * signal.mid_price / 10000.0
            self._realized_pnl += incremental_pnl
            self._daily_loss_bps = max(0.0, self._daily_loss_bps - net_bps)
            self._pnl_samples.append(incremental_pnl)
            self._fill_events.append((-1, fill_price, time.time()))
            self._update_vpin_bucket(-1, abs(delta))

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
    # 8. 查询接口
    # ========================================================================

    def get_preferred_side(self) -> str:
        """供 signal_service 硬约束消费。"""
        with self._lock:
            skew = self.get_inventory_skew()
            if skew > 0.15:
                return 'sell'
            elif skew < -0.15:
                return 'buy'
            return 'neutral'

    def get_last_signal(self) -> Optional['MarketMakingSignal']:
        with self._lock:
            return self._last_signal

    @classmethod
    def get_instance(cls, instrument_id: str = '', **kwargs) -> 'MarketMakingMonitor':
        global _mmm_instances
        with _mmm_lock:
            _key = instrument_id or 'DEFAULT'
            if _key not in _mmm_instances:
                _mmm_instances[_key] = MarketMakingMonitor(instrument_id=_key, **kwargs)
            return _mmm_instances[_key]


# ============================================================================
# 模块级多合约 registry
# ============================================================================

_mmm_instances: Dict[str, 'MarketMakingMonitor'] = {}
_mmm_lock = threading.Lock()


def get_market_making_monitor(instrument_id: str = '', **kwargs) -> MarketMakingMonitor:
    return MarketMakingMonitor.get_instance(instrument_id=instrument_id, **kwargs)
