# -*- coding: utf-8 -*-
"""
S6做市商策略实盘模拟监控模块（顶级基金水准）
================================================

MODULE_ID: S6-MMM-001

设计目标
--------
为 S6(MARKET_MAKING) 策略提供**实盘模拟**级别的做市商行为监控：

1. **Theoretical Price 计算**：基于中间价 + inventory skew 的理论价格模型。
2. **双边报价**：bid/ask 理论价差，half_spread 随波动率与竞争因子动态调整。
3. **Inventory 管理**：模拟持仓跟踪 + 偏斜系数，持仓越深报价越偏向减仓方向。
4. **Adverse Selection 检测**：识别被"毒单"成交的风险（成交后价格持续不利移动）。
5. **做市行为监控指标**：报价命中率、价差捕获率、inventory 周转率、毒单率、做市夏普。
6. **实盘模拟**：生成做市信号 + 快照保存，**不实际下单**。
7. **不参与资金分配**：不调用 OrderService.send_order，不调用 route_capital。

背景说明
--------
- S6 做市策略当前完全没有信号生成代码（grep handle_market_making 无结果）。
- S6 的 tp_mult=0.4 导致止盈目标 0.44（低于开仓价），是 P0 bug。
- S6 不参与资金分配（capital_allocation=0）。
- S6 只为监控做市商行为，让策略能够正确选择方向。
- 信号和快照需要保存下来供其他策略参考方向。

技术约束
--------
- 仅使用 Python 标准库 + 已有 ali2026v3_trading 模块。
- logging 使用标准 logging 模块。
- 快照保存为 JSON 格式到 logs/market_making_monitor/ 目录。
- 不引入新的外部依赖。
- 文件编码 UTF-8，代码注释用中文。
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Any, Deque, Dict, List, Optional, Tuple

# ── 尽可能复用既有基础设施（带降级，保证可独立 import）──────────────────────
try:  # 优先使用项目统一序列化工具（支持 numpy/dataclass/timestamp 等）
    from ali2026v3_trading.infra.serialization_utils import json_dumps as _json_dumps
except Exception:  # pragma: no cover - 降级路径
    _json_dumps = None

try:  # 优先使用项目统一 logger 工厂
    from ali2026v3_trading.infra._helpers import get_logger as _get_logger
except Exception:  # pragma: no cover - 降级路径
    def _get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
        _lg = logging.getLogger(name)
        if level is not None:
            _lg.setLevel(level)
        return _lg


logger = _get_logger(__name__)

# ── 默认参数（顶级做市商经验值）──────────────────────────────────────────
_DEFAULT_MAX_INVENTORY = 10            # 单合约最大模拟持仓（手/单位）
_DEFAULT_MAX_INVENTORY_NOTIONAL = 0.0  # 0 表示按手数限制；>0 则按名义价值限制
_DEFAULT_BASE_SPREAD_BPS = 8.0         # 基准 half-spread（bps）
_DEFAULT_VOL_LOOKBACK = 60             # 波动率回看窗口（tick 数）
_DEFAULT_VOL_TARGET_BPS = 12.0         # 波动率归一化目标（bps）
_DEFAULT_COMPETITION_FACTOR = 1.0      # 竞争因子（>1 表示竞争更激烈，收窄价差）
_DEFAULT_INVENTORY_FACTOR_BPS = 24.0   # inventory skew 影响上限（bps）
_DEFAULT_ADVERSE_WINDOW = 20           # 毒单检测回看窗口（tick 数）
_DEFAULT_ADVERSE_THRESHOLD_BPS = 6.0   # 毒单触发阈值（价格不利移动 bps）
_DEFAULT_SNAPSHOT_DIR = "logs/market_making_monitor"
_DEFAULT_PRICE_EPS = 1e-9             # 价格比较容差
_MIN_VOL_SAMPLE = 5                   # 计算波动率所需最小样本
# FIX-20260711-S6-TOP-TIER: 节流参数（P0: 避免每tick IO）
_DEFAULT_SIGNAL_INTERVAL_TICKS = 10   # 信号生成间隔（tick数）
_DEFAULT_SNAPSHOT_INTERVAL_SEC = 5.0  # 快照生成间隔（秒）
# FIX-20260711-S6-TOP-TIER: 波动率年化因子（P2: tick级波动率标准化）
# 期权市场典型tick频率~2/秒，240分钟交易日=28800秒→~57600tick/日
# 年化因子 = sqrt(252) ≈ 15.87 用于日级波动率年化；tick级用sqrt(ticks_per_day)
_DEFAULT_VOL_SCALE = 1.0              # 波动率缩放因子（1.0=原始，>1年化）


# ============================================================================
# 数据类
# ============================================================================

@dataclass(slots=True)
class MarketMakingSignal:
    """S6 做市商模拟信号快照。

    该信号**不触发实盘下单**，仅作为方向参考与行为审计保存。
    """

    instrument_id: str = ""
    bid_price: float = 0.0          # 理论买价
    ask_price: float = 0.0          # 理论卖价
    theo_price: float = 0.0         # 理论价格（mid + inventory skew）
    mid_price: float = 0.0          # 中间价
    spread: float = 0.0             # 理论价差（ask - bid）
    spread_bps: float = 0.0         # 理论价差（bps）
    inventory: float = 0.0          # 当前模拟持仓（正=多头，负=空头）
    inventory_skew: float = 0.0     # inventory 偏斜系数 [-1, 1]
    adverse_selection_flag: bool = False  # 是否检测到毒单风险
    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp: float = 0.0
    # 辅助信息（便于审计方向选择）
    half_spread: float = 0.0
    volatility_bps: float = 0.0
    competition_factor: float = 1.0
    # 方向建议：基于 inventory skew 给出的"更想成交"的一侧
    # "buy"  表示当前更愿意买（inventory 偏空或为 0）
    # "sell" 表示当前更愿意卖（inventory 偏多）
    # "neutral" 表示均衡
    preferred_side: str = "neutral"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# 核心监控类
# ============================================================================

class MarketMakingMonitor:
    """S6做市商策略实盘模拟监控模块（顶级基金水准）。

    核心能力
    --------
    - Theoretical Price 计算（基于中间价 + inventory skew）
    - 双边报价理论价差计算（bid-ask spread）
    - Inventory 管理与偏斜（持仓方向影响报价偏移）
    - Adverse Selection 检测（检测被毒单成交的风险）
    - 做市行为监控指标（报价命中率、价差捕获率、inventory 周转率）
    - 实盘模拟：生成做市信号 + 快照保存，不实际下单
    - 不参与资金分配

    线程安全
    --------
    内部状态使用可重入锁保护，支持 on_tick / on_bar 多线程回调。

    使用示例
    --------
    >>> monitor = MarketMakingMonitor(instrument_id="rb2601")
    >>> monitor.on_tick({"instrument_id": "rb2601", "last_price": 3500.0,
    ...                  "bid_price": 3499.0, "ask_price": 3501.0})
    >>> signal = monitor.generate_simulated_signal()
    >>> monitor.save_snapshot(signal)
    """

    # ── 类常量：明确声明不参与资金分配 / 不下单 ──
    PARTICIPATES_CAPITAL_ALLOCATION: bool = False
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
    ) -> None:
        """初始化做市商监控器。

        参数
        ----
        instrument_id : 合约 ID
        max_inventory : 单合约最大模拟持仓（手数）
        max_inventory_notional : 若 >0，则按名义价值限制 inventory（覆盖手数限制）
        base_spread_bps : 基准 half-spread（bps），波动率为 0 时的最小价差
        vol_lookback : 波动率回看窗口（tick 数）
        vol_target_bps : 波动率归一化目标，用于把波动率映射到价差倍数
        competition_factor : 竞争因子，>1 收窄价差，<1 放宽价差
        inventory_factor_bps : inventory skew 对理论价格影响的上限（bps）
        adverse_window : 毒单检测回看窗口（tick 数）
        adverse_threshold_bps : 毒单触发阈值（价格不利移动 bps）
        snapshot_dir : 快照保存目录
        price_tick : 最小变动价位（>0 时报价对齐到 tick）
        signal_interval_ticks : 信号生成间隔（tick数，P0节流，避免每tick IO）
        snapshot_interval_sec : 快照生成间隔（秒，P0节流，时间维度兜底）
        vol_scale : 波动率缩放因子（1.0=原始tick级，>1可年化，P2标准化）
        """
        # ── 参数校验 ──
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
        # FIX-20260711-S6-TOP-TIER: 节流参数（P0）
        self.signal_interval_ticks: int = int(signal_interval_ticks)
        self.snapshot_interval_sec: float = float(snapshot_interval_sec)
        # FIX-20260711-S6-TOP-TIER: 波动率缩放（P2）
        self.vol_scale: float = float(vol_scale)

        # ── 运行时状态（受锁保护）──
        self._lock = threading.RLock()
        self._inventory: float = 0.0                  # 模拟持仓（正多负空）
        self._last_mid_price: float = 0.0             # 最近中间价
        self._last_bid: float = 0.0                   # 最近市场买价
        self._last_ask: float = 0.0                   # 最近市场卖价
        self._last_last_price: float = 0.0           # 最近成交价

        # 价格序列（用于波动率与毒单检测）
        self._price_history: Deque[float] = deque(maxlen=self.vol_lookback)
        # 成交事件：方向(+1买/-1卖) + 成交价 + 时间戳
        self._fill_events: Deque[Tuple[int, float, float]] = deque(
            maxlen=self.adverse_window
        )

        # ── 做市行为统计计数器 ──
        self._quote_count: int = 0            # 报价次数（生成信号次数）
        self._quote_hit_count: int = 0        # 报价命中次数（模拟成交）
        self._spread_captured_bps: float = 0.0  # 累计捕获价差（bps）
        self._spread_offered_bps: float = 0.0  # 累计提供价差（bps）
        self._adverse_count: int = 0          # 毒单事件次数
        self._inventory_peak: float = 0.0     # 持仓峰值（用于周转率）
        self._inventory_turnover_sum: float = 0.0  # 累计|Δinventory|
        self._realized_pnl: float = 0.0       # 模拟已实现盈亏
        self._pnl_samples: Deque[float] = deque(maxlen=128)  # 用于夏普

        self._last_signal: Optional[MarketMakingSignal] = None
        self._snapshot_count: int = 0
        # FIX-20260711-S6-TOP-TIER: 节流状态（P0: 避免每tick IO）
        self._tick_count: int = 0                     # tick计数器
        self._last_signal_time: float = 0.0           # 上次信号生成时间戳

        logger.info(
            "[S6-MMM] 初始化完成 inst=%s max_inv=%.2f base_spread=%.1fbps "
            "vol_lookback=%d competition=%.2f adverse_thr=%.1fbps | "
            "参与资金分配=%s 下单=%s",
            self.instrument_id, self.max_inventory, self.base_spread_bps,
            self.vol_lookback, self.competition_factor, self.adverse_threshold_bps,
            self.PARTICIPATES_CAPITAL_ALLOCATION, self.SENDS_REAL_ORDERS,
        )

    # ========================================================================
    # 1. Theoretical Price 计算
    # ========================================================================

    def compute_theoretical_price(self, mid_price: float) -> float:
        """计算理论价格。

        公式
        ----
            theo_price = mid_price + inventory_skew * inventory_factor

        其中 inventory_factor = inventory_factor_bps * mid_price / 10000，
        inventory_skew ∈ [-1, 1]。

        持仓越多（多头），理论价格下移（更愿意卖）；
        持仓越空，理论上移（更愿意买）。

        参数
        ----
        mid_price : 当前中间价

        返回
        ----
        理论价格（已对齐到 price_tick 若设置）
        """
        if mid_price <= 0:
            logger.debug("[S6-MMM] compute_theoretical_price: mid_price<=0, 返回 0")
            return 0.0

        skew = self.get_inventory_skew()
        # inventory 影响量（价格单位）：bps → 价格
        inventory_factor = self.inventory_factor_bps * mid_price / 10000.0
        theo_price = mid_price + skew * inventory_factor

        # 对齐到最小变动价位
        theo_price = self._align_to_tick(theo_price)

        logger.debug(
            "[S6-MMM] theo_price 计算: mid=%.6f skew=%.4f factor=%.6f → theo=%.6f",
            mid_price, skew, inventory_factor, theo_price,
        )
        return theo_price

    # ========================================================================
    # 2. 双边报价
    # ========================================================================

    def compute_bid_ask_quotes(
        self,
        mid_price: float,
        *,
        volatility_bps: Optional[float] = None,
    ) -> Tuple[float, float, float]:
        """计算理论买价和卖价。

        公式
        ----
            bid = theo_price - half_spread
            ask = theo_price + half_spread

        half_spread 基于波动率与 competition_factor 动态调整：
            half_spread = max(base_spread, base_spread * (vol / vol_target) / competition)

        参数
        ----
        mid_price : 当前中间价
        volatility_bps : 当前波动率（bps），None 则自动从价格历史计算

        返回
        ----
        (bid, ask, half_spread)
        """
        if mid_price <= 0:
            logger.debug("[S6-MMM] compute_bid_ask_quotes: mid_price<=0, 返回 (0,0,0)")
            return 0.0, 0.0, 0.0

        if volatility_bps is None:
            volatility_bps = self._estimate_volatility_bps()

        # 波动率倍数：波动越高，价差越宽
        vol_multiplier = max(1.0, volatility_bps / max(self.vol_target_bps, _DEFAULT_PRICE_EPS))
        # 竞争因子：竞争越激烈（>1），价差越窄
        effective_spread_bps = self.base_spread_bps * vol_multiplier / max(
            self.competition_factor, _DEFAULT_PRICE_EPS
        )
        # 确保 >= 基准的一半（half_spread 不低于基准）
        half_spread_bps = max(self.base_spread_bps, effective_spread_bps)
        half_spread = half_spread_bps * mid_price / 10000.0

        theo_price = self.compute_theoretical_price(mid_price)
        bid = self._align_to_tick(theo_price - half_spread)
        ask = self._align_to_tick(theo_price + half_spread)

        # 防御：确保 ask > bid（极端波动/对齐情况下可能相等）
        if ask <= bid:
            ask = self._align_to_tick(bid + max(half_spread, self.price_tick if self.price_tick > 0 else _DEFAULT_PRICE_EPS))

        logger.debug(
            "[S6-MMM] 双边报价: mid=%.6f vol=%.2fbps half_spread=%.6f(bps=%.2f) "
            "→ bid=%.6f ask=%.6f theo=%.6f",
            mid_price, volatility_bps, half_spread, half_spread_bps,
            bid, ask, theo_price,
        )
        return bid, ask, half_spread

    # ========================================================================
    # 3. Inventory 管理
    # ========================================================================

    def update_inventory(self, delta: float, *, fill_price: Optional[float] = None) -> float:
        """更新模拟持仓。

        参数
        ----
        delta : 持仓变动（正=加多，负=加空）
        fill_price : 模拟成交价（用于毒单检测与已实现盈亏），可选

        返回
        ----
        更新后的 inventory
        """
        with self._lock:
            prev_inv = self._inventory
            new_inv = prev_inv + delta

            # 限制在 max_inventory 内（双向）
            cap = self._effective_max_inventory(fill_price)
            if cap > 0:
                new_inv = max(-cap, min(cap, new_inv))

            actual_delta = new_inv - prev_inv
            self._inventory = new_inv

            # 周转率累计（|Δ|）
            self._inventory_turnover_sum += abs(actual_delta)
            # 峰值跟踪
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))

            # 记录成交事件用于毒单检测：方向 +1 买 / -1 卖
            if fill_price is not None and fill_price > 0 and actual_delta != 0.0:
                direction = 1 if actual_delta > 0 else -1
                self._fill_events.append((direction, float(fill_price), time.time()))

            logger.info(
                "[S6-MMM] inventory 更新: prev=%.4f delta=%.4f actual_delta=%.4f → new=%.4f "
                "cap=%.4f fill_price=%s",
                prev_inv, delta, actual_delta, self._inventory, cap, fill_price,
            )
            return self._inventory

    def get_inventory_skew(self) -> float:
        """计算 inventory 偏斜系数。

        公式
        ----
            inventory_skew = inventory / max_inventory   ∈ [-1, 1]

        返回
        ----
        偏斜系数，正值表示多头持仓（理论价下移），负值表示空头（理论价上移）
        """
        with self._lock:
            cap = self._effective_max_inventory(self._last_mid_price)
            if cap <= 0:
                return 0.0
            skew = self._inventory / cap
            # 钳制到 [-1, 1]
            return max(-1.0, min(1.0, skew))

    def get_inventory(self) -> float:
        """获取当前模拟持仓。"""
        with self._lock:
            return self._inventory

    # ========================================================================
    # 4. Adverse Selection 检测
    # ========================================================================

    def detect_adverse_selection(self) -> bool:
        """检测毒单成交风险。

        原理
        ----
        基于价格变动方向与成交方向的一致性：
        - 做市商在 bid 成交（被买方吃单，对应我方卖出/做空）后，
          若价格持续下跌（对我方空头有利），则是"好单"；
          若价格持续上涨（对我方空头不利），则是"毒单"。
        - 反之同理。

        判定：取最近 adverse_window 内的成交事件，计算成交后价格的不利移动，
              若超过 adverse_threshold_bps，标记为 adverse selection。

        返回
        ----
        True 表示当前存在毒单风险（最近成交后价格不利移动超阈值）
        """
        with self._lock:
            if not self._fill_events or self._last_last_price <= 0:
                return False

            current_price = self._last_last_price
            adverse_detected = False

            for direction, fill_price, _ts in self._fill_events:
                if fill_price <= 0:
                    continue
                # 价格变动（bps）
                price_move_bps = (current_price - fill_price) / fill_price * 10000.0
                # 不利移动判定：
                #   方向 +1（我方买入/加多）：价格上涨有利，下跌不利 → 不利移动 = -price_move
                #   方向 -1（我方卖出/加空）：价格下跌有利，上涨不利 → 不利移动 = +price_move
                adverse_move_bps = -direction * price_move_bps
                if adverse_move_bps >= self.adverse_threshold_bps:
                    adverse_detected = True
                    break

            if adverse_detected:
                self._adverse_count += 1
                logger.info(
                    "[S6-MMM] ⚠ 检测到 Adverse Selection: 当前价=%.6f 阈值=%.2fbps "
                    "事件数=%d 毒单累计=%d",
                    current_price, self.adverse_threshold_bps,
                    len(self._fill_events), self._adverse_count,
                )
            return adverse_detected

    # ========================================================================
    # 5. 做市行为监控指标
    # ========================================================================

    def compute_metrics(self) -> Dict[str, float]:
        """计算做市行为监控指标。

        返回指标
        --------
        - quote_hit_rate : 报价命中率（命中次数 / 报价次数）
        - spread_capture_rate : 价差捕获率（已捕获 / 已提供）
        - inventory_turnover : inventory 周转率（累计|Δ| / (2*峰值)）
        - adverse_selection_rate : 毒单率（毒单次数 / max(报价次数,1)）
        - sharpe_ratio : 做市夏普比率（模拟，基于盈亏样本）
        - inventory : 当前持仓
        - inventory_skew : 当前偏斜系数
        - realized_pnl : 模拟已实现盈亏
        """
        with self._lock:
            quote_hit_rate = (
                self._quote_hit_count / self._quote_count
                if self._quote_count > 0 else 0.0
            )
            spread_capture_rate = (
                self._spread_captured_bps / self._spread_offered_bps
                if self._spread_offered_bps > 0 else 0.0
            )
            inventory_turnover = (
                self._inventory_turnover_sum / (2.0 * max(self._inventory_peak, _DEFAULT_PRICE_EPS))
                if self._inventory_peak > 0 else 0.0
            )
            adverse_selection_rate = (
                self._adverse_count / self._quote_count
                if self._quote_count > 0 else 0.0
            )
            sharpe_ratio = self._compute_sharpe()

            metrics = {
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
            }
            logger.debug(
                "[S6-MMM] 指标计算: hit_rate=%.4f capture=%.4f turnover=%.4f "
                "adv_rate=%.4f sharpe=%.4f",
                quote_hit_rate, spread_capture_rate, inventory_turnover,
                adverse_selection_rate, sharpe_ratio,
            )
            return metrics

    # ========================================================================
    # 6. 实盘模拟：信号生成 + 快照保存
    # ========================================================================

    def generate_simulated_signal(
        self,
        *,
        tick_data: Optional[Dict[str, Any]] = None,
    ) -> MarketMakingSignal:
        """生成模拟做市信号。

        重要：本方法**不调用 OrderService.send_order**，仅生成信号快照。

        参数
        ----
        tick_data : 可选的 tick 数据（用于补充最新价格），None 则用内部缓存

        返回
        ----
        MarketMakingSignal 信号对象
        """
        with self._lock:
            if tick_data is not None:
                self._ingest_tick(tick_data)

            mid_price = self._last_mid_price
            if mid_price <= 0:
                logger.warning(
                    "[S6-MMM] 生成信号失败：无有效 mid_price（inst=%s）", self.instrument_id
                )
                # 仍返回一个空信号，便于上层流程不中断
                signal = MarketMakingSignal(
                    instrument_id=self.instrument_id,
                    timestamp=time.time(),
                    metrics=self.compute_metrics(),
                )
                self._last_signal = signal
                return signal

            volatility_bps = self._estimate_volatility_bps()
            bid, ask, half_spread = self.compute_bid_ask_quotes(
                mid_price, volatility_bps=volatility_bps
            )
            theo_price = self.compute_theoretical_price(mid_price)
            spread = ask - bid
            spread_bps = (spread / mid_price * 10000.0) if mid_price > 0 else 0.0
            skew = self.get_inventory_skew()
            adverse_flag = self.detect_adverse_selection()

            # 方向建议：基于 inventory skew
            # FIX-20260711-S6-TOP-TIER: P1 统一阈值为0.15（与get_preferred_side一致）
            if skew > 0.15:
                preferred_side = "sell"   # 偏多 → 更想卖
            elif skew < -0.15:
                preferred_side = "buy"    # 偏空 → 更想买
            else:
                preferred_side = "neutral"

            signal = MarketMakingSignal(
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
            )

            # ── 关键断言：理论价格必须在 bid 和 ask 之间 ──
            assert bid - _DEFAULT_PRICE_EPS <= theo_price <= ask + _DEFAULT_PRICE_EPS, (
                f"理论价格 {theo_price} 不在 bid {bid} 与 ask {ask} 之间"
            )

            # ── 关键断言：inventory 不超过 max_inventory ──
            cap = self._effective_max_inventory(mid_price)
            assert abs(self._inventory) <= cap + _DEFAULT_PRICE_EPS, (
                f"inventory {self._inventory} 超过 max_inventory {cap}"
            )

            # 统计：报价次数 + 提供价差
            self._quote_count += 1
            self._spread_offered_bps += spread_bps

            # FIX-20260711-S6-TOP-TIER: P2 移除_simulate_quote_hit调用
            # 时序错误修复：_simulate_quote_hit现在在on_tick中调用，
            # 使用NEW tick价格检查上一信号是否被穿越，而非用生成信号时的价格

            self._last_signal = signal
            logger.info(
                "[S6-MMM] 信号生成: inst=%s mid=%.6f theo=%.6f bid=%.6f ask=%.6f "
                "spread=%.2fbps inv=%.4f skew=%.4f adv=%s side=%s | "
                "⚠不下单 不参与资金分配",
                self.instrument_id, mid_price, theo_price, bid, ask,
                spread_bps, self._inventory, skew, adverse_flag, preferred_side,
            )
            return signal

    def save_snapshot(self, signal: Optional[MarketMakingSignal] = None) -> str:
        """保存信号快照为 JSON 文件。

        参数
        ----
        signal : 要保存的信号，None 则保存最近一次生成的信号

        返回
        ----
        快照文件绝对路径
        """
        if signal is None:
            signal = self._last_signal
        if signal is None:
            logger.warning("[S6-MMM] save_snapshot: 无可用信号，跳过保存")
            return ""

        # 确保目录存在
        os.makedirs(self.snapshot_dir, exist_ok=True)

        # 文件名：instrument_时间戳_序号.json
        safe_inst = self.instrument_id or "unknown"
        safe_inst = "".join(c for c in safe_inst if c.isalnum() or c in ("_", "-"))
        ts = int(signal.timestamp) if signal.timestamp > 0 else int(time.time())
        fname = f"{safe_inst}_{ts}_{self._snapshot_count}.json"
        fpath = os.path.join(self.snapshot_dir, fname)

        payload = {
            "module": "S6-MMM",
            "version": "1.0",
            "instrument_id": self.instrument_id,
            "participates_capital_allocation": self.PARTICIPATES_CAPITAL_ALLOCATION,
            "sends_real_orders": self.SENDS_REAL_ORDERS,
            "signal": signal.to_dict(),
            "saved_at": time.time(),
        }

        # 序列化（优先项目统一工具，降级标准 json）
        if _json_dumps is not None:
            content = _json_dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
        else:
            content = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True, default=str)

        try:
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(content)
        except OSError as e:
            logger.error("[S6-MMM] 快照写入失败 path=%s err=%s", fpath, e)
            return ""

        self._snapshot_count += 1

        # ── 关键断言：信号快照保存后断言文件存在 ──
        assert os.path.exists(fpath), f"快照文件保存后不存在: {fpath}"

        logger.info("[S6-MMM] 快照已保存: %s (size=%d bytes)", fpath, os.path.getsize(fpath))
        return fpath

    # ========================================================================
    # 7. 集成接口
    # ========================================================================

    def on_tick(self, tick_data: Dict[str, Any]) -> None:
        """tick 数据回调。

        参数
        ----
        tick_data : tick 字典，字段包括 instrument_id / last_price / bid_price / ask_price
                    （兼容 bid_price1/ask_price1）

        FIX-20260711-S6-TOP-TIER 优化:
        - P0: 节流模式，每 signal_interval_ticks 或 snapshot_interval_sec 生成一次信号+快照
        - P2: _simulate_quote_hit 使用NEW tick价格检查上一信号是否被穿越
        """
        if not isinstance(tick_data, dict):
            logger.debug("[S6-MMM] on_tick: 非 dict 输入，跳过")
            return

        with self._lock:
            # 1. 摄入新tick（更新 _last_last_price 等状态）
            self._ingest_tick(tick_data)
            self._tick_count += 1

            # 2. P2: 用NEW tick价格检查上一信号是否被穿越（时序正确）
            if self._last_signal is not None:
                try:
                    self._simulate_quote_hit(self._last_signal)
                except Exception as e:  # noqa: BLE001
                    logger.debug("[S6-MMM] _simulate_quote_hit 异常(不中断): %s", e)

        # 3. P0: 节流判断 — 是否到了生成新信号的时间
        _now = time.time()
        _ticks_ok = self._tick_count >= self.signal_interval_ticks
        _time_ok = (_now - self._last_signal_time) >= self.snapshot_interval_sec
        if not (_ticks_ok or _time_ok):
            return

        # 4. 生成信号 + 保存快照
        try:
            signal = self.generate_simulated_signal()
            self.save_snapshot(signal)
            # 重置节流计数器
            with self._lock:
                self._tick_count = 0
                self._last_signal_time = _now
        except AssertionError as e:
            logger.error("[S6-MMM] on_tick 信号生成断言失败: %s", e)
        except Exception as e:  # noqa: BLE001 - 监控模块不能因异常中断主链路
            logger.exception("[S6-MMM] on_tick 处理异常: %s", e)

    def on_bar(self, bar_data: Dict[str, Any]) -> None:
        """K线数据回调。

        参数
        ----
        bar_data : K线字典，字段包括 instrument_id / close / high / low / open
        """
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
                # 用 close 作为 bid/ask 近似（bar 无买卖盘）
                self._last_bid = low
                self._last_ask = high
                self._price_history.append(close)
            logger.debug(
                "[S6-MMM] on_bar: inst=%s close=%.6f mid=%.6f",
                self.instrument_id, close, mid,
            )
            # bar 回调不强制生成信号（避免与 on_tick 重复），仅更新状态
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM] on_bar 解析异常: %s", e)

    def get_monitoring_report(self) -> Dict[str, Any]:
        """获取监控报告。

        返回
        ----
        包含当前状态、最新信号、指标、配置的完整报告字典
        """
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
                    # FIX-20260711-S6-TOP-TIER: 新增节流+波动率配置
                    "signal_interval_ticks": self.signal_interval_ticks,
                    "snapshot_interval_sec": self.snapshot_interval_sec,
                    "vol_scale": self.vol_scale,
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
                    # FIX-20260711-S6-TOP-TIER: 新增节流状态
                    "tick_count": self._tick_count,
                    "last_signal_time": self._last_signal_time,
                },
                "metrics": metrics,
                "last_signal": last_signal_dict,
                "report_time": time.time(),
            }

    # ========================================================================
    # 内部辅助方法
    # ========================================================================

    def _ingest_tick(self, tick_data: Dict[str, Any]) -> None:
        """从 tick 字典提取价格并更新内部状态（调用方需持锁）。"""
        try:
            inst = tick_data.get("instrument_id")
            if inst and not self.instrument_id:
                self.instrument_id = str(inst)

            last_price = float(tick_data.get("last_price", 0.0) or 0.0)
            # 兼容 bid_price / bid_price1 两种字段
            bid = float(
                tick_data.get("bid_price", tick_data.get("bid_price1", 0.0)) or 0.0
            )
            ask = float(
                tick_data.get("ask_price", tick_data.get("ask_price1", 0.0)) or 0.0
            )

            if last_price > 0:
                self._last_last_price = last_price
                self._price_history.append(last_price)

            if bid > 0 and ask > 0:
                self._last_bid = bid
                self._last_ask = ask
                self._last_mid_price = (bid + ask) / 2.0
            elif last_price > 0 and self._last_mid_price <= 0:
                # 无买卖盘时用成交价近似中间价
                self._last_mid_price = last_price
                self._last_bid = last_price
                self._last_ask = last_price
        except (ValueError, TypeError) as e:
            logger.debug("[S6-MMM] _ingest_tick 解析异常: %s", e)

    def _estimate_volatility_bps(self) -> float:
        """从价格历史估算波动率（bps）。

        使用最近 tick 价格序列的收益率标准差 × sqrt(N) 作为波动率估计。
        调用方需持锁。
        """
        prices = list(self._price_history)
        if len(prices) < _MIN_VOL_SAMPLE:
            return self.base_spread_bps  # 样本不足时回退基准

        # 收益率序列
        rets: List[float] = []
        for i in range(1, len(prices)):
            prev = prices[i - 1]
            if prev > 0:
                rets.append((prices[i] - prev) / prev)

        if len(rets) < _MIN_VOL_SAMPLE - 1:
            return self.base_spread_bps

        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        std = math.sqrt(var)
        # FIX-20260711-S6-TOP-TIER: P2 波动率标准化（vol_scale可年化）
        # 转为 bps 并应用缩放因子（1.0=原始tick级，>1可年化）
        vol_bps = std * 10000.0 * self.vol_scale
        return vol_bps if vol_bps > 0 else self.base_spread_bps

    def _simulate_quote_hit(self, signal: MarketMakingSignal) -> None:
        """模拟报价命中：若市场价穿越我方报价，则视为命中并模拟成交。

        调用方需持锁。
        """
        market_price = self._last_last_price
        if market_price <= 0:
            return

        # 买价被击穿（市场价 <= 我方 bid）→ 我方被买方吃单，即我方"卖出"
        if signal.bid_price > 0 and market_price <= signal.bid_price:
            self._quote_hit_count += 1
            # 模拟卖出：inventory 减少
            fill_price = signal.bid_price
            delta = -1.0
            self._inventory = self._clamp_inventory(self._inventory + delta, market_price)
            self._inventory_turnover_sum += abs(delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            # 捕获价差：卖在 bid，理论应买回在 mid，捕获 ≈ half_spread
            captured_bps = signal.half_spread / signal.mid_price * 10000.0 if signal.mid_price > 0 else 0.0
            self._spread_captured_bps += captured_bps
            # FIX-20260711-S6-TOP-TIER: P2 存储增量PnL（非累计值），修正sharpe经济含义
            incremental_pnl = captured_bps * signal.mid_price / 10000.0
            self._realized_pnl += incremental_pnl
            self._pnl_samples.append(incremental_pnl)
            self._fill_events.append((-1, fill_price, time.time()))
            logger.debug(
                "[S6-MMM] 模拟命中(bid): market=%.6f <= bid=%.6f 捕获=%.2fbps",
                market_price, signal.bid_price, captured_bps,
            )

        # 卖价被击穿（市场价 >= 我方 ask）→ 我方被卖方吃单，即我方"买入"
        elif signal.ask_price > 0 and market_price >= signal.ask_price:
            self._quote_hit_count += 1
            fill_price = signal.ask_price
            delta = 1.0
            self._inventory = self._clamp_inventory(self._inventory + delta, market_price)
            self._inventory_turnover_sum += abs(delta)
            self._inventory_peak = max(self._inventory_peak, abs(self._inventory))
            captured_bps = signal.half_spread / signal.mid_price * 10000.0 if signal.mid_price > 0 else 0.0
            self._spread_captured_bps += captured_bps
            # FIX-20260711-S6-TOP-TIER: P2 存储增量PnL（非累计值），修正sharpe经济含义
            incremental_pnl = captured_bps * signal.mid_price / 10000.0
            self._realized_pnl += incremental_pnl
            self._pnl_samples.append(incremental_pnl)
            self._fill_events.append((1, fill_price, time.time()))
            logger.debug(
                "[S6-MMM] 模拟命中(ask): market=%.6f >= ask=%.6f 捕获=%.2fbps",
                market_price, signal.ask_price, captured_bps,
            )

    def _clamp_inventory(self, inv: float, price: float) -> float:
        """钳制 inventory 到 max_inventory 范围内。调用方需持锁。"""
        cap = self._effective_max_inventory(price)
        if cap <= 0:
            return inv
        return max(-cap, min(cap, inv))

    def _effective_max_inventory(self, price: Optional[float]) -> float:
        """计算有效的 max_inventory。

        若设置了 max_inventory_notional 且 price>0，则按名义价值限制：
            max_inv = max_inventory_notional / price
        否则使用手数限制 max_inventory。

        调用方需持锁。
        """
        if self.max_inventory_notional > 0 and price is not None and price > 0:
            return self.max_inventory_notional / price
        return self.max_inventory

    def _align_to_tick(self, price: float) -> float:
        """对齐价格到最小变动价位。"""
        if self.price_tick <= 0:
            return price
        return round(price / self.price_tick) * self.price_tick

    def _compute_sharpe(self) -> float:
        """计算做市夏普比率（模拟，基于增量盈亏样本）。

        FIX-20260711-S6-TOP-TIER: P2 修复样本污染
        - 旧逻辑：_pnl_samples存储累计_realized_pnl，计算一阶差分→sharpe含义为"累计PnL加速率"
        - 新逻辑：_pnl_samples存储每笔增量PnL，直接计算mean/std→sharpe含义为"每笔交易PnL的夏普比率"

        调用方需持锁。
        """
        if len(self._pnl_samples) < 2:
            return 0.0
        # 直接使用增量PnL样本（无需差分）
        samples = list(self._pnl_samples)
        mean = sum(samples) / len(samples)
        var = sum((s - mean) ** 2 for s in samples) / len(samples)
        std = math.sqrt(var)
        if std <= _DEFAULT_PRICE_EPS:
            return 0.0
        # 年化因子简化为 1（做市高频场景，样本已是高频）
        return mean / std

    # FIX-20260711-S5S6-CHAIN: 添加查询方法，供signal_service硬约束过滤消费

    def get_preferred_side(self) -> str:
        """获取当前做市preferred_side（供signal_service硬约束消费）

        返回值:
            'buy'  — 偏空库存，更想买（策略BUY方向将被支持）
            'sell' — 偏多库存，更想卖（策略SELL方向将被支持）
            'neutral' — 库存平衡，不约束方向
        """
        with self._lock:
            skew = self.get_inventory_skew()
            if skew > 0.15:
                return 'sell'
            elif skew < -0.15:
                return 'buy'
            else:
                return 'neutral'

    def get_last_signal(self) -> Optional['MarketMakingSignal']:
        """获取最近一次生成的做市信号"""
        with self._lock:
            return self._last_signal

    @classmethod
    def get_instance(cls, instrument_id: str = '', **kwargs) -> 'MarketMakingMonitor':
        """获取或创建MarketMakingMonitor实例（多合约registry）

        FIX-20260711-S6-TOP-TIER: P2 单例改造为多合约registry
        - 旧逻辑：全局单例，不支持多合约
        - 新逻辑：per-instrument实例字典，每个合约独立inventory/skew/preferred_side
        - 向后兼容：instrument_id为空时使用'DEFAULT'键

        参数
        ----
        instrument_id : 合约ID，为空时使用'DEFAULT'
        **kwargs : 首次创建时传入的构造参数

        返回
        ----
        对应合约的MarketMakingMonitor实例
        """
        global _mmm_instances
        with _mmm_lock:
            _key = instrument_id or 'DEFAULT'
            if _key not in _mmm_instances:
                _mmm_instances[_key] = MarketMakingMonitor(instrument_id=_key, **kwargs)
            return _mmm_instances[_key]


# ============================================================================
# 模块级多合约registry（FIX-20260711-S6-TOP-TIER: P2 单例→多合约registry）
# ============================================================================

_mmm_instances: Dict[str, 'MarketMakingMonitor'] = {}
_mmm_lock = threading.Lock()


def get_market_making_monitor(instrument_id: str = '', **kwargs) -> 'MarketMakingMonitor':
    """获取MarketMakingMonitor实例（多合约registry）"""
    return MarketMakingMonitor.get_instance(instrument_id=instrument_id, **kwargs)


# ============================================================================
# 模块自检（可直接 python 执行验证）
# ============================================================================

def _self_test() -> None:
    """模块自检：验证核心流程可运行。"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    m = MarketMakingMonitor(instrument_id="rb2601_TEST", price_tick=1.0)
    # 注入若干 tick
    for i, p in enumerate([3500.0, 3501.0, 3499.5, 3500.5, 3501.5, 3502.0, 3498.5, 3500.0]):
        m.on_tick({
            "instrument_id": "rb2601_TEST",
            "last_price": p,
            "bid_price": p - 1.0,
            "ask_price": p + 1.0,
        })
    sig = m.generate_simulated_signal()
    path = m.save_snapshot(sig)
    report = m.get_monitoring_report()
    print("[SELF-TEST] snapshot path:", path)
    print("[SELF-TEST] metrics:", report["metrics"])
    print("[SELF-TEST] last signal side:", sig.preferred_side, "theo:", sig.theo_price)


if __name__ == "__main__":
    _self_test()
