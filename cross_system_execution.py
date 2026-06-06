"""Unified execution primitives shared by production/backtest/judgment systems."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List
from ali2026v3_trading.resilience_utils import safe_float_to_int


@dataclass(slots=True)
class UnifiedSlippageModel:
    base_bps: float = 2.0
    impact_factor: float = 0.05

    def estimate_price(self, mid_price: float, volume: float, side: str) -> float:
        if mid_price <= 0:
            return 0.0
        signed = 1.0 if str(side).upper() in ('BUY', 'CLOSE_SHORT') else -1.0
        impact_bps = self.base_bps + self.impact_factor * math.sqrt(max(volume, 0.0))
        return mid_price * (1.0 + signed * impact_bps / 10000.0)


@dataclass(slots=True)
class UnifiedPositionSizer:
    max_risk_ratio: float = 0.2  # R26-P2对齐标注: 评判系统保守定位(0.2)，生产默认0.8，属有意差异——评判系统需更严格风控

    def size(self, equity: float, risk_per_lot: float, lots_min: int = 1, lots_max: int = 100) -> int:
        if equity <= 0 or risk_per_lot <= 0:
            return lots_min
        # R27-P2-FP-10修复: int()截断→safe_float_to_int()
        raw = safe_float_to_int((equity * self.max_risk_ratio) / risk_per_lot)
        return max(lots_min, min(lots_max, raw))


@dataclass(slots=True)
class BacktestLatencyController:
    min_cycle_delay_ms: int = 5

    def should_yield(self, last_cycle_ts: float) -> bool:
        return (time.time() - last_cycle_ts) * 1000.0 < self.min_cycle_delay_ms


@dataclass(slots=True)
class BacktestOrderSplitter:
    max_lots_per_child: int = 10

    def split(self, lots: int) -> List[int]:
        # R27-P2-FP-11修复: int()截断→safe_float_to_int()
        remain = max(0, safe_float_to_int(lots))
        chunks: List[int] = []
        while remain > 0:
            child = min(self.max_lots_per_child, remain)
            chunks.append(child)
            remain -= child
        return chunks or [0]


@dataclass(slots=True)
class CrossSystemExecutionKernel:
    slippage_model: UnifiedSlippageModel = field(default_factory=UnifiedSlippageModel)
    position_sizer: UnifiedPositionSizer = field(default_factory=UnifiedPositionSizer)
    latency_controller: BacktestLatencyController = field(default_factory=BacktestLatencyController)
    order_splitter: BacktestOrderSplitter = field(default_factory=BacktestOrderSplitter)

    def snapshot(self) -> Dict[str, Any]:
        return {
            'slippage_base_bps': self.slippage_model.base_bps,
            'position_max_risk_ratio': self.position_sizer.max_risk_ratio,
            'min_cycle_delay_ms': self.latency_controller.min_cycle_delay_ms,
            'max_lots_per_child': self.order_splitter.max_lots_per_child,
        }
