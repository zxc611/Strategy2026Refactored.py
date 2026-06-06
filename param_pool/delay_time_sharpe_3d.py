#!/usr/bin/env python3
"""
延迟-时间参数-夏普 三维映射表

将"延迟"从外部约束转化为内部参数：
对每个延迟档位，遍历所有可行的时间参数组合，记录夏普，
形成一张可查询、可插值、可实时调用的映射表。

维度X: 延迟档位 (Delay Tier) - 0, 25, 50, 80, 120, 200 ms
维度Y: 时间参数组合 (Time Parameter Set) - mode/ticks/bars per strategy
维度Z: 夏普比率 (Sharpe Ratio) - 组合/单策略/分HMM状态

基于现有task_scheduler.py的四策略回测引擎，扩展延迟注入+时间参数化。
"""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import as_completed  # EC-P2-12: 删除未使用的ProcessPoolExecutor导入
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    from ali2026v3_trading.data_access import get_data_access
    from ali2026v3_trading.db_adapter import connect, get_duckdb_module
    duckdb = get_duckdb_module()
except ImportError:
    duckdb = None
import numpy as np
import pandas as pd
from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_MINUTE

try:
    from task_scheduler import (
        PARAM_DEFAULTS,
        PARAM_DEFAULTS_HFT,
        PARAM_DEFAULTS_BOX_EXTREME,
        PARAM_DEFAULTS_BOX_SPRING,
        PARAM_DEFAULTS_ARBITRAGE,
        PARAM_DEFAULTS_MARKET_MAKING,
        run_backtest,
        run_backtest_hft,
        run_backtest_box_extreme,
        run_backtest_box_spring,
        run_backtest_arbitrage,
        run_backtest_market_making,
    )
except ImportError:
    PARAM_DEFAULTS = {}
    PARAM_DEFAULTS_HFT = {}
    PARAM_DEFAULTS_BOX_EXTREME = {}
    PARAM_DEFAULTS_BOX_SPRING = {}
    PARAM_DEFAULTS_ARBITRAGE = {}
    PARAM_DEFAULTS_MARKET_MAKING = {}

try:
    from ali2026v3_trading.config_params import get_param
    SLIPPAGE_BPS = get_param('default_slippage_bps', 3.0)
except Exception:
    try:
        from task_scheduler import SLIPPAGE_BPS as _ts_slippage
        SLIPPAGE_BPS = _ts_slippage
    except ImportError:
        SLIPPAGE_BPS = 3.0
    run_backtest = None
    run_backtest_hft = None
    run_backtest_box_extreme = None
    run_backtest_box_spring = None
    run_backtest_arbitrage = None
    run_backtest_market_making = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# P2-R11-07修复: DuckDB路径使用sharpe_3d/子目录，与主系统数据隔离
_SHARPE_3D_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sharpe_3d')
os.makedirs(_SHARPE_3D_DIR, exist_ok=True)
MAPPING_DB = os.path.join(_SHARPE_3D_DIR, "delay_time_sharpe_3d.duckdb")


# ============================================================================
# 一、时间参数化配置
# ============================================================================

@dataclass(slots=True)
class TimeParams:
    mode: str
    ticks: int
    bars: int
    delay_ms: float
    prediction_model: str = "momentum"
    confirmation_threshold: float = 0.7

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "ticks": self.ticks,
            "bars": self.bars,
            "delay_ms": self.delay_ms,
            "prediction_model": self.prediction_model,
            "confirmation_threshold": self.confirmation_threshold,
        }

    def key(self) -> str:
        return f"{self.mode}/{self.ticks}/{self.bars}/{self.delay_ms}"


# P-10修复: DELAY_TIERS从20档位收缩为6核心档位（手册4.2节）
DELAY_TIERS = [0, 25, 50, 80, 120, 200]

# P-10备注: DEFAULT_TIME_PARAMS仍保留20档历史数据，用于回测兼容性
# 实际使用时通过_find_nearest_tier()映射到6核心档位
# TODO(R17-P2-DOC-02): 未来版本可清理冗余数据，仅保留6档

DELAY_TIER_LABELS = {
    0: "零延迟(同机房)",
    25: "低延迟+(同城典型)",
    50: "中低延迟极限(跨区远)",
    80: "中延迟++(个人典型)",
    120: "中高延迟++(云远端)",
    200: "高延迟极限(跨洲远)",
}

# C-18修复: STRATEGY_NAMES扩展为6策略，与主回测系统对齐
STRATEGY_NAMES = ["high_freq", "resonance", "box", "spring", "arbitrage", "market_making"]

DEFAULT_TIME_PARAMS: Dict[str, Dict[float, TimeParams]] = {
    "high_freq": {
        0:   TimeParams("immediate", 0, 0, 0),
        10:  TimeParams("lead", 1, 0, 10, "momentum"),
        15:  TimeParams("lead", 1, 0, 15, "momentum"),
        20:  TimeParams("lead", 1, 0, 20, "momentum"),
        25:  TimeParams("lead", 1, 0, 25, "momentum"),
        30:  TimeParams("lead", 1, 0, 30, "momentum"),
        35:  TimeParams("lead", 1, 0, 35, "momentum"),
        40:  TimeParams("lead", 2, 0, 40, "momentum"),
        45:  TimeParams("lead", 2, 0, 45, "momentum"),
        50:  TimeParams("lead", 2, 0, 50, "momentum"),
        60:  TimeParams("lead", 2, 0, 60, "order_flow"),
        70:  TimeParams("lead", 3, 0, 70, "order_flow"),
        80:  TimeParams("lead", 3, 0, 80, "order_flow"),
        90:  TimeParams("lead", 3, 0, 90, "order_flow"),
        100: TimeParams("lead", 4, 0, 100, "order_flow"),
        120: TimeParams("lead", 4, 0, 120, "order_flow"),
        140: TimeParams("lead", 5, 0, 140, "microstructure"),
        160: TimeParams("lead", 5, 0, 160, "microstructure"),
        180: TimeParams("lead", 6, 0, 180, "microstructure"),
        200: TimeParams("lead", 6, 0, 200, "microstructure"),
    },
    "resonance": {
        0:   TimeParams("immediate", 0, 0, 0),
        10:  TimeParams("immediate", 0, 0, 10),
        15:  TimeParams("immediate", 0, 0, 15),
        20:  TimeParams("immediate", 0, 0, 20),
        25:  TimeParams("immediate", 0, 0, 25),
        30:  TimeParams("immediate", 0, 0, 30),
        35:  TimeParams("lag", 0, 1, 35),
        40:  TimeParams("lag", 0, 1, 40),
        45:  TimeParams("lag", 0, 1, 45),
        50:  TimeParams("lag", 0, 1, 50),
        60:  TimeParams("lag", 0, 1, 60),
        70:  TimeParams("lag", 0, 2, 70),
        80:  TimeParams("lag", 0, 2, 80),
        90:  TimeParams("lag", 0, 2, 90),
        100: TimeParams("lag", 0, 2, 100),
        120: TimeParams("lag", 0, 3, 120),
        140: TimeParams("lag", 0, 3, 140),
        160: TimeParams("lag", 0, 4, 160),
        180: TimeParams("lag", 0, 4, 180),
        200: TimeParams("lag", 0, 5, 200),
    },
    "box": {
        0:   TimeParams("immediate", 0, 0, 0),
        10:  TimeParams("lag", 0, 1, 10),
        15:  TimeParams("lag", 0, 1, 15),
        20:  TimeParams("lag", 0, 1, 20),
        25:  TimeParams("lag", 0, 1, 25),
        30:  TimeParams("lag", 0, 1, 30),
        35:  TimeParams("lag", 0, 1, 35),
        40:  TimeParams("lag", 0, 1, 40),
        45:  TimeParams("lag", 0, 1, 45),
        50:  TimeParams("lag", 0, 1, 50),
        60:  TimeParams("lag", 0, 2, 60),
        70:  TimeParams("lag", 0, 2, 70),
        80:  TimeParams("lag", 0, 2, 80),
        90:  TimeParams("lag", 0, 2, 90),
        100: TimeParams("lag", 0, 2, 100),
        120: TimeParams("lag", 0, 3, 120),
        140: TimeParams("lag", 0, 3, 140),
        160: TimeParams("lag", 0, 4, 160),
        180: TimeParams("lag", 0, 4, 180),
        200: TimeParams("lag", 0, 5, 200),
    },
    "spring": {
        0:   TimeParams("immediate", 0, 0, 0),
        10:  TimeParams("lead", 1, 0, 10, "momentum"),
        15:  TimeParams("lead", 1, 0, 15, "momentum"),
        20:  TimeParams("lead", 1, 0, 20, "momentum"),
        25:  TimeParams("lead", 1, 0, 25, "momentum"),
        30:  TimeParams("lead", 1, 0, 30, "momentum"),
        35:  TimeParams("lead", 2, 0, 35, "momentum"),
        40:  TimeParams("lead", 2, 0, 40, "momentum"),
        45:  TimeParams("lead", 2, 0, 45, "momentum"),
        50:  TimeParams("lead", 2, 0, 50, "momentum"),
        60:  TimeParams("lead", 2, 0, 60, "order_flow"),
        70:  TimeParams("lead", 2, 1, 70, "order_flow"),
        80:  TimeParams("lead", 2, 1, 80, "order_flow"),
        90:  TimeParams("lead", 3, 1, 90, "order_flow"),
        100: TimeParams("lead", 3, 1, 100, "order_flow"),
        120: TimeParams("lead", 3, 2, 120, "order_flow"),
        140: TimeParams("lead", 3, 2, 140, "order_flow"),
        160: TimeParams("lag", 0, 2, 160),
        180: TimeParams("lag", 0, 3, 180),
        200: TimeParams("lag", 0, 3, 200),
    },
    "arbitrage": {
        0:   TimeParams("immediate", 0, 0, 0),
        10:  TimeParams("lead", 1, 0, 10, "microstructure"),
        15:  TimeParams("lead", 1, 0, 15, "microstructure"),
        20:  TimeParams("lead", 1, 0, 20, "microstructure"),
        25:  TimeParams("lead", 1, 0, 25, "microstructure"),
        30:  TimeParams("lead", 1, 0, 30, "microstructure"),
        35:  TimeParams("lead", 2, 0, 35, "microstructure"),
        40:  TimeParams("lead", 2, 0, 40, "microstructure"),
        45:  TimeParams("lead", 2, 0, 45, "microstructure"),
        50:  TimeParams("lead", 2, 0, 50, "microstructure"),
        60:  TimeParams("lead", 2, 0, 60, "microstructure"),
        70:  TimeParams("lead", 3, 0, 70, "microstructure"),
        80:  TimeParams("lead", 3, 0, 80, "microstructure"),
        90:  TimeParams("lead", 3, 0, 90, "microstructure"),
        100: TimeParams("lead", 3, 0, 100, "microstructure"),
        120: TimeParams("lead", 4, 0, 120, "microstructure"),
        140: TimeParams("lead", 4, 0, 140, "microstructure"),
        160: TimeParams("lead", 5, 0, 160, "microstructure"),
        180: TimeParams("lead", 5, 0, 180, "microstructure"),
        200: TimeParams("lead", 6, 0, 200, "microstructure"),
    },
    "market_making": {
        0:   TimeParams("immediate", 0, 0, 0),
        10:  TimeParams("lead", 1, 0, 10, "order_flow"),
        15:  TimeParams("lead", 1, 0, 15, "order_flow"),
        20:  TimeParams("lead", 1, 0, 20, "order_flow"),
        25:  TimeParams("lead", 1, 0, 25, "order_flow"),
        30:  TimeParams("lead", 1, 0, 30, "order_flow"),
        35:  TimeParams("lead", 2, 0, 35, "order_flow"),
        40:  TimeParams("lead", 2, 0, 40, "order_flow"),
        45:  TimeParams("lead", 2, 0, 45, "order_flow"),
        50:  TimeParams("lead", 2, 0, 50, "order_flow"),
        60:  TimeParams("lead", 2, 0, 60, "order_flow"),
        70:  TimeParams("lead", 3, 0, 70, "order_flow"),
        80:  TimeParams("lead", 3, 0, 80, "order_flow"),
        90:  TimeParams("lead", 3, 0, 90, "order_flow"),
        100: TimeParams("lead", 3, 0, 100, "order_flow"),
        120: TimeParams("lead", 4, 0, 120, "order_flow"),
        140: TimeParams("lead", 4, 0, 140, "order_flow"),
        160: TimeParams("lead", 5, 0, 160, "order_flow"),
        180: TimeParams("lead", 5, 0, 180, "order_flow"),
        200: TimeParams("lead", 6, 0, 200, "order_flow"),
    },
}


# ============================================================================
# 二、策略-参数映射（策略标识 → 回测函数 + 默认参数）
# ============================================================================

STRATEGY_RUNNERS = {
    "high_freq": {
        "run_fn": "run_backtest_hft",
        "default_params": PARAM_DEFAULTS_HFT,
        "description": "S1高频趋势共振：tick级决策，lead模式提前布局",
    },
    "resonance": {
        "run_fn": "run_backtest",
        "default_params": PARAM_DEFAULTS,
        "description": "S2分钟级趋势共振：分钟Bar决策，lag模式等待确认",
    },
    "box": {
        "run_fn": "run_backtest_box_extreme",
        "default_params": PARAM_DEFAULTS_BOX_EXTREME,
        "description": "S3箱体极值：箱体边界反转，lag模式防假突破",
    },
    "spring": {
        "run_fn": "run_backtest_box_spring",
        "default_params": PARAM_DEFAULTS_BOX_SPRING,
        "description": "S4箱体弹簧：波动率脉冲，混合lead+lag模式",
    },
    "arbitrage": {
        "run_fn": "run_backtest_arbitrage",
        "default_params": PARAM_DEFAULTS_ARBITRAGE,
        "description": "S5微观结构套利：价格偏离公允价值→反向套利，lead模式快速捕捉",
    },
    "market_making": {
        "run_fn": "run_backtest_market_making",
        "default_params": PARAM_DEFAULTS_MARKET_MAKING,
        "description": "S6做市防御：挂单库存管理+方向不反转，lead模式订单流驱动",
    },
}


# ============================================================================
# 三、DuckDB三维映射表
# ============================================================================

class DelayTimeSharpe3D:
    """
    延迟-时间参数-夏普三维映射表。

    基于DuckDB内存表存储，支持快速查询和插值。
    对每个延迟档位，遍历所有可行的时间参数组合，记录夏普。
    """

    def __init__(self, db_path: str = MAPPING_DB):
        self.db_path = db_path
        self.conn = connect(db_path)
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS delay_tiers (
                delay_ms FLOAT PRIMARY KEY,
                tier_label VARCHAR,
                description VARCHAR
            )
        """)
        existing = self.conn.execute(
            "SELECT COUNT(*) FROM delay_tiers"
        ).fetchone()[0]
        if existing == 0:
            for d in DELAY_TIERS:
                self.conn.execute(
                    "INSERT INTO delay_tiers VALUES (?, ?, ?)",  # 预留回测分析查询表(P2-9)
                    [float(d), f"{d}ms", DELAY_TIER_LABELS.get(d, "")],
                )

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS time_param_sets (
                param_set_id INTEGER PRIMARY KEY,
                strategy VARCHAR,
                delay_ms FLOAT,
                mode VARCHAR,
                ticks INTEGER,
                bars INTEGER,
                prediction_model VARCHAR,
                confirmation_threshold FLOAT,
                is_default BOOLEAN DEFAULT FALSE,
                UNIQUE(strategy, delay_ms, mode, ticks, bars)
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS param_set_id_seq START 1
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sharpe_results (
                result_id INTEGER PRIMARY KEY,
                param_set_id INTEGER,
                backtest_id VARCHAR,
                strategy VARCHAR,
                delay_ms FLOAT,
                mode VARCHAR,
                ticks INTEGER,
                bars INTEGER,
                total_sharpe FLOAT,
                total_return FLOAT,
                max_drawdown FLOAT,
                num_signals INTEGER,
                strategy_type VARCHAR,
                hmm_low_vol_sharpe FLOAT DEFAULT 0.0,
                hmm_normal_sharpe FLOAT DEFAULT 0.0,
                hmm_high_vol_sharpe FLOAT DEFAULT 0.0,
                slippage_mean FLOAT DEFAULT 0.0,
                slippage_std FLOAT DEFAULT 0.0,
                win_rate FLOAT DEFAULT 0.0,
                profit_loss_ratio FLOAT DEFAULT 0.0,
                total_trades INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS result_id_seq START 1
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS grid_search_meta (
                search_id VARCHAR PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_combinations INTEGER,
                completed INTEGER DEFAULT 0,
                status VARCHAR DEFAULT 'running',
                delay_range VARCHAR,
                strategy_list VARCHAR
            )
        """)

    def _seed_default_params(self):
        """将四策略的默认时间参数写入表"""
        for strategy, delay_map in DEFAULT_TIME_PARAMS.items():
            for delay_ms, tp in delay_map.items():
                existing = self.conn.execute("""
                    SELECT COUNT(*) FROM time_param_sets
                    WHERE strategy=? AND delay_ms=? AND mode=? AND ticks=? AND bars=?
                """, [strategy, float(delay_ms), tp.mode, tp.ticks, tp.bars]).fetchone()[0]
                if existing == 0:
                    self.conn.execute("""
                        INSERT INTO time_param_sets
                        (param_set_id, strategy, delay_ms, mode, ticks, bars,
                         prediction_model, confirmation_threshold, is_default)
                        VALUES (nextval('param_set_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        strategy, float(delay_ms), tp.mode, tp.ticks, tp.bars,
                        tp.prediction_model, tp.confirmation_threshold, True,
                    ])

    def generate_variants(
        self,
        default: TimeParams,
        tick_range: Optional[List[int]] = None,
        bar_range: Optional[List[int]] = None,
    ) -> List[TimeParams]:
        """围绕默认值生成参数变体"""
        if tick_range is None:
            tick_range = [0, 1, 2, 3, 4, 5, 6]
        if bar_range is None:
            bar_range = [0, 1, 2, 3, 4, 5]

        variants = []

        if default.mode == "immediate":
            variants.append(default)
            return variants

        if default.mode == "lead":
            for t in tick_range:
                if abs(t - default.ticks) <= 2:
                    variants.append(TimeParams(
                        "lead", t, 0, default.delay_ms, default.prediction_model,
                    ))
            for b in bar_range:
                if 0 < b <= 2 and default.ticks > 0:
                    variants.append(TimeParams(
                        "lead", default.ticks, b, default.delay_ms, default.prediction_model,
                    ))

        if default.mode == "lag":
            for b in bar_range:
                if abs(b - default.bars) <= 2:
                    variants.append(TimeParams(
                        "lag", 0, b, default.delay_ms,
                        confirmation_threshold=default.confirmation_threshold,
                    ))
            for t in tick_range:
                if 0 < t <= 2 and default.bars > 0:
                    variants.append(TimeParams(
                        "lag", t, default.bars, default.delay_ms,
                        confirmation_threshold=default.confirmation_threshold,
                    ))

        if default.ticks > 0 and default.bars > 0:
            seen = {(v.ticks, v.bars, v.mode) for v in variants}
            for t in [max(0, default.ticks - 1), default.ticks, default.ticks + 1]:
                for b in [max(0, default.bars - 1), default.bars, default.bars + 1]:
                    if (t, b, "lead_lag") not in seen and t > 0 and b > 0:
                        variants.append(TimeParams(
                            "lead_lag", t, b, default.delay_ms,
                            default.prediction_model, default.confirmation_threshold,
                        ))

        seen_keys = set()
        unique_variants = []
        for v in variants:
            k = v.key()
            if k not in seen_keys:
                seen_keys.add(k)
                unique_variants.append(v)

        return unique_variants

    def _inject_delay_to_bar_data(
        self,
        bar_data: pd.DataFrame,
        delay_ms: float,
    ) -> pd.DataFrame:
        """向Bar数据注入延迟效果

        延迟效果通过以下方式模拟：
        1. 时间戳偏移：bar的minute字段后移delay_ms/60000个分钟
        2. 价格滑点增加：延迟越大，滑点越大
        3. 信息衰减：strength/imbalance随延迟衰减
        """
        if delay_ms <= 0:
            return bar_data

        delayed = bar_data.copy()

        delay_seconds = delay_ms / 1000.0
        decay_factor = np.exp(-delay_seconds / 60.0)

        if "strength" in delayed.columns:
            delayed["strength"] = delayed["strength"] * decay_factor
        if "imbalance" in delayed.columns:
            delayed["imbalance"] = delayed["imbalance"] * decay_factor

        extra_slippage_bps = SLIPPAGE_BPS * (1 + delay_ms / 50.0)
        if "bid_ask_spread" in delayed.columns:
            spread_col = delayed["bid_ask_spread"]
            price_col = delayed["close"] if "close" in delayed.columns else pd.Series([1.0] * len(delayed))
            extra_spread = price_col * extra_slippage_bps / 10000.0
            delayed["bid_ask_spread"] = spread_col + extra_spread

        return delayed

    def _apply_time_params_to_backtest(
        self,
        params: Dict[str, Any],
        tp: TimeParams,
        strategy: str,
    ) -> Dict[str, Any]:
        """将时间参数转换为回测引擎可识别的参数注入

        映射关系：
        - lead模式 + ticks > 0 → hft_signal_confirm_ticks = ticks (S1)
                               → state_confirm_bars = ticks (S2/S3/S4)
        - lag模式 + bars > 0 → 额外确认bars，通过增加state_confirm_bars实现
        - lead_lag混合 → 同时设置两种确认参数
        """
        modified = dict(params)

        if strategy == "high_freq":
            if tp.mode in ("lead", "lead_lag") and tp.ticks > 0:
                modified["hft_signal_confirm_ticks"] = tp.ticks
            if tp.mode in ("lag", "lead_lag") and tp.bars > 0:
                modified["hft_signal_confirm_ticks"] = max(
                    tp.ticks, tp.bars * 3
                )

        else:
            base_confirm_bars = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5

            if tp.mode in ("lead", "lead_lag") and tp.ticks > 0:
                modified["state_confirm_bars"] = max(1, base_confirm_bars - tp.ticks + 1)
            if tp.mode in ("lag", "lead_lag") and tp.bars > 0:
                modified["state_confirm_bars"] = base_confirm_bars + tp.bars

            if tp.mode == "lag" and tp.bars > 0:
                extra_cooldown = tp.bars * 60.0
                base_cooldown = params.get("signal_cooldown_sec", 60.0)
                modified["signal_cooldown_sec"] = base_cooldown + extra_cooldown

            if tp.mode == "lead_lag":
                if tp.ticks > 0:
                    modified["state_confirm_bars"] = max(
                        1, base_confirm_bars - tp.ticks + 1
                    )
                if tp.bars > 0:
                    modified["state_confirm_bars"] = (
                        modified.get("state_confirm_bars", base_confirm_bars) + tp.bars
                    )

        return modified

    def _run_single_backtest(
        self,
        bar_data: pd.DataFrame,
        strategy: str,
        tp: TimeParams,
        base_params: Dict[str, Any],
        train: bool = True,
    ) -> Dict[str, Any]:
        """单次回测：注入延迟 → 应用时间参数 → 运行策略 → 计算指标"""
        delayed_data = self._inject_delay_to_bar_data(bar_data, tp.delay_ms)
        modified_params = self._apply_time_params_to_backtest(base_params, tp, strategy)

        runner_info = STRATEGY_RUNNERS[strategy]
        run_fn_name = runner_info["run_fn"]

        if run_fn_name == "run_backtest_hft":
            if run_backtest_hft is None:
                logger.error("[Backtest] run_backtest_hft is None, skipping")
                return {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": "run_backtest_hft is None"}
            result = run_backtest_hft(modified_params, delayed_data, train, "hft")
        elif run_fn_name == "run_backtest":
            if run_backtest is None:
                logger.error("[Backtest] run_backtest is None, skipping")
                return {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": "run_backtest is None"}
            result = run_backtest(modified_params, delayed_data, train, "main")
        elif run_fn_name == "run_backtest_box_extreme":
            if run_backtest_box_extreme is None:
                logger.error("[Backtest] run_backtest_box_extreme is None, skipping")
                return {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": "run_backtest_box_extreme is None"}
            result = run_backtest_box_extreme(modified_params, delayed_data, train, "box_extreme")
        elif run_fn_name == "run_backtest_box_spring":
            if run_backtest_box_spring is None:
                logger.error("[Backtest] run_backtest_box_spring is None, skipping")
                return {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": "run_backtest_box_spring is None"}
            result = run_backtest_box_spring(modified_params, delayed_data, train, "box_spring")
        else:
            if run_backtest is None:
                logger.error("[Backtest] run_backtest is None, skipping")
                return {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": "run_backtest is None"}
            result = run_backtest(modified_params, delayed_data, train, "main")

        return result

    def _calculate_hmm_sharpes(
        self,
        bar_data: pd.DataFrame,
        returns_per_bar: np.ndarray,
    ) -> Dict[str, float]:
        """分HMM状态(低/中/高波动)计算夏普

        使用简单IV三分位分割近似HMM状态。
        """
        hmm_sharpes = {"LOW_VOL": 0.0, "NORMAL": 0.0, "HIGH_VOL": 0.0}

        if "iv" not in bar_data.columns or len(returns_per_bar) == 0:
            return hmm_sharpes

        iv = bar_data["iv"].replace(0, np.nan).dropna()
        if len(iv) < 30:
            return hmm_sharpes

        q33 = iv.quantile(0.33)
        q66 = iv.quantile(0.66)

        n_bars = min(len(bar_data), len(returns_per_bar))
        iv_vals = bar_data["iv"].values[:n_bars]  # R21-MEM-P2-01修复: ndarray切片返回视图，无额外内存开销
        ret_vals = returns_per_bar[:n_bars]

        for label, mask_fn in [
            ("LOW_VOL", lambda v: v <= q33),
            ("NORMAL", lambda v: (v > q33) & (v <= q66)),
            ("HIGH_VOL", lambda v: v > q66),
        ]:
            mask = mask_fn(iv_vals)
            regime_returns = ret_vals[mask]
            if len(regime_returns) > 10:
                mean_r = np.mean(regime_returns)
                std_r = np.std(regime_returns)
                hmm_sharpes[label] = (
                    np.sqrt(ANNUALIZE_FACTOR_MINUTE) * mean_r / std_r if std_r > 1e-10 else 0.0
                )

        return hmm_sharpes

    def _store_result(
        self,
        strategy: str,
        tp: TimeParams,
        backtest_result: Dict[str, Any],
        hmm_sharpes: Optional[Dict[str, float]] = None,
    ):
        """存储单次回测结果到DuckDB"""
        if hmm_sharpes is None:
            hmm_sharpes = {"LOW_VOL": 0.0, "NORMAL": 0.0, "HIGH_VOL": 0.0}

        param_set_id = self.conn.execute("""
            SELECT param_set_id FROM time_param_sets
            WHERE strategy=? AND delay_ms=? AND mode=? AND ticks=? AND bars=?
        """, [strategy, tp.delay_ms, tp.mode, tp.ticks, tp.bars]).fetchone()

        if param_set_id is None:
            param_set_id = self.conn.execute("""
                INSERT INTO time_param_sets
                (param_set_id, strategy, delay_ms, mode, ticks, bars,
                 prediction_model, confirmation_threshold, is_default)
                VALUES (nextval('param_set_id_seq'), ?, ?, ?, ?, ?, ?, ?, FALSE)
                RETURNING param_set_id
            """, [strategy, tp.delay_ms, tp.mode, tp.ticks, tp.bars,
                  tp.prediction_model, tp.confirmation_threshold]).fetchone()[0]
        else:
            param_set_id = param_set_id[0]

        self.conn.execute("""
            INSERT INTO sharpe_results
            (result_id, param_set_id, backtest_id, strategy, delay_ms, mode, ticks, bars,
             total_sharpe, total_return, max_drawdown, num_signals, strategy_type,
             hmm_low_vol_sharpe, hmm_normal_sharpe, hmm_high_vol_sharpe)
            VALUES (nextval('result_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            param_set_id,
            f"{strategy}_{tp.delay_ms}ms_{tp.mode}_{tp.ticks}t_{tp.bars}b",
            strategy, tp.delay_ms, tp.mode, tp.ticks, tp.bars,
            backtest_result.get("sharpe", float('nan')),
            backtest_result.get("total_return", 0.0),
            backtest_result.get("max_drawdown", 0.0),
            backtest_result.get("num_signals", 0),
            backtest_result.get("strategy_type", strategy),
            hmm_sharpes.get("LOW_VOL", 0.0),
            hmm_sharpes.get("NORMAL", 0.0),
            hmm_sharpes.get("HIGH_VOL", 0.0),
        ])

    def run_3d_grid_search(
        self,
        bar_data: pd.DataFrame,
        delay_range: Optional[List[float]] = None,
        tick_range: Optional[List[int]] = None,
        bar_range: Optional[List[int]] = None,
        strategies: Optional[List[str]] = None,
        train: bool = True,
        max_workers: int = 1,
    ) -> pd.DataFrame:
        """三维网格搜索

        对每个延迟档位，遍历所有可行的时间参数组合，记录夏普。
        """
        if delay_range is None:
            delay_range = DELAY_TIERS
        if strategies is None:
            strategies = STRATEGY_NAMES

        self._seed_default_params()

        search_id = f"search_{int(time.time())}"
        total_combos = 0
        all_tasks = []

        for delay_ms in delay_range:
            for strategy in strategies:
                default_tp = DEFAULT_TIME_PARAMS[strategy].get(delay_ms)
                if default_tp is None:
                    continue
                variants = self.generate_variants(default_tp, tick_range, bar_range)
                base_params = STRATEGY_RUNNERS[strategy]["default_params"]
                for tp in variants:
                    all_tasks.append((strategy, tp, base_params))
                    total_combos += 1

        self.conn.execute("""
            INSERT INTO grid_search_meta  -- 预留回测分析查询表(P2-9)
            (search_id, start_time, total_combinations, status, delay_range, strategy_list)
            VALUES (?, CURRENT_TIMESTAMP, ?, 'running', ?, ?)
        """, [search_id, total_combos,
              str(delay_range), str(strategies)])

        logger.info(
            "[3D_GRID] 开始网格搜索: %d组合, 延迟=%s, 策略=%s",
            total_combos, delay_range, strategies,
        )

        results = []
        completed = 0

        _DATA_SIZE_THRESHOLD_MB = 200
        _bar_data_mb = bar_data.memory_usage(deep=True).sum() / (1024 * 1024) if isinstance(bar_data, pd.DataFrame) else 0
        if max_workers > 1 and _bar_data_mb > _DATA_SIZE_THRESHOLD_MB:
            logger.warning(
                "[3D_GRID] DataFrame %.1fMB超过阈值%dMB，回退单进程执行",
                _bar_data_mb, _DATA_SIZE_THRESHOLD_MB,
            )
            max_workers = 1

        if max_workers <= 1:
            for strategy, tp, base_params in all_tasks:
                r = self._run_single_backtest(
                    bar_data, strategy, tp, base_params, train,
                )
                if r.get('error'):
                    logger.warning("[3D_GRID] Skipping variant due to: %s", r['error'])
                    completed += 1
                    continue
                self._store_result(strategy, tp, r)
                results.append({
                    "delay_ms": tp.delay_ms,
                    "strategy": strategy,
                    "mode": tp.mode,
                    "ticks": tp.ticks,
                    "bars": tp.bars,
                    **r,
                })
                completed += 1
                if completed % 50 == 0:
                    logger.info("[3D_GRID] 进度: %d/%d", completed, total_combos)
        else:
            # R21-CC-P1-02修复: CPU密集型回测任务理论上应使用ProcessPoolExecutor绕过GIL，
            # 但实际无法改为ProcessPoolExecutor，原因如下：
            # 1. _worker_3d_backtest内部创建DelayTimeSharpe3D对象（含DuckDB连接），DuckDB连接不可pickle
            # 2. bar_data为大型DataFrame（可能>200MB），pickle序列化/反序列化开销远超GIL阻塞收益
            # 3. 回测函数run_backtest等依赖模块级全局状态，多进程fork可能导致状态不一致
            # 因此保留ThreadPoolExecutor，但限制max_workers=1以避免GIL竞争（回测本身是CPU密集型，
            # 多线程无法真正并行，反而增加上下文切换开销）
            _effective_workers = min(max_workers, 1)  # R21-CC-P1-02修复: 限制为1
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=_effective_workers) as executor:
                futures = {}
                for strategy, tp, base_params in all_tasks:
                    future = executor.submit(
                        _worker_3d_backtest,
                        bar_data, strategy, tp, base_params, train,
                    )
                    futures[future] = (strategy, tp)

                for future in as_completed(futures):
                    strategy, tp = futures[future]
                    try:
                        r = future.result()
                        # R21-CC04修复: 检查回测结果有效性，异常结果标记error而非零值写入数据库
                        if r is None or not isinstance(r, dict):
                            logger.error("[3D_GRID] 回测返回无效结果: %s - result=%s", strategy, r)
                            r = {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": "invalid_result"}
                    except Exception as e:
                        logger.error("[3D_GRID] 回测异常: %s - %s", strategy, e)
                        # R21-CC04修复: 异常结果用NaN而非0.0，防止零值污染数据库最优参数查询
                        r = {"sharpe": float('nan'), "total_return": float('nan'), "max_drawdown": float('nan'), "num_signals": 0, "error": str(e)}

                    self._store_result(strategy, tp, r)
                    results.append({
                        "delay_ms": tp.delay_ms,
                        "strategy": strategy,
                        "mode": tp.mode,
                        "ticks": tp.ticks,
                        "bars": tp.bars,
                        **r,
                    })
                    completed += 1

        self.conn.execute("""
            UPDATE grid_search_meta
            SET end_time=CURRENT_TIMESTAMP, completed=?, status='completed'
            WHERE search_id=?
        """, [completed, search_id])

        logger.info("[3D_GRID] 完成: %d/%d组合", completed, total_combos)
        return pd.DataFrame(results)

    def get_optimal_params(
        self,
        delay_ms: float,
        objective: str = "total_sharpe",
        strategy: Optional[str] = None,
    ) -> pd.DataFrame:
        """查询给定延迟下的最优参数"""
        # R21-CC04修复: 排除NaN/异常结果，防止零值污染最优参数查询
        where_clause = "delay_ms = ? AND total_sharpe IS NOT NaN AND total_sharpe != 0.0"
        params = [float(delay_ms)]

        if strategy is not None:
            where_clause += " AND strategy = ?"  # R21-MEM-P2-04修复: SQL条件拼接，单次+=可接受
            params.append(strategy)

        sql = f"""
            SELECT strategy, delay_ms, mode, ticks, bars,
                   total_sharpe, total_return, max_drawdown, num_signals,
                   hmm_low_vol_sharpe, hmm_normal_sharpe, hmm_high_vol_sharpe
            FROM sharpe_results
            WHERE {where_clause}
            ORDER BY {objective} DESC
            LIMIT 10
        """
        return self.conn.execute(sql, params).fetchdf()

    def get_sharpe_surface(
        self,
        strategy: str,
        delay_ms: float,
    ) -> pd.DataFrame:
        """获取夏普曲面（ticks × bars × sharpe）用于可视化"""
        # R21-CC04修复补全: 排除NaN/异常结果
        return self.conn.execute("""
            SELECT ticks, bars, mode, total_sharpe, max_drawdown, total_return
            FROM sharpe_results
            WHERE strategy = ? AND delay_ms = ? AND total_sharpe IS NOT NaN AND total_sharpe != 0.0
            ORDER BY ticks, bars
        """, [strategy, float(delay_ms)]).fetchdf()

    def get_core_mapping_table(self) -> pd.DataFrame:
        """获取核心映射表：延迟 × 最优时间参数 × 夏普

        每个延迟档位取每个策略的最优行，汇总为组合。
        """
        # R21-CC04修复补全: 排除NaN/异常结果 — NaN在DuckDB DESC排序中排最前，会被误选为最优
        return self.conn.execute("""
            WITH best_per_strategy AS (
                SELECT strategy, delay_ms, mode, ticks, bars,
                       total_sharpe, total_return, max_drawdown,
                       ROW_NUMBER() OVER (
                           PARTITION BY strategy, delay_ms
                           ORDER BY total_sharpe DESC
                       ) as rn
                FROM sharpe_results
                WHERE total_sharpe IS NOT NaN AND total_sharpe != 0.0
            )
            SELECT strategy, delay_ms, mode, ticks, bars,
                   total_sharpe, total_return, max_drawdown
            FROM best_per_strategy
            WHERE rn = 1
            ORDER BY delay_ms, strategy
        """).fetchdf()

    def get_hmm_breakdown(self, delay_ms: float) -> pd.DataFrame:
        """获取分HMM状态夏普分解"""
        # R21-CC04修复补全: 排除NaN — AVG()对NaN传播导致整个结果为NaN
        return self.conn.execute("""
            SELECT strategy,
                   AVG(hmm_low_vol_sharpe) as hmm_low_vol_sharpe,
                   AVG(hmm_normal_sharpe) as hmm_normal_sharpe,
                   AVG(hmm_high_vol_sharpe) as hmm_high_vol_sharpe
            FROM sharpe_results
            WHERE delay_ms = ? AND total_sharpe IS NOT NaN
            GROUP BY strategy
            ORDER BY strategy
        """, [float(delay_ms)]).fetchdf()

    def get_sensitivity_table(
        self,
        delay_ms: float,
        strategy: str,
    ) -> pd.DataFrame:
        """获取参数敏感度表"""
        # R21-CC04修复补全: 排除NaN/异常结果
        return self.conn.execute("""
            SELECT ticks, bars, mode,
                   total_sharpe, max_drawdown, total_return, num_signals
            FROM sharpe_results
            WHERE delay_ms = ? AND strategy = ? AND total_sharpe IS NOT NaN AND total_sharpe != 0.0
            ORDER BY total_sharpe DESC
        """, [float(delay_ms), strategy]).fetchdf()

    def get_sharpe_decay_curve(self) -> pd.DataFrame:
        """获取组合夏普随延迟的衰减曲线"""
        # R21-CC04修复补全: 排除NaN — MAX()对NaN返回NaN导致衰减曲线断裂
        return self.conn.execute("""
            WITH best_per_delay AS (
                SELECT delay_ms, strategy,
                       MAX(total_sharpe) as best_sharpe,
                       MAX(total_return) as best_return,
                       MIN(max_drawdown) as best_drawdown
                FROM sharpe_results
                WHERE total_sharpe IS NOT NaN AND total_sharpe != 0.0
                GROUP BY delay_ms, strategy
            )
            SELECT delay_ms, strategy, best_sharpe, best_return, best_drawdown
            FROM best_per_delay
            ORDER BY delay_ms, strategy
        """).fetchdf()

    def export_all_tables(self) -> Dict[str, pd.DataFrame]:
        """导出所有表为DataFrame字典"""
        tables = {}
        for table_name in ["delay_tiers", "time_param_sets", "sharpe_results", "grid_search_meta"]:
            tables[table_name] = self.conn.execute(
                f"SELECT * FROM {table_name}"
            ).fetchdf()
        return tables

    def query_delay_tier(self, delay_ms: float) -> Optional[Dict[str, Any]]:  # P2-9修复: 提供delay_tiers查询消费方法
        """查询指定延迟档位的参数配置"""
        try:
            rows = self.conn.execute("SELECT delay_ms, tier_label, description FROM delay_tiers WHERE delay_ms = ?", [float(delay_ms)]).fetchall()
            if rows:
                return {'delay_ms': rows[0][0], 'tier_label': rows[0][1], 'description': rows[0][2]}
        except Exception as e:
            logging.debug("[DelayTimeSharpe3D] P2-9: query_delay_tier查询失败: %s", e)
        return None

    def close(self):
        self.conn.close()


# ============================================================================
# 四、实盘策略选择器
# ============================================================================

class LiveStrategySelector:
    """实盘策略选择器：根据实时测量的延迟，查询映射表，选择最优时间参数"""

    def __init__(self, mapping_table: DelayTimeSharpe3D):
        self.mapping = mapping_table
        self._current_delay_ms = 0.0
        self._ema_delay_alpha = 0.1
        self._last_params: Dict[str, TimeParams] = {}
        self._degrade_threshold_ms = 120.0
        self._sample_rates: Dict[str, float] = {}
        self._degrade_factor = 2

    def on_tick(self, tick: Dict[str, Any]):
        measured_delay = self._measure_delay(tick)
        self._current_delay_ms = (
            (1 - self._ema_delay_alpha) * self._current_delay_ms
            + self._ema_delay_alpha * measured_delay
        )

        nearest_tier = self._find_nearest_tier(self._current_delay_ms)
        optimal = self.mapping.get_optimal_params(nearest_tier, "total_sharpe")
        self._apply_params(optimal)

        if self._current_delay_ms > self._degrade_threshold_ms:
            self._degrade_frequency()

    def _measure_delay(self, tick: Dict[str, Any]) -> float:
        exchange_ts = tick.get("timestamp_ms", 0)
        local_ts = int(time.time() * 1000)
        return max(0, local_ts - exchange_ts)

    def _find_nearest_tier(self, delay_ms: float) -> float:
        return float(min(DELAY_TIERS, key=lambda d: abs(d - delay_ms)))

    def _apply_params(self, optimal_df: pd.DataFrame):
        if optimal_df.empty:
            return
        for _, row in optimal_df.iterrows():
            strategy = row["strategy"]
            self._last_params[strategy] = TimeParams(
                mode=row["mode"],
                ticks=int(row["ticks"]),
                bars=int(row["bars"]),
                delay_ms=float(row["delay_ms"]),
            )

    def _degrade_frequency(self):
        logger.warning(
            "[DEGRADE] 延迟%.1fms超过%.1fms阈值，高频策略降级",
            self._current_delay_ms, self._degrade_threshold_ms,
        )
        for strategy, params in self._last_params.items():
            old_rate = self._sample_rates.get(strategy, 1.0)
            new_rate = old_rate * self._degrade_factor
            self._sample_rates[strategy] = new_rate
            if params.mode == "lead":
                self._last_params[strategy] = TimeParams(
                    mode="lag",
                    ticks=params.ticks,
                    bars=max(params.bars, 1),
                    delay_ms=params.delay_ms,
                )
                logger.info(
                    "[DEGRADE] %s: mode lead→lag, sample_rate %.1f→%.1f",
                    strategy, old_rate, new_rate,
                )
            elif params.bars < 2:
                self._last_params[strategy] = TimeParams(
                    mode=params.mode,
                    ticks=params.ticks,
                    bars=params.bars + 1,
                    delay_ms=params.delay_ms,
                )
                logger.info(
                    "[DEGRADE] %s: bars %d→%d, sample_rate %.1f→%.1f",
                    strategy, params.bars, params.bars + 1, old_rate, new_rate,
                )

    def get_current_params(self) -> Dict[str, TimeParams]:
        return dict(self._last_params)

    def get_current_delay(self) -> float:
        return self._current_delay_ms


# ============================================================================
# 五、工作进程函数
# ============================================================================

def _worker_3d_backtest(
    bar_data: pd.DataFrame,
    strategy: str,
    tp: TimeParams,
    base_params: Dict[str, Any],
    train: bool,
) -> Dict[str, Any]:
    """工作进程：运行单次回测"""
    mapping = DelayTimeSharpe3D()
    try:
        result = mapping._run_single_backtest(bar_data, strategy, tp, base_params, train)
    finally:
        mapping.close()
    return result


# ============================================================================
# 六、命令行入口
# ============================================================================

def main():
    """命令行入口：运行三维网格搜索并输出结果"""
    import argparse
    parser = argparse.ArgumentParser(description="延迟-时间参数-夏普三维映射表")
    parser.add_argument("--db", type=str, default=MAPPING_DB, help="DuckDB路径")
    parser.add_argument("--preprocessed-db", type=str, default="preprocessed.duckdb",
                        help="预处理数据DuckDB路径")
    parser.add_argument("--delay", type=float, nargs="*", default=None,
                        help="延迟档位列表(ms)")
    parser.add_argument("--strategies", type=str, nargs="*", default=None,
                        help="策略列表")
    parser.add_argument("--workers", type=int, default=1, help="并行进程数")
    args = parser.parse_args()

    try:
        pre_conn = connect(args.preprocessed_db, read_only=True)
        bar_data = pre_conn.execute("""
            SELECT * FROM minute_data
            WHERE minute >= '2023-01-01' AND minute < '2026-01-01'
            ORDER BY minute
        """).fetchdf()
        pre_conn.close()
    except Exception as e:
        logger.error("无法加载预处理数据: %s", e)
        return

    if bar_data.empty:
        logger.error("预处理数据为空")
        return

    logger.info("加载 %d 条Bar数据", len(bar_data))

    mapping = DelayTimeSharpe3D(args.db)

    results_df = mapping.run_3d_grid_search(
        bar_data=bar_data,
        delay_range=[float(d) for d in args.delay] if args.delay else None,
        strategies=args.strategies,
        train=True,
        max_workers=args.workers,
    )

    logger.info("\n=== 核心映射表 ===")
    core = mapping.get_core_mapping_table()
    print(core.to_string(index=False))

    logger.info("\n=== 夏普衰减曲线 ===")
    decay = mapping.get_sharpe_decay_curve()
    print(decay.to_string(index=False))

    for d in (args.delay or DELAY_TIERS):
        logger.info("\n=== %dms HMM分解 ===", int(d))
        hmm = mapping.get_hmm_breakdown(float(d))
        print(hmm.to_string(index=False))

    mapping.close()
    logger.info("结果已保存至 %s", args.db)


if __name__ == "__main__":
    main()
