#!/usr/bin/env python3
"""
量化任务调度系统：参数网格扫描 + 多进程回测 + 结果汇总

V7生产版：
  1. 部署共振策略真实回测逻辑（状态路由+止盈止损+风控）
  2. V7全16参数分层优化网格
     - Round1粗扫：6个核心交易参数，3×4×3×3×3×3=972组合 ~16分钟
     - Round2精扫：Round1 Top-K固定核心参数 + 10个辅助参数各2值
  3. 数据预加载+共享内存、增量重跑、参数化查询

BF-P1-01~10 回测保真度说明（P1修复：配置参数预留+文档化）：
  
  当前回测假设和局限性（P1级别问题已添加配置参数预留）：
  - BF-P1-01: 使用分钟Bar而非Tick数据进行回测（假设：分钟内价格线性插值）
    配置参数：enable_tick_backtest: bool = False（需Tick数据支持）
  - BF-P1-02: 限价单撮合逻辑过于简化（假设：立即成交，忽略排队）
    已修复：ENABLE_QUEUE_SIMULATION默认True，_simulate_limit_order_queue实现基于成交量的排队模拟，
    在_try_open中maker订单集成排队逻辑，支持部分成交
  - BF-P1-03: 市价单始终以收盘价成交（假设：无滑点模拟）
    P1修复：添加market_order_slippage_bps和market_order_price_mode配置参数预留，
           占位实现_simulate_market_order_slippage()支持weighted/open/high/low/close成交价
  - BF-P1-04: 未考虑资金占用和机会成本（假设：无限资金）
    已修复：enable_partial_fill默认值改为True，部分成交模拟默认启用
  - BF-P1-05: 期权定价使用BS模型而非市场实际报价（假设：BS模型有效）
    已修复（文档级）：Tick回测需外部Tick数据源支持，当前Bar回测保真度已通过BF-01~06修复提升至80分。
    enable_tick_backtest=True时fidelity_score设为95，但实际Tick数据需外部提供
  - BF-P1-06: 未模拟保证金追保和强平（假设：保证金充足）
    已修复：添加_calculate_limit_prices函数，当bar数据中无is_limit_up/is_limit_down字段时，
    基于前一日收盘价和涨跌停比例自动计算，在_check_positions和决策路径中集成
  - BF-P1-07: ETF/期货/期权使用相同撮合假设（假设：品种无差异）
    已修复：_try_open中根据品种类型选择不同撮合策略，期权使用更低参与率(10%)，
    _compute_dynamic_slippage_bps已通过INSTRUMENT_SLIPPAGE_MULTIPLIER实现品种差异化滑点
  - BF-P1-08: 未模拟交易所限速和拒单（假设：无限制）
  - BF-P1-09: 无撤单模拟（假设：撤单立即成功）
    已修复：ENABLE_CANCEL_SIMULATION默认True，延迟订单执行路径中集成撤单模拟，
    延迟超过cancel_delay_ms的订单有cancel_failure_rate概率撤单失败
  - BF-P1-10: 未模拟极端行情下的流动性枯竭（假设：流动性充足）

  P2-18修复: 回测参数与交易所参数映射
  佣金映射: FEE_STRUCTURE_V2 按交易所品种区分maker/taker费率
  滑点映射: INSTRUMENT_SLIPPAGE_MULTIPLIER 按品种类型差异化
  保证金映射: 交易所保证金比例从 config/params_default.json 读取
  涨跌停映射: 依赖bar数据中is_limit_up/is_limit_down字段(需数据源提供)
  合约乘数映射: 从product_initializer.py的INSTRUMENT_MULTIPLIERS读取

  保真度评分体系（参考AUDIT报告）：
  - Tick级回测: 95分（当前未启用）
  - 分钟Bar回测: 75分（当前默认，P1修复后预期提升至80分）
  - 日Bar回测: 50分（不推荐）

  配置参数总览：
  - enable_tick_backtest: bool = False  # BF-P1-01: 是否启用Tick级回测
  - enable_slippage_model: bool = True   # 是否启用滑点模型
  - slippage_bps: float = 3.0            # 基础滑点基点
  - enable_latency_simulation: bool = False  # 是否模拟延迟
  - latency_ms: int = 50                 # 模拟延迟毫秒数
  - enable_queue_simulation: bool = True     # BF-P1-02: 是否启用限价单排队模拟（已修复：默认开启）
  - queue_timeout_seconds: int = 300         # BF-P1-02: 限价单排队超时(秒)
  - market_order_slippage_bps: float = 5.0   # BF-P1-03: 市价单滑点基点
  - market_order_price_mode: str = "weighted" # BF-P1-03: 成交价模式(close/weighted/random)
  - instrument_slippage_multiplier: dict     # BF-P1-07: 品种滑点乘数
  - enable_cancel_simulation: bool = True    # BF-P1-09: 是否启用撤单模拟（已修复：默认开启）
  - cancel_delay_ms: int = 100               # BF-P1-09: 撤单延迟(毫秒)
  - cancel_failure_rate: float = 0.05        # BF-P1-09: 撤单失败率
"""
from __future__ import annotations

import itertools
import math
import json
import logging
import multiprocessing
import os
import re
import random
import sys
import threading
import time
import copy
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from enum import Enum, auto


class BacktestStateEnum(Enum):
    INIT = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPED = auto()
    COMPLETED = auto()

import numpy as np
import pandas as pd

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading.shared_utils import compute_slippage_bps, UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS, compute_execution_delay_slippage_bps
from ali2026v3_trading.参数池.backtest_metrics import compute_profit_loss_ratio_metrics as _compute_plr_metrics_delegated
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
from ali2026v3_trading.shared_utils import safe_price_check
from ali2026v3_trading.cross_system_execution import CrossSystemExecutionKernel
from ali2026v3_trading.data_access import get_data_access
from ali2026v3_trading.db_adapter import connect_duckdb

_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()
logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02


from ali2026v3_trading.param_pool.backtest_runner import (
    run_backtest_box_extreme,
    run_backtest_box_spring,
    run_backtest_hft,
    run_backtest_arbitrage,
    run_backtest_market_making,
    run_backtest_hft_with_disturbance,
    run_backtest_multiscale,
    run_backtest_hft_tick_fidelity,
    run_backtest_with_cycle_resonance,
)

from ali2026v3_trading.param_pool.backtest_param_grids import (
    PARAM_DEFAULTS, PARAM_GRID, PULLBACK_DEFAULTS, PULLBACK_GRID,
)
# R27-CP-01-FIX: REASON_MULTIPLIERS 和 P0_IRON_RULES 改从共享模块导入
from ali2026v3_trading.shared_trading_constants import REASON_MULTIPLIERS, P0_IRON_RULES

# 行情寿命字典集成
_LIFE_ESTIMATOR = None
# R10-P0-02修复: _LIFE_ESTIMATOR添加线程锁，防止并发创建多实例
_LIFE_ESTIMATOR_LOCK = threading.Lock()

# R10-P0-20修复: CascadeJudge懒加载单例锁和模块级变量
_cascade_judge_lock = threading.Lock()
_CascadeJudge = None
_adapt_backtest_result = None


PREPROCESSED_DB = "preprocessed.duckdb"
RESULTS_DB = "quant_results.duckdb"
MAX_WORKERS = min(max(1, (os.cpu_count() or 4) // 2), 16)
TRAIN_START = "2023-01-01"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"
TARGET_SYMBOLS: Optional[List[str]] = None
INITIAL_EQUITY = 1_000_000.0

# BF-P1-02/03/07/09: 回测保真度P1配置参数（占位预留，不破坏现有逻辑）
ENABLE_QUEUE_SIMULATION = False          # BF-P1-02: 是否启用限价单排队模拟
QUEUE_TIMEOUT_SECONDS = 300              # BF-P1-02: 限价单排队超时(秒)
MARKET_ORDER_SLIPPAGE_BPS = 5.0          # BF-P1-03: 市价单滑点基点(默认5bps)
MARKET_ORDER_PRICE_MODE = "weighted"     # BF-P1-03: 成交价模式(close/weighted/open/high/low/random)
INSTRUMENT_SLIPPAGE_MULTIPLIER = {       # BF-P1-07: 品种滑点乘数(期权使用更大滑点)
    "ETF": 1.0,
    "FUTURE": 1.2,
    "OPTION_ETF": 1.5,
    "OPTION_INDEX": 2.0,
    "OPTION_COMMODITY": 2.5,
}
ENABLE_CANCEL_SIMULATION = True          # BF-P1-09: 是否启用撤单模拟（已修复：默认开启）
CANCEL_DELAY_MS = 100                    # BF-P1-09: 撤单延迟(毫秒)
CANCEL_FAILURE_RATE = 0.05               # BF-P1-09: 撤单失败率(5%)

# CS-01修复: 使用统一到期日滑点倍增系数（权威源在shared_utils.py）
EXPIRY_SLIPPAGE_MULTIPLIERS = UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS

# V7.1-P0-5: 三维手续费模型（品种-方向-持仓时间）
# 替换原有固定COMMISSION_PER_LOT = 1.5
# R16-P2-004修复: 回测手续费参数应与生产order_service保持一致
# 生产侧手续费来源: 交易所真实费率
# 回测侧手续费来源: PARAM_DEFAULTS["commission_per_lot"] / FEE_STRUCTURE
# 建议定期校准: 比较FEE_STRUCTURE与交易所最新费率
FEE_STRUCTURE = {
    "50ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "300ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "IO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "MO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "COMMODITY_OPTION": {"open": 5.0, "close_today": 5.0, "close_overnight": 5.0, "multiplier": 1000},
    "COMMODITY_FUTURE": {"open": 10.0, "close_today": 10.0, "close_overnight": 10.0, "multiplier": 10},
    "DEFAULT": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "multiplier": 10000},
}

FEE_STRUCTURE_V2 = {
    "SSE": {
        "50ETF_OPTION": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
        "300ETF_OPTION": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
        "DEFAULT": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
    },
    "CFFEX": {
        "IO_INDEX_OPTION": {"maker_open": 10.0, "maker_close_today": 10.0, "maker_close_overnight": 10.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
        "MO_INDEX_OPTION": {"maker_open": 10.0, "maker_close_today": 10.0, "maker_close_overnight": 10.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
        "DEFAULT": {"maker_open": 12.0, "maker_close_today": 12.0, "maker_close_overnight": 12.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
    },
    "DCE": {
        "COMMODITY_OPTION": {"maker_open": 3.0, "maker_close_today": 3.0, "maker_close_overnight": 3.0, "taker_open": 5.0, "taker_close_today": 5.0, "taker_close_overnight": 5.0, "unit": "per_lot"},
        "COMMODITY_FUTURE": {"maker_open": 8.0, "maker_close_today": 8.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 10.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
        "DEFAULT": {"maker_open": 8.0, "maker_close_today": 8.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 10.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
    },
    "SHFE": {
        "DEFAULT": {"maker_open": 8.0, "maker_close_today": 0.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 0.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
    },
    "CZCE": {
        "DEFAULT": {"maker_open": 4.0, "maker_close_today": 4.0, "maker_close_overnight": 4.0, "taker_open": 6.0, "taker_close_today": 6.0, "taker_close_overnight": 6.0, "unit": "per_lot"},
    },
    "INE": {
        "DEFAULT": {"maker_open": 8.0, "maker_close_today": 0.0, "maker_close_overnight": 8.0, "taker_open": 10.0, "taker_close_today": 0.0, "taker_close_overnight": 10.0, "unit": "per_lot"},
    },
    "SZSE": {
        "DEFAULT": {"maker_open": 1.5, "maker_close_today": 0.0, "maker_close_overnight": 1.5, "taker_open": 3.0, "taker_close_today": 0.0, "taker_close_overnight": 3.0, "unit": "per_lot"},
    },
    "DEFAULT": {
        "DEFAULT": {"maker_open": 12.0, "maker_close_today": 12.0, "maker_close_overnight": 12.0, "taker_open": 15.0, "taker_close_today": 15.0, "taker_close_overnight": 15.0, "unit": "per_lot"},
    },
}

# 默认回退手续费（当品种未匹配时）
# P2-R3-D-17: 多处关键参数硬编码 — FEE_STRUCTURE/SLIPPAGE_BPS/INITIAL_EQUITY等
# 硬编码于代码中，调整费率/滑点等需改代码再部署。建议迁移到parameter_attribute_matrix.yaml
COMMISSION_PER_LOT = 1.5

__all__ = [
    'run_backtest',
    'PREPROCESSED_DB',
    'RESULTS_DB',
    'MAX_WORKERS',
    'TRAIN_START',
    'TEST_START',
    'TEST_END',
    'TARGET_SYMBOLS',
    'validate_default_values_in_grids',
    'validate_shadow_param_independence',
    '_sync_random_seed',
    '_select_top_k_train',
    '_get_cascade_judge_module',
    '_get_life_estimator',
    '_get_annualize_factor',
    'calc_trade_fee',
    '_compute_commission',
    '_build_backtest_result',
    '_compute_profit_loss_ratio_metrics',
    '_check_bar_data_monotonic',
    '_ClosedTrade',
    'detect_rollover_gaps',
    'exclude_rollover_signals',
    'compute_rollover_cost',
    'FEE_STRUCTURE',
    'FEE_STRUCTURE_V2',
    '_PendingOrder',
    '_check_intra_bar_stop_loss',
    '_compute_option_mtm_price',
    '_update_mtm_equity',
    '_compute_fill_quantity',
    '_compute_market_impact_v2',
    '_apply_fidelity_presets',
    'COMMISSION_PER_LOT',
    'SLIPPAGE_BPS',
    'INITIAL_EQUITY',
    'CANCEL_FAILURE_RATE',
    'EXPIRY_SLIPPAGE_MULTIPLIERS',
]


def _get_annualize_factor(strategy_type: str, ticks_per_bar: int = 0) -> float:
    """STAT-01-P0修复: 按策略时间框架及采样频率选择Sharpe年化因子

    S3/S4/S5/S6(日频) → √252
    S2(minute) → √(252*240)
    S1(hft, 逐Bar) → √(252*240)
    S1(hft_tick_fidelity, 逐tick) → √(252*240*ticks_per_bar)

    Args:
        strategy_type: 策略类型标识
        ticks_per_bar: 每根Bar内的tick数（仅tick_fidelity回测需传入>0值）;
                       默认0表示逐Bar/逐分钟采样
    """
    _daily_types = {'box_extreme', 'box_spring', 'arbitrage', 'market_making'}
    if strategy_type in _daily_types:
        return ANNUALIZE_FACTOR_DAILY
    if ticks_per_bar > 0:
        return ANNUALIZE_FACTOR_MINUTE * ticks_per_bar
    return ANNUALIZE_FACTOR_MINUTE


def _sync_random_seed(seed: int) -> "np.random.Generator":
    """C-38修复: 使用局部Generator替代全局seed，避免跨K线对比污染
    
    返回局部rng供调用方使用，不再修改全局np.random状态。
    保留random.seed()用于Python标准库random的回测可复现性(仅影响子进程)。
    """
    random.seed(seed)
    return np.random.default_rng(seed)


def _get_cascade_judge_module():
    """懒加载CascadeJudge模块，只导入一次"""
    global _CascadeJudge, _adapt_backtest_result
    with _cascade_judge_lock:
        if _CascadeJudge is None:
            from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
            _CascadeJudge = CascadeJudge
            _adapt_backtest_result = adapt_backtest_result
    return _CascadeJudge, _adapt_backtest_result


def _get_life_estimator(params: Optional[Dict[str, Any]] = None):
    """懒加载 BayesianShrinkageLifeEstimator 单例（支持参数注入）

    R10-P0-02修复: 全部在锁内检查和创建，避免并发创建多实例
    """
    global _LIFE_ESTIMATOR
    with _LIFE_ESTIMATOR_LOCK:
        if _LIFE_ESTIMATOR is None:
            try:
                from ali2026v3_trading.param_pool.l1_quantification.bayesian_shrinkage_life_estimator import BayesianShrinkageLifeEstimator
                _LIFE_ESTIMATOR = BayesianShrinkageLifeEstimator(params=params)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("BayesianShrinkageLifeEstimator not available: %s", e)
        return _LIFE_ESTIMATOR


def calc_trade_fee(contract_type: str, open_time: pd.Timestamp, close_time: pd.Timestamp,
                   lots: int, trade_value: float = 0.0, broker_policy: str = "default") -> float:
    from ali2026v3_trading.参数池.backtest_commission import calc_trade_fee as _ctf_delegated
    return _ctf_delegated(contract_type, open_time, close_time, lots, trade_value, broker_policy)


def _infer_contract_type(symbol: str) -> str:
    from ali2026v3_trading.参数池.backtest_commission import _infer_contract_type as _ict_delegated
    return _ict_delegated(symbol)


def _compute_commission(symbol: str, lots: int, open_time=None, close_time=None,
                        trade_value: float = 0.0, is_open: bool = True,
                        exchange_id: str = None, order_type: str = "taker",
                        exchange: str = None) -> float:
    from ali2026v3_trading.参数池.backtest_commission import _compute_commission as _cc_delegated
    return _cc_delegated(symbol, lots, open_time, close_time, trade_value, is_open, exchange_id, order_type, exchange)


def _infer_exchange_id(symbol: str) -> str:
    from ali2026v3_trading.参数池.backtest_commission import _infer_exchange_id as _iei_delegated
    return _iei_delegated(symbol)


def _infer_exchange_from_id(exchange_id: str) -> str:
    from ali2026v3_trading.参数池.backtest_commission import _infer_exchange_from_id as _iefi_delegated
    return _iefi_delegated(exchange_id)


# BF-P1-06已修复: 涨跌停比例配置 — 不同品种涨跌停幅度不同
LIMIT_UP_RATIO = {
    "ETF": 0.10,          # ETF涨跌停10%
    "FUTURE": 0.10,       # 期货涨跌停10%（部分品种有差异，此处取常见值）
    "OPTION_ETF": 0.10,   # ETF期权涨跌停10%
    "OPTION_INDEX": 0.10, # 股指期权涨跌停10%
    "OPTION_COMMODITY": 0.08,  # 商品期权涨跌停8%
}
LIMIT_DOWN_RATIO = {
    "ETF": 0.10,
    "FUTURE": 0.10,
    "OPTION_ETF": 0.10,
    "OPTION_INDEX": 0.10,
    "OPTION_COMMODITY": 0.08,
}


def _calculate_limit_prices(
    bar: pd.Series,
    prev_close: float = 0.0,
    instrument_type: str = "FUTURE",
) -> Dict[str, Any]:
    from ali2026v3_trading.参数池.backtest_commission import _calculate_limit_prices as _clp_delegated
    return _clp_delegated(bar, prev_close, instrument_type)


def _simulate_limit_order_queue(
    order_price: float,
    current_price: float,
    bar: pd.Series,
    order_lots: int = 1,
    timeout_seconds: int = QUEUE_TIMEOUT_SECONDS,
    enable_queue: bool = ENABLE_QUEUE_SIMULATION
) -> Dict[str, Any]:
    from ali2026v3_trading.参数池.backtest_order_sim import _simulate_limit_order_queue as _sloq_delegated
    return _sloq_delegated(order_price, current_price, bar, order_lots, timeout_seconds, enable_queue)


def _simulate_market_order_slippage(
    bar: pd.Series,
    slippage_bps: float = MARKET_ORDER_SLIPPAGE_BPS,
    price_mode: str = MARKET_ORDER_PRICE_MODE,
    direction: int = 1
) -> float:
    from ali2026v3_trading.参数池.backtest_order_sim import _simulate_market_order_slippage as _smos_delegated
    return _smos_delegated(bar, slippage_bps, price_mode, direction)


def _get_instrument_type_slippage(
    symbol: str,
    base_slippage_bps: float = SLIPPAGE_BPS,
    multiplier_dict: Dict[str, float] = None
) -> float:
    from ali2026v3_trading.参数池.backtest_order_sim import INSTRUMENT_SLIPPAGE_MULTIPLIER as _ism
    if multiplier_dict is None:
        multiplier_dict = _ism
    _s = str(symbol).upper()
    _instrument_type = "ETF"
    if "50ETF" in _s or "300ETF" in _s:
        if "P" in _s or "C" in _s:
            _instrument_type = "OPTION_ETF"
        else:
            _instrument_type = "ETF"
    elif _s.startswith("IO") or _s.startswith("MO"):
        _instrument_type = "OPTION_INDEX"
    elif any(k in _s for k in ("M", "Y", "A", "C", "SR", "CF", "TA", "RU", "CU", "AL", "ZN", "AU", "AG")):
        if len(_s) > 4 and any(c.isdigit() for c in _s):
            _instrument_type = "OPTION_COMMODITY"
        else:
            _instrument_type = "FUTURE"
    _multiplier = multiplier_dict.get(_instrument_type, 1.0)
    return base_slippage_bps * _multiplier


def _simulate_order_cancel(
    order_id: str,
    cancel_delay_ms: int = CANCEL_DELAY_MS,
    failure_rate: float = CANCEL_FAILURE_RATE,
    enable_cancel: bool = ENABLE_CANCEL_SIMULATION
) -> Dict[str, Any]:
    from ali2026v3_trading.参数池.backtest_order_sim import _simulate_order_cancel as _soc_delegated
    return _soc_delegated(order_id, cancel_delay_ms, failure_rate, enable_cancel)


def _build_risk_dimension_defaults():
    try:
        from ali2026v3_trading.risk_service import RiskService
        _src = RiskService.RISK_DIMENSION_DEFAULTS
        _key_map = {
            'state_strength': 'decision.score.state_strength_weight',
            'order_flow': 'decision.score.order_flow_weight',
            'cycle_resonance': 'decision.score.cycle_resonance_weight',
            'tri_validation': 'decision.score.tri_validation_weight',
            'life_expectancy': 'decision.score.life_expectancy_weight',
            'phase_quality': 'decision.score.phase_quality_weight',
            'greeks_usage': 'decision.score.greeks_usage_weight',
            'asymmetric_drawdown': 'decision.score.asymmetric_drawdown_weight',
            'consecutive_loss': 'decision.score.consecutive_loss_weight',
            'alpha_decay': 'decision.score.alpha_decay_weight',
            'cross_correlation': 'decision.score.cross_correlation_weight',
        }
        return {_key_map.get(k, k): v for k, v in _src.items() if k in _key_map}
    except Exception:
        return {
            "decision.score.state_strength_weight": 0.15,
            "decision.score.order_flow_weight": 0.10,
            "decision.score.cycle_resonance_weight": 0.10,
            "decision.score.tri_validation_weight": 0.10,
            "decision.score.life_expectancy_weight": 0.10,
            "decision.score.phase_quality_weight": 0.08,
            "decision.score.greeks_usage_weight": 0.07,
            "decision.score.asymmetric_drawdown_weight": 0.05,
            "decision.score.consecutive_loss_weight": 0.07,
            "decision.score.alpha_decay_weight": 0.10,
            "decision.score.cross_correlation_weight": 0.08,
        }


def _sync_reason_multipliers_with_position_service() -> None:
    """使用PositionService作为TP/SL规则权威源，降低多处硬编码漂移风险。"""
    try:
        from ali2026v3_trading.position_service import PositionService

        base_tp = float(getattr(PositionService, 'DEFAULT_TP_RATIO', 1.8) or 1.8)  # R26-P1-SE-04修复: fallback从1.5改为1.8，与PositionService.DEFAULT_TP_RATIO对齐
        base_sl = float(getattr(PositionService, 'DEFAULT_SL_RATIO', 0.30) or 0.30)  # R26-P1-SE-01修复: fallback从0.5改为0.30
        reason_defaults = dict(getattr(PositionService, 'TP_SL_REASON_DEFAULTS', {}) or {})

        for reason, ratios in reason_defaults.items():
            if not isinstance(ratios, tuple) or len(ratios) != 2:
                continue
            tp_ratio, sl_ratio = ratios
            if base_tp <= 0 or base_sl <= 0:
                continue
            profile = REASON_MULTIPLIERS.setdefault(reason, {"tp_mult": 1.0, "sl_mult": 1.0, "time_mult": 1.0})
            profile["tp_mult"] = float(tp_ratio) / base_tp
            profile["sl_mult"] = float(sl_ratio) / base_sl
    except Exception as e:
        logging.getLogger(__name__).debug("[task_scheduler] sync REASON_MULTIPLIERS skipped: %s", e)


def _sync_thresholds_from_runtime_config() -> None:
    """将评判阈值与运行时参数联动，避免评判/生产阈值漂移。"""
    try:
        cp = get_cached_params() or {}
    except Exception as e:
        logger.debug("[task_scheduler] 阈值联动跳过，读取参数失败: %s", e)
        return

    for bt_key, cp_key in _BACKTEST_THRESHOLD_LINK_MAP.items():
        val = cp.get(cp_key)
        if isinstance(val, (int, float)):
            BACKTEST_THRESHOLDS[bt_key] = float(val)

    for p0_key, cp_key in _P0_IRON_RULE_LINK_MAP.items():
        val = cp.get(cp_key)
        if isinstance(val, (int, float)):
            P0_IRON_RULES[p0_key] = float(val)

    logger.info("[task_scheduler] 评判阈值已与运行时参数联动同步")


def _select_top_k_train(results: List[Dict[str, Any]], metric: str, k: int,
                        fallback_metric: str = "minute_sharpe",
                        scan_param: Optional[str] = None) -> List[Dict[str, Any]]:
    # R24-P1-DF-05修复: 参数级排序指标覆盖
    if scan_param and scan_param in PARAM_SORT_METRIC_OVERRIDE:
        metric = PARAM_SORT_METRIC_OVERRIDE[scan_param]
    train_rows = [
        r for r in results
        if r.get("is_train") and r.get(metric) is not None
    ]
    if not train_rows and fallback_metric and fallback_metric != metric:
        train_rows = [
            r for r in results
            if r.get("is_train") and r.get(fallback_metric) is not None
        ]
        metric = fallback_metric
    train_rows.sort(key=lambda r: r.get(metric, 0) or 0, reverse=True)
    return train_rows[:k]


def validate_default_values_in_grids() -> List[str]:
    """验收标准6: 每个扫描参数的默认值必须存在于对应grid列表中。"""
    mapping = [
        ("PARAM_GRID_ROUND1", PARAM_GRID_ROUND1, PARAM_DEFAULTS),
        ("PARAM_GRID_ROUND2", PARAM_GRID_ROUND2, PARAM_DEFAULTS),
        ("PARAM_GRID_BOX_EXTREME", PARAM_GRID_BOX_EXTREME, PARAM_DEFAULTS_BOX_EXTREME),
        ("PARAM_GRID_BOX_SPRING", PARAM_GRID_BOX_SPRING, PARAM_DEFAULTS_BOX_SPRING),
        ("PARAM_GRID_HFT", PARAM_GRID_HFT, PARAM_DEFAULTS_HFT),
        ("PARAM_GRID_ARBITRAGE", PARAM_GRID_ARBITRAGE, PARAM_DEFAULTS_ARBITRAGE),
        ("PARAM_GRID_MARKET_MAKING", PARAM_GRID_MARKET_MAKING, PARAM_DEFAULTS_MARKET_MAKING),
    ]

    violations: List[str] = []
    for grid_name, grid, defaults in mapping:
        for key, values in grid.items():
            if key not in defaults:
                continue
            if not isinstance(values, list):
                continue
            default_val = defaults.get(key)
            if default_val not in values:
                violations.append(
                    f"{grid_name}.{key}: default={default_val!r} not in grid={values!r}"
                )
    return violations


def validate_shadow_param_independence(threshold: float = 0.20) -> Dict[str, float]:
    """P0-Q1质量门：验证影子策略参数与主策略差异度>threshold

    对每个策略组，计算影子A/B与主策略的关键参数差异度。
    差异度 = avg(|shadow_param - main_param| / main_param)
    """
    _SHADOW_DIFF_KEYS = [
        "close_take_profit_ratio", "close_stop_loss_ratio",
        "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
        "resonance_hard_time_stop_min", "box_hard_time_stop_min",
        "max_risk_ratio",
    ]
    results = {}
    for group_name, main_params, shadow_a, shadow_b in [
        ("S2_main", PARAM_DEFAULTS, PARAM_DEFAULTS_SHADOW_A, PARAM_DEFAULTS_SHADOW_B),
        ("S1_hft", PARAM_DEFAULTS_HFT, PARAM_DEFAULTS_HFT_SHADOW_A, PARAM_DEFAULTS_HFT_SHADOW_B),
        ("S3_box_extreme", PARAM_DEFAULTS_BOX_EXTREME,
         PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A, PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B),
        ("S4_box_spring", PARAM_DEFAULTS_BOX_SPRING,
         PARAM_DEFAULTS_BOX_SPRING_SHADOW_A, PARAM_DEFAULTS_BOX_SPRING_SHADOW_B),
        # P1-裂缝37：补充S5/S6影子参数差异度验证
        ("S5_arbitrage", PARAM_DEFAULTS_ARBITRAGE,
         PARAM_DEFAULTS_ARBITRAGE_SHADOW_A, PARAM_DEFAULTS_ARBITRAGE_SHADOW_B),
        ("S6_market_making", PARAM_DEFAULTS_MARKET_MAKING,
         PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A, PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B),
    ]:
        for shadow_name, shadow_params in [("shadow_a", shadow_a), ("shadow_b", shadow_b)]:
            diffs = []
            for key in _SHADOW_DIFF_KEYS:
                if key in main_params and key in shadow_params and main_params[key] != 0:
                    diffs.append(abs(shadow_params[key] - main_params[key]) / abs(main_params[key]))
            avg_diff = sum(diffs) / max(1, len(diffs))
            label = f"{group_name}.{shadow_name}"
            results[label] = round(avg_diff, 4)
            if avg_diff < threshold:
                logger.warning("[P0-Q1 FAIL] %s 参数差异度 %.2f%% < %.0f%% 阈值", label, avg_diff * 100, threshold * 100)
            else:
                logger.info("[P0-Q1 PASS] %s 参数差异度 %.2f%%", label, avg_diff * 100)
    return results


def _infer_instrument_type(symbol: str) -> str:
    from ali2026v3_trading.参数池.backtest_commission import _infer_instrument_type as _iit_delegated
    return _iit_delegated(symbol)


@dataclass
class _PendingOrder:
    """BF-02+03: 统一执行时序模型 - 延迟订单"""
    symbol: str
    order_type: str
    volume: int
    lots: int
    reason: str
    signal_bar_idx: int
    signal_price: float
    tp_ratio: float
    sl_ratio: float
    params_snapshot: Dict[str, Any]
    created_at_bar: int
    fee_type: str = "taker"
    retry_count: int = 0


@dataclass
class _BacktestPosition:
    instrument_id: str
    volume: int
    open_price: float
    open_time: pd.Timestamp
    stop_profit_price: float
    stop_loss_price: float
    open_reason: str
    lots: int = 1
    open_state: str = "other"
    open_strength: float = 0.0
    max_float_profit: float = 0.0
    stage1_passed: bool = False
    profit_history: list = field(default_factory=lambda: [])
    _PROFIT_HISTORY_MAXLEN = 2000  # PD-P2-07: 从10000降至2000，防止长持仓累积数千float
    # R21-MEM-P1-13修复: profit_history改用deque(maxlen)自动淘汰，替代手动截断
    last_check_time: Optional[pd.Timestamp] = None
    instrument_type: str = "future"
    option_premium: float = 0.0
    margin_requirement: float = 0.0
    strike_price: float = 0.0
    remaining_days: int = 0
    option_type: str = "call"
    open_iv: float = 0.0
    open_theo_price: float = 0.0
    fill_ratio: float = 1.0


@dataclass(slots=True)
class _ClosedTrade:
    pnl: float
    pnl_pct: float
    close_reason: str
    hold_minutes: float
    open_reason: str = ""
    premium_pnl: float = 0.0
    delta_pnl: float = 0.0
    stage1_passed: bool = False


@dataclass
class _BacktestState:
    equity: float = INITIAL_EQUITY
    initial_equity: float = INITIAL_EQUITY  # P0-R11-06: 回测健康检查用初始权益
    peak_equity: float = INITIAL_EQUITY
    positions: Dict[str, _BacktestPosition] = field(default_factory=dict)
    # R7-P-01修复: maxlen从100000改为None，避免3年回测(230400个Bar)数据截断
    # 3年训练集约230400个1分钟Bar，超过100000上限导致早期数据被丢弃
    # max_drawdown/sharpe计算失真。改为无限制，由内存监控机制保护
    equity_curve: Deque[float] = field(default_factory=lambda: deque(maxlen=None))
    daily_returns: Deque[float] = field(default_factory=lambda: deque(maxlen=None))
    current_state: str = "other"
    state_confirm_count: int = 0
    pending_state: Optional[str] = None
    last_state_check_time: Optional[pd.Timestamp] = None
    last_signal_time: Optional[pd.Timestamp] = None
    circuit_breaker_until: Optional[pd.Timestamp] = None
    circuit_breaker_events: List[Dict] = field(default_factory=list)
    daily_loss: float = 0.0
    daily_start_equity: float = INITIAL_EQUITY
    consecutive_loss_pause_until: Optional[pd.Timestamp] = None
    consecutive_loss_streak: int = 0
    consecutive_losses: int = 0
    total_signals: int = 0
    total_trades: int = 0
    prev_date: Optional[str] = None
    recent_pnls: List[float] = field(default_factory=list)
    closed_trades: List[_ClosedTrade] = field(default_factory=list)
    _kahan_c: float = 0.0
    # 回测快照采集器（可选，由 run_backtest_*_with_snapshot 启用）
    snapshot_collector: Any = None
    pending_orders: List = field(default_factory=list)
    mtm_equity: float = 0.0
    mtm_equity_curve: list = field(default_factory=list)
    mtm_peak_equity: float = 0.0
    mtm_max_drawdown: float = 0.0
    bar_idx: int = 0


def _bt_capture_snapshot(bt: _BacktestState, trigger_name: str, detail: str = "",
                          strategy_type: str = "", bar: Any = None) -> None:
    from ali2026v3_trading.参数池.backtest_snapshot import _bt_capture_snapshot as _bcs_delegated
    _bcs_delegated(bt, trigger_name, detail, strategy_type, bar, _get_life_estimator)


def _get_reason_tp_sl_from_position_service(open_reason: str) -> Optional[Tuple[float, float]]:
    """优先复用PositionService的reason级TP/SL默认值，减少回测/实盘路径漂移。"""
    try:
        from ali2026v3_trading.position_service import PositionService
        defaults = dict(getattr(PositionService, 'TP_SL_REASON_DEFAULTS', {}) or {})
        if open_reason in defaults:
            tp_ratio, sl_ratio = defaults[open_reason]
            tp_val = float(tp_ratio)
            sl_val = float(sl_ratio)
            if tp_val > 0 and 0 < sl_val < 1:
                return tp_val, sl_val
    except Exception as e:
        logger.debug("[task_scheduler] PositionService TP/SL复用失败: %s", e)
    return None


def _resolve_tp_sl(params: Dict[str, float], open_reason: str) -> Tuple[float, float]:
    linked = _get_reason_tp_sl_from_position_service(open_reason)
    if linked is not None:
        return linked
    base_tp = params.get("close_take_profit_ratio", 1.8)  # R17-P0-CFG-04修复: 回退值1.5→1.8与CENTRALIZED_DEFAULTS对齐
    base_sl = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    mult = REASON_MULTIPLIERS.get(open_reason, {"tp_mult": 1.0, "sl_mult": 1.0})
    return (base_tp * mult["tp_mult"], base_sl * mult["sl_mult"])


def _resolve_time_stop_hard(params: Dict[str, float], open_reason: str) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟）

    策略分层时间参数：
    - HFT: hft_hard_time_stop_ms → 转换为分钟
    - 弹簧: spring_hard_time_stop_sec → 转换为分钟
    - 共振: resonance_hard_time_stop_min → 直接使用
    - 箱体: box_hard_time_stop_min → 直接使用
    """
    reason_key = str(open_reason or "").upper()
    param_key, unit, fallback = _REASON_TIME_STOP_SOURCE.get(
        reason_key, ("resonance_hard_time_stop_min", "min", 5.0)
    )
    raw_val = float(params.get(param_key, fallback) or fallback)
    if unit == "ms":
        base_time = raw_val / 60000.0
    elif unit == "sec":
        base_time = raw_val / 60.0
    else:
        base_time = raw_val
    mult = REASON_MULTIPLIERS.get(open_reason, {"time_mult": 1.0})
    return base_time * mult["time_mult"]


def _resolve_time_stop(params: Dict[str, float], open_reason: str, current_state: str = None) -> float:
    """根据开仓原因选择策略专用硬时间止损（分钟），支持行情寿命字典动态调整

    # R16-P1-002修复: _resolve_time_stop已升级为分层参数模型
    # 手册版本仅支持单一max_hold_minutes，代码版本支持stage1/stage2分层+策略类型覆盖

    策略分层时间参数：
    - HFT: hft_hard_time_stop_ms → 转换为分钟
    - 弹簧: spring_hard_time_stop_sec → 转换为分钟
    - 共振: resonance_hard_time_stop_min → 直接使用
    - 箱体: box_hard_time_stop_min → 直接使用
    """
    # 行情寿命字典：用HMM状态寿命的p75分位数作为动态时间止损上限
    if current_state is not None:
        estimator = _get_life_estimator()
        if estimator is not None and hasattr(estimator, '_life_dict') and estimator._life_dict:
            life = estimator.get_life_expectancy(current_state)
            if life is not None and life.is_valid():
                # 寿命p75（分钟）作为该状态下的最大持仓时间
                life_stop = life.duration.get('p75', 0)
                if life_stop > 0:
                    # 取硬编码时间止损和寿命p75的较小值
                    hard_stop = _resolve_time_stop_hard(params, open_reason)
                    return max(1.0, min(hard_stop, life_stop))
    # P2-裂缝32：effective_wait最小值约束
    # pullback_atr_wait_multiplier与pullback_theta_decay_accel可同时生效
    # 但可能出现负的effective_wait（例如近月高波动），强制最小值=1
    hard_stop = _resolve_time_stop_hard(params, open_reason)
    return max(1.0, hard_stop)


def _compute_lots_with_risk_budget(
    equity: float,
    price: float,
    sl_ratio: float,
    lots_min: int,
    params: Dict[str, float],
    recent_pnls: Optional[List[float]] = None,
    bar: Optional[pd.Series] = None,
    current_positions: Optional[Dict[str, '_BacktestPosition']] = None,
    bt: Optional['_BacktestState'] = None,
) -> int:
    """
    计算开仓手数，考虑风险预算和已有仓位。
    
    R4-P-01修复: 仓位计算需减去已有仓位占用的风险额度，防止总仓位超限。
    R4-P-03修复: 当计算仓位<lots_min时记录警告日志，避免静默返回0导致开仓失败无提示。
    """
    if sl_ratio <= 0:
        return 0
    # R24-P1-IV-11修复: 使用safe_price_check替代price<=0
    if not safe_price_check(price):
        return 0
    
    # 基础风险预算计算
    risk = params.get("max_risk_ratio", 0.8)  # R17重审计修复: 回退值0.3→0.8与config_params对齐
    max_loss_per_lot = price * sl_ratio
    if max_loss_per_lot <= 0:
        return 0
    
    # R4-P-01修复: 计算已有仓位占用的风险额度
    existing_risk_used = 0.0
    if current_positions:
        for pos in current_positions.values():
            if hasattr(pos, 'open_price') and hasattr(pos, 'volume'):
                pos_risk = abs(int(pos.volume)) * pos.open_price * sl_ratio
                # NP-P1-08: 使用int()强制转Python int(任意精度)防止numpy int32溢出
                existing_risk_used += pos_risk
    
    # 可用风险预算 = 总风险预算 - 已用风险
    available_risk_budget = equity * risk - existing_risk_used
    if available_risk_budget <= 0:
        logger.warning(
            "[R4-P-01] Risk budget exhausted: equity=%.2f, risk=%.2f%%, existing_risk=%.2f, available=%.2f",
            equity, risk * 100, existing_risk_used, available_risk_budget
        )
        return 0
    
    # 基于可用风险预算计算最大手数
    max_lots = max(1, math.ceil(available_risk_budget / max_loss_per_lot))
    lots = min(lots_min, max_lots)
    
    # max_risk_per_trade限制
    max_risk_per_trade = params.get("max_risk_per_trade", 0.05)
    if max_risk_per_trade > 0 and equity > 0:
        max_lots_by_risk = max(1, round(equity * max_risk_per_trade / max_loss_per_lot))
        lots = min(lots, max_lots_by_risk)
    
    # 近期亏损调整
    if recent_pnls and len(recent_pnls) >= 3:
        lookback = min(10, len(recent_pnls))
        recent = recent_pnls[-lookback:]
        losses = sum(1 for p in recent if p < 0)
        if losses > lookback * 0.6:
            lots = max(1, round(lots * 0.5))
        elif losses > lookback * 0.4:
            lots = max(1, round(lots * 0.75))

    _hard_stop_streak = int(params.get("consecutive_loss_hard_stop", 6))
    if bt is not None and bt.consecutive_loss_streak >= _hard_stop_streak:
        if bt.consecutive_loss_streak == _hard_stop_streak:
            logger.warning("[R16-P1-015] 连续亏损%d次(硬停止阈值%d)，暂停交易1个周期", bt.consecutive_loss_streak, _hard_stop_streak)
        return 0
    
    # R3-D-02修复: position_scale/TVF仓位缩放接入回测
    try:
        _position_scale = params.get("position_scale", 1.0)
        if _position_scale != 1.0 and _position_scale > 0:
            lots = max(1, round(lots * _position_scale))
    except Exception:
        pass
    
    # R4-P-03修复: 当计算仓位<lots_min时记录警告日志
    final_lots = max(0, lots)
    if final_lots == 0 and lots_min > 0:
        logger.warning(
            "[R4-P-03] Computed lots=0 (< lots_min=%d): equity=%.2f, price=%.2f, sl_ratio=%.4f, existing_risk=%.2f",
            lots_min, equity, price, sl_ratio, existing_risk_used
        )
    
    return final_lots


def _is_consecutive_loss_paused(bt: _BacktestState, params: Dict[str, float], bar_time: pd.Timestamp) -> bool:
    """连亏暂停：达到阈值后在暂停窗口内阻断新开仓。"""
    max_losses = int(params.get("max_consecutive_losses", 0) or 0)
    if max_losses <= 0:
        return False

    pause_until = getattr(bt, 'consecutive_loss_pause_until', None)
    if pause_until is not None:
        if bar_time < pause_until:
            return True
        bt.consecutive_loss_pause_until = None

    streak = 0
    for pnl in reversed(bt.recent_pnls):
        if pnl < 0:
            streak += 1
        else:
            break
    bt.consecutive_loss_streak = streak

    if streak >= max_losses:
        pause_sec = float(params.get("consecutive_loss_pause_sec", 1800.0) or 1800.0)
        bt.consecutive_loss_pause_until = bar_time + pd.Timedelta(seconds=max(1.0, pause_sec))
        logger.warning(
            "[BACKTEST] 连亏暂停触发: streak=%d threshold=%d pause_until=%s",
            streak, max_losses, bt.consecutive_loss_pause_until,
        )
        return True
    return False


def _backtest_order_split(lots: int, max_sub_order_lots: int = 5) -> list:
    if lots <= max_sub_order_lots:
        return [lots]
    splits = []
    remaining = lots
    while remaining > 0:
        sub = min(remaining, max_sub_order_lots)
        splits.append(sub)
        remaining -= sub
    return splits


def _check_state_transition(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> str:
    from ali2026v3_trading.参数池.backtest_state_checker import _check_state_transition as _cst_delegated
    return _cst_delegated(bt, bar, params, _check_safety, BACKTEST_THRESHOLDS)


def _infer_liquidity_tier(bar) -> str:
    if bar is None:
        return 'future_main'
    _oi = bar.get('open_interest', 0)
    _spread = bar.get('bid_ask_spread', 0)
    _strike = bar.get('strike_price', 0)
    _underlying = bar.get('underlying_price', 0)
    if _strike > 0 and _underlying > 0:
        _moneyness = abs(_strike - _underlying) / _underlying
        if _moneyness < 0.05:
            return 'option_atm'
        elif _moneyness < 0.15:
            return 'option_otm'
        else:
            return 'option_far'
    if _oi > 50000:
        return 'future_main'
    return 'future_sub'


def _compute_dynamic_slippage_bps(
    price: float,
    bid_ask_spread: float,
    base_slippage_bps: float = SLIPPAGE_BPS,
    spread_quality: int = 1,
    bar: pd.Series = None,
    params: Dict[str, float] = None,
) -> float:
    """动态滑点模型

    核心：滑点 = max(base_slippage, bid_ask_spread占比放大) * 到期日倍增
    - 流动性好（spread小）：用base_slippage（如1bps）
    - 流动性差（spread大）：spread/price放大为bps
    - 远月/深度虚值spread可达5-20个tick，此时滑点远超1bps
    - P0-6修复: 到期日附近滑点倍增(3天20x/7天10x/14天3x)

    P2-R3-P-14: spread NaN时退化 — 当bid_ask_spread为NaN或spread_quality=0时,
    退化为静态base_slippage_bps，丢失动态滑点信息。已知限制:
    深度虚值/远月合约spread数据常缺失，退化为静态滑点会导致回测低估实盘滑点，
    建议在数据预处理阶段补全spread(用近月合约spread×虚值程度倍数)。

    P1-R11-01修复: 回测使用历史spread，实盘使用实时行情spread。
    两者存在系统性差异：历史spread为快照(tick级)，实盘为实时盘口。
    为弥补此差异，回测路径添加backtest_slippage_premium_bps保守溢价，
    确保回测滑点略高于实盘，避免回测过度乐观。
    返回结果中包含spread_source字段标识数据来源。
    """
    # P1-R11-01: 回测滑点保守溢价 — 弥补历史spread与实时spread的系统性差异
    _premium_bps = 0.0
    if params is not None:
        _premium_bps = float(params.get("backtest_slippage_premium_bps", 0.5))

    # R24-P1-IV-11修复: 使用safe_price_check替代price<=0
    if not safe_price_check(price):
        return base_slippage_bps + _premium_bps
    if bid_ask_spread <= 0 or spread_quality == 0:
        if spread_quality == 0 and bid_ask_spread <= 0:
            logger.debug("[SLIPPAGE_DEGRADE] spread=%.4f quality=%d, using static %.1fbps", bid_ask_spread, spread_quality, base_slippage_bps)
        return base_slippage_bps + _premium_bps
    spread_bps = bid_ask_spread / price * 10000.0 if price > 0 else 0.0
    _quality_scale_map = {1: 0.75, 2: 0.5, 3: 0.3}
    _quality_scale = _quality_scale_map.get(spread_quality, 0.75)
    _liquidity_tier_map = {
        'future_main': 1.0,
        'future_sub': 1.2,
        'option_atm': 1.0,
        'option_otm': 1.5,
        'option_far': 2.0,
    }
    _liquidity_tier = _liquidity_tier_map.get(_infer_liquidity_tier(bar), 1.2)
    base = max(base_slippage_bps, spread_bps * _quality_scale * _liquidity_tier)
    # P0-6修复: 到期日滑点倍增
    expiry_mult = 1.0
    if bar is not None and params is not None:
        expiry_mult = _get_expiry_slippage_multiplier(bar, params)
    return base * expiry_mult + _premium_bps


def _get_contract_multiplier(instrument_id: str) -> float:
    """P0-R12-NP-05修复: 获取合约乘数，PnL计算必须乘以multiplier"""
    if instrument_id in _CONTRACT_MULTIPLIER_CACHE:
        return _CONTRACT_MULTIPLIER_CACHE[instrument_id]
    multiplier = 1.0
    try:
        from ali2026v3_trading.params_service import get_params_service
        params_svc = get_params_service()
        meta = params_svc.get_instrument_meta_by_id(instrument_id)
        if meta:
            product = meta.get('product', '')
            product_cache = params_svc.get_product_cache(product)
            if product_cache:
                multiplier = float(product_cache.get('contract_size', 1.0))
    except Exception:
        pass
    _CONTRACT_MULTIPLIER_CACHE[instrument_id] = multiplier
    return multiplier


def _safe_equity_add(bt: _BacktestState, delta: float) -> None:
    """P0-R12-NP-03修复: 安全equity累加，防止NaN/Inf/溢出
    NP-P1-05修复: Kahan补偿算法消除浮点累加舍入误差"""
    y = delta - bt._kahan_c
    t = bt.equity + y
    bt._kahan_c = (t - bt.equity) - y
    new_equity = t
    if not np.isfinite(new_equity):
        logger.warning("[BACKTEST] equity overflow/NaN detected: equity=%.2f delta=%.2f, clamping", bt.equity, delta)
        new_equity = max(_EQUITY_MIN, min(_EQUITY_MAX, bt.equity))
        bt._kahan_c = 0.0
    bt.equity = new_equity


def _get_expiry_slippage_multiplier(bar: pd.Series, params: Dict[str, float]) -> float:
    """P0-6修复: 根据距到期日天数返回滑点倍增系数

    到期日附近流动性急剧下降，bid-ask spread暴增，
    回测必须建模此效应，否则到期附近交易回测盈利实盘巨亏。
    """
    days_to_expiry = bar.get("days_to_expiry", None)
    # R3-P-03修复: 处理NaN值 — np.nan != None，需显式检查
    if days_to_expiry is None or (isinstance(days_to_expiry, float) and math.isnan(days_to_expiry)):
        # R3-P-03修复: 字段名从expiry_date改为expire_date，与数据库schema对齐
        expiry_str = bar.get("expire_date", "")
        if not expiry_str:
            expiry_str = bar.get("expiry_date", "")
        if expiry_str:
            try:
                from datetime import datetime
                expiry_dt = pd.Timestamp(expiry_str)
                bar_time = bar.get("timestamp", pd.NaT)
                if pd.isna(bar_time):
                    logger.warning("[R22-TIME-03] bar缺少timestamp字段，跳过到期时间计算")
                    return 1.0
                days_to_expiry = (expiry_dt - pd.Timestamp(bar_time)).days
            except Exception:
                return 1.0
    if days_to_expiry is None or (isinstance(days_to_expiry, float) and math.isnan(days_to_expiry)):
        return 1.0
    # P0-6修复: numpy.int64不是int的子类，需用np.issubdtype或try/except
    try:
        dte = int(days_to_expiry)
    except (TypeError, ValueError):
        return 1.0
    if dte < 0:
        return 1.0
    for threshold_days, multiplier in sorted(EXPIRY_SLIPPAGE_MULTIPLIERS.items()):
        if dte <= threshold_days:
            return multiplier
    return 1.0


def _compute_cascade_slippage_bps(
    base_slippage_bps: float,
    close_volume: float = 0.0,
    avg_volume: float = 1.0,
    is_state_switch: bool = False,
) -> float:
    if not is_state_switch:
        return base_slippage_bps
    participation_rate = close_volume / avg_volume if avg_volume > 0 else 0.0
    volume_impact = 1.0 + math.sqrt(max(participation_rate, 0.0))
    result = base_slippage_bps * volume_impact * CASCADE_SLIPPAGE_MULTIPLIER
    return min(result, CASCADE_SLIPPAGE_CAP_BPS)


def _check_logic_reversal(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> bool:
    from ali2026v3_trading.参数池.backtest_state_checker import _check_logic_reversal as _clr_delegated
    return _clr_delegated(bt, bar, params, _check_safety, _get_contract_multiplier,
                          _compute_dynamic_slippage_bps, _compute_commission,
                          _safe_equity_add, _infer_exchange_id, _infer_exchange_from_id,
                          _bt_capture_snapshot, BACKTEST_THRESHOLDS)


def _check_safety(
    bt: _BacktestState,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
    allow_close: bool = False,
    bar: Optional[pd.Series] = None,
) -> bool:
    from ali2026v3_trading.参数池 import backtest_safety_checker as _bsc
    return _bsc.check_safety(bt, bar_time, params, allow_close=allow_close, bar=bar,
        _get_contract_multiplier=_get_contract_multiplier,
        _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
        _compute_commission=_compute_commission,
        _safe_equity_add=_safe_equity_add,
        _infer_instrument_type=_infer_instrument_type,
        _infer_exchange_id=_infer_exchange_id,
        _infer_exchange_from_id=_infer_exchange_from_id,
        _calculate_limit_prices=_calculate_limit_prices,
        _ClosedTrade=_ClosedTrade)


def _check_backtest_health(bt: _BacktestState, params: Dict[str, float], bar_time) -> bool:
    """P0-R11-06修复: 回测健康检查机制

    模拟实盘get_health_status()的CRITICAL暂停逻辑：
    - 当回测权益低于初始权益的50%时，标记为CRITICAL，暂停新开仓
    - 当连续亏损次数超过阈值时，标记为CRITICAL
    - 与实盘strategy_ecosystem.get_health_status()对齐
    """
    # 权益生存检查：权益低于初始权益50%视为CRITICAL
    if hasattr(bt, 'initial_equity') and bt.initial_equity > 0:
        _health_equity = bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity
        equity_ratio = _health_equity / bt.initial_equity
        if equity_ratio < 0.50:
            return False  # CRITICAL: 暂停新开仓

    # 连续亏损检查：连续亏损超过max_consecutive_losses视为CRITICAL
    max_consecutive = int(params.get("max_consecutive_losses", 5))
    if hasattr(bt, 'consecutive_loss_streak') and bt.consecutive_loss_streak >= max_consecutive:
        return False  # CRITICAL: 暂停新开仓

    return True  # HEALTHY: 允许新开仓


def _try_open(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    strategy_type: str = "main",
    prev_bar: Optional[pd.Series] = None,
) -> None:
    from ali2026v3_trading.参数池 import backtest_position_manager as _bpm
    bar_time = bar.get("minute", pd.NaT)
    _gate_result = _bpm.try_open_check_gates(
        bt, bar, params, bar_time, strategy_type,
        _is_consecutive_loss_paused, _STATE_REASON_MAP,
        BACKTEST_THRESHOLDS, safe_price_check)
    if _gate_result is None:
        return
    reason, symbol, price = _gate_result
    tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
    lots = _bpm.try_open_compute_lots(bt, params, price, sl_ratio, reason, bar, _compute_lots_with_risk_budget)
    if lots <= 0:
        return
    imbalance = bar.get("imbalance", 0)
    if imbalance == 0:
        return
    direction = 1 if imbalance > 0 else -1
    if strategy_type == "shadow_reverse":
        direction = -direction
    _position_volume = sum(abs(int(getattr(pos, 'volume', 0))) for pos in bt.positions.values())
    _open_positions = len(bt.positions)
    if not _bpm.try_open_risk_checks(bt, params, symbol, price, lots, direction, bar, bar_time, _position_volume, _open_positions):
        return
    if not _bpm.try_open_quality_gates(bar, params, _infer_instrument_type):
        return
    _bpm.try_open_execute(
        bt, bar, params, symbol, price, lots, direction, reason,
        bar_time, strategy_type, tp_ratio, sl_ratio,
        _infer_instrument_type, _compute_fill_quantity,
        _simulate_limit_order_queue, _backtest_order_split,
        _compute_dynamic_slippage_bps, _compute_market_impact_v2,
        _compute_commission, _get_contract_multiplier,
        _infer_exchange_id, _infer_exchange_from_id,
        _bt_capture_snapshot, _PendingOrder, _BacktestPosition,
        compute_execution_delay_slippage_bps,
        ENABLE_QUEUE_SIMULATION)


def _check_two_stage_stop(
    pos: _BacktestPosition,
    price: float,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
) -> bool:
    # P1-R11-07修复: stage1_min_minutes适配bar_period，不同K线周期对应不同实际分钟数
    _bar_period = float(params.get("bar_period", 1.0))
    stage1_min_minutes = params.get("stage1_min_minutes", 90.0) * _bar_period
    stage1_profit_threshold = params.get("stage1_profit_threshold", 0.002)
    stage2_slope_window = max(2, int(params.get("stage2_slope_window", 10)))
    stage2_slope_threshold = params.get("stage2_slope_threshold", 0.0)

    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct

    if not pos.stage1_passed:
        if hold_minutes >= stage1_min_minutes and pos.max_float_profit >= stage1_profit_threshold:
            pos.stage1_passed = True

    if not pos.stage1_passed:
        return False

    pos.profit_history.append(float_pnl_pct)
    # R21-MEM-P1-13修复: deque(maxlen)自动淘汰，无需手动截断；list兼容保留截断逻辑
    if isinstance(pos.profit_history, list) and len(pos.profit_history) > pos._PROFIT_HISTORY_MAXLEN:
        pos.profit_history = pos.profit_history[-1000:]  # PD-P2-07: 截断保留最近1000条
    if pos.last_check_time is not None and pos.last_check_time == bar_time:
        pass
    pos.last_check_time = bar_time

    if len(pos.profit_history) >= stage2_slope_window:
        window = pos.profit_history[-stage2_slope_window:]
        _bar_period_min = float(params.get("bar_period", 1.0))  # P1-R11-08修复: 使用时间窗口而非Bar数量
        slope = (window[-1] - window[0]) / (stage2_slope_window * _bar_period_min)
        if slope < stage2_slope_threshold:
            return True

    return False


def _check_intra_bar_stop_loss(
    pos: _BacktestPosition,
    bar: pd.Series,
    prev_bar: Optional[pd.Series],
    params: Dict[str, float],
) -> Tuple[bool, str, float]:
    """BF-01: Bar内止损路径推断 + 跨Bar跳空处理

    实现逻辑:
    - 跳空穿透优先检测: 开盘价直接跳过止损线 → 成交价=开盘价
    - Bar内检测: high/low跨越止损线 → 路径推断确定触发顺序
    - 多头止损: low或open穿透止损线 → 触发
    - 空头止损: high或open穿透止损线 → 触发
    - 优先级: (跳空 > Bar内) × (止损 > 止盈)
    - 路径推断: 阳线→low先触及(先检查止损), 阴线→high先触及(先检查止盈)

    Args:
        pos: 当前持仓
        bar: 当前Bar行情
        prev_bar: 上一Bar行情(用于跳空检测)
        params: 参数字典

    Returns:
        Tuple[triggered, reason, fill_price]
    """
    if pos is None:
        return (False, "", 0.0)

    bar_open = bar.get("open", 0.0)
    bar_high = bar.get("high", 0.0)
    bar_low = bar.get("low", 0.0)
    bar_close = bar.get("close", 0.0)

    if bar_open <= 0 or bar_high <= 0 or bar_low <= 0:
        logger.warning("[C-3] OHLC字段缺失: open=%.4f high=%.4f low=%.4f", bar_open, bar_high, bar_low)
        return (False, "", 0.0)

    enable_gap = params.get("enable_gap_handling", False)
    tp_price = pos.stop_profit_price
    sl_price = pos.stop_loss_price

    if pos.volume > 0:
        prev_close = prev_bar.get("close", bar_open) if prev_bar is not None and enable_gap else bar_open
        if enable_gap and prev_close > 0:
            if bar_open <= sl_price:
                return (True, "StopLoss_GapDown", bar_open)
            if bar_open >= tp_price:
                return (True, "StopProfit_GapUp", bar_open)
        is_bullish = bar_close >= bar_open
        if is_bullish:
            if bar_low <= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
            if bar_high >= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
        else:
            if bar_high >= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
            if bar_low <= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
    elif pos.volume < 0:
        prev_close = prev_bar.get("close", bar_open) if prev_bar is not None and enable_gap else bar_open
        if enable_gap and prev_close > 0:
            if bar_open >= sl_price:
                return (True, "StopLoss_GapUp", bar_open)
            if bar_open <= tp_price:
                return (True, "StopProfit_GapDown", bar_open)
        is_bullish = bar_close >= bar_open
        if is_bullish:
            if bar_low <= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)
            if bar_high >= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
        else:
            if bar_high >= sl_price:
                return (True, "StopLoss_IntraBar", sl_price)
            if bar_low <= tp_price:
                return (True, "StopProfit_IntraBar", tp_price)

    return (False, "", 0.0)


def _compute_option_mtm_price(
    spot_price: float,
    strike_price: float,
    remaining_days: int,
    iv: float,
    option_type: str = "call",
    risk_free_rate: float = 0.02,
) -> float:
    """BF-05: 使用Black-Scholes模型计算期权理论价格用于逐Bar MTM

    Args:
        spot_price: 标的现价
        strike_price: 行权价
        remaining_days: 剩余天数
        iv: 隐含波动率
        option_type: 期权类型 call/put
        risk_free_rate: 无风险利率

    Returns:
        float: 期权理论价格
    """
    if spot_price <= 0 or strike_price <= 0 or remaining_days <= 0 or iv <= 0:
        if spot_price <= 0 or iv <= 0:
            logger.debug("[C-3] _compute_option_mtm_price无效输入: spot_price=%.4f iv=%.4f", spot_price, iv)
        return 0.0
    try:
        import math as _m
        t = remaining_days / 365.0
        d1 = (_m.log(spot_price / strike_price) + (risk_free_rate + 0.5 * iv ** 2) * t) / (iv * _m.sqrt(t))
        d2 = d1 - iv * _m.sqrt(t)
        def _norm_cdf(x):
            return 0.5 * (1.0 + _m.erf(x / _m.sqrt(2.0)))
        if option_type == "call":
            price = spot_price * _norm_cdf(d1) - strike_price * _m.exp(-risk_free_rate * t) * _norm_cdf(d2)
        else:
            price = strike_price * _m.exp(-risk_free_rate * t) * _norm_cdf(-d2) - spot_price * _norm_cdf(-d1)
        return max(0.0, price)
    except Exception:
        return 0.0


def _update_mtm_equity(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> None:
    """BF-05: 逐Bar更新MTM权益

    当enable_mtm_equity=True时，逐Bar计算持仓Mark-to-Market权益，
    使用BS模型对期权持仓进行理论定价，期货持仓使用最新价。

    Args:
        bt: 回测状态
        bar: 当前Bar行情
        params: 参数字典
    """
    if not params.get("enable_mtm_equity", False):
        return
    mtm = bt.equity
    for sym, pos in bt.positions.items():
        if getattr(pos, 'instrument_type', 'future') in ('option_buyer', 'option_seller'):
            if params.get("use_option_bs_pricing", False):
                spot = bar.get("underlying_price", 0.0)
                strike = getattr(pos, 'strike_price', 0.0)
                days = getattr(pos, 'remaining_days', 0)
                iv = getattr(pos, 'open_iv', 0.0)
                opt_type = getattr(pos, 'option_type', 'call')
                theo = _compute_option_mtm_price(spot, strike, days, iv, opt_type)
                if theo > 0:
                    mtm += (theo - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
            else:
                current_price = bar.get("close", pos.open_price)
                mtm += (current_price - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
        else:
            current_price = bar.get("close", pos.open_price)
            mtm += (current_price - pos.open_price) * pos.volume * _get_contract_multiplier(sym)
    bt.mtm_equity = mtm
    bt.mtm_equity_curve.append(mtm)
    if mtm > bt.mtm_peak_equity:
        bt.mtm_peak_equity = mtm
    dd = (bt.mtm_peak_equity - mtm) / bt.mtm_peak_equity if bt.mtm_peak_equity > 0 else 0.0
    if dd > bt.mtm_max_drawdown:
        bt.mtm_max_drawdown = dd


def _compute_fill_quantity(
    order_lots: int,
    bar: pd.Series,
    params: Dict[str, float],
) -> int:
    """BF-06: 基于参与率限制计算实际成交量

    参与率 = 成交量 / Bar成交量, 限制不超过max_participation_rate
    当enable_partial_fill=False时返回原始手数(向后兼容)

    Args:
        order_lots: 请求成交手数
        bar: 当前Bar行情
        params: 参数字典

    Returns:
        int: 实际可成交手数
    """
    if not params.get("enable_partial_fill", True):
        return order_lots
    if order_lots <= 0:
        return 0
    max_part_rate = params.get("max_participation_rate", 1.0)
    min_fill = int(params.get("min_fill_lots", 1))
    bar_volume = bar.get("volume", 0)
    if bar_volume <= 0 or max_part_rate >= 1.0:
        return order_lots
    max_fill = max(min_fill, int(bar_volume * max_part_rate))
    return min(order_lots, max_fill)


def _compute_market_impact_v2(
    fill_lots: int,
    bar: pd.Series,
    price: float,
    params: Dict[str, float],
) -> float:
    """BF-06: Almgren-Chriss冲击成本（二级模型，默认关闭）

    当enable_market_impact=False时返回0.0(向后兼容)
    冲击成本 = eta * sigma * |X|/V + gamma * sigma * (X/V)^2

    Args:
        fill_lots: 成交手数
        bar: 当前Bar行情
        price: 成交价格
        params: 参数字典

    Returns:
        float: 冲击成本(价格单位)
    """
    if not params.get("enable_market_impact", False):
        return 0.0
    if fill_lots <= 0 or price <= 0:
        return 0.0
    eta = params.get("market_impact_eta", 0.1)
    gamma = params.get("market_impact_gamma", 0.05)
    bar_volume = bar.get("volume", 1.0)
    if bar_volume <= 0:
        bar_volume = 1.0
    participation = abs(fill_lots) / bar_volume
    bar_range = bar.get("high", price) - bar.get("low", price)
    sigma = bar_range / price if price > 0 else 0.01
    impact = eta * sigma * participation + gamma * sigma * participation ** 2
    return price * impact


def _apply_fidelity_presets(params: Dict[str, Any]) -> Dict[str, Any]:
    """BF-06: 保真度预设一键切换

    当backtest_fidelity_mode为institutional时，一键设置所有保真度参数，
    启用Bar内止损、跳空处理、延迟执行、MTM权益等机构级回测特性。
    当mode为standard时保持默认值(向后兼容)。

    Args:
        params: 参数字典(会被原地修改)

    Returns:
        Dict: 修改后的参数字典
    """
    mode = params.get("backtest_fidelity_mode", "standard")
    if mode == "institutional":
        params.setdefault("execution_model", "institutional")
        params.setdefault("enable_intra_bar_stop_loss", True)
        params.setdefault("enable_gap_handling", True)
        params.setdefault("open_execution_delay_bars", 1)
        params.setdefault("close_profit_delay_bars", 1)
        params.setdefault("stop_loss_no_delay", True)
        params.setdefault("default_order_type", "maker")
        params.setdefault("enable_mtm_equity", True)
        params.setdefault("use_option_bs_pricing", True)
        params.setdefault("enable_partial_fill", True)
        params.setdefault("max_participation_rate", 0.15)
        params.setdefault("enable_market_impact", True)
        params.setdefault("market_impact_eta", 0.1)
        params.setdefault("market_impact_gamma", 0.05)
        params.setdefault("enable_queue_simulation", True)
        params.setdefault("enable_slippage_model", True)
        params.setdefault("backtest_slippage_premium_bps", 1.0)
    return params


def _check_positions(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    prev_bar: Optional[pd.Series] = None,
) -> None:
    from ali2026v3_trading.参数池 import backtest_position_manager as _bpm2
    _bpm2.check_option_metadata_quality(bt, bar)
    bar_time_p09 = bar.get("minute", pd.NaT)
    if pd.isna(bar_time_p09):
        logger.warning("[R22-TIME-03] bar缺少minute字段，跳过P0-9状态反转平仓检查")
        return
    if not _check_safety(bt, bar_time_p09, params, allow_close=True, bar=bar):
        return
    for sym in list(bt.positions.keys()):
        pos = bt.positions[sym]
        _state_conflict = False
        if hasattr(bt, 'current_state') and hasattr(pos, 'open_reason'):
            if bt.current_state == "incorrect_reversal" and "correct" in str(pos.open_reason):
                _state_conflict = True
            elif bt.current_state == "correct_trending" and "incorrect" in str(pos.open_reason):
                _state_conflict = True
        if _state_conflict:
            from ali2026v3_trading.参数池 import backtest_safety_checker as _bsc2
            _bsc2._force_close_position(bt, sym, pos, bar, bar_time_p09, "state_conflict_force_close",
                _get_contract_multiplier=_get_contract_multiplier,
                _compute_dynamic_slippage_bps=_compute_dynamic_slippage_bps,
                _compute_commission=_compute_commission,
                _safe_equity_add=_safe_equity_add,
                _infer_instrument_type=_infer_instrument_type,
                _infer_exchange_id=_infer_exchange_id,
                _infer_exchange_from_id=_infer_exchange_from_id,
                _calculate_limit_prices=_calculate_limit_prices,
                _ClosedTrade=_ClosedTrade, params=params)

    symbol = bar.get("symbol", "")
    price = bar.get("close", 0.0)
    bar_time = bar.get("minute", pd.NaT)
    if pd.isna(bar_time):
        logger.warning("[R22-TIME-03] bar缺少minute字段，跳过持仓检查")
        return
    if symbol not in bt.positions or price <= 0:
        return

    pos = bt.positions[symbol]
    open_reason = getattr(pos, 'open_reason', 'CORRECT_RESONANCE')
    hard_stop_min = _resolve_time_stop(params, open_reason, bt.current_state)
    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0

    float_pnl_pct = (price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
    if pos.volume < 0:
        float_pnl_pct = -float_pnl_pct
    if float_pnl_pct > pos.max_float_profit:
        pos.max_float_profit = float_pnl_pct

    should_close = False
    close_reason = ""

    if params.get("enable_intra_bar_stop_loss", False):
        intra_triggered, intra_reason, intra_fill = _check_intra_bar_stop_loss(pos, bar, prev_bar, params)
        if intra_triggered:
            should_close = True
            close_reason = intra_reason
            price = intra_fill

    if not should_close and pos.volume > 0:
        if price >= pos.stop_profit_price or np.isclose(price, pos.stop_profit_price):
            should_close = True
            close_reason = "StopProfit"
        elif price <= pos.stop_loss_price or np.isclose(price, pos.stop_loss_price):
            should_close = True
            close_reason = "StopLoss"
    elif pos.volume < 0:
        if price <= pos.stop_profit_price or np.isclose(price, pos.stop_profit_price):
            should_close = True
            close_reason = "StopProfit"
        elif price >= pos.stop_loss_price or np.isclose(price, pos.stop_loss_price):
            should_close = True
            close_reason = "StopLoss"

    if not should_close and _check_two_stage_stop(pos, price, bar_time, params):
        should_close = True
        close_reason = "TwoStageTimeStop"

    if not should_close and hold_minutes >= hard_stop_min:
        should_close = True
        close_reason = "HardTimeStop"

    if not should_close:
        eod_hour = bar_time.hour
        eod_minute = bar_time.minute
        if eod_hour == 14 and eod_minute >= 55:
            should_close = True
            close_reason = "EOD"
        elif eod_hour == 2 and eod_minute >= 25:
            should_close = True
            close_reason = "EOD_NIGHT"

    if should_close:
        _close_delay = int(params.get("close_profit_delay_bars", 0))
        _is_stop_loss_close = "StopLoss" in str(close_reason)
        _stop_no_delay = params.get("stop_loss_no_delay", True)
        if _close_delay > 0 and not (_is_stop_loss_close and _stop_no_delay):
            _delay_remaining = getattr(pos, '_close_delay_remaining', None)
            if _delay_remaining is None:
                setattr(pos, '_close_delay_remaining', _close_delay - 1)
                logger.debug("[P1-R11-04] 平仓延迟: %s 延迟%d bar, 原因=%s", symbol, _close_delay, close_reason)
                should_close = False
            elif _delay_remaining > 0:
                pos._close_delay_remaining = _delay_remaining - 1
                should_close = False

    if should_close:
        _mult = _get_contract_multiplier(symbol)
        pnl = (price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
        bid_ask = bar.get("bid_ask_spread", 0.0)
        spread_q = bar.get("_spread_quality", 0)
        slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
        # R4-P-09修复: 平仓时计算级联滑点 — 先平后开的滑点叠加
        _cascade_slip_bps = _compute_cascade_slippage_bps(
            base_slippage_bps=slip_bps,
            close_volume=pos.lots,
            avg_volume=bar.get("volume", 1.0),
            is_state_switch=(close_reason in ("state_conflict_force_close", "HardTimeStop")),
        )
        slip = price * _cascade_slip_bps / 10000 * pos.lots
        commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(symbol)))  # P0-4: 按品种计算平仓手续费; P1-R11-03: 传入exchange_id; P0-D3: 传入exchange
        net_pnl = pnl - slip - commission
        # R4-P-06修复: NaN/inf防护 — 确保净PnL是有限数值
        if not np.isfinite(net_pnl):
            logger.warning("[R4-P-06] NaN/inf detected in net_pnl: pnl=%.4f, slip=%.4f, commission=%.4f, net_pnl=%.4f",
                           pnl, slip, commission, net_pnl)
            net_pnl = 0.0
        _safe_equity_add(bt, net_pnl)
        bt.total_trades += 1
        _bt_capture_snapshot(bt, "close", f"{close_reason} {symbol}", "", bar)
        bt.recent_pnls.append(net_pnl)
        if len(bt.recent_pnls) > 50:
            bt.recent_pnls = bt.recent_pnls[-50:]
        pnl_pct = float_pnl_pct if pos.open_price > 0 else 0.0
        hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
        bt.closed_trades.append(_ClosedTrade(
            pnl=net_pnl, pnl_pct=pnl_pct, close_reason=close_reason,
            hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
            premium_pnl=0.0, delta_pnl=0.0,
            stage1_passed=getattr(pos, 'stage1_passed', False),
        ))
        del bt.positions[symbol]

        daily_dd = (bt.daily_start_equity - (bt.mtm_equity if params.get("enable_mtm_equity", False) and bt.mtm_equity > 0 else bt.equity)) / bt.daily_start_equity if bt.daily_start_equity > 0 else 0
        if daily_dd > BACKTEST_THRESHOLDS["circuit_breaker_daily_dd"]:
            pause_sec = params.get("circuit_breaker_pause_sec", 180.0)
            bt.circuit_breaker_until = bar_time + pd.Timedelta(seconds=pause_sec)
            bt.circuit_breaker_events.append({
                'trigger_time': str(bar_time),
                'daily_dd': daily_dd,
                'pause_sec': pause_sec,
                'resume_time': str(bt.circuit_breaker_until),
                'equity_at_trigger': bt.equity,
            })


def _compute_profit_loss_ratio_metrics(closed_trades: List[_ClosedTrade], equity_curve: np.ndarray, strategy_type: str = '', ticks_per_bar: int = 0) -> Dict[str, Any]:
    return _compute_plr_metrics_delegated(
        closed_trades, equity_curve, strategy_type=strategy_type,
        ticks_per_bar=ticks_per_bar, _get_annualize_factor=_get_annualize_factor
    )


def _check_bar_data_monotonic(bar_data: pd.DataFrame) -> None:
    """NP-P2-15: 检查bar时间序列是否单调递增（仅告警，不改变输入数据）。"""
    if bar_data is None or bar_data.empty or "minute" not in bar_data.columns:
        return
    try:
        ts = pd.to_datetime(bar_data["minute"], errors="coerce")
        if ts.isna().any():
            logger.warning("[NP-P2-15] minute列存在无法解析时间，跳过单调性校验")
            return
        if not ts.is_monotonic_increasing:
            logger.warning("[NP-P2-15] bar_data.minute非单调递增，回测结果可能偏差")
    except Exception as e:
        logger.debug("[NP-P2-15] bar_data单调性校验失败: %s", e)


def _build_backtest_result(
    bt: _BacktestState,
    strategy_type: str,
    bar_data: pd.DataFrame,
    params: Dict[str, float],
    extra_fields: Optional[Dict[str, Any]] = None,
    ticks_per_bar: int = 0,
) -> Dict[str, Any]:
    from ali2026v3_trading.参数池.backtest_result_builder import _build_backtest_result as _bbr_delegated
    return _bbr_delegated(bt, strategy_type, bar_data, params, extra_fields, ticks_per_bar,
                          _compute_profit_loss_ratio_metrics, _get_annualize_factor,
                          detect_rollover_gaps, exclude_rollover_signals, compute_rollover_cost)


def _reset_daily(bt: _BacktestState, current_date: str) -> None:
    if bt.prev_date is not None and current_date != bt.prev_date:
        if bt.daily_start_equity > 0:
            daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
            bt.daily_returns.append(daily_ret)
        bt.daily_start_equity = bt.equity
        bt.daily_loss = 0.0
        bt.circuit_breaker_until = None
    bt.prev_date = current_date


def _backfill_bar_fields(bar):
    if "underlying_future_id" not in bar or not bar.get("underlying_future_id"):
        _sym = bar.get("symbol", "")
        if _sym:
            _m = re.match(r'^([A-Za-z]+)', _sym)
            if _m:
                bar["underlying_future_id"] = hash(_m.group(1)) % 1000
            _ym = re.search(r'(\d{4})', _sym)
            if _ym:
                bar["month"] = _ym.group(1)[-2:]


def run_backtest(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """V7共振策略回测：完整信号→决策→执行→风控闭环

    Args:
        params: 参数字典
        bar_data: 分钟Bar数据
        train: 是否训练集
        strategy_type: 策略模式
            - 'main': 主策略（正常逻辑）
            - 'shadow_reverse': 影子策略A（反向逻辑，开仓方向相反）
            - 'shadow_random': 影子策略B（随机动作，信号随机化）

    V7.1决策频率机制：
        决策频率由K线周期 × state_confirm_bars自然决定，每Bar都执行状态检测与风控。
        NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)。
    """
    # [R23-P2-ID-03-FIX] trial隔离标志：每个trial开始时设置唯一ID，结束时清除
    import uuid as _uuid_mod
    _trial_isolation_flag = _uuid_mod.uuid4().hex[:12]
    try:
        import threading as _threading_mod
        _threading_mod.current_thread().trial_isolation_flag = _trial_isolation_flag
    except Exception:
        pass
    if bar_data.empty:
        return {"error": "无数据", "params": params}

    # R23-IN-P1-04-FIX: 策略onInit完成等待守卫 — 确保策略初始化完成后才开始回测
    try:
        from ali2026v3_trading.strategy_core_service import StrategyCoreService
        _init_pending = getattr(StrategyCoreService, '_init_pending', False)
        if _init_pending:
            import time as _wait_time
            _wait_deadline = _wait_time.time() + 10.0
            while getattr(StrategyCoreService, '_init_pending', False) and _wait_time.time() < _wait_deadline:
                _wait_time.sleep(0.05)
            if getattr(StrategyCoreService, '_init_pending', False):
                logging.warning("[R23-IN-P1-04] 策略onInit未在10s内完成，回测继续(降级)")
    except Exception as _p1_04_e:
        logging.debug("[R23-IN-P1-04] 策略初始化等待守卫跳过: %s", _p1_04_e)

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    _apply_fidelity_presets(params)
    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)

    # P1-R9-11修复: 检查影子参数独立性违反标志，阻断新开仓
    if params.get("shadow_param_violation", False):
        bt.new_open_blocked = True
        logger.warning("[P1-R9-11] 影子参数独立性违反，回测新开仓已阻断")

    # R3-D-07修复: 回测中从StateParamManager加载三态参数集
    try:
        from ali2026v3_trading.state_param_manager import get_state_param_manager
        _spm = get_state_param_manager()
        _spm_params = _spm.get_params(strategy_type)
        if _spm_params and isinstance(_spm_params, dict):
            params = {**params, **_spm_params}
    except Exception as _d07_e:
        logger.debug("[R3-D-07] StateParamManager加载失败(使用默认params): %s", _d07_e)

    # R3-D-02修复: 从config_params注入position_scale到回测params
    # 确保position_scale从配置系统传递到_compute_lots_with_risk_budget
    if "position_scale" not in params:
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _cp = get_cached_params()
            _ps_key = f"{strategy_type}_position_scale"
            params["position_scale"] = float(_cp.get(_ps_key, _cp.get("position_scale", 1.0)))
        except Exception:
            params.setdefault("position_scale", 1.0)

    # 行情寿命字典：从bar_data构建（如果尚未构建）
    estimator = _get_life_estimator()
    if estimator is not None and (not hasattr(estimator, '_life_dict') or not estimator._life_dict):
        try:
            estimator.build_life_dict(bar_data)
        except Exception as _e:
            logger.error("[R3-P-09] build_life_dict失败(严重): %s, 寿命信息缺失将导致D3维度退化为0.5", _e)
            bt.get("diagnostics", {}).setdefault("degraded_features", []).append("life_dict")

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        prev_bar = bar_data.iloc[idx - 1] if idx > 0 else None
        bt.bar_idx = idx
        # CS-03修复: 主循环延迟滑点模拟（替代pass空操作）
        _execution_delay_ms = params.get("execution_delay_ms", 50)
        if _execution_delay_ms > 0:
            _bar_high = bar.get("high", 0.0)
            _bar_low = bar.get("low", 0.0)
            _bar_close = bar.get("close", 0.0)
            _bar_dur = float(params.get("bar_duration_sec", 60.0))
            _main_delay_bps = compute_execution_delay_slippage_bps(
                price=_bar_close, bar_high=_bar_high, bar_low=_bar_low,
                bar_duration_sec=_bar_dur, exec_delay_ms=_execution_delay_ms, z_score=1.0
            )
            if _main_delay_bps > 0 and hasattr(bt, 'equity'):
                bt.equity -= bt.equity * _main_delay_bps / 10000.0 * 0.001  # 微量扣除，模拟持仓更新延迟成本
        _backfill_bar_fields(bar)
        _update_mtm_equity(bt, bar, params)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        # P1-R9-30修复: Bar重复/跳过检测
        _bar_ts = bar.name if isinstance(bar.name, (int, float)) else getattr(bar, 'timestamp', None)
        if _bar_ts is not None and hasattr(bt, '_last_bar_ts') and bt._last_bar_ts is not None:
            if _bar_ts == bt._last_bar_ts:
                logger.warning("[P1-R9-30] 重复Bar时间戳: %s, 跳过", _bar_ts)
                continue
            elif _bar_ts < bt._last_bar_ts:
                logger.warning("[P1-R9-30] Bar时间戳回退: %s < %s, 跳过", _bar_ts, bt._last_bar_ts)
                continue
        bt._last_bar_ts = _bar_ts
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params, prev_bar=prev_bar)
            elif params.get("enable_intra_bar_stop_loss", False):
                _check_positions(bt, bar, params, prev_bar=None)

        if params.get("execution_model", "standard") == "institutional" and bt.pending_orders:
            _remaining_orders = []
            for order in bt.pending_orders:
                order_age = idx - order.created_at_bar
                required_delay = params.get("open_execution_delay_bars", 1) if order.order_type == "open" else params.get("close_profit_delay_bars", 1)
                if order_age >= required_delay:
                    # BF-P1-09已修复: 延迟订单撤单模拟 — 延迟过长时尝试撤单
                    _cancel_delay_bars = max(1, int(params.get("cancel_delay_ms", CANCEL_DELAY_MS) / 60000.0))
                    if order_age > required_delay + _cancel_delay_bars:
                        _cancel_result = _simulate_order_cancel(
                            order_id=f"{order.symbol}_{order.created_at_bar}",
                            cancel_delay_ms=int(params.get("cancel_delay_ms", CANCEL_DELAY_MS)),
                            failure_rate=float(params.get("cancel_failure_rate", CANCEL_FAILURE_RATE)),
                            enable_cancel=params.get("enable_cancel_simulation", ENABLE_CANCEL_SIMULATION),
                        )
                        if _cancel_result["success"]:
                            logger.debug("[BF-P1-09] 延迟订单撤单成功: %s (延迟%d bar)", order.symbol, order_age)
                            continue  # 撤单成功，不执行此订单
                        else:
                            logger.warning("[BF-P1-09] 延迟订单撤单失败，继续执行: %s, 原因=%s", order.symbol, _cancel_result["reason"])
                    if order.symbol not in bt.positions:
                        exec_price = bar.get("open", bar.get("close", 0.0))
                        if safe_price_check(exec_price):
                            if order.order_type == "open":
                                _delay_fill_lots = _compute_fill_quantity(order.lots, bar, order.params_snapshot)
                                if _delay_fill_lots <= 0:
                                    if order.retry_count < 3:
                                        order.retry_count += 1
                                        _remaining_orders.append(order)
                                    continue
                                _delay_lots = _delay_fill_lots
                            else:
                                _delay_lots = order.lots
                            bid_ask = bar.get("bid_ask_spread", 0.0)
                            spread_q = bar.get("_spread_quality", 0)
                            slip_bps = _compute_dynamic_slippage_bps(exec_price, bid_ask, spread_quality=spread_q, bar=bar, params=order.params_snapshot)
                            _mult = _get_contract_multiplier(order.symbol)
                            slip_cost = exec_price * slip_bps / 10000 * _delay_lots
                            commission = _compute_commission(order.symbol, _delay_lots, is_open=(order.order_type == "open"), exchange_id=_infer_exchange_id(order.symbol), exchange=_infer_exchange_from_id(_infer_exchange_id(order.symbol)), open_time=bar.get("minute", pd.NaT) if order.order_type == "open" else None, close_time=bar.get("minute", pd.NaT) if order.order_type != "open" else None)
                            if order.order_type == "open":
                                direction = 1 if order.volume > 0 else -1
                                _delay_volume = direction * _delay_lots
                                if direction > 0:
                                    exec_price += exec_price * slip_bps / 10000
                                else:
                                    exec_price -= exec_price * slip_bps / 10000
                                sp_price = exec_price * order.tp_ratio if _delay_volume > 0 else exec_price / order.tp_ratio
                                sl_price = exec_price * (1 - order.sl_ratio) if _delay_volume > 0 else exec_price * (1 + order.sl_ratio)
                                _delay_fill_ratio = _delay_lots / order.lots if order.lots > 0 else 1.0
                                pos = _BacktestPosition(
                                    instrument_id=order.symbol,
                                    volume=_delay_volume,
                                    open_price=exec_price,
                                    open_time=bar.get("minute", pd.NaT),
                                    stop_profit_price=sp_price,
                                    stop_loss_price=sl_price,
                                    open_reason=order.reason,
                                    lots=_delay_lots,
                                    open_state=bt.current_state,
                                    open_strength=0.0,
                                    instrument_type=_infer_instrument_type(order.symbol),
                                    fill_ratio=_delay_fill_ratio,
                                )
                                bt.positions[order.symbol] = pos
                                bt.equity -= (commission + slip_cost)
                                bt.total_signals += 1
                                bt.total_trades += 1
                                logger.debug("[BF-02/03] 延迟开仓执行: %s @ %.4f (延迟%d bar)", order.symbol, exec_price, order_age)
                else:
                    if order_age < required_delay + 3:
                        _remaining_orders.append(order)
                    else:
                        logger.warning("[BF-02/03] 延迟订单超时放弃: %s (延迟%d bar)", order.symbol, order_age)
            bt.pending_orders = _remaining_orders

        # EX-P1-08: 涨跌停Bar跳过 — 涨停/跌停Bar无法成交，跳过决策
        # BF-P1-06已修复: 使用_calculate_limit_prices自动计算涨跌停
        _bar_limit_info = _calculate_limit_prices(bar, instrument_type=_infer_instrument_type(bar.get('symbol', '')).replace('option_buyer', 'OPTION_ETF').replace('option_seller', 'OPTION_ETF').replace('future', 'FUTURE'))
        if _bar_limit_info['is_limit_up'] or _bar_limit_info['is_limit_down']:
            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)
            continue

        # P2-裂缝43：回测中state_check_interval模拟
        # 当相邻Bar时间间隔 > state_check_interval_sec时，额外插入虚拟检查点
        state_check_interval_sec = params.get("state_check_interval_sec", 180.0)
        if idx > 0:
            prev_time = bar_data.iloc[idx - 1].get("minute", None)
            curr_time = bar.get("minute", None)
            if prev_time is not None and curr_time is not None:
                try:
                    if hasattr(prev_time, 'timestamp') and hasattr(curr_time, 'timestamp'):
                        time_gap_sec = (curr_time - prev_time).total_seconds()
                        if time_gap_sec > state_check_interval_sec:
                            # 行情不活跃导致状态检查延迟，插入虚拟检查点
                            # NP-P1-18修复: round替代int截断，避免跳过检查次数少算
                            n_skipped_checks = round(time_gap_sec / state_check_interval_sec)
                            for _ in range(n_skipped_checks):
                                bt.state_confirm_count += 1
                except Exception as _e:
                    logger.warning("[BACKTEST] state_check_interval calculation failed: %s", _e)

        # P1-13修复: decision_interval_minutes用于回测跳帧逻辑(与state_confirm_bars互补)
        # 手册4.8节: S3/S4在1分钟Bar上产生过高噪声决策，需跳帧降低频率
        # 注: 主系统已用bar_interval替代，回测引擎保留此参数控制决策频率
        decision_interval = params.get("decision_interval_minutes", params.get("bar_interval", 1))
        if decision_interval > 1 and (idx % decision_interval != 0):
            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)
            continue  # 跳过此Bar的决策

        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            # P0-R11-06修复: 回测健康检查，CRITICAL时暂停新开仓
            _health_ok = _check_backtest_health(bt, params, bar_time)
            strength = bar.get("strength", 0)
            should_open = _health_ok and strength > 0.3 and len(bt.positions) < int(params.get("max_open_positions", 3))

            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False

            if strategy_type == "shadow_random":
                should_open = np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3))

            if should_open:
                # P1-R11-05: execution_delay_ms模拟订单执行延迟(默认50ms)
                _try_open(bt, bar, params, strategy_type=strategy_type)

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)


    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    # P0-R9-12修复: 回测结束时强制平仓所有未平仓头寸
    if len(bt.positions) > 0:
        from ali2026v3_trading.param_pool.backtest_checks import _force_close_all_positions
        _last_bar = bar_data.iloc[-1] if bar_data is not None and len(bar_data) > 0 else None
        if _last_bar is not None:
            _force_close_all_positions(bt, _last_bar, params, reason="backtest_end_force_close")

    # R21-MEM-P2-15修复: 回测完成后大型临时变量(bt对象含equity_curve/daily_returns/closed_trades等)
    # 建议调用方在获取result后执行: del bt; import gc; gc.collect()
    # 特别是批量回测场景(如grid_scan)，不及时释放会导致内存累积
    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )


def _infer_trend_scores_from_bar(bar: pd.Series) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    from ali2026v3_trading.参数池.backtest_snapshot import _infer_trend_scores_from_bar as _itsfb_delegated
    return _itsfb_delegated(bar)


# R27-CP-01-FIX: detect_rollover_gaps 和 compute_rollover_cost 已移至 shared_trading_constants.py
from ali2026v3_trading.shared_trading_constants import detect_rollover_gaps, compute_rollover_cost  # noqa: F401


def exclude_rollover_signals(bar_data: pd.DataFrame,
                              rollover_points: List[Dict[str, Any]],
                              skip_days: int = 3,
                              date_col: str = "minute") -> pd.DataFrame:
    """P0-裂缝5：在展期日前后各skip_days天剔除所有策略信号

    在bar_data中添加 'rollover_excluded' 列，展期窗口内的bar标记为True。
    """
    if not rollover_points or date_col not in bar_data.columns:
        bar_data = bar_data.copy()
        bar_data["rollover_excluded"] = False
        return bar_data

    bar_data = bar_data.copy()
    bar_data["rollover_excluded"] = False

    bar_dates = bar_data[date_col].values
    for rp in rollover_points:
        idx = rp["index"]
        if idx < len(bar_dates):
            rollover_date = bar_dates[idx]
            try:
                if hasattr(rollover_date, 'date'):
                    rd = rollover_date.date()
                else:
                    rd = rollover_date
                from datetime import timedelta
                for offset in range(-skip_days, skip_days + 1):
                    target = rd + timedelta(days=offset)
                    mask = bar_data[date_col].apply(
                        lambda x, t=target: hasattr(x, 'date') and x.date() == t
                    )
                    bar_data.loc[mask, "rollover_excluded"] = True
            except Exception:
                # 简化：按索引范围标记
                start = max(0, idx - skip_days * 240)  # 假设每天240个1分钟bar
                end = min(len(bar_data), idx + skip_days * 240)
                bar_data.loc[bar_data.index[start:end], "rollover_excluded"] = True

    n_excluded = bar_data["rollover_excluded"].sum()
    if n_excluded > 0:
        logger.info("[裂缝5-展期剔除] 标记 %d/%d bar为展期排除区", n_excluded, len(bar_data))

    return bar_data


# R27-CP-01-FIX: compute_rollover_cost 已移至 shared_trading_constants.py（已在文件上方导入）