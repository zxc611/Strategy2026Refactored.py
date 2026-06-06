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
    from ali2026v3_trading.参数池.backtest_loop import _build_risk_dimension_defaults as _impl
    return _impl()


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
    from ali2026v3_trading.参数池.backtest_loop import validate_shadow_param_independence as _impl
    return _impl(threshold)


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
    from ali2026v3_trading.参数池.backtest_loop import _resolve_time_stop_hard as _impl
    return _impl(params, open_reason)


def _resolve_time_stop(params: Dict[str, float], open_reason: str, current_state: str = None) -> float:
    from ali2026v3_trading.参数池.backtest_loop import _resolve_time_stop as _impl
    return _impl(params, open_reason, current_state)


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
    from ali2026v3_trading.参数池.backtest_position_manager import _compute_lots_with_risk_budget as _impl
    return _impl(equity, price, sl_ratio, lots_min, params, recent_pnls=recent_pnls, bar=bar, current_positions=current_positions, bt=bt)


def _is_consecutive_loss_paused(bt: _BacktestState, params: Dict[str, float], bar_time: pd.Timestamp) -> bool:
    from ali2026v3_trading.参数池.backtest_loop import _is_consecutive_loss_paused as _impl
    return _impl(bt, params, bar_time)


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
    from ali2026v3_trading.参数池.backtest_loop import _compute_dynamic_slippage_bps as _impl
    return _impl(price, bid_ask_spread, base_slippage_bps=base_slippage_bps, spread_quality=spread_quality, bar=bar, params=params)


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
    from ali2026v3_trading.参数池.backtest_loop import _get_expiry_slippage_multiplier as _impl
    return _impl(bar, params)


def _compute_cascade_slippage_bps(
    base_slippage_bps: float,
    close_volume: float = 0.0,
    avg_volume: float = 1.0,
    is_state_switch: bool = False,
) -> float:
    from ali2026v3_trading.参数池.backtest_loop import _compute_cascade_slippage_bps as _impl
    return _impl(base_slippage_bps, close_volume, avg_volume, is_state_switch)


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
    from ali2026v3_trading.参数池.backtest_loop import _check_backtest_health as _impl
    return _impl(bt, params, bar_time)


def _try_open(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    strategy_type: str = "main",
    prev_bar: Optional[pd.Series] = None,
) -> None:
    from ali2026v3_trading.参数池.backtest_loop import _try_open as _impl
    return _impl(bt, bar, params, strategy_type=strategy_type, prev_bar=prev_bar)


def _check_two_stage_stop(
    pos: _BacktestPosition,
    price: float,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
) -> bool:
    from ali2026v3_trading.参数池.backtest_loop import _check_two_stage_stop as _impl
    return _impl(pos, price, bar_time, params)


def _check_intra_bar_stop_loss(
    pos: _BacktestPosition,
    bar: pd.Series,
    prev_bar: Optional[pd.Series],
    params: Dict[str, float],
) -> Tuple[bool, str, float]:
    from ali2026v3_trading.参数池.backtest_loop import _check_intra_bar_stop_loss as _impl
    return _impl(pos, bar, prev_bar, params)


def _compute_option_mtm_price(
    spot_price: float,
    strike_price: float,
    remaining_days: int,
    iv: float,
    option_type: str = "call",
    risk_free_rate: float = 0.02,
) -> float:
    from ali2026v3_trading.参数池.backtest_loop import _compute_option_mtm_price as _impl
    return _impl(spot_price, strike_price, remaining_days, iv, option_type=option_type, risk_free_rate=risk_free_rate)


def _update_mtm_equity(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> None:
    from ali2026v3_trading.参数池.backtest_loop import _update_mtm_equity as _impl
    return _impl(bt, bar, params)


def _compute_fill_quantity(
    order_lots: int,
    bar: pd.Series,
    params: Dict[str, float],
) -> int:
    from ali2026v3_trading.参数池.backtest_loop import _compute_fill_quantity as _impl
    return _impl(order_lots, bar, params)


def _compute_market_impact_v2(
    fill_lots: int,
    bar: pd.Series,
    price: float,
    params: Dict[str, float],
) -> float:
    from ali2026v3_trading.参数池.backtest_loop import _compute_market_impact_v2 as _impl
    return _impl(fill_lots, bar, price, params)


def _apply_fidelity_presets(params: Dict[str, Any]) -> Dict[str, Any]:
    from ali2026v3_trading.参数池.backtest_loop import _apply_fidelity_presets as _impl
    return _impl(params)


def _check_positions(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    prev_bar: Optional[pd.Series] = None,
) -> None:
    _check_positions_phase1_state_conflict(bt, bar, params)
    _check_positions_phase2_close_check(bt, bar, params, prev_bar)


def _check_positions_phase1_state_conflict(bt, bar, params):
    from ali2026v3_trading.参数池.backtest_loop import _check_positions_phase1_state_conflict as _impl
    return _impl(bt, bar, params)


def _check_positions_phase2_close_check(bt, bar, params, prev_bar):
    from ali2026v3_trading.参数池.backtest_loop import _check_positions_phase2_close_check as _impl
    return _impl(bt, bar, params, prev_bar)


def _determine_close_decision(pos, price, bar_time, bar, prev_bar, params, hold_minutes, hard_stop_min):
    from ali2026v3_trading.参数池.backtest_loop import _determine_close_decision as _impl
    return _impl(pos, price, bar_time, bar, prev_bar, params, hold_minutes, hard_stop_min)


def _execute_position_close(bt, pos, symbol, price, close_reason, bar, bar_time, params, float_pnl_pct):
    from ali2026v3_trading.参数池.backtest_loop import _execute_position_close as _impl
    return _impl(bt, pos, symbol, price, close_reason, bar, bar_time, params, float_pnl_pct)


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
    params, bt = _run_backtest_setup(params, bar_data, train, strategy_type)
    if bt is None:
        return params
    _run_backtest_main_loop(bt, bar_data, params, strategy_type)
    return _run_backtest_finalize(bt, bar_data, params, strategy_type)


def _run_backtest_setup(params, bar_data, train, strategy_type):
    from ali2026v3_trading.参数池.backtest_loop import _run_backtest_setup as _impl
    return _impl(params, bar_data, train, strategy_type)


def _run_backtest_main_loop(bt, bar_data, params, strategy_type):
    from ali2026v3_trading.参数池.backtest_loop import _run_backtest_main_loop as _impl
    return _impl(bt, bar_data, params, strategy_type)


def _process_pending_orders(bt, bar, params, idx):
    from ali2026v3_trading.参数池.backtest_loop import _process_pending_orders as _impl
    return _impl(bt, bar, params, idx)


def _run_backtest_finalize(bt, bar_data, params, strategy_type):
    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    if len(bt.positions) > 0:
        try:
            from ali2026v3_trading.param_pool.backtest_checks import _force_close_all_positions
        except ImportError:
            from ali2026v3_trading.参数池.backtest_checks import _force_close_all_positions
        _last_bar = bar_data.iloc[-1] if bar_data is not None and len(bar_data) > 0 else None
        if _last_bar is not None:
            _force_close_all_position(bt, _last_bar, params, reason="backtest_end_force_close")

    return _build_backtest_result(bt=bt, strategy_type=strategy_type, bar_data=bar_data, params=params)


def _infer_trend_scores_from_bar(bar: pd.Series) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    from ali2026v3_trading.参数池.backtest_snapshot import _infer_trend_scores_from_bar as _itsfb_delegated
    return _itsfb_delegated(bar)


# R27-CP-01-FIX: detect_rollover_gaps 和 compute_rollover_cost 已移至 shared_trading_constants.py
from ali2026v3_trading.shared_trading_constants import detect_rollover_gaps, compute_rollover_cost  # noqa: F401


def exclude_rollover_signals(bar_data: pd.DataFrame,
                              rollover_points: List[Dict[str, Any]],
                              skip_days: int = 3,
                              date_col: str = "minute") -> pd.DataFrame:
    from ali2026v3_trading.参数池.backtest_loop import exclude_rollover_signals as _impl
    return _impl(bar_data, rollover_points, skip_days=skip_days, date_col=date_col)


# R27-CP-01-FIX: compute_rollover_cost 已移至 shared_trading_constants.py（已在文件上方导入）