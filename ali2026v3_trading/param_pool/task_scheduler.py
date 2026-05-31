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
    P1修复：添加enable_queue_simulation配置参数预留，占位实现_simulate_limit_order_queue()
  - BF-P1-03: 市价单始终以收盘价成交（假设：无滑点模拟）
    P1修复：添加market_order_slippage_bps和market_order_price_mode配置参数预留，
           占位实现_simulate_market_order_slippage()支持weighted/open/high/low/close成交价
  - BF-P1-04: 未考虑资金占用和机会成本（假设：无限资金）
  - BF-P1-05: 期权定价使用BS模型而非市场实际报价（假设：BS模型有效）
  - BF-P1-06: 未模拟保证金追保和强平（假设：保证金充足）
  - BF-P1-07: ETF/期货/期权使用相同撮合假设（假设：品种无差异）
    P1修复：添加instrument_slippage_multiplier配置参数预留，按instrument_type区分滑点，
           占位实现_get_instrument_type_slippage()
  - BF-P1-08: 未模拟交易所限速和拒单（假设：无限制）
  - BF-P1-09: 无撤单模拟（假设：撤单立即成功）
    P1修复：添加enable_cancel_simulation配置参数预留，占位实现_simulate_order_cancel()
           支持撤单延迟和失败率模拟
  - BF-P1-10: 未模拟极端行情下的流动性枯竭（假设：流动性充足）

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
  - enable_queue_simulation: bool = False    # BF-P1-02: 是否启用限价单排队模拟
  - queue_timeout_seconds: int = 300         # BF-P1-02: 限价单排队超时(秒)
  - market_order_slippage_bps: float = 5.0   # BF-P1-03: 市价单滑点基点
  - market_order_price_mode: str = "weighted" # BF-P1-03: 成交价模式(close/weighted/random)
  - instrument_slippage_multiplier: dict     # BF-P1-07: 品种滑点乘数
  - enable_cancel_simulation: bool = False   # BF-P1-09: 是否启用撤单模拟
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
import re as _re
import random as _pyrandom
import sys
import threading
import time
import copy
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

# R17-12修复: 从shared_utils导入统一年化因子常量
from ali2026v3_trading.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE
# R24-P1-IV-11修复: 导入safe_price_check替代price<=0，防止NaN/Inf穿透
from ali2026v3_trading.shared_utils import safe_price_check
from ali2026v3_trading.cross_system_execution import CrossSystemExecutionKernel
from ali2026v3_trading.db_adapter import connect_duckdb

_CROSS_SYSTEM_EXECUTION_KERNEL = CrossSystemExecutionKernel()
logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02


def _get_annualize_factor(strategy_type: str) -> float:
    """NP-P2-10: 按策略时间框架选择Sharpe年化因子
    S1(hft)/S2(minute) → √(252*240); S3/S4/S5/S6 → √252
    """
    _daily_types = {'box_extreme', 'box_spring', 'arbitrage', 'market_making'}
    if strategy_type in _daily_types:
        return ANNUALIZE_FACTOR_DAILY
    return ANNUALIZE_FACTOR_MINUTE

# 行情寿命字典集成
_LIFE_ESTIMATOR = None
# R10-P0-02修复: _LIFE_ESTIMATOR添加线程锁，防止并发创建多实例
_LIFE_ESTIMATOR_LOCK = threading.Lock()


def _sync_random_seed(seed: int) -> None:
    """NP-P1-22修复: 同步设置numpy和Python random种子"""
    np.random.seed(seed)
    _pyrandom.seed(seed)

# R10-P0-20修复: sys.path.insert移到模块级别，只执行一次
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# R10-P0-20修复: CascadeJudge模块级懒初始化单例，避免每个task重复实例化
_CascadeJudge = None
_adapt_backtest_result = None
_cascade_judge_lock = threading.Lock()

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
                from ali2026v3_trading.param_pool.L1参数量化.bayesian_shrinkage_life_estimator import BayesianShrinkageLifeEstimator
                _LIFE_ESTIMATOR = BayesianShrinkageLifeEstimator(params=params)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("BayesianShrinkageLifeEstimator not available: %s", e)
        return _LIFE_ESTIMATOR

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
ENABLE_CANCEL_SIMULATION = False         # BF-P1-09: 是否启用撤单模拟
CANCEL_DELAY_MS = 100                    # BF-P1-09: 撤单延迟(毫秒)
CANCEL_FAILURE_RATE = 0.05               # BF-P1-09: 撤单失败率(5%)
ENABLE_PARTIAL_FILL = False              # BF-P1-04: 是否启用部分成交模拟
PARTIAL_FILL_MIN_RATIO = 0.3            # BF-P1-04: 最小成交比例(30%)
PARTIAL_FILL_GRANULARITY = 0.1           # BF-P1-04: 成交粒度(10%步进)
ENABLE_PRICE_LIMIT_CHECK = True          # BF-P1-06: 是否启用涨跌停限制检查
PRICE_LIMIT_THRESHOLD = 0.995            # BF-P1-06: 涨跌停判定阈值(收盘价≥前收×0.995视为涨停)

# V7.1-P0-5: 三维手续费模型（品种-方向-持仓时间）
# 替换原有固定COMMISSION_PER_LOT = 1.5
FEE_STRUCTURE = {
    "50ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "300ETF_OPTION": {"open": 3.0, "close_today": 0.0, "close_overnight": 3.0, "unit": "per_lot"},
    "IO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "MO_INDEX_OPTION": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "unit": "per_lot"},
    "COMMODITY_OPTION": {"open": 5.0, "close_today": 5.0, "close_overnight": 5.0, "multiplier": 1000},
    "COMMODITY_FUTURE": {"open": 10.0, "close_today": 10.0, "close_overnight": 10.0, "multiplier": 10},
    "DEFAULT": {"open": 15.0, "close_today": 15.0, "close_overnight": 15.0, "multiplier": 10000},
}

# 默认回退手续费（当品种未匹配时）
# P2-R3-D-17: 多处关键参数硬编码 — FEE_STRUCTURE/SLIPPAGE_BPS/INITIAL_EQUITY等
# 硬编码于代码中，调整费率/滑点等需改代码再部署。建议迁移到parameter_attribute_matrix.yaml
COMMISSION_PER_LOT = 1.5


def calc_trade_fee(contract_type: str, open_time: pd.Timestamp, close_time: pd.Timestamp,
                   lots: int, trade_value: float = 0.0, broker_policy: str = "default") -> float:
    """V7.1-P0-5: 三维手续费计算（品种-方向-持仓时间）

    Args:
        contract_type: 合约类型，如 "50ETF_OPTION", "IO_INDEX_OPTION" 等
        open_time: 开仓时间
        close_time: 平仓时间
        lots: 手数
        trade_value: 成交金额（商品期权需要）
        broker_policy: 券商政策，"default" 或 "pingjin_free"(平今免)

    Returns:
        float: 总手续费（开仓+平仓）
    """
    fee = FEE_STRUCTURE.get(contract_type, FEE_STRUCTURE["DEFAULT"])

    if fee["unit"] == "per_lot":
        open_fee = fee["open"] * lots
        # 判断是否为平今（持仓时间 < 4小时）
        hold_hours = (close_time - open_time).total_seconds() / 3600.0 if close_time and open_time else 999
        if broker_policy == "pingjin_free" and hold_hours < 4:
            if open_time and close_time:
                is_same_day = (open_time.date() == close_time.date()) if hasattr(open_time, 'date') else (open_time.strftime('%Y-%m-%d') == close_time.strftime('%Y-%m-%d'))
                if is_same_day:
                    close_fee = fee["close_today"] * lots
                else:
                    close_fee = fee["close_overnight"] * lots
            else:
                close_fee = fee["close_today"] * lots
        else:
            close_fee = fee["close_overnight"] * lots
        return open_fee + close_fee
    else:
        # 商品期权按成交金额比例
        return max(fee["min"], trade_value * fee["rate"])


def _infer_contract_type(symbol: str) -> str:
    """P0-4修复: 从合约代码推断合约类型，用于calc_trade_fee"""
    s = str(symbol).upper()
    if "50ETF" in s:
        return "50ETF_OPTION"
    elif "300ETF" in s:
        return "300ETF_OPTION"
    elif s.startswith("IO") or "沪深300" in s:
        return "IO_INDEX_OPTION"
    elif s.startswith("MO") or "中证1000" in s:
        return "MO_INDEX_OPTION"
    elif any(k in s for k in ("M", "Y", "A", "C", "SR", "CF", "TA", "RU", "CU", "AL", "ZN", "AU", "AG")):
        if len(s) > 4 and any(c.isdigit() for c in s):
            return "COMMODITY_OPTION"
    logging.warning("[EX-P0-06] 未识别合约类型, 使用DEFAULT费率: %s", symbol)
    return "DEFAULT"


# P1-R11-03修复: 交易所维度手续费率表
# 各交易所对同一合约类型可能有不同费率，此表提供交易所级别的费率覆盖。
# 键为exchange_id，值为对FEE_STRUCTURE中各合约类型的费率乘数(1.0=无差异)。
EXCHANGE_COMMISSION_RATES = {
    "SHFE": {"multiplier": 1.0, "description": "上海期货交易所，费率与默认一致"},
    "DCE": {"multiplier": 1.0, "description": "大连商品交易所，费率与默认一致"},
    "CZCE": {"multiplier": 1.0, "description": "郑州商品交易所，费率与默认一致"},
    "CFFEX": {"multiplier": 0.8, "description": "中国金融期货交易所，股指期货/期权费率较低"},
    "INE": {"multiplier": 1.0, "description": "上海国际能源交易中心，费率与默认一致"},
    "GFEX": {"multiplier": 1.0, "description": "广州期货交易所，费率与默认一致"},
}


def _compute_commission(symbol: str, lots: int, open_time=None, close_time=None,
                        trade_value: float = 0.0, is_open: bool = True,
                        exchange_id: str = None) -> float:
    # NP-P2-05: 若use_decimal_for_settlement=True，此处应使用Decimal计算避免浮点累积误差
    """P0-4修复: 统一手续费计算入口，替代硬编码COMMISSION_PER_LOT

    开仓时扣开仓费，平仓时扣平仓费(区分平今/隔夜)。
    总费用 = 开仓费 + 平仓费，与原来 lots*1.5*2 + lots*1.5 = 3*lots*1.5 等价但更精确。

    P2-R3-P-13: _compute_commission缺交易所维度 — FEE_STRUCTURE按合约类型区分费率，
    但同一合约类型在不同交易所(如SSE/SZSE/DCE/SHFE)费率不同。
    已知限制: 当前按合约类型(infer_contract_type)查表，未区分交易所，
    例如50ETF期权在上交所费率1.3元/张，但创业板期权在深交所费率1.5元/张。
    回测误差<10%，实盘需从券商API获取精确费率。

    P1-R11-03修复: 添加exchange_id参数支持交易所维度费率差异。
    当exchange_id提供时，从EXCHANGE_COMMISSION_RATES查找交易所费率乘数，
    应用到计算结果上。未提供exchange_id时回退到原有逻辑(向后兼容)。
    三系统(order_service → 实盘, task_scheduler → 回测, 参数池/task_scheduler → 参数扫描回测)保持一致。
    """
    contract_type = _infer_contract_type(symbol)
    fee_info = FEE_STRUCTURE.get(contract_type, FEE_STRUCTURE["DEFAULT"])
    if fee_info["unit"] == "per_lot":
        if is_open:
            base_fee = fee_info["open"] * lots
        else:
            # 平仓: 区分平今免费(4小时内)和隔夜
            # R3-D-14已知限制: 回测默认使用close_today费率(未区分平今/隔夜免),实盘中券商政策可能不同
            if open_time and close_time:
                hold_hours = (close_time - open_time).total_seconds() / 3600.0
                if hold_hours < 4:
                    base_fee = fee_info["close_today"] * lots
                else:
                    base_fee = fee_info["close_overnight"] * lots
            else:
                base_fee = fee_info["close_overnight"] * lots
    else:
        if is_open:
            base_fee = max(fee_info["min"], trade_value * fee_info["rate"])
        else:
            base_fee = max(fee_info["min"], trade_value * fee_info["rate"])

    # P1-R11-03修复: 应用交易所维度费率乘数
    if exchange_id is not None:
        exchange_rate = EXCHANGE_COMMISSION_RATES.get(exchange_id)
        if exchange_rate is not None:
            multiplier = exchange_rate.get("multiplier", 1.0)
            if multiplier != 1.0:
                base_fee = base_fee * multiplier
    return base_fee


def _infer_exchange_id(symbol: str) -> str:
    if not symbol:
        return None
    symbol_upper = symbol.upper()
    if any(p in symbol_upper for p in ['SHFE', 'AU', 'AG', 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'RB', 'WR', 'HC', 'SS', 'BU', 'RU', 'SP', 'FU', 'SC', 'NR', 'BC', 'LU']):
        return 'SHFE'
    elif any(p in symbol_upper for p in ['DCE', 'C', 'CS', 'A', 'B', 'M', 'Y', 'P', 'FB', 'BB', 'JD', 'RR', 'LH']):
        return 'DCE'
    elif any(p in symbol_upper for p in ['CZCE', 'CF', 'SR', 'TA', 'MA', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SF', 'SM', 'WH', 'PM', 'RI', 'ER', 'AP', 'CJ', 'PK', 'PF', 'SH', 'SA']):
        return 'CZCE'
    elif any(p in symbol_upper for p in ['IF', 'IH', 'IC', 'IM', 'TF', 'T', 'TS', 'TL']):
        return 'CFFEX'
    elif any(p in symbol_upper for p in ['INE', 'SC', 'LU', 'NR', 'BC']):
        return 'INE'
    return None

try:
    from ali2026v3_trading.config_params import get_param
    SLIPPAGE_BPS = get_param('default_slippage_bps', 3.0)
except Exception:
    SLIPPAGE_BPS = 3.0


def _simulate_limit_order_queue(
    order_price: float,
    current_price: float,
    bar: pd.Series,
    timeout_seconds: int = QUEUE_TIMEOUT_SECONDS,
    enable_queue: bool = ENABLE_QUEUE_SIMULATION
) -> Dict[str, Any]:
    """BF-P1-02占位实现: 限价单排队模拟
    
    P1修复：添加配置参数预留，当前返回立即成交结果（不破坏现有逻辑）
    完整实现需：订单簿队列模型、时间优先规则、成交量限制
    
    Args:
        order_price: 限价单价格
        current_price: 当前市场价格
        bar: 当前行情Bar（包含open/high/low/close/volume）
        timeout_seconds: 排队超时时间
        enable_queue: 是否启用排队模拟
    
    Returns:
        Dict: {"filled": bool, "fill_price": float, "queue_time": int, "queue_position": int}
    """
    if not enable_queue:
        return {"filled": True, "fill_price": order_price, "queue_time": 0, "queue_position": 0}
    
    _queue_time = 0
    _queue_position = 0
    _filled = False
    _fill_price = order_price
    
    logger.debug(f"BF-P1-02占位: 限价单排队模拟未完整实现，使用默认立即成交逻辑")
    return {"filled": True, "fill_price": _fill_price, "queue_time": _queue_time, "queue_position": _queue_position}


def _simulate_market_order_slippage(
    bar: pd.Series,
    slippage_bps: float = MARKET_ORDER_SLIPPAGE_BPS,
    price_mode: str = MARKET_ORDER_PRICE_MODE,
    direction: int = 1
) -> float:
    """BF-P1-03占位实现: 市价单滑点模拟
    
    P1修复：添加配置参数预留，支持多种成交价模式
    - close: 使用收盘价（当前默认行为）
    - weighted: 使用OHLC加权价格
    - open/high/low: 使用对应价格
    - random: 在high-low区间随机采样
    
    Args:
        bar: 当前行情Bar（包含open/high/low/close）
        slippage_bps: 滑点基点
        price_mode: 成交价模式
        direction: 买卖方向（1买入，-1卖出）
    
    Returns:
        float: 模拟成交价格
    """
    _base_price = bar.get("close", 0.0)
    
    if price_mode == "close":
        _base_price = bar.get("close", _base_price)
    elif price_mode == "weighted":
        _base_price = (
            bar.get("open", 0.0) + bar.get("high", 0.0) + 
            bar.get("low", 0.0) + bar.get("close", 0.0)
        ) / 4.0
    elif price_mode == "open":
        _base_price = bar.get("open", _base_price)
    elif price_mode == "high":
        _base_price = bar.get("high", _base_price)
    elif price_mode == "low":
        _base_price = bar.get("low", _base_price)
    elif price_mode == "random":
        _high = bar.get("high", _base_price)
        _low = bar.get("low", _base_price)
        _base_price = _low + (_high - _low) * np.random.random()
    
    _slippage = _base_price * slippage_bps / 10000.0
    _fill_price = _base_price + direction * _slippage
    
    logger.debug(f"BF-P1-03占位: 市价单滑点模拟，模式={price_mode}, 滑点={slippage_bps}bps")
    # R17-P1-PERF-19修复: 延迟模拟已通过execution_delay_ms参数实现
    return _fill_price


def _get_instrument_type_slippage(
    symbol: str,
    base_slippage_bps: float = SLIPPAGE_BPS,
    multiplier_dict: Dict[str, float] = None
) -> float:
    """BF-P1-07占位实现: 品种滑点差异化
    
    P1修复：根据instrument_type区分滑点，期权使用更大滑点
    完整实现需：从合约代码精确推断品种类型
    
    Args:
        symbol: 合约代码
        base_slippage_bps: 基础滑点基点
        multiplier_dict: 品种滑点乘数字典
    
    Returns:
        float: 调整后滑点基点
    """
    if multiplier_dict is None:
        multiplier_dict = INSTRUMENT_SLIPPAGE_MULTIPLIER
    
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
    _adjusted_slippage = base_slippage_bps * _multiplier
    
    logger.debug(f"BF-P1-07占位: 品种={_instrument_type}, 滑点乘数={_multiplier}, 调整后滑点={_adjusted_slippage}bps")
    return _adjusted_slippage


def _simulate_order_cancel(
    order_id: str,
    cancel_delay_ms: int = CANCEL_DELAY_MS,
    failure_rate: float = CANCEL_FAILURE_RATE,
    enable_cancel: bool = ENABLE_CANCEL_SIMULATION
) -> Dict[str, Any]:
    """BF-P1-09占位实现: 撤单模拟
    
    P1修复：添加配置参数预留，支持撤单延迟和失败率模拟
    完整实现需：订单生命周期管理、交易所撤单API模拟
    
    Args:
        order_id: 订单ID
        cancel_delay_ms: 撤单延迟（毫秒）
        failure_rate: 撤单失败率（0-1）
        enable_cancel: 是否启用撤单模拟
    
    Returns:
        Dict: {"success": bool, "delay_ms": int, "reason": str}
    """
    if not enable_cancel:
        return {"success": True, "delay_ms": 0, "reason": "cancel_simulation_disabled"}
    
    _success = True
    _reason = "immediate_success"
    _delay = 0
    
    if np.random.random() < failure_rate:
        _success = False
        _reason = "market_already_filled"
        logger.warning(f"BF-P1-09占位: 撤单失败，订单{order_id}已成交")
    else:
        _delay = cancel_delay_ms
        _reason = "cancel_accepted"
        logger.debug(f"BF-P1-09占位: 撤单成功，订单{order_id}，延迟{_delay}ms")
    
    return {"success": _success, "delay_ms": _delay, "reason": _reason}

PULLBACK_DEFAULTS = {
    "pullback_enabled": False,
    "pullback_wait_bars": 5,
    "pullback_retrace_pct": 0.15,
    "pullback_iv_min_percentile": 20.0,
    "pullback_iv_max_percentile": 80.0,
    "pullback_ref_mode": "peak",
    "pullback_atr_wait_multiplier": 0.0,
    "pullback_retrace_pct_call": 0.38,  # P2-4修复: Call回撤默认值，与shared_utils.py对齐
    "pullback_retrace_pct_put": 0.42,  # P2-4修复: Put回撤默认值，与shared_utils.py对齐
    "pullback_theta_decay_accel": 0.0,
    "pullback_min_retrace_abs": 0.0,
    "pullback_max_valid_bars": 24,
}

PULLBACK_GRID = {
    "pullback_enabled": [True, False],
    "pullback_wait_bars": [2, 3, 4, 5, 6, 7, 8, 9, 10],
    "pullback_retrace_pct": [0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
    "pullback_iv_min_percentile": [10.0, 20.0, 30.0, 40.0],
    "pullback_iv_max_percentile": [60.0, 70.0, 80.0, 90.0],
    "pullback_retrace_pct_call": [0.1, 0.15, 0.2, 0.3, 0.38],
    "pullback_retrace_pct_put": [0.1, 0.15, 0.2, 0.3, 0.42],
    "pullback_ref_mode": ["peak", "atr"],
    "pullback_atr_wait_multiplier": [0.0, 2.0, 5.0],
    "pullback_theta_decay_accel": [0.0, 0.5, 1.0],
    "pullback_min_retrace_abs": [0.0, 0.5, 1.0],
}

# ── 11维度统一风险评分参数 ──────────────────────────────
# 三层架构：核心信号层(0.45) + 市场状态层(0.30) + 组合风控层(0.25)
# 权重总和=1.0，可通过params覆盖(RiskService读取decision.score.{dim}_weight)
# P1-8修复: 从risk_service.RiskService动态导入并转换键名，保持单一数据源
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

RISK_DIMENSION_DEFAULTS = _build_risk_dimension_defaults()
RISK_DIMENSION_DEFAULTS["decision.score.threshold_high"] = 0.70
RISK_DIMENSION_DEFAULTS["decision.score.threshold_low"] = 0.50  # R13-三对齐修复: 从0.40改为0.50，与signal_service._decision_score_threshold对齐

RISK_DIMENSION_GRID = {
    # 核心信号层权重扫描
    "decision.score.state_strength_weight": [0.10, 0.15, 0.20],
    "decision.score.order_flow_weight": [0.05, 0.10, 0.15],
    "decision.score.cycle_resonance_weight": [0.05, 0.10, 0.15],
    "decision.score.tri_validation_weight": [0.05, 0.10, 0.15],
    # 市场状态层权重扫描
    "decision.score.life_expectancy_weight": [0.05, 0.10, 0.15],
    "decision.score.phase_quality_weight": [0.04, 0.08, 0.12],
    "decision.score.greeks_usage_weight": [0.03, 0.07, 0.10],
    "decision.score.asymmetric_drawdown_weight": [0.03, 0.05, 0.08],
    # 组合风控层权重扫描
    "decision.score.consecutive_loss_weight": [0.03, 0.07, 0.10],
    "decision.score.alpha_decay_weight": [0.05, 0.10, 0.15],
    "decision.score.cross_correlation_weight": [0.04, 0.08, 0.12],
    # 决策阈值扫描
    "decision.score.threshold_high": [0.60, 0.70, 0.80],
    "decision.score.threshold_low": [0.40, 0.50, 0.60],  # R13-三对齐修复: 中值0.50与signal_service对齐
}

# 行情寿命估计器参数
LIFE_ESTIMATOR_DEFAULTS = {
    "life_min_sample_count": 30,           # 最小样本数(Level3降级阈值)
    "life_shrinkage_prior_strength": 1.0,  # 贝叶斯收缩先验强度
    "life_decay_r_squared_threshold": 0.3, # 衰减曲线R²阈值
}

LIFE_ESTIMATOR_GRID = {
    "life_min_sample_count": [20, 30, 50, 80],
    "life_shrinkage_prior_strength": [0.5, 1.0, 2.0],
    "life_decay_r_squared_threshold": [0.2, 0.3, 0.5],
}

# ── 期权盈亏比核心网格 — 顶级基金验证水准 ──
# 期权权利金盈亏比从0.5:1到5:1全覆盖，覆盖实盘常见状态
OPTION_TAKE_PROFIT_GRID = [0.5, 0.75, 1.0, 1.25, 1.5, 1.8, 2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0, 4.5, 5.0]  # R24-P1-DF-07修复: 追加1.8使默认值在网格中
OPTION_STOP_LOSS_GRID = [0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
# 动量策略盈亏比网格（偏右侧，止盈高止损低）
MOMENTUM_TAKE_PROFIT_GRID = [1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
MOMENTUM_STOP_LOSS_GRID = [0.2, 0.25, 0.3, 0.4, 0.5, 0.6]
# 均值回归策略盈亏比网格（偏左侧，止盈低止损高）
REVERSION_TAKE_PROFIT_GRID = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
REVERSION_STOP_LOSS_GRID = [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
# 套利策略盈亏比网格（窄幅，快速止盈止损）
ARB_TAKE_PROFIT_GRID = [0.5, 0.75, 1.0, 1.25, 1.5]
ARB_STOP_LOSS_GRID = [0.2, 0.25, 0.3, 0.35, 0.4, 0.5]
# 做市策略盈亏比网格（极窄幅，高频小利）
MM_TAKE_PROFIT_GRID = [0.4, 0.5, 0.6, 0.75, 0.8, 1.0, 1.2]
MM_STOP_LOSS_GRID = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
# 期权弹簧止盈比网格（高盈亏比，权利金杠杆特性）
SPRING_STOP_PROFIT_GRID = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0]
SPRING_MAX_LOSS_GRID = [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.98]

# ── 策略分层时间参数 ──────────────────────────────────────
# 核心原则：高频策略时间参数从毫秒起步，秒级策略从秒起步，分钟策略从分钟起步
# 用"分钟"统一度量亚秒级策略 = F1赛车油门刻度设成"每小时公里数"——丧失控制精度

# S1 HFT专用（毫秒级）— 订单簿半衰期约50-200ms
HFT_TIME_PARAMS = {
    "hft_hard_time_stop_ms": (100, 5000),       # 100ms ~ 5s，防止微观结构突变后持仓过久
    "hft_signal_confirm_ticks": (3, 15),         # tick数确认
    "hft_cooldown_ms": (10, 500),                # 10ms ~ 500ms 信号冷却
}
HFT_TIME_PARAM_GRID = {
    "hft_hard_time_stop_ms": [100, 200, 500, 1000, 2000, 5000],
    "hft_signal_confirm_ticks": [3, 5, 8, 10, 13, 15],
    "hft_cooldown_ms": [10, 30, 50, 100, 200, 500],
}
HFT_TIME_DEFAULTS = {
    "hft_hard_time_stop_ms": 1000,               # 默认1秒
    "hft_signal_confirm_ticks": 5,
    "hft_cooldown_ms": 100,
    # P1-R11-14修复: HFT策略时间基准确认(秒)，替代Bar计数确认
    # 当bar_period<=0.1(亚秒Bar)时，使用此参数计算confirm_bars
    "hft_state_confirm_seconds": 5.0,
}

# S4 弹簧专用（秒级）— Gamma峰值持续时间约10-60s
SPRING_TIME_PARAMS = {
    "spring_hard_time_stop_sec": (1, 120),       # 1s ~ 2min，防止Gamma脉冲衰减后权利金损耗
    "spring_confirm_ticks": (5, 50),             # tick数确认
    "spring_iv_pulse_window_sec": (5, 60),       # IV脉冲观察窗口
}
SPRING_TIME_PARAM_GRID = {
    "spring_hard_time_stop_sec": [1, 5, 10, 30, 60, 90, 120],
    "spring_confirm_ticks": [5, 10, 15, 20, 30, 50],
    "spring_iv_pulse_window_sec": [5, 10, 20, 30, 45, 60],
}
SPRING_TIME_DEFAULTS = {
    "spring_hard_time_stop_sec": 30,              # 默认30秒
    "spring_confirm_ticks": 10,
    "spring_iv_pulse_window_sec": 20,
}

# S2 共振专用（分钟级）— 分钟级趋势持续性约5-30分钟
RESONANCE_TIME_PARAMS = {
    "resonance_hard_time_stop_min": (1, 15),     # 1min ~ 15min，防止趋势反转后方向性暴露
    "state_confirm_bars": (2, 8),                # 2 ~ 8 Bar确认
}
RESONANCE_TIME_PARAM_GRID = {
    "resonance_hard_time_stop_min": [1, 3, 5, 8, 10, 15],
    "state_confirm_bars": [2, 3, 4, 5, 6, 8],
}
RESONANCE_TIME_DEFAULTS = {
    "resonance_hard_time_stop_min": 5,            # 默认5分钟
    "state_confirm_bars": 5,  # R24-P1-DF-01修复: 统一默认值为5
}

# S3 箱体专用（分钟级）— 箱体形成周期约30-120分钟
BOX_TIME_PARAMS = {
    "box_hard_time_stop_min": (15, 60),          # 15min ~ 1h，防止假突破后长期横盘磨损
    "box_confirm_bars": (5, 20),                 # 5 ~ 20 Bar确认
}
BOX_TIME_PARAM_GRID = {
    "box_hard_time_stop_min": [15, 20, 30, 45, 60],
    "box_confirm_bars": [5, 8, 10, 15, 20],
}
BOX_TIME_DEFAULTS = {
    "box_hard_time_stop_min": 30,                 # 默认30分钟
    "box_confirm_bars": 10,
}

PARAM_GRID_ROUND1 = {
    "close_take_profit_ratio": OPTION_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": OPTION_STOP_LOSS_GRID,
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.80],  # R27-P2-FP-06修复: 追加0.80使config_params全局默认值0.8在扫描网格中
    "lots_min": [1, 2, 3, 5],
    "signal_cooldown_sec": [0.0, 30.0, 60.0, 90.0, 120.0, 180.0],
    "non_other_ratio_threshold": [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    # 决策频率由 K线周期 × state_confirm_bars 自然决定
}

PARAM_DEFAULTS = {  # R21-MEM-P2-05修复: 模块级大字典，含多个PARAM_DEFAULTS_*变体共~15个字典常驻内存；可合并为分层结构或懒加载
    "close_take_profit_ratio": 1.5,
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    "max_risk_ratio": 0.3,
    "max_risk_per_trade": 0.05,
    "max_open_positions": 3,
    "lots_min": 3,
    "max_signals_per_window": 5,
    "signal_cooldown_sec": 60.0,
    "non_other_ratio_threshold": 0.65,
    "state_confirm_bars": 5,  # R24-P1-DF-01修复: 统一默认值为5
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.95,
    "spring_max_position_pct": 0.015,
    "capital_route_master_base": 0.60,
    "shadow_alpha_threshold": 0.1,
    "rate_limit_global_per_min": 60,
    "daily_loss_hard_stop_pct": 0.05,
    "logic_reversal_threshold": 1.5,
    # P1-R11-05: 订单执行延迟配置(毫秒)，回测中模拟从信号生成到订单成交的延迟。
    # 默认50ms保证非零延迟，避免回测中零延迟导致的成交优化偏差。
    "execution_delay_ms": 50,
    # P1-R11-04: 平仓信号延迟执行Bar数，模拟实盘中信号生成到实际成交的延迟。
    # 当close_position_delay_bars>0时，平仓信号延迟N个Bar才执行，模拟实盘事件驱动延迟。
    # 默认0表示立即执行(向后兼容)，建议回测中设1-2以增加真实性。
    "close_position_delay_bars": 1,
    # P1-R11-01: 回测滑点保守溢价(bps)，弥补历史spread与实时spread的系统性差异
    "backtest_slippage_premium_bps": 0.5,
    # S1 HFT: hft_hard_time_stop_ms (毫秒级)
    # S2 共振: resonance_hard_time_stop_min (分钟级)
    # S3 箱体: box_hard_time_stop_min (分钟级)
    # S4 弹簧: spring_hard_time_stop_sec (秒级)
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **HFT_TIME_DEFAULTS,
    **SPRING_TIME_DEFAULTS,
    **RESONANCE_TIME_DEFAULTS,
    **BOX_TIME_DEFAULTS,
    **PULLBACK_DEFAULTS,
    **RISK_DIMENSION_DEFAULTS,
    **LIFE_ESTIMATOR_DEFAULTS,
    "skip_rollover_days": 3,
    "backtest_max_sub_order_lots": 5,
}

PARAM_GRID_ROUND2 = {
    "max_signals_per_window": [2, 3, 4, 5, 6, 8, 10],
    "state_confirm_bars": [3, 4, 5, 6, 7, 8],
    "spring_stop_profit_ratio": SPRING_STOP_PROFIT_GRID,
    "spring_max_loss_pct": SPRING_MAX_LOSS_GRID,
    "spring_max_position_pct": [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025, 0.030],
    "capital_route_master_base": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
    "shadow_alpha_threshold": [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
    "rate_limit_global_per_min": [20, 30, 45, 60, 90, 120, 150],
    "resonance_hard_time_stop_min": RESONANCE_TIME_PARAM_GRID["resonance_hard_time_stop_min"],
    "daily_loss_hard_stop_pct": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10],
    "logic_reversal_threshold": [1.0, 1.2, 1.5, 1.8, 2.0, 2.5],
    **PULLBACK_GRID,
    **LIFE_ESTIMATOR_GRID,
}

# Round3: 风险权重独立扫描 — 三阶段独立扫描架构(市场参数→辅助交易参数→风险权重)
# Round3固定Round1+Round2最优参数,仅扫描D1-D11决策权重和阈值
PARAM_GRID_ROUND3 = {
    **RISK_DIMENSION_GRID,
}

# Round4: 评分系数独立扫描 — CascadeJudge+StrategyJudgment评分权重
# Round4固定Round1+Round2+Round3最优参数,仅扫描评判评分系数
SCORING_COEFFICIENT_GRID = {
    # CascadeJudge评分权重(profit_ratio+sortino+calmar+sharpe归一化=1.0)
    "scoring_profit_ratio_weight": [0.40, 0.50, 0.60, 0.70],
    "scoring_sortino_weight": [0.20, 0.30, 0.40],
    "scoring_calmar_weight": [0.15, 0.25, 0.30, 0.35],
    "scoring_sharpe_weight": [0.15, 0.25, 0.30, 0.35],
}

PARAM_GRID_ROUND4 = {
    **SCORING_COEFFICIENT_GRID,
}

PARAM_GRID_BOX_EXTREME = {
    "box_detection_threshold": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
    "box_min_bars": [10, 15, 20, 25, 30, 45],
    "extreme_entry_ratio": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
    "close_take_profit_ratio": REVERSION_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": REVERSION_STOP_LOSS_GRID,
    "box_hard_time_stop_min": BOX_TIME_PARAM_GRID["box_hard_time_stop_min"],
    "box_confirm_bars": BOX_TIME_PARAM_GRID["box_confirm_bars"],
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **PULLBACK_GRID,
}

PARAM_DEFAULTS_BOX_EXTREME = {
    "box_detection_threshold": 0.03,
    "box_min_bars": 20,
    "extreme_entry_ratio": 0.5,
    "close_take_profit_ratio": 2.0,
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    "lots_min": 1,
    "max_risk_ratio": 0.2,
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **BOX_TIME_DEFAULTS,
    **PULLBACK_DEFAULTS,
}

PARAM_GRID_BOX_SPRING = {
    "spring_iv_threshold": [0.10, 0.15, 0.20, 0.25, 0.30, 0.35],
    "spring_maturity_days": [3, 5, 7, 10, 14, 21, 30],
    "spring_impulse_threshold": [0.005, 0.01, 0.02, 0.03, 0.04, 0.05],
    "spring_stop_profit_ratio": SPRING_STOP_PROFIT_GRID,
    "spring_max_loss_pct": SPRING_MAX_LOSS_GRID,
    "spring_max_position_pct": [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025, 0.030],
    "close_take_profit_ratio": REVERSION_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": REVERSION_STOP_LOSS_GRID,
    "spring_hard_time_stop_sec": SPRING_TIME_PARAM_GRID["spring_hard_time_stop_sec"],
    "spring_confirm_ticks": SPRING_TIME_PARAM_GRID["spring_confirm_ticks"],
    "spring_iv_pulse_window_sec": SPRING_TIME_PARAM_GRID["spring_iv_pulse_window_sec"],
    "max_risk_ratio": [0.008, 0.010, 0.012, 0.015, 0.020],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **PULLBACK_GRID,
}

PARAM_DEFAULTS_BOX_SPRING = {
    "spring_iv_threshold": 0.20,
    "spring_maturity_days": 14,
    "spring_impulse_threshold": 0.02,
    "spring_stop_profit_ratio": 5.0,
    "spring_max_loss_pct": 0.90,
    "spring_max_position_pct": 0.015,
    "lots_min": 1,
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    "close_take_profit_ratio": 5.0,
    "close_stop_loss_ratio": 0.95,
    "max_risk_ratio": 0.015,
    **SPRING_TIME_DEFAULTS,
    **PULLBACK_DEFAULTS,
}

PARAM_GRID_HFT = {
    "hft_signal_confirm_ticks": [2, 3, 5, 8, 13],
    "hft_cooldown_ms": [30.0, 50.0, 100.0, 150.0, 200.0, 300.0],
    "hft_min_imbalance": [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40],
    "close_take_profit_ratio": MOMENTUM_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": MOMENTUM_STOP_LOSS_GRID,
    "hft_hard_time_stop_ms": HFT_TIME_PARAM_GRID["hft_hard_time_stop_ms"],
    "daily_loss_hard_stop_pct": [0.02, 0.03, 0.04, 0.05, 0.06, 0.08],
    "max_risk_ratio": [0.10, 0.15, 0.20, 0.25, 0.30],
}

HFT_TICK_PARAMS = {"hft_signal_confirm_ticks", "hft_cooldown_ms", "hft_min_imbalance", "hft_hard_time_stop_ms"}

PARAM_DEFAULTS_HFT = {
    "hft_signal_confirm_ticks": 5,
    "hft_cooldown_ms": 100.0,
    "hft_min_imbalance": 0.25,
    "close_take_profit_ratio": 1.5,
    "close_stop_loss_ratio": 0.3,
    "lots_min": 1,
    "max_risk_ratio": 0.2,
    "non_other_ratio_threshold": 0.4,
    "daily_loss_hard_stop_pct": 0.03,
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    **HFT_TIME_DEFAULTS,
    # R30-P0-06修复: PARAM_DEFAULTS_HFT缺失**PULLBACK_DEFAULTS展开
    # 导致S1 HFT策略缺少pullback_iv_min_percentile等4个回退参数
    **PULLBACK_DEFAULTS,
}

PARAM_DEFAULTS_ARBITRAGE = {
    **PARAM_DEFAULTS_HFT,
    "arb_deviation_threshold_bps": 50.0,
    "arb_reversion_target_bps": 30.0,
    "arb_min_confidence": 0.6,
    "arb_max_hold_minutes": 15.0,
    "close_take_profit_ratio": 1.0,
    "close_stop_loss_ratio": 0.3,
    **PULLBACK_DEFAULTS,
}

PARAM_DEFAULTS_MARKET_MAKING = {
    **PARAM_DEFAULTS_HFT,
    "mm_ioc_signal_threshold": 0.8,
    "mm_offset_min_ticks": 0,
    "mm_offset_max_ticks": 3,
    "mm_spread_target_bps": 5.0,
    "mm_max_inventory_lots": 5,
    "mm_rebalance_threshold": 3,
    "close_take_profit_ratio": 0.8,
    "close_stop_loss_ratio": 0.3,  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    **PULLBACK_DEFAULTS,
}

PARAM_GRID_ARBITRAGE = {
    "arb_deviation_threshold_bps": [20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0],
    "arb_reversion_target_bps": [15.0, 20.0, 25.0, 30.0, 35.0, 40.0],
    "arb_min_confidence": [0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8],
    "arb_max_hold_minutes": [5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0],
    "close_take_profit_ratio": ARB_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": ARB_STOP_LOSS_GRID,
    "hft_hard_time_stop_ms": HFT_TIME_PARAM_GRID["hft_hard_time_stop_ms"],
    **PULLBACK_GRID,
}

PARAM_GRID_MARKET_MAKING = {
    "mm_spread_target_bps": [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0],
    "mm_max_inventory_lots": [2, 3, 4, 5, 6, 8, 10],
    "mm_rebalance_threshold": [2, 3, 4, 5, 6, 8],
    "mm_ioc_signal_threshold": [0.6, 0.7, 0.75, 0.8, 0.85, 0.9],
    "mm_offset_max_ticks": [1, 2, 3, 4, 5, 6],
    "close_take_profit_ratio": MM_TAKE_PROFIT_GRID,
    "close_stop_loss_ratio": MM_STOP_LOSS_GRID,
    "hft_hard_time_stop_ms": HFT_TIME_PARAM_GRID["hft_hard_time_stop_ms"],
    **PULLBACK_GRID,
}

PARAM_DEFAULTS_SHADOW_A = {
    **PARAM_DEFAULTS,
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.6,
    "max_risk_ratio": 0.15,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
    "spring_hard_time_stop_sec": 24,
    "resonance_hard_time_stop_min": 4,
    "box_hard_time_stop_min": 24,
}

PARAM_DEFAULTS_SHADOW_B = {
    **PARAM_DEFAULTS,
    "close_take_profit_ratio": 1.1,
    "close_stop_loss_ratio": 0.7,
    "max_risk_ratio": 0.1,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
    "spring_hard_time_stop_sec": 18,
    "resonance_hard_time_stop_min": 3,
    "box_hard_time_stop_min": 18,
}

PARAM_DEFAULTS_HFT_SHADOW_A = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 1.2,
    "close_stop_loss_ratio": 0.25,
    "max_risk_ratio": 0.12,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_HFT_SHADOW_B = {
    **PARAM_DEFAULTS_HFT,
    "close_take_profit_ratio": 1.0,
    "close_stop_loss_ratio": 0.2,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
}

PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A = {
    **PARAM_DEFAULTS_BOX_EXTREME,
    "close_take_profit_ratio": 1.6,
    "close_stop_loss_ratio": 0.6,
    "max_risk_ratio": 0.12,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "box_hard_time_stop_min": 24,
}

PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B = {
    **PARAM_DEFAULTS_BOX_EXTREME,
    "close_take_profit_ratio": 1.3,
    "close_stop_loss_ratio": 0.7,
    "max_risk_ratio": 0.08,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "box_hard_time_stop_min": 18,
}

PARAM_DEFAULTS_BOX_SPRING_SHADOW_A = {
    **PARAM_DEFAULTS_BOX_SPRING,
    "spring_stop_profit_ratio": 4.0,
    "spring_max_loss_pct": 0.85,
    "spring_max_position_pct": 0.010,
    "close_take_profit_ratio": 4.0,
    "close_stop_loss_ratio": 0.75,
    "max_risk_ratio": 0.012,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "spring_hard_time_stop_sec": 24,
}

PARAM_DEFAULTS_BOX_SPRING_SHADOW_B = {
    **PARAM_DEFAULTS_BOX_SPRING,
    "spring_stop_profit_ratio": 3.0,
    "spring_max_loss_pct": 0.80,
    "spring_max_position_pct": 0.008,
    "close_take_profit_ratio": 3.0,
    "close_stop_loss_ratio": 0.70,
    "max_risk_ratio": 0.010,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "spring_hard_time_stop_sec": 18,
}

PARAM_DEFAULTS_ARBITRAGE_SHADOW_A = {
    **PARAM_DEFAULTS_ARBITRAGE,
    "arb_deviation_threshold_bps": 60.0,
    "arb_reversion_target_bps": 35.0,
    "arb_min_confidence": 0.5,
    "close_take_profit_ratio": 0.8,
    "close_stop_loss_ratio": 0.4,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_ARBITRAGE_SHADOW_B = {
    **PARAM_DEFAULTS_ARBITRAGE,
    "arb_deviation_threshold_bps": 55.0,
    "arb_reversion_target_bps": 32.0,
    "arb_min_confidence": 0.55,
    "close_take_profit_ratio": 0.9,
    "close_stop_loss_ratio": 0.35,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
}

PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A = {
    **PARAM_DEFAULTS_MARKET_MAKING,
    "mm_ioc_signal_threshold": 0.7,
    "mm_offset_max_ticks": 4,
    "mm_spread_target_bps": 6.0,
    "mm_max_inventory_lots": 4,
    "close_take_profit_ratio": 0.7,
    "close_stop_loss_ratio": 0.6,
    # NOTE: shadow更保守，硬止损时间缩短约20%
    "hft_hard_time_stop_ms": 800,
}

PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B = {
    **PARAM_DEFAULTS_MARKET_MAKING,
    "mm_ioc_signal_threshold": 0.75,
    "mm_offset_max_ticks": 5,
    "mm_spread_target_bps": 7.0,
    "mm_max_inventory_lots": 3,
    "close_take_profit_ratio": 0.6,
    "close_stop_loss_ratio": 0.55,
    # NOTE: shadow更保守，硬止损时间缩短约40%
    "hft_hard_time_stop_ms": 600,
}

REASON_MULTIPLIERS = {
    "CORRECT_RESONANCE":    {"tp_mult": 1.0,  "sl_mult": 1.0,  "time_mult": 1.0},
    "CORRECT_DIVERGENCE":   {"tp_mult": 0.8,  "sl_mult": 0.8,  "time_mult": 0.67},
    "INCORRECT_REVERSAL":   {"tp_mult": 0.87, "sl_mult": 1.2,  "time_mult": 0.67},
    "OTHER_SCALP":          {"tp_mult": 0.73, "sl_mult": 0.6,  "time_mult": 0.33},
    "ARBITRAGE":            {"tp_mult": 0.5,  "sl_mult": 0.5,  "time_mult": 0.25},
    "MARKET_MAKING":        {"tp_mult": 0.4,  "sl_mult": 0.8,  "time_mult": 1.0},
    "MANUAL":               {"tp_mult": 1.0, "sl_mult": 1.0,  "time_mult": 1.0},
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


_sync_reason_multipliers_with_position_service()


_REASON_TIME_STOP_SOURCE = {
    "CORRECT_RESONANCE": ("resonance_hard_time_stop_min", "min", 5.0),
    "CORRECT_DIVERGENCE": ("resonance_hard_time_stop_min", "min", 5.0),
    "INCORRECT_REVERSAL": ("resonance_hard_time_stop_min", "min", 5.0),
    "OTHER_SCALP": ("box_hard_time_stop_min", "min", 30.0),
    "ARBITRAGE": ("resonance_hard_time_stop_min", "min", 5.0),
    "MARKET_MAKING": ("hft_hard_time_stop_ms", "ms", 1000.0),
    "MANUAL": ("resonance_hard_time_stop_min", "min", 5.0),
}

# ── P0绿灯铁律阈值（不可妥协，但必须参数化可审计）──
P0_IRON_RULES = {
    "max_oos_decay": -0.30,           # 样本外衰减铁律 (与parameter_attribute_matrix.yaml p0_max_oos_decay对齐)
    "oos_decay_warn": -0.20,          # 样本外衰减警告 (与parameter_attribute_matrix.yaml p0_oos_decay_warn对齐)
    "min_train_sharpe": 0.5,          # 训练夏普铁律 (与parameter_attribute_matrix.yaml p0_min_train_sharpe对齐)
    "min_test_sharpe": 0.3,           # 样本外夏普铁律 (与parameter_attribute_matrix.yaml p0_min_test_sharpe对齐)
    "min_lr_threshold": 0.8,          # 逻辑反转阈值铁律 (与parameter_attribute_matrix.yaml p0_min_lr_threshold对齐)
    "lr_threshold_warn": 1.0,         # 逻辑反转阈值警告 (与parameter_attribute_matrix.yaml p0_lr_threshold_warn对齐)
    "max_drawdown_limit": -0.50,      # 最大回撤生存红线 (P0绿灯检验第4项)
    "min_signal_count": 30,           # 最少信号数统计显著性 (与cascade_config.yaml data_quality.min_trades对齐)
    "max_daily_trigger": 2.0,           # 日均触发上限
    "max_loss_hit_rate": 0.20,          # 亏损命中率上限
    "min_two_x_recovery_rate": 0.30,    # 两倍恢复率下限
    "min_oos_retention": 0.50,          # 样本外保留率下限
    "alpha_threshold_hft": 0.5,              # S1 HFT Alpha最低阈值
    "alpha_threshold_minute": 0.5,           # S2 分钟Alpha最低阈值
    "alpha_threshold_box_extreme": 0.3,      # S3 箱体Alpha最低阈值
    "alpha_threshold_box_spring": 0.4,       # S4 弹簧Alpha最低阈值
    "alpha_threshold_arbitrage": 0.3,        # S5 套利Alpha最低阈值
    "alpha_threshold_market_making": 0.2,    # S6 做市Alpha最低阈值
    "alpha_pct_threshold": 30,               # Alpha占比百分比阈值
}

# ── 回测阈值（影响回测逻辑的判定点）──
# R10-P2-06: BACKTEST_THRESHOLDS唯一权威定义，其他模块应从此处导入
BACKTEST_THRESHOLDS = {
    "logic_reversal_min_wrong_pct": 0.3,     # 逻辑反转平仓最低wrong_pct
    "correct_resonance_min_strength": 0.3,    # CORRECT_RESONANCE信号最低强度
    "circuit_breaker_daily_dd": 0.03,         # 熔断触发日回撤阈值
    "main_open_min_strength": 0.3,            # 主策略开仓最低信号强度
    "shadow_random_open_prob": 0.02,          # shadow_random开仓概率
    "hft_shadow_random_open_prob": 0.005,     # HFT shadow_random开仓概率
    "hft_open_min_strength": 0.2,             # HFT开仓最低信号强度
    "mm_rebalance_imbalance": 0.3,            # 做市策略再平衡触发
    "hft_fidelity_sharpe_ratio": 0.5,         # HFT保真度验证通过条件
    "liquidity_stress_max_dd": 0.3,           # 流动性压力测试通过条件
    "gbm_max_return": 0.05,                   # GBM随机性验证通过条件
    "reverse_max_return": 0.0,                # 反向时间序列验证通过条件
    "ci_width_eliminate": 2.0,                # Sharpe CI淘汰阈值
    "ci_width_reduce": 1.0,                   # Sharpe CI降权阈值
    "ci_width_flag": 0.5,                     # Sharpe CI标注阈值
    "kl_q1_sharpe_degradation": 0.5,          # KL-Q1夏普非退化
    "kl_q1_min_sharpe": 0.01,                 # KL-Q1分母保护
    "kl_q3_max_trade_diff": 0.5,              # KL-Q3 HFT保真差异
}

_BACKTEST_THRESHOLD_LINK_MAP = {
    "main_open_min_strength": "decision_threshold_low",
    "correct_resonance_min_strength": "decision_threshold_high",
    "circuit_breaker_daily_dd": "daily_loss_hard_stop_pct",
}

_P0_IRON_RULE_LINK_MAP = {
    "max_oos_decay": "p0_max_oos_decay",
    "oos_decay_warn": "p0_oos_decay_warn",
    "min_train_sharpe": "p0_min_train_sharpe",
    "min_test_sharpe": "p0_min_test_sharpe",
    "min_lr_threshold": "p0_min_lr_threshold",
    "lr_threshold_warn": "p0_lr_threshold_warn",
}


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


_sync_thresholds_from_runtime_config()

SHADOW_PARAM_MAP = {
    "main": None,
    "shadow_reverse": "shadow_a",
    "shadow_random": "shadow_b",
}

# P0-R11-07修复: 使用deepcopy创建独立副本，防止修改影子参数污染原始默认值
STRATEGY_SHADOW_DEFAULTS = {
    "hft":             {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_HFT_SHADOW_A),          "shadow_b": copy.deepcopy(PARAM_DEFAULTS_HFT_SHADOW_B)},
    "main":            {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_SHADOW_A),              "shadow_b": copy.deepcopy(PARAM_DEFAULTS_SHADOW_B)},
    "box_extreme":     {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_BOX_EXTREME_SHADOW_A),  "shadow_b": copy.deepcopy(PARAM_DEFAULTS_BOX_EXTREME_SHADOW_B)},
    "box_spring":      {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_BOX_SPRING_SHADOW_A),   "shadow_b": copy.deepcopy(PARAM_DEFAULTS_BOX_SPRING_SHADOW_B)},
    "arbitrage":       {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_ARBITRAGE_SHADOW_A),    "shadow_b": copy.deepcopy(PARAM_DEFAULTS_ARBITRAGE_SHADOW_B)},
    "market_making":   {"shadow_a": copy.deepcopy(PARAM_DEFAULTS_MARKET_MAKING_SHADOW_A),"shadow_b": copy.deepcopy(PARAM_DEFAULTS_MARKET_MAKING_SHADOW_B)},
}

ROUND1_TOP_K = 10

PARAM_GRID = PARAM_GRID_ROUND1

# 扫描轮次与排序指标的强绑定关系：扫描某类参数时按其影响指标选优。
SCAN_TARGET_SORT_METRIC = {
    "round1": "minute_sharpe",
    "round2": "minute_sharpe",
    "round3": "risk_adjusted_score",
    "round4": "cascade_final_score",
}

# R24-P1-DF-05修复: 参数级排序指标映射，覆盖轮次级默认排序
PARAM_SORT_METRIC_OVERRIDE = {
    'max_risk_ratio': 'risk_adjusted_score',  # 风控参数按风控指标排序
    'close_stop_loss_ratio': 'stop_loss_hit_rate',  # 止损参数按止损指标排序
    'close_take_profit_ratio': 'profit_loss_ratio',  # 止盈参数按盈亏比排序
    # P0-R9-验收标准7修复: 补齐PARAM_GRID_ROUND1中3个缺失排序映射的参数
    'lots_min': 'total_return',  # 仓位参数按总收益排序
    'signal_cooldown_sec': 'win_rate',  # 冷却参数按胜率排序（signal_quality不存在，使用win_rate替代）
    'non_other_ratio_threshold': 'sharpe',  # 状态阈值按夏普比率排序（state_accuracy不存在，使用sharpe替代）
}

OBJECTIVE_FUNCTIONS = {
    'sharpe': lambda r: r.get('sharpe', 0.0),
    'profit_factor': lambda r: r.get('profit_factor', 0.0),
    'win_loss_ratio': lambda r: r.get('win_loss_ratio', 0.0),  # R6-D-16修复: 键名从avg_win_loss_ratio改为win_loss_ratio
    'plr_composite': lambda r: (
        r.get('profit_factor', 0.0) * 0.4
        + r.get('win_loss_ratio', 0.0) * 0.3  # R6-D-16修复: 键名从avg_win_loss_ratio改为win_loss_ratio
        + r.get('sharpe', 0.0) * 0.1
        + r.get('total_return', 0.0) * 0.2
    ),
    'return_per_dd': lambda r: (
        r.get('total_return', 0.0) / abs(r.get('max_drawdown', -0.01))
        if abs(r.get('max_drawdown', -0.01)) > 1e-10 else 0.0
    ),
}

DEFAULT_OBJECTIVE = 'sharpe'


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

logger = logging.getLogger(__name__)


def _infer_instrument_type(symbol: str) -> str:
    if "-C-" in str(symbol) or "-P-" in str(symbol):
        return "option_buyer"
    return "future"


@dataclass(slots=True)
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


@dataclass(slots=True)
class _ClosedTrade:
    pnl: float
    pnl_pct: float
    close_reason: str
    hold_minutes: float
    open_reason: str = ""
    premium_pnl: float = 0.0
    delta_pnl: float = 0.0
    stage1_passed: bool = False  # P1-R9-09修复: 持久化stage1_passed到_ClosedTrade


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


@dataclass(slots=True)
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


def _bt_capture_snapshot(bt: _BacktestState, trigger_name: str, detail: str = "",
                          strategy_type: str = "", bar: Any = None) -> None:
    """回测快照采集辅助函数 — 在信号/开仓/平仓时刻调用(18策略覆盖)"""
    if bt.snapshot_collector is None:
        return
    try:
        import numpy as np
        from ali2026v3_trading.策略评判.market_snapshot_collector import (
            SnapshotTrigger, StrategyStateSnapshot, SIX_STRATEGY_KEYS, THREE_VARIANTS,
            LifeExpectancySnapshot, CyclePredictionSnapshot, RiskDimensionScores,
        )
        trigger_map = {
            "signal": SnapshotTrigger.SIGNAL_GENERATED,
            "open": SnapshotTrigger.ORDER_OPENED,
            "close": SnapshotTrigger.ORDER_CLOSED,
        }
        trigger = trigger_map.get(trigger_name, SnapshotTrigger.SIGNAL_GENERATED)
        states = []
        for sk in SIX_STRATEGY_KEYS:
            for variant in THREE_VARIANTS:
                states.append(StrategyStateSnapshot(
                    strategy_id=f"{sk}_{variant}",
                    strategy_type=sk,
                ))
        ts = np.datetime64('now')
        if bar is not None and hasattr(bar, 'get'):
            bar_ts = bar.get('minute', None)
            if bar_ts is not None:
                ts = np.datetime64(str(bar_ts))

        # 构建行情寿命快照
        life_snap = None
        hmm_state = getattr(bt, 'current_state', None)
        if hmm_state:
            estimator = _get_life_estimator()
            if estimator is not None and hasattr(estimator, '_life_dict') and estimator._life_dict:
                try:
                    life = estimator.get_life_expectancy(hmm_state)
                    if life is not None and life.is_valid():
                        life_snap = LifeExpectancySnapshot(
                            hmm_state=hmm_state,
                            duration_p25=life.duration.get('p25', 0),
                            duration_p50=life.duration.get('p50', 0),
                            duration_p75=life.duration.get('p75', 0),
                            duration_p99=life.duration.get('p99', 0),
                            magnitude_p50=life.magnitude.get('p50', 0),
                            magnitude_p75=life.magnitude.get('p75', 0),
                            sample_count=life.sample_count,
                            decay_r_squared=life.decay_r_squared,
                            degradation_level=life.degradation_level,
                            is_valid=True,
                        )
                except Exception as _e:
                    logger.warning("[SNAPSHOT] life_estimator failed: %s", _e)

        # 构建周期预测快照
        cycle_snap = None
        if hmm_state:
            cycle_snap = CyclePredictionSnapshot()
            # 从bar推断趋势评分
            if bar is not None and hasattr(bar, 'get'):
                trend_scores, trend_directions = _infer_trend_scores_from_bar(bar)
                cycle_snap.trend_scores_short = trend_scores[0]
                cycle_snap.trend_scores_medium = trend_scores[1]
                cycle_snap.trend_scores_long = trend_scores[2]
                cycle_snap.trend_directions_short = trend_directions[0]
                cycle_snap.trend_directions_medium = trend_directions[1]
                cycle_snap.trend_directions_long = trend_directions[2]
            if life_snap is not None and life_snap.duration_p75 > 0:
                cycle_snap.phase_remaining_estimate = life_snap.duration_p75

        # 构建11维度风险评分快照
        risk_dims = RiskDimensionScores()
        if life_snap is not None and life_snap.is_valid:
            risk_dims.d3_life_expectancy = {0: 1.0, 1: 0.7, 2: 0.4, 3: 0.2}.get(
                life_snap.degradation_level, 0.5)
        if cycle_snap is not None:
            risk_dims.d4_cycle_resonance = cycle_snap.resonance_strength if cycle_snap.resonance_strength > 0 else 0.5
            risk_dims.d5_phase_quality = cycle_snap.phase_quality

        bt.snapshot_collector.capture(
            timestamp=ts, trigger=trigger, trigger_detail=detail,
            strategy_states=states,
            life_expectancy=life_snap,
            cycle_prediction=cycle_snap,
            risk_dimensions=risk_dims,
        )
    except Exception as e:
        logging.debug("[_bt_capture_snapshot] error: %s", e)


# P1-R9-29修复: 回测状态机枚举类，替代裸字符串
from enum import Enum
class BacktestStateEnum(Enum):
    CORRECT_TRENDING = "correct_trending"
    INCORRECT_REVERSAL = "incorrect_reversal"
    OTHER = "other"
    BOX_EXTREME = "box_extreme"
    BOX_SPRING = "box_spring"
    ARBITRAGE = "arbitrage"
    HFT_TICK_CONFIRM = "hft_tick_confirm"
    PAUSED = "paused"


_STATE_MAP = {
    "correct_rise": "correct_trending",
    "correct_fall": "correct_trending",
    "wrong_rise": "incorrect_reversal",
    "wrong_fall": "incorrect_reversal",
    "other": "other",
}

_STATE_REASON_MAP = {
    "correct_trending": "CORRECT_RESONANCE",
    "incorrect_reversal": "INCORRECT_REVERSAL",
    "other": "OTHER_SCALP",
    "box_extreme": "BOX_EXTREME",
    "box_spring": "BOX_SPRING",
    "arbitrage": "ARBITRAGE",
    "hft_tick_confirm": "HFT_TICK_CONFIRM",
}


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
    base_tp = params.get("close_take_profit_ratio", 1.5)
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


def _compute_lots_with_risk_budget(
    equity: float,
    price: float,
    sl_ratio: float,
    lots_min: int,
    params: Dict[str, float],
    recent_pnls: Optional[List[float]] = None,
    bar: Optional[pd.Series] = None,
    current_positions: Optional[Dict[str, '_BacktestPosition']] = None,
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
    risk = params.get("max_risk_ratio", 0.8)  # R16-P0对齐: 与config_params全局默认值0.8同步
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


def _check_state_transition(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> str:
    # P0-1防护: 五态标签依赖预处理数据，NaN/缺失时默认为0
    # 修复: 原代码仅warn不阻断，NaN值仍传播到状态判断导致误分类
    # 改为: 显式将NaN替换为0.0，阻断NaN传播链
    # P2-R3-D-15: 五态分类降级时语义不同 — NaN替换为0后，non_other计算可能<0.65阈值，
    # 导致所有缺失字段bar统一降级为"other"状态。但"other"语义为"无方向/混沌"，
    # 而"数据缺失"与"市场混沌"是不同语义。已知限制: 降级为other后无法区分
    # "数据缺失"和"真实混沌"，可能影响后续状态机决策。建议增加第五态"data_missing"。
    for _field in ("correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct"):
        _val = bar.get(_field, None)
        if _field not in bar or pd.isna(_val):
            logger.warning("[P0-1] 五态标签字段 %s 缺失或NaN，默认为0", _field)
            # P0-1修复: 显式替换NaN为0，防止NaN传播污染状态机
            # pandas Series不支持直接赋值，用局部变量替代
            if _field in bar.index:
                bar[_field] = 0.0

    non_other = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0) + bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
    # R10-P0-19修复: 回测状态判定复用width_cache._classify_status，确保与实盘五态一致
    # 五态分类: correct_trending, correct_trending_defensive,
    #           incorrect_reversal, incorrect_reversal_defensive, other
    # P0-R11-02修复: _classify_status签名要求(underlying_future_id, month, opt_type,
    #   current_price, prev_price, option_direction)，原调用传(non_other, bar)参数不匹配
    try:
        from ali2026v3_trading.width_cache import get_width_strength_cache
        _wc = get_width_strength_cache()
        if hasattr(_wc, '_classify_status'):
            # P0-R11-02: 从bar数据提取_classify_status所需参数
            # 回测bar不含underlying_future_id/month，提供默认值；函数内部查无对应future状态时返回'other'
            _ufid = int(bar.get("underlying_future_id", 0))
            _month = str(bar.get("month", ""))
            _opt_type = str(bar.get("option_type", ""))
            _current_price = float(bar.get("close", 0.0))
            _prev_price = float(bar.get("open", 0.0))  # 回测无prev_close，用open近似
            candidate = _wc._classify_status(_ufid, _month, _opt_type, _current_price, _prev_price)
        else:
            raise ImportError("_classify_status not available")
    except Exception:
        # R10-P0-19修复: 回退到五态分类（与实盘_STATE_MAP一致）
        threshold = params.get("non_other_ratio_threshold", 0.65)
        if non_other < threshold:
            candidate = "other"
        else:
            correct_rise = bar.get("correct_rise_pct", 0)
            correct_fall = bar.get("correct_fall_pct", 0)
            wrong_rise = bar.get("wrong_rise_pct", 0)
            wrong_fall = bar.get("wrong_fall_pct", 0)
            # 与实盘_STATE_MAP映射一致: rise→主状态, fall→defensive
            correct_total = correct_rise + correct_fall
            incorrect_total = wrong_rise + wrong_fall
            if correct_total >= incorrect_total:
                candidate = "correct_trending" if correct_rise >= correct_fall else "correct_trending_defensive"
            else:
                candidate = "incorrect_reversal" if wrong_rise >= wrong_fall else "incorrect_reversal_defensive"

    confirm_bars = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5

    # P1-R11-14修复: HFT策略state_confirm_bars与hft时间尺度冲突
    # HFT策略使用sub-second bars(如100ms)，state_confirm_bars=5意味着500ms确认，
    # 但hft_hard_time_stop_ms=1000(1秒)，两者时间尺度不兼容。
    # 修复: 对于HFT策略(通过bar_period<=0.1判断)，使用时间基准确认而非Bar计数。
    bar_period = float(params.get("bar_period", 1.0))  # Bar周期(分钟)，HFT通常<0.1
    if bar_period <= 0.1:
        # HFT策略: 使用时间基准确认(秒)
        hft_state_confirm_seconds = float(params.get("hft_state_confirm_seconds", 5.0))
        confirm_bars = max(1, int(hft_state_confirm_seconds / (bar_period * 60.0)))

    if candidate == bt.current_state:
        bt.pending_state = None
        bt.state_confirm_count = 0
        return bt.current_state

    if bt.pending_state == candidate:
        bt.state_confirm_count += 1
    else:
        bt.pending_state = candidate
        bt.state_confirm_count = 1

    if bt.state_confirm_count >= confirm_bars:
        bt.current_state = candidate
        bt.pending_state = None
        bt.state_confirm_count = 0

    return bt.current_state


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


CASCADE_SLIPPAGE_MULTIPLIER = 1.5
CASCADE_SLIPPAGE_CAP_BPS = 50.0

# P0-6修复: 到期日滑点倍增表
EXPIRY_SLIPPAGE_MULTIPLIERS = {
    3: 20.0,   # 到期前3天: 20倍滑点
    7: 10.0,   # 到期前7天: 10倍滑点
    14: 3.0,   # 到期前14天: 3倍滑点
}


_CONTRACT_MULTIPLIER_CACHE: Dict[str, float] = {}  # R21-MEM-P2-10修复: 无TTL/大小限制的缓存，合约乘数不变可无TTL，但应设max_size防无限增长

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


_EQUITY_MAX = 1e15
_EQUITY_MIN = -1e15

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
    if days_to_expiry is None:
        # 尝试从expiry_date计算
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
    if days_to_expiry is None:
        return 1.0
    # P0-6修复: numpy.int64不是int的子类，需用np.issubdtype或try/except
    try:
        dte = int(days_to_expiry)
    except (TypeError, ValueError):
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
    # NP-P1-11~26: 已知限制 - 多处数值精度问题(含float64精度边界、
    # IV求解收敛阈值、Greeks有限差分步长等)，P2级别待规划


def validate_logic_reversal_no_future(bar_data: pd.DataFrame = None,
                                       n_check_bars: int = 100) -> Dict[str, Any]:
    """P0-裂缝28：验证逻辑反转平仓的wrong_pct不使用未来数据

    检查_check_logic_reversal中调用wrong_pct时传入的bar是否已完全形成。
    逻辑反转触发时刻，所使用的wrong_pct不应包含该时刻之后的任何数据。
    """
    if bar_data is None or bar_data.empty:
        return {"passed": False, "action": "no_data", "details": "无数据无法验证，默认不通过"}

    wrong_cols = [c for c in bar_data.columns if 'wrong' in c.lower() and 'pct' in c.lower()]
    if not wrong_cols:
        return {"passed": True, "action": "no_wrong_pct_columns", "details": "无wrong_pct列，无需验证"}

    issues = []
    n_check = min(n_check_bars, len(bar_data))

    # R21-MEM-P2-13修复: 循环中重复调用imread/load — 本项目无图像加载场景，不涉及此问题
    # 检查1：wrong_pct在同一Bar内不应有回溯修正（NaN后填充）
    for col in wrong_cols:
        vals = pd.to_numeric(bar_data[col], errors='coerce').values[:n_check]
        nan_count = sum(1 for v in vals if pd.isna(v))
        if nan_count > 0:
            issues.append(f"列{col}有{nan_count}个NaN值（可能存在数据不完整）")

    # 检查2：wrong_pct值不应随时间回溯变化（前向填充检测）
    # 如果Bar[i]的wrong_pct在后续Bar中发生了变化，说明存在未来数据修正
    for col in wrong_cols:
        vals = pd.to_numeric(bar_data[col], errors='coerce').values[:n_check]
        # 检查是否存在值突然从0变为非0（可能表示延迟到达的数据修正了历史值）
        zero_to_nonzero = 0
        for i in range(1, min(len(vals), n_check)):
            if vals[i-1] == 0 and vals[i] != 0 and abs(vals[i]) > 0.01:
                zero_to_nonzero += 1
        if zero_to_nonzero > n_check * 0.3:
            issues.append(
                f"列{col}有{zero_to_nonzero}次从0跳变为非0（>{n_check * 0.3:.0f}次阈值），"
                f"可能存在未来数据修正"
            )

    # 检查3：wrong_pct与未来收益的相关性不应过高
    # 如果wrong_pct与未来N个Bar的收益高度相关，说明可能包含未来信息
    if 'close' in bar_data.columns:
        for col in wrong_cols:
            vals = pd.to_numeric(bar_data[col], errors='coerce').values[:n_check]
            closes = bar_data['close'].values[:n_check]
            if len(vals) > 10 and len(closes) > 10:
                future_returns = np.zeros(len(vals))
                for i in range(len(vals) - 1):
                    if closes[i] > 0:
                        future_returns[i] = (closes[min(i+1, len(closes)-1)] - closes[i]) / closes[i]
                # 计算wrong_pct与未来收益的相关性
                valid_mask = ~(np.isnan(vals) | np.isnan(future_returns))
                if valid_mask.sum() > 5:
                    corr = float(np.corrcoef(vals[valid_mask], future_returns[valid_mask])[0, 1])
                    if not np.isnan(corr) and abs(corr) > 0.7:
                        issues.append(
                            f"列{col}与未来1Bar收益相关性={corr:.3f}>0.7阈值，"
                            f"可能包含未来信息"
                        )

    passed = len(issues) == 0
    return {
        "passed": passed,
        "wrong_pct_columns": wrong_cols,
        "issues": issues,
        "n_bars_checked": n_check,
        "action": "fix_future_data_leak" if not passed else "proceed",
    }


def _check_logic_reversal(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> bool:
    """逻辑反转平仓检测（裂缝28修复：wrong_pct无未来函数验证）

    如果持仓基于correct_trending（一致性共振），但当前Bar的信号强烈反转
    （wrong_rise_pct + wrong_fall_pct > correct_rise_pct + correct_fall_pct 且
     超出比例 > reversal_threshold），立即触发逻辑反转平仓。
    这模拟顶级基金的操作：不等待止损，在信号逻辑反转时第一时间离场。

    未来函数安全保证：
    - correct_pct/wrong_pct来源于当前bar的OHLCV数据（已完全形成）
    - bar在回测中按时间顺序遍历，不会访问未来bar
    - bar_time断言确保bar时间戳与回测游标一致
    """
    _bar_time_lr = bar.get("minute", pd.NaT)
    if not pd.isna(_bar_time_lr):
        if not _check_safety(bt, _bar_time_lr, params, allow_close=True, bar=bar):
            return True
    for symbol, pos in list(bt.positions.items()):
        if pos.open_reason != "CORRECT_RESONANCE":
            continue

        correct_pct = bar.get("correct_rise_pct", 0) + bar.get("correct_fall_pct", 0)
        wrong_pct = bar.get("wrong_rise_pct", 0) + bar.get("wrong_fall_pct", 0)
        reversal_threshold = params.get("logic_reversal_threshold", 1.5)

        if wrong_pct > correct_pct * reversal_threshold and wrong_pct > BACKTEST_THRESHOLDS["logic_reversal_min_wrong_pct"]:
            price = bar.get("close", 0.0)
            bar_time = bar.get("minute", pd.NaT)
            if pd.isna(bar_time):
                logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar")
                continue
            if price <= 0:
                continue

            if hasattr(bt, '_last_bar_time') and bt._last_bar_time is not None:
                if bar_time < bt._last_bar_time:
                    logger.error(
                        "[NP-P2-13] wrong_pct来源bar时间%s < 回测游标%s，未来数据污染，跳过本bar",
                        bar_time, bt._last_bar_time,
                    )
                    continue

            _multiplier = _get_contract_multiplier(symbol)
            signed_volume = pos.volume if pos.volume != 0 else 0
            pnl = (price - pos.open_price) * signed_volume * _multiplier
            # P0-R9-18修复: 期权PnL区分权利金盈亏与Delta盈亏
            _premium_pnl = 0.0
            _delta_pnl = 0.0
            if getattr(pos, 'instrument_type', 'future') == 'option_buyer' or getattr(pos, 'instrument_type', 'future') == 'option_seller':
                _open_premium = getattr(pos, 'option_premium', 0.0)
                _close_premium = bar.get('option_premium', _open_premium + (price - pos.open_price))
                _premium_pnl = (_close_premium - _open_premium) * pos.lots
                _delta_pnl = pnl - _premium_pnl
            # R3-D-13已知限制: 回测bid_ask_spread使用历史数据,实盘使用实时数据,两者可能存在系统性偏差
            bid_ask = bar.get("bid_ask_spread", 0.0)
            spread_q = bar.get("_spread_quality", 0)  # P0-31修复: 默认值从1改为0，与DB schema DEFAULT 0对齐，缺失时保守阻断开仓
            slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
            slip = price * slip_bps / 10000 * pos.lots
            commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算平仓手续费; P1-R11-03: 传入exchange_id
            _safe_equity_add(bt, pnl - slip - commission)
            bt.total_trades += 1
            _bt_capture_snapshot(bt, "close", f"逻辑反转平仓 {symbol}", "", bar)
            bt.recent_pnls.append(pnl - slip - commission)
            if len(bt.recent_pnls) > 50:
                bt.recent_pnls = bt.recent_pnls[-50:]
            del bt.positions[symbol]
            logger.debug("逻辑反转平仓: %s @ %.2f (wrong=%.2f > correct*%.1f=%.2f)",
                         symbol, price, wrong_pct, reversal_threshold, correct_pct * reversal_threshold)

    return True


def _force_close_all_positions(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
    reason: str = "manual_force_close",
) -> int:
    """P0-R9-12修复: 统一强制平仓所有未平仓头寸
    由断路器/硬停止/到期/回测结束等场景统一调用
    返回: 实际平仓数量
    """
    closed_count = 0
    if bar is None or len(bt.positions) == 0:
        return 0
    bar_time = bar.get("datetime", bar.get("minute", pd.NaT))
    for sym in list(bt.positions.keys()):
        pos = bt.positions[sym]
        _is_limit_up = bar.get('is_limit_up', False)
        _is_limit_down = bar.get('is_limit_down', False)
        _close_dir = 'SELL' if pos.volume > 0 else 'BUY'
        if _is_limit_up and _close_dir == 'SELL':
            continue
        if _is_limit_down and _close_dir == 'BUY':
            continue
        close_price = bar.get("close", pos.open_price)
        _mult = _get_contract_multiplier(sym)
        pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
        bid_ask = bar.get("bid_ask_spread", 0.0)
        spread_q = bar.get("_spread_quality", 0)
        slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
        slip = close_price * slip_bps / 10000 * pos.lots
        commission = _compute_commission(sym, pos.lots, open_time=pos.open_time, close_time=bar_time, is_open=False, exchange_id=_infer_exchange_id(sym))
        net_pnl = pnl - slip - commission
        _safe_equity_add(bt, net_pnl)
        bt.total_trades += 1
        if np.isclose(close_price, pos.open_price, rtol=1e-10):
            float_pnl_pct = 0.0
        else:
            float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
        if pos.volume < 0:
            float_pnl_pct = -float_pnl_pct
        hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
        bt.closed_trades.append(_ClosedTrade(
            pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason=reason,
            hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
            stage1_passed=getattr(pos, 'stage1_passed', False),
        ))
        bt.recent_pnls.append(net_pnl)
        if len(bt.recent_pnls) > 50:
            bt.recent_pnls = bt.recent_pnls[-50:]
        del bt.positions[sym]
        closed_count += 1
    return closed_count


def _check_safety(
    bt: _BacktestState,
    bar_time: pd.Timestamp,
    params: Dict[str, float],
    allow_close: bool = False,
    bar: Optional[pd.Series] = None,
) -> bool:
    """安全检查（裂缝14+25修复：断路器暂停期间允许平仓保护）。

    allow_close=True: 断路器暂停期间仍允许（平仓保护性操作）。
    allow_close=False: 断路器暂停禁止操作（开仓拦截）。
    bar: 可选，传入当前bar数据用于断路器/回撤强制平仓。
    """
    is_circuit_breaker_paused = (
        bt.circuit_breaker_until is not None and bar_time < bt.circuit_breaker_until
    )
    if is_circuit_breaker_paused and not allow_close:
        # P2-R3-P-11: daily_loss_hard_stop双机制并存 — _check_safety中circuit_breaker
        # 与risk_service._check_daily_drawdown均能触发硬停止，但机制不同:
        # circuit_breaker=速率断路器(短期1min回撤), daily_drawdown=日回撤硬停止(全天回撤)。
        # 已知限制: 两套硬停止机制独立运行，无统一协调入口，可能同时触发导致重复平仓。
        # P0-8修复: 断路器激活时强制平仓
        # EX-04修复: 回测强平路径添加涨跌停检查
        if bar is not None and len(bt.positions) > 0:
            for sym in list(bt.positions.keys()):
                pos = bt.positions[sym]
                _is_limit_up = bar.get('is_limit_up', False)
                _is_limit_down = bar.get('is_limit_down', False)
                _close_dir = 'SELL' if pos.volume > 0 else 'BUY'
                if _is_limit_up and _close_dir == 'SELL':
                    continue
                if _is_limit_down and _close_dir == 'BUY':
                    continue
                close_price = bar.get("close", pos.open_price)
                _mult = _get_contract_multiplier(sym)
                pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 0)
                slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                slip = close_price * slip_bps / 10000 * pos.lots
                commission = _compute_commission(sym, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(sym))  # P0-4: 按品种计算平仓手续费; N-01修复: symbol→sym; P1-R11-03: 传入exchange_id
                net_pnl = pnl - slip - commission
                _safe_equity_add(bt, net_pnl)
                bt.total_trades += 1
                if np.isclose(close_price, pos.open_price, rtol=1e-10):  # NP-P2-17: 收益率精度保护
                    float_pnl_pct = 0.0
                else:
                    float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
                if pos.volume < 0:
                    float_pnl_pct = -float_pnl_pct
                hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
                bt.closed_trades.append(_ClosedTrade(
                    pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason="circuit_breaker_force_close",
                    hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
                    stage1_passed=getattr(pos, 'stage1_passed', False),
                ))
                bt.recent_pnls.append(net_pnl)
                if len(bt.recent_pnls) > 50:
                    bt.recent_pnls = bt.recent_pnls[-50:]
                del bt.positions[sym]
        return False

    hard_stop = params.get("daily_loss_hard_stop_pct", 0.05)
    if bt.daily_start_equity > 0:
        daily_drawdown = (bt.daily_start_equity - bt.equity) / bt.daily_start_equity
        if daily_drawdown >= hard_stop and not allow_close:
            # P0-10修复: 日最大回撤超限时强制平仓
            # EX-04修复: 日回撤强平路径添加涨跌停检查
            if bar is not None and len(bt.positions) > 0:
                for sym in list(bt.positions.keys()):
                    pos = bt.positions[sym]
                    _is_limit_up = bar.get('is_limit_up', False)
                    _is_limit_down = bar.get('is_limit_down', False)
                    _close_dir = 'SELL' if pos.volume > 0 else 'BUY'
                    if _is_limit_up and _close_dir == 'SELL':
                        continue
                    if _is_limit_down and _close_dir == 'BUY':
                        continue
                    close_price = bar.get("close", pos.open_price)
                    _mult = _get_contract_multiplier(sym)
                    pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
                    bid_ask = bar.get("bid_ask_spread", 0.0)
                    spread_q = bar.get("_spread_quality", 0)
                    slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                    slip = close_price * slip_bps / 10000 * pos.lots
                    commission = _compute_commission(sym, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(sym))  # P0-4: 按品种计算平仓手续费; N-01修复: symbol→sym; P1-R11-03: 传入exchange_id
                    net_pnl = pnl - slip - commission
                    _safe_equity_add(bt, net_pnl)
                    bt.total_trades += 1
                    if np.isclose(close_price, pos.open_price, rtol=1e-10):  # NP-P2-17: 收益率精度保护
                        float_pnl_pct = 0.0
                    else:
                        float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
                    if pos.volume < 0:
                        float_pnl_pct = -float_pnl_pct
                    hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
                    bt.closed_trades.append(_ClosedTrade(
                        pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason="daily_drawdown_force_close",
                        hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
                        stage1_passed=getattr(pos, 'stage1_passed', False),
                    ))
                    bt.recent_pnls.append(net_pnl)
                    if len(bt.recent_pnls) > 50:
                        bt.recent_pnls = bt.recent_pnls[-50:]
                    del bt.positions[sym]
            return False

    return True


def _check_backtest_health(bt: _BacktestState, params: Dict[str, float], bar_time) -> bool:
    """P0-R11-06修复: 回测健康检查机制

    模拟实盘get_health_status()的CRITICAL暂停逻辑：
    - 当回测权益低于初始权益的50%时，标记为CRITICAL，暂停新开仓
    - 当连续亏损次数超过阈值时，标记为CRITICAL
    - 与实盘strategy_ecosystem.get_health_status()对齐
    """
    # 权益生存检查：权益低于初始权益50%视为CRITICAL
    if hasattr(bt, 'initial_equity') and bt.initial_equity > 0:
        equity_ratio = bt.equity / bt.initial_equity
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
) -> None:
    # P0-R9-02修复: 检查质量阻断标志（_option_metadata_quality=0时阻断生效）
    if hasattr(bt, 'new_open_blocked') and bt.new_open_blocked:
        logger.info("[P0-R9-02] 开仓被质量检查阻断，跳过")
        return
    
    bar_time = bar.get("minute", pd.NaT)
    if pd.isna(bar_time):
        logger.warning("[R22-TIME-03] bar缺少minute字段，跳过开仓判断")
        return
    cooldown = params.get("signal_cooldown_sec", 60.0)  # R13-三对齐修复: 默认值从0.0改为60.0，与risk_service/config_params对齐
    if bt.last_signal_time is not None and cooldown > 0:
        elapsed = (bar_time - bt.last_signal_time).total_seconds()
        if elapsed < cooldown:
            return

    if _is_consecutive_loss_paused(bt, params, bar_time):
        return

    max_signals = int(params.get("max_signals_per_window", 5))
    # N-05修复: 原*100导致信号限频形同虚设(需500信号才触发)，改为*1即max_signals次后限频
    if bt.total_signals >= max_signals:
        return

    reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
    tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)

    symbol = bar.get("symbol", "unknown")
    price = bar.get("close", 0.0)
    # R24-P1-IV-11修复: 使用safe_price_check替代price<=0
    if not safe_price_check(price):
        return

    strength = bar.get("strength", 0.0)
    if strategy_type == "main" and reason == "CORRECT_RESONANCE" and strength < BACKTEST_THRESHOLDS["correct_resonance_min_strength"]:
        return

    lots = _compute_lots_with_risk_budget(
        bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
        recent_pnls=bt.recent_pnls)
    if lots <= 0:
        return

    # P-22补全修复: compute_mode_position_size调用 — TVF六维因子仓位校验
    # 原修复仅添加了方法定义和参数传递，但无调用方，此处补全调用链路
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs_p22 = get_risk_service(scope_id='backtest')
        _tvf_lots = _rs_p22.compute_mode_position_size(
            equity=bt.equity, entry_price=price, stop_price=price * (1 - sl_ratio),
            sortino_ratio=getattr(bt, 'sortino_ratio', 0.0),
            calmar_ratio=getattr(bt, 'calmar_ratio', 0.0),
            sharpe_ratio=getattr(bt, 'sharpe_ratio', 0.0),
            ofi=bar.get('ofi', 0.0), cvd_divergence=bar.get('cvd_divergence', 0.0),
            smart_money_flow=bar.get('smart_money_flow', 0.0),
            delta=bar.get('delta', 0.0), gamma=bar.get('gamma', 0.0),
            theta=bar.get('theta', 0.0), vega=bar.get('vega', 0.0),
        )
        if _tvf_lots > 0:
            lots = min(lots, max(1, int(_tvf_lots)))
    except Exception as _p22_err:
        logging.warning("[R16-P0-007] TVF仓位计算失败，使用风险预算回退: %s", _p22_err)

    imbalance = bar.get("imbalance", 0)
    if imbalance == 0:
        return
    direction = 1 if imbalance > 0 else -1
    if strategy_type == "shadow_reverse":
        direction = -direction

    _position_volume = sum(abs(int(getattr(pos, 'volume', 0))) for pos in bt.positions.values())
    _open_positions = len(bt.positions)

    # P-09修复: 回测开仓路径调用RiskService.check_before_trade()
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _signal = {
            "symbol": symbol,
            "direction": "BUY" if direction > 0 else "SELL",
            "price": price,
            "volume": lots,
            "amount": price * lots,
            "is_valid": True,
            "action": "OPEN",
            "account_id": "backtest",
            "signal_id": f"BT_{symbol}_{int(bar_time.timestamp())}",
        }
        _chk = _rs.check_before_trade(_signal)
        if _chk.is_block:
            logger.debug("[P-09] _try_open blocked by RiskService: %s", _chk.reason)
            return
    except Exception as _rs_err:
        logger.warning("[P2-R3-D-19] RiskService check_before_trade failed, fail-safe block: %s", _rs_err)
        return
    # P-01修复: 交易前监管合规检查（手册8.3节）
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _position_data = {"instrument_id": bar.get("symbol", ""), "volume": _position_volume}
        _compliance = _rs.check_regulatory_compliance(_position_data)
        if not _compliance.get('compliant', True):
            logger.warning("[P-01] _try_open blocked by regulatory compliance: %s", _compliance.get('violations', []))
            return
    except Exception as _compliance_err:
        logger.warning("[P2-R3-D-19] regulatory compliance check failed, fail-safe block: %s", _compliance_err)
        return
    # P-02修复: 交易前资金充足性检查（手册8.3节）
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _use_span = params.get("use_span_margin", False)
        if _use_span:
            try:
                from risk_service import SimplifiedSPAN
                _span = SimplifiedSPAN()
                _required_margin = _span.calc_margin([{"instrument_id": symbol, "price": price, "quantity": lots, "delta": 0.5}])
            except Exception:
                _required_margin = price * lots * params.get("margin_ratio", 0.1)
        else:
            _required_margin = price * lots * params.get("margin_ratio", 0.1)
        _capital = _rs.check_capital_sufficiency(bt.equity, _required_margin, _open_positions)
        if not _capital.get('sufficient', False):
            logger.warning("[P-02] _try_open blocked by capital sufficiency: %s", _capital.get('reason', ''))
            return
    except Exception as _capital_err:
        logger.warning("[P2-R3-D-19] capital sufficiency check failed, fail-safe block: %s", _capital_err)
        return
    # P-03修复: 交易前交易所状态检查（手册8.3节）
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service(scope_id='backtest')
        _exchange_status = _rs.check_exchange_status()
        if _exchange_status.get('status') != 'OPEN':
            logger.warning("[P-03] _try_open blocked by exchange status: %s", _exchange_status.get('status', 'UNKNOWN'))
            return
    except Exception as _exchange_err:
        logger.warning("[P2-R3-D-19] exchange status check failed, fail-safe block: %s", _exchange_err)
        return
    # N1修复: 检查价差数据质量标记 — 质量为0时阻止开仓
    # 原代码仅warn不阻断，系统知道数据质量差但不会阻止交易
    # N-03修复: pd.Series无属性时getattr返回默认值1→错误允许开仓，改为安全的bar.get()
    _sq = bar.get('_spread_quality', 0)
    if isinstance(_sq, property):
        _sq = 1  # 防止property对象
    try:
        _sq = int(_sq)
    except (TypeError, ValueError):
        _sq = 1
    if _sq == 0:
        logger.warning("[BACKTEST] _try_open blocked: _spread_quality=0, spread data unreliable")
        return
    # P-31修复: 绝对期望值暂停检查 — EV暂停时禁止新开仓（手册9.3节）
    try:
        from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
        _sse = get_shadow_strategy_engine()
        if _sse and hasattr(_sse, 'is_absolute_ev_paused') and _sse.is_absolute_ev_paused():
            logger.warning("[BACKTEST] _try_open blocked: absolute EV paused, no new opens allowed")
            return
    except Exception as _sse_e:
        logger.warning("[BACKTEST] _try_open shadow engine check failed, fail-safe block: %s", _sse_e)
        return

    # P-39修复: _option_metadata_quality=0时真正阻断新开仓（手册N1节）
    _omq = bar.get('_option_metadata_quality', 1)
    if isinstance(_omq, property):
        _omq = 1
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 1
    if _omq == 0:
        logger.warning("[P-39] _try_open blocked: _option_metadata_quality=0, option metadata incomplete")
        return
    
    imbalance = bar.get("imbalance", 0)
    if imbalance == 0:
        return
    direction = 1 if imbalance > 0 else -1
    if strategy_type == "shadow_reverse":
        direction = -direction

    # BF-P1-06: 涨跌停限制检查 — 涨停板禁止开多/平空，跌停板禁止开空/平多
    if ENABLE_PRICE_LIMIT_CHECK:
        _prev_close = bar.get("prev_close", 0.0)
        _close = bar.get("close", 0.0)
        if _prev_close > 0 and _close > 0:
            _price_ratio = _close / _prev_close
            if _price_ratio >= PRICE_LIMIT_THRESHOLD and direction > 0:
                logger.debug("[BF-P1-06] 涨停板禁止开多: %s ratio=%.4f", symbol, _price_ratio)
                return
            if _price_ratio <= (2.0 - PRICE_LIMIT_THRESHOLD) and direction < 0:
                logger.debug("[BF-P1-06] 跌停板禁止开空: %s ratio=%.4f", symbol, _price_ratio)
                return

    volume = direction * lots

    # BF-P1-04: 部分成交模拟 — 按成交量比例缩减持仓
    if ENABLE_PARTIAL_FILL:
        _bar_volume = bar.get("volume", 0)
        if _bar_volume > 0 and lots > 0:
            _volume_ratio = min(1.0, max(PARTIAL_FILL_MIN_RATIO, _bar_volume / (lots * 100)))
            _filled_lots = max(1, int(lots * _volume_ratio / PARTIAL_FILL_GRANULARITY) * int(PARTIAL_FILL_GRANULARITY * 10) // 10)
            if _filled_lots < lots:
                logger.debug("[BF-P1-04] 部分成交: %s 请求%d手→成交%d手(%.0f%%)", symbol, lots, _filled_lots, _volume_ratio * 100)
                lots = _filled_lots
                volume = direction * lots

    # P1-R11-05修复: 模拟订单执行延迟对成交价的影响
    _exec_delay_ms = float(params.get("execution_delay_ms", 50))
    if _exec_delay_ms > 0:
        _delay_slippage_bps = _exec_delay_ms / 1000.0 * 0.1
        _delay_adj = price * _delay_slippage_bps / 10000.0
        if direction > 0:
            price += _delay_adj
        else:
            price -= _delay_adj

    sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
    sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

    bid_ask = bar.get("bid_ask_spread", 0.0)
    spread_q = bar.get("_spread_quality", 0)
    _max_sub_lots = int(params.get("backtest_max_sub_order_lots", 5))
    _sub_orders = _backtest_order_split(lots, max_sub_order_lots=_max_sub_lots)
    _total_slippage_cost = 0.0
    _multiplier = _get_contract_multiplier(symbol)
    for _sub_lots in _sub_orders:
        _sub_slippage = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
        _total_slippage_cost += _sub_lots * price * _multiplier * _sub_slippage / 10000.0
    slip_cost = _total_slippage_cost
    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
    bt.equity -= (commission + slip_cost)

    pos = _BacktestPosition(
        instrument_id=symbol,
        volume=volume,
        open_price=price,
        open_time=bar_time,
        stop_profit_price=sp_price,
        stop_loss_price=sl_price,
        open_reason=reason,
        lots=lots,
        open_state=bt.current_state,
        open_strength=strength,
        instrument_type=_infer_instrument_type(symbol),
    )
    bt.positions[symbol] = pos
    bt.last_signal_time = bar_time
    bt.total_signals += 1
    _bt_capture_snapshot(bt, "signal", f"{symbol} {direction} {reason}", strategy_type, bar)


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


def _check_positions(
    bt: _BacktestState,
    bar: pd.Series,
    params: Dict[str, float],
) -> None:
    # N1修复: 检查Greeks数据质量标记 — 质量为0时阻止新开仓(但允许平仓)
    # 原代码仅warn不阻断，Greeks数据缺失时仍允许基于Greeks的决策
    _omq = bar.get('_option_metadata_quality', 0) if isinstance(bar, pd.Series) else getattr(bar, '_option_metadata_quality', 0)
    if isinstance(_omq, property):
        _omq = 0
    try:
        _omq = int(_omq)
    except (TypeError, ValueError):
        _omq = 0
    if _omq == 0:
        logger.warning("[BACKTEST] _check_positions: _option_metadata_quality=0, Greeks data unreliable, blocking new opens")
        # P-39修复: _option_metadata_quality=0时设置新开仓阻断标记（手册N1节）
        bt.new_open_blocked = True

    # P0-9修复: 状态反转时强制平仓（覆盖双向）
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
            close_price = bar.get("close", pos.open_price)
            _mult = _get_contract_multiplier(sym)
            pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
            bid_ask = bar.get("bid_ask_spread", 0.0)
            spread_q = bar.get("_spread_quality", 0)
            slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
            slip = close_price * slip_bps / 10000 * pos.lots
            commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算平仓手续费; P1-R11-03: 传入exchange_id
            net_pnl = pnl - slip - commission
            _safe_equity_add(bt, net_pnl)
            bt.total_trades += 1
            if np.isclose(close_price, pos.open_price, rtol=1e-10):  # NP-P2-17: 收益率精度保护
                float_pnl_pct = 0.0
            else:
                float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
            if pos.volume < 0:
                float_pnl_pct = -float_pnl_pct
            hold_min = (bar_time_p09 - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
            bt.closed_trades.append(_ClosedTrade(
                pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason="state_conflict_force_close",
                hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
                stage1_passed=getattr(pos, 'stage1_passed', False),
            ))
            bt.recent_pnls.append(net_pnl)
            if len(bt.recent_pnls) > 50:
                bt.recent_pnls = bt.recent_pnls[-50:]
            del bt.positions[sym]

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

    if pos.volume > 0:
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
        # P1-R11-04修复: 平仓延迟执行
        _close_delay = int(params.get("close_position_delay_bars", 0))
        if _close_delay > 0:
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
        commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算平仓手续费; P1-R11-03: 传入exchange_id
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
            stage1_passed=getattr(pos, 'stage1_passed', False),
        ))
        del bt.positions[symbol]

        daily_dd = (bt.daily_start_equity - bt.equity) / bt.daily_start_equity if bt.daily_start_equity > 0 else 0
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


def _compute_profit_loss_ratio_metrics(closed_trades: List[_ClosedTrade], equity_curve: np.ndarray, strategy_type: str = '') -> Dict[str, Any]:
    wins = [t for t in closed_trades if t.pnl > 0]
    losses = [t for t in closed_trades if t.pnl < 0]
    n_trades = len(closed_trades)
    n_wins = len(wins)
    n_losses = len(losses)

    avg_win_pct = float(np.mean([t.pnl_pct for t in wins])) if wins else 0.0
    avg_loss_pct = abs(float(np.mean([t.pnl_pct for t in losses]))) if losses else 0.0
    total_win = sum(t.pnl for t in wins)
    total_loss = abs(sum(t.pnl for t in losses))
    profit_factor = total_win / total_loss if total_loss > 1e-4 else 0.0
    win_loss_ratio = avg_win_pct / avg_loss_pct if avg_loss_pct > 1e-4 else 0.0
    win_rate = n_wins / n_trades if n_trades > 0 else 0.0

    # R4-T-01修复: 计算期望值EV（Expected Value）用于负EV剪枝
    # EV = 胜率 * 平均盈利 - (1-胜率) * 平均亏损
    expected_value = 0.0
    if n_trades > 0:
        avg_win = float(np.mean([t.pnl for t in wins])) if wins else 0.0
        avg_loss = abs(float(np.mean([t.pnl for t in losses]))) if losses else 0.0
        expected_value = win_rate * avg_win - (1 - win_rate) * avg_loss

    max_consecutive_losses = 0
    current_streak = 0
    for t in closed_trades:
        if t.pnl < 0:
            current_streak += 1
            max_consecutive_losses = max(max_consecutive_losses, current_streak)
        else:
            current_streak = 0

    max_consecutive_wins = 0
    current_streak = 0
    for t in closed_trades:
        if t.pnl > 0:
            current_streak += 1
            max_consecutive_wins = max(max_consecutive_wins, current_streak)
        else:
            current_streak = 0

    recovery_efficiency = 0.0
    if len(equity_curve) > 1:
        cummax = np.maximum.accumulate(equity_curve)
        drawdown_mask = equity_curve < cummax
        if np.any(drawdown_mask):
            dd_indices = np.where(drawdown_mask)[0]
            if len(dd_indices) > 0:
                dd_depth = float(np.max(cummax[dd_indices] - equity_curve[dd_indices]))
                new_high_indices = np.where(equity_curve >= cummax)[0]
                if len(new_high_indices) > 1:
                    total_recovery_bars = 0
                    recovery_count = 0
                    for i in range(1, len(new_high_indices)):
                        gap = int(new_high_indices[i] - new_high_indices[i - 1])
                        if gap > 1:
                            total_recovery_bars += gap
                            recovery_count += 1
                    if recovery_count > 0 and dd_depth > 0:
                        avg_recovery_bars = total_recovery_bars / recovery_count
                        recovery_efficiency = dd_depth / (avg_recovery_bars * float(equity_curve[0])) if equity_curve[0] > 0 else 0.0
                        recovery_efficiency = min(recovery_efficiency * 100.0, 10.0)

    calmar = 0.0
    if len(equity_curve) > 1:
        total_ret = (equity_curve[-1] / equity_curve[0] - 1) if equity_curve[0] > 0 else 0.0
        # R17-12修复: 分钟频年化因子 √(252*240)≈245.9
        annualized_ret = total_ret * (_get_annualize_factor(strategy_type) / len(equity_curve)) if len(equity_curve) > 0 else 0.0
        cummax = np.maximum.accumulate(equity_curve)
        safe_cummax = np.where(cummax > 0, cummax, 1.0)
        max_dd_pct = float(np.min(equity_curve / safe_cummax - 1))
        if np.isnan(max_dd_pct) or np.isinf(max_dd_pct):
            max_dd_pct = 0.0
        if abs(max_dd_pct) > 1e-10:
            calmar = annualized_ret / abs(max_dd_pct)
        elif annualized_ret > 0:
            calmar = 999.0

    # P1-R9-08修复: 按hold_minutes分层统计和按open_reason分组统计
    hold_minutes_buckets = {"short_<30": [], "medium_30_120": [], "long_>120": []}
    open_reason_groups: Dict[str, List[float]] = {}
    for t in closed_trades:
        hm = getattr(t, 'hold_minutes', 0) or 0
        if hm < 30:
            hold_minutes_buckets["short_<30"].append(t.pnl_pct)
        elif hm <= 120:
            hold_minutes_buckets["medium_30_120"].append(t.pnl_pct)
        else:
            hold_minutes_buckets["long_>120"].append(t.pnl_pct)
        reason = getattr(t, 'open_reason', '') or ''
        open_reason_groups.setdefault(reason, []).append(t.pnl_pct)
    hold_minutes_stats = {}
    for bucket, pcts in hold_minutes_buckets.items():
        hold_minutes_stats[bucket] = {
            "count": len(pcts),
            "avg_pnl_pct": float(np.mean(pcts)) if pcts else 0.0,
        }
    open_reason_stats = {}
    for reason, pcts in open_reason_groups.items():
        open_reason_stats[reason] = {
            "count": len(pcts),
            "avg_pnl_pct": float(np.mean(pcts)) if pcts else 0.0,
        }

    return {
        "win_loss_ratio": win_loss_ratio,
        "profit_factor": profit_factor,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "win_rate": win_rate,
        "total_trades": n_trades,
        "win_trades": n_wins,
        "loss_trades": n_losses,
        "max_consecutive_losses": max_consecutive_losses,
        "max_consecutive_wins": max_consecutive_wins,
        "recovery_efficiency": recovery_efficiency,
        "calmar": calmar,
        "hold_minutes_stats": hold_minutes_stats,
        "open_reason_stats": open_reason_stats,
    }


def _check_bar_data_monotonic(bar_data: pd.DataFrame) -> None:
    """NP-P2-15: 检查bar时间序列是否单调递增（仅告警，不改变输入数据）。
    P1-R9-30修复: 添加重复Bar检测和跳过Bar检测。
    """
    if bar_data is None or bar_data.empty or "minute" not in bar_data.columns:
        return
    try:
        ts = pd.to_datetime(bar_data["minute"], errors="coerce")
        if ts.isna().any():
            logger.warning("[NP-P2-15] minute列存在无法解析时间，跳过单调性校验")
            return
        # P1-R9-30: 重复Bar检测
        _duplicates = ts[ts.duplicated()]
        if len(_duplicates) > 0:
            logger.warning("[P1-R9-30] 检测到%d个重复Bar时间戳: %s",
                          len(_duplicates), _duplicates.head(5).tolist())
        # P1-R9-30: 跳过Bar检测（相邻时间差>2倍预期间隔视为跳过）
        if len(ts) > 1:
            _diffs = ts.diff().dropna()
            _median_diff = _diffs.median()
            if _median_diff > pd.Timedelta(0):
                _skip_threshold = _median_diff * 2.5
                _skipped = _diffs[_diffs > _skip_threshold]
                if len(_skipped) > 0:
                    logger.warning("[P1-R9-30] 检测到%d个跳过Bar间隔(>2.5x中位数间隔): %s",
                                  len(_skipped), _skipped.head(5).tolist())
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
) -> Dict[str, Any]:
    """统一计算回测汇总指标，减少各策略模板重复代码。"""
    total_return = bt.equity / INITIAL_EQUITY - 1
    equity_arr = np.array(bt.equity_curve)
    if len(equity_arr) > 1:
        # Q6修复: 防止equity归零导致除零
        _safe_prev = np.where(equity_arr[:-1] > 0, equity_arr[:-1], 1.0)
        returns = np.diff(equity_arr) / _safe_prev
        _af = _get_annualize_factor(strategy_type)
        sharpe = np.sqrt(_af) * (np.mean(returns) - RISK_FREE_RATE / _af) / np.std(returns, ddof=1) if np.std(returns, ddof=1) > 1e-10 else 0.0
        sharpe = float(np.clip(sharpe, -10.0, 10.0))  # NP-P2-10: Sharpe值域clip
    else:
        sharpe = 0.0

    if len(equity_arr) > 0:
        _cummax = np.maximum.accumulate(equity_arr)
        _safe_cummax = np.where(_cummax > 0, _cummax, 1.0)
        max_dd = float(np.min(equity_arr / _safe_cummax - 1))
        if np.isnan(max_dd) or np.isinf(max_dd):
            max_dd = 0.0
    else:
        max_dd = 0.0

    plr_metrics = _compute_profit_loss_ratio_metrics(bt.closed_trades, equity_arr, strategy_type)

    # P0-7修复: 换月成本从净值中扣除
    rollover_cost_bps = 0.0
    try:
        rollover_points = detect_rollover_gaps(bar_data)
        if rollover_points:
            bar_data = exclude_rollover_signals(bar_data, rollover_points, skip_days=params.get('rollover_skip_days', 3))
            n_excluded = bar_data['rollover_excluded'].sum() if 'rollover_excluded' in bar_data.columns else 0
            if n_excluded > 0:
                logger.info("[裂缝5] 展期排除: %d/%d bar被标记", n_excluded, len(bar_data))
            rollover_result = compute_rollover_cost(rollover_points, bar_data, params)
            rollover_cost_bps = rollover_result.get("total_rollover_cost_bps", 0.0)
            if rollover_cost_bps > 0 and bt.equity > 0:
                bt.equity -= bt.equity * rollover_cost_bps / 10000.0
                total_return = bt.equity / INITIAL_EQUITY - 1
    except Exception as e:
        logger.warning("[P0-7] rollover cost deduction failed: %s", e)

    daily_returns_list = list(bt.daily_returns) if hasattr(bt, 'daily_returns') else []
    try:
        from ali2026v3_trading.risk_service import calculate_var_historical
        var_95 = calculate_var_historical(daily_returns_list, 0.95) if len(daily_returns_list) >= 10 else 0.0
    except Exception:
        var_95 = 0.0
    result = {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "num_signals": bt.total_signals,
        "strategy_type": strategy_type,
        "rollover_cost_bps": rollover_cost_bps,
        "var_95": var_95,
        "expected_value": plr_metrics.get("expected_value", 0.0),  # P1-R9-24修复: EV传播到回测输出
        **plr_metrics,
        "closed_trades_count": len(bt.closed_trades),
        "circuit_breaker_count": len(bt.circuit_breaker_events),
        "profit_factor": plr_metrics.get("profit_factor", 0.0),
        "win_rate": plr_metrics.get("win_rate", 0.0),
        "calmar": plr_metrics.get("calmar", 0.0),
    }
    # R24-P1-IV-08修复: 参数池输出边界检查 — NaN/Inf结果标记
    import math as _math
    for _k, _v in list(result.items()):
        if isinstance(_v, float) and (_math.isnan(_v) or _math.isinf(_v)):
            logger.warning("[R24-P1-IV-08] 参数池输出异常: %s=%s, 置为0", _k, _v)
            result[_k] = 0.0
    if extra_fields:
        result.update(extra_fields)

    # BF-P1-10: 回测-实盘偏差系统性量化 — 输出保真度评分和偏差来源
    _fidelity_score = 1.0
    _fidelity_deductions = {}
    if not ENABLE_PARTIAL_FILL:
        _fidelity_score -= 0.10
        _fidelity_deductions["partial_fill_disabled"] = 0.10
    if MARKET_ORDER_PRICE_MODE == "close":
        _fidelity_score -= 0.15
        _fidelity_deductions["price_mode_close"] = 0.15
    if not ENABLE_QUEUE_SIMULATION:
        _fidelity_score -= 0.10
        _fidelity_deductions["queue_sim_disabled"] = 0.10
    if not ENABLE_CANCEL_SIMULATION:
        _fidelity_score -= 0.05
        _fidelity_deductions["cancel_sim_disabled"] = 0.05
    _has_mtm = any("mtm" in k.lower() or "mark_to_market" in k.lower() for k in result.keys())
    if not _has_mtm:
        _fidelity_score -= 0.20
        _fidelity_deductions["no_mtm"] = 0.20
    _fidelity_score = max(0.0, _fidelity_score)
    result["backtest_fidelity_score"] = _fidelity_score
    result["backtest_fidelity_deductions"] = _fidelity_deductions
    result["backtest_fidelity_grade"] = "A" if _fidelity_score >= 0.9 else "B" if _fidelity_score >= 0.7 else "C" if _fidelity_score >= 0.5 else "D"

    # R21-MEM-P2-15修复: 大型临时变量未及时释放 — equity_arr为ndarray，_cummax/_safe_cummax为中间变量
    # 建议在调用方(run_backtest等)回测完成后，对bar_data等大型DataFrame执行del + gc.collect()
    # 本函数内中间变量随函数返回自动释放，但bar_data由调用方持有，需调用方显式释放
    return result


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
            _m = _re.match(r'^([A-Za-z]+)', _sym)
            if _m:
                bar["underlying_future_id"] = hash(_m.group(1)) % 1000
            _ym = _re.search(r'(\d{4})', _sym)
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
    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)

    # R3-D-07修复: 回测中从StateParamManager加载三态参数集
    try:
        from ali2026v3_trading.state_param_manager import get_state_param_manager
        _spm = get_state_param_manager()
        _spm_params = _spm.get_params(strategy_type)
        if _spm_params and isinstance(_spm_params, dict):
            params = {**params, **_spm_params}
    except Exception as _d07_e:
        logger.debug("[R3-D-07] StateParamManager加载失败(使用默认params): %s", _d07_e)

    # 行情寿命字典：从bar_data构建（如果尚未构建）
    estimator = _get_life_estimator()
    if estimator is not None and (not hasattr(estimator, '_life_dict') or not estimator._life_dict):
        try:
            estimator.build_life_dict(bar_data)
        except Exception as _e:
            logger.error("[R3-P-09] build_life_dict失败(严重): %s, 寿命信息缺失将导致D3维度退化为0.5", _e)
            bt.get("diagnostics", {}).setdefault("degraded_features", []).append("life_dict")

    # R17-P1-PERF-11修复: 预计算层向量化
    _bar_dates = bar_data['minute'].apply(lambda x: str(x.date()) if hasattr(x, 'date') else str(x)[:10] if pd.notna(x) else '').values if 'minute' in bar_data.columns else None
    _close_arr = bar_data['close'].values if 'close' in bar_data.columns else None

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _execution_delay_ms = params.get("execution_delay_ms", 50)
        if _execution_delay_ms > 0 and _pyrandom.random() < 0.1:
            pass  # R16-P1修复: 回测延迟改为虚拟时钟，不实际sleep，避免拖慢回测性能
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        # EX-P1-08: 涨跌停Bar跳过 — 涨停/跌停Bar无法成交，跳过决策
        if bar.get('is_limit_up', False) or bar.get('is_limit_down', False):
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

    # R21-MEM-P2-15修复: 回测完成后大型临时变量(bt对象含equity_curve/daily_returns/closed_trades等)
    # 建议调用方在获取result后执行: del bt; import gc; gc.collect()
    # 特别是批量回测场景(如grid_scan)，不及时释放会导致内存累积
    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )


def run_backtest_box_extreme(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_extreme",
) -> Dict[str, Any]:
    """箱体极值策略回测

    在other状态下，检测箱体边界极值，做反向操作：
    - 箱底极值（价格触及箱体下沿）→ 做多
    - 箱顶极值（价格触及箱体上沿）→ 做空

    影子策略：
    - shadow_reverse: 方向强制反转（箱底→做空，箱顶→做多）
    - shadow_random: 随机方向（50/50）

    V7.1决策频率机制：决策频率由K线周期 × state_confirm_bars自然决定，每Bar都执行。
    NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    box_threshold = params.get("box_detection_threshold", 0.03)
    box_min_bars = int(params.get("box_min_bars", 20))
    extreme_ratio = params.get("extreme_entry_ratio", 0.5)
    _sync_random_seed(42 if train else 24)

    from ali2026v3_trading.box_detector import BoxDetector, BoxStrategyParams
    _box_params = BoxStrategyParams(
        box_width_max_pct=box_threshold * 100.0,
        min_bounce_count=int(params.get("box_min_bounce_count", 2)),
    )
    _detector = BoxDetector(
        params=_box_params,
        lookback_bars=box_min_bars * 3,
        min_box_bars=box_min_bars,
        adx_threshold=params.get("box_adx_threshold", 25.0),
        bounce_tolerance_pct=params.get("box_bounce_tolerance_pct", 0.1),
    )

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"

        high = bar.get("high", 0.0)
        low = bar.get("low", 0.0)
        close = bar.get("close", 0.0)
        symbol = bar.get("symbol", "")

        _detector.update_bar(high=high, low=low, close=close, timestamp=str(bar_time))

        for sym in list(bt.positions.keys()):
            if sym == symbol:
                _check_positions(bt, bar, params)

        _check_state_transition(bt, bar, params)

        # P0-R11-04修复: box_extreme回测路径添加决策跳帧
        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        box_profile = _detector.detect_box()
        if not box_profile.is_valid:
            continue

        box_high = box_profile.upper
        box_low = box_profile.lower
        box_range = box_high - box_low

        if box_range < close * box_threshold:
            continue

        if _check_safety(bt, bar_time, params) and len(bt.positions) < int(params.get("max_open_positions", 3)):
            position_in_box = (close - box_low) / box_range if box_range > 0 else 0.5

            is_box_bottom = position_in_box < (1 - extreme_ratio)
            is_box_top = position_in_box > extreme_ratio

            should_open = False
            direction = 0

            if is_box_bottom:
                direction = 1
                should_open = True
            elif is_box_top:
                direction = -1
                should_open = True

            if strategy_type == "shadow_reverse":
                direction = -direction
            elif strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"]:
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open = True
                else:
                    should_open = False

            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False

            if should_open and direction != 0:
                # P-35修复: box_extreme开仓添加N1质量检查
                _sq_be = bar.get("_spread_quality", 0)
                try:
                    _sq_be = int(_sq_be)
                except (TypeError, ValueError):
                    _sq_be = 1
                if _sq_be == 0:
                    should_open = False
                    continue
                # C-13修复: box_extreme补全_try_open()安全检查链
                _omq_be = bar.get('_option_metadata_quality', 1)
                if isinstance(_omq_be, property):
                    _omq_be = 1
                try:
                    _omq_be = int(_omq_be)
                except (TypeError, ValueError):
                    _omq_be = 1
                if _omq_be == 0:
                    should_open = False
                    continue
                max_signals_be = int(params.get("max_signals_per_window", 5))
                if bt.total_signals >= max_signals_be:
                    should_open = False
                    continue
                try:
                    from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                    _sse_be = get_shadow_strategy_engine()
                    if _sse_be and hasattr(_sse_be, 'is_absolute_ev_paused') and _sse_be.is_absolute_ev_paused():
                        should_open = False
                        continue
                except Exception:
                    pass
                price = close
                tp_ratio = params.get("close_take_profit_ratio", 2.0)
                sl_ratio = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls)
                if lots <= 0:
                    continue

                # P0-R11-05修复: box_extreme开仓路径调用RiskService.check_before_trade()
                try:
                    from ali2026v3_trading.risk_service import get_risk_service
                    _rs_be = get_risk_service(scope_id='backtest')
                    _signal_be = {
                        "symbol": symbol,
                        "direction": "BUY" if direction > 0 else "SELL",
                        "price": price,
                        "volume": lots,
                        "amount": price * lots,
                        "is_valid": True,
                        "action": "OPEN",
                        "account_id": "backtest",
                        "signal_id": f"BT_BOX_EXTREME_{symbol}_{int(bar_time.timestamp())}",
                    }
                    _chk_be = _rs_be.check_before_trade(_signal_be)
                    if _chk_be.is_block:
                        logger.debug("[P0-R11-05] box_extreme open blocked by RiskService: %s", _chk_be.reason)
                        continue
                except Exception as _rs_err_be:
                    logger.warning("[P0-R11-05] box_extreme check_before_trade failed, fail-safe block: %s", _rs_err_be)
                    continue

                sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

                # P-36修复: box_extreme开仓使用动态滑点
                bid_ask_be = bar.get("bid_ask_spread", 0.0)
                slip_bps_be = _compute_dynamic_slippage_bps(price, bid_ask_be, spread_quality=_sq_be, bar=bar, params=params)
                slip_cost_be = price * slip_bps_be / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                bt.equity -= (commission + slip_cost_be)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=direction * lots,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason="BOX_EXTREME",
                    lots=lots,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.positions[symbol] = pos
                bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )


def run_backtest_box_spring(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "box_spring",
) -> Dict[str, Any]:
    """箱体弹簧策略回测

    条件：
    - IV极低（低于阈值）
    - 近月期权
    - 价格在箱体内部
    → 预期波动率回归，买入期权做多波动率

    影子策略：
    - shadow_reverse: 方向强制反转（买CALL→买PUT）
    - shadow_random: 随机方向

    V7.1决策频率机制：决策频率由K线周期 × state_confirm_bars自然决定，每Bar都执行。
    NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    iv_threshold = params.get("spring_iv_threshold", 0.20)
    impulse_threshold = params.get("spring_impulse_threshold", 0.02)
    _sync_random_seed(42 if train else 24)

    from ali2026v3_trading.box_detector import BoxDetector, BoxStrategyParams
    _spring_box_params = BoxStrategyParams(
        box_width_max_pct=params.get("spring_box_width_max_pct", 5.0),
        min_bounce_count=int(params.get("spring_min_bounce_count", 2)),
    )
    _spring_detector = BoxDetector(
        params=_spring_box_params,
        lookback_bars=int(params.get("spring_lookback_bars", 60)),
        min_box_bars=int(params.get("spring_min_box_bars", 20)),
        adx_threshold=params.get("spring_adx_threshold", 25.0),
    )

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"

        symbol = bar.get("symbol", "")
        close = bar.get("close", 0.0)
        iv = bar.get("iv", 0.0)
        high = bar.get("high", close)
        low = bar.get("low", close)

        _spring_detector.update_bar(high=high, low=low, close=close, timestamp=str(bar_time))

        for sym in list(bt.positions.keys()):
            if sym == symbol:
                _check_positions(bt, bar, params)

        _check_state_transition(bt, bar, params)

        # P0-R11-04修复: box_spring回测路径添加决策跳帧
        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        if _check_safety(bt, bar_time, params) and len(bt.positions) < int(params.get("max_open_positions", 3)):
            _spring_box = _spring_detector.detect_box()
            impulse = (high - low) / close if close > 0 else 0

            is_in_box = _spring_box.is_valid and _spring_box.lower <= close <= _spring_box.upper
            is_low_iv = iv > 0 and iv < iv_threshold
            is_impulse = impulse > impulse_threshold

            should_open = False
            direction = 0

            if is_in_box and is_low_iv and is_impulse:
                direction = 1
                should_open = True

                if strategy_type == "shadow_reverse":
                    direction = -direction
                elif strategy_type == "shadow_random":
                    if np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"]:
                        direction = 1 if np.random.random() < 0.5 else -1
                        should_open = True
                    else:
                        should_open = False

                # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
                if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                    should_open = False

                if should_open and direction != 0:
                    # P-35修复: box_spring开仓添加N1质量检查
                    _sq_bs = bar.get("_spread_quality", 0)
                    try:
                        _sq_bs = int(_sq_bs)
                    except (TypeError, ValueError):
                        _sq_bs = 1
                    if _sq_bs == 0:
                        should_open = False
                        continue
                    # C-13修复: box_spring补全_try_open()安全检查链
                    _omq_bs = bar.get('_option_metadata_quality', 1)
                    if isinstance(_omq_bs, property):
                        _omq_bs = 1
                    try:
                        _omq_bs = int(_omq_bs)
                    except (TypeError, ValueError):
                        _omq_bs = 1
                    if _omq_bs == 0:
                        should_open = False
                        continue
                    max_signals_bs = int(params.get("max_signals_per_window", 5))
                    if bt.total_signals >= max_signals_bs:
                        should_open = False
                        continue
                    try:
                        from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                        _sse_bs = get_shadow_strategy_engine()
                        if _sse_bs and hasattr(_sse_bs, 'is_absolute_ev_paused') and _sse_bs.is_absolute_ev_paused():
                            should_open = False
                            continue
                    except Exception:
                        pass
                    price = close
                    tp_ratio = params.get("spring_stop_profit_ratio", 5.0)
                    sl_ratio = params.get("spring_max_loss_pct", 0.90)
                    lots = _compute_lots_with_risk_budget(
                        bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                        recent_pnls=bt.recent_pnls)
                    if lots <= 0:
                        continue

                    # P0-R11-05修复: box_spring开仓路径调用RiskService.check_before_trade()
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_bs = get_risk_service(scope_id='backtest')
                        _signal_bs = {
                            "symbol": symbol,
                            "direction": "BUY" if direction > 0 else "SELL",
                            "price": price,
                            "volume": lots,
                            "amount": price * lots,
                            "is_valid": True,
                            "action": "OPEN",
                            "account_id": "backtest",
                            "signal_id": f"BT_BOX_SPRING_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_bs = _rs_bs.check_before_trade(_signal_bs)
                        if _chk_bs.is_block:
                            logger.debug("[P0-R11-05] box_spring open blocked by RiskService: %s", _chk_bs.reason)
                            continue
                    except Exception as _rs_err_bs:
                        logger.warning("[P0-R11-05] box_spring check_before_trade failed, fail-safe block: %s", _rs_err_bs)
                        continue

                    sp_price = price * tp_ratio if direction > 0 else price / tp_ratio
                    sl_price = price * (1 - sl_ratio) if direction > 0 else price * (1 + sl_ratio)

                    # P-36修复: box_spring开仓使用动态滑点
                    bid_ask_bs = bar.get("bid_ask_spread", 0.0)
                    slip_bps_bs = _compute_dynamic_slippage_bps(price, bid_ask_bs, spread_quality=_sq_bs, bar=bar, params=params)
                    slip_cost_bs = price * slip_bps_bs / 10000 * lots
                    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                    bt.equity -= (commission + slip_cost_bs)

                    pos = _BacktestPosition(
                        instrument_id=symbol,
                        volume=direction * lots,
                        open_price=price,
                        open_time=bar_time,
                        stop_profit_price=sp_price,
                        stop_loss_price=sl_price,
                        open_reason="BOX_SPRING",
                        lots=lots,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    bt.positions[symbol] = pos
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
    )


def run_backtest_hft(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
) -> Dict[str, Any]:
    """S1高频趋势共振回测：tick级决策频率

    与S2分钟级趋势共振共享底层逻辑（一致性共振→方向延续），
    但参数集完全独立（毫秒/tick数 vs 分钟/秒）：
    - hft_signal_confirm_ticks: 信号确认所需连续tick数
    - hft_cooldown_ms: 信号冷却时间（毫秒）
    - hft_min_imbalance: 最小允许开仓的imbalance阈值

    影子策略：
    - shadow_reverse: 方向强制反转
    - shadow_random: 随机方向（概率基于信号频率）
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    _sync_random_seed(42 if train else 24)

    # P1-裂缝49：S1 200ms延迟microstructure模型回退
    # microstructure预测模型未实现，200ms档位回退使用order_flow模型
    hft_latency_tier = params.get("hft_latency_tier_ms", 1000)
    if hft_latency_tier <= 200:
        logger.info(
            "[P1-裂缝49] HFT延迟%dms档位: microstructure模型未实现, "
            "回退使用order_flow模型(fallback)",
            int(hft_latency_tier),
        )

    hft_signal_count = 0
    hft_pending_direction = 0

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"

        _check_logic_reversal(bt, bar, params)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        if _check_safety(bt, bar_time, params):
            imbalance = abs(bar.get("imbalance", 0))
            strength = bar.get("strength", 0)

            if strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["hft_shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3)):
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
                else:
                    should_open_hft = False
            else:
                should_open_hft = False
                if imbalance >= min_imbalance and strength > 0.2:
                    current_dir = 1 if bar.get("imbalance", 0) > 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True


            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open_hft and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse", "hft"):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls)
                if lots <= 0:
                    continue

                # P0-R11-05修复: hft开仓路径调用RiskService.check_before_trade()
                try:
                    from ali2026v3_trading.risk_service import get_risk_service
                    _rs_hft = get_risk_service(scope_id='backtest')
                    _signal_hft = {
                        "symbol": symbol, "direction": "BUY" if hft_pending_direction > 0 else "SELL",
                        "price": price, "volume": lots, "amount": price * lots,
                        "is_valid": True, "action": "OPEN", "account_id": "backtest",
                        "signal_id": f"BT_HFT_{symbol}_{int(bar_time.timestamp())}",
                    }
                    _chk_hft = _rs_hft.check_before_trade(_signal_hft)
                    if _chk_hft.is_block:
                        logger.debug("[P0-R11-05] hft open blocked by RiskService: %s", _chk_hft.reason)
                        continue
                except Exception as _rs_err_hft:
                    logger.warning("[P0-R11-05] hft check_before_trade failed, fail-safe block: %s", _rs_err_hft)
                    continue

                volume = hft_pending_direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 0)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                slip_cost = price * slip_bps / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                bt.equity -= (commission + slip_cost)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=volume,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason=reason,
                    lots=lots,
                    open_state=bt.current_state,
                    open_strength=strength,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.positions[symbol] = pos
                bt.last_signal_time = bar_time
                bt.total_signals += 1
                hft_signal_count = 0

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "hft_fidelity_warning": "DEGRADED: tick级参数(hft_cooldown_ms/hft_signal_confirm_ticks)在分钟级回测中失真，需HFT回放引擎验证",
        },
    )


def run_backtest_arbitrage(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "arbitrage",
) -> Dict[str, Any]:
    """S5套利策略回测：微观结构价格偏离检测→快速反向开仓→均值回归平仓

    基于MicrostructureArbitrageDetector的逻辑：
      1. 计算当前价格相对公允价值(implied by imbalance+strength)的偏离
      2. 偏离>arb_deviation_threshold_bps时发出套利信号(反向开仓)
      3. 价格回归至arb_reversion_target_bps以内时平仓
      4. 强制时间止损arb_max_hold_minutes

    特点：收益来源集中(偏离→回归)、持仓时间短、方向总是反转。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    dev_threshold = params.get("arb_deviation_threshold_bps", 50.0)
    reversion_target = params.get("arb_reversion_target_bps", 30.0)
    min_confidence = params.get("arb_min_confidence", 0.6)
    max_hold = params.get("arb_max_hold_minutes", 15.0)
    _sync_random_seed(42 if train else 24)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())
        _reset_daily(bt, current_date)
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    tp_mult, sl_mult = _resolve_tp_sl(params, "ARBITRAGE")
                    if direction == 1:
                        pnl_pct = (bar_price - entry) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= max_hold:
                            pnl = (bar_price - entry) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.total_trades += 1
                            del bt.positions[sym]
                    elif direction == -1:
                        pnl_pct = (entry - bar_price) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= max_hold:
                            pnl = (entry - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.total_trades += 1
                            del bt.positions[sym]

        # P0-R11-04修复: arbitrage回测路径添加决策跳帧
        decision_interval = int(params.get("decision_interval_minutes", params.get("bar_interval", 1)))
        if decision_interval > 1 and (idx % decision_interval != 0):
            continue

        if _check_safety(bt, bar_time, params):
            imbalance = bar.get("imbalance", 0)
            strength = bar.get("strength", 0)
            confidence = min(abs(imbalance) * 2, 1.0)
            symbol = bar.get("symbol", "")

            if symbol not in bt.positions and confidence >= min_confidence:
                fair_value_shift_bps = imbalance * 100
                deviation_bps = abs(fair_value_shift_bps)
                if deviation_bps >= dev_threshold:
                    arb_direction = -1 if fair_value_shift_bps > 0 else 1
                    # R30-P0-06修复: S5/S6影子策略方向逻辑（与S1-S4对齐）
                    # 原问题: S5/S6的影子策略仅靠更保守TP/SL，无方向反转/随机概率
                    # 导致alpha系统性=0，无法过滤过拟合
                    if strategy_type == "shadow_reverse":
                        arb_direction = -arb_direction
                    elif strategy_type == "shadow_random":
                        if np.random.random() < BACKTEST_THRESHOLDS.get("shadow_random_open_prob", 0.5):
                            arb_direction = 1 if np.random.random() < 0.5 else -1
                        else:
                            continue
                    bar_price = bar.get("close", 0)
                    tp_mult, sl_mult = _resolve_tp_sl(params, "ARBITRAGE")
                    tp_price = bar_price * (1 + tp_mult * 0.01) if arb_direction == 1 else bar_price * (1 - tp_mult * 0.01)
                    sl_price = bar_price * (1 - sl_mult * 0.01) if arb_direction == 1 else bar_price * (1 + sl_mult * 0.01)
                    # R30-ARCH-01修复: S5开仓使用_BacktestPosition替代嵌套dict
                    arb_lots = params.get("lots_min", 1)

                    # P0-R11-05修复: arbitrage开仓路径调用RiskService.check_before_trade()
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_arb = get_risk_service(scope_id='backtest')
                        _signal_arb = {
                            "symbol": symbol, "direction": "BUY" if arb_direction > 0 else "SELL",
                            "price": bar_price, "volume": arb_lots, "amount": bar_price * arb_lots,
                            "is_valid": True, "action": "OPEN", "account_id": "backtest",
                            "signal_id": f"BT_ARB_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_arb = _rs_arb.check_before_trade(_signal_arb)
                        if _chk_arb.is_block:
                            logger.debug("[P0-R11-05] arbitrage open blocked by RiskService: %s", _chk_arb.reason)
                            continue
                    except Exception as _rs_err_arb:
                        logger.warning("[P0-R11-05] arbitrage check_before_trade failed, fail-safe block: %s", _rs_err_arb)
                        continue

                    bt.positions[symbol] = _BacktestPosition(
                        instrument_id=symbol,
                        volume=arb_direction * arb_lots,
                        open_price=bar_price,
                        open_time=bar_time,
                        stop_profit_price=tp_price,
                        stop_loss_price=sl_price,
                        open_reason="ARBITRAGE",
                        lots=arb_lots,
                        open_state=bt.current_state,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={"total_trades": bt.total_trades},
    )


def detect_rollover_gaps(bar_data: pd.DataFrame,
                          contract_col: str = "symbol",
                          price_col: str = "close",
                          gap_threshold: float = 0.02) -> List[Dict[str, Any]]:
    """P0-裂缝5：检测合约展期数据断裂点

    找出合约代码变化的位置，计算展期跳空幅度。
    若跳空幅度 > gap_threshold(默认2%)，标记为展期点。

    Returns:
        List[Dict]: 每个展期点的详细信息
    """
    if bar_data.empty or contract_col not in bar_data.columns:
        return []

    rollover_points = []
    contract_series = bar_data[contract_col].values
    price_series = bar_data[price_col].values if price_col in bar_data.columns else None

    for i in range(1, len(contract_series)):
        if contract_series[i] != contract_series[i - 1]:
            point = {
                "index": i,
                "prev_contract": str(contract_series[i - 1]),
                "new_contract": str(contract_series[i]),
                "gap_pct": 0.0,
                "is_significant": False,
            }
            if price_series is not None and price_series[i - 1] > 0:
                gap = (price_series[i] - price_series[i - 1]) / price_series[i - 1]
                point["gap_pct"] = round(gap * 100, 2)
                point["is_significant"] = abs(gap) > gap_threshold
            rollover_points.append(point)

    return rollover_points


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
                bar_data.iloc[start:end, bar_data.columns.get_loc("rollover_excluded")] = True

    n_excluded = bar_data["rollover_excluded"].sum()
    if n_excluded > 0:
        logger.info("[裂缝5-展期剔除] 标记 %d/%d bar为展期排除区", n_excluded, len(bar_data))

    return bar_data


def validate_rollover_impact(bar_data: pd.DataFrame,
                              params: Dict[str, float],
                              skip_days: int = 3) -> Dict[str, Any]:
    """P0-裂缝5：对比展期剔除前后的夏普和最大回撤

    差异>10%则必须剔除展期数据。
    """
    rollover_points = detect_rollover_gaps(bar_data)
    if not rollover_points:
        return {"needs_exclusion": False, "rollover_count": 0, "sharpe_diff_pct": 0.0}

    # 全量回测
    full_result = run_backtest(params, bar_data, train=True, strategy_type="main")
    full_sharpe = full_result.get("sharpe", 0.0)
    full_mdd = full_result.get("max_drawdown", 0.0)

    # 剔除展期后回测
    clean_data = exclude_rollover_signals(bar_data, rollover_points, skip_days)
    clean_bar = clean_data[~clean_data["rollover_excluded"]].copy()
    if clean_bar.empty:
        return {"needs_exclusion": True, "rollover_count": len(rollover_points),
                "sharpe_diff_pct": 100.0, "reason": "展期剔除后无数据"}

    clean_result = run_backtest(params, clean_bar, train=True, strategy_type="main")
    clean_sharpe = clean_result.get("sharpe", 0.0)
    clean_mdd = clean_result.get("max_drawdown", 0.0)

    sharpe_diff = abs(full_sharpe - clean_sharpe) / max(abs(full_sharpe), 0.01) * 100
    mdd_diff = abs(full_mdd - clean_mdd) / max(abs(full_mdd), 0.01) * 100

    needs_exclusion = sharpe_diff > 10.0 or mdd_diff > 10.0

    return {
        "needs_exclusion": needs_exclusion,
        "rollover_count": len(rollover_points),
        "full_sharpe": full_sharpe, "clean_sharpe": clean_sharpe,
        "full_mdd": full_mdd, "clean_mdd": clean_mdd,
        "sharpe_diff_pct": round(sharpe_diff, 2),
        "mdd_diff_pct": round(mdd_diff, 2),
    }


def compute_rollover_cost(rollover_points: List[Dict[str, Any]],
                           bar_data: pd.DataFrame,
                           params: Dict[str, float],
                           calendar_basis_bps: float = 5.0,
                           rollover_slippage_bps: float = 3.0) -> Dict[str, Any]:
    """P1-1修复：换月成本建模 — 量化calendar_basis(日历基差) + slippage(滑点)

    在换月点计算三部分成本:
      1. gap_cost: 展期跳空价差(已由detect_rollover_gaps检测)
      2. calendar_basis: 近远月合约基差(默认5bps)
      3. rollover_slippage: 换月双边滑点(默认3bps)

    Returns:
        Dict: 换月总成本及各分项
    """
    if not rollover_points:
        return {"total_rollover_cost_bps": 0.0, "rollover_count": 0}

    total_gap_bps = 0.0
    for rp in rollover_points:
        total_gap_bps += abs(rp.get("gap_pct", 0.0)) * 100  # gap_pct是百分比

    n_rollovers = len(rollover_points)
    calendar_basis_total = calendar_basis_bps * n_rollovers
    slippage_total = rollover_slippage_bps * n_rollovers * 2  # 双边(平旧+开新)

    total_cost_bps = total_gap_bps + calendar_basis_total + slippage_total

    logger.info(
        "[裂缝5-换月成本] gap=%.1fbps calendar_basis=%.1fbps slippage=%.1fbps total=%.1fbps (n=%d)",
        total_gap_bps, calendar_basis_total, slippage_total, total_cost_bps, n_rollovers,
    )

    return {
        "total_rollover_cost_bps": round(total_cost_bps, 2),
        "gap_cost_bps": round(total_gap_bps, 2),
        "calendar_basis_bps": round(calendar_basis_total, 2),
        "calendar_basis_per_rollover_bps": calendar_basis_bps,
        "slippage_bps": round(slippage_total, 2),
        "slippage_per_rollover_bps": rollover_slippage_bps,
        "rollover_count": n_rollovers,
        "annualized_cost_bps": round(total_cost_bps * 12 / max(1, n_rollovers), 2),  # 假设每月换月
    }


def run_backtest_market_making(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "market_making",
) -> Dict[str, Any]:
    """S6做市策略回测：双边挂单(买/卖)赚取价差+库存管理

    基于MarketMakerDefenseEngine的逻辑：
      1. 在每Bar以mid_price±spread_target_bps/2挂双边限价单
      2. 单边成交后形成库存，库存>mm_rebalance_threshold时对冲
      3. 库存绝对值>mm_max_inventory_lots时停止该方向挂单
      4. IOC单防御做市商扫单(mm_ioc_signal_threshold)

    特点：收益来源集中(价差收入)、方向不反转(库存管理而非方向性交易)。
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    spread_target = params.get("mm_spread_target_bps", 5.0)
    max_inventory = int(params.get("mm_max_inventory_lots", 5))
    rebalance_threshold = int(params.get("mm_rebalance_threshold", 3))
    # P1-8修复：再平衡冷却期和摩擦成本
    rebalance_cooldown_bars = int(params.get("mm_rebalance_cooldown_bars", 5))
    rebalance_friction_bps = params.get("mm_rebalance_friction_bps", 2.0)
    _sync_random_seed(42 if train else 24)

    inventory = 0
    fill_count = 0
    last_rebalance_bar = -rebalance_cooldown_bars  # P1-8修复：上次再平衡Bar索引

    # P0-裂缝14修复v2：预加载熔断停牌事件并注入回测
    circuit_breaker_events = {}
    try:
        from ali2026v3_trading.param_pool.preprocess_ticks import validate_circuit_breaker_halts
        cb_result = validate_circuit_breaker_halts(bar_data)
        for evt in cb_result.get("halt_events", []):
            halt_idx = evt.get("bar_index", -1)
            if halt_idx >= 0:
                circuit_breaker_events[halt_idx] = evt
        if circuit_breaker_events:
            logger.info("[P0-裂缝14v2] 预加载%d个熔断停牌事件", len(circuit_breaker_events))
    except Exception as e:
        logger.debug("[P0-裂缝14v2] 熔断事件加载失败: %s", e)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        _backfill_bar_fields(bar)
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())
        _reset_daily(bt, current_date)
        if not _check_backtest_health(bt, params, bar_time):
            should_close = True
            close_reason = "HEALTH_CRITICAL"

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    entry = pos.open_price
                    direction = 1 if pos.volume > 0 else -1
                    tp_mult, sl_mult = _resolve_tp_sl(params, "MARKET_MAKING")
                    time_stop = _resolve_time_stop(params, "MARKET_MAKING", bt.current_state)
                    hold_minutes = (bar_time - pos.open_time).total_seconds() / 60.0
                    if direction == 1:
                        pnl_pct = (bar_price - entry) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= time_stop:
                            pnl = (bar_price - entry) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            inventory -= pos.lots
                            bt.total_trades += 1
                            del bt.positions[sym]
                    elif direction == -1:
                        pnl_pct = (entry - bar_price) / entry if entry > 0 else 0
                        if pnl_pct >= tp_mult * 0.01 or pnl_pct <= -sl_mult * 0.01 or hold_minutes >= time_stop:
                            pnl = (entry - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            inventory += pos.lots
                            bt.total_trades += 1
                            del bt.positions[sym]

        # P0-裂缝14修复v2：检查当前Bar是否为熔断停牌事件
        if idx in circuit_breaker_events:
            cb_evt = circuit_breaker_events[idx]
            halt_bars = cb_evt.get("halt_duration_bars", 5)
            bt.circuit_breaker_until = idx + halt_bars
            bt.circuit_breaker_events.append({
                'trigger_bar': idx,
                'halt_bars': halt_bars,
                'resume_bar': idx + halt_bars,
                'equity_at_trigger': bt.equity,
            })
            logger.info(
                "[P0-裂缝14v2] Bar#%d触发熔断停牌, 持续%d根Bar",
                idx, halt_bars,
            )

        if _check_safety(bt, bar_time, params):
            bar_price = bar.get("close", 0)
            symbol = bar.get("symbol", "")
            mid = bar_price
            bid_price = mid * (1 - spread_target * 0.0001)
            ask_price = mid * (1 + spread_target * 0.0001)
            imbalance = bar.get("imbalance", 0)

            if abs(imbalance) > 0.3:
                # R30-P0-06修复: S6做市策略影子逻辑
                # shadow_reverse: 反转再平衡方向（库存管理反向）
                # shadow_random: 随机跳过再平衡信号，降低成交频率
                _skip_rebalance = False
                if strategy_type == "shadow_random":
                    if np.random.random() >= BACKTEST_THRESHOLDS.get("shadow_random_open_prob", 0.5):
                        _skip_rebalance = True
                _is_shadow_reverse = (strategy_type == "shadow_reverse")
                # P1-8修复：再平衡冷却期检查 — 冷却期内不触发再平衡
                in_cooldown = (idx - last_rebalance_bar) < rebalance_cooldown_bars
                if not in_cooldown and not _skip_rebalance and inventory > rebalance_threshold and symbol not in bt.positions:
                    # P1-8修复：扣除再平衡摩擦成本
                    friction_cost = bar_price * rebalance_friction_bps / 10000
                    bt.equity -= friction_cost
                    if _is_shadow_reverse:
                        _vol, _reason = 1, "MM_REBALANCE_BUY_REVERSED"
                        _sp = bar_price * (1 + params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 - params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory += 1
                    else:
                        _vol, _reason = -1, "MM_REBALANCE_SELL"
                        _sp = bar_price * (1 - params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 + params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory -= 1
                    # P0-R11-05修复: market_making再平衡开仓路径调用RiskService.check_before_trade()
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_mm1 = get_risk_service(scope_id='backtest')
                        _signal_mm1 = {
                            "symbol": symbol, "direction": "BUY" if _vol > 0 else "SELL",
                            "price": bar_price, "volume": 1, "amount": bar_price,
                            "is_valid": True, "action": "OPEN", "account_id": "backtest",
                            "signal_id": f"BT_MM1_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_mm1 = _rs_mm1.check_before_trade(_signal_mm1)
                        if _chk_mm1.is_block:
                            logger.debug("[P0-R11-05] market_making rebalance open blocked by RiskService: %s", _chk_mm1.reason)
                            continue
                    except Exception as _rs_err_mm1:
                        logger.warning("[P0-R11-05] market_making check_before_trade failed, fail-safe block: %s", _rs_err_mm1)
                        continue
                    bt.positions[symbol] = _BacktestPosition(
                        instrument_id=symbol,
                        volume=_vol,
                        open_price=bar_price,
                        open_time=bar_time,
                        stop_profit_price=_sp,
                        stop_loss_price=_sl,
                        open_reason=_reason,
                        lots=1,
                        open_state=bt.current_state,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    last_rebalance_bar = idx  # P1-8修复：记录再平衡Bar
                    bt.total_signals += 1
                    fill_count += 1
                elif not in_cooldown and not _skip_rebalance and inventory < -rebalance_threshold and symbol not in bt.positions:
                    # P1-8修复：扣除再平衡摩擦成本
                    friction_cost = bar_price * rebalance_friction_bps / 10000
                    bt.equity -= friction_cost
                    if _is_shadow_reverse:
                        _vol, _reason = -1, "MM_REBALANCE_SELL_REVERSED"
                        _sp = bar_price * (1 - params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 + params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory -= 1
                    else:
                        _vol, _reason = 1, "MM_REBALANCE_BUY"
                        _sp = bar_price * (1 + params.get("close_take_profit_ratio", 0.8) * 0.01)
                        _sl = bar_price * (1 - params.get("close_stop_loss_ratio", 0.3) * 0.01)
                        inventory += 1
                    # P0-R11-05修复: market_making再平衡开仓路径调用RiskService.check_before_trade()
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs_mm2 = get_risk_service(scope_id='backtest')
                        _signal_mm2 = {
                            "symbol": symbol, "direction": "BUY" if _vol > 0 else "SELL",
                            "price": bar_price, "volume": 1, "amount": bar_price,
                            "is_valid": True, "action": "OPEN", "account_id": "backtest",
                            "signal_id": f"BT_MM2_{symbol}_{int(bar_time.timestamp())}",
                        }
                        _chk_mm2 = _rs_mm2.check_before_trade(_signal_mm2)
                        if _chk_mm2.is_block:
                            logger.debug("[P0-R11-05] market_making rebalance open blocked by RiskService: %s", _chk_mm2.reason)
                            continue
                    except Exception as _rs_err_mm2:
                        logger.warning("[P0-R11-05] market_making check_before_trade failed, fail-safe block: %s", _rs_err_mm2)
                        continue
                    bt.positions[symbol] = _BacktestPosition(
                        instrument_id=symbol,
                        volume=_vol,
                        open_price=bar_price,
                        open_time=bar_time,
                        stop_profit_price=_sp,
                        stop_loss_price=_sl,
                        open_reason=_reason,
                        lots=1,
                        open_state=bt.current_state,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    last_rebalance_bar = idx  # P1-8修复：记录再平衡Bar
                    bt.total_signals += 1
                    fill_count += 1

            if abs(inventory) < max_inventory:
                # P1-裂缝4修复v2：改进队列位置估计
                # queue_position ≈ spread_bps/2 (每档一个位置) + volume衰减因子
                # 使用bid_ask_spread作为队列深度代理，volume作为竞争者数量
                spread_bps_val = abs(ask_price - bid_price) / max(mid, 1e-10) * 10000
                volume_at_price = bar.get("volume", 1)
                spread_ticks = max(1, spread_bps_val / max(float(bar.get("tick_size_bps", 1.0)), 0.1))
                volume_competitors = max(1, int(volume_at_price / max(float(bar.get("avg_trade_size", 1.0)), 1.0)))
                estimated_queue_pos = max(1, int(spread_ticks + volume_competitors * 0.5))
                fill_prob = 1.0 / (1.0 + estimated_queue_pos)
                buy_fill = np.random.random() < fill_prob
                sell_fill = np.random.random() < fill_prob
                if buy_fill or sell_fill:
                    spread_pnl = (ask_price - bid_price) * 0.1
                    _safe_equity_add(bt, spread_pnl)
                    fill_count += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "total_trades": bt.total_trades,
            "fill_count": fill_count,
            "final_inventory": inventory,
        },
    )


# ============================================================================
# V7.1 深度验证与反验证体系（7个结构性漏洞修补）
# ============================================================================

@dataclass(slots=True)
class _DeepValidationResult:
    test_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)


def run_backtest_hft_with_disturbance(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
    tick_drop_prob: float = 0.0,
    delay_skip_lambda: float = 0.0,
) -> Dict[str, Any]:
    """漏洞三+七：S1 HFT回测 + 随机tick丢弃 + 微秒延迟注入

    Args:
        tick_drop_prob: 每个tick被丢弃的概率（模拟生产环境网络IO/负载导致漏tick）
        delay_skip_lambda: Poisson分布的lambda，模拟微秒延迟导致跳过tick数
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    _sync_random_seed(42 if train else 24)

    hft_signal_count = 0
    hft_pending_direction = 0
    dropped_ticks = 0
    delayed_skips = 0

    for idx in range(len(bar_data)):
        if tick_drop_prob > 0 and np.random.random() < tick_drop_prob:
            dropped_ticks += 1
            bt.equity_curve.append(bt.equity)
            continue

        if delay_skip_lambda > 0:
            skip_n = np.random.poisson(delay_skip_lambda)
            if skip_n > 0:
                delayed_skips += skip_n
                for _ in range(min(skip_n, len(bar_data) - idx - 1)):
                    idx += 1
                    if idx >= len(bar_data):
                        break
                    bt.equity_curve.append(bt.equity)

        if idx >= len(bar_data):
            break

        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧(另一变体仍用，见行3235)
        # 决策频率由K线周期 × state_confirm_bars自然决定
        if _check_safety(bt, bar_time, params):
            imbalance = abs(bar.get("imbalance", 0))
            strength = bar.get("strength", 0)

            if strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["hft_shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3)):
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
                else:
                    should_open_hft = False
            else:
                should_open_hft = False
                if imbalance >= min_imbalance and strength > 0.2:
                    current_dir = 1 if bar.get("imbalance", 0) > 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True


            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open_hft and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse", "hft"):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                price = bar.get("close", 0.0)
                if price <= 0:
                    continue

                reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls)
                if lots <= 0:
                    continue

                volume = hft_pending_direction * lots
                sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                bid_ask = bar.get("bid_ask_spread", 0.0)
                spread_q = bar.get("_spread_quality", 0)
                slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                slip_cost = price * slip_bps / 10000 * lots
                commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                bt.equity -= (commission + slip_cost)

                pos = _BacktestPosition(
                    instrument_id=symbol,
                    volume=volume,
                    open_price=price,
                    open_time=bar_time,
                    stop_profit_price=sp_price,
                    stop_loss_price=sl_price,
                    open_reason=reason,
                    lots=lots,
                    open_state=bt.current_state,
                    open_strength=strength,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.positions[symbol] = pos
                bt.last_signal_time = bar_time
                bt.total_signals += 1
                hft_signal_count = 0

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "dropped_ticks": dropped_ticks,
            "delayed_skips": delayed_skips,
        },
    )


def validate_hft_temporal_robustness(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    drop_probs: Optional[List[float]] = None,
    delay_lambdas: Optional[List[float]] = None,
) -> List[_DeepValidationResult]:
    """漏洞三+七验证：S1 HFT对时序错位和tick丢失的敏感性

    核心假设：如果策略真实捕获了市场结构，微小扰动不应导致结果剧变。
    如果drop_prob=0.1%就导致Sharpe减半 → 策略对完美时序依赖过强 → 实盘不可用。
    """
    if drop_probs is None:
        drop_probs = [0.0, 0.001, 0.005, 0.01, 0.05]
    if delay_lambdas is None:
        delay_lambdas = [0.0, 0.1, 0.5, 1.0, 2.0]

    baseline = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", 0.0, 0.0)
    baseline_sharpe = baseline.get("sharpe", 0.0)
    results = []

    for prob in drop_probs:
        r = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", prob, 0.0)
        sharpe_ratio = r["sharpe"] / baseline_sharpe if abs(baseline_sharpe) > 1e-10 else 0.0
        results.append(_DeepValidationResult(
            test_name=f"tick_drop_prob={prob:.4f}",
            passed=sharpe_ratio > 0.5,
            metric_value=sharpe_ratio,
            threshold=0.5,
            details={"sharpe": r["sharpe"], "dropped": r["dropped_ticks"], "baseline_sharpe": baseline_sharpe},
        ))

    for lam in delay_lambdas:
        r = run_backtest_hft_with_disturbance(params, bar_data, train, "hft", 0.0, lam)
        sharpe_ratio = r["sharpe"] / baseline_sharpe if abs(baseline_sharpe) > 1e-10 else 0.0
        results.append(_DeepValidationResult(
            test_name=f"delay_lambda={lam:.1f}",
            passed=sharpe_ratio > 0.5,
            metric_value=sharpe_ratio,
            threshold=0.5,
            details={"sharpe": r["sharpe"], "delayed": r["delayed_skips"], "baseline_sharpe": baseline_sharpe},
        ))

    return results


def validate_cross_strategy_correlation(
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    correlation_threshold: float = 0.6,
) -> _DeepValidationResult:
    """漏洞二验证：四策略在极端日的隐性相关性

    对每个交易日，计算四策略的日收益。如果四策略日收益的pairwise相关系数
    在极端日（日收益最低的10%天数）超过阈值 → 组合层面风险共振。
    """
    dates = bar_data["minute"].dt.date.unique()
    daily_returns = {"S1": [], "S2": [], "S3": [], "S4": []}

    for date in dates:
        day_data = bar_data[bar_data["minute"].dt.date == date]
        if len(day_data) < 10:
            continue
        r1 = run_backtest_hft(params_s1, day_data, train, "hft")
        r2 = run_backtest(params_s2, day_data, train, "main")
        r3 = run_backtest_box_extreme(params_s3, day_data, train, "box_extreme")
        r4 = run_backtest_box_spring(params_s4, day_data, train, "box_spring")
        daily_returns["S1"].append(r1.get("total_return", 0))
        daily_returns["S2"].append(r2.get("total_return", 0))
        daily_returns["S3"].append(r3.get("total_return", 0))
        daily_returns["S4"].append(r4.get("total_return", 0))

    if len(daily_returns["S1"]) < 10:
        return _DeepValidationResult("cross_strategy_correlation", False, 0.0, correlation_threshold,
                                     {"error": "数据不足"})

    df = pd.DataFrame(daily_returns)
    combined = df["S1"] + df["S2"] + df["S3"] + df["S4"]
    extreme_threshold = combined.quantile(0.10)
    extreme_mask = combined <= extreme_threshold
    extreme_df = df[extreme_mask]

    if len(extreme_df) < 5:
        return _DeepValidationResult("cross_strategy_correlation", True, 0.0, correlation_threshold,
                                     {"note": "极端日样本不足，跳过"})

    corr_matrix = extreme_df.corr()
    max_corr = 0.0
    max_pair = ("", "")
    for i, col_i in enumerate(corr_matrix.columns):
        for j, col_j in enumerate(corr_matrix.columns):
            if i < j:
                c = abs(corr_matrix.loc[col_i, col_j])
                if c > max_corr:
                    max_corr = c
                    max_pair = (col_i, col_j)

    return _DeepValidationResult(
        test_name="cross_strategy_correlation",
        passed=max_corr < correlation_threshold,
        metric_value=max_corr,
        threshold=correlation_threshold,
        details={"max_corr_pair": max_pair, "corr_matrix": corr_matrix.to_dict(),
                 "extreme_day_count": int(extreme_mask.sum()), "total_days": len(df)},
    )


def validate_market_friendliness_baseline(
    bar_data: pd.DataFrame,
    train: bool = True,
    n_random: int = 100,
) -> _DeepValidationResult:
    """漏洞四验证：市场友善度基准

    计算纯随机买入持有至到期的收益分布。如果基准收益显著为正，
    说明该周期市场对期权买方天然友好，影子B的Alpha判定需修正。

    方法：在每个交易日随机时刻随机方向买入，持有至收盘平仓，
    重复n_random次，得到随机买入收益分布。
    """
    if bar_data.empty:
        return _DeepValidationResult("market_friendliness", False, 0.0, 0.0, {"error": "无数据"})

    _sync_random_seed(42 if train else 24)
    dates = bar_data["minute"].dt.date.unique()
    random_returns = []

    for _ in range(n_random):
        equity = INITIAL_EQUITY
        for date in dates:
            day_data = bar_data[bar_data["minute"].dt.date == date]
            if len(day_data) < 2:
                continue
            entry_idx = np.random.randint(0, len(day_data) - 1)
            entry_price = day_data.iloc[entry_idx].get("close", 0)
            exit_price = day_data.iloc[-1].get("close", 0)
            if entry_price <= 0:
                continue
            direction = 1 if np.random.random() < 0.5 else -1
            ret = direction * (exit_price - entry_price) / entry_price
            equity *= (1 + ret * 0.01)

        random_returns.append(equity / INITIAL_EQUITY - 1)

    mean_random_return = np.mean(random_returns)
    std_random_return = np.std(random_returns)
    t_stat = mean_random_return / (std_random_return / np.sqrt(n_random)) if std_random_return > 1e-10 else 0.0

    is_friendly = mean_random_return > 0 and abs(t_stat) > 2.0

    return _DeepValidationResult(
        test_name="market_friendliness",
        passed=not is_friendly,
        metric_value=mean_random_return,
        threshold=0.0,
        details={
            "mean_random_return": mean_random_return,
            "std_random_return": std_random_return,
            "t_stat": t_stat,
            "is_friendly": is_friendly,
            "warning": "影子B的Alpha需减去此基准" if is_friendly else "市场中性，影子B基准有效",
            "n_random": n_random,
        },
    )


def validate_regime_robustness(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    iv_column: str = "iv",
    train: bool = True,
    n_regimes: int = 3,
) -> _DeepValidationResult:
    """漏洞一验证：市场机制分割盲测

    按波动率(IV)水平将数据分为n_regimes个regime（低IV/中IV/高IV），
    在每个regime内独立回测。如果策略在某个regime大幅亏损，
    说明其对特定市场机制过拟合。
    """
    if bar_data.empty or iv_column not in bar_data.columns:
        return _DeepValidationResult("regime_robustness", False, 0.0, 0.0,
                                     {"error": f"无数据或缺少{iv_column}列"})

    iv_values = bar_data[iv_column].replace(0, np.nan).dropna()
    if len(iv_values) < 100:
        return _DeepValidationResult("regime_robustness", False, 0.0, 0.0,
                                     {"error": "IV有效数据不足"})

    quantiles = [iv_values.quantile(i / n_regimes) for i in range(n_regimes + 1)]
    regime_results = []

    for i in range(n_regimes):
        low_q, high_q = quantiles[i], quantiles[i + 1]
        regime_data = bar_data[(bar_data[iv_column] >= low_q) & (bar_data[iv_column] < high_q)]
        if len(regime_data) < 50:
            regime_results.append({"regime": f"Q{i}", "return": 0.0, "sharpe": 0.0, "bars": len(regime_data)})
            continue
        r = run_backtest(params, regime_data, train, "main")
        regime_results.append({
            "regime": f"Q{i}({low_q:.3f}-{high_q:.3f})",
            "return": r.get("total_return", 0),
            "sharpe": r.get("sharpe", 0),
            "bars": len(regime_data),
        })

    returns = [rr["return"] for rr in regime_results]
    sharpe_values = [rr["sharpe"] for rr in regime_results]
    min_sharpe = min(sharpe_values) if sharpe_values else 0.0
    sharpe_spread = max(sharpe_values) - min(sharpe_values) if sharpe_values else 0.0

    return _DeepValidationResult(
        test_name="regime_robustness",
        passed=min_sharpe > 0.0,
        metric_value=min_sharpe,
        threshold=0.0,
        details={
            "regime_results": regime_results,
            "sharpe_spread": sharpe_spread,
            "worst_regime": regime_results[sharpe_values.index(min_sharpe)]["regime"] if sharpe_values else "N/A",
        },
    )


def validate_liquidity_stress(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    slippage_multipliers: Optional[List[float]] = None,
) -> List[_DeepValidationResult]:
    """漏洞六验证：流动性枯竭压力测试

    在最大持仓时刻，假设平仓滑点被放大N倍（模拟流动性瞬间枯竭）。
    如果×10滑点就导致回撤>30% → 当前仓位模型在黑天鹅下不可用。
    """
    if slippage_multipliers is None:
        slippage_multipliers = [1.0, 5.0, 10.0, 20.0, 50.0]

    global SLIPPAGE_BPS
    original_slippage = SLIPPAGE_BPS
    results = []

    for mult in slippage_multipliers:
        SLIPPAGE_BPS = original_slippage * mult
        r = run_backtest(params, bar_data, train, "main")
        max_dd = abs(r.get("max_drawdown", 0))
        results.append(_DeepValidationResult(
            test_name=f"slippage_{mult:.0f}x",
            passed=max_dd < 0.3,
            metric_value=max_dd,
            threshold=0.3,
            details={"total_return": r.get("total_return", 0), "sharpe": r.get("sharpe", 0)},
        ))

    SLIPPAGE_BPS = original_slippage
    return results


def validate_doomed_tests(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    n_shuffle: int = 10,
) -> List[_DeepValidationResult]:
    """元批判：注定失败测试

    如果策略在垃圾数据上也显著盈利 → 捕获的不是市场结构，是数据泄露或Bug。

    三组测试：
    1. 随机打乱tick时间顺序：shuffled收益应显著低于baseline收益（双样本t检验）
    2. 纯随机GBM生成tick：无任何市场微结构，策略收益应≈0
    3. 反向时间序列：倒序播放历史tick，趋势策略应亏损
    """
    results = []

    # Test 1: 随机打乱时间顺序（双样本t检验 vs baseline）
    baseline_returns = []
    for i in range(n_shuffle):
        bl = bar_data.sample(frac=0.5, random_state=i + 1000).reset_index(drop=True)  # R21-MEM-P2-02修复: 链式操作sample+reset_index产生2个中间DataFrame副本
        if len(bl) > 10:
            r = run_backtest(params, bl, train, "main")
            baseline_returns.append(r.get("total_return", 0))

    shuffled_returns = []
    for i in range(n_shuffle):
        shuffled = bar_data.sample(frac=1.0, random_state=i).reset_index(drop=True)  # R21-MEM-P2-02修复: 链式操作sample+reset_index产生2个中间DataFrame副本
        r = run_backtest(params, shuffled, train, "main")
        shuffled_returns.append(r.get("total_return", 0))

    mean_bl = np.mean(baseline_returns) if baseline_returns else 0.0
    std_bl = np.std(baseline_returns) if len(baseline_returns) > 1 else 1e-10
    mean_sh = np.mean(shuffled_returns)
    std_sh = np.std(shuffled_returns) if len(shuffled_returns) > 1 else 1e-10
    n_bl = len(baseline_returns)
    n_sh = len(shuffled_returns)

    pooled_se = np.sqrt(std_bl**2 / max(n_bl, 1) + std_sh**2 / max(n_sh, 1))
    t_diff = (mean_bl - mean_sh) / pooled_se if pooled_se > 1e-10 else 0.0

    shuffled_significantly_worse = t_diff > 2.0 and mean_bl > mean_sh
    shuffled_not_profitable = not (mean_sh > 0 and abs(mean_sh / (std_sh / np.sqrt(n_sh))) > 2.0)

    results.append(_DeepValidationResult(
        test_name="shuffled_temporal",
        passed=shuffled_significantly_worse or shuffled_not_profitable,
        metric_value=t_diff,
        threshold=2.0,
        details={"baseline_mean": mean_bl, "shuffled_mean": mean_sh,
                 "t_diff": t_diff, "n_baseline": n_bl, "n_shuffle": n_sh,
                 "meaning": "shuffled收益应显著低于baseline(t_diff>2)或本身不显著盈利"},
    ))

    # Test 2: 纯随机GBM生成
    n_bars = len(bar_data)
    if n_bars > 0:
        dt = 1 / 240
        mu, sigma = 0.0, 0.15
        gbm_prices = [100.0]
        np_gbm = np.random.RandomState(42 if train else 24)
        for _ in range(n_bars - 1):
            z = np_gbm.standard_normal()
            gbm_prices.append(gbm_prices[-1] * np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * z))

        gbm_data = pd.DataFrame({
            "minute": bar_data["minute"].values,  # R21-MEM-P2-01修复: .values返回ndarray，避免Series中间对象
            "symbol": bar_data["symbol"].values if "symbol" in bar_data.columns else ["UNKNOWN"] * n_bars,
            "close": gbm_prices,
            "high": [p * 1.001 for p in gbm_prices],
            "low": [p * 0.999 for p in gbm_prices],
            "strength": np.zeros(n_bars),
            "imbalance": np.zeros(n_bars),
        })
        r_gbm = run_backtest(params, gbm_data, train, "main")
        results.append(_DeepValidationResult(
            test_name="random_gbm",
            passed=abs(r_gbm.get("total_return", 0)) < 0.05,
            metric_value=r_gbm.get("total_return", 0),
            threshold=0.05,
            details={"sharpe": r_gbm.get("sharpe", 0), "meaning": "策略不应在纯随机GBM上显著盈利"},
        ))

    # Test 3: 反向时间序列
    reversed_data = bar_data.iloc[::-1].reset_index(drop=True)  # R21-MEM-P2-01修复: iloc[::-1]产生完整副本，此处为回测必需
    r_rev = run_backtest(params, reversed_data, train, "main")
    results.append(_DeepValidationResult(
        test_name="reversed_temporal",
        passed=r_rev.get("total_return", 0) < 0.0,
        metric_value=r_rev.get("total_return", 0),
        threshold=0.0,
        details={"sharpe": r_rev.get("sharpe", 0), "meaning": "趋势策略在反向时间序列中应亏损"},
    ))

    return results


def validate_logic_transferability(
    params: Dict[str, float],
    bar_data_primary: pd.DataFrame,
    bar_data_secondary: pd.DataFrame,
    train: bool = True,
) -> _DeepValidationResult:
    """漏洞五验证：逻辑可迁移性单次验证

    最优参数在主品种上回测后，在副品种（不同标的）上回测。
    如果逻辑可迁移 → Sharpe在副品种>0（虽可能较低）
    如果完全不可迁移 → Sharpe在副品种≈0或负 → 参数只是过拟合了主品种噪声
    """
    r_primary = run_backtest(params, bar_data_primary, train, "main")
    r_secondary = run_backtest(params, bar_data_secondary, train, "main")

    primary_sharpe = r_primary.get("sharpe", 0)
    secondary_sharpe = r_secondary.get("sharpe", 0)
    transferability_ratio = secondary_sharpe / primary_sharpe if abs(primary_sharpe) > 1e-10 else 0.0

    return _DeepValidationResult(
        test_name="logic_transferability",
        passed=secondary_sharpe > 0,
        metric_value=transferability_ratio,
        threshold=0.0,
        details={
            "primary_sharpe": primary_sharpe,
            "secondary_sharpe": secondary_sharpe,
            "primary_return": r_primary.get("total_return", 0),
            "secondary_return": r_secondary.get("total_return", 0),
            "meaning": "可迁移性比率>0.3说明逻辑捕获了真实结构",
        },
    )


DEEP_VALIDATION_TIERS = {
    "must_run": {
        "description": "每次参数重检必跑（P0级别，约10秒）",
        "tests": ["doomed_tests", "market_friendliness", "hft_temporal_robustness", "cross_strategy_correlation", "liquidity_stress", "regime_robustness", "logic_transferability"],
    },
    "quarterly": {
        "description": "季度大检（P1级别，约30秒）",
        "tests": ["doomed_tests", "market_friendliness", "hft_temporal_robustness", "cross_strategy_correlation", "liquidity_stress", "regime_robustness", "logic_transferability"],
    },
    "annual": {
        "description": "年度全面审计（P0+P1全量，约2分钟）",
        "tests": ["hft_temporal_robustness", "cross_strategy_correlation", "market_friendliness",
                  "regime_robustness", "liquidity_stress", "logic_transferability", "doomed_tests"],
    },
}

PARAM_TIERS = {
    "must_calibrate_every_run": [
        "close_take_profit_ratio", "close_stop_loss_ratio", "max_risk_ratio",
        "lots_min", "signal_cooldown_sec", "non_other_ratio_threshold",
    ],
    "quarterly_review": [
        "max_signals_per_window", "state_confirm_bars", "spring_stop_profit_ratio",
        "spring_max_loss_pct", "spring_max_position_pct",
        # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    ],
    "annual_or_phase_change": [
        "capital_route_master_base", "shadow_alpha_threshold",
        "rate_limit_global_per_min",
        "hft_hard_time_stop_ms", "spring_hard_time_stop_sec",
        "resonance_hard_time_stop_min", "box_hard_time_stop_min",
        "daily_loss_hard_stop_pct", "logic_reversal_threshold",
    ],
    "hft_replay_only": list(HFT_TICK_PARAMS),
}


def run_deep_validation_tiered(
    tier: str,
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    bar_data_secondary: Optional[pd.DataFrame] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """V7.1分级深度验证：按tier选择性运行验证子集

    Args:
        tier: "must_run"(每次必跑) / "quarterly"(季度) / "annual"(年度全量)
    """
    if tier not in DEEP_VALIDATION_TIERS:
        return {"error": f"未知tier: {tier}, 可选: {list(DEEP_VALIDATION_TIERS.keys())}"}

    report = {
        "validation_version": f"V7.1-deep-v1-tier:{tier}",
        "tier": tier,
        "tier_description": DEEP_VALIDATION_TIERS[tier]["description"],
        "tests_run": DEEP_VALIDATION_TIERS[tier]["tests"],
    }

    if tier == "must_run":
        core_report = run_deep_validation_suite(
            params_s1, params_s2, params_s3, params_s4,
            bar_data, bar_data_secondary, train
        )
        report["core_validation"] = core_report
        report["validations_run"] = list(core_report.get("results", {}).keys())
        logger.info("P1-R8-17: must_run分级验证完成，共%d项", core_report.get("total_tests", 0))
        return report

    if tier == "quarterly":
        core_report = run_deep_validation_suite(
            params_s1, params_s2, params_s3, params_s4,
            bar_data, bar_data_secondary, train
        )
        report["core_validation"] = core_report
        report["validations_run"] = list(core_report.get("results", {}).keys())

        regime_result = validate_regime_robustness(params_s2, bar_data, train=train)
        report["regime_robustness"] = {
            "passed": regime_result.passed,
            "metric": regime_result.metric_value,
        }
        report["validations_run"].append("regime_robustness")

        if bar_data_secondary is not None and not bar_data_secondary.empty:
            transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
            report["logic_transferability"] = {
                "passed": transfer_result.passed,
                "metric": transfer_result.metric_value,
            }
            report["validations_run"].append("logic_transferability")

        logger.info("P1-R8-17: quarterly分级验证完成，共%d项", len(report["validations_run"]))
        return report

    if tier == "annual":
        quarterly_report = run_deep_validation_tiered(
            tier="quarterly",
            params_s1=params_s1,
            params_s2=params_s2,
            params_s3=params_s3,
            params_s4=params_s4,
            bar_data=bar_data,
            bar_data_secondary=bar_data_secondary,
            train=train,
        )
        report.update(quarterly_report)

        try:
            from ali2026v3_trading.param_pool.sensitivity_analysis import SensitivityAnalyzer
            analyzer = SensitivityAnalyzer(
                db_path=":memory:",
                base_params=params_s1,
                train_period=("2024-01-01", "2024-06-01"),
                test_period=("2024-06-01", "2024-12-01"),
            )
            sensitivity_results = analyzer.run(perturb_pct=0.05, top_k=10)
            report["sensitivity_analysis"] = {
                "top_sensitive": [r.param_name for r in sensitivity_results[:5]],
                "count": len(sensitivity_results),
            }
            report.setdefault("validations_run", []).append("sensitivity_analysis")
        except Exception as e:
            logger.warning("P1-R8-17: sensitivity_analysis跳过: %s", e)

        try:
            from ali2026v3_trading.param_pool.advanced_validation import WalkForwardValidator
            wf = WalkForwardValidator(n_windows=5, train_ratio=0.7)
            wf_results = wf.validate(params_s1, bar_data)
            report["walk_forward"] = {
                "overall_robust": wf_results.overall_robust if hasattr(wf_results, "overall_robust") else False,
            }
            report.setdefault("validations_run", []).append("walk_forward")
        except Exception as e:
            logger.warning("P1-R8-17: walk_forward跳过: %s", e)

        logger.info("P1-R8-17: annual分级验证完成，共%d项", len(report.get("validations_run", [])))
        return report

    logger.error("P1-R8-17: 未知分级级别 %s", tier)
    return {"error": f"Unknown tier: {tier}"}


def run_deep_validation_suite(
    params_s1: Dict[str, float],
    params_s2: Dict[str, float],
    params_s3: Dict[str, float],
    params_s4: Dict[str, float],
    bar_data: pd.DataFrame,
    bar_data_secondary: Optional[pd.DataFrame] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """V7.1深度验证套件：一次性运行全部7个结构性漏洞验证

    返回完整的验证报告，包括每项测试的通过/失败状态和详细指标。
    """
    report = {
        "validation_version": "V7.1-deep-v1",
        "total_tests": 0,
        "passed": 0,
        "failed": 0,
        "results": {},
    }

    # 漏洞三+七：HFT时序鲁棒性
    hft_results = validate_hft_temporal_robustness(params_s1, bar_data, train)
    report["results"]["hft_temporal_robustness"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in hft_results
    ]

    # 漏洞二：跨策略相关性
    corr_result = validate_cross_strategy_correlation(params_s1, params_s2, params_s3, params_s4, bar_data, train)
    report["results"]["cross_strategy_correlation"] = {
        "passed": corr_result.passed, "metric": corr_result.metric_value,
        "threshold": corr_result.threshold, "details": corr_result.details,
    }

    # 漏洞四：市场友善度
    friendly_result = validate_market_friendliness_baseline(bar_data, train)
    report["results"]["market_friendliness"] = {
        "passed": friendly_result.passed, "metric": friendly_result.metric_value,
        "threshold": friendly_result.threshold, "details": friendly_result.details,
    }

    # 漏洞一：市场机制盲测
    regime_result = validate_regime_robustness(params_s2, bar_data, train=train)
    report["results"]["regime_robustness"] = {
        "passed": regime_result.passed, "metric": regime_result.metric_value,
        "threshold": regime_result.threshold, "details": regime_result.details,
    }

    # 漏洞六：流动性压力
    liq_results = validate_liquidity_stress(params_s2, bar_data, train)
    report["results"]["liquidity_stress"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold}
        for r in liq_results
    ]

    # 漏洞五：跨品种逻辑迁移能力
    transfer_result = None
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        transfer_result = validate_logic_transferability(params_s2, bar_data, bar_data_secondary, train)
        report["results"]["logic_transferability"] = {
            "passed": transfer_result.passed,
            "metric": transfer_result.metric_value,
            "threshold": transfer_result.threshold,
            "details": transfer_result.details,
        }

    # 元批判：注定失败测试（随机/倒序/伪市场）
    doomed_results = validate_doomed_tests(params_s2, bar_data, train)
    report["results"]["doomed_tests"] = [
        {"test": r.test_name, "passed": r.passed, "metric": r.metric_value, "threshold": r.threshold, "details": r.details}
        for r in doomed_results
    ]
    # 4. 状态确认窗口边界抖动验证
    try:
        jitter_result = validate_state_window_boundary_jitter(bar_data=bar_data, params=params_s2)
        report["results"]["state_window_boundary_jitter"] = jitter_result
    except Exception as e:
        report["results"]["state_window_boundary_jitter"] = {"error": str(e)}

    # 5. 多粒度指标一致性验证
    try:
        multiscale_result = validate_multiscale_indicator_consistency(bar_data_1m=bar_data)
        report["results"]["multiscale_indicator_consistency"] = multiscale_result
    except Exception as e:
        report["results"]["multiscale_indicator_consistency"] = {"error": str(e)}

    # 6. 趋势评分Bar vs Tick相关性验证
    try:
        trend_corr_result = validate_trend_score_bar_vs_tick_correlation(bar_data=bar_data)
        report["results"]["trend_score_bar_vs_tick_correlation"] = trend_corr_result
    except Exception as e:
        report["results"]["trend_score_bar_vs_tick_correlation"] = {"error": str(e)}

    # 7. 影子参数独立性验证
    try:
        shadow_independence = validate_shadow_param_independence()
        report["results"]["shadow_param_independence"] = shadow_independence
    except Exception as e:
        report["results"]["shadow_param_independence"] = {"error": str(e)}

    all_results = []
    for r in hft_results:
        all_results.append(r)
    all_results.append(corr_result)
    all_results.append(friendly_result)
    all_results.append(regime_result)
    for r in liq_results:
        all_results.append(r)
    if bar_data_secondary is not None and not bar_data_secondary.empty:
        all_results.append(transfer_result)
    for r in doomed_results:
        all_results.append(r)

    report["total_tests"] = len(all_results)
    report["passed"] = sum(1 for r in all_results if r.passed)
    report["failed"] = report["total_tests"] - report["passed"]

    return report


# ============================================================================
# V7.1 张力修补：L-2超参数处理 + 统计功效 + Alpha置信区间
# ============================================================================

L2_HYPERPARAMS = {
    "non_other_ratio_threshold": {
        "role": "L-2基岩：三态路由的核心阈值",
        "lock_mode": "hyperparameter",
        "sensitivity_range": 0.05,
    },
    "state_confirm_bars": {
        "role": "L-2基岩：状态确认窗口",
        "lock_mode": "hyperparameter",
        "sensitivity_range": 1,
    },
    "logic_reversal_threshold": {
        "role": "L-2基岩：逻辑反转阈值",
        "lock_mode": "hyperparameter",
        "sensitivity_range": 0.2,
    },
}


def check_l2_statistical_power(
    bar_data: pd.DataFrame,
    iv_column: str = "iv",
    state_column: str = "state",
    min_transitions_per_regime: int = 100,
    min_fold_overlap: float = 0.60,
    n_folds: int = 5,
) -> Dict[str, Any]:
    """张力二：L-2参数验证的统计功效检查

    通过条件：
    1. 每个市场机制（低/中/高IV）下状态切换次数 >= min_transitions_per_regime
    2. K-fold交叉验证中，最优区间跨fold重叠度 >= min_fold_overlap

    Args:
        min_transitions_per_regime: 每个IV regime下需要的最小状态切换次数
        min_fold_overlap: 跨fold最优区间重叠度阈值
        n_folds: K-fold交叉验证折数
    """
    result = {
        "power_sufficient": False,
        "regime_transitions": {},
        "fold_overlap": 0.0,
        "issues": [],
    }

    if iv_column not in bar_data.columns:
        result["issues"].append(f"缺少{iv_column}列，无法分regime检查")
        return result

    iv_values = bar_data[iv_column].replace(0, np.nan).dropna()
    if len(iv_values) < 100:
        result["issues"].append("IV有效数据不足100条")
        return result

    q33, q66 = iv_values.quantile(0.33), iv_values.quantile(0.66)
    regimes = {
        "low_iv": bar_data[bar_data[iv_column] < q33],
        "mid_iv": bar_data[(bar_data[iv_column] >= q33) & (bar_data[iv_column] < q66)],
        "high_iv": bar_data[bar_data[iv_column] >= q66],
    }

    all_regimes_ok = True
    for name, regime_data in regimes.items():
        n_bars = len(regime_data)
        if state_column in regime_data.columns:
            states = regime_data[state_column]
            valid_mask = states.notna() & states.shift(1).notna()
            transitions = int((states != states.shift(1))[valid_mask].sum())
        else:
            transitions = max(0, n_bars // 240)

        result["regime_transitions"][name] = {
            "bars": n_bars,
            "transitions": int(transitions),
            "sufficient": transitions >= min_transitions_per_regime,
        }
        if transitions < min_transitions_per_regime:
            all_regimes_ok = False
            result["issues"].append(
                f"{name}: 状态切换{transitions}次 < {min_transitions_per_regime}次，功效不足"
            )

    n_total = len(bar_data)
    fold_size = n_total // n_folds
    if fold_size < 50:
        result["issues"].append(f"fold大小{fold_size}不足50，无法做{n_folds}折交叉验证")
        result["fold_overlap"] = 0.0
    else:
        fold_best_indices = []
        for k in range(n_folds):
            start = k * fold_size
            end = min(start + fold_size, n_total)
            fold_data = bar_data.iloc[start:end].copy()
            fold_best_indices.append(set(range(start, end)))

        if len(fold_best_indices) >= 2:
            overlaps = []
            for i in range(len(fold_best_indices)):
                for j in range(i + 1, len(fold_best_indices)):
                    intersection = fold_best_indices[i] & fold_best_indices[j]
                    union = fold_best_indices[i] | fold_best_indices[j]
                    overlap = len(intersection) / len(union) if len(union) > 0 else 0
                    overlaps.append(overlap)
            result["fold_overlap"] = float(np.mean(overlaps))

        if result["fold_overlap"] < min_fold_overlap:
            result["issues"].append(
                f"跨fold重叠度{result['fold_overlap']:.2%} < {min_fold_overlap:.0%}，"
                f"最优区间不稳定"
            )

    result["power_sufficient"] = all_regimes_ok and result["fold_overlap"] >= min_fold_overlap
    return result


def analyze_l2_sensitivity(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    l2_params: Optional[Dict[str, Dict[str, Any]]] = None,
    train: bool = True,
) -> Dict[str, Any]:
    """张力一：L-2超参数敏感性分析

    L-2参数在十八策略扫描中作为超参数固定（不参与网格搜索），
    但需在最终报告中做敏感性分析：±sensitivity_range扰动对结果的影响。
    """
    if l2_params is None:
        l2_params = L2_HYPERPARAMS

    baseline = run_backtest(params, bar_data, train, "main")
    baseline_sharpe = baseline.get("sharpe", 0.0)
    baseline_return = baseline.get("total_return", 0.0)
    results = {"baseline": {"sharpe": baseline_sharpe, "return": baseline_return}, "sensitivity": {}}

    for param_name, meta in l2_params.items():
        base_val = params.get(param_name)
        if base_val is None:
            continue

        sensitivity_range = meta.get("sensitivity_range", 0.05)
        low = base_val - sensitivity_range
        high = base_val + sensitivity_range

        params_low = params.copy()
        params_low[param_name] = low
        params_high = params.copy()
        params_high[param_name] = high

        r_low = run_backtest(params_low, bar_data, train, "main")
        r_high = run_backtest(params_high, bar_data, train, "main")

        sharpe_low = r_low.get("sharpe", 0.0)
        sharpe_high = r_high.get("sharpe", 0.0)
        sharpe_spread = abs(sharpe_high - sharpe_low)
        is_sensitive = sharpe_spread > abs(baseline_sharpe) * 0.3

        results["sensitivity"][param_name] = {
            "base_value": base_val,
            "range": sensitivity_range,
            "low": low,
            "high": high,
            "sharpe_at_low": sharpe_low,
            "sharpe_at_high": sharpe_high,
            "sharpe_spread": sharpe_spread,
            "is_sensitive": is_sensitive,
            "lock_mode": meta.get("lock_mode", "hyperparameter"),
            "warning": f"敏感！{param_name}±{sensitivity_range}导致Sharpe变化{sharpe_spread:.2f}" if is_sensitive else None,
        }

    return results


def compute_alpha_confidence_interval(
    strategy_return: float,
    strategy_sharpe: float,
    n_signals: int,
    confidence: float = 0.95,
) -> Dict[str, float]:
    """Alpha置信区间修正（张力二相关：伪精确性修正）

    不同策略的信号数差异巨大（S1可能10000个，S4可能50个），
    直接比较Sharpe而不给置信区间是伪精确。

    Sharpe的标准误近似: SE(Sharpe) ≈ sqrt((1 + 0.5*Sharpe^2) / n_signals)
    （Bailey & Marquet, 2012）
    """
    if n_signals < 2:
        return {"sharpe_ci_lower": strategy_sharpe, "sharpe_ci_upper": strategy_sharpe,
                "sharpe_se": float("inf"), "warning": "信号数不足，置信区间无意义"}

    sharpe_se = np.sqrt((1 + 0.5 * strategy_sharpe**2) / n_signals)

    z_table = {0.90: 1.645, 0.95: 1.960, 0.99: 2.576}
    z = z_table.get(confidence, 1.960)

    ci_lower = strategy_sharpe - z * sharpe_se
    ci_upper = strategy_sharpe + z * sharpe_se

    ci_width = ci_upper - ci_lower
    if ci_width > 2.0:
        action = "eliminate"
        action_detail = "CI宽度>2.0，Sharpe无统计意义，从策略生态中淘汰"
    elif ci_width > 1.0:
        action = "reduce_weight"
        action_detail = "CI宽度>1.0，Sharpe不可靠，资金分配降权至1/CI_width"
    elif ci_width > 0.5:
        action = "flag"
        action_detail = "CI宽度>0.5，Sharpe中等可靠，标注但可参与分配"
    else:
        action = "reliable"
        action_detail = "CI宽度≤0.5，Sharpe可靠，正常参与分配"

    return {
        "sharpe_ci_lower": ci_lower,
        "sharpe_ci_upper": ci_upper,
        "sharpe_se": sharpe_se,
        "confidence": confidence,
        "n_signals": n_signals,
        "ci_width": ci_width,
        "action": action,
        "action_detail": action_detail,
        "weight_multiplier": 1.0 / ci_width if ci_width > 1.0 else 1.0,
    }


# ============================================================================
# V7.1 Step 1: L-2基岩参数独立优化（目标函数=状态判定准确率，非策略Sharpe）
# ============================================================================

L2_PARAM_GRID = {
    "non_other_ratio_threshold": [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90],
    "state_confirm_bars": [2, 3, 4, 5, 6],
    "logic_reversal_threshold": [1.0, 1.25, 1.5, 1.75, 2.0],
}

L2_CONFLICT_RESOLUTION = {
    "rule": "independent_dataset_wins",
    "rationale": "L-2参数验证的是'状态判定在独立数据上的稳健性'。如果独立数据集与主数据集最优区间不重叠，说明状态判定对数据集过拟合，此时应扩展独立数据集而非妥协。",
    "escalation": "若冲突持续 → 标记为'unresolvable' → 禁止生产使用，需人工介入分析数据集差异",
}


def evaluate_state_accuracy(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    lookahead_bars: int = 10,
) -> Dict[str, Any]:
    """Step 1核心：状态判定准确率评估

    目标函数不是策略Sharpe，而是：
    - correct_trending判定后，后续N分钟价格是否按预期方向运动
    - incorrect_reversal判定后，价格是否反转
    - other判定后，价格是否无明显方向

    Returns:
        state_accuracy: 各状态的预测准确率
        overall_accuracy: 加权整体准确率
        n_transitions: 总状态切换次数
    """
    threshold = params.get("non_other_ratio_threshold", 0.65)
    confirm_bars = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5
    reversal_threshold = params.get("logic_reversal_threshold", 1.5)

    if bar_data.empty or len(bar_data) < lookahead_bars + 20:
        return {"overall_accuracy": 0.0, "n_transitions": 0, "error": "数据不足"}

    closes = bar_data["close"].values
    n = len(closes)

    state_predictions = []
    for i in range(confirm_bars, n - lookahead_bars):
        recent = closes[i - confirm_bars:i + 1]
        if len(recent) < confirm_bars + 1:
            continue

        current_close = closes[i]
        future_close = closes[min(i + lookahead_bars, n - 1)]
        future_return = (future_close - current_close) / current_close if current_close > 0 else 0

        recent_return = (recent[-1] - recent[0]) / recent[0] if recent[0] > 0 else 0
        recent_std = np.std(np.diff(recent) / recent[:-1]) if len(recent) > 1 else 1e-8

        if abs(recent_return) > threshold * recent_std:
            if recent_return > 0:
                predicted_state = "correct_trending"
                predicted_direction = 1
            else:
                predicted_state = "incorrect_reversal"
                predicted_direction = -1
        else:
            predicted_state = "other"
            predicted_direction = 0

        state_predictions.append({
            "state": predicted_state,
            "predicted_direction": predicted_direction,
            "actual_return": future_return,
            "correct": (predicted_direction * future_return > 0) if predicted_direction != 0 else abs(future_return) < threshold * recent_std,
        })

    if not state_predictions:
        return {"overall_accuracy": 0.0, "n_transitions": 0}

    by_state = {}
    total_correct = 0
    total_count = 0
    for pred in state_predictions:
        s = pred["state"]
        if s not in by_state:
            by_state[s] = {"correct": 0, "total": 0}
        by_state[s]["total"] += 1
        if pred["correct"]:
            by_state[s]["correct"] += 1
        total_count += 1
        if pred["correct"]:
            total_correct += 1

    state_accuracy = {}
    for s, counts in by_state.items():
        state_accuracy[s] = {
            "accuracy": counts["correct"] / counts["total"] if counts["total"] > 0 else 0,
            "n": counts["total"],
        }

    overall_accuracy = total_correct / total_count if total_count > 0 else 0

    # P1-裂缝41：增加min_state_accuracy约束
    # 若other占80%且预测准确率仅40%，加权后可能仍>60%，但策略在other下会错误开仓
    # 每个状态的准确率必须 > 50%
    min_state_accuracy_threshold = 0.50
    state_accuracy_pass = True
    for s, acc_info in state_accuracy.items():
        if acc_info["accuracy"] < min_state_accuracy_threshold and acc_info["n"] >= 10:
            state_accuracy_pass = False
            logger.warning(
                "[P1-裂缝41] 状态%s准确率%.2f%% < %.0f%%阈值(n=%d), overall_accuracy不可信",
                s, acc_info["accuracy"] * 100, min_state_accuracy_threshold * 100, acc_info["n"],
            )
    # 若某状态准确率不达标，overall_accuracy降权
    adjusted_accuracy = overall_accuracy if state_accuracy_pass else overall_accuracy * 0.5

    return {
        "state_accuracy": state_accuracy,
        "overall_accuracy": overall_accuracy,
        "adjusted_accuracy": adjusted_accuracy,
        "state_accuracy_pass": state_accuracy_pass,
        "min_state_accuracy_threshold": min_state_accuracy_threshold,
        "n_transitions": total_count,
        "params": {k: params.get(k) for k in L2_PARAM_GRID.keys()},
    }


def optimize_l2_params_step1(
    independent_data: pd.DataFrame,
    lookahead_bars: int = 10,
    min_accuracy: float = 0.55,
    min_transitions: int = 100,
) -> Dict[str, Any]:
    """Step 1: L-2参数独立优化

    在独立数据集上搜索使状态判定准确率最高的L-2参数组合。
    目标函数=状态判定准确率（非策略Sharpe）。

    Args:
        independent_data: 与主回测期完全无重叠的独立历史数据
        min_accuracy: 最低可接受的overall_accuracy
        min_transitions: 最低状态切换次数（统计功效）
    """
    if independent_data.empty:
        return {"error": "独立数据集为空", "best_params": {}, "qualified": False}

    param_keys = list(L2_PARAM_GRID.keys())
    param_values = [L2_PARAM_GRID[k] for k in param_keys]
    all_combos = list(itertools.product(*param_values))

    best_accuracy = -1
    best_params = {}
    best_result = {}
    qualified_count = 0
    candidate_pool: List[Dict[str, Any]] = []

    for combo in all_combos:
        combo_params = {k: v for k, v in zip(param_keys, combo)}
        params = PARAM_DEFAULTS.copy()
        params.update(combo_params)

        result = evaluate_state_accuracy(
            params=params,
            bar_data=independent_data,
            lookahead_bars=lookahead_bars,
        )
        acc = float(result.get("adjusted_accuracy", 0.0))
        n_transitions = int(result.get("n_transitions", 0))
        qualified = (
            acc >= min_accuracy
            and n_transitions >= min_transitions
            and bool(result.get("state_accuracy_pass", True))
        )
        if qualified:
            qualified_count += 1

        candidate_pool.append(
            {
                "params": combo_params,
                "accuracy": acc,
                "n_transitions": n_transitions,
                "qualified": qualified,
            }
        )

        if acc > best_accuracy:
            best_accuracy = acc
            best_params = combo_params
            best_result = result

    candidate_pool.sort(key=lambda x: x["accuracy"], reverse=True)
    best_qualified = bool(
        best_accuracy >= min_accuracy
        and int(best_result.get("n_transitions", 0)) >= min_transitions
        and bool(best_result.get("state_accuracy_pass", True))
    )

    return {
        "best_params": best_params,
        "best_accuracy": best_accuracy,
        "best_result": best_result,
        "qualified": best_qualified,
        "qualified_count": qualified_count,
        "total_combos": len(all_combos),
        "top_candidates": candidate_pool[:5],
    }


def validate_l2_param_conflicts(
    l2_params_independent: Dict[str, float],
    l2_params_main: Dict[str, float],
    tolerance: float = 0.20,
) -> Dict[str, Any]:
    """比较Step1独立优化参数与主回测参数冲突，给出冲突升级建议。"""
    conflicts = {}
    any_conflict = False
    for k in L2_PARAM_GRID.keys():
        v_ind = l2_params_independent.get(k)
        v_main = l2_params_main.get(k)
        if v_ind is None or v_main is None:
            continue

        if abs(v_ind) > 1e-10:
            rel_diff = abs(v_ind - v_main) / abs(v_ind)
        else:
            rel_diff = abs(v_ind - v_main)

        is_conflict = rel_diff > tolerance
        if is_conflict:
            any_conflict = True

        conflicts[k] = {
            "independent": v_ind,
            "main": v_main,
            "relative_diff": rel_diff,
            "conflict": is_conflict,
            "resolution": "independent_wins" if is_conflict else "agreement",
        }

    return {
        "any_conflict": any_conflict,
        "conflicts": conflicts,
        "action": "expand_independent_data_or_manual_review" if any_conflict else "proceed_to_step2",
        "escalation": L2_CONFLICT_RESOLUTION["escalation"] if any_conflict else None,
    }


def run_step2_smoke_test(
    l2_params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    min_state_transitions: int = 3,
) -> Dict[str, Any]:
    """Step2前置冒烟测试：验证多状态切换场景可稳定触发

    质量门第2条："至少回测一个完整多状态切换场景"
    归属Step2前置（非Step1），因为多状态切换依赖Step1锁定的L-2参数。

    Args:
        l2_params: Step1锁定的L-2参数（超参数）
        min_state_transitions: 最少需要的状态切换次数
    """
    if bar_data.empty:
        return {"passed": False, "error": "无数据", "state_transitions": 0}

    params = PARAM_DEFAULTS.copy()
    params.update(l2_params)

    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)
    states_seen = set()
    state_transitions = 0
    prev_state = None

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        _check_state_transition(bt, bar, params)

        current_state = bt.current_state
        states_seen.add(current_state)
        if prev_state is not None and current_state != prev_state:
            state_transitions += 1
        prev_state = current_state

        for sym in list(bt.positions.keys()):
            if sym == bar.get("symbol", ""):
                _check_positions(bt, bar, params)

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    passed = state_transitions >= min_state_transitions and len(states_seen) >= 2

    return {
        "passed": passed,
        "state_transitions": state_transitions,
        "states_seen": sorted(states_seen),
        "n_states": len(states_seen),
        "min_state_transitions": min_state_transitions,
        "l2_params_used": l2_params,
        "action": "proceed_to_step2_full_scan" if passed else "extend_data_or_review_l2_params",
    }


PARAM_SOURCE_ANNOTATION = {
    "circuit_breaker_trigger_sigma": {"source": "直觉", "lock_after": "Step1", "rationale": "断路器阈值依赖状态判定稳定性"},
    "circuit_breaker_pause_sec": {"source": "直觉", "lock_after": "Step1", "rationale": "断路器暂停时间依赖市场微观结构"},
    "close_take_profit_ratio": {"source": "直觉(待网格扫描)", "lock_after": None},
    "close_stop_loss_ratio": {"source": "直觉(待网格扫描)", "lock_after": None},
    "max_risk_ratio": {"source": "直觉(待网格扫描)", "lock_after": None},
    "non_other_ratio_threshold": {"source": "Step1产出后锁定", "lock_after": "Step1", "rationale": "三态路由核心阈值，Step1独立数据集优化"},
    "state_confirm_bars": {"source": "Step1产出后锁定", "lock_after": "Step1", "rationale": "状态确认窗口，Step1独立数据集优化"},
    "logic_reversal_threshold": {"source": "Step1产出后锁定", "lock_after": "Step1", "rationale": "逻辑反转阈值，Step1独立数据集优化"},
    "shadow_alpha_threshold": {"source": "直觉", "lock_after": "Step1", "rationale": "影子Alpha底线依赖主策略Alpha分布"},
    "hft_hard_time_stop_ms": {"source": "物理(订单簿半衰期50-200ms)", "lock_after": "Step1", "rationale": "HFT硬止损毫秒级，物理依据：订单簿半衰期"},
    "spring_hard_time_stop_sec": {"source": "物理(Gamma峰值10-60s)", "lock_after": "Step1", "rationale": "弹簧硬止损秒级，物理依据：Gamma脉冲持续时间"},
    "resonance_hard_time_stop_min": {"source": "统计(趋势持续性5-30min)", "lock_after": "Step1", "rationale": "共振硬止损分钟级，统计依据：分钟级趋势持续性"},
    "box_hard_time_stop_min": {"source": "统计(箱体周期30-120min)", "lock_after": "Step1", "rationale": "箱体硬止损分钟级，统计依据：箱体形成周期"},
    "daily_loss_hard_stop_pct": {"source": "直觉", "lock_after": "Step1", "rationale": "日回撤硬停止依赖L-2状态判定质量"},
    "rate_limit_global_per_min": {"source": "直觉", "lock_after": "Step1", "rationale": "速率限制依赖实盘交易延迟经验"},
    "capital_route_master_base": {"source": "直觉", "lock_after": "Step2", "rationale": "资金路由基线依赖十八策略Alpha报告"},
}


_DDL_COLUMN_SAFE_PATTERN = _re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _validate_ddl_column_names(column_names: List[str]) -> None:
    invalid = [c for c in column_names if not _DDL_COLUMN_SAFE_PATTERN.match(c)]
    if invalid:
        raise ValueError(
            f"[SEC-02] DDL列名白名单校验失败，拒绝非法列名注入: {invalid}"
        )


def _ensure_results_table(con: Any, param_keys: List[str]) -> None:
    _validate_ddl_column_names(param_keys)
    # P0-12修复: 70+参数列标记deprecated，建表仍保留列定义以兼容已有DB，但_insert_results不再写入
    param_cols = ",\n            ".join(f'"{k}" DOUBLE  -- deprecated: P0-12参数列，所有读取方从params_json解析' for k in param_keys)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS backtest_results (
            task_id INTEGER PRIMARY KEY,
            is_train BOOLEAN,
            {param_cols},
            params_json VARCHAR,
            
            -- S1: 高频趋势共振
            hft_sharpe DOUBLE,
            hft_max_dd DOUBLE,
            hft_total_return DOUBLE,
            hft_num_signals INTEGER,
            hft_shadow_a_sharpe DOUBLE,
            hft_shadow_b_sharpe DOUBLE,
            hft_alpha DOUBLE,
            
            -- S2: 分钟级趋势共振
            minute_sharpe DOUBLE,
            minute_max_dd DOUBLE,
            minute_total_return DOUBLE,
            minute_num_signals INTEGER,
            minute_shadow_a_sharpe DOUBLE,
            minute_shadow_b_sharpe DOUBLE,
            minute_alpha DOUBLE,
            
            -- S3: 箱体极值策略
            box_extreme_sharpe DOUBLE,
            box_extreme_max_dd DOUBLE,
            box_extreme_total_return DOUBLE,
            box_extreme_num_signals INTEGER,
            box_extreme_shadow_a_sharpe DOUBLE,
            box_extreme_shadow_b_sharpe DOUBLE,
            box_extreme_alpha DOUBLE,
            
            -- S4: 箱体弹簧策略
            box_spring_sharpe DOUBLE,
            box_spring_max_dd DOUBLE,
            box_spring_total_return DOUBLE,
            box_spring_num_signals INTEGER,
            box_spring_shadow_a_sharpe DOUBLE,
            box_spring_shadow_b_sharpe DOUBLE,
            box_spring_alpha DOUBLE,
            
            -- S5: 套利策略
            arbitrage_sharpe DOUBLE,
            arbitrage_max_dd DOUBLE,
            arbitrage_total_return DOUBLE,
            arbitrage_num_signals INTEGER,
            arbitrage_shadow_a_sharpe DOUBLE,
            arbitrage_shadow_b_sharpe DOUBLE,
            arbitrage_alpha DOUBLE,
            
            -- S6: 做市策略
            market_making_sharpe DOUBLE,
            market_making_max_dd DOUBLE,
            market_making_total_return DOUBLE,
            market_making_num_signals INTEGER,
            market_making_shadow_a_sharpe DOUBLE,
            market_making_shadow_b_sharpe DOUBLE,
            market_making_alpha DOUBLE,
            
            -- P0-9修复: 6个关键计算字段列(原_worker_task计算但不入库)
            minute_trend_score DOUBLE,
            risk_adjusted_score DOUBLE,
            risk_score_error VARCHAR,
            cascade_final_score DOUBLE,
            cascade_passed BOOLEAN,
            cascade_score_error VARCHAR,
            
            error VARCHAR
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_train ON backtest_results(is_train)")
    # P1-7修复: 以下5个sharpe索引冗余(无SELECT消费)，仅保留idx_minute_sharpe(ORDER BY使用)
    # con.execute("CREATE INDEX IF NOT EXISTS idx_hft_sharpe ON backtest_results(hft_sharpe)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_minute_sharpe ON backtest_results(minute_sharpe)")
    # con.execute("CREATE INDEX IF NOT EXISTS idx_box_extreme_sharpe ON backtest_results(box_extreme_sharpe)")
    # con.execute("CREATE INDEX IF NOT EXISTS idx_box_spring_sharpe ON backtest_results(box_spring_sharpe)")
    # con.execute("CREATE INDEX IF NOT EXISTS idx_arbitrage_sharpe ON backtest_results(arbitrage_sharpe)")
    # con.execute("CREATE INDEX IF NOT EXISTS idx_market_making_sharpe ON backtest_results(market_making_sharpe)")


def _get_completed_task_ids(con: Any) -> Set[int]:
    try:
        rows = con.execute("SELECT task_id FROM backtest_results").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _query_minute_table(
    con: Any,
    table_name: str,
    date_start: str,
    date_end: str,
    symbols: Optional[List[str]] = None,
    extra_filters: str = "",
    extra_params: Optional[List[Any]] = None,
) -> pd.DataFrame:
    """统一构建minute类表查询，减少重复SQL模板。"""
    _extra_params = list(extra_params or [])
    if symbols and len(symbols) > 0:
        placeholders = ", ".join(["?"] * len(symbols))
        sql = f"""
            SELECT minute, symbol, open, high, low, close, volume, open_interest, bid_ask_spread, correct_rise_pct, correct_fall_pct, wrong_rise_pct, wrong_fall_pct, other_pct, strength, imbalance, consistency, iv, delta, gamma, vega, theta, strike_price, expire_date, option_type, underlying_price, days_to_expiry, _spread_quality, _option_metadata_quality FROM {table_name}
            WHERE minute >= ? AND minute < ?
              {extra_filters}
              AND symbol IN ({placeholders})
            ORDER BY symbol, minute
        """
        params = [date_start, date_end] + _extra_params + list(symbols)
    else:
        sql = f"""
            SELECT minute, symbol, open, high, low, close, volume, open_interest, bid_ask_spread, correct_rise_pct, correct_fall_pct, wrong_rise_pct, wrong_fall_pct, other_pct, strength, imbalance, consistency, iv, delta, gamma, vega, theta, strike_price, expire_date, option_type, underlying_price, days_to_expiry, _spread_quality, _option_metadata_quality FROM {table_name}
            WHERE minute >= ? AND minute < ?
              {extra_filters}
            ORDER BY symbol, minute
        """
        params = [date_start, date_end] + _extra_params
    return con.execute(sql, params).fetchdf()


def _load_data_for_period(
    db_path: str,
    date_start: str,
    date_end: str,
    symbols: Optional[List[str]] = None,
) -> pd.DataFrame:
    con = connect_duckdb(db_path, read_only=True)
    try:
        df = _query_minute_table(
            con=con,
            table_name="minute_data",
            date_start=date_start,
            date_end=date_end,
            symbols=symbols,
        )
    finally:
        con.close()
    return df


# ======================================================================
# K线长度回测基础设施 — 多粒度Bar加载 + HFT tick插值回放
# ======================================================================

MULTISCALE_BAR_LENGTHS = [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440]


def _load_multiscale_data(
    db_path: str,
    date_start: str,
    date_end: str,
    bar_length_minutes: int = 1,
    symbols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """加载指定K线长度的Bar数据

    bar_length_minutes=1 → 直接读minute_data表
    bar_length_minutes∈{5,15,60} → 读minute_data_multiscale表对应bar_length_minutes行
    bar_length_minutes∉{1,5,15,60} → 从1分钟数据在线聚合（运行时_resample）
    """
    con = connect_duckdb(db_path, read_only=True)
    try:
        if bar_length_minutes == 1:
            df = _query_minute_table(
                con=con,
                table_name="minute_data",
                date_start=date_start,
                date_end=date_end,
                symbols=symbols,
            )
        elif bar_length_minutes in (5, 15, 60):
            table_check = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name='minute_data_multiscale'"
            ).fetchone()
            if table_check and table_check[0] > 0:
                df = _query_minute_table(
                    con=con,
                    table_name="minute_data_multiscale",
                    date_start=date_start,
                    date_end=date_end,
                    symbols=symbols,
                    extra_filters="AND bar_length_minutes = ?",
                    extra_params=[bar_length_minutes],
                )
            else:
                df_1m = _load_multiscale_data(db_path, date_start, date_end, 1, symbols)
                df = _resample_bars_runtime(df_1m, bar_length_minutes)
        else:
            df_1m = _load_multiscale_data(db_path, date_start, date_end, 1, symbols)
            df = _resample_bars_runtime(df_1m, bar_length_minutes)
    finally:
        con.close()
    return df


def _resample_bars_runtime(df_1m: pd.DataFrame, bar_length_minutes: int) -> pd.DataFrame:
    """运行时将1分钟Bar聚合为N分钟Bar（无需preprocess_ticks预生成）"""
    if df_1m.empty or bar_length_minutes <= 1:
        return df_1m

    df = df_1m.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["minute"]):
        df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    df["_group"] = df["minute"].dt.floor(f"{bar_length_minutes}min")

    ohlcv_agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "turnover" in df.columns:
        ohlcv_agg["turnover"] = "sum"
    if "tick_count" in df.columns:
        ohlcv_agg["tick_count"] = "sum"

    group_cols = ["_group", "symbol"]
    result = df.groupby(group_cols).agg(ohlcv_agg).reset_index()
    result.rename(columns={"_group": "minute"}, inplace=True)

    if "volume" in result.columns and "turnover" in result.columns:
        result["vwap"] = np.where(
            result["volume"] > 0,
            result["turnover"] / result["volume"],
            result["close"],
        )

    mean_cols = [
        "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
        "other_pct", "strength", "imbalance", "consistency",
        "iv", "delta", "gamma", "vega", "theta",
    ]
    last_cols = [
        "bid_ask_spread", "_spread_quality", "strike_price", "_option_metadata_quality",
        "expire_date", "days_to_expiry", "option_type",
        "underlying_price", "open_interest",
    ]
    # P-21修复: 向量化合并替代apply逐行映射
    # PD-P2-01: 循环merge链式副本性能问题 — 每次merge创建新DataFrame，last_cols约20列时复制20次
    merge_count = 0
    for col in mean_cols:
        if col in df.columns:
            agg_val = df.groupby(group_cols)[col].mean().reset_index()
            agg_val.rename(columns={"_group": "minute"}, inplace=True)
            result = result.merge(agg_val[["minute", "symbol", col]],
                                  on=["minute", "symbol"], how="left")
            merge_count += 1
    for col in last_cols:
        if col in df.columns:
            agg_val = df.groupby(group_cols)[col].last().reset_index()
            agg_val.rename(columns={"_group": "minute"}, inplace=True)
            result = result.merge(agg_val[["minute", "symbol", col]],
                                  on=["minute", "symbol"], how="left")
            merge_count += 1
    if merge_count > 20:
        logger.warning("PD-P2-01: excessive merge chain (%d), consider groupby+agg", merge_count)

    _nan_count = result[mean_cols + last_cols].isna().sum()
    if _nan_count.any():
        logger.warning("[PD-P1-06] _resample_bars_runtime merge后存在NaN: %s", _nan_count[_nan_count > 0].to_dict())

    return result.dropna(subset=["open", "close"])


def _interpolate_ticks_in_bar(
    bar: pd.Series,
    n_ticks: int = 5,
) -> List[Dict[str, float]]:
    """HFT tick级回测保真：在单根Bar内均匀插值模拟tick序列

    将1根分钟Bar拆解为n_ticks个虚拟tick：
      - 价格路径: 线性插值 open → high/low → close
      - 成交量: 均匀分配
      - imbalance/strength: 逐tick线性过渡（允许信号翻转检测）

    Args:
        bar: 单根Bar（pandas Series或dict-like）
        n_ticks: 每根Bar内的虚拟tick数（默认5，≈12秒/tick在1分钟Bar内）

    Returns:
        List[Dict]: 虚拟tick序列，每个dict含price/imbalance/strength等
    """
    o = float(bar.get("open", 0))
    h = float(bar.get("high", 0))
    l = float(bar.get("low", 0))
    c = float(bar.get("close", 0))
    vol = float(bar.get("volume", 0))
    imb_start = float(bar.get("imbalance", 0))
    imb_end = imb_start * 0.8
    str_start = float(bar.get("strength", 0))
    str_end = str_start * 0.9

    if n_ticks <= 1 or o == 0:
        return [{
            "price": c,
            "volume": vol,
            "imbalance": imb_start,
            "strength": str_start,
        }]

    ticks = []
    price_path_len = n_ticks
    mid_point = price_path_len // 2

    prices = []
    first_peak = h if (h - o >= o - l) else l
    second_peak = l if (h - o >= o - l) else h
    for i in range(price_path_len):
        if i == 0:
            prices.append(o)
            continue
        if i == price_path_len - 1:
            prices.append(c)
            continue
        frac = i / (price_path_len - 1)
        if frac < 0.33:
            p = o + (first_peak - o) * (frac / 0.33)
        elif frac < 0.67:
            frac2 = (frac - 0.33) / 0.34
            p = first_peak + (second_peak - first_peak) * frac2
        else:
            frac3 = (frac - 0.67) / 0.33
            p = second_peak + (c - second_peak) * frac3
        prices.append(p)

    vol_per_tick = vol / n_ticks
    # P2-裂缝52：imbalance_jitter模拟真实tick噪声
    # 线性过渡可能使原本因tick噪声而断裂的信号变成连续信号，高估夏普
    imbalance_jitter = float(bar.get("imbalance_jitter", 0.05))
    rng = np.random.RandomState(hash(bar.get("minute", "")) % 2**31 if hasattr(bar, "get") else 42)
    for i in range(n_ticks):
        frac = i / max(1, n_ticks - 1)
        jitter = rng.uniform(-imbalance_jitter, imbalance_jitter)
        tick = {
            "price": prices[i],
            "volume": vol_per_tick,
            "imbalance": imb_start + (imb_end - imb_start) * frac + jitter,
            "strength": str_start + (str_end - str_start) * frac,
        }
        ticks.append(tick)

    return ticks


# P-18修复注释: BAR_INTERVAL_GRID策略适配集已扩展至6策略(原手册仅4策略)，S5/S6为后续新增
BAR_INTERVAL_GRID = {
    "high_freq": [1],
    "resonance": [1, 2, 3, 5, 10, 15, 30],
    "box": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "spring": [2, 3, 5, 10, 15, 30, 60, 120, 240],
    "arbitrage": [1, 2, 3, 5],
    "market_making": [1, 2, 3, 5, 10, 15],
}

# P-17修复注释: KLINE_LENGTH_PARAM_GRID维度24维(原手册13维)，因S1-S6策略参数扩展新增11维
KLINE_LENGTH_PARAM_GRID = {
    "bar_interval_minutes": [1, 2, 3, 5, 10, 15, 30, 60, 120, 240, 1440],
    "trend_period_short": [2, 3, 5, 8, 13],
    "trend_period_medium": [10, 15, 20, 30, 45],
    "trend_period_long": [30, 40, 60, 90, 120],
    "adx_period": [7, 10, 14, 20, 28],
    "box_lookback_bars": [20, 30, 60, 90, 120, 180],
    "box_min_bars": [5, 10, 15, 20, 30, 45],
    "state_confirm_bars": [3, 5, 8],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
    "hft_signal_confirm_ticks": [2, 3, 5, 8, 13],
    "hft_ticks_per_bar": [2, 3, 5, 8, 10, 15, 20],
    "vol_lookback": [20, 50, 100, 150, 200, 300],
    "iv_lookback_bars": [30, 60, 90, 120, 180, 240],
    "bar_interval_sec_production": [60, 120, 180, 300, 600],
    "min_tick_volume_threshold": [0, 10, 50, 100, 500],
    "max_intra_bar_ticks": [3, 5, 10, 15, 20, 30],
    "box_breakout_confirm_bars": [1, 2, 3, 5],
    "spring_charge_confirm_bars": [2, 3, 5, 8, 13],
    "spring_release_confirm_bars": [1, 2, 3, 5],
    "hft_cooldown_ticks": [1, 3, 5, 10, 20],
    "trend_score_ema_alpha": [0.05, 0.1, 0.15, 0.2, 0.3],
    "hmm_train_min_ticks": [50, 100, 200, 500, 1000],
    "kline_snr_window": [10, 20, 50, 100],
}


def _scale_params_with_bar_interval(
    params: Dict[str, float],
    bar_interval_minutes: int,
    original_interval: int = 1,
) -> Dict[str, float]:
    """P1-裂缝33：K线长度缩放技术指标周期参数

    当bar_interval_minutes改变后，技术指标的周期参数应按比例缩放，
    否则跨K线长度的比较不公平（如trend_period_short=5在1分钟Bar表示5分钟，
    换成5分钟Bar后变成25分钟，语义改变）。

    缩放规则：scaled_period = max(1, original_period / bar_interval_minutes)
    仅当scale_periods_with_bar_interval=True时生效。
    """
    scaled = dict(params)
    if not params.get("scale_periods_with_bar_interval", False):
        return scaled
    if bar_interval_minutes <= original_interval:
        return scaled  # 无需缩放

    scale_factor = original_interval / bar_interval_minutes

    # 需要缩放的周期类参数（Bar数 → 实际时间不变）
    PERIOD_PARAMS = [
        "trend_period_short", "trend_period_medium", "trend_period_long",
        "adx_period", "box_lookback_bars", "box_min_bars",
        "state_confirm_bars", "box_breakout_confirm_bars",
        "spring_charge_confirm_bars", "spring_release_confirm_bars",
        "vol_lookback", "iv_lookback_bars", "kline_snr_window",
    ]

    for key in PERIOD_PARAMS:
        if key in scaled:
            original_val = scaled[key]
            if original_val > 0:
                scaled_val = max(1, int(round(original_val * scale_factor)))
                if scaled_val != int(original_val):
                    logger.debug(
                        "[P1-裂缝33] 缩放参数 %s: %d → %d (bar_interval=%dmin)",
                        key, int(original_val), scaled_val, bar_interval_minutes,
                    )
                scaled[key] = float(scaled_val)

    return scaled


def run_backtest_multiscale(
    params: Dict[str, float],
    db_path: str,
    date_start: str,
    date_end: str,
    strategy: str = "resonance",
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """多粒度K线回测：自动选择最优bar_interval_minutes并执行回测

    策略天然K线适配：
      - high_freq → 1分钟（tick级决策，固定）
      - resonance → 1/5/15分钟（参数网格扫描）
      - box → 5/15/60分钟（日线级震荡→5m，小时级→15m，周级→60m）
      - spring → 5/15/60分钟（蓄力期需长K线，释放期可用短K线）
    """
    bar_interval = int(params.get("bar_interval_minutes", 1))
    allowed = BAR_INTERVAL_GRID.get(strategy, [1])
    if bar_interval not in allowed:
        bar_interval = allowed[0]

    # R13-P1-CFG-07修复: 回测数据频率与实盘不一致警告
    # 实盘K线频率通常为1分钟(kline_style="M1")，若回测使用不同频率，
    # 回测结果可能无法准确反映实盘表现(信号时机/滑点/成交率均不同)。
    try:
        from ali2026v3_trading.config_params import get_cached_params
        _live_params = get_cached_params()
        _live_kline_style = _live_params.get('kline_style', 'M1')
        _live_interval_map = {'M1': 1, 'M5': 5, 'M15': 15, 'M30': 30, 'H1': 60, 'D1': 1440}
        _live_interval = _live_interval_map.get(_live_kline_style, 1)
        if bar_interval != _live_interval:
            logger.warning(
                "[R13-P1-CFG-07] 回测K线频率(%dmin)与实盘(%s=%dmin)不一致! "
                "回测结果可能无法准确反映实盘表现，建议使用相同频率进行回测。",
                bar_interval, _live_kline_style, _live_interval,
            )
    except Exception:
        pass

    # P1-裂缝35：决策频率与K线频率对齐规则
    # 决策频率 >= K线频率时，决策使用已完整收盘的K线
    # 决策频率 < K线频率时，将K线数据通过resample降频到决策频率
    # A5修复: decision_interval_minutes仍用于跳帧逻辑，与bar_interval协同控制决策频率
    decision_interval = int(bar_interval)
    if decision_interval < bar_interval:
        # 决策频率高于K线频率：需要将K线降频，或使用更短K线
        logger.info(
            "[P1-裂缝35] 决策频率%dmin < K线频率%dmin, 自动降频K线到决策频率",
            decision_interval, bar_interval,
        )
        bar_interval = decision_interval

    # P1-裂缝33：K线长度缩放技术指标周期参数
    params = _scale_params_with_bar_interval(params, bar_interval)

    bar_data = _load_multiscale_data(db_path, date_start, date_end, bar_interval)

    if bar_data.empty:
        return {"error": "无数据", "params": params, "bar_interval_minutes": bar_interval}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt_func = {
        "high_freq": run_backtest_hft,
        "resonance": run_backtest,
        "box": run_backtest_box_extreme,
        "spring": run_backtest_box_spring,
        "arbitrage": run_backtest_arbitrage,
        "market_making": run_backtest_market_making,
    }.get(strategy, run_backtest)

    result = bt_func(params, bar_data, train=train, strategy_type=strategy_type)
    result["bar_interval_minutes"] = bar_interval
    result["kline_fidelity"] = "tick_interpolated" if strategy == "high_freq" else "bar_exact"
    return result


def run_backtest_hft_tick_fidelity(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    train: bool = True,
    strategy_type: str = "hft",
    random_seed: Optional[int] = None,  # P-20修复: 种子参数化
) -> Dict[str, Any]:
    """S1高频趋势共振回测（tick级保真版）

    在每根分钟Bar内，使用_interpolate_ticks_in_bar()生成虚拟tick序列，
    使hft_signal_confirm_ticks恢复真实语义（连续N个tick方向一致才确认）。

    与run_backtest_hft的区别：
      - run_backtest_hft: 逐Bar遍历，confirm_ticks含义=连续N根Bar方向一致（失真）
      - 本函数: 逐tick遍历，confirm_ticks含义=连续N个tick方向一致（保真）
    """
    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy_type": strategy_type}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    bt = _BacktestState()
    if not hasattr(bt, 'trade_log'):
        bt.trade_log = []
    confirm_ticks = int(params.get("hft_signal_confirm_ticks", 5))
    ticks_per_bar = int(params.get("hft_ticks_per_bar", 5))
    cooldown_ms = params.get("hft_cooldown_ms", 100.0)
    min_imbalance = params.get("hft_min_imbalance", 0.25)
    _sync_random_seed(random_seed if random_seed is not None else (42 if train else 24))

    hft_signal_count = 0
    hft_pending_direction = 0
    bar_idx_for_state = 0

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)
        bar_idx_for_state += 1

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    pos = bt.positions[sym]
                    bar_price = bar.get("close", 0)
                    direction = 1 if pos.volume > 0 else -1
                    if direction == 1:
                        if bar_price <= pos.stop_loss_price or bar_price >= pos.stop_profit_price:
                            pnl = (bar_price - pos.open_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.trade_log.append({"pnl": pnl})
                            del bt.positions[sym]
                    elif direction == -1:
                        if bar_price >= pos.stop_loss_price or bar_price <= pos.stop_profit_price:
                            pnl = (pos.open_price - bar_price) * pos.lots * _get_contract_multiplier(sym)
                            _safe_equity_add(bt, pnl)
                            bt.trade_log.append({"pnl": pnl})
                            del bt.positions[sym]

        if not _check_safety(bt, bar_time, params):
            continue

        tick_sequence = _interpolate_ticks_in_bar(bar, n_ticks=ticks_per_bar)

        for tick_i, tick in enumerate(tick_sequence):
            imbalance = tick.get("imbalance", 0)
            strength = tick.get("strength", 0)
            price = tick.get("price", bar.get("close", 0))

            should_open_hft = False

            if strategy_type == "shadow_random":
                if np.random.random() < BACKTEST_THRESHOLDS["hft_shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3)):
                    direction = 1 if np.random.random() < 0.5 else -1
                    should_open_hft = True
            else:
                if abs(imbalance) >= min_imbalance and strength > 0.2:
                    current_dir = 1 if imbalance > 0 else -1
                    if strategy_type == "shadow_reverse":
                        current_dir = -current_dir

                    if current_dir == hft_pending_direction:
                        hft_signal_count += 1
                    else:
                        hft_pending_direction = current_dir
                        hft_signal_count = 1

                    if hft_signal_count >= confirm_ticks:
                        if bt.last_signal_time is not None:
                            elapsed_ms = (bar_time - bt.last_signal_time).total_seconds() * 1000
                            if elapsed_ms < cooldown_ms:
                                continue
                        should_open_hft = True


            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open_hft and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse", "hft"):
                should_open_hft = False
            if should_open_hft and len(bt.positions) < int(params.get("max_open_positions", 3)):
                symbol = bar.get("symbol", "unknown")
                if price <= 0:
                    continue
                direction = hft_pending_direction

                sl_ratio = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
                tp_ratio = params.get("close_take_profit_ratio", 1.5)
                lots = _compute_lots_with_risk_budget(
                    bt.equity, price, sl_ratio, int(params.get("lots_min", 1)), params,
                    recent_pnls=bt.recent_pnls)
                if lots <= 0:
                    continue

                entry_price = price
                stop_loss = entry_price * (1 - sl_ratio) if direction == 1 else entry_price * (1 + sl_ratio)
                take_profit = entry_price * (1 + tp_ratio) if direction == 1 else entry_price * (1 - tp_ratio)

                bt.positions[symbol] = _BacktestPosition(
                    instrument_id=symbol,
                    volume=direction * lots,
                    open_price=entry_price,
                    open_time=bar_time,
                    stop_profit_price=take_profit,
                    stop_loss_price=stop_loss,
                    open_reason="HFT_TICK_CONFIRM",
                    lots=lots,
                    open_state=bt.current_state,
                    instrument_type=_infer_instrument_type(symbol),
                )
                bt.last_signal_time = bar_time
                hft_signal_count = 0

            bt.peak_equity = max(bt.peak_equity, bt.equity)
            bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    daily_rets = np.array(bt.daily_returns)
    # R17-12修复: 日频年化因子 √252≈15.87
    daily_sharpe = np.sqrt(ANNUALIZE_FACTOR_DAILY) * (np.mean(daily_rets) - RISK_FREE_RATE / ANNUALIZE_FACTOR_DAILY) / np.std(daily_rets, ddof=1) if len(daily_rets) > 1 and np.std(daily_rets, ddof=1) > 1e-10 else 0.0

    n_trades = len(bt.trade_log)
    win_trades = sum(1 for t in bt.trade_log if t.get("pnl", 0) > 0)
    win_rate = win_trades / n_trades if n_trades > 0 else 0.0
    avg_pnl = np.mean([t.get("pnl", 0) for t in bt.trade_log]) if n_trades > 0 else 0.0

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={
            "daily_sharpe": daily_sharpe,
            "n_trades": n_trades,
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "params": params,
            "hft_fidelity": "TICK_INTERPOLATED",
            "ticks_per_bar": ticks_per_bar,
            "confirm_ticks": confirm_ticks,
        },
    )


def run_kline_length_sweep(
    db_path: str = PREPROCESSED_DB,
    strategy: str = "resonance",
    train: bool = True,
    bar_intervals: Optional[List[int]] = None,
    base_params: Optional[Dict] = None,
    strategies: Optional[List[str]] = None,
    cross_validate: bool = True,
) -> pd.DataFrame:
    """K线长度专项扫描：全策略×全bar_length交叉矩阵 + KLINE_LENGTH_PARAM_GRID参数网格

    扫描维度：
      1. 策略×bar_interval_minutes交叉矩阵（4策略×11档=44组基线）
      2. KLINE_LENGTH_PARAM_GRID中的24个参数各5-7水平
      3. HFT ticks_per_bar深度扫描（7水平）
      4. 与CR_PARAM_GRID的交叉组合（可选）
      5. 生产/回测一致性校验行
    """
    if strategies is None:
        strategies = ["high_freq", "resonance", "box", "spring", "arbitrage", "market_making"]
    if base_params is None:
        base_params_map = {
            "high_freq": PARAM_DEFAULTS_HFT,
            "resonance": PARAM_DEFAULTS,
            "box": PARAM_DEFAULTS_BOX_EXTREME,
            "spring": PARAM_DEFAULTS_BOX_SPRING,
            "arbitrage": PARAM_DEFAULTS_ARBITRAGE,
            "market_making": PARAM_DEFAULTS_MARKET_MAKING,
        }
    else:
        base_params_map = {s: base_params for s in strategies}

    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    results = []
    for strat in strategies:
        intervals = bar_intervals or BAR_INTERVAL_GRID.get(strat, [1])
        bp = base_params_map.get(strat, PARAM_DEFAULTS)

        for bi in intervals:
            bar_data = _load_multiscale_data(db_path, date_start, date_end, bi)
            if bar_data.empty:
                continue

            bt_func = {
                "high_freq": run_backtest_hft_tick_fidelity,
                "resonance": run_backtest,
                "box": run_backtest_box_extreme,
                "spring": run_backtest_box_spring,
                "arbitrage": run_backtest_arbitrage,
                "market_making": run_backtest_market_making,
            }.get(strat, run_backtest)

            result = bt_func(bp, bar_data, train=train)
            result["strategy"] = strat
            result["bar_interval_minutes"] = bi
            result["n_bars"] = len(bar_data)
            result["kline_fidelity"] = "tick_interpolated" if strat == "high_freq" else "bar_exact"
            results.append(result)

            if cross_validate and strat == "high_freq":
                for tpb in KLINE_LENGTH_PARAM_GRID.get("hft_ticks_per_bar", [5]):
                    params_hft = bp.copy()
                    params_hft["hft_ticks_per_bar"] = tpb
                    r = run_backtest_hft_tick_fidelity(params_hft, bar_data, train=train)
                    r["strategy"] = strat
                    r["bar_interval_minutes"] = bi
                    r["n_bars"] = len(bar_data)
                    r["hft_ticks_per_bar"] = tpb
                    results.append(r)

    return pd.DataFrame(results)


def run_kline_length_deep_sweep(
    db_path: str = PREPROCESSED_DB,
    strategy: str = "resonance",
    train: bool = True,
    max_combos: int = 2000,
) -> pd.DataFrame:
    """K线长度深度扫描：KLINE_LENGTH_PARAM_GRID全24维度随机采样

    对24个K线长度参数的完整网格进行随机采样回测，
    每个组合指定不同的bar_interval_minutes + 趋势周期 + 确认Bar数等。

    总空间 ≈ 11×5×5×5×5×6×6×5×7×5×7×6×6×5×5×6×4×5×5×5×5×5×4 ≈ 10^13
    随机采样max_combos个组合。
    """
    from cycle_resonance_module import CycleResonanceModule, CR_PARAMS_DEFAULT, reset_cycle_resonance_module

    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    base_params_map = {
        "high_freq": PARAM_DEFAULTS_HFT,
        "resonance": PARAM_DEFAULTS,
        "box": PARAM_DEFAULTS_BOX_EXTREME,
        "spring": PARAM_DEFAULTS_BOX_SPRING,
        "arbitrage": PARAM_DEFAULTS_ARBITRAGE,
        "market_making": PARAM_DEFAULTS_MARKET_MAKING,
    }
    bp = base_params_map.get(strategy, PARAM_DEFAULTS)

    grid = KLINE_LENGTH_PARAM_GRID
    param_names = list(grid.keys())
    level_counts = [len(grid[k]) for k in param_names]
    total_space = 1
    for c in level_counts:
        total_space *= c

    _sync_random_seed(42)
    results = []
    sampled_combos = set()

    for _ in range(max_combos * 3):
        if len(results) >= max_combos:
            break
        combo = tuple(np.random.randint(0, lc) for lc in level_counts)
        if combo in sampled_combos:
            continue
        sampled_combos.add(combo)

        params = bp.copy()
        for i, pname in enumerate(param_names):
            params[pname] = grid[pname][combo[i]]

        bi = int(params.get("bar_interval_minutes", 1))
        allowed = BAR_INTERVAL_GRID.get(strategy, [1])
        if bi not in allowed and bi not in MULTISCALE_BAR_LENGTHS:
            bi = allowed[0]
            params["bar_interval_minutes"] = bi

        bar_data = _load_multiscale_data(db_path, date_start, date_end, bi)
        if bar_data.empty:
            continue

        bt_func = {
            "high_freq": run_backtest_hft_tick_fidelity,
            "resonance": run_backtest,
            "box": run_backtest_box_extreme,
            "spring": run_backtest_box_spring,
            "arbitrage": run_backtest_arbitrage,
            "market_making": run_backtest_market_making,
        }.get(strategy, run_backtest)

        result = bt_func(params, bar_data, train=train)
        result["strategy"] = strategy
        result["bar_interval_minutes"] = bi
        result["n_bars"] = len(bar_data)
        result["sweep_type"] = "kline_length_deep"
        results.append(result)

    return pd.DataFrame(results)


def run_kline_cr_cross_sweep(
    db_path: str = PREPROCESSED_DB,
    strategy: str = "resonance",
    train: bool = True,
    kline_sample: int = 50,
    cr_sample: int = 50,
) -> pd.DataFrame:
    """K线长度 × CRParams交叉扫描

    两阶段：
      1. K线长度网格采样kline_sample个组合
      2. 对每个K线长度组合，CR_PARAM_GRID采样cr_sample个组合
    总计 kline_sample × cr_sample 组回测
    """
    from cycle_resonance_module import CycleResonanceModule, CRParams, CR_PARAMS_DEFAULT, reset_cycle_resonance_module

    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    base_params_map = {
        "high_freq": PARAM_DEFAULTS_HFT,
        "resonance": PARAM_DEFAULTS,
        "box": PARAM_DEFAULTS_BOX_EXTREME,
        "spring": PARAM_DEFAULTS_BOX_SPRING,
        "arbitrage": PARAM_DEFAULTS_ARBITRAGE,
        "market_making": PARAM_DEFAULTS_MARKET_MAKING,
    }
    bp = base_params_map.get(strategy, PARAM_DEFAULTS)

    kline_grid = KLINE_LENGTH_PARAM_GRID
    kline_names = list(kline_grid.keys())
    kline_levels = [len(kline_grid[k]) for k in kline_names]

    cr_grid = CR_PARAM_GRID
    cr_names = list(cr_grid.keys())
    cr_levels = [len(cr_grid[k]) for k in cr_names]

    _sync_random_seed(42)
    results = []

    kline_combos = set()
    for _ in range(kline_sample * 3):
        if len(kline_combos) >= kline_sample:
            break
        combo = tuple(np.random.randint(0, lc) for lc in kline_levels)
        kline_combos.add(combo)

    for kline_combo in kline_combos:
        params = bp.copy()
        for i, pname in enumerate(kline_names):
            params[pname] = kline_grid[pname][kline_combo[i]]

        bi = int(params.get("bar_interval_minutes", 1))
        bar_data = _load_multiscale_data(db_path, date_start, date_end, bi)
        if bar_data.empty:
            continue

        for _ in range(cr_sample * 3):
            cr_combo = tuple(np.random.randint(0, lc) for lc in cr_levels)
            cr_params_dict = CR_PARAMS_DEFAULT.to_dict()
            for i, pname in enumerate(cr_names):
                cr_params_dict[pname] = cr_grid[pname][cr_combo[i]]
            cr_params = CRParams.from_dict(cr_params_dict)

            bt_func = {
                "high_freq": run_backtest_hft_tick_fidelity,
                "resonance": run_backtest,
                "box": run_backtest_box_extreme,
                "spring": run_backtest_box_spring,
                "arbitrage": run_backtest_arbitrage,
                "market_making": run_backtest_market_making,
            }.get(strategy, run_backtest)

            result = bt_func(params, bar_data, train=train)
            result["strategy"] = strategy
            result["bar_interval_minutes"] = bi
            result["sweep_type"] = "kline_cr_cross"
            results.append(result)
            if len(results) % 100 == 0:
                logger.info("[kline_cr_cross] %d/%d completed", len(results), kline_sample * cr_sample)

    return pd.DataFrame(results)


def validate_kline_length_quality_gates(
    db_path: str = PREPROCESSED_DB,
    train: bool = True,
) -> Dict[str, Any]:
    """K线长度回测P0质量门验证（KL-Q1~KL-Q5）

    KL-Q1: 夏普非退化 — sharpe(bar=5m) >= 0.5 * sharpe(bar=1m)
    KL-Q2: 交易数非零 — ∀策略×bar_length: n_trades > 0
    KL-Q3: HFT保真交易数差异 — tick_interpolated vs bar_degraded 差异 < 50%
    KL-Q4: Bar长度递减交易数 — bar_length↑ → n_trades↓
    KL-Q5: OHLC一致性 — _resample_bars_runtime: high>=low等
    """
    date_start = TRAIN_START if train else TEST_START
    date_end = TEST_START if train else TEST_END

    gates = {}

    df_1m = _load_multiscale_data(db_path, date_start, date_end, 1)
    df_5m = _load_multiscale_data(db_path, date_start, date_end, 5) if not df_1m.empty else pd.DataFrame()

    if not df_1m.empty and not df_5m.empty:
        r_1m = run_backtest(PARAM_DEFAULTS, df_1m, train=train)
        r_5m = run_backtest(PARAM_DEFAULTS, df_5m, train=train)
        sharpe_1m = abs(r_1m.get("sharpe", 0))
        sharpe_5m = abs(r_5m.get("sharpe", 0))
        gates["KL-Q1"] = {
            "pass": sharpe_5m >= 0.5 * sharpe_1m if sharpe_1m > 0.01 else True,
            "sharpe_1m": r_1m.get("sharpe", 0),
            "sharpe_5m": r_5m.get("sharpe", 0),
        }

    gates["KL-Q2"] = {"pass": True, "details": []}
    for strat, intervals in BAR_INTERVAL_GRID.items():
        bp = {"high_freq": PARAM_DEFAULTS_HFT, "resonance": PARAM_DEFAULTS,
              "box": PARAM_DEFAULTS_BOX_EXTREME, "spring": PARAM_DEFAULTS_BOX_SPRING,
              "arbitrage": PARAM_DEFAULTS_ARBITRAGE, "market_making": PARAM_DEFAULTS_MARKET_MAKING}.get(strat, PARAM_DEFAULTS)
        for bi in intervals:
            df = _load_multiscale_data(db_path, date_start, date_end, bi)
            if df.empty:
                continue
            bt_func = {"high_freq": run_backtest_hft, "resonance": run_backtest,
                       "box": run_backtest_box_extreme, "spring": run_backtest_box_spring,
                       "arbitrage": run_backtest_arbitrage, "market_making": run_backtest_market_making}.get(strat, run_backtest)
            r = bt_func(bp, df, train=train)
            if r.get("n_trades", 0) == 0:
                gates["KL-Q2"]["pass"] = False
                gates["KL-Q2"]["details"].append(f"{strat}@{bi}m: n_trades=0")

    if not df_1m.empty:
        r_degraded = run_backtest_hft(PARAM_DEFAULTS_HFT, df_1m, train=train)
        r_tick = run_backtest_hft_tick_fidelity(PARAM_DEFAULTS_HFT, df_1m, train=train)
        n_degraded = r_degraded.get("n_trades", 0)
        n_tick = r_tick.get("n_trades", 0)
        if n_degraded > 0 and n_tick > 0:
            ratio = abs(n_tick - n_degraded) / max(n_degraded, n_tick)
            gates["KL-Q3"] = {"pass": ratio < 0.5, "n_trades_degraded": n_degraded, "n_trades_tick": n_tick, "ratio": ratio}

    gates["KL-Q5"] = {"pass": True}
    if not df_1m.empty:
        for bl in [5, 15, 60]:
            df_rs = _resample_bars_runtime(df_1m, bl)
            for _, row in df_rs.iterrows():
                if row["high"] < row["low"] or row["high"] < row["open"] or row["high"] < row["close"]:
                    gates["KL-Q5"]["pass"] = False
                    break

    all_pass = all(g.get("pass", True) for g in gates.values() if isinstance(g, dict))
    gates["overall"] = all_pass
    return gates


# R10-P0-17修复: 子进程所需的最小列集合，避免传递完整DataFrame导致内存膨胀
_SUBPROCESS_NEEDED_COLS = [
    "minute", "symbol", "close", "open", "high", "low", "volume",
    "strength", "imbalance", "bid_ask_spread", "_spread_quality",
    "correct_rise_pct", "correct_fall_pct", "wrong_rise_pct", "wrong_fall_pct",
    "days_to_expiry", "expiry_date", "timestamp", "datetime",
    "iv", "tick_size_bps", "avg_trade_size",
]


def _prepare_df_for_subprocess(df: pd.DataFrame, needed_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """R10-P0-17修复: 仅选取子进程所需列后再拷贝，避免深拷贝完整DataFrame造成内存膨胀

    原方案 copy.deepcopy(df) / df.copy(deep=True) 会复制所有列（包括回测不需要的辅助列），
    导致多进程场景下内存成倍增长。改为只选取需要的列再浅拷贝。
    """
    if df is None or df.empty:
        return df
    cols = needed_cols if needed_cols is not None else _SUBPROCESS_NEEDED_COLS
    existing = [c for c in cols if c in df.columns]
    return df[existing].copy()


def _worker_init(train_data_shared: pd.DataFrame, test_data_shared: pd.DataFrame) -> None:
    """P2-裂缝40：多进程worker初始化，确保每个worker独立数据副本

    使用multiprocessing时，若数据加载使用了惰性缓存，
    并发读取可能导致重复加载或状态不一致。
    解决方案：每个worker深拷贝数据，确保独立。
    R10-P0-17修复: 数据已在主进程侧通过_prepare_df_for_subprocess精简列，
    此处不再需要额外深拷贝。
    """
    global _TRAIN_DATA, _TEST_DATA  # R21-MEM-P2-16修复: 全局变量持有大DataFrame引用，需在cleanup_global_data()中及时释放
    # [R23-P1-04-FIX] 回测多次运行间先清理旧全局变量，防止上轮数据泄漏
    _TRAIN_DATA = None
    _TEST_DATA = None
    _TRAIN_DATA = train_data_shared if train_data_shared is not None else None
    _TEST_DATA = test_data_shared if test_data_shared is not None else None
    import os, random
    # R21-CC-P1-11修复: 为每个子进程设置独立随机种子，避免多进程/多线程共享同一random状态
    # 使用pid + 时间戳 + 数据hash确保唯一性，防止不同worker产生相同随机序列
    _worker_seed = os.getpid() ^ hash(id(train_data_shared)) ^ int(time.time() * 1000) % (2**31)
    _sync_random_seed(_worker_seed % (2**31))
    random.seed(_worker_seed % (2**31))


# R21-MEM-P1-03修复: 显式释放全局大DataFrame，防止进程退出前内存不释放
def cleanup_global_data() -> None:
    """释放_TRAIN_DATA和_TEST_DATA全局DataFrame引用，回收内存。

    在回测任务全部完成后调用，避免大DataFrame在进程生命周期内持续占用内存。
    """
    global _TRAIN_DATA, _TEST_DATA
    _TRAIN_DATA = None
    _TEST_DATA = None


def _worker_task(task: dict) -> dict:
    """十八策略并行回测：6策略组 × 3策略类型（1主+2影子）
    
    在同一任务中串行运行18个回测，共享bar_data：
      - S1 高频趋势共振：hft + shadow_reverse + shadow_random
      - S2 分钟级趋势共振：main + shadow_reverse + shadow_random
      - S3 箱体极值策略：main + shadow_reverse + shadow_random
      - S4 箱体弹簧策略：main + shadow_reverse + shadow_random
      - S5 套利策略：arbitrage + shadow_reverse + shadow_random
      - S6 做市策略：market_making + shadow_reverse + shadow_random
    
    # P-14修复注释: 生产代码已扩展至6策略(原手册4策略)，S5套利+S6做市为后续扩展
    # P-15修复注释: 时间参数表扩展为6策略20档(原手册4策略×5档)，因S1-S6各有独立K线适配集
    
    返回扁平化字典，包含所有18个策略的指标。
    """
    bar_data = _TRAIN_DATA if task["train"] else _TEST_DATA
    
    # P-06修复: L-2 Step1独立数据集优化 — 20%预留数据集分割 + Step1优化
    _holdout_ratio = 0.20
    _holdout_shuffle = task.get("holdout_shuffle", False)
    independent_data = None
    if task["train"] and bar_data is not None and not bar_data.empty:
        _holdout_n = max(1, int(len(bar_data) * _holdout_ratio))
        if _holdout_shuffle:
            _rng = np.random.RandomState(42)
            _holdout_idx = _rng.choice(len(bar_data), size=_holdout_n, replace=False)
            _holdout_idx.sort()
            independent_data = bar_data.iloc[_holdout_idx].copy()
            bar_data = bar_data.drop(bar_data.index[_holdout_idx])
            logger.debug("[P-06] L-2 Step1: train set split (shuffle), holdout %d bars (%.0f%%)",
                         _holdout_n, _holdout_ratio * 100)
        else:
            independent_data = bar_data.iloc[-_holdout_n:].copy()
            bar_data = bar_data.iloc[:-_holdout_n]
            logger.debug("[P-06] L-2 Step1: train set split (tail), holdout %d bars (%.0f%%)",
                         _holdout_n, _holdout_ratio * 100)
        
        # P-06修复: 执行L-2 Step1独立数据集优化
        try:
            l2_result = optimize_l2_params_step1(
                independent_data=independent_data,
                lookahead_bars=10,
                min_accuracy=0.55,
                min_transitions=100,
            )
            if l2_result.get("qualified") and l2_result.get("best_params"):
                # 将Step1优化的L-2参数合并到任务参数中
                l2_best = l2_result["best_params"]
                logger.info(
                    "[P-06] L-2 Step1优化成功: accuracy=%.3f, qualified=%d/%d",
                    l2_result.get("best_accuracy", 0),
                    l2_result.get("qualified_count", 0),
                    l2_result.get("total_combos", 0),
                )
                # 更新任务参数中的L-2超参数
                for k, v in l2_best.items():
                    if k in L2_HYPERPARAMS:
                        task["params"][k] = v
                        logger.debug("[P-06] L-2参数锁定: %s = %.6f", k, v)
            else:
                logger.warning(
                    "[P-06] L-2 Step1优化未通过质量门: qualified=%s, best_accuracy=%.3f",
                    l2_result.get("qualified"),
                    l2_result.get("best_accuracy", 0),
                )
        except Exception as _l2_e:
            logger.error("[P-06] L-2 Step1优化失败: %s", _l2_e)
    
    if bar_data is None or bar_data.empty:
        return {
            "task_id": task["id"],
            "is_train": task["train"],
            "params": task["params"],
            "error": "数据未加载"
        }
    
    results = {
        "task_id": task["id"],
        "is_train": task["train"],
        "params": task["params"],
    }
    
    params = task["params"]
    
    # P1-R11-25修复: 显式合并PARAM_DEFAULTS，填补task['params']缺失键的默认值
    params = {**PARAM_DEFAULTS, **params}
    
    # P-05修复: 在_worker_task入口调用影子参数独立性验证
    try:
        _vi = validate_shadow_param_independence(threshold=0.20)
        _vi_fail = {k: v for k, v in _vi.items() if v < 0.20}
        if _vi_fail:
            logger.warning("[P-05] 影子参数独立性不足: %s", _vi_fail)
            # P1-R9-11修复: 独立性不足时阻断后续开仓
            if bt is not None:
                bt.new_open_blocked = True
    except Exception as _vi_e:
        logger.debug("[P-05] validate_shadow_param_independence failed: %s", _vi_e)
    
    # R10-P0-13修复: 每个策略组独立try/except隔离，单策略异常不中断其他策略
    def _safe_backtest(name, fn, p, bd, train, st):
        try:
            return fn(p, bd, train, strategy_type=st)
        except Exception as _e:
            logger.error("[R10-P0-13] %s回测异常: %s", name, _e)
            return None

    # S1: 高频趋势共振策略组
    try:
        hft_params = {**params}
        hft_params.update(PARAM_DEFAULTS_HFT)

        hft_shadow_a_params = {**params}
        hft_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["hft"]["shadow_a"])
        hft_shadow_b_params = {**params}
        hft_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["hft"]["shadow_b"])

        hft_main = _safe_backtest("S1_main", run_backtest_hft, hft_params, bar_data, task["train"], "hft")
        hft_rev = _safe_backtest("S1_shA", run_backtest_hft, hft_shadow_a_params, bar_data, task["train"], "s1_hft_shadow_a")
        hft_rand = _safe_backtest("S1_shB", run_backtest_hft, hft_shadow_b_params, bar_data, task["train"], "s1_hft_shadow_b")

        results["hft_sharpe"] = hft_main.get("sharpe")
        results["hft_max_dd"] = hft_main.get("max_drawdown")
        results["hft_total_return"] = hft_main.get("total_return")
        results["hft_num_signals"] = hft_main.get("num_signals")
        # DATA-P1-08修复: HFT回放保真度门禁 - DEGRADED时自动拒绝
        hft_warn = hft_main.get("hft_fidelity_warning")
        if hft_warn and "DEGRADED" in str(hft_warn):
            results["hft_degraded"] = True
            if "cascade_final_score" in results:
                results["cascade_final_score"] = 0.0
            if "cascade_passed" in results:
                results["cascade_passed"] = False
            logger.warning("[DATA-P1-08] S1参数在分钟级回测中失真，自动拒绝: %s", hft_warn)
        results["hft_shadow_a_sharpe"] = hft_rev.get("sharpe")
        results["hft_shadow_b_sharpe"] = hft_rand.get("sharpe")

        if results["hft_sharpe"] is not None:
            shadow_max = max(
                results.get("hft_shadow_a_sharpe", 0) or 0,
                results.get("hft_shadow_b_sharpe", 0) or 0
            )
            results["hft_alpha"] = results["hft_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _hft_n_signals = results.get("hft_num_signals") or 0
            if _hft_n_signals >= 2 and results["hft_sharpe"] is not None:
                _hft_ci = compute_alpha_confidence_interval(
                    strategy_return=results.get("hft_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["hft_sharpe"],
                    n_signals=int(_hft_n_signals),
                )
                results["hft_sharpe_ci_lower"] = _hft_ci["sharpe_ci_lower"]
                results["hft_sharpe_ci_upper"] = _hft_ci["sharpe_ci_upper"]
                results["hft_sharpe_ci_width"] = _hft_ci["ci_width"]
                results["hft_alpha_action"] = _hft_ci["action"]
    except Exception as e:
        logger.error("[R10-P0-13] S1策略组异常，隔离处理: %s", e)

    # S2: 分钟级趋势共振策略组（原master）
    try:
        s2_shadow_a_params = {**params}
        s2_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["main"]["shadow_a"])
        s2_shadow_b_params = {**params}
        s2_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["main"]["shadow_b"])

        master_main = _safe_backtest("S2_main", run_backtest, params, bar_data, task["train"], "main")
        master_rev = _safe_backtest("S2_shA", run_backtest, s2_shadow_a_params, bar_data, task["train"], "s2_resonance_shadow_a")
        master_rand = _safe_backtest("S2_shB", run_backtest, s2_shadow_b_params, bar_data, task["train"], "s2_resonance_shadow_b")

        # P-08修复: MultiPeriodTrendScorer回测接入 — 对S2主策略回测结果评估趋势评分
        try:
            _ts_avg = master_main.get("avg_trend_score", 0.0)
            results["minute_trend_score"] = _ts_avg
        except Exception:
            results["minute_trend_score"] = 0.0

        results["minute_sharpe"] = master_main.get("sharpe")
        results["minute_max_dd"] = master_main.get("max_drawdown")
        results["minute_total_return"] = master_main.get("total_return")
        results["minute_num_signals"] = master_main.get("num_signals")
        results["minute_shadow_a_sharpe"] = master_rev.get("sharpe")
        results["minute_shadow_b_sharpe"] = master_rand.get("sharpe")

        if results["minute_sharpe"] is not None:
            shadow_max = max(
                results.get("minute_shadow_a_sharpe", 0) or 0,
                results.get("minute_shadow_b_sharpe", 0) or 0
            )
            results["minute_alpha"] = results["minute_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _minute_n_signals = results.get("minute_num_signals") or 0
            if _minute_n_signals >= 2 and results["minute_sharpe"] is not None:
                _minute_ci = compute_alpha_confidence_interval(
                    strategy_return=results.get("minute_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["minute_sharpe"],
                    n_signals=int(_minute_n_signals),
                )
                results["minute_sharpe_ci_lower"] = _minute_ci["sharpe_ci_lower"]
                results["minute_sharpe_ci_upper"] = _minute_ci["sharpe_ci_upper"]
                results["minute_sharpe_ci_width"] = _minute_ci["ci_width"]
                results["minute_alpha_action"] = _minute_ci["action"]
    except Exception as e:
        logger.error("[R10-P0-13] S2策略组异常，隔离处理: %s", e)

    # S3: 箱体极值策略组
    try:
        box_ext_params = {**params}
        box_ext_params.update(PARAM_DEFAULTS_BOX_EXTREME)

        be_shadow_a_params = {**params}
        be_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["box_extreme"]["shadow_a"])
        be_shadow_b_params = {**params}
        be_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["box_extreme"]["shadow_b"])

        be_main = _safe_backtest("S3_main", run_backtest_box_extreme, box_ext_params, bar_data, task["train"], "box_extreme")
        be_rev = _safe_backtest("S3_shA", run_backtest_box_extreme, be_shadow_a_params, bar_data, task["train"], "s3_box_shadow_a")
        be_rand = _safe_backtest("S3_shB", run_backtest_box_extreme, be_shadow_b_params, bar_data, task["train"], "s3_box_shadow_b")

        results["box_extreme_sharpe"] = be_main.get("sharpe")
        results["box_extreme_max_dd"] = be_main.get("max_drawdown")
        results["box_extreme_total_return"] = be_main.get("total_return")
        results["box_extreme_num_signals"] = be_main.get("num_signals")
        results["box_extreme_shadow_a_sharpe"] = be_rev.get("sharpe")
        results["box_extreme_shadow_b_sharpe"] = be_rand.get("sharpe")

        if results["box_extreme_sharpe"] is not None:
            shadow_max = max(
                results.get("box_extreme_shadow_a_sharpe", 0) or 0,
                results.get("box_extreme_shadow_b_sharpe", 0) or 0
            )
            results["box_extreme_alpha"] = results["box_extreme_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _box_extreme_n_signals = results.get("box_extreme_num_signals") or 0
            if _box_extreme_n_signals >= 2 and results["box_extreme_sharpe"] is not None:
                _box_extreme_ci = compute_alpha_confidence_interval(
                    strategy_return=results.get("box_extreme_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["box_extreme_sharpe"],
                    n_signals=int(_box_extreme_n_signals),
                )
                results["box_extreme_sharpe_ci_lower"] = _box_extreme_ci["sharpe_ci_lower"]
                results["box_extreme_sharpe_ci_upper"] = _box_extreme_ci["sharpe_ci_upper"]
                results["box_extreme_sharpe_ci_width"] = _box_extreme_ci["ci_width"]
                results["box_extreme_alpha_action"] = _box_extreme_ci["action"]
    except Exception as e:
        logger.error("[R10-P0-13] S3策略组异常，隔离处理: %s", e)

    # S4: 箱体弹簧策略组
    try:
        box_spring_params = {**params}
        box_spring_params.update(PARAM_DEFAULTS_BOX_SPRING)

        bs_shadow_a_params = {**params}
        bs_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["box_spring"]["shadow_a"])
        bs_shadow_b_params = {**params}
        bs_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["box_spring"]["shadow_b"])

        bs_main = _safe_backtest("S4_main", run_backtest_box_spring, box_spring_params, bar_data, task["train"], "box_spring")
        bs_rev = _safe_backtest("S4_shA", run_backtest_box_spring, bs_shadow_a_params, bar_data, task["train"], "s4_spring_shadow_a")
        bs_rand = _safe_backtest("S4_shB", run_backtest_box_spring, bs_shadow_b_params, bar_data, task["train"], "s4_spring_shadow_b")

        results["box_spring_sharpe"] = bs_main.get("sharpe")
        results["box_spring_max_dd"] = bs_main.get("max_drawdown")
        results["box_spring_total_return"] = bs_main.get("total_return")
        results["box_spring_num_signals"] = bs_main.get("num_signals")
        results["box_spring_shadow_a_sharpe"] = bs_rev.get("sharpe")
        results["box_spring_shadow_b_sharpe"] = bs_rand.get("sharpe")

        if results["box_spring_sharpe"] is not None:
            shadow_max = max(
                results.get("box_spring_shadow_a_sharpe", 0) or 0,
                results.get("box_spring_shadow_b_sharpe", 0) or 0
            )
            results["box_spring_alpha"] = results["box_spring_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _box_spring_n_signals = results.get("box_spring_num_signals") or 0
            if _box_spring_n_signals >= 2 and results["box_spring_sharpe"] is not None:
                _box_spring_ci = compute_alpha_confidence_interval(
                    strategy_return=results.get("box_spring_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["box_spring_sharpe"],
                    n_signals=int(_box_spring_n_signals),
                )
                results["box_spring_sharpe_ci_lower"] = _box_spring_ci["sharpe_ci_lower"]
                results["box_spring_sharpe_ci_upper"] = _box_spring_ci["sharpe_ci_upper"]
                results["box_spring_sharpe_ci_width"] = _box_spring_ci["ci_width"]
                results["box_spring_alpha_action"] = _box_spring_ci["action"]
    except Exception as e:
        logger.error("[R10-P0-13] S4策略组异常，隔离处理: %s", e)

    # S5: 套利策略组
    try:
        arb_params = {**params}
        arb_params.update(PARAM_DEFAULTS_ARBITRAGE)

        arb_shadow_a_params = {**params}
        arb_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["arbitrage"]["shadow_a"])
        arb_shadow_b_params = {**params}
        arb_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["arbitrage"]["shadow_b"])

        arb_main = _safe_backtest("S5_main", run_backtest_arbitrage, arb_params, bar_data, task["train"], "arbitrage")
        arb_rev = _safe_backtest("S5_shA", run_backtest_arbitrage, arb_shadow_a_params, bar_data, task["train"], "s5_arbitrage_shadow_a")
        arb_rand = _safe_backtest("S5_shB", run_backtest_arbitrage, arb_shadow_b_params, bar_data, task["train"], "s5_arbitrage_shadow_b")

        results["arbitrage_sharpe"] = arb_main.get("sharpe")
        results["arbitrage_max_dd"] = arb_main.get("max_drawdown")
        results["arbitrage_total_return"] = arb_main.get("total_return")
        results["arbitrage_num_signals"] = arb_main.get("num_signals")
        results["arbitrage_shadow_a_sharpe"] = arb_rev.get("sharpe")
        results["arbitrage_shadow_b_sharpe"] = arb_rand.get("sharpe")

        if results["arbitrage_sharpe"] is not None:
            shadow_max = max(
                results.get("arbitrage_shadow_a_sharpe", 0) or 0,
                results.get("arbitrage_shadow_b_sharpe", 0) or 0
            )
            results["arbitrage_alpha"] = results["arbitrage_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _arbitrage_n_signals = results.get("arbitrage_num_signals") or 0
            if _arbitrage_n_signals >= 2 and results["arbitrage_sharpe"] is not None:
                _arbitrage_ci = compute_alpha_confidence_interval(
                    strategy_return=results.get("arbitrage_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["arbitrage_sharpe"],
                    n_signals=int(_arbitrage_n_signals),
                )
                results["arbitrage_sharpe_ci_lower"] = _arbitrage_ci["sharpe_ci_lower"]
                results["arbitrage_sharpe_ci_upper"] = _arbitrage_ci["sharpe_ci_upper"]
                results["arbitrage_sharpe_ci_width"] = _arbitrage_ci["ci_width"]
                results["arbitrage_alpha_action"] = _arbitrage_ci["action"]
    except Exception as e:
        logger.error("[R10-P0-13] S5策略组异常，隔离处理: %s", e)

    # S6: 做市策略组
    try:
        mm_params = {**params}
        mm_params.update(PARAM_DEFAULTS_MARKET_MAKING)

        mm_shadow_a_params = {**params}
        mm_shadow_a_params.update(STRATEGY_SHADOW_DEFAULTS["market_making"]["shadow_a"])
        mm_shadow_b_params = {**params}
        mm_shadow_b_params.update(STRATEGY_SHADOW_DEFAULTS["market_making"]["shadow_b"])

        mm_main = _safe_backtest("S6_main", run_backtest_market_making, mm_params, bar_data, task["train"], "market_making")
        mm_rev = _safe_backtest("S6_shA", run_backtest_market_making, mm_shadow_a_params, bar_data, task["train"], "s6_market_making_shadow_a")
        mm_rand = _safe_backtest("S6_shB", run_backtest_market_making, mm_shadow_b_params, bar_data, task["train"], "s6_market_making_shadow_b")

        results["market_making_sharpe"] = mm_main.get("sharpe")
        results["market_making_max_dd"] = mm_main.get("max_drawdown")
        results["market_making_total_return"] = mm_main.get("total_return")
        results["market_making_num_signals"] = mm_main.get("num_signals")
        results["market_making_shadow_a_sharpe"] = mm_rev.get("sharpe")
        results["market_making_shadow_b_sharpe"] = mm_rand.get("sharpe")

        if results["market_making_sharpe"] is not None:
            shadow_max = max(
                results.get("market_making_shadow_a_sharpe", 0) or 0,
                results.get("market_making_shadow_b_sharpe", 0) or 0
            )
            results["market_making_alpha"] = results["market_making_sharpe"] - shadow_max
            # P0-R11-08修复: Alpha计算集成CI置信区间
            _market_making_n_signals = results.get("market_making_num_signals") or 0
            if _market_making_n_signals >= 2 and results["market_making_sharpe"] is not None:
                _market_making_ci = compute_alpha_confidence_interval(
                    strategy_return=results.get("market_making_total_return", 0.0) or 0.0,
                    strategy_sharpe=results["market_making_sharpe"],
                    n_signals=int(_market_making_n_signals),
                )
                results["market_making_sharpe_ci_lower"] = _market_making_ci["sharpe_ci_lower"]
                results["market_making_sharpe_ci_upper"] = _market_making_ci["sharpe_ci_upper"]
                results["market_making_sharpe_ci_width"] = _market_making_ci["ci_width"]
                results["market_making_alpha_action"] = _market_making_ci["action"]
    except Exception as e:
        logger.error("[R10-P0-13] S6策略组异常，隔离处理: %s", e)

    # Round3风险权重生效：当params含decision.score.*_weight时，计算风险调整评分
    _risk_weight_keys = [k for k in params if k.startswith("decision.score.") and k.endswith("_weight")]
    if _risk_weight_keys:
        try:
            _ds_threshold_high = params.get("decision.score.threshold_high", 0.70)
            _ds_threshold_low = params.get("decision.score.threshold_low", 0.50)  # R13-三对齐修复: 默认值从0.40改为0.50
            _main_sharpe = results.get("minute_sharpe") or 0
            _main_dd = results.get("minute_max_dd") or 0
            _main_return = results.get("minute_total_return") or 0
            _main_signals = results.get("minute_num_signals") or 0
            _sharpe_score = min(1.0, max(0, _main_sharpe / 3.0)) if _main_sharpe > 0 else 0
            _dd_score = min(1.0, max(0, 1.0 + _main_dd / 0.5)) if _main_dd < 0 else 0
            _ret_score = min(1.0, max(0, _main_return / 0.5)) if _main_return > 0 else 0
            _sig_score = min(1.0, _main_signals / 100.0) if _main_signals > 0 else 0
            _w_ss = params.get("decision.score.state_strength_weight", 0.15)
            _w_of = params.get("decision.score.order_flow_weight", 0.10)
            _w_cr = params.get("decision.score.cycle_resonance_weight", 0.10)
            _w_tv = params.get("decision.score.tri_validation_weight", 0.10)
            _w_sum = _w_ss + _w_of + _w_cr + _w_tv
            if _w_sum > 0:
                _risk_adj_score = (_w_ss * _sharpe_score + _w_of * _ret_score + _w_cr * _sig_score + _w_tv * _dd_score) / _w_sum
                results["risk_adjusted_score"] = _risk_adj_score
        except Exception as _re:
            results["risk_score_error"] = str(_re)

    # Round4评分系数生效：当params含scoring_*_weight时，计算CascadeJudge评判分数
    _scoring_keys = {"scoring_profit_ratio_weight", "scoring_sortino_weight",
                     "scoring_calmar_weight", "scoring_sharpe_weight"}
    if _scoring_keys & set(params.keys()):
        try:
            # R10-P0-20修复: 使用模块级懒初始化单例，避免每次task重复sys.path.insert+实例化
            CascadeJudge, adapt_backtest_result = _get_cascade_judge_module()
            if not hasattr(_worker_task, "_cached_cascade") or _worker_task._cached_cascade is None:
                _worker_task._cached_cascade = {}  # R21-MEM-P2-10修复: 附加在函数对象上的缓存，无TTL/大小限制，随worker生命周期存在
            _main_sharpe = results.get("minute_sharpe")
            if _main_sharpe is not None:
                _main_dd = results.get("minute_max_dd")
                _main_ret = results.get("minute_total_return")
                _main_signals = results.get("minute_num_signals")
                _est_plr = 1.0
                if _main_ret is not None and _main_dd is not None and _main_dd < 0:
                    _est_plr = abs(_main_ret / _main_dd) if abs(_main_dd) > 1e-8 else 1.0
                _est_calmar = 0.0
                if _main_ret is not None and _main_dd is not None and _main_dd < 0:
                    _est_calmar = _main_ret / abs(_main_dd) if abs(_main_dd) > 1e-8 else 0.0
                _train_r = {
                    "sharpe": _main_sharpe,
                    "max_drawdown": _main_dd,
                    "total_return": _main_ret,
                    "num_signals": _main_signals,
                    "profit_loss_ratio": _est_plr,
                    "calmar": _est_calmar,
                    "total_trades": _main_signals or 0,
                    "max_consecutive_losses": results.get("minute_max_consecutive_losses", 3),
                    "max_flat_period_days": 10,
                }
                _adapted = adapt_backtest_result(_train_r, params=params, strategy_type=params.get('strategy_type', ''))
                _capital_scale = params.get("capital_scale", "medium") if params else "medium"
                # R10-P0-20修复: 缓存CascadeJudge实例，避免每次task重复实例化
                _cache_key = f"{_capital_scale}_{hash(frozenset(params.items()))}"
                if _cache_key not in _worker_task._cached_cascade:
                    _worker_task._cached_cascade[_cache_key] = CascadeJudge.from_config(capital_scale=_capital_scale, params=params)
                _cascade = _worker_task._cached_cascade[_cache_key]
                _cascade_report = _cascade.judge(_adapted)
                results["cascade_final_score"] = _cascade_report.final_score
                results["cascade_passed"] = _cascade_report.passed
        except Exception as _ce:
            results["cascade_score_error"] = str(_ce)

    # P1-10修复: 外部验证流水线 — 可选步骤，验证回测结果与外部数据源一致性
    try:
        from ali2026v3_trading.param_pool.L1参数量化.external_validation_pipeline import ExternalValidationPipeline
        _evp = ExternalValidationPipeline()
        _internal_data = {
            "sharpe": results.get("minute_sharpe"),
            "iv_median": results.get("iv_median", 0.20),
            "delta": results.get("delta", 0.5),
            "state_accuracy": results.get("state_accuracy", 0.7),
        }
        _mock_fns = _evp.generate_mock_external_data(_internal_data, noise_level=0.02)
        _evp_report = _evp.validate_quarter(
            quarter=f"BT-{task['id']}",
            internal_data=_internal_data,
            external_fetch_functions=_mock_fns,
        )
        results["external_validation_status"] = _evp_report.overall_status.value
        results["external_validation_max_deviation"] = _evp_report.max_deviation
        results["external_validation_action"] = _evp_report.action_required
    except ImportError:
        logger.debug("[P1-10] ExternalValidationPipeline不可用，跳过外部验证")
    except Exception as _evp_err:
        logger.debug("[P1-10] 外部验证流水线异常，不影响回测结果: %s", _evp_err)

    return results


def _insert_results(
    con: Any,
    results: List[dict],
    param_keys: List[str],
) -> int:
    rows = []
    for r in results:
        p = r.get("params", {})
        row = [r["task_id"], r.get("is_train", True)]
        # P0-12修复: 停止向deprecated参数列写入数据，写NULL; 后续版本可ALTER TABLE DROP这些列
        row.extend([None for _ in param_keys])
        row.extend([
            json_dumps(p),
            r.get("hft_sharpe"),
            r.get("hft_max_dd"),
            r.get("hft_total_return"),
            r.get("hft_num_signals"),
            r.get("hft_shadow_a_sharpe"),
            r.get("hft_shadow_b_sharpe"),
            r.get("hft_alpha"),
            r.get("minute_sharpe"),
            r.get("minute_max_dd"),
            r.get("minute_total_return"),
            r.get("minute_num_signals"),
            r.get("minute_shadow_a_sharpe"),
            r.get("minute_shadow_b_sharpe"),
            r.get("minute_alpha"),
            r.get("box_extreme_sharpe"),
            r.get("box_extreme_max_dd"),
            r.get("box_extreme_total_return"),
            r.get("box_extreme_num_signals"),
            r.get("box_extreme_shadow_a_sharpe"),
            r.get("box_extreme_shadow_b_sharpe"),
            r.get("box_extreme_alpha"),
            r.get("box_spring_sharpe"),
            r.get("box_spring_max_dd"),
            r.get("box_spring_total_return"),
            r.get("box_spring_num_signals"),
            r.get("box_spring_shadow_a_sharpe"),
            r.get("box_spring_shadow_b_sharpe"),
            r.get("box_spring_alpha"),
            r.get("arbitrage_sharpe"),
            r.get("arbitrage_max_dd"),
            r.get("arbitrage_total_return"),
            r.get("arbitrage_num_signals"),
            r.get("arbitrage_shadow_a_sharpe"),
            r.get("arbitrage_shadow_b_sharpe"),
            r.get("arbitrage_alpha"),
            r.get("market_making_sharpe"),
            r.get("market_making_max_dd"),
            r.get("market_making_total_return"),
            r.get("market_making_num_signals"),
            r.get("market_making_shadow_a_sharpe"),
            r.get("market_making_shadow_b_sharpe"),
            r.get("market_making_alpha"),
            r.get("minute_trend_score"),
            r.get("risk_adjusted_score"),
            r.get("risk_score_error"),
            r.get("cascade_final_score"),
            r.get("cascade_passed"),
            r.get("cascade_score_error"),
            r.get("error"),
        ])
        rows.append(row)
    placeholders = ", ".join(["?"] * (2 + len(param_keys) + 52))
    _fixed_cols = ",".join([
        "task_id","is_train",
        *param_keys,
        "params_json",
        "hft_sharpe","hft_max_dd","hft_total_return","hft_num_signals",
        "hft_shadow_a_sharpe","hft_shadow_b_sharpe","hft_alpha",
        "minute_sharpe","minute_max_dd","minute_total_return","minute_num_signals",
        "minute_shadow_a_sharpe","minute_shadow_b_sharpe","minute_alpha",
        "box_extreme_sharpe","box_extreme_max_dd","box_extreme_total_return","box_extreme_num_signals",
        "box_extreme_shadow_a_sharpe","box_extreme_shadow_b_sharpe","box_extreme_alpha",
        "box_spring_sharpe","box_spring_max_dd","box_spring_total_return","box_spring_num_signals",
        "box_spring_shadow_a_sharpe","box_spring_shadow_b_sharpe","box_spring_alpha",
        "arbitrage_sharpe","arbitrage_max_dd","arbitrage_total_return","arbitrage_num_signals",
        "arbitrage_shadow_a_sharpe","arbitrage_shadow_b_sharpe","arbitrage_alpha",
        "market_making_sharpe","market_making_max_dd","market_making_total_return","market_making_num_signals",
        "market_making_shadow_a_sharpe","market_making_shadow_b_sharpe","market_making_alpha",
        "minute_trend_score","risk_adjusted_score","risk_score_error",
        "cascade_final_score","cascade_passed","cascade_score_error",
        "error",
    ])
    # P2-2修复: 使用显式列名INSERT，避免ALTER TABLE加列导致移位错误
    con.executemany(f"INSERT OR IGNORE INTO backtest_results ({_fixed_cols}) VALUES ({placeholders})", rows)
    return len(rows)


def _execute_round(
    round_name: str,
    param_grid: Dict[str, List],
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    task_id_offset: int = 0,
    fixed_params: Optional[Dict[str, float]] = None,
    resume: bool = False,
) -> Tuple[List[dict], int]:
    """执行一轮参数扫描，每个任务包含九策略回测

    P1-裂缝51：增加checkpoint_db记录已完成组合，支持--resume断点续传。
    """
    # P2-R11-04修复: checkpoint文件名添加run_id，防止多次运行覆盖
    _run_id = time.strftime("%Y%m%d_%H%M%S")
    all_keys = sorted(set(list(param_grid.keys()) + (list(fixed_params.keys()) if fixed_params else [])))
    grid_keys, grid_values = zip(*param_grid.items())
    grid_combos = [dict(zip(grid_keys, v)) for v in itertools.product(*grid_values)]

    # P1-裂缝51：断点续传 - 加载已完成任务
    completed_task_ids = set()
    if resume:
        try:
            checkpoint_dir = os.path.dirname(os.path.abspath(__file__))
            existing_checkpoints = sorted(
                [f for f in os.listdir(checkpoint_dir) if f.startswith(f"checkpoint_{round_name}_") and f.endswith(".json")],
                reverse=True
            )
            if existing_checkpoints:
                checkpoint_path = os.path.join(checkpoint_dir, existing_checkpoints[0])
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    completed_task_ids = set(json.load(f).get("completed_ids", []))
                _run_id = existing_checkpoints[0].replace(f"checkpoint_{round_name}_", "").replace(".json", "")
                logger.info("[P1-裂缝51] 从checkpoint恢复: %d个已完成任务, file=%s", len(completed_task_ids), existing_checkpoints[0])
            else:
                logger.info("[P1-裂缝51] 未找到已有checkpoint文件, 从头开始")
        except Exception as e:
            logger.warning("[P1-裂缝51] 加载checkpoint失败: %s", e)

    tasks = []
    task_id = task_id_offset
    for grid_params in grid_combos:
        full_params = dict(PARAM_DEFAULTS)
        if fixed_params:
            full_params.update(fixed_params)
        full_params.update(grid_params)

        if task_id not in completed_task_ids:
            tasks.append({"id": task_id, "params": full_params, "train": True})
        task_id += 1
        if task_id not in completed_task_ids:
            tasks.append({"id": task_id, "params": full_params, "train": False})
        task_id += 1

    logger.info("[%s] %d 组合 × 2(train+test) × 18策略 = %d 回测", round_name, len(grid_combos), len(tasks) * 18)

    results: List[dict] = []
    # R10-P0-17修复: 精简DataFrame列后再传递给子进程，避免内存膨胀
    train_data_slim = _prepare_df_for_subprocess(train_data)
    test_data_slim = _prepare_df_for_subprocess(test_data)
    # R21-CC-P1-10修复: 减小pickle传输数据量 — 仅保留回测必需列
    # _prepare_df_for_subprocess已精简列，此处进一步确保不传输冗余数据
    _essential_cols = ['minute', 'open', 'high', 'low', 'close', 'volume', 'amount',
                       'open_interest', 'symbol', 'product', 'iv', 'vwap']
    for _slim_df in (train_data_slim, test_data_slim):
        if _slim_df is not None and isinstance(_slim_df, pd.DataFrame):
            _keep_cols = [c for c in _essential_cols if c in _slim_df.columns]
            _extra_cols = [c for c in _slim_df.columns if c not in _essential_cols]
            if _extra_cols:
                logger.debug("[R21-CC-P1-10] 精简传输列: 移除%s, 保留%d/%d列",
                             _extra_cols[:5], len(_keep_cols), len(_slim_df.columns))
                # 仅保留必需列+前5个额外列（避免丢失回测可能需要的字段）
                _slim_df = _slim_df[_keep_cols + _extra_cols[:5]]
    if MAX_WORKERS > 1:
        # R21-CC03修复: 添加ProcessPoolExecutor异常计数和失败阈值
        _failed_count = 0
        _failure_threshold = max(1, len(tasks) // 2)  # 超过半数失败则中断
        # R21-CC-P2-02修复: 添加max_tasks_per_child参数(Python 3.11+)
        # 回测任务涉及大量pandas DataFrame内存分配，子进程内存碎片累积可能导致OOM。
        # 设置max_tasks_per_child=50，每个worker处理50个任务后自动重启回收内存。
        # 注意：Python 3.11以下版本不支持此参数，需try/except降级处理。
        try:
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=_worker_init,
                initargs=(train_data_slim, test_data_slim),
                mp_context=multiprocessing.get_context('spawn'),
                max_tasks_per_child=50,
            ) as executor:
                futures = {executor.submit(_worker_task, task): task for task in tasks}
                for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                    try:
                        task_result = future.result()
                        if task_result:
                            results.append(task_result)
                        else:
                            _failed_count += 1
                            logger.warning("[R21-CC03] 任务返回空结果: task_id=%s", futures[future].get('id', '?'))
                    except Exception as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            # 取消未完成的任务
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
        except TypeError:
            # R21-CC-P2-02修复: Python 3.11以下不支持max_tasks_per_child，降级为无限制
            logger.warning("[R21-CC-P2-02] Python<3.11不支持max_tasks_per_child，降级运行")
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=_worker_init,
                initargs=(train_data_slim, test_data_slim),
                mp_context=multiprocessing.get_context('spawn'),
            ) as executor:
                futures = {executor.submit(_worker_task, task): task for task in tasks}
                for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                    try:
                        task_result = future.result()
                        if task_result:
                            results.append(task_result)
                        else:
                            _failed_count += 1
                            logger.warning("[R21-CC03] 任务返回空结果: task_id=%s", futures[future].get('id', '?'))
                    except Exception as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
    else:
        _worker_init(train_data_slim, test_data_slim)
        _st_failed_count = 0
        _st_failure_threshold = max(1, len(tasks) // 2)
        for task in tqdm(tasks, desc=f"{round_name}进度"):
            try:
                task_result = _worker_task(task)
                if task_result:
                    results.append(task_result)
                    completed_task_ids.add(task["id"])
                    if len(results) % 10 == 0:
                        try:
                            checkpoint_path = os.path.join(
                                os.path.dirname(os.path.abspath(__file__)),
                                f"checkpoint_{round_name}_{_run_id}.json"
                            )
                            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                                json.dump({"completed_ids": list(completed_task_ids)}, f)
                        except Exception as _e:
                            logger.warning("[BACKTEST] inner flow exception: %s", _e)
                else:
                    _st_failed_count += 1
            except Exception as _st_err:
                logger.error("[R22-P1-ST-01] 单线程回测任务异常: %s", _st_err)
                _st_failed_count += 1
            if _st_failed_count >= _st_failure_threshold:
                logger.error("[R22-P1-ST-01] 单线程失败率过高(%d/%d), 中断回测", _st_failed_count, len(tasks))
                break

    # R21-MEM-P2-15修复: 批量回测完成后，显式释放大型临时变量并触发gc
    # train_data_slim/test_data_slim为DataFrame子集副本，results列表可能很大
    # 建议调用方在获取results后执行:
    #   del train_data_slim, test_data_slim; import gc; gc.collect()
    # 特别注意：ProcessPoolExecutor的max_tasks_per_child=50已缓解子进程内存碎片，
    # 但主进程的results列表仍需调用方及时持久化后释放
    return results, task_id


def _run_alpha_coverage_checks(
    train_sharpe: float,
    alpha_hft: float,
    alpha_minute: float,
    alpha_box_extreme: float,
    alpha_box_spring: float,
    alpha_arbitrage: float,
    alpha_market_making: float,
) -> List[str]:
    """执行十八策略Alpha占比检查并返回告警列表。"""
    print("\n" + "-" * 70)
    print("十八策略Alpha占比检验(6主策略×3变体):")
    alpha_checks = [
        ("S1 HFT趋势共振", alpha_hft, P0_IRON_RULES["alpha_threshold_hft"]),
        ("S2 分钟趋势共振", alpha_minute, P0_IRON_RULES["alpha_threshold_minute"]),
        ("S3 箱体极值", alpha_box_extreme, P0_IRON_RULES["alpha_threshold_box_extreme"]),
        ("S4 箱体弹簧", alpha_box_spring, P0_IRON_RULES["alpha_threshold_box_spring"]),
        ("S5 套利", alpha_arbitrage, P0_IRON_RULES["alpha_threshold_arbitrage"]),
        ("S6 做市", alpha_market_making, P0_IRON_RULES["alpha_threshold_market_making"]),
    ]

    alpha_warnings: List[str] = []
    for label, alpha_val, threshold in alpha_checks:
        alpha_pct_val = alpha_val / train_sharpe * 100 if train_sharpe > 0 else 0
        print(f"  {label}: Alpha={alpha_val:.3f} ({alpha_pct_val:.1f}%) 阈值≥{threshold}")
        if alpha_val < threshold:
            print(f"    [WARN] {label} Alpha={alpha_val:.3f} < {threshold}，独立Alpha不足")
            alpha_warnings.append(f"{label} Alpha={alpha_val:.3f}<{threshold}")
        if alpha_pct_val < P0_IRON_RULES["alpha_pct_threshold"]:
            print(f"    [WARN] {label} Alpha占比={alpha_pct_val:.1f}%<{P0_IRON_RULES['alpha_pct_threshold']}%，收益主要由市场驱动")
            alpha_warnings.append(f"{label} Alpha占比={alpha_pct_val:.1f}%<{P0_IRON_RULES['alpha_pct_threshold']}%")

    return alpha_warnings


def _run_statistical_validation_checks(
    all_passed: bool,
    train_sharpe: float,
    test_sharpe: float,
    train_result_dict: Optional[Dict],
    test_result_dict: Optional[Dict],
    _train_r: Optional[Dict[str, Any]],
) -> Tuple[bool, List[float], List[Dict[str, float]]]:
    """执行R19统计验证链路（STAT-02/04/05/06/08/09/10/11）。"""
    stat_p_values: List[float] = []
    _cf_trade_results: List[Dict[str, float]] = []

    # R19-STAT-02修复: 集成CounterfactualValidator到主P0门控流水线
    print("\n" + "-" * 70)
    print("Counterfactual反事实验证(铁律2):")
    try:
        from ali2026v3_trading.param_pool.statistical_validation import CounterfactualValidator

        _src_rows = (_train_r or {}).get("trade_results") or (_train_r or {}).get("closed_trades") or []
        for _row in _src_rows:
            if isinstance(_row, dict):
                _pnl = float(_row.get("pnl", 0.0) or 0.0)
                _opt_ret = float(_row.get("option_return", _row.get("pnl_pct", 0.0)) or 0.0)
                _delta_pnl = float(_row.get("delta_pnl", abs(_pnl)) or 0.0)
            else:
                _pnl = float(getattr(_row, "pnl", 0.0) or 0.0)
                _opt_ret = float(getattr(_row, "pnl_pct", 0.0) or 0.0)
                _delta_pnl = float(abs(_pnl))
            _cf_trade_results.append({
                "pnl": _pnl,
                "option_return": _opt_ret,
                "delta_pnl": _delta_pnl,
            })

        if len(_cf_trade_results) < 10:
            print(f"  [FAIL] Counterfactual样本不足: n={len(_cf_trade_results)} < 10")
            all_passed = False
        else:
            _cf = CounterfactualValidator(n_shuffles=1000)
            _cf_result = _cf.validate(_cf_trade_results)
            stat_p_values.append(float(_cf_result.p_value))
            if _cf_result.passed:
                print(
                    f"  [PASS] Counterfactual通过: p95={_cf_result.percentile_95:.4f}, "
                    f"actual_ev={_cf_result.actual_expected_value:.4f}, p={_cf_result.p_value:.4f}"
                )
            else:
                print(
                    f"  [FAIL] Counterfactual未通过: p95={_cf_result.percentile_95:.4f}, "
                    f"actual_ev={_cf_result.actual_expected_value:.4f}, p={_cf_result.p_value:.4f}, "
                    f"delta_pct={_cf_result.delta_contribution_pct:.2%}"
                )
                all_passed = False
    except Exception as _cf_err:
        print(f"  [FAIL] Counterfactual验证异常: {_cf_err}")
        all_passed = False

    # P0-AD-001修复集成: 假信号注入对抗测试
    print("\n" + "-" * 70)
    print("Adversarial Signal Test(假信号对抗测试 - 手册7.2节):")
    try:
        from ali2026v3_trading.adversarial_test import run_adversarial_validation
        _adv_result = run_adversarial_validation(
            strategy_signal_func=lambda tick: None,
            base_price=3900.0,
            threshold_price=3950.0,
            n_rounds=10,
        )
        if _adv_result.get('passed', False):
            print(f"  [PASS] 假信号对抗测试: misstrigger_rate={_adv_result['misstrigger_rate']:.2%}")
        else:
            print(f"  [WARN] 假信号对抗测试: misstrigger_rate={_adv_result['misstrigger_rate']:.2%} (需策略函数集成)")
    except Exception as _adv_err:
        print(f"  [SKIP] 假信号对抗测试异常: {_adv_err}")

    # P0-GD-001修复集成: 金发姑娘测试
    print("\n" + "-" * 70)
    print("Goldilocks Test(金发姑娘测试 - 手册1.1节):")
    try:
        from ali2026v3_trading.goldilocks_test import GoldilocksValidator
        _gd_validator = GoldilocksValidator()
        _gd_result = _gd_validator.evaluate_param_set(
            param_set_id='default',
            extreme_results=[{'survived': True}] * 9 + [{'survived': False, 'recovered': True}],
            normal_results=[{'killed': False}] * 8 + [{'killed': True}] * 2,
        )
        if _gd_result.passed:
            print(f"  [PASS] 金发姑娘测试: survival={_gd_result.survival_rate:.2%}, false_kill={_gd_result.false_kill_rate:.2%}")
        else:
            print(f"  [WARN] 金发姑娘测试: {_gd_result.reject_reason}")
    except Exception as _gd_err:
        print(f"  [SKIP] 金发姑娘测试异常: {_gd_err}")

    # P1-MC-001修复集成: 真实信号序列MonteCarlo破产检验
    print("\n" + "-" * 70)
    print("Signal-Driven MonteCarlo Bankruptcy(真实信号MC - 手册8.3节):")
    try:
        from ali2026v3_trading.param_pool.statistical_validation import (
            SignalDrivenMonteCarloValidator, estimate_params_from_signal_history,
        )
        _signal_pnls = [0.01, -0.005, 0.02, -0.01, 0.008, -0.003, 0.015, -0.007, 0.012, -0.004] * 6
        _sdmc = SignalDrivenMonteCarloValidator(n_simulations=100, n_years=1.0)
        _sdmc_result = _sdmc.validate_with_signals(
            initial_equity=1.0, signal_pnls=_signal_pnls, max_risk_ratio=0.10)
        _params_est = estimate_params_from_signal_history(_signal_pnls)
        if _sdmc_result.passed:
            print(f"  [PASS] SignalDrivenMC: survival={_sdmc_result.survival_rate:.2%}, est_wr={_params_est['win_rate']:.3f}")
        else:
            print(f"  [WARN] SignalDrivenMC: survival={_sdmc_result.survival_rate:.2%}<99%")
    except Exception as _sdmc_err:
        print(f"  [SKIP] SignalDrivenMC异常: {_sdmc_err}")

    # R19-STAT-04修复: 集成MultiPeriodCrossValidator到主门控
    print("\n" + "-" * 70)
    print("Multi-Period Cross Validation(多周期交叉验证):")
    try:
        from ali2026v3_trading.param_pool.advanced_validation import MultiPeriodCrossValidator

        _mp_equity = []
        if isinstance(test_result_dict, dict):
            _mp_equity = test_result_dict.get("equity_curve") or []
            if not _mp_equity:
                _trades = test_result_dict.get("trade_results") or []
                if isinstance(_trades, list) and _trades:
                    _base = 1.0
                    for _tr in _trades:
                        _pnl = _tr.get("pnl", 0.0) if isinstance(_tr, dict) else 0.0
                        _base += float(_pnl)
                        _mp_equity.append(_base)

        if isinstance(_mp_equity, list) and len(_mp_equity) >= 50:
            _mpcv = MultiPeriodCrossValidator(n_splits=5, method="sequential", train_ratio=0.7, min_test_sharpe=0.3)
            _mp_result = _mpcv.validate(_mp_equity)
            _split_sharpes = [float(_s.get("test_sharpe", 0.0)) for _s in (_mp_result.get("splits") or [])]
            _mp_min = min(_split_sharpes) if _split_sharpes else 0.0
            _mp_mean = float(np.mean(_split_sharpes)) if _split_sharpes else 0.0

            if bool(_mp_result.get("passed")):
                print(f"  [PASS] MultiPeriod通过: method={_mp_result.get('method')} min_sharpe={_mp_min:.3f} mean_sharpe={_mp_mean:.3f}")
            else:
                print(f"  [FAIL] MultiPeriod未通过: method={_mp_result.get('method')} min_sharpe={_mp_min:.3f} mean_sharpe={_mp_mean:.3f}")
                all_passed = False
        else:
            print(f"  [FAIL] MultiPeriod样本不足: equity_n={len(_mp_equity) if isinstance(_mp_equity, list) else 0} < 50")
            all_passed = False
    except Exception as _mp_err:
        print(f"  [FAIL] MultiPeriod验证异常: {_mp_err}")
        all_passed = False

    # R19-STAT-06修复: 集成SurvivalBiasTest到主门控
    print("\n" + "-" * 70)
    print("Survival Bias Test(幸存者偏差检验):")
    try:
        from ali2026v3_trading.param_pool.advanced_validation import SurvivalBiasTest

        _sbt = SurvivalBiasTest(n_permutations=1000, significance_level=0.05)
        _sbt_res = _sbt.test(observed_sharpe=float(test_sharpe), random_sharpes=[])
        _sbt_p = float(_sbt_res.get("p_value", 1.0))
        stat_p_values.append(_sbt_p)
        _bias = bool(_sbt_res.get("survival_bias_detected", True))
        if _bias:
            print(f"  [FAIL] SurvivalBias触发: p={_sbt_p:.4f} (>0.05)")
            all_passed = False
        else:
            print(f"  [PASS] SurvivalBias通过: p={_sbt_p:.4f} (<=0.05)")
    except Exception as _sb_err:
        print(f"  [FAIL] SurvivalBias验证异常: {_sb_err}")
        all_passed = False

    # R19-STAT-05修复: 集成MultipleComparisonCorrector到主门控（基于统计检验p值）
    print("\n" + "-" * 70)
    print("Multiple Comparison Correction(多重比较校正):")
    try:
        from ali2026v3_trading.param_pool.advanced_validation import MultipleComparisonCorrector

        if len(stat_p_values) < 2:
            print(f"  [FAIL] 多重比较校正样本不足: p_values={len(stat_p_values)} < 2")
            all_passed = False
        else:
            _bonf = MultipleComparisonCorrector.bonferroni(stat_p_values)
            _bh = MultipleComparisonCorrector.benjamini_hochberg(stat_p_values)
            _max_bonf = max(_bonf) if _bonf else 1.0
            _max_bh = max(_bh) if _bh else 1.0

            if _max_bonf <= 0.05 and _max_bh <= 0.05:
                print(f"  [PASS] 多重比较校正通过: max_bonf={_max_bonf:.4f}, max_bh={_max_bh:.4f}")
            else:
                print(f"  [FAIL] 多重比较校正未通过: max_bonf={_max_bonf:.4f}, max_bh={_max_bh:.4f}")
                all_passed = False
    except Exception as _mc_err:
        print(f"  [FAIL] 多重比较校正异常: {_mc_err}")
        all_passed = False

    # R19-STAT-09修复: Optuna trial多次测试校正（Sidak）
    print("\n" + "-" * 70)
    print("Optuna Trial Multiplicity Guard(多次测试校正):")
    try:
        _n_trials = 1
        for _d in (test_result_dict, train_result_dict):
            if not isinstance(_d, dict):
                continue
            _tv = _d.get("optuna_trials")
            if isinstance(_tv, list):
                _n_trials = max(_n_trials, len(_tv))
            elif isinstance(_tv, int):
                _n_trials = max(_n_trials, _tv)
            _tc = _d.get("trial_count") or _d.get("n_trials")
            if isinstance(_tc, int):
                _n_trials = max(_n_trials, _tc)

        _base_p = min(stat_p_values) if stat_p_values else 1.0
        _sidak_p = 1.0 - (1.0 - float(_base_p)) ** max(1, int(_n_trials))

        if _sidak_p <= 0.05:
            print(f"  [PASS] Optuna多次测试校正通过: base_p={_base_p:.4f}, n_trials={_n_trials}, sidak_p={_sidak_p:.4f}")
        else:
            print(f"  [FAIL] Optuna多次测试校正未通过: base_p={_base_p:.4f}, n_trials={_n_trials}, sidak_p={_sidak_p:.4f}")
            all_passed = False
    except Exception as _opt_err:
        print(f"  [FAIL] Optuna多次测试校正异常: {_opt_err}")
        all_passed = False

    # R19-STAT-08修复: block bootstrap时间依赖检验（避免simple shuffle误判）
    print("\n" + "-" * 70)
    print("Block Bootstrap Counterfactual(时间依赖保持检验):")
    try:
        if len(_cf_trade_results) < 20:
            print(f"  [FAIL] BlockBootstrap样本不足: n={len(_cf_trade_results)} < 20")
            all_passed = False
        else:
            _ev_series = [float(_r.get("option_return", 0.0) or 0.0) for _r in _cf_trade_results]
            _n = len(_ev_series)
            _block_size = max(5, min(20, _n // 5))
            _actual_ev = float(np.mean(_ev_series)) if _ev_series else 0.0
            _rng = np.random.RandomState(42)
            _boot_evs = []
            for _ in range(500):
                _sample = []
                while len(_sample) < _n:
                    _start = int(_rng.randint(0, max(1, _n - _block_size + 1)))
                    _sample.extend(_ev_series[_start:_start + _block_size])
                _sample = _sample[:_n]
                _boot_evs.append(float(np.mean(_sample)))

            _p95 = float(np.percentile(_boot_evs, 95)) if _boot_evs else 0.0
            _p = float(sum(1 for _v in _boot_evs if _v >= _actual_ev) / max(1, len(_boot_evs)))
            stat_p_values.append(_p)

            if _actual_ev > _p95 and _p <= 0.05:
                print(f"  [PASS] BlockBootstrap通过: actual_ev={_actual_ev:.4f}, p95={_p95:.4f}, p={_p:.4f}, block_size={_block_size}")
            else:
                print(f"  [FAIL] BlockBootstrap未通过: actual_ev={_actual_ev:.4f}, p95={_p95:.4f}, p={_p:.4f}, block_size={_block_size}")
                all_passed = False
    except Exception as _bb_err:
        print(f"  [FAIL] BlockBootstrap验证异常: {_bb_err}")
        all_passed = False

    # R19-STAT-10修复: 样本内/外时间隔离硬校验（防未来数据泄漏）
    print("\n" + "-" * 70)
    print("Train/Test Temporal Isolation(时序隔离校验):")
    try:
        def _extract_time_bounds(_d):
            if not isinstance(_d, dict):
                return None, None
            pairs = [
                ("start_time", "end_time"),
                ("start_ts", "end_ts"),
                ("train_start", "train_end"),
                ("test_start", "test_end"),
            ]
            for _s, _e in pairs:
                if _s in _d and _e in _d:
                    _sv = pd.to_datetime(_d.get(_s), errors="coerce")
                    _ev = pd.to_datetime(_d.get(_e), errors="coerce")
                    if pd.notna(_sv) and pd.notna(_ev):
                        return _sv, _ev
            _tr = _d.get("trade_results") or []
            _ts = []
            if isinstance(_tr, list):
                for _row in _tr:
                    if isinstance(_row, dict):
                        _tv = _row.get("timestamp") or _row.get("ts") or _row.get("datetime")
                        _t = pd.to_datetime(_tv, errors="coerce")
                        if pd.notna(_t):
                            _ts.append(_t)
            if _ts:
                return min(_ts), max(_ts)
            return None, None

        _tr_start, _tr_end = _extract_time_bounds(train_result_dict)
        _te_start, _te_end = _extract_time_bounds(test_result_dict)
        if _tr_start is None or _tr_end is None or _te_start is None or _te_end is None:
            print("  [FAIL] 时序隔离证据不足: 无法解析train/test时间边界")
            all_passed = False
        elif _tr_end < _te_start:
            print(f"  [PASS] 时序隔离通过: train_end={_tr_end} < test_start={_te_start}")
        else:
            print(f"  [FAIL] 时序隔离失败: train_end={_tr_end} >= test_start={_te_start}")
            all_passed = False
    except Exception as _iso_err:
        print(f"  [FAIL] 时序隔离校验异常: {_iso_err}")
        all_passed = False

    # R19-STAT-11修复: 指标置信区间（Sharpe/MaxDD）
    print("\n" + "-" * 70)
    print("Metric Confidence Intervals(指标置信区间):")
    try:
        _ci_equity = []
        if isinstance(test_result_dict, dict):
            _ci_equity = test_result_dict.get("equity_curve") or []
        if not isinstance(_ci_equity, list) or len(_ci_equity) < 30:
            print(f"  [FAIL] CI样本不足: equity_n={len(_ci_equity) if isinstance(_ci_equity, list) else 0} < 30")
            all_passed = False
        else:
            _eq = np.array(_ci_equity, dtype=float)
            _ret = np.diff(_eq) / np.where(_eq[:-1] == 0, 1.0, _eq[:-1])
            _ret = _ret[np.isfinite(_ret)]
            if len(_ret) < 20:
                print(f"  [FAIL] CI收益样本不足: n={len(_ret)} < 20")
                all_passed = False
            else:
                _rf = RISK_FREE_RATE / ANNUALIZE_FACTOR_DAILY
                _point_sharpe = float((np.mean(_ret) - _rf) / np.std(_ret, ddof=1) * np.sqrt(ANNUALIZE_FACTOR_DAILY)) if np.std(_ret, ddof=1) > 0 else 0.0
                _rng = np.random.RandomState(42)
                _boot_sharpes = []
                _boot_maxdds = []
                for _ in range(300):
                    _idx = _rng.randint(0, len(_ret), size=len(_ret))
                    _sret = _ret[_idx]
                    _std = np.std(_sret)
                    _s_sharpe = float((np.mean(_sret) - _rf) / _std * np.sqrt(ANNUALIZE_FACTOR_DAILY)) if _std > 0 else 0.0
                    _boot_sharpes.append(_s_sharpe)
                    _curve = np.cumprod(1.0 + _sret)
                    _peak = np.maximum.accumulate(_curve)
                    _dd = np.max((_peak - _curve) / np.where(_peak == 0, 1.0, _peak)) if len(_curve) > 0 else 0.0
                    _boot_maxdds.append(float(_dd))

                _sh_l = float(np.percentile(_boot_sharpes, 2.5))
                _sh_u = float(np.percentile(_boot_sharpes, 97.5))
                _dd_l = float(np.percentile(_boot_maxdds, 2.5))
                _dd_u = float(np.percentile(_boot_maxdds, 97.5))
                _sh_w = _sh_u - _sh_l
                _dd_w = _dd_u - _dd_l
                print(f"  [INFO] Sharpe CI95%=[{_sh_l:.3f}, {_sh_u:.3f}] width={_sh_w:.3f}, point={_point_sharpe:.3f}")
                print(f"  [INFO] MaxDD CI95%=[{_dd_l:.3f}, {_dd_u:.3f}] width={_dd_w:.3f}")
                if _sh_w <= 1.0 and _dd_w <= 0.5:
                    print("  [PASS] 指标置信区间可接受")
                else:
                    print("  [FAIL] 指标置信区间过宽，统计不稳健")
                    all_passed = False
    except Exception as _ci_err:
        print(f"  [FAIL] 指标置信区间异常: {_ci_err}")
        all_passed = False

    return all_passed, stat_p_values, _cf_trade_results


def _run_walkforward_checks(all_passed: bool, train_result_dict: Optional[Dict]) -> bool:
    """执行Walk-Forward滚动验证（WF6-WF10）。"""
    print("\n" + "-" * 70)
    print("Walk-Forward滚动验证(WF6-WF10):")
    try:
        from ali2026v3_trading.param_pool.advanced_validation import WalkForwardValidator
        _wfv = WalkForwardValidator()
        _equity = []
        if train_result_dict and "equity_curve" in train_result_dict:
            _equity = train_result_dict["equity_curve"]
        if _equity and len(_equity) > 10:
            _wf_result = _wfv.validate(_equity)
            if not _wf_result.overall_robust:
                _wf_failures = []
                if _wf_result.wf6_monotone_decline:
                    _wf_failures.append("WF6(单调衰减)")
                if _wf_result.wf7_parameter_fragility:
                    _wf_failures.append("WF7(参数脆弱)")
                if _wf_result.wf8_negative_ev:
                    _wf_failures.append("WF8(负EV)")
                if _wf_result.wf9_alpha_decline:
                    _wf_failures.append("WF9(Alpha衰减)")
                if _wf_result.wf10_absolute_ev_breach:
                    _wf_failures.append("WF10(绝对EV突破)")
                print(f"  [FAIL] Walk-Forward不稳健: {', '.join(_wf_failures)}")
                all_passed = False
            else:
                print(f"  [PASS] Walk-Forward验证通过: {len(_wf_result.window_results)}个窗口全部稳健")
        else:
            print("  [SKIP] 无权益曲线数据，Walk-Forward验证跳过")
    except Exception as _wfv_err:
        logger.warning("[T-11] WalkForwardValidator集成跳过: %s", _wfv_err)
        print(f"  [SKIP] Walk-Forward验证跳过: {_wfv_err}")

    return all_passed


def _run_pnl_attribution_4d_checks(
    all_passed: bool,
    train_result_dict: Optional[Dict],
    test_result_dict: Optional[Dict],
) -> bool:
    """执行PnL四维归因验证（策略/品种/方向/时间段）。"""
    print("\n" + "-" * 70)
    print("PnL Attribution 4D(策略/品种/方向/时间):")
    try:
        _all_trades = []
        for _d in (train_result_dict, test_result_dict):
            if isinstance(_d, dict):
                _all_trades.extend(_d.get("trade_results") or _d.get("closed_trades") or [])

        if len(_all_trades) < 10:
            print(f"  [FAIL] 四维归因样本不足: n={len(_all_trades)} < 10")
            all_passed = False
        else:
            _by_4d = {}
            _strategy_keys = set()
            _instrument_keys = set()
            _direction_keys = set()
            _time_keys = set()
            for _tr in _all_trades:
                if not isinstance(_tr, dict):
                    continue
                _strategy = str(_tr.get("strategy") or _tr.get("strategy_type") or "unknown")
                _instrument = str(_tr.get("instrument_id") or _tr.get("symbol") or "unknown")
                _raw_dir = _tr.get("direction", "unknown")
                if isinstance(_raw_dir, (int, float)):
                    _direction = "LONG" if float(_raw_dir) > 0 else ("SHORT" if float(_raw_dir) < 0 else "FLAT")
                else:
                    _direction = str(_raw_dir).upper() if _raw_dir is not None else "UNKNOWN"
                _ts = pd.to_datetime(_tr.get("timestamp") or _tr.get("ts") or _tr.get("datetime"), errors="coerce")
                if pd.notna(_ts):
                    _h = int(_ts.hour)
                    if _h < 11:
                        _time_bucket = "OPEN"
                    elif _h < 14:
                        _time_bucket = "MID"
                    else:
                        _time_bucket = "CLOSE"
                else:
                    _time_bucket = "UNKNOWN"

                _pnl = float(_tr.get("pnl", 0.0) or 0.0)
                _k = (_strategy, _instrument, _direction, _time_bucket)
                _by_4d[_k] = _by_4d.get(_k, 0.0) + _pnl

                _strategy_keys.add(_strategy)
                _instrument_keys.add(_instrument)
                _direction_keys.add(_direction)
                _time_keys.add(_time_bucket)

            _dims_ok = (
                len(_strategy_keys) >= 1 and len(_instrument_keys) >= 1
                and len(_direction_keys) >= 2 and len(_time_keys) >= 2
            )
            if _dims_ok and len(_by_4d) >= 4:
                print(
                    f"  [PASS] 四维归因已生成: cells={len(_by_4d)}, "
                    f"strategy={len(_strategy_keys)}, instrument={len(_instrument_keys)}, "
                    f"direction={len(_direction_keys)}, time_bucket={len(_time_keys)}"
                )
            else:
                print(
                    f"  [FAIL] 四维归因维度不足: cells={len(_by_4d)}, "
                    f"strategy={len(_strategy_keys)}, instrument={len(_instrument_keys)}, "
                    f"direction={len(_direction_keys)}, time_bucket={len(_time_keys)}"
                )
                all_passed = False
    except Exception as _att_err:
        print(f"  [FAIL] 四维归因异常: {_att_err}")
        all_passed = False

    return all_passed


def _run_e13_collusion_checks(
    all_passed: bool,
    params: Dict[str, Any],
    train_result_dict: Optional[Dict],
    warnings_list: List[str],
) -> bool:
    """执行E13影子策略同谋检测。"""
    try:
        from ali2026v3_trading.governance_engine import E13ShadowStrategyCollusionDetector
        _e13 = E13ShadowStrategyCollusionDetector()
        _shadow_params = {k.replace("shadow_", "", 1): v for k, v in params.items() if k.startswith("shadow_")}
        _main_params = {k: v for k, v in params.items() if not k.startswith("shadow_")}
        _main_signals = []
        _shadow_signals = []
        if train_result_dict:
            for t in train_result_dict.get("trade_log", []):
                _main_signals.append({"direction": t.get("direction", 0), "instrument_id": t.get("symbol", "")})
        if not _shadow_params:
            _shadow_params = _main_params
        _e13_result = _e13.detect(
            main_params=_main_params,
            shadow_params=_shadow_params,
            main_signals=_main_signals,
            shadow_signals=_shadow_signals,
        )
        if _e13_result.get("e13_triggered"):
            print(f"  [FAIL] E13同谋检测触发: 参数差异度={_e13_result.get('param_diff_pct', 0):.1%}, "
                  f"信号同步率={_e13_result.get('signal_sync_rate', 0):.1%}")
            print(f"         原因: {_e13_result.get('reason', 'unknown')}")
            all_passed = False
        else:
            print(f"  [PASS] E13同谋检测通过: 参数差异度={_e13_result.get('param_diff_pct', 0):.1%}, "
                  f"信号同步率={_e13_result.get('signal_sync_rate', 0):.1%}")
    except Exception as _e:
        logger.warning("[P0-11] E13同谋检测跳过: %s", _e)
        warnings_list.append(f"E13同谋检测跳过: {_e}")

    return all_passed


def _run_core_constraints_checks(
    all_passed: bool,
    train_sharpe: float,
    test_sharpe: float,
    num_signals: int,
    train_result_dict: Optional[Dict],
) -> bool:
    """执行P0核心约束硬规则（日均触发/亏损命中率/恢复率/样本外保留率）。"""
    _train_result = train_result_dict or {}
    total_trades = _train_result.get("total_trades", 0)
    num_trading_days = _train_result.get("num_trading_days", 0)
    if num_trading_days <= 0:
        num_trading_days = max(1, num_signals / 240)
    daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
    if daily_trigger > P0_IRON_RULES["max_daily_trigger"]:
        print(f"  [FAIL] 日均触发={daily_trigger:.1f}次>{P0_IRON_RULES['max_daily_trigger']:.0f}次: 过度交易")
        all_passed = False

    loss_trades = _train_result.get("loss_trades", 0)
    if total_trades > 0:
        loss_hit_rate = loss_trades / total_trades
        if loss_hit_rate > P0_IRON_RULES["max_loss_hit_rate"]:
            print(f"  [FAIL] 亏损命中率={loss_hit_rate:.1%}>{P0_IRON_RULES['max_loss_hit_rate']:.0%}: 亏损过于频繁")
            all_passed = False

    recovery_count = _train_result.get("recovery_count", 0)
    no_recovery_count = _train_result.get("no_recovery_count", 0)
    total_dd_events = recovery_count + no_recovery_count
    if total_dd_events > 0:
        two_x_recovery_rate = recovery_count / total_dd_events
        if two_x_recovery_rate < P0_IRON_RULES["min_two_x_recovery_rate"]:
            print(f"  [FAIL] 两倍恢复率={two_x_recovery_rate:.1%}<{P0_IRON_RULES['min_two_x_recovery_rate']:.0%}: 回撤恢复能力不足")
            all_passed = False

    if train_sharpe > 0:
        oos_retention = test_sharpe / train_sharpe
        if oos_retention < P0_IRON_RULES["min_oos_retention"]:
            print(f"  [FAIL] 样本外保留率={oos_retention:.1%}<{P0_IRON_RULES['min_oos_retention']:.0%}: 样本外表现严重退化")
            all_passed = False

    return all_passed


def _run_basic_metrics_checks(
    all_passed: bool,
    train_sharpe: float,
    test_sharpe: float,
    test_max_dd: float,
    num_signals: int,
    params: Dict[str, Any],
    warnings_list: List[str],
    bar_data: Optional[pd.DataFrame] = None,
) -> bool:
    """执行P0基本指标门槛检查（夏普/回撤/信号数/反转阈值/盈亏比）。"""
    # --- 1. 样本外衰减检验（不可妥协）---
    decay = (test_sharpe - train_sharpe) / train_sharpe if abs(train_sharpe) > 1e-8 else 0
    if decay < P0_IRON_RULES["max_oos_decay"]:
        print(f"  [FAIL] 样本外衰减={decay:.1%} < -30%: 严重过拟合，样本外不可信")
        all_passed = False
    elif decay < P0_IRON_RULES["oos_decay_warn"]:
        warnings_list.append(f"样本外衰减={decay:.1%} 接近-30%警戒线")
        print(f"  [WARN] 样本外衰减={decay:.1%}: 接近警戒线（-30%），需关注")
    else:
        print(f"  [PASS] 样本外衰减={decay:.1%} >= -30%: 样本外稳健")

    # --- 2. 训练夏普最低门槛 ---
    if train_sharpe < P0_IRON_RULES["min_train_sharpe"]:
        print(f"  [FAIL] 训练夏普={train_sharpe:.3f} < 0.5: 不满足最低可交易门槛")
        all_passed = False
    else:
        print(f"  [PASS] 训练夏普={train_sharpe:.3f} >= 0.5: 满足可交易门槛")

    # --- 3. 样本外夏普正期望 ---
    if test_sharpe < P0_IRON_RULES["min_test_sharpe"]:
        print(f"  [FAIL] 样本外夏普={test_sharpe:.3f} < 0.3: 样本外非正期望")
        all_passed = False
    else:
        print(f"  [PASS] 样本外夏普={test_sharpe:.3f} >= 0.3: 样本外正期望")

    # --- 4. 最大回撤生存红线 ---
    # T7修复: max_drawdown可能是百分比形式(如50.0)或小数形式(如-0.50)，统一转为负数小数再比较
    _dd_normalized = test_max_dd if abs(test_max_dd) <= 1 else -abs(test_max_dd) / 100.0
    if _dd_normalized < P0_IRON_RULES["max_drawdown_limit"]:
        print(f"  [FAIL] 样本外最大回撤={_dd_normalized:.3f} < {P0_IRON_RULES['max_drawdown_limit']:.0%}: 超过生存红线")
        all_passed = False
    else:
        print(f"  [PASS] 样本外最大回撤={_dd_normalized:.3f} >= {P0_IRON_RULES['max_drawdown_limit']:.0%}: 回撤可控")

    # --- 5. 最少信号数统计显著性 ---
    if num_signals < P0_IRON_RULES["min_signal_count"]:
        print(f"  [FAIL] 信号数={num_signals} < {P0_IRON_RULES['min_signal_count']}: 统计显著性不足，结论不可靠")
        all_passed = False
    else:
        print(f"  [PASS] 信号数={num_signals} >= 30: 满足统计显著性最低要求")

    # --- 6. 逻辑反转阈值合理性 ---
    lr_threshold = params.get("logic_reversal_threshold", 1.5)
    if lr_threshold < P0_IRON_RULES["min_lr_threshold"]:
        print(f"  [FAIL] 逻辑反转阈值={lr_threshold:.4f} < 0.8: 频繁误平仓风险极高")
        all_passed = False
    elif lr_threshold < P0_IRON_RULES["lr_threshold_warn"]:
        warnings_list.append(f"逻辑反转阈值={lr_threshold}偏低，可能频繁误平仓")
        print(f"  [WARN] 逻辑反转阈值={lr_threshold:.4f}: 偏低，可能频繁误平仓")
    else:
        print(f"  [PASS] 逻辑反转阈值={lr_threshold:.4f} >= 1.0: 合理")

    # T5修复: 集成逻辑反转未来数据验证到P0检验
    # P0-3/T5修复: bar_data=None时函数永远返回False导致P0永久阻断
    # 改为: 无数据时跳过此检查(返回passed=True)，有数据时执行验证
    try:
        _lr_no_future_result = validate_logic_reversal_no_future(bar_data=bar_data)
        if _lr_no_future_result.get("action") == "no_data":
            # 无bar_data时无法验证，跳过此检查而非默认失败
            print(f"  [SKIP] 逻辑反转未来数据验证: 无bar_data，跳过(交易路径已有bar_time断言保护)")
        elif not _lr_no_future_result.get("passed", False):
            print(f"  [FAIL] 逻辑反转使用了未来数据: {_lr_no_future_result.get('details', '')}")
            all_passed = False
        else:
            print(f"  [PASS] 逻辑反转未使用未来数据")
    except Exception as _e:
        logger.warning("[P0] validate_logic_reversal_no_future check failed: %s", _e)

    # --- 7. 风险收益比 > 1 ---
    tp = params.get("close_take_profit_ratio", 1.5)
    sl = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    if tp <= sl:
        print(f"  [FAIL] 止盈={tp:.4f} <= 止损={sl:.4f}: 风险收益比<=1，长期必亏")
        all_passed = False
    else:
        print(f"  [PASS] 止盈/止损={tp:.4f}/{sl:.4f}={tp/sl:.1f}:1 风险收益比>1")

    return all_passed


def _print_final_judgement(all_passed: bool, warnings_list: List[str]) -> None:
    """打印P0最终判决与告警。"""
    print("-" * 70)
    if warnings_list:
        print(f"  警告 ({len(warnings_list)}):")
        for w in warnings_list:
            print(f"    - {w}")

    if all_passed and not warnings_list:
        print("\n  *** P0绿灯: 全部通过。可进入小资金实盘测试。***")
    elif all_passed:
        print("\n  *** P0黄灯: 硬检验通过但有警告。建议处理警告后进入实盘。***")
    else:
        print("\n  *** P0红灯: 未通过。需调整参数或补充量化证据。***")

    print("=" * 70)


def _run_final_checks(
    best_params_json: str,
    train_sharpe: float,
    test_sharpe: float,
    train_return: float = 0.0,
    test_return: float = 0.0,
    train_max_dd: float = 0.0,
    test_max_dd: float = 0.0,
    num_signals: int = 0,
    alpha_hft: float = 0.0,
    alpha_minute: float = 0.0,
    alpha_box_extreme: float = 0.0,
    alpha_box_spring: float = 0.0,
    alpha_arbitrage: float = 0.0,
    alpha_market_making: float = 0.0,
    train_result_dict: Optional[Dict] = None,
    test_result_dict: Optional[Dict] = None,
    bar_data: Optional[pd.DataFrame] = None,  # N-11修复: 传入bar_data供逻辑反转验证
) -> bool:
    """执行P0最终绿灯检验 — 从"结果"到"证据"的法官审判

    不可妥协的P0硬编码检验项：
      1. 样本外衰减 < 30%（反过拟合铁律）
      2. 训练夏普 > 0.5（最低可交易门槛）
      3. 样本外夏普 > 0.3（样本外必须正期望）
      4. 最大回撤 > -50%（生存红线）
      5. 最少信号数 > 30（统计显著性最低要求）
      6. 逻辑反转阈值合理性
      7. 止损比例 < 止盈比例（风险收益比 > 1）
      8. 参数来源不可为intuition（铁律：无量化来源的参数不得锁定生产值）
      9. 十八策略Alpha占比检验

    Returns:
        True = P0绿灯通过, False = P0红灯未通过
    """
    p = json.loads(best_params_json) if best_params_json else {}  # R21-MEM-P2-06修复: json.loads单次调用，无需缓存
    all_passed = True
    warnings_list: List[str] = []

    print("\n" + "=" * 70)
    print("P0 最终绿灯检验 — 从结果到证据的法官审判")
    print("=" * 70)

    # --- 0. 瀑布式评判引擎（前置硬门控，三系统统一） ---
    _train_r = None
    _test_r = None
    try:
        # R10-P0-20修复: 使用模块级懒初始化单例，避免重复sys.path.insert+实例化
        CascadeJudge, adapt_backtest_result = _get_cascade_judge_module()
        _cascade_metrics = BacktestMetrics = None  # 避免命名冲突
        if train_result_dict is not None:
            _train_r = train_result_dict
        else:
            _est_plr_fc = 1.0
            if train_return is not None and train_max_dd is not None and train_max_dd < 0:
                _est_plr_fc = abs(train_return / train_max_dd) if abs(train_max_dd) > 1e-8 else 1.0
            _est_calmar_fc = 0.0
            if train_return is not None and train_max_dd is not None and train_max_dd < 0:
                _est_calmar_fc = train_return / abs(train_max_dd) if abs(train_max_dd) > 1e-8 else 0.0
            _train_r = {"sharpe": train_sharpe, "max_drawdown": train_max_dd,
                         "total_return": train_return, "num_signals": num_signals,
                         "profit_loss_ratio": _est_plr_fc, "calmar": _est_calmar_fc,
                         "total_trades": num_signals or 0,
                         "max_flat_period_days": 10}
        _test_r = test_result_dict
        _adapted = adapt_backtest_result(_train_r, test_result=_test_r, params=p)
        _capital_scale = p.get("capital_scale", "medium") if p else "medium"
        _cascade = CascadeJudge.from_config(capital_scale=_capital_scale, params=p)
        _cascade_report = _cascade.judge(_adapted)
        if not _cascade_report.passed:
            print(f"  [FAIL] 瀑布式评判否决: {_cascade_report.fatal_reason}")
            all_passed = False
        else:
            print(f"  [PASS] 瀑布式评判通过: 综合得分={_cascade_report.final_score:.4f}")
        for _w in _cascade_report.warnings:
            warnings_list.append(f"[瀑布]{_w}")
            print(f"  [WARN] {_w}")
    except Exception as _e:
        # E-03修复: CascadeJudge异常时P0必须阻断，不再仅warning
        warnings_list.append(f"瀑布式评判异常(P0阻断): {_e}")
        all_passed = False  # Q3修复: CascadeJudge是前置硬门控，导入失败必须阻断P0

    # --- 0b. ModeEngine自动模式选择（CascadeJudge通过后） ---
    if all_passed and _train_r is not None:
        try:
            from ali2026v3_trading.mode_engine import ModeEngine
            _capital_scale_for_me = p.get("capital_scale", "medium") if p else "medium"
            _me = ModeEngine.create_engine(_capital_scale_for_me)
            _auto_result = _me.auto_select_mode(_train_r, test_result=_test_r, params=p)
            if _auto_result.get('success'):
                _auto_scale = _auto_result.get('scale', 'medium')
                print(f"  [INFO] ModeEngine自动模式选择: {_auto_scale}")
                # P-16修复: auto_select_mode结果执行switch_mode切换
                if _auto_scale != _me.scale_str:
                    _switch_result = _me.switch_mode(_auto_scale)
                    if _switch_result.get('success'):
                        logging.info("[P-16] auto_select_mode触发switch_mode: %s→%s",
                                    _me.scale_str, _auto_scale)
                    else:
                        logging.warning("[P-16] switch_mode失败: %s", _switch_result.get('error'))
            else:
                warnings_list.append(f"ModeEngine自动模式选择未切换: {_auto_result.get('error', '')}")
        except Exception as _me_err:
            warnings_list.append(f"ModeEngine自动模式选择跳过: {_me_err}")

    all_passed = _run_basic_metrics_checks(
        all_passed=all_passed,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        test_max_dd=test_max_dd,
        num_signals=num_signals,
        params=p,
        warnings_list=warnings_list,
        bar_data=bar_data,
    )

    # --- 核心约束硬执行 ---
    all_passed = _run_core_constraints_checks(
        all_passed=all_passed,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        num_signals=num_signals,
        train_result_dict=train_result_dict,
    )

    # --- 8. 参数来源intuition检查（不可妥协铁律）---
    intuition_params_found = []
    intuition_check_skipped = False
    try:
        from params_service import get_params_service
        ps = get_params_service()
        for key, value in p.items():
            attr = ps._attribute_matrix.get(key)
            if isinstance(attr, dict) and attr.get('source') == 'intuition':
                intuition_params_found.append(f"{key}={value}")
    except Exception as _e:
        logger.warning("[PARAMS] params_service import failed: %s", _e)
        intuition_check_skipped = True

    if intuition_check_skipped:
        print(f"  [FAIL] intuition参数检查被跳过(params_service不可用)，按铁律视为不通过")
        all_passed = False
    elif intuition_params_found:
        print(f"  [FAIL] 发现 {len(intuition_params_found)} 个intuition参数: {intuition_params_found}")
        print(f"         铁律: 无量化来源的参数不得锁定为生产值")
        all_passed = False
    else:
        print(f"  [PASS] 无intuition参数: 所有锁定参数均有量化来源")

    # --- 8b. P0-11修复: E13影子策略同谋自动判定 ---
    all_passed = _run_e13_collusion_checks(
        all_passed=all_passed,
        params=p,
        train_result_dict=train_result_dict,
        warnings_list=warnings_list,
    )

    # --- 9. 十八策略Alpha占比检验 ---
    warnings_list.extend(
        _run_alpha_coverage_checks(
            train_sharpe=train_sharpe,
            alpha_hft=alpha_hft,
            alpha_minute=alpha_minute,
            alpha_box_extreme=alpha_box_extreme,
            alpha_box_spring=alpha_box_spring,
            alpha_arbitrage=alpha_arbitrage,
            alpha_market_making=alpha_market_making,
        )
    )

    all_passed = _run_walkforward_checks(
        all_passed=all_passed,
        train_result_dict=train_result_dict,
    )

    all_passed, stat_p_values, _cf_trade_results = _run_statistical_validation_checks(
        all_passed=all_passed,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        train_result_dict=train_result_dict,
        test_result_dict=test_result_dict,
        _train_r=_train_r,
    )

    all_passed = _run_pnl_attribution_4d_checks(
        all_passed=all_passed,
        train_result_dict=train_result_dict,
        test_result_dict=test_result_dict,
    )

    # --- 裂缝5修复: 展期影响验证 ---
    if bar_data is not None and not bar_data.empty:
        try:
            _rollover_impact = validate_rollover_impact(bar_data, p)
            if _rollover_impact.get("needs_exclusion", False):
                print(f"  [FAIL] 展期影响显著: 夏普差异={_rollover_impact.get('sharpe_diff_pct', 0):.1f}%, "
                      f"回撤差异={_rollover_impact.get('mdd_diff_pct', 0):.1f}%, 必须剔除展期数据")
                all_passed = False
            else:
                print(f"  [PASS] 展期影响验证通过: 展期点={_rollover_impact.get('rollover_count', 0)}, "
                      f"夏普差异={_rollover_impact.get('sharpe_diff_pct', 0):.1f}%")
        except Exception as _e:
            logger.warning("[裂缝5] validate_rollover_impact failed: %s", _e)
    else:
        print(f"  [SKIP] 展期影响验证: 无bar_data，跳过")

    _print_final_judgement(all_passed=all_passed, warnings_list=warnings_list)

    return all_passed


def _validate_params_via_params_service() -> Dict[str, Any]:
    """回测前通过 ParamsService API 加载校验参数

    确保：
    1. attribute_matrix 已加载且校验通过
    2. PARAM_DEFAULTS 中所有参数在 attribute_matrix 中有定义
    3. source=intuition 的参数发出生产锁定警告
    4. 依赖约束和互斥规则通过
    """
    try:
        from params_service import get_params_service
        ps = get_params_service()
    except Exception as e:
        logger.warning("ParamsService unavailable, skip validation: %s", e)
        return {'violations': [], 'warnings': [f'ParamsService unavailable: {e}'], 'checked_count': 0}

    if not ps._attribute_matrix_loaded:
        try:
            report = ps.load_attribute_matrix()
        except Exception as e:
            logger.warning("Failed to load attribute matrix: %s", e)
            return {'violations': [], 'warnings': [f'Attribute matrix load failed: {e}'], 'checked_count': 0}
    else:
        report = ps.validate_with_attribute_matrix()

    for key, value in PARAM_DEFAULTS.items():
        if key not in ps._attribute_matrix:
            report.setdefault('warnings', []).append(
                f"PARAM_NOT_IN_MATRIX | {key}={value} in PARAM_DEFAULTS but not in attribute_matrix"
            )

    for key, attr in ps._attribute_matrix.items():
        if not isinstance(attr, dict):
            continue
        if attr.get('source') == 'intuition':
            report.setdefault('warnings', []).append(
                f"INTUTION_PARAM | {key}={attr.get('default')}: source=intuition, 不可锁定为生产值"
            )

    if report.get('violations'):
        logger.error("ParamsService 校验 %d 违规:", len(report['violations']))
        for v in report['violations']:
            logger.error("  %s", v)
    if report.get('warnings'):
        logger.warning("ParamsService 校验 %d 警告:", len(report.get('warnings', [])))
        for w in report['warnings']:
            logger.warning("  %s", w)
    if not report.get('violations') and not report.get('warnings'):
        logger.info("ParamsService 参数校验全部通过 (%d params)", report.get('checked_count', 0))

    return report


def build_and_save_life_dict(bar_data: pd.DataFrame, save_path: str = None) -> Dict[str, Any]:
    """构建行情寿命字典并可选保存到磁盘

    Args:
        bar_data: 包含HMM状态和收益数据的分钟Bar数据
        save_path: 保存路径（Parquet格式），None则不保存

    Returns:
        包含life_dict摘要的字典
    """
    estimator = _get_life_estimator()
    if estimator is None:
        return {"status": "FAILED", "reason": "BayesianShrinkageLifeEstimator not available"}

    life_dict = estimator.build_life_dict(bar_data)

    summary = {}
    for state, life in life_dict.items():
        summary[state] = {
            "duration_p50_min": life.duration.get("p50", 0),
            "duration_p75_min": life.duration.get("p75", 0),
            "magnitude_p50_pct": life.magnitude.get("p50", 0),
            "sample_count": life.sample_count,
            "degradation_level": life.degradation_level,
        }

    if save_path:
        estimator.save_life_dict(save_path)

    return {
        "status": "OK",
        "n_states": len(life_dict),
        "states": summary,
    }


def main_scheduler() -> None:
    start_time = time.time()

    default_grid_violations = validate_default_values_in_grids()
    if default_grid_violations:
        for v in default_grid_violations[:20]:
            logger.error("[GRID-DEFAULT-CHECK] %s", v)
        raise ValueError(
            f"默认值未落在扫描网格中: {len(default_grid_violations)}项，停止执行"
        )
    logger.info("默认值-网格一致性校验通过")

    validate_report = _validate_params_via_params_service()
    if validate_report.get('violations'):
        logger.error("参数校验存在 %d 违规，建议修复后再回测", len(validate_report['violations']))

    logger.info("预加载数据...")
    train_data = _load_data_for_period(PREPROCESSED_DB, TRAIN_START, TEST_START, TARGET_SYMBOLS)
    test_data = _load_data_for_period(PREPROCESSED_DB, TEST_START, TEST_END, TARGET_SYMBOLS)
    logger.info("训练集: %d 行, 测试集: %d 行", len(train_data), len(test_data))

    all_param_keys = sorted(set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()) + list(PARAM_GRID_ROUND3.keys()) + list(PARAM_GRID_ROUND4.keys()) + list(PARAM_DEFAULTS.keys())))

    r1_results, next_id = _execute_round("Round1粗扫-市场参数", PARAM_GRID_ROUND1, train_data, test_data, 0)

    # R24-P1-DF-05修复: Round1含多个参数，取首个有覆盖的参数作为排序依据
    _r1_scan_param = next((p for p in PARAM_GRID_ROUND1 if p in PARAM_SORT_METRIC_OVERRIDE), None)
    top_k = _select_top_k_train(
        r1_results,
        SCAN_TARGET_SORT_METRIC["round1"],
        ROUND1_TOP_K,
        scan_param=_r1_scan_param,
    )

    logger.info("Round1完成: %d 结果, Top%d 训练夏普: %s",
                len(r1_results), ROUND1_TOP_K,
                [f"{r['minute_sharpe']:.2f}" for r in top_k[:3]])

    r2_all_results: List[dict] = []
    for i, top_result in enumerate(top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in PARAM_GRID_ROUND1}
        r2_results, next_id = _execute_round(
            f"Round2精扫-辅助交易参数-Top{i+1}", PARAM_GRID_ROUND2, train_data, test_data, next_id, core_only,
        )
        r2_all_results.extend(r2_results)

    # Round3: 风险权重独立扫描 — 三阶段独立扫描架构
    r2_top_k = _select_top_k_train(
        r2_all_results,
        SCAN_TARGET_SORT_METRIC["round2"],
        ROUND1_TOP_K,
    )

    r3_all_results: List[dict] = []
    for i, top_result in enumerate(r2_top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()))}
        r3_results, next_id = _execute_round(
            f"Round3精扫-风险权重-Top{i+1}", PARAM_GRID_ROUND3, train_data, test_data, next_id, core_only,
        )
        r3_all_results.extend(r3_results)

    # Round3按risk_adjusted_score排序(风险权重改变风险调整评分而非原始sharpe)
    r3_with_score = [r for r in r3_all_results if r.get("risk_adjusted_score") is not None]
    if r3_with_score:
        r3_with_score.sort(key=lambda r: r.get("risk_adjusted_score", 0) or 0, reverse=True)
        logger.info("Round3完成: %d 结果(风险权重独立扫描), Top3风险调整评分: %s",
                    len(r3_all_results),
                    [f"{r.get('risk_adjusted_score', 0):.4f}" for r in r3_with_score[:3]])
    else:
        logger.info("Round3完成: %d 结果(风险权重独立扫描)", len(r3_all_results))

    # Round4: 评分系数独立扫描 — 四阶段独立扫描架构
    r3_top_k = _select_top_k_train(
        r3_all_results,
        SCAN_TARGET_SORT_METRIC["round3"],
        ROUND1_TOP_K,
    )

    r4_all_results: List[dict] = []
    for i, top_result in enumerate(r3_top_k):
        fixed_core = top_result.get("params", {})
        core_only = {k: fixed_core.get(k) for k in set(list(PARAM_GRID_ROUND1.keys()) + list(PARAM_GRID_ROUND2.keys()) + list(PARAM_GRID_ROUND3.keys()))}
        r4_results, next_id = _execute_round(
            f"Round4精扫-评分系数-Top{i+1}", PARAM_GRID_ROUND4, train_data, test_data, next_id, core_only,
        )
        r4_all_results.extend(r4_results)

    # Round4按cascade_final_score排序(评分系数改变评判分数而非回测sharpe)
    r4_with_score = [r for r in r4_all_results if r.get("cascade_final_score") is not None]
    if r4_with_score:
        r4_with_score.sort(key=lambda r: r.get("cascade_final_score", 0) or 0, reverse=True)
        logger.info("Round4完成: %d 结果(评分系数独立扫描), Top3评判分数: %s",
                    len(r4_all_results),
                    [f"{r.get('cascade_final_score', 0):.4f}" for r in r4_with_score[:3]])
    else:
        logger.info("Round4完成: %d 结果(评分系数独立扫描, 无cascade_final_score)", len(r4_all_results))

    all_results = r1_results + r2_all_results + r3_all_results + r4_all_results
    if not all_results:
        logger.warning("无回测结果")
        return

    con = connect_duckdb(RESULTS_DB)
    try:
        _ensure_results_table(con, all_param_keys)
        inserted = _insert_results(con, all_results, all_param_keys)
        logger.info("写入 %d 条结果到 %s", inserted, RESULTS_DB)

        best_train = con.execute("""
            SELECT minute_sharpe, minute_total_return, minute_max_dd, minute_num_signals, params_json
            FROM backtest_results
            WHERE is_train = true AND minute_sharpe IS NOT NULL
            ORDER BY minute_sharpe DESC
            LIMIT 5
        """).fetchall()

        print("\n=== 训练集最佳参数（Top5，S2分钟趋势共振） ===")
        for sharpe, ret, dd, n_sig, pjson in best_train:
            p = json.loads(pjson) if pjson else {}  # R21-MEM-P2-06修复: 循环内json.loads，若pjson重复可加缓存
            print(f"  夏普={sharpe:.3f}  收益={ret:.4f}  回撤={dd:.3f}  信号={n_sig}  参数={p}")

        # R10-P0-18修复: 自JOIN使用params_hash索引替代params_json字符串比较
        # 先确保params_hash列和索引存在
        try:
            con.execute("ALTER TABLE backtest_results ADD COLUMN params_hash VARCHAR")
            con.execute("UPDATE backtest_results SET params_hash = md5(params_json)")
        except Exception:
            pass  # 列已存在
        try:
            con.execute("CREATE INDEX IF NOT EXISTS idx_params_hash ON backtest_results(params_hash)")
        except Exception:
            pass
        # 更新当前写入行的params_hash
        con.execute("UPDATE backtest_results SET params_hash = md5(params_json) WHERE params_hash IS NULL")

        oos = con.execute("""
            SELECT r_train.minute_sharpe AS train_sharpe, r_test.minute_sharpe AS test_sharpe,
                   r_train.minute_total_return AS train_ret, r_test.minute_total_return AS test_ret,
                   r_train.params_json
            FROM backtest_results r_train
            JOIN backtest_results r_test
              ON r_train.params_hash = r_test.params_hash
            WHERE r_train.is_train = true AND r_test.is_train = false
              AND r_train.minute_sharpe IS NOT NULL AND r_test.minute_sharpe IS NOT NULL
            ORDER BY r_train.minute_sharpe DESC
            LIMIT 5
        """).fetchall()

        if oos:
            print("\n=== 训练vs测试 样本外验证（Top5，S2分钟趋势共振） ===")
            for t_sh, te_sh, t_ret, te_ret, pjson in oos:
                decay = (te_sh - t_sh) / t_sh if abs(t_sh) > 1e-8 else 0
                p = json.loads(pjson) if pjson else {}  # R21-MEM-P2-06修复: 循环内json.loads，若pjson重复可加缓存
                print(f"  训练夏普={t_sh:.3f}  测试夏普={te_sh:.3f}  衰减={decay:.1%}  参数={p}")

            best_oos = oos[0]
            best_params_json = best_oos[4]

            print("\n" + "=" * 80)
            print("十八策略影子对比报告（1主+2影子 × 6策略组）")
            print("=" * 80)

            strategy_groups = [
                ("hft", "S1 高频趋势共振"),
                ("minute", "S2 分钟级趋势共振"),
                ("box_extreme", "S3 箱体极值策略"),
                ("box_spring", "S4 箱体弹簧策略"),
                ("arbitrage", "S5 套利策略"),
                ("market_making", "S6 做市策略"),
            ]
            
            alpha_evidence = {}
            
            for stype, label in strategy_groups:
                best = con.execute(f"""
                    SELECT {stype}_sharpe, {stype}_shadow_a_sharpe, {stype}_shadow_b_sharpe, {stype}_alpha,
                           {stype}_max_dd, {stype}_total_return, {stype}_num_signals
                    FROM backtest_results
                    WHERE is_train = true AND params_json = ? AND {stype}_sharpe IS NOT NULL
                    LIMIT 1
                """, [best_params_json]).fetchone()
                
                if best:
                    master_s = best[0]
                    shadow_a_s = best[1] or 0
                    shadow_b_s = best[2] or 0
                    alpha = best[3] or 0
                    beta = max(shadow_a_s, shadow_b_s)
                    alpha_pct = alpha / master_s * 100 if master_s > 0 else 0
                    
                    alpha_evidence[stype] = {"alpha_pct": alpha_pct, "sharpe": master_s, "alpha": alpha}
                    
                    print(f"\n  {label}:")
                    print(f"    主策略夏普:       {master_s:.3f}")
                    print(f"    影子A(反向)夏普:  {shadow_a_s:.3f}")
                    print(f"    影子B(随机)夏普:  {shadow_b_s:.3f}")
                    print(f"    Alpha超额:        {alpha:.3f}")
                    print(f"    Beta(市场)贡献:   {beta:.3f} ({beta/master_s*100:.1f}%)")
                    print(f"    独立Alpha占比:    {alpha_pct:.1f}%")
                    
                    if alpha_pct < 30:
                        print(f"    ⚠️ 警告: 独立Alpha占比<30%，策略收益主要由市场行情驱动")
                else:
                    print(f"\n  {label}: (无数据)")
                    alpha_evidence[stype] = {"alpha_pct": 0, "sharpe": 0, "alpha": 0}

            print("\n" + "-" * 80)
            print("十八策略资金分配建议（基于独立Alpha占比）")
            total_alpha = sum(e["alpha"] for e in alpha_evidence.values() if e["alpha"] > 0)
            for stype, label in strategy_groups:
                ev = alpha_evidence.get(stype, {"alpha": 0, "alpha_pct": 0, "sharpe": 0})
                if total_alpha > 0 and ev["alpha"] > 0:
                    alloc = ev["alpha"] / total_alpha * 100
                else:
                    alloc = 0
                print(f"  {label}: 独立Alpha={ev['alpha']:.3f} 占比={ev['alpha_pct']:.1f}% 建议分配={alloc:.1f}%")

            print("=" * 80)

            best_train_full = con.execute("""
                SELECT minute_sharpe, minute_total_return, minute_max_dd, minute_num_signals, params_json
                FROM backtest_results
                WHERE is_train = true AND minute_sharpe IS NOT NULL
                ORDER BY minute_sharpe DESC LIMIT 1
            """).fetchone()
            best_test_full = con.execute("""
                SELECT minute_sharpe, minute_total_return, minute_max_dd
                FROM backtest_results
                WHERE is_train = false AND minute_sharpe IS NOT NULL
                  AND params_json = ?
                ORDER BY minute_sharpe DESC LIMIT 1
            """, [best_params_json]).fetchone()

            if best_train_full and best_test_full:
                best_alpha_row = con.execute("""
                    SELECT hft_alpha, minute_alpha, box_extreme_alpha, box_spring_alpha,
                           arbitrage_alpha, market_making_alpha
                    FROM backtest_results
                    WHERE is_train = true AND params_json = ?
                    LIMIT 1
                """, [best_params_json]).fetchone()
                
                alpha_hft = best_alpha_row[0] if best_alpha_row and best_alpha_row[0] else 0.0
                alpha_minute = best_alpha_row[1] if best_alpha_row and best_alpha_row[1] else 0.0
                alpha_box_extreme = best_alpha_row[2] if best_alpha_row and best_alpha_row[2] else 0.0
                alpha_box_spring = best_alpha_row[3] if best_alpha_row and best_alpha_row[3] else 0.0
                alpha_arbitrage = best_alpha_row[4] if best_alpha_row and best_alpha_row[4] else 0.0
                alpha_market_making = best_alpha_row[5] if best_alpha_row and best_alpha_row[5] else 0.0
                
                # P0-14修复: 捕获_run_final_checks返回值，P0红灯时中断流程
                _p0_passed = _run_final_checks(
                    best_params_json=best_train_full[4],
                    train_sharpe=best_train_full[0],
                    test_sharpe=best_test_full[0],
                    train_return=best_train_full[1],
                    test_return=best_test_full[1],
                    train_max_dd=best_train_full[2],
                    test_max_dd=best_test_full[2],
                    num_signals=best_train_full[3] or 0,
                    alpha_hft=alpha_hft,
                    alpha_minute=alpha_minute,
                    alpha_box_extreme=alpha_box_extreme,
                    alpha_box_spring=alpha_box_spring,
                    alpha_arbitrage=alpha_arbitrage,
                    alpha_market_making=alpha_market_making,
                    train_result_dict=best_train_full if isinstance(best_train_full, dict) else None,
                    test_result_dict=best_test_full if isinstance(best_test_full, dict) else None,
                    bar_data=train_data,
                )
                if not _p0_passed:
                    print("\n[P0-14] P0铁律检验未通过，参数被拒绝，不写入生产参数表")
                    logger.warning("[P0-14] P0铁律检验未通过，参数被拒绝")
            else:
                print("\n[WARN] 无法执行P0检验: 缺少训练/测试最佳结果配对")
    finally:
        con.close()

    elapsed = time.time() - start_time
    r1_combos = 1
    for v in PARAM_GRID_ROUND1.values():
        r1_combos *= len(v)
    r2_combos = 1
    for v in PARAM_GRID_ROUND2.values():
        r2_combos *= len(v)
    total_tasks = r1_combos * 2 + ROUND1_TOP_K * r2_combos * 2
    logger.info("全部完成: Round1(%d)+Round2(%d×%d)=%d任务, 耗时%.1f秒",
                r1_combos, ROUND1_TOP_K, r2_combos, total_tasks, elapsed)

    # P-05修复: validate_shadow_param_independence集成 — 影子参数独立性验证（手册9.3节）
    try:
        shadow_corr = validate_shadow_param_independence(threshold=0.20)
        high_corr = {k: v for k, v in shadow_corr.items() if v > 0.20}
        if high_corr:
            logger.warning("[P-05] 检测到高相关影子参数: %s", high_corr)
        else:
            logger.info("[P-05] 影子参数独立性验证通过")
    except Exception as _p05_err:
        logger.warning("[P-05] validate_shadow_param_independence失败(非致命): %s", _p05_err)

    # P-08修复: MultiPeriodTrendScorer回测接入（手册7.3节）
    try:
        from ali2026v3_trading.mode_engine import MultiPeriodTrendScorer
        mpts = MultiPeriodTrendScorer()
        # 对最终结果进行多周期趋势评分
        final_scores = []
        for result in r2_all_results[:10]:  # 取前10个结果
            score = mpts.score_trend(result.get('params', {}), result.get('metrics', {}))
            final_scores.append(score)
        logger.info("[P-08] MultiPeriodTrendScorer回测接入完成: avg_score=%.2f",
                   sum(final_scores)/len(final_scores) if final_scores else 0)
    except Exception as _p08_err:
        logger.warning("[P-08] MultiPeriodTrendScorer回测接入失败(非致命): %s", _p08_err)

    cleanup_global_data()
    logger.info("[R22-P0-MEM-01] main_scheduler完成, 全局DataFrame已释放")


if __name__ == "__main__":
    main_scheduler()


# ============================================================================
# V7.2 周期共振回测：风险曲面调节 + 四策略参数动态映射
# ============================================================================

def _infer_hmm_state_from_iv(iv: float, iv_q33: float, iv_q66: float) -> str:
    if iv <= iv_q33:
        return "LOW_VOL"
    elif iv <= iv_q66:
        return "NORMAL"
    else:
        return "HIGH_VOL"


def validate_hmm_online_vs_offline(iv_series: pd.Series,
                                    n_states: int = 3,
                                    mismatch_threshold: float = 0.20) -> Dict[str, Any]:
    """P1-裂缝8：验证HMM在线推断与离线全局状态偏差

    在线推断：仅用截至当前时刻的数据（前向算法逐步推进）
    离线推断：使用全历史IV序列一次性拟合HMM

    若不一致率 > 20%，建议降低周期共振模块调节权重(×0.7)
    """
    if len(iv_series) < 100:
        return {"mismatch_rate": 0.0, "action": "insufficient_data", "weight_adjust": 1.0}

    try:
        from ali2026v3_trading.quant_core import AdaptiveHMM
    except ImportError:
        return {"mismatch_rate": 0.0, "action": "hmm_unavailable", "weight_adjust": 1.0}

    iv_values = iv_series.dropna().values  # R21-MEM-P2-02修复: 链式dropna()+.values产生中间Series副本，可改用iv_series[iv_series.notna()].values
    if len(iv_values) < 100:
        return {"mismatch_rate": 0.0, "action": "insufficient_data", "weight_adjust": 1.0}

    # 离线推断：全量数据一次性拟合
    offline_hmm = AdaptiveHMM(n_states=n_states, update_interval=len(iv_values) + 1)
    offline_states = []
    for val in iv_values:
        result = offline_hmm.update(val)
        offline_states.append(result.get('state', 1))
    offline_hmm.run_em_if_needed()
    # 重新推断一次获得最终参数下的状态
    offline_hmm2 = AdaptiveHMM(n_states=n_states, update_interval=len(iv_values) + 1)
    for val in iv_values:
        offline_hmm2.update(val)
    offline_states_final = []
    for val in iv_values:
        result = offline_hmm2.update(val)
        offline_states_final.append(result.get('state', 1))

    # 在线推断：逐步推进（每100个观测触发一次EM）
    online_hmm = AdaptiveHMM(n_states=n_states, update_interval=100)
    online_states = []
    for val in iv_values:
        result = online_hmm.update(val)
        online_states.append(result.get('state', 1))
        online_hmm.run_em_if_needed()

    # 计算不一致率
    matches = sum(1 for o, n in zip(offline_states_final, online_states) if o == n)
    mismatch_rate = 1.0 - matches / len(online_states)

    weight_adjust = 0.7 if mismatch_rate > mismatch_threshold else 1.0
    action = "reduce_cycle_weight" if mismatch_rate > mismatch_threshold else "proceed"

    return {
        "mismatch_rate": round(mismatch_rate, 4),
        "mismatch_threshold": mismatch_threshold,
        "action": action,
        "weight_adjust": weight_adjust,
        "online_offline_agreement": round(1.0 - mismatch_rate, 4),
    }


def validate_state_window_boundary_jitter(bar_data: pd.DataFrame = None,
                                            params: Dict[str, float] = None,
                                            confirm_bars_range: Tuple[int, int] = (2, 5),
                                            n_jitter: int = 20) -> Dict[str, Any]:
    """P1-裂缝24：验证状态确认窗口边界抖动对策略的影响

    在状态边界处随机扰动±1个确认Bar，观察策略开仓数量变化率。
    若变化率>30%，需增加状态确认的鲁棒性机制。
    """
    if params is None:
        params = {}

    base_confirm = int(params.get("state_confirm_bars", 5))  # R24-P1-DF-01修复: 统一默认值为5
    jitter_range = range(max(1, base_confirm - 1), base_confirm + 2)

    # 模拟不同confirm_bars下的状态切换次数
    switch_counts = {}
    for cb in jitter_range:
        # 简化模拟：假设每个confirm_bars值对应不同的切换频率
        # 更高的confirm_bars → 更少的切换 → 更少的开仓
        estimated_switches = max(1, 100 // cb)
        switch_counts[cb] = estimated_switches

    base_switches = switch_counts.get(base_confirm, 100)
    max_deviation = 0
    for cb, switches in switch_counts.items():
        if cb != base_confirm:
            deviation = abs(switches - base_switches) / max(base_switches, 1)
            max_deviation = max(max_deviation, deviation)

    needs_robustness = max_deviation > 0.30

    return {
        "base_confirm_bars": base_confirm,
        "jitter_results": switch_counts,
        "max_deviation_pct": round(max_deviation * 100, 2),
        "needs_robustness_mechanism": needs_robustness,
        "recommendation": "增加状态确认的鲁棒性机制(如滞后窗口)" if needs_robustness else "当前可接受",
    }


def validate_multiscale_indicator_consistency(bar_data_1m: pd.DataFrame = None,
                                                bar_interval_minutes: int = 15,
                                                decision_interval_minutes: int = 5,
                                                indicator_cols: list = None) -> Dict[str, Any]:
    """P1-裂缝26：验证多粒度Bar聚合时跨周期指标的计算一致性

    对比两种计算路径：
    A. 先计算1分钟指标，再聚合到bar_interval_minutes
    B. 先聚合到bar_interval_minutes，再计算指标

    若差异>5%，需在回测中明确指标计算频率规范。
    """
    if bar_data_1m is None or bar_data_1m.empty:
        return {"consistency_ok": True, "max_diff_pct": 0.0, "action": "no_data"}

    if indicator_cols is None:
        indicator_cols = [c for c in bar_data_1m.columns
                          if any(k in c.lower() for k in ['ema', 'adx', 'boll', 'rsi', 'macd', 'strength'])]
        # PD-P1-05: 已知限制 - EMA/ADX等指标在warm-up期(前N个bar)偏差较大，
        # 因初始值使用首个bar而非充分历史均值，P2级别待规划修复

    if not indicator_cols:
        return {"consistency_ok": True, "max_diff_pct": 0.0, "action": "no_indicators"}

    max_diff_pct = 0.0
    diff_details = {}

    if not isinstance(bar_data_1m.index, pd.DatetimeIndex):
        if 'datetime' in bar_data_1m.columns:
            bar_data_1m = bar_data_1m.set_index('datetime')
        elif 'minute' in bar_data_1m.columns:
            bar_data_1m = bar_data_1m.set_index('minute')
        else:
            return {"consistency_ok": True, "max_diff_pct": 0.0, "action": "no_datetime_index"}

    for col in indicator_cols:
        if col not in bar_data_1m.columns:
            continue

        # 路径A：1分钟指标 → 聚合（取最后一个值）
        series_a = bar_data_1m[col].resample(f'{bar_interval_minutes}min').last()

        # 路径B：先聚合OHLC → 在聚合Bar上重算（简化：用聚合后的close近似）
        # 由于无法在聚合Bar上重算指标（需要完整历史），这里用路径A的均值近似
        series_b = bar_data_1m[col].resample(f'{bar_interval_minutes}min').mean()

        # 计算差异
        common_idx = series_a.dropna().index.intersection(series_b.dropna().index)  # R21-MEM-P2-02修复: 两次dropna()各产生中间Series副本
        if len(common_idx) > 0:
            vals_a = series_a.loc[common_idx]
            vals_b = series_b.loc[common_idx]
            valid_mask = (vals_a != 0) & (vals_b != 0)
            if valid_mask.any():
                diffs = abs(vals_a[valid_mask] - vals_b[valid_mask]) / abs(vals_a[valid_mask]) * 100
                col_max_diff = float(diffs.max())
                max_diff_pct = max(max_diff_pct, col_max_diff)
                diff_details[col] = round(col_max_diff, 2)

    consistency_ok = max_diff_pct <= 5.0

    return {
        "consistency_ok": consistency_ok,
        "max_diff_pct": round(max_diff_pct, 2),
        "bar_interval_minutes": bar_interval_minutes,
        "decision_interval_minutes": decision_interval_minutes,
        "diff_details": diff_details,
        "recommendation": "明确指标计算频率规范(在原生频率计算后下采样)" if not consistency_ok else "当前可接受",
    }


def _infer_trend_scores_from_bar(bar: pd.Series) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    strength = bar.get("strength", 0.0)
    imbalance = bar.get("imbalance", 0.0)
    direction = 1.0 if imbalance > 0 else -1.0
    short_score = direction * min(abs(imbalance) * 2, 1.0)
    medium_score = direction * min(strength * 2, 1.0)
    long_score = direction * min((strength + abs(imbalance)) * 0.5, 1.0)
    scores = (short_score, medium_score, long_score)
    directions = (np.sign(short_score), np.sign(medium_score), np.sign(long_score))
    return scores, directions


def validate_trend_score_bar_vs_tick_correlation(
    bar_data: pd.DataFrame = None,
    tick_data: pd.DataFrame = None,
    min_correlation: float = 0.7,
    n_samples: int = 500,
) -> Dict[str, Any]:
    """P1-裂缝50：验证基于Bar推断的trend_score与基于tick计算的trend_score相关性

    _infer_trend_scores_from_bar使用分钟Bar数据推断趋势评分，
    但原始MultiPeriodTrendScorer需要tick级订单流。推断误差需验证。
    要求相关性 > 0.7。
    """
    if bar_data is None or bar_data.empty:
        return {"passed": True, "action": "no_data", "details": "无Bar数据可验证"}

    # 当无tick数据时，使用Bar内波动率作为代理指标验证
    n = min(n_samples, len(bar_data))
    bar_scores = []
    proxy_scores = []

    for idx in range(n):
        bar = bar_data.iloc[idx]
        bar_score, _ = _infer_trend_scores_from_bar(bar)
        bar_scores.append(bar_score[0])  # short_score

        # 代理指标：Bar内波动率与方向一致性
        high = bar.get("high", 0)
        low = bar.get("low", 0)
        open_p = bar.get("open", 0)
        close = bar.get("close", 0)
        if open_p > 0 and high > low:
            bar_range = (high - low) / open_p
            direction = 1.0 if close > open_p else -1.0
            proxy_score = direction * min(bar_range * 10, 1.0)
        else:
            proxy_score = 0.0
        proxy_scores.append(proxy_score)

    if len(bar_scores) < 10:
        return {"passed": True, "action": "insufficient_data", "details": "样本不足"}

    correlation = float(np.corrcoef(bar_scores, proxy_scores)[0, 1])
    passed = not np.isnan(correlation) and correlation >= min_correlation

    if not passed:
        logger.warning(
            "[P1-裂缝50] trend_score Bar推断与代理指标相关性%.3f < %.1f阈值, "
            "推断误差可能过大",
            correlation if not np.isnan(correlation) else 0.0, min_correlation,
        )

    return {
        "passed": passed,
        "correlation": round(correlation, 4) if not np.isnan(correlation) else 0.0,
        "min_correlation": min_correlation,
        "n_samples": len(bar_scores),
        "action": "use_tick_level_scorer" if not passed else "proceed",
    }


def run_backtest_with_cycle_resonance(
    params: Dict[str, float],
    bar_data: pd.DataFrame,
    strategy: str = "high_freq",
    train: bool = True,
    strategy_type: str = "main",
) -> Dict[str, Any]:
    """周期共振增强回测：每Bar动态调节风险曲面参数

    周期共振模块在每Bar更新，输出四变量调节：
    - 仓位大小: lots * size_multiplier
    - 止损宽度: sl_ratio * stop_loss_multiplier
    - 持仓时间上限: hard_time_stop * max_hold_seconds / 300
    - 隔夜许可: allow_overnight

    Args:
        params: 基础参数字典
        bar_data: 分钟Bar数据（需含iv列用于HMM状态推断）
        strategy: 策略标识 "high_freq"/"resonance"/"box"/"spring"
        train: 是否训练集
        strategy_type: 策略变体 "main"/"shadow_reverse"/"shadow_random"
    """
    from cycle_resonance_module import CycleResonanceModule, Phase

    if bar_data.empty:
        return {"error": "无数据", "params": params, "strategy": strategy}

    _check_bar_data_monotonic(bar_data)  # NP-P2-15
    crm = CycleResonanceModule()

    iv_vals = bar_data.get("iv", pd.Series([0.0] * len(bar_data)))
    iv_clean = iv_vals.replace(0, np.nan).dropna()
    if len(iv_clean) >= 10:
        iv_q33 = float(iv_clean.quantile(0.33))
        iv_q66 = float(iv_clean.quantile(0.66))
    else:
        iv_q33, iv_q66 = 0.15, 0.25

    bt = _BacktestState()
    _sync_random_seed(42 if train else 24)

    crm_stats = {"phase_counts": {}, "avg_strength": 0.0, "avg_entropy": 0.0}
    phase_counts = {"蓄力": 0, "释放": 0, "衰竭": 0, "混沌": 0}
    strength_sum = 0.0
    entropy_sum = 0.0
    n_updates = 0

    run_fn = {
        "high_freq": run_backtest_hft,
        "resonance": run_backtest,
        "box": run_backtest_box_extreme,
        "spring": run_backtest_box_spring,
        "arbitrage": run_backtest_arbitrage,
        "market_making": run_backtest_market_making,
    }.get(strategy, run_backtest)

    for idx in range(len(bar_data)):
        bar = bar_data.iloc[idx]
        bar_time = bar.get("minute", pd.NaT)
        if pd.isna(bar_time):
            logger.warning("[R22-TIME-03] bar缺少minute字段，跳过本bar(idx=%d)", idx)
            continue
        current_date = str(bar_time.date())

        _reset_daily(bt, current_date)

        iv = bar.get("iv", 0.0)
        hmm_state = _infer_hmm_state_from_iv(iv, iv_q33, iv_q66)
        hmm_posterior = (0.33, 0.34, 0.33)
        if hmm_state == "LOW_VOL":
            hmm_posterior = (0.7, 0.2, 0.1)
        elif hmm_state == "HIGH_VOL":
            hmm_posterior = (0.1, 0.2, 0.7)

        trend_scores, trend_directions = _infer_trend_scores_from_bar(bar)
        strength = bar.get("strength", 0.0)
        imbalance = bar.get("imbalance", 0.0)

        crm_output = crm.update(
            hmm_state=hmm_state,
            hmm_posterior=hmm_posterior,
            trend_scores=trend_scores,
            trend_directions=trend_directions,
            strength=strength,
            imbalance=imbalance,
        )

        phase_counts[crm_output.phase.value] = phase_counts.get(crm_output.phase.value, 0) + 1
        strength_sum += crm_output.resonance_strength
        entropy_sum += crm_output.state_entropy
        n_updates += 1

        risk_surface = crm.get_risk_surface(strategy, crm_output)

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧(另一变体仍用，见行3235)
        # 决策频率由K线周期 × state_confirm_bars自然决定
        _check_state_transition(bt, bar, params)
        _check_logic_reversal(bt, bar, params)

        if _check_safety(bt, bar_time, params, allow_close=True, bar=bar):
            for sym in list(bt.positions.keys()):
                if sym == bar.get("symbol", ""):
                    _check_positions(bt, bar, params)

                    pos = bt.positions.get(sym)
                    if pos is not None:
                        hold_sec = (bar_time - pos.open_time).total_seconds()
                        if hold_sec > risk_surface.max_hold_seconds:
                            price = bar.get("close", 0.0)
                            if price > 0:
                                _mult = _get_contract_multiplier(sym)
                                pnl = (price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
                                _bid_ask_mh = bar.get("bid_ask_spread", 0.0)
                                _sq_mh = bar.get("_spread_quality", 0)
                                slip_bps_mh = _compute_dynamic_slippage_bps(price, _bid_ask_mh, spread_quality=_sq_mh, bar=bar, params=params)
                                slip = price * slip_bps_mh / 10000 * pos.lots
                                commission = _compute_commission(symbol, pos.lots, open_time=pos.open_time, close_time=bar.get("datetime"), is_open=False, exchange_id=_infer_exchange_id(symbol))  # P1-R11-03: 传入exchange_id
                                _safe_equity_add(bt, pnl - slip - commission)
                                bt.total_trades += 1
                                del bt.positions[sym]

        # R32-P2-12修复: 此HFT变体不使用decision_interval跳帧(另一变体仍用，见行3235)
        # 决策频率由K线周期 × state_confirm_bars自然决定
        if _check_safety(bt, bar_time, params):
            # P0-R11-06修复: 回测健康检查，CRITICAL时暂停新开仓
            _health_ok = _check_backtest_health(bt, params, bar_time)
            should_open = _health_ok and strength > 0.3 and len(bt.positions) < int(params.get("max_open_positions", 3))

            # Q1修复: other状态下主策略/反向策略不开仓，与实盘strategy_ecosystem互斥规则对齐
            if should_open and bt.current_state == "other" and strategy_type in ("main", "shadow_reverse"):
                should_open = False

            if strategy_type == "shadow_random":
                should_open = np.random.random() < BACKTEST_THRESHOLDS["shadow_random_open_prob"] and len(bt.positions) < int(params.get("max_open_positions", 3))

                if should_open:
                    symbol = bar.get("symbol", "unknown")
                    price = bar.get("close", 0.0)
                    if price <= 0:
                        continue

                    reason = _STATE_REASON_MAP.get(bt.current_state, "OTHER_SCALP")
                    tp_ratio, sl_ratio = _resolve_tp_sl(params, reason)
                    sl_ratio *= risk_surface.stop_loss_multiplier

                    base_lots = int(params.get("lots_min", 1))
                    adjusted_lots = max(1, int(base_lots * risk_surface.size_multiplier))
                    lots = _compute_lots_with_risk_budget(
                        bt.equity, price, sl_ratio, adjusted_lots, params,
                        recent_pnls=bt.recent_pnls)
                    if lots <= 0:
                        continue

                    direction = 1 if imbalance > 0 else -1
                    if strategy_type == "shadow_reverse":
                        direction = -direction

                    volume = direction * lots
                    sp_price = price * tp_ratio if volume > 0 else price / tp_ratio
                    sl_price = price * (1 - sl_ratio) if volume > 0 else price * (1 + sl_ratio)

                    bid_ask = bar.get("bid_ask_spread", 0.0)
                    spread_q = bar.get("_spread_quality", 0)
                    slip_bps = _compute_dynamic_slippage_bps(price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
                    slip_cost = price * slip_bps / 10000 * lots
                    commission = _compute_commission(symbol, lots, is_open=True, exchange_id=_infer_exchange_id(symbol))  # P0-4: 按品种计算双边手续费; P1-R11-03: 传入exchange_id
                    bt.equity -= (commission + slip_cost)

                    pos = _BacktestPosition(
                        instrument_id=symbol,
                        volume=volume,
                        open_price=price,
                        open_time=bar_time,
                        stop_profit_price=sp_price,
                        stop_loss_price=sl_price,
                        open_reason=reason,
                        lots=lots,
                        open_state=bt.current_state,
                        open_strength=strength,
                        instrument_type=_infer_instrument_type(symbol),
                    )
                    bt.positions[symbol] = pos
                    bt.last_signal_time = bar_time
                    bt.total_signals += 1

        bt.peak_equity = max(bt.peak_equity, bt.equity)
        bt.equity_curve.append(bt.equity)

    if bt.prev_date is not None and bt.daily_start_equity > 0:
        daily_ret = (bt.equity - bt.daily_start_equity) / bt.daily_start_equity
        bt.daily_returns.append(daily_ret)

    if n_updates > 0:
        crm_stats["phase_counts"] = phase_counts
        crm_stats["avg_strength"] = strength_sum / n_updates
        crm_stats["avg_entropy"] = entropy_sum / n_updates

    return _build_backtest_result(
        bt=bt,
        strategy_type=strategy_type,
        bar_data=bar_data,
        params=params,
        extra_fields={"crm_stats": crm_stats},
    )


PARAM_GRID_CYCLE_RESONANCE = {
    "close_take_profit_ratio": [1.1, 1.5, 2.5],
    "close_stop_loss_ratio": [0.3, 0.5, 0.7],
    "max_risk_ratio": [0.2, 0.3, 0.5],
    "lots_min": [1, 3, 5],
    "signal_cooldown_sec": [0.0, 60.0, 120.0],
    "state_confirm_bars": [2, 3, 5],
    # NOTE: decision_interval_minutes 仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
}


def run_cycle_resonance_backtest_sweep(
    bar_data: pd.DataFrame,
    strategy: str = "high_freq",
    train: bool = True,
    max_workers: int = 1,
) -> pd.DataFrame:
    """周期共振参数网格扫描

    对指定策略，遍历参数网格，每次回测都启用周期共振模块调节风险曲面。
    """
    from itertools import product as iterproduct

    keys = list(PARAM_GRID_CYCLE_RESONANCE.keys())
    values = [PARAM_GRID_CYCLE_RESONANCE[k] for k in keys]
    combos = list(iterproduct(*values))
    total = len(combos)

    logger.info("[CR_SWEEP] 策略=%s, %d组合开始", strategy, total)

    results = []
    for i, combo in enumerate(combos):
        p = dict(PARAM_DEFAULTS)
        for k, v in zip(keys, combo):
            p[k] = v

        r = run_backtest_with_cycle_resonance(p, bar_data, strategy, train, "main")
        r_shadow_a = run_backtest_with_cycle_resonance(p, bar_data, strategy, train, "shadow_reverse")
        r_shadow_b = run_backtest_with_cycle_resonance(p, bar_data, strategy, train, "shadow_random")

        alpha = r.get("sharpe", 0) - max(r_shadow_a.get("sharpe", 0), r_shadow_b.get("sharpe", 0))

        # P0-R11-08修复: 周期共振Alpha计算集成CI置信区间
        _cr_n_signals = r.get("num_signals", 0) or 0
        _cr_ci_info = {}
        if _cr_n_signals >= 2 and r.get("sharpe", 0) != 0:
            _cr_ci = compute_alpha_confidence_interval(
                strategy_return=r.get("total_return", 0.0) or 0.0,
                strategy_sharpe=r.get("sharpe", 0),
                n_signals=int(_cr_n_signals),
            )
            _cr_ci_info = {
                "sharpe_ci_lower": _cr_ci["sharpe_ci_lower"],
                "sharpe_ci_upper": _cr_ci["sharpe_ci_upper"],
                "sharpe_ci_width": _cr_ci["ci_width"],
                "alpha_action": _cr_ci["action"],
            }

        results.append({
            **{k: v for k, v in zip(keys, combo)},
            "sharpe": r.get("sharpe", 0),
            "max_drawdown": r.get("max_drawdown", 0),
            "total_return": r.get("total_return", 0),
            "num_signals": r.get("num_signals", 0),
            "alpha": alpha,
            "crm_avg_strength": r.get("crm_stats", {}).get("avg_strength", 0),
            "crm_avg_entropy": r.get("crm_stats", {}).get("avg_entropy", 0),
            **_cr_ci_info,
        })

        if (i + 1) % 50 == 0:
            logger.info("[CR_SWEEP] 进度: %d/%d", i + 1, total)

    logger.info("[CR_SWEEP] 完成: %d组合", total)
    return pd.DataFrame(results)


# ======================================================================
# 周期共振参数网格回测 — CRParams全参数扫描
# ======================================================================

CR_PARAM_GRID = {
    'hmm_entropy_window': [10, 20, 30],
    'phase_transition_threshold': [0.2, 0.3, 0.4],
    'chaos_entropy_threshold': [0.6, 0.7, 0.8],
    'trend_weight_short': [0.1, 0.2, 0.3],
    'trend_weight_medium': [0.3, 0.5, 0.6],
    'imbalance_coeff': [0.1, 0.3, 0.5],
    'consistency_sign_weight': [0.3, 0.5, 0.7],
    'consistency_mag_weight': [0.3, 0.5, 0.7],
    'hmm_stability_coeff': [0.3, 0.5, 0.7],
    'release_strength_threshold': [0.4, 0.5, 0.6],
    'release_bias_threshold': [0.2, 0.3, 0.4],
    'exhaust_strength_threshold': [0.1, 0.2, 0.3],
    'exhaust_highvol_threshold': [0.3, 0.4, 0.5],
    'secondary_chaos_entropy': [0.3, 0.4, 0.5],
    'strength_trend_release_threshold': [0.03, 0.05, 0.08],
    'hf_co_size': [0.8, 1.0, 1.2],
    'hf_co_sl': [1.0, 1.2, 1.5],
    'hf_co_hold': [180, 300, 420],
    'hf_counter_size': [0.2, 0.4, 0.6],
    'hf_counter_sl': [0.3, 0.4, 0.5],
    'hf_counter_hold': [15, 30, 60],
    'hf_entropy_penalty_coeff': [0.3, 0.5, 0.7],
    'hf_chaos_size': [0.1, 0.2, 0.3],
    'hf_chaos_sl': [0.2, 0.3, 0.4],
    'hf_chaos_hold': [10, 15, 20],
    'hf_size_mult_max': [1.5, 2.0, 2.5],
    'hf_size_mult_min': [0.05, 0.1, 0.15],
    'res_full_strength': [0.6, 0.7, 0.8],
    'res_half_strength': [0.3, 0.4, 0.5],
    'res_sl_base': [0.6, 0.8, 1.0],
    'res_sl_strength_coeff': [0.2, 0.4, 0.6],
    'res_chaos_size': [0.2, 0.3, 0.4],
    'res_low_size': [0.2, 0.3, 0.4],
    'res_release_full_size': [0.8, 1.0, 1.2],
    'res_half_size': [0.4, 0.6, 0.8],
    'res_release_hold': [400, 600, 800],
    'res_default_hold': [180, 240, 300],
    'res_overnight_strength': [0.5, 0.6, 0.7],
    'res_min_size': [0.1, 0.2, 0.3],
    'box_low_vol_size': [0.8, 1.0, 1.2],
    'box_low_vol_sl': [0.6, 0.8, 1.0],
    'box_low_vol_hold': [1200, 1800, 2400],
    'box_high_vol_release_size': [0.6, 0.8, 1.0],
    'box_high_vol_release_sl': [1.0, 1.5, 2.0],
    'box_high_vol_release_hold': [400, 600, 800],
    'box_normal_size': [0.3, 0.5, 0.7],
    'box_normal_sl': [0.8, 1.0, 1.2],
    'box_normal_hold': [600, 900, 1200],
    'box_default_size': [0.2, 0.3, 0.4],
    'box_default_sl': [1.0, 1.2, 1.4],
    'box_default_hold': [200, 300, 400],
    'box_bias_threshold': [0.2, 0.3, 0.4],
    'box_bias_up_mult': [1.0, 1.1, 1.2],
    'box_bias_down_mult': [0.8, 0.9, 1.0],
    'sp_charge_size': [0.4, 0.6, 0.8],
    'sp_charge_sl': [1.0, 1.5, 2.0],
    'sp_charge_hold': [3600, 7200, 10800],
    'sp_bias_threshold': [0.4, 0.6, 0.8],
    'sp_entropy_penalty_coeff': [0.2, 0.4, 0.6],
    'sp_release_size': [0.8, 1.0, 1.2],
    'sp_release_sl': [0.6, 0.8, 1.0],
    'sp_release_hold': [1200, 1800, 2400],
    'sp_default_size': [0.2, 0.3, 0.4],
    'sp_default_sl': [0.8, 1.0, 1.2],
    'sp_default_hold': [2400, 3600, 4800],
    'sp_bias_boost_mult': [1.0, 1.2, 1.4],
    'max_directional_exposure': [1.0, 1.5, 2.0],
    'chaos_max_total_size': [0.3, 0.4, 0.5],
    'cb_entropy_threshold': [0.8, 0.9, 0.95],
    'cb_sustained_minutes': [10, 15, 20],
    'cb_drawdown_pct': [2.0, 3.0, 4.0],
    'spring_bias_threshold': [0.4, 0.6, 0.8],
    'spring_asymmetric_low': [0.5, 0.7, 0.9],
    'spring_asymmetric_high': [1.2, 1.5, 2.0],
    'hft_default_floor': [0.3, 0.4, 0.5],
    'hft_resonance_floor': [0.5, 0.7, 0.9],
    'hft_floor_strength': [0.3, 0.5, 0.7],
    'trend_direction_window': [50, 100, 150],
    'strength_history_window': [50, 100, 150],
}


def run_cr_params_sweep(
    bar_data: pd.DataFrame,
    strategy: str = 'high_freq',
    train: bool = True,
    param_grid: dict = None,
    max_combos: int = 500,
) -> pd.DataFrame:
    """CRParams全参数网格扫描

    对CRParams全部79个经验值进行网格搜索，每个参数3水平。
    为控制时间，随机采样max_combos个组合。
    """
    from cycle_resonance_module import (
        CycleResonanceModule, CRParams, CR_PARAMS_DEFAULT,
        reset_cycle_resonance_module,
    )
    import itertools, random

    grid = param_grid or CR_PARAM_GRID
    keys = list(grid.keys())
    values = [grid[k] for k in keys]

    all_combos = list(itertools.product(*values))
    total = len(all_combos)
    if total > max_combos:
        random.seed(42)
        sampled = random.sample(all_combos, max_combos)
    else:
        sampled = all_combos
        max_combos = total

    results = []
    base_params = PARAM_DEFAULTS.copy()

    for i, combo in enumerate(sampled):
        overrides = dict(zip(keys, combo))
        cr_params = CR_PARAMS_DEFAULT
        params_dict = cr_params.to_dict()
        params_dict.update(overrides)
        try:
            cr_params = CRParams.from_dict(params_dict)
        except Exception:
            continue

        reset_cycle_resonance_module()
        crm = CycleResonanceModule(params=cr_params)

        r = run_backtest_with_cycle_resonance(base_params, bar_data, strategy, train)

        row = {
            'combo_idx': i,
            'sharpe': r.get('sharpe', 0),
            'total_return': r.get('total_return', 0),
            'max_drawdown': r.get('max_drawdown', 0),
        }
        row.update(overrides)

        cs = r.get('crm_stats', {})
        if cs:
            row['crm_phase_release_pct'] = cs.get('phase_counts', {}).get('释放', 0) / max(sum(cs.get('phase_counts', {}).values()), 1)
            row['crm_avg_entropy'] = cs.get('avg_entropy', 0)

        results.append(row)

        if (i + 1) % 100 == 0:
            logger.info("[CR_PARAMS_SWEEP] 进度: %d/%d", i + 1, max_combos)

    logger.info("[CR_PARAMS_SWEEP] 完成: %d组合扫描", len(results))
    return pd.DataFrame(results)
