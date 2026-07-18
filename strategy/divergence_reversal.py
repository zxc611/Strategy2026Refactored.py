# MODULE_ID: M1-252
"""Divergence Reversal Strategy Module — 背离反转策略模块

独立策略模块, 不从属于数据预处理管线。

三层背离检测体系:
  L1: 跨期期货背离 — 当月期货创出新高/低，但季月主力合约未跟随
  L2: 远月期权权利金集体背离 — 下月及远月期权权利金未创出新高/低
  L3: 当月期权近实值权利金背离 — 同向最接近实值的期权权利金未创出新高/低

期权价值状态五级分类 (Moneyness):
  DITM=0 深度实值  moneyness > 2*ATM_THRESH
  ITM =1 实值      ATM_THRESH < moneyness <= 2*ATM_THRESH
  ATM =2 平值      |moneyness| <= ATM_THRESH
  OTM =3 虚值      -2*ATM_THRESH <= moneyness < -ATM_THRESH
  DOTM=4 深度虚值  moneyness < -2*ATM_THRESH

使用方式:
  from strategy.divergence_reversal import (
      DivergenceReversalModule,
      DivergenceReversalOutput,
      DivergenceReversalParams,
      get_divergence_reversal_module,
  )
  module = get_divergence_reversal_module()
  output = module.update(df, current_ym=(2026, 6))
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from infra._helpers import get_logger

logger = get_logger(__name__)

# ── 期权价值状态枚举 ──────────────────────────────────────────────
MONEYNESS_DITM = 0  # 深度实值 Deep In-The-Money
MONEYNESS_ITM = 1   # 实值 In-The-Money
MONEYNESS_ATM = 2   # 平值 At-The-Money
MONEYNESS_OTM = 3   # 虚值 Out-of-The-Money
MONEYNESS_DOTM = 4  # 深度虚值 Deep Out-of-The-Money

MONEYNESS_LABELS = {
    MONEYNESS_DITM: "DITM",
    MONEYNESS_ITM: "ITM",
    MONEYNESS_ATM: "ATM",
    MONEYNESS_OTM: "OTM",
    MONEYNESS_DOTM: "DOTM",
}

# 季月: 3/6/9/12
QUARTERLY_MONTHS = {3, 6, 9, 12}

# ── 正则回退解析（仅作为 InstrumentCacheService 不可用时的最后手段） ──
_RE_FUTURE = re.compile(r'^([A-Za-z]+)(\d{3,4})$')
_RE_OPTION = re.compile(r'^([A-Za-z]+)(\d{3,4})-?([CP])-?(\d+(?:\.\d+)?)$')


# ══════════════════════════════════════════════════════════════════
# 参数与输出数据类
# ══════════════════════════════════════════════════════════════════

@dataclass
class DivergenceReversalParams:
    """背离反转模块参数 — 全部从 param pool 读取

    [FIX-20260712-S7-V3] 用户本意重构:
    - kline_period: 使用日K线/小时K线 (非分钟K线)
    - primary_layer: 以L3(当月最活跃实值期权权利金背离)为主信号
    - min_active_volume: 最活跃实值期权最低成交量筛选
    - 不叠加MACD/RSI技术指标背离
    """
    lookback: int = 20               # 新高/新低回看窗口
    atm_threshold: float = 0.03      # 平值阈值
    w_future: float = 0.35           # L1权重
    w_option_coll: float = 0.35      # L2权重
    w_option_itm: float = 0.30       # L3权重 (derived: 1.0 - w_future - w_option_coll)
    consistency_boost: float = 1.5   # 三层同向加成因子
    min_ratio: float = 0.6           # 远月期权集体背离比例阈值
    trend_significance: float = 1e-6 # 趋势斜率显著性阈值
    div_strength_clip: float = 1.0   # 背离强度裁剪上限
    signal_threshold: float = 0.15   # 信号触发阈值, 综合信号绝对值需超过此值才触发
    take_profit_ratio: float = 1.8   # 止盈比率
    stop_loss_ratio: float = 0.3     # 止损比率
    max_risk_ratio: float = 0.5      # 最大风险比率
    hard_time_stop_min: float = 60.0 # 硬止损时间(分钟)
    cooldown_bars: int = 10          # 冷却期(bar数)
    position_scale: float = 0.3      # 仓位缩放系数
    moneyness_depth: float = 0.06    # 五级分类深度实/虚值阈值(=2*atm_threshold by default)
    shadow_variant: str = "master"   # 影子变体: "master" / "shadow_a" / "shadow_b"
    # [FIX-20260712-S7-V3] 新增参数 — 用户本意: 日K/小时K线 + 最活跃实值期权
    kline_period: str = "hourly"     # K线周期: "daily" / "hourly" / "minute"
    min_active_volume: float = 1.0   # 最活跃实值期权最低成交量(筛选非活跃合约)
    primary_layer: str = "L3"        # 主信号层: "L3"(用户本意) / "L1L2L3"(三层综合)
    # [FIX-20260712-S7-V3-P2] 活跃度度量方式 + SQL缓存TTL
    activity_metric: str = "total_volume"  # "total_volume" / "last_bar_volume" / "combined"
    sql_cache_ttl_sec: float = 0.0  # SQL查询缓存TTL(秒), 0=按kline_period自动调整

    def __post_init__(self):
        # 确保 w_option_itm 满足权重守恒
        self.w_option_itm = round(1.0 - self.w_future - self.w_option_coll, 6)
        # moneyness_depth 默认 = 2 * atm_threshold (若未显式设置)
        if self.moneyness_depth == 0.06 and self.atm_threshold != 0.03:
            self.moneyness_depth = round(2.0 * self.atm_threshold, 6)
        # Shadow 变体参数调整 (8个参数: lookback/consistency/position/cooldown + tp/sl/risk/time)
        if self.shadow_variant == "shadow_a":
            self.lookback = max(5, int(self.lookback * 0.8))
            self.consistency_boost = round(self.consistency_boost * 0.9, 4)
            self.position_scale = round(self.position_scale * 0.8, 4)
            self.cooldown_bars = max(1, int(self.cooldown_bars * 1.2))
            self.take_profit_ratio = round(self.take_profit_ratio * 0.67, 4)
            self.stop_loss_ratio = round(self.stop_loss_ratio * 2.0, 4)
            self.max_risk_ratio = round(self.max_risk_ratio * 0.3, 4)
            self.hard_time_stop_min = round(self.hard_time_stop_min * 0.8, 4)
        elif self.shadow_variant == "shadow_b":
            self.lookback = max(3, int(self.lookback * 0.6))
            self.consistency_boost = round(self.consistency_boost * 0.8, 4)
            self.position_scale = round(self.position_scale * 0.6, 4)
            self.cooldown_bars = max(1, int(self.cooldown_bars * 1.5))
            self.take_profit_ratio = round(self.take_profit_ratio * 0.61, 4)
            self.stop_loss_ratio = round(self.stop_loss_ratio * 2.33, 4)
            self.max_risk_ratio = round(self.max_risk_ratio * 0.2, 4)
            self.hard_time_stop_min = round(self.hard_time_stop_min * 0.6, 4)

    def to_dict(self) -> Dict:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: Dict) -> DivergenceReversalParams:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_param_pool(cls, shadow_variant: str = "master") -> DivergenceReversalParams:
        """从 param pool 读取参数，回退到默认值"""
        params = {"shadow_variant": shadow_variant}
        try:
            from config.config_service import get_param
            _KEY_MAP = {
                "lookback": "divergence_lookback",
                "atm_threshold": "divergence_atm_threshold",
                "w_future": "divergence_w_future",
                "w_option_coll": "divergence_w_option_coll",
                "consistency_boost": "divergence_consistency_boost",
                "min_ratio": "divergence_min_ratio",
                "trend_significance": "divergence_trend_significance",
                "div_strength_clip": "divergence_div_strength_clip",
                "signal_threshold": "divergence_signal_threshold",
                "take_profit_ratio": "divergence_take_profit_ratio",
                "stop_loss_ratio": "divergence_stop_loss_ratio",
                "max_risk_ratio": "divergence_max_risk_ratio",
                "hard_time_stop_min": "divergence_hard_time_stop_min",
                "cooldown_bars": "divergence_cooldown_bars",
                "position_scale": "divergence_position_scale",
                "moneyness_depth": "divergence_moneyness_depth",
                # [S7-V3] 新增参数
                "kline_period": "divergence_kline_period",
                "min_active_volume": "divergence_min_active_volume",
                "primary_layer": "divergence_primary_layer",
                # [S7-V3-P2] 活跃度度量 + SQL缓存TTL
                "activity_metric": "divergence_activity_metric",
                "sql_cache_ttl_sec": "divergence_sql_cache_ttl_sec",
            }
            # FIX: get_param不支持点分路径，直接从params.yaml读取parameter_attributes
            _pa = {}
            try:
                import os as _os
                from infra.serialization_utils import yaml_safe_load
                _yaml_path = _os.path.join(
                    _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                    'config', 'params.yaml'
                )
                with open(_yaml_path, 'r', encoding='utf-8') as _f:
                    _raw = yaml_safe_load(_f) or {}
                _pa = _raw.get('parameter_attributes', {})
            except (IOError, OSError, ValueError, KeyError, TypeError, ImportError):
                pass
            for attr, key in _KEY_MAP.items():
                # 优先从params.yaml嵌套读取，回退到get_param扁平读取
                _saef = _pa.get(key, {})
                val = _saef.get('default') if isinstance(_saef, dict) else None
                if val is None:
                    val = get_param(key)
                if val is not None:
                    params[attr] = type(cls.__dataclass_fields__[attr].default)(val)
        except (ImportError, AttributeError, KeyError, TypeError):
            pass
        return cls(**params)


@dataclass
class DivergenceReversalOutput:
    """背离反转模块输出"""
    option_moneyness_state: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.int32))
    div_future_cross_term: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.float64))
    div_option_premium_coll: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.float64))
    div_option_near_itm: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.float64))
    div_reversal_signal: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.float64))
    n_rows: int = 0
    n_bearish: int = 0   # 看跌背离次数
    n_bullish: int = 0   # 看涨背离次数

    def to_dict(self) -> Dict:
        return {
            "n_rows": self.n_rows,
            "n_bearish": self.n_bearish,
            "n_bullish": self.n_bullish,
            "avg_reversal_signal": float(np.mean(self.div_reversal_signal)) if self.n_rows > 0 else 0.0,
        }

    def to_dataframe(self) -> pd.DataFrame:
        if self.n_rows == 0:
            return pd.DataFrame(
                np.zeros((0, 5)),
                columns=[
                    'option_moneyness_state', 'div_future_cross_term',
                    'div_option_premium_coll', 'div_option_near_itm',
                    'div_reversal_signal',
                ],
            )
        return pd.DataFrame({
            'option_moneyness_state': self.option_moneyness_state,
            'div_future_cross_term': self.div_future_cross_term,
            'div_option_premium_coll': self.div_option_premium_coll,
            'div_option_near_itm': self.div_option_near_itm,
            'div_reversal_signal': self.div_reversal_signal,
        })


# ══════════════════════════════════════════════════════════════════
# [FIX-20260712-S7-V3] 交易信号数据类 — 用户本意: 反趋势开仓
# ══════════════════════════════════════════════════════════════════

@dataclass
class DivergenceSignal:
    """背离反转交易信号 — 明确的开仓方向信号

    用户本意:
    - 日K线/小时K线上，期货趋势在继续(出现新高/新低)
    - 当月最活跃实值期权权利金趋势没有继续(不再出现新高/新低)
    - → 反趋势开仓(期货创新高但期权不创新高 → 看跌反趋势开仓)
    """
    direction: str = ""              # "BUY"(看涨反趋势) | "SELL"(看跌反趋势) | ""
    strength: float = 0.0            # 信号强度 [0, 1]
    futures_symbol: str = ""         # 期货合约代码
    option_symbol: str = ""          # 最活跃实值期权合约代码
    futures_price: float = 0.0       # 期货当前价格
    option_premium: float = 0.0      # 期权权利金(当前)
    futures_new_high: bool = False   # 期货是否创出新高
    futures_new_low: bool = False    # 期货是否创出新低
    option_new_high: bool = False    # 期权权利金是否创出新高
    option_new_low: bool = False     # 期权权利金是否创出新低
    divergence_type: str = ""        # "bearish"(看跌背离) | "bullish"(看涨背离)
    reason: str = "DIVERGENCE_REVERSAL"
    kline_period: str = "hourly"     # K线周期
    timestamp: float = 0.0           # 信号时间戳
    bar_time: str = ""               # K线时间标签

    @property
    def is_valid(self) -> bool:
        """信号是否有效(有明确方向)"""
        return self.direction in ("BUY", "SELL") and self.strength > 0.0

    @property
    def is_counter_trend_buy(self) -> bool:
        """是否为反趋势做多(看涨背离: 期货新低但期权不创新低)"""
        return self.direction == "BUY"

    @property
    def is_counter_trend_sell(self) -> bool:
        """是否为反趋势做空(看跌背离: 期货新高但期权不创新高)"""
        return self.direction == "SELL"

    def to_dict(self) -> Dict:
        return {
            "direction": self.direction,
            "strength": round(self.strength, 4),
            "futures_symbol": self.futures_symbol,
            "option_symbol": self.option_symbol,
            "futures_price": self.futures_price,
            "option_premium": self.option_premium,
            "futures_new_high": self.futures_new_high,
            "futures_new_low": self.futures_new_low,
            "option_new_high": self.option_new_high,
            "option_new_low": self.option_new_low,
            "divergence_type": self.divergence_type,
            "reason": self.reason,
            "kline_period": self.kline_period,
            "timestamp": self.timestamp,
            "bar_time": self.bar_time,
        }


# ══════════════════════════════════════════════════════════════════
# 1. 期权价值状态五级分类
# ══════════════════════════════════════════════════════════════════

def compute_option_moneyness_state(
    underlying_price: np.ndarray,
    strike_price: np.ndarray,
    option_type: np.ndarray,
    atm_threshold: float = 0.03,
    moneyness_depth: float = 0.06,
) -> np.ndarray:
    """向量化计算期权价值状态五级分类

    Parameters
    ----------
    underlying_price : 标的价格数组
    strike_price     : 行权价数组
    option_type      : 期权类型数组 (字符串 'C'/'CALL' 或 'P'/'PUT')
    atm_threshold    : 平值阈值 (默认3%)
    moneyness_depth  : 五级分类深度实/虚值阈值 (默认6%=2*atm_threshold)

    Returns
    -------
    np.ndarray : 整数数组, 值域 0-4 (DITM/ITM/ATM/OTM/DOTM)
    """
    n = len(underlying_price)
    if n == 0:
        return np.array([], dtype=np.int32)

    underlying_safe = np.where(np.isfinite(underlying_price) & (underlying_price > 0),
                               underlying_price, 1.0)
    strike_safe = np.where(np.isfinite(strike_price) & (strike_price > 0),
                           strike_price, 1.0)

    # 标准化期权类型为大写首字母
    opt_str = option_type.astype(str)
    is_call = np.zeros(n, dtype=bool)
    for i in range(n):
        s = opt_str[i].strip().upper()
        is_call[i] = s.startswith('C')

    # 计算内在价值比率 moneyness
    # Call: moneyness = (underlying - strike) / underlying
    # Put:  moneyness = (strike - underlying) / underlying
    moneyness = np.where(
        is_call,
        (underlying_safe - strike_safe) / underlying_safe,
        (strike_safe - underlying_safe) / underlying_safe,
    )
    moneyness = np.nan_to_num(moneyness, nan=0.0)

    # 五级分类
    double_atm = moneyness_depth
    state = np.full(n, MONEYNESS_ATM, dtype=np.int32)

    state[moneyness > double_atm] = MONEYNESS_DITM
    state[(moneyness > atm_threshold) & (moneyness <= double_atm)] = MONEYNESS_ITM
    state[(moneyness >= -double_atm) & (moneyness < -atm_threshold)] = MONEYNESS_OTM
    state[moneyness < -double_atm] = MONEYNESS_DOTM

    return state


# ══════════════════════════════════════════════════════════════════
# 2. 合约月份解析工具
# ══════════════════════════════════════════════════════════════════

def _parse_ym_str(ym_str: str) -> Tuple[int, int]:
    """解析 year_month 字符串（来自 instrument metadata）

    支持3位(如609→2026年9月)和4位(如2609→2026年9月)格式
    """
    ym = int(ym_str)
    if ym < 1000:
        # 3位: YMM 格式, 需要推断年代
        from datetime import datetime
        year_digit = ym // 100
        month = ym % 100
        current_decade = (datetime.now().year // 10) * 10
        year = current_decade + year_digit
        if abs(year - datetime.now().year) > 5:
            year = current_decade - 10 + year_digit
        return year, month
    else:
        # 4位: YYMM 格式
        return 2000 + ym // 100, ym % 100


def _parse_year_month_fallback(ym_str: str) -> Tuple[int, int]:
    """正则回退解析合约年月字符串为 (year, month) 元组

    仅在 InstrumentCacheService 不可用时使用。'
    支持3位(如609→2026年9月)和4位(如2609→2026年9月)格式
    3位格式: 第一位是年的个位, 后两位是月份
    """
    ym = int(ym_str)
    if ym < 1000:
        # 3位: YMM 格式, 如 609 → Y=6, MM=09
        year_digit = ym // 100
        month = ym % 100
        # 推断年份: 当前年代(2020s) + year_digit
        from datetime import datetime
        current_decade = (datetime.now().year // 10) * 10
        year = current_decade + year_digit
        # 如果推断年份距离当前超过5年, 尝试上一个/下一个年代
        if abs(year - datetime.now().year) > 5:
            year = current_decade - 10 + year_digit
        return year, month
    else:
        # 4位: YYMM 格式, 如 2609 → 2026年9月
        return 2000 + ym // 100, ym % 100


def _classify_contract_month(year: int, month: int, current_ym: Tuple[int, int]) -> str:
    """判断合约属于当月/下月/当季/下季/远季

    [FIX-20260712-S7-V3] 动态合约月份分类 — 替代原硬编码2607/2608/2609/2612/2703/2706
    根据当前年月动态确定合约归属，避免合约到期后模块失效。

    规则:
    - 当月(current_month): 合约月份 == 当前月份
    - 下月(next_month): 合约月份 == 当前月份+1
    - 当季月(current_quarter): 当季季月(3/6/9/12)中>=当前月份的最近一个
    - 下季月(next_quarter_1): 下一季季月
    - 远季(next_quarter_2/far_quarter): 更远的季月

    Parameters
    ----------
    year       : 合约年份
    month      : 合约月份
    current_ym : 当前年月 (year, month)

    Returns
    -------
    str : 'current_month' | 'next_month' | 'current_quarter' |
          'next_quarter_1' | 'next_quarter_2' | 'far'
    """
    cur_year, cur_month = current_ym
    # 合约的绝对月数 (0-indexed: 2026年1月 = 24312)
    contract_ym = year * 12 + (month - 1)
    current_ym_abs = cur_year * 12 + (cur_month - 1)
    diff = contract_ym - current_ym_abs  # >0表示未来合约, =0表示当月, <0表示已过期

    if diff < 0:
        return 'far'  # 已过期合约归入远月

    if diff == 0:
        return 'current_month'
    if diff == 1:
        return 'next_month'

    # 计算季月(3/6/9/12, 0-indexed月份: 2/5/8/11, 即 m%3==2)
    def _next_quarter_month(base: int) -> int:
        """从base开始找>=base的最近季月(3/6/9/12)"""
        m = base % 12  # 0-indexed月份 (0=1月, 2=3月, 5=6月, 8=9月, 11=12月)
        q_check = m % 3  # 季月时 q_check == 2
        if q_check == 2:
            return base  # base本身就是季月
        return base + (2 - q_check)  # 距下一个季月的月数

    # 当季月: >=当前月份的最近季月
    cur_q = _next_quarter_month(current_ym_abs)
    # 如果当季月就是当前月，则当季月归为current_month，取下一个季月
    if cur_q == current_ym_abs:
        cur_q = cur_q + 3  # 下一个季月

    next_q = cur_q + 3
    if contract_ym == cur_q:
        return 'current_quarter'
    if contract_ym == next_q:
        return 'next_quarter_1'
    if contract_ym == next_q + 3:
        return 'next_quarter_2'
    if contract_ym == next_q + 6:
        return 'far_quarter'

    return 'far'


def _build_contract_group_map(
    symbols: np.ndarray,
    symbol_ym_map: Dict[str, Tuple[int, int]],
    current_ym: Tuple[int, int],
) -> Dict[str, np.ndarray]:
    """按合约月份分组, 返回 {group_name: index_array}

    Parameters
    ----------
    symbols       : df['symbol'].values, 每行的合约代码
    symbol_ym_map : {symbol: (year, month)} 映射
    current_ym    : 当前年月 (year, month)
    """
    groups: Dict[str, List[int]] = {}
    for i, sym in enumerate(symbols):
        sym_str = str(sym)
        ym = symbol_ym_map.get(sym_str, (0, 0))
        grp = _classify_contract_month(ym[0], ym[1], current_ym)
        groups.setdefault(grp, []).append(i)
    return {k: np.array(v, dtype=np.int64) for k, v in groups.items()}


# ══════════════════════════════════════════════════════════════════
# 3. 新高/新低检测
# ══════════════════════════════════════════════════════════════════

def _rolling_new_extremum(
    values: np.ndarray,
    window: int = 20,
    detect_high: bool = True,
) -> np.ndarray:
    """滚动窗口检测是否创出新高/新低

    Returns
    -------
    np.ndarray : 布尔数组, True表示当前bar创出新高/新低
    """
    n = len(values)
    if n == 0:
        return np.array([], dtype=bool)

    result = np.zeros(n, dtype=bool)
    if n < window:
        return result

    s = pd.Series(values)
    if detect_high:
        rolling_max = s.rolling(window=window, min_periods=window).max()
        prev_rolling_max = s.shift(1).rolling(window=window, min_periods=window).max()
        result = (s.values == rolling_max.values) & ~np.isnan(rolling_max.values)
        result = result & (s.values > prev_rolling_max.fillna(s).values)
    else:
        rolling_min = s.rolling(window=window, min_periods=window).min()
        prev_rolling_min = s.shift(1).rolling(window=window, min_periods=window).min()
        result = (s.values == rolling_min.values) & ~np.isnan(rolling_min.values)
        result = result & (s.values < prev_rolling_min.fillna(s).values)

    return result


def _rolling_trend_direction(
    values: np.ndarray,
    window: int = 20,
    trend_significance: float = 1e-6,
) -> np.ndarray:
    """滚动窗口线性回归斜率方向

    Returns
    -------
    np.ndarray : +1 上升趋势, -1 下降趋势, 0 无趋势
    """
    n = len(values)
    if n < window:
        return np.zeros(n, dtype=np.float64)

    slopes = np.zeros(n, dtype=np.float64)
    x = np.arange(window, dtype=np.float64)
    x_mean = x.mean()
    x_centered = x - x_mean
    denom = np.sum(x_centered ** 2)

    for i in range(window - 1, n):
        y = values[i - window + 1: i + 1]
        if np.any(~np.isfinite(y)):
            continue
        y_mean = y.mean()
        slope = np.sum(x_centered * (y - y_mean)) / (denom + 1e-12)
        slopes[i] = slope

    slope_sign = np.sign(slopes)
    price_scale = np.where(values > 0, values, 1.0)
    normalized_slope = slopes / (price_scale + 1e-8)
    significant = np.abs(normalized_slope) > trend_significance

    return slope_sign * significant.astype(np.float64)


# ══════════════════════════════════════════════════════════════════
# 4. L1: 跨期期货背离检测
# ══════════════════════════════════════════════════════════════════

def _detect_future_cross_term_divergence(
    df: pd.DataFrame,
    symbol_ym_map: Dict[str, Tuple[int, int]],
    current_ym: Tuple[int, int],
    lookback: int = 20,
    trend_significance: float = 1e-6,
    div_strength_clip: float = 1.0,
) -> np.ndarray:
    """当月期货创出新高(低), 但季月主力合约未跟随 → 背离

    Returns
    -------
    np.ndarray : 背离强度 [-1, 1], 负值看跌背离, 正值看涨背离
    """
    n = len(df)
    if n == 0:
        return np.zeros(0, dtype=np.float64)

    result = np.zeros(n, dtype=np.float64)
    minutes = df['minute'].values

    groups = _build_contract_group_map(df['symbol'].values, symbol_ym_map, current_ym)

    cm_idx = groups.get('current_month', np.array([], dtype=np.int64))
    if len(cm_idx) == 0:
        return result

    quarter_keys = ['current_quarter', 'next_quarter_1', 'next_quarter_2', 'next_quarter_3']
    qm_indices = np.concatenate(
        [groups.get(k, np.array([], dtype=np.int64)) for k in quarter_keys]
    ) if any(k in groups for k in quarter_keys) else np.array([], dtype=np.int64)

    if len(qm_indices) == 0:
        return result

    cm_df = df.iloc[cm_idx].copy()
    qm_df = df.iloc[qm_indices].copy()

    cm_agg = cm_df.groupby('minute').agg(
        close_cm=('close', 'last'),
        high_cm=('high', 'max'),
        low_cm=('low', 'min'),
    ).sort_index()

    qm_agg = qm_df.groupby('minute').agg(
        close_qm=('close', 'last'),
        high_qm=('high', 'max'),
        low_qm=('low', 'min'),
    ).sort_index()

    merged = cm_agg.join(qm_agg, how='inner')
    if len(merged) < lookback:
        return result

    cm_new_high = _rolling_new_extremum(merged['close_cm'].values, lookback, detect_high=True)
    cm_new_low = _rolling_new_extremum(merged['close_cm'].values, lookback, detect_high=False)
    qm_new_high = _rolling_new_extremum(merged['close_qm'].values, lookback, detect_high=True)
    qm_new_low = _rolling_new_extremum(merged['close_qm'].values, lookback, detect_high=False)
    cm_trend = _rolling_trend_direction(merged['close_cm'].values, lookback, trend_significance)

    bearish_div = cm_new_high & ~qm_new_high & (cm_trend > 0)
    bullish_div = cm_new_low & ~qm_new_low & (cm_trend < 0)

    cm_slopes = _rolling_trend_direction(merged['close_cm'].values, lookback, trend_significance)
    qm_slopes = _rolling_trend_direction(merged['close_qm'].values, lookback, trend_significance)
    div_strength = np.clip(np.abs(cm_slopes - qm_slopes), 0.0, div_strength_clip)

    merged_signal = np.zeros(len(merged), dtype=np.float64)
    merged_signal[bearish_div] = -div_strength[bearish_div]
    merged_signal[bullish_div] = div_strength[bullish_div]

    minute_to_signal = dict(zip(merged.index, merged_signal))
    for i in range(n):
        m = minutes[i]
        if m in minute_to_signal:
            result[i] = minute_to_signal[m]

    return result


# ══════════════════════════════════════════════════════════════════
# 5. L2: 远月期权权利金集体背离检测
# ══════════════════════════════════════════════════════════════════

def _detect_option_premium_collective_divergence(
    df: pd.DataFrame,
    symbol_ym_map: Dict[str, Tuple[int, int]],
    current_ym: Tuple[int, int],
    lookback: int = 20,
    min_ratio: float = 0.6,
    trend_significance: float = 1e-6,
    div_strength_clip: float = 1.0,
) -> np.ndarray:
    """下月及远月期权权利金集体未创出新高(低) → 背离

    Returns
    -------
    np.ndarray : 背离强度 [-1, 1]
    """
    n = len(df)
    if n == 0:
        return np.zeros(0, dtype=np.float64)

    result = np.zeros(n, dtype=np.float64)

    has_option = 'option_type' in df.columns
    if not has_option:
        return result

    _opt_type_str = df['option_type'].fillna('').astype(str).str.strip()
    opt_mask = (_opt_type_str != '') & (_opt_type_str.str.upper() != 'NONE') \
               & (_opt_type_str.str.upper() != 'NAN')
    if not opt_mask.any():
        return result

    minutes = df['minute'].values

    groups = _build_contract_group_map(df['symbol'].values, symbol_ym_map, current_ym)
    cm_idx = groups.get('current_month', np.array([], dtype=np.int64))

    far_keys = ['next_month', 'current_quarter', 'next_quarter_1',
                'next_quarter_2', 'next_quarter_3', 'far']
    far_opt_idx = np.concatenate(
        [groups.get(k, np.array([], dtype=np.int64)) for k in far_keys]
    ) if any(k in groups for k in far_keys) else np.array([], dtype=np.int64)

    far_opt_idx = np.intersect1d(far_opt_idx, np.where(opt_mask)[0])

    if len(cm_idx) == 0 or len(far_opt_idx) == 0:
        return result

    cm_df = df.iloc[cm_idx]
    cm_agg = cm_df.groupby('minute').agg(
        close_cm=('close', 'last'),
        high_cm=('high', 'max'),
        low_cm=('low', 'min'),
    ).sort_index()

    if len(cm_agg) < lookback:
        return result

    far_opt_df = df.iloc[far_opt_idx].copy()
    far_agg = far_opt_df.groupby('minute').agg(
        close_far=('close', 'mean'),
        count_far=('close', 'count'),
    ).sort_index()

    if len(far_agg) < lookback:
        return result

    merged = cm_agg.join(far_agg, how='inner')
    if len(merged) < lookback:
        return result

    cm_new_high_aligned = _rolling_new_extremum(merged['close_cm'].values, lookback, True)
    cm_new_low_aligned = _rolling_new_extremum(merged['close_cm'].values, lookback, False)
    far_new_high_aligned = _rolling_new_extremum(merged['close_far'].values, lookback, True)
    far_new_low_aligned = _rolling_new_extremum(merged['close_far'].values, lookback, False)
    cm_trend = _rolling_trend_direction(merged['close_cm'].values, lookback, trend_significance)

    bearish = cm_new_high_aligned & ~far_new_high_aligned & (cm_trend > 0)
    bullish = cm_new_low_aligned & ~far_new_low_aligned & (cm_trend < 0)

    cm_slopes = _rolling_trend_direction(merged['close_cm'].values, lookback, trend_significance)
    far_slopes = _rolling_trend_direction(merged['close_far'].values, lookback, trend_significance)
    div_strength = np.clip(np.abs(cm_slopes - far_slopes), 0.0, div_strength_clip)

    merged_signal = np.zeros(len(merged), dtype=np.float64)
    merged_signal[bearish] = -div_strength[bearish]
    merged_signal[bullish] = div_strength[bullish]

    minute_to_signal = dict(zip(merged.index, merged_signal))
    for i in range(n):
        m = minutes[i]
        if m in minute_to_signal:
            result[i] = minute_to_signal[m]

    return result


# ══════════════════════════════════════════════════════════════════
# 6. L3: 当月期权同向最接近实值权利金背离检测
# ══════════════════════════════════════════════════════════════════

def _detect_option_near_itm_divergence(
    df: pd.DataFrame,
    symbol_ym_map: Dict[str, Tuple[int, int]],
    current_ym: Tuple[int, int],
    lookback: int = 20,
    atm_threshold: float = 0.03,
    trend_significance: float = 1e-6,
    div_strength_clip: float = 1.0,
) -> np.ndarray:
    """当月期权同向最接近实值之权利金未创出新高(低) → 背离

    Returns
    -------
    np.ndarray : 背离强度 [-1, 1]
    """
    n = len(df)
    if n == 0:
        return np.zeros(0, dtype=np.float64)

    result = np.zeros(n, dtype=np.float64)

    has_option = 'option_type' in df.columns and 'strike_price' in df.columns \
                 and 'underlying_price' in df.columns
    if not has_option:
        return result

    minutes = df['minute'].values

    groups = _build_contract_group_map(df['symbol'].values, symbol_ym_map, current_ym)
    cm_idx = groups.get('current_month', np.array([], dtype=np.int64))

    _opt_str = df['option_type'].fillna('').astype(str).str.strip()
    _is_valid_opt = (_opt_str != '') & (_opt_str.str.upper() != 'NONE') & (_opt_str.str.upper() != 'NAN')
    is_future = ~_is_valid_opt
    cm_future_idx = np.intersect1d(cm_idx, np.where(is_future)[0])

    opt_mask = _is_valid_opt
    cm_opt_idx = np.intersect1d(cm_idx, np.where(opt_mask)[0])

    if len(cm_future_idx) == 0 or len(cm_opt_idx) == 0:
        return result

    cm_fut_df = df.iloc[cm_future_idx]
    cm_agg = cm_fut_df.groupby('minute').agg(
        close_cm=('close', 'last'),
    ).sort_index()

    if len(cm_agg) < lookback:
        return result

    cm_new_high = _rolling_new_extremum(cm_agg['close_cm'].values, lookback, True)
    cm_new_low = _rolling_new_extremum(cm_agg['close_cm'].values, lookback, False)
    cm_trend = _rolling_trend_direction(cm_agg['close_cm'].values, lookback, trend_significance)

    cm_opt_df = df.iloc[cm_opt_idx].copy()
    opt_type_upper = cm_opt_df['option_type'].str.upper().str.strip()
    underlying = cm_opt_df['underlying_price'].to_numpy(dtype=np.float64)
    strike = cm_opt_df['strike_price'].to_numpy(dtype=np.float64)

    is_call = opt_type_upper.str.startswith('C').values
    moneyness = np.where(
        is_call,
        (underlying - strike) / np.where(underlying > 0, underlying, 1.0),
        (strike - underlying) / np.where(underlying > 0, underlying, 1.0),
    )
    moneyness = np.nan_to_num(moneyness, nan=0.0)
    cm_opt_df['_moneyness'] = moneyness
    cm_opt_df['_is_call'] = is_call

    # 每分钟找最接近实值的看涨和看跌期权 (避免groupby.apply的索引问题)
    # 筛选实值期权
    itm_calls = cm_opt_df[cm_opt_df['_is_call'] & (cm_opt_df['_moneyness'] > 0)].copy()
    itm_puts = cm_opt_df[~cm_opt_df['_is_call'] & (cm_opt_df['_moneyness'] > 0)].copy()

    near_itm_dict = {}
    for minute_val in cm_agg.index:
        row = {}
        # 当分钟的实值看涨: moneyness最小(最接近平值)
        mc = itm_calls[itm_calls['minute'] == minute_val]
        if len(mc) > 0:
            idx = mc['_moneyness'].idxmin()
            row['near_itm_call_close'] = mc.loc[idx, 'close']
        # 当分钟的实值看跌
        mp = itm_puts[itm_puts['minute'] == minute_val]
        if len(mp) > 0:
            idx = mp['_moneyness'].idxmin()
            row['near_itm_put_close'] = mp.loc[idx, 'close']
        if row:
            near_itm_dict[minute_val] = row

    if not near_itm_dict:
        return result

    near_itm_agg = pd.DataFrame.from_dict(near_itm_dict, orient='index')
    near_itm_agg.index.name = 'minute'

    merged = cm_agg.join(near_itm_agg, how='inner')
    if len(merged) < lookback:
        return result

    # 看涨期权近实值权利金新高检测
    bearish_call = np.zeros(len(merged), dtype=bool)
    if 'near_itm_call_close' in merged.columns:
        call_series = merged['near_itm_call_close'].dropna()
        if len(call_series) >= lookback:
            call_new_high = _rolling_new_extremum(call_series.values, lookback, True)
            call_new_high_full = np.zeros(len(merged), dtype=bool)
            for j, idx in enumerate(merged.index):
                if idx in call_series.index:
                    pos = call_series.index.get_loc(idx)
                    if pos < len(call_new_high):
                        call_new_high_full[j] = call_new_high[pos]
            bearish_call = cm_new_high[:len(merged)] & ~call_new_high_full & (cm_trend[:len(merged)] > 0)

    # 看跌期权近实值权利金新低检测
    bullish_put = np.zeros(len(merged), dtype=bool)
    if 'near_itm_put_close' in merged.columns:
        put_series = merged['near_itm_put_close'].dropna()
        if len(put_series) >= lookback:
            put_new_low = _rolling_new_extremum(put_series.values, lookback, False)
            put_new_low_full = np.zeros(len(merged), dtype=bool)
            for j, idx in enumerate(merged.index):
                if idx in put_series.index:
                    pos = put_series.index.get_loc(idx)
                    if pos < len(put_new_low):
                        put_new_low_full[j] = put_new_low[pos]
            bullish_put = cm_new_low[:len(merged)] & ~put_new_low_full & (cm_trend[:len(merged)] < 0)

    cm_slopes = _rolling_trend_direction(merged['close_cm'].values, lookback, trend_significance)
    div_strength = np.clip(np.abs(cm_slopes), 0.0, div_strength_clip)

    merged_signal = np.zeros(len(merged), dtype=np.float64)
    merged_signal[bearish_call] = -div_strength[bearish_call]
    merged_signal[bullish_put] = div_strength[bullish_put]

    minute_to_signal = dict(zip(merged.index, merged_signal))
    for i in range(n):
        m = minutes[i]
        if m in minute_to_signal:
            result[i] = minute_to_signal[m]

    return result


# ══════════════════════════════════════════════════════════════════
# 7. 综合背离反转信号
# ══════════════════════════════════════════════════════════════════

def _compute_reversal_signal(
    div_future: np.ndarray,
    div_option_coll: np.ndarray,
    div_option_itm: np.ndarray,
    params: DivergenceReversalParams,
) -> np.ndarray:
    """三层背离加权合成综合反转信号"""
    n = len(div_future)
    if n == 0:
        return np.zeros(0, dtype=np.float64)

    composite = (params.w_future * div_future
                 + params.w_option_coll * div_option_coll
                 + params.w_option_itm * div_option_itm)

    signs = np.sign(np.stack([div_future, div_option_coll, div_option_itm], axis=0))
    sign_sum = np.sum(signs, axis=0)
    consistency_boost = np.where(np.abs(sign_sum) == 3, params.consistency_boost, 1.0)

    result = np.clip(composite * consistency_boost, -params.div_strength_clip, params.div_strength_clip)

    # 信号触发阈值过滤: 低于阈值的信号归零
    result[np.abs(result) < params.signal_threshold] = 0.0

    return result


# ══════════════════════════════════════════════════════════════════
# 8. 核心策略模块类
# ══════════════════════════════════════════════════════════════════

class DivergenceReversalModule:
    """背离反转策略模块

    独立策略模块, 遵循代码库约定:
    - 不继承基类
    - 线程安全 (RLock)
    - 提供 to_dict() 序列化
    - 通过 get_divergence_reversal_module() 单例工厂暴露
    """

    def __init__(self, params: Optional[DivergenceReversalParams] = None):
        self._params = params or DivergenceReversalParams()
        self._lock = threading.RLock()
        self._last_output: Optional[DivergenceReversalOutput] = None
        self._update_count = 0
        self._instrument_cache = None  # 延迟初始化 InstrumentCacheService

    @property
    def params(self) -> DivergenceReversalParams:
        return self._params

    @property
    def last_output(self) -> Optional[DivergenceReversalOutput]:
        return self._last_output

    def _get_instrument_cache(self):
        """延迟获取 InstrumentCacheService 实例"""
        if self._instrument_cache is None:
            try:
                from config._params_instrument_cache import InstrumentCacheService
                self._instrument_cache = InstrumentCacheService()
            except (ImportError, AttributeError):
                self._instrument_cache = False  # 标记不可用
        return self._instrument_cache if self._instrument_cache is not False else None

    def _resolve_symbol_ym_map(self, df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """构建 symbol -> (year, month) 映射

        优先级:
        1. df 中的 year_month 列（由 preprocess pipeline 添加）'
        2. InstrumentCacheService 的 get_instrument_meta_by_id
        3. 正则回退解析（最后手段）
        """
        symbols = df['symbol'].values
        symbol_ym_map: Dict[str, Tuple[int, int]] = {}

        # 检查 df 是否有 year_month 列
        has_ym_column = 'year_month' in df.columns

        # 获取 InstrumentCacheService
        cache = self._get_instrument_cache()

        for sym in symbols:
            sym_str = str(sym)
            if sym_str in symbol_ym_map:
                continue

            ym_resolved = None

            # 1. 尝试从 df 的 year_month 列获取
            if has_ym_column:
                ym_val = df.loc[df['symbol'] == sym_str, 'year_month'].iloc[0]
                if pd.notna(ym_val) and str(ym_val).strip():
                    try:
                        ym_resolved = _parse_ym_str(str(int(ym_val)))
                    except (ValueError, TypeError):
                        pass

            # 2. 尝试从 InstrumentCacheService 获取
            if ym_resolved is None and cache is not None:
                try:
                    meta = cache.get_instrument_meta_by_id(sym_str)
                    if meta and 'year_month' in meta and meta['year_month'] is not None:
                        ym_str = str(meta['year_month'])
                        if ym_str.strip():
                            ym_resolved = _parse_ym_str(ym_str)
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass

            # 3. 正则回退解析
            if ym_resolved is None:
                m = _RE_FUTURE.match(sym_str)
                if not m:
                    m = _RE_OPTION.match(sym_str)
                if m:
                    ym_str = m.group(2)
                    try:
                        ym_resolved = _parse_year_month_fallback(ym_str)
                    except (ValueError, IndexError):
                        pass

            # 如果都无法解析，使用 (0, 0) 标记为远月
            if ym_resolved is None:
                ym_resolved = (0, 0)

            symbol_ym_map[sym_str] = ym_resolved

        return symbol_ym_map

    def update(
        self,
        df: pd.DataFrame,
        current_ym: Optional[Tuple[int, int]] = None,
    ) -> DivergenceReversalOutput:
        """核心更新方法: 输入DataFrame, 输出背离反转信号

        Parameters
        ----------
        df          : 包含 minute_data 全部列的 DataFrame
                      必须包含: symbol, minute, close, high, low
                      期权行还需: option_type, strike_price, underlying_price
        current_ym  : 当前年月 (year, month), 若为None则从minute列推断

        Returns
        -------
        DivergenceReversalOutput
        """
        with self._lock:
            return self._update_impl(df, current_ym)

    def _update_impl(
        self,
        df: pd.DataFrame,
        current_ym: Optional[Tuple[int, int]],
    ) -> DivergenceReversalOutput:
        n = len(df)
        if n == 0:
            output = DivergenceReversalOutput()
            self._last_output = output
            return output

        # 推断当前年月
        if current_ym is None:
            if 'minute' in df.columns:
                first_minute = pd.to_datetime(df['minute'].iloc[0])
                current_ym = (first_minute.year, first_minute.month)
            else:
                # [FIX-20260712-AUDIT-P1] 硬编码(2026,6)回退值改为动态获取当前年月
                _now = datetime.now()
                current_ym = (_now.year, _now.month)

        p = self._params

        # 构建 symbol -> (year, month) 映射
        symbol_ym_map = self._resolve_symbol_ym_map(df)

        # ── 1. 期权价值状态五级分类 ──
        moneyness_state = np.full(n, MONEYNESS_ATM, dtype=np.int32)
        if 'underlying_price' in df.columns and 'strike_price' in df.columns \
                and 'option_type' in df.columns:
            opt_type_str = df['option_type'].fillna('').astype(str).str.strip()
            opt_mask = (opt_type_str != '') & (opt_type_str.str.upper() != 'NONE') \
                       & (opt_type_str.str.upper() != 'NAN')
            if opt_mask.any():
                underlying = df['underlying_price'].to_numpy(dtype=np.float64)
                strike = df['strike_price'].to_numpy(dtype=np.float64)
                opt_type = opt_type_str.values
                states = compute_option_moneyness_state(
                    underlying, strike, opt_type, p.atm_threshold, p.moneyness_depth)
                moneyness_state[opt_mask] = states[opt_mask]

        # ── 2. L1: 跨期期货背离 ──
        div_future = _detect_future_cross_term_divergence(
            df, symbol_ym_map, current_ym, p.lookback,
            p.trend_significance, p.div_strength_clip)

        # ── 3. L2: 远月期权权利金集体背离 ──
        div_option_coll = _detect_option_premium_collective_divergence(
            df, symbol_ym_map, current_ym, p.lookback, p.min_ratio,
            p.trend_significance, p.div_strength_clip)

        # ── 4. L3: 当月期权近实值权利金背离 ──
        div_option_itm = _detect_option_near_itm_divergence(
            df, symbol_ym_map, current_ym, p.lookback, p.atm_threshold,
            p.trend_significance, p.div_strength_clip)

        # ── 5. 综合背离反转信号 ──
        reversal_signal = _compute_reversal_signal(
            div_future, div_option_coll, div_option_itm, p)

        # 统计
        n_bearish = int(np.sum(reversal_signal < -0.01))
        n_bullish = int(np.sum(reversal_signal > 0.01))

        output = DivergenceReversalOutput(
            option_moneyness_state=moneyness_state,
            div_future_cross_term=div_future,
            div_option_premium_coll=div_option_coll,
            div_option_near_itm=div_option_itm,
            div_reversal_signal=reversal_signal,
            n_rows=n,
            n_bearish=n_bearish,
            n_bullish=n_bullish,
        )

        self._last_output = output
        self._update_count += 1
        logger.info(
            "DivergenceReversal update #%d: n=%d, bearish=%d, bullish=%d, avg_signal=%.4f, variant=%s",
            self._update_count, n, n_bearish, n_bullish,
            float(np.mean(reversal_signal)) if n > 0 else 0.0,
            p.shadow_variant,
        )
        return output

    # ════════════════════════════════════════════════════════════
    # [FIX-20260712-S7-V3] 信号生成 — 用户本意: 反趋势开仓
    # ════════════════════════════════════════════════════════════

    # kline_period 参数与 klines_raw.period 列的映射
    _KLINE_PERIOD_SQL_MAP = {
        "daily": "D1",
        "hourly": "H1",
        "minute": "M1",
    }

    def get_sql_period(self) -> str:
        """返回当前参数对应的 klines_raw.period 值 (D1/H1/M1)

        [FIX-20260712-S7-V3-P1] 使 strategy_business_layer 能按配置周期过滤K线，
        避免 SQL 始终读取 M1 分钟K线导致 kline_period 参数失效。
        """
        return self._KLINE_PERIOD_SQL_MAP.get(self._params.kline_period, "M1")

    def get_sql_cache_ttl(self) -> float:
        """[S7-V3-P2] 返回SQL查询缓存TTL(秒)

        按kline_period自动调整:
        - daily:  300秒 (日K线一天只更新一次, 缓存5分钟足够)
        - hourly:  60秒 (小时K线每小时更新, 缓存1分钟)
        - minute:  10秒 (分钟K线频繁更新, 缓存10秒)
        若用户显式设置 sql_cache_ttl_sec > 0, 则优先使用用户值
        """
        p = self._params
        if p.sql_cache_ttl_sec > 0:
            return p.sql_cache_ttl_sec
        _DEFAULT_TTL = {"daily": 300.0, "hourly": 60.0, "minute": 10.0}
        return _DEFAULT_TTL.get(p.kline_period, 60.0)

    def _find_most_active_itm_option(
        self,
        df: pd.DataFrame,
        symbol_ym_map: Dict[str, Tuple[int, int]],
        current_ym: Tuple[int, int],
        min_volume: float = 1.0,
        activity_metric: str = "total_volume",
    ) -> Optional[Tuple[str, str, float, float]]:
        """筛选当月最活跃实值期权

        用户本意: "当月一个最活跃实值期权交易价格(权利金)"
        选择标准: 当月合约中，实值(ITM)且成交量最大的期权

        [FIX-20260712-S7-V3-P2] activity_metric 支持三种活跃度度量:
        - "total_volume": 按当日总成交量排序(默认, 反映整体流动性)
        - "last_bar_volume": 按最后一根K线成交量排序(反映当前活跃度)
        - "combined": 总成交量*0.4 + 末根成交量*0.6(兼顾整体与当前)

        Returns
        -------
        Tuple[option_symbol, option_type, strike, volume] | None
        """
        if len(df) == 0:
            return None

        groups = _build_contract_group_map(df['symbol'].values, symbol_ym_map, current_ym)
        cm_idx = groups.get('current_month', np.array([], dtype=np.int64))
        if len(cm_idx) == 0:
            return None

        cm_df = df.iloc[cm_idx].copy()

        # 筛选期权行(有option_type且非空)
        if 'option_type' not in cm_df.columns or 'strike_price' not in cm_df.columns:
            return None
        opt_str = cm_df['option_type'].fillna('').astype(str).str.strip()
        is_opt = (opt_str != '') & (opt_str.str.upper() != 'NONE') & (opt_str.str.upper() != 'NAN')
        cm_opt = cm_df[is_opt].copy()
        if len(cm_opt) == 0:
            return None

        # 需要underlying_price计算moneyness
        if 'underlying_price' not in cm_opt.columns:
            return None

        opt_type_upper = cm_opt['option_type'].str.upper().str.strip()
        underlying = cm_opt['underlying_price'].to_numpy(dtype=np.float64)
        strike = cm_opt['strike_price'].to_numpy(dtype=np.float64)

        is_call = opt_type_upper.str.startswith('C').values
        moneyness = np.where(
            is_call,
            (underlying - strike) / np.where(underlying > 0, underlying, 1.0),
            (strike - underlying) / np.where(underlying > 0, underlying, 1.0),
        )
        moneyness = np.nan_to_num(moneyness, nan=-1.0)
        cm_opt['_moneyness'] = moneyness
        cm_opt['_is_call'] = is_call

        # 筛选实值期权 (moneyness > 0)
        # 用户本意: "最活跃实值期权" — 不限定最接近平值，而是成交量最大的实值期权
        itm_opts = cm_opt[cm_opt['_moneyness'] > 0].copy()
        if len(itm_opts) == 0:
            return None

        # 按成交量筛选(如果volume列存在)
        if 'volume' in itm_opts.columns:
            itm_opts = itm_opts[itm_opts['volume'].fillna(0) >= min_volume]
            if len(itm_opts) == 0:
                return None
            # [S7-V3-P2] 按 activity_metric 选择排序键
            agg_dict = {
                'total_vol': ('volume', 'sum'),
                'last_vol': ('volume', 'last'),
                'close': ('close', 'last'),
                'strike': ('strike_price', 'last'),
                'opt_type': ('option_type', 'last'),
            }
            latest_bar = itm_opts.groupby('symbol').agg(**agg_dict)
            if activity_metric == "last_bar_volume":
                latest_bar = latest_bar.sort_values('last_vol', ascending=False)
                vol_val_key = 'last_vol'
            elif activity_metric == "combined":
                # 归一化后加权: 总量*0.4 + 末根*0.6
                _tv = latest_bar['total_vol'].astype(float)
                _lv = latest_bar['last_vol'].astype(float)
                _tv_norm = _tv / (_tv.max() if _tv.max() > 0 else 1.0)
                _lv_norm = _lv / (_lv.max() if _lv.max() > 0 else 1.0)
                latest_bar['_combined_score'] = _tv_norm * 0.4 + _lv_norm * 0.6
                latest_bar = latest_bar.sort_values('_combined_score', ascending=False)
                vol_val_key = 'total_vol'
            else:
                latest_bar = latest_bar.sort_values('total_vol', ascending=False)
                vol_val_key = 'total_vol'
        else:
            # 无volume列时，取close最大(通常最活跃)的实值期权
            latest_bar = itm_opts.groupby('symbol').agg(
                close=('close', 'last'),
                strike=('strike_price', 'last'),
                opt_type=('option_type', 'last'),
            ).sort_values('close', ascending=False)
            vol_val_key = None

        if len(latest_bar) == 0:
            return None

        top = latest_bar.iloc[0]
        opt_symbol = str(latest_bar.index[0])
        opt_type = str(top.get('opt_type', '')).strip().upper()
        strike_val = float(top.get('strike', 0))
        vol_val = float(top.get(vol_val_key, 0)) if vol_val_key else 0.0

        return (opt_symbol, opt_type, strike_val, vol_val)

    def generate_signal(
        self,
        df: pd.DataFrame,
        current_ym: Optional[Tuple[int, int]] = None,
    ) -> DivergenceSignal:
        """生成背离反转交易信号 — 用户本意的核心实现

        用户本意:
        - 日K线(小时K线)上，期货趋势在继续(出现新高/新低)
        - 当月最活跃实值期权权利金趋势没有继续(不再出现新高/新低)
        - → 反趋势开仓

        判定逻辑:
        1. 期货创出新高 → 期权权利金未创出新高 → 看跌背离 → SELL(反趋势做空)
        2. 期货创出新低 → 期权权利金未创出新低 → 看涨背离 → BUY(反趋势做多)
        3. 不叠加MACD/RSI技术指标

        Parameters
        ----------
        df          : K线DataFrame, 包含 symbol/minute(或bar_time)/close/high/low/volume
                      期权行还需: option_type/strike_price/underlying_price
        current_ym  : 当前年月, None则从df推断

        Returns
        -------
        DivergenceSignal
        """
        import time as _time
        from datetime import datetime as _datetime

        empty_signal = DivergenceSignal(kline_period=self._params.kline_period)

        with self._lock:
            n = len(df)
            if n == 0:
                return empty_signal

            # 推断当前年月
            if current_ym is None:
                _now = _datetime.now()
                if 'minute' in df.columns:
                    try:
                        first_minute = pd.to_datetime(df['minute'].iloc[0])
                        current_ym = (first_minute.year, first_minute.month)
                    except (ValueError, TypeError, KeyError):
                        current_ym = (_now.year, _now.month)
                else:
                    current_ym = (_now.year, _now.month)

            p = self._params
            symbol_ym_map = self._resolve_symbol_ym_map(df)

            # [S7-V3-P2] 按 primary_layer 分发信号生成逻辑
            if p.primary_layer == "L1L2L3":
                return self._generate_signal_composite(
                    df, current_ym, symbol_ym_map, p)

            # 默认 L3 模式: 当月最活跃实值期权背离
            # 找当月最活跃实值期权
            active_itm = self._find_most_active_itm_option(
                df, symbol_ym_map, current_ym, p.min_active_volume,
                activity_metric=p.activity_metric)
            if active_itm is None:
                logger.debug("[S7-V3] 未找到当月活跃实值期权, 无法生成信号")
                return empty_signal

            opt_symbol, opt_type, _strike, _vol = active_itm

            # 获取当月期货合约数据
            groups = _build_contract_group_map(df['symbol'].values, symbol_ym_map, current_ym)
            cm_idx = groups.get('current_month', np.array([], dtype=np.int64))
            if len(cm_idx) == 0:
                return empty_signal

            cm_df = df.iloc[cm_idx]
            opt_str = cm_df['option_type'].fillna('').astype(str).str.strip() if 'option_type' in cm_df.columns else pd.Series([''] * len(cm_df))
            is_future = (opt_str == '') | (opt_str.str.upper() == 'NONE') | (opt_str.str.upper() == 'NAN')
            cm_future = cm_df[is_future]

            if len(cm_future) < p.lookback:
                logger.debug("[S7-V3] 当月期货数据不足(%d < %d), 无法检测背离",
                             len(cm_future), p.lookback)
                return empty_signal

            # 按时间排序聚合期货收盘价
            time_col = 'minute' if 'minute' in cm_future.columns else cm_future.columns[0]
            fut_agg = cm_future.groupby(time_col).agg(
                close=('close', 'last'),
                high=('high', 'max') if 'high' in cm_future.columns else ('close', 'last'),
                low=('low', 'min') if 'low' in cm_future.columns else ('close', 'last'),
            ).sort_index()

            if len(fut_agg) < p.lookback:
                return empty_signal

            # 期货新高/新低检测
            fut_close = fut_agg['close'].values
            fut_new_high = _rolling_new_extremum(fut_close, p.lookback, True)
            fut_new_low = _rolling_new_extremum(fut_close, p.lookback, False)
            fut_trend = _rolling_trend_direction(fut_close, p.lookback, p.trend_significance)

            # 获取最活跃实值期权的权利金序列
            opt_df = df[df['symbol'] == opt_symbol].copy()
            if len(opt_df) < p.lookback:
                logger.debug("[S7-V3] 期权'%s'数据不足(%d < %d)",
                             opt_symbol, len(opt_df), p.lookback)
                return empty_signal

            opt_agg = opt_df.groupby(time_col if time_col in opt_df.columns else opt_df.columns[0]).agg(
                close=('close', 'last'),
            ).sort_index()

            if len(opt_agg) < p.lookback:
                return empty_signal

            opt_close = opt_agg['close'].values
            opt_new_high = _rolling_new_extremum(opt_close, p.lookback, True)
            opt_new_low = _rolling_new_extremum(opt_close, p.lookback, False)

            # 取最近一根K线的背离状态
            last_idx = len(fut_agg) - 1
            # 对齐期权和期货的时间索引
            common_idx = fut_agg.index.intersection(opt_agg.index)
            if len(common_idx) < p.lookback:
                return empty_signal

            last_common = common_idx[-1]
            fut_pos = fut_agg.index.get_loc(last_common)
            opt_pos = opt_agg.index.get_loc(last_common)

            fut_nh = bool(fut_new_high[fut_pos]) if fut_pos < len(fut_new_high) else False
            fut_nl = bool(fut_new_low[fut_pos]) if fut_pos < len(fut_new_low) else False
            opt_nh = bool(opt_new_high[opt_pos]) if opt_pos < len(opt_new_high) else False
            opt_nl = bool(opt_new_low[opt_pos]) if opt_pos < len(opt_new_low) else False
            trend_dir = float(fut_trend[fut_pos]) if fut_pos < len(fut_trend) else 0.0

            fut_price = float(fut_close[fut_pos])
            opt_premium = float(opt_close[opt_pos])

            # 核心背离判定 (用户本意):
            # 期货趋势继续(新高/新低) + 期权权利金趋势未继续(不创新高/新低) → 反趋势开仓
            signal = DivergenceSignal(
                futures_symbol=str(cm_future['symbol'].iloc[-1] if 'symbol' in cm_future.columns else ''),
                option_symbol=opt_symbol,
                futures_price=fut_price,
                option_premium=opt_premium,
                futures_new_high=fut_nh,
                futures_new_low=fut_nl,
                option_new_high=opt_nh,
                option_new_low=opt_nl,
                kline_period=p.kline_period,
                timestamp=_time.time(),
                bar_time=str(last_common),
            )

            # 看跌背离: 期货创新高 + 期权权利金未创新高 → 反趋势做空(SELL)
            if fut_nh and not opt_nh and trend_dir > 0:
                signal.direction = "SELL"
                signal.divergence_type = "bearish"
                signal.strength = min(1.0, abs(trend_dir) * 10.0 + 0.3)
                logger.info(
                    "[S7-V3] 看跌背离信号: 期货新高但期权'%s'权利金未新高 → SELL "
                    "(fut_price=%.2f opt_premium=%.2f strength=%.2f period=%s)",
                    opt_symbol, fut_price, opt_premium, signal.strength, p.kline_period,
                )

            # 看涨背离: 期货创新低 + 期权权利金未创新低 → 反趋势做多(BUY)
            elif fut_nl and not opt_nl and trend_dir < 0:
                signal.direction = "BUY"
                signal.divergence_type = "bullish"
                signal.strength = min(1.0, abs(trend_dir) * 10.0 + 0.3)
                logger.info(
                    "[S7-V3] 看涨背离信号: 期货新低但期权'%s'权利金未新低 → BUY "
                    "(fut_price=%.2f opt_premium=%.2f strength=%.2f period=%s)",
                    opt_symbol, fut_price, opt_premium, signal.strength, p.kline_period,
                )
            else:
                logger.debug(
                    "[S7-V3] 无背离: fut_nh=%s fut_nl=%s opt_nh=%s opt_nl=%s trend=%.4f",
                    fut_nh, fut_nl, opt_nh, opt_nl, trend_dir,
                )

            return signal

    def _generate_signal_composite(
        self,
        df: pd.DataFrame,
        current_ym: Tuple[int, int],
        symbol_ym_map: Dict[str, Tuple[int, int]],
        p: DivergenceReversalParams,
    ) -> DivergenceSignal:
        """[S7-V3-P2] L1L2L3 三层综合背离信号生成

        当 primary_layer="L1L2L3" 时调用此方法:
        1. 运行 L1(跨期期货) + L2(远月集体) + L3(近实值) 三层背离检测
        2. 用 _compute_reversal_signal 加权合成综合信号
        3. 取最后一根K线的综合信号值判定方向:
           - composite < -signal_threshold → SELL(看跌背离, 反趋势做空)
           - composite > +signal_threshold → BUY(看涨背离, 反趋势做多)
        4. 交易标的仍选当月最活跃实值期权(与L3模式一致, 保证可执行性)

        用户本意: L3为主信号, L1L2L3模式为可选增强(三层共振时信号更强)
        """
        import time as _time

        empty_signal = DivergenceSignal(kline_period=p.kline_period)

        n = len(df)
        if n == 0:
            return empty_signal

        # ── 1. 运行三层背离检测 ──
        div_future = _detect_future_cross_term_divergence(
            df, symbol_ym_map, current_ym, p.lookback,
            p.trend_significance, p.div_strength_clip)
        div_option_coll = _detect_option_premium_collective_divergence(
            df, symbol_ym_map, current_ym, p.lookback, p.min_ratio,
            p.trend_significance, p.div_strength_clip)
        div_option_itm = _detect_option_near_itm_divergence(
            df, symbol_ym_map, current_ym, p.lookback, p.atm_threshold,
            p.trend_significance, p.div_strength_clip)

        # ── 2. 加权合成综合信号 ──
        composite = _compute_reversal_signal(
            div_future, div_option_coll, div_option_itm, p)

        if len(composite) == 0:
            return empty_signal

        # ── 3. 取最后一根K线的综合信号值 ──
        last_val = float(composite[-1])
        if abs(last_val) < p.signal_threshold:
            logger.debug(
                "[S7-V3-L1L2L3] 综合信号=%.4f 低于阈值%.2f, 无背离",
                last_val, p.signal_threshold)
            return empty_signal

        # ── 4. 选当月最活跃实值期权作为交易标的 ──
        active_itm = self._find_most_active_itm_option(
            df, symbol_ym_map, current_ym, p.min_active_volume,
            activity_metric=p.activity_metric)
        if active_itm is None:
            logger.debug("[S7-V3-L1L2L3] 未找到当月活跃实值期权")
            return empty_signal

        opt_symbol, opt_type, _strike, _vol = active_itm

        # 获取当月期货和期权的最新价格
        groups = _build_contract_group_map(df['symbol'].values, symbol_ym_map, current_ym)
        cm_idx = groups.get('current_month', np.array([], dtype=np.int64))
        if len(cm_idx) == 0:
            return empty_signal

        cm_df = df.iloc[cm_idx]
        opt_str = cm_df['option_type'].fillna('').astype(str).str.strip() if 'option_type' in cm_df.columns else pd.Series([''] * len(cm_df))
        is_future = (opt_str == '') | (opt_str.str.upper() == 'NONE') | (opt_str.str.upper() == 'NAN')
        cm_future = cm_df[is_future]

        fut_price = float(cm_future['close'].iloc[-1]) if len(cm_future) > 0 and 'close' in cm_future.columns else 0.0
        fut_symbol = str(cm_future['symbol'].iloc[-1]) if len(cm_future) > 0 and 'symbol' in cm_future.columns else ''

        opt_df = df[df['symbol'] == opt_symbol]
        opt_premium = float(opt_df['close'].iloc[-1]) if len(opt_df) > 0 and 'close' in opt_df.columns else 0.0

        # 时间标签
        time_col = 'minute' if 'minute' in df.columns else None
        bar_time = str(df[time_col].iloc[-1]) if time_col else ""

        # ── 5. 判定方向 ──
        signal = DivergenceSignal(
            futures_symbol=fut_symbol,
            option_symbol=opt_symbol,
            futures_price=fut_price,
            option_premium=opt_premium,
            kline_period=p.kline_period,
            timestamp=_time.time(),
            bar_time=bar_time,
        )

        if last_val < -p.signal_threshold:
            # 看跌背离 → 反趋势做空
            signal.direction = "SELL"
            signal.divergence_type = "bearish"
            signal.strength = min(1.0, abs(last_val))
            logger.info(
                "[S7-V3-L1L2L3] 三层综合看跌背离: composite=%.4f → SELL "
                "(fut=%s@%.2f opt=%s@%.2f L1=%.3f L2=%.3f L3=%.3f)",
                last_val, fut_symbol, fut_price, opt_symbol, opt_premium,
                float(div_future[-1]) if len(div_future) > 0 else 0.0,
                float(div_option_coll[-1]) if len(div_option_coll) > 0 else 0.0,
                float(div_option_itm[-1]) if len(div_option_itm) > 0 else 0.0,
            )
        elif last_val > p.signal_threshold:
            # 看涨背离 → 反趋势做多
            signal.direction = "BUY"
            signal.divergence_type = "bullish"
            signal.strength = min(1.0, abs(last_val))
            logger.info(
                "[S7-V3-L1L2L3] 三层综合看涨背离: composite=%.4f → BUY "
                "(fut=%s@%.2f opt=%s@%.2f L1=%.3f L2=%.3f L3=%.3f)",
                last_val, fut_symbol, fut_price, opt_symbol, opt_premium,
                float(div_future[-1]) if len(div_future) > 0 else 0.0,
                float(div_option_coll[-1]) if len(div_option_coll) > 0 else 0.0,
                float(div_option_itm[-1]) if len(div_option_itm) > 0 else 0.0,
            )

        return signal

    def to_dict(self) -> Dict:
        return {
            "params": self._params.to_dict(),
            "update_count": self._update_count,
            "last_output": self._last_output.to_dict() if self._last_output else None,
        }


# ══════════════════════════════════════════════════════════════════
# 9. 单例工厂
# ══════════════════════════════════════════════════════════════════

_module_instance: Optional[DivergenceReversalModule] = None
_module_lock = threading.Lock()


def get_divergence_reversal_module(
    params: Optional[DivergenceReversalParams] = None,
    reset: bool = False,
) -> DivergenceReversalModule:
    """获取背离反转模块全局单例"""
    global _module_instance
    if _module_instance is None or reset:
        with _module_lock:
            if _module_instance is None or reset:
                _module_instance = DivergenceReversalModule(params)
                logger.info("DivergenceReversalModule singleton created")
    return _module_instance
