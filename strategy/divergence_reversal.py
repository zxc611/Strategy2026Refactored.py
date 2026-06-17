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
  from ali2026v3_trading.strategy.divergence_reversal import (
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

from ali2026v3_trading.infra.logging_utils import get_logger

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
    """背离反转模块参数 — 全部从 param pool 读取"""
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
            from ali2026v3_trading.config.config_service import get_param
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
            }
            for attr, key in _KEY_MAP.items():
                val = get_param(f"parameter_attributes.{key}.default")
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

    仅在 InstrumentCacheService 不可用时使用。
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

    Parameters
    ----------
    year       : 合约年份
    month      : 合约月份
    current_ym : 当前年月 (year, month)

    Returns
    -------
    str : 'current_month' | 'next_month' | 'current_quarter' |
          'next_quarter_1' | 'next_quarter_2' | 'next_quarter_3' | 'far'
    """
    cur_year, cur_month = current_ym
    contract_num = year * 12 + month
    current_num = cur_year * 12 + cur_month

    if contract_num < current_num:
        return 'far'

    if contract_num == current_num:
        return 'current_month'

    if contract_num == current_num + 1:
        return 'next_month'

    def _next_quarter_month(y: int, mo: int) -> Tuple[int, int]:
        for qm in sorted(QUARTERLY_MONTHS):
            if mo <= qm:
                return y, qm
        return y + 1, 3

    cq_year, cq_month = _next_quarter_month(cur_year, cur_month)
    current_quarter_num = cq_year * 12 + cq_month

    if contract_num == current_quarter_num:
        return 'current_quarter'

    nq_year, nq_month = cq_year, cq_month
    for i in range(1, 4):
        if nq_month == 12:
            nq_year += 1
            nq_month = 3
        else:
            nq_month += 3
        nq_num = nq_year * 12 + nq_month
        if contract_num == nq_num:
            return f'next_quarter_{i}'

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
                from ali2026v3_trading.config._params_instrument_cache import InstrumentCacheService
                self._instrument_cache = InstrumentCacheService()
            except (ImportError, AttributeError):
                self._instrument_cache = False  # 标记不可用
        return self._instrument_cache if self._instrument_cache is not False else None

    def _resolve_symbol_ym_map(self, df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """构建 symbol -> (year, month) 映射

        优先级:
        1. df 中的 year_month 列（由 preprocess pipeline 添加）
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
                current_ym = (2026, 6)

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
