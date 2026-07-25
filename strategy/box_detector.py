"""
box_detector.py - 箱体检测与极值判断模块

V7次系统2核心组件：箱底/箱顶识别 + 极值子状态

设计原理：
  箱形震荡本质：价格在箱底和箱顶之间反复，方向不延续。
  共振策略在箱体内不追趋势，而是在"转折点"反向操作：
    箱底极值：期货跌到箱底 + 全链看跌共振 → "跌透了" → 反向做多
    箱顶极值：期货涨到箱顶 + 全链看涨共振 → "涨透了" → 反向做空

箱体检测方法：
  1. 价格振幅法：近期高低价差收窄 + ADX低 → 箱体识别
  2. 行权价聚类法：虚值期权行权价在箱体边界处聚集
  3. VWAP带法：VWAP上下轨收窄 → 箱体

极值判断：
  - 价格触及箱底/箱顶
  - 全链期权呈现共振（方向一致性极高）
  - IV处于近期高位（>50分位数）
  - 订单流出现衰竭迹象

调用现有方法：
  - WidthStrengthCache.get_width_strength_summary() → 五态分布
  - TTypeService.compute_decision_score() → 综合决策
  - MicrostructureAnalyzer.get_composite_assessment() → 订单流综合
  - IVCalculator.implied_volatility() → IV计算

新增方法：
  - detect_box() → 箱体识别
  - classify_extreme_state() → 极值子状态判断
  - check_iv_filter() → IV高位过滤
  - check_order_flow_exhaustion() → 订单流衰竭确认
"""
from __future__ import annotations

import logging
import math
import threading
import time
from bisect import bisect_left, insort
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from infra._helpers import get_logger  # R9-5
from infra.shared_utils import CHINA_TZ as _CHINA_TZ

logger = get_logger(__name__)  # R9-5


class BoxType(Enum):
    """箱体类型枚举 — S3/S4策略箱体标准化
    
    日内交易(dte≤5): 至少3根日K线相近高低点结成的小箱形
    隔夜交易(dte>5): 至少3根周K线相近高低点结成的中箱形
    
    注: tick级箱体已废弃(风险过大)，K线箱体为唯一箱体来源
    """
    INTRADAY_SMALL = auto()    # 日内小箱形（3+ 日K线相近高低点）
    OVERNIGHT_MEDIUM = auto()  # 隔夜中箱形（3+ 周K线相近高低点）


@dataclass(slots=True)
class KLineBoxProfile:
    """K线级别箱体轮廓 — 日K/周K结构确认（S3/S4唯一箱体来源）"""
    box_type: BoxType = BoxType.INTRADAY_SMALL  # 默认日内小箱形
    upper: float = 0.0       # 箱体上沿（K线高点的聚类中心）
    lower: float = 0.0       # 箱体下沿（K线低点的聚类中心）
    width_pct: float = 0.0   # 箱体宽度百分比
    bar_count: int = 0       # 构成箱体的K线数
    is_valid: bool = False   # 是否满足最低K线数+宽度要求
    confidence: float = 0.0  # 箱体置信度

    @property
    def is_confirmed(self) -> bool:
        """K线箱体确认 = is_valid"""
        return self.is_valid

    def to_dict(self) -> Dict[str, Any]:
        return {k: (v.value if isinstance(v, BoxType) else v) for k, v in asdict(self).items()}


@dataclass(slots=True)
class BoxProfile:
    """箱体轮廓"""
    box_id: str
    timestamp: str
    is_box: bool = False
    box_type: str = ''
    upper: float = 0.0
    lower: float = 0.0
    median: float = 0.0
    width_pct: float = 0.0
    confidence: float = 0.0
    confidence_source: str = 's3_box'  # P1-29修复: 区分box/extreme来源
    duration_bars: int = 0
    bounce_count: int = 0
    adx: float = 0.0
    kline_box_confirmed: bool = False    # K线箱体是否确认(前置条件)
    kline_box_type: str = ''             # K线箱体类型(INTRADAY_SMALL/OVERNIGHT_MEDIUM)

    @property
    def is_valid(self) -> bool:
        return self.is_box and self.upper > self.lower > 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ExtremeState:
    """极值子状态"""
    timestamp: str
    extreme_type: str = ''
    is_bottom_extreme: bool = False
    is_top_extreme: bool = False
    price_position_pct: float = 0.0
    resonance_direction: str = ''
    resonance_strength: float = 0.0
    iv_percentile: float = 0.0
    iv_filter_passed: bool = False
    flow_exhaustion_detected: bool = False
    confidence: float = 0.0
    confidence_source: str = 'extreme'  # P1-29修复: 区分box/extreme来源
    tradeable: bool = False
    kline_box_confirmed: bool = False      # K线箱体是否确认(前置条件)
    kline_box_type: str = ''               # K线箱体类型(INTRADAY_SMALL/OVERNIGHT_MEDIUM)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BoxStrategyParams:
    """箱体策略参数（比趋势策略更苛刻）"""
    max_hold_minutes: int = 30
    take_profit_ratio: float = 0.4
    stop_loss_ratio: float = 0.3
    max_risk_ratio: float = 0.05
    iv_percentile_min: float = 50.0
    signal_cooldown_sec: float = 60.0  # R27-P0-CD-06修复: 120.0→60.0，与config_params全局默认值对齐
    position_scale: float = 0.3
    lots_min: int = 1
    option_buy_lots_max: int = 10
    min_extreme_confidence: float = 0.6
    min_bounce_count: int = 2
    box_width_max_pct: float = 5.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BoxDetector:
    """箱体检测与极值判断引擎

    职责：
    1. 基于价格序列识别箱体（箱底/箱顶）
    2. 在箱体边界判断极值子状态
    3. IV过滤：仅在IV高位时允许箱体买方操作
    4. 订单流衰竭确认：大单停手、小单反向
    5. 生成箱体交易信号
    """

    ADX_DEFAULT_VALUE = 50.0
    ADX_MULTIPLIER = 100.0
    BOX_HISTORY_MAXLEN = 100
    FLOW_IMBALANCE_THRESHOLD = 0.2
    CVD_SLOPE_THRESHOLD = 0.01
    WIDTH_SCORE_WEIGHT = 0.25
    ADX_SCORE_WEIGHT = 0.25
    BOUNCE_SCORE_WEIGHT = 0.30
    PLR_SCORE_WEIGHT = 0.20
    PRICE_SCORE_WEIGHT = 0.25
    RESONANCE_SCORE_WEIGHT = 0.30
    IV_SCORE_WEIGHT = 0.25
    FLOW_SCORE_WEIGHT = 0.20
    BOTTOM_THRESHOLD_RATIO = 0.15
    TOP_THRESHOLD_RATIO = 0.15

    def __init__(
        self,
        params: Optional[BoxStrategyParams] = None,
        lookback_bars: int = 60,
        min_box_bars: int = 20,
        adx_period: int = 14,
        adx_threshold: float = 25.0,
        bounce_tolerance_pct: float = 0.1,
        iv_history_maxlen: int = 1000,
        box_gain_ratio: float = 0.5,
        plr_normalization_base: float = 3.0,
    ):
        self._lock = threading.RLock()
        self._params = params or BoxStrategyParams()
        self._lookback_bars = lookback_bars
        self._min_box_bars = min_box_bars
        self._adx_period = adx_period
        self._adx_threshold = adx_threshold
        self._bounce_tolerance_pct = bounce_tolerance_pct
        self._box_gain_ratio = box_gain_ratio
        self._plr_normalization_base = plr_normalization_base

        self._price_highs: deque = deque(maxlen=lookback_bars)
        self._price_lows: deque = deque(maxlen=lookback_bars)
        self._price_closes: deque = deque(maxlen=lookback_bars)
        self._volumes: deque = deque(maxlen=lookback_bars)
        self._timestamps: deque = deque(maxlen=lookback_bars)

        self._current_box: Optional[BoxProfile] = None
        self._box_history: deque = deque(maxlen=self.BOX_HISTORY_MAXLEN)
        self._extreme_state: Optional[ExtremeState] = None

        self._iv_history: deque = deque(maxlen=iv_history_maxlen)
        self._iv_sorted: List[float] = []
        self._bounce_at_upper: int = 0
        self._bounce_at_lower: int = 0

        self._stats = {
            'bars_processed': 0,
            'boxes_detected': 0,
            'bottom_extremes': 0,
            'top_extremes': 0,
            'iv_filtered': 0,
            'flow_exhaustion_confirmed': 0,
            'tradeable_signals': 0,
            'false_breakout_filtered': 0,  # [FIX-20260712-S3] 假突破过滤统计
        }

        # [FIX-20260712-S3] 假突破过滤模块 — 上策H-Rev: 假突破过滤
        # 记录最近极值信号的价格和时间，用于检测假突破
        self._breakout_tracker: deque = deque(maxlen=20)  # [(timestamp, price, extreme_type), ...]
        self._false_breakout_lookback_sec = 120.0  # 回看窗口：2分钟内的突破
        self._false_breakout_retrace_ratio = 0.50  # 价格回落超过50%视为假突破

        self._box_id_counter: int = 0

        # FIX-56: per-instrument信号冷却追踪，防止同一合约每tick重复触发信号
        # 根因: classify_extreme_state的fallback箱体强制is_bottom=True/is_top=True，
        #       且signal_cooldown_sec参数(60.0)从未被使用，导致每tick都返回tradeable=True
        # 修复: 记录每个instrument最后一次信号时间，在signal_cooldown_sec内不再触发
        self._last_signal_time: Dict[str, float] = {}

        # K线箱体实例变量 — S3/S4策略箱体标准化
        # 日内交易(dte≤5): 至少3根日K线相近高低点结成的小箱形(INTRADAY_SMALL)
        # 隔夜交易(dte>5): 至少3根周K线相近高低点结成的中箱形(OVERNIGHT_MEDIUM)
        self._kline_box_daily: Optional[KLineBoxProfile] = None    # 日K小箱形缓存
        self._kline_box_weekly: Optional[KLineBoxProfile] = None  # 周K中箱形缓存
        self._kline_box_instrument_id: str = ''       # 当前K线箱体对应的合约ID
        self._kline_box_last_update: float = 0.0      # 上次K线箱体更新时间戳
        self._kline_box_cache_ttl_sec: float = 300.0  # K线箱体缓存TTL(5分钟)
        self._kline_min_bars_daily: int = 3           # 日K箱体最低K线数
        self._kline_min_bars_weekly: int = 3          # 周K箱体最低K线数
        self._kline_box_width_max_pct: float = 5.0    # K线箱体最大宽度百分比

        logger.info("[BoxDetector] 初始化完成, lookback=%d, min_bars=%d",
                     lookback_bars, min_box_bars)

    @property
    def params(self) -> BoxStrategyParams:
        return self._params

    def update_bar(
        self,
        high: float,
        low: float,
        close: float,
        volume: float = 0.0,
        timestamp: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._price_highs.append(high)
            self._price_lows.append(low)
            self._price_closes.append(close)
            self._volumes.append(volume)
            self._timestamps.append(timestamp or datetime.now(_CHINA_TZ).isoformat())
            self._stats['bars_processed'] += 1

    def update_iv(self, iv: float) -> None:
        with self._lock:
            if iv is None or iv <= 0:
                return
            if len(self._iv_history) == self._iv_history.maxlen:
                evicted = self._iv_history[0]
                self._iv_history.append(iv)
                try:
                    self._iv_sorted.remove(evicted)
                except ValueError:
                    pass
                insort(self._iv_sorted, iv)
            else:
                self._iv_history.append(iv)
                insort(self._iv_sorted, iv)

    @staticmethod
    def _compute_adx_simplified(highs, lows, closes, period: int = 14) -> float:
        if len(closes) < period + 1:
            return BoxDetector.ADX_DEFAULT_VALUE
        plus_dm_list = []
        minus_dm_list = []
        tr_list = []
        for i in range(1, min(len(closes), period + 1)):
            idx = len(closes) - 1 - i
            if idx < 0:
                break
            h_curr = highs[-(i)]
            l_curr = lows[-(i)]
            h_prev = highs[-(i + 1)]
            l_prev = lows[-(i + 1)]
            c_prev = closes[-(i + 1)]

            up_move = h_curr - h_prev
            down_move = l_prev - l_curr

            plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
            minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0

            tr1 = h_curr - l_curr
            tr2 = abs(h_curr - c_prev)
            tr3 = abs(l_curr - c_prev)
            tr = max(tr1, tr2, tr3)

            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)
            tr_list.append(tr)

        if not tr_list or sum(tr_list) < 1e-10:
            return BoxDetector.ADX_DEFAULT_VALUE

        avg_plus_dm = sum(plus_dm_list) / len(plus_dm_list)
        avg_minus_dm = sum(minus_dm_list) / len(minus_dm_list)
        avg_tr = sum(tr_list) / len(tr_list)

        if avg_tr < 1e-10:
            return BoxDetector.ADX_DEFAULT_VALUE

        plus_di = BoxDetector.ADX_MULTIPLIER * avg_plus_dm / avg_tr
        minus_di = BoxDetector.ADX_MULTIPLIER * avg_minus_dm / avg_tr

        di_sum = plus_di + minus_di
        if di_sum < 1e-10:
            return 0.0

        dx = BoxDetector.ADX_MULTIPLIER * abs(plus_di - minus_di) / di_sum
        return dx

    @staticmethod
    def _find_support_resistance(
        lows,
        highs,
        n_clusters: int = 2,
        tolerance_pct: float = 0.3,
    ) -> Tuple[List[float], List[float]]:
        if len(lows) < 5 or len(highs) < 5:
            return [], []

        low_list = sorted(lows)
        high_list = sorted(highs)

        def cluster(prices: List[float]) -> List[Tuple[float, int]]:
            if not prices:
                return []
            clusters = []
            current_center = prices[0]
            current_count = 1
            current_sum = prices[0]
            for p in prices[1:]:
                if abs(p - current_center) / max(abs(current_center), 1e-10) < tolerance_pct / 100.0:
                    current_count += 1
                    current_sum += p
                    current_center = current_sum / current_count
                else:
                    clusters.append((current_center, current_count))
                    current_center = p
                    current_count = 1
                    current_sum = p
            clusters.append((current_center, current_count))
            return clusters

        low_clusters = sorted(cluster(low_list), key=lambda x: x[1], reverse=True)
        high_clusters = sorted(cluster(high_list), key=lambda x: x[1], reverse=True)

        supports = [c[0] for c in low_clusters[:n_clusters]]
        resistances = [c[0] for c in high_clusters[:n_clusters]]

        return supports, resistances

    def detect_box(self) -> BoxProfile:
        with self._lock:
            now_str = datetime.now(_CHINA_TZ).isoformat()
            self._box_id_counter += 1
            box_id = f"BOX-{self._box_id_counter:06d}"

            if len(self._price_closes) < self._min_box_bars:
                return BoxProfile(box_id=box_id, timestamp=now_str)

            closes = list(self._price_closes)
            highs = list(self._price_highs)
            lows = list(self._price_lows)

            recent_high = max(highs[-self._min_box_bars:])
            recent_low = min(lows[-self._min_box_bars:])
            recent_close = closes[-1]

            if recent_close < 1e-10:
                return BoxProfile(box_id=box_id, timestamp=now_str)

            width_pct = (recent_high - recent_low) / recent_close * 100.0

            adx = self._compute_adx_simplified(highs, lows, closes, period=self._adx_period)

            is_box = width_pct <= self._params.box_width_max_pct and adx < self._adx_threshold

            supports, resistances = self._find_support_resistance(lows, highs)

            if supports and resistances:
                box_lower = supports[0]
                box_upper = resistances[0]
            else:
                box_lower = recent_low
                box_upper = recent_high

            if box_upper <= box_lower:
                box_upper = recent_high
                box_lower = recent_low

            median = (box_upper + box_lower) / 2.0
            confidence = 0.0
            bounce_count = 0

            if is_box:
                tolerance = (box_upper - box_lower) * self._bounce_tolerance_pct
                for low in lows[-self._min_box_bars:]:
                    if abs(low - box_lower) <= tolerance:
                        bounce_count += 1
                for high in highs[-self._min_box_bars:]:
                    if abs(high - box_upper) <= tolerance:
                        bounce_count += 1

                width_score = max(0.0, 1.0 - width_pct / self._params.box_width_max_pct)
                adx_score = max(0.0, 1.0 - adx / self._adx_threshold)
                bounce_score = min(1.0, bounce_count / (self._params.min_bounce_count * 2))

                plr_score = 0.0
                box_height = box_upper - box_lower
                if box_height > 1e-10:
                    mid_price = (box_upper + box_lower) / 2.0
                    potential_loss = abs(mid_price - box_lower) if abs(mid_price - box_lower) > 1e-10 else 1e-10
                    potential_plr = BoxDetector.estimate_plr(box_height, potential_loss, self._box_gain_ratio)
                    plr_score = min(1.0, potential_plr / self._plr_normalization_base)

                confidence = self.WIDTH_SCORE_WEIGHT * width_score + self.ADX_SCORE_WEIGHT * adx_score + self.BOUNCE_SCORE_WEIGHT * bounce_score + self.PLR_SCORE_WEIGHT * plr_score

                is_box = bounce_count >= self._params.min_bounce_count

            profile = BoxProfile(
                box_id=box_id,
                timestamp=now_str,
                is_box=is_box,
                box_type='range' if is_box else '',
                upper=box_upper,
                lower=box_lower,
                median=median,
                width_pct=width_pct,
                confidence=confidence,
                duration_bars=len(closes),
                bounce_count=bounce_count,
                adx=adx,
            )

            # K线箱体字段注入 — 使用缓存的K线箱体状态
            if self._kline_box_daily is not None and self._kline_box_daily.is_valid:
                profile.kline_box_confirmed = True
                profile.kline_box_type = 'INTRADAY_SMALL'
            elif self._kline_box_weekly is not None and self._kline_box_weekly.is_valid:
                profile.kline_box_confirmed = True
                profile.kline_box_type = 'OVERNIGHT_MEDIUM'

            if is_box:
                # K线箱体时代: detect_box()不再更新_current_box
                # _current_box由_update_current_box_from_kline()独占更新
                # 保留box_history记录用于历史分析
                self._box_history.append(profile)
                self._stats['boxes_detected'] += 1

            return profile

    def get_current_box(self) -> Optional[BoxProfile]:
        with self._lock:
            return self._current_box

    def classify_extreme_state(
        self,
        current_price: float,
        resonance_direction: str = '',
        resonance_strength: float = 0.0,
        current_iv: float = 0.0,
        flow_imbalance: float = 0.0,
        cvd_slope: float = 0.0,
        instrument_id: str = '',
    ) -> ExtremeState:
        with self._lock:
            now_str = datetime.now(_CHINA_TZ).isoformat()

            # FIX-56: per-instrument信号冷却检查
            # 根因: fallback箱体每tick都返回tradeable=True，5分钟内生成29万次信号
            # 修复: 使用signal_cooldown_sec参数(60秒)，同一instrument在冷却期内不重复触发
            if instrument_id and self._params.signal_cooldown_sec > 0:
                import time as _time_mod
                _now_ts = _time_mod.time()
                _last_ts = self._last_signal_time.get(instrument_id, 0.0)
                _elapsed = _now_ts - _last_ts
                if _elapsed < self._params.signal_cooldown_sec:
                    # FIX-S3-2: cooldown 过滤诊断日志
                    # FIX-S3-COOLDOWN-20260722: 降级为DEBUG(原WARNING)，462次cooldown过滤产生大量噪音
                    # 根因: 策略重启后_last_signal_time重置为0，所有instrument的elapsed=0.0s
                    #   触发cooldown过滤WARNING，每分钟20+条，合计462条
                    # 修复: 正常cooldown过滤降级为DEBUG，仅首次(重启后)保留INFO
                    _log_level = logging.DEBUG
                    if _last_ts == 0.0:
                        _log_level = logging.INFO  # 重启后首次cooldown保留INFO级别
                    logger.log(_log_level,
                        "[S3-BOX] cooldown过滤: inst=%s elapsed=%.1fs < cooldown=%.1fs",
                        instrument_id, _elapsed, self._params.signal_cooldown_sec
                    )
                    # 冷却期内，返回不可交易状态
                    # FIX-56b: 同步更新_extreme_state，防止config_layer读到陈旧的tradeable=True
                    _blocked_state = ExtremeState(timestamp=now_str)
                    self._extreme_state = _blocked_state
                    return _blocked_state

            if self._current_box is None or not self._current_box.is_valid:
                # V4-FIX-O8: 无有效箱体=不开仓(fail-closed)
                # 原则: 数据不可用=证据不足=不开仓，而非数据不可用=虚构箱体放行
                # 清理旧fallback箱体对象，防止其他路径读取绕过阻断
                self._current_box = None
                # FIX-20260723-O8-THROTTLE: 日志限频(60s冷却)，原每tick打WARNING→1472次/下午
                _o8_now = time.time()
                _o8_last = getattr(self.__class__, '_o8_warn_ts', None)
                if not isinstance(_o8_last, dict):
                    _o8_last = {}
                    setattr(self.__class__, '_o8_warn_ts', _o8_last)
                _o8_inst = getattr(self, '_current_instrument_id', '')
                if _o8_now - _o8_last.get(_o8_inst, 0.0) >= 60:
                    logging.info("[S3-BOX] V4-FIX-O8: 无有效箱体, 返回空状态 (fail-closed) inst=%s price=%.2f",
                                    _o8_inst, current_price)
                    _o8_last[_o8_inst] = _o8_now
                return ExtremeState(timestamp=now_str)

            box = self._current_box
            box_range = box.upper - box.lower

            if box_range < 1e-10:
                return ExtremeState(timestamp=now_str)

            price_position_pct = (current_price - box.lower) / box_range * 100.0

            is_bottom = current_price <= box.lower + box_range * self.BOTTOM_THRESHOLD_RATIO
            is_top = current_price >= box.upper - box_range * self.TOP_THRESHOLD_RATIO

            # V4-FIX-O8-RESIDUAL: 删除 fallback 箱体特殊处理逻辑。
            # 原则: 数据不可用=不开仓，fallback 箱体属于"虚构箱体放行"的回退反模式。
            # 自 V4-FIX-O8 起，无有效箱体时直接返回空状态，_current_box 被置 None，
            # 因此 box_id 为 fallback 的分支永远不会到达此处；保留该分支即为死代码与 bypass 隐患。

            is_bottom_extreme = False
            is_top_extreme = False
            extreme_type = ''

            if is_bottom and resonance_direction in ('fall', 'correct_fall', 'wrong_fall'):
                is_bottom_extreme = True
                extreme_type = 'box_bottom_extreme'
                self._stats['bottom_extremes'] += 1
            elif is_top and resonance_direction in ('rise', 'correct_rise', 'wrong_rise'):
                is_top_extreme = True
                extreme_type = 'box_top_extreme'
                self._stats['top_extremes'] += 1

            iv_percentile = self._compute_iv_percentile(current_iv)
            # FIX-20260714-S3: IV不可用时(current_iv=0)不阻塞信号，仅当IV有值且低于阈值时才过滤
            # V4-FIX-O9: IV=0(数据不可用)时iv_filter_passed=False(fail-closed)
            # 原则: 数据不可用=不满足条件, 不再放行iv_filter_passed=True
            iv_filter_passed = iv_percentile >= self._params.iv_percentile_min if current_iv > 0 else False
            if not iv_filter_passed and current_iv > 0:
                self._stats['iv_filtered'] += 1
                # FIX-S3-1: IV filter 过滤诊断日志（WARNING级别写入signals.jsonl）
                # 根因: IV百分位 < iv_percentile_min(50.0) 时信号被过滤，但无日志记录导致无法排查
                logger.warning(
                    "[S3-BOX] IV filter过滤: inst=%s iv_percentile=%.1f < min=%.1f (current_iv=%.4f)",
                    instrument_id, iv_percentile, self._params.iv_percentile_min, current_iv
                )

            imbalance_exhausted = abs(flow_imbalance) < self.FLOW_IMBALANCE_THRESHOLD
            cvd_stalling = abs(cvd_slope) < self.CVD_SLOPE_THRESHOLD
            flow_exhaustion = imbalance_exhausted or cvd_stalling
            if flow_exhaustion:
                self._stats['flow_exhaustion_confirmed'] += 1

            confidence = 0.0
            if extreme_type:
                price_score = 1.0 - min(price_position_pct, 100.0 - price_position_pct) / 50.0
                resonance_score = min(resonance_strength, 1.0)
                iv_score = iv_percentile / 100.0 if iv_filter_passed else 0.0
                flow_score = 1.0 if flow_exhaustion else 0.3

                confidence = (
                    self.PRICE_SCORE_WEIGHT * max(0.0, price_score) +
                    self.RESONANCE_SCORE_WEIGHT * resonance_score +
                    self.IV_SCORE_WEIGHT * iv_score +
                    self.FLOW_SCORE_WEIGHT * flow_score
                )

            # V4-FIX-O9: 不跳过confidence检查 (原FIX-S3S4-13 fallback箱体跳过confidence是bypass)
            # 原则: 数据不可用=不开仓, 不应通过跳过confidence来放行低质量信号
            # O-8已阻断fallback箱体创建, _is_fallback_box恒为False, 移除该条件不影响逻辑
            _is_fallback_box = getattr(box, 'box_id', '') == 'fallback'
            tradeable = (
                extreme_type != '' and
                confidence >= self._params.min_extreme_confidence and
                iv_filter_passed and
                (is_bottom_extreme or is_top_extreme)
            )

            # FIX-S3-3: confidence不足诊断日志（WARNING级别写入signals.jsonl）
            # 根因: confidence < min_extreme_confidence(0.7) 时信号不可交易，但无日志记录导致无法排查
            if not tradeable and extreme_type:
                logger.warning(
                    "[S3-BOX] confidence不足: inst=%s confidence=%.3f < min=%.3f "
                    "(extreme_type=%s iv_passed=%s is_bottom=%s is_top=%s)",
                    instrument_id, confidence, self._params.min_extreme_confidence,
                    extreme_type, iv_filter_passed, is_bottom_extreme, is_top_extreme
                )

            if tradeable:
                self._stats['tradeable_signals'] += 1
                # FIX-56: 记录信号触发时间，用于冷却判断
                if instrument_id:
                    import time as _time_mod
                    self._last_signal_time[instrument_id] = _time_mod.time()

            state = ExtremeState(
                timestamp=now_str,
                extreme_type=extreme_type,
                is_bottom_extreme=is_bottom_extreme,
                is_top_extreme=is_top_extreme,
                price_position_pct=price_position_pct,
                resonance_direction=resonance_direction,
                resonance_strength=resonance_strength,
                iv_percentile=iv_percentile,
                iv_filter_passed=iv_filter_passed,
                flow_exhaustion_detected=flow_exhaustion,
                confidence=confidence,
                tradeable=tradeable,
            )

            # K线箱体字段注入 — 使用缓存的K线箱体状态
            if self._kline_box_daily is not None and self._kline_box_daily.is_valid:
                state.kline_box_confirmed = True
                state.kline_box_type = 'INTRADAY_SMALL'
            elif self._kline_box_weekly is not None and self._kline_box_weekly.is_valid:
                state.kline_box_confirmed = True
                state.kline_box_type = 'OVERNIGHT_MEDIUM'

            self._extreme_state = state
            return state

    def check_iv_filter(self, current_iv: float) -> bool:
        with self._lock:
            if current_iv <= 0:
                return False
            percentile = self._compute_iv_percentile(current_iv)
            passed = percentile >= self._params.iv_percentile_min
            if not passed and current_iv > 0:
                self._stats['iv_filtered'] += 1
            return passed

    @staticmethod
    def compute_iv_percentile(iv_value: float, iv_sorted_list: List[float]) -> float:
        if not iv_sorted_list or iv_value <= 0:
            return 50.0  # FIX-S3S4-8: IV历史为空时返回50.0(中位)而非0.0，避免0.0<iv_percentile_min(50.0)过滤掉所有信号
        count_below = bisect_left(iv_sorted_list, iv_value)
        return count_below / len(iv_sorted_list) * 100.0

    @staticmethod
    def estimate_plr(box_height: float, avg_loss: float, box_gain_ratio: float = 0.5) -> float:
        if box_height < 1e-10 or avg_loss < 1e-10:
            return 0.0
        potential_gain = box_height * box_gain_ratio
        return potential_gain / avg_loss

    def _compute_iv_percentile(self, current_iv: float) -> float:
        return BoxDetector.compute_iv_percentile(current_iv, self._iv_sorted)

    def check_order_flow_exhaustion(
        self,
        flow_imbalance: float,
        cvd_slope: float,
    ) -> bool:
        with self._lock:
            imbalance_exhausted = abs(flow_imbalance) < self.FLOW_IMBALANCE_THRESHOLD
            cvd_stalling = abs(cvd_slope) < self.CVD_SLOPE_THRESHOLD

            exhaustion = imbalance_exhausted or cvd_stalling

            if exhaustion:
                self._stats['flow_exhaustion_confirmed'] += 1

            return exhaustion

    def determine_trade_direction(self, extreme_state: ExtremeState) -> str:
        if extreme_state.is_bottom_extreme and extreme_state.tradeable:
            return 'long'
        elif extreme_state.is_top_extreme and extreme_state.tradeable:
            return 'short'
        return ''

    # ========================================================================
    # K线箱体检测 — S3/S4策略箱体标准化前置条件
    # 日内交易(dte≤5): 至少3根日K线相近高低点结成的小箱形(INTRADAY_SMALL)
    # 隔夜交易(dte>5): 至少3根周K线相近高低点结成的中箱形(OVERNIGHT_MEDIUM)
    # ========================================================================

    @staticmethod
    def _aggregate_to_weekly_klines(
        daily_bars: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """将日K线聚合为周K线

        聚合规则：同一交易周的日K线合并为一根周K线
          - open = 周内第一根日K线的open
          - high = 周内所有日K线的最高high
          - low  = 周内所有日K线的最低low
          - close = 周内最后一根日K线的close
          - volume = 周内所有日K线的volume之和

        Args:
            daily_bars: 日K线列表，每项包含 timestamp, open, high, low, close, volume

        Returns:
            周K线列表，按时间正序排列
        """
        if not daily_bars:
            return []

        weekly: Dict[str, Dict[str, Any]] = {}
        for bar in daily_bars:
            ts_str = bar.get('timestamp', '')
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=_CHINA_TZ)
                # ISO周数: "2026-W30" 格式
                week_key = ts.strftime('%G-W%V')
            except (ValueError, TypeError):
                continue

            if week_key not in weekly:
                weekly[week_key] = {
                    'timestamp': ts_str,
                    'open': float(bar.get('open', 0.0)),
                    'high': float(bar.get('high', 0.0)),
                    'low': float(bar.get('low', 0.0)),
                    'close': float(bar.get('close', 0.0)),
                    'volume': float(bar.get('volume', 0.0)),
                }
            else:
                w = weekly[week_key]
                w['high'] = max(w['high'], float(bar.get('high', 0.0)))
                w['low'] = min(w['low'], float(bar.get('low', 0.0)))
                w['close'] = float(bar.get('close', 0.0))
                w['volume'] += float(bar.get('volume', 0.0))
                # 保留第一根的open和timestamp

        # 按timestamp排序
        return sorted(weekly.values(), key=lambda x: x.get('timestamp', ''))

    @staticmethod
    def _detect_kline_box_from_bars(
        bars: List[Dict[str, Any]],
        min_bars: int = 3,
        width_max_pct: float = 5.0,
        box_type: BoxType = BoxType.INTRADAY_SMALL,
    ) -> KLineBoxProfile:
        """从K线列表中检测箱体

        算法：
        1. 取最近 min_bars 根K线
        2. 高点聚类找上沿，低点聚类找下沿
        3. 宽度 ≤ width_max_pct% 且 K线数 ≥ min_bars → 箱体确认

        Args:
            bars: K线列表，每项包含 high, low, close, timestamp
            min_bars: 最少K线数（日K=3，周K=3）
            width_max_pct: 箱体最大宽度百分比
            box_type: 箱体类型(INTRADAY_SMALL/OVERNIGHT_MEDIUM)

        Returns:
            KLineBoxProfile 箱体轮廓
        """
        if len(bars) < min_bars:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(bars),
                is_valid=False,
            )

        # 取最近 min_bars * 2 根K线（扩大搜索范围），但不超过总数
        recent = bars[-(min_bars * 2):] if len(bars) >= min_bars * 2 else bars

        highs = [float(b.get('high', 0.0)) for b in recent if float(b.get('high', 0.0)) > 0]
        lows = [float(b.get('low', 0.0)) for b in recent if float(b.get('low', 0.0)) > 0]
        closes = [float(b.get('close', 0.0)) for b in recent if float(b.get('close', 0.0)) > 0]

        if not highs or not lows or not closes:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(recent),
                is_valid=False,
            )

        recent_close = closes[-1]
        if recent_close < 1e-10:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(recent),
                is_valid=False,
            )

        # 聚类法找高低点的聚类中心
        supports, resistances = BoxDetector._find_support_resistance(
            lows, highs, n_clusters=2, tolerance_pct=0.3,
        )

        if supports and resistances:
            box_lower = supports[0]
            box_upper = resistances[0]
        else:
            box_upper = max(highs)
            box_lower = min(lows)

        if box_upper <= box_lower or box_lower <= 0:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(recent),
                is_valid=False,
            )

        width_pct = (box_upper - box_lower) / recent_close * 100.0

        # 检查有多少根K线的高低点落在箱体容差范围内
        tolerance = (box_upper - box_lower) * 0.1  # 10%容差
        confirming_bars = 0
        for b in recent:
            h = float(b.get('high', 0.0))
            l = float(b.get('low', 0.0))
            if h <= 0 or l <= 0:
                continue
            # 高点接近上沿 或 低点接近下沿
            near_upper = abs(h - box_upper) <= tolerance
            near_lower = abs(l - box_lower) <= tolerance
            if near_upper or near_lower:
                confirming_bars += 1

        is_valid = (
            width_pct <= width_max_pct
            and confirming_bars >= min_bars
        )

        confidence = 0.0
        if is_valid:
            # 置信度: 宽度得分 * 0.4 + K线确认数得分 * 0.6
            width_score = max(0.0, 1.0 - width_pct / width_max_pct)
            bar_score = min(1.0, confirming_bars / (min_bars * 2))
            confidence = 0.4 * width_score + 0.6 * bar_score

        return KLineBoxProfile(
            box_type=box_type,
            upper=box_upper,
            lower=box_lower,
            width_pct=round(width_pct, 4),
            bar_count=confirming_bars,
            is_valid=is_valid,
            confidence=round(confidence, 4),
        )

    def detect_kline_box(
        self,
        instrument_id: str,
        daily_bars: Optional[List[Dict[str, Any]]] = None,
        force: bool = False,
    ) -> Tuple[Optional[KLineBoxProfile], Optional[KLineBoxProfile]]:
        """检测日K小箱体和周K中箱体

        数据源优先级:
        1. 外部传入daily_bars参数
        2. DataService.get_symbol_daily_ohlc查询
        3. 内部tick缓存降采样(最后降级方案)

        Args:
            instrument_id: 合约ID
            daily_bars: 日K线数据(外部传入)，None则自动获取
            force: 是否强制重新计算(忽略缓存TTL)

        Returns:
            (daily_box, weekly_box) 日K小箱体和周K中箱体
        """
        with self._lock:
            now = time.time()
            # 缓存命中: 同一合约 + TTL未过期 + 非强制
            if (not force
                    and instrument_id == self._kline_box_instrument_id
                    and (now - self._kline_box_last_update) < self._kline_box_cache_ttl_sec
                    and self._kline_box_daily is not None):
                return self._kline_box_daily, self._kline_box_weekly

            # 数据源优先级: 外部传入 > DataService > tick缓存降采样
            bars = daily_bars
            if bars is None:
                bars = self._fetch_daily_bars_from_dataservice(instrument_id)
            if bars is None or len(bars) < 1:
                bars = self._build_daily_bars_from_cache()

            # 日K小箱体检测
            daily_box = self._detect_kline_box_from_bars(
                bars=bars,
                min_bars=self._kline_min_bars_daily,
                width_max_pct=self._kline_box_width_max_pct,
                box_type=BoxType.INTRADAY_SMALL,
            )

            # 周K中箱体检测: 先将日K线聚合为周K线
            weekly_bars = self._aggregate_to_weekly_klines(bars)
            weekly_box = self._detect_kline_box_from_bars(
                bars=weekly_bars,
                min_bars=self._kline_min_bars_weekly,
                width_max_pct=self._kline_box_width_max_pct,
                box_type=BoxType.OVERNIGHT_MEDIUM,
            )

            # 更新缓存
            self._kline_box_daily = daily_box
            self._kline_box_weekly = weekly_box
            self._kline_box_instrument_id = instrument_id
            self._kline_box_last_update = now

            logger.info(
                "[KLINE-BOX] detect_kline_box: inst=%s daily=%s(upper=%.2f lower=%.2f w=%.2f%% bars=%d conf=%.3f) "
                "weekly=%s(upper=%.2f lower=%.2f w=%.2f%% bars=%d conf=%.3f)",
                instrument_id,
                '✓' if daily_box.is_valid else '✗',
                daily_box.upper, daily_box.lower, daily_box.width_pct, daily_box.bar_count, daily_box.confidence,
                '✓' if weekly_box.is_valid else '✗',
                weekly_box.upper, weekly_box.lower, weekly_box.width_pct, weekly_box.bar_count, weekly_box.confidence,
            )

            return daily_box, weekly_box

    def _build_daily_bars_from_cache(self) -> List[Dict[str, Any]]:
        """从内部tick级价格缓存构建日K线数据

        Returns:
            日K线列表(近似，仅当外部日K线不可用时的降级方案)
        """
        if len(self._price_closes) < 3:
            return []

        # 简化日K线构建：每lookback_bars/tick_per_day根bar合为一根"日K线"
        # 这是一个近似方案，真实日K线应从DataService获取
        closes = list(self._price_closes)
        highs = list(self._price_highs)
        lows = list(self._price_lows)
        timestamps = list(self._timestamps)

        bars = []
        # 按天分组（简化: 以日期字符串为key）
        daily_groups: Dict[str, Dict[str, Any]] = {}
        for i in range(len(closes)):
            ts_str = timestamps[i] if i < len(timestamps) else ''
            if not ts_str:
                continue
            try:
                day_key = ts_str[:10]  # "2026-07-24"
            except (IndexError, TypeError):
                continue

            if day_key not in daily_groups:
                daily_groups[day_key] = {
                    'timestamp': ts_str,
                    'open': closes[i],
                    'high': highs[i] if i < len(highs) else closes[i],
                    'low': lows[i] if i < len(lows) else closes[i],
                    'close': closes[i],
                    'volume': 0.0,
                }
            else:
                g = daily_groups[day_key]
                g['high'] = max(g['high'], highs[i] if i < len(highs) else closes[i])
                g['low'] = min(g['low'], lows[i] if i < len(lows) else closes[i])
                g['close'] = closes[i]

        bars = sorted(daily_groups.values(), key=lambda x: x.get('timestamp', ''))
        return bars

    def _fetch_daily_bars_from_dataservice(
        self,
        instrument_id: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """从DataService获取日K线数据 — K线箱体的主数据源

        优先使用DataService.get_symbol_daily_ohlc查询数据库中日K线聚合数据。
        失败时返回None（调用方会降级到_build_daily_bars_from_cache）。

        Args:
            instrument_id: 合约ID

        Returns:
            日K线列表 或 None(获取失败)
        """
        try:
            from data.data_service import get_data_service
            ds = get_data_service()
            if ds is None:
                return None

            from datetime import date, timedelta as _td
            end_date = date.today()
            # 取最近10个交易日的日K线(足够覆盖3根日K线箱体检测)
            start_date = end_date - _td(days=21)  # 3周≈15交易日

            result = ds.get_symbol_daily_ohlc(instrument_id, start_date, end_date)
            if result is None:
                # FIX-KLINE-FLUSH-20260725-FALLBACK: 主数据源返回None时仍尝试klines_raw fallback,
                # 避免DataService视图缺失/异常时直接放弃K线箱体检测。
                klines_raw_bars = self._fetch_daily_bars_from_klines_raw(instrument_id)
                if klines_raw_bars:
                    logger.info("[KLINE-BOX] get_symbol_daily_ohlc=None, klines_raw fallback: inst=%s bars=%d",
                                instrument_id, len(klines_raw_bars))
                    return klines_raw_bars
                return None

            # 兼容pa.Table和pd.DataFrame
            if hasattr(result, 'to_pydict'):
                data = result.to_pydict()
            elif hasattr(result, 'to_dict'):
                data = result.to_dict(orient='list')
            else:
                return None

            if not data or not data.get('date'):
                return None

            # 转换为_detect_kline_box_from_bars所需格式
            # 注意: symbol_daily_aggregates视图列名为
            #   open_price/close_price/high_price/low_price/total_volume(非open/close/high/low/volume)
            bars = []
            dates = data.get('date', [])
            opens = data.get('open_price', data.get('open', []))
            highs = data.get('high_price', data.get('high', []))
            lows = data.get('low_price', data.get('low', []))
            closes = data.get('close_price', data.get('close', []))
            volumes = data.get('total_volume', data.get('volume', []))

            for i in range(len(dates)):
                d = dates[i]
                # 处理date/datetime类型
                if hasattr(d, 'isoformat'):
                    ts_str = d.isoformat()
                else:
                    ts_str = str(d)

                o = float(opens[i]) if i < len(opens) and opens[i] is not None else 0.0
                h = float(highs[i]) if i < len(highs) and highs[i] is not None else 0.0
                l = float(lows[i]) if i < len(lows) and lows[i] is not None else 0.0
                c = float(closes[i]) if i < len(closes) and closes[i] is not None else 0.0
                v = float(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0.0

                if h <= 0 or l <= 0 or c <= 0:
                    continue

                bars.append({
                    'timestamp': ts_str,
                    'open': o,
                    'high': h,
                    'low': l,
                    'close': c,
                    'volume': v,
                })

            if bars:
                return bars

            # FIX-KLINE-FLUSH-20260725: symbol_daily_aggregates表无数据时,
            # 从klines_raw表查询M1 K线并聚合成日K线(flush_incomplete_klines落库的K线)
            # 根因: flush_incomplete_klines落库到klines_raw表, 但原代码只查symbol_daily_aggregates
            #   → klines_raw数据从未被读取 → K线管道断裂
            # 修复: 增加klines_raw fallback, 把M1 K线按trade_date聚合成日K线
            klines_raw_bars = self._fetch_daily_bars_from_klines_raw(instrument_id)
            if klines_raw_bars:
                logger.info("[KLINE-BOX] _fetch_daily_bars_from_klines_raw fallback: inst=%s bars=%d",
                            instrument_id, len(klines_raw_bars))
                return klines_raw_bars

            return None

        except Exception as e:
            logger.debug("[KLINE-BOX] _fetch_daily_bars_from_dataservice failed: inst=%s err=%s",
                         instrument_id, e)
            return None

    def _fetch_daily_bars_from_klines_raw(self, instrument_id: str) -> Optional[List[Dict[str, Any]]]:
        """从klines_raw表查询M1 K线并聚合成日K线(FIX-KLINE-FLUSH-20260725)

        当symbol_daily_aggregates表无数据时, 从klines_raw表查询flush_incomplete_klines
        落库的M1 K线, 按trade_date聚合成日K线。

        Returns:
            日K线列表 或 None(获取失败)
        """
        try:
            from data.data_service import get_data_service
            ds = get_data_service()
            if ds is None:
                return None

            # 查询internal_id(klines_raw用internal_id, 不是instrument_id)
            from config.params_service import get_params_service
            ps = get_params_service()
            meta = ps.get_instrument_meta_by_id(instrument_id)
            if meta is None:
                return None
            internal_id = meta.get('internal_id')
            if internal_id is None:
                return None

            # 查询最近21天的M1 K线(覆盖3周≈15交易日)
            from datetime import date, timedelta as _td
            end_date = date.today()
            start_date = end_date - _td(days=21)

            sql = """
                SELECT trade_date, open, high, low, close, volume
                FROM klines_raw
                WHERE internal_id = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date, timestamp
            """
            result = ds.query(sql, [internal_id, start_date, end_date])
            if result is None:
                return None

            # 兼容pa.Table和pd.DataFrame
            if hasattr(result, 'to_pydict'):
                data = result.to_pydict()
            elif hasattr(result, 'to_dict'):
                data = result.to_dict(orient='list')
            else:
                return None

            if not data or not data.get('trade_date'):
                return None

            # 按trade_date聚合成日K线
            trade_dates = data.get('trade_date', [])
            opens = data.get('open', [])
            highs = data.get('high', [])
            lows = data.get('low', [])
            closes = data.get('close', [])
            volumes = data.get('volume', [])

            daily_groups: Dict[str, Dict[str, Any]] = {}
            for i in range(len(trade_dates)):
                d = trade_dates[i]
                if hasattr(d, 'isoformat'):
                    day_key = d.isoformat()
                else:
                    day_key = str(d)

                o = float(opens[i]) if i < len(opens) and opens[i] is not None else 0.0
                h = float(highs[i]) if i < len(highs) and highs[i] is not None else 0.0
                l = float(lows[i]) if i < len(lows) and lows[i] is not None else 0.0
                c = float(closes[i]) if i < len(closes) and closes[i] is not None else 0.0
                v = float(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0.0

                if h <= 0 or l <= 0 or c <= 0:
                    continue

                if day_key not in daily_groups:
                    daily_groups[day_key] = {
                        'timestamp': day_key,
                        'open': o,
                        'high': h,
                        'low': l,
                        'close': c,
                        'volume': v,
                    }
                else:
                    g = daily_groups[day_key]
                    g['high'] = max(g['high'], h)
                    g['low'] = min(g['low'], l)
                    g['close'] = c
                    g['volume'] += v

            bars = sorted(daily_groups.values(), key=lambda x: x.get('timestamp', ''))
            return bars if bars else None

        except Exception as e:
            logger.debug("[KLINE-BOX] _fetch_daily_bars_from_klines_raw failed: inst=%s err=%s",
                         instrument_id, e)
            return None

    def check_kline_box_precondition(
        self,
        instrument_id: str,
        days_to_expiry: int = 0,
        daily_bars: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[bool, Optional[KLineBoxProfile]]:
        """K线箱体前置条件检查 — S3/S4策略信号产生的唯一箱体来源

        规则：
        - 日内交易(dte≤5): 必须有日K小箱体确认(INTRADAY_SMALL)
        - 隔夜交易(dte>5): 必须有周K中箱体确认(OVERNIGHT_MEDIUM)
        - K线箱体确认后自动更新_current_box，作为信号生成的箱体边界

        Args:
            instrument_id: 合约ID
            days_to_expiry: 距到期日天数(0=默认按隔夜)
            daily_bars: 外部传入的日K线数据

        Returns:
            (passed, kline_box) 是否通过前置条件 + 匹配的K线箱体轮廓
        """
        daily_box, weekly_box = self.detect_kline_box(
            instrument_id=instrument_id,
            daily_bars=daily_bars,
        )

        # dte≤5 → 日内交易 → 检查日K小箱体
        if days_to_expiry <= 5:
            if daily_box is not None and daily_box.is_valid:
                # K线箱体确认 → 更新_current_box为K线箱体边界
                self._update_current_box_from_kline(daily_box, 'INTRADAY_SMALL')
                return True, daily_box
            else:
                logger.debug(
                    "[KLINE-BOX] PRECONDITION FAIL: inst=%s dte=%d 日内交易需日K小箱体(upper=%.2f lower=%.2f bars=%d valid=%s)",
                    instrument_id, days_to_expiry,
                    daily_box.upper if daily_box else 0.0,
                    daily_box.lower if daily_box else 0.0,
                    daily_box.bar_count if daily_box else 0,
                    daily_box.is_valid if daily_box else False,
                )
                return False, daily_box

        # dte>5 → 隔夜交易 → 检查周K中箱体
        else:
            if weekly_box is not None and weekly_box.is_valid:
                # K线箱体确认 → 更新_current_box为K线箱体边界
                self._update_current_box_from_kline(weekly_box, 'OVERNIGHT_MEDIUM')
                return True, weekly_box
            else:
                logger.debug(
                    "[KLINE-BOX] PRECONDITION FAIL: inst=%s dte=%d 隔夜交易需周K中箱体(upper=%.2f lower=%.2f bars=%d valid=%s)",
                    instrument_id, days_to_expiry,
                    weekly_box.upper if weekly_box else 0.0,
                    weekly_box.lower if weekly_box else 0.0,
                    weekly_box.bar_count if weekly_box else 0,
                    weekly_box.is_valid if weekly_box else False,
                )
                return False, weekly_box

    def _update_current_box_from_kline(
        self,
        kline_box: KLineBoxProfile,
        kline_type_str: str,
    ) -> None:
        """K线箱体确认后更新_current_box — 使classify_extreme_state使用K线箱体边界

        关键：tick级箱体已废弃，K线箱体是S3/S4的唯一箱体来源。
        此方法将K线箱体的upper/lower转化为BoxProfile，供下游信号判断使用。
        """
        with self._lock:
            now_str = datetime.now(_CHINA_TZ).isoformat()
            self._box_id_counter += 1
            box_id = f"KLINE-{kline_type_str}-{self._box_id_counter:06d}"

            median = (kline_box.upper + kline_box.lower) / 2.0 if kline_box.upper > kline_box.lower > 0 else 0.0

            profile = BoxProfile(
                box_id=box_id,
                timestamp=now_str,
                is_box=True,
                box_type=f'kline_{kline_type_str.lower()}',
                upper=kline_box.upper,
                lower=kline_box.lower,
                median=median,
                width_pct=kline_box.width_pct,
                confidence=kline_box.confidence,
                duration_bars=kline_box.bar_count,
                bounce_count=kline_box.bar_count,
                adx=0.0,  # K线箱体不使用ADX
                kline_box_confirmed=True,
                kline_box_type=kline_type_str,
            )

            self._current_box = profile
            self._box_history.append(profile)
            self._stats['boxes_detected'] += 1
            self._bounce_at_lower = 0
            self._bounce_at_upper = 0

            logger.info(
                "[KLINE-BOX] _update_current_box: box_id=%s type=%s upper=%.2f lower=%.2f w=%.2f%% bars=%d conf=%.3f",
                box_id, kline_type_str, kline_box.upper, kline_box.lower,
                kline_box.width_pct, kline_box.bar_count, kline_box.confidence,
            )

    # [FIX-20260712-S3] 假突破过滤模块 — 上策H-Rev
    # 原理: 极值突破后若价格迅速回落回箱体内，说明是假突破，应过滤信号
    # 实现: 记录最近突破信号，检查当前价格是否已从突破点回落超过50%
    def check_false_breakout(self, current_price: float, extreme_type: str) -> bool:
        """检查当前信号是否为假突破

        Args:
            current_price: 当前价格
            extreme_type: 极值类型 ('box_bottom_extreme' / 'box_top_extreme')

        Returns:
            True = 通过过滤（不是假突破），False = 假突破（应过滤掉）
        """
        with self._lock:
            # [FIX-20260712-S3-P1] 无箱体时无法计算突破距离，放行信号避免 crash
            if self._current_box is None:
                return True

            now = time.time()
            # 清理过期记录
            while self._breakout_tracker:
                _ts, _price, _etype = self._breakout_tracker[0]
                if now - _ts > self._false_breakout_lookback_sec:
                    self._breakout_tracker.popleft()
                else:
                    break

            # 检查是否有同类型的近期突破记录
            for _ts, _price, _etype in self._breakout_tracker:
                if _etype != extreme_type:
                    continue
                # 计算价格从突破点的回落比例
                if _price <= 0 or current_price <= 0:
                    continue
                price_move = abs(current_price - _price)
                breakout_distance = abs(_price - (self._current_box.lower if 'bottom' in extreme_type else self._current_box.upper))
                if breakout_distance > 0:
                    retrace_ratio = price_move / breakout_distance
                    if retrace_ratio > self._false_breakout_retrace_ratio:
                        self._stats['false_breakout_filtered'] += 1
                        logger.debug("[BoxDetector] 假突破过滤: extreme=%s breakout_price=%.2f current=%.2f retrace=%.1f%%",
                                     extreme_type, _price, current_price, retrace_ratio * 100)
                        return False
                break  # 只检查最近一条同类型记录

            # 记录当前突破信号
            self._breakout_tracker.append((now, current_price, extreme_type))
            return True

    def get_extreme_state(self) -> Optional[ExtremeState]:
        with self._lock:
            return self._extreme_state

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'component': 'box_detector',
                'status': 'OK',
                'current_box': self._current_box.to_dict() if self._current_box else None,
                'extreme_state': self._extreme_state.to_dict() if self._extreme_state else None,
                'bars_processed': self._stats['bars_processed'],
                'boxes_detected': self._stats['boxes_detected'],
                'tradeable_signals': self._stats['tradeable_signals'],
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['current_box_valid'] = self._current_box.is_valid if self._current_box else False
            stats['iv_history_size'] = len(self._iv_history)
            stats['price_bars'] = len(self._price_closes)
            stats['adx_period'] = self._adx_period
            stats['adx_threshold'] = self._adx_threshold
            stats['bounce_tolerance_pct'] = self._bounce_tolerance_pct
            return stats

    def get_box_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return [b.to_dict() for b in list(self._box_history)[-limit:]]

    def estimate_potential_plr(self, current_price: float, direction: str) -> float:
        with self._lock:
            if not self._current_box or self._current_box.lower is None or self._current_box.upper is None:
                return 0.0
            box_bottom = self._current_box.lower
            box_top = self._current_box.upper
            box_height = box_top - box_bottom
            if box_height < 1e-10:
                return 0.0
            if direction == 'long':
                risk = current_price - box_bottom
                reward = box_top - current_price
            else:
                risk = box_top - current_price
                reward = current_price - box_bottom
            if risk < 1e-10:
                return 0.0
            return reward / risk


_box_detector: Optional[BoxDetector] = None
_box_detector_lock = threading.Lock()


def get_box_detector(**kwargs) -> BoxDetector:
    global _box_detector
    with _box_detector_lock:
        if _box_detector is None:
            if not kwargs:
                try:
                    from config.config_service import get_cached_params
                    all_params = get_cached_params()
                    detector_keys = ['box_gain_ratio', 'plr_normalization_base']
                    for k in detector_keys:
                        if k in all_params and k not in kwargs:
                            kwargs[k] = all_params[k]
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
            _box_detector = BoxDetector(**kwargs)
        return _box_detector


__all__ = [
    'BoxDetector',
    'BoxProfile',
    'ExtremeState',
    'BoxStrategyParams',
    'BoxType',
    'KLineBoxProfile',
    'get_box_detector',
]
