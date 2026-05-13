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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
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
    duration_bars: int = 0
    bounce_count: int = 0
    adx: float = 0.0

    @property
    def is_valid(self) -> bool:
        return self.is_box and self.upper > self.lower > 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
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
    tradeable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BoxStrategyParams:
    """箱体策略参数（比趋势策略更苛刻）"""
    max_hold_minutes: int = 30
    take_profit_ratio: float = 0.4
    stop_loss_ratio: float = 0.3
    max_risk_ratio: float = 0.05
    iv_percentile_min: float = 50.0
    signal_cooldown_sec: float = 120.0
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

    def __init__(
        self,
        params: Optional[BoxStrategyParams] = None,
        lookback_bars: int = 60,
        min_box_bars: int = 20,
        adx_period: int = 14,
        adx_threshold: float = 25.0,
        bounce_tolerance_pct: float = 0.1,
        iv_history_maxlen: int = 1000,
    ):
        self._lock = threading.RLock()
        self._params = params or BoxStrategyParams()
        self._lookback_bars = lookback_bars
        self._min_box_bars = min_box_bars
        self._adx_period = adx_period
        self._adx_threshold = adx_threshold
        self._bounce_tolerance_pct = bounce_tolerance_pct

        self._price_highs: deque = deque(maxlen=lookback_bars)
        self._price_lows: deque = deque(maxlen=lookback_bars)
        self._price_closes: deque = deque(maxlen=lookback_bars)
        self._volumes: deque = deque(maxlen=lookback_bars)
        self._timestamps: deque = deque(maxlen=lookback_bars)

        self._current_box: Optional[BoxProfile] = None
        self._box_history: deque = deque(maxlen=100)
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
        }

        self._box_id_counter: int = 0
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
            self._timestamps.append(timestamp or datetime.now().isoformat())
            self._stats['bars_processed'] += 1

    def update_iv(self, iv: float) -> None:
        with self._lock:
            if len(self._iv_history) == self._iv_history.maxlen:
                oldest = self._iv_history[0]
                idx = bisect_left(self._iv_sorted, oldest)
                if idx < len(self._iv_sorted) and self._iv_sorted[idx] == oldest:
                    self._iv_sorted.pop(idx)
            self._iv_history.append(iv)
            insort(self._iv_sorted, iv)

    @staticmethod
    def _compute_adx_simplified(highs, lows, closes, period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
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
            return 50.0

        avg_plus_dm = sum(plus_dm_list) / len(plus_dm_list)
        avg_minus_dm = sum(minus_dm_list) / len(minus_dm_list)
        avg_tr = sum(tr_list) / len(tr_list)

        if avg_tr < 1e-10:
            return 50.0

        plus_di = 100.0 * avg_plus_dm / avg_tr
        minus_di = 100.0 * avg_minus_dm / avg_tr

        di_sum = plus_di + minus_di
        if di_sum < 1e-10:
            return 0.0

        dx = 100.0 * abs(plus_di - minus_di) / di_sum
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
            now_str = datetime.now().isoformat()
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

                confidence = 0.3 * width_score + 0.3 * adx_score + 0.4 * bounce_score

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

            if is_box:
                self._current_box = profile
                self._box_history.append(profile)
                self._stats['boxes_detected'] += 1
                self._bounce_at_lower = 0
                self._bounce_at_upper = 0

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
    ) -> ExtremeState:
        with self._lock:
            now_str = datetime.now().isoformat()

            if self._current_box is None or not self._current_box.is_valid:
                return ExtremeState(timestamp=now_str)

            box = self._current_box
            box_range = box.upper - box.lower

            if box_range < 1e-10:
                return ExtremeState(timestamp=now_str)

            price_position_pct = (current_price - box.lower) / box_range * 100.0

            is_bottom = current_price <= box.lower + box_range * 0.15
            is_top = current_price >= box.upper - box_range * 0.15

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
            iv_filter_passed = iv_percentile >= self._params.iv_percentile_min if current_iv > 0 else False
            if not iv_filter_passed and current_iv > 0:
                self._stats['iv_filtered'] += 1

            imbalance_exhausted = abs(flow_imbalance) < 0.2
            cvd_stalling = abs(cvd_slope) < 0.01
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
                    0.25 * max(0.0, price_score) +
                    0.30 * resonance_score +
                    0.25 * iv_score +
                    0.20 * flow_score
                )

            tradeable = (
                extreme_type != '' and
                confidence >= self._params.min_extreme_confidence and
                iv_filter_passed and
                (is_bottom_extreme or is_top_extreme)
            )

            if tradeable:
                self._stats['tradeable_signals'] += 1

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

    def _compute_iv_percentile(self, current_iv: float) -> float:
        if not self._iv_sorted or current_iv <= 0:
            return 0.0
        count_below = bisect_left(self._iv_sorted, current_iv)
        return count_below / len(self._iv_sorted) * 100.0

    def check_order_flow_exhaustion(
        self,
        flow_imbalance: float,
        cvd_slope: float,
    ) -> bool:
        with self._lock:
            imbalance_exhausted = abs(flow_imbalance) < 0.2
            cvd_stalling = abs(cvd_slope) < 0.01

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


_box_detector: Optional[BoxDetector] = None
_box_detector_lock = threading.Lock()


def get_box_detector(**kwargs) -> BoxDetector:
    global _box_detector
    if _box_detector is None:
        with _box_detector_lock:
            if _box_detector is None:
                _box_detector = BoxDetector(**kwargs)
    return _box_detector


__all__ = [
    'BoxDetector',
    'BoxProfile',
    'ExtremeState',
    'BoxStrategyParams',
    'get_box_detector',
]
