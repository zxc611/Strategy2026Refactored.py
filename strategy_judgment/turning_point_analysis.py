# [M3-10] 拐点显微镜
# MODULE_ID: M3-628
"""

æç¹æ¾å¾®é?(Turning Point Microscope) â?ç­ç¥å¾®è§è¡ä¸ºè¯æ­ç³»ç»çæ°æ®åºç¡



åè½ï¼?
  1. Tick â?å¢å¼ºBarï¼VWAP/é«ä½ç¹æ¶é´åç§?é«ä½ç¹æäº¤é/ä¹°åä»·å·®ï¼?
  2. å¨çº¿é«ä½ç¹è¯å«ï¼ä»ä½¿ç¨åå²æ°æ®ï¼ç¦æ­¢æªæ¥ä¿¡æ¯æ³é²ï¼?
  3. 多周期均线位置计算（5/10/15/30/60期MA + 标准化偏离度 + 均线排列形态）

  4. åºå"ææ¾é«ä½ç?ä¸?æç«¯è¡æ"



å³é®è®¾è®¡å³ç­ï¼?
  - VWAP作为Bar代表价，而非简单收盘价（无偏估计）

  - 高低点不仅记录价格，还记录时间戳和成交量分布

  - 均线计算严格使用截至当前Bar的数据（当前Bar的close只参与下一Bar的均线）

  - 极值识别使用rolling窗口+前瞻确认（N-Bar后确认，非实时最优）

"""

from __future__ import annotations



import logging

from collections import deque

from dataclasses import dataclass, field, asdict

from enum import Enum

from typing import Any, Dict, List, Optional, Tuple



import numpy as np
import pandas as pd

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5



logger = get_logger(__name__)  # R9-5





class ExtremeRegion(Enum):

    NORMAL = "NORMAL"

    NEAR_HIGH = "NEAR_HIGH"

    NEAR_LOW = "NEAR_LOW"

    EXTREME_HIGH = "EXTREME_HIGH"

    EXTREME_LOW = "EXTREME_LOW"





class MAAlignment(Enum):

    BULLISH = "多头排列"

    BEARISH = "空头排列"

    CONVERGENT = "均线收敛"

    INTERTWINED = "均线缠绕"

    TRANSITIONAL = "过渡态"





@dataclass(slots=True)

class EnhancedBar:

    timestamp: np.datetime64

    symbol: str

    open: float

    high: float

    low: float

    close: float

    volume: int

    vwap: float

    high_time_offset_ms: int

    low_time_offset_ms: int

    volume_at_high: float

    volume_at_low: float

    bid_ask_spread_avg: float

    open_interest: int = 0

    tick_count: int = 0

    extreme_region: ExtremeRegion = ExtremeRegion.NORMAL

    price_vs_mas: Dict[str, float] = field(default_factory=dict)

    ma_alignment: MAAlignment = MAAlignment.TRANSITIONAL

    price_ma_deviation_sigma: Dict[str, float] = field(default_factory=dict)

    ma_curvatures: Dict[str, float] = field(default_factory=dict)

    atr14: float = 0.0



    def to_dict(self) -> Dict[str, Any]:

        d = asdict(self)

        d['extreme_region'] = self.extreme_region.value

        d['ma_alignment'] = self.ma_alignment.value

        d['timestamp'] = str(self.timestamp)

        return d





@dataclass(slots=True)

class _TickAccumulator:

    bar_start_ts: Optional[np.datetime64] = None

    symbol: str = ""

    open: float = 0.0

    high: float = -np.inf

    low: float = np.inf

    close: float = 0.0

    total_volume: int = 0

    total_turnover: float = 0.0

    open_interest: int = 0

    tick_count: int = 0

    high_ts: Optional[np.datetime64] = None

    low_ts: Optional[np.datetime64] = None

    volume_at_high: float = 0.0

    volume_at_low: float = 0.0

    bid_ask_spread_sum: float = 0.0

    prev_total_volume: int = 0



    def reset(self, first_tick_ts: np.datetime64, symbol: str):

        self.bar_start_ts = first_tick_ts

        self.symbol = symbol

        self.open = 0.0

        self.high = -np.inf

        self.low = np.inf

        self.close = 0.0

        self.total_volume = 0

        self.total_turnover = 0.0

        self.open_interest = 0

        self.tick_count = 0

        self.high_ts = None

        self.low_ts = None

        self.volume_at_high = 0.0

        self.volume_at_low = 0.0

        self.bid_ask_spread_sum = 0.0

        self.prev_total_volume = 0





class TurningPointMicroscope:

    """

    æç¹æ¾å¾®éï¼Tickâå¢å¼ºBar + å¨çº¿é«ä½ç¹è¯å?+ åçº¿ä½ç½®



    ä½¿ç¨æ¹æ³ï¼?
      1. 每个tick调用 process_tick()

      2. 分钟Bar完成时自动回调，产生EnhancedBar

      3. 每个EnhancedBar包含极值区域判定和均线位置信息

    """



    DEFAULT_MA_PERIODS = [5, 10, 15, 30, 60]



    def __init__(

        self,

        symbol: str,

        ma_periods: Optional[List[int]] = None,

        extreme_window: int = 20,

        extreme_quantile: float = 0.05,

        extreme_check_bars: int = 3,

        atr_period: int = 14,

        minute_ms: int = 60_000,

        on_bar_complete: Optional[callable] = None,

    ):

        self._symbol = symbol

        self._ma_periods = ma_periods or self.DEFAULT_MA_PERIODS

        self._extreme_window = extreme_window

        self._extreme_quantile = extreme_quantile

        self._extreme_check_bars = extreme_check_bars

        self._atr_period = atr_period

        self._minute_ms = minute_ms

        self._on_bar_complete = on_bar_complete



        self._vwap_buffers: Dict[int, deque] = {p: deque(maxlen=p) for p in self._ma_periods}

        self._close_buffer: deque = deque(maxlen=max(self._ma_periods) + 2)

        self._high_buffer: deque = deque(maxlen=self._extreme_window + self._extreme_check_bars + 1)

        self._low_buffer: deque = deque(maxlen=self._extreme_window + self._extreme_check_bars + 1)

        self._tr_buffer: deque = deque(maxlen=self._atr_period + 1)



        self._tick_acc = _TickAccumulator()

        self._bar_count = 0

        self._last_bar: Optional[EnhancedBar] = None

        self._current_bar_start_ms: int = -1



        self._all_bars: List[EnhancedBar] = []

        self._extreme_snapshots: List[EnhancedBar] = []



    def process_tick(

        self,

        timestamp: np.datetime64,

        last_price: float,

        volume: int,

        turnover: float,

        bid_price1: float = 0.0,

        ask_price1: float = 0.0,

        open_interest: int = 0,

        total_volume: int = 0,

    ) -> Optional[EnhancedBar]:

        tick_volume = max(0, total_volume - self._tick_acc.prev_total_volume)

        self._tick_acc.prev_total_volume = total_volume

        if tick_volume == 0 and self._tick_acc.tick_count > 0:

            tick_volume = 0



        if self._tick_acc.tick_count == 0:

            self._tick_acc.reset(timestamp, self._symbol)

            self._tick_acc.open = last_price



        if last_price > self._tick_acc.high:

            self._tick_acc.high = last_price

            self._tick_acc.high_ts = timestamp

            self._tick_acc.volume_at_high = tick_volume

        elif last_price == self._tick_acc.high:

            self._tick_acc.volume_at_high += tick_volume



        if last_price < self._tick_acc.low:

            self._tick_acc.low = last_price

            self._tick_acc.low_ts = timestamp

            self._tick_acc.volume_at_low = tick_volume

        elif last_price == self._tick_acc.low:

            self._tick_acc.volume_at_low += tick_volume



        self._tick_acc.close = last_price

        self._tick_acc.total_volume += tick_volume

        self._tick_acc.total_turnover += (last_price * tick_volume)

        self._tick_acc.open_interest = open_interest

        self._tick_acc.tick_count += 1



        if ask_price1 > 0 and bid_price1 > 0:

            self._tick_acc.bid_ask_spread_sum += (ask_price1 - bid_price1)



        if pd.isna(timestamp):

            return None

        bar_ts_ms = int(timestamp.astype('datetime64[ms]').astype(np.int64))

        bar_start_ms = bar_ts_ms - (bar_ts_ms % self._minute_ms)



        emitted_bar = None

        if self._current_bar_start_ms >= 0 and bar_start_ms != self._current_bar_start_ms:

            emitted_bar = self._emit_bar(timestamp)



        self._current_bar_start_ms = bar_start_ms



        return emitted_bar



    def _emit_bar(self, current_ts: np.datetime64) -> EnhancedBar:

        acc = self._tick_acc

        if acc.tick_count == 0:

            return self._last_bar



        vwap = acc.total_turnover / acc.total_volume if acc.total_volume > 0 else acc.close

        bid_ask_avg = acc.bid_ask_spread_sum / acc.tick_count if acc.tick_count > 0 else 0.0



        bar_start = acc.bar_start_ts or current_ts

        high_offset = 0

        low_offset = 0

        if acc.high_ts is not None:

            high_offset = int((acc.high_ts.astype('datetime64[ms]').astype(np.int64)

                               - bar_start.astype('datetime64[ms]').astype(np.int64)))

        if acc.low_ts is not None:

            low_offset = int((acc.low_ts.astype('datetime64[ms]').astype(np.int64)

                              - bar_start.astype('datetime64[ms]').astype(np.int64)))



        bar = EnhancedBar(

            timestamp=bar_start,

            symbol=self._symbol,

            open=acc.open,

            high=acc.high,

            low=acc.low,

            close=acc.close,

            volume=acc.total_volume,

            vwap=vwap,

            high_time_offset_ms=high_offset,

            low_time_offset_ms=low_offset,

            volume_at_high=acc.volume_at_high,

            volume_at_low=acc.volume_at_low,

            bid_ask_spread_avg=bid_ask_avg,

            open_interest=acc.open_interest,

            tick_count=acc.tick_count,

        )



        self._update_indicators(bar)

        self._bar_count += 1

        self._last_bar = bar

        self._all_bars.append(bar)



        if bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,

                                  ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):

            self._extreme_snapshots.append(bar)



        if self._on_bar_complete is not None:

            self._on_bar_complete(bar)



        self._tick_acc = _TickAccumulator()

        return bar



    def _update_indicators(self, bar: EnhancedBar) -> None:

        vwap = bar.vwap

        close = bar.close



        for period in self._ma_periods:

            self._vwap_buffers[period].append(vwap)

        self._close_buffer.append(close)

        self._high_buffer.append(bar.high)

        self._low_buffer.append(bar.low)



        prev_close = self._close_buffer[-2] if len(self._close_buffer) >= 2 else close

        prev_high = self._high_buffer[-2] if len(self._high_buffer) >= 2 else bar.high

        prev_low = self._low_buffer[-2] if len(self._low_buffer) >= 2 else bar.low

        tr = max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))

        self._tr_buffer.append(tr)



        price_vs_mas = {}

        price_ma_deviation_sigma = {}

        ma_curvatures = {}

        ma_values = {}



        atr14 = float(np.mean(list(self._tr_buffer))) if len(self._tr_buffer) > 0 else 1e-8

        bar.atr14 = atr14



        for period in self._ma_periods:

            buf = self._vwap_buffers[period]

            if len(buf) >= period:

                ma_val = float(np.mean(list(buf)))

                ma_values[period] = ma_val



                deviation = (vwap - ma_val) / atr14 if atr14 > 1e-8 else 0.0

                price_vs_mas[f"ma{period}"] = float(np.sign(vwap - ma_val))

                price_ma_deviation_sigma[f"ma{period}"] = float(deviation)



                if len(buf) >= period + 1:

                    prev_vals = list(buf)

                    if len(prev_vals) >= 3:

                        recent_ma = np.mean(prev_vals[-period:])

                        mid_ma = np.mean(prev_vals[-period - 1:-1])

                        far_ma = np.mean(prev_vals[-period - 2:-2]) if len(prev_vals) >= period + 2 else mid_ma

                        curvature = recent_ma - 2 * mid_ma + far_ma

                        ma_curvatures[f"ma{period}"] = float(curvature / atr14 if atr14 > 1e-8 else 0.0)

            else:

                ma_values[period] = vwap

                price_vs_mas[f"ma{period}"] = 0.0

                price_ma_deviation_sigma[f"ma{period}"] = 0.0

                ma_curvatures[f"ma{period}"] = 0.0



        bar.price_vs_mas = price_vs_mas

        bar.price_ma_deviation_sigma = price_ma_deviation_sigma

        bar.ma_curvatures = ma_curvatures



        bar.ma_alignment = self._classify_ma_alignment(ma_values)



        bar.extreme_region = self._detect_extreme_region(vwap)



    def _detect_extreme_region(self, current_price: float) -> ExtremeRegion:

        """

        å¨çº¿æå¼è¯å?â?ä»ä½¿ç¨å½åBaråä¹åçæ°æ®



        ç®æ³ï¼?
          1. 取最近extreme_window个Bar的high/low

          2. 如果当前价格接近局部高点（前quantile分位）→ NEAR_HIGH

          3. 如果当前价格接近局部低点（后quantile分位）→ NEAR_LOW

          4. é¢å¤æ£æ¥ï¼å¦æåç¦»åº¦è¶è¿?Ï â?EXTREME_HIGH/EXTREME_LOWï¼æç«¯è¡æï¼

        """

        if len(self._high_buffer) < 3:

            return ExtremeRegion.NORMAL



        recent_highs = list(self._high_buffer)[-self._extreme_window:]

        recent_lows = list(self._low_buffer)[-self._extreme_window:]



        local_max = max(recent_highs)

        local_min = min(recent_lows)

        price_range = local_max - local_min



        if price_range < 1e-8:

            return ExtremeRegion.NORMAL



        high_threshold = local_max - self._extreme_quantile * price_range

        low_threshold = local_min + self._extreme_quantile * price_range



        atr = self._last_bar.atr14 if self._last_bar and self._last_bar.atr14 > 0 else price_range * 0.02

        extreme_sigma = 3.0



        is_near_high = current_price >= high_threshold

        is_near_low = current_price <= low_threshold



        if is_near_high:

            deviation_sigma = (local_max - current_price) / atr if atr > 1e-8 else 0.0

            if deviation_sigma > extreme_sigma:

                return ExtremeRegion.EXTREME_HIGH

            return ExtremeRegion.NEAR_HIGH



        if is_near_low:

            deviation_sigma = (current_price - local_min) / atr if atr > 1e-8 else 0.0

            if deviation_sigma > extreme_sigma:

                return ExtremeRegion.EXTREME_LOW

            return ExtremeRegion.NEAR_LOW



        return ExtremeRegion.NORMAL



    def _classify_ma_alignment(self, ma_values: Dict[int, float]) -> MAAlignment:

        """

        åçº¿æåå½¢æåç±?


        多头排列：MA5 > MA10 > MA15 > MA30 > MA60

        空头排列：MA5 < MA10 < MA15 < MA30 < MA60

        æ¶æï¼ç­å¨æä¸é¿å¨æåçº¿å·®å¼ç¼©å°?
        ç¼ ç»ï¼åçº¿é¢ç¹äº¤å?
        """

        sorted_periods = sorted(ma_values.keys())

        if len(sorted_periods) < 3:

            return MAAlignment.TRANSITIONAL



        vals = [ma_values[p] for p in sorted_periods]



        bullish_count = sum(1 for i in range(len(vals) - 1) if vals[i] > vals[i + 1])

        bearish_count = sum(1 for i in range(len(vals) - 1) if vals[i] < vals[i + 1])

        n_pairs = len(vals) - 1



        if bullish_count == n_pairs:

            return MAAlignment.BULLISH

        if bearish_count == n_pairs:

            return MAAlignment.BEARISH



        if len(vals) >= 3:

            short_long_spread = abs(vals[0] - vals[-1])

            max_spread = max(abs(vals[i] - vals[j]) for i in range(len(vals)) for j in range(i + 1, len(vals)))

            if max_spread > 1e-8 and short_long_spread / max_spread < 0.3:

                return MAAlignment.CONVERGENT



        if bullish_count >= 1 and bearish_count >= 1:

            return MAAlignment.INTERTWINED



        return MAAlignment.TRANSITIONAL



    def get_extreme_snapshots(self) -> List[EnhancedBar]:

        return list(self._extreme_snapshots)



    def get_all_bars(self) -> List[EnhancedBar]:

        return list(self._all_bars)



    def flush(self) -> Optional[EnhancedBar]:

        """emit最后一个未完成的Bar"""

        if self._tick_acc.tick_count > 0:

            return self._emit_bar(np.datetime64('now'))

        return None



    def get_statistics(self) -> Dict[str, Any]:

        return {

            "symbol": self._symbol,

            "total_bars": self._bar_count,

            "extreme_bar_count": len(self._extreme_snapshots),

            "near_high_count": sum(1 for b in self._extreme_snapshots

                                   if b.extreme_region == ExtremeRegion.NEAR_HIGH),

            "near_low_count": sum(1 for b in self._extreme_snapshots

                                  if b.extreme_region == ExtremeRegion.NEAR_LOW),

            "extreme_high_count": sum(1 for b in self._extreme_snapshots

                                     if b.extreme_region == ExtremeRegion.EXTREME_HIGH),

            "extreme_low_count": sum(1 for b in self._extreme_snapshots

                                    if b.extreme_region == ExtremeRegion.EXTREME_LOW),

        }



    @staticmethod

    def build_from_preprocessed_df(

        df: Any,

        symbol: str,

        ma_periods: Optional[List[int]] = None,

        extreme_window: int = 20,

        extreme_quantile: float = 0.05,

    ) -> 'TurningPointMicroscope':

        """

        从预处理DataFrame（如。preprocess.py输出）构建拐点显微镜



        DataFrame需包含列：timestamp/minute, open, high, low, close, volume, vwap,

                           bid_ask_spread, open_interest, tick_count

        """

        import pandas as pd



        scope = TurningPointMicroscope(

            symbol=symbol,

            ma_periods=ma_periods,

            extreme_window=extreme_window,

            extreme_quantile=extreme_quantile,

        )



        ts_col = "minute" if "minute" in df.columns else "timestamp"



        for _, row in df.iterrows():

            ts = np.datetime64(row[ts_col])

            bar = EnhancedBar(

                timestamp=ts,

                symbol=symbol,

                open=float(row.get("open", 0)),

                high=float(row.get("high", 0)),

                low=float(row.get("low", 0)),

                close=float(row.get("close", 0)),

                volume=int(row.get("volume", 0)),

                vwap=float(row.get("vwap", row.get("close", 0))),

                high_time_offset_ms=0,

                low_time_offset_ms=0,

                volume_at_high=0.0,

                volume_at_low=0.0,

                bid_ask_spread_avg=float(row.get("bid_ask_spread", 0)),

                open_interest=int(row.get("open_interest", 0)),

                tick_count=int(row.get("tick_count", 0)),

            )

            scope._update_indicators(bar)

            scope._bar_count += 1

            scope._last_bar = bar

            scope._all_bars.append(bar)

            if bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,

                                      ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):

                scope._extreme_snapshots.append(bar)



        return scope





# ============================================================================

# å±æ¯æç¹æ è®°ï¼å resonance_turning_point_marker.pyï¼?
# ============================================================================

"""

周期共振拐点标记 (Resonance Turning Point Marker)



ç»åå¨æå±æ¯æ¨¡å(CycleResonanceModule)çååéè¾åºï¼?
æ è¯é¢æè½¬æç¹åå®éè½¬æç¹ï¼å½¢ææç¹å?ä¸?åçå®æ´æ è®°é¾ã?


æ ¸å¿è®¾è®¡ï¼?
  1. 预期转折点：周期共振相位转换时（蓄力→释放、释放→衰竭等）'
     标记"市场预期即将发生转折"，但价格尚未确认

  2. å®éè½¬æç¹ï¼EnhancedBarçæå¼åºå?+ å±æ¯æ¹åç¡®è®¤

     æ è®°"ä»·æ ¼å·²å®éåçè½¬æ?ï¼ä¸é¢æè½¬æç¹éå¯?
  3. 转折点分类：

     - è¶å¿å»¶ç»­åè½¬æï¼ç­å¨æåçº¿èµ°å¹³ä½é¿å¨æä»å»¶ç»­ â?è¶å¿ä¸­ç»§

     - è¶å¿åè½¬åè½¬æï¼åçº¿æ¶æ+ä»·æ ¼åç¦»åº¦â¥2Ï â?åè½¬åå

     - éè¡æå¼åè½¬æï¼æ··æ²ç¸ä½?ç¼ ç»æå â?éè¡åºé´è¾¹ç

  4. 预期-实际配对：评估共振模块的预测能力（预期→实际的时间差和方向一致性）

"""



import logging

from collections import deque

from dataclasses import dataclass, field, asdict

from enum import Enum

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING



if TYPE_CHECKING:

    from ..param_pool.optimization.cycle_sharpe import CycleResonanceOutput



import numpy as np





from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5



logger = get_logger(__name__)  # R9-5





class TurningPointType(Enum):

    EXPECTED_TREND_CONTINUATION = "预期权趋势延续"

    EXPECTED_TREND_REVERSAL = "预期权趋势反转"

    EXPECTED_OSCILLATION_EXTREME = "é¢æ_éè¡æå?"

    ACTUAL_TREND_CONTINUATION = "实际的趋势延续"

    ACTUAL_TREND_REVERSAL = "实际的趋势反转"

    ACTUAL_OSCILLATION_EXTREME = "å®é_éè¡æå?"





class _ResonancePhase(Enum):

    CHARGE = "蓄力"

    RELEASE = "释放"

    EXHAUST = "衰竭"

    CHAOS = "混沌"





@dataclass(slots=True)

class TurningPointRecord:

    timestamp: np.datetime64

    symbol: str

    tp_type: TurningPointType

    price: float

    phase: str

    directional_bias: float

    resonance_strength: float

    state_entropy: float

    ma_alignment: str

    price_deviation_sigma: float

    vwap: float

    extreme_region: str

    paired_with: Optional[str] = None

    confirmation_bars: int = 0

    confidence: float = 0.0

    metadata: Dict[str, Any] = field(default_factory=dict)



    @property

    def is_expected(self) -> bool:

        return self.tp_type.value.startswith("预期")



    @property

    def is_actual(self) -> bool:

        return self.tp_type.value.startswith("实际")



    @property

    def is_reversal(self) -> bool:

        return "反转" in self.tp_type.value



    @property

    def is_continuation(self) -> bool:

        return "延续" in self.tp_type.value



    def to_dict(self) -> Dict[str, Any]:

        d = asdict(self)

        d['tp_type'] = self.tp_type.value

        d['timestamp'] = str(self.timestamp)

        return d





@dataclass(slots=True)

class _ExpectedTP:

    record: TurningPointRecord

    created_bar_idx: int

    confirmed: bool = False

    confirmation_record: Optional[TurningPointRecord] = None





class ResonanceTurningPointMarker:

    """

    å¨æå±æ¯æç¹æ è®°å?


    å¨çº¿å¤çæµç¨ï¼?
      1. æ¯ä¸ªEnhancedBarå®æåè°ç?process_bar()

      2. æ£æµå¨æå±æ¯ç¸ä½è½¬æ?â?çæé¢æè½¬æç?
      3. æ£æµæå¼åºå?å±æ¯ç¡®è®¤ â?çæå®éè½¬æç?
      4. 尝试预期-实际配对

    """



    def __init__(

        self,

        symbol: str,

        confirmation_window_bars: int = 10,

        deviation_reversal_threshold: float = 2.0,

        deviation_continuation_threshold: float = 0.8,

        resonance_reversal_min_strength: float = 0.3,

        max_unconfirmed_expected: int = 50,

    ):

        self._symbol = symbol

        self._confirmation_window = confirmation_window_bars

        self._deviation_reversal_threshold = deviation_reversal_threshold

        self._deviation_continuation_threshold = deviation_continuation_threshold

        self._resonance_reversal_min = resonance_reversal_min_strength

        self._max_unconfirmed = max_unconfirmed_expected



        self._last_phase: Optional[_ResonancePhase] = None

        self._last_directional_bias: float = 0.0

        self._last_resonance_strength: float = 0.0

        self._last_state_entropy: float = 0.0

        self._bias_direction_history: deque = deque(maxlen=20)



        self._unconfirmed_expected: List[_ExpectedTP] = []

        self._all_turning_points: List[TurningPointRecord] = []

        self._bar_count = 0



    def process_bar(

        self,

        bar: EnhancedBar,

        cr_output: Optional[Any] = None,

    ) -> List[TurningPointRecord]:

        """

        å¤çä¸ä¸ªå¢å¼ºBarï¼æ£æµå¹¶è®°å½è½¬æç?


        Args:

            bar: EnhancedBarï¼å«æå¼åºåååçº¿ä½ç½®ï¼?
            cr_output: CycleResonanceOutputï¼å«ååéï¼ï¼å¯é?
                       è¥ä¸ºNoneåéçº§ä¸ºçº¯ä»·æ ¼æå¼æ£æµ?


        Returns:

            æ¬Baræ°äº§ççè½¬æç¹åè¡?
        """

        new_tps = []



        phase = None

        directional_bias = 0.0

        resonance_strength = 0.0

        state_entropy = 0.5



        if cr_output is not None:

            phase = _ResonancePhase(cr_output.phase.value)

            directional_bias = cr_output.directional_bias

            resonance_strength = cr_output.resonance_strength

            state_entropy = cr_output.state_entropy



        if self._last_phase is not None and phase is not None:

            if phase != self._last_phase:

                expected_tp = self._detect_expected_turning_point(

                    bar, phase, directional_bias, resonance_strength, state_entropy,

                )

                if expected_tp is not None:

                    new_tps.append(expected_tp)

                    self._unconfirmed_expected.append(

                        _ExpectedTP(record=expected_tp, created_bar_idx=self._bar_count)

                    )



        if bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,

                                  ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):

            actual_tp = self._detect_actual_turning_point(

                bar, phase, directional_bias, resonance_strength, state_entropy,

            )

            if actual_tp is not None:

                new_tps.append(actual_tp)

                self._try_pair_expected(actual_tp)



        self._last_phase = phase

        self._last_directional_bias = directional_bias

        self._last_resonance_strength = resonance_strength

        self._last_state_entropy = state_entropy

        self._bias_direction_history.append(np.sign(directional_bias))

        self._bar_count += 1



        self._expire_unconfirmed()



        self._all_turning_points.extend(new_tps)

        return new_tps



    def _detect_expected_turning_point(

        self,

        bar: EnhancedBar,

        new_phase: _ResonancePhase,

        bias: float,

        strength: float,

        entropy: float,

    ) -> Optional[TurningPointRecord]:

        """

        相位转换时检测预期转折点



        é»è¾ï¼?
          - èåâéæ¾ï¼é¢æè¶å¿çåï¼å»¶ç»­æåè½¬åå³äºbiasæ¹åï¼?
          - 释放→衰竭：预期趋势即将结束

          - Xâæ··æ²ï¼é¢æå¸åºå¤±åºï¼ä¸å¯äº¤æ?
          - 混沌→蓄力：预期市场将进入新周期

        """

        max_dev = 0.0

        for v in bar.price_ma_deviation_sigma.values():

            max_dev = max(max_dev, abs(v))



        tp_type = None

        confidence = 0.0



        if new_phase == _ResonancePhase.RELEASE:

            if abs(bias) > 0.3 and strength > 0.4:

                if max_dev > self._deviation_reversal_threshold:

                    tp_type = TurningPointType.EXPECTED_TREND_REVERSAL

                    confidence = min(1.0, strength * abs(bias) * 0.8)

                else:

                    tp_type = TurningPointType.EXPECTED_TREND_CONTINUATION

                    confidence = min(1.0, strength * 0.6)

        elif new_phase == _ResonancePhase.EXHAUST:

            tp_type = TurningPointType.EXPECTED_TREND_REVERSAL

            confidence = min(1.0, (1.0 - entropy) * strength * 0.7)

        elif new_phase == _ResonancePhase.CHAOS:

            tp_type = TurningPointType.EXPECTED_OSCILLATION_EXTREME

            confidence = min(1.0, entropy * 0.5)

        elif new_phase == _ResonancePhase.CHARGE:

            tp_type = TurningPointType.EXPECTED_OSCILLATION_EXTREME

            confidence = min(1.0, (1.0 - entropy) * 0.4)



        if tp_type is None or confidence < 0.1:

            return None



        return TurningPointRecord(

            timestamp=bar.timestamp,

            symbol=self._symbol,

            tp_type=tp_type,

            price=bar.vwap,

            phase=new_phase.value,

            directional_bias=bias,

            resonance_strength=strength,

            state_entropy=entropy,

            ma_alignment=bar.ma_alignment.value,

            price_deviation_sigma=max_dev,

            vwap=bar.vwap,

            extreme_region=bar.extreme_region.value,

            confidence=confidence,

        )



    def _detect_actual_turning_point(

        self,

        bar: EnhancedBar,

        phase: Optional[_ResonancePhase],

        bias: float,

        strength: float,

        entropy: float,

    ) -> Optional[TurningPointRecord]:

        """

        æå¼åºå?å±æ¯ç¡®è®¤ â?å®éè½¬æç?


        åç±»é»è¾ï¼?
          - åçº¿æ¶æ + ä»·æ ¼åç¦»åº¦â¥2Ï + å±æ¯å¼ºåº¦>0.3 â?åè½¬

          - ç­å¨æèµ°å¹?+ é¿å¨æå»¶ç»?â?å»¶ç»­

          - æ··æ²ç¸ä½ + ç¼ ç»æå â?éè¡æå?
        """

        is_high = bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.EXTREME_HIGH)

        is_low = bar.extreme_region in (ExtremeRegion.NEAR_LOW, ExtremeRegion.EXTREME_LOW)



        max_dev = 0.0

        for v in bar.price_ma_deviation_sigma.values():

            max_dev = max(max_dev, abs(v))



        tp_type = None

        confidence = 0.0



        if bar.ma_alignment in (MAAlignment.CONVERGENT, MAAlignment.INTERTWINED):

            if max_dev >= self._deviation_reversal_threshold and strength > self._resonance_reversal_min:

                tp_type = TurningPointType.ACTUAL_TREND_REVERSAL

                confidence = min(1.0, max_dev / 4.0 * strength * 0.9)

            elif phase == _ResonancePhase.CHAOS:

                tp_type = TurningPointType.ACTUAL_OSCILLATION_EXTREME

                confidence = min(1.0, entropy * 0.7)

            else:

                tp_type = TurningPointType.ACTUAL_OSCILLATION_EXTREME

                confidence = 0.3



        elif bar.ma_alignment == MAAlignment.BULLISH:

            if is_high and max_dev >= self._deviation_reversal_threshold:

                tp_type = TurningPointType.ACTUAL_TREND_REVERSAL

                confidence = min(1.0, max_dev / 3.0 * 0.8)

            elif is_low:

                tp_type = TurningPointType.ACTUAL_TREND_CONTINUATION

                confidence = min(1.0, strength * 0.6)



        elif bar.ma_alignment == MAAlignment.BEARISH:

            if is_low and max_dev >= self._deviation_reversal_threshold:

                tp_type = TurningPointType.ACTUAL_TREND_REVERSAL

                confidence = min(1.0, max_dev / 3.0 * 0.8)

            elif is_high:

                tp_type = TurningPointType.ACTUAL_TREND_CONTINUATION

                confidence = min(1.0, strength * 0.6)



        if tp_type is None or confidence < 0.1:

            return None



        direction = "high" if is_high else ("low" if is_low else "neutral")



        return TurningPointRecord(

            timestamp=bar.timestamp,

            symbol=self._symbol,

            tp_type=tp_type,

            price=bar.vwap,

            phase=phase.value if phase else "UNKNOWN",

            directional_bias=bias,

            resonance_strength=strength,

            state_entropy=entropy,

            ma_alignment=bar.ma_alignment.value,

            price_deviation_sigma=max_dev,

            vwap=bar.vwap,

            extreme_region=bar.extreme_region.value,

            confidence=confidence,

            metadata={"direction": direction, "bar_extreme": bar.extreme_region.value},

        )



    def _try_pair_expected(self, actual_tp: TurningPointRecord) -> None:

        """

        尝试将实际转折点与最近的未确认预期转折点配对



        éå¯¹æ¡ä»¶ï¼?
          1. é¢æä¸å®éç±»åæ¹åä¸è´ï¼é½é¢æåè½¬âå®éåè½¬ï¼?
          2. æ¶é´å·®å¨ç¡®è®¤çªå£å?
          3. 方向一致（同为高点或低点）'
        """

        best_match = None

        best_idx = -1

        best_distance = float('inf')



        for i, exp in enumerate(self._unconfirmed_expected):

            if exp.confirmed:

                continue



            bars_elapsed = self._bar_count - exp.created_bar_idx

            if bars_elapsed > self._confirmation_window:

                continue



            type_compatible = self._are_types_compatible(exp.record.tp_type, actual_tp.tp_type)

            if not type_compatible:

                continue



            if bars_elapsed < best_distance:

                best_distance = bars_elapsed

                best_match = exp

                best_idx = i



        if best_match is not None:

            best_match.confirmed = True

            best_match.confirmation_record = actual_tp

            best_match.record.paired_with = id(actual_tp)

            actual_tp.paired_with = id(best_match.record)

            actual_tp.confirmation_bars = best_distance



    def _are_types_compatible(self, expected: TurningPointType, actual: TurningPointType) -> bool:

        if "反转" in expected.value and "反转" in actual.value:

            return True

        if "延续" in expected.value and "延续" in actual.value:

            return True

        if "震荡" in expected.value and "震荡" in actual.value:

            return True

        return False



    def _expire_unconfirmed(self) -> None:

        self._unconfirmed_expected = [

            exp for exp in self._unconfirmed_expected

            if not exp.confirmed and (self._bar_count - exp.created_bar_idx) <= self._confirmation_window

        ]

        if len(self._unconfirmed_expected) > self._max_unconfirmed:

            self._unconfirmed_expected = self._unconfirmed_expected[-self._max_unconfirmed:]



    def get_all_turning_points(self) -> List[TurningPointRecord]:

        return list(self._all_turning_points)



    def get_expected_turning_points(self) -> List[TurningPointRecord]:

        return [tp for tp in self._all_turning_points if tp.is_expected]



    def get_actual_turning_points(self) -> List[TurningPointRecord]:

        return [tp for tp in self._all_turning_points if tp.is_actual]



    def get_paired_turning_points(self) -> List[Tuple[TurningPointRecord, TurningPointRecord]]:

        pairs = []

        expected_map = {}

        for exp in self._unconfirmed_expected:

            if exp.confirmed and exp.confirmation_record is not None:

                pairs.append((exp.record, exp.confirmation_record))

        return pairs



    def get_prediction_accuracy(self) -> Dict[str, Any]:

        """

        å¨æå±æ¯æ¨¡åçæç¹é¢æµè½åè¯ä¼?


        Returns:

            包含命中率、平均确认时间、方向一致性等统计

        """

        expected = self.get_expected_turning_points()

        paired = self.get_paired_turning_points()



        total_expected = len(expected)

        total_paired = len(paired)

        hit_rate = total_paired / total_expected if total_expected > 0 else 0.0



        confirmation_bars = [p[1].confirmation_bars for p in paired]

        avg_confirmation = float(np.mean(confirmation_bars)) if confirmation_bars else 0.0



        direction_match = 0

        direction_total = 0

        for exp_tp, act_tp in paired:

            exp_is_reversal = exp_tp.is_reversal

            act_is_reversal = act_tp.is_reversal

            if exp_is_reversal == act_is_reversal:

                direction_match += 1

            direction_total += 1



        direction_accuracy = direction_match / direction_total if direction_total > 0 else 0.0



        return {

            "total_expected": total_expected,

            "total_actual": len(self.get_actual_turning_points()),

            "total_paired": total_paired,

            "hit_rate": hit_rate,

            "avg_confirmation_bars": avg_confirmation,

            "direction_accuracy": direction_accuracy,

        }

