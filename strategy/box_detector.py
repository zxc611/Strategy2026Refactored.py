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
import traceback
from bisect import bisect_left, insort
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from infra._helpers import get_logger  # R9-5
from infra.shared_utils import CHINA_TZ as _CHINA_TZ

logger = get_logger(__name__)  # R9-5

# FIX-D1-ECO-INJECTION-V4-20260731: 删除模块级_d1_get_kline/_d1_market_center/set_d1_kline_provider
# 根因: V3版本通过模块级全局变量注入,与StrategyEcosystem.get_kline形成双通道,违反原则2(四唯一)
# 修复: lifecycle_bind._do_bind_platform_apis将get_kline/_runtime_market_center注入
#        StrategyEcosystem单例, 本模块仅从eco获取(单一渠道)


class BoxType(Enum):
    """箱体类型枚举 — S3/S4策略箱体标准化
    
    日内交易(S3策略): 至少3根日K线, 三高点/三低点差异<=10个最小变动单位 → 小箱形(INTRADAY_SMALL)
    隔夜交易(S4策略): 至少3根周K线, 三高点/三低点差异<=10个最小变动单位 → 中箱形(OVERNIGHT_MEDIUM)
    
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
    box_width_max_pct: float = 1.0  # FIX-BOX-DUAL-WIDTH-20260730: 5%→1%,三高点/三低点百分比宽度上限(期货)
    iv_history_min_for_percentile: int = 20  # FIX-S3S4-9: IV历史不足此数时返回50.0(中位),避免冷启动期误过滤

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

    # FIX-DEL-TICKBOX-V2-CLEANUP-V4-20260729: 删除已废弃tick级箱体相关常量
    # 删除: ADX_DEFAULT_VALUE, ADX_MULTIPLIER (被删除的_compute_adx_simplified使用)
    #       WIDTH_SCORE_WEIGHT, ADX_SCORE_WEIGHT, BOUNCE_SCORE_WEIGHT, PLR_SCORE_WEIGHT (旧tick级detect_box评分用)
    # 保留: BOX_HISTORY_MAXLEN, FLOW_IMBALANCE_THRESHOLD, CVD_SLOPE_THRESHOLD,
    #       PRICE_SCORE_WEIGHT, RESONANCE_SCORE_WEIGHT, IV_SCORE_WEIGHT, FLOW_SCORE_WEIGHT,
    #       BOTTOM_THRESHOLD_RATIO, TOP_THRESHOLD_RATIO (classify_extreme_state仍在使用)
    BOX_HISTORY_MAXLEN = 100
    FLOW_IMBALANCE_THRESHOLD = 0.2
    CVD_SLOPE_THRESHOLD = 0.01
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
        # FIX-DEL-TICKBOX-V2-20260729: 彻底删除tick级箱体(用户明确要求"tick级箱体已经明确要求删除,不发保留")
        # 删除参数: min_box_bars, adx_period, adx_threshold, bounce_tolerance_pct, box_gain_ratio, plr_normalization_base
        # 删除状态: _min_box_bars, _price_highs, _price_lows, _price_closes, _volumes, _timestamps,
        #          _bounce_at_upper, _bounce_at_lower, _box_gain_ratio, _plr_normalization_base,
        #          _adx_period, _adx_threshold, _bounce_tolerance_pct
        # 保留: _current_box(K线箱体填充), _iv_history(update_iv填充), _kline_box_cache(K线箱体缓存)
        #       classify_extreme_state(消费K线箱体+IV), check_kline_box_precondition/detect_kline_box(K线箱体主链)
        iv_history_maxlen: int = 1000,
    ):
        self._lock = threading.RLock()
        self._params = params or BoxStrategyParams()
        self._lookback_bars = lookback_bars  # 保留用于兼容日志,不再用于tick级deque

        self._current_box: Optional[BoxProfile] = None
        self._box_history: deque = deque(maxlen=self.BOX_HISTORY_MAXLEN)
        self._extreme_state: Optional[ExtremeState] = None

        self._iv_history: deque = deque(maxlen=iv_history_maxlen)
        self._iv_sorted: List[float] = []

        self._stats = {
            'bars_processed': 0,
            'boxes_detected': 0,
            'bottom_extremes': 0,
            'top_extremes': 0,
            'iv_filtered': 0,
            'flow_exhaustion_confirmed': 0,
            'tradeable_signals': 0,
            'false_breakout_filtered': 0,
        }

        # [FIX-20260712-S3] 假突破过滤模块 — 上策H-Rev: 假突破过滤
        self._breakout_tracker: deque = deque(maxlen=20)
        self._false_breakout_lookback_sec = 120.0
        self._false_breakout_retrace_ratio = 0.50

        self._box_id_counter: int = 0

        # FIX-56: per-instrument信号冷却追踪
        self._last_signal_time: Dict[str, float] = {}

        # K线箱体实例变量 — S3/S4策略箱体标准化(K线箱体为唯一箱体来源)
        # S3日内策略: 至少3根日K线, 三高点/三低点差异<=10*tick_size (INTRADAY_SMALL)
        # S4隔夜策略: 至少3根周K线, 三高点/三低点差异<=10*tick_size (OVERNIGHT_MEDIUM)
        # FIX-S3-CACHE-20260728: per-instrument字典缓存
        self._kline_box_cache: Dict[str, Tuple[Optional[KLineBoxProfile], Optional[KLineBoxProfile], float]] = {}
        self._kline_box_cache_ttl_sec: float = 300.0  # K线箱体缓存TTL(5分钟)
        self._kline_min_bars_daily: int = 3           # S3日内: 日K箱体最低K线数
        self._kline_min_bars_weekly: int = 3          # S4隔夜: 周K箱体最低K线数
        # FIX-BOX-DUAL-WIDTH-20260730: 参数化百分比宽度上限,默认1%(用户要求:5%太大→1%)
        # 从BoxStrategyParams.box_width_max_pct读取,支持YAML参数池配置
        self._kline_box_width_max_pct: float = getattr(self._params, 'box_width_max_pct', 1.0) or 1.0

        # FIX-DEL-TICKBOX-V2-20260729: _kline_box_daily/_weekly 实例属性初始化(供detect_box/classify_extreme_state读取)
        # 旧bug: 第477/480/662/665行引用 self._kline_box_daily/weekly 但__init__未定义 → AttributeError
        # 修复: 初始化为None, 由detect_kline_box/check_kline_box_precondition更新
        self._kline_box_daily: Optional[KLineBoxProfile] = None
        self._kline_box_weekly: Optional[KLineBoxProfile] = None

        logger.info("[BoxDetector] 初始化完成(tick级箱体已删除), iv_history=%d, kline_min_daily=%d, kline_min_weekly=%d",
                     iv_history_maxlen, self._kline_min_bars_daily, self._kline_min_bars_weekly)

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
        # FIX-DEL-TICKBOX-V2-20260729: tick级箱体已删除(用户明确要求)
        # 旧实现: 追加到 _price_highs/_price_lows/_price_closes/_volumes/_timestamps
        # 新实现: no-op(tick级状态已删除); K线箱体由detect_kline_box从DataService获取
        # 保留接口兼容(box_spring_detector.update_box注释仍调用, 但实际已pass)
        # 仍更新IV历史(classify_extreme_state消费IV百分位)
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

    # FIX-DEL-TICKBOX-V2-CLEANUP-V4-20260729: 删除死代码 _compute_adx_simplified 和 _find_support_resistance
    # 这两个@staticmethod是旧tick级箱体detect_box()的辅助方法, tick级箱体已彻底删除后无任何调用方
    # 保留为死代码违反"非半拉子工程"和"彻底清零"原则, 故删除
    # 历史参考: docs/audit/模块功能详细报告.md L149-150 仍保留对这两个方法的描述

    def detect_box(self) -> BoxProfile:
        # FIX-DEL-TICKBOX-V2-20260729: tick级箱体已删除(用户明确要求)
        # 旧实现: 基于 _price_highs/_price_lows/_min_box_bars/adx/bounce 计算tick级箱体
        # 新实现: 返回空BoxProfile(is_box=False); K线箱体由detect_kline_box/check_kline_box_precondition处理
        # _current_box由_update_current_box_from_kline()独占更新(K线箱体边界)
        # 保留接口兼容(无外部调用方, 但保留以防遗漏)
        with self._lock:
            now_str = datetime.now(_CHINA_TZ).isoformat()
            self._box_id_counter += 1
            box_id = f"BOX-{self._box_id_counter:06d}"
            profile = BoxProfile(box_id=box_id, timestamp=now_str)
            # K线箱体字段注入 — 使用缓存的K线箱体状态
            if self._kline_box_daily is not None and self._kline_box_daily.is_valid:
                profile.kline_box_confirmed = True
                profile.kline_box_type = 'INTRADAY_SMALL'
            elif self._kline_box_weekly is not None and self._kline_box_weekly.is_valid:
                profile.kline_box_confirmed = True
                profile.kline_box_type = 'OVERNIGHT_MEDIUM'
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
            # FIX-S3-COLDSTART-20260803: IV冷启动期(iv_sorted_list为空/不足→percentile=0.0)允许降级
            # 根因: 重启后iv_sorted_list为空→percentile=0.0<50→全部拦截→S3冷启动期零下单(持续数十分钟)
            #   iv_percentile=0.0有两层含义: (1)iv_sorted_list为空/不足=数据不可用→应降级通过
            #                                 (2)IV极低=真实百分位低→应拦截
            #   区分方法: iv_sorted_list长度<min_history=数据不可用→降级
            # 修复: 数据不可用时iv_filter_passed=True(降级), IV有历史数据但percentile低时仍拦截
            # 安全: 置信度评分中iv_score=0(降级时)拉低confidence,需其他维度补足,不会产生低质量信号
            _iv_history_available = len(self._iv_sorted) >= self._params.iv_history_min_for_percentile
            if current_iv <= 0:
                iv_filter_passed = False  # IV=0: 数据不可用,降级通过(见下方覆盖)
                if not _iv_history_available:
                    iv_filter_passed = True  # IV历史不足: 冷启动降级,避免阻断所有信号
            else:
                iv_filter_passed = iv_percentile >= self._params.iv_percentile_min
                if not iv_filter_passed and not _iv_history_available:
                    iv_filter_passed = True  # IV历史不足: 冷启动降级
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

            # FIX-S3-3: 不可交易诊断日志（WARNING级别写入signals.jsonl）
            # FIX-S3-LOG-20260731: 修正日志格式, 区分"confidence不足"和"IV filter拦截"
            # 原bug: confidence=0.687≥0.600但日志说"confidence不足"→误导排查方向
            # 实际: tradeable=False可能是confidence不足 OR iv_filter未通过 OR 无extreme_type
            if not tradeable and extreme_type:
                _reason = ''
                if confidence < self._params.min_extreme_confidence:
                    _reason = 'confidence不足'
                elif not iv_filter_passed:
                    _reason = 'IV filter拦截'
                else:
                    _reason = '其他条件不满足'
                logger.warning(
                    "[S3-BOX] %s: inst=%s confidence=%.3f(%.3f) iv_passed=%s iv_pct=%.1f "
                    "(extreme_type=%s price_s=%.3f res_s=%.3f iv_s=%.3f flow_s=%.3f flow_exh=%s)",
                    _reason, instrument_id, confidence, self._params.min_extreme_confidence,
                    iv_filter_passed, iv_percentile,
                    extreme_type, price_score, resonance_score, iv_score, flow_score, flow_exhaustion
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
    def compute_iv_percentile(iv_value: float, iv_sorted_list: List[float], min_history: int = 20) -> float:
        if not iv_sorted_list or iv_value <= 0:
            return 0.0  # FIX-S3S4-8-FIX-20260731: 数据不可用返回0.0(fail-closed), 不再合成50.0放行(原则7: 禁用虚假数据)
        if len(iv_sorted_list) < min_history:
            return 0.0  # FIX-S3S4-9-FIX-20260731: IV历史不足返回0.0(fail-closed), 冷启动期不交易(原则7: 禁用虚假数据)
        count_below = bisect_left(iv_sorted_list, iv_value)
        return count_below / len(iv_sorted_list) * 100.0

    @staticmethod
    def estimate_plr(box_height: float, avg_loss: float, box_gain_ratio: float = 0.5) -> float:
        if box_height < 1e-10 or avg_loss < 1e-10:
            return 0.0
        potential_gain = box_height * box_gain_ratio
        return potential_gain / avg_loss

    def _compute_iv_percentile(self, current_iv: float) -> float:
        return BoxDetector.compute_iv_percentile(
            current_iv, self._iv_sorted, self._params.iv_history_min_for_percentile)

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
    # S3日内策略: 至少3根日K线, 三高点/三低点差异<=10个最小变动单位 → 小箱形(INTRADAY_SMALL)
    # S4隔夜策略: 至少3根周K线, 三高点/三低点差异<=10个最小变动单位 → 中箱形(OVERNIGHT_MEDIUM)
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
        width_max_pct: float = 1.0,  # FIX-BOX-DUAL-WIDTH-20260730: 5%→1%,参数化
        box_type: BoxType = BoxType.INTRADAY_SMALL,
        instrument_id: str = '',  # FIX-S4-DIAG-FIX-20260803: 补充instrument_id参数(原BOX-DIAG日志引用未定义变量导致NameError)
        # FIX-S3S4-TICKS-TOLERANCE-V2-20260729: 重写为"三高点/三低点内部差异"判定
        # 用户2026-07-29澄清(关键):
        #   "高\低点差异在10个最小变动单位内"是指【三高点内部差异】和【三低点内部差异】,
        #    不是指箱体宽度(箱顶-箱底)或单根K线的(high-low)。
        # 即: 取最近 min_bars 根K线, 提取它们的high组成"高点集合", low组成"低点集合",
        #     要求 max(高点集合)-min(高点集合) <= ticks_tolerance*tick_size (三高点内部差异)
        #     且 max(低点集合)-min(低点集合) <= ticks_tolerance*tick_size (三低点内部差异)
        # 箱体上沿 = 高点集合均值; 箱体下沿 = 低点集合均值
        tick_size: float = 0.0,
        ticks_tolerance: int = 10,
    ) -> KLineBoxProfile:
        """从K线列表中检测箱体 (FIX-S3S4-TICKS-TOLERANCE-V2-20260729 重写版)

        算法：
        1. 取最近 min_bars 根K线作为箱体构成K线
        2. 提取这min_bars根K线的high组成高点集合, low组成低点集合
        3. 判定(用户明确要求"三高点/三低点差异在10个最小变动单位内"):
           - 三高点内部差异 = max(highs) - min(highs) <= ticks_tolerance * tick_size
           - 三低点内部差异 = max(lows) - min(lows)   <= ticks_tolerance * tick_size
           - 两条件都满足 → is_valid=True (高低点占相近, 形成箱体)
        4. 箱体上沿 = 高点集合均值; 箱体下沿 = 低点集合均值
        5. tick_size>0时启用ticks判定(主判定); tick_size=0时退化为百分比宽度判定(向后兼容)
        6. bars < min_bars 时 fail-closed 返回 is_valid=False, upper=0, lower=0

        Args:
            bars: K线列表，每项包含 high, low, close, timestamp
            min_bars: 最少K线数（日K=3，周K=3）
            width_max_pct: 百分比宽度上限(tick_size=0时使用的兼容判定)
            box_type: 箱体类型(INTRADAY_SMALL/OVERNIGHT_MEDIUM)
            tick_size: 品种最小变动价位(>0时启用三高点/三低点差异判定)
            ticks_tolerance: tick容差倍数(默认10, 即"10个最小变动单位以内")

        Returns:
            KLineBoxProfile 箱体轮廓
        """
        # FIX-S3S4-MINBARS-3-V2-20260729: bars<min_bars时fail-closed
        # 用户要求: bars=1时返回 upper=0.00, lower=0.00, valid=False (数据不足不开仓)
        if len(bars) < min_bars:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(bars),
                is_valid=False,
            )

        # FIX-S3S4-MINBARS-3-V2-20260729: 取最近 min_bars 根K线(严格取min_bars根, 非min_bars*2)
        # 用户原意: S3日内3根日K, S4隔夜3根周K — 就是3根, 不是6根
        recent = bars[-min_bars:]

        # 提取三高点集合和三低点集合
        highs = [float(b.get('high', 0.0)) for b in recent if float(b.get('high', 0.0)) > 0]
        lows = [float(b.get('low', 0.0)) for b in recent if float(b.get('low', 0.0)) > 0]
        closes = [float(b.get('close', 0.0)) for b in recent if float(b.get('close', 0.0)) > 0]

        if len(highs) < min_bars or len(lows) < min_bars or not closes:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=min(len(highs), len(lows)),
                is_valid=False,
            )

        recent_close = closes[-1]
        if recent_close < 1e-10:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(recent),
                is_valid=False,
            )

        # FIX-S3S4-TICKS-TOLERANCE-V2-20260729: 三高点/三低点内部差异
        highs_max, highs_min = max(highs), min(highs)
        lows_max, lows_min = max(lows), min(lows)
        highs_spread = highs_max - highs_min   # 三高点内部差异
        lows_spread = lows_max - lows_min       # 三低点内部差异

        # 箱体上沿 = 高点集合均值; 箱体下沿 = 低点集合均值
        box_upper = sum(highs) / len(highs)
        box_lower = sum(lows) / len(lows)

        if box_upper <= box_lower or box_lower <= 0:
            return KLineBoxProfile(
                box_type=box_type,
                bar_count=len(recent),
                is_valid=False,
            )

        width_pct = (box_upper - box_lower) / recent_close * 100.0

        # FIX-BOX-SPREAD-PCT-20260730: 1%阈值是三高点(三低点)差异百分比,不是箱体宽度百分比
        # 用户明确纠正: "1%是三高点（三低点）之间的差异，不是三高点到三低点的宽度"
        #   - 三高点差异百分比 = (max(highs)-min(highs)) / close * 100 → 通常0.1-0.5%
        #   - 箱体宽度百分比 = (avg(highs)-avg(lows)) / close * 100 → 通常1-3%
        # 旧BUG: width_pct_ok用箱体宽度百分比(~1.54%)与1%比较→永远REJECT
        # 修复: 1%阈值应用于三高点差异百分比AND三低点差异百分比(两者都<=1%才通过)
        #   IH2608实证: 三高点差异4点/2928=0.14% < 1% → PASS(旧代码错误REJECT)
        highs_spread_pct = highs_spread / recent_close * 100.0  # 三高点差异百分比
        lows_spread_pct = lows_spread / recent_close * 100.0    # 三低点差异百分比

        # FIX-S3S4-TICKS-TOLERANCE-V2-20260729: 双判定
        # 判定1(tick_size>0): 三高点差异<=10*tick_size 且 三低点差异<=10*tick_size (绝对值)
        # 判定2: 三高点差异百分比<=width_max_pct 且 三低点差异百分比<=width_max_pct (百分比)
        # OR逻辑: 任一判定通过即is_valid=True
        if tick_size > 0.0:
            ticks_threshold = ticks_tolerance * tick_size
            highs_close = highs_spread <= ticks_threshold   # 三高点相近(绝对值)
            lows_close = lows_spread <= ticks_threshold      # 三低点相近(绝对值)
            ticks_ok = highs_close and lows_close
            # FIX-BOX-SPREAD-PCT-20260730: 用三高点/三低点差异百分比,不是箱体宽度百分比
            spread_pct_ok = highs_spread_pct <= width_max_pct and lows_spread_pct <= width_max_pct
        else:
            # tick_size=0(未读取到品种规格): 退化为三高点/三低点差异百分比判定
            ticks_ok = False
            spread_pct_ok = highs_spread_pct <= width_max_pct and lows_spread_pct <= width_max_pct

        is_valid = ticks_ok or spread_pct_ok

        # confirming_bars: 计算多少根K线的高低点贴近箱体上下沿(用于confidence)
        tolerance = (box_upper - box_lower) * 0.1 if (box_upper - box_lower) > 0 else tick_size
        confirming_bars = 0
        for b in recent:
            h = float(b.get('high', 0.0))
            l = float(b.get('low', 0.0))
            if h <= 0 or l <= 0:
                continue
            if abs(h - box_upper) <= tolerance or abs(l - box_lower) <= tolerance:
                confirming_bars += 1
        # 至少min_bars根K线构成箱体(用户要求3根)
        if confirming_bars < min_bars:
            confirming_bars = len(recent)  # 三高点/三低点本身即构成箱体

        confidence = 0.0
        if is_valid:
            if tick_size > 0.0 and ticks_threshold > 0:
                # ticks判定模式: 紧凑度得分 = 1 - max(高点差异, 低点差异) / threshold
                compact_ratio = max(highs_spread, lows_spread) / ticks_threshold
                width_score = max(0.0, 1.0 - compact_ratio)
            else:
                # FIX-BOX-SPREAD-PCT-20260730: confidence与is_valid一致,用spread_pct非width_pct
                _max_spread_pct = max(highs_spread_pct, lows_spread_pct)
                width_score = max(0.0, 1.0 - _max_spread_pct / width_max_pct) if width_max_pct > 0 else 0.0
            bar_score = min(1.0, confirming_bars / (min_bars * 2))
            confidence = 0.4 * width_score + 0.6 * bar_score

        # FIX-S4-DIAG-20260803: 增加详细诊断日志,输出三高点/三低点差异百分比
        # 用户要求: 需确认哪些品种三高点差异≤1% AND 三低点差异≤1%
        _diag_valid = "✓" if is_valid else "✗"
        _diag_tick = f"tick_ok={ticks_ok}" if tick_size > 0 else "no_tick"
        logger.info(
            "[BOX-DIAG] inst=%s box_type=%s %s valid=%s highs=[%s] lows=[%s] "
            "highs_spread=%.4f(%.4f%%) lows_spread=%.4f(%.4f%%) box_width=%.4f(%.4f%%) "
            "tick_size=%.4f %s spread_pct_ok=%s",
            instrument_id, box_type.name, _diag_valid, is_valid,
            ",".join(f"{h:.2f}" for h in highs), ",".join(f"{l:.2f}" for l in lows),
            highs_spread, highs_spread_pct, lows_spread, lows_spread_pct,
            box_upper - box_lower, width_pct,
            tick_size, _diag_tick, spread_pct_ok,
        )

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
        2. DataService.get_symbol_daily_ohlc查询(symbol_daily_aggregates)
        3. ticks_raw表聚合(FIX-S3-KLINE-TICKSRAW-20260728: 第四层fallback)
        4. 内部tick缓存降采样(最后降级方案)

        Args:
            instrument_id: 合约ID
            daily_bars: 日K线数据(外部传入)，None则自动获取
            force: 是否强制重新计算(忽略缓存TTL)

        Returns:
            (daily_box, weekly_box) 日K小箱体和周K中箱体
        """
        with self._lock:
            now = time.time()
            # FIX-S3-CACHE-20260728: per-instrument缓存命中检查
            if not force and instrument_id in self._kline_box_cache:
                _cached_daily, _cached_weekly, _cached_ts = self._kline_box_cache[instrument_id]
                if (now - _cached_ts) < self._kline_box_cache_ttl_sec and _cached_daily is not None:
                    # FIX-DEL-TICKBOX-V2-20260729: 缓存命中时也需更新实例属性
                    # 旧bug: 缓存命中直接return, 不更新_kline_box_daily/weekly
                    #   → detect_box/classify_extreme_state读取旧值(可能来自其他合约)导致串台
                    self._kline_box_daily = _cached_daily
                    self._kline_box_weekly = _cached_weekly
                    return _cached_daily, _cached_weekly

            # 数据源优先级: 外部传入 > DataService > klines_raw > ticks_raw聚合 > tick缓存降采样
            # FIX-KLINE-PRELOAD-FALLBACK-V2-20260728: 增加klines_raw直接fallback
            bars = daily_bars
            bars_source = 'external'
            if bars is None:
                bars = self._fetch_daily_bars_from_dataservice(instrument_id)
                bars_source = 'dataservice'
            # FIX-S3S4-MINBARS-3-V2-20260729: 删除 _min_bars_needed=max(3,15)=15 的统一阈值
            # 用户2026-07-29澄清: S3日内容只需3根日K线(不需要15根); S4隔夜才需3根周K线(=15根日K)
            # 旧设计 _min_bars_needed=15 把S3和S4的阈值混为一谈, 违背用户原意:
            #   - S3日内: 只需满足 daily_min=3, 不应被 weekly_min*5=15 的阈值阻塞
            #   - S4隔夜: 需要 weekly_min*5=15 根日K(聚合为3根周K)
            # 修复: 用日K阈值(daily_min=3)和周K阈值(weekly_min*5=15)分别触发fallback
            #   - bars >= daily_min=3 即可进行日K箱体检测(S3日内)
            #   - bars >= weekly_min*5=15 才能进行周K箱体检测(S4隔夜, 不够则weekly_box.is_valid=False)
            _daily_min_needed = self._kline_min_bars_daily             # S3: 3根日K
            _weekly_min_needed = self._kline_min_bars_weekly * 5      # S4: 3根周K = 15根日K
            # 日K fallback阈值: 至少满足S3日内3根日K
            if bars is None or len(bars) < _daily_min_needed:
                _prev_len = len(bars) if bars else 0
                _kraw_bars = self._fetch_daily_bars_from_klines_raw(instrument_id)
                if _kraw_bars and len(_kraw_bars) > _prev_len:
                    bars = _kraw_bars
                    bars_source = 'klines_raw'
            if bars is None or len(bars) < _daily_min_needed:
                _prev_len = len(bars) if bars else 0
                _traw_bars = self._fetch_daily_bars_from_ticks_raw(instrument_id)
                if _traw_bars and len(_traw_bars) > _prev_len:
                    bars = _traw_bars
                    bars_source = 'ticks_raw'
            if bars is None or len(bars) < _daily_min_needed:
                _prev_len = len(bars) if bars else 0
                _cache_bars = self._build_daily_bars_from_cache()
                if _cache_bars and len(_cache_bars) > _prev_len:
                    bars = _cache_bars
                    bars_source = 'tick_cache'
            # FIX-S4-D1-KLINE-20260730: 当日K数据不足以形成3根周K(需15根日K)时,
            # 从MarketCenter直接加载D1日K线, 绕过M1→日K聚合的数据量限制。
            # 根因: klines_raw仅累积7根日K(history_minutes=1440每日加载1天M1),
            #       7根日K聚合为2根周K < 3根 → S4 weekly_box.is_valid=False → S4永远0下单。
            # 修复: 当bars < _weekly_min_needed(15)时, 尝试从MarketCenter获取D1日K线。
            #       MarketCenter D1可提供20+根日K, 足以聚合3根周K。
            # 不改变策略逻辑: 仅新增数据源, 箱体检测算法不变, fail-closed保持。
            # FIX-D1-FALLBACK-DIAG-20260730: 在D1条件判断前打印bars值,确诊为何14:30日志无"D1 fallback触发"
            _d1_bars_len = len(bars) if bars else 0
            logger.info("[KLINE-BOX] D1条件检查: inst=%s bars=%d daily_min=%d weekly_min=%d will_trigger=%s",
                       instrument_id, _d1_bars_len, _daily_min_needed, _weekly_min_needed,
                       str(bars is None or _d1_bars_len < _weekly_min_needed))
            if bars is None or len(bars) < _weekly_min_needed:
                _prev_len = len(bars) if bars else 0
                logger.info("[KLINE-BOX] D1 fallback触发: inst=%s bars=%d < weekly_min=%d, 尝试MarketCenter D1",
                           instrument_id, _prev_len, _weekly_min_needed)
                _mc_bars = self._fetch_daily_bars_from_market_center(instrument_id)
                if _mc_bars and len(_mc_bars) > _prev_len:
                    bars = _mc_bars
                    bars_source = 'market_center_d1'
                    logger.info("[KLINE-BOX] D1 fallback成功: inst=%s D1_bars=%d > prev=%d, source=market_center_d1",
                               instrument_id, len(_mc_bars), _prev_len)
                else:
                    logger.info("[KLINE-BOX] D1 fallback未改善: inst=%s mc_bars=%d prev=%d",
                               instrument_id, len(_mc_bars) if _mc_bars else 0, _prev_len)
            # 注: 不再为S4周K单独触发更多fallback — S4若bars<15则weekly聚合后<3根周K,
            #     _detect_kline_box_from_bars会自然返回is_valid=False(fail-closed), 符合用户原意

            # FIX-S3S4-TICKS-TOLERANCE-V2-20260729: 读取品种tick_size用于"三高点/三低点差异"判定
            # 用户需求: "高\低点差异在10个最小变动单位内" — 指三高点内部差异和三低点内部差异
            try:
                from config.instrument_spec import get_tick_size_for_instrument
                _tick_size = get_tick_size_for_instrument(instrument_id)
            except Exception as e:
                logger.debug("[KLINE-BOX] get_tick_size_for_instrument failed: inst=%s err=%s",
                             instrument_id, e)
                _tick_size = 0.0

            # 日K小箱体检测 (S3日内: 3根日K, 三高点/三低点差异<=10*tick_size)
            daily_box = self._detect_kline_box_from_bars(
                bars=bars if bars else [],
                min_bars=self._kline_min_bars_daily,
                width_max_pct=self._kline_box_width_max_pct,
                box_type=BoxType.INTRADAY_SMALL,
                instrument_id=instrument_id,  # FIX-S4-DIAG-FIX-20260803
                tick_size=_tick_size,  # FIX-S3S4-TICKS-TOLERANCE-V2-20260729
            )

            # 周K中箱体检测 (S4隔夜: 3根周K, 三高点/三低点差异<=10*tick_size)
            # 先将日K线聚合为周K线; bars<15根日K时聚合后<3根周K, 自然返回is_valid=False
            weekly_bars = self._aggregate_to_weekly_klines(bars if bars else [])
            weekly_box = self._detect_kline_box_from_bars(
                bars=weekly_bars,
                min_bars=self._kline_min_bars_weekly,
                width_max_pct=self._kline_box_width_max_pct,
                box_type=BoxType.OVERNIGHT_MEDIUM,
                instrument_id=instrument_id,  # FIX-S4-DIAG-FIX-20260803
                tick_size=_tick_size,  # FIX-S3S4-TICKS-TOLERANCE-V2-20260729
            )

            # FIX-S3-CACHE-20260728: 更新per-instrument缓存
            self._kline_box_cache[instrument_id] = (daily_box, weekly_box, now)

            # FIX-DEL-TICKBOX-V2-20260729: 更新实例属性供detect_box/classify_extreme_state读取
            self._kline_box_daily = daily_box
            self._kline_box_weekly = weekly_box

            # 限制缓存大小防止内存泄漏
            if len(self._kline_box_cache) > 500:
                _oldest_key = min(self._kline_box_cache, key=lambda k: self._kline_box_cache[k][2])
                del self._kline_box_cache[_oldest_key]

            # FIX-KLINE-BOX-DIAG-20260803: 改为per-instrument首次+is_valid时输出INFO
            # 原: 前5次+每1000次 → 仅10/64品种可见,58品种无日志
            # 新: 每个品种首次调用 + 任何box成立时 → 全品种覆盖+关键事件不遗漏
            if not hasattr(self, '_diag_kbox_logged_insts'):
                self._diag_kbox_logged_insts = set()
            _first_for_inst = instrument_id not in self._diag_kbox_logged_insts
            self._diag_kbox_logged_insts.add(instrument_id)
            _kbox_log_level = logging.INFO if (
                _first_for_inst or daily_box.is_valid or weekly_box.is_valid
            ) else logging.DEBUG
            logger.log(
                _kbox_log_level,
                "[KLINE-BOX] detect_kline_box: inst=%s src=%s daily=%s(upper=%.2f lower=%.2f w=%.2f%% bars=%d conf=%.3f) "
                "weekly=%s(upper=%.2f lower=%.2f w=%.2f%% bars=%d conf=%.3f) bars_len=%d",
                instrument_id, bars_source,
                '✓' if daily_box.is_valid else '✗',
                daily_box.upper, daily_box.lower, daily_box.width_pct, daily_box.bar_count, daily_box.confidence,
                '✓' if weekly_box.is_valid else '✗',
                weekly_box.upper, weekly_box.lower, weekly_box.width_pct, weekly_box.bar_count, weekly_box.confidence,
                len(bars) if bars else 0,
            )

            return daily_box, weekly_box

    def _build_daily_bars_from_cache(self) -> List[Dict[str, Any]]:
        """从内部tick级价格缓存构建日K线数据

        Returns:
            日K线列表(近似，仅当外部日K线不可用时的降级方案)
        """
        # FIX-DEL-TICKBOX-V2-20260729: tick级缓存(_price_closes等)已删除
        # 旧实现: 从 _price_closes/_price_highs/_price_lows/_timestamps 按天聚合成日K线
        # 新实现: 返回空列表(tick级状态已删除, 无法从tick缓存构建日K线)
        # 调用方(detect_kline_box fallback链路)会因此跳过此fallback, 符合用户原意
        # (用户要求K线从DataService/klines_raw/ticks_raw获取, 不从tick缓存构建)
        return []

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
            # FIX-S3S4-KLINE-RANGE-V2-20260729: 查询前20天内所有K线(用户明确要求)
            # 用户2026-07-29要求: "历史k线应当查询\接受前20天内所有k线,以满足S4需要"
            # S4隔夜需要3根周K(=15根日K), 20天≈14-15个交易日, 覆盖3周周K聚合需求
            start_date = end_date - _td(days=20)

            # FIX-S3-TICKSRAW-CACHEBYPASS-20260728: 直接用ds.query并禁用缓存,
            # 避免首次预加载(symbol_daily_aggregates为空)时空结果被缓存
            _sda_sql = """
                SELECT * FROM symbol_daily_aggregates
                WHERE instrument_id = ? AND date BETWEEN ? AND ?
                ORDER BY date
            """
            result = ds.query(_sda_sql, [instrument_id, start_date, end_date], use_cache=False)
            # FIX-S3-DIAG-20260728: 记录query返回值类型和大小
            _result_rows = 0
            if result is not None:
                if hasattr(result, 'num_rows'):
                    _result_rows = result.num_rows
                elif hasattr(result, 'shape'):
                    _result_rows = result.shape[0] if hasattr(result.shape, '__len__') else 0
                elif hasattr(result, '__len__'):
                    try:
                        _result_rows = len(result)
                    except:
                        _result_rows = -1
            logger.info("[KLINE-BOX] sda_query: inst=%s result_type=%s rows=%d",
                        instrument_id, type(result).__name__, _result_rows)
            if result is None:
                # FIX-KLINE-FLUSH-20260725-FALLBACK: 主数据源返回None时仍尝试klines_raw fallback,
                # 避免DataService视图缺失/异常时直接放弃K线箱体检测。
                klines_raw_bars = self._fetch_daily_bars_from_klines_raw(instrument_id)
                if klines_raw_bars:
                    logger.info("[KLINE-BOX] get_symbol_daily_ohlc=None, klines_raw fallback: inst=%s bars=%d",
                                instrument_id, len(klines_raw_bars))
                    return klines_raw_bars
                # FIX-S3-KLINE-TICKSRAW-20260728: klines_raw也失败时, 尝试ticks_raw fallback
                ticks_raw_bars = self._fetch_daily_bars_from_ticks_raw(instrument_id)
                if ticks_raw_bars:
                    return ticks_raw_bars
                return None

            # 兼容pa.Table和pd.DataFrame
            if hasattr(result, 'to_pydict'):
                data = result.to_pydict()
            elif hasattr(result, 'to_dict'):
                data = result.to_dict(orient='list')
            else:
                return None

            if not data or not data.get('date'):
                # FIX-KLINE-PRELOAD-FALLBACK-20260728: 视图返回空结果时也执行klines_raw fallback
                # 根因: get_symbol_daily_ohlc返回非None但data为空(视图存在但无数据)时,
                #   原代码直接return None, 跳过了klines_raw fallback → K线箱体检测永远失败
                # 修复: 与result=None分支一致, 尝试klines_raw fallback
                klines_raw_bars = self._fetch_daily_bars_from_klines_raw(instrument_id)
                if klines_raw_bars:
                    logger.info("[KLINE-BOX] data empty(no date), klines_raw fallback: inst=%s bars=%d",
                                instrument_id, len(klines_raw_bars))
                    return klines_raw_bars
                # FIX-S3-KLINE-TICKSRAW-20260728: klines_raw也失败时, 尝试ticks_raw fallback
                ticks_raw_bars = self._fetch_daily_bars_from_ticks_raw(instrument_id)
                if ticks_raw_bars:
                    return ticks_raw_bars
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

            # FIX-S3-MINBARS-FALLBACK-20260729: bars非空但行数不足min_bars时也执行fallback
            # 根因: sda_query返回2行数据, if bars: return bars 直接返回2行
            #   → detect_kline_box收到2行 → min_bars=20检查失败 → 0箱体检测
            # 修复: 仅当bars行数>=min_bars时才直接返回, 否则继续尝试klines_raw fallback
            if bars and len(bars) >= self._kline_min_bars_daily:
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

            # FIX-S3-KLINE-TICKSRAW-20260728: klines_raw也失败时, 尝试ticks_raw fallback
            ticks_raw_bars = self._fetch_daily_bars_from_ticks_raw(instrument_id)
            if ticks_raw_bars:
                return ticks_raw_bars

            return None

        except Exception as e:
            # FIX-S3-DIAG-20260728: 升级为INFO级别, 便于诊断_fetch_daily_bars_from_dataservice静默失败
            logger.info("[KLINE-BOX] _fetch_daily_bars_from_dataservice EXCEPTION: inst=%s err=%s type=%s",
                         instrument_id, e, type(e).__name__)
            traceback.print_exc()
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
            # FIX-KLINE-RAW-FALLBACK-20260728: internal_id不可用时尝试instrument_id直接查询
            # 根因: get_instrument_meta_by_id在启动早期返回None(DB未就绪)或
            #   internal_id字段缺失 → _fetch_daily_bars_from_klines_raw→None
            #   → detect_kline_box所有数据源失败 → K线箱体永远未确认 → S3/S4永远0下单
            # 修复: 三层fallback: (1)internal_id查询 (2)instrument_id查询 (3)返回None
            # 不改变策略逻辑: 仅数据源获取方式, 箱体检测/信号生成逻辑不变
            from config.params_service import get_params_service
            ps = get_params_service()
            meta = ps.get_instrument_meta_by_id(instrument_id)
            internal_id = meta.get('internal_id') if meta else None
            use_instrument_id_fallback = False
            if internal_id is None:
                # Fallback: 尝试用instrument_id直接查询(部分实现klines_raw用instrument_id)
                use_instrument_id_fallback = True
                _query_id = instrument_id
                _query_col = 'instrument_id'
                logging.debug("[KLINE-BOX] internal_id=None for inst=%s, trying instrument_id fallback", instrument_id)
            else:
                _query_id = internal_id
                _query_col = 'internal_id'

            # FIX-S3S4-KLINE-RANGE-V2-20260729: 查询前20天内所有M1 K线(用户明确要求, 满足S4需要)
            from datetime import date, timedelta as _td
            end_date = date.today()
            start_date = end_date - _td(days=20)

            # FIX-KLINE-RAW-FALLBACK-20260728: 动态列名支持internal_id/instrument_id两种查询
            sql = f"""
                SELECT trade_date, open, high, low, close, volume
                FROM klines_raw
                WHERE {_query_col} = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date, timestamp
            """
            result = ds.query(sql, [_query_id, start_date, end_date], use_cache=False)
            if result is None and use_instrument_id_fallback:
                # instrument_id fallback也失败, 尝试用internal_id查询(双重保险)
                logging.debug("[KLINE-BOX] instrument_id fallback failed for inst=%s, trying internal_id", instrument_id)
                if internal_id is not None:
                    _fallback_sql = """
                        SELECT trade_date, open, high, low, close, volume
                        FROM klines_raw
                        WHERE internal_id = ? AND trade_date BETWEEN ? AND ?
                        ORDER BY trade_date, timestamp
                    """
                    result = ds.query(_fallback_sql, [internal_id, start_date, end_date], use_cache=False)
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

    def _fetch_daily_bars_from_market_center(
        self,
        instrument_id: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """从MarketCenter直接加载D1(日)K线数据 (FIX-S4-D1-KLINE-20260730)

        根因: S4隔夜策略需要3根周K线(=15根日K), 但klines_raw表仅累积7根日K
              (history_minutes=1440每日仅加载1天M1数据, MarketCenter M1仅7天数据)。
              7根日K聚合为2根周K < 3根 → weekly_box.is_valid=False → S4永远0下单。

        修复: 直接从MarketCenter请求D1(日)K线, 绕过M1→日K聚合的限制。
              MarketCenter.get_kline_data(style='D1')返回历史日K线,
              可提供20+根日K, 足以聚合3根周K满足S4需求。

        设计原则:
          - 不改变策略逻辑: 仅新增数据源, 箱体检测算法(_detect_kline_box_from_bars)不变
          - 不错误降级: 返回的数据格式与_fetch_daily_bars_from_klines_raw一致, 同样的校验
          - fail-closed: 任何异常返回None, 不影响已有数据源的结果
          - 非半拉子工程: 完整的exchange推断+D1请求+格式转换+异常处理

        Args:
            instrument_id: 合约ID(期货, 如IF2608/IH2608)

        Returns:
            日K线列表 或 None(获取失败/MarketCenter不可用)
        """
        try:
            import re as _re
            # FIX-D1-ECO-INJECTION-V4-20260731: 直接从StrategyEcosystem获取(单渠道)
            # 根因已修复: lifecycle_bind将get_kline/_runtime_market_center注入StrategyEcosystem单例
            # 原V3版本通过模块级_d1_get_kline全局变量注入,与eco.get_kline形成双通道(违反原则2)
            from strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            if eco is None:
                logger.warning("[KLINE-BOX] _fetch_daily_bars_from_market_center: get_strategy_ecosystem()=None, inst=%s", instrument_id)
                return None
            get_kline_fn = getattr(eco, 'get_kline', None)
            if not callable(get_kline_fn):
                # fallback: 从eco._runtime_market_center获取get_kline_data
                mc = getattr(eco, '_runtime_market_center', None)
                if mc and hasattr(mc, 'get_kline_data'):
                    get_kline_fn = mc.get_kline_data
                else:
                    logger.warning("[KLINE-BOX] _fetch_daily_bars_from_market_center: get_kline不可用, eco.get_kline=%s, _runtime_mc=%s, inst=%s",
                                   get_kline_fn, mc, instrument_id)
                    return None

            # 3. 推断交易所代码(从instrument_id提取product → 查instrument_spec)
            exchange = ''
            try:
                from config.instrument_spec import _lookup_spec_case_insensitive
                # 期货: 提取字母前缀作为product (IF2608→IF, rb2609→rb)
                m = _re.match(r'^([A-Za-z]+)', instrument_id.strip())
                if m:
                    product = m.group(1)
                    spec = _lookup_spec_case_insensitive(product)
                    if spec:
                        exchange = spec.exchange
            except Exception:
                pass

            if not exchange:
                # fallback: 常见交易所尝试
                # CFFEX品种: IF/IH/IC/IM/IO/HO/MO/T/TF/TS/TL
                _product = _re.match(r'^([A-Za-z]+)', instrument_id.strip())
                _prod = _product.group(1).upper() if _product else ''
                if _prod in ('IF', 'IH', 'IC', 'IM', 'IO', 'HO', 'MO', 'T', 'TF', 'TS', 'TL'):
                    exchange = 'CFFEX'
                elif _prod in ('CU', 'AL', 'ZN', 'PB', 'NI', 'AU', 'AG', 'RB', 'WR', 'HC', 'SS', 'BU', 'RU', 'NR', 'FU', 'BC', 'LU', 'BC'):
                    exchange = 'SHFE'
                elif _prod in ('A', 'B', 'M', 'Y', 'P', 'C', 'CS', 'JD', 'LH', 'RR', 'I', 'J', 'JM', 'L', 'V', 'PP', 'EG', 'EB', 'PG', 'CJ'):
                    exchange = 'DCE'
                elif _prod in ('CF', 'SR', 'TA', 'OI', 'RI', 'RS', 'RM', 'FG', 'SF', 'SM', 'AP', 'CJ', 'UR', 'SA', 'PF', 'PK', 'SH', 'PX', 'PR'):
                    exchange = 'CZCE'
                else:
                    return None  # 无法确定交易所, fail-closed

            # 4. 请求D1(日)K线, 取最近30根(覆盖3周+裕量)
            try:
                klines = get_kline_fn(exchange, instrument_id=instrument_id, style='D1', count=-30)
            except TypeError:
                # FIX-D1-POSITION-ARG-V3-20260731: 位置参数fallback(从eco._runtime_market_center获取)
                # 根因: get_kline_fn(exchange, instrument_id, 'D1')的第3个位置参数在
                #   _compat_get_kline_data签名(exchange, instrument_id=None, instrument=None, style="M1")
                #   中赋给instrument而非style → style取默认"M1" → silent退化为M1数据
                # 修复: 使用eco._runtime_market_center.get_kline_data直接调用(绕过_compat), 仍用style='D1' kwarg
                try:
                    _mc = getattr(eco, '_runtime_market_center', None)
                    if _mc and callable(getattr(_mc, 'get_kline_data', None)):
                        klines = _mc.get_kline_data(
                            exchange=exchange, instrument_id=instrument_id, style='D1', count=-30)
                    else:
                        logger.warning("[KLINE-BOX] _fetch_daily_bars: TypeError fallback无raw MC可用, inst=%s exchange=%s",
                                       instrument_id, exchange)
                        return None
                except Exception as _te:
                    logger.warning("[KLINE-BOX] _fetch_daily_bars: raw MC调用也失败, inst=%s exchange=%s err=%s",
                                   instrument_id, exchange, _te)
                    return None

            if not klines:
                logger.debug("[KLINE-BOX] _fetch_daily_bars_from_market_center: klines为空, inst=%s exchange=%s", instrument_id, exchange)
                return None

            # 5. 转换为日K线格式(与_fetch_daily_bars_from_klines_raw一致)
            daily_bars: List[Dict[str, Any]] = []
            for k in klines:
                try:
                    # PythonGO K线对象格式: {'datetime': datetime, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float}
                    dt = k.get('datetime') if isinstance(k, dict) else getattr(k, 'datetime', None)
                    o = float(k.get('open', 0) if isinstance(k, dict) else getattr(k, 'open', 0))
                    h = float(k.get('high', 0) if isinstance(k, dict) else getattr(k, 'high', 0))
                    l = float(k.get('low', 0) if isinstance(k, dict) else getattr(k, 'low', 0))
                    c = float(k.get('close', 0) if isinstance(k, dict) else getattr(k, 'close', 0))
                    v = float(k.get('volume', 0) if isinstance(k, dict) else getattr(k, 'volume', 0))

                    if dt is None or h <= 0 or l <= 0 or c <= 0:
                        continue

                    # 转换datetime为日期字符串(timestamp字段)
                    if hasattr(dt, 'strftime'):
                        ts_str = dt.strftime('%Y-%m-%d')
                    elif hasattr(dt, 'isoformat'):
                        ts_str = dt.isoformat()[:10]
                    else:
                        ts_str = str(dt)[:10]

                    daily_bars.append({
                        'timestamp': ts_str,
                        'open': o,
                        'high': h,
                        'low': l,
                        'close': c,
                        'volume': v,
                    })
                except (ValueError, TypeError, AttributeError):
                    continue

            # 按timestamp排序去重(同一日期可能有多根)
            if not daily_bars:
                logger.debug("[KLINE-BOX] _fetch_daily_bars_from_market_center: daily_bars为空(所有K线转换失败), inst=%s", instrument_id)
                return None

            seen_dates: Dict[str, Dict[str, Any]] = {}
            for bar in daily_bars:
                d = bar['timestamp']
                if d not in seen_dates:
                    seen_dates[d] = bar
                else:
                    # 同日取合并(high取最大, low取最小)
                    seen_dates[d]['high'] = max(seen_dates[d]['high'], bar['high'])
                    seen_dates[d]['low'] = min(seen_dates[d]['low'], bar['low'])
                    seen_dates[d]['close'] = bar['close']
                    seen_dates[d]['volume'] += bar['volume']

            result = sorted(seen_dates.values(), key=lambda x: x.get('timestamp', ''))
            logger.info(
                "[KLINE-BOX] _fetch_daily_bars_from_market_center: inst=%s exchange=%s D1_bars=%d",
                instrument_id, exchange, len(result),
            )
            return result if result else None

        except Exception as e:
            # FIX-D1-FALLBACK-LOG-20260730: 升级为WARNING, 原logger.debug在INFO级别不可见
            # 导致D1 fallback失败时完全无法诊断MarketCenter可用性
            logger.warning("[KLINE-BOX] _fetch_daily_bars_from_market_center failed: inst=%s err=%s type=%s",
                         instrument_id, e, type(e).__name__)
            return None

    def _fetch_daily_bars_from_ticks_raw(self, instrument_id: str) -> Optional[List[Dict[str, Any]]]:
        """从ticks_raw表查询tick数据并聚合成日K线(FIX-S3-KLINE-TICKSRAW-20260728)

        当symbol_daily_aggregates和klines_raw表均无数据时, 从ticks_raw表查询
        原始tick数据, 按date聚合成日K线(open/high/low/close/volume)。

        根因: K线预加载50个期货全部失败(ok=0 fail=50), 原因是:
          1. symbol_daily_aggregates表无数据(从未聚合过日K线)
          2. klines_raw表无数据(flush_incomplete_klines未落库或表为空)
          3. _build_daily_bars_from_cache返回空(update_bar从未被调用)
          → 三层fallback全部失败 → K线箱体永远未确认 → S3/S4永远0下单

        修复: 增加第四层fallback, 从ticks_raw表查询原始tick数据,
          用DuckDB的arg_min/arg_max聚合函数按date聚合成日K线。
          ticks_raw表由实时tick写入管道持续填充, 是最可靠的数据源。

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
            # FIX-S3S4-KLINE-RANGE-V2-20260729: 查询前20天内所有ticks(用户明确要求, 满足S4需要)
            start_date = end_date - _td(days=20)

            # FIX-S3-KLINE-TICKSRAW-20260728: 用DuckDB聚合函数从ticks_raw构建日K线
            # arg_min(value, order): 返回按order排序后的第一个value (即open)
            # arg_max(value, order): 返回按order排序后的最后一个value (即close)
            # FIX-S3-TICKSRAW-CACHEBYPASS-20260728: 禁用查询缓存, 避免首次预加载时
            #   ticks_raw为空导致空结果被缓存, 后续重试返回缓存空结果而非实时查询
            sql = """
                SELECT
                    date as trade_date,
                    arg_min(last_price, timestamp) as open,
                    max(last_price) as high,
                    min(last_price) as low,
                    arg_max(last_price, timestamp) as close,
                    coalesce(sum(volume), 0) as volume
                FROM ticks_raw
                WHERE instrument_id = ?
                  AND date BETWEEN ? AND ?
                  AND last_price > 0
                GROUP BY date
                ORDER BY date
            """
            # FIX-S3-TICKSRAW-CACHEBYPASS-20260728: use_cache=False绕过查询缓存
            result = ds.query(sql, [instrument_id, start_date, end_date], use_cache=False)
            if result is None:
                logger.info("[KLINE-BOX] ticks_raw query returned None: inst=%s", instrument_id)
                return None

            # 兼容pa.Table和pd.DataFrame
            if hasattr(result, 'to_pydict'):
                data = result.to_pydict()
            elif hasattr(result, 'to_dict'):
                data = result.to_dict(orient='list')
            else:
                logger.info("[KLINE-BOX] ticks_raw result type unknown: inst=%s type=%s",
                            instrument_id, type(result).__name__)
                return None

            if not data or not data.get('trade_date'):
                # FIX-S3-TICKSRAW-DIAG-20260728: 诊断ticks_raw为何返回空结果
                # 查询ticks_raw中实际有哪些instrument_id(仅首次诊断时执行)
                if not getattr(self, '_ticks_raw_diag_done', False):
                    self._ticks_raw_diag_done = True
                    try:
                        diag_sql = "SELECT DISTINCT instrument_id FROM ticks_raw LIMIT 20"
                        diag_result = ds.query(diag_sql, use_cache=False)
                        if diag_result is not None:
                            if hasattr(diag_result, 'to_pydict'):
                                diag_data = diag_result.to_pydict()
                            elif hasattr(diag_result, 'to_dict'):
                                diag_data = diag_result.to_dict(orient='list')
                            else:
                                diag_data = {}
                            _insts = diag_data.get('instrument_id', [])
                            logger.info("[KLINE-BOX] ticks_raw DIAG: queried inst=%s but no rows. "
                                        "ticks_raw has %d distinct instrument_ids: %s",
                                        instrument_id, len(_insts), _insts[:10])
                    except Exception as _diag_err:
                        logger.info("[KLINE-BOX] ticks_raw DIAG failed: %s", _diag_err)
                return None

            trade_dates = data.get('trade_date', [])
            opens = data.get('open', [])
            highs = data.get('high', [])
            lows = data.get('low', [])
            closes = data.get('close', [])
            volumes = data.get('volume', [])

            bars = []
            for i in range(len(trade_dates)):
                d = trade_dates[i]
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
                logger.info("[KLINE-BOX] ticks_raw fallback: inst=%s bars=%d dates=[%s..%s]",
                            instrument_id, len(bars),
                            bars[0].get('timestamp', '')[:10],
                            bars[-1].get('timestamp', '')[:10])
            return bars if bars else None

        except Exception as e:
            # FIX-S3-TICKSRAW-DIAG-20260728: 升级为INFO级别, 便于诊断静默失败
            logger.info("[KLINE-BOX] _fetch_daily_bars_from_ticks_raw failed: inst=%s err=%s",
                         instrument_id, e)
            return None

    def check_kline_box_precondition(
        self,
        instrument_id: str,
        days_to_expiry: int = 0,
        daily_bars: Optional[List[Dict[str, Any]]] = None,
        box_type: Optional[BoxType] = None,
    ) -> Tuple[bool, Optional[KLineBoxProfile]]:
        """K线箱体前置条件检查 — S3/S4策略信号产生的唯一箱体来源

        FIX-DEL-DTE-SWITCH-20260729: 删除 dte≤5/dte>5 条件切换(用户明确要求)
        用户原意: S3是日内策略(永远用日K箱体), S4是隔夜策略(永远用周K箱体),
        不应通过 days_to_expiry 来切换箱体类型。

        新规则:
        - box_type=INTRADAY_SMALL → 只检查日K小箱体(S3日内)
        - box_type=OVERNIGHT_MEDIUM → 只检查周K中箱体(S4隔夜)
        - box_type=None → 同时检查两个箱体, 优先返回日K(日内合约更可能日K有效),
                          隔夜合约更可能周K有效(日K不足3根时周K也不足, 自然fail-closed)
        - K线箱体确认后自动更新_current_box, 作为信号生成的箱体边界

        Args:
            instrument_id: 合约ID
            days_to_expiry: 距到期日天数(仅用于日志, 不再用于切换箱体类型)
            daily_bars: 外部传入的日K线数据
            box_type: 明确指定箱体类型(可选, None=同时检查两个)

        Returns:
            (passed, kline_box) 是否通过前置条件 + 匹配的K线箱体轮廓
        """
        daily_box, weekly_box = self.detect_kline_box(
            instrument_id=instrument_id,
            daily_bars=daily_bars,
        )

        # FIX-DEL-DTE-SWITCH-20260729: 删除 dte≤5/dte>5 切换, 改为按 box_type 选择
        if box_type == BoxType.INTRADAY_SMALL:
            # S3日内: 只检查日K小箱体
            if daily_box is not None and daily_box.is_valid:
                self._update_current_box_from_kline(daily_box, 'INTRADAY_SMALL')
                return True, daily_box
            else:
                logger.debug(
                    "[KLINE-BOX] PRECONDITION FAIL: inst=%s dte=%d 日K小箱体未确认(upper=%.2f lower=%.2f bars=%d valid=%s)",
                    instrument_id, days_to_expiry,
                    daily_box.upper if daily_box else 0.0,
                    daily_box.lower if daily_box else 0.0,
                    daily_box.bar_count if daily_box else 0,
                    daily_box.is_valid if daily_box else False,
                )
                return False, daily_box

        elif box_type == BoxType.OVERNIGHT_MEDIUM:
            # S4隔夜: 只检查周K中箱体
            if weekly_box is not None and weekly_box.is_valid:
                self._update_current_box_from_kline(weekly_box, 'OVERNIGHT_MEDIUM')
                return True, weekly_box
            else:
                logger.debug(
                    "[KLINE-BOX] PRECONDITION FAIL: inst=%s dte=%d 周K中箱体未确认(upper=%.2f lower=%.2f bars=%d valid=%s)",
                    instrument_id, days_to_expiry,
                    weekly_box.upper if weekly_box else 0.0,
                    weekly_box.lower if weekly_box else 0.0,
                    weekly_box.bar_count if weekly_box else 0,
                    weekly_box.is_valid if weekly_box else False,
                )
                return False, weekly_box

        else:
            # box_type=None: 同时检查两个箱体(兼容旧调用方)
            # 优先日K(日内合约更可能日K有效), 日K无效时检查周K(隔夜合约)
            if daily_box is not None and daily_box.is_valid:
                self._update_current_box_from_kline(daily_box, 'INTRADAY_SMALL')
                return True, daily_box
            elif weekly_box is not None and weekly_box.is_valid:
                self._update_current_box_from_kline(weekly_box, 'OVERNIGHT_MEDIUM')
                return True, weekly_box
            else:
                logger.debug(
                    "[KLINE-BOX] PRECONDITION FAIL: inst=%s dte=%d 日K和周K箱体均未确认 "
                    "(daily: upper=%.2f lower=%.2f bars=%d valid=%s; "
                    "weekly: upper=%.2f lower=%.2f bars=%d valid=%s)",
                    instrument_id, days_to_expiry,
                    daily_box.upper if daily_box else 0.0,
                    daily_box.lower if daily_box else 0.0,
                    daily_box.bar_count if daily_box else 0,
                    daily_box.is_valid if daily_box else False,
                    weekly_box.upper if weekly_box else 0.0,
                    weekly_box.lower if weekly_box else 0.0,
                    weekly_box.bar_count if weekly_box else 0,
                    weekly_box.is_valid if weekly_box else False,
                )
                return False, daily_box

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
            # FIX-DEL-TICKBOX-V2-20260729: _bounce_at_lower/_bounce_at_upper已删除(tick级状态)
            # 旧代码 self._bounce_at_lower=0 / self._bounce_at_upper=0 会触发AttributeError

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
            # FIX-DEL-TICKBOX-V2-20260729: 删除已废弃的tick级状态读取
            # 旧代码读 len(self._price_closes)/self._adx_period/self._adx_threshold/self._bounce_tolerance_pct
            # 这些属性已删除(tick级箱体彻底删除), 读取会触发AttributeError
            # 新代码: 报告K线箱体状态(替代tick级price_bars)
            stats['kline_box_daily_valid'] = self._kline_box_daily.is_valid if self._kline_box_daily else False
            stats['kline_box_weekly_valid'] = self._kline_box_weekly.is_valid if self._kline_box_weekly else False
            stats['kline_box_daily_upper'] = self._kline_box_daily.upper if self._kline_box_daily else 0.0
            stats['kline_box_daily_lower'] = self._kline_box_daily.lower if self._kline_box_daily else 0.0
            stats['kline_box_weekly_upper'] = self._kline_box_weekly.upper if self._kline_box_weekly else 0.0
            stats['kline_box_weekly_lower'] = self._kline_box_weekly.lower if self._kline_box_weekly else 0.0
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
    """BoxDetector 单例工厂

    FIX-DEL-TICKBOX-V2-CLEANUP-V4-20260729: 不再从 config 加载已删除的 tick 级参数
    旧代码: 从 get_cached_params() 加载 'box_gain_ratio', 'plr_normalization_base' 并作为
            **kwargs 传给 BoxDetector — 这两个参数已在 FIX-DEL-TICKBOX-V2-20260729 中删除,
            若配置中存在这些 key 会触发 TypeError: unexpected keyword argument
    新代码: 直接以默认参数构造(tick级箱体已删除, BoxDetector.__init__只接受 params/lookback_bars/iv_history_maxlen)
    """
    global _box_detector
    with _box_detector_lock:
        if _box_detector is None:
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
