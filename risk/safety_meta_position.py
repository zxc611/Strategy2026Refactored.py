# [M1-38] __ȫԪ__-Ӳֹͣ__Ϲ_

# MODULE_ID: M1-223

from __future__ import annotations



import logging

import time

import threading

from datetime import datetime, timedelta

from typing import Any, Dict, List, Optional, Tuple



from ali2026v3_trading.infra.shared_utils import safe_int, safe_float, CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

from ali2026v3_trading.risk.risk_support import safe_get_float, api_version, _get_tz_aware_now, CircuitBreakerStateStore

from ali2026v3_trading.position.margin_manager import MarginManager

from ali2026v3_trading.governance.compliance_checker import ComplianceChecker

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_jsonl_append_line









class HardTimeStopAndComplianceService:

    """硬时间止损合规+保证金展期+健康+统计服务



    从SafetyMetaLayer提取的非断路器非回撤逻辑

    """



    def __init__(self, params: Any, owner: Any):

        self.params = params

        self.owner = owner

        self._lock = threading.RLock()

        self._stats_lock = threading.Lock()



        self._logic_reversal_priority: Dict[str, bool] = {}

        self._rollover_cost_bps: float = 0.0

        self._rollover_count: int = 0



        self._margin_manager = MarginManager()

        self._compliance_checker = ComplianceChecker(owner)



    _STRATEGY_HARD_STOP_OVERRIDES = {
        'spring': {'stage1_minutes': 5.0, 'stage2_minutes': 15.0, 'stage1_profit_threshold': 0.001},  # [FIX-20260712-S4] 180→5min/480→15min(匹配弹簧短持仓特性)
        'box': {'stage1_minutes': 120.0, 'stage2_minutes': 360.0, 'stage1_profit_threshold': 0.001},
        'arbitrage': {'stage1_minutes': 60.0, 'stage2_minutes': 180.0, 'stage1_profit_threshold': 0.005},
        'market_making': {'stage1_minutes': 30.0, 'stage2_minutes': 120.0, 'stage1_profit_threshold': 0.01},
        'high_freq': {'stage1_minutes': 1.0, 'stage2_minutes': 1.0, 'stage1_profit_threshold': 0.001},  # [FIX-20260712-S1] 60秒硬止损(原20-45分钟)
        'resonance': {'stage1_minutes': 3.0, 'stage2_minutes': 5.0, 'stage1_profit_threshold': 0.005},
        'divergence': {'stage1_minutes': 45.0, 'stage2_minutes': 90.0, 'stage1_profit_threshold': 0.003},
        'intraday': {'stage1_minutes': 120.0, 'stage2_minutes': 240.0, 'stage1_profit_threshold': 0.002},  # [FIX-20260712-S2] S2日内: 2-4小时
    }

    def check_position_hard_time_stop(self, position_id: str, open_time,

                                       max_profit_reached: float,

                                       profit_slope: float = 0.0,

                                       peak_profit_pct: float = 0.0,

                                       current_profit_pct: float = 0.0,

                                       bar_time: Optional[float] = None,

                                       stats: Dict[str, Any] = None,

                                       strategy_group: str = '') -> Optional[str]:

        if isinstance(open_time, datetime):

            open_time = open_time.timestamp()

        elif not isinstance(open_time, (int, float)):

            try:

                open_time = float(open_time)

            except (ValueError, TypeError):

                logging.warning("[SafetyMetaLayer] open_time类型无效: %s, 跳过硬时间止损检查", type(open_time).__name__)

                return None



        if self._logic_reversal_priority.get(position_id, False):

            logging.info(

                "[SafetyMetaLayer] P1-裂缝36: position=%s 逻辑反转已触发，"

                "硬时间止损降级为备用（优先级：逻辑反转>阶段2>阶段1)",

                position_id,

            )

            return None



        stage1_min = self._get_stage1_minutes(strategy_group)

        stage2_min = self._get_stage2_minutes(strategy_group)

        stage1_threshold = self._get_stage1_profit_threshold(strategy_group)



        now = bar_time if bar_time is not None else time.time()

        elapsed_min = (now - open_time) / 60.0



        if elapsed_min >= stage1_min and max_profit_reached < stage1_threshold:

            if stats is not None:

                with self._stats_lock:

                    stats["hard_time_stop_triggers"] += 1

            if not hasattr(self, '_stage1_warn_count'):
                self._stage1_warn_count = {}
            _warn_key = position_id
            self._stage1_warn_count[_warn_key] = self._stage1_warn_count.get(_warn_key, 0) + 1
            if self._stage1_warn_count[_warn_key] <= 3 or self._stage1_warn_count[_warn_key] % 1000 == 0:

                logging.warning(

                    "[SafetyMetaLayer] _阶段1硬止损触发！position=%s, 已持%.0fmin, "

                    "最高浮）%.2f%% < 阶段1要求=%.2f%% (count=%d)",

                    position_id, elapsed_min, max_profit_reached * 100, stage1_threshold * 100,
                    self._stage1_warn_count[_warn_key]

                )

            return f"HardTimeStop@{elapsed_min:.0f}min(profit<{stage1_threshold:.1%})"



        if stage1_min <= elapsed_min < stage2_min:

            stage2_slope_threshold = -0.01

            _slope_tolerance = 1e-9

            if profit_slope < stage2_slope_threshold - _slope_tolerance:

                if stats is not None:

                    with self._stats_lock:

                        stats["hard_time_stop_triggers"] += 1

                if not hasattr(self, '_stage2_warn_count'):
                    self._stage2_warn_count = {}
                _s2_key = position_id
                self._stage2_warn_count[_s2_key] = self._stage2_warn_count.get(_s2_key, 0) + 1
                if self._stage2_warn_count[_s2_key] <= 3 or self._stage2_warn_count[_s2_key] % 1000 == 0:

                    logging.warning(

                        "[SafetyMetaLayer] 阶段2硬止损触发斜率衰减)！position=%s, 已持%.0fmin, "

                        "profit_slope=%.6f < threshold=%.4f (count=%d)",

                        position_id, elapsed_min, profit_slope, stage2_slope_threshold,
                        self._stage2_warn_count[_s2_key]

                    )

                return f"HardTimeStop@{elapsed_min:.0f}min(slope<{stage2_slope_threshold})"



            if peak_profit_pct > 0 and current_profit_pct < peak_profit_pct * 0.5:

                if stats is not None:

                    with self._stats_lock:

                        stats["hard_time_stop_triggers"] += 1

                logging.warning(

                    "[SafetyMetaLayer] _阶段2硬止损触发回撤单0%%)！position=%s, 已持%.0fmin, "

                    "peak=%.2f%% current=%.2f%%",

                    position_id, elapsed_min, peak_profit_pct * 100, current_profit_pct * 100

                )

                return f"HardTimeStop@{elapsed_min:.0f}min(drawdown>50%)"



        return None



    def set_logic_reversal_triggered(self, position_id: str, triggered: bool) -> None:

        with self._lock:

            self._logic_reversal_priority[position_id] = triggered

            if triggered:

                logging.info(

                    "[SafetyMetaLayer] P1-裂缝36: position=%s 逻辑反转触发送弁"

                    "硬时间止损降级为备用",

                    position_id,

                )



    def check_regulatory_compliance(self, *args, **kwargs):

        return self._compliance_checker.check_regulatory_compliance(*args, **kwargs)



    @api_version("1.0")

    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,

                                   open_positions: int = 0, max_positions: int = 50,

                                   existing_margin_used: float = 0.0,

                                   daily_start_equity: Optional[float] = None) -> Dict[str, Any]:

        daily_start = daily_start_equity or equity

        return self._margin_manager.check_capital_sufficiency(

            equity, required_margin, open_positions, max_positions, existing_margin_used,

            daily_start_equity=daily_start,

        )



    def reserve_margin(self, *args, **kwargs):

        return self._margin_manager.reserve_margin(*args, **kwargs)



    def release_margin(self, *args, **kwargs):

        return self._margin_manager.release_margin(*args, **kwargs)



    def check_exchange_status(self, exchange: str = "AUTO") -> Dict[str, Any]:

        exchanges_status: Dict[str, str] = {}

        tradeable = True

        reason = "ok"

        try:

            from ali2026v3_trading.infra.scheduler_service import is_market_open

            target_exchanges = [exchange] if exchange != "AUTO" else ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX']

            for exch in target_exchanges:

                try:

                    is_open = is_market_open(exch)

                    exchanges_status[exch] = "OPEN" if is_open else "CLOSED"

                    if not is_open:

                        tradeable = False

                        reason = f"{exch}_closed"

                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                    exchanges_status[exch] = "UNKNOWN"

        except ImportError:

            exchanges_status["fallback"] = "scheduler_service_unavailable"

            reason = "scheduler_unavailable"

        return {"tradeable": tradeable, "exchanges": exchanges_status, "reason": reason}



    def compute_and_track_rollover_cost(self, bar_data, params: Optional[Dict] = None) -> Dict[str, Any]:

        try:

            from ali2026v3_trading.infra.commission_utils import detect_rollover_gaps, compute_rollover_cost

            rollover_points = detect_rollover_gaps(bar_data)

            if rollover_points:

                cost_result = compute_rollover_cost(

                    rollover_points, bar_data, params or {},

                    calendar_basis_bps=5.0, rollover_slippage_bps=3.0,

                )

                with self._lock:

                    self._rollover_cost_bps += cost_result.get('total_rollover_cost_bps', 0.0)

                    self._rollover_count += cost_result.get('rollover_count', 0)

                logging.info(

                    "[SafetyMetaLayer] P1-1: 换月成本累计=%.1fbps (累计%d_",

                    self._rollover_cost_bps, self._rollover_count,

                )

                return cost_result

            return {"total_rollover_cost_bps": 0.0, "rollover_count": 0}

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.debug("[SafetyMetaLayer] P1-1: compute_rollover_cost调用失败: %s", e)

            return {"total_rollover_cost_bps": 0.0, "rollover_count": 0, "error": str(e)}



    def get_rollover_cost_summary(self) -> Dict[str, Any]:

        with self._lock:

            return {

                "total_rollover_cost_bps": self._rollover_cost_bps,

                "rollover_count": self._rollover_count,

            }



    def get_health_status(self, trading_paused_until: float, circuit_breaker_calm_until: float,

                           pause_reason: str, daily_hard_stop_triggered: bool,

                           daily_drawdown: float, daily_new_open_blocked: bool,

                           circuit_breaker_shadow_mode: bool, circuit_breaker_shadow_until: float,

                           algo_paused: bool, algo_paused_until: float,

                           algo_pause_reason: str) -> Dict[str, Any]:

        _trigger_source = None

        health_status = 'OK'

        details: Dict[str, Any] = {}



        with self._lock:

            now = time.time()

            trading_paused = now < trading_paused_until

            circuit_breaker_active = now < circuit_breaker_calm_until

            if trading_paused or circuit_breaker_active:

                health_status = 'CRITICAL'

                _trigger_source = 'circuit_breaker'

                details['circuit_breaker'] = {

                    'trading_paused': trading_paused,

                    'calm_until': circuit_breaker_calm_until,

                    'pause_reason': pause_reason,

                }



            if daily_hard_stop_triggered:

                health_status = 'CRITICAL'

                _trigger_source = 'daily_drawdown' if _trigger_source is None else _trigger_source

                details['daily_hard_stop'] = {

                    'triggered': True,

                    'daily_drawdown_pct': daily_drawdown,

                    'new_open_blocked': daily_new_open_blocked,

                }



            if algo_paused and now < algo_paused_until:

                if health_status != 'CRITICAL':

                    health_status = 'WARNING'

                details['algo_paused'] = {

                    'paused': True,

                    'reason': algo_pause_reason,

                    'remaining_sec': max(0, algo_paused_until - now),

                }



            if circuit_breaker_shadow_mode and now < circuit_breaker_shadow_until:

                if health_status == 'OK':

                    health_status = 'WARNING'

                details['shadow_mode'] = {

                    'active': True,

                    'remaining_sec': max(0, circuit_breaker_shadow_until - now),

                }



            if daily_new_open_blocked and health_status == 'OK':

                health_status = 'WARNING'

                details['new_open_blocked'] = True



        return {

            'health_status': health_status,

            'triggered_by': _trigger_source if health_status == 'CRITICAL' else None,

            'details': details,

        }



    def get_stats(self, stats: Dict[str, Any], trading_paused_until: float,

                   daily_new_open_blocked: bool, daily_hard_stop_triggered: bool,

                   daily_drawdown: float) -> Dict[str, Any]:

        with self._lock:

            result = dict(stats)

            result['trading_paused'] = time.time() < trading_paused_until

            result['new_open_blocked'] = daily_new_open_blocked

            result['hard_stop_triggered'] = daily_hard_stop_triggered

            result['daily_drawdown_pct'] = daily_drawdown

            return result



    def _get_hard_time_stop_minutes(self) -> float:
        """DEPRECATED: 旧的单值硬止损方法，按固定优先级hft→spring→resonance→box。
        新代码应使用 _get_stage1_minutes(strategy_group) / _get_stage2_minutes(strategy_group)，
        它们通过 _STRATEGY_HARD_STOP_OVERRIDES 实现策略分层。
        本方法保留仅为 risk_circuit_breaker 向后兼容。"""

        hft_ms = safe_get_float(self.params, "hft_hard_time_stop_ms", 0)

        if hft_ms > 0:

            return hft_ms / 60000.0

        spring_sec = safe_get_float(self.params, "spring_hard_time_stop_sec", 0)

        if spring_sec > 0:

            return spring_sec / 60.0

        resonance_min = safe_get_float(self.params, "resonance_hard_time_stop_min", 0)

        if resonance_min > 0:

            return resonance_min

        box_min = safe_get_float(self.params, "box_hard_time_stop_min", 0)

        if box_min > 0:

            return box_min

        return 5.0



    def _get_min_profit_threshold(self) -> float:

        return safe_get_float(self.params, "min_profit_threshold", 0.002)



    def _get_stage1_minutes(self, strategy_group: str = '') -> float:

        _override = self._STRATEGY_HARD_STOP_OVERRIDES.get(strategy_group, {})
        if 'stage1_minutes' in _override:
            return _override['stage1_minutes']

        _bar_period = safe_get_float(self.params, "bar_period", 1.0)

        val = safe_get_float(self.params, "stage1_min_minutes", None)

        if val is not None and val > 0:

            return val * _bar_period

        _fallback = safe_get_float(self.params, "stage1_minutes", 90.0)

        if _fallback <= 0:

            logging.warning("[R5-L-01] stage1_minutes=%s<=0，使用默所0.0", _fallback)

            _fallback = 90.0

        return _fallback * _bar_period



    def _get_stage2_minutes(self, strategy_group: str = '') -> float:

        _override = self._STRATEGY_HARD_STOP_OVERRIDES.get(strategy_group, {})
        if 'stage2_minutes' in _override:
            return _override['stage2_minutes']

        return safe_get_float(self.params, "stage2_minutes", 240.0)



    def _get_stage1_profit_threshold(self, strategy_group: str = '') -> float:

        _override = self._STRATEGY_HARD_STOP_OVERRIDES.get(strategy_group, {})
        if 'stage1_profit_threshold' in _override:
            return _override['stage1_profit_threshold']

        return safe_get_float(self.params, "stage1_profit_threshold", 0.002)



    def confirm_daily_resume(self, caller_id: str = "unknown", resume_token: Optional[str] = None,

                              approval_context: Optional[Dict[str, Any]] = None,

                              drawdown_monitor=None, circuit_breaker_service=None,

                              stats: Dict[str, Any] = None) -> bool:

        with self._lock:

            if drawdown_monitor is None:

                return False

            if not drawdown_monitor._daily_hard_stop_triggered:

                logging.info("[SafetyMetaLayer] confirm_daily_resume: 未处于硬停止状态，无需恢复")

                return False



            if approval_context is None or not approval_context.get('approver_id'):

                logging.critical(

                    "[OPS-14] confirm_daily_resume缺少审批! 必须提供approval_context包含approver_id. "

                    "caller_id=%s", caller_id

                )

                try:

                    from ali2026v3_trading.risk.risk_service import alert as _alert, AlertLevel as _AlertLevel

                    _alert(_AlertLevel.P0, 'confirm_resume_no_approval',

                           f'confirm_daily_resume缺少审批 caller={caller_id}',

                           {'caller_id': caller_id})

                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                    logging.warning("[R22-EP-P1] RiskService exception swallowed")

                    pass

                return False



            if resume_token is not None:

                if circuit_breaker_service and resume_token != circuit_breaker_service._resume_token:

                    logging.critical(

                        "[SafetyMetaLayer] R15-SEC-04: 恢复令牌不匹配！拒绝恢复 caller=%s", caller_id

                    )

                    return False

                if circuit_breaker_service:

                    circuit_breaker_service._resume_token = None

            else:

                if not caller_id.startswith("MANUAL_"):

                    logging.critical(

                        "[SafetyMetaLayer] R15-SEC-04: 非人工操作尝试恢复交易被拒绝！caller=%s",

                        caller_id

                    )

                    return False

            _now_t = time.time()

            _dt_now = datetime.fromtimestamp(_now_t, tz=_CHINA_TZ)

            _today_key = _dt_now.strftime("%Y-%m-%d") if _dt_now.hour >= 18 else (_dt_now - timedelta(days=1)).strftime("%Y-%m-%d")

            if hasattr(drawdown_monitor, '_last_resume_date') and drawdown_monitor._last_resume_date == _today_key:

                logging.critical(

                    "[SafetyMetaLayer] P1-R11-10: 同交易日内已执行过恢复操作，拒绝重复恢复"

                    "caller=%s today=%s last_resume=%s",

                    caller_id, _today_key, getattr(drawdown_monitor, '_last_resume_time', 'N/A'),

                )

                return False

            logging.critical(

                "[SafetyMetaLayer] _人工确认恢复交易！日回撤硬停止已解除，交易恢复正则"

                "caller_id=%s timestamp=%s",

                caller_id, _get_tz_aware_now().isoformat()

            )



            _market_safe = True

            if circuit_breaker_service:

                _market_safe = circuit_breaker_service.verify_market_safety_before_resume(

                    drawdown_monitor._equity_series,

                    drawdown_monitor._daily_start_equity,

                    drawdown_monitor._daily_peak_equity,

                    drawdown_monitor._daily_drawdown,

                )

            if not _market_safe:

                logging.critical(

                    "[SafetyMetaLayer] INV-RSK-01: 市场安全验证未通过，恢复被拒绝! "

                    "caller_id=%s, 市场仍处于异常状态",

                    caller_id,

                )

                return False



            drawdown_monitor._daily_hard_stop_triggered = False

            drawdown_monitor._daily_new_open_blocked = False

            drawdown_monitor._daily_drawdown = 0.0

            drawdown_monitor._last_resume_date = _today_key

            drawdown_monitor._last_resume_time = _now_t

            if stats is not None:

                stats['confirm_resume_history'] = stats.get('confirm_resume_history', [])

                resume_record = {

                    'timestamp': _get_tz_aware_now().isoformat(),

                    'caller_id': caller_id,

                    'daily_drawdown_at_resume': drawdown_monitor._daily_drawdown,

                }

                stats['confirm_resume_history'].append(resume_record)



            try:

                if not hasattr(self, "_state_store") or self._state_store is None:

                    self._state_store = CircuitBreakerStateStore(self.params)

                self._state_store.write_compliance_audit(

                    caller_id, approval_context, drawdown_monitor._daily_drawdown,

                )

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logging.warning("[SafetyMetaLayer] CMP-P1-02: 合规审批记录写入失败: %s", e)

            return True

