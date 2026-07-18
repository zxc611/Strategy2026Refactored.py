# MODULE_ID: M1-272
"""strategy_ecosystem.services — 合并 _ops / _governance / _trading_ev"""
from __future__ import annotations

import logging
import os
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

from infra._helpers import get_logger  # R9-5
from infra.metrics_registry import count_call
from infra.shared_utils import CHINA_TZ
from signal.plr_calculator import get_plr_calculator

from strategy.strategy_ecosystem._models import (
    EcosystemTradeRecord,
    SlotState,
    StrategySlot,
    VALID_STRATEGY_TRANSITIONS,
    OBSERVATION_PERIOD_SEC,
)

logger = get_logger(__name__)  # R9-5


# ============================================================
# Section 1: OpsService (原 _ops.py)
# ============================================================

class OpsService:
    """并行运行、回归检查、紧急降级相关方法"""

    def enable_parallel_run(self, old_strategy: str, new_strategy: str,
                            max_duration_sec: float = 3600.0) -> Dict[str, Any]:
        """UPG-P1-03修复: 启用并行运行模式"""
        with self._lock:
            self._parallel_run_mode = True
            self._parallel_run_old_strategy = old_strategy
            self._parallel_run_new_strategy = new_strategy
            self._parallel_run_start_time = time.time()
            self._parallel_run_max_duration_sec = max_duration_sec
            logger.info(
                "UPG-P1-03: 并行运行模式已启用 old=%s new=%s max_duration=%.0fs",
                old_strategy, new_strategy, max_duration_sec,
            )
            return {
                'status': 'PARALLEL_RUN_ENABLED',
                'old_strategy': old_strategy,
                'new_strategy': new_strategy,
                'max_duration_sec': max_duration_sec,
            }

    def disable_parallel_run(self, switch_to_new: bool = True) -> Dict[str, Any]:
        """UPG-P1-03修复: 禁用并行运行模式"""
        with self._lock:
            target = (self._parallel_run_new_strategy if switch_to_new
                      else self._parallel_run_old_strategy)
            self._parallel_run_mode = False
            old_strategy = self._parallel_run_old_strategy
            new_strategy = self._parallel_run_new_strategy
            self._parallel_run_old_strategy = None
            self._parallel_run_new_strategy = None

            if target and target != self._active_strategy:
                self.switch_active_strategy(target)

            logger.info(
                "UPG-P1-03: 并行运行模式已禁用 switch_to_new=%s target=%s",
                switch_to_new, target,
            )
            return {
                'status': 'PARALLEL_RUN_DISABLED',
                'switched_to': target,
                'old_strategy': old_strategy,
                'new_strategy': new_strategy,
            }

    def is_parallel_run_active(self) -> bool:
        """UPG-P1-03修复: 检查是否处于并行运行模式"""
        with self._lock:
            if not self._parallel_run_mode:
                return False
            elapsed = time.time() - self._parallel_run_start_time
            if elapsed > self._parallel_run_max_duration_sec:
                logger.warning(
                    "UPG-P1-03: 并行运行超时(%.0fs > %.0fs), 自动切换到新策略",
                    elapsed, self._parallel_run_max_duration_sec,
                )
                self.disable_parallel_run(switch_to_new=True)
                return False
            return True

    def regression_check(self) -> Dict[str, Any]:
        """UPG-P1-13修复: 回归检查关键不变量"""
        result = {'passed': True, 'violations': [], 'checked_invariants': 0}
        with self._lock:
            total_alloc = sum(s.capital_allocation for s in [
                self._master, self._divergence, self._other,
                self._spring, self._arbitrage, self._market_making,
            ])
            result['checked_invariants'] += 1
            if abs(total_alloc - 1.0) > 0.05:
                result['violations'].append(
                    f"资金分配总和={total_alloc:.4f} != 1.0 (偏差>5%)"
                )
                result['passed'] = False

            active_count = sum(1 for s in [
                self._master, self._divergence, self._other,
                self._spring, self._arbitrage, self._market_making,
            ] if s.state == SlotState.ACTIVE and not s.paused)
            result['checked_invariants'] += 1
            if active_count < 1:
                result['violations'].append("无活跃策略(至少需要1个)")
                result['passed'] = False

            for s in [self._master, self._divergence, self._other,
                      self._spring, self._arbitrage, self._market_making]:
                if s.state == SlotState.ACTIVE and not s.paused and s.expected_value < 0:
                    if s.total_pnl != 0 or s.consecutive_losses > 3:
                        result['checked_invariants'] += 1
                        result['violations'].append(
                            f"策略{s.strategy_id}期望值={s.expected_value:.4f} < 0"
                        )
                        result['passed'] = False

            for s in [self._master, self._divergence, self._other,
                      self._spring, self._arbitrage, self._market_making]:
                result['checked_invariants'] += 1
                if s.state not in VALID_STRATEGY_TRANSITIONS:
                    result['violations'].append(
                        f"策略{s.strategy_id}状态={s.state}不在合法状态集中"
                    )
                    result['passed'] = False

        if result['violations']:
            logger.warning("UPG-P1-13: 回归检查失败 violations=%s", result['violations'])
        else:
            logger.info("UPG-P1-13: 回归检查通过 checked=%d", result['checked_invariants'])
        return result

    def emergency_degrade(self, target_active_count: int = 1,
                          caller_id: str = "unknown",
                          reason: str = "") -> Dict[str, Any]:
        """OPS-04修复: 紧急降级 — 减少活跃策略数量"""
        result = {
            'degraded_strategies': [],
            'active_strategies': [],
            'result_per_strategy': {},
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'caller_id': caller_id,
            'reason': reason,
            'target_active_count': target_active_count,
        }
        priority_order = ['master', 'spring', 'reverse', 'other', 'arbitrage', 'market_making']

        with self._lock:
            active_slots = []
            for sid in priority_order:
                slot = self._get_slot(sid)
                if slot and slot.state == SlotState.ACTIVE and not slot.paused:
                    active_slots.append(sid)

            to_pause = []
            for sid in priority_order:
                if sid in active_slots and len(active_slots) - len(to_pause) > target_active_count:
                    to_pause.append(sid)

            for sid in to_pause:
                slot = self._get_slot(sid)
                if slot:
                    slot.paused = True
                    slot.pause_timestamp = time.time()
                    slot.pause_reason = f'EMERGENCY_DEGRADE: {reason or "ops_degrade"}'
                    slot.state = SlotState.STANDBY
                    result['degraded_strategies'].append(sid)
                    result['result_per_strategy'][sid] = 'paused'
                    logger.warning(
                        "[OPS-04] 策略%s已降级暂停 reason=%s caller=%s",
                        sid, reason, caller_id,
                    )

            for sid in priority_order:
                slot = self._get_slot(sid)
                if slot and not slot.paused and slot.state == SlotState.ACTIVE:
                    result['active_strategies'].append(sid)

        try:
            from risk.risk_service import operations_audit_log
            operations_audit_log(
                action='EMERGENCY_DEGRADE',
                operator=caller_id,
                result='success',
                detail=result,
            )
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass

        logger.critical(
            "[OPS-04] emergency_degrade 完成: degraded=%s, active=%s, caller=%s",
            result['degraded_strategies'], result['active_strategies'], caller_id,
        )
        return result


OpsMixin = OpsService


# ============================================================
# Section 2: GovernanceService (原 _governance.py)
# ============================================================

class GovernanceService:
    """治理引擎、灰度部署、策略恢复相关方法"""

    def configure_grayscale(self, grayscale_config: Dict[str, Any]) -> Dict[str, Any]:
        """P2-项17修复: 基于配置的灰度部署"""
        strategy_id = grayscale_config.get('strategy_id', '')
        traffic_pct = grayscale_config.get('traffic_pct', 10)
        duration_sec = grayscale_config.get('duration_sec', 3600)
        rollback_on_error = grayscale_config.get('rollback_on_error', True)

        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None:
                return {'success': False, 'error': f'未知策略: {strategy_id}'}

            self._grayscale_config = {
                'strategy_id': strategy_id,
                'traffic_pct': traffic_pct,
                'duration_sec': duration_sec,
                'rollback_on_error': rollback_on_error,
                'start_time': time.time(),
                'active': True,
            }
            logger.info(
                "[StrategyEcosystem] 灰度部署配置: strategy=%s traffic=%.0f%% duration=%.0fs rollback=%s",
                strategy_id, traffic_pct, duration_sec, rollback_on_error,
            )
            return {'success': True, 'config': self._grayscale_config}

    def get_grayscale_status(self) -> Dict[str, Any]:
        """P2-项17修复: 获取当前灰度部署状态"""
        config = getattr(self, '_grayscale_config', None)
        if config is None or not config.get('active'):
            return {'active': False}
        elapsed = time.time() - config.get('start_time', 0)
        remaining = max(0, config.get('duration_sec', 0) - elapsed)
        return {
            'active': True,
            'strategy_id': config.get('strategy_id', ''),
            'traffic_pct': config.get('traffic_pct', 0),
            'remaining_sec': round(remaining, 1),
            'elapsed_sec': round(elapsed, 1),
        }

    def reload_param_sets(self, param_updates: Dict[str, Any],
                           grayscale: bool = False,
                           grayscale_pct: float = 10.0) -> Dict[str, Any]:
        """P2-项18修复: 热更新参数集，支持灰度发布"""
        result = {'updated': [], 'grayscale': grayscale}

        with self._lock:
            for strategy_id, updates in param_updates.items():
                slot = self._get_slot(strategy_id)
                if slot is None:
                    result[strategy_id] = 'not_found'
                    continue

                if strategy_id == 'master':
                    if 's1_params' in updates:
                        self._s1_params.update(updates['s1_params'])
                    if 's2_params' in updates:
                        self._s2_params.update(updates['s2_params'])
                    result['updated'].append(strategy_id)
                else:
                    params_attr = f'_{strategy_id}_params'
                    if hasattr(self, params_attr):
                        getattr(self, params_attr).update(updates)
                        result['updated'].append(strategy_id)

                if grayscale:
                    slot._grayscale_active = True
                    slot._grayscale_pct = grayscale_pct
                    logger.info(
                        "[StrategyEcosystem] 灰度热更新: strategy=%s pct=%.0f%%",
                        strategy_id, grayscale_pct,
                    )

        return result

    def run_governance_checks(self, returns: List[float] = None,
                              window_results: List[Dict] = None,
                              pnl_attribution: Dict[str, float] = None,
                              param_sources: Dict[str, str] = None) -> Dict[str, Any]:
        """手册14节要求: 执行所有治理检测器(E7-E13/WF6-10)"""
        results = {}
        any_triggered = False
        degradation_reasons = []

        if self._wf_elimination_checker is not None:
            try:
                if window_results is not None:
                    wf_result = self._wf_elimination_checker.check_all(window_results)
                    results['wf6_10'] = wf_result
                    if wf_result.get('triggered', False):
                        any_triggered = True
                        degradation_reasons.append('WF6-10: 样本外窗口消除触发')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.debug("[Governance] WF6-10检查异常: %s", e)

        if self._e7_checker is not None:
            try:
                if pnl_attribution is not None:
                    e7_result = self._e7_checker.check(pnl_attribution)
                    results['e7'] = e7_result
                    if e7_result.get('triggered', False) or e7_result.get('e7_triggered', False):
                        any_triggered = True
                        degradation_reasons.append('E7: 收益不可解释性触发')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.debug("[Governance] E7检查异常: %s", e)

        if self._e8e9e10_checker is not None:
            try:
                if returns is not None:
                    e8e9e10_result = self._e8e9e10_checker.check_all(returns=returns)
                    results['e8e9e10'] = e8e9e10_result
                    if e8e9e10_result.get('triggered', False) or e8e9e10_result.get('e8e9e10_triggered', False):
                        any_triggered = True
                        degradation_reasons.append('E8/E9/E10: 综合风险触发')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.debug("[Governance] E8E9E10检查异常: %s", e)

        if self._e11_checker is not None:
            try:
                if param_sources is not None:
                    e11_result = self._e11_checker.check(param_sources)
                    results['e11'] = e11_result
                    if e11_result.get('triggered', False):
                        any_triggered = True
                        degradation_reasons.append('E11: 量化来源缺失触发')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.debug("[Governance] E11检查异常: %s", e)

        if any_triggered:
            self._governance_degraded = True
            self._governance_degradation_reasons = degradation_reasons
            for strategy_id in ['master', 'reverse', 'spring', 'other']:
                self.freeze_strategy_slot(strategy_id)
            logger.critical(
                "[StrategyEcosystem] 治理引擎触发自动降级! 原因: %s",
                '; '.join(degradation_reasons)
            )
            try:
                from strategy.shadow_strategy_facade import get_shadow_strategy_engine
                shadow = get_shadow_strategy_engine()
                if shadow:
                    shadow.record_governance_degradation(degradation_reasons)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logger.warning("[Governance] 通知影子引擎异常: %s", e)
        else:
            self._governance_degraded = False
            self._governance_degradation_reasons = []

        results['degraded'] = any_triggered
        results['degradation_reasons'] = degradation_reasons
        return results

    def is_governance_degraded(self) -> bool:
        """检查治理引擎是否已触发降级"""
        return getattr(self, '_governance_degraded', False)

    def resume_strategy(self, strategy_id: str) -> bool:
        resume_cooldown_sec = getattr(self, '_resume_cooldown_sec', 0) or float(
            os.environ.get('RESUME_COOLDOWN_SEC', '900.0'))
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot and slot.paused:
                if slot.frozen:
                    logger.warning("[StrategyEcosystem] %s策略仍处于冻结态，禁止直接恢复", strategy_id)
                    return False
                if getattr(self, '_governance_degraded', False):
                    logger.warning("[StrategyEcosystem] 治理降级仍生效，禁止恢复策略%s", strategy_id)
                    return False
                if slot.pause_timestamp > 0:
                    elapsed = time.time() - slot.pause_timestamp
                    if elapsed < resume_cooldown_sec:
                        logger.warning(
                            "[StrategyEcosystem] %s策略暂停仅%.1f秒(冷却期%.0f秒)，禁止恢复",
                            strategy_id, elapsed, resume_cooldown_sec,
                        )
                        return False
                slot.paused = False
                slot.pause_reason = ''
                slot.pause_timestamp = 0.0
                slot._observation_period_until = time.time() + OBSERVATION_PERIOD_SEC
                slot._observation_position_scale = 0.5
                try:
                    from risk.risk_service import get_risk_service
                    _rs = get_risk_service()
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                    _rs = None
                if _rs and hasattr(_rs, 'confirm_alpha_decay_recovery'):
                    _rs.confirm_alpha_decay_recovery()
                logger.info("[StrategyEcosystem] %s策略已恢复(观察期%.0f秒, 仓位缩放%.0f%%)",
                            strategy_id, OBSERVATION_PERIOD_SEC, slot._observation_position_scale * 100)
                return True
        return False


GovernanceMixin = GovernanceService


# ============================================================
# Section 3: TradingEVService (原 _trading_ev.py)
# ============================================================

class TradingEVService:
    """交易记录、EV计算、PLR统计相关方法"""

    def _get_slot(self, strategy_id: str) -> Optional[StrategySlot]:
        slot_map = {
            'master': self._master,
            'divergence': self._divergence,
            'other': self._other,
            'spring': self._spring,
            'arbitrage': self._arbitrage,
            'market_making': self._market_making,
        }
        return slot_map.get(strategy_id)

    def _is_same_delta_direction(self, my_id: str, my_direction: str, other_id: str) -> bool:
        other_slot = self._master if other_id == 'master' else self._divergence
        other_dir = getattr(other_slot, 'last_direction', '')
        if not other_dir:
            return True
        other_open_reason = getattr(other_slot, 'last_open_reason', '')
        if other_open_reason and any(kw in other_open_reason.upper() for kw in ('HEDGE', 'DELTA_HEDGE', '对冲')):
            return False
        # FIX-DIR-DUAL-TRACK: 兼容策略内部值('BUY'/'SELL')、record值('long'/'short')和平台规范值('0'/'1')
        my_delta_sign = 1 if my_direction in ('long', 'BUY', 'buy', '0') else -1
        other_delta_sign = 1 if other_dir in ('long', 'BUY', 'buy', '0') else -1
        return my_delta_sign * other_delta_sign > 0

    def _check_cross_strategy_risk(self, instrument_id: str, direction: str) -> str:
        try:
            from position.position_service import (
                aggregate_greeks_exposure,
                get_cross_strategy_risk_guard,
            )
            guard = get_cross_strategy_risk_guard()
            pos_svc = self._get_position_service()
            if pos_svc is None:
                return 'PASS'
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            level, reason, detail = guard.check(exposure)
            if level in (guard.BLOCK, guard.CIRCUIT_BREAK):
                return level
            return 'PASS'
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem._check_cross_strategy_risk] Error: %s, fail-safe阻断", e)
            return 'BLOCK'

    def _get_position_service(self):
        try:
            from position.position_service import get_position_service
            return get_position_service()
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            return None

    def record_spring_trade(self, direction: str, pnl: float = 0.0):
        logging.info("[R16-P2-2.4] BoxSpring信号传递: direction=%s active_strategy=%s spring_position_count=%d pnl=%.2f",
                     direction, self._active_strategy, self._spring.position_count, pnl)
        with self._lock:
            self._spring.position_count += 1
            self._spring_trades.append(EcosystemTradeRecord(
                trade_id=self._generate_trade_id(),
                strategy_id='spring',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                action='OPEN',
                direction=direction,
                open_reason='BOX_SPRING',
                market_state=self._active_strategy,
            ))
            self._stats['total_trades'] += 1
            self._stats['spring_trades'] += 1
            _sub_id = 's1_hft' if 'hft' in str(self._active_strategy).lower() else 's2_resonance'
            if hasattr(self._master, 'record_sub_strategy_trade'):
                self._master.record_sub_strategy_trade(_sub_id, pnl)
            if self._snapshot_collector is not None:
                try:
                    import numpy as np
                    self._snapshot_collector.capture_signal_point(
                        timestamp=np.datetime64('now'),
                        trigger_detail=f"spring {direction}",
                    )
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                    logging.debug("[StrategyEcosystem] spring snapshot error: %s", e)

    def process_other_strategy_signal(
        self,
        current_price: float,
        resonance_direction: str = '',
        resonance_strength: float = 0.0,
        current_iv: float = 0.0,
        flow_imbalance: float = 0.0,
        cvd_slope: float = 0.0,
        instrument_id: str = '',
    ) -> Dict[str, Any]:
        if self._other.paused:
            return {'action': 'skip', 'reason': 'other策略已暂停'}

        extreme = self._box_detector.classify_extreme_state(
            current_price=current_price,
            resonance_direction=resonance_direction,
            resonance_strength=resonance_strength,
            current_iv=current_iv,
            flow_imbalance=flow_imbalance,
            cvd_slope=cvd_slope,
            instrument_id=instrument_id,
        )

        if not extreme.tradeable:
            return {
                'action': 'no_signal',
                'extreme_state': extreme.to_dict(),
                'reason': '极值不满足交易条件',
            }

        # [FIX-20260712-S3] 假突破过滤 — 上策H-Rev: 过滤假突破信号
        if extreme.extreme_type:
            _pass_fb = self._box_detector.check_false_breakout(current_price, extreme.extreme_type)
            if not _pass_fb:
                return {
                    'action': 'filtered',
                    'extreme_state': extreme.to_dict(),
                    'reason': '假突破过滤: 价格回落超过50%',
                }

        trade_dir = self._box_detector.determine_trade_direction(extreme)

        if not trade_dir:
            return {
                'action': 'no_signal',
                'extreme_state': extreme.to_dict(),
                'reason': '无明确交易方向',
            }

        allowed, reason = self.check_mutual_exclusion('other', trade_dir)
        if not allowed:
            return {
                'action': 'blocked',
                'extreme_state': extreme.to_dict(),
                'reason': reason,
            }

        with self._lock:
            record = EcosystemTradeRecord(
                trade_id=self._generate_trade_id(),
                strategy_id='other',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                action='OPEN',
                direction=trade_dir,
                open_reason='OTHER_SCALP',
                market_state='other',
            )
            self._other_trades.append(record)
            self._other.position_count += 1
            self._stats['other_trades'] += 1
            self._stats['total_trades'] += 1

        return {
            'action': 'open',
            'direction': trade_dir,
            'strategy_id': 'other',
            'open_reason': 'OTHER_SCALP',
            'extreme_state': extreme.to_dict(),
            'params': self._box_detector.params.to_dict(),
            'trade_id': record.trade_id,
        }

    def compute_defense_net_benefit(
        self,
        defense_mode_loss: float,
        ignore_mode_loss: float,
        ignore_mode_win_trades: int,
        ignore_mode_avg_win: float,
        other_state_ratio: float,
    ) -> Dict[str, Any]:
        """P1-裂缝31：other状态防御净收益量化（含机会成本）"""
        defense_opportunity_cost = ignore_mode_win_trades * ignore_mode_avg_win * other_state_ratio
        net_benefit = (ignore_mode_loss - defense_mode_loss) - defense_opportunity_cost
        should_defend = net_benefit > 0

        if not should_defend and defense_mode_loss < ignore_mode_loss:
            logger.info(
                "[StrategyEcosystem] P1-裂缝31: 防御减少损失%.2f但机会成本%.2f更高, "
                "净收益=%.2f, 不启用防御",
                ignore_mode_loss - defense_mode_loss, defense_opportunity_cost, net_benefit,
            )

        return {
            "defense_mode_loss": round(defense_mode_loss, 2),
            "ignore_mode_loss": round(ignore_mode_loss, 2),
            "defense_opportunity_cost": round(defense_opportunity_cost, 2),
            "net_benefit": round(net_benefit, 2),
            "should_defend": should_defend,
            "defense_savings": round(ignore_mode_loss - defense_mode_loss, 2),
        }

    @count_call()
    def record_strategy_pnl(
        self,
        strategy_id: str,
        pnl: float,
        commission: float = 0.0,
        slippage: float = 0.0,
        target_plr: float = 0.0,
    ) -> None:
        with self._lock:
            net_pnl = pnl - commission - slippage
            slot = self._get_slot(strategy_id)
            if slot is None:
                return

            _y = net_pnl - slot._kahan_c_total
            _t = slot.total_pnl + _y
            slot._kahan_c_total = (_t - slot.total_pnl) - _y
            slot.total_pnl = _t

            trades = self._get_trades(strategy_id)
            if trades:
                for trade in reversed(trades):
                    if trade.is_open:
                        trade.pnl = pnl
                        trade.net_pnl = net_pnl
                        trade.is_open = False
                        trade.target_plr = target_plr or slot.target_plr
                        if slot._loss_pnl_sum > 1e-10:
                            trade.actual_plr = slot._win_pnl_sum / slot._loss_pnl_sum if abs(slot._loss_pnl_sum) > 1e-10 else 1e9
                        elif net_pnl > 0:
                            trade.actual_plr = 0.0
                        else:
                            trade.actual_plr = 0.0
                        slot.position_count = max(0, slot.position_count - 1)
                        _y = net_pnl - slot._kahan_c_closed
                        _t = slot._closed_pnl_sum + _y
                        slot._kahan_c_closed = (_t - slot._closed_pnl_sum) - _y
                        slot._closed_pnl_sum = _t
                        slot._closed_count += 1
                        if net_pnl > 0:
                            _y = net_pnl - slot._kahan_c_win
                            _t = slot._win_pnl_sum + _y
                            slot._kahan_c_win = (_t - slot._win_pnl_sum) - _y
                            slot._win_pnl_sum = _t
                            slot._win_count += 1
                        elif net_pnl < 0:
                            _abs_pnl = abs(net_pnl)
                            _y = _abs_pnl - slot._kahan_c_loss
                            _t = slot._loss_pnl_sum + _y
                            slot._kahan_c_loss = (_t - slot._loss_pnl_sum) - _y
                            slot._loss_pnl_sum = _t
                            slot._loss_count += 1
                        break
            self.update_plr_stats(strategy_id)
            plr_stats = self.compute_plr_stats(strategy_id)
            if plr_stats:
                plr_ratio = plr_stats.get('plr_ratio', 0.0)
                consecutive_losses = plr_stats.get('consecutive_losses', 0)
                if plr_ratio < self.PLR_FREEZE_RATIO_THRESHOLD and consecutive_losses >= self.PLR_FREEZE_CONSECUTIVE_LOSSES:
                    self.freeze_strategy_slot(strategy_id)
                    logging.warning("[StrategyEcosystem] 策略%s因PLR过低(%.2f)和连续亏损(%d次)被自动冻结",
                                    strategy_id, plr_ratio, consecutive_losses)

            try:
                _plr_calc = get_plr_calculator(default_target_plr=slot.target_plr)
                _trade_records = []
                trades_deque = self._get_trades(strategy_id)
                if trades_deque:
                    for t in trades_deque:
                        if not t.is_open:
                            _trade_records.append({'pnl': getattr(t, 'net_pnl', getattr(t, 'pnl', 0.0))})
                if _trade_records:
                    _plr_calc.update_from_trades(strategy_id, _trade_records)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _plr_err:
                logging.debug("[StrategyEcosystem] ProfitLossRatioCalculator同步更新异常: %s", _plr_err)

    def _get_trades(self, strategy_id: str) -> Optional[deque]:
        trades_map = {
            'master': self._master_trades,
            'reverse': self._reverse_trades,
            'other': self._other_trades,
            'spring': self._spring_trades,
            'arbitrage': self._arbitrage_trades,
            'market_making': self._market_making_trades,
        }
        return trades_map.get(strategy_id)

    def compute_expected_value(self, strategy_id: str) -> float:
        trades = self._get_trades(strategy_id)
        if trades is None:
            return 0.0
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)

    def compute_plr_stats(self, strategy_id: str) -> Dict[str, float]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None or slot._closed_count == 0:
                return {}
            n_win = slot._win_count
            n_loss = slot._loss_count
            total_win = slot._win_pnl_sum
            total_loss = slot._loss_pnl_sum
            avg_win = total_win / n_win if n_win > 0 else 0.0
            avg_loss = total_loss / n_loss if n_loss > 0 else 0.0
            win_rate = n_win / slot._closed_count if slot._closed_count > 0 else 0.0
            loss_rate = n_loss / slot._closed_count if slot._closed_count > 0 else 0.0
            expected_value = avg_win * win_rate - avg_loss * loss_rate
            profit_factor = total_win / total_loss if total_loss > 1e-10 else 0.0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 0.0
            return {
                'expected_value': expected_value,
                'win_rate': win_rate,
                'loss_rate': loss_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'win_loss_ratio': win_loss_ratio,
                'total_trades': slot._closed_count,
                'win_count': n_win,
                'loss_count': n_loss,
                'total_pnl': slot.total_pnl,
                'target_plr': slot.target_plr,
                'current_plr': profit_factor,
                'plr_ratio': profit_factor / slot.target_plr if slot.target_plr > 0 else 0.0,
            }

    @count_call()
    def update_plr_stats(self, strategy_id: str) -> Dict[str, float]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None or slot._closed_count == 0:
                return {}
            total_win = slot._win_pnl_sum
            total_loss = slot._loss_pnl_sum
            n_win = slot._win_count
            n_loss = slot._loss_count
            slot.profit_factor = total_win / total_loss if total_loss > 1e-10 else 0.0
            avg_win = total_win / n_win if n_win > 0 else 0.0
            avg_loss = total_loss / n_loss if n_loss > 0 else 0.0
            slot.win_loss_ratio = (avg_win / avg_loss) if avg_loss > 1e-10 else 0.0
            trades = self._get_trades(strategy_id)
            if trades:
                closed = [t for t in trades if not t.is_open]
                streak = 0
                max_streak = 0
                for t in reversed(closed):
                    if t.net_pnl < 0:
                        streak += 1
                        max_streak = max(max_streak, streak)
                    else:
                        break
                slot.consecutive_losses = max_streak
            logger.info(
                "[PLR] strategy=%s win_loss_ratio=%.2f profit_factor=%.2f consecutive_losses=%d target_plr=%.1f plr_ratio=%.2f",
                strategy_id, slot.win_loss_ratio, slot.profit_factor,
                slot.consecutive_losses, slot.target_plr,
                slot.profit_factor / slot.target_plr if slot.target_plr > 0 else 0.0,
            )
            return {
                'win_loss_ratio': slot.win_loss_ratio,
                'profit_factor': slot.profit_factor,
                'consecutive_losses': slot.consecutive_losses,
            }

    def check_absolute_ev_bottomline(self) -> Dict[str, Any]:
        with self._lock:
            results = {}
            any_breached = False

            for sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making'):
                slot = self._get_slot(sid)
                if slot._closed_count > 0:
                    ev = slot._closed_pnl_sum / slot._closed_count
                else:
                    ev = 0.0
                slot.expected_value = ev

                breached = ev < 0 and slot._closed_count >= self.MIN_TRADES_FOR_EV

                if breached and not slot.paused:
                    slot.paused = True
                    slot.pause_timestamp = time.time()
                    slot.pause_reason = f'期望值={ev:.4f}<0'
                    any_breached = True
                    self._stats['ev_breaches'] += 1
                    logger.critical(
                        "[StrategyEcosystem] %s策略绝对期望值底线突破! EV=%.4f, 暂停!",
                        sid, ev,
                    )

                results[sid] = {
                    'ev': ev,
                    'breached': breached,
                    'paused': slot.paused,
                }

            shadow_ev_info = {}
            try:
                from strategy.shadow_strategy_facade import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_absolute_ev_paused():
                        logger.critical(
                            "[StrategyEcosystem] 影子引擎绝对EV暂停激活，所有策略暂停"
                        )
                        for sid in ('master', 'reverse', 'other', 'spring'):
                            slot = self._get_slot(sid)
                            if slot is not None and not slot.paused:
                                slot.paused = True
                                slot.pause_timestamp = time.time()
                                slot.pause_reason = 'shadow_engine_ev_paused'
                                any_breached = True
                                self._stats['ev_breaches'] += 1
                                logger.critical(
                                    "[StrategyEcosystem] %s策略被影子引擎EV暂停触发暂停!", sid
                                )
                    shadow_ev = _sse.get_master_expected_value()
                    shadow_ev_info = {
                        'shadow_master_ev': shadow_ev,
                        'shadow_ev_paused': _sse.is_absolute_ev_paused(),
                        'shadow_degradation': _sse.is_degradation_active(),
                    }
                    if shadow_ev < 0:
                        logger.warning(
                            "[StrategyEcosystem] 影子引擎master EV为负(%.4f)，融合到master策略EV检查",
                            shadow_ev,
                        )
                        _master_slot = self._get_slot('master')
                        if _master_slot is not None and not _master_slot.paused:
                            _master_slot.paused = True
                            _master_slot.pause_timestamp = time.time()
                            _master_slot.pause_reason = f'shadow_ev_negative={shadow_ev:.4f}'
                            any_breached = True
                            self._stats['ev_breaches'] += 1
                            logger.critical(
                                "[StrategyEcosystem] master策略被影子引擎负EV触发暂停! shadow_ev=%.4f",
                                shadow_ev,
                            )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _sse_err:
                shadow_ev_info = {'error': str(_sse_err)}
                logger.debug("[StrategyEcosystem] 影子引擎EV融合检查跳过: %s", _sse_err)

            cascade_result = {}
            # [DRY-RUN A-04] dry_run模式下跳过CascadeJudge评判，仅记录日志直接放行
            _dry_run_mode = False
            try:
                from config.config_service import get_cached_params as _gcp_dry
                _dry_run_mode = bool(_gcp_dry().get('dry_run_mode', False))
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _dr_err:
                logging.debug("[DRY-RUN] dry_run_mode读取失败(默认False): %s", _dr_err)
            if _dry_run_mode:
                cascade_result = {'passed': True, 'score': 1.0, 'fatal': None, 'dry_run_skipped': True}
                logger.info("[DRY-RUN] 瀑布式评判已跳过(dry_run_mode=True)")
            else:
                try:
                    import sys, os
                    _project_root = os.path.dirname(os.path.abspath(__file__))
                    if _project_root not in sys.path and os.path.isdir(_project_root):
                        sys.path.insert(0, _project_root)
                    from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
                    _slot = self._get_slot('master')
                    _live_metrics = {
                        'sharpe': getattr(_slot, 'sharpe', 0.0),
                        'calmar': getattr(_slot, 'calmar', 0.0),
                        'sortino_ratio': getattr(_slot, 'sortino', getattr(_slot, 'sharpe', 0.0) * 1.2),
                        'profit_loss_ratio': getattr(_slot, 'profit_factor', 1.0),
                        'max_consecutive_losses': getattr(_slot, 'consecutive_losses', 0),
                        'win_rate': getattr(_slot, 'win_rate', 0.5),
                        'total_trades': getattr(_slot, 'total_trades', 0),
                        'max_flat_period_days': getattr(_slot, 'max_flat_period_days', 999),
                        'peak_margin_used': getattr(_slot, 'peak_margin_used', 0.0),
                        'sharpe_ci_lower': getattr(_slot, 'sharpe_ci_lower', 0.0),
                        'sharpe_ci_upper': getattr(_slot, 'sharpe_ci_upper', 0.0),
                        'sharpe_ci_width': getattr(_slot, 'sharpe_ci_width', 0.0),
                        'alpha_action': getattr(_slot, 'alpha_action', 'hold'),
                    }
                    _adapted = adapt_backtest_result(_live_metrics, strategy_type=getattr(_slot, 'strategy_type', ''))
                    try:
                        from config.config_service import get_cached_params
                        _params_for_cascade = get_cached_params()
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                        _params_for_cascade = None
                    _capital_scale_str = self._capital_scale.value if self._capital_scale else 'medium'
                    _cascade = CascadeJudge.from_config(capital_scale=_capital_scale_str, params=_params_for_cascade)
                    _cascade_report = _cascade.judge(_adapted)
                    cascade_result = {
                        'passed': _cascade_report.passed,
                        'score': _cascade_report.final_score,
                        'fatal': _cascade_report.fatal_reason,
                    }
                    if not _cascade_report.passed:
                        logger.warning("[StrategyEcosystem] 瀑布式评判否决: %s", _cascade_report.fatal_reason)

                    _ci_width = _live_metrics.get('sharpe_ci_width', 0.0)
                    _alpha_action = _live_metrics.get('alpha_action', 'hold')
                    try:
                        from param_pool.state_param_sets import BACKTEST_THRESHOLDS as _CI_THRESHOLDS
                    except ImportError:
                        try:
                            from param_pool.backtest_config import BACKTEST_THRESHOLDS as _CI_THRESHOLDS
                        except ImportError:
                            _CI_THRESHOLDS = {}
                    _max_ci_width = _CI_THRESHOLDS.get('max_ci_width', 0.5)
                    _min_ci_width = _CI_THRESHOLDS.get('min_ci_width', 0.05)
                    if _ci_width > _max_ci_width:
                        logger.warning("[StrategyEcosystem] Alpha CI过宽: ci_width=%.4f > max=%.4f，CI置信区间不足", _ci_width, _max_ci_width)
                    if _alpha_action == 'eliminate':
                        cascade_result['ci_veto'] = 'eliminate'
                        logger.warning("[StrategyEcosystem] Alpha CI否决(eliminate): ci_width=%.4f, 策略应从生态中淘汰", _ci_width)
                    elif _alpha_action == 'reduce_weight':
                        cascade_result['ci_veto'] = 'reduce_weight'
                        logger.warning("[StrategyEcosystem] Alpha CI降权(reduce_weight): ci_width=%.4f, 资金分配降权", _ci_width)
                    elif _alpha_action == 'flag':
                        cascade_result['ci_flag'] = True
                        logger.info("[StrategyEcosystem] Alpha CI标注(flag): ci_width=%.4f, Sharpe中等可靠", _ci_width)

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
                    cascade_result = {'error': str(_e)}

            try:
                from strategy.strategy_ecosystem import validate_ev_cache_time_decay
                _ev_decay_result = validate_ev_cache_time_decay(ecosystem=self)
                if _ev_decay_result and not _ev_decay_result.get('decay_valid', True):
                    logging.warning("[R16-P1-03] EV缓存时间衰减验证失败: %s", _ev_decay_result)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _evd_err:
                logging.debug("[R16-P1-03] EV衰减验证异常(非阻断): %s", _evd_err)

            try:
                _slot = self._get_slot('master')
                if _slot is not None and _slot._closed_count >= 20:
                    _recent_pnls = [t for t in getattr(_slot, '_recent_closed_pnls', [])[-20:]]
                    if len(_recent_pnls) >= 10:
                        _avg = sum(_recent_pnls) / len(_recent_pnls)
                        _std = (sum((x - _avg) ** 2 for x in _recent_pnls) / len(_recent_pnls)) ** 0.5
                        _sharpe = _avg / _std if _std > 0 else 0
                        if _sharpe < -0.5:
                            logging.warning("[R16-P2-04] Sharpe衰减检测: sharpe=%.2f< -0.5, 建议降级", _sharpe)
                        _neg_pnls = [x for x in _recent_pnls if x < 0]
                        if len(_neg_pnls) >= 5:
                            _down_std = (sum((x - _avg) ** 2 for x in _neg_pnls) / len(_neg_pnls)) ** 0.5
                            _sortino = _avg / _down_std if _down_std > 0 else 0
                            if _sortino < -0.3:
                                logging.warning("[R16-P2-04] Sortino衰减检测: sortino=%.2f< -0.3, 下行风险过大", _sortino)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _sharpe_err:
                logging.debug("[R16-P2-04] Sharpe衰减检测异常(非阻断): %s", _sharpe_err)

            return {
                'any_breached': any_breached,
                'strategies': results,
                'cascade_judge': cascade_result,
                'shadow_engine': shadow_ev_info,
            }


TradingEVMixin = TradingEVService


# P1-裂缝31: 模块级compute_defense_net_benefit包装器
def compute_defense_net_benefit(
    defense_mode_loss: float,
    ignore_mode_loss: float,
    ignore_mode_win_trades: int,
    ignore_mode_avg_win: float,
    other_state_ratio: float,
) -> Dict[str, Any]:
    """other状态防御净收益量化（含机会成本）"""
    defense_opportunity_cost = ignore_mode_win_trades * ignore_mode_avg_win * other_state_ratio
    net_benefit = (ignore_mode_loss - defense_mode_loss) - defense_opportunity_cost
    should_defend = net_benefit > 0
    return {
        "defense_mode_loss": round(defense_mode_loss, 2),
        "ignore_mode_loss": round(ignore_mode_loss, 2),
        "defense_opportunity_cost": round(defense_opportunity_cost, 2),
        "net_benefit": round(net_benefit, 2),
        "should_defend": should_defend,
        "defense_savings": round(ignore_mode_loss - defense_mode_loss, 2),
    }


# validate_ev_cache_time_decay 已迁入 __init__.py，此处使用懒导入避免循环依赖
# 原位置: from strategy.strategy_ecosystem import validate_ev_cache_time_decay
