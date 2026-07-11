# MODULE_ID: M1-270
"""strategy_ecosystem._core - StrategyEcosystem主类(组合所有Mixin)
+ CapitalRoutingService (从。capital_routing.py迁入)
"""
from __future__ import annotations

import logging
import os
import threading
import time
import types  # 用于。_getattr__方法重绑定
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

from ali2026v3_trading.config.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, STRATEGY_MODE_OTHER,
)
from ali2026v3_trading.strategy.box_detector import BoxDetector, BoxProfile, ExtremeState, BoxStrategyParams
from ali2026v3_trading.strategy_judgment.strategy_judgment_facade import CapitalScale
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from ali2026v3_trading.infra.metrics_registry import count_call
from ali2026v3_trading.infra.shared_utils import CHINA_TZ

from ali2026v3_trading.strategy.strategy_ecosystem._models import (
    SlotState,
    StrategySlot,
    CapitalRoute,
    EcosystemTradeRecord,
    EV_CACHE_TTL_DEFAULTS,
    EV_CACHE_TTL,
    OBSERVATION_PERIOD_SEC,
    _require_interface,
)
# EcosystemMonitoringService 懒导入，避免 strategy_monitoring_layer -> _models -> __init__ -> _core 循环依赖
from ali2026v3_trading.strategy.strategy_ecosystem.services import (
    TradingEVService, TradingEVMixin,
    GovernanceService, GovernanceMixin,
    OpsService, OpsMixin,
)

# P-12修复: LiveStrategySelector实盘集成初始化
try:
    from ali2026v3_trading.param_pool.optimization.cycle_sharpe import LiveStrategySelector
    from ali2026v3_trading.infra.metrics_registry import mark_module_loaded
    mark_module_loaded('strategy_ecosystem_live_strategy_selector')
except ImportError as _ie:
    LiveStrategySelector = None
    try:
        from ali2026v3_trading.infra.metrics_registry import mark_module_failed
        mark_module_failed('strategy_ecosystem_live_strategy_selector', _ie)
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
        pass

logger = get_logger(__name__)  # R9-5


# ============================================================
# CapitalRoutingService (从 _capital_routing.py 迁入)
# 职责: 资金路由相关方法
# ============================================================

class CapitalRoutingService:
    """资金路由相关方法"""

    @_require_interface(('_capital_scale', '_master', '_divergence', '_other'))
    def _propagate_capital_scale(self) -> None:
        if self._capital_scale is None:
            return
        scale_str = self._capital_scale.value if hasattr(self._capital_scale, 'value') else str(self._capital_scale).lower()
        try:
            from ali2026v3_trading.governance.mode_engine import ModeEngine
            me = ModeEngine.create_engine(scale_str)
            self._mode_engine = me
            me.register_component('StrategyEcosystem', self)
            result = me.switch_mode(scale_str, reason='ecosystem_capital_scale_change')
            if not result.get('success'):
                logger.warning("[StrategyEcosystem] ModeEngine switch failed: %s", result)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem] ModeEngine unavailable, direct propagation: %s", e)
            self._propagate_capital_scale_fallback(scale_str)

    def _propagate_capital_scale_fallback(self, scale_str: str) -> None:
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            rs = get_risk_service()
            rs.set_capital_scale(scale_str)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem] propagate to RiskService failed: %s", e)
        try:
            from ali2026v3_trading.signal.signal_service import get_signal_service
            ss = get_signal_service()
            if scale_str == 'small':
                ss.enable_plr_filter(min_estimated_plr=2.0)
            elif scale_str == 'medium':
                ss.enable_plr_filter(min_estimated_plr=1.5)
            else:
                ss.disable_plr_filter()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem] propagate to SignalService failed: %s", e)
        try:
            from ali2026v3_trading.config.state_param import get_state_param_manager
            spm = get_state_param_manager()
            spm.set_capital_scale(scale_str)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem] propagate to StateParamManager failed: %s", e)
        try:
            from ali2026v3_trading.strategy.box_spring_strategy_impl import get_box_spring_strategy
            bss = get_box_spring_strategy()
            if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                bss.set_capital_scale(scale_str)
            else:
                setattr(bss, '_capital_scale', scale_str)
                logger.warning("[StrategyEcosystem] BoxSpringStrategy无set_capital_scale方法，使用setattr设计capital_scale=%s", scale_str)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem] propagate to BoxSpringStrategy failed: %s", e)

    def _load_capital_route_params(self) -> Dict[str, float]:
        """R7-M-12修复: 从配置参数读取capital_route_s1~s6"""
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            params = get_cached_params()
            return {
                's1_hft': params.get('capital_route_s1_hft', 0.30),
                's2_minute': params.get('capital_route_s2_minute', 0.20),
                's3_box_extreme': params.get('capital_route_s3_box_extreme', 0.15),
                's4_box_spring': params.get('capital_route_s4_box_spring', 0.15),
                's5_arbitrage': params.get('capital_route_s5_arbitrage', 0.10),
                's6_market_making': params.get('capital_route_s6_market_making', 0.10),
                's7_divergence_reversal': params.get('capital_route_s7_divergence_reversal', 0.20),
            }
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.debug("[R7-M-12] 读取capital_route参数失败，使用类常量默认值: %s", e)
            return {}

    def set_master_substrategy_pause(self, strategy_key: str, paused: bool = True) -> None:
        """允许在共享master槽位内独立暂停S1/S2子策略。"""
        if strategy_key not in self._master_substrategy_pause:
            return
        with self._lock:
            self._master_substrategy_pause[strategy_key] = bool(paused)
        logger.info("[StrategyEcosystem] master子策略暂停开关: %s paused=%s", strategy_key, paused)

    @count_call()
    @_require_interface(('_lock', '_capital_route', '_master', '_divergence', '_other'))
    def route_capital(self, active_state: str) -> Dict[str, float]:
        self.on_bar_update()
        with self._lock:
            cr = self._capital_route
            _route_params = self._load_capital_route_params()

            if not cr.dynamic_enabled:
                return {
                    'master': _route_params.get('s1_hft', cr.master_base),
                    'divergence': _route_params.get('s7_divergence_reversal', cr.divergence_base),
                    'other': _route_params.get('s3_box_extreme', cr.other_base),
                    'spring': _route_params.get('s4_box_spring', self._spring.capital_allocation),
                    'arbitrage': 0.0,  # FIX-20260711-S5S6: 不参与资金分配
                    'market_making': 0.0,  # FIX-20260711-S5S6: 不参与资金分配
                }

            if self._capital_scale == CapitalScale.SMALL:
                spring_base = _route_params.get('s4_box_spring', self.CAPITAL_ALLOC_SMALL_SPRING)
                master_base = _route_params.get('s1_hft', self.CAPITAL_ALLOC_SMALL_MASTER)
                divergence_base = _route_params.get('s7_divergence_reversal', self.CAPITAL_ALLOC_SMALL_REVERSE)
                other_base = _route_params.get('s3_box_extreme', self.CAPITAL_ALLOC_SMALL_OTHER)
            elif self._capital_scale == CapitalScale.LARGE:
                spring_base = _route_params.get('s4_box_spring', self.CAPITAL_ALLOC_LARGE_SPRING)
                master_base = _route_params.get('s1_hft', self.CAPITAL_ALLOC_LARGE_MASTER)
                divergence_base = _route_params.get('s7_divergence_reversal', self.CAPITAL_ALLOC_LARGE_REVERSE)
                other_base = _route_params.get('s3_box_extreme', self.CAPITAL_ALLOC_LARGE_OTHER)
            else:
                spring_base = _route_params.get('s4_box_spring', cr.spring_base)
                master_base = _route_params.get('s1_hft', cr.master_base)
                divergence_base = _route_params.get('s7_divergence_reversal', cr.divergence_base)
                other_base = _route_params.get('s3_box_extreme', cr.other_base)

            allocations = {
                'master': cr.min_maintenance_ratio,
                'divergence': cr.min_maintenance_ratio,
                'other': cr.min_maintenance_ratio,
                'spring': spring_base,
            }

            if active_state == STRATEGY_MODE_CORRECT_TRENDING:
                allocations['master'] = master_base + cr.master_active_boost
                remaining = 1.0 - allocations['master'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['divergence'] += remaining * self.REMAINING_ALLOC_MASTER
                    allocations['other'] += remaining * self.REMAINING_ALLOC_MASTER_OTHER
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_MASTER_SPRING
            elif active_state == STRATEGY_MODE_INCORRECT_REVERSAL:
                allocations['divergence'] = divergence_base + cr.master_active_boost
                remaining = 1.0 - allocations['divergence'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['master'] += remaining * self.REMAINING_ALLOC_REVERSE_MASTER
                    allocations['other'] += remaining * self.REMAINING_ALLOC_REVERSE_OTHER
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_REVERSE_SPRING
            elif active_state == 'other':
                allocations['other'] = other_base + cr.master_active_boost
                remaining = 1.0 - allocations['other'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['master'] += remaining * self.REMAINING_ALLOC_OTHER_MASTER
                    allocations['divergence'] += remaining * self.REMAINING_ALLOC_OTHER_REVERSE
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_OTHER_SPRING
            else:
                allocations['master'] = master_base
                allocations['divergence'] = divergence_base
                allocations['other'] = other_base
                allocations['spring'] = spring_base

            # P1-裂缝38：应用状态联动乘子表
            state_multipliers = self.STATE_STRATEGY_MULTIPLIERS.get(active_state, {})
            strategy_to_slot = {
                'master': ['s1_hft', 's2_resonance'],
                'divergence': ['s7_divergence_reversal'],
                'other': ['s3_box_extreme'],
                'spring': ['s4_box_spring'],
            }
            for slot_name, strategy_keys in strategy_to_slot.items():
                multiplier = 1.0
                for sk in strategy_keys:
                    if sk in self._master_substrategy_pause and self._master_substrategy_pause.get(sk, False):
                        continue
                    if sk in state_multipliers:
                        multiplier = state_multipliers[sk]
                        break
                if multiplier != 1.0:
                    allocations[slot_name] *= multiplier

            # R16-P1-18修复: S1-S4交叉管理独立性
            _master_slot = self._get_slot('master')
            if _master_slot is not None:
                _s1_ev = _master_slot.get_sub_strategy_ev('s1_hft')
                _s2_ev = _master_slot.get_sub_strategy_ev('s2_resonance')
                _s1_pause = self._master_substrategy_pause.get('s1_hft', False)
                _s2_pause = self._master_substrategy_pause.get('s2_resonance', False)
                if _s1_pause and not _s2_pause:
                    allocations['master'] *= 0.5
                elif _s2_pause and not _s1_pause:
                    allocations['master'] *= 0.5
                elif _s1_pause and _s2_pause:
                    allocations['master'] *= 0.0
                if _s1_ev < 0 and _s2_ev >= 0:
                    allocations['master'] *= 0.6
                elif _s2_ev < 0 and _s1_ev >= 0:
                    allocations['master'] *= 0.6

            # 切换持仓保护
            if self._handover_from and self._handover_to:
                _handover_slot = self._get_slot(self._handover_from)
                if _handover_slot is not None and _handover_slot.position_count > 0:
                    _min_alloc = max(cr.min_maintenance_ratio, self._handover_min_alloc)
                    _from_alloc = allocations.get(self._handover_from, 0.0)
                    if _from_alloc < _min_alloc:
                        _donor = self._handover_to if self._handover_to in allocations else None
                        _delta = _min_alloc - _from_alloc
                        if _donor and allocations.get(_donor, 0.0) > _delta:
                            allocations[self._handover_from] = _from_alloc + _delta
                            allocations[_donor] = max(0.0, allocations[_donor] - _delta)
                else:
                    self._handover_from = None
                    self._handover_to = None
                    self._handover_started_ts = 0.0

            # [R16-P1-13修复] 资金分配原子性保护
            _total_alloc = sum(allocations.values())
            if abs(_total_alloc - 1.0) > 0.01:
                logger.warning("[R16-P1-13] 资金分配总和不等于100%%: total=%.4f, 自动归一化", _total_alloc)
            total = sum(allocations.values())
            if abs(total - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= total

            # P1-R9-13修复: alpha_ratio深度参与路由
            if cr.alpha_ratio > 0.0 and cr.alpha_ratio <= 1.0:
                _alpha_w = cr.alpha_ratio
                allocations['master'] = allocations.get('master', 0.0) * (1.0 + _alpha_w * 0.15)
                allocations['divergence'] = allocations.get('divergence', 0.0) * (1.0 - _alpha_w * 0.10)
                allocations['other'] = allocations.get('other', 0.0) * (1.0 - _alpha_w * 0.05)
                allocations['spring'] = allocations.get('spring', 0.0) * (1.0 - _alpha_w * 0.05)
                _total_after = sum(allocations.values())
                if abs(_total_after - 1.0) > 1e-6 and _total_after > 1e-6:
                    for k in allocations:
                        allocations[k] /= _total_after

            # P0-R8-09修复: CRM周期共振模块接入route_capital
            try:
                from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_degradation_active():
                        logger.warning("[StrategyEcosystem] 影子引擎降级激活，资金分配降至min_maintenance_ratio")
                        for slot_name in ('master', 'divergence', 'other', 'spring'):
                            if slot_name in allocations:
                                allocations[slot_name] = cr.min_maintenance_ratio
                    elif _sse.is_absolute_ev_paused():
                        logger.warning("[StrategyEcosystem] 影子引擎EV暂停激活，资金分配降至min_maintenance_ratio")
                        for slot_name in ('master', 'divergence', 'other', 'spring'):
                            if slot_name in allocations:
                                allocations[slot_name] = cr.min_maintenance_ratio
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _sse_err:
                logger.debug("[StrategyEcosystem] 影子引擎降级检查跳过: %s", _sse_err)

            try:
                from ali2026v3_trading.param_pool.optimization.cycle_sharpe import get_cycle_resonance_module
                crm = get_cycle_resonance_module()
                if crm:
                    _SLOT_TO_CRM_STRATEGY = {'master': 'resonance', 'divergence': 'divergence', 'other': 'high_freq', 'spring': 'spring'}
                    for slot_name in ['master', 'divergence', 'other', 'spring']:
                        try:
                            _crm_strategy = _SLOT_TO_CRM_STRATEGY.get(slot_name, slot_name)
                            risk_adj = crm.get_risk_surface(strategy=_crm_strategy)
                            if risk_adj and hasattr(risk_adj, 'size_multiplier'):
                                crm_mult = risk_adj.size_multiplier
                                if crm_mult > 0 and crm_mult != 1.0:
                                    allocations[slot_name] *= crm_mult
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                            pass
                    _volatility_regime = None
                    try:
                        from ali2026v3_trading.config.params_service import get_params_service
                        _ps_crm = get_params_service()
                        _volatility_regime = _ps_crm.get_str('volatility_regime', None)
                        if _volatility_regime not in ('low', 'normal', 'high', 'extreme'):
                            _volatility_regime = None
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                        logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                        pass
                    _crm_output = getattr(crm, '_last_output', None)
                    _crm_phase = getattr(_crm_output, 'phase', None) if _crm_output else None
                    if _crm_phase is not None:
                        _priority_list = crm.get_signal_priority(_crm_phase, volatility_regime=_volatility_regime)
                        _SLOT_MAP = {'resonance': 'master', 'spring': 'spring', 'high_freq': 'other', 'divergence': 'divergence'}
                        _ranked_allocs = []
                        for _p_slot in _priority_list:
                            _alloc_key = _SLOT_MAP.get(_p_slot)
                            if _alloc_key and _alloc_key in allocations:
                                _ranked_allocs.append((_alloc_key, allocations[_alloc_key]))
                        for _missing_key in ('master', 'divergence', 'other', 'spring'):
                            if _missing_key not in [k for k, _ in _ranked_allocs]:
                                _ranked_allocs.append((_missing_key, allocations.get(_missing_key, 0.0)))
                        _PRIORITY_BOOST = 1.15
                        if _ranked_allocs:
                            allocations[_ranked_allocs[0][0]] *= _PRIORITY_BOOST
                        logger.debug("[P2-CR-004] CRM信号优先级激活: phase=%s regime=%s priority=%s top=%s",
                                     _crm_phase, _volatility_regime,
                                     _priority_list[:2] if _priority_list else [],
                                     _ranked_allocs[0][0] if _ranked_allocs else None)
                    total2 = sum(v for k, v in allocations.items() if k in ('master', 'divergence', 'other', 'spring'))
                    if total2 > 1e-10:
                        for k in ('master', 'divergence', 'other', 'spring'):
                            allocations[k] /= total2
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.debug("[StrategyEcosystem] CRM route_capital integration: %s", e)

            # R3-P-01修复: s5_arbitrage/s6_market_making路由
            # FIX-20260711-S5S6: S5套利/S6做市改为实盘模拟监控模式，不参与资金分配
            allocations['arbitrage'] = 0.0
            allocations['market_making'] = 0.0
            allocations['divergence'] = _route_params.get('s7_divergence_reversal', cr.divergence_base)

            _total_alloc = sum(allocations.values())
            if _total_alloc > 1e-10 and abs(_total_alloc - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= _total_alloc

            # P1-CA-002修复: 资金路由动态调整
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                _dynamic_enabled = _ps.get_bool('capital_route_dynamic_enabled', False)
                if _dynamic_enabled:
                    _perf_weights = {}
                    for slot_name, strat in [('master', self._master), ('divergence', self._divergence),
                                             ('other', self._other), ('spring', self._spring)]:
                        if strat and hasattr(strat, 'win_rate') and hasattr(strat, 'trade_count'):
                            wr = getattr(strat, 'win_rate', 0.5)
                            tc = getattr(strat, 'trade_count', 0)
                            _perf_weights[slot_name] = wr if tc >= 20 else 0.5
                        else:
                            _perf_weights[slot_name] = 0.5
                    _total_pw = sum(_perf_weights.values())
                    if _total_pw > 1e-10:
                        for slot_name in _perf_weights:
                            base_ratio = allocations.get(slot_name, 0.0)
                            perf_adj = _perf_weights[slot_name] / _total_pw
                            blend = _ps.get_float('capital_route_perf_blend', 0.3)
                            allocations[slot_name] = base_ratio * (1.0 - blend) + perf_adj * blend
                        _renorm = sum(allocations.get(k, 0.0) for k in allocations)
                        if abs(_renorm - 1.0) > 1e-6 and _renorm > 1e-10:
                            for k in allocations:
                                allocations[k] /= _renorm
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _e:
                logger.debug("[P1-CA-002] 资金路由动态调整跳过: %s", _e)

            self._master.capital_allocation = allocations['master']
            self._divergence.capital_allocation = allocations['divergence']
            self._other.capital_allocation = allocations['other']
            self._spring.capital_allocation = allocations.get('spring', cr.spring_base)
            self._arbitrage.capital_allocation = allocations.get('arbitrage', cr.arbitrage_base)
            self._market_making.capital_allocation = allocations.get('market_making', cr.market_making_base)
            self._divergence.capital_allocation = allocations.get('divergence', cr.divergence_base)

            # P2-CA-003修复: S5(套利)/S6(做市)互斥验证
            s5_alloc = allocations.get('arbitrage', 0.0)
            s6_alloc = allocations.get('market_making', 0.0)
            if s5_alloc > 0.05 and s6_alloc > 0.05:
                try:
                    from ali2026v3_trading.config.params_service import get_params_service
                    _ps = get_params_service()
                    _s5_s6_mutex_mode = _ps.get_str('capital_route_s5_s6_mutex', 'arbitrage_priority')
                    if _s5_s6_mutex_mode == 'arbitrage_priority':
                        allocations['market_making'] = min(s6_alloc, 0.05)
                        logger.debug("[P2-CA-003] S5/S6互斥: 套利优先, 做市限制为%.3f", allocations['market_making'])
                    elif _s5_s6_mutex_mode == 'market_making_priority':
                        allocations['arbitrage'] = min(s5_alloc, 0.05)
                        logger.debug("[P2-CA-003] S5/S6互斥: 做市优先, 套利限制为%.3f", allocations['arbitrage'])
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _e:
                    logger.debug("[P2-CA-003] S5/S6互斥检查跳过: %s", _e)

            # P1-8修复：再平衡摩擦成本扣除
            _rebalance_friction_bps = float(os.environ.get('REBALANCE_FRICTION_BPS', '2.0'))
            rebalance_friction_bps = getattr(cr, 'rebalance_friction_bps', _rebalance_friction_bps)
            total_friction_pct = rebalance_friction_bps / 10000.0
            if self._stats['capital_rebalances'] > 0:
                for k in allocations:
                    allocations[k] = max(0.0, allocations[k] * (1.0 - total_friction_pct))

            self._stats['capital_rebalances'] += 1
            self._auto_upgrade_capital_scale()

            _now_obs = time.time()
            for _slot_name in ('master', 'divergence', 'other', 'spring', 'arbitrage', 'market_making'):
                _slot = self._get_slot(_slot_name)
                if _slot is not None and _slot.state == SlotState.RETIRED:
                    allocations[_slot_name] = 0.0
                    continue
                if _slot is not None and _slot._observation_period_until is not None:
                    if _now_obs < _slot._observation_period_until:
                        _obs_scale = _slot._observation_position_scale
                        allocations[_slot_name] = allocations.get(_slot_name, 0.0) * _obs_scale
                    else:
                        _slot._observation_period_until = None
                        _slot._observation_position_scale = 1.0

            # R16-P2-16修复: Alpha置信区间重叠影响资金分配
            try:
                from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None and hasattr(_sse, 'alpha_confidence_overlap_test'):
                    _overlap = _sse.alpha_confidence_overlap_test()
                    if _overlap and not _overlap.get('overlap_valid', True):
                        _overlap_scale = 0.8
                        for k in allocations:
                            allocations[k] *= _overlap_scale
                        logging.warning("[R16-P2-16] Alpha置信区间重叠，资金分配缩放至80%%")
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass

            # 最终归一化
            _final_total = sum(allocations.values())
            if _final_total > 1e-10 and abs(_final_total - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= _final_total

            return allocations

    def enforce_l2_dataset_binding(self, dataset_id: str) -> bool:
        with self._lock:
            if not dataset_id:
                self._l2_dataset_enforced = False
                self._l2_dataset_id = ''
                return False
            self._l2_dataset_enforced = True
            self._l2_dataset_id = dataset_id
            return True

    def _auto_upgrade_capital_scale(self) -> None:
        if not self._capital_scale_upgrade_enabled:
            return
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            _rs = get_risk_service(None)
            if _rs is not None:
                _upgrade = _rs.evaluate_capital_scale_upgrade()
                if _upgrade.get('eligible'):
                    logger.info("[R16-P1-05] risk_service升级评估通过: %s", _upgrade)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass
        total_trades = int(self._stats.get('total_trades', 0))
        ev_breaches = int(self._stats.get('ev_breaches', 0))
        if total_trades < 20 or ev_breaches > 0:
            return
        if self._capital_scale is None and hasattr(CapitalScale, 'MEDIUM'):
            self._capital_scale = CapitalScale.MEDIUM
            logger.info("[StrategyEcosystem] capital_scale自动升级: None -> %s", self._capital_scale)
        elif self._capital_scale == getattr(CapitalScale, 'MEDIUM', self._capital_scale) and total_trades >= 100 and ev_breaches == 0:
            target = getattr(CapitalScale, 'LARGE', self._capital_scale)
            if target != self._capital_scale:
                self._capital_scale = target
                logger.info("[StrategyEcosystem] capital_scale自动升级: MEDIUM -> %s", self._capital_scale)

    def get_capital_allocations(self) -> Dict[str, float]:
        with self._lock:
            return {
                'master': self._master.capital_allocation,
                'divergence': self._divergence.capital_allocation,
                'other': self._other.capital_allocation,
                'spring': self._spring.capital_allocation,
                'arbitrage': self._arbitrage.capital_allocation,
                'market_making': self._market_making.capital_allocation,
                'divergence': self._divergence.capital_allocation,
            }

    def normalize_capital_allocation(self) -> Dict[str, float]:
        """INV-11/INV-CON-02: 归一化资金分配，确保所有策略分配之和为1.0"""
        with self._lock:
            cr = self._capital_route
            raw_allocations = {
                'master': self._master.capital_allocation,
                'divergence': self._divergence.capital_allocation,
                'other': self._other.capital_allocation,
                'spring': self._spring.capital_allocation,
                'arbitrage': self._arbitrage.capital_allocation,
                'market_making': self._market_making.capital_allocation,
                'divergence': self._divergence.capital_allocation,
            }
            total = sum(raw_allocations.values())
            if total <= 0:
                logger.warning("[INV-11] 资金分配总和<=0 (%.6f)，无法归一化", total)
                return raw_allocations
            if abs(total - 1.0) > 1e-6:
                normalized = {k: v / total for k, v in raw_allocations.items()}
                logger.info(
                    "[INV-11] 资金分配归一化: total=%.6f → 1.0, 调整前=%s",
                    total, {k: round(v, 4) for k, v in raw_allocations.items()},
                )
                self._master.capital_allocation = normalized['master']
                self._divergence.capital_allocation = normalized['divergence']
                self._other.capital_allocation = normalized['other']
                self._spring.capital_allocation = normalized['spring']
                self._arbitrage.capital_allocation = normalized['arbitrage']
                self._market_making.capital_allocation = normalized['market_making']
                self._divergence.capital_allocation = normalized['divergence']
                return normalized
            return raw_allocations

    def _precompute_capital_route_ratios(self, base_capital: float, ratios: Dict[str, float]) -> Dict[str, float]:
        """预计算资金路由比例: base_capital * ratio"""
        from ali2026v3_trading.strategy.strategy_ecosystem import _precompute_capital_route_ratios as _impl
        return _impl(base_capital, ratios)

    def _precompute_division(self, numerator: float, denominator: float, key: str = '') -> float:
        """预计算浮点除法并缓存"""
        from ali2026v3_trading.strategy.strategy_ecosystem import _precompute_division as _impl
        return _impl(numerator, denominator, key)


CapitalRoutingMixin = CapitalRoutingService


class StrategyEcosystem:
    """策略生态系统 — Facade组合（消灭Mixin继承）'
    六大策略协同：
    - correct_trending → 主策略(S2共振)
    - incorrect_reversal → 反向策略(S2影子A)
    - other → 震荡市策略(S3箱体极值)
    - spring → 弹簧策略(S4弹簧)
    - arbitrage → 套利策略(S5套利)
    - market_making → 做市策略(S6做市)
    """

    MIN_TRADES_FOR_EV = 5

    # R14-P1-DOC-P1-01修复: 资金分配权重配置
    CAPITAL_ALLOC_MASTER_BASE = 0.60
    CAPITAL_ALLOC_REVERSE_BASE = 0.25
    CAPITAL_ALLOC_OTHER_BASE = 0.15
    CAPITAL_ALLOC_SPRING_BASE = 0.15
    CAPITAL_ALLOC_ARBITRAGE_BASE = 0.10
    CAPITAL_ALLOC_MARKET_MAKING_BASE = 0.10
    # 资金规模调整比例
    CAPITAL_ALLOC_SMALL_MASTER = 0.30
    CAPITAL_ALLOC_SMALL_REVERSE = 0.20
    CAPITAL_ALLOC_SMALL_OTHER = 0.15
    CAPITAL_ALLOC_SMALL_SPRING = 0.35
    CAPITAL_ALLOC_LARGE_MASTER = 0.50
    CAPITAL_ALLOC_LARGE_REVERSE = 0.30
    CAPITAL_ALLOC_LARGE_OTHER = 0.15
    CAPITAL_ALLOC_LARGE_SPRING = 0.05
    # 动态分配剩余资金权重
    REMAINING_ALLOC_MASTER = 0.50
    REMAINING_ALLOC_REVERSE = 0.50
    REMAINING_ALLOC_OTHER_MASTER = 0.40
    REMAINING_ALLOC_OTHER_REVERSE = 0.40
    REMAINING_ALLOC_OTHER_SPRING = 0.20
    REMAINING_ALLOC_REVERSE_MASTER = 0.50
    REMAINING_ALLOC_REVERSE_OTHER = 0.30
    REMAINING_ALLOC_REVERSE_SPRING = 0.20
    REMAINING_ALLOC_MASTER_OTHER = 0.30
    REMAINING_ALLOC_MASTER_SPRING = 0.20

    # P1-裂缝38：状态联动资金分配乘子表
    STATE_STRATEGY_MULTIPLIERS = {
        STRATEGY_MODE_CORRECT_TRENDING: {
            's1_hft': 1.5, 's2_resonance': 1.5, 's3_box_extreme': 0.8,
            's4_box_spring': 0.8, 's5_arbitrage': 1.0, 's6_market_making': 1.0,
            's7_divergence_reversal': 0.8,
        },
        STRATEGY_MODE_INCORRECT_REVERSAL: {
            's1_hft': 1.2, 's2_resonance': 1.2, 's3_box_extreme': 0.5,
            's4_box_spring': 0.5, 's5_arbitrage': 1.0, 's6_market_making': 1.0,
            's7_divergence_reversal': 1.5,
        },
        'other': {
            's1_hft': 0.6, 's2_resonance': 0.6, 's3_box_extreme': 1.5,
            's4_box_spring': 1.5, 's5_arbitrage': 1.0, 's6_market_making': 1.0,
            's7_divergence_reversal': 0.6,
        },
    }

    # PLR冻结条件阈值
    PLR_FREEZE_RATIO_THRESHOLD = 0.5
    PLR_FREEZE_CONSECUTIVE_LOSSES = 5

    # R13-P0-API-06修复: 状态→策略ID映射
    _STATE_TO_STRATEGY_MAP = {
        STRATEGY_MODE_CORRECT_TRENDING: 'master',
        STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: 'divergence',
        STRATEGY_MODE_INCORRECT_REVERSAL: 'divergence',
        'incorrect_reversal_defensive': 'divergence',
        STRATEGY_MODE_OTHER: 'other',
    }

    def __init__(
        self,
        capital_route: Optional[CapitalRoute] = None,
        box_params: Optional[BoxStrategyParams] = None,
        log_dir: Optional[str] = None,
        capital_scale: Optional[CapitalScale] = None,
    ):
        self._lock = threading.RLock()
        self._capital_route = capital_route or CapitalRoute()
        self._capital_scale = capital_scale
        # R27-P0-DR-13修复: 多策略隔板
        self._strategy_bulkheads: Dict[str, Dict[str, Any]] = {
            STRATEGY_MODE_CORRECT_TRENDING: {'max_signals_per_min': 30, 'max_capital_pct': 0.60, 'signals_count': 0, 'last_reset': 0.0},
            STRATEGY_MODE_INCORRECT_REVERSAL: {'max_signals_per_min': 20, 'max_capital_pct': 0.25, 'signals_count': 0, 'last_reset': 0.0},
            'other': {'max_signals_per_min': 15, 'max_capital_pct': 0.15, 'signals_count': 0, 'last_reset': 0.0},
            'spring': {'max_signals_per_min': 15, 'max_capital_pct': 0.15, 'signals_count': 0, 'last_reset': 0.0},
            'arbitrage': {'max_signals_per_min': 10, 'max_capital_pct': 0.10, 'signals_count': 0, 'last_reset': 0.0},
            'market_making': {'max_signals_per_min': 50, 'max_capital_pct': 0.10, 'signals_count': 0, 'last_reset': 0.0},
            'divergence': {'max_signals_per_min': 20, 'max_capital_pct': 0.20, 'signals_count': 0, 'last_reset': 0.0},
        }
        self._strategy_error_counts: Dict[str, int] = {}
        self._strategy_circuit_breaker_threshold: int = 5
        self._strategy_circuit_breaker_at: Dict[str, float] = {}
        self._strategy_circuit_breaker_auto_recovery_sec: float = 300.0

        _box_detector_kwargs = {}
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            _all_params = get_cached_params()
            for _dk in ('box_gain_ratio', 'plr_normalization_base'):
                if _dk in _all_params:
                    _box_detector_kwargs[_dk] = _all_params[_dk]
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _cfg_err:
            logging.debug("[StrategyEcosystem] BoxDetector config params load skipped: %s", _cfg_err)
            _all_params = {}
        self._box_detector = BoxDetector(params=box_params, **_box_detector_kwargs)
        self._last_bar_data: Dict[str, float] = {}

        # P1-32修复: 从ConfigService覆盖资金分配权重
        try:
            from ali2026v3_trading.config.config_service import get_config_service
            _cfg = get_config_service()
            _cap_cfg = getattr(_cfg, 'trading', None)
            if _cap_cfg:
                for _attr in ['CAPITAL_ALLOC_MASTER_BASE', 'CAPITAL_ALLOC_REVERSE_BASE',
                              'CAPITAL_ALLOC_OTHER_BASE', 'CAPITAL_ALLOC_SPRING_BASE']:
                    _val = getattr(_cap_cfg, _attr.lower(), None)
                    if _val is not None:
                        setattr(self, _attr, _val)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass

        self._log_dir = log_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'ecosystem'
        )
        os.makedirs(self._log_dir, exist_ok=True)

        self._master = StrategySlot(
            strategy_id='master',
            strategy_type=STRATEGY_MODE_CORRECT_TRENDING,
            state=SlotState.ACTIVE,
            capital_allocation=self.CAPITAL_ALLOC_MASTER_BASE,
        )
        self._reverse = self._divergence
        self._other = StrategySlot(
            strategy_id='other',
            strategy_type='box_extreme',
            state=SlotState.STANDBY,
            capital_allocation=self.CAPITAL_ALLOC_OTHER_BASE,
        )
        self._spring = StrategySlot(
            strategy_id='spring',
            strategy_type='box_spring',
            state=SlotState.ACTIVE,
            capital_allocation=self._capital_route.spring_base,
        )
        # FIX-20260711-S5S6: S5套利/S6做市改为实盘模拟监控模式，不参与资金分配
        self._arbitrage = StrategySlot(
            strategy_id='arbitrage',
            strategy_type='arbitrage',
            state=SlotState.STANDBY,
            capital_allocation=0.0,
        )
        self._market_making = StrategySlot(
            strategy_id='market_making',
            strategy_type='market_making',
            state=SlotState.STANDBY,
            capital_allocation=0.0,
        )
        self._divergence = StrategySlot(
            strategy_id='divergence',
            strategy_type='divergence',
            state=SlotState.STANDBY,
            capital_allocation=self._capital_route.divergence_base,
        )

        # P-12修复: LiveStrategySelector实盘集成初始化
        self._live_strategy_selector: Optional[Any] = None
        self._degrade_frequency_callbacks: List = []
        if LiveStrategySelector is not None:
            try:
                self._live_strategy_selector = LiveStrategySelector(mapping_table=None)
                self._degrade_frequency_callbacks.append(self._on_degrade_frequency)
                logging.info("[StrategyEcosystem] LiveStrategySelector集成成功(含P-13回调)")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[StrategyEcosystem] LiveStrategySelector初始化失败: %s", e)

        self._active_strategy: str = 'master'
        self._prev_active_strategy: str = 'master'
        self._strategy_instance_cache: Dict[str, Any] = {}

        # UPG-P1-03修复: 并行运行模式
        self._parallel_run_mode: bool = False
        self._parallel_run_old_strategy: Optional[str] = None
        self._parallel_run_new_strategy: Optional[str] = None
        self._parallel_run_start_time: float = 0.0
        self._parallel_run_max_duration_sec: float = 3600.0
        self._strategy_lineage: Dict[str, List[Dict[str, Any]]] = {}
        self._strategy_param_evolution: deque = deque(maxlen=1000)
        self._strategy_doc_links: Dict[str, str] = {
            'master': 'docs/strategies/master.md',
            'divergence': 'docs/strategies/divergence.md',
            'other': 'docs/strategies/other.md',
            'spring': 'docs/strategies/spring.md',
            'arbitrage': 'docs/strategies/arbitrage.md',
            'market_making': 'docs/strategies/market_making.md',
            'divergence': 'docs/strategies/divergence.md',
        }
        self._l2_dataset_enforced: bool = True
        self._l2_dataset_id: str = 'l2_default_dataset'
        self._capital_scale_upgrade_enabled: bool = True
        self._retired_strategies: Dict[str, Dict[str, Any]] = {}
        self._revival_requests: deque = deque(maxlen=200)

        self._master_trades: deque = deque(maxlen=10000)
        self._divergence_trades: deque = deque(maxlen=10000)
        self._other_trades: deque = deque(maxlen=10000)
        self._spring_trades: deque = deque(maxlen=10000)
        self._arbitrage_trades: deque = deque(maxlen=10000)
        self._market_making_trades: deque = deque(maxlen=10000)
        self._divergence_trades: deque = deque(maxlen=10000)

        self._master_equity: deque = deque(maxlen=10000)
        self._divergence_equity: deque = deque(maxlen=10000)
        self._other_equity: deque = deque(maxlen=10000)

        self._trade_id_counter: int = 0
        self._last_route_time: float = 0.0
        self._ev_cache: Optional[Dict[str, Any]] = None
        self._ev_cache_ts: float = 0.0
        _strategy_type = str(_all_params.get('strategy_type', 'default')).lower() if _all_params else 'default'
        _ev_ttl_default = EV_CACHE_TTL_DEFAULTS.get(_strategy_type, EV_CACHE_TTL_DEFAULTS['default'])
        self._ev_cache_ttl: float = float(_all_params.get('ev_cache_ttl', _ev_ttl_default)) if _all_params else _ev_ttl_default
        self._mode_engine = None
        self._snapshot_collector = None
        self._bar_counter: int = 0
        self._s1_params = {
            'strategy_id': 's1_hft',
            'signal_confirm_ticks': 3,
            'cooldown_ms': 50,
            'min_imbalance': 0.15,
            'hft_hard_time_stop_ms': 60000,
            'daily_drawdown_pct': 0.03,
            'risk_ratio': 0.2,
        }
        self._s2_params = {
            'strategy_id': 's2_resonance',
            'state_confirm_bars': 5,
            'cooldown_sec': 0,
            'min_strength': 0.3,
            'resonance_hard_time_stop_min': 5,
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
        }
        self._master_substrategy_pause = {
            's1_hft': False,
            's2_resonance': False,
        }
        self._handover_from: Optional[str] = None
        self._handover_to: Optional[str] = None
        self._handover_started_ts: float = 0.0
        self._handover_min_alloc: float = 0.10
        self._s3_params = {
            'strategy_id': 's3_box',
            'box_hard_time_stop_min': 30,
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
        }
        self._s4_params = {
            'strategy_id': 's4_spring',
            'spring_hard_time_stop_sec': 600,
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
        }

        self._stats = {
            'total_trades': 0,
            'master_trades': 0,
            'divergence_trades': 0,
            'other_trades': 0,
            'spring_trades': 0,
            'arbitrage_trades': 0,
            'market_making_trades': 0,
            'state_switches': 0,
            'mutual_exclusion_blocks': 0,
            'capital_rebalances': 0,
            'ev_breaches': 0,
        }

        # 集成治理检查器
        try:
            from ali2026v3_trading.governance.governance_engine import (
                E12ReverseStrategyPseudoIndependenceDetector,
                E13ShadowStrategyCollusionDetector,
                MultiStateSwitchBacktestScenario,
                WF6ToWF10EliminationChecker,
                E8E9E10EliminationChecker,
                E7UnexplainedReturnChecker,
                E11QuantitativeSourceChecker,
            )
            _gov_cfg = self._config if hasattr(self, '_config') and self._config else {}
            self._e12_detector = E12ReverseStrategyPseudoIndependenceDetector(
                max_correlation_threshold=_gov_cfg.get("e12_max_correlation_threshold", 0.3),
                min_trade_count=_gov_cfg.get("e12_min_trade_count", 20),
            )
            self._e13_detector = E13ShadowStrategyCollusionDetector(
                min_param_diff_pct=_gov_cfg.get("e13_min_param_diff_pct", 0.20),
                max_signal_sync_rate=_gov_cfg.get("e13_max_signal_sync_rate", 0.7),
                min_trade_count=_gov_cfg.get("e13_min_trade_count", 20),
            )
            self._multi_state_scenario = MultiStateSwitchBacktestScenario()
            self._wf_elimination_checker = WF6ToWF10EliminationChecker()
            self._e8e9e10_checker = E8E9E10EliminationChecker()
            self._e7_checker = E7UnexplainedReturnChecker()
            self._e11_checker = E11QuantitativeSourceChecker()
            logger.info("[StrategyEcosystem] 治理检查器已集成: E7/E8/E9/E10/E11/E12/E13/WF6-10")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.warning("[StrategyEcosystem] 治理检查器集成失败: %s", e)
            self._e12_detector = None
            self._e13_detector = None
            self._multi_state_scenario = None
            self._wf_elimination_checker = None
            self._e8e9e10_checker = None
            self._e7_checker = None
            self._e11_checker = None

        logger.info("[StrategyEcosystem] 初始化完成")

        self._capital_routing_service = CapitalRoutingService()
        from ali2026v3_trading.strategy.lifecycle_service import LifecycleService as StrategyLifecycleService
        self._lifecycle_service = StrategyLifecycleService(provider=self, strategy_id=getattr(self, '_strategy_id', ''))
        self._trading_ev_service = TradingEVService()
        from ali2026v3_trading.strategy.strategy_monitoring_layer import EcosystemMonitoringService
        self._monitoring_service = EcosystemMonitoringService()
        self._governance_service = GovernanceService()
        self._ops_service = OpsService()

        self._propagate_capital_scale()

    def __getattr__(self, name):
        for svc_attr in ('_capital_routing_service', '_lifecycle_service',
                         '_trading_ev_service', '_monitoring_service',
                         '_governance_service', '_ops_service'):
            svc = self.__dict__.get(svc_attr)
            if svc is not None and hasattr(svc, name):
                val = getattr(svc, name)
                # 修复: 将服务类的方法重新绑定到StrategyEcosystem实例，
                # 使@_require_interface等装饰器检查正确的self
                if callable(val) and hasattr(val, '__func__'):
                    return types.MethodType(val.__func__, self)
                return val
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def get_active_strategy(self) -> str:
        """返回当前活跃策略名称"""
        with self._lock:
            return self._active_strategy

    def on_state_switched(self, old_state: str, new_state: str) -> None:
        """状态切换回调：当SPM状态变化时联动策略切换"""
        strategy_map = {
            'correct_trending': 'master',
            'incorrect_reversal': 'divergence',
            'other': 'other',
            'spring': 'spring',
        }
        target = strategy_map.get(new_state)
        if target:
            self.switch_active_strategy(target)

    def switch_active_strategy(self, target: str) -> Dict[str, Any]:
        """切换活跃策略"""
        with self._lock:
            # 映射测试中的策略名到内部名
            name_map = {
                'incorrect_reversal': 'divergence',
                'correct_trending': 'master',
            }
            internal_target = name_map.get(target, target)
            if internal_target not in ('master', 'divergence', 'other', 'spring', 'arbitrage', 'market_making'):
                return {'switched': False, 'reason': f'unknown strategy: {target}'}
            # 同策略不切换
            if internal_target == self._active_strategy:
                return {'switched': False, 'reason': 'same strategy'}
            prev = self._active_strategy
            self._prev_active_strategy = prev
            self._active_strategy = internal_target
            return {
                'switched': True,
                'from': prev,
                'to': internal_target,
                'close_old_positions_first': True,
                'capital_allocations': self.get_capital_allocations(),
            }

    def resolve_strategy_id_from_state(self, state: str) -> str:
        """将状态映射到策略ID"""
        state_to_strategy = {
            'correct_trending': 'master',
            'incorrect_reversal': 'divergence',
            'other': 'other',
            'spring': 'spring',
            'arbitrage': 'arbitrage',
            'market_making': 'market_making',
        }
        return state_to_strategy.get(state, 'master')

    def check_mutual_exclusion(self, strategy_id: str, direction: str, instrument_id: str = '') -> tuple:
        """检查互斥约束（策略切换后某些方向不允许开仓）"""
        with self._lock:
            # other状态下禁止master和reverse新开仓
            if self._active_strategy == 'other' and strategy_id in ('master', 'divergence'):
                return (False, f'{strategy_id} blocked in other state')
            return (True, 'allowed')

    def get_health_status(self) -> Dict[str, Any]:
        """返回生态系统健康状态"""
        with self._lock:
            result = {
                'component': 'strategy_ecosystem',
                'status': 'OK',
                'active_strategy': self._active_strategy,
                'degradation_triggered': False,
                'absolute_ev_breached': getattr(self, '_absolute_ev_breached', False),
                'frozen': False,
                'governance_degraded': getattr(self, '_governance_degraded', False),
            }
            try:
                result['strategies'] = {
                    'master': {'paused': self._master.paused, 'frozen': self._master.frozen},
                    'divergence': {'paused': self._divergence.paused, 'frozen': self._divergence.frozen},
                    'other': {'paused': self._other.paused, 'frozen': self._other.frozen},
                }
            except (ValueError, KeyError, TypeError, AttributeError):
                result['strategies'] = {}
            try:
                result['capital_allocations'] = self.route_capital(self._active_strategy)
            except (ValueError, KeyError, TypeError, AttributeError):
                result['capital_allocations'] = {}
            try:
                result['capital_scale'] = str(self._capital_scale)
            except (ValueError, KeyError, TypeError, AttributeError):
                result['capital_scale'] = 'unknown'
            try:
                result['box_detector'] = 'active' if self._box_detector else 'none'
            except (ValueError, KeyError, TypeError, AttributeError):
                result['box_detector'] = 'none'
            return result

    def get_ecosystem_stats(self) -> Dict[str, Any]:
        """返回生态系统统计信息"""
        with self._lock:
            stats = dict(self._stats)
            stats['active_strategy'] = self._active_strategy
            stats['master_pnl'] = self._master.total_pnl
            stats['divergence_pnl'] = self._divergence.total_pnl
            stats['other_pnl'] = self._other.total_pnl
            stats['spring_pnl'] = self._spring.total_pnl
            stats['master_ev'] = self._master.expected_value
            stats['divergence_ev'] = self._divergence.expected_value
            stats['other_ev'] = self._other.expected_value
            stats['spring_ev'] = self._spring.expected_value
            stats['total_trades'] = stats.get('total_trades', 0)
            stats['master_trades'] = stats.get('master_trades', 0)
            stats['divergence_trades'] = stats.get('divergence_trades', 0)
            return stats

    def get_stats(self) -> Dict[str, Any]:
        """返回生态系统统计信息（别名：get_ecosystem_stats）"""
        return self.get_ecosystem_stats()

    def set_snapshot_collector(self, collector) -> None:
        self._snapshot_collector = collector

    def should_make_decision(self, strategy_id: str) -> bool:
        import warnings
        logging.warning("[DEPRECATED] should_make_decision is deprecated, always returns True")
        warnings.warn(
            "should_make_decision is deprecated: decision frequency is now determined by "
            "K-line period × state_confirm_bars. This method always returns True and will "
            "be removed in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        return True

    def _on_degrade_frequency(self, old_freq: int, new_freq: int, reason: str = "") -> None:
        """当LiveStrategySelector降频时，通知策略层调整决策频率"""
        logging.info("[P-13] _degrade_frequency回调: %d→%d, reason=%s", old_freq, new_freq, reason)
        for sid in ('master', 'divergence', 'other', 'spring', 'arbitrage', 'market_making'):
            slot = self._get_slot(sid)
            if slot is not None and hasattr(slot, 'decision_frequency'):
                slot.decision_frequency = new_freq

    def notify_degrade_frequency(self, old_freq: int, new_freq: int, reason: str = "") -> None:
        """外部接口：触发降频回调链"""
        import inspect
        for cb in self._degrade_frequency_callbacks:
            try:
                sig = inspect.signature(cb)
                params = list(sig.parameters.values())
                required_count = sum(1 for p in params if p.default is inspect.Parameter.empty)
                if required_count > 3:
                    logging.warning("[P-13] 降频回调签名不匹配(需要>3个必选参数): %s", cb)
                    continue
                cb(old_freq, new_freq, reason)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[P-13] 降频回调异常: %s", e)

    def get_s1_params(self) -> Dict[str, Any]:
        return dict(self._s1_params)

    def get_s2_params(self) -> Dict[str, Any]:
        return dict(self._s2_params)

    def update_s1_params(self, **kwargs) -> None:
        self._s1_params.update(kwargs)

    def update_s2_params(self, **kwargs) -> None:
        self._s2_params.update(kwargs)

    # R27-P0-DR-13修复: 多策略隔板信号配额检查
    def check_strategy_bulkhead(self, strategy_name: str) -> bool:
        """检查策略是否超过信号配额（隔板保护），返回True=允许，False=阻断"""
        import time as _time
        bh = self._strategy_bulkheads.get(strategy_name)
        if bh is None:
            return True
        now = _time.time()
        if bh.get('max_signals_per_min', 1) == 0:
            _cb_at = self._strategy_circuit_breaker_at.get(strategy_name, 0.0)
            if _cb_at > 0 and (now - _cb_at) >= self._strategy_circuit_breaker_auto_recovery_sec:
                bh['max_signals_per_min'] = self._strategy_bulkheads.get(strategy_name, {}).get('_original_max_signals', 15)
                self._strategy_error_counts[strategy_name] = 0
                self._strategy_circuit_breaker_at.pop(strategy_name, None)
                logging.info("[R27-P0-DR-13] 策略隔板自动恢复: %s 配额恢复为%d/min",
                            strategy_name, bh['max_signals_per_min'])
            else:
                return False
        if now - bh.get('last_reset', 0.0) >= 60.0:
            bh['signals_count'] = 0
            bh['last_reset'] = now
        if bh['signals_count'] >= bh['max_signals_per_min']:
            logging.warning("[R27-P0-DR-13] 策略隔板阻断: %s 信号数%d>=配额%d/min",
                            strategy_name, bh['signals_count'], bh['max_signals_per_min'])
            return False
        bh['signals_count'] += 1
        return True

    def record_strategy_error(self, strategy_name: str, error: Exception) -> None:
        """记录策略异常，连续错误超阈值则触发策略断路"""
        count = self._strategy_error_counts.get(strategy_name, 0) + 1
        self._strategy_error_counts[strategy_name] = count
        if count >= self._strategy_circuit_breaker_threshold:
            logging.critical("[R27-P0-DR-13] 策略断路触发: %s 连续错误%d>=%d",
                             strategy_name, count, self._strategy_circuit_breaker_threshold)
            bh = self._strategy_bulkheads.get(strategy_name)
            if bh:
                bh['_original_max_signals'] = bh.get('max_signals_per_min', 15)
                bh['max_signals_per_min'] = 0
                self._strategy_circuit_breaker_at[strategy_name] = time.time()

    def reset_strategy_error(self, strategy_name: str) -> None:
        """策略成功执行后重置异常计数"""
        self._strategy_error_counts[strategy_name] = 0

    def on_bar_update(self) -> None:
        self._bar_counter += 1
        try:
            if hasattr(self, '_last_bar_data') and self._last_bar_data:
                bd = self._box_detector
                d = self._last_bar_data
                bd.update_bar(
                    high=d.get('high', 0.0), low=d.get('low', 0.0),
                    close=d.get('close', 0.0), volume=d.get('volume', 0.0),
                )
                if d.get('iv', 0.0) > 0:
                    bd.update_iv(d['iv'])
                bd.detect_box()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _bd_err:
            logging.warning("[StrategyEcosystem] BoxDetector update/detect failed: %s", _bd_err)
        if self._bar_counter % 60 == 0:
            try:
                self.validate_position_consistency()
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass
            try:
                self.normalize_capital_allocation()
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass

    @property
    def box_detector(self) -> BoxDetector:
        return self._box_detector

    def _generate_trade_id(self) -> str:
        with self._lock:
            self._trade_id_counter += 1
            counter = self._trade_id_counter
        ts = datetime.now(CHINA_TZ).strftime('%Y%m%d%H%M%S')
        return f"ECO-{ts}-{counter:06d}"

    def reset_trade_id_counter_on_new_day(self) -> None:
        """在新交易日开始时调用，重置交易ID计数器"""
        today = datetime.now(CHINA_TZ).strftime('%Y%m%d')
        with self._lock:
            if not hasattr(self, '_last_trade_id_reset_date'):
                self._last_trade_id_reset_date = today
                return
            if self._last_trade_id_reset_date != today:
                self._trade_id_counter = 0
                self._last_trade_id_reset_date = today
                logger.info("[R13-P1-DEAD-08] _trade_id_counter已重置, new_date=%s", today)


# 裂缝38修复: 提供StrategyEcosystemCore别名供测试导入
StrategyEcosystemCore = StrategyEcosystem
