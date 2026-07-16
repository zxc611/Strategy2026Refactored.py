# MODULE_ID: M1-073
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from governance.mode_config import ModeConfig

from governance.mode_position_sizing import PredictiveStateEngine


class ModeEnginePropagationService:

    def __init__(self, facade=None):
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


    def _apply_to_risk_service(
        self, config: ModeConfig, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            rs = self._component_registry.get('RiskService')
            if rs is None:
                from risk.risk_service import get_risk_service
                rs = get_risk_service()
            rs.set_capital_scale(scale_str)
            try:
                if hasattr(rs, 'params') and rs.params is not None:
                    rs.params['consecutive_loss_limit'] = config.consecutive_loss_limit
                    rs.params['asymmetric_risk_enabled'] = config.asymmetric_risk_enabled
                    rs.params['recovery_timeout_seconds'] = config.recovery_timeout_seconds
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _d08_e:
                logging.debug('[R3-D-08] ModeConfig参数传递到RiskService.params失败: %s', _d08_e)
            propagated.append('RiskService')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            errors.append(f'RiskService: {e}')
            logging.error('[ModeEngine] RiskService propagation failed (critical): %s', e)

    def _apply_to_predictive_state_engine(
        self, config: ModeConfig, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            pse = PredictiveStateEngine.get_instance()
            self._component_registry['PredictiveStateEngine'] = pse
            propagated.append('PredictiveStateEngine')
            logging.info('[P-07] PredictiveStateEngine activated for scale=%s', scale_str)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            errors.append(f'PredictiveStateEngine: {e}')
            logging.error('[ModeEngine] PredictiveStateEngine propagation failed (non-critical, skipping): %s', e)

    def _auto_register_known_components(self) -> None:
        """在。_init__中尝试自动注册所有已知组件，避免仅注册单个组件"""
        _known_components = [
            ('RiskService', 'risk.risk_service', 'get_risk_service'),
            ('SignalService', 'signal.signal_service', 'get_signal_service'),
            ('StateParamManager', 'config.state_param', 'get_state_param_manager'),
            ('BoxSpringStrategy', 'strategy.box_spring_strategy_impl', 'get_box_spring_strategy'),
        ]
        for name, module_path, factory_name in _known_components:
            if name in self._component_registry:
                continue
            try:
                import importlib
                mod = importlib.import_module(module_path)
                factory = getattr(mod, factory_name, None)
                if factory and callable(factory):
                    instance = factory()
                    self._component_registry[name] = instance
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

    def _sync_component_registry(self, component_name: str, component_instance: Any) -> None:
        """确保组件注册表与import获取的实例同步"""
        try:
            self._component_registry[component_name] = component_instance
            logging.debug('[P-23] Component registry synced: %s', component_name)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning('[P-23] Component registry sync failed for %s: %s', component_name, e)

    def _apply_to_signal_service(
        self, config: ModeConfig, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            ss = self._component_registry.get('SignalService')
            if ss is None:
                from signal.signal_service import get_signal_service
                ss = get_signal_service()
            if config.plr_filter_enabled and config.min_estimated_plr > 0:
                ss.enable_plr_filter(min_estimated_plr=config.min_estimated_plr)
            else:
                ss.disable_plr_filter()
            propagated.append('SignalService')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            errors.append(f'SignalService: {e}')
            logging.error('[ModeEngine] SignalService propagation failed (critical): %s (mode=%s)', e, scale_str)

    def _apply_to_state_param_manager(
        self, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            spm = self._component_registry.get('StateParamManager')
            if spm is None:
                from config.state_param import get_state_param_manager
                spm = get_state_param_manager()
            spm.set_capital_scale(scale_str)
            propagated.append('StateParamManager')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            errors.append(f'StateParamManager: {e}')
            logging.error('[ModeEngine] StateParamManager propagation failed (non-critical, skipping): %s', e)

    def _apply_to_box_spring_strategy(
        self, scale_str: str,
        propagated: List[str], errors: List[str],
    ) -> None:
        try:
            bss = self._component_registry.get('BoxSpringStrategy')
            if bss is None:
                from strategy.box_spring_strategy_impl import get_box_spring_strategy
                bss = get_box_spring_strategy()
            if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                bss.set_capital_scale(scale_str)
            else:
                bss._capital_scale = scale_str
            propagated.append('BoxSpringStrategy')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            errors.append(f'BoxSpringStrategy: {e}')
            logging.error('[ModeEngine] BoxSpringStrategy propagation failed (non-critical, skipping): %s', e)

    def _rollback(
        self, old_config: Optional[ModeConfig], old_scale: str,
        propagated: List[str],
    ) -> None:
        logging.warning('[ModeEngine] Rolling back %d components', len(propagated))
        for comp in reversed(propagated):
            try:
                if comp == 'RiskService':
                    rs = self._component_registry.get('RiskService')
                    if rs is None:
                        from risk.risk_service import get_risk_service
                        rs = get_risk_service()
                    rs.set_capital_scale(old_scale)
                elif comp == 'SignalService':
                    if old_config is None:
                        continue
                    ss = self._component_registry.get('SignalService')
                    if ss is None:
                        from signal.signal_service import get_signal_service
                        ss = get_signal_service()
                    if old_config.plr_filter_enabled:
                        ss.enable_plr_filter(min_estimated_plr=old_config.min_estimated_plr)
                    else:
                        ss.disable_plr_filter()
                elif comp == 'StateParamManager':
                    spm = self._component_registry.get('StateParamManager')
                    if spm is None:
                        from config.state_param import get_state_param_manager
                        spm = get_state_param_manager()
                    spm.set_capital_scale(old_scale)
                elif comp == 'BoxSpringStrategy':
                    bss = self._component_registry.get('BoxSpringStrategy')
                    if bss is None:
                        from strategy.box_spring_strategy_impl import get_box_spring_strategy
                        bss = get_box_spring_strategy()
                    if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                        bss.set_capital_scale(old_scale)
                    else:
                        bss._capital_scale = old_scale
                elif comp == 'OrderService':
                    logging.info('[ModeEngine] R25-TO-P1-01-FIX: OrderService回滚(无需特殊操作)')
                elif comp == 'PositionService':
                    logging.info('[ModeEngine] R25-TO-P1-01-FIX: PositionService回滚(无需特殊操作)')
                elif comp == 'PredictiveStateEngine':
                    logging.info('[ModeEngine] R26-P1-TO-05: PredictiveStateEngine回滚(无需特殊操作)')
                else:
                    logging.warning('[ModeEngine] R26-P1-TO-05: _rollback: 未知组件 %s，无法回滚', comp)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as re:
                logging.error('[ModeEngine] Rollback failed for %s: %s', comp, re)

    def check_auto_recovery(self) -> Optional[str]:
        """定期检查降级后是否满足恢复条件，满足则自动切回原规模
        Returns:
            恢复到的规模字符串，或None（未恢复）'
        """
        if self._degraded_at is None or self._degraded_from is None:
            return None
        now = time.time()
        if now - self._last_recovery_check < self._auto_recovery_check_interval:
            return None
        self._last_recovery_check = now
        try:
            rs = self._component_registry.get('RiskService')
            if rs is None:
                from risk.risk_service import get_risk_service
                rs = get_risk_service()
            safety = getattr(rs, '_safety_meta_layer', None)
            if safety is not None:
                # [P0-29修复] 从 _risk_cb_half_open 派生暂停状态
                cb = getattr(safety, '_risk_cb_half_open', None)
                paused_until = (cb.opened_at + cb.open_duration
                                if cb is not None and cb.state == 'OPEN' and cb.opened_at > 0
                                else 0.0)
                if paused_until > now:
                    logging.info("[R27-P0-DR-08] 断路器仍在暂停中(剩余%.0fs)，暂不恢复",
                                paused_until - now)
                    return None
                hard_stop = getattr(safety, '_daily_hard_stop_triggered', False)
                if hard_stop:
                    logging.info("[R27-P0-DR-08] 日回撤硬停止仍生效，暂不恢复")
                    return None
            target_scale = self._degraded_from
            logging.info("[R27-P0-DR-08] 降级恢复条件满足: %s→%s", self._scale_str, target_scale)
            result = self.switch_mode(target_scale, reason='auto_recovery_from_degradation')
            if result.get('success'):
                return target_scale
            else:
                logging.warning("[R27-P0-DR-08] 自动恢复切换失败: %s", result.get('error'))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-08] 自动恢复检查异常: %s", e)
        return None

ModeEnginePropagationMixin = ModeEnginePropagationService
