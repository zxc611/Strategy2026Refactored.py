# [M1-29] ��ع�������
# MODULE_ID: M1-211
"""
Phase4-Sprint11: RiskCheckEngine �?risk_check_service规则引擎模式
将risk_check_service.py(1396�?的硬编码检查链重构为可注册规则引擎
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable
from dataclasses import dataclass


@runtime_checkable
class RiskRule(Protocol):
    name: str
    severity: str

    def check(self, context: 'RiskContext') -> 'RiskRuleResult':
        ...


@dataclass(slots=True)
class RiskContext:
    signal: Dict[str, Any]
    equity: float = 0.0
    position_data: Optional[Dict[str, Any]] = None
    risk_service: Any = None


@dataclass
class RiskRuleResult:
    rule_name: str
    passed: bool
    severity: str = 'P2'
    reason: str = ''
    details: Dict[str, Any] = None


@dataclass(slots=True)
class RiskCheckReport:
    passed: bool
    results: List[RiskRuleResult]
    blocking_result: Optional[RiskRuleResult] = None

    @property
    def failed_rules(self) -> List[RiskRuleResult]:
        return [r for r in self.results if not r.passed]


class RiskCheckEngine:
    """规则引擎 �?替代risk_check_service硬编码检查链

    规则按severity排序: P0 > P1 > P2
    P0规则失败立即返回(阻断)，P1/P2规则失败记录但不阻断
    """

    def __init__(self, rules: Optional[List[RiskRule]] = None):
        self._rules: List[RiskRule] = []
        if rules:
            for rule in rules:
                self.register(rule)

    def register(self, rule: RiskRule) -> None:
        self._rules.append(rule)
        self._rules.sort(key=lambda r: {'P0': 0, 'P1': 1, 'P2': 2}.get(r.severity, 3))

    def run_checks(self, context: RiskContext) -> RiskCheckReport:
        results = []
        for rule in self._rules:
            try:
                result = rule.check(context)
                results.append(result)
                if not result.passed and result.severity == 'P0':
                    return RiskCheckReport(passed=False, results=results, blocking_result=result)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[RiskCheckEngine] 规则%s执行异常: %s", rule.name, e)
                results.append(RiskRuleResult(rule_name=rule.name, passed=False, severity='P1', reason=f'异常: {e}'))
        return RiskCheckReport(passed=all(r.passed for r in results), results=results)

    @property
    def rule_names(self) -> List[str]:
        return [r.name for r in self._rules]

    def check_sharpe_iron_rule(self, signal: Dict[str, Any]) -> RiskRuleResult:
        """P0铁律门控: Sharpe比率铁律检�?

        当策略Sharpe低于阈值时阻断交易，防止低质量信号开仓�?
        该方法从risk_check_service._check_sharpe_iron_rule迁移而来�?
        """
        try:
            from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE
            _sharpe_threshold = DEFAULT_PARAM_TABLE.get('sharpe_iron_rule_threshold', 0.5)
            signal_sharpe = signal.get('sharpe', None) if signal else None
            if signal_sharpe is not None and signal_sharpe < _sharpe_threshold:
                return RiskRuleResult(
                    rule_name='sharpe_iron_rule', passed=False, severity='P0',
                    reason=f'Sharpe={signal_sharpe:.3f}<{_sharpe_threshold}',
                )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[RiskCheckEngine.check_sharpe_iron_rule] 检查异�? %s", e)
        return RiskRuleResult(rule_name='sharpe_iron_rule', passed=True, severity='P0')


class PositionLimitRule:
    name = "position_limit"
    severity = "P0"

    def check(self, context: RiskContext) -> RiskRuleResult:
        max_positions = 3
        try:
            if context.risk_service and hasattr(context.risk_service, '_params'):
                max_positions = int(context.risk_service._params.get('max_open_positions', 3))
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        position_count = 0
        if context.position_data:
            position_count = context.position_data.get('position_count', 0)
        if position_count >= max_positions:
            return RiskRuleResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason=f"持仓{position_count}≥上限{max_positions}")
        return RiskRuleResult(rule_name=self.name, passed=True, severity=self.severity)


class MarginSufficiencyRule:
    name = "margin_sufficiency"
    severity = "P0"

    def check(self, context: RiskContext) -> RiskRuleResult:
        if context.equity <= 0:
            return RiskRuleResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason="权益�?")
        return RiskRuleResult(rule_name=self.name, passed=True, severity=self.severity)


class DailyDrawdownRule:
    name = "daily_drawdown"
    severity = "P0"

    def __init__(self, hard_stop_pct: float = None):
        self._hard_stop_pct = hard_stop_pct

    def check(self, context: RiskContext) -> RiskRuleResult:
        dd_pct = 0.0
        _prev_5day_avg = 0.0
        _multiplier = 2.0
        _daily_start_equity = None
        if context.position_data:
            dd_pct = context.position_data.get('daily_drawdown_pct', 0.0)
            _prev_5day_avg = context.position_data.get('prev_5day_avg_profit', 0.0)
            _multiplier = context.position_data.get('daily_drawdown_multiplier', 2.0)
            _daily_start_equity = context.position_data.get('daily_start_equity', None)
        from ali2026v3_trading.infra.risk_rules import resolve_and_check_daily_drawdown
        should_stop, reason = resolve_and_check_daily_drawdown(
            daily_drawdown_pct=dd_pct,
            hard_stop_pct=self._hard_stop_pct,
            prev_5day_avg_profit=_prev_5day_avg,
            multiplier=_multiplier,
            daily_start_equity=_daily_start_equity,
        )
        if should_stop:
            return RiskRuleResult(rule_name=self.name, passed=False, severity=self.severity, reason=reason)
        return RiskRuleResult(rule_name=self.name, passed=True, severity=self.severity)


class NearExpiryRule:
    name = "near_expiry"
    severity = "P1"

    def check(self, context: RiskContext) -> RiskRuleResult:
        days = 999
        if context.position_data:
            days = context.position_data.get('days_to_expiry', 999)
        if days <= 3:
            return RiskRuleResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason=f"距到期仅{days}�?)
        return RiskRuleResult(rule_name=self.name, passed=True, severity=self.severity)


class SignalCooldownRule:
    name = "signal_cooldown"
    severity = "P2"

    def check(self, context: RiskContext) -> RiskRuleResult:
        return RiskRuleResult(rule_name=self.name, passed=True, severity=self.severity)


def create_default_risk_check_engine() -> RiskCheckEngine:
    engine = RiskCheckEngine()
    engine.register(PositionLimitRule())
    engine.register(MarginSufficiencyRule())
    engine.register(DailyDrawdownRule())
    engine.register(NearExpiryRule())
    engine.register(SignalCooldownRule())
    return engine