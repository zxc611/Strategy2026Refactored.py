"""
Phase4-Sprint11: RiskCheckEngine — risk_check_service规则引擎模式
将risk_check_service.py(1396行)的硬编码检查链重构为可注册规则引擎
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable
from dataclasses import dataclass


@runtime_checkable
class RiskRule(Protocol):
    name: str
    severity: str

    def check(self, context: 'RiskContext') -> 'RiskCheckResult':
        ...


@dataclass(slots=True)
class RiskContext:
    signal: Dict[str, Any]
    equity: float = 0.0
    position_data: Optional[Dict[str, Any]] = None
    risk_service: Any = None


@dataclass
class RiskCheckResult:
    rule_name: str
    passed: bool
    severity: str = 'P2'
    reason: str = ''
    details: Dict[str, Any] = None


@dataclass(slots=True)
class RiskCheckReport:
    passed: bool
    results: List[RiskCheckResult]
    blocking_result: Optional[RiskCheckResult] = None

    @property
    def failed_rules(self) -> List[RiskCheckResult]:
        return [r for r in self.results if not r.passed]


class RiskCheckEngine:
    """规则引擎 — 替代risk_check_service硬编码检查链

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
            except Exception as e:
                logging.warning("[RiskCheckEngine] 规则%s执行异常: %s", rule.name, e)
                results.append(RiskCheckResult(rule_name=rule.name, passed=False, severity='P1', reason=f'异常: {e}'))
        return RiskCheckReport(passed=all(r.passed for r in results), results=results)

    @property
    def rule_names(self) -> List[str]:
        return [r.name for r in self._rules]


class PositionLimitRule:
    name = "position_limit"
    severity = "P0"

    def check(self, context: RiskContext) -> RiskCheckResult:
        max_positions = 3
        try:
            if context.risk_service and hasattr(context.risk_service, '_params'):
                max_positions = int(context.risk_service._params.get('max_open_positions', 3))
        except Exception:
            pass
        position_count = 0
        if context.position_data:
            position_count = context.position_data.get('position_count', 0)
        if position_count >= max_positions:
            return RiskCheckResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason=f"持仓{position_count}≥上限{max_positions}")
        return RiskCheckResult(rule_name=self.name, passed=True, severity=self.severity)


class MarginSufficiencyRule:
    name = "margin_sufficiency"
    severity = "P0"

    def check(self, context: RiskContext) -> RiskCheckResult:
        if context.equity <= 0:
            return RiskCheckResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason="权益≤0")
        return RiskCheckResult(rule_name=self.name, passed=True, severity=self.severity)


class DailyDrawdownRule:
    name = "daily_drawdown"
    severity = "P0"

    def check(self, context: RiskContext) -> RiskCheckResult:
        dd_pct = 0.0
        if context.position_data:
            dd_pct = context.position_data.get('daily_drawdown_pct', 0.0)
        hard_stop = 0.05
        if dd_pct >= hard_stop:
            return RiskCheckResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason=f"日回撤{dd_pct:.2%}≥硬停止{hard_stop:.2%}")
        return RiskCheckResult(rule_name=self.name, passed=True, severity=self.severity)


class NearExpiryRule:
    name = "near_expiry"
    severity = "P1"

    def check(self, context: RiskContext) -> RiskCheckResult:
        days = 999
        if context.position_data:
            days = context.position_data.get('days_to_expiry', 999)
        if days <= 3:
            return RiskCheckResult(rule_name=self.name, passed=False, severity=self.severity,
                                   reason=f"距到期仅{days}天")
        return RiskCheckResult(rule_name=self.name, passed=True, severity=self.severity)


class SignalCooldownRule:
    name = "signal_cooldown"
    severity = "P2"

    def check(self, context: RiskContext) -> RiskCheckResult:
        return RiskCheckResult(rule_name=self.name, passed=True, severity=self.severity)


def create_default_risk_check_engine() -> RiskCheckEngine:
    engine = RiskCheckEngine()
    engine.register(PositionLimitRule())
    engine.register(MarginSufficiencyRule())
    engine.register(DailyDrawdownRule())
    engine.register(NearExpiryRule())
    engine.register(SignalCooldownRule())
    return engine