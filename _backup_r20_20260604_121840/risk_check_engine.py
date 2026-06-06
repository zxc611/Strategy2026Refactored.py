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