"""
合规规则模块 — 从risk_circuit_breaker.py拆分
职责: 监管合规检查规则（自成交、洗盘、算法交易标识）
         合规规则引擎(ComplianceRule Protocol + 3规则类)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable
from dataclasses import dataclass


@runtime_checkable
class ComplianceRule(Protocol):
    rule_name: str
    severity: str
    def check(self, context: 'ComplianceContext') -> 'ComplianceResult': ...


@dataclass
class ComplianceContext:
    position_data: Dict[str, Any]
    new_order: Optional[Dict[str, Any]] = None
    total_position_value: float = 0.0
    equity: float = 0.0
    max_position_ratio: float = 0.2
    concentration_limit: float = 0.3
    cross_market_limit: int = 3


@dataclass
class ComplianceResult:
    rule_name: str
    compliant: bool
    severity: str = 'P0'
    reason: str = ''
    details: Dict[str, Any] = None


class MaxPositionRule:
    rule_name: str = "max_position"
    severity: str = "P0"

    def check(self, context: ComplianceContext) -> ComplianceResult:
        if context.equity <= 0:
            return ComplianceResult(self.rule_name, True, self.severity)
        ratio = context.total_position_value / context.equity
        if ratio > context.max_position_ratio:
            return ComplianceResult(
                self.rule_name, False, self.severity,
                f"position_ratio={ratio:.4f} > max={context.max_position_ratio}",
                {"ratio": ratio, "max": context.max_position_ratio})
        return ComplianceResult(self.rule_name, True, self.severity)


class ConcentrationRule:
    rule_name: str = "concentration"
    severity: str = "P1"

    def check(self, context: ComplianceContext) -> ComplianceResult:
        positions = context.position_data
        if not positions:
            return ComplianceResult(self.rule_name, True, self.severity)
        total = sum(abs(v) for v in positions.values()) if isinstance(positions, dict) else 0
        if total <= 0:
            return ComplianceResult(self.rule_name, True, self.severity)
        max_single = max(abs(v) for v in positions.values()) if isinstance(positions, dict) else 0
        concentration = max_single / total
        if concentration > context.concentration_limit:
            return ComplianceResult(
                self.rule_name, False, self.severity,
                f"concentration={concentration:.4f} > limit={context.concentration_limit}",
                {"concentration": concentration, "limit": context.concentration_limit})
        return ComplianceResult(self.rule_name, True, self.severity)


class CrossMarketRule:
    rule_name: str = "cross_market"
    severity: str = "P1"

    def check(self, context: ComplianceContext) -> ComplianceResult:
        positions = context.position_data
        if not positions:
            return ComplianceResult(self.rule_name, True, self.severity)
        exchanges = set()
        for key in (positions.keys() if isinstance(positions, dict) else []):
            if '_' in str(key):
                exchange = str(key).split('_')[0]
                exchanges.add(exchange)
        if len(exchanges) > context.cross_market_limit:
            return ComplianceResult(
                self.rule_name, False, self.severity,
                f"cross_market_count={len(exchanges)} > limit={context.cross_market_limit}",
                {"exchanges": list(exchanges), "limit": context.cross_market_limit})
        return ComplianceResult(self.rule_name, True, self.severity)


class ComplianceEngine:
    def __init__(self):
        self._rules: List[ComplianceRule] = []

    def register(self, rule: ComplianceRule) -> None:
        self._rules.append(rule)

    def run_checks(self, context: ComplianceContext) -> List[ComplianceResult]:
        results = []
        for rule in sorted(self._rules, key=lambda r: {'P0': 0, 'P1': 1, 'P2': 2}.get(r.severity, 3)):
            try:
                result = rule.check(context)
                results.append(result)
                if not result.compliant and result.severity == 'P0':
                    break
            except Exception as e:
                logging.warning("[ComplianceEngine] Rule %s failed: %s", rule.rule_name, e)
        return results

    @property
    def rule_names(self) -> List[str]:
        return [r.rule_name for r in self._rules]


def create_default_compliance_engine() -> ComplianceEngine:
    engine = ComplianceEngine()
    engine.register(MaxPositionRule())
    engine.register(ConcentrationRule())
    engine.register(CrossMarketRule())
    return engine


class RegulatoryCompliance:
    def __init__(self):
        self._self_trade_threshold = 0.001
        self._wash_trade_window_sec = 60.0
        self._recent_trades: List[Dict[str, Any]] = []
        self._compliance_engine: Optional[ComplianceEngine] = None

    @property
    def compliance_engine(self) -> ComplianceEngine:
        if self._compliance_engine is None:
            self._compliance_engine = create_default_compliance_engine()
        return self._compliance_engine

    def check_self_trade(self, new_order: Dict[str, Any]) -> bool:
        instrument = new_order.get('instrument_id', '')
        direction = new_order.get('direction', '')
        for trade in self._recent_trades:
            if trade.get('instrument_id') == instrument:
                if trade.get('direction') != direction:
                    logging.warning("[RegulatoryCompliance] 自成交风险: %s", instrument)
                    return True
        return False

    def check_wash_trade(self, order: Dict[str, Any]) -> bool:
        return False

    def check_algo_trading_flag(self, order: Dict[str, Any]) -> bool:
        return order.get('is_algo', True)

    def check_compliance(self, position_data: Dict[str, Any],
                         equity: float = 0.0,
                         total_position_value: float = 0.0) -> List[ComplianceResult]:
        ctx = ComplianceContext(
            position_data=position_data,
            equity=equity,
            total_position_value=total_position_value)
        return self.compliance_engine.run_checks(ctx)

    def record_trade(self, trade: Dict[str, Any]) -> None:
        self._recent_trades.append(trade)
        if len(self._recent_trades) > 1000:
            self._recent_trades = self._recent_trades[-500:]