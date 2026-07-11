# MODULE_ID: M1-078
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
Phase4-Sprint12: ValidationRegistry — backtest_state验证器注册模式
将backtest_state.py的硬编码验证链重构为可注册验证器
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable
from dataclasses import dataclass


@runtime_checkable
class BacktestValidator(Protocol):
    name: str

    def validate(self, result: 'BacktestResult') -> 'ValidationResult':
        ...


@dataclass
class BacktestResult:
    strategy_name: str = ''
    total_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_loss_ratio: float = 0.0
    total_trades: int = 0
    params: Dict[str, Any] = None


@dataclass
class ValidationResult:
    validator_name: str
    passed: bool
    score: float = 0.0
    reason: str = ''
    details: Dict[str, Any] = None


@dataclass
class ValidationReport:
    passed: bool
    results: List[ValidationResult]
    overall_score: float = 0.0

    @property
    def failed_validators(self) -> List[str]:
        return [r.validator_name for r in self.results if not r.passed]


class ValidationRegistry:
    """验证器注册中心 — 替代backtest_state硬编码验证链

    支持动态注册/注销验证器，按注册顺序执行
    """

    def __init__(self):
        self._validators: Dict[str, BacktestValidator] = {}
        self._order: List[str] = []

    def register(self, name: str, validator: BacktestValidator) -> None:
        if name in self._validators:
            logging.warning("[ValidationRegistry] 验证器%s已存在,将被覆盖", name)
        self._validators[name] = validator
        if name not in self._order:
            self._order.append(name)

    def unregister(self, name: str) -> None:
        self._validators.pop(name, None)
        if name in self._order:
            self._order.remove(name)

    def run_all(self, result: BacktestResult) -> ValidationReport:
        results = []
        _total_score = 0.0
        for name in self._order:
            validator = self._validators.get(name)
            if validator is None:
                continue
            try:
                vr = validator.validate(result)
                results.append(vr)
                _total_score += vr.score
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[ValidationRegistry] 验证器%s异常: %s", name, e)
                results.append(ValidationResult(validator_name=name, passed=False, reason=f'异常: {e}'))
        _avg_score = _total_score / len(results) if results else 0.0
        return ValidationReport(
            passed=all(r.passed for r in results),
            results=results,
            overall_score=_avg_score,
        )

    def run_one(self, name: str, result: BacktestResult) -> Optional[ValidationResult]:
        validator = self._validators.get(name)
        if validator is None:
            return None
        try:
            return validator.validate(result)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            return ValidationResult(validator_name=name, passed=False, reason=f'异常: {e}')

    @property
    def validator_names(self) -> List[str]:
        return list(self._order)


class SharpeRatioValidator:
    name = "sharpe_ratio"

    def validate(self, result: BacktestResult) -> ValidationResult:
        threshold = 0.5
        passed = result.sharpe_ratio >= threshold
        return ValidationResult(
            validator_name=self.name, passed=passed,
            score=min(result.sharpe_ratio / 2.0, 1.0),
            reason=f"Sharpe={result.sharpe_ratio:.2f}{'≥' if passed else '<'}{threshold}")


class MaxDrawdownValidator:
    name = "max_drawdown"

    def validate(self, result: BacktestResult) -> ValidationResult:
        threshold = 0.30
        passed = result.max_drawdown <= threshold
        return ValidationResult(
            validator_name=self.name, passed=passed,
            score=1.0 - result.max_drawdown if result.max_drawdown < 1.0 else 0.0,
            reason=f"MaxDD={result.max_drawdown:.2%}{'≤' if passed else '>'}{threshold:.0%}")


class WinRateValidator:
    name = "win_rate"

    def validate(self, result: BacktestResult) -> ValidationResult:
        threshold = 0.35
        passed = result.win_rate >= threshold
        return ValidationResult(
            validator_name=self.name, passed=passed,
            score=result.win_rate,
            reason=f"WinRate={result.win_rate:.2%}{'≥' if passed else '<'}{threshold:.0%}")


class ProfitLossRatioValidator:
    name = "profit_loss_ratio"

    def validate(self, result: BacktestResult) -> ValidationResult:
        threshold = 1.0
        passed = result.profit_loss_ratio >= threshold
        return ValidationResult(
            validator_name=self.name, passed=passed,
            score=min(result.profit_loss_ratio / 2.0, 1.0),
            reason=f"PLR={result.profit_loss_ratio:.2f}{'≥' if passed else '<'}{threshold}")


class MinTradesValidator:
    name = "min_trades"

    def validate(self, result: BacktestResult) -> ValidationResult:
        threshold = 10
        passed = result.total_trades >= threshold
        return ValidationResult(
            validator_name=self.name, passed=passed,
            score=min(result.total_trades / 50.0, 1.0),
            reason=f"Trades={result.total_trades}{'≥' if passed else '<'}{threshold}")


def create_default_validation_registry() -> ValidationRegistry:
    registry = ValidationRegistry()
    registry.register("sharpe_ratio", SharpeRatioValidator())
    registry.register("max_drawdown", MaxDrawdownValidator())
    registry.register("win_rate", WinRateValidator())
    registry.register("profit_loss_ratio", ProfitLossRatioValidator())
    registry.register("min_trades", MinTradesValidator())
    return registry