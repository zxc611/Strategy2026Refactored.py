#!/usr/bin/env python3
"""
三层瀑布式评判引擎 — 生产级实现

架构：盈亏比(发动机) → 三角验证(安全壳) → 小资金约束(驾驶座)

与参数池优化脚本的集成方式：
  from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
  judge = CascadeJudge()
  adapted = adapt_backtest_result(train_result, test_result, params)
  report = judge.judge(adapted)

设计约束：
  - sortino_ratio: 优先取回测引擎直接返回值，兜底从returns序列计算下行标准差，最保守取sharpe*1.2
  - max_flat_period_days: 优先从nav_curve直接计算，无NAV时安全降级设999避免误判
  - peak_margin_used: 优先取回测引擎直接返回值，兜底从max_risk_ratio估算
"""

from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import math


class GateResult(Enum):
    PASS = auto()
    WARN = auto()
    BLOCK = auto()


@dataclass
class GateReport:
    gate_name: str
    result: GateResult
    value: float
    threshold: float
    reason: str = ""
    details: Dict = field(default_factory=dict)


@dataclass
class BacktestMetrics:
    """瀑布式评判所需的标准指标集"""
    profit_loss_ratio: float = 1.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    sharpe_ratio: float = 0.0
    max_consecutive_losses: int = 0
    max_flat_period_days: int = 0
    peak_margin_used: float = 0.0
    num_trades: int = 0
    test_profit_loss_ratio: float = 0.0
    overfit_flag: bool = False


@dataclass
class CascadeReport:
    passed: bool
    final_score: float
    gates: List[GateReport]
    warnings: List[str] = field(default_factory=list)
    fatal_reason: Optional[str] = None
    metrics: Optional[BacktestMetrics] = None

    def __str__(self):
        lines = ["=" * 50, "瀑布式评判报告", "=" * 50]
        for gate in self.gates:
            status = "PASS" if gate.result == GateResult.PASS else "WARN" if gate.result == GateResult.WARN else "BLOCK"
            lines.append(f"[{status}] {gate.gate_name}: {gate.value:.4f} (阈值: {gate.threshold})")
            if gate.reason:
                lines.append(f"   -> {gate.reason}")
        lines.append("-" * 50)
        if self.passed:
            lines.append(f"最终结果: 通过 | 综合得分: {self.final_score:.4f}")
        else:
            lines.append(f"最终结果: 否决 | 原因: {self.fatal_reason}")
        if self.warnings:
            lines.append(f"警告: {self.warnings}")
        return "\n".join(lines)


def _compute_max_flat_from_nav(nav_curve, tolerance: float = 0.001) -> int:
    """从净值曲线直接计算最大横盘天数

    横盘定义：NAV连续未创新高(tolerance以上)的最大天数
    Args:
        nav_curve: 净值序列
        tolerance: 创新高阈值(0.001=0.1%, 日频适用; 分钟频可放宽至0.005)
    """
    if not nav_curve or len(nav_curve) < 2:
        return 999
    peak = nav_curve[0]
    days_since_peak = 0
    max_flat = 0
    for nav in nav_curve[1:]:
        if nav > peak * (1.0 + tolerance):
            peak = nav
            max_flat = max(max_flat, days_since_peak)
            days_since_peak = 0
        else:
            days_since_peak += 1
    max_flat = max(max_flat, days_since_peak)
    return max_flat


def adapt_backtest_result(
    train_result: Dict[str, Any],
    test_result: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    sortino_est_mult: float = 1.2,
    sortino_est_add: float = 0.3,
    peak_margin_factor: float = 0.85,
    overfit_ratio: float = 1.5,
) -> BacktestMetrics:
    """将task_scheduler.run_backtest的dict返回值适配为BacktestMetrics

    字段推导规则:
    - profit_loss_ratio: 直接取train_result
    - sharpe_ratio: 直接取train_result
    - calmar_ratio: 直接取train_result
    - sortino_ratio: 优先取回测引擎直接值，次选从returns序列计算下行标准差，最保守取sharpe*1.2
    - max_consecutive_losses: 直接取train_result
    - max_flat_period_days: 优先从nav_curve直接计算，无NAV时安全降级999
    - peak_margin_used: 优先取回测引擎直接值，兜底从max_risk_ratio估算
    - test_profit_loss_ratio: 从test_result提取，用于交叉验证
    - overfit_flag: 训练/测试盈亏比差异>50%时标记过拟合
    """
    r = train_result
    t = test_result
    p = params or {}

    profit_loss_ratio = r.get("profit_loss_ratio", 1.0)
    sharpe_ratio = r.get("sharpe", 0.0)
    calmar_ratio = r.get("calmar", 0.0)
    max_consecutive_losses = r.get("max_consecutive_losses", 0)
    win_rate = r.get("win_rate", 0.5)
    num_trades = r.get("total_trades", 0)

    if "sortino_ratio" in r:
        sortino_ratio = r["sortino_ratio"]
    elif "returns" in r and isinstance(r["returns"], (list, tuple)) and len(r["returns"]) >= 10:
        returns = r["returns"]
        mean_ret = sum(returns) / len(returns)
        downside = [min(0, ret - mean_ret) ** 2 for ret in returns]
        n_downside = sum(1 for d in downside if d > 0)
        downside_dev = math.sqrt(sum(downside) / n_downside) if n_downside > 0 else 0.0
        sortino_ratio = (mean_ret / downside_dev * math.sqrt(252)) if downside_dev > 1e-10 else 0.0
    else:
        sortino_ratio = min(sharpe_ratio * sortino_est_mult, sharpe_ratio + sortino_est_add)

    nav_curve = r.get("nav_curve", None)
    if nav_curve is not None and isinstance(nav_curve, (list, tuple)) and len(nav_curve) >= 2:
        max_flat_period_days = _compute_max_flat_from_nav(nav_curve)
    else:
        max_flat_period_days = 999

    max_risk_ratio = p.get("max_risk_ratio", 0.3)
    peak_margin_used = r.get("peak_margin_used", None)
    if peak_margin_used is None:
        peak_margin_used = min(1.0, max_risk_ratio * peak_margin_factor)

    test_profit_loss_ratio = 0.0
    overfit_flag = False
    if t is not None:
        test_profit_loss_ratio = t.get("profit_loss_ratio", 0.0)
        if profit_loss_ratio > 0.1 and test_profit_loss_ratio != 0:
            ratio = abs(profit_loss_ratio / test_profit_loss_ratio)
            if ratio > overfit_ratio:
                overfit_flag = True

    return BacktestMetrics(
        profit_loss_ratio=profit_loss_ratio,
        sortino_ratio=sortino_ratio,
        calmar_ratio=calmar_ratio,
        sharpe_ratio=sharpe_ratio,
        max_consecutive_losses=max_consecutive_losses,
        max_flat_period_days=max_flat_period_days,
        peak_margin_used=peak_margin_used,
        num_trades=num_trades,
        test_profit_loss_ratio=test_profit_loss_ratio,
        overfit_flag=overfit_flag,
    )


class CascadeJudge:
    """
    三层瀑布式评判引擎
    盈亏比(发动机) → 三角验证(安全壳) → 小资金约束(驾驶座)
    """

    def __init__(
        self,
        min_profit_ratio: float = 1.8,
        min_sortino: float = 1.5,
        min_calmar: float = 0.8,
        min_sharpe: float = 1.2,
        max_consecutive_losses: int = 5,
        max_flat_days: int = 20,
        max_margin_ratio: float = 0.50,
        profit_ratio_weight: float = 0.60,
        sortino_weight: float = 0.40,
        calmar_weight: float = 0.30,
        sharpe_weight: float = 0.30,
        min_trades: int = 30,
        sortino_est_mult: float = 1.2,
        sortino_est_add: float = 0.3,
        peak_margin_factor: float = 0.85,
        overfit_ratio: float = 1.5,
        sigmoid_pr_center: float = 1.8,
        sigmoid_pr_scale: float = 1.2,
        sigmoid_sortino_center: float = 1.5,
        sigmoid_sortino_scale: float = 1.0,
        sigmoid_calmar_center: float = 0.8,
        sigmoid_calmar_scale: float = 0.5,
        sigmoid_sharpe_center: float = 1.2,
        sigmoid_sharpe_scale: float = 0.8,
    ):
        self.thresholds = {
            "profit_ratio": min_profit_ratio,
            "sortino": min_sortino,
            "calmar": min_calmar,
            "sharpe": min_sharpe,
        }
        self.constraints = {
            "consecutive_losses": max_consecutive_losses,
            "flat_days": max_flat_days,
            "margin_ratio": max_margin_ratio,
            "min_trades": min_trades,
        }
        self.weights = {
            "profit_ratio": profit_ratio_weight,
            "sortino": sortino_weight,
            "calmar": calmar_weight,
            "sharpe": sharpe_weight,
        }
        tri_total = sortino_weight + calmar_weight + sharpe_weight
        self.tri_weights = {
            "sortino": sortino_weight / tri_total,
            "calmar": calmar_weight / tri_total,
            "sharpe": sharpe_weight / tri_total,
        }
        self.est_params = {
            "sortino_mult": sortino_est_mult,
            "sortino_add": sortino_est_add,
            "peak_margin_factor": peak_margin_factor,
            "overfit_ratio": overfit_ratio,
        }
        self.sigmoid = {
            "pr_center": sigmoid_pr_center, "pr_scale": sigmoid_pr_scale,
            "sortino_center": sigmoid_sortino_center, "sortino_scale": sigmoid_sortino_scale,
            "calmar_center": sigmoid_calmar_center, "calmar_scale": sigmoid_calmar_scale,
            "sharpe_center": sigmoid_sharpe_center, "sharpe_scale": sigmoid_sharpe_scale,
        }

    @classmethod
    def from_config(cls, config_path: Optional[str] = None) -> "CascadeJudge":
        """从cascade_config.yaml加载三系统统一配置"""
        if config_path is None:
            import os
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "cascade_config.yaml",
            )
        try:
            import yaml
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            return cls(
                min_profit_ratio=cfg.get("gate1_profit_ratio", {}).get("min", 1.8),
                min_sortino=cfg.get("gate2_sortino", {}).get("min", 1.5),
                min_calmar=cfg.get("gate2_calmar", {}).get("min", 0.8),
                min_sharpe=cfg.get("gate2_sharpe", {}).get("min", 1.2),
                max_consecutive_losses=cfg.get("gate3_consecutive_losses", {}).get("max", 5),
                max_flat_days=cfg.get("gate3_flat_days", {}).get("max", 20),
                max_margin_ratio=cfg.get("gate3_margin_ratio", {}).get("max", 0.50),
                profit_ratio_weight=cfg.get("scoring", {}).get("profit_ratio_weight", 0.60),
                sortino_weight=cfg.get("scoring", {}).get("sortino_weight", 0.40),
                calmar_weight=cfg.get("scoring", {}).get("calmar_weight", 0.30),
                sharpe_weight=cfg.get("scoring", {}).get("sharpe_weight", 0.30),
                min_trades=cfg.get("data_quality", {}).get("min_trades", 30),
                sortino_est_mult=cfg.get("estimation", {}).get("sortino_est_mult", 1.2),
                sortino_est_add=cfg.get("estimation", {}).get("sortino_est_add", 0.3),
                peak_margin_factor=cfg.get("estimation", {}).get("peak_margin_factor", 0.85),
                overfit_ratio=cfg.get("estimation", {}).get("overfit_ratio", 1.5),
                sigmoid_pr_center=cfg.get("sigmoid", {}).get("pr_tri_center", 1.8),
                sigmoid_pr_scale=cfg.get("sigmoid", {}).get("pr_tri_scale", 1.2),
                sigmoid_sortino_center=cfg.get("sigmoid", {}).get("sortino_tri_center", 1.5),
                sigmoid_sortino_scale=cfg.get("sigmoid", {}).get("sortino_tri_scale", 1.0),
                sigmoid_calmar_center=cfg.get("sigmoid", {}).get("calmar_tri_center", 0.8),
                sigmoid_calmar_scale=cfg.get("sigmoid", {}).get("calmar_tri_scale", 0.5),
                sigmoid_sharpe_center=cfg.get("sigmoid", {}).get("sharpe_tri_center", 1.2),
                sigmoid_sharpe_scale=cfg.get("sigmoid", {}).get("sharpe_tri_scale", 0.8),
            )
        except Exception:
            return cls()

    def judge(self, metrics: BacktestMetrics) -> CascadeReport:
        gates = []
        warnings = []

        # ========== 第零关：数据质量前置检查 ==========
        min_trades = self.constraints["min_trades"]
        if metrics.num_trades > 0 and metrics.num_trades < min_trades:
            gate0 = GateReport(
                gate_name="数据质量检查",
                result=GateResult.BLOCK,
                value=float(metrics.num_trades),
                threshold=float(min_trades),
                reason=f"交易次数{metrics.num_trades}<{min_trades}，统计样本不足，评判不可信",
                details={"num_trades": metrics.num_trades, "min_trades": min_trades},
            )
            gates.append(gate0)
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate0.reason, metrics=metrics)

        # ========== 过拟合检查 ==========
        if metrics.overfit_flag:
            warnings.append(
                f"过拟合警告：训练盈亏比{metrics.profit_loss_ratio:.2f} vs 测试{metrics.test_profit_loss_ratio:.2f}，差异>50%"
            )

        # ========== 第一关：盈亏比验证（发动机） ==========
        gate1 = self._check_gate(
            name="盈亏比验证",
            value=metrics.profit_loss_ratio,
            threshold=self.thresholds["profit_ratio"],
            hard_block=True,
            reason_template="盈亏比{value:.2f} < 阈值{threshold}，策略盈利核心逻辑失效",
            details={"profit_loss_ratio": metrics.profit_loss_ratio},
        )
        gates.append(gate1)
        if gate1.result == GateResult.BLOCK:
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate1.reason, metrics=metrics)

        # ========== 第二关：三角验证（安全壳） ==========
        gate2a = self._check_gate(
            name="索提诺验证",
            value=metrics.sortino_ratio,
            threshold=self.thresholds["sortino"],
            hard_block=True,
            reason_template="索提诺{value:.2f} < 阈值{threshold}，下行风险失控",
            details={"sortino": metrics.sortino_ratio},
        )
        gates.append(gate2a)
        if gate2a.result == GateResult.BLOCK:
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate2a.reason, metrics=metrics)

        gate2b = self._check_gate(
            name="卡玛验证",
            value=metrics.calmar_ratio,
            threshold=self.thresholds["calmar"],
            hard_block=True,
            reason_template="卡玛{value:.2f} < 阈值{threshold}，回撤路径不可承受",
            details={"calmar": metrics.calmar_ratio},
        )
        gates.append(gate2b)
        if gate2b.result == GateResult.BLOCK:
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate2b.reason, metrics=metrics)

        gate2c = self._check_gate(
            name="夏普验证",
            value=metrics.sharpe_ratio,
            threshold=self.thresholds["sharpe"],
            hard_block=False,
            reason_template="夏普{value:.2f} < 阈值{threshold}，整体风险收益比偏低",
            details={"sharpe": metrics.sharpe_ratio},
        )
        gates.append(gate2c)
        if gate2c.result == GateResult.WARN:
            warnings.append(f"夏普比率偏低({metrics.sharpe_ratio:.2f})，建议关注策略稳定性")

        # ========== 第三关：小资金约束（驾驶座） ==========
        gate3a = self._check_constraint(
            name="连续亏损约束",
            value=metrics.max_consecutive_losses,
            threshold=self.constraints["consecutive_losses"],
            direction="max",
            reason_template="连续亏损{value}次 > 阈值{threshold}次，心理承受线突破",
            details={"max_consecutive_losses": metrics.max_consecutive_losses},
        )
        gates.append(gate3a)
        if gate3a.result == GateResult.BLOCK:
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate3a.reason, metrics=metrics)

        gate3b = self._check_constraint(
            name="横盘天数约束",
            value=metrics.max_flat_period_days,
            threshold=self.constraints["flat_days"],
            direction="max",
            reason_template="横盘{value}天 > 阈值{threshold}天，机会成本过高",
            details={"max_flat_period_days": metrics.max_flat_period_days, "nav_based": metrics.max_flat_period_days != 999},
        )
        gates.append(gate3b)
        if gate3b.result == GateResult.BLOCK:
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate3b.reason, metrics=metrics)

        gate3c = self._check_constraint(
            name="保证金约束",
            value=metrics.peak_margin_used,
            threshold=self.constraints["margin_ratio"],
            direction="max",
            reason_template="保证金峰值{value:.1%} > 阈值{threshold:.1%}，强平风险",
            details={"peak_margin_used": metrics.peak_margin_used},
        )
        gates.append(gate3c)
        if gate3c.result == GateResult.BLOCK:
            return CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate3c.reason, metrics=metrics)

        # ========== 综合评分 ==========
        final_score = self._calculate_score(
            metrics.profit_loss_ratio,
            metrics.sortino_ratio,
            metrics.calmar_ratio,
            metrics.sharpe_ratio,
        )

        return CascadeReport(passed=True, final_score=final_score, gates=gates, warnings=warnings, metrics=metrics)

    def _check_gate(self, name: str, value: float, threshold: float,
                    hard_block: bool, reason_template: str,
                    details: Optional[Dict] = None) -> GateReport:
        if value >= threshold:
            return GateReport(gate_name=name, result=GateResult.PASS, value=value, threshold=threshold, details=details or {})
        elif hard_block:
            return GateReport(
                gate_name=name, result=GateResult.BLOCK, value=value, threshold=threshold,
                reason=reason_template.format(value=value, threshold=threshold),
                details=details or {},
            )
        else:
            return GateReport(
                gate_name=name, result=GateResult.WARN, value=value, threshold=threshold,
                reason=reason_template.format(value=value, threshold=threshold),
                details=details or {},
            )

    def _check_constraint(self, name: str, value: float, threshold: float,
                          direction: str, reason_template: str,
                          details: Optional[Dict] = None) -> GateReport:
        passed = value <= threshold if direction == "max" else value >= threshold
        if passed:
            return GateReport(gate_name=name, result=GateResult.PASS, value=value, threshold=threshold, details=details or {})
        else:
            return GateReport(
                gate_name=name, result=GateResult.BLOCK, value=value, threshold=threshold,
                reason=reason_template.format(value=value, threshold=threshold),
                details=details or {},
            )

    def _calculate_score(self, profit_ratio: float, sortino: float,
                         calmar: float, sharpe: float) -> float:
        """Sigmoid归一化综合评分（三角验证权重内部归一化，参数从配置读取）"""
        s = self.sigmoid
        pr_norm = 1.0 / (1.0 + math.exp(-(profit_ratio - s["pr_center"]) / s["pr_scale"]))
        sortino_norm = 1.0 / (1.0 + math.exp(-(sortino - s["sortino_center"]) / s["sortino_scale"]))
        calmar_norm = 1.0 / (1.0 + math.exp(-(calmar - s["calmar_center"]) / s["calmar_scale"]))
        sharpe_norm = 1.0 / (1.0 + math.exp(-(sharpe - s["sharpe_center"]) / s["sharpe_scale"]))

        tri_score = (self.tri_weights["sortino"] * sortino_norm +
                     self.tri_weights["calmar"] * calmar_norm +
                     self.tri_weights["sharpe"] * sharpe_norm)

        final = self.weights["profit_ratio"] * pr_norm + (1.0 - self.weights["profit_ratio"]) * tri_score
        return round(final, 4)
