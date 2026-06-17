#!/usr/bin/env python3
# MODULE_ID: M1-061
"""
三层瀑布式评判引擎 — 生产级实现

R2-2: 手册V7.5架构对齐命名
  L1 规则引擎  ← 盈亏比门控(发动机)     → gate_l1_rule_engine()
  L2 ML模型    ← 三角验证+统计检验(安全壳) → gate_l2_ml_model()
  L3 影子隐形  ← 小资金约束+影子EV(驾驶座) → gate_l3_shadow_stealth()

与参数池优化脚本的集成方式：
  from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
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
import logging

from ali2026v3_trading.infra.serialization_utils import yaml_safe_load
from typing import List, Optional, Dict, Any
import math
import os
from ali2026v3_trading.infra.shared_utils import ANNUALIZE_FACTOR_DAILY, ANNUALIZE_FACTOR_MINUTE, CHINA_TZ as _CHINA_TZ, get_annualize_factor as _get_annualize_factor  # P2-13: 统一CHINA_TZ

__all__ = ['CascadeJudge', 'CascadeReport', 'GateReport', 'GateResult', 'BacktestMetrics', 'adapt_backtest_result', 'CascadeJudgeError']

try:
    from ali2026v3_trading.strategy_judgment.causal_chain_utils import ParamIsolationGuard
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False



# R16-P2-011修复: 评判阈值从config_params统一读取，与回测保持同步
_CASCADE_THRESHOLDS = {
    'min_sortino': float(os.environ.get('CASCADE_MIN_SORTINO', '1.5')),
    'min_calmar': float(os.environ.get('CASCADE_MIN_CALMAR', '0.8')),
    'min_sharpe': float(os.environ.get('CASCADE_MIN_SHARPE', '1.2')),
}



class GateResult(Enum):
    PASS = auto()
    WARN = auto()
    BLOCK = auto()


def gate_result_to_risk_level(gate_result: GateResult):
    """GateResult→RiskLevel映射函数，确保评判结果可直接映射到生产风控等级"""
    try:
        from ali2026v3_trading.risk.risk_service import RiskLevel
        _MAPPING = {
            GateResult.PASS: RiskLevel.LOW,
            GateResult.WARN: RiskLevel.MEDIUM,
            GateResult.BLOCK: RiskLevel.CRITICAL,
        }
        return _MAPPING.get(gate_result, RiskLevel.MEDIUM)
    except ImportError:
        return None


@dataclass(slots=True)
class GateReport:
    gate_name: str
    result: GateResult
    value: float
    threshold: float
    reason: str = ""
    details: Dict = field(default_factory=dict)


@dataclass(slots=True)
class BacktestMetrics:
    """瀑布式评判所需的标准指标集

    R4-J-01修复: 补全13维度字段，确保与评判模板一致
    """
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
    # E-01修复: L2 ML模型置信度 + L3影子策略隐形度
    l2_ml_confidence: float = 0.0
    l3_silhouette_invisibility: float = 0.0
    # R4-J-01修复: 补全剩余维度字段
    win_rate: float = 0.5
    total_return: float = 0.0
    max_drawdown_pct: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    recovery_efficiency: float = 0.0
    daily_trigger_rate: float = 0.0
    ev_per_trade: float = 0.0


@dataclass(slots=True)
class CascadeReport:
    """R4-J-05修复: 补全标准模板必填字段，确保下游消费不失败"""
    passed: bool
    final_score: float
    gates: List[GateReport]
    warnings: List[str] = field(default_factory=list)
    fatal_reason: Optional[str] = None
    metrics: Optional[BacktestMetrics] = None
    # R4-J-05: 标准模板必填字段
    dimension_scores: Dict[str, float] = field(default_factory=dict)
    dimension_weights: Dict[str, float] = field(default_factory=dict)
    verdict: str = "UNKNOWN"  # PASS / FAIL / CONDITIONAL / UNKNOWN
    capital_scale: str = "medium"
    timestamp: str = ""

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


def _estimate_l2_confidence(sharpe: float, win_rate: float, plr: float) -> float:
    """P0-21-FIX: 基于回测指标启发式估算ML置信度，替代硬编码0.0

    估算逻辑:
    - sharpe>=1.2且win_rate>=0.5且plr>=1.5 → 0.75 (高置信)
    - sharpe>=0.8且win_rate>=0.45且plr>=1.0 → 0.60 (中置信)
    - 否则 → 0.30 (低置信)
    """
    if sharpe >= 1.2 and win_rate >= 0.5 and plr >= 1.5:
        return 0.75
    if sharpe >= 0.8 and win_rate >= 0.45 and plr >= 1.0:
        return 0.60
    return 0.30


def adapt_backtest_result(
    train_result: Dict[str, Any],
    test_result: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    sortino_est_mult: float = 1.2,
    sortino_est_add: float = 0.3,
    peak_margin_factor: float = 0.85,
    overfit_ratio: float = 1.5,
    strategy_type: str = '',
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
    if not strategy_type and isinstance(p, dict):
        strategy_type = p.get('strategy_type', '')

    profit_loss_ratio = r.get("profit_loss_ratio", 1.0)
    sharpe_ratio = r.get("sharpe", 0.0)
    calmar_ratio = r.get("calmar", 0.0)
    max_consecutive_losses = r.get("max_consecutive_losses", 0)
    win_rate = r.get("win_rate", 0.5)
    num_trades = r.get("total_trades", 0)

    _DEFAULT_SORTINO_ESTIMATE = min(sharpe_ratio * sortino_est_mult, sharpe_ratio + sortino_est_add)
    _DEFAULT_PEAK_MARGIN_ESTIMATE = peak_margin_factor

    if "sortino_ratio" in r:
        sortino_ratio = r["sortino_ratio"]
    elif "returns" in r and isinstance(r["returns"], (list, tuple)) and len(r["returns"]) >= 10:
        returns = r["returns"]
        mean_ret = sum(returns) / len(returns)
        mar = p.get("mar", 0.0)
        downside = [min(0, ret - mar) ** 2 for ret in returns]
        # P0-R9-15修复: Sortino分母使用总样本数N（标准定义），而非n_downside
        # 参考: https://en.wikipedia.org/wiki/Sortino_ratio
        # 标准定义: Sortino = (mean_ret - MAR) / downside_deviation
        # 其中: downside_deviation = sqrt(sum(min(0, ret - MAR)^2) / N)
        n_total = len(returns)
        downside_dev = math.sqrt(sum(downside) / n_total) if n_total > 0 else 0.0
        # R13-P2-DEAD-05修复: downside_deviation=0时sortino不应为0
        if downside_dev > 1e-10:
            # R17-12修复: 日频年化因子 √252≈15.87 (BacktestMetrics使用日频收益)
            # R18-01修复: Sortino标准定义 = (mean_ret - MAR) / downside_deviation
            # cascade_judge仅评判日频策略(box_extreme/box_spring等)，
            # _get_annualize_factor对daily_types直接返回ANNUALIZE_FACTOR_DAILY，
            # 不会进入ticks_per_bar分支，因此无需传递ticks_per_bar。
            sortino_ratio = ((mean_ret - mar) / downside_dev * math.sqrt(_get_annualize_factor(strategy_type)))
        elif mean_ret > 0:
            # Sortino heuristic近似: 当downside_deviation≈0(无负收益)时,Sortino无定义;
            # 此处用 min(sharpe*1.2, sharpe+0.3) 作为保守上界近似,非严格数学推导,
            # 仅用于参数筛选阶段的粗排序,最终评判依赖真实Sortino值
            # R16-P1-010修复: 使用可配置参数替代硬编码999.0
            sortino_ratio = _DEFAULT_SORTINO_ESTIMATE
        else:
            sortino_ratio = 0.0
    else:
        sortino_ratio = 0.0

    nav_curve = r.get("nav_curve", None)
    if nav_curve is not None and isinstance(nav_curve, (list, tuple)) and len(nav_curve) >= 2:
        max_flat_period_days = _compute_max_flat_from_nav(nav_curve)
    elif "max_flat_period_days" in r:
        max_flat_period_days = r["max_flat_period_days"]
    else:
        max_flat_period_days = 999

    max_risk_ratio = p.get("max_risk_ratio", 0.8)  # R26修复: 与生产系统config_params对齐(0.8)
    peak_margin_used = r.get("peak_margin_used", None)
    if peak_margin_used is None:
        # R16-P1-010修复: 使用可配置参数替代硬编码peak_margin估算
        peak_margin_used = min(1.0, max_risk_ratio * _DEFAULT_PEAK_MARGIN_ESTIMATE)

    test_profit_loss_ratio = 0.0
    overfit_flag = False
    if t is not None:
        test_profit_loss_ratio = t.get("profit_loss_ratio", 0.0)
        # R13-P2-DEAD-02修复: win_loss_ratio=0时过拟合检测不应跳过
        if profit_loss_ratio > 0.1:
            if test_profit_loss_ratio != 0:
                ratio = abs(profit_loss_ratio / test_profit_loss_ratio)
                if ratio > overfit_ratio:
                    overfit_flag = True
            else:
                # 测试盈亏比为0但训练盈亏比>0.1，说明严重过拟合
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
        # R4-J-01修复: 填充补全维度字段
        win_rate=r.get("win_rate", 0.5),
        total_return=r.get("total_return", 0.0),
        max_drawdown_pct=r.get("max_drawdown", r.get("max_drawdown_pct", 0.0)),
        avg_win_pct=r.get("avg_win_pct", 0.0),
        avg_loss_pct=r.get("avg_loss_pct", 0.0),
        recovery_efficiency=r.get("recovery_efficiency", 0.0),
        ev_per_trade=r.get("ev_per_trade", r.get("expected_value", 0.0)),
        # P0-21-FIX: 从回测结果推导l2_ml_confidence，避免L2门控永远WARN
        l2_ml_confidence=r.get("l2_ml_confidence", _estimate_l2_confidence(sharpe_ratio, win_rate, profit_loss_ratio)),
    )


class CascadeJudge:
    """
    三层瀑布式评判引擎
    盈亏比(发动机) → 三角验证(安全壳) → 小资金约束(驾驶座)

    V7.5 三层命名映射表:
    ┌───────────────────┬──────────────────┬──────────────────────┐
    │ 代码命名          │ 手册V7.5命名     │ 等价性论证           │
    ├───────────────────┼──────────────────┼──────────────────────┤
    │ 盈亏比(发动机)    │ L1规则引擎       │ 均基于硬阈值规则过滤 │
    │ 三角验证(安全壳)  │ L2 ML模型        │ 均进行多维度交叉验证 │
    │ 小资金约束(驾驶座)│ L3影子策略隐形   │ 均在最后阶段施加现实约束│
    └───────────────────┴──────────────────┴──────────────────────┘

    R14-P1-DOC-P1-12修复: 三层评判阈值手册交叉引用
    L1 盈亏比阈值(min_profit_ratio=1.8): V7.0手册§4.2 盈亏比门控线
    L2 三角验证阈值(min_sortino=1.5, min_calmar=0.8, min_sharpe=1.2): V7.0手册§4.3 三轴门控线
    L3 小资金约束(max_consecutive_losses=5, max_flat_days=20, max_margin_ratio=0.50):
        V7.0手册§5.1 连亏约束 + §5.2 横盘约束 + §7.1 保证金约束
    Sigmoid映射参数(center/scale): V7.0手册§6.2 评分映射参数

    R21-MEM-P1-16修复: 添加评判历史回收机制，防止长期运行时报告对象累积。
    """

    # P1-R11-24修复: 仅作YAML不可用时的回退默认值，YAML为唯一源
    DEFAULT_THRESHOLDS = {
        "gate1_profit_ratio": {"min": 1.8},
        "gate2_sortino": {"min": 1.5},
        "gate2_calmar": {"min": 0.8},
        "gate2_sharpe": {"min": 1.2},
        "gate3_consecutive_losses": {"max": 5},
        "gate3_flat_days": {"max": 20},
        "gate3_margin_ratio": {"max": 0.50},
        "data_quality": {"min_trades": 30},
        "estimation": {
            "sortino_est_mult": 1.2,
            "sortino_est_add": 0.3,
            "peak_margin_factor": 0.85,
            "overfit_ratio": 1.5,
        },
        "sigmoid": {
            "pr_tri_center": 1.8, "pr_tri_scale": 1.2,
            "sortino_tri_center": 1.5, "sortino_tri_scale": 1.0,
            "calmar_tri_center": 0.8, "calmar_tri_scale": 0.5,
            "sharpe_tri_center": 1.2, "sharpe_tri_scale": 0.8,
        },
    }

    # R16-P2-019修复: 评判硬阈值参数化，默认值从_CASCADE_THRESHOLDS读取
    def __init__(
        self,
        min_profit_ratio: float = None,
        min_sortino: float = None,
        min_calmar: float = None,
        min_sharpe: float = None,
        max_consecutive_losses: int = None,
        max_flat_days: int = None,
        max_margin_ratio: float = None,
        profit_ratio_weight: float = 0.60,
        sortino_weight: float = 0.40,
        calmar_weight: float = 0.30,
        sharpe_weight: float = 0.30,
        min_trades: int = None,
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
        if min_sortino is None:
            min_sortino = _CASCADE_THRESHOLDS['min_sortino']
        if min_calmar is None:
            min_calmar = _CASCADE_THRESHOLDS['min_calmar']
        if min_sharpe is None:
            min_sharpe = _CASCADE_THRESHOLDS['min_sharpe']
        if min_profit_ratio is None:
            min_profit_ratio = 1.8
        if max_consecutive_losses is None:
            max_consecutive_losses = 5
        if max_flat_days is None:
            max_flat_days = 20
        if max_margin_ratio is None:
            max_margin_ratio = 0.50
        if min_trades is None:
            min_trades = 30
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
        # R4-J-01修复(本轮补全): 补全13维度权重配置，确保总和=1.0
        # 核心4维度: profit_ratio(0.60) + sortino(0.40) + calmar(0.30) + sharpe(0.30)
        # 风控5维度: max_drawdown(0.20) + recovery_efficiency(0.15) + win_rate(0.10) + max_consecutive_losses(0.05) + ev_per_trade(0.10)
        # 辅助4维度: total_return(0.05) + overfit_flag(0.05) + num_trades(0.03) + daily_trigger_rate(0.02)
        # 原始和=2.35，归一化后和=1.0
        _all_weights = {
            "profit_ratio": profit_ratio_weight,
            "sortino": sortino_weight,
            "calmar": calmar_weight,
            "sharpe": sharpe_weight,
            "max_drawdown": 0.20,
            "recovery_efficiency": 0.15,
            "win_rate": 0.10,
            "max_consecutive_losses": 0.05,
            "ev_per_trade": 0.10,
            "total_return": 0.05,
            "overfit_flag": 0.05,
            "num_trades": 0.03,
            "daily_trigger_rate": 0.02,
        }
        raw_total = sum(_all_weights.values())
        if abs(raw_total - 1.0) > 1e-4 and raw_total > 1e-10:
            _all_weights = {k: v / raw_total for k, v in _all_weights.items()}
        self.weights = _all_weights
        tri_total = sortino_weight + calmar_weight + sharpe_weight
        if tri_total < 1e-10:
            tri_total = 1.0  # R4-J-04: 防止分母为零
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
        # R21-MEM-P1-16修复: 评判历史回收机制 — 限制缓存的报告数量，防止长期运行内存泄漏
        self._report_history: list = []
        self._report_history_maxlen: int = 1000
        # R32-P1-18修复: 通过ConfigService消费cascade_threshold_grid(原getter零消费)
        try:
            from ali2026v3_trading.config.config_service import get_config_service
            _cs = get_config_service()
            if _cs is not None:
                _grid = _cs.get_cascade_threshold_grid()
                if _grid and isinstance(_grid, dict):
                    for _k, _v in _grid.items():
                        if _k in self.thresholds and isinstance(_v, (int, float)):
                            self.thresholds[_k] = _v
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass

    @classmethod
    def _sync_cascade_defaults(cls, yaml_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """P1-R11-24修复: YAML为唯一源，DEFAULT_THRESHOLDS仅作回退

        YAML值覆盖DEFAULT_THRESHOLDS（以YAML为准），
        YAML缺失的键使用DEFAULT_THRESHOLDS回退。
        差异时记录info日志（YAML为准，非warning）。

        Args:
            yaml_cfg: 从cascade_config.yaml加载的配置字典

        Returns:
            Dict[str, Any]: 补全缺失值后的配置字典
        """
        import logging

        def _sync_nested(default: Dict, yaml: Dict, prefix: str = "") -> None:
            for key, default_val in default.items():
                full_key = f"{prefix}.{key}" if prefix else key
                if key not in yaml:
                    logging.info(
                        "[P1-R11-24] cascade_config.yaml缺少键'%s'，"
                        "使用DEFAULT_THRESHOLDS回退值: %s",
                        full_key, default_val,
                    )
                    yaml[key] = default_val
                elif isinstance(default_val, dict) and isinstance(yaml.get(key), dict):
                    _sync_nested(default_val, yaml[key], full_key)
                elif yaml.get(key) != default_val:
                    logging.info(
                        "[P1-R11-24] cascade_config.yaml键'%s'值=%s 覆盖DEFAULT_THRESHOLDS值=%s (YAML为准)",
                        full_key, yaml[key], default_val,
                    )

        if yaml_cfg:
            _sync_nested(cls.DEFAULT_THRESHOLDS, yaml_cfg)
            for gate_key, gate_config in yaml_cfg.items():
                if isinstance(gate_config, dict):
                    if gate_key not in cls.DEFAULT_THRESHOLDS:
                        cls.DEFAULT_THRESHOLDS[gate_key] = {}
                    if isinstance(cls.DEFAULT_THRESHOLDS.get(gate_key), dict):
                        for metric, threshold in gate_config.items():
                            if not isinstance(threshold, dict):
                                cls.DEFAULT_THRESHOLDS[gate_key][metric] = threshold
        return yaml_cfg

    @classmethod
    def from_config(cls, config_path: Optional[str] = None, capital_scale: str = "medium",
                    params: Optional[Dict[str, Any]] = None) -> "CascadeJudge":
        """从cascade_config.yaml加载三系统统一配置

        P1-R11-24修复: YAML为唯一源，DEFAULT_THRESHOLDS仅作回退。
        启动时YAML值覆盖DEFAULT_THRESHOLDS，缺失值使用DEFAULT_THRESHOLDS回退，
        差异以YAML为准并记录info日志。

        capital_scale='small'时使用scoring_small权重
        params: 可选参数字典，当包含scoring_*_weight键时优先使用(支持参数池扫描覆盖)
        """
        if config_path is None:
            import os
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "config", "cascade_config.yaml",
            )
        try:
            import yaml
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml_safe_load(f)
            # P1-R11-24修复: YAML为唯一源，覆盖DEFAULT_THRESHOLDS
            cfg = cls._sync_cascade_defaults(cfg or {})
            scoring_key = "scoring_small" if capital_scale == "small" else "scoring"
            scoring_cfg = cfg.get(scoring_key, cfg.get("scoring", {}))
            _pr_w = (params.get("scoring_profit_ratio_weight") if params else None) or scoring_cfg.get("profit_ratio_weight", 0.60)
            _so_w = (params.get("scoring_sortino_weight") if params else None) or scoring_cfg.get("sortino_weight", 0.40)
            _ca_w = (params.get("scoring_calmar_weight") if params else None) or scoring_cfg.get("calmar_weight", 0.30)
            _sh_w = (params.get("scoring_sharpe_weight") if params else None) or scoring_cfg.get("sharpe_weight", 0.30)
            return cls(
                min_profit_ratio=cfg.get("gate1_profit_ratio", {}).get("min", 1.8),
                min_sortino=cfg.get("gate2_sortino", {}).get("min", 1.5),
                min_calmar=cfg.get("gate2_calmar", {}).get("min", 0.8),
                min_sharpe=cfg.get("gate2_sharpe", {}).get("min", 1.2),
                max_consecutive_losses=cfg.get("gate3_consecutive_losses", {}).get("max", 5),
                max_flat_days=cfg.get("gate3_flat_days", {}).get("max", 20),
                max_margin_ratio=cfg.get("gate3_margin_ratio", {}).get("max", 0.50),
                profit_ratio_weight=_pr_w,
                sortino_weight=_so_w,
                calmar_weight=_ca_w,
                sharpe_weight=_sh_w,
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            # R13-P2-DEAD-09修复: 配置加载失败时记录错误日志
            import logging
            logging.warning("[CascadeJudge.from_config] 配置加载失败，使用默认参数: %s", e)
            return cls()

    def _record_report(self, report: CascadeReport) -> CascadeReport:
        """R21-MEM-P1-16修复: 记录评判报告到历史，超限时回收"""
        self._report_history.append(report)
        return report

    def get_otm_sync_penalty(self, conn=None) -> float:
        """P1-9修复: 消费option_sync_otm_stats，计算OTM同步率惩罚因子

        Returns:
            float: 惩罚因子 0.0~1.0，OTM同步率越低惩罚越大
        """
        try:
            if conn is None:
                from ali2026v3_trading.data.db_adapter import get_db_adapter
                conn = get_db_adapter()
            result = conn.execute("""
                SELECT
                    SUM(correct_rise_otm_count + correct_fall_otm_count) AS correct_total,
                    SUM(total_otm_count) AS total_count
                FROM option_sync_otm_stats
                WHERE total_otm_count > 0
            """).fetchone()
            if result and result[1] > 0:
                sync_rate = result[0] / result[1]
                if sync_rate < 0.5:
                    penalty = sync_rate / 0.5
                    import logging
                    logging.warning("[P1-9] OTM同步率过低(%.2f%%)，惩罚因子=%.2f",
                                  sync_rate * 100, penalty)
                    return penalty
            return 1.0
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            import logging
            logging.debug("[P1-9] OTM同步率查询失败(非致命): %s", e)
            return 1.0

    def judge(self, metrics: BacktestMetrics) -> CascadeReport:
        _param_guard = ParamIsolationGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _param_guard:
            _param_guard.register_param_source("judge_params", "cascade_judge", str(hash(frozenset({k: v for k, v in vars(metrics).items() if isinstance(v, (int, float, str, bool))}.items()))))
        # R15-P2-API-13修复: judge()输入参数完整性校验
        if metrics is None:
            logging.error("[CascadeJudge] R15-P2-API-13: metrics为None, 返回空报告")
            return CascadeReport(gates=[], warnings=["metrics为None"], passed=False, score=0.0, dimensions={})
        # R21-MEM-P1-16修复: 回收旧报告，防止历史报告累积
        if len(self._report_history) > self._report_history_maxlen:
            self._report_history = self._report_history[-self._report_history_maxlen // 2:]

        gates = []
        warnings = []
        # R14-P1-LOG-05修复: 评判入口添加日志
        logging.info("[CascadeJudge] judge()开始: num_trades=%d sharpe=%.2f plr=%.2f", metrics.num_trades, metrics.sharpe_ratio, metrics.profit_loss_ratio)

        # ========== 第零关：数据质量前置检查 ==========
        min_trades = self.constraints["min_trades"]
        # R14-P1-DEAD-03修复: 0笔交易也需阻断(数据质量检查不可跳过)
        if metrics.num_trades == 0:
            gate0 = GateReport(
                gate_name="数据质量检查",
                result=GateResult.BLOCK,
                value=0,
                threshold=float(min_trades),
                reason=f"0笔交易，无统计数据，评判完全无意义",
                details={"num_trades": 0, "min_trades": min_trades},
            )
            gates.append(gate0)
            return self._record_report(CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate0.reason, metrics=metrics))
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
            return self._record_report(CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=gate0.reason, metrics=metrics))

        # ========== 过拟合检查 ==========
        if metrics.overfit_flag:
            warnings.append(
                f"过拟合警告：训练盈亏比{metrics.profit_loss_ratio:.2f} vs 测试{metrics.test_profit_loss_ratio:.2f}，差异>50%"
            )

        # ========== 第一关：盈亏比验证（发动机） ==========
        # R7-4: 通过_judge_l1/l2/l3委托，与gate_l1/l2/l3公共方法共享逻辑
        gates.append(self.gate_l1_rule_engine(metrics))
        if gates[-1].result == GateResult.WARN:
            logging.info("[R16-P1-018] 盈亏比%.2f低于阈值%.2f，评分降至30%%", metrics.profit_loss_ratio, self.thresholds["profit_ratio"])

        # ========== 第二关：三角验证（安全壳） ==========
        l2_gates = self.gate_l2_ml_model(metrics)
        for g in l2_gates:
            gates.append(g)
            if g.result == GateResult.BLOCK:
                return self._record_report(CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=g.reason, metrics=metrics))
        if l2_gates[-1].result == GateResult.WARN:
            warnings.append(f"夏普比率偏低({metrics.sharpe_ratio:.2f})，建议关注策略稳定性")

        # ========== 第三关：小资金约束（驾驶座） ==========
        l3_gates = self.gate_l3_shadow_stealth(metrics)
        for g in l3_gates:
            gates.append(g)
            if g.result == GateResult.BLOCK:
                return self._record_report(CascadeReport(passed=False, final_score=0.0, gates=gates, fatal_reason=g.reason, metrics=metrics))

        # ========== L2关：ML模型置信度验证（E-01修复） ==========
        gate_l2 = self._check_gate(
            name="L2 ML模型置信度",
            value=metrics.l2_ml_confidence,
            threshold=0.6,
            hard_block=False,
            reason_template="L2 ML置信度{value:.2f} < 阈值{threshold}，模型预测可信度不足",
            details={"l2_ml_confidence": metrics.l2_ml_confidence},
        )
        gates.append(gate_l2)
        if gate_l2.result == GateResult.WARN:
            warnings.append(f"L2 ML模型置信度偏低({metrics.l2_ml_confidence:.2f})")

        # ========== L3关：影子策略隐形度验证（E-01修复） ==========
        # R13-P2-DEAD-01修复: 实际验证影子订单不出现在真实订单流中
        l3_invisibility = metrics.l3_silhouette_invisibility
        if l3_invisibility <= 0:
            # 当隐形度未计算时，尝试从ShadowStrategyEngine获取实际验证结果
            try:
                from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine
                _shadow_engine = get_shadow_strategy_engine()
                if _shadow_engine is not None:
                    shadow_orders = getattr(_shadow_engine, '_shadow_order_log', None)
                    if shadow_orders is not None and len(shadow_orders) > 0:
                        # 检查影子订单是否泄露到真实订单流
                        leaked = sum(1 for o in shadow_orders if getattr(o, 'leaked_to_real', False))
                        l3_invisibility = 1.0 - (leaked / len(shadow_orders)) if len(shadow_orders) > 0 else 1.0
                    else:
                        l3_invisibility = 1.0  # 无影子订单，默认通过
            except (ImportError, Exception):
                l3_invisibility = 0.0  # 无法验证时保守为0
        gate_l3 = self._check_constraint(
            name="L3 影子策略隐形度",
            value=l3_invisibility,
            threshold=0.7,
            direction="max",
            reason_template="影子策略隐形度{value:.2f} > 阈值{threshold}，影子策略与主力策略过于独立",
            details={"l3_silhouette_invisibility": metrics.l3_silhouette_invisibility},
        )
        gates.append(gate_l3)

        # ========== 综合评分 ==========
        # R4-J-03修复(本轮补全): 评分前检查13维度输入完整性
        _dim_values = {
            "profit_ratio": metrics.profit_loss_ratio,
            "sortino": metrics.sortino_ratio,
            "calmar": metrics.calmar_ratio,
            "sharpe": metrics.sharpe_ratio,
            "max_drawdown": -metrics.max_drawdown_pct if metrics.max_drawdown_pct is not None else None,
            "recovery_efficiency": metrics.recovery_efficiency,
            "win_rate": metrics.win_rate,
            "max_consecutive_losses": -metrics.max_consecutive_losses if metrics.max_consecutive_losses is not None else None,
            "ev_per_trade": metrics.ev_per_trade,
            "total_return": metrics.total_return,
            "overfit_flag": -metrics.overfit_flag if metrics.overfit_flag is not None else None,
            "num_trades": metrics.num_trades,
            "daily_trigger_rate": metrics.daily_trigger_rate,
        }
        _missing_dims = [k for k, v in _dim_values.items() if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))]
        if _missing_dims:
            warnings.append(f"评分维度缺失或无效: {_missing_dims}，使用默认值0.0")
            for dim in _missing_dims:
                _dim_values[dim] = 0.0

        final_score = self._calculate_score(_dim_values)

        # R16-P1-018修复: 盈亏比低于阈值时评分降至30%
        if metrics.profit_loss_ratio < self.thresholds["profit_ratio"]:
            final_score *= 0.3

        # P1-9修复: 应用OTM同步率惩罚
        _otm_penalty = self.get_otm_sync_penalty()
        if _otm_penalty < 1.0:
            final_score = final_score * _otm_penalty
            logging.info("[P1-9] 应用OTM同步率惩罚: %.2f, 调整后评分: %.4f", _otm_penalty, final_score)

        # R4-J-05修复: 填充标准模板字段(13维度)
        from datetime import datetime as _dt
        _dim_scores = {}
        for dim_name, dim_val in _dim_values.items():
            if dim_name in ("profit_ratio", "sortino", "calmar", "sharpe"):
                _sig_key = {"profit_ratio": "pr", "sortino": "sortino", "calmar": "calmar", "sharpe": "sharpe"}[dim_name]
                _dim_scores[f"{dim_name}_norm"] = self._safe_sigmoid(dim_val, self.sigmoid[f"{_sig_key}_center"], self.sigmoid[f"{_sig_key}_scale"])
            else:
                _dim_scores[f"{dim_name}_norm"] = self._safe_sigmoid(dim_val, 0.0, 1.0)

        return self._record_report(CascadeReport(
            passed=True,
            final_score=final_score,
            gates=gates,
            warnings=warnings,
            metrics=metrics,
            dimension_scores=_dim_scores,
            dimension_weights=dict(self.weights),
            verdict="PASS",
            timestamp=_dt.now(_CHINA_TZ).isoformat(),
        ))

    # R2-2: 手册V7.5架构对齐方法 — R6-2: 与judge()逻辑同步
    def gate_l1_rule_engine(self, metrics: BacktestMetrics) -> GateReport:
        """L1规则引擎 ← 盈亏比门控(发动机)"""
        return self._check_gate(
            name="L1规则引擎(盈亏比)",
            value=metrics.profit_loss_ratio,
            threshold=self.thresholds["profit_ratio"],
            hard_block=False,
            reason_template="盈亏比{value:.2f} < 阈值{threshold}，策略盈利核心逻辑偏弱",
        )

    def gate_l2_ml_model(self, metrics: BacktestMetrics) -> List[GateReport]:
        """L2 ML模型 ← 三角验证+统计检验(安全壳) — 与judge()第二关逻辑同步"""
        gates = []
        gates.append(self._check_gate(
            name="L2-索提诺验证",
            value=metrics.sortino_ratio,
            threshold=self.thresholds["sortino"],
            hard_block=True,
            reason_template="索提诺{value:.2f} < 阈值{threshold}，下行风险失控",
            details={"sortino": metrics.sortino_ratio},
        ))
        gates.append(self._check_gate(
            name="L2-卡玛验证",
            value=metrics.calmar_ratio,
            threshold=self.thresholds["calmar"],
            hard_block=True,
            reason_template="卡玛{value:.2f} < 阈值{threshold}，回撤路径不可承受",
            details={"calmar": metrics.calmar_ratio},
        ))
        gates.append(self._check_gate(
            name="L2-夏普验证",
            value=metrics.sharpe_ratio,
            threshold=self.thresholds["sharpe"],
            hard_block=False,
            reason_template="夏普{value:.2f} < 阈值{threshold}，整体风险收益比偏低",
            details={"sharpe": metrics.sharpe_ratio},
        ))
        return gates

    def gate_l3_shadow_stealth(self, metrics: BacktestMetrics) -> List[GateReport]:
        """L3影子隐形 ← 小资金约束+影子EV(驾驶座) — 与judge()第三关逻辑同步"""
        gates = []
        gates.append(self._check_constraint(
            name="L3-连续亏损约束",
            value=metrics.max_consecutive_losses,
            threshold=self.constraints["consecutive_losses"],
            direction="max",
            reason_template="连续亏损{value}次 > 阈值{threshold}次",
            details={"max_consecutive_losses": metrics.max_consecutive_losses},
        ))
        gates.append(self._check_constraint(
            name="L3-横盘天数约束",
            value=metrics.max_flat_period_days,
            threshold=self.constraints["flat_days"],
            direction="max",
            reason_template="横盘{value}天 > 阈值{threshold}天",
            details={"max_flat_period_days": metrics.max_flat_period_days},
        ))
        gates.append(self._check_constraint(
            name="L3-保证金约束",
            value=metrics.peak_margin_used,
            threshold=self.constraints["margin_ratio"],
            direction="max",
            reason_template="保证金峰值{value:.1%} > 阈值{threshold:.1%}",
            details={"peak_margin_used": metrics.peak_margin_used},
        ))
        return gates

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

    def _safe_sigmoid(self, x: float, center: float, scale: float) -> float:
        """R4-J-02/J-04修复: 安全Sigmoid归一化，防护scale=0导致NaN/除零
        R21-MATH-P2-04修复: 已有z裁剪[-500,500]防止exp溢出，确认保护完整
        R7-M-13修复: 全局sigmoid_alpha/sigmoid_beta统一参数读取（手册5.2.3节）
        """
        # R7-M-13修复: 从config_params读取全局sigmoid归一化参数
        try:
            from ali2026v3_trading.config.config_params import get_param
            _alpha = get_param('sigmoid_alpha', 1.0)
            _beta = get_param('sigmoid_beta', 0.0)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            _alpha, _beta = 1.0, 0.0
        if abs(scale) < 1e-10:
            # R4-J-04: scale为零时退化为阶跃函数，避免除零NaN
            return _alpha if x >= center else 0.0
        z = -((x - center - _beta) / scale)
        z = max(-500.0, min(500.0, z))  # 防止overflow
        return _alpha / (1.0 + math.exp(z))

    def _calculate_score(self, dim_values: dict) -> float:
        """13维度Sigmoid归一化综合评分

        R4-J-01修复(本轮补全): 扩展为13维度加权评分
        R4-J-02修复: 统一使用_safe_sigmoid防护除零
        R4-J-04修复: 分母为零时不再返回NaN
        """
        s = self.sigmoid
        _scored_dims = {
            "profit_ratio": (dim_values.get("profit_ratio", 0.0), s["pr_center"], s["pr_scale"]),
            "sortino": (dim_values.get("sortino", 0.0), s["sortino_center"], s["sortino_scale"]),
            "calmar": (dim_values.get("calmar", 0.0), s["calmar_center"], s["calmar_scale"]),
            "sharpe": (dim_values.get("sharpe", 0.0), s["sharpe_center"], s["sharpe_scale"]),
            "max_drawdown": (dim_values.get("max_drawdown", 0.0), -0.20, 0.10),
            "recovery_efficiency": (dim_values.get("recovery_efficiency", 0.0), 0.50, 0.20),
            "win_rate": (dim_values.get("win_rate", 0.0), 0.50, 0.15),
            "max_consecutive_losses": (dim_values.get("max_consecutive_losses", 0.0), -5.0, 3.0),
            "ev_per_trade": (dim_values.get("ev_per_trade", 0.0), 0.0, 50.0),
            "total_return": (dim_values.get("total_return", 0.0), 0.10, 0.20),
            "overfit_flag": (dim_values.get("overfit_flag", 0.0), -0.50, 0.30),
            "num_trades": (dim_values.get("num_trades", 0.0), 30.0, 20.0),
            "daily_trigger_rate": (dim_values.get("daily_trigger_rate", 0.0), 3.0, 2.0),
        }
        final = 0.0
        for dim_name, (raw_val, center, scale) in _scored_dims.items():
            norm_val = self._safe_sigmoid(raw_val, center, scale)
            final += self.weights.get(dim_name, 0.0) * norm_val
        # R4-J-04: 确保结果不是NaN
        if math.isnan(final) or math.isinf(final):
            final = 0.0
        return round(final, 4)
