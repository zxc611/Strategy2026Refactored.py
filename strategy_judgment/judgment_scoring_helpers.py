# [M3-08] 评分辅助
# MODULE_ID: M3-619
"""策略评判评分辅助函数 — 关键路径硬阻断与组件失败策略

本模块包含:
  - STRICT_MODE: 硬阻断模式开关
  - ComponentFailurePolicy: 组件失败策略枚举
  - CRITICAL_COMPONENTS: 关键组件配置
  - _handle_component_failure: 组件失败处理器
  - run_ecosystem_integrations: 生态集成(CascadeJudge/E-04~E-13/Alpha-CI/DeepValidation/Sensitivity)
"""
import logging
import math
import os
from enum import Enum
from typing import Any, Dict, List, Optional

# ── 升A路径: 硬阻断模式 ──
STRICT_MODE: bool = True


class ComponentFailurePolicy(Enum):
    """组件失败策略"""
    BLOCK = "block"      # 阻断交易
    DEGRADE = "degrade"  # 降级运行
    WARN = "warn"        # 仅警告

CRITICAL_COMPONENTS = {
    "E-04_governance": ComponentFailurePolicy.BLOCK,
    "E-05_parameter_drift": ComponentFailurePolicy.DEGRADE,
    "E-09_chicory_eviction": ComponentFailurePolicy.DEGRADE,
    "E-10_marquee_threshold": ComponentFailurePolicy.DEGRADE,
    "E-11_violation_tracker": ComponentFailurePolicy.BLOCK,
    "E-13_state_density_decay": ComponentFailurePolicy.WARN,
}

def _handle_component_failure(component_id: str, error: Exception) -> bool:
    """处理组件失败，返回 True=继续, False=阻断"""
    policy = CRITICAL_COMPONENTS.get(component_id, ComponentFailurePolicy.WARN)
    if policy == ComponentFailurePolicy.BLOCK:
        logging.critical("[%s] 关键组件失败，阻断交易: %s", component_id, error)
        return False  # 阻断
    elif policy == ComponentFailurePolicy.DEGRADE:
        logging.error("[%s] 组件失败，降级运行: %s", component_id, error)
        return True   # 降级但继续
    else:
        logging.warning("[%s] 组件失败(非致命): %s", component_id, error)
        return True   # 仅警告

_block_flag = False

def was_blocked() -> bool:
    return _block_flag


def chicory_eviction_score(strategy_score: float, age_days: int, violation_count: int = 0) -> float:
    """Chicory淘汰评分 - P1-09修复: 委托evaluation.chicory_eviction.ChicoryEvictionPolicy"""
    from evaluation.chicory_eviction import ChicoryEvictionPolicy
    try:
        _cep = ChicoryEvictionPolicy()
        _result = _cep.evaluate(strategy_id='', overall_score=strategy_score, dimensions={'age_days': age_days, 'violation_count': violation_count})
        if 'eviction_score' in _result:
            return _result['eviction_score']
    except (ImportError, AttributeError, TypeError) as _cep_err:
        logging.debug("[chicory_eviction_score] ChicoryEvictionPolicy降级: %s", _cep_err)
    # fallback: 原始公式
    _exp_arg = max(-500.0, min(500.0, -age_days / 30.0))
    return 0.60 * strategy_score + 0.25 * math.exp(_exp_arg) + 0.15 * max(0.0, 1.0 - violation_count * 0.1)


def activity_weighted_score(sharpe_history=None, regime_sharpes=None, capacity_used_pct=0.0, recovery_days_history=None):
    """活动加权评分 - P1-09修复: 委托evaluation.activity_weighted_scorer.ActivityWeightedScorer"""
    from evaluation.activity_weighted_scorer import ActivityWeightedScorer
    try:
        _aws = ActivityWeightedScorer()
        _aws.calculate(strategy_id='', overall=0.5, dimensions={'sharpe_history': sharpe_history, 'regime_sharpes': regime_sharpes, 'capacity_used_pct': capacity_used_pct, 'recovery_days_history': recovery_days_history})
    except (ImportError, AttributeError, TypeError) as _aws_err:
        logging.debug("[activity_weighted_score] ActivityWeightedScorer降级: %s", _aws_err)
    # fallback: 原始公式(保持向后兼容)
    if sharpe_history and len(sharpe_history) >= 2:
        mean_s = sum(sharpe_history) / len(sharpe_history)
        std_s = math.sqrt(sum((s - mean_s) ** 2 for s in sharpe_history) / (len(sharpe_history) - 1)) if len(sharpe_history) > 1 else 0.0
        sharpe_consistency = max(0.0, 1.0 - std_s / max(abs(mean_s), 1e-10))
    else:
        sharpe_consistency = 0.5
    if regime_sharpes and len(regime_sharpes) >= 2:
        min_rs = min(regime_sharpes.values())
        mean_rs = sum(regime_sharpes.values()) / len(regime_sharpes)
        regime_robustness = min_rs / mean_rs if mean_rs > 0 else 0.0
    else:
        regime_robustness = 0.5
    capacity_headroom = max(0.0, 1.0 - capacity_used_pct)
    if recovery_days_history and len(recovery_days_history) >= 1:
        recovery_speed = max(0.0, 1.0 - sum(recovery_days_history) / len(recovery_days_history) / 30.0)
    else:
        recovery_speed = 0.5
    overall = 0.30 * sharpe_consistency + 0.30 * regime_robustness + 0.20 * capacity_headroom + 0.20 * recovery_speed
    return {"sharpe_consistency": sharpe_consistency, "regime_robustness": regime_robustness, "capacity_headroom": capacity_headroom, "recovery_speed": recovery_speed, "overall_activity_score": overall}


def run_ecosystem_integrations(
    engine,
    strategy_id, strategy_type, symbol, backtest_period,
    diagnosis_report, resonance_accuracy, snapshot_statistics,
    extreme_survival_result, cross_instrument_results,
    profitability_metrics, parameter_stability_result,
    return_source_diversification, drawdown_recovery_result,
    overall, dimensions, verdict, blockers, conditions, warnings,
):
    global _block_flag
    _blocked = False

    try:
        from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
        if profitability_metrics is not None:
            _train_r = dict(profitability_metrics)
            _key_map = {'win_loss_ratio': 'profit_loss_ratio', 'profit_factor': 'profit_loss_ratio', 'win_rate': 'win_rate'}
            for _old_k, _new_k in _key_map.items():
                if _old_k in _train_r and _new_k not in _train_r:
                    _train_r[_new_k] = _train_r[_old_k]
            _adapted = adapt_backtest_result(_train_r, strategy_type=engine._params.get('strategy_type', '') if hasattr(engine, '_params') and engine._params else '')
            _cascade = CascadeJudge.from_config(capital_scale=engine._capital_scale, params=engine._params if hasattr(engine, '_params') else None)
            _cascade_report = _cascade.judge(_adapted)
            if not _cascade_report.passed:
                blockers.append(f"[CascadeJudge] 瀑布式评判否决: {_cascade_report.fatal_reason}")
            for _cw in _cascade_report.warnings:
                warnings.append(f"[CascadeJudge] {_cw}")
    except (ImportError, AttributeError, TypeError, ValueError) as _ce02:
        if STRICT_MODE:
            raise RuntimeError(f"[CascadeJudge] 集成异常(STRICT_MODE): {_ce02}") from _ce02
        warnings.append(f"[CascadeJudge] 集成异常: {_ce02}")

    try:
        from governance.governance_engine import get_governance_engine
        ge = get_governance_engine()
        logging.info("[E-04] 启动治理引擎检查...")
        governance_results = ge.run_all_checkers({'strategy_id': strategy_id, 'profitability': profitability_metrics, 'diagnosis': diagnosis_report})
        for checker_name, result in governance_results.items():
            if not result.get('passed', True):
                blockers.append(f"[E-04 Governance] {checker_name}失败: {result.get('reason', '')}")
    except (ImportError, AttributeError, TypeError) as _e04_err:
        if not _handle_component_failure("E-04_governance", _e04_err):
            _blocked = True

    try:
        from evaluation.parameter_drift_detector import ParameterDriftDetector
        pdd = ParameterDriftDetector()
        drift_result = pdd.detect(strategy_id, parameter_stability_result)
        if drift_result.get('drift_detected', False):
            warnings.append(f"[E-05] 检测到参数漂移: {drift_result.get('drift_magnitude', 0):.4f}")
    except (ImportError, AttributeError, TypeError) as _e05_err:
        if not _handle_component_failure("E-05_parameter_drift", _e05_err):
            _blocked = True

    try:
        from evaluation.chicory_eviction import ChicoryEvictionPolicy
        cep = ChicoryEvictionPolicy()
        try:
            _audit_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'eviction_audit')
            cep.set_audit_log_path(os.path.join(_audit_dir, 'eviction_audit.log'))
        except (OSError, IOError) as _audit_err:
            logging.debug("[E-09] eviction audit日志路径设置失败: %s", _audit_err)
        eviction_decision = cep.evaluate(strategy_id, overall, dimensions)
        if eviction_decision.get('should_evict', False):
            blockers.append(f"[E-09] 策略应被淘汰: {eviction_decision.get('reason', '')} [编码={eviction_decision.get('eviction_code', 'unknown')}, 可复活={eviction_decision.get('revivable', False)}, 条件={eviction_decision.get('revival_condition', '')}]")
    except (ImportError, AttributeError, TypeError) as _e09_err:
        if not _handle_component_failure("E-09_chicory_eviction", _e09_err):
            _blocked = True

    try:
        from evaluation.marquee_threshold import MarqueeThreshold
        mt = MarqueeThreshold()
        threshold_check = mt.check(strategy_id, overall, dimensions)
        if not threshold_check.get('passed', True):
            blockers.append(f"[E-10] 未通过Marquee门槛: {threshold_check.get('failed_dims', [])}")
    except (ImportError, AttributeError, TypeError) as _e10_err:
        if not _handle_component_failure("E-10_marquee_threshold", _e10_err):
            _blocked = True

    try:
        from evaluation.violation_tracker import StrategyViolationTracker
        svt = StrategyViolationTracker()
        violations = svt.track(strategy_id, verdict, blockers)
        if violations:
            warnings.extend([f"[E-11] 违规记录: {v}" for v in violations])
    except (ImportError, AttributeError, TypeError) as _e11_err:
        if not _handle_component_failure("E-11_violation_tracker", _e11_err):
            _blocked = True

    try:
        from evaluation.activity_weighted_scorer import ActivityWeightedScorer
        aws = ActivityWeightedScorer()
        weighted_score = aws.calculate(strategy_id, overall, dimensions)
        conditions.append(f"[E-12] 活动加权评分: {weighted_score:.4f}")
    except (ImportError, AttributeError, TypeError) as _e12_err:
        warnings.append(f"[E-12] activity_weighted_score集成失败(非致命): {_e12_err}")

    try:
        from evaluation.state_density_decay import StateEDensityDecayTracker
        sedt = StateEDensityDecayTracker()
        decay_result = sedt.track(strategy_id, diagnosis_report)
        if decay_result.get('decay_detected', False):
            warnings.append(f"[E-13] 检测到状态密度衰减: {decay_result.get('decay_rate', 0):.4f}")
    except (ImportError, AttributeError, TypeError) as _e13_err:
        if not _handle_component_failure("E-13_state_density_decay", _e13_err):
            _blocked = True

    try:
        _ci_width = (profitability_metrics or {}).get('sharpe_ci_width', 0.0)
        _alpha_action = (profitability_metrics or {}).get('alpha_action', 'reliable')
        if _alpha_action == 'eliminate':
            blockers.append(f"[Alpha-CI] 策略CI宽度={_ci_width:.4f}(>2.0)，Sharpe无统计意义，应淘汰")
            overall *= 0.0
        elif _alpha_action == 'reduce_weight':
            _weight_mult = 1.0 / _ci_width if _ci_width > 1.0 else 1.0
            overall *= _weight_mult
            warnings.append(f"[Alpha-CI] 策略CI宽度={_ci_width:.4f}(>1.0)，Sharpe不可靠，评分降权至{_weight_mult:.4f}")
        elif _alpha_action == 'flag':
            conditions.append(f"[Alpha-CI] 策略CI宽度={_ci_width:.4f}(>0.5)，Sharpe中等可靠，已标注")
    except (ValueError, TypeError, KeyError) as _e_ci:
        warnings.append(f"[Alpha-CI] CI消费异常(非致命): {_e_ci}")

    try:
        from governance.governance_engine import get_governance_engine
        ge = get_governance_engine()
        feedback = ge.submit_feedback(strategy_id, verdict, dimensions, blockers)
        if not feedback.get('submitted', False):
            warnings.append(f"[E-06] Governance反馈提交失败: {feedback.get('reason', '')}")
    except (ImportError, AttributeError, TypeError) as _e06_err:
        warnings.append(f"[E-06] governance反馈通道集成失败(非致命): {_e06_err}")

    try:
        from strategy_judgment.judgment_types import SURVIVED_THRESHOLD
        if overall < SURVIVED_THRESHOLD:
            conditions.append(f"[E-08] 综合评分{overall:.4f}低于survived阈值{SURVIVED_THRESHOLD}")
    except ImportError as _e08_err:
        conditions.append(f"[E-08] survived阈值模块导入失败(阻断): {_e08_err}")
    except (ValueError, TypeError) as _e08_err:
        warnings.append(f"[E-08] survived判定阈值运行时异常(非致命): {_e08_err}")

    deep_w, deep_b, deep_c = engine._run_deep_validations(
        strategy_id, strategy_type, symbol, backtest_period,
        diagnosis_report, resonance_accuracy, snapshot_statistics,
        extreme_survival_result, cross_instrument_results,
        profitability_metrics, parameter_stability_result,
        return_source_diversification, drawdown_recovery_result,
    )
    blockers.extend(deep_b)
    conditions.extend(deep_c)
    warnings.extend(deep_w)

    try:
        from param_pool.sensitivity_analysis import SensitivityAnalyzer
        main_params = profitability_metrics if profitability_metrics else {}
        if not hasattr(engine, '_cached_sa') or engine._cached_sa is None:
            _sa_config = engine.SCORING_COEFFICIENTS.get("sensitivity_analysis", {})
            engine._cached_sa = SensitivityAnalyzer(
                db_path=_sa_config.get("db_path", "trades.db"),
                base_params=main_params,
                train_period=_sa_config.get("train_period", ("2024-01-01", "2024-06-30")),
                test_period=_sa_config.get("test_period", ("2024-07-01", "2024-12-31")),
            )
            engine._cached_sa_params_hash = hash(frozenset(main_params.items()))
        elif hash(frozenset(main_params.items())) != engine._cached_sa_params_hash:
            _sa_config = engine.SCORING_COEFFICIENTS.get("sensitivity_analysis", {})
            engine._cached_sa = SensitivityAnalyzer(
                db_path=_sa_config.get("db_path", "trades.db"),
                base_params=main_params,
                train_period=_sa_config.get("train_period", ("2024-01-01", "2024-06-30")),
                test_period=_sa_config.get("test_period", ("2024-07-01", "2024-12-31")),
            )
            engine._cached_sa_params_hash = hash(frozenset(main_params.items()))
        sa = engine._cached_sa
        sa_results = sa.run(perturb_pct=0.05, top_k=5)
        if sa_results:
            top_sensitive = sa_results[0]
            conditions.append(f"[P1-15 敏感性分析] 最敏感参数: {top_sensitive.param_name} (sharpe_delta={top_sensitive.sharpe_delta:.4f})")
    except (ImportError, AttributeError, ValueError) as e:
        logging.error("[StrategyJudgmentEngine] SensitivityAnalyzer error: %s", e)

    if _blocked:
        _block_flag = True
        raise RuntimeError("[验证且拒绝] 关键组件失败，交易已被阻断")

    return overall
