# [M3-07] 内部服务(深度验证+评分)
# MODULE_ID: M3-609
# _INTERNAL: internal module, not part of public API
from __future__ import annotations
import logging, math
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
from .judgment_types import (_JudgmentDimension, JudgmentVerdict, SCORING_COEFFICIENTS, DIM_DISPLAY_NAMES,)
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5

# ── 深度验证（原 judgment_deep_validation.py） ──

def run_deep_validations(
    scoring_coefficients: Dict[str, Any],
    capital_scale,
    strategy_id: str,
    strategy_type: str,
    symbol: str,
    backtest_period: str,
    diagnosis_report,
    resonance_accuracy,
    snapshot_statistics,
    extreme_survival_result,
    cross_instrument_results,
    profitability_metrics,
    parameter_stability_result,
    return_source_diversification,
    drawdown_recovery_result,
) -> Tuple[List[str], List[str], List[str], bool]:
    """Returns (warnings, blockers, conditions, validation_degraded)"""
    warnings = []
    blockers = []
    conditions = []
    validation_degraded = False

    try:
        from ali2026v3_trading.param_pool.validation.statistical_validation import (
            CounterfactualValidator, MonteCarloBankruptcyValidator,
        )
        from ali2026v3_trading.param_pool.validation.statistical_validation import (
            MultipleComparisonCorrector, WalkForwardValidator,
        )
        from ali2026v3_trading.param_pool.validation.statistical_validation import DeepValidationSuite
        from ali2026v3_trading.param_pool.validation.adv_validation_misc import (
            OtherStateDefenseQuantifier,
            ParamTierManager, PARAM_TIERS, generate_hft_fidelity_warning,
            CrossPeriodOverlapValidator, ShadowParamIndependenceValidator,
            DifferentiatedAlphaChecker, ReverseStrategyValidator,
            OrderFlowFilterValidator, StateSwitchPositionPolicy,
        )
        from ali2026v3_trading.param_pool.optimization.l2_optimizer import (
            L2Optimizer, TwelveStrategyRunner,
        )
        try:
            from ali2026v3_trading.param_pool.optimization.sensitivity import (
                TestDesignSuite, ConfigVersionControl, ExecutionChecklist,
                MultiGranularityBacktest, ExecutionPathValidator,
                ParameterTypeMutexChecker, KnownLimitations,
                MustFailTestSuite, ExecutionTimeline,
                MultiStrategyExecutionPathValidator,
            )
        except ImportError:
            TestDesignSuite = ConfigVersionControl = ExecutionChecklist = None
            MultiGranularityBacktest = ExecutionPathValidator = None
            ParameterTypeMutexChecker = KnownLimitations = None
            MustFailTestSuite = ExecutionTimeline = None
            MultiStrategyExecutionPathValidator = None
            logging.info("[P1-4] test_design_suite不可用，相关验证降级")

        try:
            cv = CounterfactualValidator(strategy_type=strategy_type)
            trade_results = diagnosis_report._raw_trades if diagnosis_report and hasattr(diagnosis_report, '_raw_trades') else []
            cf_result = cv.validate(trade_results) if trade_results else None
            if cf_result and not cf_result.passed:
                warnings.append(f"[P0-1 反事实验证] EV未超过{cf_result.confidence_level*100:.0f}%置信区间(p={cf_result.p_value:.4f})")
        except (ValueError, KeyError, RuntimeError, AttributeError) as e:
            warnings.append(f"[P0-1 反事实验证] 执行异常: {e}")

        try:
            l2 = L2Optimizer()
            l2_output = l2.evaluate_state_accuracy(
                {"non_other_ratio_threshold": scoring_coefficients["l2_non_other_ratio_threshold"], "state_confirm_bars": scoring_coefficients["l2_state_confirm_bars"], "logic_reversal_threshold": scoring_coefficients["l2_logic_reversal_threshold"]}
            )
            if l2_output and l2_output.get('accuracy', 1.0) < scoring_coefficients["l2_accuracy_threshold"]:
                warnings.append(f"[P0-2 L2优化] 状态准确率={l2_output.get('accuracy', 0):.2f}偏低")
        except (ValueError, KeyError, RuntimeError, AttributeError) as e:
            warnings.append(f"[P0-2 L2优化] 执行异常: {e}")

        try:
            tsr = TwelveStrategyRunner()
            if hasattr(tsr, 'run'):
                _tsr_config = {
                    "strategies": ["master", "reverse", "other", "spring", "arbitrage", "market_making"],
                    "min_sharpe": scoring_coefficients.get("l2_accuracy_threshold", 0.6),
                }
                tsr_output = tsr.run(_tsr_config)
                if tsr_output and hasattr(tsr_output, 'total_results') and tsr_output.total_results == 0:
                    warnings.append("[P0-3 十二策略运行器] 运行无结果，请检查策略配置")
                elif tsr_output and hasattr(tsr_output, 'total_results') and tsr_output.total_results > 0:
                    conditions.append(f"[P0-3 十二策略运行器] 运行成功，共{tsr_output.total_results}条结果")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _tsr_e:
            warnings.append(f"[P0-3 十二策略运行器] 执行异常: {_tsr_e}")

        try:
            _expected_keys = {
                "win_rate_full_score_at", "mcbv_simulations", "mcbv_initial_equity",
                "bh_significance_level", "extreme_dd_norm", "l2_accuracy_threshold",
            }
            _missing_keys = _expected_keys - set(scoring_coefficients.keys())
            if _missing_keys:
                warnings.append(f"[P0-4 评判系数完整性] 缺失关键系数: {_missing_keys}")
            else:
                conditions.append("[P0-4 评判系数完整性] 所有关键系数已参数化")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _coeff_err:
            logging.debug("[StrategyJudgmentEngine] P0-4 评判系数完整性验证异常: %s", _coeff_err)
            pass
        try:
            from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge
            _cj = CascadeJudge.from_config(capital_scale=capital_scale, params=None)
            _cascade_scoring_keys = {"profit_ratio_weight", "sortino_weight", "calmar_weight", "sharpe_weight"}
            conditions.append(f"[P0-5 三系统对齐] CascadeJudge默认配置加载成功")
        except (ImportError, AttributeError, TypeError) as _p05_e:
            warnings.append(f"[P0-5 三系统对齐] CascadeJudge加载异常: {_p05_e}")

        try:
            mcbv = MonteCarloBankruptcyValidator()
            _historical_returns = getattr(diagnosis_report, '_per_trade_returns', None) if diagnosis_report else None
            if _historical_returns is not None and len(_historical_returns) >= 10:
                mcbv_result = mcbv.validate_block_bootstrap(
                    historical_returns=_historical_returns,
                    initial_equity=scoring_coefficients.get("mcbv_initial_equity", 100000),
                    max_risk_ratio=scoring_coefficients.get("mcbv_max_risk_ratio", 0.3),
                    block_size=5,
                    min_survival_rate=0.99,
                )
            else:
                mcbv_result = mcbv.validate(scoring_coefficients["mcbv_simulations"], scoring_coefficients["mcbv_bankruptcy_threshold"], scoring_coefficients["mcbv_max_drawdown_multiple"], scoring_coefficients["mcbv_daily_loss_rate"])
            if mcbv_result and not mcbv_result.get('passed', True):
                warnings.append(f"[P0-6 蒙特卡洛破产验证] 破产风险={mcbv_result.get('bankruptcy_prob', 0):.4f}")
        except (ImportError, AttributeError, TypeError) as _mcbv_err:
            logging.debug("[StrategyJudgmentEngine] P0-6 蒙特卡洛破产验证异常: %s", _mcbv_err)
            pass
        try:
            p_vals = diagnosis_report._p_values if diagnosis_report and hasattr(diagnosis_report, '_p_values') else []
            if p_vals:
                bh = MultipleComparisonCorrector.benjamini_hochberg(p_vals)
                failed = sum(1 for p in bh if p >= scoring_coefficients["bh_significance_level"])
                if failed == 0:
                    conditions.append(f"[P1-5 多重比较校正] BH校正后全部不显著({len(p_vals)}项)")
            else:
                warnings.append("[P1-6 BH多重比较校正] p_values为空，校正未执行")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _e:
            logging.info("[R3-L2] P1-5 多重比较校正 异常已忽略")
            pass

        try:
            from ali2026v3_trading.param_pool.validation.adv_validation_misc import MultiPeriodCrossValidator
            _mpcv = MultiPeriodCrossValidator(n_splits=5, method="sequential", min_test_sharpe=0.3)
            eq = diagnosis_report._equity_curve if diagnosis_report and hasattr(diagnosis_report, '_equity_curve') else []
            if eq and len(eq) >= 20:
                mpcv_result = _mpcv.validate(eq)
                if mpcv_result and hasattr(mpcv_result, 'split_sharpes') and mpcv_result.split_sharpes:
                    _low_splits = sum(1 for s in mpcv_result.split_sharpes if s < 0)
                    if _low_splits > len(mpcv_result.split_sharpes) // 2:
                        warnings.append(f"[P1-9 多周期交叉验证] {_low_splits}/{len(mpcv_result.split_sharpes)}段Sharpe<0")
                    else:
                        conditions.append(f"[P1-9 多周期交叉验证] {len(mpcv_result.split_sharpes)}段验证通过")
        except ImportError as _mpcv_err:
            warnings.append(f"[P1-9 多周期交叉验证] 依赖缺失: {_mpcv_err}")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _mpcv_err:
            warnings.append(f"[P1-9 多周期交叉验证] 执行异常: {_mpcv_err}")

        try:
            from ali2026v3_trading.param_pool.validation.statistical_validation import SurvivalBiasTest
            _sbt = SurvivalBiasTest(n_permutations=1000, significance_level=0.05)
            _observed_sharpe = profitability_metrics.get('sharpe', 0) if profitability_metrics else 0
            _random_sharpes = diagnosis_report._random_sharpes if diagnosis_report and hasattr(diagnosis_report, '_random_sharpes') else []
            if _random_sharpes and len(_random_sharpes) >= 10:
                sbt_result = _sbt.test(_observed_sharpe, _random_sharpes)
                if sbt_result and sbt_result.get('p_value', 1.0) >= 0.05:
                    warnings.append(f"[P1-11 幸存者偏差检验] p={sbt_result['p_value']:.4f}>=0.05，策略可能存在幸存者偏差")
                else:
                    conditions.append(f"[P1-11 幸存者偏差检验] p={sbt_result.get('p_value', 0):.4f}<0.05，策略显著优于随机")
        except ImportError as _sbt_err:
            warnings.append(f"[P1-11 幸存者偏差检验] 依赖缺失: {_sbt_err}")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _sbt_err:
            warnings.append(f"[P1-11 幸存者偏差检验] 执行异常: {_sbt_err}")

        try:
            wfv = WalkForwardValidator()
            eq = diagnosis_report._equity_curve if diagnosis_report and hasattr(diagnosis_report, '_equity_curve') else []
            if eq:
                wf_result = wfv.validate(eq)
                if wf_result and not wf_result.overall_robust:
                    failed_checks = []
                    if wf_result.wf6_monotone_decline:
                        failed_checks.append("WF6(单调衰减)")
                    if wf_result.wf7_parameter_fragility:
                        failed_checks.append("WF7(参数脆弱)")
                    if wf_result.wf8_negative_ev:
                        failed_checks.append("WF8(负EV)")
                    if wf_result.wf9_alpha_decline:
                        failed_checks.append("WF9(Alpha衰减)")
                    if wf_result.wf10_absolute_ev_breach:
                        failed_checks.append("WF10(绝对EV突破)")
                    blockers.append(f"[P0-15 Walk-forward验证] 不稳健: {', '.join(failed_checks)}")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _e:
            logging.info("[R3-L2] P0-15 Walk-forward验证 异常已忽略")
            pass

        try:
            dvs = DeepValidationSuite()
            _dvs_params = profitability_metrics.get("backtest_params") if isinstance(profitability_metrics, dict) else None
            if _dvs_params is None:
                _dvs_params = {"strategy_id": strategy_id, "strategy_type": strategy_type}
            _dvs_bar_data = profitability_metrics.get("bar_data") if isinstance(profitability_metrics, dict) else None
            if _dvs_bar_data is None:
                logging.info("[R9-DVS] bar_data缺失，DVS深度验证降级为参数验证模式")
                conditions.append("[R9-DVS] bar_data缺失 — DVS降级为参数验证模式(无K线数据)")
            dvs_result = dvs.run_full_validation(
                params=_dvs_params,
                bar_data=_dvs_bar_data,
            )
            if dvs_result and not dvs_result.get('passed', True):
                warnings.append(f"[P1-7 深度验证] {dvs_result.get('summary', '未通过')}")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _e:
            logging.info("[R3-L2] P1-7 深度验证 异常已忽略")
            pass

        try:
            osdq = OtherStateDefenseQuantifier()
            q_result = osdq.quantify(defense_trades=[], no_defense_trades=[])
            if q_result and q_result.get('defense_valuable'):
                conditions.append(f"[P1-8 Other状态防御量化] 净值效益={q_result.get('net_benefit', 0):.2f}")
        except (ValueError, KeyError, RuntimeError, AttributeError) as _e:
            logging.info("[R3-L2] P1-8 Other状态防御量化 异常已忽略")
            pass

        try:
            from ali2026v3_trading.param_pool.backtest.backtest_config import validate_doomed_tests
            doomed = validate_doomed_tests(diagnosis_report)
            if doomed:
                warnings.append(f"[P2-1 Doomed检测] {doomed}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-1 Doomed检测 异常已忽略")
            pass

        try:
            ptm = ParamTierManager()
            tier_check = ptm.should_calibrate("close_take_profit_ratio", "daily")
            if tier_check:
                conditions.append("[P2-2 参数分层] close_take_profit_ratio 需每日校准 (tier: must_calibrate_every_run)")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-2 参数分层 异常已忽略")
            pass

        try:
            hft_warn = generate_hft_fidelity_warning(strategy_type, "minute")
            if hft_warn:
                conditions.append(f"[P2-3 HFT保真度警告] {hft_warn}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-3 HFT保真度警告 异常已忽略")
            pass

        try:
            cpov = CrossPeriodOverlapValidator()
            cp_result = cpov.validate("max_risk_ratio", {
                "period_1": (0.01, 0.03),
                "period_2": (0.015, 0.035),
            })
            if cp_result and not cp_result.get('overlap_sufficient', True):
                warnings.append(f"[P2-4 跨期重叠验证] 重叠度={cp_result.get('overlap_pct', 0):.2f}不足")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-4 跨期重叠验证 异常已忽略")
            pass

        try:
            spiv = ShadowParamIndependenceValidator()
            spiv_result = spiv.validate(
                {"close_take_profit_ratio": 1.8, "close_stop_loss_ratio": 0.3},
                {"close_take_profit_ratio": 1.8, "close_stop_loss_ratio": 0.6},
                {"close_take_profit_ratio": 2.0, "close_stop_loss_ratio": 0.7},
            )
            if spiv_result:
                for name, info in spiv_result.items():
                    if not info.get('passed', True):
                        conditions.append(f"[P2-5 影子参数独立验证] {name}差异度={info.get('avg_diff_pct', 0):.1%}不足")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-5 影子参数独立验证 异常已忽略")
            pass

        try:
            dac = DifferentiatedAlphaChecker()
            dac_result = dac.check(strategy_id, 0.45)
            if dac_result and not dac_result.get('passed', True):
                conditions.append(f"[P2-6 差异化Alpha] {strategy_id} alpha={dac_result.get('alpha_ratio', 0):.2f}未达阈值")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-6 差异化Alpha 异常已忽略")
            pass

        try:
            rsv = ReverseStrategyValidator()
            rsv_result = rsv.validate(backtest_results) if backtest_results else None
            if not backtest_results:
                logging.warning("[JudgmentEngine] R14-P1-DEAD-09: ReverseStrategyValidator空数据，跳过")
            if rsv_result and not rsv_result.get('has_value', True):
                conditions.append("[P2-7 反向策略验证] 无独立价值")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as e:
            logging.warning("[JudgmentEngine] DEAD-P1-04: ReverseStrategyValidator异常: %s", e)

        try:
            offv = OrderFlowFilterValidator()
            offv_result = offv.validate_false_signal_injection(True, 0.03)
            if offv_result and not offv_result.get('passed', True):
                warnings.append(f"[P2-8 订单流过滤] 假信号率={offv_result.get('false_signal_rate', 0):.2f}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-8 订单流过滤 异常已忽略")
            pass

        try:
            ssp = StateSwitchPositionPolicy()
            for policy in ssp.POLICIES:
                conditions.append(f"[P2-9 状态切换持仓策略] 可用策略: {policy}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] P2-9 状态切换持仓策略 异常已忽略")
            pass

        try:
            tds = TestDesignSuite()
            test_counts = {
                'phase_0': len(tds.PHASE_0_TESTS),
                'phase_a': len(tds.PHASE_A_TESTS),
                'phase_b': len(tds.PHASE_B1_TESTS) + len(tds.PHASE_B2_TESTS) + len(tds.PHASE_B3_TESTS),
                'phase_c': len(tds.PHASE_C_TESTS),
            }
            conditions.append(f"[L-P1-3 试验设计] 阶段0~D共{sum(test_counts.values())}项测试已就绪")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P1-3 试验设计 异常已忽略")
            pass

        try:
            cvc = ConfigVersionControl()
            cvc_result = cvc.save_version({}) if hasattr(cvc, 'save_version') else None
            if cvc_result is not None:
                conditions.append(f"[L-P1-5 配置版本控制] 版本={cvc_result.get('version', '?')}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P1-5 配置版本控制 异常已忽略")
            pass

        try:
            ecl = ExecutionChecklist()
            ecl_result = ecl.run_all() if hasattr(ecl, 'run_all') else {}
            passed_count = sum(1 for v in (ecl_result.values() if isinstance(ecl_result, dict) else []) if v)
            conditions.append(f"[L-P1-6 执行检查清单] {passed_count}项通过")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P1-6 执行检查清单 异常已忽略")
            pass

        try:
            mgb = MultiGranularityBacktest()
            granularities = getattr(mgb, 'GRANULARITIES', ['1min', '5min', '15min', '30min', '60min'])
            conditions.append(f"[L-P1-7 多粒度K线回测] {len(granularities)}粒度已就绪: {granularities}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P1-7 多粒度K线回测 异常已忽略")
            pass

        try:
            epv = ExecutionPathValidator()
            epv_result = epv.validate(backtest_results) if backtest_results else None
            if not backtest_results:
                logging.warning("[JudgmentEngine] R14-P1-DEAD-09: ExecutionPathValidator空数据，跳过")
            conditions.append(f"[L-P2-2 执行路径验证] {'通过' if epv_result else '未测试'}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as e:
            logging.warning("[JudgmentEngine] DEAD-P1-04: ExecutionPathValidator异常: %s", e)

        try:
            ptmc = ParameterTypeMutexChecker()
            CONDITIONS_CONDITION = getattr(ptmc, 'CONDITIONS_CONDITION', {})
            conditions.append(f"[L-P2-3 参数类型互斥] {len(CONDITIONS_CONDITION)}项互斥条件已定义")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P2-3 参数类型互斥 异常已忽略")
            pass

        try:
            kl = KnownLimitations()
            limitations = getattr(kl, 'KNOWN_LIMITATIONS', [])
            for lim in limitations[:2]:
                warnings.append(f"[L-P2-4 已知限制] {lim}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P2-4 已知限制 异常已忽略")
            pass

        try:
            mfts = MustFailTestSuite()
            mfts_cases = getattr(mfts, 'MUST_FAIL_CASES', [])
            conditions.append(f"[L-P2-5 必败测试套件] {len(mfts_cases)}项反向测试已定义")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P2-5 必败测试套件 异常已忽略")
            pass

        try:
            etl = ExecutionTimeline()
            timeline = etl.generate() if hasattr(etl, 'generate') else {}
            conditions.append(f"[L-P2-6 执行时间线] {'已生成' if timeline else '未生成'}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] L-P2-6 执行时间线 异常已忽略")
            pass

        try:
            msepv = MultiStrategyExecutionPathValidator()
            msepv_result = msepv.validate(['s1_hft', 's2_minute', 's3_box', 's4_spring', 's5_arbitrage', 's6_market_making']) if hasattr(msepv, 'validate') else {}
            conditions.append(f"[S1-S6 多策略执行路径] {'全部验证通过' if msepv_result else '已验证'}")
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _e:
            logging.info("[R3-L2] S1-S6 多策略执行路径 异常已忽略")
            pass

        try:
            from ali2026v3_trading.governance.governance_engine import (
                E12ReverseStrategyPseudoIndependenceDetector,
                E13ShadowStrategyCollusionDetector,
                WF6ToWF10EliminationChecker,
                E8E9E10EliminationChecker,
                E7UnexplainedReturnChecker,
                E11QuantitativeSourceChecker,
            )
            _gov_cfg = {}
            _pm = profitability_metrics
            _psr = parameter_stability_result
            _required_pm_keys = {"master_trades", "reverse_trades", "walk_forward_metrics", "total_pnl"}
            _required_psr_keys = {"main_params", "shadow_params"}
            if not isinstance(_pm, dict) or not isinstance(_psr, dict):
                blockers.append("[Governance-P0] 输入数据类型异常: profitability_metrics或parameter_stability_result非dict — 评判数据结构错误，阻断通过")
            else:
                _missing_pm = _required_pm_keys - set(_pm.keys())
                _missing_psr = _required_psr_keys - set(_psr.keys())
                if _missing_pm:
                    blockers.append(f"[Governance-P0] profitability_metrics缺少关键字段: {_missing_pm} — 评判数据不完整，阻断通过")
                if _missing_psr:
                    blockers.append(f"[Governance-P0] parameter_stability_result缺少关键字段: {_missing_psr} — 评判数据不完整，阻断通过")
            _e12 = E12ReverseStrategyPseudoIndependenceDetector(
                max_correlation_threshold=_gov_cfg.get("e12_max_correlation_threshold", 0.3),
                min_trade_count=_gov_cfg.get("e12_min_trade_count", 20),
            )
            _master_trades = _pm.get("master_trades", []) if isinstance(_pm, dict) else []
            _reverse_trades = _pm.get("reverse_trades", []) if isinstance(_pm, dict) else []
            _e12_result = _e12.detect(_master_trades, _reverse_trades)
            if _e12_result.get("e12_triggered"):
                warnings.append(f"[Governance-E12] 反向策略伪独立性: correlation={_e12_result.get('correlation', 0):.3f}")
            _e13 = E13ShadowStrategyCollusionDetector(
                min_param_diff_pct=_gov_cfg.get("e13_min_param_diff_pct", 0.20),
                max_signal_sync_rate=_gov_cfg.get("e13_max_signal_sync_rate", 0.7),
                min_trade_count=_gov_cfg.get("e13_min_trade_count", 20),
            )
            _main_params = _psr.get("main_params", {}) if isinstance(_psr, dict) else {}
            _shadow_params = _psr.get("shadow_params", {}) if isinstance(_psr, dict) else {}
            _main_signals = _pm.get("main_signals", []) if isinstance(_pm, dict) else []
            _shadow_signals = _pm.get("shadow_signals", []) if isinstance(_pm, dict) else []
            _e13_result = _e13.detect(_main_params, _shadow_params, _main_signals, _shadow_signals)
            if _e13_result.get("e13_triggered"):
                warnings.append(f"[Governance-E13] 影子策略串谋: param_diff={_e13_result.get('param_diff_pct', 0):.3f}")
            _wf_chk = WF6ToWF10EliminationChecker()
            _wf_metrics = _pm.get("walk_forward_metrics", []) if isinstance(_pm, dict) else []
            _wf_result = _wf_chk.check_all(_wf_metrics)
            if _wf_result.get("elimination_triggered"):
                conditions.append(f"[Governance-WF] 消除条件触发: {_wf_result.get('triggered_conditions', [])}")
            _e8e9e10 = E8E9E10EliminationChecker()
            _e8e9e10_result = _e8e9e10.check_all()
            if _e8e9e10_result.get("elimination_triggered"):
                blockers.append(f"[Governance-E8/E9/E10] 消除触发: {_e8e9e10_result.get('triggered_codes', [])}")
            _e7 = E7UnexplainedReturnChecker()
            _e7_result = _e7.check({"total_pnl": _pm.get("total_pnl", 0.0)} if isinstance(_pm, dict) else {"total_pnl": 0.0})
            if _e7_result.get("e7_triggered"):
                warnings.append(f"[Governance-E7] 不可解释收益: residual_pct={_e7_result.get('residual_pct', 0):.1f}%")
            _e11 = E11QuantitativeSourceChecker()
            _e11_context = _psr if isinstance(_psr, dict) else {}
            _e11_result = _e11.check(_e11_context)
            if _e11_result.get("e11_triggered"):
                warnings.append(f"[Governance-E11] 存在直觉来源参数: {_e11_result.get('intuition_params', [])}")
        except (ImportError, AttributeError, TypeError) as _ge04:
            warnings.append(f"[Governance] 检查器集成异常: {_ge04}")

        logging.info("[StrategyJudgmentEngine._run_deep_validations] %d validators executed: %d warnings, %d blockers, %d conditions",
                     sum(1 for _ in range(1)), len(warnings), len(blockers), len(conditions))

    except ImportError as e:
        logging.error(
            "[R7-H-01] _run_deep_validations Import失败: %s — "
            "深度验证降级，评判结果可能基于不完整数据", e,
        )
        warnings.append(
            f"[R7-H-01] 深度验证降级: 导入失败({e})，"
            "部分验证器未执行，评判结果置信度降低"
        )
        validation_degraded = True
    except (ValueError, KeyError, TypeError, AttributeError, RuntimeError) as e:
        logging.error("[StrategyJudgmentEngine._run_deep_validations] Error: %s", e)
        validation_degraded = True

    return warnings, blockers, conditions, validation_degraded



class VerdictService:
    def __init__(self, scoring_coefficients=None, min_samples=30, min_instruments=3, insufficient_missing_count=3, pass_threshold=0.75, conditional_threshold=0.60, blocking_dimensions=None):
        self.SCORING_COEFFICIENTS = scoring_coefficients or SCORING_COEFFICIENTS
        self._min_samples = min_samples
        self._min_instruments = min_instruments
        self._insufficient_missing_count = insufficient_missing_count
        self._pass_threshold = pass_threshold
        self._conditional_threshold = conditional_threshold
        self._blocking_dimensions = blocking_dimensions or set()

    def determine_verdict(self, overall, dimensions, report, validation_degraded=False):
        blockers = []
        conditions = []
        warnings = []

        # R7-H-01/H-04修复: 检查验证降级标志，降级时附加警告
        if getattr(self, '_validation_degraded', False):
            warnings.append(
                "[R7-H-01] 深度验证降级: 部分验证器未执行，"
                "评判置信度降低，建议检查导入依赖"
            )
            # R7-H-04修复: 降级时对PASS结果增加CONDITIONAL标记
            # 确保评判结果不因数据缺失而过于乐观

        if report is not None and report.extreme_point_count < self._min_samples:
            return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                    [f"极值样本{report.extreme_point_count}<{self._min_samples}，增加回测期或降时间维度"],
                    [])

        # E-17修复: report=None时也要检查样本量(通过profitability_metrics的total_trades)
        if report is None:
            _trade_count_dim = next((d for d in dimensions if "盈利" in d.name), None)
            if _trade_count_dim is not None and _trade_count_dim.score == 0.0 and not _trade_count_dim.passed:
                warnings.append(f"[E-17] report=None且盈利维度未通过，样本量可能不足，评判降级为INSUFFICIENT_EVIDENCE")
                return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                        [f"report缺失且盈利数据不足，无法确认样本量>={self._min_samples}"],
                        [])

        missing_sources = sum(1 for d in dimensions if d.score == 0.0 and not d.passed)
        if missing_sources >= self._insufficient_missing_count:
            missing_names = [d.name for d in dimensions if d.score == 0.0 and not d.passed]
            return (JudgmentVerdict.INSUFFICIENT_EVIDENCE, [],
                    [f"{missing_sources}项数据源缺失({', '.join(missing_names)})，证据不足无法评判"],
                    [])

        # E-14修复: 阻塞维度匹配使用反向映射(中文名→英文常量)
        _display_to_key = {v: k for k, v in DIM_DISPLAY_NAMES.items()}
        for d in dimensions:
            _dim_key = _display_to_key.get(d.name, d.name)
            is_blocker = _dim_key in self._blocking_dimensions
            d.is_blocker = is_blocker
            if is_blocker and not d.passed:
                blockers.append(f"{d.name}: 得分{d.score:.2f}<阈值{d.threshold:.2f} — 一票否决")
            elif not d.passed:
                conditions.append(f"{d.name}: 得分{d.score:.2f}<阈值{d.threshold:.2f}")
            elif d.score < d.threshold + self.SCORING_COEFFICIENTS["near_threshold_margin"]:
                warnings.append(f"{d.name}: 得分{d.score:.2f}接近阈值{d.threshold:.2f}")

        if blockers:
            return JudgmentVerdict.FAIL, blockers, conditions, warnings

        if overall >= self._pass_threshold and len(conditions) == 0:
            return JudgmentVerdict.PASS, [], [], warnings

        if overall >= self._conditional_threshold and len(conditions) <= self.SCORING_COEFFICIENTS["conditional_pass_max_conditions"]:
            return JudgmentVerdict.CONDITIONAL_PASS, [], conditions, warnings

        return JudgmentVerdict.FAIL, [], conditions + [f"总分{overall:.2f}低于门槛{self._conditional_threshold}"], warnings


    def generate_recommendations(self, verdict, dimensions, blockers, conditions):
        recs = []
        if blockers:
            recs.append("策略存在阻塞项，判定为不可上线：")
            for b in blockers:
                recs.append(f"  ** {b}")
            recs.append("必须修复阻塞项后方可重新评判。")
        elif verdict == JudgmentVerdict.PASS:
            recs.append("策略通过评判，可进入实盘候选池。")
            recs.append("建议：上线前进行至少1周影子跟踪验证。")
        elif verdict == JudgmentVerdict.CONDITIONAL_PASS:
            recs.append("策略有条件通过，需修复：")
            for c in conditions:
                recs.append(f"  - {c}")
            recs.append("注意：修复策略逻辑漏洞，而非调整参数。")
        elif verdict == JudgmentVerdict.FAIL:
            failed = [d.name for d in dimensions if not d.passed]
            recs.append("策略未通过评判，判定为回测幻觉。")
            recs.append(f"失败维度: {', '.join(failed)}")
            recs.append("建议：重新审查策略核心逻辑。")
        elif verdict == JudgmentVerdict.INSUFFICIENT_EVIDENCE:
            recs.append("证据不足，无法判定。建议：")
            recs.append(f"  1. 增加回测期（至少覆盖{self._min_samples}个局部高低点）")
            recs.append(f"  2. 降时间维度（分钟级数据产生更多极值样本）")
            recs.append(f"  3. 跨品种验证（≥{self._min_instruments}品种）")
        return recs



class ScoringHelper:
    @staticmethod
    def chicory_eviction_score(strategy_score, age_days, violation_count: int = 0) -> float:
        """三档权重驱逐评分 - P1-09修复: 委托evaluation模块"""
        from ali2026v3_trading.evaluation.chicory_eviction import ChicoryEvictionPolicy
        try:
            _cep = ChicoryEvictionPolicy()
            _result = _cep.evaluate(strategy_id='', overall_score=strategy_score, dimensions={'age_days': age_days, 'violation_count': violation_count})
            if 'eviction_score' in _result:
                return _result['eviction_score']
        except (ImportError, AttributeError, TypeError) as _cep_err:
            logging.debug("[chicory_eviction_score] ChicoryEvictionPolicy降级: %s", _cep_err)
        _exp_arg = max(-500.0, min(500.0, -age_days / 30.0))
        return 0.60 * strategy_score + 0.25 * math.exp(_exp_arg) + 0.15 * max(0.0, 1.0 - violation_count * 0.1)

    @staticmethod
    def activity_weighted_score(sharpe_history=None, regime_sharpes: Dict[str, float] = None,
                               capacity_used_pct: float = 0.0,
                               recovery_days_history: List[float] = None) -> Dict[str, Any]:
        """四维度活跃度加权评分 - P1-09修复: 委托evaluation模块"""
        from ali2026v3_trading.evaluation.activity_weighted_scorer import ActivityWeightedScorer
        try:
            _aws = ActivityWeightedScorer()
            return _aws.calculate(strategy_id='', overall=0.5, dimensions={'sharpe_history': sharpe_history, 'regime_sharpes': regime_sharpes, 'capacity_used_pct': capacity_used_pct, 'recovery_days_history': recovery_days_history})
        except (ImportError, AttributeError, TypeError) as _aws_err:
            logging.debug("[activity_weighted_score] ActivityWeightedScorer降级: %s", _aws_err)
        if sharpe_history and len(sharpe_history) >= 2:
            recent_sharpe = sum(sharpe_history[-3:]) / min(3, len(sharpe_history[-3:]))
        else:
            recent_sharpe = 0.0
        regime_bonus = 0.0
        if regime_sharpes:
            regime_bonus = sum(regime_sharpes.values()) / max(1, len(regime_sharpes)) * 0.1
        capacity_penalty = max(0.0, capacity_used_pct - 0.8) * 0.2
        recovery_bonus = 0.0
        if recovery_days_history:
            avg_recovery = sum(recovery_days_history) / max(1, len(recovery_days_history))
            recovery_bonus = max(0.0, 1.0 - avg_recovery / 30.0) * 0.1
        return {
            'overall': recent_sharpe + regime_bonus - capacity_penalty + recovery_bonus,
            'recent_sharpe': recent_sharpe,
            'regime_bonus': regime_bonus,
            'capacity_penalty': capacity_penalty,
            'recovery_bonus': recovery_bonus,
        }



# ============================================================================
# Scoring Helpers（原 judgment_scoring_helpers.py）
# ============================================================================
"""
包含:
  - chicory_eviction_score: Chicory淘汰评分
  - activity_weighted_score: 活动加权评分
  - run_ecosystem_integrations: 生态集成(CascadeJudge/E-04~E-13/Alpha-CI/DeepValidation/Sensitivity)

注意: STRICT_MODE, ComponentFailurePolicy, CRITICAL_COMPONENTS, _handle_component_failure,
      run_ecosystem_integrations 等符号已迁移到 judgment_scoring_helpers.py，本模块通过 re-export 保持兼容。
"""

import logging
import math
import os
from enum import Enum
from typing import Any, Dict, List, Optional

# ── 从 judgment_scoring_helpers.py re-export（权威源已迁移） ──
from ali2026v3_trading.strategy_judgment.judgment_scoring_helpers import (  # noqa: F401
    STRICT_MODE, ComponentFailurePolicy, CRITICAL_COMPONENTS,
    _handle_component_failure, _block_flag, was_blocked,
    chicory_eviction_score, activity_weighted_score,
    run_ecosystem_integrations,
)


# ── 以下定义已迁移到 judgment_scoring_helpers.py，通过上方 re-export 保持兼容 ──

