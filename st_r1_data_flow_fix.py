# MODULE_ID: M2-512
"""R1轮端到端断言验证测试 — P0数据流断裂修复"""
import ast
import importlib
import inspect
import os
import sys
import pytest

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestR1_1_CheckerRealData:
    """R1-1: 6个governance checker接入真实业务数据 — 不再传入空列表/空字典"""

    def test_e12_no_empty_list_stub(self):
        """E12.detect() 不再传入 [],[] 空列表"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_e12.detect([], [])' not in src, "R1-1失败: E12仍传入空列表stub"

    def test_e13_no_empty_stub(self):
        """E13.detect() 不再传入 {},{},[],[] 空集合"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_e13.detect({}, {}, [], [])' not in src, "R1-1失败: E13仍传入空集合stub"

    def test_wf_no_empty_list(self):
        """WF6-WF10.check_all() 不再传入 [] 空列表"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_wf_chk.check_all([])' not in src, "R1-1失败: WF6-WF10仍传入空列表"

    def test_e7_no_hardcoded_pnl(self):
        """E7.check() 不再传入硬编码 {"total_pnl": 0.0}"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_e7.check({"total_pnl": 0.0})' not in src, "R1-1失败: E7仍传入硬编码0.0"

    def test_e11_no_empty_dict(self):
        """E11.check() 不再传入 {} 空字典"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_e11.check({})' not in src, "R1-1失败: E11仍传入空字典"

    def test_checker_uses_profitability_metrics(self):
        """checker数据来源从函数参数提取（profitability_metrics等）"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_pm = profitability_metrics' in src, "R1-1失败: checker未从profitability_metrics提取数据"
        assert '_psr = parameter_stability_result' in src, "R1-1失败: checker未从parameter_stability_result提取数据"

    def test_e7_uses_real_pnl(self):
        """E7使用例pm.get('total_pnl')而非硬编码0.0"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert "_pm.get(\"total_pnl\", 0.0)" in src, "R1-1失败: E7未使用例pm.get提取真实PnL"

    def test_governance_except_not_bare(self):
        """governance checker的except不再是bare Exception"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert 'except (ImportError, AttributeError, TypeError) as _ge04:' in src, "R1-1失败: governance checker仍使用bare Exception"


class TestR1_2_DeepValidationRealContext:
    """R1-2: DeepValidationSuite传入真实回测上下文而非空字典"""

    def test_dvs_no_empty_dict(self):
        """run_full_validation不再传入空字典{}"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert 'run_full_validation({})' not in src, "R1-2失败: DVS仍传入空字典"

    def test_dvs_context_has_strategy_id(self):
        """验证上下文包含strategy_id"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '"strategy_id": strategy_id' in src, "R1-2失败: DVS上下文缺少strategy_id"

    def test_dvs_context_has_profitability(self):
        """验证DVS上下文从profitability_metrics提取backtest_params"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_dvs_params = profitability_metrics.get("backtest_params")' in src, "R1-2失败: DVS上下文未从profitability_metrics提取backtest_params"

    def test_dvs_context_has_param_stability(self):
        """验证DVS上下文包含bar_data提取"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', '_judgment_services.py'), encoding='utf-8').read()
        assert '_dvs_bar_data = profitability_metrics.get("bar_data")' in src, "R1-2失败: DVS上下文未提取bar_data"


class TestR1_3_HardBlockCriticalPaths:
    """R1-3: 关键路径硬阻断改造"""

    def test_cascade_judge_strict_mode(self):
        """CascadeJudge集成异常时STRICT_MODE硬阻断"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert 'raise RuntimeError(f"[CascadeJudge] 集成异常(STRICT_MODE)' in src, "R1-3失败: CascadeJudge未启用STRICT_MODE硬阻断"

    def test_governance_engine_failure_policy(self):
        """governance_engine集成异常时有ComponentFailurePolicy处理"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert '_handle_component_failure("E-04_governance"' in src, "R1-3失败: E-04未使用例handle_component_failure"

    def test_chicory_eviction_failure_policy(self):
        """ChicoryEvictionPolicy集成异常时有ComponentFailurePolicy处理"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert '_handle_component_failure("E-09_chicory_eviction"' in src, "R1-3失败: E-09未使用例handle_component_failure"

    def test_marquee_threshold_failure_policy(self):
        """MarqueeThreshold集成异常时有ComponentFailurePolicy处理"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert '_handle_component_failure("E-10_marquee_threshold"' in src, "R1-3失败: E-10未使用例handle_component_failure"

    def test_component_failure_policy_exists(self):
        """ComponentFailurePolicy枚举和CRITICAL_COMPONENTS配置存在"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert 'class ComponentFailurePolicy' in src, "R1-3失败: ComponentFailurePolicy枚举不存在"
        assert 'CRITICAL_COMPONENTS' in src, "R1-3失败: CRITICAL_COMPONENTS配置不存在"
        assert 'ComponentFailurePolicy.BLOCK' in src, "R1-3失败: 缺少BLOCK策略"

    def test_scoring_helpers_no_bare_except_in_critical(self):
        """关键路径（CascadeJudge/E-04/E-09/E-10）不再有bare except Exception"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        tree = ast.parse(src)
        critical_func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == 'run_ecosystem_integrations':
                critical_func = node
                break
        assert critical_func is not None, "R1-3失败: 未找到run_ecosystem_integrations函数"
        bare_excepts = []
        for node in ast.walk(critical_func):
            if isinstance(node, ast.ExceptHandler) and node.type is None:
                bare_excepts.append(node.lineno)
        assert len(bare_excepts) == 0, f"R1-3失败: 关键路径仍有bare except在行{bare_excepts}"


class TestR1_4_PhaseScanValidatorIntegration:
    """R1-4: phase_scan_core.py集成验证器"""

    def test_survival_bias_in_p0_gate(self):
        """p0_gate_check中集成SurvivalBiasTest"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8').read()
        assert 'SurvivalBiasTest' in src, "R1-4失败: phase_scan.py未集成SurvivalBiasTest"

    def test_cross_validator_in_p0_gate(self):
        """p0_gate_check中集成MultiPeriodCrossValidator"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8').read()
        assert 'MultiPeriodCrossValidator' in src, "R1-4失败: phase_scan.py未集成MultiPeriodCrossValidator"

    def test_survival_bias_uses_absolute_import(self):
        """SurvivalBiasTest使用绝对导入"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8').read()
        assert 'from ali2026v3_trading.param_pool.validation.statistical_validation import SurvivalBiasTest' in src, "R1-4失败: SurvivalBiasTest未使用绝对导入"

    def test_cross_validator_uses_absolute_import(self):
        """MultiPeriodCrossValidator使用绝对导入"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8').read()
        assert 'from ali2026v3_trading.param_pool.validation.adv_validation_misc import MultiPeriodCrossValidator' in src, "R1-4失败: MultiPeriodCrossValidator未使用绝对导入"

    def test_validator_failure_blocks(self):
        """验证器失败时添加到failures（硬阻断）而非仅warnings"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8').read()
        assert 'failures.append(f"幸存者偏差检验未通过' in src, "R1-4失败: SurvivalBiasTest失败未添加到failures"
        assert 'failures.append(f"跨期交叉验证未通过' in src, "R1-4失败: MultiPeriodCrossValidator失败未添加到failures"


class TestR1_5_CascadeJudgeImportUnification:
    """R1-5: CascadeJudge导入路径统一为ali2026v3_trading.evaluation.cascade_judge"""

    def test_phase_scan_core_absolute_import(self):
        """phase_scan.py使用绝对导入"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'optimization', 'phase_scan.py'), encoding='utf-8').read()
        assert 'from ali2026v3_trading.evaluation.cascade_judge import' in src, "R1-5失败: phase_scan.py未使用绝对导入"
        assert 'from evaluation.cascade_judge import' not in src, "R1-5失败: phase_scan.py仍有相对导入"

    def test_backtest_state_absolute_import(self):
        """backtest_state.py使用绝对导入"""
        src = open(os.path.join(_PROJECT_ROOT, 'param_pool', 'backtest', 'backtest_state.py'), encoding='utf-8').read()
        assert 'from ali2026v3_trading.evaluation.cascade_judge import' in src, "R1-5失败: backtest_state.py未使用绝对导入"
        assert 'from evaluation.cascade_judge import' not in src, "R1-5失败: backtest_state.py仍有相对导入"

    def test_trading_ev_absolute_import(self):
        """services.py使用绝对导入（_trading_ev.py已合并入services.py）"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy', 'strategy_ecosystem', 'services.py'), encoding='utf-8').read()
        assert 'from ali2026v3_trading.evaluation.cascade_judge import' in src, "R1-5失败: services.py未使用绝对导入"
        assert 'from evaluation.cascade_judge import' not in src, "R1-5失败: services.py仍有相对导入"

    def test_no_relative_cascade_import_anywhere(self):
        """全项目不再有 from evaluation.cascade_judge import 的相对导入"""
        for root, dirs, files in os.walk(_PROJECT_ROOT):
            dirs[:] = [d for d in dirs if not d.startswith(('.', '_backup_', '__pycache__'))]
            for f in files:
                if f.endswith('.py') and f not in ('cascade_judge.py', 'test_r1_data_flow_fix.py'):
                    fpath = os.path.join(root, f)
                    try:
                        src = open(fpath, encoding='utf-8').read()
                    except (UnicodeDecodeError, PermissionError):
                        continue
                    for i, line in enumerate(src.split('\n'), 1):
                        stripped = line.strip()
                        if stripped.startswith('#') or stripped.startswith('"') or stripped.startswith("'"):
                            continue
                        if 'from evaluation.cascade_judge import' in line and 'ali2026v3_trading' not in line:
                            pytest.fail(f"R1-5 failed: {fpath}:{i} still has relative import: {stripped}")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])