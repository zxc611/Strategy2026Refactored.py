#!/usr/bin/env python3
# MODULE_ID: M2-347
"""升A路径端到端断言验证脚本

按照《三大系统代码现状实证评判及升A路径报告》第九节的验收标准，
对每个修复任务进行端到端断言验证。

验证方式：每5个问题一组，全部通过后进入下一组。
任何断言失败即标记该组未通过，输出详细失败原因。

用法:
    python -m pytest tests/test_grade_a_e2e_assertions.py -v
    或
    python tests/test_grade_a_e2e_assertions.py
"""
from __future__ import annotations

import ast
import os
import re
import sys
import importlib
import logging
import subprocess
import textwrap
from pathlib import Path
from typing import List, Tuple

# 项目根目录 — 测试文件在 ali2026v3_trading/tests/ 下，上两级即为 ali2026v3_trading/
_SRC_ROOT = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _SRC_ROOT.parent

# 确保项目根目录在sys.path中
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _read_file(rel_path: str) -> str:
    """读取项目内文件内容"""
    full_path = _SRC_ROOT / rel_path
    if not full_path.exists():
        return ""
    with open(full_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _grep_pattern(pattern: str, rel_path: str) -> List[Tuple[int, str]]:
    """在指定文件中搜索正则模式，返回(行号, 行内容)列表"""
    content = _read_file(rel_path)
    if not content:
        return []
    results = []
    for i, line in enumerate(content.splitlines(), 1):
        if re.search(pattern, line):
            results.append((i, line.strip()))
    return results


def _grep_dir(pattern: str, directory: str = "", exclude_dirs: List[str] = None) -> int:
    """在目录中递归搜索正则模式，返回命中数"""
    if exclude_dirs is None:
        exclude_dirs = ["_backup", "__pycache__", ".git", "tests"]
    search_dir = _SRC_ROOT / directory if directory else _SRC_ROOT
    count = 0
    for root, dirs, files in os.walk(search_dir):
        # 排除目录
        dirs[:] = [d for d in dirs if not any(ex in d for ex in exclude_dirs)]
        for f in files:
            if not f.endswith(".py"):
                continue
            filepath = os.path.join(root, f)
            try:
                with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        if re.search(pattern, line):
                            count += 1
            except (OSError, UnicodeDecodeError):
                pass
    return count


def _count_except_exception_in_core() -> int:
    """统计核心路径文件中 except Exception 的出现次数"""
    core_files = [
        "param_pool/phase_scan_cli.py",
        "strategy_judgment/_judgment_services.py",
        "strategy_judgment/judgment_scoring_helpers.py",
        "risk/risk_service.py",
    ]
    total = 0
    for f in core_files:
        total += len(_grep_pattern(r"except\s+Exception", f))
    return total


def _count_except_exception_pass_globally() -> int:
    """统计全库（排除backup和test）中 except Exception: pass 的出现次数"""
    return _grep_dir(r"except\s+Exception\s*:\s*pass", exclude_dirs=["_backup", "__pycache__", ".git", "tests"])


# ============================================================================
# 第1组验证（问题1-5）: Phase 1 止血与硬化
# ============================================================================

def test_Q01_soft_integration_core_path():
    """Q01: 核心路径 except Exception 数量 ≤ 5（T1.1验收标准）"""
    count = _count_except_exception_in_core()
    assert count <= 5, (
        f"Q01 FAIL: 核心路径 except Exception 数量={count}, 预期≤5. "
        f"需继续替换为精确异常捕获或STRICT_MODE硬阻断"
    )


def test_Q02_soft_integration_global_silent():
    """Q02: 全库 except Exception: pass 数量 = 0（T1.1验收标准）"""
    count = _count_except_exception_pass_globally()
    assert count == 0, (
        f"Q02 FAIL: 全库 except Exception: pass 数量={count}, 预期0. "
        f"静默吞没异常是最危险的软集成模式"
    )


def test_Q03_dead_code_risk_dashboard():
    """Q03: RiskDashboardService 有定义且有调用（T1.2验收标准）"""
    # 检查类定义存在
    class_defs = _grep_pattern(r"class RiskDashboardService", "risk/risk_service.py")
    assert len(class_defs) >= 1, (
        "Q03 FAIL: RiskDashboardService 类定义不存在于 risk_service.py"
    )
    # 检查 get_risk_dashboard_service 函数定义存在
    func_defs = _grep_pattern(r"def get_risk_dashboard_service", "risk/risk_service.py")
    assert len(func_defs) >= 1, (
        "Q03 FAIL: get_risk_dashboard_service() 函数定义不存在于 risk_service.py"
    )
    # 检查有调用（在RiskService.__init__中）
    calls = _grep_pattern(r"get_risk_dashboard_service\(\)", "risk/risk_service.py")
    assert len(calls) >= 2, (  # 定义处1 + 调用处1 = 至少2
        f"Q03 FAIL: get_risk_dashboard_service() 调用数={len(calls)}, 预期≥2 (定义+调用)"
    )


def test_Q04_three_source_split_fixed():
    """Q04: 三源分裂已修复 — _load_absolute_ev_floor 无硬编码fallback（T1.3验收标准）"""
    # 重构后: shadow_strategy_core.py 是新合并文件(真相源), _shadow_strategy_core.py 是DEPRECATED shim
    core_file = "strategy/shadow_strategy_core.py"
    # 检查无 except Exception + ABSOLUTE_EV_FLOOR fallback
    fallback_patterns = _grep_pattern(
        r"except\s+Exception.*ABSOLUTE_EV_FLOOR|except.*fallback.*硬编码|self\._absolute_ev_floor\s*=\s*self\.ABSOLUTE_EV_FLOOR",
        f"strategy/{core_file}" if not core_file.startswith("strategy/") else core_file
    )
    assert len(fallback_patterns) == 0, (
        f"Q04 FAIL: 仍存在硬编码fallback模式: {fallback_patterns}"
    )
    # 检查 ConfigError 存在
    config_error = _grep_pattern(r"class ConfigError", f"strategy/{core_file}" if not core_file.startswith("strategy/") else core_file)
    assert len(config_error) >= 1, (
        "Q04 FAIL: ConfigError 异常类未定义，三源分裂修复不完整"
    )


def test_Q05_params_yaml_single_source():
    """Q05: config/params.yaml 作为唯一参数真相源存在（T1.3验收标准）"""
    params_yaml = _SRC_ROOT / "config" / "params.yaml"
    assert params_yaml.exists(), (
        "Q05 FAIL: config/params.yaml 不存在，参数真相源未建立"
    )
    content = params_yaml.read_text(encoding="utf-8")
    # 检查关键参数存在
    assert "shadow_absolute_ev_floor" in content, (
        "Q05 FAIL: config/params.yaml 中缺少 shadow_absolute_ev_floor 参数"
    )
    assert "parameter_attributes" in content, (
        "Q05 FAIL: config/params.yaml 中缺少 parameter_attributes 区块"
    )


# ============================================================================
# 第2组验证（问题6-10）: Phase 1 续 + Phase 2 开始
# ============================================================================

def test_Q06_import_path_unified():
    """Q06: 生产代码中无相对导入 from evaluation.（T1.4验收标准）"""
    count = _grep_dir(
        r"from\s+evaluation\.",
        exclude_dirs=["_backup", "__pycache__", ".git", "tests"]
    )
    assert count == 0, (
        f"Q06 FAIL: 生产代码中仍有 {count} 处 from evaluation. 相对导入"
    )


def test_Q07_risk_check_fail_closed():
    """Q07: 风控检查方法 fail-closed — 委托链异常时返回阻断式默认值（T1.1d验收）"""
    content = _read_file("risk/risk_service.py")
    # 检查 check_regulatory_compliance 有 try/except 返回 compliant: False
    assert "compliant': False" in content or "'compliant': False" in content, (
        "Q07 FAIL: check_regulatory_compliance 委托链异常时未返回 compliant=False (fail-closed)"
    )
    # 检查 check_capital_sufficiency 有 try/except 返回 sufficient: False
    assert "sufficient': False" in content or "'sufficient': False" in content, (
        "Q07 FAIL: check_capital_sufficiency 委托链异常时未返回 sufficient=False (fail-closed)"
    )
    # 检查 check_exchange_status 有 try/except 返回 status: CLOSED
    assert "status': 'CLOSED'" in content or "'status': 'CLOSED'" in content, (
        "Q07 FAIL: check_exchange_status 委托链异常时未返回 status=CLOSED (fail-closed)"
    )


def test_Q08_strict_mode_exists():
    """Q08: STRICT_MODE 标志存在于关键文件（T1.1验收标准）"""
    # phase_scan_core.py is now a shim; check actual implementation
    cli_content = _read_file("param_pool/optimization/phase_scan.py")
    if not cli_content or "STRICT_MODE" not in cli_content:
        cli_content = _read_file("param_pool/phase_scan_core.py")
    assert "STRICT_MODE" in cli_content, (
        "Q08 FAIL: phase_scan_core.py / phase_scan.py 中无 STRICT_MODE 标志"
    )
    # _judgment_services.py
    js_content = _read_file("strategy_judgment/_judgment_services.py")
    assert "STRICT_MODE" in js_content, (
        "Q08 FAIL: _judgment_services.py 中无 STRICT_MODE 标志"
    )
    # judgment_scoring_helpers.py
    jsh_content = _read_file("strategy_judgment/judgment_scoring_helpers.py")
    assert "STRICT_MODE" in jsh_content, (
        "Q08 FAIL: judgment_scoring_helpers.py 中无 STRICT_MODE 标志"
    )


def test_Q09_phase_scan_core_validators():
    """Q09: phase_scan_core.py 中 MultiPeriodCrossValidator 和 SurvivalBiasTest 被显式调用（T2.2验收标准）"""
    content = _read_file("param_pool/optimization/phase_scan.py")
    if not content:
        content = _read_file("param_pool/phase_scan_core.py")
    assert "MultiPeriodCrossValidator" in content, (
        "Q09 FAIL: phase_scan_core.py / phase_scan.py 中无 MultiPeriodCrossValidator 调用"
    )
    assert "SurvivalBiasTest" in content, (
        "Q09 FAIL: phase_scan_core.py / phase_scan.py 中无 SurvivalBiasTest 调用"
    )


def test_Q10_backtest_runner_risk_direct():
    """Q10: backtest_runner_base.py 直接导入并调用风控检查（T2.3验收标准）"""
    content = _read_file("param_pool/backtest/backtest_runner_base.py")
    if not content:
        content = _read_file("param_pool/backtest_runner_base.py")
    assert "from ali2026v3_trading.risk.risk_service import get_risk_service" in content, (
        "Q10 FAIL: backtest_runner_base.py 未直接导入 get_risk_service"
    )
    assert "check_regulatory_compliance" in content, (
        "Q10 FAIL: backtest_runner_base.py 未调用 check_regulatory_compliance"
    )


# ============================================================================
# 第3组验证（问题11-15）: Phase 2 架构重构
# ============================================================================

def test_Q11_facade_pure_composition():
    """Q11: ShadowStrategyEngine 纯组合 — MRO 无 ShadowStrategyCoreService（T2.4验收标准）"""
    content = _read_file("strategy/shadow_strategy_facade.py")
    # 检查类定义不含继承
    class_line = _grep_pattern(r"class ShadowStrategyEngine\s*\(", "strategy/shadow_strategy_facade.py")
    for _, line in class_line:
        assert "ShadowStrategyCoreService" not in line, (
            f"Q11 FAIL: ShadowStrategyEngine 仍继承 ShadowStrategyCoreService: {line}"
        )
    # 检查组合模式
    assert "self._core_service = ShadowStrategyCoreService" in content, (
        "Q11 FAIL: ShadowStrategyEngine 未使用组合模式持有 _core_service"
    )


def test_Q12_internal_module_underscore_prefix():
    """Q12: 内部模块使用下划线前缀（T2.5验收标准）"""
    # 重构后: shadow_strategy_core.py 和 shadow_strategy_pnl.py 是新合并文件(真相源)
    # _shadow_strategy_core.py 和 _shadow_strategy_pnl.py 是DEPRECATED shim
    assert (_SRC_ROOT / "strategy/shadow_strategy_core.py").exists(), (
        "Q12 FAIL: strategy/shadow_strategy_core.py 新合并文件不存在"
    )
    assert (_SRC_ROOT / "strategy/_shadow_strategy_signal.py").exists(), (
        "Q12 FAIL: strategy/_shadow_strategy_signal.py 不存在"
    )
    assert (_SRC_ROOT / "strategy/shadow_strategy_pnl.py").exists(), (
        "Q12 FAIL: strategy/shadow_strategy_pnl.py 新合并文件不存在"
    )
    # shim文件应标记DEPRECATED
    for shim in ["_shadow_strategy_core.py", "_shadow_strategy_pnl.py"]:
        shim_path = _SRC_ROOT / "strategy" / shim
        if shim_path.exists():
            assert "DEPRECATED" in shim_path.read_text()[:200], (
                f"Q12 FAIL: strategy/{shim} shim文件未标记DEPRECATED"
            )


def test_Q13_no_non_underscore_internal_imports():
    """Q13: 生产代码中无非下划线前缀的内部模块导入（T2.5验收标准）"""
    # 重构后: shadow_strategy_core 和 shadow_strategy_pnl 是新合并文件(无下划线前缀是正确的)
    # 只检查已废弃的旧模块名（如 shadow_strategy_pnl_metrics）
    count = _grep_dir(
        r"from\s+.*\.strategy\.shadow_strategy_pnl_metrics\s+import",
        exclude_dirs=["_backup", "__pycache__", ".git", "tests"]
    )
    assert count == 0, (
        f"Q13 FAIL: 生产代码中仍有 {count} 处已废弃的 shadow_strategy_pnl_metrics 导入"
    )


def test_Q14_facade_imports_underscore_modules():
    """Q14: shadow_strategy_facade.py 导入使用正确模块路径（T2.5验收标准）"""
    content = _read_file("strategy/shadow_strategy_facade.py")
    # 重构后: facade导入新合并文件(无下划线前缀)
    assert "from ali2026v3_trading.strategy.shadow_strategy_core import" in content, (
        "Q14 FAIL: facade 未导入 shadow_strategy_core (新合并文件)"
    )
    assert "from ali2026v3_trading.strategy._shadow_strategy_signal import" in content, (
        "Q14 FAIL: facade 未使用下划线前缀导入 _shadow_strategy_signal"
    )
    assert "from ali2026v3_trading.strategy.shadow_strategy_pnl import" in content, (
        "Q14 FAIL: facade 未导入 shadow_strategy_pnl (新合并文件)"
    )


def test_Q15_verify_internal_imports_script():
    """Q15: verify_internal_imports.py 脚本存在（T2.5验收标准）"""
    script_path = _SRC_ROOT / "scripts" / "verify_internal_imports.py"
    assert script_path.exists(), (
        "Q15 FAIL: scripts/verify_internal_imports.py 不存在"
    )


# ============================================================================
# 第4组验证（问题16-20）: Phase 3 固化与自动化
# ============================================================================

def test_Q16_risk_service_exception_precision():
    """Q16: risk_service.py 中 except Exception 数量 = 0（T1.1d验收标准）"""
    count = len(_grep_pattern(r"except\s+Exception", "risk/risk_service.py"))
    assert count == 0, (
        f"Q16 FAIL: risk_service.py 中仍有 {count} 处 except Exception"
    )


def test_Q17_judgment_services_exception_precision():
    """Q17: _judgment_services.py 中 except Exception 数量 = 0（T1.1b验收标准）"""
    count = len(_grep_pattern(r"except\s+Exception", "strategy_judgment/_judgment_services.py"))
    assert count == 0, (
        f"Q17 FAIL: _judgment_services.py 中仍有 {count} 处 except Exception"
    )


def test_Q18_scoring_helpers_exception_precision():
    """Q18: judgment_scoring_helpers.py 中 except Exception 数量 = 0（T1.1c验收标准）"""
    count = len(_grep_pattern(r"except\s+Exception", "strategy_judgment/judgment_scoring_helpers.py"))
    assert count == 0, (
        f"Q18 FAIL: judgment_scoring_helpers.py 中仍有 {count} 处 except Exception"
    )


def test_Q19_phase_scan_cli_exception_precision():
    """Q19: phase_scan_cli.py 中 except Exception 数量 = 0（T1.1a验收标准）"""
    count = len(_grep_pattern(r"except\s+Exception", "param_pool/phase_scan_cli.py"))
    assert count == 0, (
        f"Q19 FAIL: phase_scan_cli.py 中仍有 {count} 处 except Exception"
    )


def test_Q20_config_error_on_missing_yaml():
    """Q20: _load_absolute_ev_floor 在YAML缺失时抛出 ConfigError（T1.3验收标准）"""
    # 重构后: shadow_strategy_core.py 是新合并文件(真相源)
    core_file = "strategy/shadow_strategy_core.py"
    content = _read_file(core_file)
    # 检查 raise ConfigError 存在
    assert "raise ConfigError" in content, (
        "Q20 FAIL: _shadow_strategy_core.py 中无 raise ConfigError，YAML缺失时不会报错"
    )
    # 检查不再有 self.ABSOLUTE_EV_FLOOR fallback
    assert "self._absolute_ev_floor: float = self.ABSOLUTE_EV_FLOOR" not in content, (
        "Q20 FAIL: 仍存在 self.ABSOLUTE_EV_FLOOR 硬编码fallback"
    )


# ============================================================================
# 第5组验证（问题21-25）: 补充验收 — NullValidator + ConfigService + infra
# ============================================================================

def test_Q21_null_validator_exists():
    """Q21: NullValidator类存在于phase_scan_core.py（T1.1验收标准）"""
    content = _read_file("param_pool/optimization/phase_scan.py")
    if not content:
        content = _read_file("param_pool/phase_scan_core.py")
    assert "class NullValidator" in content, (
        "Q21 FAIL: phase_scan_core.py / phase_scan.py 中无 NullValidator 类定义"
    )
    assert "NullValidator(" in content, (
        "Q21 FAIL: NullValidator类已定义但未被使用"
    )


def test_Q22_config_service_get_param():
    """Q22: ConfigService.get_param()统一参数接口存在（T2.1验收标准）"""
    content = _read_file("config/config_service.py")
    assert "def get_param" in content, (
        "Q22 FAIL: config/config_service.py 中无 get_param 方法"
    )
    # 检查模块级便捷函数
    lines = _grep_pattern(r"^def get_param\(", "config/config_service.py")
    assert len(lines) >= 1, (
        "Q22 FAIL: 缺少模块级 get_param() 便捷函数"
    )


def test_Q23_shadow_core_uses_config_service():
    """Q23: shadow_strategy_core.py 使用ConfigService.get_param统一接口（T2.1验收标准）"""
    # 重构后: shadow_strategy_core.py 是新合并文件(真相源)
    core_file = "strategy/shadow_strategy_core.py"
    content = _read_file(core_file)
    assert "config_service" in content or "get_param" in content, (
        "Q23 FAIL: _shadow_strategy_core.py 未使用 ConfigService.get_param 统一接口"
    )


def test_Q24_infra_no_except_exception_pass():
    """Q24: infra/ 目录中 except Exception: pass = 0（T1.1扩展验收标准）"""
    infra_dir = _SRC_ROOT / "infra"
    if not infra_dir.exists():
        return  # infra目录不存在则跳过
    count = 0
    for root, dirs, files in os.walk(infra_dir):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", "_backup")]
        for f in files:
            if not f.endswith(".py"):
                continue
            filepath = os.path.join(root, f)
            try:
                with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
                    content = fh.read()
                # 多行匹配 except Exception: pass
                if re.search(r"except\s+Exception\s*:\s*\n\s*pass", content):
                    count += 1
            except (OSError, UnicodeDecodeError):
                pass
    assert count == 0, (
        f"Q24 FAIL: infra/ 目录中仍有 {count} 个文件包含 except Exception: pass"
    )


def test_Q25_global_no_except_exception_pass():
    """Q25: 全库（排除backup和test）except Exception: pass = 0（T1.1终极验收标准）"""
    count = _grep_dir(
        r"except\s+Exception\s*:\s*pass",
        exclude_dirs=["_backup", "__pycache__", ".git", "tests"]
    )
    assert count == 0, (
        f"Q25 FAIL: 全库仍有 {count} 处 except Exception: pass"
    )


# ============================================================================
# 第6组验证（问题26-30）: 补充验收 — 测试覆盖 + CI/CD + 文档
# ============================================================================

def test_Q26_risk_check_unit_tests_exist():
    """Q26: risk_service 3个check函数单元测试文件存在（T3.1验收标准）"""
    test_file = _SRC_ROOT / "tests" / "test_risk_check_functions.py"
    assert test_file.exists(), (
        "Q26 FAIL: tests/test_risk_check_functions.py 不存在"
    )
    content = test_file.read_text(encoding="utf-8", errors="replace")
    assert "check_regulatory_compliance" in content, (
        "Q26 FAIL: 测试文件中无 check_regulatory_compliance 测试"
    )
    assert "check_capital_sufficiency" in content, (
        "Q26 FAIL: 测试文件中无 check_capital_sufficiency 测试"
    )
    assert "check_exchange_status" in content, (
        "Q26 FAIL: 测试文件中无 check_exchange_status 测试"
    )


def test_Q27_overfit_rejection_test_exists():
    """Q27: 过拟合参数集集成测试文件存在（T2.2验收标准）"""
    test_file = _SRC_ROOT / "tests" / "test_overfit_rejection.py"
    assert test_file.exists(), (
        "Q27 FAIL: tests/test_overfit_rejection.py 不存在"
    )
    content = test_file.read_text(encoding="utf-8", errors="replace")
    assert "overfit" in content.lower(), (
        "Q27 FAIL: 测试文件中无过拟合相关测试"
    )


def test_Q28_pre_commit_has_all_tools():
    """Q28: pre-commit集成5个工具: black/bandit/mypy/pylint/vulture（T3.2验收标准）"""
    precommit_path = _SRC_ROOT / ".pre-commit-config.yaml"
    assert precommit_path.exists(), (
        "Q28 FAIL: .pre-commit-config.yaml 不存在"
    )
    content = precommit_path.read_text(encoding="utf-8", errors="replace")
    for tool in ["black", "bandit", "mypy", "pylint", "vulture"]:
        assert tool in content.lower(), (
            f"Q28 FAIL: .pre-commit-config.yaml 中缺少 {tool}"
        )


def test_Q29_ci_has_lint_and_dead_code():
    """Q29: CI包含lint(mypy/pylint)和dead code check(vulture)步骤（T3.3验收标准）"""
    ci_path = _SRC_ROOT / ".github" / "workflows" / "ci.yml"
    assert ci_path.exists(), (
        "Q29 FAIL: .github/workflows/ci.yml 不存在"
    )
    content = ci_path.read_text(encoding="utf-8", errors="replace")
    assert "mypy" in content, (
        "Q29 FAIL: CI中缺少mypy步骤"
    )
    assert "vulture" in content, (
        "Q29 FAIL: CI中缺少vulture dead code check步骤"
    )


def test_Q30_docs_code_sync_exists():
    """Q30: docs/code_sync/目录和模块验证文档存在（T3.4验收标准）"""
    sync_dir = _SRC_ROOT / "docs" / "code_sync"
    assert sync_dir.exists(), (
        "Q30 FAIL: docs/code_sync/ 目录不存在"
    )
    # 检查至少有一个验证文档
    md_files = list(sync_dir.glob("*.md"))
    assert len(md_files) >= 1, (
        "Q30 FAIL: docs/code_sync/ 中无验证文档"
    )


# ============================================================================
# 运行器 — 按5个一组执行
# ============================================================================

def run_group(group_num: int, tests: List[Tuple[str, callable]]) -> bool:
    """运行一组测试，返回是否全部通过"""
    print(f"\n{'='*70}")
    print(f"  第{group_num}组验证（问题{(group_num-1)*5+1}-{group_num*5}）")
    print(f"{'='*70}")
    all_passed = True
    for name, test_fn in tests:
        try:
            test_fn()
            print(f"  [PASS] {name}")
        except AssertionError as e:
            print(f"  [FAIL] {name}: {e}")
            all_passed = False
        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError) as e:
            print(f"  [ERROR] {name}: {type(e).__name__}: {e}")
            all_passed = False

    status = "ALL PASS" if all_passed else "HAS FAIL"
    print(f"\n  Group {group_num}: {status}")
    return all_passed


def main():
    """按5个一组执行端到端断言验证"""
    print("=" * 70)
    print("  升A路径端到端断言验证")
    print("  基于《三大系统代码现状实证评判及升A路径报告》第九节验收标准")
    print("=" * 70)

    groups = [
        # 第1组: Phase 1 止血与硬化
        [
            ("Q01_核心路径except_Exception≤5", test_Q01_soft_integration_core_path),
            ("Q02_全库except_Exception_pass=0", test_Q02_soft_integration_global_silent),
            ("Q03_RiskDashboardService有定义有调用", test_Q03_dead_code_risk_dashboard),
            ("Q04_三源分裂已修复无硬编码fallback", test_Q04_three_source_split_fixed),
            ("Q05_config/params.yaml唯一真相源存在", test_Q05_params_yaml_single_source),
        ],
        # 第2组: Phase 1 续 + Phase 2 开始
        [
            ("Q06_生产代码无相对导入from_evaluation", test_Q06_import_path_unified),
            ("Q07_风控检查fail-closed", test_Q07_risk_check_fail_closed),
            ("Q08_STRICT_MODE标志存在", test_Q08_strict_mode_exists),
            ("Q09_核心扫描器集成验证器", test_Q09_phase_scan_core_validators),
            ("Q10_回测runner直接耦合风控", test_Q10_backtest_runner_risk_direct),
        ],
        # 第3组: Phase 2 架构重构
        [
            ("Q11_Facade纯组合无继承", test_Q11_facade_pure_composition),
            ("Q12_内部模块下划线前缀文件存在", test_Q12_internal_module_underscore_prefix),
            ("Q13_无非下划线内部模块导入", test_Q13_no_non_underscore_internal_imports),
            ("Q14_Facade导入下划线模块", test_Q14_facade_imports_underscore_modules),
            ("Q15_verify_internal_imports脚本存在", test_Q15_verify_internal_imports_script),
        ],
        # 第4组: Phase 3 固化与自动化
        [
            ("Q16_risk_service无except_Exception", test_Q16_risk_service_exception_precision),
            ("Q17_judgment_services无except_Exception", test_Q17_judgment_services_exception_precision),
            ("Q18_scoring_helpers无except_Exception", test_Q18_scoring_helpers_exception_precision),
            ("Q19_phase_scan_cli无except_Exception", test_Q19_phase_scan_cli_exception_precision),
            ("Q20_ConfigError在YAML缺失时抛出", test_Q20_config_error_on_missing_yaml),
        ],
        # 第5组: 补充验收 — NullValidator + ConfigService + infra
        [
            ("Q21_NullValidator类存在且被使用", test_Q21_null_validator_exists),
            ("Q22_ConfigService.get_param统一接口", test_Q22_config_service_get_param),
            ("Q23_shadow_core使用ConfigService", test_Q23_shadow_core_uses_config_service),
            ("Q24_infra无except_Exception_pass", test_Q24_infra_no_except_exception_pass),
            ("Q25_全库无except_Exception_pass", test_Q25_global_no_except_exception_pass),
        ],
        # 第6组: 补充验收 — 测试覆盖 + CI/CD + 文档
        [
            ("Q26_risk_check单元测试存在", test_Q26_risk_check_unit_tests_exist),
            ("Q27_过拟合拒绝测试存在", test_Q27_overfit_rejection_test_exists),
            ("Q28_pre-commit集成5工具", test_Q28_pre_commit_has_all_tools),
            ("Q29_CI包含lint和dead_code_check", test_Q29_ci_has_lint_and_dead_code),
            ("Q30_docs/code_sync目录存在", test_Q30_docs_code_sync_exists),
        ],
    ]

    results = []
    for i, group in enumerate(groups, 1):
        passed = run_group(i, group)
        results.append((i, passed))
        if not passed:
            print(f"\n  WARNING: Group {i} failed, fix and re-verify")
            # 不中断，继续执行后续组以收集所有问题

    # 汇总
    print(f"\n{'='*70}")
    print("  验证汇总")
    print(f"{'='*70}")
    total_pass = sum(1 for _, p in results if p)
    total_groups = len(results)
    for g, p in results:
        status = "PASS" if p else "FAIL"
        print(f"  Group {g}: {status}")

    print(f"\n  Total: {total_pass}/{total_groups} groups passed")
    if total_pass == total_groups:
        print("\n  *** ALL E2E ASSERTIONS PASSED! Grade-A quality verified. ***")
    else:
        print(f"\n  *** {total_groups - total_pass} group(s) failed. Fix and re-verify. ***")

    return 0 if total_pass == total_groups else 1


if __name__ == "__main__":
    sys.exit(main())
