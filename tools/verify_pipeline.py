#!/usr/bin/env python3
"""自动化验收流水线 — 提交前/CI执行"""

import subprocess
import sys
import os
import argparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# BASE_DIR is ali2026v3_trading/tools/.. = ali2026v3_trading/
PROJECT_DIR = BASE_DIR
TOOLS_DIR = os.path.join(BASE_DIR, "tools")

# 排除的目录
EXCLUDE_DIRS = frozenset({
    "__pycache__", "archive", "tools", "tests",
    "_verify_logs", "_wal_orders", ".git", ".ci",
})


def _collect_py_files():
    """收集项目中所有 .py 文件（排除 __pycache__、archive、tools 目录）"""
    py_files = []
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            if f.endswith(".py"):
                py_files.append(os.path.join(root, f))
    return py_files


def _check_py_compile():
    """语法检查: 编译所有 .py 文件"""
    errors = []
    py_files = _collect_py_files()
    if not py_files:
        print("    未找到 .py 文件")
        return errors

    for path in py_files:
        try:
            with open(path, "rb") as fh:
                compile(fh.read(), path, "exec")
        except SyntaxError as e:
            rel = os.path.relpath(path, PROJECT_DIR)
            errors.append(f"{rel}: {e}")
    return errors


def _check_import_chain():
    """导入链完整性检查"""
    errors = []
    critical_modules = [
        "ali2026v3_trading.infra.shared_utils",
        "ali2026v3_trading.infra.resilience",
        "ali2026v3_trading.infra.health",
        "ali2026v3_trading.infra.event_bus",
        "ali2026v3_trading.config.config_service",
        "ali2026v3_trading.lifecycle.lifecycle_state_machine",
        "ali2026v3_trading.strategy_judgment.judgment_scoring_helpers",
        "ali2026v3_trading.evaluation.cascade_judge",
        "ali2026v3_trading.governance.governance_engine",
    ]
    # 确保父目录在 sys.path 中以支持包导入
    parent_dir = os.path.dirname(BASE_DIR)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    for mod in critical_modules:
        try:
            __import__(mod)
        except ImportError as e:
            errors.append(f"{mod}: {e}")
    return errors


def _script_exists(script_name):
    """检查 tools/ 目录下指定脚本是否存在"""
    return os.path.isfile(os.path.join(TOOLS_DIR, script_name))


# 外部脚本检查项: (名称, 脚本文件名, 命令模板, 是否阻塞)
EXTERNAL_CHECKS = [
    ("call_chain", "check_call_chain.py", "python tools/check_call_chain.py", True),
    ("dead_code", "check_dead_code.py", "python tools/check_dead_code.py", False),
    ("micro_files", "check_micro_files.py", "python tools/check_micro_files.py", False),
    ("tool_scripts", "check_tool_scripts.py", "python tools/check_tool_scripts.py", True),
    ("api_whitelist", "check_api_whitelist.py", "python tools/check_api_whitelist.py", True),
    ("pytest_fast", None, "python -m pytest tests/ -x --timeout=60", False),
]

# 内置检查项: (名称, 检查函数, 是否阻塞)
INLINE_CHECKS = [
    ("py_compile", _check_py_compile, True),
    ("import_chain", _check_import_chain, True),
]


def _run_external_check(name, script_name, cmd, blocking):
    """运行外部脚本检查，脚本不存在时跳过并发出警告"""
    if script_name is not None and not _script_exists(script_name):
        print(f"  [SKIP] 脚本 {script_name} 不存在，跳过 {name} 检查")
        return "skip", []

    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, cwd=PROJECT_DIR
        )
        if result.returncode != 0:
            details = result.stderr.strip() or result.stdout.strip()
            return "fail", [details] if details else [f"退出码 {result.returncode}"]
        return "pass", []
    except Exception as e:
        return "fail", [str(e)]


def run_pipeline(quick=False):
    """执行验收流水线

    Args:
        quick: 为 True 时仅运行 py_compile 和 import_chain 检查
    """
    print("=" * 60)
    print("自动化验收流水线" + (" [快速模式]" if quick else ""))
    print("=" * 60)

    failures = []
    warnings = []
    skipped = []

    # --- 内置检查 ---
    for name, check_fn, blocking in INLINE_CHECKS:
        print(f"\n[{name}] 检查中...")
        try:
            errors = check_fn()
            if errors:
                if blocking:
                    failures.append(name)
                    print(f"  [FAIL] {len(errors)} 项问题:")
                else:
                    warnings.append(name)
                    print(f"  [WARN] {len(errors)} 项问题(非阻塞):")
                for e in errors[:10]:
                    print(f"    - {e}")
                if len(errors) > 10:
                    print(f"    ... 还有 {len(errors) - 10} 项")
            else:
                print("  [PASS]")
        except Exception as e:
            failures.append(name)
            print(f"  [FAIL] 检查异常: {e}")

    # --- 外部脚本检查 (快速模式下跳过) ---
    if not quick:
        for name, script_name, cmd, blocking in EXTERNAL_CHECKS:
            print(f"\n[{name}] 检查中...")
            status, errors = _run_external_check(name, script_name, cmd, blocking)
            if status == "skip":
                skipped.append(name)
            elif status == "fail":
                if blocking:
                    failures.append(name)
                    print(f"  [FAIL] {len(errors)} 项问题:")
                else:
                    warnings.append(name)
                    print(f"  [WARN] {len(errors)} 项问题(非阻塞):")
                for e in errors[:10]:
                    print(f"    - {e}")
            else:
                print("  [PASS]")

    # --- 汇总 ---
    print("\n" + "=" * 60)
    if skipped:
        print(f"已跳过: {skipped}")
    if failures:
        print(f"{len(failures)} 项检查失败: {failures}")
        print("提交被阻止")
        return 1
    elif warnings:
        print(f"{len(warnings)} 项警告(非阻塞): {warnings}")
        print("验收通过(有警告)")
        return 0
    else:
        print("全部检查通过")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="自动化验收流水线 — 提交前/CI执行"
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="快速模式: 仅运行 py_compile 和 import_chain 检查",
    )
    args = parser.parse_args()
    sys.exit(run_pipeline(quick=args.quick))


if __name__ == "__main__":
    main()
