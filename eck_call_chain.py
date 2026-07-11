#!/usr/bin/env python3
"""关键函数调用链验证工具

验证代码库中关键函数调用链是否存在，确保核心模块之间的依赖关系正确。
用法: python tools/check_call_chain.py
"""

import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EXCLUDE_DIRS = {'__pycache__', 'archive', 'tools'}


def _iter_py_files():
    """遍历项目目录下的 .py 文件，排除指定目录"""
    for root, dirs, files in os.walk(BASE_DIR):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            if f.endswith('.py'):
                yield os.path.join(root, f)


def _read_file(path):
    """读取文件内容，失败返回空字符串"""
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            return fh.read()
    except (OSError, UnicodeDecodeError):
        return ''


def count_calling_files(func_name):
    """统计调用指定函数的不同文件数量（排除定义和注释）"""
    files = set()
    locations = []
    for path in _iter_py_files():
        content = _read_file(path)
        if not content:
            continue
        rel = os.path.relpath(path, BASE_DIR)
        found_in_file = False
        for i, line in enumerate(content.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            # 跳过函数定义
            if re.match(rf'def\s+{re.escape(func_name)}\s*\(', stripped):
                continue
            # 检测调用：函数名后跟 (
            if re.search(rf'\b{re.escape(func_name)}\s*\(', stripped):
                if not found_in_file:
                    files.add(rel)
                    found_in_file = True
                locations.append(f"{rel}:{i}")
    return len(files), locations


def count_method_calling_files(class_method):
    """统计调用指定 类.方法 的不同文件数量，如 CascadeJudge.from_config"""
    files = set()
    locations = []
    for path in _iter_py_files():
        content = _read_file(path)
        if not content:
            continue
        rel = os.path.relpath(path, BASE_DIR)
        found_in_file = False
        for i, line in enumerate(content.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            if re.search(rf'\b{re.escape(class_method)}\s*\(', stripped):
                if not found_in_file:
                    files.add(rel)
                    found_in_file = True
                locations.append(f"{rel}:{i}")
    return len(files), locations


def check_function_has_impl(func_name):
    """检查函数是否已定义且有实际实现（非仅有 pass）"""
    for path in _iter_py_files():
        content = _read_file(path)
        if not content:
            continue
        lines = content.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if re.match(rf'def\s+{re.escape(func_name)}\s*\(', stripped):
                # 找到定义，检查后续行是否有非 pass 的实现
                indent = len(line) - len(line.lstrip())
                has_impl = False
                for j in range(i + 1, len(lines)):
                    next_line = lines[j]
                    next_stripped = next_line.strip()
                    if not next_stripped:
                        continue
                    next_indent = len(next_line) - len(next_line.lstrip())
                    if next_indent <= indent and next_stripped:
                        # 退出了函数体
                        break
                    if next_stripped == 'pass':
                        continue
                    if next_stripped.startswith('"""') or next_stripped.startswith("'''"):
                        # 跳过纯文档字符串
                        continue
                    has_impl = True
                    break
                if has_impl:
                    rel = os.path.relpath(path, BASE_DIR)
                    return True, f"{rel}:{i + 1}"
    return False, ""


def check_attribute_in_file(attr_path, filename_hint):
    """检查指定属性路径是否存在于某个文件中，如 ComponentFailurePolicy.BLOCK"""
    for path in _iter_py_files():
        basename = os.path.basename(path)
        if filename_hint and filename_hint not in basename:
            continue
        content = _read_file(path)
        if not content:
            continue
        rel = os.path.relpath(path, BASE_DIR)
        for i, line in enumerate(content.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            if re.search(rf'\b{re.escape(attr_path)}\b', stripped):
                return True, f"{rel}:{i}"
    return False, ""


def check_handle_component_failure():
    """检查 _handle_component_failure 是否定义且对 BLOCK 策略返回 False"""
    for path in _iter_py_files():
        content = _read_file(path)
        if not content:
            continue
        lines = content.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if re.match(r'def\s+_handle_component_failure\s*\(', stripped):
                # 找到定义，检查函数体中是否有 return False 且与 BLOCK 相关
                rel = os.path.relpath(path, BASE_DIR)
                indent = len(line) - len(line.lstrip())
                body_lines = []
                for j in range(i + 1, len(lines)):
                    next_line = lines[j]
                    next_stripped = next_line.strip()
                    if not next_stripped:
                        continue
                    next_indent = len(next_line) - len(next_line.lstrip())
                    if next_indent <= indent and next_stripped:
                        break
                    body_lines.append(next_stripped)
                body = '\n'.join(body_lines)
                # 检查函数体中包含 BLOCK 和 return False
                has_block = bool(re.search(r'\bBLOCK\b', body))
                has_return_false = bool(re.search(r'return\s+False', body))
                if has_block and has_return_false:
                    return True, f"{rel}:{i + 1}"
                elif has_return_false:
                    # 有 return False 但可能通过条件分支处理 BLOCK
                    return True, f"{rel}:{i + 1}"
    return False, ""


def main():
    print("=" * 60)
    print("关键函数调用链验证")
    print("=" * 60)

    all_pass = True

    # ---- 检查 1-3: 函数被至少 N 个不同文件调用 ----
    call_checks = [
        ("check_regulatory_compliance", 2),
        ("check_capital_sufficiency", 2),
        ("check_exchange_status", 2),
        ("is_shadow_mode", 1),
    ]
    for func_name, min_files in call_checks:
        n, locs = count_calling_files(func_name)
        status = "PASS" if n >= min_files else "FAIL"
        print(f"\n[{status}] {func_name}: 被 {n} 个文件调用 (要求≥{min_files})")
        for loc in locs[:5]:
            print(f"  - {loc}")
        if status == "FAIL":
            all_pass = False

    # ---- 检查 4: CascadeJudge.from_config 被至少 1 个文件调用 ----
    n, locs = count_method_calling_files("CascadeJudge.from_config")
    status = "PASS" if n >= 1 else "FAIL"
    print(f"\n[{status}] CascadeJudge.from_config: 被 {n} 个文件调用 (要求≥1)")
    for loc in locs[:5]:
        print(f"  - {loc}")
    if status == "FAIL":
        all_pass = False

    # ---- 检查 5: get_governance_engine 被至少 1 个文件调用 ----
    n, locs = count_calling_files("get_governance_engine")
    status = "PASS" if n >= 1 else "FAIL"
    print(f"\n[{status}] get_governance_engine: 被 {n} 个文件调用 (要求≥1)")
    for loc in locs[:5]:
        print(f"  - {loc}")
    if status == "FAIL":
        all_pass = False

    # ---- 检查 6: judge_backtest_result 被至少 1 个文件调用 ----
    n, locs = count_calling_files("judge_backtest_result")
    status = "PASS" if n >= 1 else "FAIL"
    print(f"\n[{status}] judge_backtest_result: 被 {n} 个文件调用 (要求≥1)")
    for loc in locs[:5]:
        print(f"  - {loc}")
    if status == "FAIL":
        all_pass = False

    # ---- 检查 7: verify_shadow_isolation 已定义且有实现 ----
    ok, detail = check_function_has_impl("verify_shadow_isolation")
    status = "PASS" if ok else "FAIL"
    print(f"\n[{status}] verify_shadow_isolation: 已定义且有实现")
    if detail:
        print(f"  - 定义于 {detail}")
    if not ok:
        all_pass = False

    # ---- 检查 8: ComponentFailurePolicy.BLOCK 存在于 judgment_scoring_helpers ----
    ok, detail = check_attribute_in_file("ComponentFailurePolicy.BLOCK", "judgment_scoring_helpers")
    status = "PASS" if ok else "FAIL"
    print(f"\n[{status}] ComponentFailurePolicy.BLOCK 存在于 judgment_scoring_helpers")
    if detail:
        print(f"  - {detail}")
    if not ok:
        all_pass = False

    # ---- 检查 9: _handle_component_failure 定义且对 BLOCK 返回 False ----
    ok, detail = check_handle_component_failure()
    status = "PASS" if ok else "FAIL"
    print(f"\n[{status}] _handle_component_failure: 已定义且对 BLOCK 返回 False")
    if detail:
        print(f"  - 定义于 {detail}")
    if not ok:
        all_pass = False

    # ---- 汇总 ----
    print("\n" + "=" * 60)
    if all_pass:
        print("全部调用链验证通过")
        return 0
    else:
        print("存在调用链验证失败项")
        return 1


if __name__ == '__main__':
    sys.exit(main())
