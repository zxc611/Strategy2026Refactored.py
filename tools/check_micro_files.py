#!/usr/bin/env python3
"""检测微型文件：≤50行且仅含1个函数/类的.py文件（应合并）"""
import os
import sys
import ast

EXCLUDE_DIRS = {"__pycache__", "archive", "tools", "tests"}
MAX_LINES = 50
MAX_DEFS = 1  # class + function 总数上限


def is_micro_file(filepath):
    """判断文件是否为微型文件"""
    try:
        with open(filepath, encoding="utf-8") as f:
            lines = f.readlines()
    except (OSError, UnicodeDecodeError):
        return False

    if len(lines) > MAX_LINES:
        return False

    try:
        tree = ast.parse("".join(lines))
    except SyntaxError:
        return False

    class_count = sum(1 for node in ast.walk(tree) if isinstance(node, ast.ClassDef))
    func_count = sum(1 for node in ast.walk(tree) if isinstance(node, ast.FunctionDef))

    return (class_count + func_count) <= MAX_DEFS


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    micro_files = []

    for dirpath, dirnames, filenames in os.walk(project_root):
        # 排除指定目录
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]

        for filename in filenames:
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(dirpath, filename)
            if is_micro_file(filepath):
                rel = os.path.relpath(filepath, project_root)
                micro_files.append(rel)

    if micro_files:
        print(f"发现 {len(micro_files)} 个微型文件（≤{MAX_LINES}行且≤{MAX_DEFS}个定义）：")
        for f in micro_files:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print(f"未发现微型文件（≤{MAX_LINES}行且≤{MAX_DEFS}个定义）")
        sys.exit(0)


if __name__ == "__main__":
    main()
