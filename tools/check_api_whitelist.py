#!/usr/bin/env python3
"""验证关键包的 __init__.py 是否定义了 __all__ 且长度合理"""
import os
import sys
import ast

PACKAGES = [
    "strategy",
    "risk",
    "evaluation",
    "governance",
    "config",
    "infra",
    "lifecycle",
    "order",
    "position",
    "strategy_judgment",
]
MAX_ALL_LENGTH = 30


def check_init_has_all(init_path):
    """检查 __init__.py 是否定义了 __all__，并返回其长度（未定义返回 -1）"""
    try:
        with open(init_path, encoding="utf-8") as f:
            source = f.read()
    except (OSError, UnicodeDecodeError):
        return -1

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return -1

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        return len(node.value.elts)
                    return 0  # __all__ 定义但无法解析长度
    return -1


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    failures = []

    for pkg in PACKAGES:
        init_path = os.path.join(project_root, pkg, "__init__.py")
        rel = os.path.relpath(init_path, project_root)

        if not os.path.isfile(init_path):
            failures.append(f"{rel}: 文件不存在")
            continue

        all_len = check_init_has_all(init_path)

        if all_len == -1:
            failures.append(f"{rel}: 缺少 __all__ 定义")
        elif all_len > MAX_ALL_LENGTH:
            failures.append(f"{rel}: __all__ 有 {all_len} 项（超过上限 {MAX_ALL_LENGTH}）")

    if failures:
        print(f"发现 {len(failures)} 个问题：")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print(f"所有 {len(PACKAGES)} 个包的 __init__.py 均通过检查")
        sys.exit(0)


if __name__ == "__main__":
    main()
