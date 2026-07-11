#!/usr/bin/env python3
"""检测混入运行时模块目录的工具脚本"""
import os
import sys
import fnmatch

EXCLUDE_DIRS = {"__pycache__", "archive", "tools", "tests"}
TOOL_PATTERNS = ["_gen_*.py", "_split_*.py", "_do_count*.py", "_do_verify*.py"]


def is_tool_script(filename):
    """判断文件名是否匹配工具脚本模式"""
    return any(fnmatch.fnmatch(filename, pat) for pat in TOOL_PATTERNS)


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    found = []

    for dirpath, dirnames, filenames in os.walk(project_root):
        # 排除指定目录
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]

        for filename in filenames:
            if is_tool_script(filename):
                filepath = os.path.join(dirpath, filename)
                rel = os.path.relpath(filepath, project_root)
                found.append(rel)

    if found:
        print(f"发现 {len(found)} 个混入运行时目录的工具脚本：")
        for f in found:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("未发现混入运行时目录的工具脚本")
        sys.exit(0)


if __name__ == "__main__":
    main()
