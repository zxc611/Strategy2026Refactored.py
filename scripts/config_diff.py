#!/usr/bin/env python3
"""
R17-P2-CFG-04: 配置diff工具

比较两个配置文件(或当前配置与默认配置)之间的差异。
用法: python scripts/config_diff.py [file1] [file2]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_json_config(path: str) -> Dict[str, Any]:
    """加载JSON配置文件"""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def diff_configs(config_a: Dict[str, Any], config_b: Dict[str, Any],
                 label_a: str = "A", label_b: str = "B") -> List[Tuple[str, Any, Any]]:
    """比较两个配置字典，返回差异列表 [(key, value_a, value_b), ...]"""
    all_keys = sorted(set(config_a.keys()) | set(config_b.keys()))
    diffs = []
    for key in all_keys:
        val_a = config_a.get(key, _MISSING)
        val_b = config_b.get(key, _MISSING)
        if val_a != val_b:
            diffs.append((key, val_a, val_b))
    return diffs


class _MissingSentinel:
    """标记键不存在"""
    pass


_MISSING = _MissingSentinel()


def format_diff(diffs: List[Tuple[str, Any, Any]],
                label_a: str = "A", label_b: str = "B") -> str:
    """格式化差异输出"""
    if not diffs:
        return "No differences found."
    lines = [f"{'Key':<40} {label_a:<20} {label_b:<20}"]
    lines.append("-" * 80)
    for key, val_a, val_b in diffs:
        va = "<missing>" if isinstance(val_a, _MissingSentinel) else repr(val_a)
        vb = "<missing>" if isinstance(val_b, _MissingSentinel) else repr(val_b)
        lines.append(f"{key:<40} {va:<20} {vb:<20}")
    return "\n".join(lines)


def main():
    if len(sys.argv) < 3:
        print("Usage: python config_diff.py <config_file_a> <config_file_b>")
        sys.exit(1)

    path_a, path_b = sys.argv[1], sys.argv[2]
    config_a = load_json_config(path_a)
    config_b = load_json_config(path_b)

    diffs = diff_configs(config_a, config_b, label_a=Path(path_a).name, label_b=Path(path_b).name)
    print(format_diff(diffs, label_a=Path(path_a).name, label_b=Path(path_b).name))


if __name__ == "__main__":
    main()
