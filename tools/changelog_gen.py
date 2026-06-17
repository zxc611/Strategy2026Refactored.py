#!/usr/bin/env python
"""changelog_gen.py — 从 git 历史生成 CHANGELOG.md。

用法:
    python tools/changelog_gen.py
    python tools/changelog_gen.py --since 2025-01-01
    python tools/changelog_gen.py --output CHANGELOG.md
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── 路径设置 ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ── 常规提交类型映射 ──────────────────────────────────────
CONVENTIONAL_TYPES = {
    "feat": "Features",
    "fix": "Bug Fixes",
    "refactor": "Refactoring",
    "perf": "Performance",
    "docs": "Documentation",
    "test": "Tests",
    "chore": "Chores",
    "build": "Build",
    "ci": "CI",
    "style": "Style",
}

# 标题排序权重
_SECTION_ORDER = [
    "Features",
    "Bug Fixes",
    "Refactoring",
    "Performance",
    "Documentation",
    "Tests",
    "Build",
    "CI",
    "Style",
    "Chores",
    "Other",
]


def _get_version() -> str:
    """从 strategy_judgment/__init__.py 读取 __version__。"""
    init_path = os.path.join(BASE_DIR, "strategy_judgment", "__init__.py")
    if not os.path.isfile(init_path):
        return "0.0.0"
    try:
        with open(init_path, "r", encoding="utf-8") as f:
            for line in f:
                m = re.match(r'^__version__\s*=\s*["\']([^"\']+)["\']', line)
                if m:
                    return m.group(1)
    except OSError:
        pass
    return "0.0.0"


def _run_git(args: List[str]) -> Optional[str]:
    """执行 git 命令并返回 stdout，失败返回 None。"""
    try:
        result = subprocess.run(
            ["git"] + args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=BASE_DIR,
            timeout=30,
        )
        if result.returncode == 0 and result.stdout is not None:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    return None


def _parse_commit(line: str) -> Optional[Tuple[str, str, str]]:
    """解析一行 git log 输出，返回 (type, scope_or_empty, subject)。

    支持的格式:
        type(scope): subject
        type: subject
    """
    line = line.strip()
    if not line:
        return None
    m = re.match(r"^([a-zA-Z]+)(?:\(([^)]*)\))?\s*:\s*(.+)$", line)
    if not m:
        return None
    return m.group(1).lower(), m.group(2) or "", m.group(3).strip()


def get_commits(since: Optional[str] = None) -> List[Dict]:
    """从 git log 获取提交记录。"""
    git_args = ["log", "--pretty=format:%s"]
    if since:
        git_args.append(f"--since={since}")
    output = _run_git(git_args)
    if not output:
        return []

    commits: List[Dict] = []
    for line in output.splitlines():
        parsed = _parse_commit(line)
        if parsed:
            ctype, scope, subject = parsed
            section = CONVENTIONAL_TYPES.get(ctype, "Other")
            commits.append(
                {
                    "type": ctype,
                    "scope": scope,
                    "subject": subject,
                    "section": section,
                    "raw": line.strip(),
                }
            )
        else:
            # 非 conventional commit，归入 Other
            commits.append(
                {
                    "type": "",
                    "scope": "",
                    "subject": line.strip(),
                    "section": "Other",
                    "raw": line.strip(),
                }
            )
    return commits


def generate_changelog(commits: List[Dict], version: str) -> str:
    """将提交记录渲染为 CHANGELOG Markdown。"""
    today = datetime.now().strftime("%Y-%m-%d")
    lines: List[str] = [
        "# Changelog",
        "",
        f"## {version} ({today})",
        "",
    ]

    if not commits:
        lines.append("No conventional commits found in git history.")
        lines.append("")
        lines.append("### Features")
        lines.append("")
        lines.append("### Bug Fixes")
        lines.append("")
        return "\n".join(lines)

    # 按类型分组
    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for c in commits:
        grouped[c["section"]].append(c)

    # 按预定义顺序输出
    for section in _SECTION_ORDER:
        items = grouped.get(section)
        if not items:
            continue
        lines.append(f"### {section}")
        lines.append("")
        for item in items:
            scope = f"**{item['scope']}**: " if item["scope"] else ""
            lines.append(f"- {scope}{item['subject']}")
        lines.append("")

    return "\n".join(lines)


def generate_template(version: str) -> str:
    """当 git 不可用时，生成基础模板。"""
    today = datetime.now().strftime("%Y-%m-%d")
    return "\n".join(
        [
            "# Changelog",
            "",
            f"## {version} ({today})",
            "",
            "### Features",
            "",
            "### Bug Fixes",
            "",
            "### Refactoring",
            "",
        ]
    )


# ── CLI ───────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate CHANGELOG.md from git history."
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Start date for git log (e.g. 2025-01-01)",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(BASE_DIR, "CHANGELOG.md"),
        help="Output file path (default: CHANGELOG.md)",
    )
    args = parser.parse_args()

    version = _get_version()
    print(f"[INFO] Version from strategy_judgment: {version}")

    # 尝试获取 git 提交
    commits = get_commits(since=args.since)
    if commits:
        print(f"[INFO] Found {len(commits)} commits in git history.")
        content = generate_changelog(commits, version)
    else:
        print("[WARN] No commits found or git not available. Generating template.")
        content = generate_template(version)

    # 写入文件
    output_path = args.output
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"[DONE] Changelog written to: {output_path}")


if __name__ == "__main__":
    main()
