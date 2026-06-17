#!/usr/bin/env python3
"""死代码检测 — 检测定义但未被调用的函数"""
import os, re, sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR

def check():
    """检测可能未被使用的公开函数"""
    warnings = []
    # 收集所有定义的函数名
    defined = {}
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', 'archive', 'tests', '_verify_logs', '_wal_orders')]
        for f in files:
            if not f.endswith('.py'):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, 'r', encoding='utf-8') as fh:
                    for i, line in enumerate(fh, 1):
                        m = re.match(r'^def ([a-z_][a-z0-9_]*)\(', line)
                        if m:
                            fname = m.group(1)
                            if not fname.startswith('_'):  # 只检查公开函数
                                rel = os.path.relpath(path, PROJECT_DIR)
                                defined[fname] = rel
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

    # 检查每个公开函数是否被调用
    all_content = ""
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', 'archive', '_verify_logs', '_wal_orders')]
        for f in files:
            if not f.endswith('.py'):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, 'r', encoding='utf-8') as fh:
                    all_content += fh.read() + "\n"
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass

    for fname, source in sorted(defined.items()):
        # Count occurrences (definition + calls)
        count = len(re.findall(rf'\b{fname}\b\s*\(', all_content))
        if count <= 1:  # Only definition, no calls
            warnings.append(f"{source}: {fname} (仅定义，无调用)")

    return warnings

def main():
    warnings = check()
    if warnings:
        print(f"发现 {len(warnings)} 个可能未使用的公开函数:")
        for w in warnings[:30]:
            print(f"  - {w}")
        if len(warnings) > 30:
            print(f"  ... 还有 {len(warnings) - 30} 个")
        return 1
    print("无死代码")
    return 0

if __name__ == '__main__':
    sys.exit(main())
