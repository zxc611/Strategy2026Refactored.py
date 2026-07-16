#!/usr/bin/env python3
"""参数三源一致性审计工具 — YAML vs Code默认值 vs 手册"""

import os
import re
import sys
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR


def extract_yaml_params():
    """从YAML文件提取参数清单"""
    params = {}
    yaml_dirs = [
        os.path.join(PROJECT_DIR, 'config'),
        os.path.join(PROJECT_DIR, 'param_pool'),
    ]
    for yaml_dir in yaml_dirs:
        if not os.path.exists(yaml_dir):
            continue
        for f in os.listdir(yaml_dir):
            if f.endswith('.yaml') or f.endswith('.yml'):
                path = os.path.join(yaml_dir, f)
                try:
                    with open(path, 'r', encoding='utf-8') as fh:
                        # P2-30修复: 使用yaml_safe_load统一入口，消除直接yaml.safe_load
                        from infra.serialization_utils import yaml_safe_load
                        data = yaml_safe_load(fh)
                        if isinstance(data, dict):
                            for k, v in _flatten_dict(data, prefix=''):
                                params[k] = {'value': v, 'source': f}
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    print(f"[WARN] Failed to parse {f}: {e}")
    return params


def _flatten_dict(d, prefix=''):
    """递归展平字典"""
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            yield from _flatten_dict(v, full_key)
        else:
            yield full_key, v


def extract_code_defaults():
    """从代码中提取DEFAULT_*常量和getattr默认值"""
    defaults = {}
    py_dirs = [
        os.path.join(PROJECT_DIR, 'config'),
        os.path.join(PROJECT_DIR, 'param_pool'),
        os.path.join(PROJECT_DIR, 'risk'),
    ]
    for py_dir in py_dirs:
        if not os.path.exists(py_dir):
            continue
        for root, dirs, files in os.walk(py_dir):
            for f in files:
                if not f.endswith('.py'):
                    continue
                path = os.path.join(root, f)
                try:
                    with open(path, 'r', encoding='utf-8') as fh:
                        content = fh.read()
                        # Find DEFAULT_* = patterns
                        for m in re.finditer(r'([A-Z_]+DEFAULT[A-Z_]*)\s*=\s*([^\n]+)', content):
                            name, value = m.group(1), m.group(2).strip()
                            defaults[name] = {'value': value, 'source': f}
                        # Find getattr(..., DEFAULT_*) patterns
                        for m in re.finditer(r'getattr\([^,]+,\s*[\'"]([a-z_]+)[\'"]\s*,\s*([^\)]+)\)', content):
                            name, value = m.group(1), m.group(2).strip()
                            defaults[name] = {'value': value, 'source': f}
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
    return defaults


def compare_sources(yaml_params, code_defaults):
    """比较YAML和代码默认值"""
    inconsistencies = []
    yaml_keys = set(yaml_params.keys())
    code_keys = set(code_defaults.keys())

    # 仅在YAML中
    yaml_only = yaml_keys - code_keys
    for k in sorted(yaml_only):
        inconsistencies.append(('YAML_ONLY', k, yaml_params[k]['value'], yaml_params[k]['source']))

    # 仅在代码中
    code_only = code_keys - yaml_keys
    for k in sorted(code_only):
        inconsistencies.append(('CODE_ONLY', k, code_defaults[k]['value'], code_defaults[k]['source']))

    return inconsistencies


def main():
    print("=" * 60)
    print("参数三源一致性审计报告")
    print("=" * 60)

    yaml_params = extract_yaml_params()
    code_defaults = extract_code_defaults()

    print(f"\nYAML参数数: {len(yaml_params)}")
    print(f"代码默认值数: {len(code_defaults)}")

    inconsistencies = compare_sources(yaml_params, code_defaults)

    if inconsistencies:
        print(f"\n发现 {len(inconsistencies)} 项不一致:")
        for type_, key, value, source in inconsistencies[:50]:
            print(f"  [{type_}] {key} = {value} (来源: {source})")
        if len(inconsistencies) > 50:
            print(f"  ... 还有 {len(inconsistencies) - 50} 项")
    else:
        print("\n无不一致项")

    # 返回退出码
    sys.exit(1 if inconsistencies else 0)


if __name__ == '__main__':
    main()
