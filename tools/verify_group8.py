#!/usr/bin/env python3
"""第8组端到端断言验证 — 第四阶段：文档与监控 + 参数修复

验证项：
1. CI配置更新（verify_pipeline/check_call_chain/param_audit_report步骤）
2. 代码文档自动同步（doc_sync.py + changelog_gen.py + 版本绑定）
3. 运行时质量监控（CodeQualityMetrics类存在于health.py）
4. 审计自动化（audit_runner.py存在且可编译）
5. 参数不一致修复（default_slippage_bps=3.0, rate_limit_window_sec=120）
"""

import os
import sys
import ast
import json
import py_compile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

errors = []

# ============================================================
# 验证1: CI配置更新
# ============================================================
print("=" * 60)
print("验证1: CI配置更新")
print("=" * 60)

ci_path = os.path.join(PROJECT_DIR, '.github', 'workflows', 'ci.yml')
if os.path.exists(ci_path):
    with open(ci_path, 'r', encoding='utf-8') as f:
        ci_content = f.read()

    required_steps = ['verify_pipeline', 'check_call_chain', 'param_audit_report']
    for step in required_steps:
        if step in ci_content:
            print(f"  [PASS] CI包含 {step} 步骤")
        else:
            errors.append(f"CI缺少 {step} 步骤")
            print(f"  [FAIL] CI缺少 {step} 步骤")
else:
    errors.append("ci.yml不存在")
    print("  [FAIL] ci.yml不存在")

# ============================================================
# 验证2: 代码文档自动同步
# ============================================================
print()
print("=" * 60)
print("验证2: 代码文档自动同步")
print("=" * 60)

# 2a: doc_sync.py
doc_sync_path = os.path.join(PROJECT_DIR, 'tools', 'doc_sync.py')
if os.path.exists(doc_sync_path):
    try:
        py_compile.compile(doc_sync_path, doraise=True)
        print("  [PASS] doc_sync.py 存在且可编译")
    except py_compile.PyCompileError as e:
        errors.append(f"doc_sync.py 编译失败: {e}")
        print(f"  [FAIL] doc_sync.py 编译失败: {e}")
else:
    errors.append("doc_sync.py 不存在")
    print("  [FAIL] doc_sync.py 不存在")

# 2b: changelog_gen.py
changelog_gen_path = os.path.join(PROJECT_DIR, 'tools', 'changelog_gen.py')
if os.path.exists(changelog_gen_path):
    try:
        py_compile.compile(changelog_gen_path, doraise=True)
        print("  [PASS] changelog_gen.py 存在且可编译")
    except py_compile.PyCompileError as e:
        errors.append(f"changelog_gen.py 编译失败: {e}")
        print(f"  [FAIL] changelog_gen.py 编译失败: {e}")
else:
    errors.append("changelog_gen.py 不存在")
    print("  [FAIL] changelog_gen.py 不存在")

# 2c: 版本绑定 (__init__.py with __version__ + get_version)
pkg_init_path = os.path.join(PROJECT_DIR, '__init__.py')
if os.path.exists(pkg_init_path):
    with open(pkg_init_path, 'r', encoding='utf-8') as f:
        init_content = f.read()
    has_version = '__version__' in init_content
    has_get_version = 'get_version' in init_content
    if has_version and has_get_version:
        print("  [PASS] __init__.py 包含 __version__ 和 get_version()")
    else:
        if not has_version:
            errors.append("__init__.py 缺少 __version__")
        if not has_get_version:
            errors.append("__init__.py 缺少 get_version()")
        print(f"  [FAIL] __init__.py 版本绑定不完整 (version={has_version}, get_version={has_get_version})")
else:
    errors.append("__init__.py (项目根) 不存在")
    print("  [FAIL] __init__.py (项目根) 不存在")

# 2d: docs/API_REFERENCE.md 已生成
api_ref_path = os.path.join(PROJECT_DIR, 'docs', 'API_REFERENCE.md')
if os.path.exists(api_ref_path):
    with open(api_ref_path, 'r', encoding='utf-8') as f:
        ref_content = f.read()
    if len(ref_content) > 100:
        print(f"  [PASS] docs/API_REFERENCE.md 已生成 ({len(ref_content)} 字符)")
    else:
        errors.append("API_REFERENCE.md 内容过短")
        print("  [FAIL] API_REFERENCE.md 内容过短")
else:
    errors.append("docs/API_REFERENCE.md 不存在")
    print("  [FAIL] docs/API_REFERENCE.md 不存在")

# 2e: CHANGELOG.md 已生成
changelog_path = os.path.join(PROJECT_DIR, 'CHANGELOG.md')
if os.path.exists(changelog_path):
    with open(changelog_path, 'r', encoding='utf-8') as f:
        cl_content = f.read()
    if len(cl_content) > 50:
        print(f"  [PASS] CHANGELOG.md 已生成 ({len(cl_content)} 字符)")
    else:
        errors.append("CHANGELOG.md 内容过短")
        print("  [FAIL] CHANGELOG.md 内容过短")
else:
    errors.append("CHANGELOG.md 不存在")
    print("  [FAIL] CHANGELOG.md 不存在")

# ============================================================
# 验证3: 运行时质量监控 (CodeQualityMetrics)
# ============================================================
print()
print("=" * 60)
print("验证3: 运行时质量监控 (CodeQualityMetrics)")
print("=" * 60)

health_path = os.path.join(PROJECT_DIR, 'infra', 'health.py')
if os.path.exists(health_path):
    with open(health_path, 'r', encoding='utf-8') as f:
        health_content = f.read()

    has_class = 'class CodeQualityMetrics' in health_content
    has_collect = 'def collect(self)' in health_content
    has_dead_imports = '_check_dead_imports' in health_content
    has_whitelist = '_check_whitelist' in health_content
    has_failure_rate = '_get_component_failure_rate' in health_content
    has_suppressed = '_count_suppressed_errors' in health_content

    all_present = all([has_class, has_collect, has_dead_imports, has_whitelist, has_failure_rate, has_suppressed])
    if all_present:
        print("  [PASS] CodeQualityMetrics 类完整（collect + 4个指标方法）")
    else:
        missing = []
        if not has_class: missing.append("class CodeQualityMetrics")
        if not has_collect: missing.append("collect()")
        if not has_dead_imports: missing.append("_check_dead_imports")
        if not has_whitelist: missing.append("_check_whitelist")
        if not has_failure_rate: missing.append("_get_component_failure_rate")
        if not has_suppressed: missing.append("_count_suppressed_errors")
        errors.append(f"CodeQualityMetrics 不完整: 缺少 {missing}")
        print(f"  [FAIL] CodeQualityMetrics 不完整: 缺少 {missing}")
else:
    errors.append("infra/health.py 不存在")
    print("  [FAIL] infra/health.py 不存在")

# ============================================================
# 验证4: 审计自动化 (audit_runner.py)
# ============================================================
print()
print("=" * 60)
print("验证4: 审计自动化 (audit_runner.py)")
print("=" * 60)

audit_runner_path = os.path.join(PROJECT_DIR, 'tools', 'audit_runner.py')
if os.path.exists(audit_runner_path):
    try:
        py_compile.compile(audit_runner_path, doraise=True)
        with open(audit_runner_path, 'r', encoding='utf-8') as f:
            ar_content = f.read()

        has_dimensions = 'AUDIT_DIMENSIONS' in ar_content
        has_run_audit = 'def run_audit' in ar_content
        has_main = 'def main' in ar_content
        has_severity = 'severity' in ar_content

        if all([has_dimensions, has_run_audit, has_main, has_severity]):
            print("  [PASS] audit_runner.py 完整（维度定义 + run_audit + main + severity过滤）")
        else:
            missing = []
            if not has_dimensions: missing.append("AUDIT_DIMENSIONS")
            if not has_run_audit: missing.append("run_audit()")
            if not has_main: missing.append("main()")
            if not has_severity: missing.append("severity过滤")
            errors.append(f"audit_runner.py 不完整: 缺少 {missing}")
            print(f"  [FAIL] audit_runner.py 不完整: 缺少 {missing}")
    except py_compile.PyCompileError as e:
        errors.append(f"audit_runner.py 编译失败: {e}")
        print(f"  [FAIL] audit_runner.py 编译失败: {e}")
else:
    errors.append("audit_runner.py 不存在")
    print("  [FAIL] audit_runner.py 不存在")

# ============================================================
# 验证5: 参数不一致修复
# ============================================================
print()
print("=" * 60)
print("验证5: 参数不一致修复")
print("=" * 60)

# 5a: default_slippage_bps = 3.0 in YAML
yaml_path = os.path.join(PROJECT_DIR, 'config', 'params.yaml')
if os.path.exists(yaml_path):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        yaml_content = f.read()

    # Find default_slippage_bps section
    if 'default_slippage_bps' in yaml_content:
        lines = yaml_content.split('\n')
        found_value = None
        for i, line in enumerate(lines):
            if 'default_slippage_bps' in line and 'default:' not in line:
                # Look ahead for default value
                for j in range(i+1, min(i+5, len(lines))):
                    if 'default:' in lines[j]:
                        val = lines[j].split('default:')[1].strip().strip('"').strip("'")
                        found_value = val
                        break
                break
        if found_value == '3.0':
            print(f"  [PASS] params.yaml default_slippage_bps default=3.0")
        else:
            errors.append(f"params.yaml default_slippage_bps default={found_value} (期望3.0)")
            print(f"  [FAIL] params.yaml default_slippage_bps default={found_value} (期望3.0)")
    else:
        errors.append("params.yaml 中未找到 default_slippage_bps")
        print("  [FAIL] params.yaml 中未找到 default_slippage_bps")

# 5b: default_slippage_bps = 3.0 in params_default.json
json_path = os.path.join(PROJECT_DIR, 'config', 'params_default.json')
if os.path.exists(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        json_content = f.read()

    try:
        data = json.loads(json_content)
        # Navigate to find default_slippage_bps
        found_slippage = None
        if isinstance(data, dict):
            for section_key, section_val in data.items():
                if isinstance(section_val, dict):
                    for param_key, param_val in section_val.items():
                        if param_key == 'default_slippage_bps' and isinstance(param_val, dict):
                            found_slippage = param_val.get('default')
                        elif isinstance(param_val, dict):
                            for k2, v2 in param_val.items():
                                if k2 == 'default_slippage_bps' and isinstance(v2, dict):
                                    found_slippage = v2.get('default')

        if found_slippage is not None:
            if found_slippage == 3.0:
                print(f"  [PASS] params_default.json default_slippage_bps default=3.0")
            else:
                errors.append(f"params_default.json default_slippage_bps default={found_slippage} (期望3.0)")
                print(f"  [FAIL] params_default.json default_slippage_bps default={found_slippage} (期望3.0)")
        else:
            # Try direct search in JSON text
            import re
            match = re.search(r'"default_slippage_bps".*?"default"\s*:\s*([\d.]+)', json_content)
            if match:
                val = float(match.group(1))
                if val == 3.0:
                    print(f"  [PASS] params_default.json default_slippage_bps default=3.0")
                else:
                    errors.append(f"params_default.json default_slippage_bps default={val} (期望3.0)")
                    print(f"  [FAIL] params_default.json default_slippage_bps default={val} (期望3.0)")
            else:
                errors.append("params_default.json 中未找到 default_slippage_bps")
                print("  [FAIL] params_default.json 中未找到 default_slippage_bps")
    except json.JSONDecodeError as e:
        errors.append(f"params_default.json 解析失败: {e}")
        print(f"  [FAIL] params_default.json 解析失败: {e}")

# 5c: rate_limit_window_sec = 120 in YAML
if os.path.exists(yaml_path):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        yaml_content = f.read()

    import re
    match = re.search(r'rate_limit_window_sec:\s*(\d+)', yaml_content)
    if match:
        val = int(match.group(1))
        if val == 120:
            print(f"  [PASS] params.yaml rate_limit_window_sec=120")
        else:
            errors.append(f"params.yaml rate_limit_window_sec={val} (期望120)")
            print(f"  [FAIL] params.yaml rate_limit_window_sec={val} (期望120)")
    else:
        errors.append("params.yaml 中未找到 rate_limit_window_sec")
        print("  [FAIL] params.yaml 中未找到 rate_limit_window_sec")

# 5d: rate_limit_window_sec fallback = 120.0 in risk_config_provider.py
rcp_path = os.path.join(PROJECT_DIR, 'risk', 'risk_config_provider.py')
if os.path.exists(rcp_path):
    with open(rcp_path, 'r', encoding='utf-8') as f:
        rcp_content = f.read()

    match = re.search(r'rate_limit_window_sec[\"\'\s,)]*([\d.]+)', rcp_content)
    if match:
        val = float(match.group(1))
        if val == 120.0:
            print(f"  [PASS] risk_config_provider.py rate_limit_window_sec fallback=120.0")
        else:
            errors.append(f"risk_config_provider.py rate_limit_window_sec fallback={val} (期望120.0)")
            print(f"  [FAIL] risk_config_provider.py rate_limit_window_sec fallback={val} (期望120.0)")
    else:
        errors.append("risk_config_provider.py 中未找到 rate_limit_window_sec")
        print("  [FAIL] risk_config_provider.py 中未找到 rate_limit_window_sec")

# ============================================================
# 汇总
# ============================================================
print()
print("=" * 60)
if errors:
    print(f"第8组验证: FAIL — {len(errors)} 项错误")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print("第8组验证: ALL PASS")
    sys.exit(0)
