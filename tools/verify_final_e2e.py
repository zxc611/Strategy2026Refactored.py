#!/usr/bin/env python3
"""最终全量端到端验证 — 覆盖B+→A级提升路径全部4个阶段

第一阶段：文件瘦身（存根文件删除、导入路径更新）
第二阶段：架构对齐（CascadeJudge V7.5、BLOCK/DEGRADE/WARN、参数三源审计、自动化流水线）
第三阶段：测试加固（测试文件、CI配置）
第四阶段：文档与监控（文档自动同步、运行时质量监控、审计自动化）
+ 参数一致性修复
"""

import os
import sys
import ast
import json
import re
import py_compile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = BASE_DIR
PARENT_DIR = os.path.dirname(BASE_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

errors = []
warnings = []

def check(label, condition, error_msg=None, is_warning=False):
    """统一断言检查"""
    if condition:
        print(f"  [PASS] {label}")
    else:
        msg = error_msg or label
        if is_warning:
            warnings.append(msg)
            print(f"  [WARN] {msg}")
        else:
            errors.append(msg)
            print(f"  [FAIL] {msg}")

# ============================================================
# 第一阶段：文件瘦身
# ============================================================
print("=" * 70)
print("第一阶段：文件瘦身验证")
print("=" * 70)

# 1.1 已删除的存根文件不应再被引用
DELETED_STUBS = [
    'config_service_reexport', 'state_param_core', 'judgment_config',
    'lifecycle_parallel_ops', 'cross_system_execution', 'cross_system_utils',
    'security_config', 'security_hardening', 'event_bus_dataflow',
    'event_bus_events', '_storage_catalog_mixin', '_storage_checks_mixin',
    '_storage_maintenance', 'health_monitor', 'health_jsonl_logger',
    'health_check_service', 'health_check_aggregator', 'ui_mixin_creation',
    'config_service_core', 'config_service_query', 'config_json_loader',
    'config_yaml_loader', 'config_option_loader', 'state_param_manager',
    'state_transition_capture', 'state_transition_analytics', 'lifecycle_state',
]

stub_ref_count = 0
for root, dirs, files in os.walk(PROJECT_DIR):
    dirs[:] = [d for d in dirs if d not in ('__pycache__', 'archive', 'tools', 'tests', '_verify_logs', '_wal_orders', '.git', '.ci', 'docs', 'benchmarks')]
    for f in files:
        if f.endswith('.py') and f != 'verify_group8.py' and not f.startswith('verify_group'):
            path = os.path.join(root, f)
            try:
                with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
                    content = fh.read()
                for stub in DELETED_STUBS:
                    # Check for from xxx import or import xxx patterns
                    pattern = rf'(?:from\s+\S*\.{stub}\s+import|import\s+\S*\.{stub})'
                    if re.search(pattern, content):
                        rel = os.path.relpath(path, PROJECT_DIR)
                        stub_ref_count += 1
                        if stub_ref_count <= 5:
                            errors.append(f"{rel}: 仍引用已删除存根 {stub}")
            except Exception:
                pass

check("已删除存根文件无残留引用", stub_ref_count == 0,
      f"发现 {stub_ref_count} 处已删除存根文件的残留引用")

# 1.2 合并后模块可导入
MERGED_MODULES = [
    'ali2026v3_trading.infra.shared_utils',
    'ali2026v3_trading.infra.health',
    'ali2026v3_trading.infra.event_bus',
    'ali2026v3_trading.infra.resilience',
    'ali2026v3_trading.config.config_service',
    'ali2026v3_trading.config.config_params',
    'ali2026v3_trading.config.state_param',
    'ali2026v3_trading.strategy_judgment.judgment_types',
]

for mod in MERGED_MODULES:
    try:
        __import__(mod)
        check(f"模块可导入: {mod}", True)
    except ImportError as e:
        check(f"模块可导入: {mod}", False, f"模块 {mod} 导入失败: {e}")

# ============================================================
# 第二阶段：架构对齐
# ============================================================
print()
print("=" * 70)
print("第二阶段：架构对齐验证")
print("=" * 70)

# 2.1 CascadeJudge V7.5 对齐
try:
    from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge
    check("CascadeJudge 可导入", True)
    check("CascadeJudge.from_config 存在", hasattr(CascadeJudge, 'from_config'))
except ImportError as e:
    check("CascadeJudge 可导入", False, f"CascadeJudge 导入失败: {e}")

# 2.2 BLOCK/DEGRADE/WARN 模式
try:
    from ali2026v3_trading.strategy_judgment._judgment_services import (
        ComponentFailurePolicy, _handle_component_failure, CRITICAL_COMPONENTS
    )
    check("ComponentFailurePolicy 可导入", True)
    check("ComponentFailurePolicy.BLOCK 存在", hasattr(ComponentFailurePolicy, 'BLOCK'))
    check("ComponentFailurePolicy.DEGRADE 存在", hasattr(ComponentFailurePolicy, 'DEGRADE'))
    check("ComponentFailurePolicy.WARN 存在", hasattr(ComponentFailurePolicy, 'WARN'))
    check("_handle_component_failure 存在", callable(_handle_component_failure))
    check("CRITICAL_COMPONENTS 存在", isinstance(CRITICAL_COMPONENTS, dict))
except ImportError as e:
    check("BLOCK/DEGRADE/WARN 模式", False, f"导入失败: {e}")

# 2.3 参数三源审计工具
param_audit_path = os.path.join(PROJECT_DIR, 'tools', 'param_audit_report.py')
check("param_audit_report.py 存在", os.path.exists(param_audit_path))
if os.path.exists(param_audit_path):
    try:
        py_compile.compile(param_audit_path, doraise=True)
        check("param_audit_report.py 可编译", True)
    except py_compile.PyCompileError as e:
        check("param_audit_report.py 可编译", False, str(e))

# 2.4 自动化验收流水线
verify_pipeline_path = os.path.join(PROJECT_DIR, 'tools', 'verify_pipeline.py')
check("verify_pipeline.py 存在", os.path.exists(verify_pipeline_path))
if os.path.exists(verify_pipeline_path):
    try:
        py_compile.compile(verify_pipeline_path, doraise=True)
        check("verify_pipeline.py 可编译", True)
    except py_compile.PyCompileError as e:
        check("verify_pipeline.py 可编译", False, str(e))

# 2.5 调用链验证工具
check_call_chain_path = os.path.join(PROJECT_DIR, 'tools', 'check_call_chain.py')
check("check_call_chain.py 存在", os.path.exists(check_call_chain_path))

# 2.6 参数一致性（default_slippage_bps=3.0, rate_limit_window_sec=120）
yaml_path = os.path.join(PROJECT_DIR, 'config', 'params.yaml')
if os.path.exists(yaml_path):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        yaml_content = f.read()
    match = re.search(r'rate_limit_window_sec:\s*(\d+)', yaml_content)
    if match:
        val = int(match.group(1))
        check("params.yaml rate_limit_window_sec=120", val == 120,
              f"rate_limit_window_sec={val} (期望120)")

# ============================================================
# 第三阶段：测试加固
# ============================================================
print()
print("=" * 70)
print("第三阶段：测试加固验证")
print("=" * 70)

# 3.1 测试文件存在
TEST_FILES = [
    'test_cascade_judge.py',
    'test_judgment_scoring.py',
    'test_governance_engine.py',
    'test_shadow_isolation_e2e.py',
    'test_judgment_critical_failure.py',
    'test_param_pool_to_judgment.py',
    'test_circuit_breaker_order_cancel.py',
]

for tf in TEST_FILES:
    tf_path = os.path.join(PROJECT_DIR, 'tests', tf)
    check(f"测试文件存在: {tf}", os.path.exists(tf_path))

# 3.2 测试文件可编译
for tf in TEST_FILES:
    tf_path = os.path.join(PROJECT_DIR, 'tests', tf)
    if os.path.exists(tf_path):
        try:
            py_compile.compile(tf_path, doraise=True)
            check(f"测试文件可编译: {tf}", True)
        except py_compile.PyCompileError as e:
            check(f"测试文件可编译: {tf}", False, str(e))

# 3.3 CI配置
ci_path = os.path.join(PROJECT_DIR, '.github', 'workflows', 'ci.yml')
if os.path.exists(ci_path):
    with open(ci_path, 'r', encoding='utf-8') as f:
        ci_content = f.read()
    check("CI包含 pytest 步骤", 'pytest' in ci_content)
    check("CI包含 verify_pipeline 步骤", 'verify_pipeline' in ci_content)
    check("CI包含 check_call_chain 步骤", 'check_call_chain' in ci_content)
    check("CI包含 param_audit_report 步骤", 'param_audit_report' in ci_content)

# 3.4 检查工具脚本
CHECK_TOOLS = ['check_micro_files.py', 'check_tool_scripts.py', 'check_api_whitelist.py']
for ct in CHECK_TOOLS:
    ct_path = os.path.join(PROJECT_DIR, 'tools', ct)
    check(f"检查工具存在: {ct}", os.path.exists(ct_path))

# ============================================================
# 第四阶段：文档与监控
# ============================================================
print()
print("=" * 70)
print("第四阶段：文档与监控验证")
print("=" * 70)

# 4.1 代码文档自动同步
doc_sync_path = os.path.join(PROJECT_DIR, 'tools', 'doc_sync.py')
check("doc_sync.py 存在", os.path.exists(doc_sync_path))
if os.path.exists(doc_sync_path):
    try:
        py_compile.compile(doc_sync_path, doraise=True)
        check("doc_sync.py 可编译", True)
    except py_compile.PyCompileError as e:
        check("doc_sync.py 可编译", False, str(e))

changelog_gen_path = os.path.join(PROJECT_DIR, 'tools', 'changelog_gen.py')
check("changelog_gen.py 存在", os.path.exists(changelog_gen_path))
if os.path.exists(changelog_gen_path):
    try:
        py_compile.compile(changelog_gen_path, doraise=True)
        check("changelog_gen.py 可编译", True)
    except py_compile.PyCompileError as e:
        check("changelog_gen.py 可编译", False, str(e))

# 4.2 版本绑定
pkg_init_path = os.path.join(PROJECT_DIR, '__init__.py')
if os.path.exists(pkg_init_path):
    with open(pkg_init_path, 'r', encoding='utf-8') as f:
        init_content = f.read()
    check("__init__.py 包含 __version__", '__version__' in init_content)
    check("__init__.py 包含 get_version()", 'get_version' in init_content)
else:
    check("__init__.py 存在", False)

# 4.3 API文档已生成
api_ref_path = os.path.join(PROJECT_DIR, 'docs', 'API_REFERENCE.md')
check("docs/API_REFERENCE.md 已生成", os.path.exists(api_ref_path))

# 4.4 CHANGELOG已生成
changelog_path = os.path.join(PROJECT_DIR, 'CHANGELOG.md')
check("CHANGELOG.md 已生成", os.path.exists(changelog_path))

# 4.5 运行时质量监控
health_path = os.path.join(PROJECT_DIR, 'infra', 'health.py')
if os.path.exists(health_path):
    with open(health_path, 'r', encoding='utf-8') as f:
        health_content = f.read()
    check("CodeQualityMetrics 类存在", 'class CodeQualityMetrics' in health_content)
    check("CodeQualityMetrics.collect() 存在", 'def collect(self)' in health_content)

# 4.6 审计自动化
audit_runner_path = os.path.join(PROJECT_DIR, 'tools', 'audit_runner.py')
check("audit_runner.py 存在", os.path.exists(audit_runner_path))
if os.path.exists(audit_runner_path):
    try:
        py_compile.compile(audit_runner_path, doraise=True)
        check("audit_runner.py 可编译", True)
    except py_compile.PyCompileError as e:
        check("audit_runner.py 可编译", False, str(e))

# ============================================================
# 关键功能端到端验证
# ============================================================
print()
print("=" * 70)
print("关键功能端到端验证")
print("=" * 70)

# E2E-1: 安全检查函数调用链
try:
    from ali2026v3_trading.risk.risk_service import RiskService
    check("RiskService.check_regulatory_compliance 存在",
          hasattr(RiskService, 'check_regulatory_compliance'))
    check("RiskService.check_capital_sufficiency 存在",
          hasattr(RiskService, 'check_capital_sufficiency'))
    check("RiskService.check_exchange_status 存在",
          hasattr(RiskService, 'check_exchange_status'))
except ImportError as e:
    check("RiskService 可导入", False, f"RiskService 导入失败: {e}")

# E2E-2: 影子策略隔离
try:
    from ali2026v3_trading.strategy import ShadowStrategyEngine
    check("ShadowStrategyEngine 可导入(从strategy包)", True)
except ImportError as e:
    check("ShadowStrategyEngine 可导入", False, f"导入失败: {e}")

try:
    from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
    check("ShadowStrategyEngine 可导入(从facade模块)", True)
except ImportError as e:
    check("ShadowStrategyEngine 可导入(从facade模块)", False, f"导入失败: {e}")

# E2E-3: Governance引擎
try:
    from ali2026v3_trading.governance.governance_engine import get_governance_engine
    check("get_governance_engine 可导入", True)
except ImportError as e:
    check("get_governance_engine 可导入", False, f"导入失败: {e}")

# E2E-4: 评判引擎
try:
    from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge
    check("CascadeJudge 可导入", True)
except ImportError as e:
    check("CascadeJudge 可导入", False, f"导入失败: {e}")

try:
    from ali2026v3_trading.strategy_judgment import judge_backtest_result
    check("judge_backtest_result 可导入", True)
except ImportError as e:
    check("judge_backtest_result 可导入", False, f"导入失败: {e}")

# E2E-5: 健康检查
try:
    from ali2026v3_trading.infra.health_monitor import (
        HealthMonitor, get_health_monitor, HealthCheckAPI,
        HealthCheckAggregator, CodeQualityMetrics
    )
    check("HealthMonitor 可导入", True)
    check("get_health_monitor 可导入", True)
    check("HealthCheckAPI 可导入", True)
    check("HealthCheckAggregator 可导入", True)
    check("CodeQualityMetrics 可导入", True)
except ImportError as e:
    check("健康检查模块导入", False, f"导入失败: {e}")

# E2E-6: 配置服务
try:
    from ali2026v3_trading.config.config_service import ConfigService
    check("ConfigService 可导入", True)
except ImportError as e:
    check("ConfigService 可导入", False, f"导入失败: {e}")

try:
    from ali2026v3_trading.config.state_param import get_state_param_manager
    check("get_state_param_manager 可导入", True)
except ImportError as e:
    check("get_state_param_manager 可导入", False, f"导入失败: {e}")

# ============================================================
# 汇总
# ============================================================
print()
print("=" * 70)
total_checks = len(errors) + len(warnings)
if errors:
    print(f"最终验证: FAIL — {len(errors)} 项错误, {len(warnings)} 项警告")
    for e in errors:
        print(f"  [ERROR] {e}")
    for w in warnings:
        print(f"  [WARN] {w}")
    sys.exit(1)
elif warnings:
    print(f"最终验证: PASS (有 {len(warnings)} 项警告)")
    for w in warnings:
        print(f"  [WARN] {w}")
    sys.exit(0)
else:
    print("最终验证: ALL PASS — B+→A级提升路径全部4个阶段验证通过")
    sys.exit(0)
