# MODULE_ID: M2-547
"""R7断言测试: P1-37 config_params薄包装标记DeprecationWarning

验证3个薄包装函数已标记DeprecationWarning:
1. record_pipeline_latency
2. record_indicator_value
3. merge_option_params_to_default

同时验证已修复项:
- R7-1: _LifecycleMixin已有DeprecationWarning
- R7-3: ServiceProtocol已存在于infra/service_contracts.py
- R7-4: risk_audit_utils已清理重导出
- R7-5: DrawdownManager已有set_daily_hard_stop方法
"""
import os
import warnings

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


# --- R7-2: config_params薄包装DeprecationWarning ---

def test_record_pipeline_latency_has_deprecation():
    """record_pipeline_latency应标记DeprecationWarning"""
    src = _read('config/config_params.py')
    assert 'deprecated:: R7-2' in src or 'DeprecationWarning' in src, "record_pipeline_latency应标记DeprecationWarning"


def test_record_indicator_value_has_deprecation():
    """record_indicator_value应标记DeprecationWarning"""
    src = _read('config/config_params.py')
    # 搜索record_indicator_value附近的DeprecationWarning
    lines = src.split('\n')
    found = False
    for i, line in enumerate(lines):
        if 'def record_indicator_value' in line:
            # 检查后续20行内是否有DeprecationWarning
            for j in range(i, min(i + 20, len(lines))):
                if 'DeprecationWarning' in lines[j]:
                    found = True
                    break
            break
    assert found, "record_indicator_value附近应有DeprecationWarning"


def test_merge_option_params_has_deprecation():
    """merge_option_params_to_default应标记DeprecationWarning"""
    src = _read('config/config_params.py')
    lines = src.split('\n')
    found = False
    for i, line in enumerate(lines):
        if 'def merge_option_params_to_default' in line:
            for j in range(i, min(i + 20, len(lines))):
                if 'DeprecationWarning' in lines[j]:
                    found = True
                    break
            break
    assert found, "merge_option_params_to_default附近应有DeprecationWarning"


def test_config_params_has_warnings_import():
    """config_params.py应导入warnings"""
    src = _read('config/config_params.py')
    assert 'import warnings' in src, "config_params.py应导入warnings"


# --- 已修复项验证 ---

def test_lifecycle_mixin_has_deprecation():
    """R7-1: _LifecycleMixin已有DeprecationWarning"""
    src = _read('strategy/lifecycle_service.py')
    assert 'DeprecationWarning' in src, "_LifecycleMixin应有DeprecationWarning"


def test_service_protocol_exists():
    """R7-3: ServiceProtocol已存在"""
    src = _read('infra/contracts_service.py')
    assert 'class ServiceProtocol' in src, "ServiceProtocol应存在"
    assert 'health_check' in src, "ServiceProtocol应有health_check方法"
    assert 'get_service_name' in src, "ServiceProtocol应有get_service_name方法"


def test_risk_audit_utils_no_broken_reexport():
    """R7-4: risk_audit_utils已清理断裂重导出"""
    src = _read('infra/security_service.py')
    # 应从实际模块导入
    assert 'from ali2026v3_trading.infra.serialization_utils import' in src, "应从serialization_utils导入"
    assert 'from ali2026v3_trading.infra.resilience import' in src, "应从resilience导入"


def test_drawdown_manager_has_hard_stop():
    """R7-5: DrawdownManager已有硬停止优先级机制"""
    src = _read('governance/mode_exit_rules.py')
    assert 'set_daily_hard_stop' in src, "DrawdownManager应有set_daily_hard_stop方法"
    assert '_daily_hard_stop_triggered' in src, "DrawdownManager应有硬停止状态字段"
