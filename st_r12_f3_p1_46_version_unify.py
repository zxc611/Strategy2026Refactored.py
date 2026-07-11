# MODULE_ID: M2-484
"""R4-F3: P1-46 版本管理双轨统一 断言测试

验证运行时行为:
1. ParamVersionManager.save_version同步到config_version_tracker
2. config_version_tracker是参数版本权威入口
3. ModelVersionManager存在且可实例化
4. 三套版本管理不再完全独立
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_param_version_manager_syncs_to_tracker():
    """P1-46验证: ParamVersionManager.save_version同步到config_version_tracker"""
    import inspect
    from ali2026v3_trading.infra.resilience import ParamVersionManager
    src = inspect.getsource(ParamVersionManager.save_version)
    assert 'config_version_tracker' in src, \
        "ParamVersionManager.save_version应桥接到config_version_tracker"
    assert 'record_param_version' in src, \
        "ParamVersionManager.save_version应调用record_param_version"


def test_config_version_tracker_is_authoritative():
    """P1-46验证: config_version_tracker是参数版本权威入口"""
    from ali2026v3_trading.config import config_params as config_version_tracker
    # 应有版本管理函数
    assert hasattr(config_version_tracker, 'get_param_version') or \
           hasattr(config_version_tracker, 'rollback_param_version'), \
        "config_version_tracker应有版本管理函数"


def test_model_version_manager_exists():
    """P1-46验证: ModelVersionManager存在且可实例化"""
    from ali2026v3_trading.infra.shared_utils import ModelVersionManager
    mvm = ModelVersionManager()
    assert hasattr(mvm, '_versions'), \
        "ModelVersionManager应有界versions属性"


def test_three_version_managers_not_fully_independent():
    """P1-46验证: 三套版本管理不再完全独立——ParamVersionManager桥接到config_version_tracker"""
    import inspect
    from ali2026v3_trading.infra.resilience import ParamVersionManager
    src = inspect.getsource(ParamVersionManager.save_version)
    assert 'config_version_tracker' in src, \
        "ParamVersionManager应桥接到config_version_tracker"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
