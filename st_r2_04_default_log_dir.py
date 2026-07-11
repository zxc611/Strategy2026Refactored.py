# MODULE_ID: M2-518
"""R2-4: 验证 DEFAULT_LOG_DIR 常量定义及3个关键模块已使用"""
import pytest


def test_default_log_dir_defined():
    """DEFAULT_LOG_DIR 应在 config._constants 中定义"""
    from ali2026v3_trading.config._params_canary_env import DEFAULT_LOG_DIR
    assert isinstance(DEFAULT_LOG_DIR, str)
    assert len(DEFAULT_LOG_DIR) > 0


def test_default_log_dir_env_override():
    """DEFAULT_LOG_DIR 应可通过环境变量覆盖"""
    import os
    os.environ['DEFAULT_LOG_DIR'] = 'test_logs'
    # 需要重新导入模块以获取新值
    import importlib
    import ali2026v3_trading.config._params_canary_env as constants_mod
    importlib.reload(constants_mod)
    assert constants_mod.DEFAULT_LOG_DIR == 'test_logs'
    # 清理
    del os.environ['DEFAULT_LOG_DIR']
    importlib.reload(constants_mod)


def test_scheduler_service_uses_default_log_dir():
    """scheduler_service 应使用 DEFAULT_LOG_DIR 而非硬编码 'logs'"""
    import inspect
    from ali2026v3_trading.infra.scheduler_service import SchedulerService
    source = inspect.getsource(SchedulerService)
    # 不应有硬编码的 'logs' 字符串在 os.path.join 中
    assert "os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')" not in source, \
        "scheduler_service 不应硬编码 'logs'"
    assert 'DEFAULT_LOG_DIR' in source, \
        "scheduler_service 应使用 DEFAULT_LOG_DIR"


def test_position_service_uses_default_log_dir():
    """position_service 应使用 DEFAULT_LOG_DIR"""
    import inspect
    from ali2026v3_trading.position.position_service import PositionService
    source = inspect.getsource(PositionService.__init__)
    assert 'DEFAULT_LOG_DIR' in source, \
        "PositionService.__init__ 应使用 DEFAULT_LOG_DIR"
