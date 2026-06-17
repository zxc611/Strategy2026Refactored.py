# MODULE_ID: M2-503
"""R1-1: 验证 _params_service_lock NameError 已修复"""
import threading
import pytest


def test_params_service_lock_defined():
    """_params_service_lock 必须在 _params_core 模块中定义，否则 reset_params_service/get_params_service 会 NameError"""
    from ali2026v3_trading.config._params_core import _params_service_lock
    assert isinstance(_params_service_lock, type(threading.Lock())), \
        "_params_service_lock 应为 threading.Lock 实例"


def test_reset_params_service_no_name_error():
    """reset_params_service() 调用不应抛出 NameError"""
    from ali2026v3_trading.config._params_core import reset_params_service
    try:
        reset_params_service()
    except NameError as e:
        pytest.fail(f"reset_params_service() 抛出 NameError: {e}")


def test_get_params_service_no_name_error():
    """get_params_service() 调用不应抛出 NameError"""
    from ali2026v3_trading.config._params_core import reset_params_service, get_params_service
    reset_params_service()
    try:
        svc = get_params_service()
        assert svc is not None
    except NameError as e:
        pytest.fail(f"get_params_service() 抛出 NameError: {e}")
    finally:
        reset_params_service()
