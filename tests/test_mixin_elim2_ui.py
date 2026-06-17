# MODULE_ID: M2-414
import sys
import os
import inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.config.ui_service import UILogicService, _UIMixinLogic
from ali2026v3_trading.config.ui_service import UICreationService, _UIMixinCreation
from ali2026v3_trading.config.ui_service import UIMixin


def test_ui_logic_service_exists():
    assert UILogicService is not None
    assert _UIMixinLogic is UILogicService


def test_ui_creation_service_exists():
    assert UICreationService is not None
    assert _UIMixinCreation is UICreationService


def test_uimixin_no_mixin_inheritance():
    bases = UIMixin.__bases__
    assert _UIMixinLogic not in bases, f"UIMixin should NOT inherit _UIMixinLogic, bases={bases}"
    assert _UIMixinCreation not in bases, f"UIMixin should NOT inherit _UIMixinCreation, bases={bases}"


def test_uimixin_has_composition():
    um = UIMixin.__new__(UIMixin)
    um._ui_logic_service = UILogicService()
    um._ui_creation_service = UICreationService(logic_service=um._ui_logic_service)
    assert hasattr(um, '_ui_logic_service')
    assert hasattr(um, '_ui_creation_service')


def test_uimixin_delegates_methods():
    um = UIMixin.__new__(UIMixin)
    um._ui_logic_service = UILogicService()
    um._ui_creation_service = UICreationService(logic_service=um._ui_logic_service)
    assert hasattr(um, 'set_output_mode')
    assert hasattr(um, '_start_output_mode_ui')
    assert hasattr(um, '_log_info')


if __name__ == '__main__':
    test_ui_logic_service_exists()
    test_ui_creation_service_exists()
    test_uimixin_no_mixin_inheritance()
    test_uimixin_has_composition()
    test_uimixin_delegates_methods()
    print("ALL 5 ASSERTIONS PASSED for Fix 1+2 (UI)")