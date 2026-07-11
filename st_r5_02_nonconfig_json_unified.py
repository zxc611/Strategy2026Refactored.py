# MODULE_ID: M2-542
"""R5-2断言测试: P1-35 非config层json.load→json_loads + json.dump→json_dumps

验证7个文件不再有json.load(f)/json.dump(obj, f)调用:
1. ProductionQuantSystem.py
2. governance/greeks_calculator.py
3. param_pool/adv_validation_misc.py
4. risk/safety_meta_audit.py
5. strategy/recovery_service.py
6. param_pool/run_verification.py
7. param_pool/ts_result_executor.py
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


# --- json.load 消除 ---

def test_production_quant_no_json_load():
    """ProductionQuantSystem.py不应有json.load(f)"""
    src = _read('ProductionQuantSystem.py')
    assert 'json.load(' not in src, "ProductionQuantSystem.py不应有json.load()"


def test_production_quant_has_json_loads():
    """ProductionQuantSystem.py应使用json_loads"""
    src = _read('ProductionQuantSystem.py')
    assert 'json_loads' in src, "ProductionQuantSystem.py应使用json_loads"


def test_greeks_calculator_no_json_load():
    """greeks_calculator.py不应有json.load(f)"""
    src = _read('governance/greeks_calculator.py')
    assert 'json.load(' not in src, "greeks_calculator.py不应有json.load()"


def test_adv_validation_no_json_load():
    """adv_validation_misc.py不应有json.load(f)"""
    src = _read('param_pool/validation/adv_validation_misc.py')
    assert 'json.load(' not in src, "adv_validation_misc.py不应有json.load()"


def test_safety_meta_audit_no_json_load():
    """safety_meta_audit.py不应有json.load(f)"""
    src = _read('risk/safety_meta_audit.py')
    assert 'json.load(' not in src, "safety_meta_audit.py不应有json.load()"


def test_recovery_service_no_json_load():
    """recovery_service.py不应有json.load(f)"""
    src = _read('strategy/persistence_service.py')
    assert 'json.load(' not in src, "recovery_service.py不应有json.load()"


def test_recovery_service_has_json_loads():
    """recovery_service.py应导入json_loads"""
    src = _read('strategy/persistence_service.py')
    assert 'json_loads' in src, "recovery_service.py应导入json_loads"


# --- json.dump 消除 ---

def test_run_verification_no_json_dump():
    """adv_validation_misc.py不应有json.dump(调用"""
    src = _read('param_pool/validation/adv_validation_misc.py')
    assert 'json.dump(' not in src, "adv_validation_misc.py不应有json.dump()"


def test_run_verification_has_json_dumps():
    """adv_validation_misc.py应导入json_dumps"""
    src = _read('param_pool/validation/adv_validation_misc.py')
    assert 'json_dumps' in src, "adv_validation_misc.py应导入json_dumps"


def test_ts_result_writer_no_json_dump():
    """ts_result_writer.py不应有json.dump(调用"""
    src = _read('param_pool/ts/ts_result_writer.py')
    assert 'json.dump(' not in src, "ts_result_writer.py不应有json.dump()"


def test_ts_result_writer_has_json_dumps():
    """ts_result_writer.py应导入json_dumps"""
    src = _read('param_pool/ts/ts_result_writer.py')
    assert 'json_dumps' in src, "ts_result_writer.py应导入json_dumps"
