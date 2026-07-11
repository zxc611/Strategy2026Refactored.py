# MODULE_ID: M2-526
"""R3-2: 验证 order/position 模块 json.load 已替换为 json_loads"""
import inspect
import pytest


def test_order_wal_no_json_load():
    """order_wal_state_service 不应有 json.load( 调用"""
    import ali2026v3_trading.order.order_wal_state_service as mod
    source = open(mod.__file__, 'r', encoding='utf-8').read()
    code_lines = [l for l in source.splitlines() if not l.strip().startswith('#')]
    code_text = '\n'.join(code_lines)
    assert 'json.load(' not in code_text, \
        "order_wal_state_service 不应有 json.load() 调用"


def test_order_persistence_no_json_load():
    """order_persistence 不应有 json.load( 调用"""
    import ali2026v3_trading.order.order_persistence as mod
    source = open(mod.__file__, 'r', encoding='utf-8').read()
    code_lines = [l for l in source.splitlines() if not l.strip().startswith('#')]
    code_text = '\n'.join(code_lines)
    assert 'json.load(' not in code_text, \
        "order_persistence 不应有 json.load() 调用"


def test_position_persistence_no_json_load():
    """position_persistence 不应有 json.load( 调用"""
    import ali2026v3_trading.position.position_persistence as mod
    source = open(mod.__file__, 'r', encoding='utf-8').read()
    code_lines = [l for l in source.splitlines() if not l.strip().startswith('#')]
    code_text = '\n'.join(code_lines)
    assert 'json.load(' not in code_text, \
        "position_persistence 不应有 json.load() 调用"


def test_json_loads_behavior():
    """json_loads 应正确反序列化含NaN的JSON"""
    from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads
    data = {"value": float('nan'), "name": "test"}
    serialized = json_dumps(data)
    restored = json_loads(serialized)
    assert restored["name"] == "test"
    import math
    assert math.isnan(restored["value"])
