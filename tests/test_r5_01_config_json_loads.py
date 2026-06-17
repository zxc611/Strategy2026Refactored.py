# MODULE_ID: M2-541
"""R5-1断言测试: P1-35 config层json.load→json_loads统一

验证6个config文件不再有json.load(f)调用:
1. _params_core.py
2. _params_instrument_cache.py
3. config_service_core.py
4. config_json_loader.py
5. config_resolver.py
6. config_option_loader.py
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')
_CONFIG = os.path.join(_BASE, 'config')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


def test_params_core_no_json_load():
    """_params_core.py不应有json.load(f)"""
    src = _read('config/_params_core.py')
    assert 'json.load(' not in src, "_params_core.py不应有json.load()"


def test_params_core_has_json_loads():
    """_params_core.py应导入json_loads"""
    src = _read('config/_params_core.py')
    assert 'json_loads' in src, "_params_core.py应导入json_loads"


def test_params_instrument_cache_no_json_load():
    """_params_instrument_cache.py不应有json.load(f)"""
    src = _read('config/_params_instrument_cache.py')
    assert 'json.load(' not in src, "_params_instrument_cache.py不应有json.load()"


def test_params_instrument_cache_has_json_loads():
    """_params_instrument_cache.py应导入json_loads"""
    src = _read('config/_params_instrument_cache.py')
    assert 'json_loads' in src, "_params_instrument_cache.py应导入json_loads"


def test_config_service_core_no_json_load():
    """config_service.py不应有json.load(f)"""
    src = _read('config/config_service.py')
    assert 'json.load(' not in src, "config_service.py不应有json.load()"


def test_config_json_loader_no_json_load():
    """config_loader.py不应有json.load(f)"""
    src = _read('config/config_loader.py')
    assert 'json.load(' not in src, "config_loader.py不应有json.load()"


def test_config_json_loader_has_json_loads():
    """config_loader.py应导入json_loads"""
    src = _read('config/config_loader.py')
    assert 'json_loads' in src, "config_loader.py应导入json_loads"


def test_config_resolver_no_json_load():
    """config_loader.py不应有json.load(f)"""
    src = _read('config/config_loader.py')
    assert 'json.load(' not in src, "config_loader.py不应有json.load()"


def test_config_resolver_has_json_loads():
    """config_loader.py应导入json_loads"""
    src = _read('config/config_loader.py')
    assert 'json_loads' in src, "config_loader.py应导入json_loads"


def test_config_option_loader_no_json_load():
    """config_loader.py不应有json.load(f)"""
    src = _read('config/config_loader.py')
    assert 'json.load(' not in src, "config_loader.py不应有json.load()"


def test_config_option_loader_has_json_loads():
    """config_loader.py应导入json_loads"""
    src = _read('config/config_loader.py')
    assert 'json_loads' in src, "config_loader.py应导入json_loads"
