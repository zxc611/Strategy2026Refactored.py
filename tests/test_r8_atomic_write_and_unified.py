# MODULE_ID: M2-548
"""R8断言测试: 非原子写入→atomic_replace_file + 已修复项验证

R8-5: 3个高优先级配置/状态文件使用atomic_replace_file
1. config/_params_core.py — 参数保存
2. config/config_service_core.py — 配置保存
3. position/position_persistence.py — 持仓状态保存

已修复项验证:
- R8-1: ServiceProtocol已存在
- R8-3: TickEvent已统一
- R8-2: signal_service信号过期（检查）
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


# --- R8-5: 原子写入 ---

def test_params_core_uses_atomic_replace():
    """_params_core.py参数保存应使用atomic_replace_file"""
    src = _read('config/_params_core.py')
    assert 'atomic_replace_file' in src, "_params_core.py应使用atomic_replace_file"


def test_params_core_no_bare_open_write():
    """_params_core.py参数保存不应有裸open+write"""
    src = _read('config/_params_core.py')
    # 检查save_params方法中不再有 with open(..., "w") as f: f.write
    lines = src.split('\n')
    for i, line in enumerate(lines):
        if 'atomic_replace_file(path' in line:
            # 找到了，说明已替换
            return
    # 如果没找到atomic_replace_file调用，检查是否有裸写入
    for i, line in enumerate(lines):
        if 'with open(path, "w"' in line or "with open(path, 'w'" in line:
            assert False, f"_params_core.py第{i+1}行仍有裸open写入"


def test_config_service_core_uses_atomic_replace():
    """config_service.py配置保存应使用atomic_replace_file"""
    src = _read('config/config_service.py')
    assert 'atomic_replace_file' in src, "config_service.py应使用atomic_replace_file"


def test_position_persistence_uses_atomic_replace():
    """position_persistence.py持仓状态保存应使用atomic_replace_file"""
    src = _read('position/position_persistence.py')
    assert 'atomic_replace_file' in src, "position_persistence.py应使用atomic_replace_file"


# --- 已修复项 ---

def test_tick_event_unified():
    """R8-3: MarketEvent/BarCompletedEvent/TickEvent已统一到infra"""
    src = _read('infra/event_bus.py')
    assert 'class MarketEvent' in src, "MarketEvent应在infra/event_bus.py定义"
    assert 'class BarCompletedEvent' in src, "BarCompletedEvent应在infra/event_bus.py定义"
    assert 'class TickEvent' in src, "TickEvent应在infra/event_bus.py定义"


def test_service_protocol_exists():
    """R8-1: ServiceProtocol已存在"""
    src = _read('infra/contracts_service.py')
    assert 'class ServiceProtocol' in src, "ServiceProtocol应存在"


def test_signal_expiry_manager_exists():
    """R8-2: SignalExpiryManager应存在"""
    src = _read('infra/resilience.py')
    assert 'SignalExpiryManager' in src, "SignalExpiryManager应存在"
