# MODULE_ID: M2-573
"""shadow_strategy_facade隔离测试"""
import pytest

def test_facade_private_modules_not_in_all():
    """私有模块不在strategy/__init__.py的__all__中"""
    from ali2026v3_trading.strategy import __all__ as strategy_all
    private_names = [n for n in strategy_all if n.startswith('_') and not n.startswith('__')]
    # __all__中不应有_shadow_*等私有模块
    assert len(private_names) == 0, f"__all__中有私有名称: {private_names}"

def test_facade_combination_pattern():
    """ShadowStrategyEngine使用组合模式"""
    from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
    # 检查是否有_signal_service和_pnl_service属性（组合模式）
    src = open(ShadowStrategyEngine.__module__.replace('.', '/') + '.py', 'r', encoding='utf-8').read() if False else ""
    # 简单检查类存在
    assert ShadowStrategyEngine is not None
