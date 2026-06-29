# MODULE_ID: M1-280
"""策略共享类型定义 - 打破 strategy_2026 <-> strategy_core_service 循环依赖

StrategyParams 从 strategy_2026.py 下沉至此独立模块，
使 strategy_core_service.py 和 strategy_2026.py 均可从此导入，消除循环依赖。
"""
from __future__ import annotations

from typing import Any, Dict


class StrategyParams:
    """策略参数容器 - 替代 type('Params', (), dict)() 匿名类

    提供可调试的 __repr__ 和 __dir__，方便排查参数问题。'
    从 strategy_2026.py 迁移至此独立模块(2026-06-12)。
    """

    __slots__ = ('_data',)

    def __init__(self, data: Dict[str, Any]):
        object.__setattr__(self, '_data', dict(data))

    def __getattr__(self, name: str):
        try:
            return object.__getattribute__(self, '_data')[name]
        except KeyError:
            raise AttributeError(
                f"'StrategyParams' object has no attribute '{name}'. "
                f"Available: {sorted(object.__getattribute__(self, '_data').keys())}"
            )

    def __setattr__(self, name: str, value):
        if name == '_data':
            object.__setattr__(self, name, value)
        else:
            object.__getattribute__(self, '_data')[name] = value

    def __repr__(self) -> str:
        data = object.__getattribute__(self, '_data')
        items = {k: v for k, v in data.items() if not k.startswith('_')}
        return f"StrategyParams({items})"

    def __dir__(self):
        data = object.__getattribute__(self, '_data')
        return sorted(data.keys())

    def as_dict(self) -> Dict[str, Any]:
        return dict(object.__getattribute__(self, '_data'))

    def get(self, key: str, default=None):
        return object.__getattribute__(self, '_data').get(key, default)


__all__ = ['StrategyParams']
