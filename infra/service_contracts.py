"""P1-34修复: 服务协议定义 — 统一 health_check / get_service_name 接口约束

多个服务类均实现了 health_check() 和 get_service_name()，
但没有一个统一的 ABC 或 Protocol 强制约束。
本模块使用 typing.Protocol 定义 ServiceProtocol，使接口契约显式化。

Protocol 是隐式实现的（structural subtyping），
其他服务类无需修改即可满足协议，只需确保协议定义存在。
"""
from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class ServiceProtocol(Protocol):
    """服务协议 — 所有服务类应满足此接口

    方法签名：
    - health_check() -> Dict[str, Any]: 返回服务健康状态字典
    - get_service_name() -> str: 返回服务名称
    """

    def health_check(self) -> Dict[str, Any]:
        """返回服务健康状态字典

        Returns:
            Dict[str, Any]: 包含服务健康状态的字典，
                至少应包含 'healthy' (bool) 和 'service_name' (str) 键
        """
        ...

    def get_service_name(self) -> str:
        """返回服务名称

        Returns:
            str: 服务名称，通常为类名
        """
        ...


__all__ = ['ServiceProtocol']
