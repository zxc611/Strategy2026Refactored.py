# MODULE_ID: M1-194
"""验证器协议基类 — R7-2: 接口一致性保证

所有统计验证器和门控验证器必须满足对应Protocol，
确保可替换性和测试一致性。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class StatisticalValidator(Protocol):
    """统计验证器协议 — test/validate方法返回Dict[str, Any]"""

    def test(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...
    def validate(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...


@runtime_checkable
class GateValidator(Protocol):
    """门控验证器协议 — check方法返回含passed键的Dict"""

    def check(self, context: Dict[str, Any]) -> Dict[str, Any]: ...