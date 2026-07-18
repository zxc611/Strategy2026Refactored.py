# [M1-46-BACKEND] 订单执行后端抽象接口
# MODULE_ID: M1-46-BACKEND
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""
OrderExecutionBackend 抽象接口 — 策略模式抽象

设计要点:
- 所有后端 (Live/DryRun/Backtest) 共享 OrderContext 数据载体
- execute() 仅负责"下单"阶段，前置校验和后置持久化由 OrderExecutor 编排层统一处理
- supports_dry_run() 决定 _pre_send_checks 是否跳过断路器/胖手指检查
- 持有 _orchestrator 反向引用，用于调用 _post_send_persist 等共享编排逻辑

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.1
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from order.order_executor_validation import OrderContext
    from order.order_base import OrderResult


class OrderExecutionBackend(ABC):
    """订单执行后端接口 — 策略模式抽象

    生产/回测/dry_run 各一个实现，由 OrderExecutor 持有。
    所有后端共享 OrderContext 数据载体。

    设计要点:
    - execute() 仅负责"下单"阶段，前置校验和后置持久化由编排层统一处理
    - supports_dry_run() 决定 _pre_send_checks 是否跳过断路器/胖手指检查
    - 持有 _orchestrator 反向引用，用于调用 _post_send_persist 等共享编排逻辑
    """

    def __init__(self, orchestrator: Any = None):
        # 反向引用 OrderExecutor，用于调用 _post_send_persist 等共享方法
        self._orchestrator = orchestrator
        # 延迟绑定的 OrderService 引用 (由 bind_service 设置)
        self._svc: Any = None

    @abstractmethod
    def execute(self, ctx: "OrderContext") -> "OrderResult":
        """执行下单: pre_check 已由编排层完成，本方法仅负责 insert 阶段

        实现要点:
        - 不得重复调用 _pre_send_checks (已由编排层完成)
        - 完成后应委托 self._orchestrator._post_send_persist(ctx) 完成后置编排
        """
        ...

    @abstractmethod
    def supports_dry_run(self) -> bool:
        """是否为虚拟成交后端 (影响 _pre_send_checks 行为)

        返回 True 时编排层将跳过断路器/胖手指/自成交等实盘校验
        """
        ...

    def bind_orchestrator(self, orchestrator: Any) -> None:
        """注入编排层引用，用于调用 _post_send_persist 等共享方法"""
        self._orchestrator = orchestrator

    def bind_service(self, svc: Any) -> None:
        """注入 OrderService 引用 (延迟绑定，避免循环依赖)"""
        self._svc = svc


__all__ = ['OrderExecutionBackend']
