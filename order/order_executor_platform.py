# [M1-46-02-DEPRECATED] 订单执行器-平台交互 (re-export shim)
# MODULE_ID: M1-46-02-SHIM
# _INTERNAL: 本模块为子系统内部实现
#
# !!! 已废弃 (DEPRECATED) !!!
# 计划下线日期: 2026-07-25 (一周观察期)
# 旧模块: _OrderExecutorPlatformMethods (Mixin 类)
# 新模块:
#   - order.backends.live_backend.LiveExecutionBackend
#   - order.backends.dry_run_backend.DryRunExecutionBackend
#   - order.backends.backtest_backend.BacktestExecutionBackend
#   - order.backends.callback_injector.PositionServiceCallbackInjector
#                                        /BusinessLayerCallbackInjector
#   - order.order_executor.OrderExecutor (编排层)
#
# 依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第七节 P7
# 迁移映射: 第四部分 项 1-15 (15 项完整迁移映射)

"""
_OrderExecutorPlatformMethods 兼容性 re-export shim

V2.0 重构: Mixin 组合模式 → 组合 + 策略模式
- _execute_platform_insert     → LiveExecutionBackend.execute / DryRunExecutionBackend.execute
- _build_platform_insert_params → LiveExecutionBackend.build_params
- _invoke_platform_insert_with_timeout → LiveExecutionBackend.invoke_insert
- _invoke_platform_cancel_with_timeout → LiveExecutionBackend.invoke_cancel
- bind_platform_apis           → LiveExecutionBackend.bind_platform_apis
- _simulate_dry_run_callbacks  → BusinessLayerCallbackInjector (路径 B)
- _post_send_persist           → OrderExecutor._post_send_persist
- send_order_split             → OrderExecutor.send_order_split
- _plan_volume_split           → OrderExecutor._plan_volume_split
- execute_by_ranking          → OrderExecutor.execute_by_ranking

迁移指引:
- 旧代码: from order.order_executor_platform import _OrderExecutorPlatformMethods
- 新代码: from order.order_executor import OrderExecutor
                                from order.backends.live_backend import LiveExecutionBackend

兼容性保证:
- _OrderExecutorPlatformMethods 名字仍可导入 (DeprecationWarning)
- 旧方法名以委托方式转发到 OrderExecutor 实例 (功能等价)
- 仅用于过渡期，禁止新代码引用本模块
"""

from __future__ import annotations

import warnings
from typing import Any

# 导入新的编排层
from order.order_executor import OrderExecutor
from order.order_executor_validation import OrderContext

# 触发 DeprecationWarning (调用栈层级=2 让警告指向调用方)
warnings.warn(
    "order.order_executor_platform._OrderExecutorPlatformMethods 已废弃, "
    "请改用 order.order_executor.OrderExecutor + order.backends.* 后端. "
    "计划下线日期: 2026-07-25 (依据: OrderExecutor_Mixin_重构最终落地方案_20260718.md P7)",
    DeprecationWarning,
    stacklevel=2,
)


class _OrderExecutorPlatformMethods:
    """[已废弃] OrderExecutor 平台交互方法 Mixin 类

    V2.0 重构后所有方法已迁移到:
    - LiveExecutionBackend (生产下单分支)
    - DryRunExecutionBackend (虚拟成交分支)
    - BacktestExecutionBackend (回测保真度模拟)
    - OrderExecutor (共享编排方法)

    本兼容性 shim 通过 __getattr__ 委托到内部 OrderExecutor 实例,
    保证旧调用链仍可工作, 但触发 DeprecationWarning 提示迁移。
    """

    # 迁移指引 (供运维参考)
    _DEPRECATED_MIGRATION_MAP = {
        '_execute_platform_insert': 'LiveExecutionBackend.execute / DryRunExecutionBackend.execute',
        '_build_platform_insert_params': 'LiveExecutionBackend.build_params',
        '_invoke_platform_insert_with_timeout': 'LiveExecutionBackend.invoke_insert',
        '_invoke_platform_cancel_with_timeout': 'LiveExecutionBackend.invoke_cancel',
        'bind_platform_apis': 'LiveExecutionBackend.bind_platform_apis',
        '_simulate_dry_run_callbacks': 'BusinessLayerCallbackInjector (路径 B)',
        '_post_send_persist': 'OrderExecutor._post_send_persist',
        'send_order_split': 'OrderExecutor.send_order_split',
        '_plan_volume_split': 'OrderExecutor._plan_volume_split',
        'execute_by_ranking': 'OrderExecutor.execute_by_ranking',
    }

    def __init__(self, order_service: Any = None):
        """初始化: 持有 OrderExecutor 实例进行委托"""
        if order_service is not None:
            # 仅用于委托, 不创建新的 OrderService 引用
            self._delegate = OrderExecutor(order_service)
        else:
            self._delegate = None

    def __getattr__(self, name: str):
        """委托未定义属性到 OrderExecutor 实例"""
        # 标准属性直接返回 (避免 __getattr__ 触发自身递归)
        if name.startswith('_') and name in ('_delegate', '_svc'):
            raise AttributeError(name)

        # 已迁移方法给出明确迁移指引
        if name in self._DEPRECATED_MIGRATION_MAP:
            _new_loc = self._DEPRECATED_MIGRATION_MAP[name]
            raise AttributeError(
                f"_OrderExecutorPlatformMethods.{name} 已废弃, "
                f"请改用 {_new_loc}. "
                f"(依据: OrderExecutor_Mixin_重构最终落地方案_20260718.md P7)"
            )

        # 委托到 _delegate (兼容旧 Mixin 调用模式)
        if self._delegate is not None and hasattr(self._delegate, name):
            return getattr(self._delegate, name)

        raise AttributeError(
            f"_OrderExecutorPlatformMethods.{name} 属性不存在 (且 _delegate 未设置或无此属性)"
        )


__all__ = ['_OrderExecutorPlatformMethods', 'OrderContext']
