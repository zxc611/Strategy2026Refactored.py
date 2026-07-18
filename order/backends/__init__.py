# [M1-46-BACKENDS] 订单执行后端实现包
# MODULE_ID: M1-46-BACKENDS
# _INTERNAL: 本模块为子系统内部实现

"""
订单执行后端实现包

包含三个具体后端:
- LiveExecutionBackend: 生产下单后端
- DryRunExecutionBackend: dry_run 虚拟成交后端 (含 CallbackInjector)
- BacktestExecutionBackend: 回测保真度模拟后端

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md
"""

__all__ = []