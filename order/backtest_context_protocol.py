# [M1-46-BTCTX] 回测上下文协议接口
# MODULE_ID: M1-46-BTCTX
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""
BacktestContext 协议 — 依赖倒置，order/ 定义，param_pool/ 实现

设计目的:
- order/ 不能依赖 param_pool/ (避免循环依赖)
- 但 BacktestExecutionBackend 需要访问 _BacktestState 的状态
- 通过定义此协议接口在 order/ 中，由 param_pool/ 的 BacktestStateAdapter 实现
- BacktestExecutionBackend 仅依赖此协议，不依赖 param_pool 的具体实现

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.4
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict


class BacktestContext(ABC):
    """回测上下文协议 — 依赖倒置接口

    _BacktestState 适配器实现此协议，使 BacktestExecutionBackend
    可访问回测状态而不引入对 param_pool/ 的循环依赖。

    9 个抽象方法覆盖回测下单所需的全部状态访问:
    - 持仓管理: add_position / reduce_position
    - 权益更新: update_equity
    - 数据访问: get_current_bar / get_current_bar_idx / get_params
    - 交易记录: record_trade
    - 快照采集: capture_snapshot
    - 延迟订单: append_pending_order / create_pending_order
    """

    @abstractmethod
    def add_position(self, instrument_id: str, volume: int, open_price: float,
                     open_time: datetime, stop_profit_price: float,
                     stop_loss_price: float, open_reason: str, lots: int,
                     instrument_type: str, fill_ratio: float = 1.0,
                     open_state: str = "other",
                     open_strength: float = 0.0) -> None:
        """新增持仓

        对应 _BacktestState.positions[instrument_id] = _BacktestPosition(...)

        [FIX-V2-P2-2] 保留 open_state/open_strength 字段,
        对齐 legacy try_open_execute 中 _BacktestPosition(..., open_state=bt.current_state,
        open_strength=bar.get("strength", 0.0)) 的语义。
        """
        ...

    @abstractmethod
    def reduce_position(self, instrument_id: str, volume: int, is_buy: bool,
                        price: float,
                        close_reason: str = "backtest_close") -> None:
        """减少持仓 (平仓)

        对应 _BacktestState 平仓逻辑，触发 _ClosedTrade 记录

        [FIX-V2-P3-1] 增加 close_reason 参数,使 _close_position_internal
        记录的 _ClosedTrade.close_reason 与调用方意图一致 (StopProfit/StopLoss/
        HardTimeStop/backtest_end_force_close 等),避免全部降级为 'backtest_close'。
        """
        ...

    @abstractmethod
    def update_equity(self, delta: float) -> None:
        """更新权益 (扣减手续费/滑点/市价冲击)

        delta 为成本正值时扣减权益，delta 为收益正值时增加权益
        内部使用 _kahan_c 保证浮点累加精度
        """
        ...

    @abstractmethod
    def get_current_bar(self) -> Any:
        """获取当前 bar (pd.Series)

        用于保真度模拟函数 (_compute_fill_quantity / _simulate_limit_order_queue 等)
        """
        ...

    @abstractmethod
    def get_current_bar_idx(self) -> int:
        """获取当前 bar 索引

        [FIX-V2-DELAY-1] 用于 institutional 延迟订单计算 order_age,
        替代 _append_pending_order 中硬编码的 0。
        """
        ...

    @abstractmethod
    def get_params(self) -> Dict[str, Any]:
        """获取当前 trial 参数

        包含滑点/手续费/队列/市价冲击等保真度参数
        """
        ...

    @abstractmethod
    def get_current_state(self) -> str:
        """获取当前回测状态 (对应 _BacktestState.current_state)

        [FIX-V2-P2-2] 用于新增持仓时记录 open_state,对齐 legacy try_open_execute。
        """
        ...

    @abstractmethod
    def record_trade(self, instrument_id: str, pnl: float, close_reason: str,
                     hold_minutes: float) -> None:
        """记录已平仓交易

        对应 _BacktestState.closed_trades.append(_ClosedTrade(...))
        """
        ...

    @abstractmethod
    def capture_snapshot(self, snapshot_type: str, message: str,
                         strategy_type: str) -> None:
        """采集快照 (OPEN_SNAPSHOT / CLOSE_SNAPSHOT)

        对应 _bt_capture_snapshot(bt, snapshot_type, message, strategy_type, bar)
        """
        ...

    @abstractmethod
    def append_pending_order(self, order: Any) -> None:
        """追加延迟订单 (institutional 模式)

        对应 bt.pending_orders.append(_PendingOrder(...))
        容量上限 100，超出时 pop(0)
        """
        ...

    @abstractmethod
    def create_pending_order(self, symbol: str, order_type: str, volume: int,
                             lots: int, reason: str, signal_bar_idx: int,
                             signal_price: float, tp_ratio: float, sl_ratio: float,
                             params_snapshot: Dict[str, Any], created_at_bar: int,
                             fee_type: str = "taker") -> Any:
        """创建延迟订单对象 (工厂方法 — 依赖倒置)

        [FIX-EXEC-V2-P2-1] BacktestExecutionBackend 不直接 import param_pool._param_grids._PendingOrder,
        通过此协议方法由 BacktestStateAdapter 创建具体 _PendingOrder 实例,
        避免 order/ → param_pool/ 的直接依赖。

        返回的对象应可直接传给 append_pending_order(order)。
        """
        ...


__all__ = ['BacktestContext']
