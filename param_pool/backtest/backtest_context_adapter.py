# [BT-CTX-ADAPTER] _BacktestState 适配器 — 实现 BacktestContext 协议
# MODULE_ID: BT-CTX-ADAPTER
# _INTERNAL: 本模块为子系统内部实现

"""
BacktestStateAdapter — 将 _BacktestState 适配到 BacktestContext 协议

依赖倒置实现:
- order/backtest_context_protocol.py 定义协议 (order/ 不依赖 param_pool/)
- param_pool/backtest/backtest_context_adapter.py 实现协议 (param_pool/ 依赖 order/)
- BacktestExecutionBackend 仅依赖 BacktestContext 协议，不依赖 param_pool/ 具体类

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.4

修复历史:
- [FIX-EXEC-V2-P0-1] reduce_position 委托 _close_position_internal (2026-07-18)
- [FIX-EXEC-V2-P0-2] append_pending_order 补全 () 与 order 参数 (2026-07-18)
- [FIX-EXEC-V2-P1-2] 4 处窄异常元组扩展为 except Exception (2026-07-18)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from order.backtest_context_protocol import BacktestContext


class BacktestStateAdapter(BacktestContext):
    """_BacktestState 适配器 — 实现 BacktestContext 协议

    将 _BacktestState 的字段访问封装为协议方法，
    使 BacktestExecutionBackend 可通过协议接口操作回测状态。

    设计要点:
    - 不直接修改 _BacktestState 的字段，通过 backtest_state 模块函数操作
    - 保留 _BacktestState 的所有原逻辑 (Kahan累加、持仓更新、快照采集)
    - 线程安全: 回测为单线程串行，无需加锁
    """

    def __init__(self, bt: Any, bar_provider: Any, params: Dict[str, Any]):
        """
        Args:
            bt: _BacktestState 实例
            bar_provider: 提供 get_current() 返回当前 bar (pd.Series)
            params: 当前 trial 参数 (Dict[str, Any])
        """
        self._bt = bt
        self._bar_provider = bar_provider
        self._params = params

    def add_position(self, instrument_id: str, volume: int, open_price: float,
                     open_time: datetime, stop_profit_price: float,
                     stop_loss_price: float, open_reason: str, lots: int,
                     instrument_type: str, fill_ratio: float = 1.0,
                     open_state: str = "other",
                     open_strength: float = 0.0) -> None:
        """新增持仓 — 对应 _BacktestState.positions[instrument_id] = _BacktestPosition(...)

        [FIX-V2-P2-2] 保留 open_state/open_strength 字段,
        对齐 legacy try_open_execute (backtest_state.py:640-646) 中
        _BacktestPosition(..., open_state=bt.current_state, open_strength=bar.get("strength", 0.0)) 的语义,
        避免 V2 路径下策略后续逻辑依赖这两个字段时出现不一致。
        """
        from param_pool._param_grids import _BacktestPosition
        pos = _BacktestPosition(
            instrument_id=instrument_id,
            volume=volume,
            open_price=open_price,
            open_time=open_time,
            stop_profit_price=stop_profit_price,
            stop_loss_price=stop_loss_price,
            open_reason=open_reason,
            lots=lots,
            instrument_type=instrument_type,
            fill_ratio=fill_ratio,
            open_state=open_state,
            open_strength=open_strength,
        )
        self._bt.positions[instrument_id] = pos

    def reduce_position(self, instrument_id: str, volume: int, is_buy: bool,
                        price: float,
                        close_reason: str = "backtest_close") -> None:
        """减少持仓 (平仓) — 委托 backtest_state._close_position_internal

        [FIX-EXEC-V2-P0-1] 根因修复:
        - 原实现引用不存在的 _close_position_internal,触发 ImportError 被窄异常吞掉
        - 现已创建该函数 (param_pool/backtest/backtest_state.py)
        - 内部完成 11 步平仓序列: pnl/slippage/commission/equity/record_trade/del

        [FIX-EXEC-V2-P1-2] 实时回调路径硬约束:
        - 原窄异常元组 (ValueError/KeyError/TypeError/AttributeError/ImportError) 已扩展为 Exception
        - 防止 ZeroDivisionError/OverflowError/OSError/RecursionError 等穿透

        [FIX-V2-P3-1] 增加 close_reason 参数,透传给 _close_position_internal,
        使 _ClosedTrade.close_reason 与调用方意图一致。
        """
        try:
            from param_pool.backtest.backtest_state import _close_position_internal
            # 从 _bar_provider 获取当前 bar,传递给 _close_position_internal 用于:
            # - bar.close (close_price 解析)
            # - bar.bid_ask_spread (slippage 计算)
            # - bar._spread_quality (slippage 质量)
            # - bar.datetime (commission close_time)
            # - bar.minute (hold_minutes 计算)
            _bar = None
            if self._bar_provider is not None:
                try:
                    _bar = self._bar_provider.get_current()
                except Exception:
                    # 实时回调路径硬约束: except Exception (bar 获取失败降级 None)
                    _bar = None
            _close_position_internal(
                self._bt, instrument_id, volume, is_buy, price,
                params=self._params,
                bar=_bar,
                close_reason=close_reason,
            )
        except Exception as e:
            # 实时回调路径硬约束 (HC-4 / NEW-1 / EXEC-V2-REFACTOR): except Exception
            # 原窄异常元组 (ValueError/KeyError/TypeError/AttributeError/ImportError) 已扩展
            # 防止 ZeroDivisionError/OverflowError/OSError/RecursionError/MemoryError 等穿透
            logging.warning("[BacktestContext] reduce_position 委托失败: %s", e)

    def update_equity(self, delta: float) -> None:
        """更新权益 — 扣减成本 (delta 为正表示成本，需扣减)

        [FIX-EXEC-V2-P1-2] 实时回调路径硬约束:
        - 原窄异常元组已扩展为 except Exception
        """
        try:
            from param_pool.backtest.backtest_state import _safe_equity_add
            _safe_equity_add(self._bt, -float(delta))  # 成本扣减
        except Exception:
            # 实时回调路径硬约束 (HC-4 / NEW-1 / EXEC-V2-REFACTOR): except Exception
            # 原窄异常元组 (ValueError/KeyError/TypeError/AttributeError/ImportError) 已扩展
            # 退化: 直接扣减 (与原 _safe_equity_add 失败时的行为一致)
            self._bt.equity -= float(delta)

    def get_current_bar(self) -> Any:
        """获取当前 bar (pd.Series)"""
        if self._bar_provider is not None:
            return self._bar_provider.get_current()
        return None

    def get_current_bar_idx(self) -> int:
        """获取当前 bar 索引

        [FIX-V2-DELAY-1] 支持 BacktestExecutionBackend 在 institutional 模式下
        使用真实 bar_idx 创建延迟订单,避免 signal_bar_idx/created_at_bar 硬编码为 0
        导致 order_age 计算错误 (所有延迟订单在第一个 bar 即被判定为超期)。
        """
        if self._bar_provider is not None:
            try:
                return int(self._bar_provider.get_bar_idx())
            except Exception:
                # 降级: bar_provider 不支持 get_bar_idx 时返回 0
                return 0
        return 0

    def get_params(self) -> Dict[str, Any]:
        """获取当前 trial 参数"""
        return self._params

    def get_current_state(self) -> str:
        """获取当前回测状态 (对应 _BacktestState.current_state)

        [FIX-V2-P2-2] 支持 BacktestExecutionBackend 在开仓时记录 open_state,
        对齐 legacy try_open_execute 中 _BacktestPosition(..., open_state=bt.current_state)。
        """
        return getattr(self._bt, 'current_state', 'other')

    def record_trade(self, instrument_id: str, pnl: float, close_reason: str,
                     hold_minutes: float) -> None:
        """记录已平仓交易 — 对应 _BacktestState.closed_trades.append(_ClosedTrade(...))

        注意: 通常由 _close_position_internal 内部完成 record_trade,
        此方法保留作为协议要求的备用接口 (例如 BacktestExecutionBackend 显式调用)。

        [FIX-EXEC-V2-P1-2] 实时回调路径硬约束:
        - 原窄异常元组已扩展为 except Exception
        """
        from param_pool._param_grids import _ClosedTrade
        _pnl_pct = 0.0
        try:
            _pos = self._bt.positions.get(instrument_id)
            if _pos is not None and _pos.open_price > 0:
                _pnl_pct = pnl / (_pos.open_price * abs(getattr(_pos, 'volume', 1)))
        except Exception:
            # 实时回调路径硬约束 (HC-4 / NEW-1 / EXEC-V2-REFACTOR): except Exception
            # 原窄异常元组 (ValueError/KeyError/TypeError/AttributeError/ZeroDivisionError) 已扩展
            _pnl_pct = 0.0
        self._bt.closed_trades.append(_ClosedTrade(
            pnl=pnl,
            pnl_pct=_pnl_pct,
            close_reason=close_reason,
            hold_minutes=hold_minutes,
        ))
        self._bt.total_trades += 1

    def capture_snapshot(self, snapshot_type: str, message: str,
                         strategy_type: str) -> None:
        """采集快照 — 委托 _bt_capture_snapshot

        [FIX-EXEC-V2-P1-2] 实时回调路径硬约束:
        - 原窄异常元组已扩展为 except Exception
        """
        try:
            from param_pool.backtest.backtest_runner_validation import _bt_capture_snapshot
            _bar = self.get_current_bar()
            _bt_capture_snapshot(self._bt, snapshot_type, message, strategy_type, _bar)
        except Exception as e:
            # 实时回调路径硬约束 (HC-4 / NEW-1 / EXEC-V2-REFACTOR): except Exception
            # 原窄异常元组 (ValueError/KeyError/TypeError/AttributeError/ImportError) 已扩展
            logging.debug("[BacktestContext] capture_snapshot 委托失败: %s", e)

    def append_pending_order(self, order: Any) -> None:
        """追加延迟订单 — institutional 模式,容量上限 100

        [FIX-EXEC-V2-P0-2] 根因修复:
        - 原代码 L135 `self._bt.pending_orders.append` 缺 () 与 order 参数
        - 仅属性访问,非方法调用,institutional 模式延迟订单完全不工作
        - 现已补全为 self._bt.pending_orders.append(order)
        """
        if len(self._bt.pending_orders) >= 100:
            self._bt.pending_orders.pop(0)
        self._bt.pending_orders.append(order)

    def create_pending_order(self, symbol: str, order_type: str, volume: int,
                             lots: int, reason: str, signal_bar_idx: int,
                             signal_price: float, tp_ratio: float, sl_ratio: float,
                             params_snapshot: Dict[str, Any], created_at_bar: int,
                             fee_type: str = "taker") -> Any:
        """创建延迟订单对象 — 工厂方法 (依赖倒置)

        [FIX-EXEC-V2-P2-1] BacktestExecutionBackend 不直接 import param_pool._param_grids._PendingOrder,
        通过此协议方法由本适配器创建 _PendingOrder 实例,
        避免 order/ → param_pool/ 的直接依赖。
        """
        from param_pool._param_grids import _PendingOrder
        return _PendingOrder(
            symbol=symbol, order_type=order_type,
            volume=volume, lots=lots,
            reason=reason, signal_bar_idx=signal_bar_idx,
            signal_price=signal_price,
            tp_ratio=tp_ratio, sl_ratio=sl_ratio,
            params_snapshot=params_snapshot,
            created_at_bar=created_at_bar,
            fee_type=fee_type,
        )
