# [BT-FID-ADAPTER] 回测保真度适配器 — 实现 BacktestFidelity 协议
# MODULE_ID: BT-FID-ADAPTER
# _INTERNAL: 本模块为子系统内部实现

"""
BacktestFidelityAdapter — 将 param_pool/backtest/ 的保真度函数适配到 BacktestFidelity 协议

依赖倒置实现 (与 BacktestStateAdapter 对称):
- order/backtest_fidelity_protocol.py 定义协议 (order/ 不依赖 param_pool/)
- param_pool/backtest/backtest_fidelity_adapter.py 实现协议 (param_pool/ 依赖 order/)
- BacktestExecutionBackend 仅依赖 BacktestFidelity 协议,不依赖 param_pool/ 具体实现

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.4
      docs/audit/OrderExecutor_V2_交付报告批判式吸收与余下问题调查报告_20260718.md (P2-1 完整依赖倒置)

[FIX-EXEC-V2-P2-1] 修复历史:
- 原 BacktestExecutionBackend 直接 import param_pool.backtest.* 6 处 (4 模块)
- 现通过本适配器委托,实现完整依赖倒置
- 覆盖 11 个保真度函数,分散于 4 个 param_pool 模块
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from order.backtest_fidelity_protocol import BacktestFidelity


class BacktestFidelityAdapter(BacktestFidelity):
    """保真度适配器 — 实现 BacktestFidelity 协议

    将 param_pool/backtest/ 的 11 个保真度函数封装为协议方法,
    使 BacktestExecutionBackend 可通过协议接口调用而不引入对 param_pool/ 的循环依赖。

    设计要点:
    - 适配器是无状态 (stateless) 的,仅做函数委托
    - 函数实现位于原模块,适配器只做 import 委托,不复制逻辑
    - 与 BacktestStateAdapter 对称: 一个适配状态访问,一个适配保真度计算
    - 线程安全: 所有委托函数均为无状态 pure functions,无需加锁
    """

    # ===== 成交量计算 =====
    def compute_fill_quantity(self, req_volume: int, bar: Any,
                              params: Dict[str, Any]) -> int:
        """委托 param_pool.backtest._backtest_fidelity._compute_fill_quantity"""
        from param_pool.backtest._backtest_fidelity import _compute_fill_quantity
        return _compute_fill_quantity(req_volume, bar, params)

    # ===== 限价单队列 =====
    def simulate_limit_order_queue(self, order_price: float, current_price: float,
                                    bar: Any, order_lots: int,
                                    enable_queue: bool = True) -> Dict[str, Any]:
        """委托 param_pool.backtest._backtest_fidelity._simulate_limit_order_queue"""
        from param_pool.backtest._backtest_fidelity import _simulate_limit_order_queue
        return _simulate_limit_order_queue(
            order_price=order_price, current_price=current_price, bar=bar,
            order_lots=order_lots, enable_queue=enable_queue
        )

    # ===== 滑点 =====
    def compute_dynamic_slippage_bps(self, price: float, bid_ask: float,
                                     spread_quality: int, bar: Any,
                                     params: Dict[str, Any]) -> float:
        """委托 param_pool.backtest.backtest_state._compute_dynamic_slippage_bps"""
        from param_pool.backtest.backtest_state import _compute_dynamic_slippage_bps
        return _compute_dynamic_slippage_bps(
            price, bid_ask, spread_quality=spread_quality, bar=bar, params=params
        )

    # ===== 订单拆分 =====
    def backtest_order_split(self, lots: int,
                             max_sub_order_lots: int = 5) -> List[int]:
        """委托 param_pool.backtest.backtest_state._backtest_order_split"""
        from param_pool.backtest.backtest_state import _backtest_order_split
        return _backtest_order_split(lots, max_sub_order_lots=max_sub_order_lots)

    # ===== 合约乘数 =====
    def get_contract_multiplier(self, instrument_id: str) -> float:
        """委托 param_pool.backtest.backtest_state._get_contract_multiplier"""
        from param_pool.backtest.backtest_state import _get_contract_multiplier
        return _get_contract_multiplier(instrument_id)

    # ===== 手续费 =====
    def compute_commission(self, instrument_id: str, lots: int,
                           is_open: bool,
                           exchange_id: Optional[str] = None,
                           exchange: Optional[str] = None,
                           open_time: Any = None,
                           close_time: Any = None) -> float:
        """委托 param_pool.backtest.backtest_state._compute_commission

        _compute_commission 实际为 infra.commission_utils.compute_commission (re-export)
        """
        from param_pool.backtest.backtest_state import _compute_commission
        # 调用方传 exchange_id/exchange 时直接使用;否则内部自动推断
        if exchange_id is not None and exchange is not None:
            return _compute_commission(
                instrument_id, lots, is_open=is_open,
                exchange_id=exchange_id, exchange=exchange,
                open_time=open_time, close_time=close_time,
            )
        # 退化: 仅传 instrument_id,内部推断
        return _compute_commission(
            instrument_id, lots, is_open=is_open,
            open_time=open_time, close_time=close_time,
        )

    # ===== 交易所识别 =====
    def infer_exchange_id(self, instrument_id: str) -> str:
        """委托 param_pool.backtest.backtest_state._infer_exchange_id"""
        from param_pool.backtest.backtest_state import _infer_exchange_id
        return _infer_exchange_id(instrument_id)

    def infer_exchange_from_id(self, exchange_id: str) -> str:
        """委托 param_pool.backtest.backtest_state._infer_exchange_from_id"""
        from param_pool.backtest.backtest_state import _infer_exchange_from_id
        return _infer_exchange_from_id(exchange_id)

    # ===== 市价冲击 =====
    def compute_market_impact_v2(self, lots: int, bar: Any, exec_price: float,
                                   params: Dict[str, Any]) -> float:
        """委托 param_pool._param_grids._compute_market_impact_v2"""
        from param_pool._param_grids import _compute_market_impact_v2
        return _compute_market_impact_v2(lots, bar, exec_price, params)

    # ===== 合约类型 =====
    def infer_instrument_type(self, symbol: str) -> str:
        """委托 param_pool.backtest.backtest_config._infer_instrument_type"""
        from param_pool.backtest.backtest_config import _infer_instrument_type
        return _infer_instrument_type(symbol)

    # ===== TP/SL =====
    def resolve_tp_sl(self, params: Dict[str, Any],
                       reason: str) -> Tuple[float, float]:
        """委托 param_pool.backtest.backtest_state._resolve_tp_sl"""
        from param_pool.backtest.backtest_state import _resolve_tp_sl
        return _resolve_tp_sl(params, reason)


__all__ = ['BacktestFidelityAdapter']
