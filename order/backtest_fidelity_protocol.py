# [M1-46-BTFID] 回测保真度协议接口
# MODULE_ID: M1-46-BTFID
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""
BacktestFidelity 协议 — 依赖倒置,order/ 定义,param_pool/ 实现

设计目的:
- order/ 不能依赖 param_pool/ (避免循环依赖)
- 但 BacktestExecutionBackend 需要调用保真度函数 (_compute_fill_quantity / _simulate_limit_order_queue 等)
- 通过定义此协议接口在 order/ 中,由 param_pool/ 的 BacktestFidelityAdapter 实现
- BacktestExecutionBackend 仅依赖此协议,不依赖 param_pool 的具体实现

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.4
      docs/audit/OrderExecutor_V2_交付报告批判式吸收与余下问题调查报告_20260718.md (P2-1 完整依赖倒置)

[FIX-EXEC-V2-P2-1] 修复历史:
- 原实现: BacktestExecutionBackend 直接 import param_pool.backtest.* 6 处 (4 个模块)
- 现实现: 通过 BacktestFidelity 协议完整抽象 11 个保真度函数,实现完整依赖倒置
- 覆盖模块: _backtest_fidelity / backtest_state / _param_grids / backtest_config
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BacktestFidelity(ABC):
    """回测保真度协议 — 依赖倒置接口

    BacktestFidelityAdapter 实现此协议,将 param_pool/backtest/ 的保真度函数
    适配到 BacktestExecutionBackend 可调用的协议接口。

    11 个抽象方法覆盖回测下单所需的全部保真度计算:
    - 成交量计算: compute_fill_quantity
    - 限价单队列: simulate_limit_order_queue
    - 滑点: compute_dynamic_slippage_bps
    - 订单拆分: backtest_order_split
    - 合约乘数: get_contract_multiplier
    - 手续费: compute_commission
    - 交易所识别: infer_exchange_id / infer_exchange_from_id
    - 市价冲击: compute_market_impact_v2
    - 合约类型: infer_instrument_type
    - TP/SL: resolve_tp_sl
    """

    @abstractmethod
    def compute_fill_quantity(self, req_volume: int, bar: Any,
                              params: Dict[str, Any]) -> int:
        """计算成交量 — 迁移自 param_pool.backtest._backtest_fidelity._compute_fill_quantity

        Args:
            req_volume: 请求成交量
            bar: 当前 bar (pd.Series)
            params: 含 max_participation_rate 等参与率参数

        Returns:
            int: 实际可成交量 (actual_lots)
        """
        ...

    @abstractmethod
    def simulate_limit_order_queue(self, order_price: float, current_price: float,
                                    bar: Any, order_lots: int,
                                    enable_queue: bool = True) -> Dict[str, Any]:
        """限价单队列模拟 — 迁移自 param_pool.backtest._backtest_fidelity._simulate_limit_order_queue

        Args:
            order_price: 订单价格
            current_price: 当前价格
            bar: 当前 bar
            order_lots: 订单手数
            enable_queue: 是否启用队列模拟

        Returns:
            Dict[str, Any]: {'filled': bool, 'fill_lots': int, ...}
        """
        ...

    @abstractmethod
    def compute_dynamic_slippage_bps(self, price: float, bid_ask: float,
                                     spread_quality: int, bar: Any,
                                     params: Dict[str, Any]) -> float:
        """动态滑点计算 — 迁移自 param_pool.backtest.backtest_state._compute_dynamic_slippage_bps

        Args:
            price: 当前价格
            bid_ask: 买卖价差
            spread_quality: 价差质量
            bar: 当前 bar
            params: 含 slippage 配置参数

        Returns:
            float: 滑点 bps
        """
        ...

    @abstractmethod
    def backtest_order_split(self, lots: int,
                             max_sub_order_lots: int = 5) -> List[int]:
        """订单拆分 — 迁移自 param_pool.backtest.backtest_state._backtest_order_split

        Args:
            lots: 总手数
            max_sub_order_lots: 单个子订单最大手数

        Returns:
            List[int]: 子订单手数列表
        """
        ...

    @abstractmethod
    def get_contract_multiplier(self, instrument_id: str) -> float:
        """合约乘数 — 迁移自 param_pool.backtest.backtest_state._get_contract_multiplier

        Args:
            instrument_id: 合约 ID

        Returns:
            float: 合约乘数
        """
        ...

    @abstractmethod
    def compute_commission(self, instrument_id: str, lots: int,
                           is_open: bool,
                           exchange_id: Optional[str] = None,
                           exchange: Optional[str] = None,
                           open_time: Any = None,
                           close_time: Any = None) -> float:
        """手续费计算 — 迁移自 param_pool.backtest.backtest_state._compute_commission

        Args:
            instrument_id: 合约 ID
            lots: 手数
            is_open: 是否开仓 (True=开仓 / False=平仓)
            exchange_id: 交易所 ID (可选,内部自动推断)
            exchange: 交易所名称 (可选,内部自动推断)
            open_time: 开仓时间 (可选,用于时段差异化费率)
            close_time: 平仓时间 (可选)

        Returns:
            float: 手续费金额
        """
        ...

    @abstractmethod
    def infer_exchange_id(self, instrument_id: str) -> str:
        """交易所 ID 推断 — 迁移自 param_pool.backtest.backtest_state._infer_exchange_id

        Args:
            instrument_id: 合约 ID

        Returns:
            str: 交易所 ID
        """
        ...

    @abstractmethod
    def infer_exchange_from_id(self, exchange_id: str) -> str:
        """交易所名称 — 迁移自 param_pool.backtest.backtest_state._infer_exchange_from_id

        Args:
            exchange_id: 交易所 ID

        Returns:
            str: 交易所名称
        """
        ...

    @abstractmethod
    def compute_market_impact_v2(self, lots: int, bar: Any, exec_price: float,
                                   params: Dict[str, Any]) -> float:
        """市价冲击 — 迁移自 param_pool._param_grids._compute_market_impact_v2

        Args:
            lots: 成交手数
            bar: 当前 bar
            exec_price: 执行价
            params: 含 market_impact 配置参数

        Returns:
            float: 市价冲击成本
        """
        ...

    @abstractmethod
    def infer_instrument_type(self, symbol: str) -> str:
        """合约类型推断 — 迁移自 param_pool.backtest.backtest_config._infer_instrument_type

        Args:
            symbol: 合约 ID

        Returns:
            str: 合约类型 ('future' / 'option_buyer' / 'option_seller')
        """
        ...

    @abstractmethod
    def resolve_tp_sl(self, params: Dict[str, Any],
                       reason: str) -> Tuple[float, float]:
        """TP/SL 比率解析 — 迁移自 param_pool.backtest.backtest_state._resolve_tp_sl

        Args:
            params: 含 tp_ratio / sl_ratio 等参数
            reason: 开仓原因 (用于差异化 TP/SL)

        Returns:
            Tuple[float, float]: (tp_ratio, sl_ratio)
        """
        ...


__all__ = ['BacktestFidelity']
