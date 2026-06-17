# MODULE_ID: M1-255
"""
K线数据服务 - 独立组合式服务（替代 HistoricalKlineMixin）

职责：
- 管理历史K线加载的启动、执行和进度跟踪
- 提供合约过滤和提供者解析功能
- 维护历史加载状态和统计信息
- 回测模式下提供时间截止墙（time cutoff wall），防止未来数据泄露

设计原则：
- 组合优于继承：通过构造函数注入 state_store / callback_group，不依赖 Mixin 宿主的 self
- 完全自包含：不使用任何 Mixin 宿主属性
- 显式属性访问：替代 __getattr__ 代理，提供显式 @property
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.shared_utils import safe_int, safe_float
from ali2026v3_trading.infra.exceptions import FutureLeakException


class KlineDataService:
    """K线数据服务 - 替代 HistoricalKlineMixin 的独立组合式服务。

    通过构造函数接收 state_store 和 callback_group，完全自包含，
    不依赖 Mixin 宿主的 self 属性。

    Args:
        state_store: 状态存储，提供 get/set 方法访问策略全局状态
        callback_group: 回调组，提供 get_registry 方法访问注册的回调
        strategy_id: 策略标识，用于日志
        is_backtest: 是否为回测模式，回测模式下启用时间截止墙
    """

    def __init__(
        self,
        state_store: Any = None,
        callback_group: Any = None,
        strategy_id: str = '',
        is_backtest: bool = False,
    ):
        self._state_store = state_store
        self._callback_group = callback_group
        self._strategy_id = strategy_id
        self._is_backtest = is_backtest

        # 时间截止墙（回测模式）
        self._time_cutoff_wall: Optional[float] = None

        # 内部 HistoricalDataManager 实例（延迟创建）
        self._historical_mgr: Any = None

    # ------------------------------------------------------------------
    # 时间截止墙（回测模式防未来数据泄露）
    # ------------------------------------------------------------------

    def set_time_cutoff(self, cutoff_time: float) -> None:
        """设置时间截止墙。回测模式下，超出此时间的数据请求将触发异常。

        Args:
            cutoff_time: 截止时间戳（Unix epoch 秒）
        """
        self._time_cutoff_wall = cutoff_time
        logging.debug(
            "[KlineDataService][strategy_id=%s] Time cutoff wall set to %.3f",
            self._strategy_id, cutoff_time,
        )

    def check_time_cutoff(self, requested_time: float) -> None:
        """检查请求时间是否超出截止墙。

        仅在回测模式且已设置 time_cutoff 时生效。
        实盘模式下此方法为空操作。

        Args:
            requested_time: 请求的数据时间戳（Unix epoch 秒）

        Raises:
            FutureLeakException: 当回测模式下请求时间超出截止墙时
        """
        if not self._is_backtest:
            return
        if self._time_cutoff_wall is None:
            return
        if requested_time > self._time_cutoff_wall:
            raise FutureLeakException(
                f"[KlineDataService][strategy_id={self._strategy_id}] "
                f"Future data leak detected: requested_time={requested_time:.3f} "
                f"exceeds time_cutoff_wall={self._time_cutoff_wall:.3f}"
            )

    # ------------------------------------------------------------------
    # 底层 HistoricalDataManager 访问
    # ------------------------------------------------------------------

    @property
    def historical_mgr(self) -> Any:
        """访问底层 HistoricalDataManager 实例。"""
        return self._historical_mgr

    # ------------------------------------------------------------------
    # 显式属性访问（替代 __getattr__ 代理）
    # ------------------------------------------------------------------

    @property
    def load_in_progress(self) -> bool:
        """历史K线加载是否正在进行。"""
        if self._historical_mgr is None:
            return False
        return self._historical_mgr._historical_load_in_progress

    @property
    def kline_result(self) -> Optional[Dict[str, Any]]:
        """历史K线加载结果。"""
        if self._historical_mgr is None:
            return None
        return self._historical_mgr._historical_kline_result

    @property
    def kline_progress(self) -> Optional[Dict[str, Any]]:
        """历史K线加载进度。"""
        if self._historical_mgr is None:
            return None
        return self._historical_mgr._historical_kline_progress

    # ------------------------------------------------------------------
    # 核心方法 - 委托到 HistoricalDataManager
    # ------------------------------------------------------------------

    def init_historical(self) -> None:
        """初始化历史K线管理器。创建 HistoricalDataManager 并调用 init_historical()。"""
        if self._historical_mgr is None:
            from ali2026v3_trading.data.historical_data_manager import HistoricalDataManager
            self._historical_mgr = HistoricalDataManager(
                state_store=self._state_store,
                callback_group=self._callback_group,
            )
        self._historical_mgr.init_historical()

    def filter_historical_month_scope(self, instrument_ids: List[str]) -> Tuple[List[str], int, str]:
        """过滤合约月份范围。

        Args:
            instrument_ids: 待过滤的合约ID列表

        Returns:
            (过滤后列表, 移除数量, 最低年月)
        """
        return self._historical_mgr.filter_historical_month_scope(instrument_ids)

    def build_historical_instruments(self) -> List[str]:
        """构建历史K线加载的合约列表。"""
        return self._historical_mgr.build_historical_instruments()

    def resolve_historical_provider(self) -> Tuple[Any, str]:
        """解析历史K线数据提供者。

        Returns:
            (provider, provider_source) 元组
        """
        return self._historical_mgr.resolve_historical_provider()

    def load_historical_klines_once(self, instruments: List[str], provider: Any, provider_source: str) -> None:
        """执行一次历史K线加载。

        Args:
            instruments: 合约列表
            provider: 数据提供者
            provider_source: 提供者来源标识
        """
        return self._historical_mgr.load_historical_klines_once(instruments, provider, provider_source)

    def notify_historical_kline_loaded(self, result: Dict[str, Any]) -> None:
        """通知历史K线加载完成。

        Args:
            result: 加载结果字典
        """
        return self._historical_mgr.notify_historical_kline_loaded(result)

    def start_historical_kline_load(self, blocking: bool = False) -> None:
        """启动历史K线加载。

        Args:
            blocking: 是否阻塞等待加载完成
        """
        return self._historical_mgr.start_historical_kline_load(blocking=blocking)

    def shutdown_historical_services(self) -> None:
        """关闭历史K线相关服务，停止加载线程。"""
        return self._historical_mgr.shutdown_historical_services()

    def reset_historical_state_for_restart(self) -> None:
        """重置历史加载状态，为重启做准备。"""
        return self._historical_mgr.reset_historical_state_for_restart()

    def emit_historical_kline_diagnostic_on_first_tick(self) -> None:
        """在首个Tick时输出历史K线诊断信息。"""
        return self._historical_mgr.emit_historical_kline_diagnostic_on_first_tick()

    def check_and_start_historical_load_on_tick(self) -> None:
        """在Tick中检查并启动历史K线加载（兜底逻辑）。"""
        return self._historical_mgr.check_and_start_historical_load_on_tick()

    def get_historical_kline_summary_lines(self) -> Tuple[str, str]:
        """获取历史K线摘要信息行。

        Returns:
            (kline_summary_line, historical_detail_line) 元组
        """
        return self._historical_mgr.get_historical_kline_summary_lines()
