"""shadow_strategy_facade.py - 影子策略子系统唯一公共入口（ARCH-01 Facade模式）

外部消费者只能通过此模块访问影子策略功能。
任何 _shadow_ 前缀的内部模块都不应被外部直接import。

PA-03 三层物理隔离: 第三层（最外层，唯一对外暴露面）

R2-1: 纯组合模式（消除继承），Core通过组合持有+显式委托

公共API（≤7个符号）:
  - ShadowStrategyEngine: 影子策略引擎（Facade，纯组合3个Service）
  - ShadowTradeRecord: 影子策略交易记录
  - AlphaMetrics: Alpha比率监控指标
  - ShadowParamsSnapshot: 影子策略参数快照
  - get_shadow_strategy_engine: 全局单例工厂函数
"""
from __future__ import annotations

from ali2026v3_trading.strategy.shadow_strategy_types import (
    ShadowTradeRecord,
    AlphaMetrics,
    ShadowParamsSnapshot,
)
from ali2026v3_trading.strategy.shadow_strategy_core import (
    ShadowStrategyCoreService,
)
from ali2026v3_trading.strategy._shadow_strategy_signal import (
    ShadowStrategySignalService,
)
from ali2026v3_trading.strategy._shadow_strategy_pnl import (
    ShadowStrategyPnLService,
)
from ali2026v3_trading.strategy import _shadow_internals as _internals


class ShadowStrategyEngine:
    """Shadow strategy engine - Facade纯组合（升A路径T2.4: 消灭继承泄漏）

    通过组合3个Service实现功能委托，不继承任何Service基类。
    Core/Signal/PnL通过__getattr__按优先级委托。
    """

    def __init__(self, **kwargs):
        self._core_service = ShadowStrategyCoreService(**kwargs)
        self._signal_service = ShadowStrategySignalService()
        self._signal_service._facade = self
        self._pnl_service = ShadowStrategyPnLService(self._core_service)
        self._pnl_service._facade = self

    def __getattr__(self, name):
        if '__getattr__recursing' in self.__dict__:
            raise AttributeError(name)
        self.__dict__['__getattr__recursing'] = True
        try:
            for svc_name in ('_core_service', '_signal_service', '_pnl_service'):
                svc = self.__dict__.get(svc_name)
                if svc is not None and hasattr(svc, name):
                    return getattr(svc, name)
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        finally:
            self.__dict__.pop('__getattr__recursing', None)

    # ── 显式委托Core公共API（避免__getattr__性能开销） ──
    @property
    def _lock(self):
        return self._core_service._lock

    @property
    def _paper_account(self):
        return self._core_service._paper_account

    @property
    def _shadow_order_log(self):
        return self._core_service._shadow_order_log

    def are_params_locked(self):
        return self._core_service.are_params_locked()

    def get_shadow_a_params(self):
        return self._core_service.get_shadow_a_params()

    def get_shadow_b_params(self):
        return self._core_service.get_shadow_b_params()

    def get_params_snapshot(self):
        return self._core_service.get_params_snapshot()

    def shutdown(self, wait=True, timeout=10.0):
        return self._core_service.shutdown(wait=wait, timeout=timeout)

    def validate_dependencies(self):
        return self._core_service.validate_dependencies()

    def relock_params(self, new_yaml_path=None, caller_id: str = "unknown"):
        return self._core_service.relock_params(new_yaml_path=new_yaml_path, caller_id=caller_id)

    def set_snapshot_collector(self, collector):
        return self._core_service.set_snapshot_collector(collector)

    @staticmethod
    def _compute_sharpe(returns, annualize_factor=252, risk_free_rate=None):
        if risk_free_rate is None:
            from ali2026v3_trading.infra.shared_utils import DEFAULT_RISK_FREE_RATE
            risk_free_rate = DEFAULT_RISK_FREE_RATE
        from ali2026v3_trading.strategy._shadow_strategy_pnl_metrics import ShadowStrategyPnLMetricsService
        return ShadowStrategyPnLMetricsService._compute_sharpe(returns, annualize_factor, risk_free_rate)

    @staticmethod
    def _compute_max_drawdown(equity_curve):
        from ali2026v3_trading.strategy._shadow_strategy_pnl_metrics import ShadowStrategyPnLMetricsService
        return ShadowStrategyPnLMetricsService._compute_max_drawdown(equity_curve)

    @staticmethod
    def _compute_expected_value(trades):
        from ali2026v3_trading.strategy._shadow_strategy_pnl_metrics import ShadowStrategyPnLMetricsService
        return ShadowStrategyPnLMetricsService._compute_expected_value(trades)

    @staticmethod
    def _equity_to_returns(equity_curve):
        from ali2026v3_trading.strategy._shadow_strategy_pnl_metrics import ShadowStrategyPnLMetricsService
        return ShadowStrategyPnLMetricsService._equity_to_returns(equity_curve)



def get_shadow_strategy_engine(**kwargs) -> ShadowStrategyEngine:
    """工厂函数：获取影子策略引擎全局单例（唯一构造路径）

    线程安全，首次调用时创建实例，后续调用返回同一实例。
    单例状态存储在 _shadow_internals 中，本模块不持有可变状态。
    """
    with _internals._shadow_engine_lock:
        if _internals._shadow_engine is None:
            if getattr(_internals, '_shadow_engine_init_failed', False):
                return None
            try:
                _internals._shadow_engine = ShadowStrategyEngine(**kwargs)
            except Exception as _init_err:
                import logging
                logging.error("[ShadowStrategyEngine] 单例创建失败: %s", _init_err)
                _internals._shadow_engine_init_failed = True
                return None
        return _internals._shadow_engine


__all__ = [
    "ShadowStrategyEngine",
    "ShadowTradeRecord",
    "AlphaMetrics",
    "ShadowParamsSnapshot",
    "get_shadow_strategy_engine",
]
