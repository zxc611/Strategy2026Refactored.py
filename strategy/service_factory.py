"""
service_factory.py - ServiceFactory (G2b Mixin消灭)
策略级服务工厂 — 确保每个策略获得独立的服务实例（PA-03隔离）

职责：
- 创建和缓存策略级服务实例（KlineDataService, TickProcessingService 等）
- 管理命名空间隔离（shadow/normal）
- 提供懒初始化的便捷属性访问
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from ali2026v3_trading.strategy.recovery_service import RecoveryService
from ali2026v3_trading.strategy.persistence_service import CheckpointService


class ServiceFactory:
    """策略级服务工厂 — 确保每个策略获得独立的服务实例（PA-03隔离）"""

    def __init__(
        self,
        strategy_id: str,
        state_store: Optional[Any] = None,
        callback_group: Optional[Any] = None,
        is_shadow: bool = False,
        is_backtest: bool = False,
    ):
        self._strategy_id = strategy_id
        self._state_store = state_store
        self._callback_group = callback_group
        self._is_shadow = is_shadow
        self._is_backtest = is_backtest
        self._namespace = f"{strategy_id}_shadow" if is_shadow else strategy_id
        # Lazy-created service instances
        self._kline_service = None
        self._tick_service = None
        self._recovery_service = None
        self._checkpoint_service = None
        self._lifecycle_service = None
        self._instrument_service = None

    @property
    def namespace(self) -> str:
        return self._namespace

    def create_kline_service(self) -> Any:
        """创建KlineDataService实例"""
        from ali2026v3_trading.strategy.kline_data_service import KlineDataService
        svc = KlineDataService(
            state_store=self._state_store,
            callback_group=self._callback_group,
            strategy_id=self._strategy_id,
            is_backtest=self._is_backtest,
        )
        self._kline_service = svc
        return svc

    def create_tick_processing_service(self, **fns) -> Any:
        """创建TickProcessingService实例

        Args:
            **fns: 注入回调函数，参见TickProcessingService.__init__参数
        """
        from ali2026v3_trading.strategy.tick_processing_service import TickProcessingService
        svc = TickProcessingService(
            state_store=self._state_store,
            callback_group=self._callback_group,
            strategy_id=self._strategy_id,
            is_backtest=self._is_backtest,
            **fns,
        )
        self._tick_service = svc
        return svc

    def create_recovery_service(self, **fns) -> RecoveryService:
        """创建RecoveryService实例

        Args:
            **fns: 注入回调函数，参见RecoveryService.__init__参数
        """
        svc = RecoveryService(
            strategy_id=self._strategy_id,
            state_store=self._state_store,
            **fns,
        )
        self._recovery_service = svc
        return svc

    def create_checkpoint_service(self, **fns) -> CheckpointService:
        """创建CheckpointService实例

        Args:
            **fns: 注入回调函数，参见CheckpointService.__init__参数
        """
        svc = CheckpointService(
            strategy_id=self._strategy_id,
            state_store=self._state_store,
            **fns,
        )
        self._checkpoint_service = svc
        return svc

    def create_lifecycle_service(self, provider=None) -> Any:
        """创建LifecycleService实例

        Args:
            provider: 策略实例引用（用于访问共享状态）
        """
        from ali2026v3_trading.strategy.lifecycle_service import LifecycleService
        svc = LifecycleService(provider=provider, strategy_id=self._strategy_id)
        self._lifecycle_service = svc
        return svc

    def create_instrument_service(self) -> Any:
        """创建InstrumentService实例"""
        from ali2026v3_trading.strategy.instrument_service import InstrumentService
        svc = InstrumentService()
        self._instrument_service = svc
        return svc

    # -- Convenience getters with lazy init --

    @property
    def kline_service(self) -> Any:
        if self._kline_service is None:
            self._kline_service = self.create_kline_service()
        return self._kline_service

    @property
    def tick_processing_service(self) -> Any:
        if self._tick_service is None:
            self._tick_service = self.create_tick_processing_service()
        return self._tick_service

    @property
    def recovery_service(self) -> RecoveryService:
        if self._recovery_service is None:
            self._recovery_service = self.create_recovery_service()
        return self._recovery_service

    @property
    def checkpoint_service(self) -> CheckpointService:
        if self._checkpoint_service is None:
            self._checkpoint_service = self.create_checkpoint_service()
        return self._checkpoint_service
