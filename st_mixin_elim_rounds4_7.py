# MODULE_ID: M2-421
import sys
import os
import inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.storage_data_write_mixin import StorageDataWriteService, _StorageDataWriteMixin
from ali2026v3_trading.data.storage_snapshot_mixin import StorageSnapshotService, _StorageSnapshotMixin
from ali2026v3_trading.data.storage_core import StorageWalService, _StorageWalMixin
from ali2026v3_trading.data.width_cache_state_mixin import WidthCacheStateService, _WidthCacheStateMixin
from ali2026v3_trading.data.width_cache_types import WidthCacheSortService, _WidthCacheSortMixin
from ali2026v3_trading.data.width_cache_query_mixin import WidthCacheQueryService, _WidthCacheQueryMixin
from ali2026v3_trading.data.width_cache_types import WidthCacheContextMixin, _WidthCacheContextMixin
from ali2026v3_trading.data.width_cache import WidthStrengthCache
from ali2026v3_trading.infra.subscription_service import (
    SubscriptionCoreService, _SubscriptionCoreMixin,
    SubscriptionInstrumentService, _SubscriptionInstrumentMixin,
    SubscriptionWALService, _SubscriptionWALMixin,
)
from ali2026v3_trading.infra.subscription_service import SubscriptionManager
from ali2026v3_trading.governance.mode_engine_propagation import ModeEnginePropagationService, ModeEnginePropagationMixin
from ali2026v3_trading.governance.mode_engine import ModeEngine
from ali2026v3_trading.order.order_base import OrderQueryService, OrderQueryMixin
from ali2026v3_trading.order.order_service import OrderService
from ali2026v3_trading.strategy.shadow_strategy_core import ShadowStrategyCoreService, ShadowStrategyCoreMixin
from ali2026v3_trading.strategy._shadow_strategy_signal import ShadowStrategySignalService, ShadowStrategySignalMixin
from ali2026v3_trading.strategy.shadow_strategy_pnl import ShadowStrategyPnLService, ShadowStrategyPnLMixin
from ali2026v3_trading.strategy.shadow_strategy_pnl import ShadowStrategyPnLService, ShadowStrategyPnLMixin, ShadowStrategyPnLMetricsService, ShadowStrategyPnLMetricsMixin
from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
from ali2026v3_trading.strategy.box_spring_detector import BoxSpringDetectorService, BoxSpringDetectorMixin
from ali2026v3_trading.strategy.box_spring_executor import BoxSpringExecutorService, BoxSpringExecutorMixin
from ali2026v3_trading.strategy.box_spring_strategy_impl import BoxSpringStrategy
from ali2026v3_trading.strategy.strategy_ecosystem._core import CapitalRoutingService, CapitalRoutingMixin
from ali2026v3_trading.strategy.lifecycle_service import StrategyLifecycleService
from ali2026v3_trading.strategy.strategy_ecosystem.services import TradingEVService, TradingEVMixin
from ali2026v3_trading.strategy.strategy_monitoring_layer import MonitoringService, MonitoringMixin
from ali2026v3_trading.strategy.strategy_ecosystem.services import GovernanceService, GovernanceMixin
from ali2026v3_trading.strategy.strategy_ecosystem.services import OpsService, OpsMixin
from ali2026v3_trading.strategy.strategy_ecosystem._core import StrategyEcosystem


def test_data_write_service():
    assert StorageDataWriteService is not None
    assert _StorageDataWriteMixin is StorageDataWriteService

def test_snapshot_service():
    assert StorageSnapshotService is not None
    assert _StorageSnapshotMixin is StorageSnapshotService

def test_wal_service():
    assert StorageWalService is not None
    assert _StorageWalMixin is StorageWalService

def test_width_cache_state_service():
    assert WidthCacheStateService is not None
    assert _WidthCacheStateMixin is WidthCacheStateService

def test_width_cache_sort_service():
    assert WidthCacheSortService is not None
    assert _WidthCacheSortMixin is WidthCacheSortService

def test_width_cache_query_service():
    assert WidthCacheQueryService is not None
    assert _WidthCacheQueryMixin is WidthCacheQueryService

def test_width_cache_context_service():
    assert WidthCacheContextMixin is not None
    assert _WidthCacheContextMixin is WidthCacheContextMixin

def test_width_strength_cache_no_mixin_inheritance():
    bases = WidthStrengthCache.__bases__
    assert _WidthCacheContextMixin not in bases
    assert _WidthCacheSortMixin not in bases
    assert _WidthCacheQueryMixin not in bases

def test_subscription_services():
    assert SubscriptionCoreService is not None
    assert _SubscriptionCoreMixin is SubscriptionCoreService
    assert SubscriptionInstrumentService is not None
    assert _SubscriptionInstrumentMixin is SubscriptionInstrumentService
    assert SubscriptionWALService is not None
    assert _SubscriptionWALMixin is SubscriptionWALService

def test_subscription_manager_no_mixin_inheritance():
    bases = SubscriptionManager.__bases__
    assert _SubscriptionCoreMixin not in bases
    assert _SubscriptionInstrumentMixin not in bases
    assert _SubscriptionWALMixin not in bases

def test_mode_engine_no_mixin_inheritance():
    bases = ModeEngine.__bases__
    assert ModeEnginePropagationMixin not in bases

def test_order_service_no_mixin_inheritance():
    bases = OrderService.__bases__
    assert OrderQueryMixin not in bases

def test_shadow_engine_no_mixin_inheritance():
    bases = ShadowStrategyEngine.__bases__
    assert ShadowStrategyCoreService not in bases
    assert ShadowStrategySignalMixin not in bases
    assert ShadowStrategyPnLMixin not in bases

def test_box_spring_no_mixin_inheritance():
    bases = BoxSpringStrategy.__bases__
    assert BoxSpringDetectorMixin not in bases
    assert BoxSpringExecutorMixin not in bases

def test_ecosystem_no_mixin_inheritance():
    bases = StrategyEcosystem.__bases__
    assert CapitalRoutingMixin not in bases
    assert TradingEVMixin not in bases
    assert MonitoringMixin not in bases
    assert GovernanceMixin not in bases
    assert OpsMixin not in bases

def test_all_facades_have_getattr():
    assert hasattr(WidthStrengthCache, '__getattr__')
    assert hasattr(SubscriptionManager, '__getattr__')
    assert hasattr(ModeEngine, '__getattr__')
    assert hasattr(OrderService, '__getattr__')
    assert hasattr(ShadowStrategyEngine, '__getattr__')
    assert hasattr(BoxSpringStrategy, '__getattr__')
    assert hasattr(StrategyEcosystem, '__getattr__')


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_') and callable(v)]
    for t in tests:
        t()
    print(f"ALL {len(tests)} ASSERTIONS PASSED for Rounds 4-7 (full Mixin elimination)")