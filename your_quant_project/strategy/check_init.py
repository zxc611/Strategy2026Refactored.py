from .platform_compat import PlatformCompatMixin
from .position_manager import PositionManagerMixin
from .context_utils import ContextMixin
from .market_calendar import MarketCalendarMixin
from .param_table import ParamTableMixin
from .instruments import InstrumentLoaderMixin
from .kline_manager import KlineManagerMixin
from .subscriptions import SubscriptionMixin
from .emergency_pause import EmergencyPauseMixin
from .scheduler_utils import SchedulerMixin
from .calculation import OptionWidthCalculationMixin
from .order_execution import OrderExecutionMixin
from .trading_logic import TradingLogicMixin
from .validation import ValidationMixin

mixins = [
    PlatformCompatMixin,
    # PositionManagerMixin if available,
    ContextMixin,
    MarketCalendarMixin,
    ParamTableMixin,
    InstrumentLoaderMixin,
    KlineManagerMixin,
    SubscriptionMixin,
    EmergencyPauseMixin,
    SchedulerMixin,
    OptionWidthCalculationMixin,
    OrderExecutionMixin,
    TradingLogicMixin,
    ValidationMixin
]

for mixin in mixins:
    if hasattr(mixin, '__init__'):
        print(f"{mixin.__name__} has __init__")
    else:
        print(f"{mixin.__name__} has NO __init__")
