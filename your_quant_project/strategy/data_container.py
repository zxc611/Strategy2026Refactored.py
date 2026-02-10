"""
Data Container Module.
Encapsulates all market data reading, loading, and subscription logic (Instruments, KLines, Calendar, Subscriptions).
"""
import os
import types
import collections
from typing import Dict, Any, List

try:
    from .market_calendar import MarketCalendarMixin
    from .instruments import InstrumentLoaderMixin
    from .kline_manager import KlineManagerMixin
    from .subscriptions import SubscriptionMixin
    from .context_utils import ContextMixin 
    from .platform_compat import PlatformCompatMixin
except ImportError:
    # Fallback to local
    from market_calendar import MarketCalendarMixin
    from instruments import InstrumentLoaderMixin
    from kline_manager import KlineManagerMixin
    from subscriptions import SubscriptionMixin
    from context_utils import ContextMixin
    from platform_compat import PlatformCompatMixin

# MarketCenter Access
try:
    from pythongo.core import MarketCenter
except ImportError:
    class MarketCenter:
        def get_instrument_data(self, *args): return None

class DataStrategyContainer(
    MarketCalendarMixin,
    InstrumentLoaderMixin,
    KlineManagerMixin,
    SubscriptionMixin
):
    """
    Encapsulated Data Strategy Container.
    Handles initialization of MarketCenter and data stores.
    """

    def __init__(self, *args, **kwargs):
        # [Data Store Properties]
        # These are used by Mixins. Initializing here to ensure they exist.
        if not hasattr(self, "managed_instruments"): self.managed_instruments = [] 
        if not hasattr(self, "future_instruments"): self.future_instruments = []
        if not hasattr(self, "option_instruments"): self.option_instruments = {}
        if not hasattr(self, "future_symbol_to_exchange"): self.future_symbol_to_exchange = {}
        if not hasattr(self, "futures_without_options"): self.futures_without_options = set()
        if not hasattr(self, "subscribed_instruments"): self.subscribed_instruments = set()
        if not hasattr(self, "kline_data"): self.kline_data = {}
        if not hasattr(self, "latest_ticks"): self.latest_ticks = {}
        if not hasattr(self, "kline_insufficient_logged"): self.kline_insufficient_logged = set()
        if not hasattr(self, "_insufficient_option_kline_logged"): self._insufficient_option_kline_logged = set() # NEW
        
        # [Critical] Avoid overriding platform-provided market_center
        if not hasattr(self, "market_center"):
            self.market_center = None

        # [Core] MarketCenter Init
        self._init_market_center_safe()
        
        # [Chain] Continue initialization chain
        super().__init__(*args, **kwargs)
        
    def _init_market_center_safe(self):
        """Safe initialization and patching of MarketCenter."""
        skip_core = os.getenv("PYSTRATEGY_DIAG_SKIP_CORE") == "1"
        if not skip_core:
            try:
                # Check if MarketCenter is available in globals or imports
                # In this scope, MarketCenter definition is at module level.
                # But we want the real one if integrated.
                
                # If we are inheriting from a BaseStrategy that has market_center, 
                # we usually don't need to do much, but the patching is critical.
                
                if "MarketCenter" in globals() and MarketCenter:
                    # Don't overwrite if existing (unless it's None)
                    if not hasattr(self, "market_center") or self.market_center is None:
                         # We can't really instantiate MarketCenter blindly if it requires context?
                         # Usually it's instantiated by the platform/framework or we instantiate it.
                         # Based on previous code, we instantiate it.
                         try:
                             self.market_center = MarketCenter()
                         except Exception:
                             pass

                    if hasattr(self, "market_center") and self.market_center:
                        try:
                            # Patch get_instrument_data
                            original_gid = getattr(self.market_center, "get_instrument_data", None)
                            if original_gid:
                                def _gid_wrapper(mc_self, exchange=None, instrument_id=None, *args, **kwargs):
                                    try:
                                        return original_gid(exchange, instrument_id)
                                    except TypeError:
                                        try:
                                            return original_gid()
                                        except Exception:
                                            return None
                                self.market_center.get_instrument_data = types.MethodType(_gid_wrapper, self.market_center)
                        except Exception:
                            pass
            except Exception as e:
                if hasattr(self, "output"):
                     self.output(f"MarketCenter init error: {e}")

    def on_init_data(self):
        """Explicit Data Init Hook."""
        pass
