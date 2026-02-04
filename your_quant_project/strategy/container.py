"""
Strategy Container Logic (Fixed & Solidified).
Encapsulates the core Strategy2026Refactored class and its initialization to prevent regression in entry scripts.
"""
import sys
import os
import types
import threading
import collections
import traceback
from datetime import datetime
from typing import Dict, Any, List

# [Robust Import] Ensure your_quant_project is importable
# This relies on the entry point setting up sys.path correctly but adds a fallback
current_file = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file))) # .../demo
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# [Logic] Import Mixins from Package (Absolute Imports Preferred)
try:
    from your_quant_project.strategy.market_calendar import MarketCalendarMixin
    from your_quant_project.strategy.param_table import ParamTableMixin
    from your_quant_project.strategy.emergency_pause import EmergencyPauseMixin
    from your_quant_project.strategy.scheduler_utils import SchedulerMixin
    from your_quant_project.strategy.calculation import OptionWidthCalculationMixin
    from your_quant_project.strategy.trading_logic import TradingLogicMixin
    from your_quant_project.strategy.validation import ValidationMixin
    from your_quant_project.strategy.order_execution import OrderExecutionMixin
    from your_quant_project.strategy.platform_compat import PlatformCompatMixin
    from your_quant_project.strategy.context_utils import ContextMixin
    from your_quant_project.strategy.position_manager import PositionManager
    from your_quant_project.strategy.params import Params
    from your_quant_project.strategy.data_container import DataStrategyContainer # [Data Container]
    
    # [Additional Mixins for "24 Files" Completeness]
    from your_quant_project.strategy.instruments import InstrumentLoaderMixin    # instruments.py
    from your_quant_project.strategy.kline_manager import KlineManagerMixin      # kline_manager.py
    from your_quant_project.strategy.subscriptions import SubscriptionMixin      # subscriptions.py

    # [UI] Try package UI, fallback to local if needed, but prefer package
    try:
        from your_quant_project.strategy.ui_mixin import UIMixin
    except ImportError:
        # If running from inside package without context, this might fail, try fallback
        from ui_mixin import UIMixin
except ImportError:
    # Fallback to relative imports if absolute fails (e.g. if package not in path)
    from .market_calendar import MarketCalendarMixin  # type: ignore
    from .param_table import ParamTableMixin  # type: ignore
    from .emergency_pause import EmergencyPauseMixin  # type: ignore
    from .scheduler_utils import SchedulerMixin  # type: ignore
    from .calculation import OptionWidthCalculationMixin  # type: ignore
    from .trading_logic import TradingLogicMixin  # type: ignore
    from .validation import ValidationMixin  # type: ignore
    from .order_execution import OrderExecutionMixin  # type: ignore
    from .platform_compat import PlatformCompatMixin  # type: ignore
    from .context_utils import ContextMixin  # type: ignore
    from .position_manager import PositionManager  # type: ignore
    from .params import Params  # type: ignore
    from .data_container import DataStrategyContainer  # type: ignore
    from .instruments import InstrumentLoaderMixin  # type: ignore
    from .kline_manager import KlineManagerMixin  # type: ignore
    from .subscriptions import SubscriptionMixin  # type: ignore
    from .ui_mixin import UIMixin  # type: ignore

# [Pythongo Support]
try:
    import pythongo
    from pythongo.base import BaseStrategy
    from pythongo.classdef import TickData
    from pythongo.core import MarketCenter
    from pythongo.utils import Scheduler
except ImportError:
    # Fallback/Mock for Dev check
    class BaseStrategy: 
        def __init__(self, *args, **kwargs): pass
        def on_init(self, *args, **kwargs): pass
        def on_start(self): pass
        def on_stop(self): pass
        def on_pause(self): pass
        def on_resume(self): pass
        def output(self, msg, **kwargs): print(msg)

    class MarketCenter: 
        def get_instrument_data(self, *args): return None
    class Scheduler: 
        def __init__(self, *args): pass
    TickData = Any

DEFAULT_MONTH_MAPPING = {
    "IF": ["IF2602", "IF2603"],
    "IH": ["IH2602", "IH2603"],
    "IM": ["IM2602", "IM2603"],
    "CU": ["CU2603", "CU2604"],
    "AL": ["AL2603", "AL2604"],
    "ZN": ["ZN2603", "ZN2604"],
    "RB": ["RB2603", "RB2604"],
    "AU": ["AU2603", "AU2604"],
    "AG": ["AG2603", "AG2604"],
    "M": ["M2603", "M2605"],
    "Y": ["Y2603", "Y2605"],
    "A": ["A2603", "A2605"],
    "JM": ["JM2604", "JM2605"],
    "I": ["I2603", "I2605"],
    "J": ["J2604", "J2605"],
    "CF": ["CF2603", "CF2605"],
    "SR": ["SR2603", "SR2605"],
    "MA": ["MA2603", "MA2605"],
    "TA": ["TA2603", "TA2605"],
}

class StrategyContainer(
    UIMixin,
    PlatformCompatMixin, 
    ContextMixin,        
    ParamTableMixin, 
    DataStrategyContainer, # [Encapsulated Data Module]
    EmergencyPauseMixin,
    InstrumentLoaderMixin, # [Added]
    KlineManagerMixin,     # [Added]
    SubscriptionMixin,     # [Added]
    MarketCalendarMixin,   # [Added]
    SchedulerMixin,
    OptionWidthCalculationMixin,
    OrderExecutionMixin, 
    TradingLogicMixin,
    ValidationMixin,
    BaseStrategy 
):
    """
    Solidified Strategy Logic Container.
    """
    
    Params = Params

    def __init__(self, *args, **kwargs):
        # [Core] Thread Safety
        self.data_lock = threading.RLock()
        
        # [Lifecycle] State Init
        self.my_state = "initializing"
        self.my_is_running = False
        self.my_is_paused = False
        self.my_trading = True
        self.my_destroyed = False
        
        # [Data Params] Handled by DataStrategyContainer
        # managed_instruments, future_instruments etc will be ensured by super().__init__ or DataStrategyContainer.__init__ manually called if needed?
        # Python MRO handles init if we call super().__init__() correctly, 
        # But BaseStrategy might not support cooperative multiple inheritance well if it doesn't call super().__init__
        # So we explicitly call DataStrategyContainer init logic or just rely on attributes being available.
        # Actually in Python MRO, we should ideally call super().__init__.
        
        self.option_width_results = collections.defaultdict(dict)
        self.zero_price_logged = set()
        self.kline_outdated_logged = set()
        self.scheduler_handle = None 

        # [Lifecycle] Base Init
        try:
            # We need to manually invoke DataContainer init to be safe about variables
            DataStrategyContainer.__init__(self) 
            super().__init__()
        except Exception as e:
            self._force_log(f"super().__init__ error: {e}")

        self.signal_times = {}
        self.history_retry_count = 0 

        # [Lifecycle] Params Init
        if (not hasattr(self, "params")) or (self.params is None):
            props = kwargs.get("props")
            if props and hasattr(props, "params"):
                self.params = props.params
            elif len(args) > 0 and hasattr(args[0], "params"):
                self.params = args[0].params
            else:
                 try:
                     self.params = Params()
                 except Exception:
                     self.params = None

        # [Core] 日志路径
        self.log_file_path = getattr(self.params, "log_file_path", "strategy_startup.log") or "strategy_startup.log"

        # [Core] 默认月份映射
        try:
            if not getattr(self.params, "month_mapping", None):
                self.params.month_mapping = DEFAULT_MONTH_MAPPING.copy()
        except Exception:
            pass

        # [Core] 尝试提前注入 API Key
        try:
            if hasattr(self, "_load_api_key"):
                self._load_api_key()
            elif hasattr(self, "load_api_key"):
                self.load_api_key(self.params)
        except Exception:
            pass

        # [Core] MarketCenter / Scheduler 初始化
        # Skipped MarketCenter here as it is handled by DataStrategyContainer
        skip_core = os.getenv("PYSTRATEGY_DIAG_SKIP_CORE") == "1"
        if not skip_core:
            try:
                if "Scheduler" in globals() and Scheduler:
                    self.scheduler = Scheduler("PythonGO")
            except Exception as e:
                self._force_log(f"Scheduler init error: {e}")

        # [Core] 订阅节流配置
        self.subscription_queue = []
        # [Robust Init] Use explicit None check or default
        p_batch = getattr(self.params, "subscription_batch_size", 10)
        self.subscription_batch_size = p_batch if p_batch is not None else 10
        
        p_interval = getattr(self.params, "subscription_interval", 1)
        self.subscription_interval = p_interval if p_interval is not None else 1
        
        self.subscription_backoff_factor = getattr(self.params, "subscription_backoff_factor", 1.0)
        self.subscription_job_ids = set()

        self._force_log("StrategyContainer Instantiated [Fixed]")

    # -------------------------------------------------------------------------
    # Properties for Framework Compatibility
    # -------------------------------------------------------------------------
    @property
    def is_running(self) -> bool:
        return getattr(self, "my_is_running", False)

    @property
    def is_paused(self) -> bool:
        return getattr(self, "my_is_paused", False)

    @property
    def is_trading(self) -> bool:
        return getattr(self, "my_trading", False)

    def on_init(self, *args, **kwargs):
        """策略初始化"""
        try:
            super().on_init(*args, **kwargs)
        except Exception:
            pass

        self.output(">>> StrategyContainer Initializing...", force=True)
        
        if hasattr(self, "_init_calculation_state"): self._init_calculation_state()
        if hasattr(self, "_init_order_execution"): self._init_order_execution()
        if hasattr(self, "_init_trading_logic"): self._init_trading_logic()

        # [Source Parity] 初始化 14:58/15:58 时间点
        try:
            from datetime import time as dtime
            def _parse_time(s: str, fallback: str) -> dtime:
                try:
                    hh, mm = s.split(":")
                    return dtime(int(hh), int(mm), 0)
                except Exception:
                    hh, mm = fallback.split(":")
                    return dtime(int(hh), int(mm), 0)

            close_overnight_time = getattr(self.params, "close_overnight_check_time", "14:58")
            close_daycut_time = getattr(self.params, "close_daycut_time", "15:58")
            self.TIME_CHECK_OVERNIGHT = _parse_time(str(close_overnight_time), "14:58")
            self.TIME_CLOSE_POSITIONS = _parse_time(str(close_daycut_time), "15:58")
        except Exception:
            pass

        if hasattr(self, "_load_param_table"):
             try:
                 data = self._load_param_table()
                 if data and isinstance(data, dict) and hasattr(self, "params"):
                     for k, v in data.items():
                         try: setattr(self.params, k, v)
                         except: pass
                     self.output(f"ParamTable loaded {len(data)} items")
             except Exception as e:
                 self.output(f"ParamTable load fail: {e}")

        if not self.validate_environment():
            self.output("Environment validation failed!")
            return
        
        if hasattr(self, "init_emergency_pause"): self.init_emergency_pause()
        
        if PositionManager:
            self.position_manager = PositionManager(self)
        else:
            self.position_manager = None
            self.output("Warning: PositionManager missing")

        self.output(">>> Initialization Complete.")

    def on_start(self):
        """策略启动"""
        try:
            super().on_start()
        except: pass
        self.output(">>> Strategy Starting...")
        self.my_is_running = True
        self.my_started = True
        self.my_is_paused = False
        self.my_trading = True
        self.my_state = "running"
        
        if hasattr(self, "_load_param_table"): self._load_param_table()

        # [UI] Start Dashboard
        try:
            if getattr(self.params, "enable_output_mode_ui", True) and not (getattr(self, "_ui_running", False) or getattr(self, "_ui_creating", False)):
                self._start_output_mode_ui()
        except Exception as e:
            self.output(f"Failed to start UI: {e}", force=True)

        # [ASYNC STARTUP]
        def _async_startup_task():
            try:
                # 1. Load Instruments
                self.output(">>> [Async] Loading Instruments...")
                if hasattr(self, "load_all_instruments"): 
                    self.load_all_instruments()
                elif hasattr(self, "load_instruments"): 
                    self.load_instruments()
                self.output(">>> [Async] Instruments Loaded.")

                # 2. Subscribe Data
                self.output(">>> [Async] Subscribing Market Data...")
                if hasattr(self, "_subscribe_in_batches"): 
                    self._subscribe_in_batches()
                elif hasattr(self, "subscribe_market_data"):
                    self.subscribe_market_data(subscribe_options=getattr(self.params, "subscribe_options", True))
                self.output(">>> [Async] Market Data Subscribed.")

                # 3. Load History
                if bool(getattr(self.params, "auto_load_history", True)):
                    self.output(">>> [Async] Loading History...")
                    if hasattr(self, "load_historical_klines"): 
                        self.load_historical_klines()
                    self.output(">>> [Async] History Loaded.")
            except Exception as e:
                self.output(f">>> [Async] Startup Error: {e}")
                traceback.print_exc()

        threading.Thread(target=_async_startup_task, daemon=True, name="StrategyAsyncStartup").start()

        # Scheduler
        if hasattr(self, "_safe_add_periodic_job"):
            self._safe_add_periodic_job("run_trading_cycle", self.run_trading_cycle, interval_seconds=getattr(self.params, "calculation_interval", 3))
            self._safe_add_periodic_job("check_chase_tasks", self.check_active_chase_tasks, interval_seconds=1)
            # Source parity: second timer for 14:58/15:58 checks
            self._safe_add_periodic_job("second_timer", self._on_second_timer, interval_seconds=1)

        self.output(">>> Strategy Started (Async Startup in Progress).")

    def _on_second_timer(self) -> None:
        """每秒定时器（对齐源策略规则4&5、规则3&6）"""
        try:
            now = datetime.now()
            current_time = now.time()

            # 规则4&5：14:58检查隔夜仓
            if hasattr(self, "TIME_CHECK_OVERNIGHT") and current_time >= self.TIME_CHECK_OVERNIGHT:
                if not hasattr(self, "last_check_date_1458"):
                    self.last_check_date_1458 = None
                if self.last_check_date_1458 != now.date():
                    if hasattr(self, "_check_overnight_positions_1458"):
                        self._check_overnight_positions_1458()
                    self.last_check_date_1458 = now.date()

            # 规则3&6：15:58执行平仓
            if hasattr(self, "TIME_CLOSE_POSITIONS") and current_time >= self.TIME_CLOSE_POSITIONS:
                if not hasattr(self, "last_check_date_1558"):
                    self.last_check_date_1558 = None
                if self.last_check_date_1558 != now.date():
                    if hasattr(self, "_execute_1558_closing"):
                        self._execute_1558_closing()
                    self.last_check_date_1558 = now.date()
        except Exception as e:
            try:
                self.output(f"异常：定时器执行失败 - {e}")
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Lifecycle Overrides
    # -------------------------------------------------------------------------
    def pause(self, *args, **kwargs):
        """收到平台暂停指令，强制触发 on_pause"""
        try:
             # [Fix] Use thread-safe logging
             print(">>> Strategy Pause Requested (Thread-Safe).")
             self.on_pause()
        except Exception: 
             pass

    def resume(self, *args, **kwargs):
        """收到平台恢复指令，强制触发 on_resume"""
        try:
             # [Fix] Use thread-safe logging
             print(">>> Strategy Resume Requested (Thread-Safe).")
             self.on_resume()
        except Exception:
             pass

    def on_pause(self):
        """策略暂停回调。"""
        # [Fix] Set flags FIRST to stop logic immediately
        self.my_is_paused = True
        self.my_trading = False
        self.my_state = "paused"
        
        # [Fix] Avoid UI interaction from background thread if possible
        # print to console is safer than self.output which involves Tkinter
        print(">>> Strategy Pause Signal Received (Flags Set).")

        try:
            super().on_pause()
        except: 
            pass

    def on_resume(self):
        """策略恢复回调。"""
        self.my_is_paused = False
        self.my_trading = True
        self.my_state = "running"
        print(">>> Strategy Resume Signal Received.")
        
        try: 
            super().on_resume()
        except: 
            pass
        self.output(">>> Strategy Resumed (Trading Enabled).")

    def on_stop(self):
        try: super().on_stop()
        except: pass

        self.output(">>> Strategy Stopped.")
        self.my_is_running = False
        self.my_started = False
        self.my_destroyed = True
        
        # [Lifecycle] Explicit Cleanup
        try:
            if hasattr(self, "stop_scheduler"):
                self.stop_scheduler()
            # [SafePause] Thread-safe UI Cleanup
            if hasattr(self, "_ui_queue"):
                # Signal the UI thread to destroy itself.
                # Assuming _ui_queue consumer can handle 'destroy' or similar, 
                # but 'ui_mixin' checks for WM_DELETE_WINDOW.
                # We should NOT call .destroy() directly from here (StrategyThread)!
                pass 
                # self._ui_root.destroy() <- UNSAFE
        except Exception:
            pass

    def _is_instrument_allowed(self, instrument_id: str, exchange: str = "") -> bool:
         # Delegate to InstrumentLoaderMixin/ValidationMixin, but they are in MRO.
         # This method is effectively provided by ValidationMixin or InstrumentLoaderMixin
         # If not found, use super
         if hasattr(super(), "_is_instrument_allowed"):
             return super()._is_instrument_allowed(instrument_id, exchange)
         return True
    
    def _is_symbol_current_or_next(self, symbol: str) -> bool:
        """[HOTFIX] 兼容旧命名"""
        if hasattr(self, '_is_symbol_specified_or_next'):
            return self._is_symbol_specified_or_next(symbol)
        else:
            return False

    def on_kline(self, kline: Any) -> None:
        """K线数据回调"""
        try:
            paused = getattr(self, "my_is_paused", False)
            if (not getattr(self, "my_is_running", False)) or paused or (getattr(self, "my_trading", True) is False) or getattr(self, "my_destroyed", False):
                return
            
            if hasattr(self, "_instruments_ready") and not getattr(self, "_instruments_ready", False):
                return

            frequency = getattr(kline, "style", getattr(self.params, "kline_style", "M1"))
            
            # 1. Update Data (KlineManagerMixin)
            if hasattr(self, "_process_kline_data"):
                self._process_kline_data(kline.exchange, kline.instrument_id, frequency, kline)
            
            # 2. Trigger Calculation (OptionWidthCalculationMixin)
            if hasattr(self, "_trigger_width_calc_for_kline"):
                self._trigger_width_calc_for_kline(kline)
                
        except Exception as e:
            pass

    def on_tick(self, tick: Any):
        if hasattr(super(), "on_tick"):
             super().on_tick(tick)
        elif TradingLogicMixin:
             TradingLogicMixin.on_tick(self, tick)

    def _force_log(self, msg):
        try:
            p = os.path.join(os.path.dirname(__file__), "strategy_failover.log")
            with open(p, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] {msg}\n")
        except: pass

    def _debug(self, msg: str) -> None:
        try:
            debug_on = getattr(self.params, "debug_output", True)
            if debug_on:
                self.output(f"[DEBUG] {msg}")
        except Exception:
            try:
                self.output(msg)
            except Exception:
                pass

    # [SafeOverride] Properties
    @property
    def started(self) -> bool: return getattr(self, "my_started", False)
    @started.setter
    def started(self, value: bool): self.my_started = value
    
    @property
    def is_running(self) -> bool: return getattr(self, "my_is_running", False)
    @is_running.setter
    def is_running(self, value: bool): self.my_is_running = value

    @property
    def running(self) -> bool: return getattr(self, "my_is_running", False)
    @running.setter
    def running(self, value: bool): self.my_is_running = value

    @property
    def is_paused(self) -> bool: return getattr(self, "my_is_paused", False)
    @is_paused.setter
    def is_paused(self, value: bool): self.my_is_paused = value

    @property
    def paused(self) -> bool: return getattr(self, "my_is_paused", False)
    @paused.setter
    def paused(self, value: bool): self.my_is_paused = value

    @property
    def trading(self) -> bool: return getattr(self, "my_trading", True)
    @trading.setter
    def trading(self, value: bool): self.my_trading = value

    @property
    def destroyed(self) -> bool: return getattr(self, "my_destroyed", False)
    @destroyed.setter
    def destroyed(self, value: bool): self.my_destroyed = value

    @property
    def state(self) -> str: return getattr(self, "my_state", "stopped")
    @state.setter
    def state(self, value: str): self.my_state = value
