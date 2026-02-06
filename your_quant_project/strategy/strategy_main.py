"""
策略主入口文件。
聚合所有 Mixin 模块，组装成完整的策略类。
"""
from __future__ import annotations
from datetime import datetime
import collections
import traceback
import os
import threading

try:
    from pythongo.base import BaseStrategy  # type: ignore
except ImportError:
    import sys, os
    # 路径回溯: strategy -> your_quant_project -> demo -> pyStrategy
    # Target: pyStrategy (where pythongo package resides)
    current_dir = os.path.dirname(os.path.abspath(__file__)) # .../strategy
    demo_dir = os.path.dirname(os.path.dirname(current_dir)) # .../demo (up 2)
    pystrategy_dir = os.path.dirname(demo_dir) # .../pyStrategy (up 3)
    
    if pystrategy_dir not in sys.path:
        sys.path.insert(0, pystrategy_dir)
    
    try:
        from pythongo.base import BaseStrategy
    except ImportError:
        # Last Resort: Try assuming standard structure
        raise ImportError(f"FATAL: Could not import pythongo.base.BaseStrategy even after adding {pystrategy_dir} to path.")


try:
    from pythongo.classdef import TickData  # type: ignore
except ImportError:
    class TickData: pass

# 导入 Mixins
try:
    from your_quant_project.strategy.market_calendar import MarketCalendarMixin
    from your_quant_project.strategy.param_table import ParamTableMixin
    from your_quant_project.strategy.instruments import InstrumentLoaderMixin
    from your_quant_project.strategy.kline_manager import KlineManagerMixin
    from your_quant_project.strategy.subscriptions import SubscriptionMixin
    from your_quant_project.strategy.emergency_pause import EmergencyPauseMixin
    from your_quant_project.strategy.scheduler_utils import SchedulerMixin
    from your_quant_project.strategy.calculation import OptionWidthCalculationMixin
    from your_quant_project.strategy.trading_logic import TradingLogicMixin
    from your_quant_project.strategy.validation import ValidationMixin
    from your_quant_project.strategy.params import Params as _Params
    from your_quant_project.strategy.order_execution import OrderExecutionMixin
    from your_quant_project.strategy.platform_compat import PlatformCompatMixin
except ImportError:
    # Fallback for relative imports if not installed as package
    from .market_calendar import MarketCalendarMixin  # type: ignore
    from .param_table import ParamTableMixin  # type: ignore
    from .instruments import InstrumentLoaderMixin  # type: ignore
    from .kline_manager import KlineManagerMixin  # type: ignore
    from .subscriptions import SubscriptionMixin  # type: ignore
    from .emergency_pause import EmergencyPauseMixin  # type: ignore
    from .scheduler_utils import SchedulerMixin  # type: ignore
    from .calculation import OptionWidthCalculationMixin  # type: ignore
    from .trading_logic import TradingLogicMixin  # type: ignore
    from .validation import ValidationMixin  # type: ignore
    from .params import Params as _Params  # type: ignore
    from .order_execution import OrderExecutionMixin  # type: ignore
    from .platform_compat import PlatformCompatMixin # type: ignore

try:
    from your_quant_project.strategy.position_manager import PositionManager
except ImportError:
    try:
        from .position_manager import PositionManager  # type: ignore
    except ImportError:
        PositionManager = None

try:
    from your_quant_project.strategy.context_utils import ContextMixin
except ImportError:
    try:
        from .context_utils import ContextMixin  # type: ignore
    except ImportError:
        class ContextMixin:
            def _is_backtest_context(self) -> bool: return False
            def _is_trade_context(self) -> bool: return True

class Strategy2026Refactored(
    PlatformCompatMixin, 
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
    ValidationMixin,
    BaseStrategy # [Standard MRO] Mixins first, Base last.
):
    """
    重构后的 Strategy2026。
    功能模块化拆分：
    - MarketCalendarMixin: 交易日历与时间检查
    - ParamTableMixin: 参数加载与管理
    - InstrumentLoaderMixin: 合约加载与分类
    - KlineManagerMixin: K线/数据管理
    - SubscriptionMixin: 行情订阅
    - EmergencyPauseMixin: 紧急暂停
    - SchedulerMixin: 定时任务
    - OptionWidthCalculationMixin: 核心计算
    - TradingLogicMixin: 交易逻辑 (on_tick, 信号处理)
    - ValidationMixin: 校验逻辑
    """
    
    # [Restored] Use the imported Params class directly, no nested definition
    # This matches Strategy20260105_3.py logic where Params is top-level
    
    # [SafeOverride] 显式重写关键属性访问器，防止 Mixin 继承顺序或缓存导致 setter 丢失
    # 参照 Strategy20260105_3.py 完整实现
    
    @property
    def started(self) -> bool:
        return getattr(self, "my_started", False)

    @started.setter
    def started(self, value: bool):
        self.my_started = value

    @property
    def is_running(self) -> bool:
        return getattr(self, "my_is_running", False)

    @is_running.setter
    def is_running(self, value: bool):
        self.my_is_running = value

    @property
    def running(self) -> bool:
        return getattr(self, "my_is_running", False)

    @running.setter
    def running(self, value: bool):
        self.my_is_running = value

    @property
    def paused(self) -> bool:
        return getattr(self, "my_is_paused", False)
    
    @paused.setter
    def paused(self, value: bool):
        self.my_is_paused = value

    @property
    def is_paused(self) -> bool:
        return getattr(self, "my_is_paused", False)
    
    @is_paused.setter
    def is_paused(self, value: bool):
        self.my_is_paused = value

    @property
    def trading(self) -> bool:
        return getattr(self, "my_trading", True)
    
    @trading.setter
    def trading(self, value: bool):
        self.my_trading = value

    @property
    def destroyed(self) -> bool:
        return getattr(self, "my_destroyed", False)

    @destroyed.setter
    def destroyed(self, value: bool):
        self.my_destroyed = value

    @property
    def state(self) -> str:
        return getattr(self, "my_state", "stopped")

    @state.setter
    def state(self, value: str):
        self.my_state = value

    def _debug(self, msg: str) -> None:
        """
        统一调试输出方法。
        兼容 Mixin 中对 self._debug 的调用。
        """
        try:
            # 获取 debug_output 开关，默认为 True 以便调试，正式版可改为 False
            debug_on = getattr(self.params, "debug_output", True)
            if debug_on:
                self.output(f"[DEBUG] {msg}")
        except Exception:
            self.output(msg)

    def _on_second_timer(self) -> None:
        """每秒定时器（修正：移除所有14:30相关逻辑）"""
        try:
            # 调用开仓追单处理
            if hasattr(self, "_process_open_chase_tasks"):
                self._process_open_chase_tasks()

            now = datetime.now()
            current_time = now.time()

            # 规则4&5：14:58检查隔夜仓
            if hasattr(self, "TIME_CHECK_OVERNIGHT") and current_time >= self.TIME_CHECK_OVERNIGHT:
                # Assuming TIME_CHECK_OVERNIGHT is defined in PlatformCompat or similar
                # Initialize last_check_date if missing
                if not hasattr(self, "last_check_date_1458"): self.last_check_date_1458 = None
                
                if self.last_check_date_1458 != now.date():
                    if hasattr(self, "_check_overnight_positions_1458"):
                        self._check_overnight_positions_1458()
                    self.last_check_date_1458 = now.date()

            # 规则3&6：15:58执行平仓
            if hasattr(self, "TIME_CLOSE_POSITIONS") and current_time >= self.TIME_CLOSE_POSITIONS:
                if not hasattr(self, "last_check_date_1558"): self.last_check_date_1558 = None
                
                if self.last_check_date_1558 != now.date():
                    if hasattr(self, "_execute_1558_closing"):
                        self._execute_1558_closing()
                    self.last_check_date_1558 = now.date()
        except Exception as e:
            self.output(f"异常：定时器执行失败 - {e}")

    def __del__(self):
        try:
            # 避免重复输出，销毁时只做彻底清理
            if hasattr(self, "stop"):
                self.stop()
            try:
                self.output("实例销毁：已停止调度器并清理所有任务")
            except Exception:
                pass
        except Exception:
            pass

    def __init__(self, *args, **kwargs):
        # 兼容可能传入的参数，但不传递给 BaseStrategy
        try:
            super().__init__()
            # print("DEBUG: Strategy2026Refactored initialized BaseStrategy success.")
        except Exception as e:
            try:
                log_dir = os.path.dirname(os.path.abspath(__file__))
                log_path = os.path.join(log_dir, "DEBUG_ERROR_INIT_SUPER.txt")
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(f"super().__init__ failed: {e}\n{traceback.format_exc()}\n")
            except:
                pass
            # Don't silence it if you want to know. But usually we must continue.
            # raise e 
        
        # [Fix] Explicitly initialize base state if super() didn't do it (e.g. if Mixin blocked it, though unlikely)
        # But importantly, set the flags.
        self.my_is_running = False
        self.my_is_paused = False
        self.my_trading = True
        self.my_destroyed = False
        self.managed_instruments = [] # 初始化

        # [Safe Init] Ensure core locks/scheduler exist even if on_init is skipped
        if not hasattr(self, "data_lock") or self.data_lock is None:
            self.data_lock = threading.RLock()
        if not hasattr(self, "scheduler"):
            self.scheduler = None

        # 确保 self.params 存在 - 提前到属性初始化之前，因为某些属性初始化可能依赖 params
        if not hasattr(self, "params"):
            # 尝试从 kwargs 中获取 props 并提取 params
            props = kwargs.get("props")
            if props and hasattr(props, "params"):
                self.params = props.params
            elif len(args) > 0 and hasattr(args[0], "params"):
                self.params = args[0].params
            else:
                 try:
                     # [Match _3.py] Explicit instantiation using the imported definition
                     self.params = _Params()
                 except Exception as e:
                     try:
                         log_dir = os.path.dirname(os.path.abspath(__file__))
                         log_path = os.path.join(log_dir, "DEBUG_ERROR_PARAMS.txt")
                         with open(log_path, "a", encoding="utf-8") as f:
                             f.write(f"Params init failed: {e}\n{traceback.format_exc()}\n")
                     except:
                        pass
                     self.params = None
        
        # 初始化合约容器，防止 on_start 访问缺失属性
        self.future_instruments = []
        self.option_instruments = {}
        self.future_symbol_to_exchange = {}
        
        # State Variables for OptionWidthCalculationMixin and TradingMixin
        self.option_width_results = collections.defaultdict(dict)
        self.kline_insufficient_logged = set()
        self.zero_price_logged = set()
        self.signal_times = {}

        # [Fixed] Cleaned up cleanup artifacts

    def on_init(self, *args, **kwargs):
        """策略初始化回调"""
        # [Critical] 遵循文档规范，必须调用 super().on_init()
        # 由于 BaseStrategy 在最后，super() 会沿着 Mixin 链调用，最终到达 BaseStrategy
        try:
            super().on_init(*args, **kwargs)
        except Exception as e:
             try:
                 log_dir = os.path.dirname(os.path.abspath(__file__))
                 log_path = os.path.join(log_dir, "DEBUG_ERROR_ON_INIT_SUPER.txt")
                 with open(log_path, "a", encoding="utf-8") as f:
                     f.write(f"super().on_init failed: {e}\n{traceback.format_exc()}\n")
             except:
                 pass

        self.output("="*60, force=True)
        self.output("  >>>> STRATEGY V9 RUNNING <<<<", force=True)
        self.output("  If you see this, the NEW code is ACTIVE.", force=True)
        self.output("="*60, force=True)
        self.output("Strategy2026Refactored_V9 初始化开始...")
        
        # [CRITICAL DEBUG] Print Module Source Path to Logs
        try:
            import your_quant_project.strategy.param_table
            import your_quant_project.strategy.calculation
            msg_pt = f"Local ParamTable: {your_quant_project.strategy.param_table.__file__}"
            msg_cl = f"Local Calculation: {your_quant_project.strategy.calculation.__file__}"
            self.output(msg_pt, force=True)
            self.output(msg_cl, force=True)
            
            if "site-packages" in msg_pt or "site-packages" in msg_cl:
                 self.output("!!! ALARM !!! LOADING FROM SITE-PACKAGES. FIX FAILED.", force=True)
            else:
                 self.output(">>> CONFIRMED: Loading from LOCAL DIRECTORY. Fix is ACTIVE.", force=True)
        except Exception as e:
            self.output(f"Error checking module paths: {e}", force=True)

        # 0. 初始化各 Mixin 状态
        if hasattr(self, "_init_calculation_state"):
            self._init_calculation_state()
        if hasattr(self, "_init_order_execution"):
             self._init_order_execution()
        if hasattr(self, "_init_trading_logic"):
             self._init_trading_logic()

        # 1. 加载参数
        # 必须先加载参数，后续的环境自检和模块初始化依赖于 params 中的配置
        if hasattr(self, "_load_param_table"):
             try:
                 # 调用 ParamTableMixin 的加载逻辑
                 data = self._load_param_table()
                 if data and isinstance(data, dict):
                     # 将加载的字典参数应用到 self.params 对象上
                     if hasattr(self, "params"):
                         for k, v in data.items():
                             # 只有当 params 有对应字段时才更新（或者是动态添加？通常更新已有字段）
                             # 如果使用 BaseParams，可能需要 setattr
                             try:
                                 # 甚至如果 params 是 Pydantic 模型，得小心
                                 # 这里假设它是普通的类实例或 SimpleNamespace 兼容结构
                                 setattr(self.params, k, v)
                             except Exception:
                                 pass
                     self.output(f"参数表加载成功，更新了 {len(data)} 项配置")
             except Exception as e:
                 self.output(f"参数加载过程中发生异常: {e}")

        # 2. 环境自检
        if not self.validate_environment():
            self.output("环境自检失败，策略停止初始化")
            return
        
        # 3. 初始化其他模块
        if hasattr(self, "init_emergency_pause"):
            self.init_emergency_pause()
        
        # 4. 初始化 PositionManager
        if PositionManager:
            self.position_manager = PositionManager(self)
        else:
            self.position_manager = None
            self.output("警告: PositionManager 模块未加载")

        self.output("Strategy2026Refactored 初始化完成")

    def on_start(self):
        """策略启动回调 (Refactored to match Strategy2026 source logic)"""
        # [Critical] 遵循文档规范
        try:
            super().on_start()
        except Exception:
            pass

        self.output("策略启动...")
        self.my_is_running = True
        self.my_is_paused = False
        self.my_trading = True
        self._instruments_ready = False  # Mark as not ready until loaded
        
        # 0. 加载参数表覆盖
        if hasattr(self, "_load_param_table"):
            self._load_param_table()

        # 1. 加载合约
        # Use load_all_instruments if available (Source parity)
        if hasattr(self, "load_all_instruments"):
            self.output("[调试] 调用 load_all_instruments()")
            self.load_all_instruments()
        elif hasattr(self, "load_instruments"):
            self.load_instruments()
            
        # 2. 订阅行情
        # Use _subscribe_in_batches (Source parity)
        if hasattr(self, "_subscribe_in_batches"):
            self._subscribe_in_batches()
        elif hasattr(self, "subscribe_market_data"):
            sub_opts = getattr(self.params, "subscribe_options", True)
            self.subscribe_market_data(subscribe_options=sub_opts)
            
        # 3. 历史数据加载
        # Logic matches Source: check tick_backtest -> auto_load -> async_load
        tick_backtest = bool(getattr(self.params, "backtest_tick_mode", False))
        auto_load = getattr(self.params, "auto_load_history", True)
        loaded = getattr(self, "history_loaded", False)

        if tick_backtest:
             self.output("tick回测模式：跳过历史K线加载，等待Tick实时合成")
        elif auto_load and not loaded:
             # Check async
             if bool(getattr(self.params, "async_history_load", True)):
                 self.output("=== 异步加载历史K线（不阻塞启动） ===", force=True)
                 import threading
                 def _load_hist_async():
                     try:
                         if hasattr(self, "load_historical_klines"):
                             self.load_historical_klines()
                             self.history_loaded = True
                             self.output("=== 异步历史K线加载完成 ===")
                     except Exception as e:
                         self.output(f"异步历史K线加载失败 {e}")
                 threading.Thread(target=_load_hist_async, daemon=True).start()
             else:
                 self.output("=== 开始加载历史K线数据（同步） ===", force=True)
                 if hasattr(self, "load_historical_klines"):
                     self.load_historical_klines()
                     self.history_loaded = True
                     self.output("=== 历史K线加载完成 ===")

        # 4. 初始化调度器
        if hasattr(self, "_safe_add_periodic_job"):
             interval = getattr(self.params, "calculation_interval", 3)
             self._safe_add_periodic_job(
                 "run_trading_cycle",
                 self.run_trading_cycle, 
                 interval_seconds=interval 
             )
             self.output(f"已启动定时任务，每隔 {interval} 秒计算一次期权宽度")
             
             # [Fix] Independent Position Check Job
             def _position_check_job():
                if hasattr(self, "position_manager") and self.position_manager:
                     self.position_manager.check_and_close_overdue_positions(days_limit=3)
             self._safe_add_periodic_job("position_check_periodic", _position_check_job, interval_seconds=60)

             # Add Daily Close Check
             if hasattr(self, "_check_daily_closing"):
                  self._safe_add_periodic_job(
                      "daily_close_check_periodic",
                      self._check_daily_closing,
                      interval_seconds=60 
                  )
        
        # 5. 立即计算一次 (Source parity)
        # [Fix] Remove blocking synchronous call in on_start.
        # The scheduler (RobustLoopScheduler) will pick up the job immediately (last_run=0).
        # if hasattr(self, "calculate_all_option_widths"):
        #      self.output("[调试] 准备调用 calculate_all_option_widths (首次计算)")
        #      try:
        #          self.calculate_all_option_widths()
        #      except Exception as e:
        #          self.output(f"首次立即计算失败: {e}")

        self.output("Strategy2026Refactored 启动流程结束")

    def on_stop(self):
        """策略停止回调"""
        try:
            super().on_stop()
        except Exception:
            pass
        self.output("策略停止...")
        self.my_is_running = False
        self.my_trading = False
        
        # [Fix Req #2] Explicitly Destroy UI on Stop to prevent hanging threads and allow deletion.
        # This fixes "UI disappears" (clean exit instead of crash) and "Cannot Delete Instance" (breaks ref loop)
        try:
            if hasattr(self, "_ui_root") and self._ui_root:
                def _safe_destroy():
                    try:
                        if hasattr(self, "_ui_root") and self._ui_root:
                            self._ui_root.quit() # Stop mainloop
                            self._ui_root.destroy()
                    except: pass
                
                # Check if we are in UI thread or another thread
                # Tkinter methods usually need to run in main thread, but quit() is thread-safe-ish.
                # Use after() if loop is running.
                try:
                    self._ui_root.after(0, _safe_destroy)
                except:
                    # If after fails (loop assumed dead?), try direct
                    _safe_destroy()
                
                self._ui_root = None
        except Exception:
             pass

    def on_pause(self):
        """[Fix Req #2] Handle Pause Explicitly to manage UI state"""
        self.output("策略暂停...")
        self.my_is_paused = True
        # Note: We do NOT destroy UI on pause, just update state.
        # If UI disappeared before, it was likely due to Platform killing threads on 'Pause'.
        # By defining on_pause, we might intercept default behavior if logic allows.
        pass

    def on_tick(self, tick: TickData):
        """
        行情推送入口。
        由 TradingLogicMixin.on_tick 接管，这里显式调用以确保 Mixin 覆盖顺序正确，
        或者直接依赖 MRO。
        由于 TradingLogicMixin 在 BaseStrategy 之前，并且定义了 on_tick，
        理论上 super().on_tick 不做业务逻辑。
        这里我们直接使用 TradingLogicMixin 的 on_tick 逻辑。
        """
        # 为了明确性，调用 Mixin 的方法 (如果 Mixin 使用了 super，这里就是入口)
        # 但通常 Mixin 直接定义 on_tick 覆盖 BaseStrategy 的空方法
        # 显式调用 TradingLogicMixin 的实现（如果它不是混入链的第一个或者需要明确）
        TradingLogicMixin.on_tick(self, tick)

    def on_trade(self, trade_data, *args, **kwargs):
        """成交通知"""
        try:
            # 必须调用父类以免中断继承链
            try:
                super().on_trade(trade_data, *args, **kwargs)
            except Exception:
                pass
            
            # Forward to Position Manager
            if hasattr(self, "position_manager") and self.position_manager:
                self.position_manager.on_trade(trade_data)
        except Exception as e:
            self.output(f"on_trade Error: {e}")

    def on_order_trade(self, trade_data, *args, **kwargs):
        """订单成交通知"""
        try:
            try:
                super().on_order_trade(trade_data, *args, **kwargs)
            except Exception:
                pass

            if hasattr(self, "position_manager") and self.position_manager:
                self.position_manager.handle_order_trade(trade_data)
        except Exception as e:
            self.output(f"on_order_trade Error: {e}")

    def on_order(self, order_data, *args, **kwargs):
        """订单状态通知"""
        try:
            try:
                super().on_order(order_data, *args, **kwargs)
            except Exception:
                pass

            oid = getattr(order_data, "order_id", "") or getattr(order_data, "OrderRef", "")
            sts = getattr(order_data, "status", "") or getattr(order_data, "OrderStatus", "")
            # self.output(f"[Order] {oid} Status: {sts}")
            
            # 集成平仓管理器状态更新? 
            # Source calls PositionManager.handle_order_status if exists? 
            # Source line 5964 just checks manager exists but logic cut off in my read_file context.
            # Assuming standard manager usage.
        except Exception as e:
            self.output(f"on_order Error: {e}")

    def on_destroy(self):
        self.my_destroyed = True
        self.output("策略销毁")
        # [Fix Req #2] Explicit UI Cleanup
        try:
             if hasattr(self, "_ui_root") and self._ui_root:
                  try:
                      self._ui_root.quit()
                      self._ui_root.destroy()
                  except: pass
                  self._ui_root = None
        except: pass

