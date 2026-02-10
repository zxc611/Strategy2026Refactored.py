"""UI module for Strategy2026."""
from __future__ import annotations
import json
import os
import time
import threading
import queue  # [SafePause] Thread-safe UI communication
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .market_calendar import is_close_debug_allowed

try:
    from .safe_pause_manager import SafePauseManager
except ImportError:
    # Try absolute import or ignore if setup incorrect
    try:
        from your_quant_project.strategy.safe_pause_manager import SafePauseManager
    except:
        SafePauseManager = None

class UIMixin:
    """Strategy UI Mixin. Contains all Tkinter based UI logic."""

    # UI State
    _ui_root: Any = None
    _ui_lbl: Any = None
    _ui_btn_debug: Any = None
    _ui_btn_debug_off: Any = None
    _ui_btn_trade: Any = None
    _ui_btn_auto: Any = None
    _ui_btn_manual: Any = None
    
    _ui_running: bool = False
    _ui_creating: bool = False
    
    # Class level singletons (shared across instances if needed, though typically one strategy per process)
    _ui_global_root: Any = None
    _ui_global_running: bool = False
    _ui_global_creating: bool = False

    def _start_output_mode_ui(self) -> None:
        """启动简易输出模式界面（调试/交易按钮）"""
        
        # [SafePause] 0. Always Init Queue First
        if not hasattr(self, "_ui_queue"):
            self._ui_queue = queue.Queue()

        # [SafePause] Initialize Manager
        if not hasattr(self, "safe_pause_manager"):
            if SafePauseManager:
                 try:
                     self.safe_pause_manager = SafePauseManager(parent_ui=None) # We pass None as UI is not ready yet
                     # Identify self as the strategy logic provider if needed
                     # self.safe_pause_manager.set_strategy(self) 
                     self.output("[SafePause] Manager Initialized.", force=True)
                 except Exception as e:
                     self.output(f"[SafePause] Manager Init Failed: {e}", force=True)

        try:
            import tkinter as tk
        except Exception:
            self.output("界面库不可用，跳过输出模式界面", force=True)
            return

        cls = self.__class__

        # [Safety] Avoid destroying Tk root from non-UI thread; prefer queue signal
        try:
            if getattr(cls, "_ui_global_running", False):
                try:
                    self._schedule_bring_output_mode_ui_front()
                    self.output("输出模式界面已在运行，已聚焦现有窗口", force=True)
                    return
                except Exception:
                    pass
            old_root = getattr(cls, "_ui_global_root", None)
            if old_root:
                try:
                    self.output("清理遗留 UI 窗口...", force=True)
                    if hasattr(self, "_ui_queue") and self._ui_queue:
                        self._ui_queue.put_nowait({"action": "destroy"})
                    else:
                        old_root.destroy()
                except Exception:
                    pass
                setattr(cls, "_ui_global_root", None)
                setattr(cls, "_ui_global_running", False)
                setattr(cls, "_ui_global_creating", False)
        except Exception:
            pass

        # 并发保护
        try:
            if getattr(self, "_ui_creating", False) or getattr(self, "_ui_running", False) or (hasattr(self, "_ui_root") and self._ui_root):
                try:
                    self._schedule_bring_output_mode_ui_front()
                except Exception:
                    pass
                self.output("输出模式界面已在运行，已聚焦现有窗口", force=True)
                return
        except Exception:
            pass

        def _ui_thread():
            try:
                root = tk.Tk()
                
                # [SafePause] 2. Queue Consumer (Runs in UI Thread)
                def _process_queue():
                    should_continue = True
                    try:
                        # [Optimization] Limit messages per frame to prevent UI freeze during log spam
                        msg_count = 0
                        while not self._ui_queue.empty() and msg_count < 20:
                            msg = self._ui_queue.get_nowait()
                            msg_count += 1
                            action = msg.get("action")
                            
                            # Handle messages
                            if action == "pause_status":
                                is_paused = msg.get("paused")
                                if is_paused:
                                    try: 
                                        root.title("输出模式控制 - [已暂停]")
                                        root.configure(bg="#f0f0f0") # Visual cue
                                    except: pass
                                else:
                                    try: 
                                        root.title("输出模式控制")
                                        root.configure(bg="SystemButtonFace")
                                    except: pass
                                    
                            elif action == "refresh_style":
                                try: self._refresh_output_mode_ui_styles()
                                except: pass
                                
                            elif action == "bring_front":
                                try:
                                    root.deiconify()
                                    root.lift()
                                    root.focus_force()
                                except: pass
                            
                            elif action == "destroy":
                                try:
                                    root.destroy()
                                    should_continue = False
                                    self._ui_running = False
                                    setattr(cls, "_ui_global_running", False)
                                except: pass

                            elif action == "log":
                                # Placeholder for safe logging
                                msg_txt = msg.get("text")
                                # print(f"[UI Safe Log] {msg_txt}")
                    except queue.Empty:
                        pass
                    except Exception as e:
                        # Prevent loop crash on one bad message
                        pass
                        
                    finally:
                        # Schedule next check (100ms) only if still running
                        if should_continue and getattr(self, "_ui_running", False):
                            try:
                                root.after(100, _process_queue)
                            except: pass # Catch destroy race
                
                # Start polling
                root.after(100, _process_queue)

                root.title("输出模式控制")
                try:
                    w = int(getattr(self.params, "ui_window_width", 320) or 320)
                    h = int(getattr(self.params, "ui_window_height", 310) or 310)
                except Exception:
                    w, h = 320, 310
                root.geometry(f"{w}x{h}")
                try:
                    root.minsize(max(300, w), max(320, h))
                except Exception:
                    pass
                
                lbl = tk.Label(root, text=f"当前模式: {getattr(self.params, 'output_mode', 'debug')}")
                lbl.pack(pady=8)

                btn_frame = tk.Frame(root)
                btn_frame.pack(fill="x", padx=12, pady=5)
                debug_frame = tk.Frame(root)
                
                BTN_WIDTH = 12
                btn_debug = tk.Button(debug_frame, text="开盘调试", width=BTN_WIDTH)
                btn_debug_off = tk.Button(debug_frame, text="收市调试", width=BTN_WIDTH)
                btn_trade = tk.Button(btn_frame, text="交易", width=BTN_WIDTH)
                btn_backtest_mode = tk.Button(btn_frame, text="回测", width=BTN_WIDTH)
                
                btn_trade.pack(side="left", expand=True, fill="x", padx=(0, 4))
                btn_backtest_mode.pack(side="left", expand=True, fill="x", padx=(4, 0))

                auto_frame = tk.Frame(root)
                auto_frame.pack(fill="x", padx=12, pady=5)
                btn_auto = tk.Button(auto_frame, text="自动交易", width=BTN_WIDTH)
                btn_manual = tk.Button(auto_frame, text="手动交易", width=BTN_WIDTH)
                btn_auto.pack(side="left", expand=True, fill="x", padx=(0, 6))
                btn_manual.pack(side="left", expand=True, fill="x", padx=(6, 0))

                # [SafePause] Pause Button
                pause_frame = tk.Frame(root)
                pause_frame.pack(fill="x", padx=12, pady=(5, 8))
                
                def _do_safe_pause():
                    try:
                        self.output(">>> [UI] 用户点击安全暂停...", force=True)
                        if hasattr(self, "safe_pause_manager") and self.safe_pause_manager:
                             # Call safe pause
                             self.safe_pause_manager.safe_pause()
                             
                             # Visual feedback
                             try:
                                 btn_safe_pause.config(text="正在暂停...", state="disabled")
                                 root.after(2000, lambda: btn_safe_pause.config(text="⚠️ 安全暂停 (Safe Pause)", state="normal"))
                             except: pass
                        else:
                             self.output("!!! 错误: 安全暂停管理器未初始化", force=True)
                    except Exception as e:
                        self.output(f"安全暂停触发失败: {e}", force=True)

                btn_safe_pause = tk.Button(pause_frame, text="⚠️ 安全暂停 (Safe Pause)", width=24, bg="#ffebee", fg="#c62828")
                btn_safe_pause.config(command=_do_safe_pause)
                btn_safe_pause.pack(fill="x")

                btn_daily = tk.Button(root, text="日结输出 (15:01)", width=24)
                btn_daily.pack(fill="x", padx=12, pady=(0, 8))

                param_frame = tk.Frame(root)
                param_frame.pack(fill="x", padx=12, pady=(0, 8))
                btn_param = tk.Button(param_frame, text="参数", width=BTN_WIDTH)
                btn_backtest = tk.Button(param_frame, text="回测参数", width=BTN_WIDTH)
                btn_param.pack(side="left", expand=True, fill="x", padx=(0, 6))
                btn_backtest.pack(side="left", expand=True, fill="x", padx=(6, 0))
                
                debug_frame.pack(fill="x", padx=12, pady=(0, 15))
                btn_debug.pack(side="left", expand=True, fill="x", padx=(0, 2))
                btn_debug_off.pack(side="left", expand=True, fill="x", padx=(2, 0))

                def _to_debug():
                    try:
                        setattr(self.params, "debug_output", False)
                        setattr(self.params, "run_profile", "full")
                        setattr(self.params, "backtest_tick_mode", False)
                        setattr(self.params, "diagnostic_output", False)
                        setattr(self.params, "test_mode", False)
                        self.DEBUG_ENABLE_MOCK_EXECUTION = False
                        try:
                            prev_age = getattr(self, "_close_debug_prev_kline_max_age_sec", None)
                            if prev_age is not None:
                                setattr(self.params, "kline_max_age_sec", prev_age)
                                self._close_debug_prev_kline_max_age_sec = None
                        except Exception:
                            pass
                    except Exception:
                        pass
                    try:
                        if hasattr(self, "resume_strategy"):
                            self.resume_strategy()
                        self.my_is_running = True
                        self.my_is_paused = False
                        self.my_state = "running"
                        self.my_trading = True
                    except Exception:
                        pass
                    try:
                        if hasattr(self, "safe_pause_manager") and self.safe_pause_manager:
                            if hasattr(self.safe_pause_manager, "reset"):
                                self.safe_pause_manager.reset()
                            elif hasattr(self.safe_pause_manager, "manager") and hasattr(self.safe_pause_manager.manager, "is_paused"):
                                self.safe_pause_manager.manager.is_paused = False
                    except Exception:
                        pass
                    try:
                        if (not hasattr(self, "scheduler")) or (self.scheduler is None):
                            if hasattr(self, "_create_default_scheduler"):
                                self.scheduler = self._create_default_scheduler()
                        else:
                            if hasattr(self.scheduler, "running") and not getattr(self.scheduler, "running"):
                                self.scheduler.start()
                    except Exception:
                        pass
                    try:
                        if hasattr(self, "_safe_add_periodic_job"):
                            calc_interval = getattr(self.params, "calculation_interval", 60)
                            self._safe_add_periodic_job("run_trading_cycle", self.run_trading_cycle, interval_seconds=calc_interval, run_async=True)

                            def _pos_check():
                                if hasattr(self, "position_manager") and self.position_manager:
                                    try:
                                        self.position_manager.check_and_close_overdue_positions(days_limit=3)
                                    except Exception:
                                        pass

                            self._safe_add_periodic_job("position_check_periodic", _pos_check, interval_seconds=60, run_async=True)
                            self._safe_add_periodic_job("check_chase_tasks", self.check_active_chase_tasks, interval_seconds=1, run_async=True)
                            self._safe_add_periodic_job("second_timer", self._on_second_timer, interval_seconds=1, run_async=True)
                    except Exception:
                        pass
                    try:
                        if hasattr(self, "run_trading_cycle"):
                            self.run_trading_cycle()
                    except Exception:
                        pass
                    self._apply_param_overrides_for_debug()
                    self.set_output_mode("trade")

                def _to_close_debug():
                    try:
                        try:
                            try:
                                allowed, reason = is_close_debug_allowed(self, minutes_to_open=30)
                                if not allowed:
                                    try:
                                        if hasattr(self, "_ui_lbl") and self._ui_lbl:
                                            msg = "收市调试不可用"
                                            if reason == "near_open":
                                                msg = "距离开盘不足30分钟"
                                            elif reason == "market_open":
                                                msg = "正在开盘！"
                                            self._ui_lbl.config(text=msg, fg="red")
                                    except Exception:
                                        pass
                                    self.output("收市调试不可用：开盘中或临近开盘", force=True)
                                    return
                            except Exception:
                                pass
                            setattr(self.params, "debug_output", True)
                            setattr(self.params, "run_profile", "full")
                            setattr(self.params, "backtest_tick_mode", False)
                            setattr(self.params, "diagnostic_output", True)
                            setattr(self.params, "test_mode", True)
                            self.DEBUG_ENABLE_MOCK_EXECUTION = True
                            try:
                                if not hasattr(self, "_close_debug_prev_kline_max_age_sec"):
                                    self._close_debug_prev_kline_max_age_sec = getattr(self.params, "kline_max_age_sec", None)
                                setattr(self.params, "kline_max_age_sec", 0)
                            except Exception:
                                pass
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "resume_strategy"):
                                self.resume_strategy()
                            self.my_is_running = True
                            self.my_is_paused = False
                            self.my_state = "running"
                            self.my_trading = True
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "safe_pause_manager") and self.safe_pause_manager:
                                if hasattr(self.safe_pause_manager, "reset"):
                                    self.safe_pause_manager.reset()
                                elif hasattr(self.safe_pause_manager, "manager") and hasattr(self.safe_pause_manager.manager, "is_paused"):
                                    self.safe_pause_manager.manager.is_paused = False
                        except Exception:
                            pass
                        try:
                            if (not hasattr(self, "scheduler")) or (self.scheduler is None):
                                if hasattr(self, "_create_default_scheduler"):
                                    self.scheduler = self._create_default_scheduler()
                            else:
                                if hasattr(self.scheduler, "running") and not getattr(self.scheduler, "running"):
                                    self.scheduler.start()
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "_safe_add_periodic_job"):
                                calc_interval = getattr(self.params, "calculation_interval", 60)
                                self._safe_add_periodic_job("run_trading_cycle", self.run_trading_cycle, interval_seconds=calc_interval, run_async=True)

                                def _pos_check():
                                    if hasattr(self, "position_manager") and self.position_manager:
                                        try:
                                            self.position_manager.check_and_close_overdue_positions(days_limit=3)
                                        except Exception:
                                            pass

                                self._safe_add_periodic_job("position_check_periodic", _pos_check, interval_seconds=60, run_async=True)
                                self._safe_add_periodic_job("check_chase_tasks", self.check_active_chase_tasks, interval_seconds=1, run_async=True)
                                self._safe_add_periodic_job("second_timer", self._on_second_timer, interval_seconds=1, run_async=True)
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "run_trading_cycle"):
                                self.run_trading_cycle()
                        except Exception:
                            pass
                        self._apply_param_overrides_for_debug()
                        self.set_output_mode("close_debug") 
                    except Exception as e:
                        try:
                            self.output(f"收市调试切换失败: {e}", force=True)
                        except Exception:
                            pass

                def _to_trade():
                    try:
                        setattr(self.params, "debug_output", False)
                        setattr(self.params, "run_profile", "full")
                        setattr(self.params, "backtest_tick_mode", False)
                        setattr(self.params, "diagnostic_output", False)
                        setattr(self.params, "test_mode", False)
                        self.DEBUG_ENABLE_MOCK_EXECUTION = False
                        try:
                            prev_age = getattr(self, "_close_debug_prev_kline_max_age_sec", None)
                            if prev_age is not None:
                                setattr(self.params, "kline_max_age_sec", prev_age)
                                self._close_debug_prev_kline_max_age_sec = None
                        except Exception:
                            pass
                    except Exception:
                        pass
                    self.set_output_mode("trade")

                def _exit_close_debug_mode(reason: str = ""):
                    try:
                        if hasattr(self, "_reset_close_debug_state"):
                            self._reset_close_debug_state()
                    except Exception:
                        pass
                    try:
                        setattr(self.params, "debug_output", False)
                        setattr(self.params, "diagnostic_output", False)
                        setattr(self.params, "test_mode", False)
                        self.DEBUG_ENABLE_MOCK_EXECUTION = False
                        prev_age = getattr(self, "_close_debug_prev_kline_max_age_sec", None)
                        if prev_age is not None:
                            setattr(self.params, "kline_max_age_sec", prev_age)
                            self._close_debug_prev_kline_max_age_sec = None
                    except Exception:
                        pass
                    try:
                        self.set_output_mode("trade")
                        if reason:
                            self.output(f"收市调试已关闭: {reason}", force=True)
                    except Exception:
                        pass

                def _to_backtest_mode():
                    try:
                        setattr(self.params, "run_profile", "backtest")
                        setattr(self.params, "backtest_tick_mode", True)
                        setattr(self.params, "output_mode", "close_debug")
                        setattr(self.params, "debug_output", False)
                        setattr(self.params, "diagnostic_output", False)
                        self._enforce_diagnostic_silence()
                    except Exception:
                        pass
                    # Usually backtest mode change needs refresh too
                    try:
                        if hasattr(self, "_ui_queue"):
                            self._ui_queue.put({"action": "refresh_style"})
                    except: pass

                def _to_auto_trading():
                    self.set_auto_trading_mode(True)

                def _to_manual_trading():
                    self.set_auto_trading_mode(False)

                def _daily_summary():
                    try:
                        # self._output_daily_signal_summary(skip_reschedule=True)
                        self.output("日结输出已触发（Mixins需实现此方法）", force=True)
                    except Exception as e:
                        try:
                            self.output(f"日结输出触发失败: {e}", force=True)
                        except Exception:
                            pass

                def _param_modify():
                    try:
                        self.output(">>> [UI] Button Click: Parameters", force=True)
                        self._on_param_modify_click()
                    except Exception as e:
                        self.output(f">>> [UI] Button Click Error: {e}", force=True)

                def _backtest_modify():
                    self._on_backtest_click()

                btn_debug.config(command=_to_debug)
                btn_debug_off.config(command=_to_close_debug)
                btn_trade.config(command=_to_trade)
                btn_backtest_mode.config(command=_to_backtest_mode)
                btn_auto.config(command=_to_auto_trading)
                btn_manual.config(command=_to_manual_trading)
                btn_daily.config(command=_daily_summary)
                btn_param.config(command=_param_modify)
                btn_backtest.config(command=_backtest_modify)

                self._exit_close_debug_mode = _exit_close_debug_mode

                try:
                    self._ui_root = root
                    self._ui_lbl = lbl
                    self._ui_btn_debug = btn_debug
                    self._ui_btn_debug_off = btn_debug_off
                    self._ui_btn_trade = btn_trade
                    self._ui_btn_auto = btn_auto
                    self._ui_btn_manual = btn_manual
                    self._ui_btn_daily = btn_daily
                    self._ui_btn_param = btn_param
                    self._ui_btn_backtest_mode = btn_backtest_mode
                    self._ui_btn_backtest = btn_backtest
                    self._ui_running = True
                    self._ui_creating = False
                    setattr(cls, "_ui_global_root", root)
                    setattr(cls, "_ui_global_running", True)
                    setattr(cls, "_ui_global_creating", False)
                except Exception:
                    pass

                self._refresh_output_mode_ui_styles()

                def _on_close():
                    try:
                        self.output("输出模式控制窗口关闭 (Triggered)", force=True)
                        try:
                            # [Debug] Check if called trace
                            import traceback
                            # self.output(f"UI Close Trace:\n{traceback.format_stack()}", force=True) 
                        except: pass
                        
                        try:
                            root.destroy()
                        except Exception:
                            pass
                        self._ui_running = False
                        self._ui_root = None
                        setattr(cls, "_ui_global_running", False)
                        setattr(cls, "_ui_global_root", None)
                    except Exception:
                        pass
                
                root.protocol("WM_DELETE_WINDOW", _on_close)
                
                try:
                     root.mainloop()
                except Exception as e:
                     import traceback
                     self.output(f"[UI ERROR] UI Mainloop crashed: {e}\n{traceback.format_exc()}", force=True)
                finally:
                     try:
                         self.output("[UI] Mainloop exited.")
                     except: pass

            except Exception as e:
                import traceback
                self.output(f"输出模式界面线程异常: {e}\n{traceback.format_exc()}", force=True)
                self._ui_running = False
                self._ui_creating = False
                setattr(cls, "_ui_global_running", False)
                setattr(cls, "_ui_global_creating", False)

        try:
            self._ui_creating = True
            setattr(cls, "_ui_global_creating", True)
            # [Fix Scenario 3] Stop Daemon death. set daemon=False to survive MainThread pause/reset.
            self.output(">>> [UI] Starting UI Thread as Non-Daemon (Persistent Mode)", force=True)
            t = threading.Thread(target=_ui_thread, daemon=False)
            t.start()
        except Exception as e:
            self.output(f"启动输出模式界面线程失败: {e}", force=True)
            try:
                self._ui_creating = False
                setattr(cls, "_ui_global_creating", False)
            except Exception:
                pass

    def _schedule_bring_output_mode_ui_front(self) -> None:
        try:
            # [SafePause] Use Queue
            if hasattr(self, "_ui_queue"):
                self._ui_queue.put({"action": "bring_front"})
        except Exception:
            pass

    def _refresh_output_mode_ui_styles(self) -> None:
        try:
            if not hasattr(self, "_ui_root") or not getattr(self, "_ui_root"):
                return
            import tkinter as tk
            cur = str(getattr(self.params, 'output_mode', 'debug')).lower()
            mode = "close_debug" if cur == "debug" else cur
            try:
                if hasattr(self, "_ui_lbl") and self._ui_lbl:
                    self._ui_lbl.config(text=f"当前模式: {cur}")
            except Exception:
                pass
            
            try:
                try:
                    f_lg = int(getattr(self.params, "ui_font_large", 11) or 11)
                    f_sm = int(getattr(self.params, "ui_font_small", 10) or 10)
                except Exception:
                    f_lg, f_sm = 11, 10
                
                is_open_debug = (mode == 'open_debug')
                is_close_debug = (mode == 'close_debug')
                is_trade_mode = (mode == 'trade')
                is_auto = getattr(self, "auto_trading_enabled", False)

                def _set_style(btn_attr, active, color="#2e7d32"):
                     btn = getattr(self, btn_attr, None)
                     if btn:
                         if active:
                             btn.config(relief=tk.SUNKEN, bg=color, fg="white", activebackground="#1b5e20", activeforeground="white", font=("Microsoft YaHei", f_lg, "bold"))
                         else:
                             btn.config(relief=tk.RAISED, bg="#f0f0f0", fg="black", activebackground="#d9d9d9", activeforeground="black", font=("Microsoft YaHei", f_sm))

                _set_style("_ui_btn_debug", is_open_debug)
                _set_style("_ui_btn_debug_off", is_close_debug, color="#ef6c00")
                _set_style("_ui_btn_trade", is_trade_mode)
                
                # Auto/Manual
                _set_style("_ui_btn_auto", is_auto, color="#1565c0")
                # [Fix] Manual Button Style: If active (manual mode), use a distinct color instead of Grey
                # _set_style("_ui_btn_manual", not is_auto, color="#757575")
                _set_style("_ui_btn_manual", not is_auto, color="#546e7a") # Blue Grey

            except Exception:
                pass
        except Exception:
            pass
            
    def _schedule_output_mode_ui_refresh(self) -> None:
        try:
             # [SafePause] Use Queue
            if hasattr(self, "_ui_queue"):
                self._ui_queue.put({"action": "refresh_style"})
        except Exception:
            pass

    def set_output_mode(self, mode: str) -> None:
        try:
            m = str(mode).lower()
            if m == "debug":
                m = "close_debug"
            if m not in ("open_debug", "close_debug", "trade"):
                self.output(f"无效输出模式: {mode}", force=True)
                return
            try:
                prev = str(getattr(self.params, "output_mode", "debug")).lower()
                if prev == "debug":
                    prev = "close_debug"
                if prev == "close_debug" and m != "close_debug":
                    if hasattr(self, "_reset_close_debug_state"):
                        self._reset_close_debug_state()
            except Exception:
                pass
            setattr(self.params, "output_mode", m)
            if m == "close_debug":
                setattr(self.params, "debug_output", True)
                setattr(self.params, "diagnostic_output", True)
            elif m == "open_debug" or m == "trade":
                setattr(self.params, "debug_output", False)
                setattr(self.params, "diagnostic_output", False)
            try:
                self._schedule_output_mode_ui_refresh()
            except Exception:
                pass
            self.output(f"输出模式切换为: {m}", force=True)
        except Exception as e:
            self.output(f"切换输出模式失败: {e}", force=True)

    def set_auto_trading_mode(self, auto: bool) -> None:
        try:
            self._reset_manual_limits_if_new_day()
            target_auto = bool(auto)
            
            # [Fix] Mode switching should not be subject to trade limits.
            # Limits apply to actual order execution, not the state of the bot.

            self.auto_trading_enabled = target_auto
            self.my_trading = target_auto
            setattr(self.params, "auto_trading_enabled", self.auto_trading_enabled)
            setattr(self.params, "auto_trading", self.auto_trading_enabled)
            if self.auto_trading_enabled:
                self.output("已切换为自动交易模式", force=True)
            else:
                self.output("已切换为手动交易模式", force=True)
            try:
                self._schedule_output_mode_ui_refresh()
            except Exception:
                pass
        except Exception as e:
            self.output(f"切换自动/手动交易模式失败: {e}", force=True)
            
    def _reset_manual_limits_if_new_day(self) -> None:
        try:
            today = datetime.now().date()
            if getattr(self, "manual_trade_date", None) != today:
                if not hasattr(self, "manual_trade_attempts"):
                     self.manual_trade_attempts = {"morning": 0, "afternoon": 0}
                else:
                    self.manual_trade_attempts = {"morning": 0, "afternoon": 0}
                self.manual_trade_date = today
        except Exception:
            pass

    def _current_session_half(self) -> Optional[str]:
        try:
            now = datetime.now()
            try:
                split = int(getattr(self.params, "morning_afternoon_split_hour", 12) or 12)
            except Exception:
                split = 12
            return "morning" if now.hour < split else "afternoon"
        except Exception:
            return None

    def _apply_param_overrides_for_debug(self) -> None:
        """确保调用 ParamTableMixin 的完整实现，避免 UI 覆盖导致异常。"""
        try:
            # 如果 ParamTableMixin 已提供完整实现，直接调用
            from .param_table import ParamTableMixin  # type: ignore
            if isinstance(self, ParamTableMixin) and hasattr(ParamTableMixin, "_apply_param_overrides_for_debug"):
                ParamTableMixin._apply_param_overrides_for_debug(self)
                return

            # 兜底：无 ParamTableMixin 时直接返回
            return
        except Exception:
            pass
            
    def _enforce_diagnostic_silence(self) -> None:
        try:
            setattr(self.params, "diagnostic_output", False)
        except Exception:
            pass

    def _on_param_modify_click(self) -> None:
        """打开简易参数编辑器 (JSON 文件)"""
        try:
            self.output(">>> [UI] Opening Param Editor...", force=True)
            import tkinter as tk
            from tkinter import messagebox
            import json
            import os

            root_obj = getattr(self, "_ui_root", None)
            if not root_obj:
                self.output(">>> [UI] Error: _ui_root is None", force=True)
                return

            # Resolve Path
            path = ""
            if hasattr(self, "_resolve_param_table_path"):
                path = self._resolve_param_table_path()
            
            self.output(f">>> [UI] Resolving path: {path}", force=True)
            
            if not path or not os.path.exists(path):
                # Fallback to default
                base_dir = os.path.dirname(os.path.abspath(__file__))
                path = os.path.join(base_dir, "param_table.json")
                self.output(f">>> [UI] Fallback path: {path}", force=True)
            
            # Create Window
            self.output(">>> [UI] Creating Toplevel...", force=True)
            editor = tk.Toplevel(root_obj)
            editor.title(f"编辑参数: {os.path.basename(path)}")
            editor.geometry("600x600")
            
            # Text Area
            text_area = tk.Text(editor, wrap="none", font=("Consolas", 10))
            scrollbar_y = tk.Scrollbar(editor, command=text_area.yview)
            scrollbar_x = tk.Scrollbar(editor, orient="horizontal", command=text_area.xview)
            text_area.config(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
            
            scrollbar_y.pack(side="right", fill="y")
            scrollbar_x.pack(side="bottom", fill="x")
            text_area.pack(expand=True, fill="both", padx=5, pady=5)
            
            # Load Content
            current_content = ""
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        current_content = f.read()
                        # Try verifying if valid JSON for display formatting
                        try:
                            parsed = json.loads(current_content)
                            current_content = json.dumps(parsed, indent=4, ensure_ascii=False)
                        except:
                            pass
                except Exception as e:
                     current_content = f"// Error reading file: {e}"
            else:
                current_content = f"// File not found: {path}\n// New file will be created on save.\n{{\n    \"params\": {{}}\n}}"

            text_area.insert("1.0", current_content)

            # Buttons
            btn_frame = tk.Frame(editor)
            btn_frame.pack(fill="x", padx=5, pady=5)

            def _save():
                content = text_area.get("1.0", "end-1c")
                try:
                    # Validate JSON
                    json.loads(content)
                    # Save
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(content)
                    # Trigger reload + apply immediately
                    applied = 0
                    if hasattr(self, "_load_param_table"):
                        table = self._load_param_table()
                        if isinstance(table, dict):
                            self._param_override_cache = table
                            param_map = table.get("params") if isinstance(table.get("params"), dict) else table
                            if isinstance(param_map, dict) and hasattr(self, "params"):
                                for k, v in param_map.items():
                                    if hasattr(self.params, k):
                                        try:
                                            setattr(self.params, k, v)
                                            applied += 1
                                        except Exception:
                                            pass
                            backtest_map = table.get("backtest_params") if isinstance(table.get("backtest_params"), dict) else {}
                            if isinstance(backtest_map, dict) and hasattr(self, "params"):
                                for k, v in backtest_map.items():
                                    if hasattr(self.params, k):
                                        try:
                                            setattr(self.params, k, v)
                                            applied += 1
                                        except Exception:
                                            pass
                    messagebox.showinfo("成功", f"参数表已保存并立即生效！\n已应用 {applied} 项")
                    # Close
                    editor.destroy()
                except json.JSONDecodeError as je:
                    messagebox.showerror("格式错误", f"JSON 格式无效:\n{je}")
                except Exception as e:
                    messagebox.showerror("保存失败", f"无法写入文件:\n{e}")

            tk.Button(btn_frame, text="保存并生效", command=_save, bg="#2e7d32", fg="white", font=("Microsoft YaHei", 10, "bold")).pack(side="right", padx=5)
            tk.Button(btn_frame, text="取消", command=editor.destroy).pack(side="right", padx=5)
            
            # Status
            lbl_path = tk.Label(btn_frame, text=path, fg="gray", font=("Arial", 8))
            lbl_path.pack(side="left", padx=5)

        except Exception as e:
            self.output(f"打开参数编辑器失败: {e}", force=True)

    def _on_backtest_click(self) -> None:
        """打开回测参数编辑器，支持保存/放弃并回写参数表。"""
        try:
            self._open_backtest_editor()
        except Exception as e:
            self.output(f"回测参数操作失败: {e}", force=True)

    def _open_backtest_editor(self) -> None:
        """回测参数编辑：编辑 backtest_params，保存时写回参数表并应用；关闭询问是否保存。"""
        try:
            import tkinter as tk
            from tkinter import messagebox
            import json
            import os
        except Exception:
            self.output("回测参数编辑器依赖tkinter，不可用", force=True)
            return

        # 载入参数表并备份原始内容，便于放弃时还原
        try:
            table = self._load_param_table() if hasattr(self, "_load_param_table") else {}
            if not isinstance(table, dict):
                table = {}
        except Exception:
            table = {}
        try:
            original_table = json.loads(json.dumps(table))  # 深拷贝
        except Exception:
            try:
                original_table = table.copy()
            except:
                original_table = {}

        # 捕获“回测开始”时的全量参数快照（按参数表路径区分），便于随时恢复
        try:
            current_param_path = None
            try:
                if hasattr(self, "_resolve_param_table_path"):
                    current_param_path = self._resolve_param_table_path()
            except Exception:
                pass

            need_snapshot = not hasattr(self, "_backtest_session_snapshot")
            if not need_snapshot:
                prev_path = getattr(self, "_backtest_session_path", None)
                if current_param_path != prev_path:
                    need_snapshot = True

            if need_snapshot:
                snapshot_copy = json.loads(json.dumps(table)) if isinstance(table, dict) else {}
                self._backtest_session_snapshot = snapshot_copy
                self._backtest_session_path = current_param_path
                self._backtest_session_time = datetime.now()
        except Exception:
            pass

        backtest_params = {}
        try:
            backtest_params = table.get("backtest_params", {}) if isinstance(table, dict) else {}
            if not isinstance(backtest_params, dict):
                backtest_params = {}
        except Exception:
            backtest_params = {}

        # 将开仓/风控参数预填入回测参数区，便于一并调参
        try:
            open_risk_keys = [
                "option_buy_lots_min",
                "option_buy_lots_max",
                "option_contract_multiplier",
                "position_limit_valid_hours_max",
                "position_limit_default_valid_hours",
                "position_limit_max_ratio",
                "position_limit_min_amount",
                "option_order_price_type",
                "option_order_time_condition",
                "option_order_volume_condition",
                "option_order_contingent_condition",
                "option_order_force_close_reason",
                "option_order_hedge_flag",
                "option_order_min_volume",
                "option_order_business_unit",
                "option_order_is_auto_suspend",
                "option_order_user_force_close",
                "option_order_is_swap",
                "close_take_profit_ratio",
                "close_overnight_check_time",
                "close_daycut_time",
                "close_max_hold_days",
                "close_overnight_loss_threshold",
                "close_overnight_profit_threshold",
                "close_max_chase_attempts",
                "close_chase_interval_seconds",
                "close_chase_task_timeout_seconds",
                "close_delayed_timeout_seconds",
                "close_delayed_max_retries",
                "close_order_price_type",
            ]
            if hasattr(self, "params"):
                for key in open_risk_keys:
                    if key not in backtest_params and hasattr(self.params, key):
                        backtest_params[key] = getattr(self.params, key)
        except Exception:
            pass

        def dumps_pretty(obj: Any) -> str:
            try:
                return json.dumps(obj, ensure_ascii=False, indent=2)
            except Exception:
                return "{}"

        try:
            root_obj = getattr(self, "_ui_root", None)
            top = tk.Toplevel(root_obj) if root_obj else tk.Tk()
            top.title("回测参数")
            top.geometry("640x560")
            try:
                top.minsize(560, 420)
                top.resizable(True, True)
            except Exception:
                pass

            frm = tk.Frame(top)
            frm.pack(fill="both", expand=True, padx=10, pady=10)
            try:
                frm.columnconfigure(0, weight=1)
                frm.rowconfigure(2, weight=1)
            except Exception:
                pass

            tk.Label(frm, text="回测参数(JSON，可新增字段)：").grid(row=0, column=0, sticky="w")

            info_row = tk.Frame(frm)
            info_row.grid(row=1, column=0, sticky="ew", pady=(0, 6))
            try:
                info_row.columnconfigure(0, weight=1)
            except Exception:
                pass
            tk.Label(
                info_row,
                justify="left",
                wraplength=560,
                fg="#444",
                text=(
                    "说明：编辑 JSON；保存即写回 backtest_params 并应用已有字段。"
                ),
            ).grid(row=0, column=0, sticky="w")
            def _show_backtest_help():
                try:
                    messagebox.showinfo(
                        "回测参数说明",
                        "使用说明：\n"
                        "1) JSON对象，保存写回参数表 backtest_params 并应用现有字段。\n"
                        "2) 可新增/修改 option_* / position_limit_* / close_* 等。\n"
                        "3) 不支持 JSON 注释；需临时关闭字段可删除或置空。\n"
                        "4) 关闭若选择不保存则恢复打开前状态。\n"
                        "5) “恢复回测起点”回到本次回测开始快照。\n\n"
                        "字段注释：\n"
                        "- option_buy_lots_min/max：期权开仓手数上下限。\n"
                        "- option_contract_multiplier：期权合约乘数。\n"
                        "- position_limit_*：开仓资金限额（有效小时/默认小时/资金比例上限/最小金额）。\n"
                        "- option_order_*：开仓委托 CTP 参数。\n"
                        "- close_take_profit_ratio：止盈倍数。\n"
                        "- close_overnight_check_time / close_daycut_time：隔夜检查、日内平仓时间。\n"
                        "- close_max_hold_days：持仓天数≥阈值时平仓。\n"
                        "- close_overnight_loss_threshold / close_overnight_profit_threshold：隔夜亏损/盈利触发阈值。\n"
                        "- close_max_chase_attempts / close_chase_interval_seconds：追单次数与间隔。\n"
                        "- close_chase_task_timeout_seconds：追单超时。\n"
                        "- close_delayed_timeout_seconds / close_delayed_max_retries：延迟平仓超时/重试。\n"
                        "- close_order_price_type：平仓委托价类型（默认限价2）。"
                    )
                except Exception:
                    pass
            tk.Button(info_row, text="说明", command=_show_backtest_help).grid(row=0, column=1, padx=(8, 0))

            # 文本区 + 滚动条
            text_frame = tk.Frame(frm)
            text_frame.grid(row=2, column=0, sticky="nsew")

            vbar = tk.Scrollbar(text_frame, orient="vertical")
            vbar.pack(side="right", fill="y")
            hbar = tk.Scrollbar(text_frame, orient="horizontal")
            hbar.pack(side="bottom", fill="x")

            txt = tk.Text(text_frame, wrap="none", height=20, yscrollcommand=vbar.set, xscrollcommand=hbar.set)
            txt.pack(side="left", fill="both", expand=True)
            vbar.config(command=txt.yview)
            hbar.config(command=txt.xview)
            txt.insert("1.0", dumps_pretty(backtest_params))

            btn_bar = tk.Frame(frm)
            btn_bar.grid(row=3, column=0, sticky="ew", pady=(8,0))

            def apply_and_save(content: str) -> bool:
                """保存到参数表并应用到当前 params，返回是否成功。"""
                try:
                    data = json.loads(content) if content.strip() else {}
                    if not isinstance(data, dict):
                        raise ValueError("回测参数需为JSON对象")

                    # 写回表
                    table_new = table if isinstance(table, dict) else {}
                    table_new["backtest_params"] = data

                    target = None
                    if hasattr(self, "_resolve_param_table_path"):
                        target = self._resolve_param_table_path()
                    
                    saved_to_file = False
                    if isinstance(target, str) and target.strip():
                        try:
                            # Verify dir exists
                            pdir = os.path.dirname(target)
                            if pdir and not os.path.exists(pdir):
                                os.makedirs(pdir, exist_ok=True)
                            with open(target, "w", encoding="utf-8") as f:
                                json.dump(table_new, f, ensure_ascii=False, indent=2)
                            saved_to_file = True
                        except Exception as e:
                            self.output(f"回测参数写文件失败: {e}", force=True)

                    if not saved_to_file:
                        try:
                            if hasattr(self.params, "param_override_table"):
                                setattr(self.params, "param_override_table", json.dumps(table_new, ensure_ascii=False))
                        except Exception:
                            pass

                    # 更新缓存并应用到当前 params（存在的字段才设置）
                    if hasattr(self, "_param_override_cache"):
                        self._param_override_cache = table_new
                    applied = 0
                    if hasattr(self, "params"):
                        for k, v in data.items():
                            if hasattr(self.params, k):
                                try:
                                    setattr(self.params, k, v)
                                    applied += 1
                                except Exception:
                                    pass
                    self.output(f"回测参数已保存，应用 {applied} 项（只对已存在字段）", force=True)
                    return True
                except Exception as e:
                    try:
                        messagebox.showerror("错误", f"保存失败: {e}")
                    except Exception:
                        pass
                    self.output(f"回测参数保存失败: {e}", force=True)
                    return False

            def restore_original():
                """恢复回测参数为打开前状态并回写。"""
                try:
                    target = None
                    if hasattr(self, "_resolve_param_table_path"):
                        target = self._resolve_param_table_path()
                        
                    if isinstance(target, str) and target.strip():
                        try:
                            with open(target, "w", encoding="utf-8") as f:
                                json.dump(original_table, f, ensure_ascii=False, indent=2)
                        except Exception:
                            pass
                    
                    if hasattr(self, "_param_override_cache"):
                        self._param_override_cache = original_table
                        
                    # Re-apply original backtest params
                    orig_bk = original_table.get("backtest_params", {})
                    if not isinstance(orig_bk, dict): orig_bk = {}
                    applied = 0
                    if hasattr(self, "params"):
                        for k, v in orig_bk.items():
                            if hasattr(self.params, k):
                                try:
                                    setattr(self.params, k, v)
                                    applied += 1
                                except Exception:
                                    pass
                    self.output(f"已放弃更改并还原参数，恢复 {applied} 项", force=True)
                except Exception as e:
                    self.output(f"还原失败: {e}", force=True)

            def restore_start_snapshot():
                """恢复到回测开始时的参数快照。"""
                try:
                    snapshot = getattr(self, "_backtest_session_snapshot", None)
                    if not snapshot:
                        messagebox.showinfo("提示", "未找到回测起点快照（可能尚未运行或初始化）。")
                        return

                    target = None
                    if hasattr(self, "_resolve_param_table_path"):
                        target = self._resolve_param_table_path()

                    if isinstance(target, str) and target.strip():
                         try:
                            with open(target, "w", encoding="utf-8") as f:
                                json.dump(snapshot, f, ensure_ascii=False, indent=2)
                         except Exception:
                             pass
                    
                    if hasattr(self, "_param_override_cache"):
                        self._param_override_cache = snapshot
                    
                    snap_bk = snapshot.get("backtest_params", {})
                    if not isinstance(snap_bk, dict): snap_bk = {}
                    
                    applied = 0
                    if hasattr(self, "params"):
                        for k, v in snap_bk.items():
                            if hasattr(self.params, k):
                                try:
                                    setattr(self.params, k, v)
                                    applied += 1
                                except Exception:
                                    pass
                    
                    txt.delete("1.0", "end")
                    txt.insert("1.0", dumps_pretty(snap_bk))
                    self.output(f"已恢复回测起点快照，恢复 {applied} 项", force=True)
                    messagebox.showinfo("成功", "已重置为回测开始时的参数状态！")

                except Exception as e:
                    self.output(f"恢复起点失败: {e}", force=True)

            def _on_save():
                 c = txt.get("1.0", "end-1c")
                 if apply_and_save(c):
                     messagebox.showinfo("成功", "保存成功，参数已更新！")
                     top.destroy()
            
            def _on_cancel():
                 # 简单直接关闭，不做复杂确认以简化操作，或者可选询问
                 top.destroy()
            
            def _on_restore_snap():
                if messagebox.askyesno("确认", "确定要丢弃当前修改，恢复到回测开始时的参数吗？"):
                    restore_start_snapshot()

            btn_save = tk.Button(btn_bar, text="保存并应用", command=_on_save, bg="#2e7d32", fg="white", font=("Microsoft YaHei", 9, "bold"))
            btn_save.pack(side="right", padx=5)

            btn_cancel = tk.Button(btn_bar, text="关闭(不保存)", command=_on_cancel)
            btn_cancel.pack(side="right", padx=5)

            btn_rest = tk.Button(btn_bar, text="恢复回测起点", command=_on_restore_snap, fg="blue")
            btn_rest.pack(side="left", padx=5)

        except Exception as e:
            self.output(f"打开回测参数编辑器失败: {e}", force=True)

    def _reset_param_edit_quota_if_new_month(self) -> None:
        pass
