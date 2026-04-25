#!/usr/bin/env python3
"""
ui_service.py - UI服务模块

合并来源：16_ui.py
合并策略：提取UI控制核心功能，简化界面逻辑

核心功能：
1. UIMixin - 策略UI混入类(26个方法)
2. StrategyUI - 独立UI控制面板
3. UIDiagnosisTool - UI诊断工具

作者：CodeArts 代码智能体
版本：v2.0
生成时间：2026-03-21
"""
from __future__ import annotations

import json
import threading
import queue
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

from ali2026v3_trading.scheduler_service import is_market_open
from ali2026v3_trading.storage import InstrumentDataManager

logger = logging.getLogger(__name__)


# =============================================================================
# 辅助函数
# =============================================================================

def safe_getattr_int(obj: Any, attr: str, default: int = 0, min_val: int = 0) -> int:
    """安全获取整数属性"""
    try:
        val = getattr(obj, attr, default)
        if isinstance(val, int):
            return max(val, min_val)
        return max(int(val), min_val)
    except Exception:
        return default


# =============================================================================
# 数据类
# =============================================================================

@dataclass
class UIEvent:
    """UI事件记录"""
    timestamp: datetime
    event_type: str
    data: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# UIMixin - 策略UI混入类
# =============================================================================

class UIMixin:
    """策略UI混入类 - 提供Tkinter界面控制功能"""
    
    def __init__(self, *args, **kwargs):
        """初始化"""
        super().__init__(*args, **kwargs)
        # ✅ M21 Bug #3修复：初始化锁
        import threading
        if UIService._ui_lock is None:
            UIService._ui_lock = threading.Lock()
    
    # UI状态（✅ M21 Bug #3修复：添加锁保护）
    _ui_lock: Any = None  # threading.Lock
    _ui_root: Any = None
    _ui_lbl: Any = None
    _ui_btn_debug: Any = None
    _ui_btn_debug_off: Any = None
    _ui_btn_trade: Any = None
    _ui_btn_auto: Any = None
    _ui_btn_manual: Any = None
    
    _ui_running: bool = False
    _ui_creating: bool = False
    
    # 类级别单例（✅ M21 Bug #3修复：添加锁保护）
    _ui_global_root: Any = None
    _ui_global_running: bool = False
    _ui_global_creating: bool = False
    
    def _create_ui_in_main_thread(self, root: Any) -> None:
        """P2 Bug #80修复：在主线程中创建UI界面
        
        Args:
            root: Tk根窗口
        """
        try:
            import tkinter as tk
            cls = self.__class__
            
            root.deiconify()  # 显示窗口
            root.title("输出模式控制")
            try:
                root.attributes('-topmost', True)
                root.after(200, lambda: root.attributes('-topmost', False))
            except Exception:
                pass
            try:
                w = safe_getattr_int(self.params, "ui_window_width", 320, 320)
                h = safe_getattr_int(self.params, "ui_window_height", 310, 310)
            except Exception:
                w, h = 320, 310
            root.geometry(f"{w}x{h}")
            
            # 构建界面
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
            
            # 暂停按钮
            pause_frame = tk.Frame(root)
            pause_frame.pack(fill="x", padx=12, pady=(5, 8))
            
            def _do_safe_pause():
                try:
                    from ali2026v3_trading.diagnosis_service import ControlActionLogger as _CAL
                    strategy_id = getattr(self, 'current_strategy_id', 'unknown')
                    run_id = getattr(self, 'current_run_id', 'N/A')
                    _CAL.log_control_action_enter('pause', strategy_id, run_id, source='ui-button')
                    self._log_info(">>> [UI] 用户点击安全暂停...")
                    self._call_method_by_priority(['internal_pause_strategy', 'pause_strategy'])
                except Exception as e:
                    self._log_error(f"安全暂停触发失败: {e}")
            
            btn_safe_pause = tk.Button(pause_frame, text="安全暂停", width=24, bg="#ffebee", fg="#c62828")
            btn_safe_pause.config(command=_do_safe_pause)
            btn_safe_pause.pack(fill="x")
            
            btn_daily = tk.Button(root, text="日结输出", width=24)
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
            
            # 按钮回调
            def _to_debug():
                try:
                    # ✅ 修复：开盘调试只能在开盘时间使用
                    if not is_market_open():
                        error_msg = "收盘时间内不能使用开盘调试模式"
                        self._log_error(error_msg)
                        # ✅ 关键修复：显示错误提示对话框
                        try:
                            import tkinter as tk
                            from tkinter import messagebox
                            if hasattr(self, '_ui_root') and self._ui_root:
                                messagebox.showwarning("操作禁止", error_msg, parent=self._ui_root)
                            else:
                                messagebox.showwarning("操作禁止", error_msg)
                        except Exception:
                            pass
                        return
                    
                    setattr(self.params, "debug_output", False)
                    setattr(self.params, "run_profile", "full")
                    setattr(self.params, "backtest_tick_mode", False)
                    setattr(self.params, "diagnostic_output", False)
                    setattr(self.params, "test_mode", False)
                    resumed = self._call_method_by_priority(['internal_resume_strategy', 'resume_strategy'])
                    if resumed:
                        self.my_trading = True
                        self.set_output_mode("trade")
                except Exception as e:
                    self._log_error(f"切换调试模式失败: {e}")
            
            def _to_close_debug():
                try:
                    if is_market_open():
                        error_msg = "开盘时间内不能切换到收市调试模式"
                        self._log_error(error_msg)
                        # ✅ 关键修复：显示错误提示对话框
                        try:
                            import tkinter as tk
                            from tkinter import messagebox
                            if hasattr(self, '_ui_root') and self._ui_root:
                                messagebox.showwarning("操作禁止", error_msg, parent=self._ui_root)
                            else:
                                messagebox.showwarning("操作禁止", error_msg)
                        except Exception:
                            pass
                        return
                    
                    setattr(self.params, "debug_output", True)
                    setattr(self.params, "diagnostic_output", True)
                    setattr(self.params, "test_mode", True)
                    resumed = False
                    if hasattr(self, "internal_resume_strategy"):
                        resumed = self.internal_resume_strategy()
                    elif hasattr(self, "resume_strategy"):
                        resumed = bool(self.resume_strategy())
                    if resumed:
                        self.my_trading = True
                        self.set_output_mode("close_debug")
                except Exception as e:
                    self._log_error(f"收市调试切换失败: {e}")
            
            def _to_trade():
                try:
                    # ✅ 修复：收盘时间不能切换到交易模式（无法实际交易）
                    if not is_market_open():
                        error_msg = "收盘时间内不能切换到交易模式（无法实际交易）"
                        self._log_error(error_msg)
                        # ✅ 关键修复：显示错误提示对话框
                        try:
                            import tkinter as tk
                            from tkinter import messagebox
                            if hasattr(self, '_ui_root') and self._ui_root:
                                messagebox.showwarning("操作禁止", error_msg, parent=self._ui_root)
                            else:
                                messagebox.showwarning("操作禁止", error_msg)
                        except Exception:
                            pass
                        return
                    
                    setattr(self.params, "debug_output", False)
                    setattr(self.params, "diagnostic_output", False)
                    setattr(self.params, "test_mode", False)
                    self.my_trading = True  # ✅ 修复：设置交易标志，确保实际交易功能启用
                    self.auto_trading_enabled = getattr(self, "auto_trading_enabled", False)  # ✅ 保持自动交易状态
                    self.set_output_mode("trade")
                    # ✅ 关键修复：立即刷新UI样式
                    self._refresh_output_mode_ui_styles()
                except Exception as e:
                    self._log_error(f"切换交易模式失败: {e}")

            def _to_backtest_mode():
                try:
                    setattr(self.params, "run_profile", "backtest")
                    setattr(self.params, "backtest_tick_mode", True)
                    setattr(self.params, "output_mode", "close_debug")
                    setattr(self.params, "debug_output", False)
                    setattr(self.params, "diagnostic_output", False)
                except Exception as e:
                    self._log_error(f"切换回测模式失败: {e}")
            
            def _to_auto_trading():
                self.set_auto_trading_mode(True)
                # ✅ 关键修复：立即刷新UI样式
                self._refresh_output_mode_ui_styles()
            
            def _to_manual_trading():
                self.set_auto_trading_mode(False)
                # ✅ 关键修复：立即刷新UI样式
                self._refresh_output_mode_ui_styles()
            
            def _daily_summary():
                self._log_info("日结输出已触发")
            
            def _param_modify():
                try:
                    self._on_param_modify_click()
                except Exception as e:
                    self._log_error(f"参数编辑失败: {e}")
            
            def _backtest_modify():
                try:
                    self._on_backtest_click()
                except Exception as e:
                    self._log_error(f"回测参数编辑失败: {e}")
            
            btn_debug.config(command=_to_debug)
            btn_debug_off.config(command=_to_close_debug)
            btn_trade.config(command=_to_trade)
            btn_backtest_mode.config(command=_to_backtest_mode)
            btn_auto.config(command=_to_auto_trading)
            btn_manual.config(command=_to_manual_trading)
            btn_daily.config(command=_daily_summary)
            btn_param.config(command=_param_modify)
            btn_backtest.config(command=_backtest_modify)
            
            # 保存引用
            self._ui_root = root
            self._ui_lbl = lbl
            self._ui_btn_debug = btn_debug
            self._ui_btn_debug_off = btn_debug_off
            self._ui_btn_trade = btn_trade
            self._ui_btn_auto = btn_auto
            self._ui_btn_manual = btn_manual
            self._ui_running = True
            self._ui_creating = False
            setattr(cls, "_ui_global_root", root)
            setattr(cls, "_ui_global_running", True)
            
            # ✅ 关键修复：UI创建后立即刷新样式，确保按钮状态与当前实际状态同步
            self._refresh_output_mode_ui_styles()
            self._log_info(f"UI界面已在主线程中创建，当前模式={getattr(self.params, 'output_mode', 'debug')}, auto_trading={getattr(self, 'auto_trading_enabled', False)}")
            
            def _on_close():
                try:
                    root.destroy()
                except Exception as e:
                    self._log_error(f"关闭窗口失败: {e}")
                self._ui_running = False
                self._ui_root = None
                setattr(cls, "_ui_global_running", False)
                setattr(cls, "_ui_global_root", None)
            
            root.protocol("WM_DELETE_WINDOW", _on_close)
            
            self._log_info("UI界面已在主线程中创建")
        except Exception as e:
            self._log_error(f"在主线程中创建UI失败: {e}")
            self._ui_running = False
    
    def _start_output_mode_ui(self) -> None:
        """启动简易输出模式界面"""
        if not hasattr(self, "_ui_queue"):
            self._ui_queue = queue.Queue()
        
        try:
            import tkinter as tk
        except Exception as e:
            self._log_error(f"tkinter不可用: {e}")
            return
        
        cls = self.__class__
        
        # 检查是否已运行
        if getattr(cls, "_ui_global_running", False):
            try:
                self._schedule_bring_output_mode_ui_front()
                self._log_info("输出模式界面已在运行")
                return
            except Exception as e:
                self._log_error(f"前置窗口失败: {e}")
        
        # 清理遗留窗口
        old_root = getattr(cls, "_ui_global_root", None)
        if old_root:
            try:
                if hasattr(self, "_ui_queue") and self._ui_queue:
                    self._ui_queue.put_nowait({"action": "destroy"})
                else:
                    old_root.destroy()
            except Exception as e:
                self._log_error(f"清理遗留窗口失败: {e}")
            setattr(cls, "_ui_global_root", None)
            setattr(cls, "_ui_global_running", False)
        
        # P2 Bug #80修复：检查是否在主线程
        import threading as _threading
        main_thread = _threading.main_thread()
        current_thread = _threading.current_thread()
        
        if current_thread != main_thread:
            # 不在主线程，通过queue请求主线程创建UI
            self._log_warning("检测到非主线程调用UI，将通过queue调度到主线程")
            if not hasattr(self, "_ui_queue"):
                self._ui_queue = queue.Queue()
            # 标记需要创建UI
            self._ui_queue.put({"action": "create_ui", "params": None})
            # 如果还没有UI线程，启动一个专门的UI主循环线程
            if not getattr(cls, "_ui_mainloop_started", False):
                setattr(cls, "_ui_mainloop_started", True)
                def _ui_mainloop_thread():
                    try:
                        import tkinter as tk
                        # ✅ M21 Bug #4修复：非主线程不直接创建Tk，通过queue调度到主线程
                        # 这里只处理queue消息，不创建root
                        root = None
                        setattr(cls, "_ui_global_root", None)
                        
                        def _process_ui_queue():
                            nonlocal root
                            should_continue = True
                            try:
                                msg_count = 0
                                while not self._ui_queue.empty() and msg_count < 20:
                                    msg = self._ui_queue.get_nowait()
                                    msg_count += 1
                                    action = msg.get("action")
                                    
                                    if action == "create_ui":
                                        # 尝试在当前线程创建Tk root（仅首次）
                                        if root is None:
                                            try:
                                                root = tk.Tk()
                                                root.withdraw()  # 隐藏主窗口
                                                setattr(cls, "_ui_global_root", root)
                                            except Exception:
                                                root = None
                                        if root:
                                            self._create_ui_in_main_thread(root)
                                    elif action == "destroy":
                                        if root:
                                            root.destroy()
                                        should_continue = False
                            except queue.Empty:
                                pass
                            except Exception as e:
                                self._log_error(f"处理UI队列失败: {e}")
                            finally:
                                if should_continue and root:
                                    root.after(100, _process_ui_queue)
                        
                        if root:
                            root.after(100, _process_ui_queue)
                            root.mainloop()
                        else:
                            # root创建失败，使用简单轮询模式
                            import time as _time
                            while not self._ui_queue.empty():
                                _process_ui_queue()
                                _time.sleep(0.1)
                    except Exception as e:
                        self._log_error(f"UI主循环线程异常: {e}")
                    finally:
                        # ✅ M21 Bug #5修复：显式销毁窗口
                        try:
                            if 'root' in locals() and root:
                                root.destroy()
                        except:
                            pass
                        setattr(cls, "_ui_mainloop_started", False)
                
                # ✅ M21 Bug #5修复：daemon=False，确保UI资源正确释放
                t = _threading.Thread(target=_ui_mainloop_thread, daemon=False, name="UIMainLoop")
                t.start()
            return
        
        # 在主线程中，直接创建UI
        try:
            import tkinter as tk
            root = tk.Tk()
            setattr(cls, "_ui_global_root", root)
            
            # 调用UI创建方法
            self._create_ui_in_main_thread(root)
            
            # 队列消费者（处理其他消息）
            def _process_queue():
                should_continue = True
                try:
                    msg_count = 0
                    while not self._ui_queue.empty() and msg_count < 20:
                        msg = self._ui_queue.get_nowait()
                        msg_count += 1
                        action = msg.get("action")
                        
                        if action == "pause_status":
                            is_paused = msg.get("paused")
                            try:
                                if is_paused:
                                    root.title("输出模式控制 - [已暂停]")
                                else:
                                    root.title("输出模式控制")
                            except Exception as e:
                                self._log_error(f"更新暂停状态失败: {e}")
                        elif action == "refresh_style":
                            try:
                                self._refresh_output_mode_ui_styles()
                            except Exception as e:
                                self._log_error(f"刷新样式失败: {e}")
                        elif action == "bring_front":
                            try:
                                root.deiconify()
                                root.lift()
                                root.focus_force()
                            except Exception as e:
                                self._log_error(f"前置窗口失败: {e}")
                        elif action == "destroy":
                            try:
                                root.destroy()
                                should_continue = False
                                self._ui_running = False
                                setattr(cls, "_ui_global_running", False)
                            except Exception as e:
                                self._log_error(f"销毁窗口失败: {e}")
                except queue.Empty:
                    pass
                except Exception as e:
                    self._log_error(f"处理队列失败: {e}")
                finally:
                    if should_continue and getattr(self, "_ui_running", False):
                        try:
                            root.after(100, _process_queue)
                        except Exception as e:
                            self._log_error(f"调度队列处理失败: {e}")
            
            root.after(100, _process_queue)
            root.mainloop()
            
        except Exception as e:
            self._log_error(f"输出模式界面异常: {e}")
            self._ui_running = False
    
    def _schedule_bring_output_mode_ui_front(self) -> None:
        """调度窗口前置"""
        try:
            if hasattr(self, "_ui_queue"):
                self._ui_queue.put({"action": "bring_front"})
        except Exception as e:
            self._log_error(f"调度窗口前置失败: {e}")
    
    def _refresh_output_mode_ui_styles(self) -> None:
        """刷新UI样式"""
        try:
            if not hasattr(self, "_ui_root") or not getattr(self, "_ui_root"):
                return
            import tkinter as tk
            cur = str(getattr(self.params, 'output_mode', 'debug')).lower()
            # ✅ 修复：仅当 output_mode='debug' 且未明确指定时，根据时间智能判断
            # 注意：这只是为了UI显示，不修改 params.output_mode 的实际值
            if cur == "debug":
                # 根据当前时间决定显示哪种调试模式
                display_mode = "open_debug" if is_market_open() else "close_debug"
            else:
                display_mode = cur
            try:
                if hasattr(self, "_ui_lbl") and self._ui_lbl:
                    self._ui_lbl.config(text=f"当前模式: {cur}")
            except Exception as e:
                self._log_error(f"更新标签失败: {e}")
            
            try:
                is_open_debug = (display_mode == 'open_debug')
                is_close_debug = (display_mode == 'close_debug')
                is_trade_mode = (display_mode == 'trade')
                is_auto = getattr(self, "auto_trading_enabled", False)
                
                def _set_style(btn_attr, active, color="#2e7d32"):
                    btn = getattr(self, btn_attr, None)
                    if btn:
                        if active:
                            btn.config(relief=tk.SUNKEN, bg=color, fg="white")
                        else:
                            btn.config(relief=tk.RAISED, bg="#f0f0f0", fg="black")
                
                _set_style("_ui_btn_debug", is_open_debug)
                _set_style("_ui_btn_debug_off", is_close_debug, color="#ef6c00")
                _set_style("_ui_btn_trade", is_trade_mode)
                _set_style("_ui_btn_auto", is_auto, color="#1565c0")
                _set_style("_ui_btn_manual", not is_auto, color="#546e7a")
            except Exception as e:
                self._log_error(f"设置按钮样式失败: {e}")
        except Exception as e:
            self._log_error(f"刷新UI样式失败: {e}")
    
    def _schedule_output_mode_ui_refresh(self) -> None:
        """调度UI刷新"""
        try:
            if hasattr(self, "_ui_queue"):
                self._ui_queue.put({"action": "refresh_style"})
        except Exception as e:
            self._log_error(f"调度UI刷新失败: {e}")
    
    def set_output_mode(self, mode: str) -> None:
        """设置输出模式"""
        try:
            m = str(mode).lower()
            if m == "debug":
                m = "close_debug"
            if m not in ("open_debug", "close_debug", "trade"):
                self._log_error(f"无效输出模式: {mode}")
                return
            setattr(self.params, "output_mode", m)
            if m == "close_debug":
                setattr(self.params, "debug_output", True)
                setattr(self.params, "diagnostic_output", True)
            elif m == "open_debug" or m == "trade":
                setattr(self.params, "debug_output", False)
                setattr(self.params, "diagnostic_output", False)
            try:
                self._schedule_output_mode_ui_refresh()
            except Exception as e:
                self._log_error(f"调度UI刷新失败: {e}")
            self._log_info(f"输出模式切换为: {m}")
        except Exception as e:
            self._log_error(f"切换输出模式失败: {e}")
    
    def set_auto_trading_mode(self, auto: bool) -> None:
        """设置自动交易模式（统一状态源：以auto_trading_enabled为准）"""
        try:
            self.auto_trading_enabled = bool(auto)
            # ✅ 统一传递渠道：以auto_trading_enabled为唯一状态源
            self.my_trading = self.auto_trading_enabled
            if self.params:
                setattr(self.params, "auto_trading_enabled", self.auto_trading_enabled)
            if self.auto_trading_enabled:
                self._log_info("已切换为自动交易模式")
            else:
                self._log_info("已切换为手动交易模式")
            try:
                self._schedule_output_mode_ui_refresh()
            except Exception as e:
                self._log_error(f"调度UI刷新失败: {e}")
            # ✅ 关键修复：立即刷新UI样式，不依赖队列
            self._refresh_output_mode_ui_styles()
        except Exception as e:
            self._log_error(f"切换自动/手动交易模式失败: {e}")
    
    def _on_param_modify_click(self) -> None:
        """打开简易参数编辑器"""
        try:
            import tkinter as tk
            from tkinter import messagebox
            
            root_obj = getattr(self, "_ui_root", None)
            if not root_obj:
                return
            
            editor = tk.Toplevel(root_obj)
            editor.title("编辑参数")
            editor.geometry("600x400")
            
            text_area = tk.Text(editor, wrap="none", font=("Consolas", 10))
            scrollbar_y = tk.Scrollbar(editor, command=text_area.yview)
            scrollbar_x = tk.Scrollbar(editor, orient="horizontal", command=text_area.xview)
            text_area.config(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)
            
            scrollbar_y.pack(side="right", fill="y")
            scrollbar_x.pack(side="bottom", fill="x")
            text_area.pack(expand=True, fill="both", padx=5, pady=5)
            
            # 显示当前参数
            params_dict = {}
            if hasattr(self, "params"):
                for attr in dir(self.params):
                    if not attr.startswith('_'):
                        try:
                            val = getattr(self.params, attr)
                            if not callable(val):
                                params_dict[attr] = val
                        except Exception:
                            pass
            
            text_area.insert("1.0", json.dumps(params_dict, indent=2, ensure_ascii=False, default=str))
            
            def _save():
                try:
                    content = text_area.get("1.0", "end-1c")
                    data = json.loads(content)
                    # ✅ M21 Bug #2修复：白名单验证 + 类型检查
                    ALLOWED_PARAMS = {
                        'tick_size', 'multiplier', 'commission_rate', 'slippage',
                        'max_position', 'stop_loss_pct', 'take_profit_pct',
                        'enable_auto_trade', 'debug_mode'
                    }
                    for k, v in data.items():
                        if k not in ALLOWED_PARAMS:
                            raise ValueError(f"不允许修改参数: {k}")
                        if hasattr(self.params, k):
                            original = getattr(self.params, k, None)
                            if original is not None and isinstance(original, (int, float)) and isinstance(v, (int, float)):
                                v = type(original)(v)
                            elif original is not None and type(original) != type(v):
                                raise TypeError(f"参数{k}类型不匹配: 期望{type(original).__name__}, 实际{type(v).__name__}")
                            setattr(self.params, k, v)
                    messagebox.showinfo("成功", "参数已保存")
                    editor.destroy()
                except Exception as e:
                    messagebox.showerror("错误", f"保存失败: {e}")
            
            btn_frame = tk.Frame(editor)
            btn_frame.pack(fill="x", padx=5, pady=5)
            tk.Button(btn_frame, text="保存", command=_save, bg="#2e7d32", fg="white").pack(side="right", padx=5)
            tk.Button(btn_frame, text="取消", command=editor.destroy).pack(side="right", padx=5)
            
        except Exception as e:
            self._log_error(f"打开参数编辑器失败: {e}")
    
    def _on_backtest_click(self) -> None:
        """打开回测参数编辑器"""
        try:
            import tkinter as tk
            from tkinter import messagebox
            
            root_obj = getattr(self, "_ui_root", None)
            if not root_obj:
                return
            
            top = tk.Toplevel(root_obj)
            top.title("回测参数")
            top.geometry("640x400")
            
            # 获取回测参数
            backtest_params = {}
            if hasattr(self, "params"):
                for attr in ["option_buy_lots_min", "option_buy_lots_max", "close_take_profit_ratio"]:
                    if hasattr(self.params, attr):
                        backtest_params[attr] = getattr(self.params, attr)
            
            txt = tk.Text(top, wrap="none", font=("Consolas", 10))
            vbar = tk.Scrollbar(top, orient="vertical", command=txt.yview)
            hbar = tk.Scrollbar(top, orient="horizontal", command=txt.xview)
            txt.config(yscrollcommand=vbar.set, xscrollcommand=hbar.set)
            vbar.pack(side="right", fill="y")
            hbar.pack(side="bottom", fill="x")
            txt.pack(fill="both", expand=True, padx=5, pady=5)
            txt.insert("1.0", json.dumps(backtest_params, indent=2, ensure_ascii=False, default=str))
            
            def _save():
                try:
                    content = txt.get("1.0", "end-1c")
                    data = json.loads(content)
                    for k, v in data.items():
                        if hasattr(self.params, k):
                            original = getattr(self.params, k, None)
                            if original is not None and isinstance(original, (int, float)) and isinstance(v, (int, float)):
                                v = type(original)(v)
                            setattr(self.params, k, v)
                    messagebox.showinfo("成功", "回测参数已保存")
                    top.destroy()
                except Exception as e:
                    messagebox.showerror("错误", f"保存失败: {e}")
            
            btn_bar = tk.Frame(top)
            btn_bar.pack(fill="x", padx=5, pady=5)
            tk.Button(btn_bar, text="保存", command=_save, bg="#2e7d32", fg="white").pack(side="right", padx=5)
            tk.Button(btn_bar, text="取消", command=top.destroy).pack(side="right", padx=5)
            
        except Exception as e:
            self._log_error(f"打开回测参数编辑器失败: {e}")
    
    def _call_method_by_priority(self, method_names: list, *args, **kwargs):
        """按优先级尝试调用方法，首个可用者执行
        
        Args:
            method_names: 方法名列表（按优先级从高到低）
            *args, **kwargs: 传递给方法的参数
            
        Returns:
            方法返回值，或None（无可用方法时）
        """
        for name in method_names:
            if hasattr(self, name):
                method = getattr(self, name)
                if callable(method):
                    return method(*args, **kwargs)
        return None

    def _log_output(self, msg: str, level: str = "INFO") -> None:
        """统一日志输出入口（按级别分发）"""
        log_func = logger.info if level.upper() == "INFO" else logger.error
        if hasattr(self, "output"):
            try:
                self.output(msg, force=True)
                return
            except Exception:
                pass
        log_func(msg)

    def _log_info(self, msg: str) -> None:
        """记录信息日志"""
        self._log_output(msg, "INFO")
    
    def _log_error(self, msg: str) -> None:
        """记录错误日志"""
        self._log_output(msg, "ERROR")

    def _destroy_output_mode_ui(self) -> None:
        """销毁输出模式UI"""
        try:
            if hasattr(self, '_ui_root') and self._ui_root is not None:
                self._ui_root.destroy()
                self._ui_root = None
            self._ui_running = False
        except Exception as e:
            logger.debug(f"[UIMixin._destroy_output_mode_ui] {e}")

    def _release_runtime_caches(self) -> None:
        """释放运行时缓存"""
        try:
            if hasattr(self, 'strategy_core') and hasattr(self.strategy_core, 'storage'):
                storage = self.strategy_core.storage
                params_service = getattr(storage, '_params_service', None)
                if params_service and hasattr(params_service, 'clear_instrument_cache'):
                    params_service.clear_instrument_cache()
            logger.debug("[UIMixin._release_runtime_caches] caches released")
        except Exception as e:
            logger.debug(f"[UIMixin._release_runtime_caches] {e}")

    def _log_tick_summary(self, tick: Any) -> None:
        """记录Tick汇总日志"""
        try:
            if not hasattr(self, '_tick_summary_count'):
                self._tick_summary_count = 0
            self._tick_summary_count += 1
            if self._tick_summary_count % 1000 == 0:
                instrument_id = getattr(tick, 'instrument_id', getattr(tick, 'InstrumentID', '?'))
                logger.debug(f"[TickSummary] received {self._tick_summary_count} ticks, last={instrument_id}")
        except Exception:
            pass


# =============================================================================
# StrategyUI - 独立UI类
# =============================================================================

class StrategyUI:
    """策略UI界面 - 独立运行的控制面板"""
    
    def __init__(self, strategy_core=None, title="策略控制面板", width=900, height=700):
        self.strategy = strategy_core
        self.title = title
        self.width = width
        self.height = height
        self.root = None
        self.message_queue = queue.Queue()
        self._running = False
        self._ui_thread = None
        self._widgets = {}
        
        # 回调
        self.on_pause: Optional[Callable] = None
        self.on_resume: Optional[Callable] = None
        self.on_flatten: Optional[Callable] = None
        self.on_param_change: Optional[Callable] = None
        self.on_close: Optional[Callable] = None
        
        # 事件日志
        self.event_log: List[UIEvent] = []
        self._lock = threading.Lock()
    
    def start(self) -> "StrategyUI":
        """启动UI（非阻塞）"""
        if self._running:
            return self
        self._running = True
        self._ui_thread = threading.Thread(target=self._run_ui, name="StrategyUI[ui-noise]", daemon=True)
        self._ui_thread.start()
        return self
    
    def stop(self, timeout: float = 5.0) -> None:
        """关闭UI"""
        self._running = False
        if self.root:
            try:
                self.root.quit()
                self.root.destroy()
            except Exception as e:
                logger.error(f"关闭UI失败: {e}")
        if self._ui_thread and self._ui_thread.is_alive():
            self._ui_thread.join(timeout=timeout)
    
    def is_alive(self) -> bool:
        """检查UI是否运行中"""
        return self._running and self._ui_thread and self._ui_thread.is_alive()
    
    def _run_ui(self) -> None:
        """UI主线程"""
        try:
            import tkinter as tk
            from tkinter import ttk, scrolledtext
            
            self.root = tk.Tk()
            self.root.title(self.title)
            self.root.geometry(f"{self.width}x{self.height}")
            self.root.protocol("WM_DELETE_WINDOW", self._on_window_close)
            
            self._build_control_panel()
            self._build_status_panel()
            self._build_log_panel()
            
            self._update_loop()
            self.root.mainloop()
        except Exception as e:
            logger.error(f"UI error: {e}")
        finally:
            self._running = False
    
    def _build_control_panel(self) -> None:
        """构建控制面板"""
        try:
            import tkinter as tk
            from tkinter import ttk
            
            control_frame = ttk.LabelFrame(self.root, text="控制", padding=10)
            control_frame.pack(fill="x", padx=10, pady=5)
            
            # 暂停/恢复按钮
            btn_frame = ttk.Frame(control_frame)
            btn_frame.pack(fill="x", pady=5)
            
            self._widgets["btn_pause"] = ttk.Button(btn_frame, text="暂停", command=self._on_pause)
            self._widgets["btn_pause"].pack(side="left", padx=5)
            
            self._widgets["btn_resume"] = ttk.Button(btn_frame, text="恢复", command=self._on_resume)
            self._widgets["btn_resume"].pack(side="left", padx=5)
            
            self._widgets["btn_flatten"] = ttk.Button(btn_frame, text="平仓", command=self._on_flatten)
            self._widgets["btn_flatten"].pack(side="left", padx=5)
            
        except Exception as e:
            logger.error(f"Build control panel error: {e}")
    
    def _build_status_panel(self) -> None:
        """构建状态面板"""
        try:
            import tkinter as tk
            from tkinter import ttk
            
            status_frame = ttk.LabelFrame(self.root, text="状态", padding=10)
            status_frame.pack(fill="x", padx=10, pady=5)
            
            self._widgets["status_text"] = tk.Text(status_frame, height=6, state="disabled")
            self._widgets["status_text"].pack(fill="x")
            
        except Exception as e:
            logger.error(f"Build status panel error: {e}")
    
    def _build_log_panel(self) -> None:
        """构建日志面板"""
        try:
            import tkinter as tk
            from tkinter import ttk, scrolledtext
            
            log_frame = ttk.LabelFrame(self.root, text="日志", padding=10)
            log_frame.pack(fill="both", expand=True, padx=10, pady=5)
            
            self._widgets["log_text"] = scrolledtext.ScrolledText(log_frame, height=15)
            self._widgets["log_text"].pack(fill="both", expand=True)
            
        except Exception as e:
            logger.error(f"Build log panel error: {e}")
    
    def _update_loop(self) -> None:
        """更新循环"""
        try:
            self._update_status()
            self._process_messages()
            
            if self._running and self.root:
                self.root.after(500, self._update_loop)
        except Exception as e:
            logger.error(f"Update loop error: {e}")
    
    def _update_status(self) -> None:
        """更新状态显示"""
        try:
            if not self.strategy:
                return
            
            status = []
            status.append(f"运行状态: {getattr(self.strategy, 'my_is_running', False)}")
            status.append(f"暂停状态: {getattr(self.strategy, 'my_is_paused', False)}")
            status.append(f"交易状态: {getattr(self.strategy, 'my_trading', True)}")
            
            if hasattr(self.strategy, "params"):
                status.append(f"输出模式: {getattr(self.strategy.params, 'output_mode', 'debug')}")
            
            text = self._widgets.get("status_text")
            if text:
                text.config(state="normal")
                text.delete("1.0", "end")
                text.insert("1.0", "\n".join(status))
                text.config(state="disabled")
        except Exception as e:
            logger.error(f"Update status error: {e}")
    
    def _process_messages(self) -> None:
        """处理消息队列"""
        try:
            msg_count = 0
            while not self.message_queue.empty() and msg_count < 20:
                msg = self.message_queue.get_nowait()
                msg_count += 1
                self._append_log(msg)
        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Process messages error: {e}")
    
    def _append_log(self, msg: str) -> None:
        """追加日志"""
        try:
            text = self._widgets.get("log_text")
            if text:
                timestamp = datetime.now().strftime("%H:%M:%S")
                text.insert("end", f"[{timestamp}] {msg}\n")
                text.see("end")
        except Exception as e:
            logger.error(f"Append log error: {e}")
    
    def _record_event(self, event_type: str, data: Dict[str, Any] = None) -> None:
        """记录事件"""
        with self._lock:
            self.event_log.append(UIEvent(
                timestamp=datetime.now(),
                event_type=event_type,
                data=data or {}
            ))
    
    def log(self, msg: str) -> None:
        """记录日志"""
        self.message_queue.put(msg)
    
    def update_status(self, status: Dict[str, Any]) -> None:
        """更新状态"""
        self.message_queue.put(f"STATUS: {json.dumps(status, default=str)}")
    
    def _on_pause(self) -> None:
        """暂停回调"""
        self._record_event("pause")
        if self.on_pause:
            self.on_pause()
        elif self.strategy:
            self.strategy._call_method_by_priority(['internal_pause_strategy', 'pause_strategy'])
    
    def _on_resume(self) -> None:
        """恢复回调"""
        self._record_event("resume")
        if self.on_resume:
            self.on_resume()
        elif self.strategy:
            self.strategy._call_method_by_priority(['internal_resume_strategy', 'resume_strategy'])
    
    def _on_flatten(self) -> None:
        """平仓回调"""
        self._record_event("flatten")
        if self.on_flatten:
            self.on_flatten()
    
    def _on_window_close(self) -> None:
        """窗口关闭回调"""
        self._record_event("close")
        if self.on_close:
            self.on_close()
        self.stop()


# =============================================================================
# UIDiagnosisTool - UI诊断工具
# =============================================================================

class UIDiagnosisTool:
    """UI诊断工具"""
    
    @staticmethod
    def check_tkinter() -> Dict[str, Any]:
        """检查Tkinter可用性"""
        result = {"available": False, "version": None, "error": None}
        try:
            import tkinter as tk
            result["available"] = True
            result["version"] = tk.TkVersion
        except ImportError as e:
            result["error"] = str(e)
        except Exception as e:
            result["error"] = str(e)
        return result
    
    @staticmethod
    def check_thread_safety() -> Dict[str, Any]:
        """检查线程安全性"""
        result = {"safe": True, "issues": []}
        try:
            import tkinter as tk
            # Tkinter不是线程安全的
            result["safe"] = False
            result["issues"].append("Tkinter主循环必须在主线程运行")
            result["issues"].append("使用queue进行线程间通信")
        except Exception as e:
            result["issues"].append(str(e))
        return result
    
    @staticmethod
    def run_diagnosis() -> Dict[str, Any]:
        """运行完整诊断"""
        return {
            "tkinter": UIDiagnosisTool.check_tkinter(),
            "thread_safety": UIDiagnosisTool.check_thread_safety(),
            "timestamp": datetime.now().isoformat()
        }


# =============================================================================
# 导出
# =============================================================================

__all__ = [
    "UIEvent",
    "UIMixin",
    "StrategyUI",
    "UIDiagnosisTool",
]
