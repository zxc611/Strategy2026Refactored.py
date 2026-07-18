# [M1-87] UI创建服务模块
#!/usr/bin/env python3

# MODULE_ID: M1-018b

"""

ui_creation_service.py - UI创建/启动服务

拆分自：ui_service.py

核心功能：
1. UICreationService - UI创建/启动服务

作者：CodeArts 代码智能体
版本：v2.1

"""

from __future__ import annotations

import threading
import queue
from typing import Any

from infra._helpers import get_logger  # R9-5
from infra.scheduler_service import is_market_open
from config.ui_logic_service import safe_getattr_int

logger = get_logger(__name__)  # R9-5


# =============================================================================
# UICreationService - UI创建/启动 服务
# =============================================================================

class UICreationService:

    """UI创建/启动 服务


    包含：_create_ui_in_main_thread, _start_output_mode_ui, _schedule_bring_output_mode_ui_front

    """

    _ui_lock = None

    @classmethod
    def _get_ui_lock(cls):
        if cls._ui_lock is None:
            import threading
            cls._ui_lock = threading.Lock()
        return cls._ui_lock

    def __init__(self, logic_service=None):

        self._logic = logic_service


    def __getattr__(self, name):

        delegated = [

            '_log_error', '_log_info', '_log_warning', '_log_output',
            'params', '_ui_lock', '_ui_root',

            '_ui_lbl', '_ui_btn_debug', '_ui_btn_debug_off',

            '_ui_btn_trade', '_ui_btn_auto', '_ui_btn_manual',

            '_ui_running', '_ui_creating', '_ui_global_root',

            '_ui_global_running', '_ui_global_creating',

            '_ui_queue', 'set_output_mode', 'set_auto_trading_mode',

            '_refresh_output_mode_ui_styles', '_call_method_by_priority',

            'auto_trading_enabled', 'my_trading', 'strategy_core',

            '_schedule_output_mode_ui_refresh',

        ]

        if name in delegated and self._logic is not None:

            return getattr(self._logic, name)

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


    def _create_ui_in_main_thread(self, root: Any) -> None:

        """P2 Bug #80修复：在主线程中创建UI界面


        Args:

            root: Tk根窗口
        """

        try:

            import tkinter as tk

            cls = type(self._logic) if getattr(self, '_logic', None) is not None else self.__class__



            root.deiconify()  # 显示窗口

            root.title("输出模式控制")

            try:

                root.attributes('-topmost', True)

                root.after(200, lambda: root.attributes('-topmost', False))

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"设置窗口置顶失败: {e}")

            try:

                w = safe_getattr_int(self.params, "ui_window_width", 320, 320)

                h = safe_getattr_int(self.params, "ui_window_height", 310, 310)

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"读取UI窗口尺寸失败: {e}")

                w, h = 320, 310

            root.geometry(f"{w}x{h}")



            # 构建界面

            lbl = tk.Label(root, text=f"当前模式: {getattr(self.params, 'output_mode', 'debug')}")

            lbl.pack(pady=8)



            btn_frame = tk.Frame(root)

            btn_frame.pack(fill="x", padx=12, pady=5)

            debug_frame = tk.Frame(root)



            BTN_WIDTH = 12

            btn_debug = tk.Button(debug_frame, text="开盘调度", width=BTN_WIDTH)

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

                    from infra.health_monitor import ControlActionLogger as _CAL

                    strategy_id = getattr(self, 'current_strategy_id', 'unknown')

                    run_id = getattr(self, 'current_run_id', 'N/A')

                    _CAL.log_control_action_enter('pause', strategy_id, run_id, source='ui-button')

                    self._log_info(">>> [UI] 用户点击安全暂停...")

                    self._call_method_by_priority(['internal_pause_strategy', 'pause_strategy'])

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

                    if not is_market_open():

                        error_msg = "收盘时间内不能使用开盘调试模块"

                        self._log_error(error_msg)

                        try:

                            import tkinter as tk

                            from tkinter import messagebox

                            if hasattr(self, '_ui_root') and self._ui_root:

                                messagebox.showwarning("操作禁止", error_msg, parent=self._ui_root)

                            else:

                                messagebox.showwarning("操作禁止", error_msg)

                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                            self._log_error(f"显示错误对话框失败 {e}")

                        return



                    setattr(self.params, "debug_output", True)

                    setattr(self.params, "run_profile", "full")

                    setattr(self.params, "backtest_tick_mode", False)

                    setattr(self.params, "diagnostic_output", True)

                    self.params.test_mode = self.params.test_mode if hasattr(self.params, 'test_mode') else False

                    resumed = self._call_method_by_priority(['internal_resume_strategy', 'resume_strategy'])

                    if resumed:

                        self.my_trading = True

                    self.set_output_mode("open_debug")

                    self._refresh_output_mode_ui_styles()

                    try:

                        root = getattr(self, '_ui_root', None)

                        if root:

                            root.update_idletasks()

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                        self._log_error(f"更新UI任务失败: {e}")

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"切换调试模式失败: {e}")



            def _to_close_debug():

                try:

                    if is_market_open():

                        error_msg = "开盘时间内不能切换到收市调试模块"

                        self._log_error(error_msg)

                        try:

                            import tkinter as tk

                            from tkinter import messagebox

                            if hasattr(self, '_ui_root') and self._ui_root:

                                messagebox.showwarning("操作禁止", error_msg, parent=self._ui_root)

                            else:

                                messagebox.showwarning("操作禁止", error_msg)

                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                            self._log_error(f"显示错误对话框失败 {e}")

                        return



                    setattr(self.params, "debug_output", True)

                    setattr(self.params, "diagnostic_output", True)

                    self.params.test_mode = self.params.test_mode if hasattr(self.params, 'test_mode') else True

                    resumed = False

                    if hasattr(self, "internal_resume_strategy"):

                        resumed = self.internal_resume_strategy()

                    elif hasattr(self, "resume_strategy"):

                        resumed = bool(self.resume_strategy())

                    if resumed:

                        self.my_trading = True

                    self.set_output_mode("close_debug")

                    self._refresh_output_mode_ui_styles()

                    try:

                        root = getattr(self, '_ui_root', None)

                        if root:

                            root.update_idletasks()

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                        self._log_error(f"更新UI任务失败: {e}")

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"收市调试切换失败: {e}")



            def _to_trade():

                try:

                    if not is_market_open():

                        error_msg = "收盘时间内不能切换到交易模式（无法实际交易）"

                        self._log_error(error_msg)

                        try:

                            import tkinter as tk

                            from tkinter import messagebox

                            if hasattr(self, '_ui_root') and self._ui_root:

                                messagebox.showwarning("操作禁止", error_msg, parent=self._ui_root)

                            else:

                                messagebox.showwarning("操作禁止", error_msg)

                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                            self._log_error(f"显示错误对话框失败 {e}")

                        return



                    # P2修复：检查params是否初始化
                    if self.params is None:
                        self._log_info("params未初始化，跳过交易模式切换")
                        return
                    
                    setattr(self.params, "debug_output", False)

                    setattr(self.params, "diagnostic_output", False)

                    self.params.test_mode = self.params.test_mode if hasattr(self.params, 'test_mode') else False

                    setattr(self.params, "run_profile", "full")

                    setattr(self.params, "backtest_tick_mode", False)

                    self.my_trading = True

                    self.auto_trading_enabled = getattr(self, "auto_trading_enabled", False)

                    resumed = self._call_method_by_priority(['internal_resume_strategy', 'resume_strategy'])

                    self.set_output_mode("trade")

                    self._refresh_output_mode_ui_styles()

                    try:

                        root = getattr(self, '_ui_root', None)

                        if root:

                            root.update_idletasks()

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                        self._log_error(f"更新UI任务失败: {e}")

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"切换交易模式失败: {e}")



            def _to_backtest_mode():

                try:

                    setattr(self.params, "run_profile", "backtest")

                    setattr(self.params, "backtest_tick_mode", True)

                    setattr(self.params, "output_mode", "close_debug")

                    setattr(self.params, "debug_output", False)

                    setattr(self.params, "diagnostic_output", False)

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"切换回测模式失败: {e}")



            def _to_auto_trading():

                self.set_auto_trading_mode(True)

                self._refresh_output_mode_ui_styles()



            def _to_manual_trading():

                self.set_auto_trading_mode(False)

                self._refresh_output_mode_ui_styles()



            def _daily_summary():

                self._log_info("日结输出已触发")



            def _param_modify():

                try:

                    self._on_param_modify_click()

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"参数编辑失败: {e}")



            def _backtest_modify():

                try:

                    self._on_backtest_click()

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

            with cls._get_ui_lock():

                setattr(cls, "_ui_global_root", root)

                setattr(cls, "_ui_global_running", True)



            self._refresh_output_mode_ui_styles()

            self._log_info(f"UI界面已在主线程中创建，当前模块{getattr(self.params, 'output_mode', 'debug')}, auto_trading={getattr(self, 'auto_trading_enabled', False)}")



            def _on_close():

                try:

                    root.destroy()

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"关闭窗口失败: {e}")

                self._ui_running = False

                self._ui_root = None

                with cls._get_ui_lock():

                    setattr(cls, "_ui_global_running", False)

                    setattr(cls, "_ui_global_root", None)



            root.protocol("WM_DELETE_WINDOW", _on_close)



            self._log_info("UI界面已在主线程中创建")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"在主线程中创建UI失败: {e}")

            self._ui_running = False


    def _start_output_mode_ui(self) -> None:

        """启动简易输出模式界）"""
        if not hasattr(self, "_ui_queue"):

            self._ui_queue = queue.Queue()



        try:

            import tkinter as tk

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"tkinter不可逆 {e}")

            return



        cls = type(self._logic) if getattr(self, '_logic', None) is not None else self.__class__



        # 检查是否已运行（加锁保护）'
        with cls._get_ui_lock():

            if getattr(cls, "_ui_global_running", False):

                try:

                    self._schedule_bring_output_mode_ui_front()

                    self._log_info("输出模式界面已在运行")

                    return

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"前置窗口失败: {e}")



        # 清理遗留窗口（加锁保护）

        with cls._get_ui_lock():

            old_root = getattr(cls, "_ui_global_root", None)

        if old_root:

            try:

                if hasattr(self, "_ui_queue") and self._ui_queue:

                    self._ui_queue.put_nowait({"action": "destroy"})

                else:

                    old_root.destroy()

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"清理遗留窗口失败: {e}")

            with cls._get_ui_lock():

                setattr(cls, "_ui_global_root", None)

                setattr(cls, "_ui_global_running", False)



        # P2 Bug #80修复：检查是否在主线程
        import threading as _threading

        main_thread = _threading.main_thread()

        current_thread = _threading.current_thread()



        if current_thread != main_thread:

            self._log_info("检测到非主线程调用UI，将通过queue调度到主线程")

            if not hasattr(self, "_ui_queue"):

                self._ui_queue = queue.Queue()

            self._ui_queue.put({"action": "create_ui", "params": None})

            if not getattr(cls, "_ui_mainloop_started", False):

                setattr(cls, "_ui_mainloop_started", True)

                def _ui_mainloop_thread():

                    try:

                        import tkinter as tk

                        root = None

                        with cls._get_ui_lock():

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

                                        if root is None:

                                            try:

                                                root = tk.Tk()

                                                root.withdraw()

                                                with cls._get_ui_lock():

                                                    setattr(cls, "_ui_global_root", root)

                                                self._log_info("UI线程中Tk root创建成功")

                                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                                self._log_error(f"UI线程中Tk root创建失败: {e}")

                                                root = None

                                        if root:

                                            self._create_ui_in_main_thread(root)

                                    elif action == "destroy":

                                        if root:

                                            try:

                                                root.destroy()

                                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                                self._log_error(f"UI窗口销毁失败 {e}")

                                        should_continue = False

                                        with cls._get_ui_lock():

                                            self._ui_running = False

                                            setattr(cls, "_ui_global_running", False)

                            except queue.Empty:

                                pass

                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                self._log_error(f"处理UI队列失败: {e}")

                            finally:

                                if should_continue and root:

                                    try:

                                        root.after(100, _process_ui_queue)

                                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                        self._log_error(f"UI队列调度失败: {e}")



                        _process_ui_queue()



                        if root:

                            root.after(100, _process_ui_queue)

                            self._log_info("UI线程进入mainloop")

                            root.mainloop()

                            self._log_info("UI线程mainloop已退避")

                        else:

                            self._log_warning("Tk root创建失败，使用轮询模块")

                            import time as _time

                            poll_count = 0

                            while poll_count < 300:

                                if not self._ui_queue.empty():

                                    _process_ui_queue()

                                _time.sleep(0.1)

                                poll_count += 1

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                        self._log_error(f"UI主循环线程异常 {e}")

                        import traceback

                        self._log_error(traceback.format_exc())

                    finally:

                        try:

                            if 'root' in locals() and root:

                                root.destroy()

                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                            self._log_error(f"UI窗口最终清理失败 {e}")

                        setattr(cls, "_ui_mainloop_started", False)

                        with cls._get_ui_lock():

                            self._ui_running = False

                            setattr(cls, "_ui_global_running", False)



                t = _threading.Thread(target=_ui_mainloop_thread, daemon=True, name="UIMainLoop")

                t.start()

                self._log_info("UI主循环线程已启动")

            return



        # 在主线程中，直接创建UI（非阻塞方式）'
        try:

            import tkinter as tk

            root = tk.Tk()

            setattr(cls, "_ui_global_root", root)



            self._create_ui_in_main_thread(root)



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

                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                self._log_error(f"更新暂停状态失败 {e}")

                        elif action == "refresh_style":

                            try:

                                self._refresh_output_mode_ui_styles()

                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                self._log_error(f"刷新样式失败: {e}")

                        elif action == "bring_front":

                            try:

                                root.deiconify()

                                root.lift()

                                root.focus_force()

                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                self._log_error(f"前置窗口失败: {e}")

                        elif action == "destroy":

                            try:

                                root.destroy()

                                should_continue = False

                                self._ui_running = False

                                setattr(cls, "_ui_global_running", False)

                            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                                self._log_error(f"销毁窗口失败 {e}")

                except queue.Empty:

                    pass

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    self._log_error(f"处理队列失败: {e}")

                finally:

                    if should_continue and getattr(self, "_ui_running", False):

                        try:

                            root.after(100, _process_queue)

                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                            self._log_error(f"调度队列处理失败: {e}")



            root.after(100, _process_queue)

            root.update()

            self._log_info("UI界面已创建（非阻塞模式）")



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"输出模式界面异常: {e}")

            self._ui_running = False


    def _schedule_bring_output_mode_ui_front(self) -> None:

        """调度窗口前置"""

        try:

            if hasattr(self, "_ui_queue"):

                self._ui_queue.put({"action": "bring_front"})

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"调度窗口前置失败: {e}")




_UIMixinCreation = UICreationService


__all__ = [
    "UICreationService",
    "_UIMixinCreation",
]