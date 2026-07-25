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

            # FIX-20260719-UI-5[V2]/UI-7[V11]: 补充 current_strategy_id
            # 根因: delegated 列表不包含 current_strategy_id，导致 _do_safe_pause 中
            #       getattr(self, 'current_strategy_id', 'unknown') 永远返回 'unknown'。
            # 修复: 1. delegated 列表补充 'current_strategy_id'
            #       2. UILogicService 中添加 current_strategy_id property（委托到 _host_ref.strategy_id）
            # 注意: internal_pause_strategy/internal_resume_strategy/pause_strategy/resume_strategy
            #       不需要加入 delegated 列表，因为 _to_close_debug 已改用 _call_method_by_priority
            #       三层路由（FIX-UI-4[V13]），不再使用 hasattr 检查。
            'current_strategy_id',

            # FIX-UI-11 (2026-07-20, RC-1): 补充 _on_param_modify_click / _on_backtest_click
            # 根因: _param_modify/_backtest_modify 闭包通过 self._on_param_modify_click() 调用，
            #       但这些方法定义在 UILogicService，未在 delegated 列表 → AttributeError
            # 证据: 2026-07-20 10:19:23/24 日志 "参数编辑失败: 'UICreationService' object has no attribute '_on_param_modify_click'"
            # 前次报告 FIX-UI-2 仅修复 _ui_root property，未识别方法名委托缺失 → 半拉子工程
            # 修复: delegated 列表追加两个方法名，使 __getattr__ 委托到 self._logic._on_xxx_click
            '_on_param_modify_click', '_on_backtest_click',

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

            except Exception as e:

                self._log_error(f"设置窗口置顶失败: {e}")

            try:

                w = safe_getattr_int(self.params, "ui_window_width", 320, 320)

                h = safe_getattr_int(self.params, "ui_window_height", 310, 310)

            except Exception as e:

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

                    # FIX-UI-7 (V11根因, 2026-07-19): 使用 _host_ref.strategy_id 替代 current_strategy_id
                    # 根因: UICreationService/UILogicService 均未定义 current_strategy_id，
                    #       delegated 列表也未包含，getattr 永远返回 'unknown'，日志无法追溯。
                    # 修复: 通过 _host_ref (Strategy2026 实例) 获取真实 strategy_id。
                    _host = getattr(self, '_host_ref', None)
                    strategy_id = getattr(_host, 'strategy_id', 'unknown') if _host is not None else 'unknown'

                    run_id = getattr(self, 'current_run_id', 'N/A')

                    _CAL.log_control_action_enter('pause', strategy_id, run_id, source='ui-button')

                    self._log_info(f">>> [UI] 用户点击安全暂停... (strategy_id={strategy_id})")

                    self._call_method_by_priority(['internal_pause_strategy', 'pause_strategy'])

                    # FIX-UI-6 (V10根因, 2026-07-19): 补充 infini.pause_strategy 双通道
                    # 根因: 原 _do_safe_pause 仅调用 _call_method_by_priority (Python侧)，
                    #       未通知 C++ 平台更新 UI 状态，导致平台显示"运行中"但实际已暂停。
                    # 修复: 补充 infini.pause_strategy 通道，与 StrategyUI._on_pause 对齐。
                    try:
                        if _host is not None and strategy_id != 'unknown':
                            from pythongo import infini
                            infini.pause_strategy(strategy_id)
                            self._log_info(f"[FIX-UI-6] infini.pause_strategy({strategy_id}) 已调用 (C++平台UI同步)")
                    except Exception as _infini_err:
                        self._log_warning(f"[FIX-UI-6] infini.pause_strategy 失败(非致命): {_infini_err}")

                    # FIX-20260720-6 (RC-20260720-1): 暂停后确认 _is_paused=True
                    # 根因: internal_pause_strategy 可能成功返回但 _is_paused 仍为 False
                    #       （如 strategy_core.pause() 内部异常被吞掉），用户以为已暂停但实际未暂停。
                    # 修复: 暂停后 3 级检查 _is_paused，若仍为 False 则记录 CRITICAL 告警。
                    try:
                        if _host is not None:
                            _sc = getattr(_host, 'strategy_core', None)
                            if _sc is not None:
                                _is_paused = getattr(_sc, '_is_paused', None)
                                if _is_paused is not True:
                                    import logging as _logging
                                    _logging.critical(
                                        "[FIX-20260720-6] 安全暂停后_is_paused仍为%s(期望True)! "
                                        "strategy_id=%s state=%s",
                                        _is_paused, strategy_id,
                                        getattr(_sc, '_state', 'N/A')
                                    )
                    except Exception as _verify_err:
                        pass  # 验证失败不阻断暂停流程

                except Exception as e:  # FIX-UI-3 (V4): 窄异常元组扩展为 except Exception

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

                        except Exception as e:

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

                    except Exception as e:

                        self._log_error(f"更新UI任务失败: {e}")

                except Exception as e:

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

                        except Exception as e:

                            self._log_error(f"显示错误对话框失败 {e}")

                        return



                    setattr(self.params, "debug_output", True)

                    setattr(self.params, "diagnostic_output", True)

                    self.params.test_mode = self.params.test_mode if hasattr(self.params, 'test_mode') else True

                    # FIX-UI-4 (V13根因, 2026-07-19): _to_close_debug 改用 _call_method_by_priority
                    # 根因: 原 hasattr(self, "internal_resume_strategy") 因 UICreationService.__getattr__
                    #       delegated 列表不含该方法名，hasattr 返回 False，resume 永不调用。
                    # 修复: 与 _to_debug L298 / _to_trade L451 对齐，统一使用 _call_method_by_priority 三层路由。
                    resumed = self._call_method_by_priority(['internal_resume_strategy', 'resume_strategy'])

                    if resumed:

                        self.my_trading = True

                    self.set_output_mode("close_debug")

                    self._refresh_output_mode_ui_styles()

                    try:

                        root = getattr(self, '_ui_root', None)

                        if root:

                            root.update_idletasks()

                    except Exception as e:

                        self._log_error(f"更新UI任务失败: {e}")

                except Exception as e:

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

                        except Exception as e:

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

                    except Exception as e:

                        self._log_error(f"更新UI任务失败: {e}")

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

                self._refresh_output_mode_ui_styles()



            def _to_manual_trading():

                self.set_auto_trading_mode(False)

                self._refresh_output_mode_ui_styles()



            def _daily_summary():

                # FIX-UI-8 (V12根因, 2026-07-19): _daily_summary 实现实际日结逻辑
                # 根因: 原 _daily_summary 仅 1 行日志，无实际日结逻辑，按钮看似无效。
                # 修复: 调用 _call_method_by_priority 路由到 Strategy2026 的日结方法（若存在），
                #       并记录详细日志供运维追溯。若无日结方法，显式标记为"未实现"而非静默。
                self._log_info(">>> [UI] 日结输出已触发，开始执行日结流程...")
                try:
                    _summary_done = self._call_method_by_priority([
                        'internal_daily_summary', 'daily_summary', 'on_daily_summary'
                    ])
                    if _summary_done:
                        self._log_info("[FIX-UI-8] 日结流程已执行完成 (strategy路由成功)")
                    else:
                        self._log_warning("[FIX-UI-8] 日结方法未在 Strategy2026 上找到，仅记录触发事件")
                except Exception as _daily_err:
                    self._log_error(f"[FIX-UI-8] 日结流程执行失败: {_daily_err}")



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

            # FIX-UI-2 (V6/V15/V16根因, 2026-07-19): 同步 _logic._ui_root
            # 根因: UILogicService._ui_root 是类属性 None (ui_logic_service.py L124)，
            #       UICreationService.__getattr__ 委托到 _logic._ui_root 返回 None，
            #       导致 _on_param_modify_click/_on_backtest_click/_refresh_output_mode_ui_styles 短路。
            # 修复: 在设置实例属性 self._ui_root 后，同步设置 _logic._ui_root，确保委托链路返回真实 root。
            try:
                if getattr(self, '_logic', None) is not None:
                    self._logic._ui_root = root
                    self._log_info("[FIX-UI-2] UILogicService._ui_root 已同步")
            except Exception as _ui_root_sync_err:
                self._log_error(f"[FIX-UI-2] _logic._ui_root 同步失败: {_ui_root_sync_err}")

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

                # FIX-UI-10 (V17, 2026-07-19): 增强 UI 销毁清理日志
                # 根因: 原 _on_close 无日志，无法追溯销毁过程是否完成，难以诊断UI重建失败问题。
                # 修复: 在销毁各阶段记录日志，便于运维追溯。
                self._log_info("[FIX-UI-10/V17] _on_close 触发，开始销毁UI窗口...")

                try:

                    root.destroy()

                    self._log_info("[FIX-UI-10/V17] root.destroy() 完成")

                except Exception as e:

                    self._log_error(f"[FIX-UI-10/V17] 关闭窗口失败: {e}")

                self._ui_running = False

                self._ui_root = None

                self._log_info("[FIX-UI-10/V17] _ui_running=False, _ui_root=None 已设置")

                # FIX-UI-2 (V6根因续, 2026-07-19): 销毁时同步清理 _logic._ui_root
                try:
                    if getattr(self, '_logic', None) is not None:
                        self._logic._ui_root = None
                        self._log_info("[FIX-UI-10/V17] _logic._ui_root=None 已同步清理")
                except Exception as _sync_err:
                    self._log_warning(f"[FIX-UI-10/V17] _logic._ui_root 清理失败(非致命): {_sync_err}")

                with cls._get_ui_lock():

                    setattr(cls, "_ui_global_running", False)

                    setattr(cls, "_ui_global_root", None)

                self._log_info("[FIX-UI-10/V17] _ui_global_running=False, _ui_global_root=None 已设置，UI销毁完成")



            root.protocol("WM_DELETE_WINDOW", _on_close)



            self._log_info("UI界面已在主线程中创建")

        except Exception as e:

            self._log_error(f"在主线程中创建UI失败: {e}")

            self._ui_running = False


    def _start_output_mode_ui(self) -> None:

        """启动简易输出模式界）"""
        if not hasattr(self, "_ui_queue"):

            self._ui_queue = queue.Queue()



        try:

            import tkinter as tk

        except Exception as e:

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

                except Exception as e:

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

            except Exception as e:

                self._log_error(f"清理遗留窗口失败: {e}")

            with cls._get_ui_lock():

                setattr(cls, "_ui_global_root", None)

                setattr(cls, "_ui_global_running", False)



        # P2 Bug #80修复：检查是否在主线程
        # FIX-UI-9 (V8, 2026-07-19): 增强线程模型日志
        # 根因: Tkinter 不是线程安全的，若 on_start 在 C++ 平台子线程被调用，
        #       UI 会在子线程创建，按钮回调可能失效或行为不确定。
        #       原代码仅记录"检测到非主线程"，未记录线程名/ident，难以诊断线程问题。
        # 修复: 记录 main_thread/current_thread 的 name/ident/is_main，便于运维追溯。
        import threading as _threading

        main_thread = _threading.main_thread()

        current_thread = _threading.current_thread()

        self._log_info(
            f"[FIX-UI-9/V8] 线程检查: main_thread={main_thread.name}(ident={main_thread.ident}), "
            f"current_thread={current_thread.name}(ident={current_thread.ident}), "
            f"is_main={current_thread == main_thread}"
        )



        if current_thread != main_thread:

            self._log_info(f"[FIX-UI-9/V8] 检测到非主线程调用UI({current_thread.name})，将通过queue调度到主线程({main_thread.name})")

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

                                            except Exception as e:

                                                self._log_error(f"UI线程中Tk root创建失败: {e}")

                                                root = None

                                        if root:

                                            self._create_ui_in_main_thread(root)

                                    elif action == "destroy":

                                        if root:

                                            try:

                                                root.destroy()

                                            except Exception as e:

                                                self._log_error(f"UI窗口销毁失败 {e}")

                                        should_continue = False

                                        with cls._get_ui_lock():

                                            self._ui_running = False

                                            setattr(cls, "_ui_global_running", False)

                            except queue.Empty:

                                pass

                            except Exception as e:

                                self._log_error(f"处理UI队列失败: {e}")

                            finally:

                                if should_continue and root:

                                    try:

                                        root.after(100, _process_ui_queue)

                                    except Exception as e:

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

                    except Exception as e:

                        self._log_error(f"UI主循环线程异常 {e}")

                        import traceback

                        self._log_error(traceback.format_exc())

                    finally:

                        try:

                            if 'root' in locals() and root:

                                root.destroy()

                        except Exception as e:

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

                            except Exception as e:

                                self._log_error(f"更新暂停状态失败 {e}")

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

                                self._log_error(f"销毁窗口失败 {e}")

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

            root.update()

            self._log_info("UI界面已创建（非阻塞模式）")



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




_UIMixinCreation = UICreationService


__all__ = [
    "UICreationService",
    "_UIMixinCreation",
]