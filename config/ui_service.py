# [M1-87] UI服务模块（门面）
#!/usr/bin/env python3

# MODULE_ID: M1-018

"""

ui_service.py - UI服务模块（门面）

拆分重构：原2519行拆分为3个文件
- ui_logic_service.py: 辅助函数 + UIEvent + UILogicService
- ui_creation_service.py: UICreationService
- ui_service.py: UIMixin + StrategyUI + UIDiagnosisTool + re-export

核心功能：
1. UIMixin - 策略UI混入口（组合门面）
2. StrategyUI - 独立UI控制面板（DEPRECATED）
3. UIDiagnosisTool - UI诊断工具

作者：CodeArts 代码智能体
版本：v2.1

"""

from __future__ import annotations

import threading
import queue
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Tuple

from infra._helpers import get_logger  # R9-5
from infra.shared_utils import CHINA_TZ
from infra.serialization_utils import json_dumps, json_loads

from config.ui_logic_service import (
    safe_getattr_int,
    UIEvent,
    UILogicService,
    _UIMixinLogic,
)
from config.ui_creation_service import (
    UICreationService,
    _UIMixinCreation,
)

logger = get_logger(__name__)  # R9-5


# =============================================================================
# UIMixin - 策略UI混入类（组合门面面
# =============================================================================

class UIMixin:

    """策略UI混入口- 提供Tkinter界面控制功能（Facade组合，消灭Mixin继承承


    [P1-2归属] 被Strategy2026(BaseStrategy, UIMixin)继承 | ServiceContainer可选注册为'ui'服务

    | 与StrategyCoreService无直接关闭StrategyCoreService仅继续核心Mixin)

    | 重构阶段: UI重构阶段处理(Phase 3+)


    组合并

    - UILogicService: UI逻辑/样式/模式切换方法

    - UICreationService: UI创建/启动方法

    """


    def __init__(self, *args, **kwargs):

        # 避免 getattr 触发尚未就绪。__getattr__ 导致 RecursionError
        _params = self.__dict__.get('params')

        self._ui_logic_service = UILogicService(params=_params)

        self._ui_creation_service = UICreationService(logic_service=self._ui_logic_service)

        # FIX-20260709-PAUSE-ROOT: 注入宿主引用，使 UILogicService._call_method_by_priority
        # 能通过 _host_ref 回溯到 Strategy2026 实例，查找 internal_pause_strategy 等方法
        self._ui_logic_service._host_ref = self
        self._ui_creation_service._host_ref = self

        try:

            super().__init__(*args, **kwargs)

        except TypeError:

            pass


    def __getattr__(self, name):

        for svc in (self._ui_logic_service, self._ui_creation_service):

            try:

                return getattr(svc, name)

            except AttributeError:

                continue

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")




# =============================================================================
# StrategyUI - 独立UI_
# =============================================================================

# DEPRECATED: StrategyUI无实例化调用方。UIMixin有消费者但StrategyUI没有。待删除或激活活
# R33-P2-7标记: 此类为死代码 _全仓零生产消费。'
# 仅被tests引用

# 待决策 集成到生产链表的删除

class StrategyUI:

    """策略UI界面 - 独立运行的控制面）"""


    def __init__(self, strategy_core=None, title="策略控制面板", width=900, height=700):

        import warnings

        warnings.warn(

            "StrategyUI is dead code (P2-7): zero production consumers. Pending integration or removal.",

            DeprecationWarning,

            stacklevel=2,

        )

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

        """启动UI（非阻塞塞"""

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

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logger.error(f"关闭UI失败: {e}")

        if self._ui_thread and self._ui_thread.is_alive():

            self._ui_thread.join(timeout=timeout)


    def is_alive(self) -> bool:

        """检查UI是否运行时"""

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

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.error(f"Build control panel error: {e}")


    def _build_status_panel(self) -> None:

        """构建状态面）"""
        try:

            import tkinter as tk

            from tkinter import ttk



            status_frame = ttk.LabelFrame(self.root, text="状态", padding=10)

            status_frame.pack(fill="x", padx=10, pady=5)



            self._widgets["status_text"] = tk.Text(status_frame, height=6, state="disabled")

            self._widgets["status_text"].pack(fill="x")



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.error(f"Build log panel error: {e}")


    def _update_loop(self) -> None:

        """更新循环"""

        try:

            self._update_status()

            self._process_messages()



            if self._running and self.root:

                self.root.after(500, self._update_loop)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.error(f"Update loop error: {e}")


    def _update_status(self) -> None:

        """更新状态显）"""
        try:

            if not self.strategy:

                return



            status = []

            status.append(f"运行状态 {getattr(self.strategy, 'my_is_running', False)}")

            status.append(f"暂停状态 {getattr(self.strategy, 'my_is_paused', False)}")

            status.append(f"交易状态 {getattr(self.strategy, 'my_trading', True)}")



            if hasattr(self.strategy, "params"):

                status.append(f"输出模式: {getattr(self.strategy.params, 'output_mode', 'debug')}")



            text = self._widgets.get("status_text")

            if text:

                text.config(state="normal")

                text.delete("1.0", "end")

                text.insert("1.0", "\n".join(status))

                text.config(state="disabled")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.error(f"Process messages error: {e}")


    def _append_log(self, msg: str) -> None:

        """追加日志"""

        try:

            text = self._widgets.get("log_text")

            if text:

                timestamp = datetime.now(CHINA_TZ).strftime("%H:%M:%S")

                text.insert("end", f"[{timestamp}] {msg}\n")

                text.see("end")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.error(f"Append log error: {e}")


    def _record_event(self, event_type: str, data: Dict[str, Any] = None) -> None:

        """记录事件"""

        with self._lock:

            self.event_log.append(UIEvent(

                timestamp=datetime.now(CHINA_TZ),

                event_type=event_type,

                data=data or {}

            ))


    def log(self, msg: str) -> None:

        """记录日志"""

        self.message_queue.put(msg)


    def update_status(self, status: Dict[str, Any]) -> None:

        """更新状态"""

        self.message_queue.put(f"STATUS: {json_dumps(status)}")


    def _on_pause(self) -> None:

        """暂停回调

        FIX-20260715-R4-DUAL-PAUSE: 双通道暂停
        根因: C++平台可能不派发on_stop/pause回调给Python策略实例
              导致用户点击暂停按钮后策略继续运行
        修复: 同时执行两个通道:
          1. infini.pause_strategy(strategy_id) — 通知C++平台暂停
          2. strategy.internal_pause_strategy() — 直接执行Python侧暂停逻辑
        """

        self._record_event("pause")

        # 通道1: 通知C++平台暂停（让C++更新UI状态）
        try:
            _sid = getattr(self.strategy, 'strategy_id', None) if self.strategy else None
            if _sid is not None:
                from pythongo import infini
                infini.pause_strategy(_sid)
                logging.info("[UI-R4-DUAL-PAUSE] infini.pause_strategy(%s) 已调用", _sid)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _infini_err:
            logging.warning("[UI-R4-DUAL-PAUSE] infini.pause_strategy失败(非致命): %s", _infini_err)

        # 通道2: 直接执行Python侧暂停逻辑（不依赖C++回调）
        if self.on_pause:

            self.on_pause()

        elif self.strategy:

            self.strategy._call_method_by_priority(['internal_pause_strategy', 'pause_strategy'])


    def _on_resume(self) -> None:

        """恢复回调

        FIX-20260715-R4-DUAL-RESUME: 双通道恢复（与R4-DUAL-PAUSE对称）
        """

        self._record_event("resume")

        # 通道1: 通知C++平台恢复
        try:
            _sid = getattr(self.strategy, 'strategy_id', None) if self.strategy else None
            if _sid is not None:
                from pythongo import infini
                infini.pause_strategy(_sid)
                logging.info("[UI-R4-DUAL-RESUME] infini.pause_strategy(%s) 已调用(恢复需C++重新启动)", _sid)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _infini_err:
            logging.warning("[UI-R4-DUAL-RESUME] infini调用失败(非致命): %s", _infini_err)

        # 通道2: 直接执行Python侧恢复逻辑
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

# R33-P2-7标记: 已集成到health_check_api.py生产链路

class UIDiagnosisTool:

    """UI诊断工具"""


    @staticmethod

    def check_tkinter() -> Dict[str, Any]:

        """检查Tkinter可用例"""

        result = {"available": False, "version": None, "error": None}

        try:

            import tkinter as tk

            result["available"] = True

            result["version"] = tk.TkVersion

        except ImportError as e:

            result["error"] = str(e)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            result["error"] = str(e)

        return result


    @staticmethod

    def check_thread_safety() -> Dict[str, Any]:

        """检查线程安全量"""

        result = {"safe": True, "issues": []}

        try:

            import tkinter as tk

            # Tkinter不是线程安全量

            result["safe"] = False

            result["issues"].append("Tkinter主循环必须在主线程运行")

            result["issues"].append("使用queue进行线程间通信")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            result["issues"].append(str(e))

        return result


    @staticmethod

    def run_diagnosis() -> Dict[str, Any]:

        """运行完整诊断"""

        return {

            "tkinter": UIDiagnosisTool.check_tkinter(),

            "thread_safety": UIDiagnosisTool.check_thread_safety(),

            "timestamp": datetime.now(CHINA_TZ).isoformat()

        }




# =============================================================================
# 导出
# =============================================================================

__all__ = [
    "safe_getattr_int",
    "UIEvent",
    "UILogicService",
    "_UIMixinLogic",
    "UICreationService",
    "_UIMixinCreation",
    "UIMixin",
    "UIService",
    "StrategyUI",
    "UIDiagnosisTool",
]

UIService = UIMixin
