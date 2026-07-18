# [M1-87] UI逻辑服务模块
#!/usr/bin/env python3

# MODULE_ID: M1-018a

"""

ui_logic_service.py - UI逻辑/样式/模式切换服务

拆分自：ui_service.py

核心功能：
1. safe_getattr_int - 安全整数属性获取辅助函数
2. UIEvent - UI事件数据类
3. UILogicService - UI逻辑/样式/模式切换服务

作者：CodeArts 代码智能体
版本：v2.1

"""

from __future__ import annotations

import threading
import logging
from typing import Any, Dict

from dataclasses import dataclass, field

from infra._helpers import get_logger  # R9-5
from infra.scheduler_service import is_market_open
from infra.serialization_utils import json_dumps, json_loads

logger = get_logger(__name__)  # R9-5


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

    except (ValueError, TypeError) as e:

        return default


# =============================================================================
# 数据类
# =============================================================================

@dataclass(slots=True)

class UIEvent:

    """UI事件记录"""

    timestamp: Any  # datetime

    event_type: str

    data: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# UILogicService - UI逻辑/样式/模式切换 服务
# =============================================================================

class UILogicService:

    """UI逻辑/样式/模式切换 服务


    包含: __init__, _get_ui_lock, 类属性 _refresh_output_mode_ui_styles,
    _schedule_output_mode_ui_refresh, set_output_mode, set_auto_trading_mode,
    _on_param_modify_click, _on_backtest_click, _call_method_by_priority,
    _log_output, _log_info, _log_error, _log_warning,
    _destroy_output_mode_ui, _release_runtime_caches, _log_tick_summary

    """


    def __init__(self, params=None):

        self.params = params

        import threading

        if self.__class__._ui_lock is None:

            self.__class__._ui_lock = threading.Lock()


    @classmethod

    def _get_ui_lock(cls):

        """获取UI锁（确保线程安全量"""

        if cls._ui_lock is None:

            import threading

            cls._ui_lock = threading.Lock()

        return cls._ui_lock


    # UI状态（_M21 Bug #3修复：添加锁保护）
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


    # 类级别单例（_M21 Bug #3修复：添加锁保护。
    _ui_global_root: Any = None

    _ui_global_running: bool = False

    _ui_global_creating: bool = False


    def _refresh_output_mode_ui_styles(self) -> None:

        """刷新UI样式"""

        try:

            if not hasattr(self, "_ui_root") or not getattr(self, "_ui_root"):

                return

            import tkinter as tk

            cur = str(getattr(self.params, 'output_mode', 'debug')).lower()

            if cur == "debug":

                display_mode = "open_debug" if is_market_open() else "close_debug"

            else:

                display_mode = cur

            try:

                if hasattr(self, "_ui_lbl") and self._ui_lbl:

                    self._ui_lbl.config(text=f"当前模式: {cur}")

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"设置按钮样式失败: {e}")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"刷新UI样式失败: {e}")


    def _schedule_output_mode_ui_refresh(self) -> None:

        """调度UI刷新"""

        try:

            if hasattr(self, "_ui_queue"):

                self._ui_queue.put({"action": "refresh_style"})

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

            elif m == "open_debug":

                setattr(self.params, "debug_output", True)

                setattr(self.params, "diagnostic_output", True)

            elif m == "trade":

                setattr(self.params, "debug_output", False)

                setattr(self.params, "diagnostic_output", False)

            try:

                self._schedule_output_mode_ui_refresh()

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"调度UI刷新失败: {e}")

            self._log_info(f"输出模式切换换 {m}")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"切换输出模式失败: {e}")


    def set_auto_trading_mode(self, auto: bool) -> None:

        """设置自动交易模式（统一状态源：以auto_trading_enabled为准）"""
        try:

            self.auto_trading_enabled = bool(auto)

            self.my_trading = self.auto_trading_enabled

            if self.params:

                setattr(self.params, "auto_trading_enabled", self.auto_trading_enabled)

            if self.auto_trading_enabled:

                self._log_info("已切换为自动交易模式")

            else:

                self._log_info("已切换为手动交易模式")

            try:

                self._schedule_output_mode_ui_refresh()

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"调度UI刷新失败: {e}")

            self._refresh_output_mode_ui_styles()

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                            self._log_error(f"读取参数{attr}失败: {e}")



            text_area.insert("1.0", json_dumps(params_dict, indent=2))



            def _save():

                try:

                    content = text_area.get("1.0", "end-1c")

                    data = json_loads(content)

                    # _M21 Bug #2修复：白名单验证 + 类型检查
                    ALLOWED_PARAMS = {

                        'tick_size', 'multiplier', 'commission_rate', 'slippage',

                        'max_position', 'stop_loss_pct', 'take_profit_pct',

                        'enable_auto_trade', 'debug_mode'

                    }

                    for k, v in data.items():

                        if k not in ALLOWED_PARAMS:

                            raise ValueError(f"不允许修改参数 {k}")

                        if hasattr(self.params, k):

                            original = getattr(self.params, k, None)

                            if original is not None and isinstance(original, (int, float)) and isinstance(v, (int, float)):

                                v = type(original)(v)

                            elif original is not None and type(original) != type(v):

                                raise TypeError(f"参数{k}类型不匹配 期望{type(original).__name__}, 实际{type(v).__name__}")

                            setattr(self.params, k, v)

                    messagebox.showinfo("成功", "参数已保存")

                    editor.destroy()

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    messagebox.showerror("错误", f"保存失败: {e}")



            btn_frame = tk.Frame(editor)

            btn_frame.pack(fill="x", padx=5, pady=5)

            tk.Button(btn_frame, text="保存", command=_save, bg="#2e7d32", fg="white").pack(side="right", padx=5)

            tk.Button(btn_frame, text="取消", command=editor.destroy).pack(side="right", padx=5)



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"打开参数编辑器失败 {e}")


    def _on_backtest_click(self) -> None:

        """打开回测参数编辑）"""
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

            txt.insert("1.0", json_dumps(backtest_params, indent=2))



            def _save():

                try:

                    content = txt.get("1.0", "end-1c")

                    data = json_loads(content)

                    for k, v in data.items():

                        if hasattr(self.params, k):

                            original = getattr(self.params, k, None)

                            if original is not None and isinstance(original, (int, float)) and isinstance(v, (int, float)):

                                v = type(original)(v)

                            setattr(self.params, k, v)

                    messagebox.showinfo("成功", "回测参数已保存")

                    top.destroy()

                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                    messagebox.showerror("错误", f"保存失败: {e}")



            btn_bar = tk.Frame(top)

            btn_bar.pack(fill="x", padx=5, pady=5)

            tk.Button(btn_bar, text="保存", command=_save, bg="#2e7d32", fg="white").pack(side="right", padx=5)

            tk.Button(btn_bar, text="取消", command=top.destroy).pack(side="right", padx=5)



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            self._log_error(f"打开回测参数编辑器失败 {e}")


    def _call_method_by_priority(self, method_names: list, *args, **kwargs):

        """按优先级尝试调用方法，首个可用者执行

        FIX-20260709-PAUSE-ROOT: 增加对宿主(Strategy2026)的查找。
        根因: _do_safe_pause() 闭包中 self 是 UICreationService 实例，
        通过 __getattr__ 委托到本方法后 self 是 UILogicService 实例，
        而 internal_pause_strategy 定义在 Strategy2026 上(非 UILogicService)，
        导致 hasattr(self, 'internal_pause_strategy') 永远 False → 暂停/恢复/删除被静默忽略。

        Args:
            method_names: 方法名列表（按优先级从高到低）
            *args, **kwargs: 传递给方法的参数
        Returns:
            方法返回值，或None（无可用方法时）
        """
        # 第一层：在 self (UILogicService) 上查找
        for name in method_names:
            if hasattr(self, name):
                method = getattr(self, name)
                if callable(method):
                    return method(*args, **kwargs)
        # 第二层：在 _host_ref (Strategy2026) 上查找（pause/resume/delete 等生命周期方法定义在此）
        # _host_ref 由 UIMixin.__init__ 注入
        try:
            _host = getattr(self, '_host_ref', None)
            if _host is not None:
                for name in method_names:
                    if hasattr(_host, name):
                        method = getattr(_host, name)
                        if callable(method):
                            logging.info("[_call_method_by_priority] 在 host(Strategy2026) 上找到并调用: %s", name)
                            return method(*args, **kwargs)
        except Exception as _cbp_e:
            logging.debug("[_call_method_by_priority] _host_ref 查找异常: %s", _cbp_e)
        # 第三层：在 strategy_core 上查找（兜底）
        try:
            _core = getattr(self, 'strategy_core', None)
            if _core is not None:
                for name in method_names:
                    if hasattr(_core, name):
                        method = getattr(_core, name)
                        if callable(method):
                            logging.info("[_call_method_by_priority] 在 strategy_core 上找到并调用: %s", name)
                            return method(*args, **kwargs)
        except Exception as _cbp_e:
            logging.debug("[_call_method_by_priority] strategy_core 查找异常: %s", _cbp_e)

        logging.warning(
            "[_call_method_by_priority] 所有目标均未找到: methods=%s",
            method_names,
        )
        return None


    def _log_output(self, msg: str, level: str = "INFO") -> None:

        """统一日志输出入口（按级别分发送"""

        # FIX-R37-UI-LOG: 修复WARNING级别被误判为ERROR的bug
        _lvl = level.upper()
        if _lvl == "INFO":
            log_func = logger.info
        elif _lvl == "WARNING":
            log_func = logger.warning
        elif _lvl == "DEBUG":
            log_func = logger.debug
        else:
            log_func = logger.error

        if hasattr(self, "output"):

            try:

                self.output(msg, force=True)

                return

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                self._log_error(f"UI输出失败: {e}")

        log_func(msg)


    def _log_info(self, msg: str) -> None:

        """记录信息日志"""

        self._log_output(msg, "INFO")


    def _log_error(self, msg: str) -> None:

        """记录错误日志"""

        self._log_output(msg, "ERROR")


    def _log_warning(self, msg: str) -> None:

        """记录警告日志"""

        self._log_output(msg, "WARNING")


    def _destroy_output_mode_ui(self) -> None:

        """销毁输出模式UI"""

        try:

            if hasattr(self, '_ui_root') and self._ui_root is not None:

                self._ui_root.destroy()

                self._ui_root = None

            self._ui_running = False

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

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

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.debug(f"[UIMixin._release_runtime_caches] {e}")


    def _log_tick_summary(self, tick: Any) -> None:

        """记录Tick汇总日志"""

        try:

            if not hasattr(self, '_tick_summary_count'):

                self._tick_summary_count = 0

            self._tick_summary_count += 1

            if self._tick_summary_count % 1000 == 0:

                instrument_id = getattr(tick, 'instrument_id', '?')

                logger.debug(f"[TickSummary] received {self._tick_summary_count} ticks, last={instrument_id}")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.debug(f"[TickSummary] 记录失败: {e}")




_UIMixinLogic = UILogicService


__all__ = [
    "safe_getattr_int",
    "UIEvent",
    "UILogicService",
    "_UIMixinLogic",
]