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

    except Exception as e:

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

        # FIX-20260719-UI-1[V1]: params 改为 property 动态委托到 _host_ref.params
        # 根因: UIMixin.__init__ 中 _params = self.__dict__.get('params') 在 super().__init__() 之前调用，
        #       此时 Strategy2026.__init__ 尚未设置 self.params（在L87才设置），所以 _params=None。
        # 修复: 通过 property 动态委托到 _host_ref.params，始终返回最新值。
        #       这里使用 _local_params 存储本地副本，避免与 property 冲突。
        self._local_params = params

        import threading

        if self.__class__._ui_lock is None:

            self.__class__._ui_lock = threading.Lock()


    # FIX-20260719-UI-1[V1]: params property 动态委托
    @property
    def params(self):
        """动态获取params：优先从 _host_ref (Strategy2026) 获取最新params，
        其次回退到本地存储的 _local_params。

        FIX-20260719-UI-1[V1]: 修复 params 初始化时序问题。
        根因: UIMixin.__init__ 中 _params = self.__dict__.get('params') 在 super().__init__() 之前调用，
              此时 Strategy2026.__init__ 尚未设置 self.params（在L87才设置），所以 _params=None。
              后续 Strategy2026.__init__ 设置 self.params 不会同步更新 UILogicService.params。
              → 所有按钮回调中 set_output_mode/set_auto_trading_mode/setattr(self.params,...) 操作的
                UILogicService.params 永远是 None，导致 set_output_mode 触发 AttributeError
                被窄异常元组捕获后静默返回。
        修复: 通过 property 动态委托到 _host_ref.params，始终返回 Strategy2026 实例的最新 params。
        """
        _host = getattr(self, '_host_ref', None)
        if _host is not None:
            _host_params = getattr(_host, 'params', None)
            if _host_params is not None:
                return _host_params
        return self._local_params

    @params.setter
    def params(self, value):
        """Setter: 写入本地存储 _local_params（保持向后兼容）。
        注意: 不会同步写入 _host_ref.params，因为 _host_ref.params 由 Strategy2026.__init__ 管理。
        """
        self._local_params = value

    # FIX-20260719-UI-2[V6/V15/V16]: _ui_root property 动态委托
    @property
    def _ui_root(self):
        """动态获取 _ui_root：从 _host_ref._ui_creation_service 实例字典中获取。

        FIX-20260719-UI-2[V6/V15/V16]: 修复 _ui_root 类属性 None 导致短路。
        根因: UILogicService._ui_root 原本是类属性 None。UICreationService.__getattr__ 中
              '_ui_root' 在 delegated 列表，委托到 self._logic._ui_root 返回类属性 None。
              → _on_param_modify_click/_on_backtest_click 中 getattr(self, '_ui_root', None)
                返回 None → 方法直接 return，对话框永远不打开。
              → _refresh_output_mode_ui_styles 中 not getattr(self, '_ui_root') 永远 True
                → 样式永远不刷新，按钮高亮状态不更新。
        修复: 通过 property 动态委托到 _host_ref._ui_creation_service._ui_root（实例属性），
              该属性在 UICreationService._create_ui_in_main_thread L563 中设置。
        """
        _host = getattr(self, '_host_ref', None)
        if _host is not None:
            _creation = getattr(_host, '_ui_creation_service', None)
            if _creation is not None:
                # 直接访问实例字典，避免触发 __getattr__ 递归
                return _creation.__dict__.get('_ui_root')
        return None

    @_ui_root.setter
    def _ui_root(self, value):
        """Setter: 写入 _host_ref._ui_creation_service 实例字典。
        如果 _host_ref 尚未注入（__init__ 阶段），存到本地实例字典。
        """
        _host = getattr(self, '_host_ref', None)
        if _host is not None:
            _creation = getattr(_host, '_ui_creation_service', None)
            if _creation is not None:
                _creation.__dict__['_ui_root'] = value
                return
        # _host_ref 尚未注入时，存到本地实例字典
        self.__dict__['_ui_root'] = value

    # FIX-20260719-UI-7[V11]: current_strategy_id property 动态委托
    @property
    def current_strategy_id(self):
        """动态获取 strategy_id：从 _host_ref (Strategy2026) 获取。

        FIX-20260719-UI-7[V11]: 修复 current_strategy_id 永远是 'unknown'。
        根因: UICreationService/UILogicService 均未定义 current_strategy_id 属性，
              delegated 列表也未包含，getattr(self, 'current_strategy_id', 'unknown')
              永远返回 'unknown'。
              → ControlActionLogger 记录的 strategy_id 永远是 'unknown'，无法追溯实际策略实例。
        修复: 通过 property 动态委托到 _host_ref.strategy_id（int类型）。
        """
        _host = getattr(self, '_host_ref', None)
        if _host is not None:
            return getattr(_host, 'strategy_id', 'unknown')
        return 'unknown'


    @classmethod

    def _get_ui_lock(cls):

        """获取UI锁（确保线程安全量"""

        if cls._ui_lock is None:

            import threading

            cls._ui_lock = threading.Lock()

        return cls._ui_lock


    # UI状态（_M21 Bug #3修复：添加锁保护）
    _ui_lock: Any = None  # threading.Lock

    # FIX-20260719-UI-2[V6/V15/V16]: _ui_root 改为 property，不再使用类属性
    # _ui_lbl 等其他UI组件引用仍保留为类属性（仅在 _create_ui_in_main_thread 中设置实例属性）
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

            elif m == "open_debug":

                setattr(self.params, "debug_output", True)

                setattr(self.params, "diagnostic_output", True)

            elif m == "trade":

                setattr(self.params, "debug_output", False)

                setattr(self.params, "diagnostic_output", False)

            try:

                self._schedule_output_mode_ui_refresh()

            except Exception as e:

                self._log_error(f"调度UI刷新失败: {e}")

            self._log_info(f"输出模式切换换 {m}")

        except Exception as e:

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

            except Exception as e:

                self._log_error(f"调度UI刷新失败: {e}")

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

            # FIX-UI-LOCK-20260720: 改用白名单方式 + 最终序列化兜底try-except
            # 根因: FIX-UI-12的"单值预检"过滤存在3处缺陷:
            #   1. 第514行最终json_dumps(params_dict)缺少try-except保护
            #   2. race condition: json_default_serializer返回obj.__dict__引用,
            #      并发线程在预检和最终序列化间向__dict__写入Lock → TOCTOU竞态
            #   3. dir()与__getattr__动态属性不匹配, 可能漏过滤
            # 证据: 2026-07-20 13:03:49.713 "打开参数编辑器失败 Object of type lock is not JSON serializable"
            # 修复(与_on_backtest_click一致的白名单方式):
            #   1. 显式白名单仅取标量参数, 从根上消除遍历到不可序列化复杂对象的可能
            #   2. 最终序列化加try-except+降级, 防止race condition导致UI打不开
            #   3. 白名单与_save()的ALLOWED_PARAMS对齐(L527-535), 保证编辑后可保存
            ALLOWED_DISPLAY_PARAMS = {
                'tick_size', 'multiplier', 'commission_rate', 'slippage',
                'max_position', 'stop_loss_pct', 'take_profit_pct',
                'enable_auto_trade', 'debug_mode',
                # 扩展白名单: 包含用户可查看的额外参数(只读展示)
                'option_buy_lots_min', 'option_buy_lots_max', 'close_take_profit_ratio',
            }

            if hasattr(self, "params"):

                for attr in ALLOWED_DISPLAY_PARAMS:

                    if hasattr(self.params, attr):

                        try:

                            val = getattr(self.params, attr)

                            if not callable(val) and isinstance(val, (int, float, str, bool, type(None), list, dict)):

                                params_dict[attr] = val

                        except Exception as e:

                            self._log_error(f"读取参数{attr}失败: {e}")



            # 最终序列化加try-except兜底, 防止race condition导致UI打不开
            try:

                _params_json = json_dumps(params_dict, indent=2)

            # FIX-UI-13 (2026-07-20): 窄异常元组扩展为 Exception
            # 根因: json_dumps 可能抛 OverflowError/RecursionError 等不在 (TypeError, ValueError) 中的异常，
            #       导致降级路径被跳过，穿透到外层 except Exception → 对话框打不开。
            # 修复: 扩展为 except Exception，与实时回调路径硬约束 (NEW-1) 一致。
            except Exception as _serialize_err:

                # 降级: 仅保留标量类型, 剔除任何残留的复杂对象
                _safe_params = {

                    k: v for k, v in params_dict.items()

                    if isinstance(v, (int, float, str, bool, type(None)))

                }

                _params_json = json_dumps(_safe_params, indent=2)

                self._log_error(f"参数序列化降级(剔除复杂对象): {_serialize_err}")

            text_area.insert("1.0", _params_json)



            def _save():

                try:

                    content = text_area.get("1.0", "end-1c")

                    data = json_loads(content)

                    # _M21 Bug #2修复 + FIX-UI-14 (2026-07-20): 白名单验证 + 类型检查
                    # FIX-UI-14 根因: ALLOWED_DISPLAY_PARAMS 含只读展示参数(option_buy_lots_min等)
                    #       不在 ALLOWED_PARAMS 中，原逻辑遍历 data.items() 遇非 ALLOWED 键直接
                    #       raise ValueError → 用户不编辑任何内容点"保存"也会失败。
                    #       半拉子教训: 方法可调用+JSON可序列化，但保存不成功 = 消费最后环节失败。
                    # 修复: 对非 ALLOWED_PARAMS 的键跳过(continue)而非报错，仅保存白名单内参数。
                    ALLOWED_PARAMS = {

                        'tick_size', 'multiplier', 'commission_rate', 'slippage',

                        'max_position', 'stop_loss_pct', 'take_profit_pct',

                        'enable_auto_trade', 'debug_mode'

                    }

                    for k, v in data.items():

                        if k not in ALLOWED_PARAMS:

                            continue  # FIX-UI-14: 跳过只读展示参数，不报错

                        if hasattr(self.params, k):

                            original = getattr(self.params, k, None)

                            if original is not None and isinstance(original, (int, float)) and isinstance(v, (int, float)):

                                v = type(original)(v)

                            elif original is not None and type(original) != type(v):

                                raise TypeError(f"参数{k}类型不匹配 期望{type(original).__name__}, 实际{type(v).__name__}")

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

            # FIX-UI-15 (2026-07-20): json_dumps 补 try-except 降级
            # 根因: 若 backtest_params 中属性值为非标量类型，json_dumps 抛 TypeError
            #       被外层 except Exception 捕获 → "打开回测参数编辑器失败" → 对话框打不开。
            # 修复: 与 _on_param_modify_click 一致，加 try-except 降级兜底。
            try:
                _backtest_json = json_dumps(backtest_params, indent=2)
            except Exception as _bt_ser_err:
                _safe_bt = {k: v for k, v in backtest_params.items()
                            if isinstance(v, (int, float, str, bool, type(None)))}
                _backtest_json = json_dumps(_safe_bt, indent=2)
                self._log_error(f"回测参数序列化降级: {_bt_ser_err}")
            txt.insert("1.0", _backtest_json)



            def _save():

                try:

                    content = txt.get("1.0", "end-1c")

                    data = json_loads(content)

                    # FIX-UI-15 (2026-07-20): 回测参数白名单验证
                    # 根因: 原 _save 遍历 data.items() 直接 setattr 任意属性到 self.params，
                    #       用户可在 JSON 中设置 'strategy'/'market_center' 等危险键。
                    # 修复: 与 _on_param_modify_click 的 _save 一致，加白名单验证。
                    ALLOWED_BACKTEST_PARAMS = {
                        'option_buy_lots_min', 'option_buy_lots_max', 'close_take_profit_ratio',
                    }
                    for k, v in data.items():

                        if k not in ALLOWED_BACKTEST_PARAMS:
                            continue  # 跳过非白名单键

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

            except Exception as e:

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

                instrument_id = getattr(tick, 'instrument_id', '?')

                logger.debug(f"[TickSummary] received {self._tick_summary_count} ticks, last={instrument_id}")

        except Exception as e:

            logger.debug(f"[TickSummary] 记录失败: {e}")




_UIMixinLogic = UILogicService


__all__ = [
    "safe_getattr_int",
    "UIEvent",
    "UILogicService",
    "_UIMixinLogic",
]