"""平台兼容性接口层。
用于提供对不同交易平台调用习惯的兼容支持（别名映射），以及状态快照等辅助功能。
严格对应 Strategy20260105_3.py 中的生命周期别名。
"""
from __future__ import annotations
import traceback
from typing import Any, Dict, Optional, Set, List
import time
import re
import os
import json

class PlatformCompatMixin:
    """平台兼容性混入类"""

    # --- 平台生命周期别名映射 ---

    def on_create(self, *args: Any, **kwargs: Any) -> None:
        self.on_init(*args, **kwargs)
        # [SafeChain] 尝试调用 BaseStrategy 的对应生命周期（如果存在）
        # 这是为了确保 UI 组件（如 BaseStrategy 中可能存在的初始化逻辑）能被执行
        if hasattr(super(), "on_create"):
            try:
                super().on_create(*args, **kwargs)
            except Exception:
                pass
    
    def Create(self, *args: Any, **kwargs: Any) -> None:
        self.on_init(*args, **kwargs)
        if hasattr(super(), "Create"):
            try:
                super().Create(*args, **kwargs)
            except Exception:
                pass

    def onLoad(self, *args: Any, **kwargs: Any) -> None:
        self.on_init(*args, **kwargs)
        if hasattr(super(), "onLoad"):
            try:
                super().onLoad(*args, **kwargs)
            except Exception:
                pass

    def initialize(self, *args: Any, **kwargs: Any) -> None:
        self.on_init(*args, **kwargs)
        if hasattr(super(), "initialize"):
            try:
                super().initialize(*args, **kwargs)
            except Exception:
                pass

    def initStrategy(self, *args: Any, **kwargs: Any) -> None:
        self.on_init(*args, **kwargs) 
        if hasattr(super(), "initStrategy"):
            try:
                super().initStrategy(*args, **kwargs)
            except Exception:
                pass

    def onStart(self, *args: Any, **kwargs: Any) -> None:
        self.on_start(*args, **kwargs)
        if hasattr(super(), "onStart"):
            try:
                super().onStart(*args, **kwargs)
            except Exception:
                pass

    def Start(self, *args: Any, **kwargs: Any) -> None:
        self.on_start(*args, **kwargs)
        if hasattr(super(), "Start"):
            try:
                super().Start(*args, **kwargs)
            except Exception:
                pass

    def startStrategy(self, *args: Any, **kwargs: Any) -> None:
        if not getattr(self, "my_started", False):
            self.on_start(*args, **kwargs)
        if hasattr(super(), "startStrategy"):
            try:
                super().startStrategy(*args, **kwargs)
            except Exception:
                pass

    def start(self) -> None:
        """Start 直接调用，通常用于本地测试或非加壳运行"""
        self.on_start()
        if hasattr(super(), "start"):
            try:
                super().start()
            except Exception:
                pass

    def stopStrategy(self, *args: Any, **kwargs: Any) -> None:
        try:
            if hasattr(self, "stop"):
                self.stop()
        except Exception:
            pass
        self.my_state = "stopped"

    def onStop(self, *args: Any, **kwargs: Any) -> None:
        if hasattr(self, "on_stop"):
            self.on_stop(*args, **kwargs)

    def Stop(self, *args: Any, **kwargs: Any) -> None:
        pause_on_stop = bool(getattr(self.params, "pause_on_stop", True))
        if pause_on_stop:
            if hasattr(self, "on_stop"):
                self.on_stop(*args, **kwargs)
        else:
            if hasattr(self, "stop"):
                self.stop(*args, **kwargs)

    def onStopStrategy(self, *args: Any, **kwargs: Any) -> None:
        if hasattr(self, "on_stop"):
            self.on_stop(*args, **kwargs)

    def pause(self, *args: Any, **kwargs: Any) -> None:
        try:
            self.output("收到 pause 调用")
            if hasattr(self, "pause_strategy"):
                self.pause_strategy()
            try:
                # 尝试调用父类 on_pause (如果存在)
                super_pause = getattr(super(), "on_pause", None)
                if callable(super_pause):
                    super_pause(*args, **kwargs)
            except Exception:
                pass
        except Exception as e:
            self.output(f"策略暂停失败: {e}\n{traceback.format_exc()}")

    def resume(self, *args: Any, **kwargs: Any) -> None:
        try:
            self.output("收到 resume 调用")
            if hasattr(self, "resume_strategy"):
                self.resume_strategy()
            try:
                super_resume = getattr(super(), "on_resume", None)
                if callable(super_resume):
                    super_resume(*args, **kwargs)
            except Exception:
                pass
        except Exception as e:
            self.output(f"策略恢复失败: {e}\n{traceback.format_exc()}")

    def pauseStrategy(self, *args: Any, **kwargs: Any) -> None:
        self.pause(*args, **kwargs)

    def resumeStrategy(self, *args: Any, **kwargs: Any) -> None:
        self.resume(*args, **kwargs)

    def onPause(self, *args: Any, **kwargs: Any) -> None:
        self.pause(*args, **kwargs)

    def onResume(self, *args: Any, **kwargs: Any) -> None:
        self.resume(*args, **kwargs)

    def Pause(self, *args: Any, **kwargs: Any) -> None:
        self.pause(*args, **kwargs)

    def Resume(self, *args: Any, **kwargs: Any) -> None:
        self.resume(*args, **kwargs)
        
    def on_start_deprecated_v1(self, *args: Any, **kwargs: Any) -> None:
        self.on_start(*args, **kwargs)

    # --- 属性与状态别名 (MOVED TO MAIN CLASS TO AVOID MRO CONFLICTS) ---
    # These properties are defined in Strategy2026Refactored to match Source _3.py logic
    # and provide explicit overrides for BaseStrategy.
    # We do NOT define them here to avoid 'setter missing' errors if Mixin is loaded before Main updates.
    
    # @property
    # def is_running(self) -> bool:
    #     return getattr(self, "my_is_running", False)
    # ... (Moved to Strategy2026Refactored) ...


    # --- 显式 Handler 别名 ---

    def handle_tick(self, tick: Any) -> None:
        if hasattr(self, "on_tick"):
            self.on_tick(tick)

    def handle_trade(self, trade: Any) -> None:
        if hasattr(self, "on_trade"):
            self.on_trade(trade)

    def handle_order(self, order: Any) -> None:
        if hasattr(self, "on_order"):
            self.on_order(order)
            
    def on_kline(self, kline: Any) -> None:
        # Refactored uses internal KLineGenerator, but expose this for external push
        pass

    # --- 上下文判断 ---

    def _is_backtest_context(self) -> bool:
        """检查是否回测上下文"""
        try:
            return str(getattr(self.params, "run_mode", "")).lower() == "backtest"
        except Exception:
            return False

    def _is_trade_context(self) -> bool:
        """检查是否实盘/模拟交易上下文"""
        try:
            return str(getattr(self.params, "run_mode", "")).lower() in ["trade", "paper"]
        except Exception:
            return False

    # --- UI & Profile Support (Shells) ---

    def _start_output_mode_ui(self) -> None:
        """启动输出模式UI"""
        pass

    def _schedule_output_mode_ui_refresh(self) -> None:
        pass

    def _schedule_bring_output_mode_ui_front(self) -> None:
        pass

    def _refresh_output_mode_ui_styles(self) -> None:
        pass

    def set_output_mode(self, mode: str) -> None:
        if hasattr(self.params, "output_mode"):
            setattr(self.params, "output_mode", mode)

    def use_lite_profile(self) -> None:
        self.apply_profile("lite")

    # --- Lifecycle Aliases ---

    def destroyStrategy(self) -> None:
        self.output("destroyStrategy called")
        self.on_stop()
        self.my_destroyed = True

    def Destroy(self) -> None:
        self.destroyStrategy()

    # @property
    # def destroyed(self) -> bool:
    #     return getattr(self, "my_destroyed", False)
    # ... (Moved to Strategy2026Refactored) ...

    def pause_strategy(self) -> None:
        self.my_is_paused = True
        # [Fix] Use thread-safe print instead of UI-bound output
        print(">>> [Compat] Strategy Paused (Thread-Safe)")
        
        # [SafePause] Send UI update request securely
        if hasattr(self, "_ui_queue") and self._ui_queue:
            try:
                self._ui_queue.put({"action": "pause_status", "paused": True})
            except: pass

    # [SafePause] Explicit alias for Manager compliance
    def safe_pause(self) -> bool:
        """安全暂停接口，返回是否成功"""
        try:
            self.pause_strategy()
            return True
        except Exception as e:
            print(f"Safe Pause Failed: {e}")
            return False

    def resume_strategy(self) -> None:
        self.my_is_paused = False
        # [Fix] Use thread-safe print instead of UI-bound output
        print(">>> [Compat] Strategy Resumed (Thread-Safe)")
        
        # [SafePause] Send UI update request securely
        if hasattr(self, "_ui_queue") and self._ui_queue:
            try:
                self._ui_queue.put({"action": "pause_status", "paused": False})
            except: pass

    def on_pause(self, *args: Any) -> None:
        self.pause_strategy()

    def on_resume(self, *args: Any) -> None:
        self.resume_strategy()
    
    def stop(self) -> None:
        """策略停止逻辑 (Source Parity)"""
        if self.my_destroyed:
            return
        
        # 停止平仓管理器
        try:
            if hasattr(self, 'position_manager') and self.position_manager:
                if hasattr(self.position_manager, "stop"):
                    self.position_manager.stop()
        except Exception:
            pass
            
        self.on_stop()

    def _set_strategy_state(self, state: str) -> None:
        self.my_state = state

    # --- Data & Caches ---

    def _cleanup_caches(self) -> None:
        """清理缓存"""
        if hasattr(self, "kline_data"):
             self.kline_data.clear() # type: ignore

    def _write_local_log(self, msg: str) -> None:
        pass # Stub

    def _diagnostic_output_allowed(self) -> bool:
        """诊断/测试输出是否允许；交易/回测统一关闭。"""
        try:
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
            if mode == "trade" or self._is_trade_context() or self._is_backtest_context():
                return False
        except Exception:
            pass
        try:
            return bool(getattr(self.params, "diagnostic_output", True))
        except Exception:
            return False

    def _debug_throttled(
        self,
        msg: str,
        category: str = "generic",
        min_interval: float = 60.0,
        force: bool = False,
    ) -> None:
        """带节流与分类控制的调试输出。可用 params.debug_disable_categories 关闭类别。"""
        try:
            allow_trade = False
            if not self._diagnostic_output_allowed():
                if self._is_trade_context():
                    if category == "trade_required":
                        allow_trade = True
                    allowlist = getattr(self.params, "trade_debug_allowlist", None)
                    allowed: Set[str] = set()
                    if hasattr(re, "split") and isinstance(allowlist, str):
                        allowed = {s.strip().lower() for s in re.split(r"[;,|\s]+", allowlist) if s.strip()}
                    elif isinstance(allowlist, (list, tuple, set)):
                        allowed = {str(s).strip().lower() for s in allowlist if str(s).strip()}
                    if category and category.lower() in allowed:
                        allow_trade = True
                if not allow_trade:
                    return
            
            debug_output = getattr(self.params, "debug_output", False)
            if not debug_output and not force and not allow_trade:
                return

            disabled = getattr(self.params, "debug_disable_categories", None)
            disabled_set: Set[str] = set()
            if hasattr(re, "split") and isinstance(disabled, str):
                disabled_set = {s.strip().lower() for s in re.split(r"[;,|\s]+", disabled) if s.strip()}
            elif isinstance(disabled, (list, tuple, set)):
                disabled_set = {str(s).strip().lower() for s in disabled if str(s).strip()}
            if category and category.lower() in disabled_set:
                return

            interval = getattr(self.params, "debug_throttle_seconds", None)
            if isinstance(interval, (int, float)) and interval >= 0:
                min_interval = float(interval)
            overrides = getattr(self.params, "debug_throttle_map", None)
            if isinstance(overrides, dict):
                ov = overrides.get(category)
                if isinstance(ov, (int, float)) and ov >= 0:
                    min_interval = float(ov)

            if min_interval <= 0:
                self.output(msg) # Mixins call self.output
                return

            now = time.time()
            if not hasattr(self, "_debug_throttle_last") or not isinstance(self._debug_throttle_last, dict):
                self._debug_throttle_last = {}
            
            last_map = self._debug_throttle_last
            last_ts = float(last_map.get(category, 0.0) or 0.0)
            if now - last_ts < min_interval:
                return
            last_map[category] = now
            
            if self._is_trade_context() and category == "trade_required":
                 # Original used self.output(msg, trade=True, trade_table=True)
                 # We fallback to standard output as signature might differ
                 self.output(msg)
            else:
                 self.output(msg)
        except Exception:
            pass

    def _is_paused_or_stopped(self) -> bool:
        return self.is_paused or (self.state in ["stopped", "stopping"])


    def _log_status_snapshot(self, stage: str = "") -> None:
        """输出当前状态快照"""
        try:
            self.output(f"[{stage}] Status: running={getattr(self, 'my_is_running', False)}, paused={getattr(self, 'my_is_paused', False)}, trading={getattr(self, 'my_trading', False)}")
        except Exception:
            pass

    def check_pause_file(self) -> None:
        """检查本地暂停文件标志 (手动干预接口)"""
        # 实现留空，视具体需求迁移
        pass
    
    def apply_profile(self, profile_name: str) -> None:
        """应用运行预设 (lite/full)"""
        try:
            if profile_name.lower() == "lite":
                self.output("应用 LITE 预设：仅订阅必要行情，减少日志")
                # 设置 params 属性
                if hasattr(self, "params"):
                    setattr(self.params, "debug_output", False)
                    setattr(self.params, "run_profile", "lite")
                    setattr(self.params, "subscribe_options", True) 
            elif profile_name.lower() == "full":
                self.output("应用 FULL 预设：全量功能开启")
                if hasattr(self, "params"):
                    setattr(self.params, "run_profile", "full")
            
            # 标记已应用
            self._profile_applied = True
        except Exception as e:
            self.output(f"应用预设失败: {e}")

    def _load_external_config(self) -> None:
        """从本地配置文件加载参数覆盖值"""
        try:
             current_dir = os.path.dirname(os.path.abspath(__file__))
             project_root = os.path.dirname(current_dir)
             
             candidates = [
                 os.path.join(project_root, "config", "strategy_settings.json"),
                 os.path.join(project_root, "strategy_settings.json"),
                 os.path.join(current_dir, "config", "strategy_settings.json"), 
             ]
             
             cfg_path = None
             for p in candidates:
                 if os.path.isfile(p):
                     cfg_path = p
                     break
            
             if not cfg_path:
                 return

             with open(cfg_path, "r", encoding="utf-8") as f:
                 data = json.load(f)
             if not isinstance(data, dict):
                 return

             applied: Dict[str, Any] = {}
             if hasattr(self, "params"):
                 for k, v in data.items():
                     if hasattr(self.params, k):
                         try:
                             setattr(self.params, k, v)
                             applied[k] = v
                         except Exception:
                             pass
                 
                 if "INFINI_API_KEY" in data:
                     try:
                         setattr(self.params, "infini_api_key", data["INFINI_API_KEY"])
                     except Exception: pass
                 if "API_KEY" in data:
                     try:
                         setattr(self.params, "api_key", data["API_KEY"])
                     except Exception: pass
             
             if applied:
                 self.output(f"已加载外部配置: {list(applied.keys())}")

        except Exception as e:
            self.output(f"外部配置加载失败: {e}")

