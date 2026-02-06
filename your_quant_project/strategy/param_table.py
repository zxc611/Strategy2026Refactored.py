"""参数表/映射加载与编辑。"""
from __future__ import annotations

import json
import os
import time
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 参数表/映射缓存（进程级）
_PARAM_CACHE: Dict[str, Any] = {}
_PARAM_CACHE_META: Dict[str, Optional[float]] = {}
_MAPPING_CACHE: Dict[str, Any] = {}
_CONTRACT_CACHE: Dict[str, bool] = {}


def load_param_table_cached(param_table_path: str) -> Dict[str, Any]:
    """带mtime缓存的参数表加载。"""
    try:
        if not param_table_path:
            return {}
        mtime = None
        try:
            mtime = os.path.getmtime(param_table_path)
        except Exception:
            mtime = None
        if param_table_path in _PARAM_CACHE and _PARAM_CACHE_META.get(param_table_path) == mtime:
            cached = _PARAM_CACHE.get(param_table_path)
            return cached if isinstance(cached, dict) else {}
        with open(param_table_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _PARAM_CACHE[param_table_path] = data
            _PARAM_CACHE_META[param_table_path] = mtime
            return data
        return {}
    except Exception:
        return {}


class ParamTableMixin:
    """参数表加载/调试/编辑相关逻辑。"""

    def output(self, msg: str, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        # [Strict Match _3.py] 完全复刻 Strategy20260105_3.py 的 output 逻辑
        """输出到平台并写入本地文件；受调试开关控制，交易/强制信息不受限"""
        try:
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
        except Exception:
            mode = "debug"
        if mode == "debug":
            mode = "close_debug"
        is_trade_like = mode in ("trade", "open_debug")

        is_trade_msg = bool(kwargs.get("trade", False))
        is_force_msg = bool(kwargs.get("force", False))
        is_trade_table = bool(kwargs.get("trade_table", False))

        try:
            trade_quiet = bool(getattr(self.params, "trade_quiet", True))
        except Exception:
            trade_quiet = True
        
        # [Safety Logic] 如果 BaseStrategy 是 Dummy，那么 super().output 可能无效
        # 此时应尽量确保日志生成，而非直接返回
        # override: 如果模式是 trade 且要求 quiet，则拦截
        if is_trade_like and trade_quiet and not is_trade_table:
            return

        # 诊断/测试输出自动识别关键字，可显式传入 diag=True
        is_diag_msg = bool(kwargs.pop("diag", False))
        try:
            msg_str = str(msg)
            if "调试" in msg_str or "诊断" in msg_str or "sanity" in msg_str.lower() or "验证" in msg_str:
                is_diag_msg = True
        except Exception:
            pass

        # 需要检查 diagnostic_output 开关
        diag_enabled = True
        try:
             diag_enabled = bool(getattr(self.params, "diagnostic_output", True))
        except Exception:
            pass
            
        if is_diag_msg and not diag_enabled and not is_trade_msg and not is_force_msg:
            return

        # 在调试模式下，默认开启调试输出开关
        try:
            dbg_enabled = diag_enabled and (bool(getattr(self.params, "debug_output", False)) or mode == "close_debug")
        except Exception:
            dbg_enabled = True # 默认开启，防止无日志

        # 交易模式下，仅输出交易或强制信息
        if is_trade_like and not is_trade_msg and not is_force_msg:
            return
        # 非交易模式，若调试开关关闭且非强制，跳过输出
        if not is_trade_msg and not is_force_msg and not dbg_enabled:
            return
            
        # [Noise Filter] 屏蔽无效映射日志
        try:
            msg_str = str(msg)
            if "使用有效映射" in msg_str or "Using effective mapping" in msg_str:
                 return
        except Exception:
            pass

        try:
            # 兼容 BaseStrategy.output 签名 (BaseStrategy.output 只接受 msg)
            if hasattr(super(), "output"):
                try:
                    super().output(msg)
                except TypeError:
                    pass
        except Exception:
            pass
            
        # [Fallback] 强制写入本地日志，防止 UI 不显示
        try:
            log_dir = os.path.dirname(os.path.abspath(__file__))
            log_path = os.path.join(log_dir, "strategy_output_fallback.log")
            # Limit file size or mode? Just append for now
            timestamp = datetime.now().strftime("%H:%M:%S")
            with open(log_path, "a", encoding="utf-8") as f:
                 f.write(f"[{timestamp}] {msg}\n")
        except:
            pass

    def _resolve_param_table_path(self) -> str:
        """解析参数表绝对路径；优先用户配置，相对路径以脚本目录为基准，最终回退同目录 param_table.json。"""
        try:
            raw = getattr(self.params, "param_override_table", "") or ""
            base_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(os.path.dirname(base_dir))
            recovery_dir = os.path.join(project_dir, "temp_git_recovery")
            candidates = []
            if isinstance(raw, str) and raw.strip():
                if os.path.isabs(raw):
                    candidates.append(raw)
                else:
                    candidates.append(os.path.join(base_dir, raw))
            candidates.append(os.path.join(base_dir, "param_table.json"))
            candidates.append(os.path.join(recovery_dir, "param_table.json"))
            candidates.append(os.path.join(project_dir, "param_table.json"))
            for p in candidates:
                if p and os.path.exists(p):
                    return p
            return candidates[0] if candidates else ""
        except Exception:
            return ""

    def _log_param_table_debug(self, message: str, path: str = "") -> None:
        """参数表调试日志节流，避免高频重复输出。"""
        try:
            if not getattr(self.params, "debug_output", False):
                return
            now = time.time()
            last_ts = getattr(self, "_param_table_log_last_ts", 0.0)
            last_path = getattr(self, "_param_table_log_last_path", "")
            if path and path != last_path:
                self._param_table_log_last_path = path
                self._param_table_log_last_ts = 0.0
                last_ts = 0.0
            interval = 60.0
            overrides = getattr(self.params, "debug_throttle_map", None)
            if isinstance(overrides, dict):
                ov = overrides.get("param_table")
                if isinstance(ov, (int, float)) and ov >= 0:
                    interval = float(ov)
            if now - last_ts < interval:
                return
            self._param_table_log_last_ts = now
            self.output(message)
        except Exception:
            pass

    def _reset_param_edit_quota_if_new_month(self) -> None:
        """跨月重置参数表修改次数。"""
        try:
            current_month = datetime.now().strftime("%Y-%m")
            if getattr(self, "param_edit_month", None) != current_month:
                self.param_edit_month = current_month
                self.param_edit_count = 0
        except Exception:
            pass

    def _load_param_table(self) -> Dict[str, Any]:
        """从参数表字段解析JSON，失败返回空表。"""
        try:
            path = self._resolve_param_table_path()
            exists = bool(path and os.path.exists(path))
            mtime = None
            if exists:
                try:
                    mtime = os.path.getmtime(path)
                except Exception:
                    mtime = None
            cache_hit = bool(exists and (path in _PARAM_CACHE) and (_PARAM_CACHE_META.get(path) == mtime))
            self._log_param_table_debug(
                f"[调试] 参数表路径: {path} | exists={exists} | cache={'hit' if cache_hit else 'miss'}",
                path=path,
            )
            if exists:
                try:
                    data = load_param_table_cached(path)
                    if isinstance(data, dict):
                        self._log_param_table_debug(
                            f"[调试] 参数表加载成功，共{len(data)}个键",
                            path=path,
                        )
                        return data
                except Exception as e:
                    self._log_param_table_debug(
                        f"[调试] 参数表JSON解析失败: {e}",
                        path=path,
                    )
            else:
                self._log_param_table_debug(
                    "[调试] 参数表文件不存在，使用空表",
                    path=path,
                )

            raw = getattr(self.params, "param_override_table", "") or ""
            if getattr(self.params, "debug_output", False):
                self._log_param_table_debug(
                    f"[调试] 参数表原始值: {raw[:100] if isinstance(raw, str) and len(raw) > 100 else raw}",
                    path=path,
                )
            if isinstance(raw, dict):
                self._log_param_table_debug(
                    f"[调试] 参数表是dict类型，共{len(raw)}个键",
                    path=path,
                )
                raw_key = f"raw:{hash(str(raw))}"
                if raw_key in _PARAM_CACHE:
                    cached = _PARAM_CACHE.get(raw_key)
                    if isinstance(cached, dict):
                        return cached
                data = dict(raw)
                _PARAM_CACHE[raw_key] = data
                return data
            if isinstance(raw, str) and raw.strip():
                try:
                    raw_key = f"raw:{hash(raw)}"
                    if raw_key in _PARAM_CACHE:
                        cached = _PARAM_CACHE.get(raw_key)
                        if isinstance(cached, dict):
                            return cached
                    data = json.loads(raw)
                    self._log_param_table_debug(
                        f"[调试] 参数表字符串解析成功，共{len(data)}个键",
                        path=path,
                    )
                    if isinstance(data, dict):
                        _PARAM_CACHE[raw_key] = data
                    return data
                except Exception as e:
                    self._log_param_table_debug(
                        f"[调试] 参数表字符串解析失败: {e}",
                        path=path,
                    )
            self._log_param_table_debug(
                "[调试] 参数表为空，返回空字典",
                path=path,
            )
            return {}
        except Exception as e:
            self._log_param_table_debug(
                f"[调试] 参数表加载异常: {e}",
                path="",
            )
            return {}

    def _apply_param_overrides_for_debug(self) -> None:
        """调试模式下根据开关加载参数表；未启用则保持硬编码。"""
        try:
            self.output("[调试] 开始应用参数表覆盖")
            overrides = self._param_override_cache or self._load_param_table()
            self.output(
                f"[调试] 参数表加载结果: overrides类型={type(overrides)}, 键数量={len(overrides) if isinstance(overrides, dict) else 0}"
            )

            switches = {}
            if isinstance(overrides, dict):
                switches = overrides.get("switches", {}) if isinstance(overrides.get("switches", {}), dict) else {}
                self.output(f"[调试] 参数表switches: {list(switches.keys())}")

            use_override_flag = bool(getattr(self.params, "use_param_overrides_in_debug", False))
            self.output(f"[调试] use_param_overrides_in_debug参数: {use_override_flag}")
            if not use_override_flag:
                use_override_flag = bool(switches.get("use_param_overrides_in_debug", False))
                self.output(f"[调试] switches中的use_param_overrides_in_debug: {use_override_flag}")

            if not use_override_flag:
                self.output("调试模式使用硬编码参数", force=True)
                return

            if not overrides:
                self.output("参数表为空，调试模式继续使用硬编码", force=True)
                return

            param_map = overrides.get("params") if isinstance(overrides, dict) else None
            if not isinstance(param_map, dict):
                param_map = overrides if isinstance(overrides, dict) else {}

            applied = 0
            for k, v in param_map.items():
                if hasattr(self.params, k):
                    try:
                        setattr(self.params, k, v)
                        applied += 1
                    except Exception:
                        pass
            if hasattr(self.params, "month_mapping"):
                mm_override = None
                if isinstance(overrides, dict):
                    mm_override = overrides.get("month_mapping")
                    if isinstance(overrides.get("params"), dict) and overrides["params"].get("month_mapping") is not None:
                        mm_override = overrides["params"].get("month_mapping")
                    if isinstance(overrides.get("original_defaults"), dict) and overrides["original_defaults"].get("month_mapping") is not None:
                        mm_override = overrides["original_defaults"].get("month_mapping")
                if mm_override is not None:
                    try:
                        setattr(self.params, "month_mapping", mm_override)
                    except Exception:
                        pass
            self._param_override_cache = overrides
            self.output(f"调试模式已应用参数表（{applied} 项）", force=True)
        except Exception as e:
            self.output(f"调试参数表应用失败: {e}", force=True)

    def _on_param_modify_click(self) -> None:
        """参数按钮：若未超额则进入参数表编辑；超额则提示。"""
        try:
            self._reset_param_edit_quota_if_new_month()
            limit = max(1, int(getattr(self.params, "param_edit_limit_per_month", 1) or 1))
            if self.param_edit_count >= limit:
                self.output("已经超额！", force=True)
                return
            self._open_param_editor()
        except Exception as e:
            self.output(f"参数表操作失败: {e}", force=True)

    def _on_backtest_click(self) -> None:
        """打开回测参数编辑器，支持保存/放弃并回写参数表。"""
        try:
            self._open_backtest_editor()
        except Exception as e:
            self.output(f"回测参数操作失败: {e}", force=True)

    def _get_current_param_json_string(self) -> str:
        try:
            path = self._resolve_param_table_path()
            if path and os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return json.dumps(data, ensure_ascii=False, indent=2)
                except Exception:
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            return f.read()
                    except Exception:
                        pass

            raw = getattr(self.params, "param_override_table", "") or ""
            if isinstance(raw, dict):
                return json.dumps(raw, ensure_ascii=False, indent=2)
            if isinstance(raw, str) and raw.strip().startswith("{"):
                return raw
            return "{}"
        except Exception:
            return "{}"

    def _open_param_editor(self) -> None:
        """[Mock] 打开参数编辑器"""
        pass
    
    def _open_backtest_editor(self) -> None:
        """[Mock] 打开回测编辑器"""
        pass

    def _align_month_mapping_to_loaded_futures(self) -> None:
        """读取已加载期货列表，对调试映射的指定月/指定下月格式进行对齐修正（Ported from Source）。"""
        try:
            # 1. 准备已加载合约集合
            loaded: set = set()
            futures = getattr(self, "future_instruments", [])
            if not futures:
                return

            # 依赖检查：需要 mixin 方法
            if not hasattr(self, "_normalize_future_id"):
                return
            
            for f in futures:
                inst_raw = str(f.get("InstrumentID", "")).upper()
                inst = self._normalize_future_id(inst_raw)
                if not inst:
                    continue
                loaded.add(inst)
                loaded.add(inst_raw)

            # 2. 获取当前映射 (直接使用 params.month_mapping)
            mapping = getattr(self.params, "month_mapping", {})
            if not mapping or not isinstance(mapping, dict):
                return
            
            changed = {}

            # 内置辅助函数 (Ported logic)
            def to_czce_style(code: str) -> Optional[str]:
                try:
                    if hasattr(self, "_extract_product_code"):
                        p = self._extract_product_code(code).upper()
                        y = self._extract_year(code)
                        mth = self._extract_month(code)
                        if p and y and mth is not None:
                             return f"{p}{str(y)[-1]}{mth:02d}".upper()
                    return None
                except Exception:
                    return None

            def to_single_month_style(code: str) -> Optional[str]:
                try:
                    if hasattr(self, "_extract_product_code"):
                        p = self._extract_product_code(code).upper()
                        yy = re.search(r"[A-Z]+(\d{2})(\d{1,2})", code)
                        if yy:
                            y2 = yy.group(1)
                            mth = int(yy.group(2))
                            return f"{p}{y2}{mth:02d}".upper()
                        cz = re.search(r"[A-Z]+(\d)(\d{2})", code)
                        if cz:
                            y1 = cz.group(1)
                            mth = int(cz.group(2))
                            return f"{p}{y1}{mth:02d}".upper()
                    return None
                except Exception:
                    return None

            # 3. 遍历检查并修正
            for prod, val in mapping.items():
                if not isinstance(val, (list, tuple)) or len(val) < 2:
                    continue
                
                cm_u = str(val[0]).upper()
                nm_u = str(val[1]).upper()
                new_cm = cm_u
                new_nm = nm_u
                
                if cm_u not in loaded:
                    alt = to_czce_style(cm_u)
                    if alt and alt in loaded: new_cm = alt
                    else:
                        alt2 = to_single_month_style(cm_u)
                        if alt2 and alt2 in loaded: new_cm = alt2
                
                if nm_u not in loaded:
                    alt = to_czce_style(nm_u)
                    if alt and alt in loaded: new_nm = alt
                    else:
                        alt2 = to_single_month_style(nm_u)
                        if alt2 and alt2 in loaded: new_nm = alt2
                        
                if (new_cm != cm_u) or (new_nm != nm_u):
                    changed[prod] = (cm_u, nm_u, new_cm, new_nm)

            if not changed:
                return

            self.output(f"[Info] 自动修正月份映射: {changed}")

            # 4. 更新 params.month_mapping
            # 注意：此处直接修改 params 对象，对于 BaseParams 通常是支持的
            for prod, vals in changed.items():
                 # vals = (old_cm, old_nm, new_cm, new_nm)
                 mapping[prod] = [vals[2], vals[3]]
            
        except Exception as e:
            self.output(f"[Error] _align_month_mapping_to_loaded_futures failed: {e}")
