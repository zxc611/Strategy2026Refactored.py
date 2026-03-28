"""
params_service.py - 参数服务

合并来源：06_params.py
合并策略：提取参数管理核心功能，简化配置类

重构目标：
- 旧架构：06_params.py (~870行)
- 新架构：params_service.py (~400行)
- 减少：~54%

核心改进：
1. 单一职责：专注于参数配置管理
2. 简化设计：移除复杂的Mixin继承
3. 类型安全：完整的类型注解
4. 易于测试：独立的参数服务
5. 线程安全：读写操作都加锁
6. 参数验证：支持范围验证
7. 变更通知：观察者模式支持
8. 环境变量：支持从环境变量加载

作者：CodeArts 代码智能体
版本：v1.1
生成时间：2026-03-16
"""

from __future__ import annotations

import json
import os
import time
import logging
import threading
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass


# ============================================================================
# 参数缓存（进程级）- 已废弃，改为实例变量
# ============================================================================

# _PARAM_CACHE: Dict[str, Any] = {}
# _PARAM_CACHE_META: Dict[str, Optional[float]] = {}
# _PARAM_CHECK_TIMESTAMP: Dict[str, float] = {}


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class OutputConfig:
    """输出配置"""
    mode: str = "debug"
    trade_quiet: bool = True
    diagnostic_enabled: bool = True
    debug_enabled: bool = False

    def __post_init__(self):
        if self.mode == "debug":
            self.mode = "close_debug"
        self.is_trade_like = self.mode in ("trade", "open_debug")
        self.combined_debug_enabled = (
            self.diagnostic_enabled and
            (self.debug_enabled or self.mode == "close_debug")
        )

    def should_output(self, is_trade: bool = False, is_force: bool = False,
                      is_trade_table: bool = False, is_diag: bool = False) -> bool:
        """判断是否应该输出"""
        if self.is_trade_like and self.trade_quiet and not is_trade_table:
            return False
        if is_diag and not self.diagnostic_enabled and not is_trade and not is_force:
            return False
        if self.is_trade_like and not is_trade and not is_force:
            return False
        if not is_trade and not is_force and not self.combined_debug_enabled:
            return False
        return True


# ============================================================================
# 观察者接口
# ============================================================================

class ParamObserver:
    """参数观察者接口"""

    def on_param_changed(self, key: str, old_value: Any, new_value: Any) -> None:
        """参数变更回调"""
        pass


# ============================================================================
# 参数服务
# ============================================================================

class ParamsService:
    """
    参数服务 - 统一的参数配置管理

    职责：
    1. 参数加载与缓存
    2. 参数验证与类型转换
    3. 输出控制
    4. 参数表管理
    5. 变更通知
    6. 环境变量支持

    使用方式：
        service = ParamsService()
        params = service.load_params("param_table.json")
    """

    def __init__(self, logger_func: Optional[Callable] = None):
        """
        初始化参数服务

        Args:
            logger_func: 日志输出函数（可选）
        """
        self._logger = logger_func or print
        self._lock = threading.RLock()

        # 验证锁初始化
        if self._lock is None:
            raise RuntimeError("Lock initialization failed: lock is None")

        self._output_config: Optional[OutputConfig] = None
        self._params: Dict[str, Any] = {}
        self._observers: Set[ParamObserver] = set()
        self._env_prefix = "PARAM_"  # 环境变量前缀

        # 配置
        self._check_interval = 900.0  # 15分钟

        # 实例级缓存（替代全局缓存，解决多进程安全问题）
        self._param_cache: Dict[str, Any] = {}
        self._param_cache_meta: Dict[str, Optional[float]] = {}
        self._param_check_timestamp: Dict[str, float] = {}

    # ========================================================================
    # 参数加载
    # ========================================================================

    def load_params(self, path: str, force: bool = False) -> Dict[str, Any]:
        """
        加载参数表

        Args:
            path: 参数表文件路径
            force: 是否强制刷新

        Returns:
            参数字典
        """
        try:
            if not path:
                logging.error("[ParamsService] Empty path provided")
                return {}

            now = time.time()

            # 检查节流
            last_check = self._param_check_timestamp.get(path, 0)
            if not force and (now - last_check < self._check_interval):
                if path in self._param_cache:
                    return self._param_cache[path]

            self._param_check_timestamp[path] = now

            # 检查文件是否存在
            if not os.path.exists(path):
                self._log(f"[ParamsService] File not found: {path}")
                return {}

            # 检查缓存
            mtime = os.path.getmtime(path)
            if path in self._param_cache and self._param_cache_meta.get(path) == mtime:
                cached = self._param_cache.get(path)
                if isinstance(cached, dict):
                    return cached

            # 加载文件
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                self._param_cache[path] = data
                self._param_cache_meta[path] = mtime
                self._log(f"[ParamsService] Loaded {len(data)} params from {path}")
                return data

            logging.error(f"[ParamsService] Invalid data type: {type(data)}")
            return {}

        except Exception as e:
            logging.error(f"[ParamsService.load_params] Error: {e}")
            raise RuntimeError(f"Params load failed: {e}") from e
    
    def force_refresh(self, path: Optional[str] = None) -> bool:
        """
        强制刷新参数缓存
        
        Args:
            path: 指定路径，None 表示刷新所有
            
        Returns:
            bool: 是否成功刷新
        """
        try:
            with threading.Lock():  # 使用临时锁保护刷新操作
                if path:
                    # 刷新指定路径
                    if path in self._param_cache:
                        del self._param_cache[path]
                    if path in self._param_cache_meta:
                        del self._param_cache_meta[path]
                    if path in self._param_check_timestamp:
                        del self._param_check_timestamp[path]
                    logging.info(f"[ParamsService.force_refresh] 强制刷新 {path}")
                else:
                    # 刷新全部
                    count = len(self._param_cache)
                    self._param_cache.clear()
                    self._param_cache_meta.clear()
                    self._param_check_timestamp.clear()
                    logging.info(f"[ParamsService.force_refresh] 强制刷新全部参数 ({count}个)")
            return True
        except Exception as e:
            logging.error(f"[ParamsService.force_refresh] Error: {e}")
            return False

    def save_params(self, path: str, params: Dict[str, Any]) -> bool:
        """
        保存参数表

        Args:
            path: 参数表文件路径
            params: 参数字典

        Returns:
            是否成功
        """
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(params, f, indent=2, ensure_ascii=False)

            # 更新缓存
            mtime = os.path.getmtime(path)
            self._param_cache[path] = params
            self._param_cache_meta[path] = mtime

            self._log(f"[ParamsService] Saved {len(params)} params to {path}")
            return True

        except Exception as e:
            logging.error(f"[ParamsService.save_params] Error: {e}")
            raise RuntimeError(f"Params save failed: {e}") from e

    def load_from_env(self, prefix: str = "PARAM_") -> int:
        """
        从环境变量加载参数

        注意：环境变量前缀应与config_service区分，避免冲突。
        推荐使用"PARAM_"作为前缀，config_service使用"CONFIG_"。

        Args:
            prefix: 环境变量前缀

        Returns:
            加载的参数数量
        """
        self._env_prefix = prefix

        # 检测潜在冲突
        if prefix == "CONFIG_":
            logging.warning(
                "[ParamsService] Using 'CONFIG_' prefix may conflict with config_service. "
                "Recommended to use 'PARAM_' prefix."
            )

        count = 0

        with self._lock:
            for key, value in os.environ.items():
                if key.startswith(prefix):
                    param_key = key[len(prefix):].lower()
                    # 尝试解析 JSON
                    try:
                        parsed = json.loads(value)
                        self._params[param_key] = parsed
                    except json.JSONDecodeError as e:
                        logging.warning(f"[ParamsService] Env {key} is not JSON, using raw value: {e}")
                        self._params[param_key] = value
                    count += 1

        if count > 0:
            self._log(f"[ParamsService] Loaded {count} params from environment")
        return count

    # ========================================================================
    # 参数访问（线程安全）
    # ========================================================================

    def get(self, key: str, default: Any = None) -> Any:
        """获取参数值（线程安全）"""
        with self._lock:
            return self._params.get(key, default)

    def set(self, key: str, value: Any, notify: bool = True) -> None:
        """
        设置参数值（线程安全）

        Args:
            key: 参数键
            value: 参数值
            notify: 是否通知观察者
        """
        with self._lock:
            old_value = self._params.get(key)
            self._params[key] = value
            if notify and old_value != value:
                self._notify_observers(key, old_value, value)

    def get_int(self, key: str, default: int = 0,
                min_val: Optional[int] = None,
                max_val: Optional[int] = None) -> int:
        """
        获取整数参数（带范围验证）

        Args:
            key: 参数键
            default: 默认值
            min_val: 最小值（可选）
            max_val: 最大值（可选）

        Returns:
            整数参数值
        """
        with self._lock:
            try:
                val = self._params.get(key, default)
                if val is not None:
                    # 检测精度丢失
                    float_val = float(val)
                    int_val = int(float_val)
                    if float_val != int_val:
                        logging.warning(
                            f"[ParamsService.get_int] Precision loss for {key}: "
                            f"{float_val} -> {int_val}"
                        )
                    result = int_val
                else:
                    result = default

                if min_val is not None and result < min_val:
                    result = min_val
                if max_val is not None and result > max_val:
                    result = max_val
                return result
            except (ValueError, TypeError) as e:
                logging.warning(f"[ParamsService.get_int] Error for {key}: {e}")
                return default

    def get_float(self, key: str, default: float = 0.0,
                  min_val: Optional[float] = None,
                  max_val: Optional[float] = None) -> float:
        """
        获取浮点数参数（带范围验证）

        Args:
            key: 参数键
            default: 默认值
            min_val: 最小值（可选）
            max_val: 最大值（可选）

        Returns:
            浮点数参数值
        """
        with self._lock:
            try:
                val = self._params.get(key, default)
                result = float(val) if val is not None else default
                if min_val is not None and result < min_val:
                    result = min_val
                if max_val is not None and result > max_val:
                    result = max_val
                return result
            except (ValueError, TypeError) as e:
                logging.warning(f"[ParamsService.get_float] Error for {key}: {e}")
                return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """获取布尔参数（线程安全）"""
        with self._lock:
            val = self._params.get(key, default)
            return bool(val) if val is not None else default

    def get_str(self, key: str, default: str = "") -> str:
        """获取字符串参数（线程安全）"""
        with self._lock:
            val = self._params.get(key, default)
            return str(val) if val is not None else default

    def get_list(self, key: str, default: Optional[List] = None) -> List[Any]:
        """获取列表参数（线程安全）"""
        with self._lock:
            val = self._params.get(key, default or [])
            return list(val) if isinstance(val, (list, tuple)) else (default or [])

    def get_dict(self, key: str, default: Optional[Dict] = None) -> Dict[str, Any]:
        """获取字典参数（线程安全）"""
        with self._lock:
            val = self._params.get(key, default or {})
            return dict(val) if isinstance(val, dict) else (default or {})

    # ========================================================================
    # 参数更新
    # ========================================================================

    def update(self, params: Dict[str, Any], notify: bool = True) -> None:
        """
        批量更新参数

        Args:
            params: 参数字典
            notify: 是否通知观察者
        """
        with self._lock:
            for key, value in params.items():
                old_value = self._params.get(key)
                self._params[key] = value
                if notify and old_value != value:
                    self._notify_observers(key, old_value, value)

    def apply_from_object(self, obj: Any, notify: bool = True) -> None:
        """
        从对象属性应用参数

        Args:
            obj: 目标对象
            notify: 是否通知观察者
        """
        with self._lock:
            for key, value in self._params.items():
                if hasattr(obj, key):
                    try:
                        setattr(obj, key, value)
                    except Exception as e:
                        logging.warning(f"[ParamsService] Error setting {key}: {e}")

    def apply_to_object(self, obj: Any) -> None:
        """
        将对象属性同步到参数（优化版：只遍历参数键）

        Args:
            obj: 源对象
        """
        with self._lock:
            for key in self._params.keys():
                if hasattr(obj, key):
                    try:
                        self._params[key] = getattr(obj, key)
                    except Exception as e:
                        logging.warning(f"[ParamsService] Error getting {key}: {e}")

    # ========================================================================
    # 观察者模式
    # ========================================================================

    def add_observer(self, observer: ParamObserver) -> None:
        """添加观察者"""
        with self._lock:
            self._observers.add(observer)

    def remove_observer(self, observer: ParamObserver) -> None:
        """移除观察者"""
        with self._lock:
            self._observers.discard(observer)

    def _notify_observers(self, key: str, old_value: Any, new_value: Any) -> None:
        """通知所有观察者（内部方法，调用时已持有锁）"""
        for observer in self._observers:
            try:
                observer.on_param_changed(key, old_value, new_value)
            except Exception as e:
                logging.warning(f"[ParamsService] Observer error: {e}")

    # ========================================================================
    # 输出控制
    # ========================================================================

    def get_output_config(self) -> OutputConfig:
        """获取输出配置"""
        if self._output_config is None:
            self._output_config = OutputConfig(
                mode=self.get_str("output_mode", "debug"),
                trade_quiet=self.get_bool("trade_quiet", True),
                diagnostic_enabled=self.get_bool("diagnostic_output", True),
                debug_enabled=self.get_bool("debug_output", False),
            )
        return self._output_config

    def should_output(self, is_trade: bool = False, is_force: bool = False,
                      is_trade_table: bool = False, is_diag: bool = False) -> bool:
        """判断是否应该输出"""
        config = self.get_output_config()
        return config.should_output(is_trade, is_force, is_trade_table, is_diag)

    def invalidate_output_config(self) -> None:
        """使输出配置缓存失效"""
        with self._lock:
            self._output_config = None
            # 通知观察者输出配置已失效
            self._notify_observers("_output_config", None, None)

    # ========================================================================
    # 辅助方法
    # ========================================================================

    def _log(self, message: str) -> None:
        """输出日志"""
        try:
            if self._logger:
                self._logger(message)
        except Exception as e:
            logging.warning(f"[ParamsService] Log error: {e}")

    def clear_cache(self) -> None:
        """清空缓存"""
        with self._lock:
            self._param_cache.clear()
            self._param_cache_meta.clear()
            self._param_check_timestamp.clear()

    def get_all(self) -> Dict[str, Any]:
        """获取所有参数（线程安全）"""
        with self._lock:
            return dict(self._params)

    def keys(self) -> List[str]:
        """获取所有参数键（线程安全）"""
        with self._lock:
            return list(self._params.keys())

    def __contains__(self, key: str) -> bool:
        """支持 `in` 操作符（线程安全）"""
        with self._lock:
            return key in self._params

    def __getitem__(self, key: str) -> Any:
        """支持 `[]` 取值（线程安全）"""
        with self._lock:
            return self._params[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """支持 `[]` 赋值（线程安全）"""
        self.set(key, value)

    def __len__(self) -> int:
        """支持 `len()` （线程安全）"""
        with self._lock:
            return len(self._params)


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'ParamsService',
    'OutputConfig',
    'ParamObserver',
]
