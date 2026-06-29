# [M1-89] 参数服务核心类
# MODULE_ID: M1-005

"""参数服务 - 核心跳(ParamsService)"""



from __future__ import annotations



import json

import os

import ast

import time

import logging

import threading

from typing import Any, Dict, List, Optional, Callable, Set

from dataclasses import dataclass



from ali2026v3_trading.infra.resilience import (

    ParamVersionManager, AtomicConfigRef, Watchdog, HeartbeatMonitor,

    RateLimitedLogger, GracefulDegradation,

    stable_sum, stable_mean, safe_normalize_weights,

    get_param_version_manager,

)

from ali2026v3_trading.infra.serialization_utils import yaml_safe_load, json_dumps, json_loads

from ali2026v3_trading.infra.shared_utils import atomic_replace_file  # R8-5



from ali2026v3_trading.config.config_dataclasses import OutputConfig, ParamObserver, ParamAuditObserver

from ali2026v3_trading.config._params_instrument_cache import InstrumentCacheService, InstrumentCacheMixin

from ali2026v3_trading.config._params_attribute_matrix import AttributeMatrixService, AttributeMatrixMixin





class ParamsService:

    """

    参数服务 - 统一的参数配置管理（Facade组合，消灭Mixin继承承


    职责任
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



        self._output_config: Optional[OutputConfig] = None

        self._params: Dict[str, Any] = {}

        self._observers: Set[ParamObserver] = set()

        self._env_prefix = "PARAM_"  # 环境变量前缀

        self._attribute_matrix: Dict[str, Any] = {}

        self._attribute_matrix_loaded: bool = False



        # 配置

        self._check_interval = 60.0  # 1分钟（与config_params CACHE_TTL对齐）


        # 实例级缓存（替代全局缓存，解决多进程安全问题）
        self._param_cache: Dict[str, Any] = {}

        self._param_cache_meta: Dict[str, Optional[float]] = {}

        self._param_check_timestamp: Dict[str, float] = {}



        self._instrument_cache_service = InstrumentCacheService(self)

        self._attribute_matrix_service = AttributeMatrixService(self)



        # P0 Bug #9修复：在。_init__中调用init_instrument_cache，确保缓存容器已初始化
        self.init_instrument_cache()



    def __getattr__(self, name):

        # 递归保护: 防止ICS/AMS.__getattr__双向委托导致无限递归

        if '__getattr__recursing' in self.__dict__:

            raise AttributeError(name)

        self.__dict__['__getattr__recursing'] = True

        try:

            _ics = self.__dict__.get('_instrument_cache_service')

            if _ics is not None:

                try:

                    return getattr(_ics, name)

                except AttributeError:

                    pass

            _ams = self.__dict__.get('_attribute_matrix_service')

            if _ams is not None:

                try:

                    return getattr(_ams, name)

                except AttributeError:

                    pass

            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        finally:

            self.__dict__.pop('__getattr__recursing', None)



        self._audit_observer = ParamAuditObserver()

        self._observers.add(self._audit_observer)



        # OPS-P1-03修复: 参数变更通知 _P1-05修复: 统一走EventBus，不再需要本地回调列表


        # UPG-P1-07修复: 热更新事务性保存存参数备份

        self._param_backup: Optional[Dict[str, Any]] = None

        self._param_backup_lock = threading.Lock()

        self._hot_update_txn_lock = threading.Lock()



        # DFG-P1-08修复: 订阅数据格式变更事件，刷新合约缓存
        try:

            from ali2026v3_trading.infra.event_bus import get_global_event_bus

            _bus = get_global_event_bus()

            if _bus is not None:

                _bus.subscribe_weak('DataFormatChangedEvent', self._on_data_format_changed)

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.warning("[R22-EP-P1] ParamsService exception swallowed")

            pass



    # ========================================================================

    # 合约缓存管理（从 storage.py 迁移除
    # ========================================================================



    # DFG-P1-08修复: 数据格式变更事件消费。'
    def _on_data_format_changed(self, event: Any) -> None:

        """DFG-P1-08修复: 消费数据格式变更事件，刷新合约缓存


        当数据格式发生变更时（如schema版本升级），

        触发合约缓存重建，确保运行时数据与数据库一致。
        """

        try:

            _source = getattr(event, 'source', '') if hasattr(event, 'source') else ''

            if isinstance(event, dict):

                _source = event.get('source', '')

            logging.info("[DFG-P1-08] ParamsService收到数据格式变更事件: source=%s", _source)

            # 标记缓存需要刷新（不立即重建，避免递归档
            with self._lock:

                self._param_cache.clear()

                self._param_cache_meta.clear()

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.warning("[R22-EP-P1] ParamsService exception swallowed")

            pass



    def load_params(self, path: str, force: bool = False) -> Dict[str, Any]:

        """

        加载参数据


        YAML为主源，JSON仅作fallback。计划统一到YAML单一。'
        Args:

            path: 参数表文件路由
            force: 是否强制刷新



        Returns:

            参数字典

        """

        # R24-P2-IV-09修复: 参数版本一致性检查——加载后验证与config_params版本hash一。
        try:

            from ali2026v3_trading.config.config_params import get_param_version

            _pv = get_param_version()

            logging.debug("[R24-P2-IV-09] ParamsService.load_params: 当前参数版本 v%d hash=%s",

                         _pv.get('version', -1), _pv.get('hash', 'N/A'))

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] ...")

            pass

        try:

            if not path:

                logging.error("[ParamsService] Empty path provided")

                return {}



            with self._lock:

                now = time.time()



                # 检查节点
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

                    data = json_loads(f.read())  # R5-1: 统一json_loads



                if isinstance(data, dict):

                    self._param_cache[path] = data

                    self._param_cache_meta[path] = mtime

                    self._log(f"[ParamsService] Loaded {len(data)} params from {path}")

                    return data



                logging.error(f"[ParamsService] Invalid data type: {type(data)}")

                return {}



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error(f"[ParamsService.load_params] Error: {e}")

            raise RuntimeError(f"Params load failed: {e}") from e

    

    def force_refresh(self, path: Optional[str] = None) -> bool:

        """

        强制刷新参数缓存

        

        Args:

            path: 指定路径，None 表示刷新所。'
        Returns:

            bool: 是否成功刷新

        """

        try:

            # P1 Bug #45修复：使用self._lock而非临时Lock

            with self._lock:

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

                    logging.info(f"[ParamsService.force_refresh] 强制刷新全部参数 ({count}_")

            return True

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error(f"[ParamsService.force_refresh] Error: {e}")

            return False



    def reload_params(self) -> bool:

        """P1-R9-15修复: 配置热重载机制"""
        try:

            with self._lock:

                self._param_cache.clear()

                self._param_cache_meta.clear()

                self._param_check_timestamp.clear()

                for _path in list(self._params.keys()):

                    try:

                        self.load_params(_path, force=True)

                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                        logging.debug("[R3-L2] ...")

                        pass

            logging.info("[ParamsService] 配置热重载成功")

            return True

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error("[ParamsService] 配置热重载失败 %s", e)

            return False



    def save_params(self, path: str, params: Dict[str, Any]) -> bool:

        """

        保存参数据


        Args:

            path: 参数表文件路由
            params: 参数字典



        Returns:

            是否成功

        """

        try:

            with self._lock:

                atomic_replace_file(path, json_dumps(params, indent=2))  # R8-5: 原子写入



                # 更新缓存

                mtime = os.path.getmtime(path)

                self._param_cache[path] = params

                self._param_cache_meta[path] = mtime



            self._log(f"[ParamsService] Saved {len(params)} params to {path}")

            return True



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error(f"[ParamsService.save_params] Error: {e}")

            raise RuntimeError(f"Params save failed: {e}") from e



    # P0-R8-14修复: 启动时MD5哈希打印

    def compute_params_md5(self, path: str = None) -> str:

        """

        手册15节要） 策略启动时打印参数文件MD5哈希，确保运行时配置与预期一致。
        """

        import hashlib

        try:

            if path is None:

                # 尝试找到参数文件路径

                cache_keys = list(self._param_cache.keys())

                if cache_keys:

                    path = cache_keys[0]

                else:

                    return "NO_PARAMS_LOADED"

            with open(path, 'rb') as f:

                content = f.read()

            # P1-64修复: 委托compute_content_hash统一哈希计算

            from ali2026v3_trading.infra.shared_utils import compute_content_hash

            md5_hash = compute_content_hash(content.decode('utf-8', errors='replace'), algorithm='md5')

            logging.critical("[ParamsService] 📋 参数文件MD5: %s (file=%s)", md5_hash, path)

            return md5_hash

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[ParamsService] MD5 compute failed: %s", e)

            return f"ERROR:{e}"



    # P0-R8-13修复: --param-version CLI支持

    @staticmethod

    def resolve_param_version(cli_version: str = None, env_version: str = None,

                              default_path: str = None) -> str:

        """

        手册15节要） 支持通过环境变量或CLI指定参数版本(_-param-version v1.2)，便于快速回滚动
        优先先 CLI > 环境变量 > 默认路径

        """

        import os

        version = cli_version or env_version or os.environ.get('PARAM_VERSION', '')

        if version:

            resolved = f"params_v{version}.json" if default_path is None else default_path.replace('.json', f'_v{version}.json')

            logging.info("[ParamsService] 解析参数版本: %s -> %s", version, resolved)

            return resolved

        if default_path:

            return default_path

        return 'params.json'



    def verify_params_startup(self, param_path: str = None) -> Dict[str, Any]:

        """

        启动时综合验验 MD5哈希 + 参数完整性检查
        Returns: {'md5': str, 'param_count': int, 'issues': list}

        """

        result = {'md5': '', 'param_count': 0, 'issues': [], 'version': ''}

        try:

            result['md5'] = self.compute_params_md5(param_path)

            if self._param_cache:

                for path, params in self._param_cache.items():

                    result['param_count'] += len(params)

            result['version'] = os.environ.get('PARAM_VERSION', 'default')

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            result['issues'].append(str(e))

        return result



    def load_from_env(self, prefix: str = "PARAM_") -> int:

        """

        从环境变量加载参数


        注意：环境变量前缀应与config_service区分，避免冲突突
        推荐使用"PARAM_"作为前缀，config_service使用"CONFIG_"_


        Args:

            prefix: 环境变量前缀



        Returns:

            加载的参数数据
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

    # 参数访问（线程安全）'
    # ========================================================================



    def get(self, key: str, default: Any = None) -> Any:

        """获取参数值（线程安全量 委托给给。getitem__+默认证"""

        try:

            return self[key]

        except KeyError:

            return default



    def set(self, key: str, value: Any, notify: bool = True) -> None:

        """

        设置参数值（线程安全量


        Args:

            key: 参数据
            value: 参数据
            notify: 是否通知观察者
        """

        self._validate_param_against_matrix(key, value)

        notify_info = None

        with self._lock:

            old_value = self._params.get(key)

            self._params[key] = value

            if notify and old_value != value:

                notify_info = (key, old_value, value)

        if notify_info:

            self._notify_observers(*notify_info)

            # R5-T-09确认/OPS-P1-03修复: 参数变更通知机制(_notify_observers+_fire_param_change_callbacks)

            self._fire_param_change_callbacks(*notify_info)



    def _validate_param_against_matrix(self, key: str, value: Any) -> None:

        if not self._attribute_matrix_loaded:

            return

        attr = self._attribute_matrix.get(key)

        if not isinstance(attr, dict):

            return

        expected_type = attr.get('type')

        if expected_type and not self._check_type(value, expected_type):

            logging.error(

                "[ParamsService] SET_REJECTED type | %s: expected %s, got %s=%r",

                key, expected_type, type(value).__name__, value,

            )

            raise ValueError(f"[R26-P0-FI-07] 参数类型验证拒绝: {key} expected {expected_type}, got {type(value).__name__}={value!r}")

        value_range = attr.get('range')

        if value_range and isinstance(value_range, list) and len(value_range) == 2:

            if isinstance(value, (int, float)):

                lo, hi = value_range

                if not (lo <= value <= hi):

                    logging.error(

                        "[ParamsService] SET_REJECTED range | %s: %r not in [%s, %s]",

                        key, value, lo, hi,

                    )

                    raise ValueError(f"[R26-P0-FI-07] 参数范围验证拒绝: {key}={value!r} not in [{lo}, {hi}]")



    def get_int(self, key: str, default: int = 0,

                min_val: Optional[int] = None,

                max_val: Optional[int] = None) -> int:

        """

        获取整数参数（带范围验证验


        Args:

            key: 参数据
            default: 默认证
            min_val: 最小值（可选）'
            max_val: 最大值（可选）



        Returns:

            整数参数据
        """

        with self._lock:

            try:

                val = self._params.get(key, default)

                if val is not None:

                    # 检测精度丢失
                    float_val = float(val)

                    int_val = round(float_val)  # [R22-TS-P1-04] 改用round避免截断

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

            key: 参数据
            default: 默认证
            min_val: 最小值（可选）

            max_val: 最大值（可选）



        Returns:

            浮点数参数据
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

        """获取字符串参数（线程安全量"""

        with self._lock:

            val = self._params.get(key, default)

            return str(val) if val is not None else default



    def get_list(self, key: str, default: Optional[List] = None) -> List[Any]:

        """获取列表参数（线程安全）"""

        # R24-P2-DF-05修复: 区分"未传默认"_显式None"，避免default or []覆盖显式None意图

        with self._lock:

            _fallback = [] if default is None else default

            val = self._params.get(key, _fallback)

            return list(val) if isinstance(val, (list, tuple)) else _fallback



    def get_dict(self, key: str, default: Optional[Dict] = None) -> Dict[str, Any]:

        """获取字典参数（线程安全）"""

        # R24-P2-DF-05修复: 区分"未传默认"_显式None"，避免default or {}覆盖显式None意图

        with self._lock:

            _fallback = {} if default is None else default

            val = self._params.get(key, _fallback)

            return dict(val) if isinstance(val, dict) else _fallback



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

        notify_list = []

        with self._lock:

            for key, value in params.items():

                old_value = self._params.get(key)

                self._params[key] = value

                if notify and old_value != value:

                    notify_list.append((key, old_value, value))

        for info in notify_list:

            self._notify_observers(*info)



    # UPG-P1-07修复: 热更新事务性保存存备份/提交/回滚方法

    def hot_update_begin(self) -> Dict[str, Any]:

        """UPG-P1-07修复: 热更新开始，备份当前参数状态


        在热更新操作前调用，将当前参数快照保存到。param_backup_
        配合hot_update_commit()或hot_update_rollback()使用例


        Returns:

            Dict: 备份的参数快照
        """

        if not self._hot_update_txn_lock.acquire(blocking=False):

            raise RuntimeError("R21-P2-UG-12: 热更新事务冲突，已有热更新正在进行")

        try:

            with self._param_backup_lock:

                with self._lock:

                    self._param_backup = dict(self._params)

                logging.info(

                    "UPG-P1-07: 热更新备份已创建, 参数数量=%d",

                    len(self._param_backup),

                )

                return dict(self._param_backup)

        except:

            self._hot_update_txn_lock.release()

            raise



    def hot_update_commit(self) -> None:

        """UPG-P1-07修复: 热更新提交，清除备份



        热更新成功后调用，清除。param_backup备份份
        """

        try:

            with self._param_backup_lock:

                if self._param_backup is not None:

                    logging.info(

                        "UPG-P1-07: 热更新已提交, 清除备份(原参数数据%d)",

                        len(self._param_backup),

                    )

                    self._param_backup = None

                else:

                    logging.debug("UPG-P1-07: 热更新提交时无备份可能已提交或未开启")

        finally:

            self._hot_update_txn_lock.release()



    def hot_update_rollback(self) -> bool:

        """UPG-P1-07修复: 热更新回滚，恢复到备份状态


        热更新失败时调用，将参数恢复到hot_update_begin()时的快照照


        Returns:

            bool: 是否成功回滚

        """

        try:

            with self._param_backup_lock:

                if self._param_backup is None:

                    logging.warning("UPG-P1-07: 热更新回滚失败 无可用备份")

                    return False

                backup = dict(self._param_backup)

                self._param_backup = None



            with self._lock:

                self._params = backup



            logging.info(

                "UPG-P1-07: 热更新已回滚, 恢复参数数量=%d",

                len(backup),

            )

            return True

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.critical("R21-P2-UG-12: 热更新回滚异常，强制释放事务时 %s", e)

            return False

        finally:

            self._hot_update_txn_lock.release()



    def hot_update(self, params: Dict[str, Any], notify: bool = True) -> Dict[str, Any]:

        """UPG-P1-07/09修复: 事务性热更新参数



        自动执行备份→更新→提交/回滚的完整事务流程。'
        更新失败时自动回滚到更新前的状态机


        Args:

            params: 要更新的参数字典

            notify: 是否通知观察者


        Returns:

            Dict: {success: bool, updated_keys: list, rolled_back: bool, error: str}

        """

        result = {'success': False, 'updated_keys': [], 'rolled_back': False, 'error': ''}



        # 步骤1: 备份

        try:

            self.hot_update_begin()

        except RuntimeError as e:

            result['error'] = str(e)

            return result



        # 步骤2: 尝试更新

        try:

            notify_list = []

            with self._lock:

                for key, value in params.items():

                    old_value = self._params.get(key)

                    self._validate_param_against_matrix(key, value)

                    self._params[key] = value

                    result['updated_keys'].append(key)

                    if notify and old_value != value:

                        notify_list.append((key, old_value, value))



            for info in notify_list:

                self._notify_observers(*info)

                self._fire_param_change_callbacks(*info)



            # 步骤3: 提交

            self.hot_update_commit()

            result['success'] = True

            logging.info(

                "UPG-P1-07: 事务性热更新成功, 更新参数数量=%d",

                len(result['updated_keys']),

            )

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            # UPG-P1-09修复: 更新失败自动回滚

            result['error'] = str(e)

            rolled_back = self.hot_update_rollback()

            result['rolled_back'] = rolled_back

            logging.error(

                "UPG-P1-09: 事务性热更新失败, 已回撤%s, error=%s",

                rolled_back, e,

            )



        return result



    def apply_from_object(self, obj: Any, notify: bool = True) -> None:

        """

        从对象属性应用参数


        Args:

            obj: 目标对象

            notify: 是否通知观察者
        """

        notify_list = []

        with self._lock:

            for key, value in self._params.items():

                if hasattr(obj, key):

                    try:

                        new_value = getattr(obj, key)

                        old_value = self._params.get(key)

                        self._params[key] = new_value

                        if notify and old_value != new_value:

                            notify_list.append((key, old_value, new_value))

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                        logging.warning(f"[ParamsService] Error setting {key}: {e}")

        for info in notify_list:

            self._notify_observers(*info)



    def apply_to_object(self, obj: Any) -> None:

        """

        将对象属性同步到参数（优化版：只遍历参数键）'
        Args:

            obj: 源对手
        """

        with self._lock:

            for key in self._params.keys():

                if hasattr(obj, key):

                    try:

                        self._params[key] = getattr(obj, key)

                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                        logging.warning(f"[ParamsService] Error getting {key}: {e}")



    # ========================================================================

    # 观察者模块
    # ========================================================================



    def add_observer(self, observer: ParamObserver) -> None:

        """添加观察者"""

        with self._lock:

            self._observers.add(observer)



    def remove_observer(self, observer: ParamObserver) -> None:

        """移除观察者"""

        with self._lock:

            self._observers.discard(observer)



    # OPS-P1-03修复: 参数变更通知回调 _P1-05修复: 统一走EventBus

    def register_param_change_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:

        """P1-05修复: 注册到EventBus单通道"""

        try:

            from ali2026v3_trading.infra.event_bus import get_global_event_bus

            bus = get_global_event_bus()

            if bus is not None:

                bus.subscribe('ParamChangedEvent', callback)

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] ...")

            pass



    def _fire_param_change_callbacks(self, key: str, old_value: Any, new_value: Any,

                                      source: str = 'set') -> None:

        """P1-05修复: 统一通过EventBus发布参数变更事件"""

        try:

            from ali2026v3_trading.infra.event_bus import get_global_event_bus, ParamChangedEvent

            bus = get_global_event_bus()

            bus_event = ParamChangedEvent(

                key=key,

                old_value=old_value,

                new_value=new_value,

                source=source,

                changed_keys=[key],

            )

            bus.publish(bus_event, async_mode=True)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.debug("[ParamsService] OPS-P1-03: EventBus参数变更推送异常 %s", e)



    def _notify_observers(self, key: str, old_value: Any, new_value: Any) -> None:

        """

        P1 Bug #48修复：通知所有观察者（避免在锁内调用外部回调导致死锁）'
        注意：调用者必须已持有self._lock_
        修复方案：复制观察者列表后，在锁外逐个调用回调

        """

        # 调用者已持有锁，直接复制列表

        observers_copy = list(self._observers)

        

        # 在锁外调用回调，避免死锁

        for observer in observers_copy:

            try:

                observer.on_param_changed(key, old_value, new_value)

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logging.warning(f"[ParamsService] Observer error: {e}")



    # ========================================================================

    # 参数属性矩阵集合
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

        notify_info = None

        with self._lock:

            self._output_config = None

            notify_info = ("_output_config", None, None)

        if notify_info:

            self._notify_observers(*notify_info)



    # ========================================================================

    # 辅助方法

    # ========================================================================



    def _log(self, message: str) -> None:

        """输出日志"""

        try:

            if self._logger:

                self._logger(message)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning(f"[ParamsService] Log error: {e}")



    # _ID唯一：clear_cache统一接口，服务ParamsService

    def clear_cache(self) -> None:

        """清空缓存"""

        with self._lock:

            self._param_cache.clear()

            self._param_cache_meta.clear()

            self._param_check_timestamp.clear()

        logging.info('[ParamsService] Cache cleared')



    def get_all(self) -> Dict[str, Any]:

        """获取所有参数（线程安全量"""

        with self._lock:

            return dict(self._params)



    def keys(self) -> List[str]:

        """获取所有参数键（线程安全）"""

        with self._lock:

            return list(self._params.keys())



    def __contains__(self, key: str) -> bool:

        """支持 `in` 操作符（线程安全量"""

        with self._lock:

            return key in self._params



    def __getitem__(self, key: str) -> Any:

        """支持 `[]` 取值（线程安全量"""

        with self._lock:

            return self._params[key]



    def __setitem__(self, key: str, value: Any) -> None:

        """支持 `[]` 赋值（线程安全量"""

        self.set(key, value)



    def __len__(self) -> int:

        """支持 `len()` （线程安全）"""

        with self._lock:

            return len(self._params)



    # ========================================================================

    # P-40修复: 分类参数API（手动.3节）'
    # ========================================================================



    def get_shadow_params(self) -> Dict[str, Any]:

        """获取影子策略参数子集



        返回key_shadow_'开头的参数以及影子策略核心配置:

        - shadow_a/reverse相关参数

        - shadow_b/random相关参数

        - alpha衰减监测参数



        Returns:

            Dict[str, Any]: 影子策略参数字典

        """

        with self._lock:

            result: Dict[str, Any] = {}

            for k, v in self._params.items():

                if isinstance(k, str) and k.startswith('shadow_'):

                    result[k] = v

            result.setdefault('shadow_alpha_threshold', 0.1)

            result.setdefault('shadow_lookback_days', 30)

            result.setdefault('absolute_ev_floor', 0.0)

            return result



    def get_quantitative_params(self) -> Dict[str, Any]:

        """获取量化策略参数子集



        返回量化策略核心配置:

        - 凯利公式参数(kelly_fraction_

        - 风控参数(max_drawdown_

        - 延迟/时间参数

        - TVF六维因子参数



        Returns:

            Dict[str, Any]: 量化策略参数字典

        """

        with self._lock:

            result: Dict[str, Any] = {}

            quant_prefixes = ('kelly_', 'risk_', 'max_', 'delay_', 'tvf_', 'sortino_', 'calmar_', 'sharpe_')

            for k, v in self._params.items():

                if isinstance(k, str) and any(k.startswith(p) for p in quant_prefixes):

                    result[k] = v

            result.setdefault('kelly_fraction', 0.25)

            result.setdefault('max_drawdown_pct', 5.0)

            result.setdefault('delay_tiers_count', 6)

            return result



    def get_intuition_params(self) -> Dict[str, Any]:

        """获取直觉策略参数子集



        返回箱体/弹簧/共振等直觉判断策略参数

        - box_开头的箱体参数

        - spring_开头的弹簧参数

        - resonance_开头的共振参数

        - 状态参数correct/incorrect/other)



        Returns:

            Dict[str, Any]: 直觉策略参数字典

        """

        with self._lock:

            result: Dict[str, Any] = {}

            intuition_prefixes = ('box_', 'spring_', 'resonance_', 'correct_', 'incorrect_', 'other_')

            for k, v in self._params.items():

                if isinstance(k, str) and any(k.startswith(p) for p in intuition_prefixes):

                    result[k] = v

            result.setdefault('box_gain_ratio', 0.618)

            result.setdefault('spring_threshold', 0.5)

            result.setdefault('resonance_period', 5)

            return result



    # ========================================================================

    # 全局单例管理

    # ========================================================================



# 全局 ParamsService 单例

_params_service_instance: Optional['ParamsService'] = None

_params_service_lock = threading.Lock()  # R1-1修复: 补回拆分时遗漏的锁定义




def reset_params_service() -> None:

    """R27-CP-03-FIX: 重置ParamsService单例，用于测试隔离"""

    global _params_service_instance

    with _params_service_lock:

        _params_service_instance = None





def get_params_service() -> ParamsService:

    """

    获取全局 ParamsService 单例

    

    Returns:

        ParamsService: 全局参数服务实例

        

    Raises:

        RuntimeError: 如果单例初始化失败
    """

    global _params_service_instance

    

    with _params_service_lock:

        if _params_service_instance is None:

            instance = ParamsService()

            try:

                instance.load_attribute_matrix()

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logging.warning("[ParamsService] Attribute matrix auto-load failed: %s", e)

            _params_service_instance = instance

        return _params_service_instance





def get_params_by_category(category: str) -> Dict[str, Any]:

    """按参数分类获取参数子。'
    Args:

        category: 参数分类名称，支持'shadow'/'quantitative'/'intuition'



    Returns:

        Dict[str, Any]: 对应分类的参数字典
    """

    ps = get_params_service()

    if category == "shadow":

        return ps.get_shadow_params()

    elif category == "quantitative":

        return ps.get_quantitative_params()

    elif category == "intuition":

        return ps.get_intuition_params()

    else:

        logging.warning("[ParamsService] Unknown category: %s, returning all params", category)

        return ps.get_all()





def get_params_by_risk_level(risk_level: str = "all") -> Dict[str, Any]:

    """按风险等级筛选参数


    Args:

        risk_level: 风险等级，支持'low'/'medium'/'high'/'all'



    Returns:

        Dict[str, Any]: 符合风险等级的参数字典
    """

    ps = get_params_service()

    all_params = ps.get_all()

    if risk_level == "all":

        return all_params

    risk_map = {"low": 1, "medium": 2, "high": 3}

    target_level = risk_map.get(risk_level, 2)

    filtered = {}

    for k, v in all_params.items():

        if not isinstance(v, (int, float)):

            continue

        attr_matrix = ps.get_attribute_matrix()

        if k in attr_matrix:

            param_risk = attr_matrix[k].get("risk_level", "medium")

            param_level = risk_map.get(param_risk, 2)

            if param_level <= target_level:

                filtered[k] = v

        else:

            filtered[k] = v

    return filtered





def get_params_by_strategy_type(strategy_type: str = "all") -> Dict[str, Any]:

    """按策略类型筛选参数


    Args:

        strategy_type: 策略类型，支持'box_extreme'/'spring'/'trend'/'arbitrage'/'market_making'/'all'



    Returns:

        Dict[str, Any]: 符合策略类型的参数字典
    """

    ps = get_params_service()

    all_params = ps.get_all()

    if strategy_type == "all":

        return all_params

    strategy_prefix_map = {

        "box_extreme": ["box_", "extreme_", "n1_"],

        "spring": ["spring_", "bounce_"],

        "trend": ["trend_", "ma_", "ema_", "macd_"],

        "arbitrage": ["arb_", "spread_"],

        "market_making": ["mm_", "quote_"],

    }

    prefixes = strategy_prefix_map.get(strategy_type, [strategy_type + "_"])

    return {k: v for k, v in all_params.items() if any(k.startswith(p) for p in prefixes)}







# ============================================================================

# 参数读取工具函数

# _strategy_core_service.py 提取，解决循环导入：

#   strategy_core_service _strategy_historical _strategy_core_service

# ============================================================================



def _read_param(params: Any, key: str, default: Any = None) -> Any:

    """接口唯一修复：统一为对象属性访问，dict先转SimpleNamespace"""

    if params is None:

        return default

    if isinstance(params, dict):

        from types import SimpleNamespace

        params = SimpleNamespace(**params)

    return getattr(params, key, default)





def get_param_value(params: Any, key: str, default: Any = None, alt_keys: Optional[List[str]] = None) -> Any:

    """接口唯一修复：统一对象访问入口，dict先转换，alt_keys为降级键）"""
    value = _read_param(params, key)

    if value is not None:

        return value

    for alt_key in alt_keys or []:

        value = _read_param(params, alt_key)

        if value is not None:

            return value

    return default





# ============================================================================

# 模块导出

# ============================================================================



__all__ = [

    'ParamsService',

    'OutputConfig',

    'ParamObserver',

    'ParamAuditObserver',

    'get_param_value',

    'get_params_by_category',

    'get_params_by_risk_level',

    'get_params_by_strategy_type',

    # UPG-P1-02: 参数迁移工具

    'migrate_params_rename',

    'migrate_params_default_change',

    'migrate_params_type_change',

    'apply_param_migration_plan',

    # UPG-P1-07/09: 热更新事务性保存
    'hot_update',

    # UPG-P1-12: 参数默认值兼容性检查
    'check_default_value_compatibility',

]





# ============================================================================

# UPG-P1-02修复: 参数迁移工具函数

# ============================================================================



# 参数迁移历史记录（进程级内
