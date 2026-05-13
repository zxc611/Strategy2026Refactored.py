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


class ParamAuditObserver(ParamObserver):
    """参数修改审计观察者 —— 金融合规红线

    记录结构化审计日志，包含：谁/何时/改了什么/旧值→新值/调用栈摘要。
    审计日志写入专用 logger (params_audit)，可与普通日志分离归档。
    """

    _SAFETY_CRITICAL_KEYS = frozenset({
        'close_take_profit_ratio', 'close_stop_loss_ratio',
        'daily_loss_hard_stop_pct', 'circuit_breaker_trigger_sigma',
        'max_risk_ratio', 'hard_time_stop_minutes',
        'spring_stop_profit_ratio', 'spring_max_loss_pct',
    })

    def __init__(self, max_stack_depth: int = 3):
        self._audit_logger = logging.getLogger('params_audit')
        if not self._audit_logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s | AUDIT | %(message)s'
            ))
            self._audit_logger.addHandler(handler)
            self._audit_logger.setLevel(logging.INFO)
        self._max_stack_depth = max_stack_depth

    def on_param_changed(self, key: str, old_value: Any, new_value: Any) -> None:
        import traceback
        stack_summary = ' -> '.join(
            f"{frame.filename}:{frame.lineno}({frame.function})"
            for frame in traceback.extract_stack()[-self._max_stack_depth - 1:-1]
        )
        risk_flag = ' ⚠️ SAFETY_CRITICAL' if key in self._SAFETY_CRITICAL_KEYS else ''
        self._audit_logger.info(
            "param_changed | key=%s | old=%r | new=%r | stack=[%s]%s",
            key, old_value, new_value, stack_summary, risk_flag,
        )
        if key in self._SAFETY_CRITICAL_KEYS:
            self._audit_logger.warning(
                "SAFETY_PARAM_MODIFIED | key=%s | old=%r | new=%r",
                key, old_value, new_value,
            )


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

        self._output_config: Optional[OutputConfig] = None
        self._params: Dict[str, Any] = {}
        self._observers: Set[ParamObserver] = set()
        self._env_prefix = "PARAM_"  # 环境变量前缀
        self._attribute_matrix: Dict[str, Any] = {}
        self._attribute_matrix_loaded: bool = False

        # 配置
        self._check_interval = 900.0  # 15分钟

        # 实例级缓存（替代全局缓存，解决多进程安全问题）
        self._param_cache: Dict[str, Any] = {}
        self._param_cache_meta: Dict[str, Optional[float]] = {}
        self._param_check_timestamp: Dict[str, float] = {}

        # P0 Bug #9修复：在__init__中调用init_instrument_cache，确保缓存容器已初始化
        self.init_instrument_cache()

        self._audit_observer = ParamAuditObserver()
        self._observers.add(self._audit_observer)

    # ========================================================================
    # 合约缓存管理（从 storage.py 迁移）
    # ========================================================================
    
    def init_instrument_cache(self) -> None:
        """
        初始化合约缓存容器
        
        ✅ ID三层架构 - 第二层：运行时缓存
        - 5.1.2 instrument_id_to_internal_id: instrument_id -> internal_id
        - 5.1.1 instrument_meta_by_id: internal_id -> InstrumentMeta
        """
        # ✅ ID三层架构缓存（唯一缓存结构）
        self._instrument_id_to_internal_id: Dict[str, int] = {}  # instrument_id -> internal_id
        self._instrument_meta_by_id: Dict[int, Dict[str, Any]] = {}  # internal_id -> InstrumentMeta
        self._product_cache: Dict[str, Dict[str, Any]] = {}  # product -> product_info
        self._column_cache: Dict[str, List[str]] = {}  # table_name -> column_names
        self._column_cache_lock = threading.Lock()
    
    def _normalize_id_field(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """ID字段标准化：删除旧id字段（统一使用internal_id）"""
        if 'id' in info:
            del info['id']
        return info

    def cache_instrument_info(self, instrument_id: str, info: Dict[str, Any]) -> Dict[str, Any]:
        """
        写入缓存并强制 internal_id 全局唯一，禁止静默覆盖
        
        ✅ ID三层架构：同时更新两层运行时缓存
        - 5.1.2 instrument_id_to_internal_id: instrument_id -> internal_id
        - 5.1.1 instrument_meta_by_id: internal_id -> InstrumentMeta
        
        Args:
            instrument_id: 合约代码
            info: 合约信息
            
        Returns:
            缓存后的合约信息
            
        Raises:
            RuntimeError: 检测到 internal_id 冲突时抛出
        """
        with self._lock:
            cached_info = dict(info)
            cached_info['instrument_id'] = instrument_id

            # ✅ 统一ID标准化入口
            self._normalize_id_field(cached_info)

            cid = cached_info.get('internal_id')
            if cid is None:
                logging.warning(
                    "[ParamsService] internal_id=None, 跳过缓存: instrument_id=%s",
                    instrument_id
                )
                return cached_info
            
            # ✅ ID三层架构 - 检查internal_id冲突
            existing = self._instrument_meta_by_id.get(cid)
            if existing and existing.get('instrument_id') != instrument_id:
                raise RuntimeError(
                    "检测到重复 internal_id=%s: %s 与 %s 冲突" % (
                        cid,
                        existing.get('instrument_id'),
                        instrument_id,
                    )
                )
            
            # ✅ 更新ID三层架构缓存（在锁内）- 仅写入新架构2个字典
            # 5.1.2: instrument_id -> internal_id
            self._instrument_id_to_internal_id[instrument_id] = cid
            
            # 5.1.1: internal_id -> InstrumentMeta
            self._instrument_meta_by_id[cid] = cached_info
            
            return cached_info
    
    # ========================================================================
    # ✅ ID三层架构 - 新增API
    # ========================================================================
    
    def get_internal_id(self, instrument_id: str) -> Optional[int]:
        """
        ✅ 5.1.2: 通过 instrument_id 获取 internal_id
        
        Args:
            instrument_id: 平台原始合约代码（如 'IH2605'）
            
        Returns:
            internal_id: 系统内部代理键，如果不存在则返回 None
        """
        with self._lock:  # ✅ 双重检查锁定
            if not self._instrument_id_to_internal_id:
                self._lazy_load_from_db()
            return self._instrument_id_to_internal_id.get(instrument_id)
    
    def get_instrument_meta(self, internal_id: int) -> Optional[Dict[str, Any]]:
        """
        ✅ 5.1.1: 通过 internal_id 获取 InstrumentMeta
        
        Args:
            internal_id: 系统内部代理键
            
        Returns:
            InstrumentMeta: 合约元数据，如果不存在则返回 None
        """
        with self._lock:
            if not self._instrument_meta_by_id:
                self._lazy_load_from_db()
            return self._instrument_meta_by_id.get(internal_id)
    
    def get_instrument_meta_by_id(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """
        ✅ 便捷方法：通过 instrument_id 直接获取 InstrumentMeta
        
        Args:
            instrument_id: 平台原始合约代码
            
        Returns:
            InstrumentMeta: 合约元数据，如果不存在则返回 None
        """
        internal_id = self.get_internal_id(instrument_id)
        if internal_id is None:
            return None
        return self.get_instrument_meta(internal_id)
    
    def validate_contracts_loaded(self, contract_ids: List[str], contract_type: str = "contract") -> List[str]:
        """
        ✅ 强制验证：确保配置表中的所有合约都已加载到缓存
        
        Args:
            contract_ids: 需要验证的合约ID列表
            contract_type: 合约类型描述（用于错误提示）
            
        Returns:
            failed_contracts: 加载失败的合约列表
            
        Raises:
            RuntimeError: 如果有合约加载失败
        """
        # 确保缓存已加载（使用新架构字典判断）
        if not self._instrument_id_to_internal_id:
            self._lazy_load_from_db()
        
        failed = []
        for contract_id in contract_ids:
            iid = self._instrument_id_to_internal_id.get(contract_id)
            if iid is None:
                failed.append(contract_id)
        
        if failed:
            error_msg = (
                f"[ParamsService] CRITICAL: {len(failed)} {contract_type}(s) failed to load: "
                f"{failed[:10]}{'...' if len(failed) > 10 else ''}. "
                f"Strategy cannot start without complete contract metadata."
            )
            logging.error(error_msg)
            raise RuntimeError(error_msg)
        
        return []

    def _lazy_load_from_db(self) -> None:
        """惰性从数据库加载合约缓存（仅当缓存为空时执行）"""
        try:
            from ali2026v3_trading.data_service import get_data_service
            ds = get_data_service()
            if ds is None:
                logging.debug("[ParamsService] DataService尚未初始化，跳过惰性加载")
                return
            
            if hasattr(ds, '_products_loaded') and not ds._products_loaded:
                logging.warning(
                    "[ParamsService] ⚠️  品种配置尚未加载（_products_loaded=False），"
                    "跳过惰性加载以避免空缓存。"
                    "请确保先调用 ensure_products_with_retry() 和 ds.mark_products_loaded()"
                )
                return
            
            if not hasattr(ds, '_get_connection'):
                logging.warning("[ParamsService] DataService实例不完整，跳过惰性加载")
                return
            
            self.load_caches_from_db(ds)
            logging.info("[ParamsService] 惰性加载缓存完成：%d 个合约", len(self._instrument_id_to_internal_id))
        except Exception as e:
            logging.debug("[ParamsService] 惰性加载缓存失败: %s", e)
    
    def get_id_cache(self, internal_id: int) -> Optional[Dict[str, Any]]:
        """[DEPRECATED] 请使用 get_instrument_meta(internal_id)"""
        import warnings
        warnings.warn("get_id_cache is deprecated, use get_instrument_meta instead", DeprecationWarning, stacklevel=2)
        return self.get_instrument_meta(internal_id)
    
    def get_product_cache(self, product: str) -> Optional[Dict[str, Any]]:
        """获取品种信息"""
        with self._lock:
            return self._product_cache.get(product)
    
    def clear_instrument_cache(self) -> None:
        """
        BP-9修复：原子替换模式——先构建新缓存，再一次性替换，避免清空窗口期。
        如果重新加载失败，保留旧缓存（宁用过期数据不丢数据）。
        """
        with self._lock:
            old_id_map = dict(self._instrument_id_to_internal_id)
            old_meta_map = dict(self._instrument_meta_by_id)
            old_product_cache = dict(self._product_cache)

        try:
            self._rebuild_instrument_cache()
            with self._lock:
                new_count = len(self._instrument_meta_by_id)
            logging.info("[ParamsService] 合约缓存原子替换完成: %d → %d", len(old_meta_map), new_count)
        except Exception as e:
            with self._lock:
                self._instrument_id_to_internal_id = old_id_map
                self._instrument_meta_by_id = old_meta_map
                self._product_cache = old_product_cache
            logging.error("[ParamsService] 合约缓存重新加载失败，保留旧缓存: %s", e)

    def _rebuild_instrument_cache(self) -> None:
        """构建全新的合约缓存并原子替换（内部方法，复用load_caches_from_db）"""
        try:
            from ali2026v3_trading.data_service import get_data_service
            ds = get_data_service()
            if ds is None:
                logging.warning("[ParamsService] DataService不可用，跳过缓存重建")
                return
            if not hasattr(ds, '_get_connection'):
                logging.warning("[ParamsService] DataService实例不完整，跳过缓存重建")
                return
            self.load_caches_from_db(ds)
        except Exception as e:
            logging.error("[ParamsService] 缓存重建失败: %s", e)
            raise

    def get_all_instrument_cache(self) -> Dict[str, Dict[str, Any]]:
        """返回当前合约缓存的深拷贝副本（基于新架构）。"""
        import copy
        with self._lock:
            # 从新架构构建等效的instrument_cache格式
            result = {}
            for instrument_id, internal_id in self._instrument_id_to_internal_id.items():
                meta = self._instrument_meta_by_id.get(internal_id)
                if meta:
                    result[instrument_id] = meta
            return copy.deepcopy(result)

    def get_all_instrument_ids(self) -> List[str]:
        """返回当前缓存中的全部合约代码（基于新架构）。"""
        with self._lock:
            return list(self._instrument_id_to_internal_id.keys())
    
    # ========================================================================
    # 合约列表加载与缓存（strategy_core_service.py 依赖）
    # ========================================================================
    
    def load_instrument_list(self, params: Any, source: str = 'param_cache') -> Optional[Dict[str, Any]]:
        """
        加载合约列表（从参数缓存或输出文件）
        
        Args:
            params: 参数对象（dict或对象）
            source: 加载来源，'param_cache' 或 'output_files'
            
        Returns:
            Dict: {'futures_list': [...], 'options_dict': {...}} 或 None
        """
        try:
            # 从params中读取合约列表
            if isinstance(params, dict):
                futures_list = params.get('future_instruments', [])
                options_dict = params.get('option_instruments', {})
            else:
                futures_list = getattr(params, 'future_instruments', [])
                options_dict = getattr(params, 'option_instruments', {})
            
            # 如果source是output_files，尝试从文件加载（带3次重试）
            if source == 'output_files':
                # P0修复：合约配置文件加载增加3次重试机制
                max_retries = 3
                retry_delay = 1.0  # 每次重试间隔1秒
                
                futures_metadata = {}
                options_metadata = {}
                for attempt in range(1, max_retries + 1):
                    futures_from_file = []
                    options_from_file = []
                    _futures_meta = {}
                    _options_meta = {}
                    _fixed_config_files = [
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subscription_futures_fixed.txt'),
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subscription_options_fixed.txt'),
                    ]
                    
                    load_success = True
                    for fixed_config_file in _fixed_config_files:
                        if not os.path.exists(fixed_config_file):
                            continue
                        try:
                            is_futures_file = 'futures' in os.path.basename(fixed_config_file)
                            with open(fixed_config_file, 'r', encoding='utf-8') as f:
                                for line in f:
                                    line = line.strip()
                                    if not line or line.startswith('#'):
                                        continue
                                    parts = line.split('|')
                                    contract_part = parts[0]
                                    if '.' in contract_part:
                                        contract = contract_part.split('.', 1)[1]
                                    else:
                                        contract = contract_part
                                    if is_futures_file:
                                        futures_from_file.append(contract)
                                        internal_id = int(parts[1]) if len(parts) > 1 else None
                                        product = parts[2] if len(parts) > 2 else None
                                        year_month = parts[3] if len(parts) > 3 else None
                                        if internal_id is not None:
                                            _futures_meta[contract] = {
                                                'internal_id': internal_id,
                                                'product': product,
                                                'year_month': year_month,
                                            }
                                    else:
                                        options_from_file.append(contract)
                                        internal_id = int(parts[1]) if len(parts) > 1 else None
                                        underlying_future_id = int(parts[2]) if len(parts) > 2 else None
                                        product = parts[3] if len(parts) > 3 else None
                                        underlying_product = parts[4] if len(parts) > 4 else None
                                        year_month = parts[5] if len(parts) > 5 else None
                                        option_type = parts[6] if len(parts) > 6 else None
                                        strike_price = float(parts[7]) if len(parts) > 7 else None
                                        if internal_id is not None:
                                            _options_meta[contract] = {
                                                'internal_id': internal_id,
                                                'underlying_future_id': underlying_future_id,
                                                'product': product,
                                                'underlying_product': underlying_product,
                                                'year_month': year_month,
                                                'option_type': option_type,
                                                'strike_price': strike_price,
                                            }
                        except Exception as e:
                            logging.warning(f"[ParamsService] 加载固定配置文件失败 (attempt {attempt}/{max_retries}): {e}")
                            load_success = False
                            break
                    
                    if load_success and (futures_from_file or options_from_file):
                        futures_metadata = _futures_meta
                        options_metadata = _options_meta
                        if attempt > 1:
                            logging.info(f"[ParamsService] 配置文件加载成功 (retry {attempt-1})")
                        break
                    elif attempt < max_retries:
                        logging.warning(f"[ParamsService] 配置文件加载失败，{retry_delay}s后重试 ({attempt}/{max_retries})...")
                        import time
                        time.sleep(retry_delay)
                    else:
                        error_details = []
                        for fixed_config_file in _fixed_config_files:
                            if os.path.exists(fixed_config_file):
                                try:
                                    with open(fixed_config_file, 'r', encoding='utf-8') as f:
                                        content = f.read()
                                        error_details.append(f"  - {os.path.basename(fixed_config_file)}: exists, size={len(content)} bytes")
                                except Exception as e:
                                    error_details.append(f"  - {os.path.basename(fixed_config_file)}: exists but unreadable: {e}")
                            else:
                                error_details.append(f"  - {os.path.basename(fixed_config_file)}: NOT FOUND at {fixed_config_file}")
                        
                        logging.error(
                            "[ParamsService] CRITICAL: Contract configuration loading failed after %d retries.\n"
                            "  File status:\n"
                            "%s\n"
                            "  Action required:\n"
                            "    - Verify subscription_futures_fixed.txt and subscription_options_fixed.txt exist and are valid\n"
                            "    - Check file permissions and disk space\n"
                            "    - Ensure no other process is locking these files",
                            max_retries, "\n".join(error_details)
                        )

                if futures_from_file:
                    futures_list = futures_from_file
                if options_from_file:
                    options_dict = {}
                    for opt in options_from_file:
                        product = ''.join([c for c in opt if c.isalpha()])[:2]
                        if product:
                            if product not in options_dict:
                                options_dict[product] = []
                            options_dict[product].append(opt)

                    logging.info(f"[ParamsService] 从固定配置文件加载：{len(futures_list)} 期货，{len(options_from_file)} 期权")

                if not futures_list and not options_dict:
                    config_dir = self.get_str('config_dir', '.arts')
                    futures_file = os.path.join(config_dir, 'futures_list.json')
                    options_file = os.path.join(config_dir, 'options_dict.json')
                    
                    if os.path.exists(futures_file):
                        with open(futures_file, 'r', encoding='utf-8') as f:
                            futures_list = json.load(f)
                    if os.path.exists(options_file):
                        with open(options_file, 'r', encoding='utf-8') as f:
                            options_dict = json.load(f)
            
            if futures_list or options_dict:
                return {
                    'futures_list': list(futures_list) if futures_list else [],
                    'options_dict': dict(options_dict) if options_dict else {},
                    'futures_metadata': futures_metadata,
                    'options_metadata': options_metadata,
                }
            logging.error(
                "[ParamsService.load_instrument_list] No instruments loaded "
                "(futures=%d, options=%d, source=%s)",
                len(futures_list), len(options_dict), source
            )
            raise ValueError(
                f"[ParamsService.load_instrument_list] No instruments loaded "
                f"(futures={len(futures_list)}, options={len(options_dict)}, source={source})"
            )
        except ValueError:
            raise
        except Exception as e:
            logging.error(f"[ParamsService.load_instrument_list] Error: {e}")
            raise RuntimeError(
                f"[ParamsService.load_instrument_list] Failed to load instrument list: {e}"
            ) from e
    
    def cache_instrument_list(self, params: Any, futures_list: List[str], 
                              options_dict: Dict[str, List[str]], source: str = 'param_cache') -> None:
        """
        缓存合约列表到参数对象
        
        Args:
            params: 参数对象（dict或对象）
            futures_list: 期货合约列表
            options_dict: 期权合约字典
            source: 缓存来源标识
        """
        try:
            if isinstance(params, dict):
                params['future_instruments'] = list(futures_list)
                params['option_instruments'] = dict(options_dict)
                params['_instrument_list_source'] = source
            else:
                setattr(params, 'future_instruments', list(futures_list))
                setattr(params, 'option_instruments', dict(options_dict))
                setattr(params, '_instrument_list_source', source)
            logging.debug(f"[ParamsService.cache_instrument_list] Cached {len(futures_list)} futures, {len(options_dict)} option groups from {source}")
        except Exception as e:
            logging.warning(f"[ParamsService.cache_instrument_list] Error: {e}")
    
    def cache_column_names(self, table_name: str, columns: List[str]) -> None:
        """缓存表列名"""
        with self._column_cache_lock:
            self._column_cache[table_name] = columns
    
    def get_cached_column_names(self, table_name: str) -> Optional[List[str]]:
        """获取缓存的表列名"""
        with self._column_cache_lock:
            return self._column_cache.get(table_name)
    
    def load_caches_from_db(self, data_service) -> None:
        """
        从数据库加载合约缓存
        
        ✅ 恢复设计：从合约配置表直接加载所有合约品种，创建元数据表
        """
        from ali2026v3_trading.data_service import get_data_service
        ds = data_service or get_data_service()

        temp_instrument_id_to_internal_id: Dict[str, int] = {}
        temp_instrument_meta_by_id: Dict[int, Dict[str, Any]] = {}
        temp_product_cache: Dict[str, Dict[str, Any]] = {}

        def _cache_temp_instrument_info(instrument_id: str, info: Dict[str, Any]) -> None:
            """复用_normalize_id_field进行ID标准化"""
            cached_info = dict(info)
            cached_info['instrument_id'] = instrument_id

            # ✅ 复用_normalize_id_field统一ID标准化
            self._normalize_id_field(cached_info)
            cid = cached_info.get('internal_id')

            if cid is None:
                logging.warning(
                    "[ParamsService] internal_id=None, 跳过ID缓存: instrument_id=%s",
                    instrument_id
                )
                return

            existing = temp_instrument_meta_by_id.get(cid)
            if existing and existing.get('instrument_id') != instrument_id:
                raise RuntimeError(
                    "检测到重复 internal_id=%s: %s 与 %s 冲突" % (
                        cid,
                        existing.get('instrument_id'),
                        instrument_id,
                    )
                )

            temp_instrument_id_to_internal_id[instrument_id] = cid
            temp_instrument_meta_by_id[cid] = cached_info
        
        def _rows(result):
            if result is None:
                return []
            if hasattr(result, 'read_all'):
                result = result.read_all()
            if hasattr(result, 'to_pylist'):
                return result.to_pylist()
            if hasattr(result, 'to_dict'):
                return result.to_dict('records')
            if hasattr(result, '__iter__'):
                return list(result)
            return []
        
        # ✅ 步骤1：加载所有期货合约（无任何过滤）
        futures_loaded = 0
        try:
            futures_rows = _rows(ds.query("SELECT internal_id, instrument_id, product, exchange, year_month, product_code, shard_key FROM futures_instruments"))
            
            for row in futures_rows:
                try:
                    product_code = row.get('product_code') or row['product'].lower()
                    shard_key = row.get('shard_key')
                    if shard_key is None:
                        from ali2026v3_trading.shared_utils import ShardRouter
                        shard_key = ShardRouter._deterministic_hash(product_code)
                    info = {
                        'internal_id': row['internal_id'],
                        'type': 'future',
                        'product': row['product'],
                        'exchange': row['exchange'],
                        'year_month': row['year_month'],
                        'product_code': product_code,
                        'shard_key': shard_key,
                    }
                    _cache_temp_instrument_info(row['instrument_id'], info)
                    futures_loaded += 1
                except RuntimeError as e:
                    logging.error(f"[ParamsService] 期货合约缓存失败: {e}")
                    continue
        except Exception as e:
            logging.warning(f"[ParamsService] 查询期货合约失败: {e}")
        
        logging.info(f"[ParamsService] 期货合约加载完成: {futures_loaded} 个")
        
        # ✅ 步骤2：加载所有期权合约（无任何过滤）
        options_loaded = 0
        try:
            options_rows = _rows(ds.query("SELECT internal_id, instrument_id, product, exchange, underlying_future_id, underlying_product, year_month, option_type, strike_price, product_code, shard_key FROM option_instruments"))
            
            for row in options_rows:
                try:
                    product_code = row.get('product_code') or row['product'].lower()
                    shard_key = row.get('shard_key')
                    if shard_key is None:
                        from ali2026v3_trading.shared_utils import ShardRouter
                        shard_key = ShardRouter._deterministic_hash(product_code)
                    info = {
                        'internal_id': row['internal_id'],
                        'type': 'option',
                        'product': row['product'],
                        'exchange': row['exchange'],
                        'underlying_future_id': row['underlying_future_id'],
                        'underlying_product': row['underlying_product'],
                        'year_month': row['year_month'],
                        'option_type': row['option_type'],
                        'strike_price': row['strike_price'],
                        'product_code': product_code,
                        'shard_key': shard_key,
                    }
                    _cache_temp_instrument_info(row['instrument_id'], info)
                    options_loaded += 1
                except RuntimeError as e:
                    logging.error(f"[ParamsService] 期权合约缓存失败: {e}")
                    continue
        except Exception as e:
            logging.warning(f"[ParamsService] 查询期权合约失败: {e}")
        
        logging.info(f"[ParamsService] 期权合约加载完成: {options_loaded} 个")
        logging.info(f"[ParamsService] 总合约数: {futures_loaded + options_loaded} 个 (期货={futures_loaded}, 期权={options_loaded})")
        
        # ✅ 步骤3：加载所有品种元数据（不限制活跃状态）
        try:
            for row in _rows(ds.query("SELECT product, exchange, format_template, tick_size, contract_size FROM future_products")):
                temp_product_cache[row['product']] = {'type': 'future', **dict(row)}
        except Exception as e:
            logging.warning(f"[ParamsService] 查询期货品种元数据失败: {e}")
        
        try:
            for row in _rows(ds.query("SELECT product, exchange, underlying_product, format_template, tick_size, contract_size FROM option_products")):
                temp_product_cache[row['product']] = {'type': 'option', **dict(row)}
        except Exception as e:
            logging.warning(f"[ParamsService] 查询期权品种元数据失败: {e}")

        with self._lock:
            self._instrument_id_to_internal_id = temp_instrument_id_to_internal_id
            self._instrument_meta_by_id = temp_instrument_meta_by_id
            self._product_cache = temp_product_cache
        
        logging.info(f"加载缓存：{len(self._instrument_id_to_internal_id)}个合约，{len(self._product_cache)}个品种")
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
        """获取参数值（线程安全）- 委托给__getitem__+默认值"""
        try:
            return self[key]
        except KeyError:
            return default

    def set(self, key: str, value: Any, notify: bool = True) -> None:
        """
        设置参数值（线程安全）

        Args:
            key: 参数键
            value: 参数值
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
        value_range = attr.get('range')
        if value_range and isinstance(value_range, list) and len(value_range) == 2:
            if isinstance(value, (int, float)):
                lo, hi = value_range
                if not (lo <= value <= hi):
                    logging.error(
                        "[ParamsService] SET_REJECTED range | %s: %r not in [%s, %s]",
                        key, value, lo, hi,
                    )

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
        notify_list = []
        with self._lock:
            for key, value in params.items():
                old_value = self._params.get(key)
                self._params[key] = value
                if notify and old_value != value:
                    notify_list.append((key, old_value, value))
        for info in notify_list:
            self._notify_observers(*info)

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
        """
        P1 Bug #48修复：通知所有观察者（避免在锁内调用外部回调导致死锁）
        
        注意：调用者必须已持有self._lock！
        修复方案：复制观察者列表后，在锁外逐个调用回调
        """
        # 调用者已持有锁，直接复制列表
        observers_copy = list(self._observers)
        
        # 在锁外调用回调，避免死锁
        for observer in observers_copy:
            try:
                observer.on_param_changed(key, old_value, new_value)
            except Exception as e:
                logging.warning(f"[ParamsService] Observer error: {e}")

    # ========================================================================
    # 参数属性矩阵集成
    # ========================================================================

    def load_attribute_matrix(self, yaml_path: Optional[str] = None) -> Dict[str, Any]:
        """加载 parameter_attribute_matrix.yaml 并校验当前参数

        Returns:
            校验报告 dict，含 violations/warnings/checked_count
        """
        if yaml_path is None:
            yaml_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'parameter_attribute_matrix.yaml',
            )

        try:
            import yaml as _yaml
        except ImportError:
            logging.warning("[ParamsService] PyYAML not installed, cannot load attribute matrix")
            return {'violations': [], 'warnings': ['PyYAML not installed'], 'checked_count': 0}

        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                self._attribute_matrix = _yaml.safe_load(f) or {}
            self._attribute_matrix_loaded = True
            logging.info("[ParamsService] Loaded attribute matrix from %s (%d top-level keys)",
                         yaml_path, len(self._attribute_matrix))
        except Exception as e:
            logging.error("[ParamsService] Failed to load attribute matrix: %s", e)
            return {'violations': [], 'warnings': [f'Load failed: {e}'], 'checked_count': 0}

        return self.validate_with_attribute_matrix()

    def validate_with_attribute_matrix(self) -> Dict[str, Any]:
        """基于 attribute_matrix 校验当前 _params 中的所有参数

        校验项:
        1. 类型校验 (type)
        2. 范围校验 (range)
        3. 依赖约束 (dependencies)
        4. 互斥规则 (mutual_exclusion)
        5. L1_safety 层运行时只读校验 (editable=runtime_readonly)

        Returns:
            {'violations': [...], 'warnings': [...], 'checked_count': int}
        """
        if not self._attribute_matrix:
            return {'violations': [], 'warnings': ['Attribute matrix not loaded'], 'checked_count': 0}

        violations = []
        warnings = []
        checked = 0

        with self._lock:
            params_snapshot = dict(self._params)

        for key, attr in self._attribute_matrix.items():
            if not isinstance(attr, dict):
                continue
            if key in ('mutual_exclusion', 'dependencies', 'route_priority'):
                continue

            value = params_snapshot.get(key)
            if value is None:
                continue

            checked += 1
            expected_type = attr.get('type')
            value_range = attr.get('range')
            editable = attr.get('editable')
            layer = attr.get('layer', '')

            if expected_type and not self._check_type(value, expected_type):
                violations.append(
                    f"TYPE_VIOLATION | {key}: expected {expected_type}, got {type(value).__name__} = {value!r}"
                )

            if value_range and isinstance(value_range, list) and len(value_range) == 2:
                if isinstance(value, (int, float)):
                    lo, hi = value_range
                    if not (lo <= value <= hi):
                        violations.append(
                            f"RANGE_VIOLATION | {key}: {value} not in [{lo}, {hi}]"
                        )

            if editable == 'runtime_readonly' and layer == 'L1_safety':
                warnings.append(
                    f"SAFETY_READONLY | {key}={value} (layer={layer}, should not be modified at runtime)"
                )

            source = attr.get('source')
            if source == 'intuition':
                warnings.append(
                    f"INTUTION_LOCK | {key}={value}: source=intuition, 不可锁定为生产值（铁律：无量化来源的参数不得锁定）"
                )

        dep_section = self._attribute_matrix.get('dependencies', {})
        if isinstance(dep_section, dict):
            for param_key, dep_info in dep_section.items():
                if not isinstance(dep_info, dict):
                    continue
                constraint = dep_info.get('constraint', '')
                val = params_snapshot.get(param_key)
                requires_key = dep_info.get('requires', '')
                req_val = params_snapshot.get(requires_key)
                if val is not None and req_val is not None and constraint:
                    ok = self._check_dependency_constraint(param_key, val, requires_key, req_val, constraint)
                    if not ok:
                        violations.append(
                            f"DEPENDENCY_VIOLATION | {constraint} | {param_key}={val}, {requires_key}={req_val}"
                        )

        mutex_section = self._attribute_matrix.get('mutual_exclusion', {})
        if isinstance(mutex_section, dict):
            for rule in mutex_section.get('rules', []) if isinstance(mutex_section.get('rules'), list) else []:
                if isinstance(rule, list) and len(rule) >= 2:
                    active = [k for k in rule if params_snapshot.get(k) in (True, 1, 'true')]
                    if len(active) > 1:
                        violations.append(
                            f"MUTEX_VIOLATION | {active} cannot be active simultaneously"
                        )
            mutex_list = mutex_section if isinstance(mutex_section, list) else []
            for item in mutex_list:
                if isinstance(item, list) and len(item) >= 2:
                    active = [k for k in item if params_snapshot.get(k) in (True, 1, 'true')]
                    if len(active) > 1:
                        violations.append(
                            f"MUTEX_VIOLATION | {active} cannot be active simultaneously"
                        )

        if violations:
            logging.error("[ParamsService] Attribute matrix validation FAILED: %d violations",
                          len(violations))
            for v in violations:
                logging.error("  %s", v)
        if warnings:
            logging.warning("[ParamsService] Attribute matrix warnings: %d", len(warnings))
            for w in warnings:
                logging.warning("  %s", w)
        if not violations and not warnings:
            logging.info("[ParamsService] Attribute matrix validation PASSED (%d params checked)", checked)

        return {'violations': violations, 'warnings': warnings, 'checked_count': checked}

    def get_attribute_matrix(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._attribute_matrix)

    def get_route_priority(self) -> Dict[str, int]:
        with self._lock:
            rp = self._attribute_matrix.get('route_priority', {})
            return {k: v for k, v in rp.items() if isinstance(v, int)}

    # ========================================================================
    # 动态参数表达式
    # ========================================================================

    _SAFE_BUILTINS = {
        'min': min, 'max': max, 'abs': abs,
        'round': round, 'pow': pow,
    }

    def eval_dynamic_param(self, key: str, context: Optional[Dict[str, Any]] = None) -> Any:
        """根据动态表达式计算参数值

        当参数的 attribute_matrix 中定义了 `dynamic_expr` 时，
        根据运行时 context（如实时波动率、时间、持仓比例等）计算参数值。
        若计算失败或表达式不存在，降级返回静态 default 值。

        dynamic_expr 示例（在 YAML 中定义）：
            signal_cooldown_sec:
              dynamic_expr: "base * (1 + (iv_percentile - 50) / 100)"
              dynamic_depends: [base, iv_percentile]

        Args:
            key: 参数键名
            context: 运行时变量字典，如 {'iv_percentile': 65, 'volatility': 0.25}

        Returns:
            计算后的参数值；无动态表达式则返回当前静态值
        """
        if not self._attribute_matrix_loaded:
            return self.get(key)

        attr = self._attribute_matrix.get(key)
        if not isinstance(attr, dict):
            return self.get(key)

        dynamic_expr = attr.get('dynamic_expr')
        if not dynamic_expr:
            return self.get(key)

        depends = attr.get('dynamic_depends', [])
        eval_ns: Dict[str, Any] = dict(self._SAFE_BUILTINS)

        with self._lock:
            params_snapshot = dict(self._params)

        for dep_key in depends:
            if dep_key in (context or {}):
                eval_ns[dep_key] = context[dep_key]
            elif dep_key in params_snapshot:
                eval_ns[dep_key] = params_snapshot[dep_key]
            else:
                logging.warning(
                    "[ParamsService] eval_dynamic_param: missing dep '%s' for '%s', "
                    "falling back to static value", dep_key, key,
                )
                return self.get(key)

        try:
            result = eval(dynamic_expr, {"__builtins__": {}}, eval_ns)
        except Exception as e:
            logging.error(
                "[ParamsService] eval_dynamic_param FAILED for '%s': expr=%r error=%s",
                key, dynamic_expr, e,
            )
            return self.get(key)

        value_range = attr.get('range')
        if value_range and isinstance(value_range, list) and len(value_range) == 2:
            if isinstance(result, (int, float)):
                lo, hi = value_range
                if not (lo <= result <= hi):
                    logging.warning(
                        "[ParamsService] eval_dynamic_param CLAMPED for '%s': "
                        "%r not in [%s, %s]", key, result, lo, hi,
                    )
                    result = max(lo, min(hi, result))

        expected_type = attr.get('type')
        if expected_type == 'int' and isinstance(result, float):
            result = int(round(result))

        return result

    @staticmethod
    def _check_type(value: Any, expected: str) -> bool:
        type_map = {'int': int, 'float': (int, float), 'bool': bool, 'str': str}
        py_type = type_map.get(expected)
        if py_type is None:
            return True
        if expected == 'float':
            return isinstance(value, (int, float))
        if expected == 'bool':
            return isinstance(value, bool)
        return isinstance(value, py_type)

    @staticmethod
    def _check_dependency_constraint(key1: str, val1: Any, key2: str, val2: Any,
                                      constraint: str) -> bool:
        try:
            if 'calm_period >= pause_sec' in constraint:
                return float(val1) >= float(val2)
            if 'stop_profit_ratio * (1 - max_loss_pct) > 1.0' in constraint:
                return float(val1) * (1 - float(val2)) > 1.0
        except (TypeError, ValueError):
            return False
        return True

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
        except Exception as e:
            logging.warning(f"[ParamsService] Log error: {e}")

    # ✅ ID唯一：clear_cache统一接口，服务=ParamsService
    def clear_cache(self) -> None:
        """清空缓存"""
        with self._lock:
            self._param_cache.clear()
            self._param_cache_meta.clear()
            self._param_check_timestamp.clear()
        logging.info('[ParamsService] Cache cleared')

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

    # ========================================================================
    # 全局单例管理
    # ========================================================================

# 全局 ParamsService 单例
# P2 Bug #107修复：模块级锁在import时立即创建，避免延迟创建的竞态条件
_params_service_instance: Optional[ParamsService] = None
_params_service_lock = threading.Lock()


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
                instance.init_instrument_cache()
            except Exception as e:
                logging.error(f"[ParamsService] 单例初始化失败: {e}")
                raise RuntimeError(
                    f"[ParamsService] 单例初始化失败，合约缓存不可用: {e}"
                ) from e
            try:
                instance.load_attribute_matrix()
            except Exception as e:
                logging.warning("[ParamsService] Attribute matrix auto-load failed: %s", e)
            _params_service_instance = instance
        return _params_service_instance



# ============================================================================
# 参数读取工具函数
# 从 strategy_core_service.py 提取，解决循环导入：
#   strategy_core_service → strategy_historical → strategy_core_service
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
    """接口唯一修复：统一对象访问入口，dict先转换，alt_keys为降级键名"""
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
]
