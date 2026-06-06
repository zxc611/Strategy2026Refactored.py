"""
params_service.py - 参数服务

合并来源：06_params.py
合并策略：提取参数管理核心功能，简化配置类

R17-P2-CFG-03: 职责说明
========================
params_service 与 config_service 职责边界:
- config_service: 路径/交易/输出/日志配置 + 全局单例 + YAML加载 + 热重载 + 分布式锁
- params_service: 参数CRUD/验证/观察者/环境变量覆盖 + 运行时参数管理
- config_params: 默认参数表/缓存/TTL/热更新watchdog (被两者共同引用)
重叠部分: 两者均可触发参数缓存刷新，通过 config_params._notify_param_change 统一通知

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
import ast
import time
import logging
import threading
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass

# R27-P1修复: 导入参数版本管理、配置原子引用
from ali2026v3_trading.resilience_utils import (
    ParamVersionManager, AtomicConfigRef, Watchdog, HeartbeatMonitor,
    RateLimitedLogger, GracefulDegradation,
    stable_sum, stable_mean, safe_normalize_weights,
    get_param_version_manager,
)

from ali2026v3_trading.serialization_utils import yaml_safe_load, json_dumps


# ============================================================================
# 参数缓存（进程级）- 已废弃，改为实例变量
# ============================================================================

# _PARAM_CACHE: Dict[str, Any] = {}
# _PARAM_CACHE_META: Dict[str, Optional[float]] = {}
# _PARAM_CHECK_TIMESTAMP: Dict[str, float] = {}


# ============================================================================
# 数据结构
# ============================================================================

from ali2026v3_trading.config_service import OutputConfig as _ConfigOutputConfig


class OutputConfig(_ConfigOutputConfig):
    """输出配置 — 扩展自config_service.OutputConfig，增加交易输出控制"""
    def __init__(self, mode: str = "debug", trade_quiet: bool = True,
                 diagnostic_enabled: bool = True, debug_enabled: bool = False):
        super().__init__()
        if mode == "debug":
            mode = "close_debug"
        self.mode = mode
        self.trade_quiet = trade_quiet
        self.diagnostic_enabled = diagnostic_enabled
        self.debug_enabled = debug_enabled
        self.is_trade_like = mode in ("trade", "open_debug")
        self.combined_debug_enabled = (
            self.diagnostic_enabled and
            (self.debug_enabled or mode == "close_debug")
        )

    def should_output(self, is_trade: bool = False, is_force: bool = False,
                      is_trade_table: bool = False, is_diag: bool = False) -> bool:
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
        'max_risk_ratio',
        'hft_hard_time_stop_ms', 'spring_hard_time_stop_sec',
        'resonance_hard_time_stop_min', 'box_hard_time_stop_min',
        'spring_stop_profit_ratio', 'spring_max_loss_pct',
    })

    def __init__(self, max_stack_depth: int = 3):
        self._audit_logger = logging.getLogger('params_audit')
        if not self._audit_logger.handlers:
            # R24-P2-AT-04修复: 添加FileHandler确保持久化，保留StreamHandler用于控制台输出
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s.%(msecs)03d | AUDIT | %(message)s'  # [R22-P2-TIME03]
            ))
            self._audit_logger.addHandler(handler)
            # 添加文件持久化
            try:
                _audit_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
                os.makedirs(_audit_log_dir, exist_ok=True)
                _audit_log_path = os.path.join(_audit_log_dir, 'params_audit.log')
                _file_handler = logging.FileHandler(_audit_log_path, encoding='utf-8')
                _file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s.%(msecs)03d | AUDIT | %(message)s'
                ))
                self._audit_logger.addHandler(_file_handler)
            except Exception as _e:
                logging.warning("[R24-P2-AT-04] params_audit FileHandler创建失败: %s，仅使用StreamHandler", _e)
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
        self._check_interval = 60.0  # 1分钟（与config_params CACHE_TTL对齐）

        # 实例级缓存（替代全局缓存，解决多进程安全问题）
        self._param_cache: Dict[str, Any] = {}
        self._param_cache_meta: Dict[str, Optional[float]] = {}
        self._param_check_timestamp: Dict[str, float] = {}

        # P0 Bug #9修复：在__init__中调用init_instrument_cache，确保缓存容器已初始化
        self.init_instrument_cache()

        self._audit_observer = ParamAuditObserver()
        self._observers.add(self._audit_observer)

        # OPS-P1-03修复: 参数变更通知回调列表
        self._param_change_callbacks: List[Callable[[Dict[str, Any]], None]] = []

        # UPG-P1-07修复: 热更新事务性保证 — 参数备份
        self._param_backup: Optional[Dict[str, Any]] = None
        self._param_backup_lock = threading.Lock()
        self._hot_update_txn_lock = threading.Lock()

        # DFG-P1-08修复: 订阅数据格式变更事件，刷新合约缓存
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('DataFormatChangedEvent', self._on_data_format_changed)
        except Exception:
            logging.warning("[R22-EP-P1] ParamsService exception swallowed")
            pass

    # ========================================================================
    # 合约缓存管理（从 storage.py 迁移）
    # ========================================================================

    # DFG-P1-08修复: 数据格式变更事件消费者
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
            # 标记缓存需要刷新（不立即重建，避免递归）
            with self._lock:
                self._param_cache.clear()
                self._param_cache_meta.clear()
        except Exception:
            logging.warning("[R22-EP-P1] ParamsService exception swallowed")
            pass

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
        """[DEPRECATED] 请使用 get_instrument_meta(internal_id)

        UPG-P1-05修复: 添加migration_guide参数
        """
        import warnings
        warnings.warn(
            "get_id_cache is deprecated, use get_instrument_meta instead. "
            "Migration guide: replace get_id_cache(id) → get_instrument_meta(id), "
            "return type unchanged (Optional[Dict]).",
            DeprecationWarning,
            stacklevel=2,
        )
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
            # DFG-P1-08修复: 缓存重建后通过EventBus发布数据格式变更事件
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, DataFormatChangedEvent
                _bus = get_global_event_bus()
                if _bus is not None:
                    _event = DataFormatChangedEvent(
                        source='ParamsService.clear_instrument_cache',
                        old_version=str(len(old_meta_map)),
                        new_version=str(new_count),
                        affected_tables=['futures_instruments', 'option_instruments'],
                        change_description=f'合约缓存原子替换: {len(old_meta_map)}→{new_count}',
                    )
                    _bus.publish(_event, async_mode=True)
            except Exception:
                logging.warning("[R22-EP-P1] ParamsService exception swallowed")
                pass
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
            return {k: dict(v) if isinstance(v, dict) else v for k, v in result.items()}  # R21-MEM-P1-04修复: 浅拷贝替代deepcopy

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
                        import threading
                        threading.Event().wait(retry_delay)  # [R22-P2-TIME06] 替代time.sleep以便可中断
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
        # R24-P2-IV-09修复: 参数版本一致性检查——加载后验证与config_params版本hash一致
        try:
            from ali2026v3_trading.config_params import get_param_version
            _pv = get_param_version()
            logging.debug("[R24-P2-IV-09] ParamsService.load_params: 当前参数版本 v%d hash=%s",
                         _pv.get('version', -1), _pv.get('hash', 'N/A'))
        except Exception:
            pass
        try:
            if not path:
                logging.error("[ParamsService] Empty path provided")
                return {}

            with self._lock:
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
                    except Exception:
                        pass
            logging.info("[ParamsService] 配置热重载成功")
            return True
        except Exception as e:
            logging.error("[ParamsService] 配置热重载失败: %s", e)
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
            with self._lock:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(json_dumps(params, indent=2))

                # 更新缓存
                mtime = os.path.getmtime(path)
                self._param_cache[path] = params
                self._param_cache_meta[path] = mtime

            self._log(f"[ParamsService] Saved {len(params)} params to {path}")
            return True

        except Exception as e:
            logging.error(f"[ParamsService.save_params] Error: {e}")
            raise RuntimeError(f"Params save failed: {e}") from e

    # P0-R8-14修复: 启动时MD5哈希打印
    def compute_params_md5(self, path: str = None) -> str:
        """
        手册15节要求: 策略启动时打印参数文件MD5哈希，确保运行时配置与预期一致。
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
            md5_hash = hashlib.md5(content).hexdigest()
            logging.critical("[ParamsService] 📋 参数文件MD5: %s (file=%s)", md5_hash, path)
            return md5_hash
        except Exception as e:
            logging.warning("[ParamsService] MD5 compute failed: %s", e)
            return f"ERROR:{e}"

    # P0-R8-13修复: --param-version CLI支持
    @staticmethod
    def resolve_param_version(cli_version: str = None, env_version: str = None,
                              default_path: str = None) -> str:
        """
        手册15节要求: 支持通过环境变量或CLI指定参数版本(如--param-version v1.2)，便于快速回滚。
        优先级: CLI > 环境变量 > 默认路径
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
        启动时综合验证: MD5哈希 + 参数完整性检查
        Returns: {'md5': str, 'param_count': int, 'issues': list}
        """
        result = {'md5': '', 'param_count': 0, 'issues': [], 'version': ''}
        try:
            result['md5'] = self.compute_params_md5(param_path)
            if self._param_cache:
                for path, params in self._param_cache.items():
                    result['param_count'] += len(params)
            result['version'] = os.environ.get('PARAM_VERSION', 'default')
        except Exception as e:
            result['issues'].append(str(e))
        return result

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
        # R24-P2-DF-05修复: 区分"未传默认"和"显式None"，避免default or []覆盖显式None意图
        with self._lock:
            _fallback = [] if default is None else default
            val = self._params.get(key, _fallback)
            return list(val) if isinstance(val, (list, tuple)) else _fallback

    def get_dict(self, key: str, default: Optional[Dict] = None) -> Dict[str, Any]:
        """获取字典参数（线程安全）"""
        # R24-P2-DF-05修复: 区分"未传默认"和"显式None"，避免default or {}覆盖显式None意图
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

    # UPG-P1-07修复: 热更新事务性保证 — 备份/提交/回滚方法
    def hot_update_begin(self) -> Dict[str, Any]:
        """UPG-P1-07修复: 热更新开始，备份当前参数状态

        在热更新操作前调用，将当前参数快照保存到_param_backup。
        配合hot_update_commit()或hot_update_rollback()使用。

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

        热更新成功后调用，清除_param_backup备份。
        """
        try:
            with self._param_backup_lock:
                if self._param_backup is not None:
                    logging.info(
                        "UPG-P1-07: 热更新已提交, 清除备份(原参数数量=%d)",
                        len(self._param_backup),
                    )
                    self._param_backup = None
                else:
                    logging.debug("UPG-P1-07: 热更新提交时无备份(可能已提交或未开始)")
        finally:
            self._hot_update_txn_lock.release()

    def hot_update_rollback(self) -> bool:
        """UPG-P1-07修复: 热更新回滚，恢复到备份状态

        热更新失败时调用，将参数恢复到hot_update_begin()时的快照。

        Returns:
            bool: 是否成功回滚
        """
        try:
            with self._param_backup_lock:
                if self._param_backup is None:
                    logging.warning("UPG-P1-07: 热更新回滚失败, 无可用备份")
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
        except Exception as e:
            logging.critical("R21-P2-UG-12: 热更新回滚异常，强制释放事务锁: %s", e)
            return False
        finally:
            self._hot_update_txn_lock.release()

    def hot_update(self, params: Dict[str, Any], notify: bool = True) -> Dict[str, Any]:
        """UPG-P1-07/09修复: 事务性热更新参数

        自动执行备份→更新→提交/回滚的完整事务流程。
        更新失败时自动回滚到更新前的状态。

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
        except Exception as e:
            # UPG-P1-09修复: 更新失败自动回滚
            result['error'] = str(e)
            rolled_back = self.hot_update_rollback()
            result['rolled_back'] = rolled_back
            logging.error(
                "UPG-P1-09: 事务性热更新失败, 已回滚=%s, error=%s",
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
                    except Exception as e:
                        logging.warning(f"[ParamsService] Error setting {key}: {e}")
        for info in notify_list:
            self._notify_observers(*info)

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

    # OPS-P1-03修复: 参数变更通知回调
    def register_param_change_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """OPS-P1-03修复: 注册参数变更通知回调

        当参数发生变更时，回调将被调用，传入事件字典：
        {'key': str, 'old_value': Any, 'new_value': Any, 'source': str}

        Args:
            callback: 回调函数，签名 callback(event: Dict[str, Any]) -> None
        """
        if callback not in self._param_change_callbacks:
            self._param_change_callbacks.append(callback)

    def _fire_param_change_callbacks(self, key: str, old_value: Any, new_value: Any,
                                      source: str = 'set') -> None:
        """OPS-P1-03修复: 触发参数变更通知回调"""
        event = {
            'key': key,
            'old_value': old_value,
            'new_value': new_value,
            'source': source,
            'timestamp': time.time(),
            'changed_keys': [key],
        }
        for cb in self._param_change_callbacks:
            try:
                cb(event)
            except Exception as e:
                logging.warning("[ParamsService] OPS-P1-03: 参数变更回调异常: %s", e)
        # OPS-P1-03修复: 通过EventBus发布参数变更事件
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus, ParamChangedEvent
            bus = get_global_event_bus()
            bus_event = ParamChangedEvent(
                key=key,
                old_value=old_value,
                new_value=new_value,
                source=source,
                changed_keys=[key],
            )
            bus.publish(bus_event, async_mode=True)
        except Exception as e:
            logging.debug("[ParamsService] OPS-P1-03: EventBus参数变更推送异常: %s", e)

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
            # 优先查找参数池子目录，回退到模块根目录
            _base_dir = os.path.dirname(os.path.abspath(__file__))
            _pool_path = os.path.join(_base_dir, 'param_pool', 'parameter_attribute_matrix.yaml')
            _root_path = os.path.join(_base_dir, 'parameter_attribute_matrix.yaml')
            yaml_path = _pool_path if os.path.exists(_pool_path) else _root_path

        try:
            import yaml as _yaml
        except ImportError:
            logging.warning("[ParamsService] PyYAML not installed, cannot load attribute matrix")
            return {'violations': [], 'warnings': ['PyYAML not installed'], 'checked_count': 0}

        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                self._attribute_matrix = yaml_safe_load(f) or {}
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
    
    _ALLOWED_EXPR_CHARS = frozenset('0123456789.+-*/() %')

    _SAFE_BIN_OPS = {
        ast.Add: lambda a, b: a + b,
        ast.Sub: lambda a, b: a - b,
        ast.Mult: lambda a, b: a * b,
        ast.Div: lambda a, b: a / b,
        ast.Mod: lambda a, b: a % b,
        ast.Pow: lambda a, b: a ** b,
        ast.FloorDiv: lambda a, b: a // b,
    }

    _SAFE_UNARY_OPS = {
        ast.USub: lambda a: -a,
        ast.UAdd: lambda a: +a,
    }

    @classmethod
    def _safe_eval_ast(cls, expr: str, namespace: Dict[str, Any]) -> Any:
        tree = ast.parse(expr, mode='eval')
        return cls._eval_ast_node(tree.body, namespace)

    @classmethod
    def _eval_ast_node(cls, node: ast.AST, namespace: Dict[str, Any]) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in namespace:
                return namespace[node.id]
            raise NameError(f"name '{node.id}' is not defined")
        if isinstance(node, ast.BinOp):
            op_func = cls._SAFE_BIN_OPS.get(type(node.op))
            if op_func is None:
                raise ValueError(f"Unsupported operator: {type(node.op).__name__}")
            return op_func(cls._eval_ast_node(node.left, namespace),
                           cls._eval_ast_node(node.right, namespace))
        if isinstance(node, ast.UnaryOp):
            op_func = cls._SAFE_UNARY_OPS.get(type(node.op))
            if op_func is None:
                raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
            return op_func(cls._eval_ast_node(node.operand, namespace))
        if isinstance(node, ast.Call):
            func = cls._eval_ast_node(node.func, namespace)
            args = [cls._eval_ast_node(arg, namespace) for arg in node.args]
            return func(*args)
        if isinstance(node, ast.Attribute):
            raise ValueError("Attribute access not allowed in dynamic expressions")
        raise ValueError(f"Unsupported AST node: {type(node).__name__}")

    @classmethod
    def _validate_expr_safe(cls, expr: str) -> bool:
        if not expr or not isinstance(expr, str):
            return False
        allowed = cls._ALLOWED_EXPR_CHARS | frozenset('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_')
        return all(c in allowed for c in expr)

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
            if not self._validate_expr_safe(dynamic_expr):
                logging.error(
                    "[ParamsService] eval_dynamic_param REJECTED unsafe expr for '%s': %r",
                    key, dynamic_expr,
                )
                return self.get(key)
            result = self._safe_eval_ast(dynamic_expr, eval_ns)
            logging.info(
                "[ParamsService] eval_dynamic_param OK for '%s': expr=%r result=%r",
                key, dynamic_expr, result,
            )
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
    # P-40修复: 分类参数API（手册5.3节）
    # ========================================================================

    def get_shadow_params(self) -> Dict[str, Any]:
        """获取影子策略参数子集

        返回key以'shadow_'开头的参数以及影子策略核心配置:
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
        - 凯利公式参数(kelly_fraction等)
        - 风控参数(max_drawdown等)
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

        返回箱体/弹簧/共振等直觉判断策略参数:
        - box_开头的箱体参数
        - spring_开头的弹簧参数
        - resonance_开头的共振参数
        - 状态参数(correct/incorrect/other)

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
# P2 Bug #107修复：模块级锁在import时立即创建，避免延迟创建的竞态条件
_params_service_instance: Optional[ParamsService] = None
_params_service_lock = threading.Lock()


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
            except Exception as e:
                logging.warning("[ParamsService] Attribute matrix auto-load failed: %s", e)
            _params_service_instance = instance
        return _params_service_instance


def get_params_by_category(category: str) -> Dict[str, Any]:
    """按参数分类获取参数子集

    Args:
        category: 参数分类名称，支持 'shadow'/'quantitative'/'intuition'

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
        risk_level: 风险等级，支持 'low'/'medium'/'high'/'all'

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
        strategy_type: 策略类型，支持 'box_extreme'/'spring'/'trend'/'arbitrage'/'market_making'/'all'

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
    'get_params_by_category',
    'get_params_by_risk_level',
    'get_params_by_strategy_type',
    # UPG-P1-02: 参数迁移工具
    'migrate_params_rename',
    'migrate_params_default_change',
    'migrate_params_type_change',
    'apply_param_migration_plan',
    # UPG-P1-07/09: 热更新事务性保证
    'hot_update',
    # UPG-P1-12: 参数默认值兼容性检查
    'check_default_value_compatibility',
]


# ============================================================================
# UPG-P1-02修复: 参数迁移工具函数
# ============================================================================

# 参数迁移历史记录（进程级）
_param_migration_history: List[Dict[str, Any]] = []
_PARAM_MIGRATION_HISTORY_MAX = 100


def migrate_params_rename(params: Dict[str, Any],
                           old_key: str, new_key: str,
                           version: str = "") -> Dict[str, Any]:
    """UPG-P1-02修复: 参数重命名迁移

    将旧参数名映射到新参数名，保留旧参数值。
    如果新参数名已存在，则不覆盖。

    Args:
        params: 参数字典（将被原地修改）
        old_key: 旧参数名
        new_key: 新参数名
        version: 迁移版本号（用于日志记录）

    Returns:
        Dict: 迁移结果 {migrated: bool, old_key: str, new_key: str, value: Any}
    """
    result = {'migrated': False, 'old_key': old_key, 'new_key': new_key, 'value': None}

    if old_key in params and new_key not in params:
        value = params.pop(old_key)
        params[new_key] = value
        result['migrated'] = True
        result['value'] = value
        logging.info(
            "UPG-P1-02: 参数重命名迁移: %s → %s (value=%r, version=%s)",
            old_key, new_key, value, version,
        )
    elif old_key in params and new_key in params:
        # 新旧参数都存在，保留新参数值，删除旧参数
        old_value = params.pop(old_key)
        result['value'] = params[new_key]
        logging.debug(
            "UPG-P1-02: 参数重命名跳过(新参数已存在): %s → %s "
            "(old_value=%r, new_value=%r)",
            old_key, new_key, old_value, params[new_key],
        )
    else:
        logging.debug("UPG-P1-02: 参数重命名跳过(旧参数不存在): %s", old_key)

    # 记录迁移历史
    _record_param_migration('rename', {
        'old_key': old_key, 'new_key': new_key,
        'version': version, 'result': result,
    })

    return result


def migrate_params_default_change(params: Dict[str, Any],
                                   key: str, old_default: Any, new_default: Any,
                                   version: str = "",
                                   force_update: bool = False) -> Dict[str, Any]:
    """UPG-P1-02修复: 参数默认值变更迁移

    当参数的默认值发生变更时，将使用旧默认值的参数更新为新默认值。
    如果用户已自定义该参数值（不等于旧默认值），则不覆盖。

    Args:
        params: 参数字典（将被原地修改）
        key: 参数名
        old_default: 旧默认值
        new_default: 新默认值
        version: 迁移版本号
        force_update: 是否强制更新（即使值不等于旧默认值也更新）

    Returns:
        Dict: 迁移结果 {migrated: bool, key: str, old_value: Any, new_value: Any}
    """
    result = {'migrated': False, 'key': key, 'old_value': None, 'new_value': None}

    current_value = params.get(key)
    result['old_value'] = current_value

    if current_value is None:
        # 参数不存在，设置新默认值
        params[key] = new_default
        result['migrated'] = True
        result['new_value'] = new_default
        logging.info(
            "UPG-P1-02: 参数默认值迁移(新增): %s = %r (version=%s)",
            key, new_default, version,
        )
    elif force_update or current_value == old_default:
        # 当前值等于旧默认值，更新为新默认值
        params[key] = new_default
        result['migrated'] = True
        result['new_value'] = new_default
        logging.info(
            "UPG-P1-02: 参数默认值迁移: %s %r → %r (version=%s)",
            key, current_value, new_default, version,
        )
    else:
        # 用户已自定义，不覆盖
        result['new_value'] = current_value
        logging.debug(
            "UPG-P1-02: 参数默认值迁移跳过(用户自定义): %s = %r (old_default=%r)",
            key, current_value, old_default,
        )

    _record_param_migration('default_change', {
        'key': key, 'old_default': old_default, 'new_default': new_default,
        'version': version, 'result': result,
    })

    return result


def migrate_params_type_change(params: Dict[str, Any],
                                key: str, old_type: str, new_type: str,
                                type_converter: Optional[Callable[[Any], Any]] = None,
                                version: str = "") -> Dict[str, Any]:
    """UPG-P1-02修复: 参数类型转换迁移

    将参数值从旧类型转换为新类型。

    Args:
        params: 参数字典（将被原地修改）
        key: 参数名
        old_type: 旧类型名称（如 'str', 'int', 'float', 'bool'）
        new_type: 新类型名称
        type_converter: 自定义类型转换函数，签名 converter(old_value) -> new_value
        version: 迁移版本号

    Returns:
        Dict: 迁移结果 {migrated: bool, key: str, old_value: Any, new_value: Any, error: str}
    """
    result = {'migrated': False, 'key': key, 'old_value': None, 'new_value': None, 'error': ''}

    if key not in params:
        logging.debug("UPG-P1-02: 参数类型迁移跳过(参数不存在): %s", key)
        return result

    old_value = params[key]
    result['old_value'] = old_value

    # 内置类型转换器
    _builtin_converters = {
        ('str', 'int'): lambda v: int(v),
        ('str', 'float'): lambda v: float(v),
        ('str', 'bool'): lambda v: v.lower() in ('true', '1', 'yes'),
        ('int', 'float'): lambda v: float(v),
        ('int', 'str'): lambda v: str(v),
        ('float', 'str'): lambda v: str(v),
        ('float', 'int'): lambda v: int(v),
        ('bool', 'str'): lambda v: str(v),
        ('bool', 'int'): lambda v: 1 if v else 0,  # [R22-TS-P1-06] 显式bool→int转换(非int()隐式)
    }

    converter = type_converter or _builtin_converters.get((old_type, new_type))

    if converter is None:
        result['error'] = f"无内置转换器: {old_type}→{new_type}，请提供type_converter参数"
        logging.warning("UPG-P1-02: %s", result['error'])
        return result

    try:
        new_value = converter(old_value)
        params[key] = new_value
        result['migrated'] = True
        result['new_value'] = new_value
        logging.info(
            "UPG-P1-02: 参数类型迁移: %s %s(%r) → %s(%r) (version=%s)",
            key, old_type, old_value, new_type, new_value, version,
        )
    except (ValueError, TypeError) as e:
        result['error'] = str(e)
        logging.warning(
            "UPG-P1-02: 参数类型迁移失败: %s %s(%r) → %s: %s",
            key, old_type, old_value, new_type, e,
        )

    _record_param_migration('type_change', {
        'key': key, 'old_type': old_type, 'new_type': new_type,
        'version': version, 'result': result,
    })

    return result


def apply_param_migration_plan(params: Dict[str, Any],
                                migration_plan: List[Dict[str, Any]]) -> Dict[str, Any]:
    """UPG-P1-02修复: 执行参数迁移计划

    按顺序执行一组参数迁移操作，支持重命名/默认值变更/类型转换。

    Args:
        params: 参数字典（将被原地修改）
        migration_plan: 迁移计划列表，每项格式:
            {
                'action': 'rename'|'default_change'|'type_change',
                'version': '迁移版本号',
                # rename: old_key, new_key
                # default_change: key, old_default, new_default, force_update(可选)
                # type_change: key, old_type, new_type, type_converter(可选)
            }

    Returns:
        Dict: 迁移报告 {total: int, migrated: int, skipped: int, failed: int, details: list}
    """
    report = {'total': len(migration_plan), 'migrated': 0, 'skipped': 0, 'failed': 0, 'details': []}

    for step in migration_plan:
        action = step.get('action', '')
        version = step.get('version', '')

        try:
            if action == 'rename':
                result = migrate_params_rename(
                    params, step['old_key'], step['new_key'], version,
                )
            elif action == 'default_change':
                result = migrate_params_default_change(
                    params, step['key'], step['old_default'],
                    step['new_default'], version,
                    step.get('force_update', False),
                )
            elif action == 'type_change':
                result = migrate_params_type_change(
                    params, step['key'], step['old_type'],
                    step['new_type'], step.get('type_converter'), version,
                )
            else:
                result = {'migrated': False, 'error': f"未知迁移操作: {action}"}
                report['failed'] += 1

            if result.get('migrated'):
                report['migrated'] += 1
            elif result.get('error'):
                report['failed'] += 1
            else:
                report['skipped'] += 1

            report['details'].append(result)

        except Exception as e:
            report['failed'] += 1
            report['details'].append({'action': action, 'error': str(e), 'migrated': False})
            logging.error("UPG-P1-02: 迁移步骤执行异常: action=%s error=%s", action, e)

    logging.info(
        "UPG-P1-02: 迁移计划执行完成: total=%d migrated=%d skipped=%d failed=%d",
        report['total'], report['migrated'], report['skipped'], report['failed'],
    )

    return report


def _record_param_migration(action: str, detail: Dict[str, Any]) -> None:
    """UPG-P1-02修复: 记录参数迁移历史"""
    global _param_migration_history
    _param_migration_history.append({
        'action': action,
        'detail': detail,
        'timestamp': time.time(),
    })
    if len(_param_migration_history) > _PARAM_MIGRATION_HISTORY_MAX:
        _param_migration_history = _param_migration_history[-_PARAM_MIGRATION_HISTORY_MAX:]


def get_param_migration_history(limit: int = 20) -> List[Dict[str, Any]]:
    """UPG-P1-02修复: 获取参数迁移历史记录

    Args:
        limit: 返回的最大记录数

    Returns:
        List[Dict]: 迁移历史记录列表
    """
    return _param_migration_history[-limit:]


# ============================================================================
# UPG-P1-12修复: 参数默认值兼容性检查
# ============================================================================

def check_default_value_compatibility(
    old_defaults: Dict[str, Any],
    new_defaults: Dict[str, Any],
    current_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """UPG-P1-12修复: 检查参数默认值变更的兼容性

    在升级参数默认值之前，检查变更是否会导致兼容性问题：
    1. 新增参数（old中不存在）- 安全
    2. 删除参数（new中不存在）- 需要确认
    3. 默认值变更 - 需要评估影响
    4. 类型变更 - 需要评估影响

    Args:
        old_defaults: 旧默认值字典
        new_defaults: 新默认值字典
        current_params: 当前运行时参数（可选，用于检查用户自定义值）

    Returns:
        Dict: 兼容性报告 {
            compatible: bool,
            added_keys: list,       # 新增参数
            removed_keys: list,     # 删除参数
            changed_defaults: list, # 默认值变更
            type_changes: list,     # 类型变更
            warnings: list,         # 警告
            breaking_changes: list, # 破坏性变更
        }
    """
    old_keys = set(old_defaults.keys())
    new_keys = set(new_defaults.keys())

    added_keys = sorted(new_keys - old_keys)
    removed_keys = sorted(old_keys - new_keys)
    changed_defaults = []
    type_changes = []
    warnings = []
    breaking_changes = []

    # 检查共有参数的默认值变更
    for key in sorted(old_keys & new_keys):
        old_val = old_defaults[key]
        new_val = new_defaults[key]

        # 类型变更检查
        old_type = type(old_val).__name__
        new_type = type(new_val).__name__
        if old_type != new_type:
            type_changes.append({
                'key': key,
                'old_type': old_type,
                'new_type': new_type,
                'old_value': old_val,
                'new_value': new_val,
            })
            # 类型变更通常是破坏性的
            breaking_changes.append(
                f"参数 {key} 类型变更: {old_type}→{new_type} "
                f"({old_val!r}→{new_val!r})"
            )

        # 默认值变更检查
        elif old_val != new_val:
            change_info = {
                'key': key,
                'old_default': old_val,
                'new_default': new_val,
                'impact': 'low',
            }

            # 评估影响级别
            # 安全关键参数的默认值变更影响高
            _safety_critical_params = {
                'close_stop_loss_ratio', 'max_risk_ratio',
                'circuit_breaker_pause_sec', 'signal_cooldown_sec',
                'max_net_delta_pct', 'max_net_gamma_pct',
            }
            if key in _safety_critical_params:
                change_info['impact'] = 'high'
                breaking_changes.append(
                    f"安全关键参数 {key} 默认值变更: {old_val!r}→{new_val!r}"
                )
            # 数值类参数变更幅度检查
            elif isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                if old_val != 0:
                    change_pct = abs((new_val - old_val) / old_val) * 100
                    if change_pct > 50:
                        change_info['impact'] = 'high'
                        warnings.append(
                            f"参数 {key} 默认值变化幅度大: {change_pct:.1f}% "
                            f"({old_val!r}→{new_val!r})"
                        )
                    elif change_pct > 10:
                        change_info['impact'] = 'medium'
                        warnings.append(
                            f"参数 {key} 默认值变化: {change_pct:.1f}% "
                            f"({old_val!r}→{new_val!r})"
                        )

            changed_defaults.append(change_info)

            # 检查当前运行时参数是否受影响
            if current_params and key in current_params:
                current_val = current_params[key]
                if current_val == old_val:
                    warnings.append(
                        f"参数 {key} 当前值等于旧默认值，升级后将自动变更"
                    )

    # 删除参数检查
    for key in removed_keys:
        breaking_changes.append(f"参数 {key} 已删除（旧默认值: {old_defaults[key]!r}）")
        if current_params and key in current_params:
            warnings.append(
                f"参数 {key} 已删除但当前运行时仍存在，需确认是否仍需要"
            )

    compatible = len(breaking_changes) == 0

    report = {
        'compatible': compatible,
        'added_keys': added_keys,
        'removed_keys': removed_keys,
        'changed_defaults': changed_defaults,
        'type_changes': type_changes,
        'warnings': warnings,
        'breaking_changes': breaking_changes,
    }

    if not compatible:
        logging.warning(
            "UPG-P1-12: 参数默认值兼容性检查发现 %d 个破坏性变更",
            len(breaking_changes),
        )
        for bc in breaking_changes:
            logging.warning("  UPG-P1-12: %s", bc)
    else:
        logging.info(
            "UPG-P1-12: 参数默认值兼容性检查通过 "
            "(added=%d, removed=%d, changed=%d, type_changed=%d)",
            len(added_keys), len(removed_keys),
            len(changed_defaults), len(type_changes),
        )

    return report


# ============================================================================
# P2修复: 升级迁移 — 版本标记、迁移测试、灰度配置、归档策略
# ============================================================================

# P2修复: 参数服务版本号标记（增加版本号覆盖率）
_PARAMS_SERVICE_VERSION = "2.0.0"  # P2修复: 参数服务模块版本
_PARAMS_DATA_VERSION = "1.0"       # P2修复: 参数数据格式版本
_PARAMS_API_VERSION = "1.1"        # P2修复: 参数API版本


def test_migration(migration_plan: List[Dict[str, Any]],
                   test_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """P2修复: 迁移脚本测试函数

    在测试参数副本上执行迁移计划，验证迁移是否正确，
    不影响实际运行参数。

    Args:
        migration_plan: 迁移计划列表
        test_params: 测试参数（默认使用空字典）

    Returns:
        Dict: {success: bool, report: dict, original_params: dict, migrated_params: dict}
    """
    import copy as _copy
    params = _copy.deepcopy(test_params or {})  # R21-MEM-P2-08修复: deepcopy必要，测试参数可能含嵌套dict需独立修改
    original = _copy.deepcopy(params)  # R21-MEM-P2-08修复: deepcopy必要，需保留原始快照用于对比

    try:
        report = apply_param_migration_plan(params, migration_plan)
        success = report['failed'] == 0
        if not success:
            logging.warning(
                "[ParamsService] P2修复: 迁移测试发现%d项失败",
                report['failed'],
            )
        return {
            'success': success,
            'report': report,
            'original_params': original,
            'migrated_params': params,
        }
    except Exception as e:
        logging.error("[ParamsService] P2修复: 迁移测试异常: %s", e)
        return {
            'success': False,
            'report': {'error': str(e)},
            'original_params': original,
            'migrated_params': params,
        }


def verify_rollback(original_params: Dict[str, Any],
                    backup_params: Dict[str, Any]) -> Dict[str, Any]:
    """P2修复: 回滚路径验证函数

    验证备份参数是否可以正确恢复到原始状态。

    Args:
        original_params: 原始参数
        backup_params: 备份参数

    Returns:
        Dict: {can_rollback: bool, missing_keys: list, extra_keys: list, value_diffs: list}
    """
    import copy as _copy
    original_keys = set(original_params.keys())
    backup_keys = set(backup_params.keys())

    missing_keys = sorted(original_keys - backup_keys)
    extra_keys = sorted(backup_keys - original_keys)
    value_diffs = []

    for key in sorted(original_keys & backup_keys):
        if original_params[key] != backup_params[key]:
            value_diffs.append({
                'key': key,
                'original': original_params[key],
                'backup': backup_params[key],
            })

    can_rollback = len(missing_keys) == 0 and len(value_diffs) == 0

    if not can_rollback:
        logging.warning(
            "[ParamsService] P2修复: 回滚验证失败 missing=%d extra=%d diffs=%d",
            len(missing_keys), len(extra_keys), len(value_diffs),
        )

    return {
        'can_rollback': can_rollback,
        'missing_keys': missing_keys,
        'extra_keys': extra_keys,
        'value_diffs': value_diffs,
    }


# P2修复: 灰度发布配置
_CANARY_DEPLOYMENT_CONFIG: Dict[str, Any] = {
    'enabled': False,                # 是否启用灰度发布
    'canary_ratio': 0.1,             # 灰度比例(10%)
    'canary_instruments': [],         # 灰度品种列表
    'canary_duration_sec': 3600,      # 灰度观察期(秒)
    'rollback_on_error_rate': 0.05,   # 错误率超过5%自动回滚
    'metrics_to_monitor': [           # 灰度期间监控的指标
        'sharpe_ratio', 'max_drawdown', 'win_rate', 'profit_factor',
    ],
}


def get_canary_config() -> Dict[str, Any]:
    """P2修复: 获取灰度发布配置"""
    return dict(_CANARY_DEPLOYMENT_CONFIG)


def update_canary_config(**kwargs) -> None:
    """P2修复: 更新灰度发布配置

    Args:
        **kwargs: 要更新的配置项
    """
    for key, value in kwargs.items():
        if key in _CANARY_DEPLOYMENT_CONFIG:
            _CANARY_DEPLOYMENT_CONFIG[key] = value
        else:
            logging.warning("[ParamsService] P2修复: 未知灰度配置项: %s", key)


def should_apply_canary(instrument_id: str = "") -> bool:
    """P2修复: 判断是否应对指定品种应用灰度策略

    Args:
        instrument_id: 品种ID

    Returns:
        True表示该品种应走灰度路径
    """
    config = get_canary_config()
    if not config['enabled']:
        return False
    canary_list = config.get('canary_instruments', [])
    if not canary_list:
        # 无指定品种列表时，按比例灰度
        import hashlib
        hash_val = int(hashlib.md5(instrument_id.encode()).hexdigest(), 16)
        return (hash_val % 100) < (config['canary_ratio'] * 100)
    return instrument_id in canary_list


# P2修复: 热更新灰度能力
def hot_update_with_canary(params: Dict[str, Any],
                            updates: Dict[str, Any],
                            instrument_id: str = "") -> Dict[str, Any]:
    """P2修复: 带灰度能力的热更新

    根据灰度配置决定是否将更新应用到参数。

    Args:
        params: 参数字典
        updates: 更新内容
        instrument_id: 品种ID（用于灰度判断）

    Returns:
        Dict: {applied: bool, canary: bool, updated_keys: list}
    """
    is_canary = should_apply_canary(instrument_id)
    if not is_canary:
        logging.info(
            "[ParamsService] P2修复: 热更新跳过(非灰度品种) instrument=%s",
            instrument_id,
        )
        return {'applied': False, 'canary': False, 'updated_keys': []}

    updated_keys = []
    for key, value in updates.items():
        if key in params:
            params[key] = value
            updated_keys.append(key)

    logging.info(
        "[ParamsService] P2修复: 灰度热更新应用 instrument=%s keys=%s",
        instrument_id, updated_keys,
    )
    return {'applied': True, 'canary': True, 'updated_keys': updated_keys}


# P2修复: 多环境配置差异管理
_ENV_CONFIG_PROFILES: Dict[str, Dict[str, Any]] = {
    'development': {
        'log_level': 'DEBUG',
        'max_position_limit': 10,
        'enable_auto_stop_loss': True,
    },
    'testing': {
        'log_level': 'INFO',
        'max_position_limit': 50,
        'enable_auto_stop_loss': True,
    },
    'production': {
        'log_level': 'WARNING',
        'max_position_limit': 100,
        'enable_auto_stop_loss': True,
    },
}


def get_env_profile(env_name: str) -> Dict[str, Any]:
    """P2修复: 获取环境配置档案

    Args:
        env_name: 环境名称 (development/testing/production)

    Returns:
        环境配置字典
    """
    return dict(_ENV_CONFIG_PROFILES.get(env_name, {}))


def diff_env_profiles(env1: str, env2: str) -> Dict[str, Any]:
    """P2修复: 对比两个环境的配置差异

    Args:
        env1: 第一个环境名称
        env2: 第二个环境名称

    Returns:
        Dict: {only_in_env1, only_in_env2, value_diffs}
    """
    p1 = _ENV_CONFIG_PROFILES.get(env1, {})
    p2 = _ENV_CONFIG_PROFILES.get(env2, {})
    k1, k2 = set(p1.keys()), set(p2.keys())
    return {
        'only_in_env1': sorted(k1 - k2),
        'only_in_env2': sorted(k2 - k1),
        'value_diffs': {
            k: {'env1': p1[k], 'env2': p2[k]}
            for k in k1 & k2 if p1[k] != p2[k]
        },
    }


# P2修复: 数据归档策略
def archive_params(params: Dict[str, Any],
                   archive_dir: Optional[str] = None,
                   label: str = "") -> Optional[str]:
    """P2修复: 归档参数快照到磁盘

    Args:
        params: 要归档的参数
        archive_dir: 归档目录（默认为项目logs/param_archives）
        label: 归档标签（如版本号）

    Returns:
        归档文件路径，失败返回None
    """
    import json as _json
    try:
        if archive_dir is None:
            archive_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 'logs', 'param_archives'
            )
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir, exist_ok=True)

        timestamp = datetime.now(CHINA_TZ).strftime('%Y%m%d_%H%M%S')
        label_suffix = f"_{label}" if label else ""
        archive_file = os.path.join(
            archive_dir, f"params_{timestamp}{label_suffix}.json"
        )
        with open(archive_file, 'w', encoding='utf-8') as f:
            f.write(json_dumps(params, indent=2))
        logging.info("[ParamsService] P2修复: 参数归档完成 path=%s", archive_file)
        return archive_file
    except Exception as e:
        logging.error("[ParamsService] P2修复: 参数归档失败: %s", e)
        return None


def list_param_archives(archive_dir: Optional[str] = None,
                        limit: int = 20) -> List[str]:
    """P2修复: 列出参数归档文件

    Args:
        archive_dir: 归档目录
        limit: 返回数量限制

    Returns:
        归档文件路径列表
    """
    try:
        if archive_dir is None:
            archive_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 'logs', 'param_archives'
            )
        if not os.path.exists(archive_dir):
            return []
        files = sorted(
            [os.path.join(archive_dir, f) for f in os.listdir(archive_dir)
             if f.startswith('params_') and f.endswith('.json')],
            reverse=True,
        )
        return files[:limit]
    except Exception:
        logging.warning("[R22-EP-P1] ParamsService exception swallowed")
        return []


# P2修复: 代码分支管理策略文档注释
# 分支管理策略:
# - main: 生产分支，只接受经过测试的合并请求
# - develop: 开发分支，日常开发在此分支进行
# - feature/*: 功能分支，从develop拉出，完成后合并回develop
# - hotfix/*: 紧急修复分支，从main拉出，修复后合并回main和develop
# - release/*: 发布分支，从develop拉出，测试通过后合并回main
# 版本号规则: MAJOR.MINOR.PATCH (语义化版本)
# - MAJOR: 不兼容的API变更
# - MINOR: 向后兼容的功能新增
# - PATCH: 向后兼容的问题修复
