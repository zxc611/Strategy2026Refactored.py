"""
模块加载状态管理 - NEW-P1-06修复
用于记录核心依赖的降级状态，避免静默吞没ImportError
"""
import logging

_LOGGER = logging.getLogger(__name__)

_MODULE_LOAD_STATUS = {
    'evaluation_parameter_drift_detector': False,
    'evaluation_chicory_eviction': False,
    'evaluation_marquee_threshold': False,
    'evaluation_violation_tracker': False,
    'evaluation_state_density_decay': False,
    'evaluation_activity_weighted_scorer': False,
    'governance_engine_evaluation_classes': False,
    'risk_service_greeks_calculator': False,
    'service_container_query_service': False,
    'service_container_greeks_calculator': False,
    'service_container_risk_service': False,
    'service_container_t_type_service': False,
    'service_container_diagnosis_service': False,
    'service_container_ui_service': False,
    'service_container_strategy_core_service': False,
    'strategy_core_service_live_strategy_selector': False,
    'strategy_ecosystem_live_strategy_selector': False,
    'strategy_lifecycle_mixin_instrument_data_manager': False,
}

def mark_module_loaded(module_key: str) -> None:
    """标记模块成功加载"""
    if module_key in _MODULE_LOAD_STATUS:
        _MODULE_LOAD_STATUS[module_key] = True
    else:
        _LOGGER.warning("[ModuleLoadStatus] 未知的模块键: %s", module_key)

def mark_module_failed(module_key: str, error: Exception) -> None:
    """标记模块加载失败并记录ERROR日志"""
    if module_key in _MODULE_LOAD_STATUS:
        _MODULE_LOAD_STATUS[module_key] = False
        _LOGGER.error("[ModuleLoadStatus] 核心依赖加载失败: %s, 错误: %s", module_key, error)
    else:
        _LOGGER.warning("[ModuleLoadStatus] 未知的模块键: %s, 错误: %s", module_key, error)

def is_module_loaded(module_key: str) -> bool:
    """检查模块是否成功加载"""
    return _MODULE_LOAD_STATUS.get(module_key, False)

def get_all_status() -> dict:
    """获取所有模块加载状态"""
    return dict(_MODULE_LOAD_STATUS)

def get_degraded_modules() -> list:
    """获取所有降级的模块列表"""
    return [k for k, v in _MODULE_LOAD_STATUS.items() if not v]
