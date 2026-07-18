# [M1-94] 参数服务门面
# MODULE_ID: M1-015
"""参数服务 - 门面模块 (re-export)"""

from config.config_dataclasses import OutputConfig, ParamObserver, ParamAuditObserver
from config._params_instrument_cache import InstrumentCacheService, InstrumentCacheMixin
from config._params_attribute_matrix import AttributeMatrixService, AttributeMatrixMixin
from config._params_core import ParamsService
from config._params_core import (
    reset_params_service, get_params_service,
    get_params_by_category, get_params_by_risk_level,
    get_params_by_strategy_type, _read_param, get_param_value,
)
from config._params_migration import (
    migrate_params_rename, migrate_params_default_change,
    migrate_params_type_change, apply_param_migration_plan,
    _record_param_migration, get_param_migration_history,
    check_default_value_compatibility,
    _PARAMS_SERVICE_VERSION, _PARAMS_DATA_VERSION, _PARAMS_API_VERSION,
    test_migration, verify_rollback,
)
from config._params_canary_env import (
    get_canary_config, update_canary_config,
    should_apply_canary, hot_update_with_canary,
    get_env_profile, diff_env_profiles,
    archive_params, list_param_archives,
)

__all__ = [
    'OutputConfig', 'ParamObserver', 'ParamAuditObserver',
    'InstrumentCacheMixin', 'AttributeMatrixMixin',
    'ParamsService',
    'reset_params_service', 'get_params_service',
    'get_params_by_category', 'get_params_by_risk_level',
    'get_params_by_strategy_type', 'get_param_value',
    'migrate_params_rename', 'migrate_params_default_change',
    'migrate_params_type_change', 'apply_param_migration_plan',
    'check_default_value_compatibility',
    'test_migration', 'verify_rollback',
    'get_canary_config', 'update_canary_config',
    'should_apply_canary', 'hot_update_with_canary',
    'get_env_profile', 'diff_env_profiles',
    'archive_params', 'list_param_archives',
]
