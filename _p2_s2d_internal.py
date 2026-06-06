"""P2-S2d: 修复子系统内部import路径，从ali2026v3_trading.xxx改为ali2026v3_trading.subdir.xxx"""
import os
import re

root = os.path.dirname(os.path.abspath(__file__))

subsystem_map = {
    'order': [f'order_{s}' for s in [
        'base', 'chase_service', 'compliance', 'executor', 'flow_analyzer',
        'flow_bridge', 'market_impact', 'persistence', 'platform_auth',
        'risk_guard', 'service', 'split_models', 'state_manager', 'sync',
        'wal_state_service',
    ]],
    'signal': ['signal_service', 'signal_generator', 'signal_filter_chain',
               'signal_timing_filter', 'signal_history_service'],
    'lifecycle': [f'lifecycle_{s}' for s in [
        'bind', 'callbacks', 'init', 'manager', 'monitor', 'parallel',
        'parallel_ops', 'platform', 'resource', 'state', 'state_machine', 'transition',
    ]],
    'config': ['config_service', 'config_params', 'config_exchange',
               'config_exchange_data', 'config_json_loader', 'config_option_loader',
               'config_resolver', 'config_state_defaults', 'config_sync',
               'config_version_tracker', 'config_facade'],
    'risk': ['risk_service', 'risk_service_core', 'risk_check_service',
             'risk_circuit_breaker', 'risk_compute_service', 'risk_config_provider',
             'risk_position_bridge', 'risk_check_engine'],
    'position': ['position_service', 'position_pnl_service', 'position_persistence',
                 'position_greeks', 'position_utils', 'position_check_service',
                 'position_command_service'],
    'data': ['data_service', 'data_access', 'data_quality_config', 'db_adapter',
             'ds_db_connection', 'ds_realtime_cache', 'ds_schema_manager'],
    'strategy': ['strategy_core_service', 'strategy_2026', 'strategy_ecosystem',
                 'strategy_config', 'strategy_judgment_service', 'strategy_business_layer',
                 'strategy_config_layer', 'strategy_monitoring_layer',
                 'strategy_lifecycle_mixin', 'strategy_tick_handler',
                 'strategy_instrument_mixin', 'strategy_core_service_builder',
                 'box_spring_strategy', 'box_detector'],
    'governance': ['governance_engine', 'greeks_calculator', 'compliance_checker',
                   'regulatory_compliance', 'validation_registry', 'param_table_provider'],
    'infra': ['shared_utils', 'shared_utils_infra', 'shared_utils_contracts',
              'shared_utils_instrument', 'shared_utils_types', 'shared_utils_sql',
              'shared_providers', 'shared_trading_constants',
              'event_bus', 'maintenance_service', 'health_check_api',
              'health_check_aggregator', 'diagnosis_service', 'diagnosis_periodic',
              'diagnosis_probe', 'scheduler_service', 'performance_monitor',
              'callback_registry', 'service_container', 'state_store',
              'concurrent_utils', 'security_config', 'resilience_config',
              'phase_feature_flag', 'metrics_registry'],
}

module_to_subdir = {}
for subdir, modules in subsystem_map.items():
    for mod in modules:
        module_to_subdir[mod] = subdir

fixed = 0
for subdir in subsystem_map:
    subdir_path = os.path.join(root, subdir)
    if not os.path.isdir(subdir_path):
        continue
    for fn in os.listdir(subdir_path):
        if not fn.endswith('.py') or fn == '__init__.py':
            continue
        fp = os.path.join(subdir_path, fn)
        with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        new_content = content
        for mod, sub in module_to_subdir.items():
            old = f'ali2026v3_trading.{mod}'
            new = f'ali2026v3_trading.{sub}.{mod}'
            new_content = new_content.replace(old, new)

        if new_content != content:
            with open(fp, 'w', encoding='utf-8') as f:
                f.write(new_content)
            fixed += 1

print(f'Fixed {fixed} files with internal import path updates')