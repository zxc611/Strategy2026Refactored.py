"""P2-S1: 11个子系统目录创建+文件移动+根目录重导出"""
import os
import shutil

root = os.path.dirname(os.path.abspath(__file__))

SUBSYSTEMS = {
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
    'execution': ['order_service'],
}

existing_dirs = {'order', 'lifecycle'}

moved_total = 0
for subdir, modules in SUBSYSTEMS.items():
    if subdir in existing_dirs and subdir != 'order' and subdir != 'lifecycle':
        continue
    subdir_path = os.path.join(root, subdir)
    os.makedirs(subdir_path, exist_ok=True)

    init_path = os.path.join(subdir_path, '__init__.py')
    if not os.path.exists(init_path):
        with open(init_path, 'w', encoding='utf-8') as f:
            f.write(f'"""{subdir}/子系统(P2-S1)"""\n')

    moved = []
    for mod in modules:
        src = os.path.join(root, f'{mod}.py')
        if os.path.exists(src) and subdir not in existing_dirs:
            dst = os.path.join(subdir_path, f'{mod}.py')
            shutil.move(src, dst)
            moved.append(mod)

            reexport = os.path.join(root, f'{mod}.py')
            with open(reexport, 'w', encoding='utf-8') as f:
                f.write(f'"""{mod}.py — 重导出模块(P2-S1目录重组后)"""\n')
                f.write(f'from ali2026v3_trading.{subdir}.{mod} import *\n')

    moved_total += len(moved)
    if moved:
        print(f'{subdir}/: moved {len(moved)} files')

print(f'\nTotal moved: {moved_total} files')