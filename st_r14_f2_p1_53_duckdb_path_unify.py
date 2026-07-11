# MODULE_ID: M2-493
"""P1-53断言测试: DuckDB连接字符串/路径统一——验证运行时行为"""
import os
import sys
import importlib
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'ali2026v3_trading'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_preprocessed_db_single_source():
    """PREPROCESSED_DB 只在 backtest_config 中定义，其余文件从该处导入"""
    import ali2026v3_trading.param_pool.backtest_config as bc
    # backtest_config 中的定义必须是绝对路径
    assert bc.PREPROCESSED_DB.endswith("preprocessed.duckdb"), f"PREPROCESSED_DB={bc.PREPROCESSED_DB}"
    assert os.path.isabs(bc.PREPROCESSED_DB), f"PREPROCESSED_DB应为绝对路径: {bc.PREPROCESSED_DB}"


def test_results_db_single_source():
    """RESULTS_DB 只在 backtest_config 中定义，其余文件从该处导入"""
    import ali2026v3_trading.param_pool.backtest_config as bc
    assert bc.RESULTS_DB.endswith("optuna_results.duckdb"), f"RESULTS_DB={bc.RESULTS_DB}"
    assert os.path.isabs(bc.RESULTS_DB), f"RESULTS_DB应为绝对路径: {bc.RESULTS_DB}"


def test_optuna_files_import_from_backtest_config():
    """optuna_multiobjective_search 从 backtest_config 导入"""
    import ali2026v3_trading.param_pool.backtest_config as bc
    import ali2026v3_trading.param_pool.optuna_multiobjective_search as oms

    assert oms.PREPROCESSED_DB == bc.PREPROCESSED_DB, "optuna_multiobjective_search的PREPROCESSED_DB未从backtest_config导入"
    assert oms.RESULTS_DB == bc.RESULTS_DB, "optuna_multiobjective_search的RESULTS_DB未从backtest_config导入"


def test_config_dataclasses_uses_constant():
    """config_dataclasses 中 strategy.duckdb 路径使用模块级常量"""
    import ali2026v3_trading.config.config_dataclasses as cd
    # _DEFAULT_STRATEGY_DB_PATH 存在
    assert hasattr(cd, '_DEFAULT_STRATEGY_DB_PATH'), "缺少。DEFAULT_STRATEGY_DB_PATH常量"
    db_path = cd._DEFAULT_STRATEGY_DB_PATH
    assert db_path.endswith("strategy.duckdb"), f"路径应以strategy.duckdb结尾: {db_path}"
    # PathConfig.db_path 默认值与常量一致
    pc = cd.PathConfig()
    assert pc.db_path == db_path, f"PathConfig.db_path={pc.db_path} != 常量={db_path}"
    # DatabaseConfig.db_path 默认值与常量一致
    dc = cd.DatabaseConfig()
    assert dc.db_path == db_path, f"DatabaseConfig.db_path={dc.db_path} != 常量={db_path}"
    # DataPathsConfig.duckdb_file 回退值与常量一致
    dpc = cd.DataPathsConfig()
    assert dpc.duckdb_file == db_path, f"DataPathsConfig.duckdb_file={dpc.duckdb_file} != 常量={db_path}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
