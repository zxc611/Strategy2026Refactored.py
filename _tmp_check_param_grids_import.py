import sys
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_QhZijintianfengPythonX64\pyStrategy\demo')
try:
    from ali2026v3_trading.param_pool.backtest.backtest_runner_validation import validate_default_values_in_grids
    print('IMPORT_OK')
    violations = validate_default_values_in_grids()
    print('VIOLATIONS:', violations)
except Exception as e:
    print('IMPORT_FAIL:', type(e).__name__, str(e))
