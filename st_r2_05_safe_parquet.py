# MODULE_ID: M2-520
"""R2-5: 验证 _preprocess.py 使用 safe_dataframe_to_parquet"""
import inspect
import pytest


def test_preprocess_ticks_imports_safe_parquet():
    """preprocess_ticks 应导入 safe_dataframe_to_parquet"""
    import ali2026v3_trading.param_pool._preprocess as mod
    assert hasattr(mod, 'safe_dataframe_to_parquet'), \
        "preprocess_ticks 应导入 safe_dataframe_to_parquet"


def test_preprocess_ticks_no_raw_to_parquet():
    """_preprocess 不应直接调用 .to_parquet()"""
    import ali2026v3_trading.param_pool._preprocess as mod
    # process_symbol was removed during merge; check the module source instead
    source = inspect.getsource(mod)
    # 不应有 .to_parquet( 调用（排除注释行）'
    code_lines = [l for l in source.splitlines() if not l.strip().startswith('#')]
    code_text = '\n'.join(code_lines)
    assert '.to_parquet(' not in code_text, \
        "_preprocess 不应直接调用 .to_parquet()，应使用 safe_dataframe_to_parquet"


def test_safe_dataframe_to_parquet_exists():
    """safe_dataframe_to_parquet 应在 serialization_utils 中可用"""
    from ali2026v3_trading.infra.serialization_utils import safe_dataframe_to_parquet
    assert callable(safe_dataframe_to_parquet)
