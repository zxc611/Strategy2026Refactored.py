# MODULE_ID: M2-527
"""R3-3: 验证 _preprocess.py 已标记弃用"""
import sys
import pytest
import warnings


def test_preprocess_ticks_emits_deprecation():
    """导入 _preprocess 应发出 DeprecationWarning"""
    # 清除模块缓存以确保重新导入触发warning
    mod_key = 'ali2026v3_trading.param_pool._preprocess'
    cached = sys.modules.pop(mod_key, None)
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import ali2026v3_trading.param_pool._preprocess
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) >= 1, \
                "导入 _preprocess 应发出 DeprecationWarning"
    finally:
        # 恢复缓存
        if cached is not None:
            sys.modules[mod_key] = cached


def test_preprocess_ticks_docstring_mentions_deprecation():
    """_preprocess 模块文档应提及弃用"""
    import ali2026v3_trading.param_pool._preprocess as mod
    assert mod.__doc__ is not None
    assert '弃用' in mod.__doc__ or 'deprecated' in mod.__doc__.lower(), \
        "_preprocess 文档应提及弃用"


def test_preprocess_pipeline_exists():
    """_preprocess.py 应存在作为权威实现"""
    import ali2026v3_trading.param_pool._preprocess as mod
    assert mod is not None
