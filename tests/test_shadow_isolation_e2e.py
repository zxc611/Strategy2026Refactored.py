#!/usr/bin/env python3
# MODULE_ID: M2-574
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_shadow_facade_import():
    from ali2026v3_trading.strategy.shadow_strategy_facade import ShadowStrategyEngine
    assert ShadowStrategyEngine is not None

def test_shadow_isolation_function():
    from ali2026v3_trading.strategy._shadow_strategy_signal import ShadowStrategySignalService
    assert hasattr(ShadowStrategySignalService, 'verify_shadow_isolation')

def test_shadow_mode_function():
    from ali2026v3_trading.strategy._shadow_strategy_signal import ShadowStrategySignalService
    assert hasattr(ShadowStrategySignalService, 'is_shadow_mode')

def test_shadow_init_whitelist():
    import ali2026v3_trading.strategy as strat
    assert hasattr(strat, '__all__') or hasattr(strat, '__getattr__')

def test_shadow_no_api_pollution():
    import ali2026v3_trading.strategy as strat
    all_symbols = getattr(strat, '__all__', [])
    assert '_shadow_internals' not in all_symbols

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            try:
                fn()
                print(f'[PASS] {name}')
            except Exception as e:
                print(f'[FAIL] {name}: {e}')
