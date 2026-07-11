# MODULE_ID: M2-433
"""P0-03断言测试: 止盈止损判断运行时行为验证"""
import sys
sys.path.insert(0, '..')

import numpy as np
from ali2026v3_trading.infra.resilience import (
    should_trigger_stop_loss,
    should_trigger_take_profit,
)


def test_take_profit_long_exact():
    assert should_trigger_take_profit(100.0, 100.0, is_long=True) == True


def test_take_profit_long_above():
    assert should_trigger_take_profit(100.01, 100.0, is_long=True) == True


def test_take_profit_long_below():
    assert should_trigger_take_profit(99.99, 100.0, is_long=True) == False


def test_take_profit_short_exact():
    assert should_trigger_take_profit(100.0, 100.0, is_long=False) == True


def test_take_profit_short_below():
    assert should_trigger_take_profit(99.99, 100.0, is_long=False) == True


def test_take_profit_short_above():
    assert should_trigger_take_profit(100.01, 100.0, is_long=False) == False


def test_stop_loss_long_exact():
    assert should_trigger_stop_loss(90.0, 90.0, is_long=True) == True


def test_stop_loss_long_below():
    assert should_trigger_stop_loss(89.99, 90.0, is_long=True) == True


def test_stop_loss_long_above():
    assert should_trigger_stop_loss(90.01, 90.0, is_long=True) == False


def test_stop_loss_short_exact():
    assert should_trigger_stop_loss(110.0, 110.0, is_long=False) == True


def test_stop_loss_short_above():
    assert should_trigger_stop_loss(110.01, 110.0, is_long=False) == True


def test_stop_loss_short_below():
    assert should_trigger_stop_loss(109.99, 110.0, is_long=False) == False


def test_stop_loss_zero_price():
    assert should_trigger_stop_loss(80.0, 0.0, is_long=True) == False
    assert should_trigger_stop_loss(80.0, -1.0, is_long=True) == False


def test_take_profit_zero_price():
    assert should_trigger_take_profit(120.0, 0.0, is_long=True) == False


def test_rtol_boundary():
    price = 100.0
    stop = 100.0 + 100.0 * 1e-9
    assert should_trigger_take_profit(price, stop, is_long=True, rtol=1e-8) == True


def test_production_and_backtest_use_same_function():
    from ali2026v3_trading.infra.resilience import should_trigger_take_profit as prod_fn
    from ali2026v3_trading.infra.resilience import should_trigger_stop_loss as prod_sl
    assert prod_fn is should_trigger_take_profit
    assert prod_sl is should_trigger_stop_loss


if __name__ == '__main__':
    import traceback
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            try:
                fn()
                print(f'  PASS: {name}')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                failed += 1
                print(f'  FAIL: {name}')
                traceback.print_exc()
    print(f'\n{len([n for n in globals() if n.startswith("test_")]) - failed}/{len([n for n in globals() if n.startswith("test_")])} passed')
    sys.exit(failed)