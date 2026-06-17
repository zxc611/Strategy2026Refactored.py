# MODULE_ID: M2-295
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import pandas as pd
import numpy as np
from ali2026v3_trading.param_pool.backtest.backtest_state import _check_two_stage_stop, _BacktestPosition

# Test flat slope
pos = _BacktestPosition(
    instrument_id='IF2605', volume=1, open_price=4000.0,
    open_time=pd.Timestamp('2026-05-10 09:30:00'),
    stop_profit_price=4000*1.015, stop_loss_price=4000*0.995,
    open_reason='CORRECT_RESONANCE',
)
pos.stage1_passed = True
pos.profit_history = [0.003] * 10
bar_time = pd.Timestamp('2026-05-10 14:00:00')
price = pos.open_price * 1.003
params = {'stage1_min_minutes': 90.0, 'stage1_profit_threshold': 0.002, 'stage2_slope_window': 10, 'stage2_slope_threshold': 0.0}

# Manually compute what happens
float_pnl_pct = (price - pos.open_price) / pos.open_price
print(f'float_pnl_pct = {float_pnl_pct!r}')
print(f'0.003 == float_pnl_pct: {0.003 == float_pnl_pct}')

# After append
history = [0.003] * 10 + [float_pnl_pct]
print(f'history len = {len(history)}')
print(f'history[-1] = {history[-1]!r}')

# Linear regression
window = history[-10:]
x = np.arange(len(window), dtype=np.float64)
y = np.array(window, dtype=np.float64)
x_mean = x.mean()
y_mean = y.mean()
numerator = np.sum((x - x_mean) * (y - y_mean))
denominator = np.sum((x - x_mean) ** 2)
slope = numerator / denominator
print(f'numerator = {numerator!r}')
print(f'denominator = {denominator!r}')
print(f'slope = {slope!r}')
print(f'slope < 0.0: {slope < 0.0}')

result = _check_two_stage_stop(pos, price, bar_time, params)
print(f'result = {result}')
