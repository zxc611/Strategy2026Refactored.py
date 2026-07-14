import sys
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_QhZijintianfengPythonX64\pyStrategy\demo')

from ali2026v3_trading.param_pool._param_defaults import (
    PARAM_DEFAULTS, PARAM_GRID_ROUND1, PARAM_GRID_ROUND2,
    PARAM_DEFAULTS_HFT, PARAM_GRID_HFT,
    PARAM_DEFAULTS_BOX_EXTREME, PARAM_GRID_BOX_EXTREME,
    PARAM_DEFAULTS_BOX_SPRING, PARAM_GRID_BOX_SPRING,
    PARAM_DEFAULTS_ARBITRAGE, PARAM_GRID_ARBITRAGE,
    PARAM_DEFAULTS_MARKET_MAKING, PARAM_GRID_MARKET_MAKING,
    PARAM_DEFAULTS_DIVERGENCE, PARAM_GRID_DIVERGENCE,
)
from ali2026v3_trading.strategy.strategy_config_layer import STRATEGY_DEFAULTS, CENTRALIZED_DEFAULTS
from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE

# Key parameters to compare
KEY_PARAMS = [
    'close_take_profit_ratio', 'close_stop_loss_ratio', 'max_risk_ratio',
    'non_other_ratio_threshold', 'state_confirm_bars', 'logic_reversal_threshold',
    'shadow_alpha_threshold', 'capital_route_master_base', 'signal_cooldown_sec',
    'lots_min', 'max_signals_per_window', 'daily_loss_hard_stop_pct',
    'hft_hard_time_stop_ms', 'spring_hard_time_stop_sec',
    'resonance_hard_time_stop_min', 'box_hard_time_stop_min',
]

print("=== Cross-system default values ===")
for p in KEY_PARAMS:
    v1 = PARAM_DEFAULTS.get(p, 'N/A')
    v2 = STRATEGY_DEFAULTS.get(p, 'N/A')
    v3 = CENTRALIZED_DEFAULTS.get(p, 'N/A')
    v4 = DEFAULT_PARAM_TABLE.get(p, 'N/A')
    status = 'OK' if len(set([str(v1), str(v2), str(v3), str(v4)]) - {'N/A'}) == 1 else 'MISMATCH'
    print(f"{p:40s} param_defaults={str(v1):>10s} strategy_defaults={str(v2):>10s} centralized={str(v3):>10s} config_params={str(v4):>10s} [{status}]")

print("\n=== Default-in-grid coverage (Round1/Round2) ===")
all_grids = {**PARAM_GRID_ROUND1, **PARAM_GRID_ROUND2}
for p in PARAM_DEFAULTS:
    if p in all_grids:
        v = PARAM_DEFAULTS[p]
        values = all_grids[p]
        in_grid = v in values if isinstance(values, list) else 'N/A'
        print(f"{p:40s} default={str(v):>10s} in_grid={in_grid}")
    else:
        print(f"{p:40s} default={str(PARAM_DEFAULTS[p]):>10s} [NOT IN ANY GRID]")

print("\n=== HFT/Box/Spring/Arb/MM/Div defaults in grids ===")
mappings = [
    ('HFT', PARAM_DEFAULTS_HFT, PARAM_GRID_HFT),
    ('BOX_EXTREME', PARAM_DEFAULTS_BOX_EXTREME, PARAM_GRID_BOX_EXTREME),
    ('BOX_SPRING', PARAM_DEFAULTS_BOX_SPRING, PARAM_GRID_BOX_SPRING),
    ('ARBITRAGE', PARAM_DEFAULTS_ARBITRAGE, PARAM_GRID_ARBITRAGE),
    ('MARKET_MAKING', PARAM_DEFAULTS_MARKET_MAKING, PARAM_GRID_MARKET_MAKING),
    ('DIVERGENCE', PARAM_DEFAULTS_DIVERGENCE, PARAM_GRID_DIVERGENCE),
]
for name, defaults, grid in mappings:
    print(f"\n{name}:")
    for p, v in defaults.items():
        if p in grid:
            values = grid[p]
            in_grid = v in values if isinstance(values, list) else 'N/A'
            if not in_grid:
                print(f"  {p:40s} default={str(v):>10s} in_grid={in_grid} grid={values}")
        # else: skip keys inherited from other defaults
