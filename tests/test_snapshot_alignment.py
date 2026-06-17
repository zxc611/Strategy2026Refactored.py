# MODULE_ID: M2-580
"""快照模块对齐验证脚本"""
import sys
sys.path.insert(0, r"c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo")

import numpy as np
from ali2026v3_trading.strategy_judgment.market_snapshot_collector import (
    MarketSnapshot, DivergenceSpecificState, ShadowAlphaState, EcosystemState,
    SnapshotTrigger, MarketSnapshotCollector, SEVEN_STRATEGY_KEYS, ALL_21_STRATEGY_IDS,
)

PASS = 0
FAIL = 0

def _check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}  {detail}")

print("=" * 60)
print("快照模块对齐验证")
print("=" * 60)

# 1. DivergenceSpecificState
ds = DivergenceSpecificState()
_check("DivergenceSpecificState字段数", len(ds.__dataclass_fields__) == 10,
       f"got {len(ds.__dataclass_fields__)}")
_check("option_moneyness_state默认ATM=2", ds.option_moneyness_state == 2)
_check("div_reversal_signal默认0", ds.div_reversal_signal == 0.0)

# 2. ShadowAlphaState s7字段
sa = ShadowAlphaState()
_check("ShadowAlphaState有s7_master_sharpe", hasattr(sa, 's7_master_sharpe'))
_check("ShadowAlphaState有s7_shadow_a_sharpe", hasattr(sa, 's7_shadow_a_sharpe'))
_check("ShadowAlphaState有s7_shadow_b_sharpe", hasattr(sa, 's7_shadow_b_sharpe'))
_check("ShadowAlphaState字段数=28(7x3+7聚合)", len(sa.__dataclass_fields__) == 28,
       f"got {len(sa.__dataclass_fields__)}")

# 3. EcosystemState divergence字段
es = EcosystemState()
_check("EcosystemState有divergence_state", hasattr(es, 'divergence_state'))
_check("EcosystemState有divergence_capital", hasattr(es, 'divergence_capital'))
_check("EcosystemState有divergence_ev", hasattr(es, 'divergence_ev'))
_check("EcosystemState字段数=24", len(es.__dataclass_fields__) == 24,
       f"got {len(es.__dataclass_fields__)}")

# 4. MarketSnapshot divergence_state
ms = MarketSnapshot(
    snapshot_id='test', timestamp=np.datetime64('now'),
    symbol='IF2606', trigger=SnapshotTrigger.SIGNAL_GENERATED
)
_check("MarketSnapshot有divergence_state", hasattr(ms, 'divergence_state'))
_check("divergence_state类型正确", isinstance(ms.divergence_state, DivergenceSpecificState))

# 5. SnapshotTrigger DIVERGENCE_REVERSAL
_check("SnapshotTrigger有DIVERGENCE_REVERSAL", hasattr(SnapshotTrigger, 'DIVERGENCE_REVERSAL'))
_check("DIVERGENCE_REVERSAL值", SnapshotTrigger.DIVERGENCE_REVERSAL.value == "背离反转信号")

# 6. MarketSnapshotCollector capture_divergence_signal
c = MarketSnapshotCollector('IF2606')
_check("capture_divergence_signal方法存在", hasattr(c, 'capture_divergence_signal'))

# 7. capture()接受divergence_state参数
ds_test = DivergenceSpecificState(
    option_moneyness_state=1, div_future_cross_term=-0.5,
    div_option_premium_coll=-0.3, div_option_near_itm=-0.4,
    div_reversal_signal=-0.45, signal_direction="bearish",
    signal_strength=0.45, three_layer_consistent=True,
)
snap = c.capture(
    timestamp=np.datetime64('now'),
    trigger=SnapshotTrigger.DIVERGENCE_REVERSAL,
    divergence_state=ds_test,
)
_check("capture传入divergence_state", snap.divergence_state.signal_direction == "bearish")
_check("div_reversal_signal传递", snap.divergence_state.div_reversal_signal == -0.45)

# 8. to_flat_dict包含divergence_state字段
flat = snap.to_flat_dict()
_check("to_flat_dict含divergence_state_option_moneyness_state",
       'divergence_state_option_moneyness_state' in flat)
_check("to_flat_dict含divergence_state_div_reversal_signal",
       'divergence_state_div_reversal_signal' in flat)
_check("to_flat_dict含divergence_state_signal_direction",
       'divergence_state_signal_direction' in flat)

# 9. 21策略标识体系
_check("SEVEN_STRATEGY_KEYS=7", len(SEVEN_STRATEGY_KEYS) == 7, f"got {len(SEVEN_STRATEGY_KEYS)}")
_check("SEVEN含divergence", "divergence" in SEVEN_STRATEGY_KEYS)
_check("ALL_21_STRATEGY_IDS=21", len(ALL_21_STRATEGY_IDS) == 21, f"got {len(ALL_21_STRATEGY_IDS)}")
_check("divergence_master存在", "divergence_master" in ALL_21_STRATEGY_IDS)
_check("divergence_shadow_a存在", "divergence_shadow_a" in ALL_21_STRATEGY_IDS)
_check("divergence_shadow_b存在", "divergence_shadow_b" in ALL_21_STRATEGY_IDS)

# 10. 背离信号增强风险评分
snap2 = c.capture(
    timestamp=np.datetime64('now'),
    trigger=SnapshotTrigger.SIGNAL_GENERATED,
    divergence_state=DivergenceSpecificState(
        three_layer_consistent=True, div_reversal_signal=-0.5
    ),
)
_check("背离信号增强风险评分", snap2.risk_dimensions.d1_state_strength < 0.5,
       f"got {snap2.risk_dimensions.d1_state_strength:.4f}")

print("\n" + "=" * 60)
print(f"验证完成: {PASS} PASS, {FAIL} FAIL, 共 {PASS+FAIL} 项")
print("=" * 60)
if FAIL > 0:
    sys.exit(1)
