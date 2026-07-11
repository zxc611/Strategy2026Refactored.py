# MODULE_ID: M2-580
"""快照模块对齐验证"""
import sys
sys.path.insert(0, r"c:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo")

import numpy as np
from ali2026v3_trading.strategy_judgment.market_snapshot_collector import (
    MarketSnapshot, DivergenceSpecificState, ShadowAlphaState, EcosystemState,
    SnapshotTrigger, MarketSnapshotCollector, SEVEN_STRATEGY_KEYS, ALL_21_STRATEGY_IDS,
)


def test_divergence_specific_state_fields():
    ds = DivergenceSpecificState()
    assert len(ds.__dataclass_fields__) == 10, f"got {len(ds.__dataclass_fields__)}"
    assert ds.option_moneyness_state == 2
    assert ds.div_reversal_signal == 0.0


def test_shadow_alpha_state_s7_fields():
    sa = ShadowAlphaState()
    assert hasattr(sa, 's7_master_sharpe')
    assert hasattr(sa, 's7_shadow_a_sharpe')
    assert hasattr(sa, 's7_shadow_b_sharpe')
    assert len(sa.__dataclass_fields__) == 28, f"got {len(sa.__dataclass_fields__)}"


def test_ecosystem_state_divergence_fields():
    es = EcosystemState()
    assert hasattr(es, 'divergence_state')
    assert hasattr(es, 'divergence_capital')
    assert hasattr(es, 'divergence_ev')
    assert len(es.__dataclass_fields__) == 24, f"got {len(es.__dataclass_fields__)}"


def test_market_snapshot_divergence_state():
    ms = MarketSnapshot(
        snapshot_id='test', timestamp=np.datetime64('now'),
        symbol='IF2606', trigger=SnapshotTrigger.SIGNAL_GENERATED
    )
    assert hasattr(ms, 'divergence_state')
    assert isinstance(ms.divergence_state, DivergenceSpecificState)


def test_snapshot_trigger_divergence_reversal():
    assert hasattr(SnapshotTrigger, 'DIVERGENCE_REVERSAL')
    assert SnapshotTrigger.DIVERGENCE_REVERSAL.value == "背离反转信号"


def test_collector_capture_divergence_signal():
    c = MarketSnapshotCollector('IF2606')
    assert hasattr(c, 'capture_divergence_signal')


def test_capture_with_divergence_state():
    c = MarketSnapshotCollector('IF2606')
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
    assert snap.divergence_state.signal_direction == "bearish"
    assert snap.divergence_state.div_reversal_signal == -0.45


def test_to_flat_dict_includes_divergence_state():
    c = MarketSnapshotCollector('IF2606')
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
    flat = snap.to_flat_dict()
    assert 'divergence_state_option_moneyness_state' in flat
    assert 'divergence_state_div_reversal_signal' in flat
    assert 'divergence_state_signal_direction' in flat


def test_strategy_keys_and_ids():
    assert len(SEVEN_STRATEGY_KEYS) == 7, f"got {len(SEVEN_STRATEGY_KEYS)}"
    assert "divergence" in SEVEN_STRATEGY_KEYS
    assert len(ALL_21_STRATEGY_IDS) == 21, f"got {len(ALL_21_STRATEGY_IDS)}"
    assert "divergence_master" in ALL_21_STRATEGY_IDS
    assert "divergence_shadow_a" in ALL_21_STRATEGY_IDS
    assert "divergence_shadow_b" in ALL_21_STRATEGY_IDS


def test_divergence_signal_enhances_risk_score():
    c = MarketSnapshotCollector('IF2606')
    snap2 = c.capture(
        timestamp=np.datetime64('now'),
        trigger=SnapshotTrigger.SIGNAL_GENERATED,
        divergence_state=DivergenceSpecificState(
            three_layer_consistent=True, div_reversal_signal=-0.5
        ),
    )
    assert snap2.risk_dimensions.d1_state_strength < 0.5, f"got {snap2.risk_dimensions.d1_state_strength:.4f}"
