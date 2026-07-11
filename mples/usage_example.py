#!/usr/bin/env python3
"""
策略评判系统 — 使用示例

演示如何将评判系统与现有回测框架集成。
"""
from __future__ import annotations

import numpy as np
from datetime import datetime, timedelta

from ali2026v3_trading.strategy_judgment import (
    BacktestIntegrationHooks,
    HookConfig,
    TurningPointMicroscope,
    ResonanceTurningPointMarker,
    MarketSnapshotCollector,
    StrategyStateSnapshot,
    StrategyBehaviorDiagnosis,
    StrategyJudgmentEngine,
    ArbitrageSpecificState,
    MarketMakingSpecificState,
    SnapshotTrigger,
)


def example_minimal_integration():
    """最小侵入集成：3行代码接入回测"""

    config = HookConfig(
        symbol="rb",
        strategy_id="high_freq_rb_v3",
        strategy_type="high_freq",
        output_dir="./策略评判断output",
    )

    hooks = BacktestIntegrationHooks(config)

    for i in range(1000):
        ts = np.datetime64('2025-01-01T09:00:00') + np.timedelta64(i, 'm')
        price = 3800.0 + 50.0 * np.sin(i * 0.05) + np.random.randn() * 5.0
        bid = price - 1.0
        ask = price + 1.0

        hooks.on_tick(
            timestamp=ts,
            last_price=price,
            volume=100,
            turnover=price * 100,
            bid_price1=bid,
            ask_price1=ask,
            open_interest=50000,
            total_volume=100 * (i + 1),
        )

        if i % 200 == 0 and i > 0:
            hooks.on_signal_generated(signal_info={
                "detail": f"动量信号@bar{i}",
                "strength": 0.6,
                "direction": "LONG",
            })

    result = hooks.on_backtest_finish(
        symbol="rb",
        backtest_period="2025-01-01~2025-01-31",
        profitability_metrics=None,
    )

    if result["judgment_report"] is not None:
        jr = result["judgment_report"]
        print(f"评判结论: {jr.verdict.value}")
        print(f"总分: {jr.overall_score:.2f}")
        print(f"可审计: {jr.is_auditable}")
        for rec in jr.recommendations:
            print(f"  → {rec}")

    return result


def example_with_cycle_resonance():
    """带周期共振模块的完整集成"""

    from ali2026v3_trading.param_pool.optimization.cycle_sharpe import (
        CycleResonanceModule, CRParams,
    )

    config = HookConfig(
        symbol="au",
        strategy_id="resonance_au_v2",
        strategy_type="resonance",
    )

    hooks = BacktestIntegrationHooks(config)
    cr_module = CycleResonanceModule(CRParams())

    for i in range(2000):
        ts = np.datetime64('2025-01-01T09:00:00') + np.timedelta64(i, 'm')
        price = 580.0 + 10.0 * np.sin(i * 0.03) + np.random.randn() * 2.0

        cr_output = cr_module.update(
            hmm_state="NORMAL",
            hmm_posterior=(0.2, 0.6, 0.2),
            trend_scores=(0.5 * np.sin(i * 0.01), 0.3 * np.cos(i * 0.02), 0.1),
            trend_directions=(np.sign(np.sin(i * 0.01)), np.sign(np.cos(i * 0.02)), 0.0),
            strength=0.5 + 0.3 * np.sin(i * 0.02),
            imbalance=0.1 * np.sin(i * 0.05),
        )

        hooks.on_tick(
            timestamp=ts,
            last_price=price,
            volume=50,
            turnover=price * 50,
            bid_price1=price - 0.5,
            ask_price1=price + 0.5,
            total_volume=50 * (i + 1),
            cr_output=cr_output,
        )

    result = hooks.on_backtest_finish(symbol="au", backtest_period="2025-01", profitability_metrics=None)

    marker = hooks.resonance_marker
    accuracy = marker.get_prediction_accuracy()
    print(f"共振预测命中率: {accuracy['hit_rate']:.2f}")
    print(f"方向准确率: {accuracy['direction_accuracy']:.2f}")

    return result


def example_from_preprocessed_data():
    """从preprocess_ticks.py输出直接构建"""

    try:
        from ali2026v3_trading.data.data_access import get_data_access
        from ali2026v3_trading.data.db_adapter import connect
        import pandas as pd
    except ImportError:
        print("需要duckdb和pandas")
        return None

    db_path = "preprocessed.duckdb"

    try:
        con = connect(db_path, read_only=True)
        df = con.execute("""
            SELECT * FROM minute_data
            WHERE symbol = 'rb'
            ORDER BY minute
            LIMIT 10000
        """).fetchdf()
        con.close()
    except (OSError, RuntimeError):
        print(f"无法读取{db_path}，使用模拟数据")
        n = 5000
        df = pd.DataFrame({
            'minute': pd.date_range('2025-01-01', periods=n, freq='1min'),
            'open': 3800.0 + 30 * np.sin(np.arange(n) * 0.04) + np.random.randn(n) * 3,
            'high': 3805.0 + 30 * np.sin(np.arange(n) * 0.04) + np.random.randn(n) * 3,
            'low': 3795.0 + 30 * np.sin(np.arange(n) * 0.04) + np.random.randn(n) * 3,
            'close': 3800.0 + 30 * np.sin(np.arange(n) * 0.04) + np.random.randn(n) * 3,
            'volume': np.random.randint(100, 1000, n),
            'vwap': 3800.0 + 30 * np.sin(np.arange(n) * 0.04),
            'bid_ask_spread': np.full(n, 2.0),
            'open_interest': np.random.randint(40000, 60000, n),
            'tick_count': np.random.randint(10, 100, n),
        })

    microscope = TurningPointMicroscope.build_from_preprocessed_df(
        df=df,
        symbol='rb',
        ma_periods=[5, 10, 15, 30, 60],
    )

    stats = microscope.get_statistics()
    print(f"Bar总数: {stats['total_bars']}")
    print(f"极值Bar数: {stats['extreme_bar_count']}")
    print(f"近高点: {stats['near_high_count']}, 近低点: {stats['near_low_count']}")
    print(f"极端高点: {stats['extreme_high_count']}, 极端低点: {stats['extreme_low_count']}")

    extreme_bars = microscope.get_extreme_snapshots()
    print(f"\n前5个极值Bar:")
    for bar in extreme_bars[:5]:
        print(f"  {bar.timestamp}: {bar.extreme_region.value}, "
              f"VWAP={bar.vwap:.2f}, 均线排列={bar.ma_alignment.value}")

    return microscope


def example_arbitrage_judgment():
    """S5套利策略评判示例：微结构套利信号源"""

    config = HookConfig(
        symbol="rb",
        strategy_id="arbitrage_rb_v1",
        strategy_type="arbitrage",
    )

    hooks = BacktestIntegrationHooks(config)

    arbitrage_state = ArbitrageSpecificState(
        deviation_bps=5.2,
        fair_value_shift_bps=1.8,
        confidence=0.75,
        direction=1,
        is_opportunity=True,
        ttl_remaining_seconds=3.5,
        total_opportunities=42,
        total_checks=1000,
    )

    for i in range(500):
        ts = np.datetime64('2025-03-01T09:00:00') + np.timedelta64(i, 'm')
        price = 3800.0 + 30.0 * np.sin(i * 0.02) + np.random.randn() * 3.0

        hooks.on_tick(
            timestamp=ts,
            last_price=price,
            volume=200,
            turnover=price * 200,
            bid_price1=price - 0.5,
            ask_price1=price + 0.5,
            total_volume=200 * (i + 1),
        )

        if i % 50 == 0:
            arbitrage_state.deviation_bps = 5.0 + np.random.randn() * 2.0
            arbitrage_state.confidence = 0.5 + 0.3 * np.sin(i * 0.1)
            hooks.update_state(arbitrage_state=arbitrage_state)
            hooks.on_signal_generated(signal_info={
                "detail": f"套利偏差@bar{i}",
                "deviation_bps": arbitrage_state.deviation_bps,
                "direction": arbitrage_state.direction,
            })

    result = hooks.on_backtest_finish(
        symbol="rb",
        backtest_period="2025-03-01~2025-03-31",
        profitability_metrics=None,
    )

    if result["judgment_report"] is not None:
        jr = result["judgment_report"]
        print(f"[套利] 评判结论: {jr.verdict.value}")
        print(f"[套利] 总分: {jr.overall_score:.2f}")

    return result


def example_market_making_judgment():
    """S6做市策略评判示例：MarketMakerDefenseEngine信号源"""

    config = HookConfig(
        symbol="au",
        strategy_id="market_making_au_v1",
        strategy_type="market_making",
    )

    hooks = BacktestIntegrationHooks(config)

    mm_state = MarketMakingSpecificState(
        current_inventory=0,
        max_inventory=5,
        bid_price=580.0,
        ask_price=580.1,
        spread_bps=17.2,
        fill_count=0,
        ioc_count=0,
        hidden_count=0,
        rebalance_needed=False,
    )

    for i in range(500):
        ts = np.datetime64('2025-04-01T09:00:00') + np.timedelta64(i, 'm')
        price = 580.0 + 5.0 * np.sin(i * 0.03) + np.random.randn() * 0.5

        hooks.on_tick(
            timestamp=ts,
            last_price=price,
            volume=100,
            turnover=price * 100,
            bid_price1=price - 0.05,
            ask_price1=price + 0.05,
            total_volume=100 * (i + 1),
        )

        if i % 20 == 0:
            mm_state.current_inventory = np.random.randint(-3, 4)
            mm_state.fill_count += np.random.randint(1, 5)
            mm_state.ioc_count += np.random.randint(0, 2)
            mm_state.rebalance_needed = abs(mm_state.current_inventory) >= 3
            hooks.update_state(market_making_state=mm_state)

        if i % 100 == 0 and i > 0:
            hooks.on_signal_generated(signal_info={
                "detail": f"做市报价@bar{i}",
                "inventory": mm_state.current_inventory,
                "spread_bps": mm_state.spread_bps,
            })

    result = hooks.on_backtest_finish(
        symbol="au",
        backtest_period="2025-04-01~2025-04-30",
        profitability_metrics=None,
    )

    if result["judgment_report"] is not None:
        jr = result["judgment_report"]
        print(f"[做市] 评判结论: {jr.verdict.value}")
        print(f"[做市] 总分: {jr.overall_score:.2f}")

    return result


def example_standalone_diagnosis():
    """独立使用诊断和评判模块"""

    diagnose = StrategyBehaviorDiagnosis(
        strategy_id="test_strategy",
        strategy_type="high_freq",
    )

    from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshot

    extreme_snaps = []
    for i in range(50):
        snap = MarketSnapshot(
            snapshot_id=f"test_{i}",
            timestamp=np.datetime64('2025-01-01') + np.timedelta64(i, 'D'),
            symbol='rb',
            trigger=SnapshotTrigger.EXTREME_REGION,
            extreme_region='NEAR_HIGH' if i % 2 == 0 else 'NEAR_LOW',
            bar_vwap=3800.0 + 50 * np.sin(i * 0.3),
            total_portfolio_pnl=100.0 * i - 20.0 * (i % 5),
        )
        extreme_snaps.append(snap)

    report = diagnose.diagnose(
        extreme_snapshots=extreme_snaps,
        all_snapshots=extreme_snaps,
        symbol='rb',
        backtest_period='2025-01',
    )

    print(f"行为一致性总分: {report.overall_score.overall_score:.2f}")
    print(f"严重度: {report.overall_score.severity.value}")
    print(f"样本量: {report.extreme_point_count}")
    print(f"置信度: {report.overall_score.confidence:.2f}")
    for dim in report.dimensions:
        print(f"  {dim.dimension}: {dim.score:.2f} ({dim.severity.value}) - {dim.pattern}")

    engine = StrategyJudgmentEngine()
    judgment = engine.judge(
        strategy_id="test_strategy",
        strategy_type="high_freq",
        symbol='rb',
        backtest_period='2025-01',
        diagnosis_report=report,
    )

    print(f"\n评判结论: {judgment.verdict.value}")
    print(f"总分: {judgment.overall_score:.2f}")
    print(f"可审计: {judgment.is_auditable}, 可复现: {judgment.is_reproducible}")
    for rec in judgment.recommendations:
        print(f"  → {rec}")

    return judgment


if __name__ == "__main__":
    print("=" * 60)
    print("示例1: 最小侵入集成")
    print("=" * 60)
    example_minimal_integration()

    print("\n" + "=" * 60)
    print("示例2: 套利策略评判")
    print("=" * 60)
    example_arbitrage_judgment()

    print("\n" + "=" * 60)
    print("示例3: 做市策略评判")
    print("=" * 60)
    example_market_making_judgment()

    print("\n" + "=" * 60)
    print("示例4: 独立诊断与评判")
    print("=" * 60)
    example_standalone_diagnosis()
