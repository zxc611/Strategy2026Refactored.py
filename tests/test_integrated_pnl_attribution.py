# MODULE_ID: M2-369
import sys
import os
import pytest
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestDirectionEnum:
    def test_values(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import Direction
        assert Direction.LONG.value == "LONG"
        assert Direction.SHORT.value == "SHORT"


class TestTimeSegmentEnum:
    def test_values(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import TimeSegment
        assert TimeSegment.OPEN.value == "OPEN"
        assert TimeSegment.MIDDAY.value == "MIDDAY"
        assert TimeSegment.CLOSE.value == "CLOSE"


class TestClassifyDirection:
    def test_positive_qty(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_direction, Direction
        assert classify_direction(1.0) == Direction.LONG

    def test_negative_qty(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_direction, Direction
        assert classify_direction(-1.0) == Direction.SHORT


class TestClassifyTimeSegment:
    def test_open_segment(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_time_segment, TimeSegment
        dt = datetime(2026, 6, 15, 9, 15, tzinfo=timezone(timedelta(hours=8)))
        assert classify_time_segment(dt) == TimeSegment.OPEN

    def test_midday_segment(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_time_segment, TimeSegment
        dt = datetime(2026, 6, 15, 11, 0, tzinfo=timezone(timedelta(hours=8)))
        assert classify_time_segment(dt) == TimeSegment.MIDDAY

    def test_close_segment(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import classify_time_segment, TimeSegment
        dt = datetime(2026, 6, 15, 14, 50, tzinfo=timezone(timedelta(hours=8)))
        assert classify_time_segment(dt) == TimeSegment.CLOSE


class TestTradeRecord:
    def test_creation(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import TradeRecord, Direction, TimeSegment
        trade = TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0, commission=1.0
        )
        assert trade.pnl == 100.0
        assert trade.commission == 1.0
        assert trade.slippage == 0.0

    def test_default_fields(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import TradeRecord, Direction, TimeSegment
        trade = TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0
        )
        assert trade.commission == 0.0
        assert trade.slippage == 0.0
        assert trade.timestamp is None


class TestPnLAttributor:
    def test_init(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor
        attr = PnLAttributor()
        assert attr is not None

    def test_add_trade(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attr = PnLAttributor()
        trade = TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0
        )
        attr.add_trade(trade)

    def test_add_trades_batch(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attr = PnLAttributor()
        trades = [
            TradeRecord(strategy="hft", instrument="au2506", direction=Direction.LONG, time_segment=TimeSegment.OPEN, pnl=100.0),
            TradeRecord(strategy="spring", instrument="rb2510", direction=Direction.SHORT, time_segment=TimeSegment.MIDDAY, pnl=-50.0),
        ]
        attr.add_trades(trades)

    def test_compute_attribution(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attr = PnLAttributor()
        for i in range(5):
            attr.add_trade(TradeRecord(
                strategy="hft", instrument="au2506",
                direction=Direction.LONG, time_segment=TimeSegment.OPEN,
                pnl=100.0 * (i + 1)
            ))
        result = attr.compute_attribution()
        assert hasattr(result, 'by_strategy')
        assert 'hft' in result.by_strategy
        assert hasattr(result, 'total_pnl')

    def test_compute_cost_attribution(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attr = PnLAttributor()
        attr.add_trade(TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0, commission=2.0, slippage=0.5
        ))
        result = attr.compute_cost_attribution()
        assert hasattr(result, 'explicit_total')
        assert hasattr(result, 'implicit_total')

    def test_compute_excess_return(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor
        attr = PnLAttributor()
        excess = attr.compute_excess_return(100.0, 50.0)
        assert isinstance(excess, float)
        assert excess == 50.0

    def test_clear(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import PnLAttributor, TradeRecord, Direction, TimeSegment
        attr = PnLAttributor()
        attr.add_trade(TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0
        ))
        attr.clear()


class TestAttributionEngine:
    def test_init(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import AttributionEngine
        engine = AttributionEngine()
        assert engine is not None

    def test_init_with_history_len(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import AttributionEngine
        engine = AttributionEngine(history_max_len=100)
        assert engine is not None

    def test_add_trade(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import AttributionEngine, TradeRecord, Direction, TimeSegment
        engine = AttributionEngine()
        engine.add_trade(TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0
        ))

    def test_generate_report(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import AttributionEngine, TradeRecord, Direction, TimeSegment
        engine = AttributionEngine()
        for i in range(5):
            engine.add_trade(TradeRecord(
                strategy="hft", instrument="au2506",
                direction=Direction.LONG, time_segment=TimeSegment.OPEN,
                pnl=100.0 * (i + 1)
            ))
        report = engine.generate_report(strategy="hft")
        assert report is not None
        assert hasattr(report, 'pnl_attribution')

    def test_get_history(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import AttributionEngine, TradeRecord, Direction, TimeSegment
        engine = AttributionEngine()
        engine.add_trade(TradeRecord(
            strategy="hft", instrument="au2506",
            direction=Direction.LONG, time_segment=TimeSegment.OPEN,
            pnl=100.0
        ))
        _ = engine.generate_report(strategy="hft")
        history = engine.get_history(strategy="hft")
        assert isinstance(history, list)

    def test_clear(self):
        from ali2026v3_trading.strategy_judgment.pnl_attribution import AttributionEngine
        engine = AttributionEngine()
        engine.clear()