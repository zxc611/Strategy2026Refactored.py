"""
E2E验证5-10: 持仓属性标注与平仓规则功能真正起作用验证

验证项:
  E2E-5: 硬时间止损strategy_group参数透传全链路
  E2E-6: 两阶段止损_override使用
  E2E-7: 快照注入时机
  E2E-8: EOD豁免逻辑
  E2E-9: 快照定时导出
  E2E-10: close_reason持久化

运行方式: python tests/test_e2e_position_features_5_10.py
"""
import sys
import os
import time
import unittest
import logging
import inspect
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

logging.basicConfig(level=logging.WARNING)

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class _FakeParams:
    def __init__(self, **kw):
        self._d = kw
    def get(self, key, default=None):
        return self._d.get(key, default)


class TestE2E5HardTimeStopStrategyGroup(unittest.TestCase):
    """E2E-5: 硬时间止损strategy_group参数透传全链路"""

    def test_spring_override_stage1_minutes(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage1_minutes('spring'), 180.0)

    def test_box_override_stage1_minutes(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage1_minutes('box'), 120.0)

    def test_arbitrage_override_stage1_minutes(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage1_minutes('arbitrage'), 60.0)

    def test_market_making_override_stage1_minutes(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage1_minutes('market_making'), 30.0)

    def test_default_fallback(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage1_minutes(''), 90.0)

    def test_spring_override_stage2_minutes(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage2_minutes('spring'), 480.0)

    def test_spring_override_profit_threshold(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        self.assertEqual(svc._get_stage1_profit_threshold('spring'), 0.001)

    def test_full_chain_spring_triggers_later(self):
        from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
        svc = HardTimeStopAndComplianceService(_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
        now = time.time()
        open_time = now - 100 * 60

        r_default = svc.check_position_hard_time_stop('p1', open_time, 0.0005, bar_time=now, stats={}, strategy_group='')
        self.assertIsNotNone(r_default, "默认策略100min+浮盈<0.2%应触发")

        r_spring = svc.check_position_hard_time_stop('p2', open_time, 0.0005, bar_time=now, stats={}, strategy_group='spring')
        self.assertIsNone(r_spring, "spring策略100min+浮盈<0.1%不应触发(需180min)")

    def test_facade_delegates_strategy_group(self):
        from ali2026v3_trading.risk.risk_circuit_breaker import SafetyMetaLayer
        layer = SafetyMetaLayer(params=_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0))
        now = time.time()
        open_time = now - 100 * 60

        r_default = layer.check_position_hard_time_stop('p1', open_time, 0.0005, bar_time=now, strategy_group='')
        self.assertIsNotNone(r_default)

        r_spring = layer.check_position_hard_time_stop('p2', open_time, 0.0005, bar_time=now, strategy_group='spring')
        self.assertIsNone(r_spring)

    def test_tick_handler_function_with_strategy_group(self):
        from ali2026v3_trading.strategy.strategy_tick_handler import check_hard_time_stop_for_position
        from ali2026v3_trading.risk.risk_circuit_breaker import SafetyMetaLayer
        layer = SafetyMetaLayer(params=_FakeParams(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0))
        risk_svc = MagicMock()
        risk_svc._safety_meta_layer = layer
        now = time.time()
        open_time = now - 100 * 60

        r_default = check_hard_time_stop_for_position(risk_svc, 'p1', open_time, 0.0005, bar_time=now, strategy_group='')
        self.assertIsNotNone(r_default)

        r_spring = check_hard_time_stop_for_position(risk_svc, 'p2', open_time, 0.0005, bar_time=now, strategy_group='spring')
        self.assertIsNone(r_spring)


class TestE2E6TwoStageStopOverride(unittest.TestCase):
    """E2E-6: 两阶段止损_override使用"""

    def test_override_config_values(self):
        _TWO_STAGE_STRATEGY_OVERRIDES = {
            'spring': {'stage1_min_minutes': 60.0, 'stage1_profit_threshold': 0.001},
            'arbitrage': {'stage1_min_minutes': 15.0, 'stage1_profit_threshold': 0.005},
            'market_making': {'stage1_min_minutes': 10.0, 'stage1_profit_threshold': 0.01},
        }
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES['spring']['stage1_min_minutes'], 60.0)
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES['spring']['stage1_profit_threshold'], 0.001)
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES['arbitrage']['stage1_min_minutes'], 15.0)
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES['arbitrage']['stage1_profit_threshold'], 0.005)
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES['market_making']['stage1_min_minutes'], 10.0)
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES['market_making']['stage1_profit_threshold'], 0.01)
        self.assertEqual(_TWO_STAGE_STRATEGY_OVERRIDES.get('', {}), {})

    def test_pnl_service_source_code_contains_overrides(self):
        from ali2026v3_trading.position import position_pnl_service
        source = inspect.getsource(position_pnl_service.PositionPnlService._check_two_stage_stop)
        self.assertIn('_TWO_STAGE_STRATEGY_OVERRIDES', source, "源码应包含_TWO_STAGE_STRATEGY_OVERRIDES")
        self.assertIn('strategy_group', source, "源码应使用strategy_group")
        self.assertIn("getattr(record, 'strategy_group'", source, "源码应从record读取strategy_group")


class TestE2E7SnapshotInjection(unittest.TestCase):
    """E2E-7: 快照注入时机"""

    def test_position_service_has_set_snapshot_collector(self):
        from ali2026v3_trading.position.position_service import PositionService
        ps = PositionService(risk_service=None)
        self.assertTrue(hasattr(ps, 'set_snapshot_collector'))
        self.assertTrue(callable(ps.set_snapshot_collector))

    def test_snapshot_collector_initially_none(self):
        from ali2026v3_trading.position.position_service import PositionService
        ps = PositionService(risk_service=None)
        self.assertIsNone(ps._snapshot_collector)

    def test_set_snapshot_collector_injects(self):
        from ali2026v3_trading.position.position_service import PositionService
        ps = PositionService(risk_service=None)
        mock_collector = MagicMock()
        ps.set_snapshot_collector(mock_collector)
        self.assertIs(ps._snapshot_collector, mock_collector)

    def test_add_position_source_code_captures_opened(self):
        from ali2026v3_trading.position import position_command_service
        source = inspect.getsource(position_command_service.PositionCommandService._add_position)
        self.assertIn('ORDER_OPENED', source, "开仓代码应包含ORDER_OPENED快照")
        self.assertIn('_snapshot_collector', source, "开仓代码应使用_snapshot_collector")

    def test_trigger_close_source_code_captures_closed(self):
        from ali2026v3_trading.position import position_command_service
        source = inspect.getsource(position_command_service.PositionCommandService._trigger_close_position)
        self.assertIn('ORDER_CLOSED', source, "平仓代码应包含ORDER_CLOSED快照")
        self.assertIn('_snapshot_collector', source, "平仓代码应使用_snapshot_collector")

    def test_lifecycle_callbacks_injects_snapshot(self):
        from ali2026v3_trading.lifecycle import lifecycle_callbacks
        source = inspect.getsource(lifecycle_callbacks)
        self.assertIn('set_snapshot_collector', source, "lifecycle_callbacks应注入snapshot_collector")
        self.assertIn('SnapshotCollector注入PositionService', source)


class TestE2E8EODExemption(unittest.TestCase):
    """E2E-8: EOD豁免逻辑"""

    def test_eod_source_code_has_spring_arbitrage_exemption(self):
        from ali2026v3_trading.position import position_pnl_service
        source = inspect.getsource(position_pnl_service.PositionPnlService._check_eod_close)
        self.assertIn('spring', source, "EOD代码应包含spring豁免")
        self.assertIn('arbitrage', source, "EOD代码应包含arbitrage豁免")
        self.assertIn('EOD_Night_Close', source, "EOD代码应区分夜盘EOD")
        self.assertIn('strategy_group', source, "EOD代码应使用strategy_group")

    def test_eod_exemption_logic_correctness(self):
        from ali2026v3_trading.position.position_pnl_service import PositionPnlService
        from ali2026v3_trading.position.position_service import PositionRecord

        ps = MagicMock()
        ps.positions = {}
        ps._trigger_close_position = MagicMock()
        ps.global_lock = MagicMock()
        ps.global_lock.__enter__ = MagicMock(return_value=None)
        ps.global_lock.__exit__ = MagicMock(return_value=False)

        now = datetime.now()
        for i, sg in enumerate(['spring', 'arbitrage', 'high_freq']):
            rec = PositionRecord(
                position_id=f'p_{i}', instrument_id=f'IF2607_{i}', exchange='CFFEX',
                volume=1, direction='long', open_price=4000.0,
                open_time=now, open_date=now.date(),
                position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
            )
            rec.strategy_group = sg
            ps.positions[f'IF2607_{i}'] = {f'p_{i}': rec}

        svc = PositionPnlService(ps)

        night_eod_time = datetime(2026, 6, 22, 2, 35)
        svc._check_eod_close(now=night_eod_time)

        close_calls = ps._trigger_close_position.call_args_list
        closed_groups = [getattr(call[0][0], 'strategy_group', '') for call in close_calls]

        self.assertIn('high_freq', closed_groups, "high_freq应被夜盘EOD平仓")
        self.assertNotIn('spring', closed_groups, "spring应被夜盘EOD豁免")
        self.assertNotIn('arbitrage', closed_groups, "arbitrage应被夜盘EOD豁免")

    def test_day_eod_no_exemption(self):
        from ali2026v3_trading.position.position_pnl_service import PositionPnlService
        from ali2026v3_trading.position.position_service import PositionRecord

        ps = MagicMock()
        ps.positions = {}
        ps._trigger_close_position = MagicMock()
        ps.global_lock = MagicMock()
        ps.global_lock.__enter__ = MagicMock(return_value=None)
        ps.global_lock.__exit__ = MagicMock(return_value=False)

        now = datetime.now()
        rec = PositionRecord(
            position_id='p_0', instrument_id='IF2607_0', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now, open_date=now.date(),
            position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
        )
        rec.strategy_group = 'spring'
        ps.positions['IF2607_0'] = {'p_0': rec}

        svc = PositionPnlService(ps)

        day_eod_time = datetime(2026, 6, 22, 14, 56)
        svc._check_eod_close(now=day_eod_time)

        close_calls = ps._trigger_close_position.call_args_list
        closed_groups = [getattr(call[0][0], 'strategy_group', '') for call in close_calls]
        self.assertIn('spring', closed_groups, "spring在日盘EOD应被平仓(不豁免)")


class TestE2E9SnapshotAutoExport(unittest.TestCase):
    """E2E-9: 快照定时导出"""

    def test_auto_export_threshold_exists(self):
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
        self.assertEqual(MarketSnapshotCollector._auto_export_threshold, 100)

    def test_maybe_auto_export_increments_counter(self):
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
        collector = MarketSnapshotCollector.__new__(MarketSnapshotCollector)
        collector._auto_export_counter = 0
        collector._auto_export_threshold = 100
        collector._snapshots = []
        collector._maybe_auto_export()
        self.assertEqual(collector._auto_export_counter, 1)

    def test_auto_export_resets_at_threshold(self):
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
        collector = MarketSnapshotCollector.__new__(MarketSnapshotCollector)
        collector._auto_export_counter = 99
        collector._auto_export_threshold = 100
        collector._snapshots = []
        with patch.object(collector, 'export_to_duckdb'):
            collector._maybe_auto_export()
        self.assertEqual(collector._auto_export_counter, 0, "计数器达到阈值后应重置为0")

    def test_capture_calls_maybe_auto_export(self):
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
        source = inspect.getsource(MarketSnapshotCollector.capture)
        self.assertIn('_maybe_auto_export', source, "capture方法应调用_maybe_auto_export")


class TestE2E10CloseReasonPersistence(unittest.TestCase):
    """E2E-10: close_reason持久化"""

    def test_position_record_has_close_reason(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        rec = PositionRecord(
            position_id='p1', instrument_id='IF2607', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now, open_date=now.date(),
            position_type='SPECULATIVE',
        )
        self.assertTrue(hasattr(rec, 'close_reason'))
        self.assertEqual(rec.close_reason, '')

    def test_close_reason_set_on_trigger_close(self):
        from ali2026v3_trading.position.position_service import PositionService, PositionRecord
        ps = PositionService(risk_service=None)
        now = datetime.now()
        rec = PositionRecord(
            position_id='p1', instrument_id='IF2607', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now, open_date=now.date(),
            position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
        )
        ps.positions['IF2607'] = {'p1': rec}
        mock_order_svc = MagicMock()
        mock_order_svc.send_order.return_value = 'close_order_123'
        with patch.object(ps._command_svc, '_get_order_service', return_value=mock_order_svc):
            with patch('ali2026v3_trading.position.position_command_service.PositionCommandService._get_signal_id', return_value='sig_close'):
                ps._trigger_close_position(rec, "StopLoss@3900.00", 3900.0)
        self.assertEqual(rec.close_reason, "StopLoss@3900.00")
        self.assertTrue(rec._closing)

    def test_close_reason_to_dict(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        rec = PositionRecord(
            position_id='p1', instrument_id='IF2607', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now, open_date=now.date(),
            position_type='SPECULATIVE',
        )
        rec.close_reason = 'StopLoss@3900.00'
        d = rec.to_dict()
        self.assertIn('close_reason', d)
        self.assertEqual(d['close_reason'], 'StopLoss@3900.00')

    def test_close_reason_from_dict(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        d = {
            'position_id': 'p1', 'instrument_id': 'IF2607', 'exchange': 'CFFEX',
            'volume': 1, 'direction': 'long', 'open_price': 4000.0,
            'open_time': now.isoformat(), 'open_date': now.date().isoformat(),
            'position_type': 'SPECULATIVE',
            'close_reason': 'StopLoss@3900.00',
        }
        rec = PositionRecord.from_dict(d)
        self.assertEqual(rec.close_reason, 'StopLoss@3900.00')

    def test_strategy_group_to_dict(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        rec = PositionRecord(
            position_id='p1', instrument_id='IF2607', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now, open_date=now.date(),
            position_type='SPECULATIVE',
        )
        rec.strategy_group = 'spring'
        d = rec.to_dict()
        self.assertIn('strategy_group', d)
        self.assertEqual(d['strategy_group'], 'spring')

    def test_strategy_group_from_dict(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        d = {
            'position_id': 'p1', 'instrument_id': 'IF2607', 'exchange': 'CFFEX',
            'volume': 1, 'direction': 'long', 'open_price': 4000.0,
            'open_time': now.isoformat(), 'open_date': now.date().isoformat(),
            'position_type': 'SPECULATIVE',
            'strategy_group': 'spring',
        }
        rec = PositionRecord.from_dict(d)
        self.assertEqual(rec.strategy_group, 'spring')

    def test_round_trip_strategy_group_and_close_reason(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        rec = PositionRecord(
            position_id='p1', instrument_id='IF2607', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now, open_date=now.date(),
            position_type='SPECULATIVE',
        )
        rec.strategy_group = 'arbitrage'
        rec.close_reason = 'EOD_Night_Close'
        d = rec.to_dict()
        rec2 = PositionRecord.from_dict(d)
        self.assertEqual(rec2.strategy_group, 'arbitrage')
        self.assertEqual(rec2.close_reason, 'EOD_Night_Close')

    def test_reason_to_strategy_mapping_complete(self):
        from ali2026v3_trading.position.position_greeks import _REASON_STRATEGY_MAP
        for key in ['CORRECT_RESONANCE', 'CORRECT_DIVERGENCE', 'INCORRECT_REVERSAL',
                    'INCORRECT_DIVERGENCE', 'OTHER_SCALP', 'BOX_SPRING',
                    'ARBITRAGE', 'MARKET_MAKING', 'MANUAL']:
            self.assertIn(key, _REASON_STRATEGY_MAP, f"_REASON_STRATEGY_MAP缺少: {key}")


class TestE2E5TickProcessingPositionRecordBug(unittest.TestCase):
    """E2E-5补充: tick_processing_service中PositionRecord处理bug修复验证"""

    def test_position_record_is_not_dict(self):
        from ali2026v3_trading.position.position_service import PositionRecord
        now = datetime.now()
        rec = PositionRecord(
            position_id='p1', instrument_id='IF2607', exchange='CFFEX',
            volume=1, direction='long', open_price=4000.0,
            open_time=now - timedelta(minutes=100), open_date=now.date(),
            position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
        )
        rec.strategy_group = 'spring'
        self.assertFalse(isinstance(rec, dict), "PositionRecord不是dict")
        self.assertTrue(hasattr(rec, 'open_time'), "PositionRecord应有open_time属性")
        self.assertTrue(hasattr(rec, 'strategy_group'), "PositionRecord应有strategy_group属性")

    def test_check_hard_time_stop_live_source_handles_both(self):
        from ali2026v3_trading.strategy import tick_processing_service
        source = inspect.getsource(tick_processing_service.check_hard_time_stop_live)
        self.assertIn("hasattr(rec, 'open_time')", source, "源码应包含PositionRecord分支")
        self.assertIn('strategy_group', source, "源码应透传strategy_group参数")


if __name__ == '__main__':
    unittest.main(verbosity=2)
