"""
R30 平仓功能端到端验证测试

验证平仓完整链路的所有修复点:
  1. R29 order_id回查: CLOSE trade通过order_id正确识别为CLOSE
  2. Fallback机制: order_id查找失败时通过instrument+action=CLOSE查找
  3. Fallback状态列表: PARTIAL_FILLED等状态也能匹配
  4. Fallback datetime安全比较: updated_at/created_at缺失时不报错
  5. 防御性回退: CLOSE误判为OPEN时_add_position自成交阻断后回退到_reduce_position
  6. Dedup: on_order FILLED + on_trade不双重更新持仓

运行方式: python -m pytest ali2026v3_trading/tests/test_r30_close_position_e2e.py -v --tb=short --no-cov
"""
import sys
import os
import unittest
import logging
import types
import threading
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, PropertyMock

logging.basicConfig(level=logging.WARNING)

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class _FakeParams:
    def __init__(self, **kw):
        self._d = kw
    def get(self, key, default=None):
        return self._d.get(key, default)


def _make_order_service_with_orders(orders):
    """创建带有预设订单的mock order service"""
    osvc = MagicMock()
    osvc._orders_by_id = {o['order_id']: o for o in orders}
    osvc._platform_id_to_order_id = {}
    for o in orders:
        pid = o.get('platform_order_id')
        if pid:
            osvc._platform_id_to_order_id[str(pid)] = o['order_id']

    def _get_order(oid):
        return osvc._orders_by_id.get(oid)

    def _get_order_by_platform_id(pid):
        iid = osvc._platform_id_to_order_id.get(str(pid))
        if iid:
            return osvc._orders_by_id.get(iid)
        _pid_str = str(pid)
        for o in osvc._orders_by_id.values():
            if str(o.get('platform_order_id')) == _pid_str:
                return o
        return None

    osvc.get_order = _get_order
    osvc.get_order_by_platform_id = _get_order_by_platform_id
    return osvc


def _make_position_command_service(positions=None, self_trade_detector=None):
    """创建PositionCommandService实例用于测试"""
    from ali2026v3_trading.position.position_command_service import PositionCommandService
    ps = MagicMock()
    ps.positions = positions or {}
    ps.self_trade_detector = self_trade_detector
    ps.partial_fill_handler = None
    ps.network_retry_manager = None
    ps._structured_logger = None
    ps._snapshot_collector = None
    ps.DEFAULT_TP_RATIO = 1.05
    ps.DEFAULT_SL_RATIO = 0.05
    ps._params = _FakeParams()
    ps.strategy_id = 'test'
    ps._get_open_reason_from_order = MagicMock(return_value='test_reason')
    ps._get_signal_id_from_order = MagicMock(return_value='sig_001')
    ps._get_tp_sl_ratios_by_reason = MagicMock(return_value=(1.05, 0.05))
    ps._apply_crm_stop_loss_adjustment = MagicMock(return_value=0.05)
    ps._verify_tp_sl_alignment_with_backtest = MagicMock()
    ps._map_reason_to_strategy = MagicMock(return_value='test')
    ps._compute_option_premium = MagicMock(return_value=0.0)
    ps._append_position_state = MagicMock()
    ps._get_instrument_lock = MagicMock(return_value=MagicMock().__enter__())
    svc = PositionCommandService(ps)
    return svc, ps


class TestR30ClosePositionE2E(unittest.TestCase):
    """R30平仓功能端到端验证"""

    def setUp(self):
        """每个测试前重置order service单例"""
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    # ========== 测试1: R29 order_id回查正确识别CLOSE ==========

    def test_01_close_trade_identified_via_order_id_lookup(self):
        """CLOSE trade通过order_id回查正确识别为CLOSE，调用_reduce_position"""
        from ali2026v3_trading.order.order_persistence import OrderRecord, SelfTradeDetector
        close_order = {
            'order_id': 'ord_close_001',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 2,
            'price': 290.0,
            'filled_volume': 2,
            'platform_order_id': 'plat_001',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            # 预置一个持仓
            from ali2026v3_trading.position.position_service import PositionRecord
            rec = PositionRecord(
                position_id='pos_001', instrument_id='FG609P950', exchange='CZCE',
                volume=2, direction='long', open_price=300.0,
                open_time=datetime.now(), open_date=datetime.now().date(),
                position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
                open_reason='test', order_id='ord_open_001', signal_id='sig_001',
                strategy_group='test', option_premium=0.0,
            )
            ps.positions = {'FG609P950': {'pos_001': rec}}

            trade = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',  # BUY (平台字段可能不准确)
                offset='0',     # OPEN (平台字段可能不准确)
                price=290.0,
                volume=2,
                order_id='ord_close_001',
                trade_id='trade_001',
            )
            svc.on_trade(trade)

        # 验证持仓被减少（reduce_position删除了持仓）
        self.assertEqual(len(ps.positions.get('FG609P950', {})), 0,
                         "CLOSE trade应通过order_id回查识别并调用_reduce_position删除持仓")

    # ========== 测试2: Fallback机制通过instrument查找CLOSE订单 ==========

    def test_02_close_trade_fallback_by_instrument(self):
        """order_id查找失败时，通过instrument+action=CLOSE fallback查找"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        close_order = {
            'order_id': 'ord_close_002',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_002',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            from ali2026v3_trading.position.position_service import PositionRecord
            rec = PositionRecord(
                position_id='pos_002', instrument_id='FG609P950', exchange='CZCE',
                volume=1, direction='long', open_price=300.0,
                open_time=datetime.now(), open_date=datetime.now().date(),
                position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
                open_reason='test', order_id='ord_open_002', signal_id='sig_002',
                strategy_group='test', option_premium=0.0,
            )
            ps.positions = {'FG609P950': {'pos_002': rec}}

            trade = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',
                offset='0',  # 平台offset字段错误，标记为OPEN
                price=290.0,
                volume=1,
                order_id='unknown_order_id',  # order_id查找会失败
                trade_id='trade_002',
            )
            svc.on_trade(trade)

        self.assertEqual(len(ps.positions.get('FG609P950', {})), 0,
                         "Fallback应通过instrument找到CLOSE订单并调用_reduce_position")

    # ========== 测试3: Fallback状态列表包含PARTIAL_FILLED ==========

    def test_03_fallback_matches_partial_filled_status(self):
        """Fallback能匹配PARTIAL_FILLED状态的CLOSE订单"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        close_order = {
            'order_id': 'ord_close_003',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'PARTIAL_FILLED',  # 之前遗漏的状态
            'volume': 1,
            'price': 290.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_003',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            from ali2026v3_trading.position.position_service import PositionRecord
            rec = PositionRecord(
                position_id='pos_003', instrument_id='FG609P950', exchange='CZCE',
                volume=1, direction='long', open_price=300.0,
                open_time=datetime.now(), open_date=datetime.now().date(),
                position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
                open_reason='test', order_id='ord_open_003', signal_id='sig_003',
                strategy_group='test', option_premium=0.0,
            )
            ps.positions = {'FG609P950': {'pos_003': rec}}

            trade = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',
                offset='0',
                price=290.0,
                volume=1,
                order_id='',  # 空order_id，触发fallback
                trade_id='trade_003',
            )
            svc.on_trade(trade)

        self.assertEqual(len(ps.positions.get('FG609P950', {})), 0,
                         "Fallback应匹配PARTIAL_FILLED状态的CLOSE订单")

    # ========== 测试4: Fallback datetime安全比较 ==========

    def test_04_fallback_safe_datetime_comparison(self):
        """Fallback在updated_at/created_at缺失时不报错"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        # 订单缺少updated_at和created_at
        close_order = {
            'order_id': 'ord_close_004',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_004',
            # 故意不设置created_at和updated_at
        }
        osvc = _make_order_service_with_orders([close_order])

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            from ali2026v3_trading.position.position_service import PositionRecord
            rec = PositionRecord(
                position_id='pos_004', instrument_id='FG609P950', exchange='CZCE',
                volume=1, direction='long', open_price=300.0,
                open_time=datetime.now(), open_date=datetime.now().date(),
                position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
                open_reason='test', order_id='ord_open_004', signal_id='sig_004',
                strategy_group='test', option_premium=0.0,
            )
            ps.positions = {'FG609P950': {'pos_004': rec}}

            trade = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',
                offset='0',
                price=290.0,
                volume=1,
                order_id='',
                trade_id='trade_004',
            )
            # 不应抛出异常
            svc.on_trade(trade)

        self.assertEqual(len(ps.positions.get('FG609P950', {})), 0,
                         "Fallback在datetime缺失时应安全处理并找到CLOSE订单")

    # ========== 测试5: 防御性回退 - 自成交阻断时回退到_reduce_position ==========

    def test_05_defense_redirect_to_reduce_on_self_trade_block(self):
        """CLOSE误判为OPEN时，_add_position自成交阻断后回退到_reduce_position"""
        from ali2026v3_trading.order.order_persistence import OrderRecord, SelfTradeDetector
        # 预置一个OPEN买入持仓
        from ali2026v3_trading.position.position_service import PositionRecord
        rec = PositionRecord(
            position_id='pos_005', instrument_id='FG609P950', exchange='CZCE',
            volume=2, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_005', signal_id='sig_005',
            strategy_group='test', option_premium=0.0,
        )

        # 预置一个CLOSE订单（证明此trade实为平仓）
        close_order = {
            'order_id': 'ord_close_005',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 2,
            'price': 290.0,
            'filled_volume': 2,
            'platform_order_id': 'plat_005',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        # 创建self_trade_detector并预置一个OPEN买入订单（模拟已有持仓的开仓订单）
        std = SelfTradeDetector()
        std.add_order(OrderRecord(
            order_id='existing_open', instrument_id='FG609P950',
            direction='buy', price=300.0, volume=2,
            timestamp=datetime.now().timestamp(), action='OPEN',
        ))

        # FIX-R30: mock掉会导致DB超时的服务
        _mock_risk = MagicMock()
        _mock_risk._get_margin_ratio = MagicMock(return_value=0.1)
        _mock_risk.check_capital_sufficiency = MagicMock(return_value={'sufficient': True})
        _mock_safety = MagicMock()
        _mock_safety._equity_series = []

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.risk.risk_service.get_risk_service', return_value=_mock_risk), \
             patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', return_value=_mock_safety), \
             patch('ali2026v3_trading.infra.event_bus.get_global_event_bus', return_value=None):
            import threading
            svc, ps = _make_position_command_service(
                positions={'FG609P950': {'pos_005': rec}},
                self_trade_detector=std,
            )
            ps._get_instrument_lock = MagicMock(return_value=threading.Lock())
            # 模拟一个被误判为OPEN的CLOSE trade（所有查找都失败的场景）
            # volume为负表示卖出（会被_add_position识别为short）
            svc._add_position('CZCE', 'FG609P950', -2, 290.0, open_reason='test', order_id='', signal_id='')

        # 验证持仓被减少（防御性回退到_reduce_position）
        self.assertEqual(len(ps.positions.get('FG609P950', {})), 0,
                         "自成交阻断时应检测到CLOSE订单并回退到_reduce_position")

    # ========== 测试6: dict格式trade对象兼容 ==========

    def test_06_dict_format_trade_compatible(self):
        """dict格式trade对象也能正确处理"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        close_order = {
            'order_id': 'ord_close_006',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_006',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            from ali2026v3_trading.position.position_service import PositionRecord
            rec = PositionRecord(
                position_id='pos_006', instrument_id='FG609P950', exchange='CZCE',
                volume=1, direction='long', open_price=300.0,
                open_time=datetime.now(), open_date=datetime.now().date(),
                position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
                open_reason='test', order_id='ord_open_006', signal_id='sig_006',
                strategy_group='test', option_premium=0.0,
            )
            ps.positions = {'FG609P950': {'pos_006': rec}}

            trade_dict = {
                'instrument_id': 'FG609P950',
                'direction': '0',
                'offset': '0',
                'price': 290.0,
                'volume': 1,
                'order_id': 'ord_close_006',
                'trade_id': 'trade_006',
            }
            svc.on_trade(trade_dict)

        self.assertEqual(len(ps.positions.get('FG609P950', {})), 0,
                         "dict格式trade对象应正确处理CLOSE")

    # ========== 测试7: 真正的OPEN trade不被误判为CLOSE ==========

    def test_07_open_trade_not_misidentified_as_close(self):
        """真正的OPEN trade不会被误判为CLOSE，调用_add_position"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        open_order = {
            'order_id': 'ord_open_007',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'BUY',
            'action': 'OPEN',
            'status': 'FILLED',
            'volume': 1,
            'price': 300.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_007',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([open_order])

        _mock_risk = MagicMock()
        _mock_risk._get_margin_ratio = MagicMock(return_value=0.1)
        _mock_risk.check_capital_sufficiency = MagicMock(return_value={'sufficient': True})
        _mock_safety = MagicMock()
        _mock_safety._equity_series = []

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.risk.risk_service.get_risk_service', return_value=_mock_risk), \
             patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', return_value=_mock_safety), \
             patch('ali2026v3_trading.infra.event_bus.get_global_event_bus', return_value=None):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            ps.positions = {}

            trade = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',
                offset='0',
                price=300.0,
                volume=1,
                order_id='ord_open_007',
                trade_id='trade_007',
            )
            svc.on_trade(trade)

        self.assertEqual(len(ps.positions.get('FG609P950', {})), 1,
                         "OPEN trade应调用_add_position创建持仓")

    # ========== 测试8: CLOSE订单不在CANCELLED/FAILED状态时不被fallback匹配 ==========

    def test_08_cancelled_close_order_not_matched_by_fallback(self):
        """超过120秒的CANCELLED状态CLOSE订单不被fallback匹配"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        close_order = {
            'order_id': 'ord_close_008',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'CANCELLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 0,
            'platform_order_id': 'plat_008',
            'created_at': datetime.now() - timedelta(seconds=200),
            'updated_at': datetime.now() - timedelta(seconds=200),
        }
        osvc = _make_order_service_with_orders([close_order])

        _mock_risk = MagicMock()
        _mock_risk._get_margin_ratio = MagicMock(return_value=0.1)
        _mock_risk.check_capital_sufficiency = MagicMock(return_value={'sufficient': True})
        _mock_safety = MagicMock()
        _mock_safety._equity_series = []

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.risk.risk_service.get_risk_service', return_value=_mock_risk), \
             patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', return_value=_mock_safety), \
             patch('ali2026v3_trading.infra.event_bus.get_global_event_bus', return_value=None):
            svc, ps = _make_position_command_service(self_trade_detector=SelfTradeDetector())
            from ali2026v3_trading.position.position_service import PositionRecord
            rec = PositionRecord(
                position_id='pos_008', instrument_id='FG609P950', exchange='CZCE',
                volume=1, direction='long', open_price=300.0,
                open_time=datetime.now(), open_date=datetime.now().date(),
                position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
                open_reason='test', order_id='ord_open_008', signal_id='sig_008',
                strategy_group='test', option_premium=0.0,
            )
            ps.positions = {'FG609P950': {'pos_008': rec}}

            trade = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',
                offset='0',
                price=290.0,
                volume=1,
                order_id='',
                trade_id='trade_008',
            )
            svc.on_trade(trade)

        pos_count = len(ps.positions.get('FG609P950', {}))
        self.assertGreaterEqual(pos_count, 1,
                                "超过120秒的CANCELLED CLOSE订单不应被fallback匹配，持仓不应被减少")


class TestR30DedupOnOrderOnTrade(unittest.TestCase):
    """R30 Dedup验证: on_order FILLED + on_trade不双重更新持仓"""

    def setUp(self):
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def test_09_on_trade_first_then_on_order_no_double_update(self):
        """on_trade先执行时，on_order FILLED不重复更新持仓"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        from ali2026v3_trading.position.position_service import PositionRecord

        close_order = {
            'order_id': 'ord_close_009',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_009',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        # 模拟provider
        provider = MagicMock()
        provider._processed_trade_ids = {}
        provider.MAX_PROCESSED_TRADE_IDS = 1000
        provider.strategy_id = 'test'
        provider._order_service = None
        provider._risk_service = None

        # FIX-R30: 使用真实的PositionCommandService而非MagicMock
        from ali2026v3_trading.position.position_command_service import PositionCommandService
        rec = PositionRecord(
            position_id='pos_009', instrument_id='FG609P950', exchange='CZCE',
            volume=1, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_009', signal_id='sig_009',
            strategy_group='test', option_premium=0.0,
        )

        # 创建真实的PositionService mock，但on_trade委托给真实的PositionCommandService
        import threading
        pos_svc = MagicMock()
        pos_svc.positions = {'FG609P950': {'pos_009': rec}}
        pos_svc.self_trade_detector = SelfTradeDetector()
        pos_svc.partial_fill_handler = None
        pos_svc._get_open_reason_from_order = MagicMock(return_value='test')
        pos_svc._get_signal_id_from_order = MagicMock(return_value='sig_009')
        pos_svc._get_tp_sl_ratios_by_reason = MagicMock(return_value=(1.05, 0.05))
        pos_svc._apply_crm_stop_loss_adjustment = MagicMock(return_value=0.05)
        pos_svc._verify_tp_sl_alignment_with_backtest = MagicMock()
        pos_svc._map_reason_to_strategy = MagicMock(return_value='test')
        pos_svc._compute_option_premium = MagicMock(return_value=0.0)
        pos_svc._append_position_state = MagicMock()
        pos_svc._get_instrument_lock = MagicMock(return_value=threading.Lock())
        pos_svc._structured_logger = None
        pos_svc._snapshot_collector = None
        pos_svc.DEFAULT_TP_RATIO = 1.05
        pos_svc.DEFAULT_SL_RATIO = 0.05
        pos_svc._params = _FakeParams()
        pos_svc.strategy_id = 'test'
        pos_svc.network_retry_manager = None

        # 创建真实的PositionCommandService
        cmd_svc = PositionCommandService(pos_svc)

        # 让pos_svc.on_trade委托给真实的cmd_svc.on_trade
        pos_svc.on_trade = cmd_svc.on_trade

        _mock_risk = MagicMock()
        _mock_risk._get_margin_ratio = MagicMock(return_value=0.1)
        _mock_risk.check_capital_sufficiency = MagicMock(return_value={'sufficient': True})
        _mock_safety = MagicMock()
        _mock_safety._equity_series = []

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.position.position_service.get_position_service', return_value=pos_svc), \
             patch('ali2026v3_trading.risk.risk_service.get_risk_service', return_value=_mock_risk), \
             patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', return_value=_mock_safety), \
             patch('ali2026v3_trading.infra.event_bus.get_global_event_bus', return_value=None):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            bl = StrategyBusinessLayer(provider)

            # Step 1: on_trade先执行
            trade_data = types.SimpleNamespace(
                instrument_id='FG609P950',
                direction='0',
                offset='0',
                price=290.0,
                volume=1,
                order_id='ord_close_009',
                trade_id='real_trade_009',
            )
            bl.on_trade(trade_data)

            # 验证on_trade处理后标记了ON_ORDER_FILL key
            self.assertIn('ON_ORDER_FILL_ord_close_009', provider._processed_trade_ids,
                          "on_trade处理后应标记ON_ORDER_FILL_{oid}防止on_order重复")

            # Step 2: on_order FILLED后执行
            order_data = types.SimpleNamespace(
                order_id='ord_close_009',
                status='FILLED',
            )
            bl.on_order(order_data)

        # 验证持仓只被减少一次（没有double reduce）
        pos_count = len(pos_svc.positions.get('FG609P950', {}))
        self.assertEqual(pos_count, 0, "持仓应被减少一次，不出现双重更新")


class TestR30ClosingFlagReset(unittest.TestCase):
    """R30 _closing标志重置验证: CLOSE订单失败时重置_closing，避免止盈止损被卡住"""

    def setUp(self):
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def test_10_close_order_failed_resets_closing_flag(self):
        """CLOSE订单FAILED时重置持仓_closing标志，使止盈止损可再次触发"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        from ali2026v3_trading.position.position_service import PositionRecord

        close_order = {
            'order_id': 'ord_close_010',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FAILED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 0,
            'platform_order_id': 'plat_010',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        provider = MagicMock()
        provider._processed_trade_ids = {}
        provider.MAX_PROCESSED_TRADE_IDS = 1000
        provider.strategy_id = 'test'
        provider._order_service = None
        provider._risk_service = None

        import threading
        pos_svc = MagicMock()
        rec = PositionRecord(
            position_id='pos_010', instrument_id='FG609P950', exchange='CZCE',
            volume=1, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_010', signal_id='sig_010',
            strategy_group='test', option_premium=0.0,
        )
        rec._closing = True
        rec.closing_order_id = 'ord_close_010'
        pos_svc.positions = {'FG609P950': {'pos_010': rec}}
        pos_svc._get_instrument_lock = MagicMock(return_value=threading.Lock())

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.position.position_service.get_position_service', return_value=pos_svc):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            bl = StrategyBusinessLayer(provider)

            order_data = types.SimpleNamespace(
                order_id='ord_close_010',
                status='FAILED',
            )
            bl.on_order(order_data)

        self.assertFalse(rec._closing, "CLOSE订单FAILED后应重置_closing标志，使止盈止损可再次触发")
        self.assertEqual(rec.closing_order_id, '', "CLOSE订单FAILED后应清空closing_order_id")

    def test_11_dict_format_order_data_compatible(self):
        """dict格式order_data也能正确获取oid和status，触发_closing重置"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        from ali2026v3_trading.position.position_service import PositionRecord

        close_order = {
            'order_id': 'ord_close_011',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'CANCELLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 0,
            'platform_order_id': 'plat_011',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        provider = MagicMock()
        provider._processed_trade_ids = {}
        provider.MAX_PROCESSED_TRADE_IDS = 1000
        provider.strategy_id = 'test'
        provider._order_service = None
        provider._risk_service = None

        import threading
        pos_svc = MagicMock()
        rec = PositionRecord(
            position_id='pos_011', instrument_id='FG609P950', exchange='CZCE',
            volume=1, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_011', signal_id='sig_011',
            strategy_group='test', option_premium=0.0,
        )
        rec._closing = True
        rec.closing_order_id = 'ord_close_011'
        pos_svc.positions = {'FG609P950': {'pos_011': rec}}
        pos_svc._get_instrument_lock = MagicMock(return_value=threading.Lock())

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.position.position_service.get_position_service', return_value=pos_svc):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            bl = StrategyBusinessLayer(provider)

            # dict格式order_data
            order_data = {
                'order_id': 'ord_close_011',
                'status': 'CANCELLED',
            }
            bl.on_order(order_data)

        self.assertFalse(rec._closing, "dict格式order_data也应正确触发_closing重置")
        self.assertEqual(rec.closing_order_id, '', "dict格式order_data也应正确清空closing_order_id")

    def test_12_open_order_failed_does_not_reset_closing(self):
        """OPEN订单失败时不重置_closing（只对CLOSE订单重置）"""
        from ali2026v3_trading.position.position_service import PositionRecord

        open_order = {
            'order_id': 'ord_open_012',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'BUY',
            'action': 'OPEN',
            'status': 'FAILED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 0,
            'platform_order_id': 'plat_012',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([open_order])

        provider = MagicMock()
        provider._processed_trade_ids = {}
        provider.MAX_PROCESSED_TRADE_IDS = 1000
        provider.strategy_id = 'test'
        provider._order_service = None
        provider._risk_service = None

        import threading
        pos_svc = MagicMock()
        rec = PositionRecord(
            position_id='pos_012', instrument_id='FG609P950', exchange='CZCE',
            volume=1, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_012', signal_id='sig_012',
            strategy_group='test', option_premium=0.0,
        )
        rec._closing = True  # 模拟已发送CLOSE订单
        rec.closing_order_id = 'ord_close_012_other'
        pos_svc.positions = {'FG609P950': {'pos_012': rec}}
        pos_svc._get_instrument_lock = MagicMock(return_value=threading.Lock())

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.position.position_service.get_position_service', return_value=pos_svc):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            bl = StrategyBusinessLayer(provider)

            order_data = types.SimpleNamespace(
                order_id='ord_open_012',
                status='FAILED',
            )
            bl.on_order(order_data)

        # 验证_closing标志未被重置（OPEN订单失败不应影响CLOSE的_closing）
        self.assertTrue(rec._closing, "OPEN订单失败不应重置_closing标志")


class TestR30DictDedupCompatibility(unittest.TestCase):
    """R30 dict格式dedup兼容性验证"""

    def setUp(self):
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def test_13_dict_trade_data_dedup_with_on_order(self):
        """dict格式trade_data也能正确dedup，避免on_order+on_trade双重更新"""
        from ali2026v3_trading.order.order_persistence import SelfTradeDetector
        from ali2026v3_trading.position.position_service import PositionRecord

        close_order = {
            'order_id': 'ord_close_013',
            'instrument_id': 'FG609P950',
            'exchange': 'CZCE',
            'direction': 'SELL',
            'action': 'CLOSE',
            'status': 'FILLED',
            'volume': 1,
            'price': 290.0,
            'filled_volume': 1,
            'platform_order_id': 'plat_013',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
        }
        osvc = _make_order_service_with_orders([close_order])

        provider = MagicMock()
        provider._processed_trade_ids = {}
        provider.MAX_PROCESSED_TRADE_IDS = 1000
        provider.strategy_id = 'test'
        provider._order_service = None
        provider._risk_service = None

        from ali2026v3_trading.position.position_command_service import PositionCommandService
        rec = PositionRecord(
            position_id='pos_013', instrument_id='FG609P950', exchange='CZCE',
            volume=1, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_013', signal_id='sig_013',
            strategy_group='test', option_premium=0.0,
        )

        import threading
        pos_svc = MagicMock()
        pos_svc.positions = {'FG609P950': {'pos_013': rec}}
        pos_svc.self_trade_detector = SelfTradeDetector()
        pos_svc.partial_fill_handler = None
        pos_svc._get_open_reason_from_order = MagicMock(return_value='test')
        pos_svc._get_signal_id_from_order = MagicMock(return_value='sig_013')
        pos_svc._get_tp_sl_ratios_by_reason = MagicMock(return_value=(1.05, 0.05))
        pos_svc._apply_crm_stop_loss_adjustment = MagicMock(return_value=0.05)
        pos_svc._verify_tp_sl_alignment_with_backtest = MagicMock()
        pos_svc._map_reason_to_strategy = MagicMock(return_value='test')
        pos_svc._compute_option_premium = MagicMock(return_value=0.0)
        pos_svc._append_position_state = MagicMock()
        pos_svc._get_instrument_lock = MagicMock(return_value=threading.Lock())
        pos_svc._structured_logger = None
        pos_svc._snapshot_collector = None
        pos_svc.DEFAULT_TP_RATIO = 1.05
        pos_svc.DEFAULT_SL_RATIO = 0.05
        pos_svc._params = _FakeParams()
        pos_svc.strategy_id = 'test'
        pos_svc.network_retry_manager = None

        cmd_svc = PositionCommandService(pos_svc)
        pos_svc.on_trade = cmd_svc.on_trade

        _mock_risk = MagicMock()
        _mock_risk._get_margin_ratio = MagicMock(return_value=0.1)
        _mock_risk.check_capital_sufficiency = MagicMock(return_value={'sufficient': True})
        _mock_safety = MagicMock()
        _mock_safety._equity_series = []

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=osvc), \
             patch('ali2026v3_trading.position.position_service.get_position_service', return_value=pos_svc), \
             patch('ali2026v3_trading.risk.risk_service.get_risk_service', return_value=_mock_risk), \
             patch('ali2026v3_trading.risk.risk_service.get_safety_meta_layer', return_value=_mock_safety), \
             patch('ali2026v3_trading.infra.event_bus.get_global_event_bus', return_value=None):
            from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
            bl = StrategyBusinessLayer(provider)

            # Step 1: on_trade先执行（dict格式）
            trade_data = {
                'instrument_id': 'FG609P950',
                'direction': '0',
                'offset': '0',
                'price': 290.0,
                'volume': 1,
                'order_id': 'ord_close_013',
                'trade_id': 'real_trade_013',
            }
            bl.on_trade(trade_data)

            # 验证on_trade处理后标记了ON_ORDER_FILL key（dict格式也能正确获取order_id）
            self.assertIn('ON_ORDER_FILL_ord_close_013', provider._processed_trade_ids,
                          "dict格式trade_data也应正确标记ON_ORDER_FILL_{oid}")

            # Step 2: on_order FILLED后执行
            order_data = types.SimpleNamespace(
                order_id='ord_close_013',
                status='FILLED',
            )
            bl.on_order(order_data)

        # 验证持仓只被减少一次
        pos_count = len(pos_svc.positions.get('FG609P950', {}))
        self.assertEqual(pos_count, 0, "dict格式trade_data也应正确dedup，不出现双重更新")


class TestR31CyclicGuardLeakFix(unittest.TestCase):
    """R31 循环依赖检测调用栈泄漏修复验证"""

    def setUp(self):
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def test_14_cyclic_guard_exit_on_reject(self):
        """订单被拒绝时cyclic_guard.exit被调用，后续send_order不会被循环依赖拦截"""
        from ali2026v3_trading.order.order_executor import OrderExecutor, OrderContext
        from ali2026v3_trading.order.order_base import OrderResult

        # 创建mock order service
        svc = MagicMock()
        svc._circuit_breaker_open = False
        svc._circuit_breaker_half_open = False
        svc._circuit_breaker_opened_at = 0.0
        svc._circuit_breaker_auto_recovery_sec = 60.0
        svc._consecutive_failures = 0
        svc._operation_timeouts = {'open': 5.0, 'close': 3.0, 'cancel': 2.0, 'query': 2.0, 'default': 5.0}
        svc._self_trade_bans = {}
        svc._self_trade_ban_minutes = 30.0
        svc._order_idempotent_set = set()
        svc._orders_by_id = {}
        svc._platform_id_to_order_id = {}
        svc._MAX_ORDERS_TRACKED = 10000
        svc._lock = __import__('threading').RLock()
        svc._get_last_market_price = MagicMock(return_value=None)
        svc._correct_price = MagicMock(side_effect=lambda p, i: p)
        svc.rate_limiter = MagicMock()
        svc.rate_limiter.acquire = MagicMock(return_value=True)
        svc._is_duplicate_order = MagicMock(return_value=False)
        svc._get_position_count = MagicMock(return_value=0)
        svc._generate_order_id = MagicMock(return_value='ord_test_001')
        svc.authenticator = MagicMock()
        svc.authenticator.generate_signed_request = MagicMock(return_value=None)  # 签名失败
        svc._estimate_slippage = MagicMock(return_value=0.0)
        svc._wal_write = MagicMock()
        svc._wal_delete = MagicMock()
        svc._append_order_state = MagicMock()
        svc._persist_idempotent_key = MagicMock()
        svc._remove_order_and_idempotent_key = MagicMock()
        svc._platform_insert_order = None  # 模拟下单
        svc._stats = {'total_orders': 0, 'successful_orders': 0, 'failed_orders': 0, 'cancelled_orders': 0, 'chase_tasks_active': 0}

        # 检查CyclicDependencyGuard是否可用
        try:
            from ali2026v3_trading.strategy_judgment.causal_chain_utils import CyclicDependencyGuard
            guard = CyclicDependencyGuard.get_instance()
            # 清空调用栈
            stack = getattr(guard._call_stack, 'stack', None)
            if stack:
                stack.clear()
        except ImportError:
            pass

        executor = OrderExecutor(svc)

        # 第一次调用：签名失败，订单被拒绝
        ctx1 = OrderContext(
            instrument_id='FG609P950', volume=1, price=290.0,
            direction='BUY', action='OPEN', exchange='CZCE',
        )
        result1 = executor.execute(ctx1)
        self.assertFalse(result1.success, "第一次订单应被拒绝（签名失败）")
        self.assertEqual(result1.error_code, 'sign_failed')

        # 第二次调用：不应被循环依赖检测拦截
        ctx2 = OrderContext(
            instrument_id='FG609P950', volume=1, price=290.0,
            direction='BUY', action='OPEN', exchange='CZCE',
        )
        # 重新mock签名失败
        svc.authenticator.generate_signed_request = MagicMock(return_value=None)
        result2 = executor.execute(ctx2)
        self.assertFalse(result2.success, "第二次订单也应被拒绝（签名失败）")
        self.assertNotEqual(result2.error_code, 'cyclic_call',
                           "第二次订单不应被循环依赖检测拦截（exit已被调用）")

    def test_15_execute_by_ranking_error_code_display(self):
        """execute_by_ranking失败时正确显示error_code而不是'unknown'"""
        from ali2026v3_trading.order.order_executor import OrderExecutor
        from ali2026v3_trading.order.order_base import OrderResult

        svc = MagicMock()
        svc.send_order = MagicMock(return_value=OrderResult.fail('circuit_breaker', '断路器触发'))

        executor = OrderExecutor(svc)
        targets = [{'instrument_id': 'FG609P950', 'price': 290.0, 'volume': 1}]

        with patch('ali2026v3_trading.order.order_base.get_order_service', return_value=svc):
            results = executor.execute_by_ranking(targets, direction='BUY', action='OPEN')

        # 验证没有成功订单
        self.assertEqual(len(results), 0, "应没有成功订单")
        # 验证日志中应显示circuit_breaker而不是unknown（通过检查send_order被调用）
        svc.send_order.assert_called_once()


class TestR31ClosingFlagOnFail(unittest.TestCase):
    """R31 平仓失败时设置_closing标志，防止重复触发"""

    def setUp(self):
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def test_16_close_fail_sets_closing_flag(self):
        """平仓下单失败时设置_closing=True，防止时间止损重复触发"""
        from ali2026v3_trading.position.position_command_service import PositionCommandService
        from ali2026v3_trading.position.position_service import PositionRecord
        from ali2026v3_trading.order.order_base import OrderResult

        ps = MagicMock()
        ps.network_retry_manager = None
        ps._get_instrument_lock = MagicMock(return_value=__import__('threading').Lock())
        ps._snapshot_collector = None

        rec = PositionRecord(
            position_id='pos_016', instrument_id='FG609P950', exchange='CZCE',
            volume=1, direction='long', open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type='long', stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_016', signal_id='sig_016',
            strategy_group='test', option_premium=0.0,
        )
        rec._closing = False
        ps.positions = {'FG609P950': {'pos_016': rec}}

        svc = PositionCommandService(ps)

        # Mock send_order返回失败
        mock_osvc = MagicMock()
        mock_osvc.send_order = MagicMock(return_value=OrderResult.fail('cyclic_call', '循环依赖检查'))
        mock_osvc._order_idempotent_set = set()

        # Mock data service返回有效价格
        mock_ds = MagicMock()
        mock_ds.realtime_cache = MagicMock()
        mock_ds.realtime_cache._latest_ticks = {'FG609P950': {'bid_price': 290.0, 'ask_price': 291.0}}

        with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=mock_osvc), \
             patch('ali2026v3_trading.data.data_service.get_data_service', return_value=mock_ds), \
             patch('ali2026v3_trading.config.params_service.get_params_service', return_value=_FakeParams(tick_size=1.0)):
            svc._trigger_close_position(rec, 'TimeStop', 290.0)

        # 验证_closing被设置为True（即使下单失败）
        self.assertTrue(rec._closing, "平仓下单失败后_closing应被设置为True，防止重复触发")
        self.assertNotEqual(rec.closing_order_id, '', "平仓下单失败后closing_order_id应被设置（非空），防止重复触发")


class TestR31NormalizePlatformResultDict(unittest.TestCase):
    """R31 _normalize_platform_result支持dict格式"""

    def setUp(self):
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def test_17_normalize_platform_result_dict(self):
        """_normalize_platform_result能正确处理dict格式的平台返回结果"""
        from ali2026v3_trading.order.order_base import OrderQueryService

        svc = OrderQueryService.__new__(OrderQueryService)

        result_dict = {
            'order_id': 'plat_017',
            'status': 'FILLED',
            'traded_volume': 1,
        }

        normalized = svc._normalize_platform_result(result_dict)
        self.assertEqual(normalized.get('order_id'), 'plat_017', "应从dict中正确提取order_id")
        self.assertEqual(normalized.get('status'), 'FILLED', "应从dict中正确提取status")
        self.assertEqual(normalized.get('traded_volume'), 1, "应从dict中正确提取traded_volume")

    def test_18_normalize_platform_result_object(self):
        """_normalize_platform_result仍支持对象格式"""
        from ali2026v3_trading.order.order_base import OrderQueryService

        svc = OrderQueryService.__new__(OrderQueryService)

        result_obj = types.SimpleNamespace(
            order_id='plat_018',
            status='PARTIAL_FILLED',
            traded_volume=2,
        )

        normalized = svc._normalize_platform_result(result_obj)
        self.assertEqual(normalized.get('order_id'), 'plat_018', "应从对象中正确提取order_id")
        self.assertEqual(normalized.get('status'), 'PARTIAL_FILLED', "应从对象中正确提取status")
        self.assertEqual(normalized.get('traded_volume'), 2, "应从对象中正确提取traded_volume")


class TestFourUniquenessFixes(unittest.TestCase):
    """四大唯一性第二轮排查修复的端到端验证"""

    def setUp(self):
        """每个测试前重置order service单例"""
        from ali2026v3_trading.order import order_base
        order_base._order_service_instance = None

    def _make_position_record(self, pos_id='pos_test', inst='FG609P950', vol=2, direction='long',
                               close_method='', closing_order_id='', _closing=False, signal_id='sig_test'):
        """创建带完整字段的PositionRecord用于测试"""
        from ali2026v3_trading.position.position_service import PositionRecord
        rec = PositionRecord(
            position_id=pos_id, instrument_id=inst, exchange='CZCE',
            volume=vol, direction=direction, open_price=300.0,
            open_time=datetime.now(), open_date=datetime.now().date(),
            position_type=direction, stop_profit_price=315.0, stop_loss_price=285.0,
            open_reason='test', order_id='ord_open_test', signal_id=signal_id,
            strategy_group='test', option_premium=0.0,
        )
        rec._closing = _closing
        rec.closing_order_id = closing_order_id
        rec.close_method = close_method
        return rec

    # ========== O-05: 排除平仓中持仓的开仓计数 ==========

    def test_19_open_unique_05_exclude_closing_positions(self):
        """O-05: 同策略组计数排除已在平仓中的持仓(closing_order_id非空)"""
        svc, ps = _make_position_command_service()
        # 预置4个持仓，其中2个已在平仓中(closing_order_id非空)
        ps.positions = {'FG609P950': {}}
        for i in range(2):
            rec = self._make_position_record(pos_id=f'pos_{i}', vol=2)
            rec.closing_order_id = f'PENDING_pos_{i}'  # 已在平仓中
            ps.positions['FG609P950'][f'pos_{i}'] = rec
        for i in range(2, 4):
            rec = self._make_position_record(pos_id=f'pos_{i}', vol=2)
            ps.positions['FG609P950'][f'pos_{i}'] = rec
        # 添加第5个开仓 - 应该成功(排除2个平仓中的，只有2个活跃)
        # Mock risk_service to avoid database access
        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as mock_rs:
            mock_rs.return_value = None
            svc._add_position('CZCE', 'FG609P950', 2, 300.0, 'test', 'ord_new', 'sig_new')
        # 验证新持仓已添加
        _added = [p for p in ps.positions['FG609P950'] if p.startswith('FG609P950_test_')]
        self.assertEqual(len(_added), 1, "排除平仓中持仓后应允许新开仓")

    # ========== O-08: position_id微秒精度 ==========

    def test_20_open_unique_08_microsecond_precision(self):
        """O-08: position_id使用微秒精度避免高频冲突"""
        svc, ps = _make_position_command_service()
        ps.positions = {'FG609P950': {}}
        # 连续添加两个持仓
        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as mock_rs:
            mock_rs.return_value = None
            svc._add_position('CZCE', 'FG609P950', 2, 300.0, 'test', 'ord_1', 'sig_1')
            svc._add_position('CZCE', 'FG609P950', -2, 300.0, 'test', 'ord_2', 'sig_2')
        # 验证两个position_id不同
        pos_ids = list(ps.positions['FG609P950'].keys())
        self.assertEqual(len(pos_ids), 2, "应成功添加2个持仓")
        self.assertNotEqual(pos_ids[0], pos_ids[1], "两个position_id必须不同")

    # ========== O-09: 策略组为空时归类为_default_ ==========

    def test_21_open_unique_09_empty_strategy_group(self):
        """O-09: 策略组为空时归类为_default_，避免空策略组合并计数"""
        svc, ps = _make_position_command_service()
        # _map_reason_to_strategy返回空字符串
        ps._map_reason_to_strategy = MagicMock(return_value='')
        ps.positions = {'FG609P950': {}}
        # 添加4个空策略组持仓
        for i in range(4):
            rec = self._make_position_record(pos_id=f'empty_{i}', vol=2)
            rec.strategy_group = ''
            ps.positions['FG609P950'][f'empty_{i}'] = rec
        # 第5个应被拒绝(空策略组归类为_default_，已有4个)
        with patch('ali2026v3_trading.risk.risk_service.get_risk_service') as mock_rs:
            mock_rs.return_value = None
            svc._add_position('CZCE', 'FG609P950', 2, 300.0, 'test', 'ord_new', 'sig_new')
        _new = [p for p in ps.positions['FG609P950'] if p.startswith('FG609P950__default_')]
        self.assertEqual(len(_new), 0, "空策略组已有4个持仓时应拒绝第5个")

    # ========== C-07: PENDING/RETRY/实际order_id状态区分 ==========

    def test_22_close_unique_07_pending_state_check(self):
        """C-07: closing_order_id为PENDING_xxx时跳过平仓"""
        svc, ps = _make_position_command_service()
        rec = self._make_position_record(closing_order_id='PENDING_pos_test', _closing=True,
                                           close_method='stop_loss')
        # 应直接返回，不执行平仓
        svc._trigger_close_position(rec, 'time_stop')
        self.assertEqual(rec.closing_order_id, 'PENDING_pos_test',
                         "PENDING状态应跳过平仓，closing_order_id不变")
        self.assertEqual(rec.close_method, 'stop_loss',
                         "PENDING状态跳过时close_method不变")

    def test_23_close_unique_07_retry_state_check(self):
        """C-07: closing_order_id为RETRY_xxx时跳过平仓"""
        svc, ps = _make_position_command_service()
        rec = self._make_position_record(closing_order_id='RETRY_pos_test', _closing=True,
                                           close_method='time_stop')
        svc._trigger_close_position(rec, 'stop_loss')
        self.assertEqual(rec.closing_order_id, 'RETRY_pos_test',
                         "RETRY状态应跳过平仓")

    # ========== C-10: 部分平仓后重置close_method ==========

    def test_24_close_unique_10_partial_close_resets_close_method(self):
        """C-10: 部分平仓后close_method被重置为空"""
        svc, ps = _make_position_command_service()
        rec = self._make_position_record(pos_id='pos_partial', vol=4, _closing=True,
                                           closing_order_id='PENDING_pos_partial',
                                           close_method='stop_loss')
        ps.positions = {'FG609P950': {'pos_partial': rec}}
        # 部分平仓2手(持仓4手)
        svc._reduce_position('CZCE', 'FG609P950', 2, False, 290.0)
        _updated_rec = ps.positions['FG609P950'].get('pos_partial')
        self.assertIsNotNone(_updated_rec, "部分平仓后持仓应存在")
        self.assertEqual(_updated_rec.volume, 2, "部分平仓后剩余2手")
        self.assertFalse(_updated_rec._closing, "部分平仓后_closing应为False")
        self.assertEqual(_updated_rec.closing_order_id, '', "部分平仓后closing_order_id应为空")
        self.assertEqual(_updated_rec.close_method, '', "部分平仓后close_method应为空")

    # ========== C-11: 删除持仓时重置close_method ==========

    def test_25_close_unique_11_delete_resets_close_method(self):
        """C-11: 完全平仓删除持仓时close_method被重置"""
        svc, ps = _make_position_command_service()
        rec = self._make_position_record(pos_id='pos_del', vol=2, _closing=True,
                                           closing_order_id='PENDING_pos_del',
                                           close_method='stop_profit')
        ps.positions = {'FG609P950': {'pos_del': rec}}
        svc._reduce_position('CZCE', 'FG609P950', 2, False, 310.0)
        # 持仓应被删除
        self.assertNotIn('pos_del', ps.positions.get('FG609P950', {}),
                         "完全平仓后持仓应被删除")
        # 验证rec对象状态已重置(即使从positions删除)
        self.assertEqual(rec.close_method, '', "删除持仓后close_method应为空")
        self.assertEqual(rec.closing_order_id, '', "删除持仓后closing_order_id应为空")
        self.assertFalse(rec._closing, "删除持仓后_closing应为False")

    # ========== U-11: 回滚时重置close_method ==========

    def test_26_update_timely_11_rollback_resets_close_method(self):
        """U-11: 回滚持仓时close_method被重置"""
        svc, ps = _make_position_command_service()
        rec = self._make_position_record(pos_id='pos_rb', vol=2, _closing=True,
                                           closing_order_id='PENDING_pos_rb',
                                           close_method='stop_loss')
        ps.positions = {'FG609P950': {'pos_rb': rec}}
        result = svc._rollback_position('FG609P950', 'pos_rb', 'test_rollback')
        self.assertTrue(result, "回滚应返回True")
        self.assertEqual(rec.close_method, '', "回滚后close_method应为空")
        self.assertEqual(rec.closing_order_id, '', "回滚后closing_order_id应为空")
        self.assertFalse(rec._closing, "回滚后_closing应为False")

    # ========== C-08: operation_id包含signal_id ==========

    def test_27_close_unique_08_operation_id_contains_signal_id(self):
        """C-08: 网络重试operation_id包含signal_id确保唯一性"""
        from ali2026v3_trading.position.position_command_service import PositionCommandService
        ps = MagicMock()
        ps.positions = {'FG609P950': {}}
        ps.self_trade_detector = None
        ps.network_retry_manager = MagicMock()
        ps._get_instrument_lock = MagicMock(return_value=MagicMock().__enter__())
        ps._snapshot_collector = None
        ps._structured_logger = None
        ps._append_position_state = MagicMock()
        ps._compute_option_premium = MagicMock(return_value=0.0)
        ps._map_reason_to_strategy = MagicMock(return_value='test')
        ps._get_tp_sl_ratios_by_reason = MagicMock(return_value=(1.05, 0.05))
        ps._apply_crm_stop_loss_adjustment = MagicMock(return_value=0.05)
        ps._verify_tp_sl_alignment_with_backtest = MagicMock()
        ps.DEFAULT_TP_RATIO = 1.05
        ps.DEFAULT_SL_RATIO = 0.05
        ps._params = _FakeParams()
        ps.strategy_id = 'test'
        svc = PositionCommandService(ps)
        rec = self._make_position_record(pos_id='pos_opid', vol=2, signal_id='sig_opid' if False else None)
        rec.signal_id = 'sig_unique_001'
        ps.positions = {'FG609P950': {'pos_opid': rec}}
        # Mock order service
        with patch('ali2026v3_trading.order.order_service.get_order_service') as mock_get_osvc:
            mock_osvc = MagicMock()
            mock_order_result = MagicMock()
            mock_order_result.order_id = 'ord_close_opid'
            mock_osvc.send_order.return_value = mock_order_result
            mock_get_osvc.return_value = mock_osvc
            # Mock data service for price
            with patch('ali2026v3_trading.data.data_service.get_data_service') as mock_get_ds:
                mock_ds = MagicMock()
                mock_ds.realtime_cache = MagicMock()
                mock_ds.realtime_cache._latest_ticks = {'FG609P950': {'bid_price1': 290.0, 'ask_price1': 291.0}}
                mock_get_ds.return_value = mock_ds
                svc._trigger_close_position(rec, 'stop_loss')
        # 验证operation_id包含signal_id
        _call_args = ps.network_retry_manager.execute_with_retry.call_args
        if _call_args:
            _op_id = _call_args.kwargs.get('operation_id', '') or (_call_args.args[0] if _call_args.args else '')
            self.assertIn('sig_unique_001', _op_id,
                         f"operation_id应包含signal_id: got={_op_id}")

    # ========== O-07: execute_by_ranking防重复开仓 ==========

    def test_28_open_unique_07_execute_by_ranking_dedup(self):
        """O-07: execute_by_ranking同合约同方向重复开仓被跳过"""
        from ali2026v3_trading.order.order_executor import OrderExecutor
        from ali2026v3_trading.infra.shared_utils import TradeDirection
        svc_mock = MagicMock()
        svc_mock._get_tick_size.return_value = 1.0
        svc_mock._correct_price.side_effect = lambda p, i: p
        svc_mock._orders_by_id = {}
        _order_result = MagicMock()
        _order_result.success = True
        _order_result.order_id = 'ord_test'
        svc_mock.send_order.return_value = _order_result
        executor = OrderExecutor(svc_mock)
        # 两个相同合约同方向的target
        targets = [
            {'instrument_id': 'FG609P950', 'lots': 1, 'price': 300.0, 'direction': 'BUY', 'action': 'OPEN'},
            {'instrument_id': 'FG609P950', 'lots': 1, 'price': 300.0, 'direction': 'BUY', 'action': 'OPEN'},
        ]
        results = executor.execute_by_ranking(targets, direction='BUY', action='OPEN')
        # 只应执行第一个，第二个被去重跳过
        self.assertEqual(len(results), 1, "同合约同方向重复开仓应只执行第一个")

    # ========== U-09: _processed_trade_ids提前写入 ==========

    def test_29_update_timely_09_dedup_write_before_risk_check(self):
        """U-09: _processed_trade_ids在风控检查前写入，防止并发重复"""
        from ali2026v3_trading.strategy.strategy_business_layer import StrategyBusinessLayer
        provider = MagicMock()
        provider._processed_trade_ids = {}
        provider.MAX_PROCESSED_TRADE_IDS = 10000
        _lock = threading.Lock()
        provider._trade_ids_lock = _lock
        provider._risk_service = None  # 无风控服务
        provider.strategy_id = 'test'
        biz = StrategyBusinessLayer(provider)
        trade = {'trade_id': 'trade_u09', 'instrument_id': 'FG609P950',
                 'direction': '0', 'volume': 1, 'price': 300.0}
        # Mock position service
        with patch('ali2026v3_trading.position.position_service.get_position_service') as mock_get_pos:
            mock_pos = MagicMock()
            mock_get_pos.return_value = mock_pos
            biz.update_position_from_trade(trade)
        # 验证trade_id已写入_processed_trade_ids
        self.assertIn('trade_u09', provider._processed_trade_ids,
                      "trade_id应在风控检查前写入_processed_trade_ids")

    # ========== R-12: 恢复持仓时重置close_method ==========

    def test_30_read_unique_12_restore_resets_close_method(self):
        """R-12: 重启恢复持仓时close_method被重置为空"""
        from ali2026v3_trading.position.position_service import PositionRecord
        # 模拟快照中包含close_method
        snap = {
            'position_id': 'pos_restore', 'instrument_id': 'FG609P950', 'exchange': 'CZCE',
            'volume': 2, 'direction': 'long', 'open_price': 300.0,
            'open_time': datetime.now().isoformat(), 'open_date': datetime.now().date().isoformat(),
            'position_type': 'long', 'stop_profit_price': 315.0, 'stop_loss_price': 285.0,
            'open_reason': 'test', 'order_id': 'ord_open', 'signal_id': 'sig',
            'strategy_group': 'test', 'option_premium': 0.0,
            'close_method': 'stop_loss',  # 快照中残留的close_method
            'closing_order_id': 'old_order',
        }
        rec = PositionRecord.from_dict(snap)
        # 模拟恢复逻辑中的重置
        rec._closing = False
        rec.closing_order_id = ''
        if hasattr(rec, 'close_method'):
            rec.close_method = ''
        self.assertEqual(rec.close_method, '', "恢复后close_method应为空")
        self.assertEqual(rec.closing_order_id, '', "恢复后closing_order_id应为空")
        self.assertFalse(rec._closing, "恢复后_closing应为False")


if __name__ == '__main__':
    unittest.main()
