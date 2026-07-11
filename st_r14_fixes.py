# MODULE_ID: M2-497
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import importlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.order.order_service import (
    _validate_order_status_transition,
    _VALID_ORDER_TRANSITIONS,
)


class TestOrderStatusTransition(unittest.TestCase):
    def test_legal_transitions(self):
        legal_cases = [
            ('PENDING', 'SUBMITTED'),
            ('PENDING', 'CANCELLED'),
            ('SUBMITTED', 'FILLED'),
            ('SUBMITTED', 'PARTIAL_FILLED'),
            ('SUBMITTED', 'CANCELLED'),
            ('SUBMITTED', 'TIMEOUT'),
            ('PARTIAL_FILLED', 'FILLED'),
            ('PARTIAL_FILLED', 'CANCELLED'),
            ('TIMEOUT', 'SUBMITTED'),
            ('FAILED', 'SUBMITTED'),
        ]
        for old, new in legal_cases:
            with self.subTest(old=old, new=new):
                self.assertTrue(
                    _validate_order_status_transition(old, new),
                    f"合法转换 {old}->{new} 应返回True",
                )

    def test_illegal_transitions(self):
        illegal_cases = [
            ('FILLED', 'SUBMITTED'),
            ('FILLED', 'CANCELLED'),
            ('CANCELLED', 'FILLED'),
            ('ALL_FILLED', 'SUBMITTED'),
            ('SUBMITTED', 'PENDING'),
        ]
        for old, new in illegal_cases:
            with self.subTest(old=old, new=new):
                self.assertFalse(
                    _validate_order_status_transition(old, new),
                    f"非法转换 {old}->{new} 应返回False",
                )

    def test_same_status_returns_true(self):
        for status in _VALID_ORDER_TRANSITIONS:
            with self.subTest(status=status):
                self.assertTrue(_validate_order_status_transition(status, status))

    def test_unknown_source_status_allows(self):
        self.assertTrue(_validate_order_status_transition('UNKNOWN_STATE', 'FILLED'))

    def test_terminal_states_have_no_outgoing(self):
        terminal = ['FILLED', 'ALL_FILLED', 'CANCELLED', '全成']
        for ts in terminal:
            with self.subTest(terminal=ts):
                self.assertEqual(len(_VALID_ORDER_TRANSITIONS.get(ts, set())), 0)


class TestFatFingerProtection(unittest.TestCase):
    def setUp(self):
        import ali2026v3_trading.order.order_service as mod
        mod._order_service_instance = None
        from ali2026v3_trading.order.order_service import get_order_service
        self.order_svc = get_order_service()

    def tearDown(self):
        import ali2026v3_trading.order.order_service as mod
        mod._order_service_instance = None

    @patch.object(
        importlib.import_module('ali2026v3_trading.order.order_service').OrderService,
        '_get_last_market_price',
        return_value=100.0,
    )
    def test_fat_finger_rejects_large_deviation(self, mock_price):
        result = self.order_svc.send_order(
            instrument_id='IF2506',
            volume=1,
            price=110.0,
            direction='BUY',
            action='OPEN',
            signal_id='test_signal',
        )
        self.assertEqual(result.error_code, 'fat_finger')

    @patch.object(
        importlib.import_module('ali2026v3_trading.order.order_service').OrderService,
        '_get_last_market_price',
        return_value=100.0,
    )
    def test_normal_price_passes(self, mock_price):
        result = self.order_svc.send_order(
            instrument_id='IF2506',
            volume=1,
            price=101.0,
            direction='BUY',
            action='OPEN',
            signal_id='test_signal',
        )
        self.assertNotEqual(result.error_code, 'fat_finger')


class TestMarginReleaseIntegration(unittest.TestCase):
    def test_release_margin_reservation_calls_risk_service(self):
        import ali2026v3_trading.order.order_service as mod
        mod._order_service_instance = None
        from ali2026v3_trading.order.order_service import get_order_service
        order_svc = get_order_service()

        with patch(
            'ali2026v3_trading.order.order_risk_guard.get_risk_service'
        ) as mock_get_rs:
            mock_rs = MagicMock()
            mock_rs.release_margin.return_value = True
            mock_get_rs.return_value = mock_rs
            order_svc._release_margin_reservation('test_order_001')
            mock_rs.release_margin.assert_called_once_with('test_order_001')

        mod._order_service_instance = None


if __name__ == '__main__':
    unittest.main()
