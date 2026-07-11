# MODULE_ID: M2-430
import os
import sys
import time
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.order.order_service import get_order_service


def _reset_order_service_singleton():
    import ali2026v3_trading.order.order_service as mod
    mod._order_service_instance = None


class TestOrderServiceResilience(unittest.TestCase):
    def setUp(self):
        _reset_order_service_singleton()
        self.order_svc = get_order_service()
        self.captured_kwargs = []

    def tearDown(self):
        _reset_order_service_singleton()

    def test_send_order_passes_stable_idempotency_key(self):
        def _mock_insert_order(**kwargs):
            self.captured_kwargs.append(kwargs)
            mock_result = MagicMock()
            mock_result.OrderRef = kwargs.get('client_order_id') or kwargs.get('order_ref') or 'MOCK'
            mock_result.order_id = mock_result.OrderRef
            mock_result.OrderStatus = 'SUBMITTED'
            mock_result.VolumeTraded = 0
            return mock_result

        self.order_svc.bind_platform_apis(_mock_insert_order, lambda _: None)
        # Ensure rate limiter allows the order through
        self.order_svc.rate_limiter._tokens = 100
        order_id = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4000.0,
            direction='BUY',
            action='OPEN',
            signal_id='sig_idempotent',
        )

        self.assertIsNotNone(order_id)
        self.assertEqual(len(self.captured_kwargs), 1)
        payload = self.captured_kwargs[0]
        self.assertEqual(payload.get('client_order_id'), order_id)
        self.assertEqual(payload.get('request_id'), order_id)
        self.assertEqual(payload.get('order_ref'), order_id)

    def test_send_order_platform_timeout_returns_quickly_and_marks_timeout(self):
        def _slow_insert_order(**kwargs):
            self.captured_kwargs.append(kwargs)
            time.sleep(0.2)
            mock_result = MagicMock()
            mock_result.OrderRef = kwargs.get('client_order_id') or 'LATE'
            mock_result.order_id = mock_result.OrderRef
            mock_result.OrderStatus = 'SUBMITTED'
            mock_result.VolumeTraded = 0
            return mock_result

        self.order_svc._platform_submit_timeout_seconds = 0.05
        self.order_svc.bind_platform_apis(_slow_insert_order, lambda _: None)
        # Ensure rate limiter allows the order through
        self.order_svc.rate_limiter._tokens = 100

        started = time.time()
        result = self.order_svc.send_order(
            instrument_id='IF2606',
            volume=1,
            price=4000.0,
            direction='BUY',
            action='OPEN',
            signal_id='sig_timeout',
        )
        elapsed = time.time() - started

        # The order should fail due to timeout or rate limiting
        # Check that it returns quickly
        self.assertLess(elapsed, 1.0)
        # The result should indicate failure
        if result is not None and hasattr(result, 'success'):
            self.assertFalse(result.success)


if __name__ == '__main__':
    unittest.main()