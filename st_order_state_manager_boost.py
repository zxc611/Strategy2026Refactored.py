# MODULE_ID: M2-431
import pytest
from unittest.mock import MagicMock, patch
from ali2026v3_trading.order.order_state_manager import OrderStateManager


class TestOrderStateManager:
    def test_init(self):
        mgr = OrderStateManager()
        assert mgr.pending_count == 0


    def test_init_custom_max(self):
        mgr = OrderStateManager(max_pending=500)
        assert mgr._max_pending == 500

    def test_add_pending(self):
        mgr = OrderStateManager()
        mgr.add_pending("order1", {"status": "SUBMITTED"})
        assert mgr.pending_count == 1
        assert mgr.get_pending("order1") is not None

    def test_remove_pending(self):
        mgr = OrderStateManager()
        mgr.add_pending("order1", {"status": "SUBMITTED"})
        result = mgr.remove_pending("order1")
        assert result is not None
        assert mgr.pending_count == 0

    def test_remove_pending_not_found(self):
        mgr = OrderStateManager()
        result = mgr.remove_pending("nonexistent")
        assert result is None

    def test_get_pending_not_found(self):
        mgr = OrderStateManager()
        assert mgr.get_pending("nonexistent") is None

    def test_scan_timeouts_no_timeouts(self):
        mgr = OrderStateManager()
        mgr.add_pending("order1", {"status": "SUBMITTED"})
        timeouts = mgr.scan_timeouts(timeout_sec=9999)
        assert timeouts == []

    def test_scan_timeouts_with_timeout(self):
        import time
        mgr = OrderStateManager()
        mgr.add_pending("order1", {"status": "SUBMITTED"})
        time.sleep(0.01)
        timeouts = mgr.scan_timeouts(timeout_sec=0.0)
        assert "order1" in timeouts

    def test_check_pending_orders(self):
        mgr = OrderStateManager()
        mgr.add_pending("order1", {"status": "SUBMITTED"})
        result = mgr.check_pending_orders()
        assert isinstance(result, list)

    def test_pending_count_multiple(self):
        mgr = OrderStateManager()
        mgr.add_pending("o1", {"status": "SUBMITTED"})
        mgr.add_pending("o2", {"status": "SUBMITTED"})
        mgr.add_pending("o3", {"status": "SUBMITTED"})
        assert mgr.pending_count == 3

    def test_add_remove_sequence(self):
        mgr = OrderStateManager()
        mgr.add_pending("o1", {"status": "SUBMITTED"})
        mgr.add_pending("o2", {"status": "SUBMITTED"})
        mgr.remove_pending("o1")
        assert mgr.pending_count == 1
        assert mgr.get_pending("o2") is not None

    def test_scan_timeouts_default_timeout(self):
        mgr = OrderStateManager()
        mgr.add_pending("order1", {"status": "SUBMITTED"})
        timeouts = mgr.scan_timeouts()
        assert isinstance(timeouts, list)