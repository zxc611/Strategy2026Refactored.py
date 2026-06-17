# MODULE_ID: M2-592
"""strategy_judgment 低覆盖率模块补充测试"""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import threading
import numpy as np
from datetime import datetime, timezone


# ── 1. analytics_manager ──
class TestAnalyticsManager:
    def test_import(self):
        from ali2026v3_trading.strategy_judgment import analytics_manager
        assert analytics_manager is not None

    def test_instance_and_properties(self):
        from ali2026v3_trading.strategy_judgment.analytics_manager import AnalyticsManager
        mgr = AnalyticsManager.__new__(AnalyticsManager)
        mgr.__init__()
        assert isinstance(mgr.stats, dict)
        assert mgr.warmup_done is False
        mgr.warmup_done = True
        assert mgr.warmup_done is True

    def test_record_methods(self):
        from ali2026v3_trading.strategy_judgment.analytics_manager import AnalyticsManager
        mgr = AnalyticsManager.__new__(AnalyticsManager)
        mgr.__init__()
        lock = threading.RLock()
        mgr.record_tick(lock)
        mgr.record_trade(lock)
        mgr.record_signal(lock)
        mgr.record_error("test error", lock)
        assert mgr.stats['total_ticks'] == 1
        assert mgr.stats['total_trades'] == 1
        assert mgr.stats['total_signals'] == 1
        assert mgr.stats['errors_count'] == 1

    def test_get_stats(self):
        from ali2026v3_trading.strategy_judgment.analytics_manager import AnalyticsManager
        mgr = AnalyticsManager.__new__(AnalyticsManager)
        mgr.__init__()
        lock = threading.RLock()
        stats = mgr.get_stats(lock, state_value='RUNNING', is_running=True, is_paused=False)
        assert isinstance(stats, dict)
        assert stats['state'] == 'RUNNING'

    def test_reset_stats(self):
        from ali2026v3_trading.strategy_judgment.analytics_manager import AnalyticsManager
        mgr = AnalyticsManager.__new__(AnalyticsManager)
        mgr.__init__()
        mgr._stats['total_ticks'] = 10
        mgr.reset_stats()
        assert mgr.stats['total_ticks'] == 0

    def test_ensure_analytics_ready(self):
        from ali2026v3_trading.strategy_judgment.analytics_manager import AnalyticsManager
        mgr = AnalyticsManager.__new__(AnalyticsManager)
        mgr.__init__()
        mgr._analytics_warmup_done = True
        assert mgr.ensure_analytics_ready(timeout=0.1) is True


# ── 2. causal_chain_utils ──
class TestCausalChainUtils:
    def test_import(self):
        from ali2026v3_trading.strategy_judgment import causal_chain_utils
        assert causal_chain_utils is not None

    def test_causal_event(self):
        from ali2026v3_trading.strategy_judgment.causal_chain_utils import CausalEvent
        evt = CausalEvent.__new__(CausalEvent)
        assert evt is not None

    def test_causal_chain_tracker(self):
        from ali2026v3_trading.strategy_judgment.causal_chain_utils import CausalChainTracker, CausalEvent
        cct = CausalChainTracker.__new__(CausalChainTracker)
        cct.__init__()
        assert cct.new_correlation_id() != ""
        cct.record_event(CausalEvent())
        assert len(cct.trace_root_cause("")) == 0
        assert cct.get_contaminated_correlations() == set()

    def test_contamination_guard(self):
        from ali2026v3_trading.strategy_judgment.causal_chain_utils import ContaminationGuard
        cg = ContaminationGuard.__new__(ContaminationGuard)
        cg.__init__()
        ok, val = cg.validate_numeric(1.0, "test")
        assert ok is True
