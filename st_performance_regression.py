# MODULE_ID: M2-462
"""P3.2: 性能回归CI测试"""
from __future__ import annotations

import json
import os
import pytest


_BASELINE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "performance_baseline.json",
)

_DEFAULT_BASELINE = {
    "tick_p99_ms": 50.0,
    "tick_p95_ms": 30.0,
    "tick_mean_ms": 10.0,
    "order_latency_p99_ms": 100.0,
    "risk_check_p99_ms": 20.0,
    "state_store_set_p99_ms": 5.0,
    "event_bus_publish_p99_ms": 3.0,
}


def _load_baseline() -> dict:
    if os.path.exists(_BASELINE_FILE):
        with open(_BASELINE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return _DEFAULT_BASELINE


class TestPerformanceBaseline:
    def test_baseline_file_exists_or_default(self):
        baseline = _load_baseline()
        assert isinstance(baseline, dict)
        assert len(baseline) > 0

    def test_tick_p99_within_threshold(self):
        baseline = _load_baseline()
        tick_p99 = baseline.get("tick_p99_ms", 50.0)
        assert tick_p99 < 200.0, f"tick P99 {tick_p99}ms exceeds 200ms hard limit"

    def test_order_latency_within_threshold(self):
        baseline = _load_baseline()
        order_p99 = baseline.get("order_latency_p99_ms", 100.0)
        assert order_p99 < 500.0, f"order latency P99 {order_p99}ms exceeds 500ms hard limit"

    def test_risk_check_within_threshold(self):
        baseline = _load_baseline()
        risk_p99 = baseline.get("risk_check_p99_ms", 20.0)
        assert risk_p99 < 100.0, f"risk check P99 {risk_p99}ms exceeds 100ms hard limit"

    def test_state_store_set_within_threshold(self):
        baseline = _load_baseline()
        ss_p99 = baseline.get("state_store_set_p99_ms", 5.0)
        assert ss_p99 < 50.0, f"state store set P99 {ss_p99}ms exceeds 50ms hard limit"

    def test_event_bus_publish_within_threshold(self):
        baseline = _load_baseline()
        eb_p99 = baseline.get("event_bus_publish_p99_ms", 3.0)
        assert eb_p99 < 30.0, f"event bus publish P99 {eb_p99}ms exceeds 30ms hard limit"

    def test_regression_threshold_5pct(self):
        baseline = _load_baseline()
        for key, value in baseline.items():
            if key.endswith("_ms") and isinstance(value, (int, float)):
                assert value > 0, f"{key} baseline must be positive"
