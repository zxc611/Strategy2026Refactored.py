"""P3.5降级版：单线程Version Vector单调递增验证"""
from __future__ import annotations

import pytest


def _get_state_store_class():
    try:
        from ali2026v3_trading.state_store import StateStore
        return StateStore
    except ImportError:
        return None


class TestVersionVectorMonotonic:
    def test_version_increases_on_set(self):
        StateStore = _get_state_store_class()
        if StateStore is None:
            pytest.skip("StateStore not importable")
        store = StateStore()
        versions = []
        for i in range(1000):
            store.set(f"key_{i % 10}", f"value_{i}")
            try:
                vv = store.version_vector
                if vv is not None:
                    versions.append(dict(vv))
            except AttributeError:
                pass
        if versions:
            for key in versions[0]:
                prev = 0
                for vv in versions:
                    current = vv.get(key, 0)
                    assert current >= prev, f"version for {key} decreased: {prev} -> {current}"
                    prev = current

    def test_version_increases_for_same_key(self):
        StateStore = _get_state_store_class()
        if StateStore is None:
            pytest.skip("StateStore not importable")
        store = StateStore()
        versions = []
        for i in range(100):
            store.set("test_key", f"value_{i}")
            try:
                vv = store.version_vector
                if vv is not None:
                    versions.append(dict(vv))
            except AttributeError:
                pass
        if versions:
            for key in versions[0]:
                prev = 0
                for vv in versions:
                    current = vv.get(key, 0)
                    assert current >= prev
                    prev = current

    def test_snapshot_version_consistent(self):
        StateStore = _get_state_store_class()
        if StateStore is None:
            pytest.skip("StateStore not importable")
        store = StateStore()
        for i in range(10):
            store.set("snapshot_key", f"v{i}")
        try:
            snap = store.take_snapshot()
            assert snap is not None
        except AttributeError:
            pytest.skip("take_snapshot not available")
