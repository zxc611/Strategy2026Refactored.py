import pytest
import sys
import os
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.state_store import StateStore, StateSnapshot, get_state_store, reset_state_store


class TestStateStore:
    def test_get_set_basic(self):
        store = StateStore()
        store.set('key1', 'value1')
        assert store.get('key1') == 'value1'

    def test_deep_copy_isolation(self):
        store = StateStore()
        original = {'a': [1, 2, 3]}
        store.set('data', original)
        got = store.get('data')
        got['a'].append(4)
        assert store.get('data')['a'] == [1, 2, 3]

    def test_snapshot_performance(self):
        store = StateStore()
        for i in range(10):
            store.set(f'key_{i}', {'data': list(range(10))})
        start = time.monotonic()
        snap = store.snapshot()
        elapsed_us = (time.monotonic() - start) * 1_000_000
        assert elapsed_us < 500, f"snapshot took {elapsed_us:.0f}us > 500us"

    def test_version_vector(self):
        store = StateStore()
        store.set('x', 1)
        assert store.get_version('x') == 1
        store.set('x', 2)
        assert store.get_version('x') == 2

    def test_callback_notification(self):
        store = StateStore()
        changes = []
        store.register_callback('key', lambda k, o, n: changes.append((k, o, n)))
        store.set('key', 'new_value')
        assert len(changes) == 1

    def test_thread_safety(self):
        store = StateStore()
        errors = []

        def writer(n):
            try:
                for i in range(100):
                    store.set(f'key_{n}', i)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0

    def test_rollback(self):
        store = StateStore()
        store.set('x', 1)
        store.take_snapshot()
        v1 = store.get_all_versions()
        store.set('x', 2)
        store.rollback(v1)
        assert store.get('x') == 1

    def test_get_default(self):
        store = StateStore()
        assert store.get('nonexistent') is None
        assert store.get('nonexistent', 42) == 42

    def test_apply_diff(self):
        store = StateStore()
        store.set('a', 1)
        store.set('b', 2)
        store.apply_diff({'a': 10, 'c': 3})
        assert store.get('a') == 10
        assert store.get('b') == 2
        assert store.get('c') == 3

    def test_snapshot_with_keys(self):
        store = StateStore()
        store.set('a', 1)
        store.set('b', 2)
        snap = store.snapshot(keys=['a'])
        assert 'a' in snap
        assert 'b' not in snap


class TestStateSnapshot:
    def test_immutability(self):
        snap = StateSnapshot({'a': 1}, {'a': 1}, time.monotonic())
        with pytest.raises(AttributeError):
            snap.data = {'b': 2}

    def test_freshness(self):
        snap = StateSnapshot({}, {}, time.monotonic())
        assert snap.is_fresh(1000) is True

    def test_data_deep_copy(self):
        original = {'a': [1, 2, 3]}
        snap = StateSnapshot(original, {'a': 1}, time.monotonic())
        data = snap.data
        data['a'].append(4)
        assert snap.data['a'] == [1, 2, 3]

    def test_version_copy(self):
        snap = StateSnapshot({}, {'x': 1}, time.monotonic())
        v = snap.version
        v['x'] = 999
        assert snap.version['x'] == 1


class TestGlobalStateStore:
    def test_singleton(self):
        reset_state_store()
        s1 = get_state_store()
        s2 = get_state_store()
        assert s1 is s2
        reset_state_store()

    def test_reset(self):
        reset_state_store()
        s1 = get_state_store()
        s1.set('x', 1)
        reset_state_store()
        s2 = get_state_store()
        assert s2.get('x') is None
