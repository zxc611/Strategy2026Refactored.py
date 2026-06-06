import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from hypothesis import given, settings, assume
from hypothesis import strategies as st
from ali2026v3_trading.state_store import StateStore, reset_state_store


class TestStateStoreProperties:
    @given(key=st.text(min_size=1, max_size=20), value=st.one_of(st.integers(), st.floats(allow_nan=False), st.text()))
    @settings(max_examples=50)
    def test_set_get_roundtrip(self, key, value):
        reset_state_store()
        store = StateStore()
        store.set(key, value)
        assert store.get(key) == value

    @given(value=st.dictionaries(st.text(min_size=1, max_size=5), st.integers()))
    @settings(max_examples=30)
    def test_deep_copy_mutation_safety(self, value):
        store = StateStore()
        store.set('data', value)
        got = store.get('data')
        if isinstance(got, dict):
            got['MUTATED_KEY'] = 999
        assert store.get('data') == value

    @given(operations=st.lists(st.tuples(st.text(min_size=1, max_size=5), st.integers(min_value=0, max_value=100))))
    @settings(max_examples=30)
    def test_version_monotonicity(self, operations):
        store = StateStore()
        for key, val in operations:
            old_ver = store.get_version(key)
            store.set(key, val)
            new_ver = store.get_version(key)
            assert new_ver >= old_ver

    @given(keys=st.lists(st.text(min_size=1, max_size=5), min_size=1, max_size=10, unique=True))
    @settings(max_examples=20)
    def test_snapshot_completeness(self, keys):
        store = StateStore()
        for k in keys:
            store.set(k, k)
        snap = store.snapshot()
        for k in keys:
            assert k in snap

    @given(key=st.text(min_size=1, max_size=10), v1=st.integers(), v2=st.integers())
    @settings(max_examples=30)
    def test_overwrite_semantics(self, key, v1, v2):
        store = StateStore()
        store.set(key, v1)
        assert store.get(key) == v1
        store.set(key, v2)
        assert store.get(key) == v2

    @given(data=st.dictionaries(st.text(min_size=1, max_size=5), st.lists(st.integers(), max_size=5)))
    @settings(max_examples=20)
    def test_nested_deep_copy_safety(self, data):
        store = StateStore()
        store.set('nested', data)
        got = store.get('nested')
        if isinstance(got, dict):
            for k, v in got.items():
                if isinstance(v, list) and len(v) > 0:
                    v.append(999999)
                    break
        assert store.get('nested') == data

    @given(keys=st.lists(st.text(min_size=1, max_size=3), min_size=1, max_size=5, unique=True))
    @settings(max_examples=15)
    def test_take_snapshot_version_capture(self, keys):
        store = StateStore()
        for k in keys:
            store.set(k, 1)
        snap = store.take_snapshot()
        for k in keys:
            assert snap.version.get(k, 0) >= 1

    @given(diff=st.dictionaries(st.text(min_size=1, max_size=5), st.integers()))
    @settings(max_examples=20)
    def test_apply_diff_idempotent_read(self, diff):
        store = StateStore()
        store.apply_diff(diff)
        for k, v in diff.items():
            assert store.get(k) == v

    @given(key=st.text(min_size=1, max_size=5))
    @settings(max_examples=20)
    def test_default_value_on_missing_key(self, key):
        store = StateStore()
        assert store.get(key) is None
        assert store.get(key, 'default') == 'default'

    @given(key=st.text(min_size=1, max_size=5), val=st.integers())
    @settings(max_examples=20)
    def test_version_increments_on_set(self, key, val):
        store = StateStore()
        store.set(key, val)
        v1 = store.get_version(key)
        store.set(key, val + 1)
        v2 = store.get_version(key)
        assert v2 == v1 + 1
