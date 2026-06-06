import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.callback_registry import CallbackRegistry, CallbackGroup, get_callback_group, reset_callback_group, ON_TICK


class TestCallbackRegistry:
    def test_register_and_invoke(self):
        reg = CallbackRegistry('test')
        results = []
        reg.register(lambda x: results.append(x))
        reg.invoke(42)
        assert results == [42]

    def test_unregister(self):
        reg = CallbackRegistry('test')
        cb = lambda: None
        reg.register(cb)
        assert len(reg) == 1
        reg.unregister(cb)
        assert len(reg) == 0

    def test_exception_isolation(self):
        reg = CallbackRegistry('test')
        results = []
        reg.register(lambda: 1 / 0)
        reg.register(lambda: results.append('ok'))
        reg.invoke()
        assert results == ['ok']

    def test_decorator_usage(self):
        reg = CallbackRegistry('test')
        results = []

        @reg.register
        def my_callback():
            results.append('called')

        reg.invoke()
        assert results == ['called']

    def test_clear(self):
        reg = CallbackRegistry('test')
        reg.register(lambda: None)
        reg.clear()
        assert len(reg) == 0

    def test_invoke_returns_results(self):
        reg = CallbackRegistry('test')
        reg.register(lambda x: x * 2)
        reg.register(lambda x: x + 1)
        results = reg.invoke(5)
        assert results == [10, 6]

    def test_contains(self):
        reg = CallbackRegistry('test')
        cb = lambda: None
        reg.register(cb)
        assert cb in reg

    def test_duplicate_register(self):
        reg = CallbackRegistry('test')
        cb = lambda: None
        reg.register(cb)
        reg.register(cb)
        assert len(reg) == 1

    def test_unregister_nonexistent(self):
        reg = CallbackRegistry('test')
        assert reg.unregister(lambda: None) is False

    def test_get_callbacks(self):
        reg = CallbackRegistry('test')
        cb = lambda: None
        reg.register(cb)
        assert reg.get_callbacks() == [cb]

    def test_name_property(self):
        reg = CallbackRegistry('my_reg')
        assert reg.name == 'my_reg'


class TestCallbackGroup:
    def test_get_registry(self):
        group = CallbackGroup()
        reg = group.get_registry('on_tick')
        assert isinstance(reg, CallbackRegistry)

    def test_invoke_all(self):
        group = CallbackGroup()
        reg = group.get_registry('test_event')
        results = []
        reg.register(lambda x: results.append(x))
        group.invoke_all('test_event', 42)
        assert results == [42]

    def test_invoke_all_nonexistent(self):
        group = CallbackGroup()
        result = group.invoke_all('no_such_event')
        assert result == {}

    def test_remove_registry(self):
        group = CallbackGroup()
        group.get_registry('test')
        assert group.remove_registry('test') is True
        assert group.remove_registry('test') is False

    def test_list_registries(self):
        group = CallbackGroup()
        group.get_registry('a')
        group.get_registry('b')
        names = group.list_registries()
        assert set(names) == {'a', 'b'}

    def test_predefined_constants(self):
        assert ON_TICK == 'on_tick'


class TestGlobalCallbackGroup:
    def test_singleton(self):
        reset_callback_group()
        g1 = get_callback_group()
        g2 = get_callback_group()
        assert g1 is g2
        reset_callback_group()

    def test_reset(self):
        reset_callback_group()
        g1 = get_callback_group()
        g1.get_registry('test')
        reset_callback_group()
        g2 = get_callback_group()
        assert g2.list_registries() == []
