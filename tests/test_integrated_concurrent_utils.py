# MODULE_ID: M2-358
import sys
import os
import pytest
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestRejectionPolicy:
    def test_enum_values(self):
        from ali2026v3_trading.infra.concurrent_utils import RejectionPolicy
        assert RejectionPolicy.ABORT.value == 'abort'
        assert RejectionPolicy.DROP.value == 'drop'
        assert RejectionPolicy.CALLER_RUNS.value == 'caller_runs'
        assert RejectionPolicy.DISCARD_OLDEST.value == 'discard_oldest'

    def test_enum_count(self):
        from ali2026v3_trading.infra.concurrent_utils import RejectionPolicy
        assert len(RejectionPolicy) == 4


class TestThreadPoolExecutorWithPolicy:
    def test_init(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=1)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.ABORT)
        assert wrapper is not None
        executor.shutdown(wait=False)

    def test_submit_success(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=10)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.ABORT)
        future = wrapper.submit(lambda: 42)
        assert future.result() == 42
        executor.shutdown(wait=True)

    def test_submit_to_queue_success(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=10)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.DROP)
        result = wrapper.submit_to_queue("item1")
        assert result is True
        executor.shutdown(wait=False)

    def test_submit_to_queue_full_drop(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=1)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.DROP)
        wrapper.submit_to_queue("item1")
        result = wrapper.submit_to_queue("item2")
        assert result is False
        executor.shutdown(wait=False)

    def test_get_stats(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=10)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.DROP)
        stats = wrapper.get_stats()
        assert 'drop_count' in stats
        assert 'caller_runs_count' in stats
        assert 'policy' in stats
        assert stats['policy'] == 'drop'
        executor.shutdown(wait=False)

    def test_caller_runs_policy(self):
        from ali2026v3_trading.infra.concurrent_utils import ThreadPoolExecutorWithPolicy, RejectionPolicy
        executor = ThreadPoolExecutor(max_workers=2)
        task_queue = queue.Queue(maxsize=10)
        wrapper = ThreadPoolExecutorWithPolicy(executor, task_queue, RejectionPolicy.CALLER_RUNS)
        result = wrapper.submit(lambda: 99)
        if hasattr(result, 'result'):
            assert result.result() == 99
        else:
            assert result == 99
        executor.shutdown(wait=False)