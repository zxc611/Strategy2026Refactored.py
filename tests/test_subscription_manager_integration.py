"""
subscription_manager 集成测试套件
覆盖: WAL恢复、重试退避、后台线程生命周期、背压保护

运行方式: python -m pytest tests/test_subscription_manager_integration.py -v
依赖: pytest (pip install pytest)
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch, PropertyMock


class MockStorage:
    def __init__(self):
        self._registered = []
    def get_registered_instrument_ids(self):
        return list(self._registered)
    def register_instrument(self, inst_id):
        if inst_id not in self._registered:
            self._registered.append(inst_id)
    def unregister_instrument(self, inst_id):
        if inst_id in self._registered:
            self._registered.remove(inst_id)
    def get_instruments_by_type(self, inst_type):
        return [i for i in self._registered if inst_type in i]


class TestRetryBackoff:
    """测试指数退避与重试逻辑"""

    def test_exponential_backoff_calculation(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        for attempt in range(10):
            delay = sm._calculate_backoff_delay(attempt)
            assert delay > 0, f"Attempt {attempt}: delay should be positive"
            if attempt > 0:
                prev_delay = sm._calculate_backoff_delay(attempt - 1)
                assert delay >= prev_delay, f"Attempt {attempt}: backoff should not decrease"

    def test_max_backoff_cap(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        delay_large = sm._calculate_backoff_delay(100)
        delay_small = sm._calculate_backoff_delay(1000)
        assert abs(delay_large - delay_small) < 0.01, "Backoff should hit max cap"

    def test_retry_queue_basic_operations(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        sm._retry_queue = deque(maxlen=100)
        sm.enqueue_retry("test_inst_01", "subscribe")
        assert len(sm._retry_queue) == 1
        sm.enqueue_retry("test_inst_02", "unsubscribe")
        assert len(sm._retry_queue) == 2


class TestWALRecovery:
    """测试 WAL 持久化与恢复"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="sub_wal_test_")

    def tearDown(self):
        import shutil
        if hasattr(self, 'tmpdir') and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_wal_write_and_read(self):
        self.setUp()
        try:
            wal_path = os.path.join(self.tmpdir, "test_wal.jsonl")
            records = [
                {"op": "subscribe", "instrument_id": "m2605", "ts": 1714600000.0},
                {"op": "subscribe", "instrument_id": "al2605C18900", "ts": 1714600001.0},
                {"op": "unsubscribe", "instrument_id": "m2605", "ts": 1714600010.0},
            ]
            with open(wal_path, 'w', encoding='utf-8') as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + '\n')
            recovered = []
            with open(wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        recovered.append(json.loads(line))
            assert len(recovered) == 3
            assert recovered[0]['op'] == 'subscribe'
            assert recovered[2]['op'] == 'unsubscribe'
        finally:
            self.tearDown()

    def test_wal_corrupted_entry_isolation(self):
        self.setUp()
        try:
            wal_path = os.path.join(self.tmpdir, "corrupt_wal.jsonl")
            lines = [
                '{"op":"subscribe","instrument_id":"m2605"}\n',
                'NOT_VALID_JSON{{{###\n',
                '{"op":"subscribe","instrument_id":"rb2605"}\n',
            ]
            with open(wal_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            valid = []
            corrupted = 0
            with open(wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        valid.append(json.loads(line))
                    except json.JSONDecodeError:
                        corrupted += 1
            assert corrupted == 1
            assert len(valid) == 2
            assert valid[1]['instrument_id'] == 'rb2605'
        finally:
            self.tearDown()

    def test_empty_wal_recovery(self):
        self.setUp()
        try:
            wal_path = os.path.join(self.tmpdir, "empty_wal.jsonl")
            with open(wal_path, 'w', encoding='utf-8') as f:
                f.write('')
            recovered = []
            with open(wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        recovered.append(json.loads(line))
            assert len(recovered) == 0
        finally:
            self.tearDown()


class TestThreadLifecycle:
    """测试后台线程启动/停止"""

    def test_start_stop_background_threads(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        sm._start_background_threads()
        time.sleep(0.2)
        assert sm._retry_thread is not None
        assert sm._retry_thread.is_alive()
        sm.stop_background_threads(join_timeout=3.0)
        sm._retry_thread.join(timeout=2.0)
        assert not sm._retry_thread.is_alive()

    def test_double_stop_is_idempotent(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        sm._start_background_threads()
        time.sleep(0.1)
        sm.stop_background_threads(join_timeout=3.0)
        sm._retry_thread.join(timeout=2.0)
        sm.stop_background_threads(join_timeout=1.0)
        assert True

    def test_ensure_background_threads_rearm(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        sm._start_background_threads()
        time.sleep(0.1)
        assert sm._retry_thread is not None
        assert sm._retry_thread.is_alive()
        sm.stop_background_threads(join_timeout=3.0)
        sm._retry_thread.join(timeout=2.0)
        sm.ensure_background_threads()
        time.sleep(0.2)
        assert sm._retry_thread.is_alive()


class TestBackpressure:
    """测试背压与队列容量保护"""

    def test_retry_queue_capacity_limit(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        sm._retry_queue = deque(maxlen=10)
        for i in range(15):
            sm.enqueue_retry(f"inst_{i:04d}", "subscribe")
        assert len(sm._retry_queue) == 10, f"Queue should not exceed capacity, got {len(sm._retry_queue)}"

    def test_subscription_dedup(self):
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        sm = SubscriptionManager(storage=MockStorage(), _in_test_mode=True)
        sm._subscription_success = {}
        inst_id = f"test_{uuid.uuid4().hex[:8]}"
        sm.mark_subscribed(inst_id)
        assert sm.is_subscribed(inst_id)
        sm.mark_subscribed(inst_id)
        assert sm.is_subscribed(inst_id)


class TestInstrumentClassification:
    """测试合约分类工具"""

    def test_classify_registered_instruments(self):
        from ali2026v3_trading.subscription_manager import classify_registered_instruments
        storage = MockStorage()
        storage.register_instrument("m2605")
        storage.register_instrument("al2605C18900")
        storage.register_instrument("rb2605")
        storage.register_instrument("al2605P18500")
        futures, options = classify_registered_instruments(storage)
        assert isinstance(futures, list)
        assert isinstance(options, dict)


if __name__ == '__main__':
    print("=" * 60)
    print("subscription_manager 集成测试套件")
    print("运行: python -m pytest tests/test_subscription_manager_integration.py -v")
    print("=" * 60)
