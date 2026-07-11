# MODULE_ID: M2-372
import sys
import os
import pytest
import numpy as np
import logging
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestNumpyRingBuffer:
    def test_init_empty(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5)
        assert buf.count == 0
        assert len(buf) == 0

    def test_append_and_snapshot(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5)
        for i in range(3):
            buf.append(float(i))
        snap = buf.snapshot()
        assert len(snap) == 3
        assert list(snap) == [0.0, 1.0, 2.0]

    def test_wrap_around(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=3)
        for i in range(5):
            buf.append(float(i))
        snap = buf.snapshot()
        assert len(snap) == 3
        assert list(snap) == [2.0, 3.0, 4.0]

    def test_sum(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            buf.append(v)
        assert buf.sum() == 15.0

    def test_mean(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            buf.append(v)
        assert abs(buf.mean() - 3.0) < 1e-9

    def test_std(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            buf.append(v)
        assert buf.std() > 0

    def test_last(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5)
        buf.append(10.0)
        buf.append(20.0)
        assert buf.last() == 20.0

    def test_sorted_values(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5)
        for v in [3.0, 1.0, 2.0]:
            buf.append(v)
        sv = buf.sorted_values()
        assert list(sv) == [1.0, 2.0, 3.0]

    def test_percentile(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=10)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            buf.append(v)
        assert buf.percentile(50.0) == 3.0

    def test_to_list(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5)
        buf.append(1.0)
        buf.append(2.0)
        assert buf.to_list() == [1.0, 2.0]

    def test_single_element(self):
        from ali2026v3_trading.data.quant_infra import NumpyRingBuffer
        buf = NumpyRingBuffer(capacity=5)
        buf.append(42.0)
        assert buf.count == 1
        assert buf.sum() == 42.0
        assert buf.mean() == 42.0
        assert buf.std() == 0.0
        assert buf.last() == 42.0


class TestRateLimitLog:
    def test_basic_call(self):
        from ali2026v3_trading.data.quant_infra import rate_limit_log
        logger = logging.getLogger("test_rate_limit")
        rate_limit_log(logger, logging.INFO, "test msg", "test_key", min_interval=0.0)

    def test_rapid_calls_no_error(self):
        from ali2026v3_trading.data.quant_infra import rate_limit_log
        logger = logging.getLogger("test_rate_limit_rapid")
        for i in range(10):
            rate_limit_log(logger, logging.DEBUG, f"msg_{i}", "key_rapid", min_interval=0.0)