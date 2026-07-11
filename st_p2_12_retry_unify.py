# MODULE_ID: M2-452
"""
P2-12 断言测试: 重试模式统一验证

验证:
1. BoundedRetry 支持 cancel_event/retry_on/on_exhausted/backoff_strategy
2. retry_with_limit 内部委托到 BoundedRetry
3. NetworkRetryManager.execute_with_retry 内部委托到 BoundedRetry
4. _ops_framework._execute_with_retry_and_timeout 使用 BoundedRetry/TimeoutGuard
5. position_command_service._schedule_close_retry 使用 BoundedRetry
6. 4个已标注TODO的文件不再有独立重试循环
"""
import re
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class TestP212RetryUnification(unittest.TestCase):
    """P2-12 重试模式统一断言"""

    def _read(self, rel_path: str) -> str:
        return (PROJECT_ROOT / rel_path).read_text(encoding="utf-8")

    def test_bounded_retry_supports_cancel_event(self):
        src = self._read("infra/resilience.py")
        self.assertIn("cancel_event", src,
                      "BoundedRetry 必须支持 cancel_event 参数")

    def test_bounded_retry_supports_retry_on(self):
        src = self._read("infra/resilience.py")
        self.assertIn("retry_on", src,
                      "BoundedRetry 必须支持 retry_on 参数")

    def test_bounded_retry_supports_on_exhausted(self):
        src = self._read("infra/resilience.py")
        self.assertIn("on_exhausted", src,
                      "BoundedRetry 必须支持 on_exhausted 参数")

    def test_bounded_retry_supports_backoff_strategy(self):
        src = self._read("infra/resilience.py")
        self.assertIn("backoff_strategy", src,
                      "BoundedRetry 必须支持 backoff_strategy 参数")

    def test_retry_with_limit_delegates_to_bounded_retry(self):
        src = self._read("infra/shared_utils.py")
        self.assertIn("BoundedRetry", src,
                      "retry_with_limit 必须委托到 BoundedRetry")
        self.assertNotIn("for attempt in range(max_retries", src,
                         "retry_with_limit 不应再有独立重试循环")

    def test_network_retry_manager_delegates_to_bounded_retry(self):
        src = self._read("order/order_persistence.py")
        self.assertIn("BoundedRetry", src,
                      "NetworkRetryManager 必须委托到 BoundedRetry")
        self.assertNotIn("while self.should_retry", src,
                         "NetworkRetryManager 不应再有 while should_retry 循环")

    def test_ops_framework_uses_bounded_retry(self):
        src = self._read("infra/ops_service.py")
        self.assertIn("BoundedRetry", src,
                      "_ops_framework 必须使用 BoundedRetry")

    def test_position_command_uses_bounded_retry(self):
        src = self._read("position/position_command_service.py")
        self.assertIn("BoundedRetry", src,
                      "_schedule_close_retry 必须使用 BoundedRetry")
        self.assertNotIn("for _retry in range(1, self.CLOSE_RETRY_MAX_ATTEMPTS", src,
                         "_schedule_close_retry 不应再有独立重试循环")

    def test_no_p2_12_todo_remaining_in_delegated_files(self):
        for rel_path in [
            "infra/shared_utils.py",
            "order/order_persistence.py",
            "infra/ops_service.py",
            "position/position_command_service.py",
        ]:
            src = self._read(rel_path)
            self.assertNotIn("TODO(P2-12)", src,
                             f"{rel_path} 不应再有 TODO(P2-12) 标注")


class TestBoundedRetryRuntime(unittest.TestCase):
    """BoundedRetry 运行时行为断言"""

    def test_basic_retry_success(self):
        from ali2026v3_trading.infra.resilience import BoundedRetry
        call_count = [0]
        def succeed_on_3rd():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("not yet")
            return "ok"
        br = BoundedRetry(max_retries=5, base_delay=0.01, on_exhausted="raise")
        result = br.execute(succeed_on_3rd)
        self.assertEqual(result, "ok")
        self.assertEqual(call_count[0], 3)

    def test_retry_exhausted_raise(self):
        from ali2026v3_trading.infra.resilience import BoundedRetry
        def always_fail():
            raise RuntimeError("fail")
        br = BoundedRetry(max_retries=2, base_delay=0.01, on_exhausted="raise")
        with self.assertRaises(RuntimeError):
            br.execute(always_fail)

    def test_retry_exhausted_return_none(self):
        from ali2026v3_trading.infra.resilience import BoundedRetry
        def always_fail():
            raise RuntimeError("fail")
        br = BoundedRetry(max_retries=2, base_delay=0.01, on_exhausted="return_none")
        result = br.execute(always_fail)
        self.assertIsNone(result)

    def test_retry_on_filter(self):
        from ali2026v3_trading.infra.resilience import BoundedRetry
        call_count = [0]
        def raise_value_error():
            call_count[0] += 1
            raise ValueError("filtered out")
        br = BoundedRetry(max_retries=3, base_delay=0.01, retry_on=(TypeError,), on_exhausted="return_none")
        with self.assertRaises(ValueError):
            br.execute(raise_value_error)
        self.assertEqual(call_count[0], 1)

    def test_cancel_event_interrupts(self):
        import threading
        from ali2026v3_trading.infra.resilience import BoundedRetry
        cancel = threading.Event()
        cancel.set()
        call_count = [0]
        def always_fail():
            call_count[0] += 1
            raise RuntimeError("fail")
        br = BoundedRetry(max_retries=5, base_delay=0.5, cancel_event=cancel, on_exhausted="raise")
        with self.assertRaises(InterruptedError):
            br.execute(always_fail)

    def test_linear_backoff(self):
        from ali2026v3_trading.infra.resilience import BoundedRetry
        br = BoundedRetry(max_retries=3, base_delay=1.0, backoff_strategy="linear")
        self.assertAlmostEqual(br._calc_delay(0), 1.0)
        self.assertAlmostEqual(br._calc_delay(1), 2.0)
        self.assertAlmostEqual(br._calc_delay(2), 3.0)

    def test_retry_with_limit_delegates(self):
        from ali2026v3_trading.infra.shared_utils import retry_with_limit
        call_count = [0]
        def succeed_on_2nd():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("not yet")
            return "ok"
        result = retry_with_limit(succeed_on_2nd, max_retries=3, base_delay=0.01)
        self.assertEqual(result, "ok")


if __name__ == "__main__":
    unittest.main()