# MODULE_ID: M2-440
"""
P1-08 断言测试: StrategyScheduler 与 SchedulerService 职责边界验证

验证:
1. StrategyScheduler 基于 APScheduler (BackgroundScheduler)
2. SchedulerService 是自研调度引擎 (无APScheduler依赖)
3. 两者在生产代码中无同时依赖
4. SchedulerService 类本身零实例化 (仅re-export MarketTimeService)
5. StrategyScheduler 有owner-based job管理 (策略业务)
6. SchedulerService 有priority/timeout/retry (通用任务)
"""
import ast
import importlib
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestSchedulerBoundary(unittest.TestCase):
    """StrategyScheduler vs SchedulerService 职责边界断言"""

    def test_strategy_scheduler_uses_apscheduler(self):
        """StrategyScheduler 底层必须是 APScheduler BackgroundScheduler"""
        src = (PROJECT_ROOT / "strategy" / "strategy_scheduler.py").read_text(encoding="utf-8")
        self.assertIn("BackgroundScheduler", src,
                      "StrategyScheduler 必须基于 APScheduler BackgroundScheduler")

    def test_scheduler_service_no_apscheduler(self):
        """SchedulerService 不得依赖 APScheduler"""
        src = (PROJECT_ROOT / "infra" / "scheduler_service.py").read_text(encoding="utf-8")
        self.assertNotIn("apscheduler", src.lower(),
                         "SchedulerService 不得导入或使用 apscheduler")

    def test_strategy_scheduler_has_owner_job_management(self):
        """StrategyScheduler 必须有 owner-based job 管理 (策略业务特征)"""
        src = (PROJECT_ROOT / "strategy" / "strategy_scheduler.py").read_text(encoding="utf-8")
        for method in ["add_job_with_owner", "remove_jobs_by_owner", "get_jobs_by_owner"]:
            self.assertIn(method, src,
                          f"StrategyScheduler 必须有 {method} (owner-based策略业务调度)")

    def test_scheduler_service_has_priority_timeout(self):
        """SchedulerService 必须有 priority/timeout/retry (通用任务特征)"""
        src = (PROJECT_ROOT / "infra" / "scheduler_service.py").read_text(encoding="utf-8")
        for feature in ["priority", "timeout", "max_retries"]:
            self.assertIn(feature, src,
                          f"SchedulerService 必须有 {feature} (通用任务调度特征)")

    def test_scheduler_service_reexports_market_time(self):
        """SchedulerService 必须 re-export MarketTimeService 系列函数"""
        src = (PROJECT_ROOT / "infra" / "scheduler_service.py").read_text(encoding="utf-8")
        for symbol in ["MarketTimeService", "is_market_open", "TimeSyncChecker"]:
            self.assertIn(symbol, src,
                          f"SchedulerService 必须 re-export {symbol}")

    def test_no_production_code_instantiates_scheduler_service(self):
        """生产代码中 SchedulerService 类零实例化 (排除测试和backup)"""
        scheduler_service_py = PROJECT_ROOT / "infra" / "scheduler_service.py"
        for py_file in PROJECT_ROOT.rglob("*.py"):
            rel = py_file.relative_to(PROJECT_ROOT)
            parts = rel.parts
            if any(p.startswith("_backup_") or p.startswith("test_") for p in parts):
                continue
            if py_file == scheduler_service_py:
                continue
            try:
                src = py_file.read_text(encoding="utf-8")
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                continue
            self.assertNotIn("SchedulerService()", src,
                             f"{rel} 不得直接实例化 SchedulerService(); "
                             f"仅应使用其 re-export 的 MarketTimeService 系列函数")

    def test_no_file_depends_on_both_scheduler_classes(self):
        """无生产代码同时依赖 StrategyScheduler 类和 SchedulerService 类"""
        strategy_scheduler_py = PROJECT_ROOT / "strategy" / "strategy_scheduler.py"
        scheduler_service_py = PROJECT_ROOT / "infra" / "scheduler_service.py"
        for py_file in PROJECT_ROOT.rglob("*.py"):
            rel = py_file.relative_to(PROJECT_ROOT)
            parts = rel.parts
            if any(p.startswith("_backup_") or p.startswith("test_") for p in parts):
                continue
            if py_file in (strategy_scheduler_py, scheduler_service_py):
                continue
            try:
                src = py_file.read_text(encoding="utf-8")
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                continue
            has_strategy_scheduler = "StrategyScheduler" in src
            has_scheduler_service = "SchedulerService" in src
            self.assertFalse(
                has_strategy_scheduler and has_scheduler_service,
                f"{rel} 不得同时依赖 StrategyScheduler 和 SchedulerService 类"
            )


if __name__ == "__main__":
    unittest.main()