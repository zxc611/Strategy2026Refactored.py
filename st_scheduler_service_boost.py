# MODULE_ID: M2-568
import pytest
import threading
from unittest.mock import MagicMock, patch
from ali2026v3_trading.infra.scheduler_service import (
    SchedulerService, JobStatus, JobInfo, OnceJobInfo, SchedulerManagerProxy,
)


class TestJobStatus:
    def test_status_values(self):
        assert JobStatus.PENDING is not JobStatus.RUNNING
        assert JobStatus.CANCELLED is not JobStatus.FAILED


class TestJobInfo:
    def test_default_values(self):
        job = JobInfo(job_id="test", func=lambda: None)
        assert job.interval == 60.0
        assert job.status == JobStatus.PENDING
        assert job.priority == 0
        assert job.timeout == 30.0
        assert job.max_retries == 0


class TestOnceJobInfo:
    def test_is_running_no_thread(self):
        job = OnceJobInfo(job_id="t", func=lambda: None, delay=1.0)
        assert job.is_running() is False

    def test_stop_no_event(self):
        job = OnceJobInfo(job_id="t", func=lambda: None, delay=1.0)
        assert job.stop() is False

    def test_stop_with_event(self):
        import threading
        job = OnceJobInfo(job_id="t", func=lambda: None, delay=1.0)
        job.stop_event = threading.Event()
        assert job.stop() is True


class TestSchedulerService:
    def test_init(self):
        svc = SchedulerService()
        assert svc.is_running() is False

    def test_add_job(self):
        svc = SchedulerService()
        job_id = svc.add_job(lambda: None, interval=10.0, job_id="j1", priority=5)
        assert job_id == "j1"
        job = svc.get_job("j1")
        assert job is not None
        assert job.priority == 5

    def test_add_job_auto_id(self):
        svc = SchedulerService()
        job_id = svc.add_job(lambda: None, interval=10.0)
        assert job_id is not None

    def test_add_job_non_callable(self):
        svc = SchedulerService()
        result = svc.add_job("not_callable")
        assert result is None

    def test_remove_job(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1")
        assert svc.remove_job("j1") is True
        assert svc.get_job("j1") is None

    def test_remove_job_not_found(self):
        svc = SchedulerService()
        assert svc.remove_job("nonexistent") is False

    def test_pause_resume_job(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1")
        assert svc.pause_job("j1") is True
        job = svc.get_job("j1")
        assert job.status == JobStatus.PAUSED
        assert svc.resume_job("j1") is True
        assert job.status == JobStatus.PENDING

    def test_pause_nonexistent(self):
        svc = SchedulerService()
        assert svc.pause_job("nonexistent") is False

    def test_resume_non_paused(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1")
        assert svc.resume_job("j1") is False

    def test_cancel_job(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1")
        assert svc.cancel_job("j1") is True
        assert svc.get_job("j1").status == JobStatus.CANCELLED

    def test_cancel_nonexistent(self):
        svc = SchedulerService()
        assert svc.cancel_job("nonexistent") is False

    def test_get_all_jobs_sorted_by_priority(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1", priority=1)
        svc.add_job(lambda: None, job_id="j2", priority=10)
        svc.add_job(lambda: None, job_id="j3", priority=5)
        jobs = svc.get_all_jobs()
        assert jobs[0].job_id == "j2"
        assert jobs[1].job_id == "j3"
        assert jobs[2].job_id == "j1"

    def test_start_stop(self):
        svc = SchedulerService()
        assert svc.start() is True
        assert svc.is_running() is True
        assert svc.start() is False
        svc.stop()
        assert svc.is_running() is False

    def test_add_once_job(self):
        svc = SchedulerService()
        job_id = svc.add_once_job(lambda: None, delay=10.0, job_id="once1")
        assert job_id == "once1"
        svc.stop()

    def test_add_once_job_duplicate(self):
        svc = SchedulerService()
        svc.start()
        job_id1 = svc.add_once_job(lambda: None, delay=60.0, job_id="once1")
        assert job_id1 is not None
        svc.stop()

    def test_cancel_once_job(self):
        svc = SchedulerService()
        svc.start()
        svc.add_once_job(lambda: None, delay=60.0, job_id="once1")
        assert svc.cancel_job("once1") is True
        svc.stop()

    def test_get_stats(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1", interval=10.0)
        stats = svc.get_stats()
        assert stats['service_name'] == 'SchedulerService'
        assert stats['total_jobs'] == 1
        assert 'j1' in stats['jobs']

    def test_clear(self):
        svc = SchedulerService()
        svc.add_job(lambda: None, job_id="j1")
        svc.clear()
        assert svc.get_job("j1") is None
        assert len(svc.get_all_jobs()) == 0


class TestSchedulerManagerProxy:
    def test_init(self):
        proxy = SchedulerManagerProxy()
        assert proxy._provider is None

    def test_get_scheduler_no_provider(self):
        proxy = SchedulerManagerProxy()
        assert proxy._get_scheduler() is None

    def test_init_scheduler_no_provider(self):
        proxy = SchedulerManagerProxy()
        proxy.init_scheduler()

    def test_stop_scheduler_no_provider(self):
        proxy = SchedulerManagerProxy()
        proxy.stop_scheduler()

    def test_add_option_status_diagnosis_job(self):
        proxy = SchedulerManagerProxy()
        proxy.add_option_status_diagnosis_job()

    def test_add_tick_sync_job(self):
        proxy = SchedulerManagerProxy()
        proxy.add_tick_sync_job()

    def test_add_14_contracts_diagnosis_job(self):
        proxy = SchedulerManagerProxy()
        proxy.add_14_contracts_diagnosis_job()

    def test_add_trading_jobs(self):
        proxy = SchedulerManagerProxy()
        proxy.add_trading_jobs()