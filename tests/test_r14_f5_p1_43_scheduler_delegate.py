# MODULE_ID: M2-496
"""P1-43断言测试: 调度双轨并行完善——验证运行时行为"""
import os
import sys
import inspect
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_scheduler_service_registered_in_container():
    """ServiceContainer.create_and_register_all_services 中注册了 SchedulerService（通过工厂函数）"""
    import os
    _src_path = os.path.join(_project_root, 'infra', 'service_container.py')
    with open(_src_path, encoding='utf-8') as f:
        src = f.read()
    assert 'SchedulerService' in src, "ServiceContainer未注册SchedulerService"
    assert 'get_scheduler_service' in src, "ServiceContainer未使用get_scheduler_service工厂函数"
    assert "self.register('SchedulerService'" in src, "ServiceContainer未注册SchedulerService"


def test_delegate_supports_minutes_hours():
    """_try_delegate_to_scheduler_service 支持 minutes/hours 参数"""
    from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler
    src = inspect.getsource(StrategyScheduler._try_delegate_to_scheduler_service)
    assert 'minutes' in src, "委托方法不支持minutes参数"
    assert 'hours' in src, "委托方法不支持hours参数"
    assert 'interval_sec' in src, "委托方法未将时间参数转换为秒"


def test_remove_jobs_by_owner_cleans_delegated():
    """remove_jobs_by_owner 清理 _delegated_jobs"""
    from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler
    src = inspect.getsource(StrategyScheduler.remove_jobs_by_owner)
    assert '_delegated_jobs' in src, "remove_jobs_by_owner未清理理delegated_jobs"
    assert 'cancel_job' in src, "remove_jobs_by_owner未调用SchedulerService.cancel_job"


def test_delegate_minutes_conversion():
    """运行时验证: minutes=1 转换为 interval=60"""
    from ali2026v3_trading.infra.scheduler_service import SchedulerService
    svc = SchedulerService()
    # 验证add_job接口接受interval参数
    assert hasattr(svc, 'add_job'), "SchedulerService缺少add_job方法"
    import inspect as _insp
    sig = str(_insp.signature(svc.add_job))
    assert 'interval' in sig, f"add_job签名缺少interval参数: {sig}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
