# MODULE_ID: M2-515
"""
test_r2_02_market_time.py - P1-08 双调度器问题修复验证

验证：
1. 从 market_time_service 可直接导入 MarketTimeService, is_market_open
2. 从 scheduler_service 仍可导入 is_market_open（向后兼容）
3. MarketTimeService 运行时可实例化
"""

import pytest


class TestMarketTimeServiceImport:
    """验证 market_time_service 模块可直接导入"""

    def test_import_from_market_time_service(self):
        """从 ali2026v3_trading.infra.market_time_service 可导入 MarketTimeService, is_market_open"""
        from ali2026v3_trading.infra.market_time_service import MarketTimeService, is_market_open
        assert MarketTimeService is not None
        assert callable(is_market_open)

    def test_import_time_sync_checker_from_market_time_service(self):
        """从 ali2026v3_trading.infra.market_time_service 可导入 TimeSyncChecker"""
        from ali2026v3_trading.infra.market_time_service import TimeSyncChecker, get_time_sync_checker
        assert TimeSyncChecker is not None
        assert callable(get_time_sync_checker)


class TestBackwardCompatibility:
    """验证 scheduler_service 向后兼容导出"""

    def test_import_is_market_open_from_scheduler_service(self):
        """从 ali2026v3_trading.infra.scheduler_service 仍可导入 is_market_open"""
        from ali2026v3_trading.infra.scheduler_service import is_market_open
        assert callable(is_market_open)

    def test_import_market_time_service_from_scheduler_service(self):
        """从 ali2026v3_trading.infra.scheduler_service 仍可导入 MarketTimeService"""
        from ali2026v3_trading.infra.scheduler_service import MarketTimeService
        assert MarketTimeService is not None

    def test_import_time_sync_checker_from_scheduler_service(self):
        """从 ali2026v3_trading.infra.scheduler_service 仍可导入 TimeSyncChecker"""
        from ali2026v3_trading.infra.scheduler_service import TimeSyncChecker
        assert TimeSyncChecker is not None

    def test_import_get_market_time_service_from_scheduler_service(self):
        """从 ali2026v3_trading.infra.scheduler_service 仍可导入 get_market_time_service"""
        from ali2026v3_trading.infra.scheduler_service import get_market_time_service
        assert callable(get_market_time_service)

    def test_import_get_time_sync_checker_from_scheduler_service(self):
        """从 ali2026v3_trading.infra.scheduler_service 仍可导入 get_time_sync_checker"""
        from ali2026v3_trading.infra.scheduler_service import get_time_sync_checker
        assert callable(get_time_sync_checker)


class TestMarketTimeServiceInstantiation:
    """验证 MarketTimeService 运行时可实例化"""

    def test_market_time_service_instantiable(self):
        """MarketTimeService 可正常实例化"""
        from ali2026v3_trading.infra.market_time_service import MarketTimeService
        svc = MarketTimeService()
        assert svc is not None
        assert hasattr(svc, 'is_market_open')
        assert hasattr(svc, 'is_trading_day')
        assert hasattr(svc, 'holidays')

    def test_market_time_service_is_market_open_callable(self):
        """MarketTimeService 实例的 is_market_open 方法可调用"""
        from ali2026v3_trading.infra.market_time_service import MarketTimeService
        svc = MarketTimeService()
        # 不论当前时间是否在交易时段，调用不应抛异常
        result = svc.is_market_open()
        assert isinstance(result, bool)

    def test_market_time_service_is_trading_day(self):
        """MarketTimeService 实例的 is_trading_day 方法可调用"""
        import datetime
        from ali2026v3_trading.infra.market_time_service import MarketTimeService
        svc = MarketTimeService()
        # 周六不应为交易日
        saturday = datetime.date(2026, 1, 3)  # 2026-01-03 是周六
        assert svc.is_trading_day(saturday) is False

    def test_time_sync_checker_instantiable(self):
        """TimeSyncChecker 可正常实例化"""
        from ali2026v3_trading.infra.market_time_service import TimeSyncChecker
        checker = TimeSyncChecker()
        assert checker is not None
        assert hasattr(checker, 'check_time_sync')
        assert hasattr(checker, 'get_status')


class TestSingleSourceOfTruth:
    """验证同一对象：从两个模块导入的是同一个类/函数"""

    def test_same_market_time_service_class(self):
        """从两个模块导入的 MarketTimeService 是同一个类"""
        from ali2026v3_trading.infra.market_time_service import MarketTimeService as MTS_direct
        from ali2026v3_trading.infra.scheduler_service import MarketTimeService as MTS_compat
        assert MTS_direct is MTS_compat

    def test_same_is_market_open_function(self):
        """从两个模块导入的 is_market_open 是同一个函数"""
        from ali2026v3_trading.infra.market_time_service import is_market_open as imo_direct
        from ali2026v3_trading.infra.scheduler_service import is_market_open as imo_compat
        assert imo_direct is imo_compat

    def test_same_time_sync_checker_class(self):
        """从两个模块导入的 TimeSyncChecker 是同一个类"""
        from ali2026v3_trading.infra.market_time_service import TimeSyncChecker as TSC_direct
        from ali2026v3_trading.infra.scheduler_service import TimeSyncChecker as TSC_compat
        assert TSC_direct is TSC_compat
