"""
test_infra_archived_activation.py — 归档模块激活测试

验证从infra/归档到tests/infra_archived/的4个模块可正常导入和执行核心功能。
"""
import pytest


class TestOpsFrameworkActivation:
    def test_ops_operation_importable(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import OpsOperation
        assert OpsOperation is not None

    def test_ops_operation_manager_importable(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import OpsOperationManager
        mgr = OpsOperationManager()
        assert mgr is not None

    def test_ops_operation_manager_get_singleton(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import get_ops_operation_manager
        mgr = get_ops_operation_manager()
        assert mgr is not None

    def test_ops_operation_execute_lifecycle(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import (
            OpsOperation, OpsOperationManager,
        )
        executed = [False]

        def my_execute():
            executed[0] = True
            return {'success': True}

        op = OpsOperation(
            operation_id='test-001',
            operation_type='test',
            execute=my_execute,
        )
        mgr = OpsOperationManager()
        result = mgr.execute_operation(op)
        assert result['success'] is True
        assert executed[0] is True

    def test_ops_operation_dry_run(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import (
            OpsOperation, OpsOperationManager,
        )
        op = OpsOperation(
            operation_id='test-dry-001',
            operation_type='test',
            execute=lambda: {'success': True},
        )
        mgr = OpsOperationManager()
        result = mgr.execute_operation(op, dry_run=True)
        assert result['dry_run'] is True

    def test_ops_operation_idempotency(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import (
            OpsOperation, OpsOperationManager,
        )
        op = OpsOperation(
            operation_id='test-idem-001',
            operation_type='test',
            execute=lambda: {'success': True},
        )
        mgr = OpsOperationManager()
        r1 = mgr.execute_operation(op)
        r2 = mgr.execute_operation(op)
        assert r1['success'] is True
        assert r2['success'] is True
        assert '幂等' in r2.get('error', '')

    def test_ops_operation_rollback(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import (
            OpsOperation, OpsOperationManager,
        )
        rolled_back = [False]

        def failing_execute():
            return {'success': False, 'error': 'simulated'}

        def my_rollback(snapshot):
            rolled_back[0] = True

        op = OpsOperation(
            operation_id='test-rollback-001',
            operation_type='test',
            execute=failing_execute,
            rollback=my_rollback,
        )
        mgr = OpsOperationManager()
        result = mgr.execute_operation(op)
        assert result['success'] is False
        assert rolled_back[0] is True

    def test_ops_operation_approval(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import (
            OpsOperation, OpsOperationManager,
        )
        mgr = OpsOperationManager()
        token = mgr.request_approval('test-approve-001', 'test', approver='admin')
        assert token is not None
        assert mgr.check_approval('test-approve-001', token) is True
        assert mgr.check_approval('test-approve-001', 'wrong') is False

    def test_ops_operation_audit_log(self):
        from ali2026v3_trading.tests.infra_archived._ops_framework import (
            OpsOperation, OpsOperationManager,
        )
        op = OpsOperation(
            operation_id='test-audit-001',
            operation_type='test',
            execute=lambda: {'success': True},
        )
        mgr = OpsOperationManager()
        mgr.execute_operation(op)
        log = mgr.get_audit_log(last_n=10)
        assert isinstance(log, list)


class TestOpsServiceActivation:
    def test_ops_service_importable(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import (
            OpsOperation, OpsOperationManager, get_ops_operation_manager,
            OpsMetrics, OpsSLA, search_ops_knowledge,
            ops_health_check, ops_run_diagnostic,
            CapacityStressTestFramework, get_capacity_stress_test,
            StandardOperatingProcedures, get_sop_manager,
        )

    def test_ops_metrics_constants(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import OpsMetrics
        assert hasattr(OpsMetrics, 'METRIC_CPU_USAGE_PCT')
        assert hasattr(OpsMetrics, 'METRIC_MEMORY_USAGE_PCT')

    def test_ops_sla_constants(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import OpsSLA
        assert OpsSLA.AVAILABILITY_TARGET_PCT == 99.9
        assert OpsSLA.INCIDENT_RESPONSE_TIME_MIN == 15

    def test_search_ops_knowledge(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import search_ops_knowledge
        results = search_ops_knowledge('磁盘')
        assert isinstance(results, list)

    def test_capacity_stress_test(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import CapacityStressTestFramework
        framework = CapacityStressTestFramework()
        framework.define_capacity_limits({'max_qps': 1000.0})
        result = framework.run_stress_test('test-1', duration_sec=1.0, target_qps=100.0)
        assert 'test_name' in result

    def test_sop_manager(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import get_sop_manager
        sop = get_sop_manager()
        procedures = sop.list_procedures()
        assert isinstance(procedures, dict)
        assert 'emergency_close_all' in procedures

    def test_sop_execute(self):
        from ali2026v3_trading.tests.infra_archived.ops_service import get_sop_manager
        sop = get_sop_manager()
        result = sop.execute_sop(
            'emergency_close_all',
            executor='test_admin',
            approval_context={'approver': 'test_admin', 'reason': '演练'},
        )
        assert result['success'] is True


class TestStorageActivation:
    def test_storage_catalog_service_importable(self):
        from ali2026v3_trading.tests.infra_archived._storage import StorageCatalogService
        assert StorageCatalogService is not None

    def test_storage_catalog_service_instantiation(self):
        from ali2026v3_trading.tests.infra_archived._storage import StorageCatalogService
        svc = StorageCatalogService(manager=None)
        assert svc is not None

    def test_storage_catalog_mixin_alias(self):
        from ali2026v3_trading.tests.infra_archived._storage import (
            StorageCatalogService, StorageCatalogMixin,
        )
        assert StorageCatalogMixin is StorageCatalogService

    def test_storage_checks_service_importable(self):
        from ali2026v3_trading.tests.infra_archived._storage import StorageChecksService
        assert StorageChecksService is not None

    def test_storage_maintenance_service_importable(self):
        from ali2026v3_trading.tests.infra_archived._storage import StorageMaintenanceService
        assert StorageMaintenanceService is not None

    def test_option_format_template(self):
        from ali2026v3_trading.tests.infra_archived._storage import StorageCatalogService
        template_cffex = StorageCatalogService._get_option_format_template('CFFEX')
        assert template_cffex == 'YYYYMM-C-XXXX'
        template_shfe = StorageCatalogService._get_option_format_template('SHFE')
        assert '{product}' in template_shfe


class TestOpsAutomationActivation:
    def test_ops_automation_importable(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import (
            ops_health_check, ops_run_diagnostic, ops_auto_repair,
            generate_ops_report, estimate_ops_cost,
            CapacityStressTestFramework, get_capacity_stress_test,
            StandardOperatingProcedures, get_sop_manager,
        )

    def test_ops_health_check(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import ops_health_check
        result = ops_health_check()
        assert 'overall_status' in result
        assert 'components' in result

    def test_ops_run_diagnostic(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import ops_run_diagnostic
        result = ops_run_diagnostic(category='all')
        assert 'timestamp' in result
        assert 'findings' in result

    def test_ops_auto_repair_unknown(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import ops_auto_repair
        result = ops_auto_repair('unknown_issue')
        assert 'repaired' in result

    def test_generate_ops_report(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import generate_ops_report
        report = generate_ops_report('daily')
        assert 'report_type' in report
        assert 'sections' in report

    def test_estimate_ops_cost(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import estimate_ops_cost
        cost = estimate_ops_cost()
        assert 'storage_cost' in cost
        assert 'compute_cost' in cost

    def test_capacity_stress_test_framework(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import CapacityStressTestFramework
        fw = CapacityStressTestFramework()
        fw.define_capacity_limits({'max_qps': 500.0})
        result = fw.run_stress_test('activation-test', duration_sec=1.0)
        assert 'test_name' in result
        report = fw.get_stress_test_report()
        assert 'total_tests' in report

    def test_sop_manager_activation(self):
        from ali2026v3_trading.tests.infra_archived._ops_automation import get_sop_manager
        sop = get_sop_manager()
        procs = sop.list_procedures()
        assert len(procs) > 0
        result = sop.execute_sop(
            'emergency_stop',
            executor='activation_test',
            approval_context={'approver': 'system', 'reason': 'activation'},
        )
        assert result['success'] is True