# MODULE_ID: M2-363
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestAuditPassport:
    def test_import(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import AuditPassport
        assert AuditPassport is not None

    def test_certified_result_import(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import CertifiedResult
        assert CertifiedResult is not None

    def test_sandbox_execution_auditor_import(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import SandboxExecutionAuditor
        assert SandboxExecutionAuditor is not None

    def test_auto_field_lineage_tracker_import(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import AutoFieldLineageTracker
        assert AutoFieldLineageTracker is not None

    def test_restricted_exec_loader_import(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import RestrictedExecLoader
        assert RestrictedExecLoader is not None


class TestRestrictedExecLoader:
    def test_safe_exec_simple(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import RestrictedExecLoader
        ns = {}
        RestrictedExecLoader.safe_exec("x = 1 + 2", ns)
        assert ns.get('x') == 3

    def test_safe_exec_blocked_builtin(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import RestrictedExecLoader
        ns = {}
        with pytest.raises(Exception):
            RestrictedExecLoader.safe_exec("__import__('os').system('echo')", ns)

    def test_allowed_modules(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import RestrictedExecLoader
        assert 'pandas' in RestrictedExecLoader.ALLOWED_MODULES
        assert 'numpy' in RestrictedExecLoader.ALLOWED_MODULES


class TestSandboxExecutionAuditor:
    def test_create_poisoned_tick_stream(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import SandboxExecutionAuditor
        auditor = SandboxExecutionAuditor(engine_class=dict)
        stream = auditor.create_poisoned_tick_stream(n_ticks=100, poison_tick_idx=50)
        assert isinstance(stream, tuple)


class TestCertifiedResult:
    def test_init(self):
        from ali2026v3_trading.param_pool.l1_quantification.meta_audit_passport import CertifiedResult
        cr = CertifiedResult(original_result={"sharpe": 1.5}, passport_info={"version": "2.2"})
        assert cr.original == {"sharpe": 1.5}