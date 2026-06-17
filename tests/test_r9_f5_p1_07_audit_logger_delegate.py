# MODULE_ID: M2-552
"""R1-F5: P1-07 AuditLogger委托到structured_audit_log 断言测试

验证运行时行为:
1. AuditLogger.log() 调用后，_delegate_to_structured_audit_log 方法存在且可调用
2. AuditLogger.log() 委托调用 structured_audit_log 时传入正确参数
3. 委托失败时不影响AuditLogger自身哈希链完整性
"""
import os
import sys
import tempfile
import pytest

# 确保项目根目录在 sys.path
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_audit_logger_has_delegate_method():
    """P1-07验证: AuditLogger实例拥有_delegate_to_structured_audit_log方法"""
    from ali2026v3_trading.config.config_logging import AuditLogger
    with tempfile.NamedTemporaryFile(suffix='.audit', delete=False) as f:
        log_path = f.name
    try:
        logger = AuditLogger(log_path)
        assert hasattr(logger, '_delegate_to_structured_audit_log'), \
            "AuditLogger应拥有_delegate_to_structured_audit_log方法"
        assert callable(getattr(logger, '_delegate_to_structured_audit_log')), \
            "_delegate_to_structured_audit_log应为可调用方法"
    finally:
        if os.path.exists(log_path):
            os.unlink(log_path)


def test_audit_logger_delegates_to_structured_audit_log():
    """P1-07验证: AuditLogger.log()委托调用structured_audit_log，传入正确参数"""
    from ali2026v3_trading.config.config_logging import AuditLogger

    # 拦截structured_audit_log调用
    captured_calls = []
    _original_sal = None

    try:
        import ali2026v3_trading.risk._utils as _utils_mod
        _original_sal = _utils_mod.structured_audit_log

        def _mock_sal(event_type, action, details=None, severity="INFO"):
            captured_calls.append({
                'event_type': event_type,
                'action': action,
                'details': details,
                'severity': severity,
            })

        _utils_mod.structured_audit_log = _mock_sal

        with tempfile.NamedTemporaryFile(suffix='.audit', delete=False) as f:
            log_path = f.name
        try:
            logger = AuditLogger(log_path)
            logger.log('test_event', {'key': 'value'})

            assert len(captured_calls) >= 1, \
                f"AuditLogger.log()应至少委托调用structured_audit_log一次，实际调用{len(captured_calls)}次"
            call = captured_calls[0]
            assert call['event_type'] == 'test_event', \
                f"委托event_type应为'test_event'，实际为'{call['event_type']}'"
            assert call['action'] == 'AuditLogger.delegated', \
                f"委托action应为'AuditLogger.delegated'，实际为'{call['action']}'"
            assert call['details'] == {'key': 'value'}, \
                f"委托details应为{{'key': 'value'}}，实际为{call['details']}"
        finally:
            if os.path.exists(log_path):
                os.unlink(log_path)
    finally:
        if _original_sal is not None:
            _utils_mod.structured_audit_log = _original_sal


def test_audit_logger_hash_chain_intact_when_delegate_fails():
    """P1-07验证: 委托失败时AuditLogger哈希链仍完整"""
    from ali2026v3_trading.config.config_logging import AuditLogger

    # 让structured_audit_log抛异常
    _original_sal = None
    try:
        import ali2026v3_trading.risk._utils as _utils_mod
        _original_sal = _utils_mod.structured_audit_log

        def _failing_sal(event_type, action, details=None, severity="INFO"):
            raise RuntimeError("模拟委托失败")

        _utils_mod.structured_audit_log = _failing_sal

        with tempfile.NamedTemporaryFile(suffix='.audit', delete=False) as f:
            log_path = f.name
        try:
            logger = AuditLogger(log_path)
            # 连续写入3条，委托失败不应影响哈希链
            hash1 = logger.log('event1', {'idx': 1})
            hash2 = logger.log('event2', {'idx': 2})
            hash3 = logger.log('event3', {'idx': 3})

            # 验证哈希链完整性
            is_valid, count = AuditLogger.verify_chain(log_path)
            assert is_valid, "委托失败时哈希链应仍完整"
            assert count == 3, f"应记录3条日志，实际{count}条"

            # 验证哈希值递进
            assert hash1 != hash2 != hash3, "连续日志的哈希值应不同"
        finally:
            if os.path.exists(log_path):
                os.unlink(log_path)
    finally:
        if _original_sal is not None:
            _utils_mod.structured_audit_log = _original_sal


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
