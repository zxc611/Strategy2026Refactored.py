"""risk_audit_utils.py — 重导出模块(归档后兼容)"""
from ali2026v3_trading.archive.audit_rounds.risk_audit_utils import *
from ali2026v3_trading.archive.audit_rounds.risk_audit_utils import _ALERT_LEVEL_LOG_MAP, _ALERT_LEVEL_TIMEOUT_SEC, _CHINA_TZ, _alert_deduplicator, _alert_deduplicator_lock, _alert_escalation_lock, _alert_escalation_tracker, _audit_chain_last_hash, _audit_chain_lock, _audit_log_lock, _audit_log_path, _check_alert_escalation, _get_audit_log_path, _normalize_risk_scope_id, _risk_service_instances, _risk_service_lock, _state_snapshot_lock
from typing import Optional, Any

def _normalize_risk_scope_id(scope_id: Optional[str] = None, strategy: Any = None) -> str:
    return scope_id or 'default'