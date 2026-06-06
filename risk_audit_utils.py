"""risk_audit_utils.py — 重导出模块(归档后兼容)"""
from ali2026v3_trading.archive.audit_rounds.risk_audit_utils import *
from typing import Optional, Any

def _normalize_risk_scope_id(scope_id: Optional[str] = None, strategy: Any = None) -> str:
    return scope_id or 'default'