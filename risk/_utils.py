# MODULE_ID: M1-209
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.shared_utils import safe_float, safe_int, safe_get_float, safe_get_int  # P1-20: 统一类型转换


def _get_tz_aware_now() -> datetime:
    return datetime.now(_CHINA_TZ)


# P1-21: api_version统一从infra.shared_utils导入
from ali2026v3_trading.infra.shared_utils import api_version


def structured_audit_log(event_type: str, action: str, details: Dict[str, Any] = None,
                         severity: str = "INFO") -> None:
    try:
        from ali2026v3_trading.risk.risk_service import audit_chain_append
        audit_chain_append(event_type, {'action': action, 'details': details or {}, 'severity': severity})
    except ImportError:
        logging.debug("[structured_audit_log] audit_chain unavailable, skipping log for %s/%s", event_type, action)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[structured_audit_log] log failed: %s", e)