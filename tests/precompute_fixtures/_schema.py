# MODULE_ID: M1-152
"""V5 schema definition — minute_data column registry & sync utility."""
from __future__ import annotations

import logging
from typing import Dict

from infra._helpers import get_logger

logger = get_logger(__name__)

SCHEMA_REQUIRED_COLUMNS_V5: Dict[str, str] = {
    "_spread_quality": "INTEGER DEFAULT 0",
    "_option_metadata_quality": "INTEGER DEFAULT 0",
    "days_to_expiry": "INTEGER",
    "_classification_mode": "INTEGER DEFAULT 1",
    "kl_rpd_k": "DOUBLE DEFAULT 0.0",
    "kl_rpd_l": "DOUBLE DEFAULT 0.5",
    "kl_rpd_r": "DOUBLE DEFAULT 0.0",
    "kl_rpd_d": "DOUBLE DEFAULT 0.0",
    "signal_s1": "DOUBLE DEFAULT 0.0",
    "signal_s2": "DOUBLE DEFAULT 0.0",
    "signal_s3": "DOUBLE DEFAULT 0.0",
    "signal_s4": "DOUBLE DEFAULT 0.0",
    "signal_s5": "DOUBLE DEFAULT 0.0",
    "signal_s6": "DOUBLE DEFAULT 0.0",
    "signal_s1_decayed": "DOUBLE DEFAULT 0.0",
    "signal_s2_decayed": "DOUBLE DEFAULT 0.0",
    "signal_s3_decayed": "DOUBLE DEFAULT 0.0",
    "signal_s4_decayed": "DOUBLE DEFAULT 0.0",
    "linkage_s1": "DOUBLE DEFAULT 0.0",
    "linkage_s2": "DOUBLE DEFAULT 0.0",
    "linkage_s3": "DOUBLE DEFAULT 0.0",
    "linkage_s4": "DOUBLE DEFAULT 0.0",
    "l0_raw_state": "INTEGER DEFAULT 2",
    "l0_smoothed_state": "INTEGER DEFAULT 2",
    "l0_state_entropy": "DOUBLE DEFAULT 0.5",
    "hmm_state": "INTEGER DEFAULT 1",
    "hmm_posterior_low": "DOUBLE DEFAULT 0.2",
    "hmm_posterior_normal": "DOUBLE DEFAULT 0.6",
    "hmm_posterior_high": "DOUBLE DEFAULT 0.2",
    "trend_score_short": "DOUBLE DEFAULT 0.0",
    "trend_score_medium": "DOUBLE DEFAULT 0.0",
    "trend_score_long": "DOUBLE DEFAULT 0.0",
    "trend_direction_short": "INTEGER DEFAULT 0",
    "trend_direction_medium": "INTEGER DEFAULT 0",
    "trend_direction_long": "INTEGER DEFAULT 0",
    "directional_bias": "DOUBLE DEFAULT 0.0",
    "resonance_strength": "DOUBLE DEFAULT 0.0",
    "cr_phase": "INTEGER DEFAULT 3",
    "state_entropy": "DOUBLE DEFAULT 0.5",
    "pullback_pct_peak": "DOUBLE DEFAULT 0.0",
    "pullback_pct_entry": "DOUBLE DEFAULT 0.0",
    "pullback_pct_ma": "DOUBLE DEFAULT 0.0",
    "pullback_pct_atr": "DOUBLE DEFAULT 0.0",
    "obos_rsi": "DOUBLE DEFAULT 50.0",
    "obos_stoch_k": "DOUBLE DEFAULT 50.0",
    "obos_cci": "DOUBLE DEFAULT 0.0",
    "obos_williams_r": "DOUBLE DEFAULT -50.0",
    "obos_signal": "DOUBLE DEFAULT 0.0",
    "tvf_trend": "DOUBLE DEFAULT 0.0",
    "tvf_volatility": "DOUBLE DEFAULT 0.5",
    "tvf_flow": "DOUBLE DEFAULT 0.0",
    "tvf_risk": "DOUBLE DEFAULT 0.5",
    "tvf_pullback": "DOUBLE DEFAULT 0.5",
    "tvf_entropy": "DOUBLE DEFAULT 0.5",
    "kelly_fraction": "DOUBLE DEFAULT 0.1",
    "position_suggestion": "DOUBLE DEFAULT 0.0",
    # V5新增：背离反转模块
    "option_moneyness_state": "INTEGER DEFAULT 2",
    "div_future_cross_term": "DOUBLE DEFAULT 0.0",
    "div_option_premium_coll": "DOUBLE DEFAULT 0.0",
    "div_option_near_itm": "DOUBLE DEFAULT 0.0",
    "div_reversal_signal": "DOUBLE DEFAULT 0.0",
}


def sync_db_schema_v5(con) -> None:
    """Detect & add missing V5 columns to minute_data."""
    try:
        cols = con.execute("DESCRIBE minute_data").fetchall()
        existing = {row[0] for row in cols}
        to_add = [(col_name, col_type) for col_name, col_type in SCHEMA_REQUIRED_COLUMNS_V5.items() if col_name not in existing]
        if not to_add:
            return
        con.execute("BEGIN TRANSACTION")
        try:
            for col_name, col_type in to_add:
                con.execute(
                    f"ALTER TABLE minute_data ADD COLUMN {col_name} {col_type}"
                )
                logger.info("[V5-schema] added column %s %s", col_name, col_type)
            con.execute("COMMIT")
            logger.info("[V5-schema] total %d columns added", len(to_add))
        except (OSError, ValueError, RuntimeError) as inner_exc:
            con.execute("ROLLBACK")
            logger.warning("[V5-schema] ALTER TABLE rolled back: %s", inner_exc)
    except (OSError, ValueError, RuntimeError) as exc:
        logger.debug("[V5-schema] sync skipped: %s", exc)