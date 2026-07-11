# MODULE_ID: M1-224
"""
risk_engine/__init__.py - 风控引擎入口（re-export层）

所有符号从risk_engine.py重导出，保持向后兼容。
"""

from ali2026v3_trading.risk_engine.risk_engine import (
    # snapshot
    RiskSnapshot,
    # log_deduplicator
    LogDeduplicator,
    # abnormal_trade_detector
    AbnormalTradeDetector,
    # input_builder
    build_snapshot,
    # result_handler
    record_result,
    record_and_publish,
    record_block_with_context,
    # shared_checks
    check_position_limit,
    check_governance_violations,
    check_margin_sufficiency,
    check_cross_instrument_limit,
    update_margin_ratio_override,
    check_regulatory_risks,
    # market_risk
    check_risk_ratio,
    check_greeks_limits,
    check_life_expectancy,
    check_spread_degradation,
    check_exchange_status,
    check_price_limit,
    check_expiry_risk,
    check_auction_session,
    check_market_risks,
    # counterparty_risk
    check_e13_collusion,
    check_strategy_health,
    check_counterparty_risks,
    # operational_risk
    check_strategy_status,
    check_rate_limit,
    check_consecutive_loss_protection,
    check_invariant_runtime,
    check_signal_validity,
    check_single_trade_risk,
    check_sharpe_iron_rule,
    check_e7_residual_block,
    check_capital_sufficiency_in_trade,
    check_shadow_ev,
    check_operational_risks,
    # engine
    RiskEngine,
)

__all__ = [
    "RiskSnapshot",
    "LogDeduplicator",
    "AbnormalTradeDetector",
    "build_snapshot",
    "record_result",
    "record_and_publish",
    "record_block_with_context",
    "check_position_limit",
    "check_governance_violations",
    "check_margin_sufficiency",
    "check_cross_instrument_limit",
    "update_margin_ratio_override",
    "check_regulatory_risks",
    "check_risk_ratio",
    "check_greeks_limits",
    "check_life_expectancy",
    "check_spread_degradation",
    "check_exchange_status",
    "check_price_limit",
    "check_expiry_risk",
    "check_auction_session",
    "check_market_risks",
    "check_e13_collusion",
    "check_strategy_health",
    "check_counterparty_risks",
    "check_strategy_status",
    "check_rate_limit",
    "check_consecutive_loss_protection",
    "check_invariant_runtime",
    "check_signal_validity",
    "check_single_trade_risk",
    "check_sharpe_iron_rule",
    "check_e7_residual_block",
    "check_capital_sufficiency_in_trade",
    "check_shadow_ev",
    "check_operational_risks",
    "RiskEngine",
]
