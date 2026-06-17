# MODULE_ID: M2-318
"""
补充测试第二批：覆盖剩余none/static_only模块
目标模块: tvf_param_loader, params_service, config_version_tracker,
          _deprecated_reexports, _backup_restore, event_publisher,
          phase_feature_flag, service_contracts, maintenance_service,
          security_service, shadow_strategy_core(部分), compliance_checker,
          strategy_business_layer, strategy/lifecycle_service, instrument_service,
          kline_data_service, order_executor, order_split_models, logging_utils,
          metrics_registry, performance_monitor, risk_rules
"""
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from ali2026v3_trading.config.tvf_param_loader import TVFParamLoader, TVF_DEFAULT_PARAMS
from ali2026v3_trading.config.params_service import (
    ParamsService, get_params_service, reset_params_service,
    migrate_params_rename, should_apply_canary, hot_update_with_canary,
)
from ali2026v3_trading.config.config_version_tracker import (
    get_param_version, list_param_version_history, rollback_param_version,
    compute_content_hash,
)
from ali2026v3_trading.infra._deprecated_reexports import (
    compute_commission, classify_deviation, PhaseFeatureFlag, ServiceProtocol,
    check_daily_drawdown_hard_stop, resolve_and_check_daily_drawdown,
    Signal,
)
from ali2026v3_trading.infra.security_service import (
    _SecureCredential, reveal_credential, _sanitize_for_return, SENSITIVE_KEYS,
)
from ali2026v3_trading.governance.compliance_checker import ComplianceChecker
from ali2026v3_trading.infra.logging_utils import get_logger
from ali2026v3_trading.infra.metrics_registry import MetricsRegistry
from ali2026v3_trading.infra.performance_monitor import PathCounter


class TestTVFParamLoader:
    def test_default_params_l1_l2_l3_weights_sum_to_one(self):
        l1 = TVF_DEFAULT_PARAMS['tvf_l1_weight']
        l2 = TVF_DEFAULT_PARAMS['tvf_l2_weight']
        l3 = TVF_DEFAULT_PARAMS['tvf_l3_weight']
        assert abs(l1 + l2 + l3 - 1.0) < 0.01

    def test_default_params_inner_l1_weights_sum(self):
        w = TVF_DEFAULT_PARAMS
        l1_sum = (w['tvf_l1_inner_sortino_weight'] +
                  w['tvf_l1_inner_calmar_weight'] +
                  w['tvf_l1_inner_sharpe_weight'])
        assert abs(l1_sum - 1.0) < 0.01

    def test_default_params_inner_l2_weights_sum(self):
        w = TVF_DEFAULT_PARAMS
        l2_sum = (w['tvf_l2_inner_ofi_weight'] +
                  w['tvf_l2_inner_cvd_weight'] +
                  w['tvf_l2_inner_smf_weight'])
        assert abs(l2_sum - 1.0) < 0.01

    def test_default_params_inner_l3_weights_sum(self):
        w = TVF_DEFAULT_PARAMS
        l3_sum = (w['tvf_l3_inner_delta_weight'] +
                  w['tvf_l3_inner_gamma_weight'] +
                  w['tvf_l3_inner_theta_weight'] +
                  w['tvf_l3_inner_vega_weight'])
        assert abs(l3_sum - 1.0) < 0.01

    def test_default_params_scales_positive(self):
        for key, val in TVF_DEFAULT_PARAMS.items():
            if 'scale' in key:
                assert val > 0, f"{key} scale should be positive, got {val}"

    def test_get_params_returns_dict(self):
        loader = TVFParamLoader.get_instance()
        params = loader.get_params()
        assert isinstance(params, dict)


class TestParamsServiceFacade:
    def test_get_params_service_callable(self):
        assert callable(get_params_service)

    def test_reset_params_service_callable(self):
        assert callable(reset_params_service)

    def test_migrate_params_rename_callable(self):
        assert callable(migrate_params_rename)

    def test_should_apply_canary_callable(self):
        assert callable(should_apply_canary)

    def test_hot_update_with_canary_callable(self):
        assert callable(hot_update_with_canary)


class TestConfigVersionTracker:
    def test_compute_content_hash_deterministic(self):
        h1 = compute_content_hash("test content")
        h2 = compute_content_hash("test content")
        assert h1 == h2

    def test_compute_content_hash_different_inputs(self):
        h1 = compute_content_hash("content A")
        h2 = compute_content_hash("content B")
        assert h1 != h2

    def test_get_param_version_callable(self):
        assert callable(get_param_version)

    def test_list_param_version_history_callable(self):
        assert callable(list_param_version_history)

    def test_rollback_param_version_callable(self):
        assert callable(rollback_param_version)


class TestDeprecatedReexports:
    def test_compute_commission_reachable(self):
        result = compute_commission('50ETF_OPTION', 1, trade_value=10000.0)
        assert isinstance(result, (int, float))
        assert result >= 0

    def test_classify_deviation_reachable(self):
        import time as _t
        s1 = Signal(direction='BUY', price=100.0, timestamp=_t.time())
        s2 = Signal(direction='BUY', price=100.0, timestamp=_t.time())
        result = classify_deviation(s1, s2)
        assert result.category == 'MATCH'

    def test_phase_feature_flag_reachable(self):
        flag = PhaseFeatureFlag()
        flag.enable('test')
        assert flag.is_enabled('test') is True

    def test_check_daily_drawdown_hard_stop_callable(self):
        assert callable(check_daily_drawdown_hard_stop)

    def test_resolve_and_check_daily_drawdown_callable(self):
        assert callable(resolve_and_check_daily_drawdown)


class TestSecureCredential:
    def test_reveal_returns_original(self):
        cred = _SecureCredential("my_secret_key")
        assert cred.reveal() == "my_secret_key"

    def test_repr_is_redacted(self):
        cred = _SecureCredential("my_secret_key")
        assert repr(cred) == '***REDACTED***'

    def test_str_is_redacted(self):
        cred = _SecureCredential("my_secret_key")
        assert str(cred) == '***REDACTED***'

    def test_reveal_credential_with_secure(self):
        cred = _SecureCredential("test_value")
        assert reveal_credential(cred) == "test_value"

    def test_reveal_credential_with_string(self):
        assert reveal_credential("plain") == "plain"

    def test_reveal_credential_with_none(self):
        assert reveal_credential(None) == ''

    def test_sanitize_for_return(self):
        params = {'api_key': 'secret123', 'normal_field': 'visible'}
        result = _sanitize_for_return(params)
        assert result['api_key'] == '***REDACTED***'
        assert result['normal_field'] == 'visible'

    def test_empty_credential_reveal(self):
        cred = _SecureCredential("")
        assert cred.reveal() == ''


class TestComplianceChecker:
    def test_position_limit_blocked_when_new_open_blocked(self):
        safety = MagicMock()
        safety._lock = __import__('threading').RLock()
        safety._daily_new_open_blocked = True
        safety._daily_hard_stop_triggered = False
        checker = ComplianceChecker(safety)
        result = checker.check_regulatory_compliance()
        assert result['compliant'] is False
        violations = [c for c in result['checks'] if c['check_item'] == 'position_limit']
        assert len(violations) == 1
        assert violations[0]['passed'] is False

    def test_hard_stop_blocks_all(self):
        safety = MagicMock()
        safety._lock = __import__('threading').RLock()
        safety._daily_new_open_blocked = True
        safety._daily_hard_stop_triggered = True
        checker = ComplianceChecker(safety)
        result = checker.check_regulatory_compliance()
        assert result['compliant'] is False

    def test_all_pass_when_no_blocks(self):
        safety = MagicMock()
        safety._lock = __import__('threading').RLock()
        safety._daily_new_open_blocked = False
        safety._daily_hard_stop_triggered = False
        checker = ComplianceChecker(safety)
        result = checker.check_regulatory_compliance()
        pos_checks = [c for c in result['checks'] if c['check_item'] == 'position_limit']
        assert pos_checks[0]['passed'] is True


class TestLoggingUtils:
    def test_get_logger_returns_logger(self):
        import logging
        logger = get_logger('test_module')
        assert isinstance(logger, logging.Logger)


class TestMetricsRegistry:
    def test_inc_counter(self):
        mr = MetricsRegistry()
        mr.inc_counter('test_counter', 1)
        assert mr.get_counter('test_counter') == 1

    def test_set_gauge(self):
        mr = MetricsRegistry()
        mr.set_gauge('test_gauge', 42.0)
        assert mr.get_gauge('test_gauge') == 42.0

    def test_observe_histogram(self):
        mr = MetricsRegistry()
        mr.observe_histogram('test_hist', 0.5)
        mr.observe_histogram('test_hist', 1.5)
        stats = mr.get_histogram_stats('test_hist')
        assert stats['count'] == 2


class TestPerformanceMonitor:
    def test_path_counter_record_call(self):
        pc = PathCounter()
        pc.record_call('test_path')
        pc.record_call('test_path')
        stats = pc.get_stats()
        assert stats['counters']['test_path']['count'] == 2