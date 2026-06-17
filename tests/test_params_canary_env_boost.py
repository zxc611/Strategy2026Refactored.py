# MODULE_ID: M2-460
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from ali2026v3_trading.governance.mode_config import TakeProfitMethod, StopLossMethod, DrawdownAction
from ali2026v3_trading.config._params_canary_env import (
    get_canary_config, update_canary_config, should_apply_canary,
    hot_update_with_canary, get_env_profile, diff_env_profiles,
    archive_params, list_param_archives,
    _CANARY_DEPLOYMENT_CONFIG, _ENV_CONFIG_PROFILES,
)


class TestCanaryConfig:
    def setup_method(self):
        update_canary_config(enabled=False, canary_ratio=0.1, canary_instruments=[])

    def test_get_canary_config_returns_copy(self):
        cfg = get_canary_config()
        assert cfg['enabled'] is False
        cfg['enabled'] = True
        assert get_canary_config()['enabled'] is False

    def test_update_canary_config_known_key(self):
        update_canary_config(enabled=True)
        assert get_canary_config()['enabled'] is True

    def test_update_canary_config_unknown_key(self):
        update_canary_config(unknown_key=123)

    def test_should_apply_canary_disabled(self):
        update_canary_config(enabled=False)
        assert should_apply_canary("IF2606") is False

    def test_should_apply_canary_with_instrument_list(self):
        update_canary_config(enabled=True, canary_instruments=['IF2606'])
        assert should_apply_canary("IF2606") is True
        assert should_apply_canary("IH2606") is False

    def test_should_apply_canary_ratio_based(self):
        update_canary_config(enabled=True, canary_instruments=[], canary_ratio=1.0)
        assert should_apply_canary("IF2606") is True

    def test_should_apply_canary_ratio_zero(self):
        update_canary_config(enabled=True, canary_instruments=[], canary_ratio=0.0)
        assert should_apply_canary("IF2606") is False


class TestHotUpdateWithCanary:
    def setup_method(self):
        update_canary_config(enabled=False)

    def test_hot_update_not_canary(self):
        params = {'key1': 'val1'}
        result = hot_update_with_canary(params, {'key1': 'new_val'}, instrument_id="IH2606")
        assert result['applied'] is False
        assert result['canary'] is False
        assert result['updated_keys'] == []

    def test_hot_update_is_canary(self):
        update_canary_config(enabled=True, canary_instruments=['IF2606'], canary_ratio=1.0)
        params = {'key1': 'val1', 'key2': 'val2'}
        result = hot_update_with_canary(params, {'key1': 'new_val'}, instrument_id="IF2606")
        assert result['applied'] is True
        assert result['canary'] is True
        assert 'key1' in result['updated_keys']

    def test_hot_update_unknown_key(self):
        update_canary_config(enabled=True, canary_instruments=['IF2606'], canary_ratio=1.0)
        params = {'key1': 'val1'}
        result = hot_update_with_canary(params, {'unknown': 'val'}, instrument_id="IF2606")
        assert result['applied'] is True
        assert result['updated_keys'] == []


class TestEnvProfiles:
    def test_get_env_profile_development(self):
        profile = get_env_profile('development')
        assert profile['log_level'] == 'DEBUG'
        assert profile['max_position_limit'] == 10

    def test_get_env_profile_production(self):
        profile = get_env_profile('production')
        assert profile['log_level'] == 'WARNING'
        assert profile['max_position_limit'] == 100

    def test_get_env_profile_unknown(self):
        profile = get_env_profile('staging')
        assert profile == {}

    def test_diff_env_profiles(self):
        diff = diff_env_profiles('development', 'production')
        assert 'value_diffs' in diff
        assert 'log_level' in diff['value_diffs']
        assert diff['value_diffs']['log_level']['env1'] == 'DEBUG'
        assert diff['value_diffs']['log_level']['env2'] == 'WARNING'

    def test_diff_env_profiles_same(self):
        diff = diff_env_profiles('development', 'development')
        assert diff['value_diffs'] == {}

    def test_diff_env_profiles_unknown(self):
        diff = diff_env_profiles('unknown1', 'unknown2')
        assert diff['only_in_env1'] == []
        assert diff['value_diffs'] == {}


class TestArchiveParams:
    def test_list_param_archives_nonexistent_dir(self):
        result = list_param_archives(archive_dir='/nonexistent_dir_for_test_12345')
        assert result == []

    def test_archive_params_success(self):
        import tempfile, shutil
        tmpdir = tempfile.mkdtemp()
        try:
            result = archive_params({'key': 'val'}, archive_dir=tmpdir)
            assert result is not None
            assert result.endswith('.json')
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)