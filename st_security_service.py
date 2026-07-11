# MODULE_ID: M2-569
"""测试 infra/security_service.py — _SecureCredential + reveal"""
import os

import pytest


# ============================================================
# _SecureCredential
# ============================================================

class TestSecureCredential:
    """_SecureCredential: 凭据混淆与还原"""

    def test_reveal_roundtrip(self):
        """混淆后reveal还原出原始值"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "my_api_key_12345"
        cred = _SecureCredential(secret)
        assert cred.reveal() == secret

    def test_reveal_empty_string(self):
        """空字符串"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        cred = _SecureCredential("")
        assert cred.reveal() == ""

    def test_reveal_long_string(self):
        """长字符串"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "A" * 1000
        cred = _SecureCredential(secret)
        assert cred.reveal() == secret

    def test_reveal_unicode(self):
        """Unicode字符串"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "密钥🔑测试"
        cred = _SecureCredential(secret)
        assert cred.reveal() == secret

    def test_reveal_special_chars(self):
        """特殊字符"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "key!@#$%^&*()_+-=[]{}|;':\",./<>?"
        cred = _SecureCredential(secret)
        assert cred.reveal() == secret

    def test_repr_redacted(self):
        """repr不泄露凭据"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "super_secret_key"
        cred = _SecureCredential(secret)
        assert '***REDACTED***' in repr(cred)
        assert secret not in repr(cred)

    def test_str_redacted(self):
        """str不泄露凭据"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "super_secret_key"
        cred = _SecureCredential(secret)
        assert '***REDACTED***' in str(cred)
        assert secret not in str(cred)

    def test_different_seeds_different_obfuscation(self):
        """不同实例产生不同混淆结果"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        cred1 = _SecureCredential("same_value")
        cred2 = _SecureCredential("same_value")
        # 不同seed导致不同混淆
        assert cred1._obfuscated != cred2._obfuscated
        # 但都能还原
        assert cred1.reveal() == "same_value"
        assert cred2.reveal() == "same_value"

    def test_obfuscation_is_not_plaintext(self):
        """混淆结果不是明文"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        secret = "plaintext_value"
        cred = _SecureCredential(secret)
        assert cred._obfuscated != secret.encode('utf-8')

    def test_slots_no_dict(self):
        """__slots__防止意外属性"""
        from ali2026v3_trading.infra.security_service import _SecureCredential
        cred = _SecureCredential("test")
        with pytest.raises(AttributeError):
            cred.unexpected_attr = "value"


# ============================================================
# reveal_credential
# ============================================================

class TestRevealCredential:
    """reveal_credential: 辅助函数"""

    def test_reveal_secure_credential(self):
        """reveal_credential处理器SecureCredential"""
        from ali2026v3_trading.infra.security_service import _SecureCredential, reveal_credential
        secret = "test_key"
        cred = _SecureCredential(secret)
        assert reveal_credential(cred) == secret

    def test_reveal_string(self):
        """reveal_credential处理普通字符串"""
        from ali2026v3_trading.infra.security_service import reveal_credential
        assert reveal_credential("plain_text") == "plain_text"

    def test_reveal_none(self):
        """reveal_credential处理None"""
        from ali2026v3_trading.infra.security_service import reveal_credential
        assert reveal_credential(None) == ''

    def test_reveal_integer(self):
        """reveal_credential处理整数"""
        from ali2026v3_trading.infra.security_service import reveal_credential
        assert reveal_credential(123) == '123'

    def test_reveal_zero(self):
        """reveal_credential处理0(falsy返回空字符串)"""
        from ali2026v3_trading.infra.security_service import reveal_credential
        assert reveal_credential(0) == ''


# ============================================================
# _sanitize_for_return
# ============================================================

class TestSanitizeForReturn:
    """_sanitize_for_return: 敏感参数脱敏"""

    def test_redacts_sensitive_keys(self):
        """敏感key被替换为***REDACTED***"""
        from ali2026v3_trading.infra.security_service import _sanitize_for_return
        params = {'api_key': 'secret123', 'name': 'test'}
        result = _sanitize_for_return(params)
        assert result['api_key'] == '***REDACTED***'
        assert result['name'] == 'test'

    def test_all_sensitive_keys_redacted(self):
        """所有SENSITIVE_KEYS都被脱敏"""
        from ali2026v3_trading.infra.security_service import _sanitize_for_return, SENSITIVE_KEYS
        params = {k: 'value' for k in SENSITIVE_KEYS}
        params['safe_key'] = 'visible'
        result = _sanitize_for_return(params)
        for k in SENSITIVE_KEYS:
            assert result[k] == '***REDACTED***'
        assert result['safe_key'] == 'visible'

    def test_non_sensitive_keys_preserved(self):
        """非敏感key原样保留"""
        from ali2026v3_trading.infra.security_service import _sanitize_for_return
        params = {'normal_key': [1, 2, 3], 'nested': {'a': 1}}
        result = _sanitize_for_return(params)
        assert result['normal_key'] == [1, 2, 3]
        assert result['nested'] == {'a': 1}


# ============================================================
# _validate_path_safety
# ============================================================

class TestValidatePathSafety:
    """_validate_path_safety: 路径安全检查"""

    def test_valid_path_within_base(self):
        """合法路径通过"""
        from ali2026v3_trading.infra.security_service import _validate_path_safety, _ALLOWED_BASE_DIRS
        base = _ALLOWED_BASE_DIRS[0]
        test_file = os.path.join(base, 'test.txt')
        result = _validate_path_safety(test_file)
        assert result.endswith('test.txt')

    def test_path_traversal_rejected(self):
        """路径遍历被拒绝"""
        from ali2026v3_trading.infra.security_service import _validate_path_safety
        with pytest.raises(ValueError, match="Path traversal detected"):
            _validate_path_safety("/etc/passwd", allowed_base_dirs=["/safe/dir"])


# ============================================================
# SecurityEventResponder
# ============================================================

class TestSecurityEventResponder:
    """SecurityEventResponder: 安全事件响应"""

    def test_report_suspicious_below_threshold(self):
        """可疑报告未达阈值不阻断"""
        from ali2026v3_trading.infra.security_service import SecurityEventResponder
        resp = SecurityEventResponder()
        result = resp.report_suspicious("source1", "test reason")
        assert result is False
        assert not resp.is_blocked("source1")

    def test_report_suspicious_reaches_threshold(self):
        """可疑报告达到阈值后阻断"""
        from ali2026v3_trading.infra.security_service import SecurityEventResponder
        resp = SecurityEventResponder()
        resp.report_suspicious("source1", "reason1")
        resp.report_suspicious("source1", "reason2")
        result = resp.report_suspicious("source1", "reason3")
        assert result is True
        assert resp.is_blocked("source1")

    def test_unblock_clears_source(self):
        """unblock清除阻断"""
        from ali2026v3_trading.infra.security_service import SecurityEventResponder
        resp = SecurityEventResponder()
        for i in range(3):
            resp.report_suspicious("source1", f"reason{i}")
        assert resp.is_blocked("source1")
        resp.unblock("source1")
        assert not resp.is_blocked("source1")

    def test_different_sources_independent(self):
        """不同source独立计数"""
        from ali2026v3_trading.infra.security_service import SecurityEventResponder
        resp = SecurityEventResponder()
        resp.report_suspicious("source1", "reason")
        resp.report_suspicious("source2", "reason")
        assert not resp.is_blocked("source1")
        assert not resp.is_blocked("source2")


# ============================================================
# SANITIZE_ERROR_MSG
# ============================================================

class TestSanitizeErrorMsg:
    """SANITIZE_ERROR_MSG: 错误消息消毒"""

    def test_sanitizes_internal_path(self):
        """内部路径被替换(production环境)"""
        from ali2026v3_trading.infra.security_service import SANITIZE_ERROR_MSG
        old_env = os.environ.get('ALI2026_SECURITY_PROFILE')
        os.environ['ALI2026_SECURITY_PROFILE'] = 'production'
        try:
            msg = "Error in /home/user/project/file.py at line 10"
            result = SANITIZE_ERROR_MSG(msg)
            assert '<PATH_REDACTED>' in result
            assert '/home/user/project/file.py' not in result
        finally:
            if old_env is None:
                os.environ.pop('ALI2026_SECURITY_PROFILE', None)
            else:
                os.environ['ALI2026_SECURITY_PROFILE'] = old_env

    def test_sanitizes_windows_path(self):
        """Windows路径被替换(production环境)"""
        from ali2026v3_trading.infra.security_service import SANITIZE_ERROR_MSG
        old_env = os.environ.get('ALI2026_SECURITY_PROFILE')
        os.environ['ALI2026_SECURITY_PROFILE'] = 'production'
        try:
            msg = "Error in C:\\Users\\test\\file.py"
            result = SANITIZE_ERROR_MSG(msg)
            assert '<PATH_REDACTED>' in result
        finally:
            if old_env is None:
                os.environ.pop('ALI2026_SECURITY_PROFILE', None)
            else:
                os.environ['ALI2026_SECURITY_PROFILE'] = old_env

    def test_no_path_unchanged(self):
        """无路径消息不变"""
        from ali2026v3_trading.infra.security_service import SANITIZE_ERROR_MSG
        msg = "Simple error message"
        result = SANITIZE_ERROR_MSG(msg)
        assert result == msg


# ============================================================
# SecurityProfile & apply_security_profile
# ============================================================

class TestSecurityProfile:
    """SecurityProfile: 安全配置"""

    def test_profiles_exist(self):
        """三个环境配置存在"""
        from ali2026v3_trading.infra.security_service import SecurityProfile
        assert SecurityProfile.DEV is not None
        assert SecurityProfile.STAGING is not None
        assert SecurityProfile.PRODUCTION is not None

    def test_apply_profile_returns_config(self):
        """apply_security_profile返回配置字典"""
        from ali2026v3_trading.infra.security_service import apply_security_profile, SecurityProfile
        config = apply_security_profile(SecurityProfile.DEV)
        assert 'log_level' in config
        assert 'allow_pickle' in config

    def test_production_disables_pickle(self):
        """PRODUCTION禁止pickle"""
        from ali2026v3_trading.infra.security_service import ENV_SECURITY_PROFILES, SecurityProfile
        config = ENV_SECURITY_PROFILES[SecurityProfile.PRODUCTION]
        assert config['allow_pickle'] is False

    def test_dev_allows_pickle(self):
        """DEV允许pickle"""
        from ali2026v3_trading.infra.security_service import ENV_SECURITY_PROFILES, SecurityProfile
        config = ENV_SECURITY_PROFILES[SecurityProfile.DEV]
        assert config['allow_pickle'] is True


# ============================================================
# _safe_unpickle
# ============================================================

class TestSafeUnpickle:
    """_safe_unpickle: pickle安全检查"""

    def test_unpickle_blocked_in_production(self):
        """PRODUCTION环境禁止unpickle"""
        from ali2026v3_trading.infra.security_service import _safe_unpickle
        import pickle
        old_env = os.environ.get('ALI2026_SECURITY_PROFILE')
        os.environ['ALI2026_SECURITY_PROFILE'] = 'production'
        try:
            data = pickle.dumps({"key": "value"})
            with pytest.raises(RuntimeError, match="unpickle不可信数据被禁止"):
                _safe_unpickle(data)
        finally:
            if old_env is None:
                os.environ.pop('ALI2026_SECURITY_PROFILE', None)
            else:
                os.environ['ALI2026_SECURITY_PROFILE'] = old_env

    def test_unpickle_allowed_with_flag(self):
        """allow_untrusted=True允许unpickle"""
        from ali2026v3_trading.infra.security_service import _safe_unpickle
        import pickle
        data = pickle.dumps({"key": "value"})
        result = _safe_unpickle(data, allow_untrusted=True)
        assert result == {"key": "value"}


# ============================================================
# _filter_sensitive_keys
# ============================================================

class TestFilterSensitiveKeys:
    """_filter_sensitive_keys: 过滤敏感key"""

    def test_filters_sensitive(self):
        """过滤敏感key"""
        from ali2026v3_trading.infra.security_service import _filter_sensitive_keys
        keys = ['api_key', 'name', 'access_key', 'normal']
        result = _filter_sensitive_keys(keys)
        assert 'api_key' not in result
        assert 'access_key' not in result
        assert 'name' in result
        assert 'normal' in result

    def test_empty_list(self):
        """空列表"""
        from ali2026v3_trading.infra.security_service import _filter_sensitive_keys
        assert _filter_sensitive_keys([]) == []


# ============================================================
# SecurityService facade
# ============================================================

class TestSecurityServiceFacade:
    """SecurityService门面"""

    def test_reveal_credential(self):
        """门面reveal_credential"""
        from ali2026v3_trading.infra.security_service import SecurityService, _SecureCredential
        cred = _SecureCredential("test")
        assert SecurityService.reveal_credential(cred) == "test"

    def test_sanitize_for_return(self):
        """门面sanitize_for_return"""
        from ali2026v3_trading.infra.security_service import SecurityService
        result = SecurityService.sanitize_for_return({'api_key': 'secret', 'name': 'test'})
        assert result['api_key'] == '***REDACTED***'

    def test_filter_sensitive_keys(self):
        """门面filter_sensitive_keys"""
        from ali2026v3_trading.infra.security_service import SecurityService
        result = SecurityService.filter_sensitive_keys(['api_key', 'name'])
        assert 'api_key' not in result
        assert 'name' in result
