# [M1-55] ƽ̨__֤__

# MODULE_ID: M1-139

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""

order_platform_auth.py - PlatformAuthenticator

R27: 从order_service.py提取的平台认证类



职责任

- 平台Token管理 (get_token, set_token)

- 防重放攻击nonce验证 (validate_nonce)

- 请求签名 (generate_signed_request)

"""

from __future__ import annotations



import hashlib

import hmac

import logging

import os

import threading

import time

from typing import Any, Dict, Optional





class PlatformAuthenticator:

    def __init__(self):

        self._token: Optional[str] = None

        self._token_expiry: float = 0.0

        self._lock = threading.Lock()

        # P1-R8-14修复: 防重放攻击击nonce缓存+时间窗口

        self._nonce_cache: Dict[str, float] = {}

        self._nonce_window_seconds: float = 300.0

        self._max_nonce_cache: int = 1000



    def get_token(self) -> Optional[str]:

        with self._lock:

            if self._token and time.time() < self._token_expiry:

                return self._token

            return None



    def set_token(self, token: str, expires_in: float = 3600.0) -> None:

        with self._lock:

            self._token = token

            self._token_expiry = time.time() + expires_in



    def validate_nonce(self, nonce: str, timestamp: Optional[float] = None) -> bool:

        """P1-R8-14修复: 验证请求nonce防重放攻击



        规则:

        1. nonce在时间窗口内必须单调递增(拒绝已使用过的nonce)

        2. 请求时间戳不得滞后超过滤window_seconds

        3. 超出缓存容量的旧nonce自动淘汰

        """

        with self._lock:

            now = time.time()

            if len(self._nonce_cache) > self._max_nonce_cache:

                _sorted = sorted(self._nonce_cache.items(), key=lambda x: x[1])

                _evict_n = len(self._nonce_cache) - self._max_nonce_cache + 100

                for k, _ in _sorted[:_evict_n]:

                    del self._nonce_cache[k]

            _expired = [k for k, t in self._nonce_cache.items() if now - t > self._nonce_window_seconds]

            for k in _expired:

                del self._nonce_cache[k]

            if nonce in self._nonce_cache:

                return False

            if timestamp is not None and (now - timestamp) > self._nonce_window_seconds:

                return False

            self._nonce_cache[nonce] = now

            return True



    def generate_signed_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:

        _nonce = f"{time.time():.6f}_{os.urandom(4).hex()}"

        _timestamp = time.time()

        _token = self.get_token() or ''

        _sign_payload = f"{_nonce}:{_timestamp:.3f}:{_token}"

        _hmac_key = os.environ.get('ORDER_SIGN_KEY', 'default_order_sign_key_change_in_prod')
        if _hmac_key == 'default_order_sign_key_change_in_prod':
            import secrets as _secrets
            _hmac_key = _secrets.token_hex(16)
            os.environ['ORDER_SIGN_KEY'] = _hmac_key
            if not hasattr(type(self), '_order_sign_key_auto_gen_warned'):
                type(self)._order_sign_key_auto_gen_warned = True
                logging.info(
                    "[SECURITY] ORDER_SIGN_KEY未配置，已自动生成随机密钥"
                )
        _signature = hmac.new(_hmac_key.encode(), _sign_payload.encode(), hashlib.sha256).hexdigest()[:16]

        if not self.validate_nonce(_nonce, _timestamp):

            logging.warning("[PlatformAuthenticator] nonce验证失败，可能重放攻击 nonce=%s", _nonce)

            return {}

        signed = dict(request_data)

        signed['_nonce'] = _nonce

        signed['_timestamp'] = _timestamp

        signed['_signature'] = _signature

        return signed



