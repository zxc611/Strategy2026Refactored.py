"""上下文环境判断与辅助工具。"""
from __future__ import annotations

import os
import json
from typing import Any, Optional, Dict

class ContextMixin:
    """提供回测/交易上下文判断及账户辅助功能的混入类"""

    def _debug_throttled(self, msg: str, category: str = "default", min_interval: float = 1.0) -> None:
        """带节流的调试输出，防止日志刷屏。"""
        import time
        if not hasattr(self, "_debug_log_times"):
            self._debug_log_times = {}
        
        now = time.time()
        last_time = self._debug_log_times.get(msg, 0)
        
        # 使用 msg 作为key可能导致内存增加，但在策略生命周期内通常有限
        if now - last_time >= min_interval:
            self._debug_log_times[msg] = now
            if hasattr(self, "_debug"):
                self._debug(msg)
            elif hasattr(self, "output"):
                self.output(msg)

    def _is_backtest_context(self) -> bool:
        """判定是否处于回测模式，兼容多种标志/配置。"""
        try:
            if bool(getattr(self.params, "backtest_tick_mode", False)):
                return True
            rp = str(getattr(self.params, "run_profile", "")).lower()
            if rp in ("backtest", "bt", "backtesting"):
                return True
            # 常见平台字段兜底
            for name in ("is_backtesting", "backtesting", "in_backtesting", "Backtesting"):
                if bool(getattr(self, name, False)):
                    return True
        except Exception:
            pass
        return False

    def _is_trade_context(self) -> bool:
        """判定是否处于交易模式（输出模式或交易标志）。"""
        try:
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
        except Exception:
            mode = "debug"
        try:
            if mode == "trade":
                return True
            if bool(getattr(self, "my_trading", False)):
                return True
        except Exception:
            pass
        return False

    def _diagnostic_output_allowed(self) -> bool:
        """诊断/测试输出是否允许；交易/回测统一关闭。"""
        try:
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
            if mode == "debug":
                mode = "close_debug"
            if mode in ("trade", "open_debug") or self._is_trade_context() or self._is_backtest_context():
                return False
        except Exception:
            pass
        try:
            return bool(getattr(self.params, "diagnostic_output", True))
        except Exception:
            return False

    def _enforce_diagnostic_silence(self) -> None:
        """在交易/回测场景下强制关闭测试/诊断输出与测试模式。"""
        try:
            mode = str(getattr(self.params, "output_mode", "debug")).lower()
            if mode == "debug":
                mode = "close_debug"
            if mode in ("trade", "open_debug") or self._is_trade_context() or self._is_backtest_context():
                 if hasattr(self.params, "diagnostic_output"):
                     setattr(self.params, "diagnostic_output", False)
                 if hasattr(self.params, "test_mode"):
                     setattr(self.params, "test_mode", False)
        except Exception:
            pass

    def _get_account_available_capital(self, account_id: str) -> Optional[float]:
        """获取可用资金 (Ported Logic)"""
        # 兼容 self.strategy 引用 (如果被包裹) 或 self 本身
        target = getattr(self, "strategy", self)
        
        try:
            if hasattr(target, "get_account"):
                result = target.get_account(account_id)
            elif hasattr(target, "query_account"):
                result = target.query_account(account_id)
            # Todo: Adapt to specific pythongo structure if result is object vs dict
            # Assuming mock return for simulation context if real API fails
            else:
                return None
            
            if result:
                 # Try common attribute names
                 for k in ["Available", "available", "balance", "Balance"]:
                     if hasattr(result, k):
                         return float(getattr(result, k))
                     if isinstance(result, dict) and k in result:
                         return float(result[k])
            return None
        except Exception:
            return None

    def _get_account_total_capital(self, account_id: str) -> Optional[float]:
        """获取总资金"""
        target = getattr(self, "strategy", self)
        try:
            if hasattr(target, "get_account"):
                result = target.get_account(account_id)
            elif hasattr(target, "query_account"):
                result = target.query_account(account_id)
            else:
                return None
                
            if result:
                 for k in ["Balance", "balance", "DynamicBalance", "dynamic_balance"]:
                     if hasattr(result, k):
                         return float(getattr(result, k))
                     if isinstance(result, dict) and k in result:
                         return float(result[k])
            return None
        except Exception:
            return None

    def _get_user_id(self) -> str:
        return str(getattr(self.params, "user_id", "") or "unknown_user")

    def _get_broker_id(self) -> str:
        return str(getattr(self.params, "broker_id", "") or "unknown_broker")
        
    def _load_api_key(self) -> None:
        """尝试加载API Key到环境变量 (Platform compat)"""
        if hasattr(self, "params"):
             self.load_api_key(self.params)

    def load_api_key(self, params: Any = None) -> Dict[str, str]:
        """
        Public API parity: Loads API keys from params or .env file 
        and sets them in os.environ.
        """
        loaded_keys: Dict[str, str] = {}
        p = params or getattr(self, "params", None)
        if not p: return {}
        
        # 1. Params
        key_infini = (getattr(p, "infini_api_key", "") or "").strip()
        key_generic = (getattr(p, "api_key", "") or "").strip()
        key_access = (getattr(p, "access_key", "") or "").strip()
        secret_access = (getattr(p, "access_secret", "") or "").strip()
        
        # 2. Env File (Logic from Source)
        try:
            # Assumes this file is in your_quant_project/strategy/
            base_dir = os.path.dirname(__file__)
            # Look up 3 levels to match original source logic "..", "..", ".env"
            # Original was Strategy file relative.
            env_path = os.path.join(base_dir, "..", "..", ".env")
            
            if os.path.isfile(env_path):
                 with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        s = line.strip()
                        if not s or s.startswith("#") or "=" not in s:
                            continue
                        k, v = s.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("\"").strip("'")
                        
                        if k == "INFINI_API_KEY" and v and not key_infini:
                            key_infini = v
                        if k == "API_KEY" and v and not key_generic:
                            key_generic = v
                        if k in ("INFINI_ACCESS_KEY", "ACCESS_KEY") and v and not key_access:
                            key_access = v
                        if k in ("INFINI_ACCESS_SECRET", "ACCESS_SECRET") and v and not secret_access:
                            secret_access = v
        except Exception:
            pass

        # 3. Set Environment
        if key_infini:
            os.environ["INFINI_API_KEY"] = key_infini
            loaded_keys["INFINI_API_KEY"] = "******"
        if key_generic:
            os.environ["API_KEY"] = key_generic
            loaded_keys["API_KEY"] = "******"
        if key_access:
            os.environ["INFINI_ACCESS_KEY"] = key_access
            os.environ["ACCESS_KEY"] = key_access
            loaded_keys["ACCESS_KEY"] = "******"
        if secret_access:
            os.environ["INFINI_ACCESS_SECRET"] = secret_access
            os.environ["ACCESS_SECRET"] = secret_access
            loaded_keys["ACCESS_SECRET"] = "******"
            
        return loaded_keys

