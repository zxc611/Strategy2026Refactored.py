"""
strategy_instrument_mixin.py — _InstrumentHelperMixin
合约加载、规范化、推导的纯逻辑方法簇。

设计原则：
- 零self状态写入（除通过参数传递）
- 可独立测试
- 不依赖 StrategyCoreService 的任何实例属性
"""

import logging
from typing import Any, Dict, List, Optional

from ali2026v3_trading.subscription_manager import SubscriptionManager


class _InstrumentHelperMixin:

    @property
    def _instrument_manager(self):
        if not hasattr(self, '_instrument_mgr'):
            from ali2026v3_trading.instrument_manager import InstrumentManager
            self._instrument_mgr = InstrumentManager()
        return self._instrument_mgr

    @staticmethod
    def _extract_contract_year_month(instrument_id: str) -> Optional[str]:
        from ali2026v3_trading.instrument_manager import InstrumentManager
        return InstrumentManager.extract_contract_year_month(instrument_id)

    def _load_instruments_from_param_cache(self, params: Any) -> Optional[Dict[str, Any]]:
        return self._instrument_manager.load_instruments_from_param_cache(params)

    def _cache_instruments_to_params(self, params: Any, futures_list: List[str],
                                     options_dict: Dict[str, List[str]], source: str) -> None:
        return self._instrument_manager.cache_instruments_to_params(params, futures_list, options_dict, source)

    def _load_instruments_from_output_files(self) -> Optional[Dict[str, Any]]:
        return self._instrument_manager.load_instruments_from_output_files()

    def _normalize_cached_futures(self, futures_list: List[str]) -> List[str]:
        return self._instrument_manager.normalize_cached_futures(futures_list)

    def _normalize_cached_options(self, options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        return self._instrument_manager.normalize_cached_options(options_dict)

    def _count_option_contracts(self, options_dict: Dict[str, List[str]]) -> int:
        return self._instrument_manager.count_option_contracts(options_dict)

    def _derive_underlying_futures_from_options(self, options_dict: Dict[str, List[str]]) -> List[str]:
        return self._instrument_manager.derive_underlying_futures_from_options(options_dict)
