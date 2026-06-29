"""
instrument_service.py — InstrumentService
独立品种服务，替代 _InstrumentHelperMixin，以组合替代继承。

设计原则：
- 零self状态写入（除通过参数传递）
- 可独立测试
- 不依赖 StrategyCoreService 的任何实例属性
"""

import logging
from typing import Any, Dict, List, Optional


import logging
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.subscription_service import SubscriptionManager


class InstrumentManager:
    def __init__(self, params_service=None):
        self._params_service = params_service
        self._future_ids = []
        self._option_ids = []

    @staticmethod
    def extract_contract_year_month(instrument_id: str) -> Optional[str]:
        normalized = str(instrument_id or '').strip()
        if not normalized:
            return None

        from ali2026v3_trading.config.params_service import get_params_service
        ps = get_params_service()
        meta = ps.get_instrument_meta_by_id(normalized)
        if meta and meta.get('year_month'):
            return meta['year_month']

        if SubscriptionManager.is_option(instrument_id):
            try:
                parsed = SubscriptionManager.parse_option(instrument_id)
                return parsed.get('year_month')
            except (ValueError, KeyError):
                return None

        return None

    def load_instruments_from_param_cache(self, params: Any) -> Optional[Dict[str, Any]]:
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            params_service = get_params_service()
            result = params_service.load_instrument_list(params, source='param_cache')
            if result:
                logging.info(f"[Helper] 从参数缓存加载: {len(result['futures_list'])} 期货, {sum(len(v) for v in result['options_dict'].values())} 期权")
            return result
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning(f"[Helper._load_instruments_from_param_cache] Error: {e}")
            return None

    def cache_instruments_to_params(self, params: Any, futures_list: List[str],
                                     options_dict: Dict[str, List[str]], source: str) -> None:
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            params_service = get_params_service()
            params_service.cache_instrument_list(params, futures_list, options_dict, source)
            logging.debug(f"[Helper._cache_instruments_to_params] Cached to {source}")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning(f"[Helper._cache_instruments_to_params] Error: {e}")

    def load_instruments_from_output_files(self) -> Optional[Dict[str, Any]]:
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            params_service = get_params_service()

            class TempParams:
                def __init__(self):
                    self.future_instruments = []
                    self.option_instruments = {}

            temp_params = TempParams()
            result = params_service.load_instrument_list(temp_params, source='output_files')
            if result:
                logging.info(f"[Helper] 从输出文件加载: {len(result['futures_list'])} 期货, {sum(len(v) for v in result['options_dict'].values())} 期权")
            return result
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning(f"[Helper._load_instruments_from_output_files] Error: {e}")
            return None

    def normalize_cached_futures(self, futures_list: List[str]) -> List[str]:
        normalized = []
        seen = set()

        for contract in futures_list:
            try:
                original_id = str(contract or '').strip()
                parsed = SubscriptionManager.parse_future(contract)
                canonical_key = f"{parsed['product']}{parsed['year_month']}"
                if canonical_key not in seen:
                    normalized.append(original_id)
                    seen.add(canonical_key)
                try:
                    from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                    DiagnosisProbeManager.on_parse_transform(
                        'core.normalize_future_cache',
                        original_id,
                        canonical_key,
                        detail='dedupe_by_canonical_key,preserve_source_id',
                        level='DEBUG',
                    )
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning(f"[Helper] 跳过无效期货合约: {contract}, error: {e}")
                try:
                    from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                    DiagnosisProbeManager.on_parse_failure('core.normalize_future_cache', str(contract or '').strip(), str(e))
                except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                    logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                    pass

        logging.debug(f"[Helper._normalize_cached_futures] {len(futures_list)} -> {len(normalized)}")
        return normalized

    def normalize_cached_options(self, options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        normalized = {}

        for product, contracts in options_dict.items():
            normalized_contracts = []
            seen = set()

            for contract in contracts:
                try:
                    original_id = str(contract or '').strip()
                    parsed = SubscriptionManager.parse_option(contract)
                    canonical_key = f"{parsed['product']}{parsed['year_month']}-{parsed['option_type']}-{int(parsed['strike_price'])}"
                    if canonical_key not in seen:
                        normalized_contracts.append(original_id)
                        seen.add(canonical_key)
                    try:
                        from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_transform(
                            'core.normalize_option_cache',
                            original_id,
                            canonical_key,
                            detail=f"source_format={parsed.get('format')},preserve_source_id",
                            level='INFO' if original_id != canonical_key else 'DEBUG',
                        )
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                        logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                        pass
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning(f"[Helper] 跳过无效期权合约: {contract}, error: {e}")
                    try:
                        from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_failure('core.normalize_option_cache', str(contract or '').strip(), str(e))
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                        logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                        pass

            if normalized_contracts:
                normalized[product] = normalized_contracts

        total_before = sum(len(v) for v in options_dict.values())
        total_after = sum(len(v) for v in normalized.values())
        logging.debug(f"[Helper._normalize_cached_options] {total_before} -> {total_after}")
        return normalized

    def count_option_contracts(self, options_dict: Dict[str, List[str]]) -> int:
        return sum(len(contracts) for contracts in options_dict.values())

    def derive_underlying_futures_from_options(self, options_dict: Dict[str, List[str]]) -> List[str]:
        underlying_set = set()
        product_month_set = set()

        for product, contracts in options_dict.items():
            for contract in contracts:
                try:
                    parsed = SubscriptionManager.parse_option(contract)
                    product_month_set.add((parsed['product'], parsed['year_month']))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning(f"[Helper] 无法解析期权合约: {contract}, error: {e}")

        if not product_month_set:
            return []

        underlying_cache = {}
        try:
            from ali2026v3_trading.data.data_service import get_data_service
            ds = get_data_service()
            for product, year_month in product_month_set:
                rows = ds.query(
                    "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                    [product, year_month]
                ).to_pylist()
                if rows:
                    underlying_cache[(product, year_month)] = rows[0]['instrument_id']
                else:
                    underlying_cache[(product, year_month)] = f"{product}{year_month}"
                    logging.debug(f"[Helper] 标的期货未注册，使用解析值: {product}{year_month}")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning(f"[Helper] 批量查询标的期货失败: {e}")

        for product, contracts in options_dict.items():
            for contract in contracts:
                try:
                    parsed = SubscriptionManager.parse_option(contract)
                    key = (parsed['product'], parsed['year_month'])
                    underlying = underlying_cache.get(key, f"{parsed['product']}{parsed['year_month']}")
                    underlying_set.add(underlying)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning(f"[Helper] 无法提取标的期货: {contract}, error: {e}")

        result = sorted(underlying_set)
        if result:
            logging.debug(f"[Helper._derive_underlying_futures_from_options] Derived {len(result)} underlying futures: {result[:5]}...")
        return result


class InstrumentService:
    """独立品种服务 - 替代_InstrumentHelperMixin，以组合替代继承"""

    def __init__(self):
                self._instrument_mgr = InstrumentManager()

    @staticmethod
    def extract_contract_year_month(instrument_id: str) -> Optional[str]:
                return InstrumentManager.extract_contract_year_month(instrument_id)

    def load_instruments_from_param_cache(self, params: Any) -> Optional[Dict[str, Any]]:
        return self._instrument_mgr.load_instruments_from_param_cache(params)

    def cache_instruments_to_params(self, params: Any, futures_list: List[str],
                                    options_dict: Dict[str, List[str]], source: str) -> None:
        return self._instrument_mgr.cache_instruments_to_params(params, futures_list, options_dict, source)

    def load_instruments_from_output_files(self) -> Optional[Dict[str, Any]]:
        return self._instrument_mgr.load_instruments_from_output_files()

    def normalize_cached_futures(self, futures_list: List[str]) -> List[str]:
        return self._instrument_mgr.normalize_cached_futures(futures_list)

    def normalize_cached_options(self, options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        return self._instrument_mgr.normalize_cached_options(options_dict)

    def count_option_contracts(self, options_dict: Dict[str, List[str]]) -> int:
        return self._instrument_mgr.count_option_contracts(options_dict)

    def derive_underlying_futures_from_options(self, options_dict: Dict[str, List[str]]) -> List[str]:
        return self._instrument_mgr.derive_underlying_futures_from_options(options_dict)


class _InstrumentHelperMixin:
    """向后兼容Shim — 已迁移至InstrumentService，请使用InstrumentService替代。"""

    def __init__(self, *args, **kwargs):
        import warnings
        warnings.warn(
            '_InstrumentHelperMixin is deprecated; use InstrumentService instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        self._instrument_svc = InstrumentService()

    @staticmethod
    def extract_contract_year_month(instrument_id: str) -> Optional[str]:
        return InstrumentService.extract_contract_year_month(instrument_id)

    def load_instruments_from_param_cache(self, params: Any) -> Optional[Dict[str, Any]]:
        return self._instrument_svc.load_instruments_from_param_cache(params)

    def cache_instruments_to_params(self, params: Any, futures_list: List[str],
                                    options_dict: Dict[str, List[str]], source: str) -> None:
        return self._instrument_svc.cache_instruments_to_params(params, futures_list, options_dict, source)

    def load_instruments_from_output_files(self) -> Optional[Dict[str, Any]]:
        return self._instrument_svc.load_instruments_from_output_files()

    def normalize_cached_futures(self, futures_list: List[str]) -> List[str]:
        return self._instrument_svc.normalize_cached_futures(futures_list)

    def normalize_cached_options(self, options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        return self._instrument_svc.normalize_cached_options(options_dict)

    def count_option_contracts(self, options_dict: Dict[str, List[str]]) -> int:
        return self._instrument_svc.count_option_contracts(options_dict)

    def derive_underlying_futures_from_options(self, options_dict: Dict[str, List[str]]) -> List[str]:
        return self._instrument_svc.derive_underlying_futures_from_options(options_dict)
