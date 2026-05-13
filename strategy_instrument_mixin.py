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

    @staticmethod
    def _extract_contract_year_month(instrument_id: str) -> Optional[str]:
        """提取合约中的四位年月（yyMM），无法提取时返回 None。

        优先使用 params_service 元数据获取年月，避免正则解析。
        """
        normalized = str(instrument_id or '').strip()
        if not normalized:
            return None

        from ali2026v3_trading.params_service import get_params_service
        ps = get_params_service()
        meta = ps.get_instrument_meta_by_id(normalized)
        if meta and meta.get('year_month'):
            return meta['year_month']

        from ali2026v3_trading.subscription_manager import SubscriptionManager
        if SubscriptionManager.is_option(instrument_id):
            try:
                parsed = SubscriptionManager.parse_option(instrument_id)
                return parsed.get('year_month')
            except (ValueError, KeyError):
                return None

        return None

    def _load_instruments_from_param_cache(self, params: Any) -> Optional[Dict[str, Any]]:
        """从参数缓存加载合约列表"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_service = get_params_service()
            result = params_service.load_instrument_list(params, source='param_cache')
            if result:
                logging.info(f"[Helper] 从参数缓存加载: {len(result['futures_list'])} 期货, {sum(len(v) for v in result['options_dict'].values())} 期权")
            return result
        except Exception as e:
            logging.warning(f"[Helper._load_instruments_from_param_cache] Error: {e}")
            return None

    def _cache_instruments_to_params(self, params: Any, futures_list: List[str],
                                     options_dict: Dict[str, List[str]], source: str) -> None:
        """将合约列表缓存到参数对象"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_service = get_params_service()
            params_service.cache_instrument_list(params, futures_list, options_dict, source)
            logging.debug(f"[Helper._cache_instruments_to_params] Cached to {source}")
        except Exception as e:
            logging.warning(f"[Helper._cache_instruments_to_params] Error: {e}")

    def _load_instruments_from_output_files(self) -> Optional[Dict[str, Any]]:
        """从输出文件加载合约列表"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_service = get_params_service()
            class TempParams:
                future_instruments = []
                option_instruments = {}

            temp_params = TempParams()
            result = params_service.load_instrument_list(temp_params, source='output_files')
            if result:
                logging.info(f"[Helper] 从输出文件加载: {len(result['futures_list'])} 期货, {sum(len(v) for v in result['options_dict'].values())} 期权")
            return result
        except Exception as e:
            logging.warning(f"[Helper._load_instruments_from_output_files] Error: {e}")
            return None

    def _normalize_cached_futures(self, futures_list: List[str]) -> List[str]:
        """规范化期货合约列表（去重、验证格式）"""
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
                    from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                    DiagnosisProbeManager.on_parse_transform(
                        'core.normalize_future_cache',
                        original_id,
                        canonical_key,
                        detail='dedupe_by_canonical_key,preserve_source_id',
                        level='DEBUG',
                    )
                except Exception:
                    pass
            except (ValueError, Exception) as e:
                logging.warning(f"[Helper] 跳过无效期货合约: {contract}, error: {e}")
                try:
                    from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                    DiagnosisProbeManager.on_parse_failure('core.normalize_future_cache', str(contract or '').strip(), str(e))
                except Exception:
                    pass

        logging.debug(f"[Helper._normalize_cached_futures] {len(futures_list)} -> {len(normalized)}")
        return normalized

    def _normalize_cached_options(self, options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """规范化期权合约字典"""
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
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_transform(
                            'core.normalize_option_cache',
                            original_id,
                            canonical_key,
                            detail=f"source_format={parsed.get('format')},preserve_source_id",
                            level='INFO' if original_id != canonical_key else 'DEBUG',
                        )
                    except Exception:
                        pass
                except (ValueError, Exception) as e:
                    logging.warning(f"[Helper] 跳过无效期权合约: {contract}, error: {e}")
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_failure('core.normalize_option_cache', str(contract or '').strip(), str(e))
                    except Exception:
                        pass

            if normalized_contracts:
                normalized[product] = normalized_contracts

        total_before = sum(len(v) for v in options_dict.values())
        total_after = sum(len(v) for v in normalized.values())
        logging.debug(f"[Helper._normalize_cached_options] {total_before} -> {total_after}")
        return normalized

    def _count_option_contracts(self, options_dict: Dict[str, List[str]]) -> int:
        """统计期权合约总数"""
        return sum(len(contracts) for contracts in options_dict.values())

    def _derive_underlying_futures_from_options(self, options_dict: Dict[str, List[str]]) -> List[str]:
        """从期权字典推导标的期货列表（去重）"""
        underlying_set = set()

        for product, contracts in options_dict.items():
            for contract in contracts:
                try:
                    parsed = SubscriptionManager.parse_option(contract)
                    from ali2026v3_trading.data_service import get_data_service
                    rows = get_data_service().query(
                        "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                        [parsed['product'], parsed['year_month']]
                    ).to_pylist()
                    if rows:
                        underlying = rows[0]['instrument_id']
                    else:
                        underlying = f"{parsed['product']}{parsed['year_month']}"
                        logging.debug(f"[Helper] 标的期货未注册，使用解析值: {underlying}")
                    underlying_set.add(underlying)
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_transform(
                            'core.derive_underlying_future',
                            str(contract or '').strip(),
                            underlying,
                            detail=f"option_format={parsed.get('format')}",
                            level='INFO',
                        )
                    except Exception:
                        pass
                except Exception as e:
                    logging.warning(f"[Helper] 无法提取标的期货: {contract}, error: {e}")
                    try:
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_parse_failure('core.derive_underlying_future', str(contract or '').strip(), str(e))
                    except Exception:
                        pass

        result = sorted(underlying_set)
        if result:
            logging.debug(f"[Helper._derive_underlying_futures_from_options] Derived {len(result)} underlying futures: {result[:5]}...")
        return result
