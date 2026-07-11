"""
instrument_service.py — 策略基础设施服务集合

合并自:
- instrument_service.py (品种服务)
- kline_data_service.py (K线数据服务)
- types.py (策略共享类型定义)

包含:
- StrategyParams: 策略参数容器
- InstrumentManager / InstrumentService: 品种合约管理
- KlineDataService: K线数据加载与缓存
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.subscription_service import SubscriptionManager
from ali2026v3_trading.infra.shared_utils import safe_int, safe_float
from ali2026v3_trading.infra.exceptions import FutureLeakException

__all__ = [
    'StrategyParams',
    'InstrumentManager', 'InstrumentService', '_InstrumentHelperMixin',
    'KlineDataService',
]


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


# ============================================================================
# StrategyParams (merged from types.py)
# ============================================================================

class StrategyParams:
    """策略参数容器 - 替代 type('Params', (), dict)() 匿名类

    提供可调试的 __repr__ 和 __dir__，方便排查参数问题。
    从 strategy_2026.py 迁移至此独立模块(2026-06-12)。
    """

    __slots__ = ('_data',)

    def __init__(self, data: Dict[str, Any]):
        object.__setattr__(self, '_data', dict(data))

    def __getattr__(self, name: str):
        try:
            return object.__getattribute__(self, '_data')[name]
        except KeyError:
            raise AttributeError(
                f"'StrategyParams' object has no attribute '{name}'. "
                f"Available: {sorted(object.__getattribute__(self, '_data').keys())}"
            )

    def __setattr__(self, name: str, value):
        if name == '_data':
            object.__setattr__(self, name, value)
        else:
            object.__getattribute__(self, '_data')[name] = value

    def __repr__(self) -> str:
        data = object.__getattribute__(self, '_data')
        items = {k: v for k, v in data.items() if not k.startswith('_')}
        return f"StrategyParams({items})"

    def __dir__(self):
        data = object.__getattribute__(self, '_data')
        return sorted(data.keys())

    def as_dict(self) -> Dict[str, Any]:
        return dict(object.__getattribute__(self, '_data'))

    def get(self, key: str, default=None):
        return object.__getattribute__(self, '_data').get(key, default)


# ============================================================================
# KlineDataService (merged from kline_data_service.py)
# ============================================================================

class KlineDataService:
    """K线数据服务 - 替代 HistoricalKlineMixin 的独立组合式服务。

    通过构造函数接收 state_store 和 callback_group，完全自包含，
    不依赖 Mixin 宿主的 self 属性。

    Args:
        state_store: 状态存储，提供 get/set 方法访问策略全局状态
        callback_group: 回调组，提供 get_registry 方法访问注册的回调
        strategy_id: 策略标识，用于日志
        is_backtest: 是否为回测模式，回测模式下启用时间截止墙
    """

    def __init__(
        self,
        state_store: Any = None,
        callback_group: Any = None,
        strategy_id: str = '',
        is_backtest: bool = False,
    ):
        self._state_store = state_store
        self._callback_group = callback_group
        self._strategy_id = strategy_id
        self._is_backtest = is_backtest

        self._time_cutoff_wall: Optional[float] = None
        self._historical_mgr: Any = None

    def set_time_cutoff(self, cutoff_time: float) -> None:
        """设置时间截止墙。回测模式下，超出此时间的数据请求将触发异常。"""
        self._time_cutoff_wall = cutoff_time
        logging.debug(
            "[KlineDataService][strategy_id=%s] Time cutoff wall set to %.3f",
            self._strategy_id, cutoff_time,
        )

    def check_time_cutoff(self, requested_time: float) -> None:
        """检查请求时间是否超出截止墙。仅在回测模式且已设置 time_cutoff 时生效。"""
        if not self._is_backtest:
            return
        if self._time_cutoff_wall is None:
            return
        if requested_time > self._time_cutoff_wall:
            raise FutureLeakException(
                f"[KlineDataService][strategy_id={self._strategy_id}] "
                f"Future data leak detected: requested_time={requested_time:.3f} "
                f"exceeds time_cutoff_wall={self._time_cutoff_wall:.3f}"
            )

    @property
    def historical_mgr(self) -> Any:
        return self._historical_mgr

    @property
    def load_in_progress(self) -> bool:
        if self._historical_mgr is None:
            return False
        return self._historical_mgr._historical_load_in_progress

    @property
    def kline_result(self) -> Optional[Dict[str, Any]]:
        if self._historical_mgr is None:
            return None
        return self._historical_mgr._historical_kline_result

    @property
    def kline_progress(self) -> Optional[Dict[str, Any]]:
        if self._historical_mgr is None:
            return None
        return self._historical_mgr._historical_kline_progress

    def init_historical(self) -> None:
        if self._historical_mgr is None:
            from ali2026v3_trading.data.historical_data_manager import HistoricalDataManager
            self._historical_mgr = HistoricalDataManager(
                state_store=self._state_store,
                callback_group=self._callback_group,
            )
        self._historical_mgr.init_historical()

    def filter_historical_month_scope(self, instrument_ids: List[str]) -> Tuple[List[str], int, str]:
        return self._historical_mgr.filter_historical_month_scope(instrument_ids)

    def build_historical_instruments(self) -> List[str]:
        return self._historical_mgr.build_historical_instruments()

    def resolve_historical_provider(self) -> Tuple[Any, str]:
        return self._historical_mgr.resolve_historical_provider()

    def load_historical_klines_once(self, instruments: List[str], provider: Any, provider_source: str) -> None:
        return self._historical_mgr.load_historical_klines_once(instruments, provider, provider_source)

    def notify_historical_kline_loaded(self, result: Dict[str, Any]) -> None:
        return self._historical_mgr.notify_historical_kline_loaded(result)

    def start_historical_kline_load(self, blocking: bool = False) -> None:
        return self._historical_mgr.start_historical_kline_load(blocking=blocking)

    def shutdown_historical_services(self) -> None:
        return self._historical_mgr.shutdown_historical_services()

    def reset_historical_state_for_restart(self) -> None:
        return self._historical_mgr.reset_historical_state_for_restart()

    def emit_historical_kline_diagnostic_on_first_tick(self) -> None:
        return self._historical_mgr.emit_historical_kline_diagnostic_on_first_tick()

    def check_and_start_historical_load_on_tick(self) -> None:
        return self._historical_mgr.check_and_start_historical_load_on_tick()

    def get_historical_kline_summary_lines(self) -> Tuple[str, str]:
        return self._historical_mgr.get_historical_kline_summary_lines()
