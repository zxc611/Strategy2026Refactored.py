"""
query_service.py - 数据查询服务 (DuckDB 版本)

从 storage.py 迁移的所有查询功能
职责：专注于数据查询、统计、导出等读操作

核心功能:
1. 合约信息查询
2. K 线/Tick 数据查询
3. 期权链查询
4. 数据统计与摘要
5. 数据导出

架构变更说明 (2026-04-12):
- 所有数据库操作通过 DataService (DuckDB) 进行
- 保留 QueryService 作为查询逻辑封装层
- _KlineAggregator 保留用于 Tick 合成 K 线

作者：CodeArts 代码智能体
版本：v2.0 (DuckDB 重构版)
生成时间：2026-03-31
更新时间：2026-04-12
"""

from __future__ import annotations

import logging
import csv
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime


def _normalize_code(code: str) -> str:
    """标准化代码（交易所/产品），保留原始格式
    
    Args:
        code: 代码字符串（交易所或产品）
    
    Returns:
        str: 原始格式的代码
    """
    if not code:
        return ''
    return str(code).strip()

try:
    from ali2026v3_trading.data_service import DataService, get_data_service
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning("[QueryService] Failed to import DataService: %s", e)
    _HAS_DATA_SERVICE = False
    DataService = None
    get_data_service = None


def _result_to_pylist(result: Any) -> List[Dict[str, Any]]:
    if result is None:
        return []
    if hasattr(result, 'to_pylist'):
        return result.to_pylist()
    if hasattr(result, 'read_all'):
        materialized = result.read_all()
        if hasattr(materialized, 'to_pylist'):
            return materialized.to_pylist()
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return []


def _table_exists(ds: Any, table_name: str) -> bool:
    rows = _result_to_pylist(ds.query(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ))
    return bool(rows and int(rows[0].get('cnt', 0) or 0) > 0)


# ============================================================================
# 轻量 K 线数据结构（从 market_data_service.py 迁移）
# ============================================================================

class LightKLine:
    """
    轻量级 K 线对象，使用__slots__优化内存
    
    适用场景：
    - 内存中临时存储大量K线数据
    - 需要比dict更高效的属性访问
    - 与pythongo.KLineData兼容的简化结构
    
    注意：生产环境建议直接使用 pyarrow.Table 或 pythongo.KLineData
    """
    __slots__ = ('open', 'high', 'low', 'close', 'volume', 'datetime',
                 'exchange', 'instrument_id', 'style', 'is_placeholder', 'source')

    def __init__(self, open_price: float, high: float, low: float,
                 close: float, volume: float, dt: datetime):
        self.open = float(open_price)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)
        self.volume = float(volume)
        self.datetime = dt
        self.exchange = ""
        self.instrument_id = ""
        self.style = "M1"
        self.is_placeholder = False
        self.source = ""


class _KlineAggregator:
    """K 线聚合器 - 从 Tick 数据合成 K 线（流式处理，内存高效）"""
    
    def __init__(self, instrument_id: str, period: str, logger: Optional[logging.Logger] = None):
        self.instrument_id = instrument_id
        self.period = period
        self.logger = logger or logging.getLogger(__name__)
        
        # ✅ BP-17修复：加锁防竞态（on_tick线程与drain线程可并发调用update）
        self._lock = threading.Lock()
        
        # 当前 K 线状态
        self.open_price: Optional[float] = None
        self.high_price: Optional[float] = None
        self.low_price: Optional[float] = None
        self.close_price: Optional[float] = None
        self.volume: int = 0
        self.amount: float = 0.0
        self.open_interest: Optional[int] = None
        
        # 当前 K 线开始时间
        self.kline_start_time: Optional[float] = None
        
        # 周期转换为秒
        self.period_seconds = self._parse_period(period)
    
    def _parse_period(self, period: str) -> int:
        """将周期字符串转换为秒数"""
        period_map = {
            '1s': 1,
            '1m': 60,
            '1min': 60,
            '5m': 300,
            '5min': 300,
            '15m': 900,
            '15min': 900,
            '30m': 1800,
            '30min': 1800,
            '60m': 3600,
            '60min': 3600,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '1d': 86400,
        }
        return period_map.get(period, 60)
    
    def update(self, tick_ts: float, price: float, 
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """更新 Tick 数据，返回完成的 K 线（如果有）"""
        # ✅ BP-17修复：加锁防竞态
        with self._lock:
            return self._update_impl(tick_ts, price, volume, amount, open_interest)
    
    def _update_impl(self, tick_ts: float, price: float, 
               volume: int = 0, amount: float = 0.0,
               open_interest: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """update的锁内实现"""
        if self.kline_start_time is None:
            # 初始化第一根 K 线
            self.kline_start_time = (tick_ts // self.period_seconds) * self.period_seconds
            self.open_price = self.high_price = self.low_price = self.close_price = price
            self.volume = volume
            self.amount = amount
            self.open_interest = open_interest
            return None
        
        # 检查是否应该开启新 K 线
        current_kline_end = self.kline_start_time + self.period_seconds
        if tick_ts >= current_kline_end:
            completed_kline = self._get_current_kline()
            # 开启新 K 线
            self.kline_start_time = (tick_ts // self.period_seconds) * self.period_seconds
            self.open_price = self.high_price = self.low_price = self.close_price = price
            self.volume = volume
            self.amount = amount
            self.open_interest = open_interest
            return completed_kline
        
        # 更新当前 K 线
        self.close_price = price
        if price > self.high_price:
            self.high_price = price
        if price < self.low_price:
            self.low_price = price
        self.volume += volume
        self.amount += amount
        if open_interest is not None:
            self.open_interest = open_interest
        
        return None
    
    def _get_current_kline(self) -> Dict[str, Any]:
        """获取当前完成的 K 线数据"""
        return {
            'instrument_id': self.instrument_id,
            'period': self.period,
            'ts': self.kline_start_time,
            'timestamp': self.kline_start_time,
            'open': self.open_price,
            'high': self.high_price,
            'low': self.low_price,
            'close': self.close_price,
            'volume': self.volume,
            'amount': self.amount,
            'open_interest': self.open_interest,
        }
    
    def reset(self):
        """重置聚合器状态"""
        self.open_price = self.high_price = self.low_price = self.close_price = None
        self.volume = 0
        self.amount = 0.0
        self.open_interest = None
        self.kline_start_time = None

    def to_state_dict(self) -> Dict[str, Any]:
        """✅ BP-17：导出聚合器状态用于持久化"""
        if self.kline_start_time is None:
            return None
        return {
            'instrument_id': self.instrument_id,
            'period': self.period,
            'open_price': self.open_price,
            'high_price': self.high_price,
            'low_price': self.low_price,
            'close_price': self.close_price,
            'volume': self.volume,
            'amount': self.amount,
            'open_interest': self.open_interest,
            'kline_start_time': self.kline_start_time,
        }

    @classmethod
    def from_state_dict(cls, state: Dict[str, Any]) -> '_KlineAggregator':
        """✅ BP-17：从持久化状态恢复聚合器"""
        agg = cls(state.get('instrument_id', ''), state.get('period', ''), logging.getLogger(__name__))
        agg.open_price = state.get('open_price')
        agg.high_price = state.get('high_price')
        agg.low_price = state.get('low_price')
        agg.close_price = state.get('close_price')
        agg.volume = state.get('volume', 0)
        agg.amount = state.get('amount', 0.0)
        agg.open_interest = state.get('open_interest')
        agg.kline_start_time = state.get('kline_start_time')
        return agg


class QueryService:
    """
    数据查询服务
    
    职责：
    1. 合约信息查询（缓存优先）
    2. K 线/Tick 数据查询
    3. 期权链关联查询
    4. 数据统计与摘要
    5. 数据导出到 CSV
    
    使用方式：
        query_service = QueryService(storage_instance)
        instruments = query_service.get_active_instruments_by_product('IF')
        kline_data = query_service.get_latest_kline('IF2603', limit=100)
    """
    
    def __init__(self, storage_instance):
        """
        初始化查询服务
        
        Args:
            storage_instance: InstrumentDataManager 实例（用于访问数据库和缓存）
        """
        self._storage = storage_instance
        # M23-Bug2: 反向索引缓存 {internal_id: instrument_id} 加速O(1)查找
        self._internal_id_to_instrument_idx: Dict[int, str] = {}
        self._idx_built = False

    def _get_cached_instruments(self) -> Dict[str, Dict[str, Any]]:
        return self._storage._params_service.get_all_instrument_cache()

    def _ensure_internal_id_index(self) -> Dict[int, str]:
        """M23-Bug2修复：确保反向索引已构建，O(1)查找"""
        if self._idx_built:
            return self._internal_id_to_instrument_idx
        
        instruments = self._get_cached_instruments()
        self._internal_id_to_instrument_idx.clear()
        for instrument_id, info in instruments.items():
            internal_id = self._get_info_internal_id(info)
            if internal_id is not None:
                self._internal_id_to_instrument_idx[internal_id] = instrument_id
        self._idx_built = True
        return self._internal_id_to_instrument_idx

    def _get_info_internal_id(self, info: Optional[Dict[str, Any]]) -> Optional[int]:
        return self._storage._get_info_internal_id(info)
    
    # ========================================================================
    # 合约分类与推断
    # ========================================================================
    
    def classify_instruments(self, instrument_ids: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
        """
        分类合约 ID 列表为期货和期权
        
        Args:
            instrument_ids: 合约 ID 列表
            
        Returns:
            Tuple[List[str], Dict[str, List[str]]]: (futures_list, options_dict)
            - futures_list: 期货合约列表
            - options_dict: 期权合约字典 {underlying: [option_ids]}
        """
        # ✅ 委托给 subscription_manager 的统一解析函数
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        return SubscriptionManager.classify_instruments(instrument_ids)
    
    # ✅ 删除classify_instruments_static，统一使用实例方法
        
    def infer_exchange_from_id(self, instrument_id: str) -> str:
        """
        从合约 ID 推断交易所（唯一方法）。
            
        Args:
            instrument_id: 合约 ID（如 'IF2603', 'CU2406'）
                
        Returns:
            str: 交易所代码（如 'CFFEX', 'SHFE'）
            
        ✅ 修复标准：统一为params_service缓存查询+config_service降级
        """
        normalized_id = str(instrument_id or '').strip()
        if not normalized_id:
            return 'UNKNOWN'
        
        # ✅ 第一优先级：params_service缓存查询（唯一权威源）
        try:
            if self._storage and self._storage._params_service:
                info = self._storage._params_service.get_instrument_meta_by_id(normalized_id)
                if info:
                    exchange = str(info.get('exchange') or '').strip()
                    if exchange and exchange.upper() != 'AUTO':
                        return exchange
        except Exception as cache_error:
            logging.warning("[QueryService] Failed to get exchange from cache: %s", cache_error)
        
        # ✅ 第二优先级：config_service降级解析
        try:
            from ali2026v3_trading.config_service import resolve_product_exchange
            
            exchange = resolve_product_exchange(normalized_id)
            if not exchange:
                logging.debug("[QueryService] resolve_product_exchange returned None for %s", normalized_id)
                return 'UNKNOWN'
            
            # 验证返回值，防止 AUTO 污染
            exchange_str = str(exchange).strip()
            if exchange_str.upper() == 'AUTO':
                logging.warning("[QueryService] resolve_product_exchange returned AUTO for %s, using UNKNOWN", normalized_id)
                return 'UNKNOWN'
            
            return exchange_str
            
        except ImportError as e:
            logging.warning("[QueryService] config_service not available: %s", e)
            return 'UNKNOWN'
        except (KeyError, AttributeError, ValueError) as e:
            # 数据缺失或格式错误，返回 UNKNOWN 标记
            logging.debug("[QueryService] resolve_product_exchange failed for %s: %s", normalized_id, e)
            return 'UNKNOWN'
        except Exception as exc:
            logging.error("[QueryService] resolve_product_exchange failed for %s: %s", normalized_id, exc, exc_info=True)
            raise
    
    # ========================================================================
    # 合约注册与缓存查询
    # ========================================================================
    
    def get_registered_instrument_ids(self, instrument_ids: Optional[List[str]] = None) -> List[str]:
        """
        获取已注册的合约 ID 列表，可选按给定 instrument_ids 过滤。
        
        Args:
            instrument_ids: 可选的合约 ID 列表进行过滤
            
        Returns:
            List[str]: 已注册的合约 ID 列表
            
        ✅ 修复标准：统一为params_service单一路径
        """
        if not instrument_ids:
            return self._storage._params_service.get_all_instrument_ids()

        registered_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids:
            normalized_id = str(instrument_id).strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)

            # ✅ 统一为params_service缓存查询（唯一权威源）
            if self._storage._params_service.get_instrument_meta_by_id(normalized_id):
                registered_ids.append(normalized_id)

        return registered_ids
    
    def ensure_registered_instruments(self, instrument_ids: List[str]) -> Dict[str, int]:
        """
        比对给定合约列表与已注册缓存/数据库，只为缺失合约创建内部 ID。
        
        Args:
            instrument_ids: 需要确保已注册的合约 ID 列表
            
        Returns:
            Dict[str, int]: 统计信息字典
        """
        normalized_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids or []:
            normalized_id = str(instrument_id).strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)
            normalized_ids.append(normalized_id)

        registered_ids = set(self.get_registered_instrument_ids(normalized_ids))
        missing_ids = [instrument_id for instrument_id in normalized_ids if instrument_id not in registered_ids]

        created_count = 0
        failed_count = 0
        for instrument_id in missing_ids:
            try:
                self._storage.register_instrument(
                    instrument_id=instrument_id,
                    exchange=self.infer_exchange_from_id(instrument_id),
                )
                created_count += 1
            except ValueError as e:
                failed_count += 1
                logging.warning("预注册合约失败 %s: %s", instrument_id, e)

        result = {
            'configured_count': len(normalized_ids),
            'registered_count': len(registered_ids),
            'missing_count': len(missing_ids),
            'created_count': created_count,
            'failed_count': failed_count,
        }
        
        # 发布预注册完成事件
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            event_bus = get_global_event_bus()
            if event_bus:
                event_bus.publish('PreRegisterCompleted', result)
                logging.info("[QueryService] 预注册完成，已发布PreRegisterCompleted事件")
        except Exception as e:
            logging.warning("[QueryService] 发布预注册事件失败: %s", e)
        
        return result

    def load_and_preregister_instruments(self, storage: Any, params: Any) -> Dict[str, Any]:
        """从合约配置文件加载合约列表并执行预注册（供on_init调用）。

        设计约束：
        1. 合约列表唯一来源：TXT配置文件，不存在任何回退机制，失败即终止初始化
        2. 预注册同步执行，创建ID映射表是后续数据关联的前提
        3. 重试3次快速失败：每步骤最多重试3次，3次均失败则抛RuntimeError
        4. 完成后通知：发布InstrumentsLoadAndPreregisterCompleted事件

        Args:
            storage: InstrumentDataManager实例（strategy_core_service.storage）
            params: 参数对象
        """
        import time as _time
        MAX_RETRIES = 3

        # ========== 阶段1：从TXT合约配置文件加载合约列表（重试3次） ==========
        selected_futures_list: List[str] = []
        selected_options_dict: Dict[str, List[str]] = {}
        last_load_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                from ali2026v3_trading.params_service import get_params_service
                ps = get_params_service()
                class _TempParams:
                    future_instruments = []
                    option_instruments = {}
                temp_params = _TempParams()
                file_result = ps.load_instrument_list(temp_params, source='output_files')
                if file_result and (file_result.get('futures_list') or file_result.get('options_dict')):
                    selected_futures_list = self._normalize_instruments(file_result['futures_list'])
                    selected_options_dict = self._normalize_options_dict(file_result['options_dict'])
                    logging.info(
                        "[Init-Load] 第%d次尝试成功: 从合约配置文件加载 %d 期货, %d 期权",
                        attempt, len(selected_futures_list),
                        self._count_option_contracts(selected_options_dict),
                    )
                    last_load_error = None
                    break
                else:
                    last_load_error = f"第{attempt}次: load_instrument_list返回空结果"
                    logging.warning("[Init-Load] %s", last_load_error)
            except Exception as e:
                last_load_error = f"第{attempt}次: {type(e).__name__}: {e}"
                logging.warning("[Init-Load] 合约配置文件加载异常: %s", last_load_error)
        else:
            error_detail = (
                f"合约配置文件加载经{MAX_RETRIES}次重试均失败，策略初始化终止。\n"
                f"最后错误: {last_load_error}\n"
                f"请检查文件是否存在且非空:\n"
                f"  - ali2026v3_trading/subscription_futures_fixed.txt\n"
                f"  - ali2026v3_trading/subscription_options_fixed.txt\n"
                f"  - ensure_products_with_retry是否已在步骤1中成功执行"
            )
            logging.error("[Init-Load] ❌ %s", error_detail)
            raise RuntimeError(error_detail)

        if not selected_futures_list and not selected_options_dict:
            error_detail = (
                f"合约配置文件加载成功但规范化后为空（期货=0, 期权=0），策略初始化终止。\n"
                f"可能原因: TXT文件内容格式错误，所有合约被_normalize过滤掉"
            )
            logging.error("[Init-Load] ❌ %s", error_detail)
            raise RuntimeError(error_detail)

        # ========== 补齐标的期货 ==========
        derived_futures = self._derive_underlying_futures(selected_options_dict)
        if derived_futures:
            existing_futures = set(selected_futures_list)
            added_futures = [item for item in derived_futures if item not in existing_futures]
            if added_futures:
                selected_futures_list.extend(added_futures)
                logging.info("[Init-Load] 从期权清单补齐 %d 个标的期货", len(added_futures))

        # 构建完整订阅列表
        subscribe_list = list(selected_futures_list)
        for option_ids in selected_options_dict.values():
            subscribe_list.extend(option_ids or [])
        seen = set()
        subscribed_instruments = [inst for inst in subscribe_list if not (inst in seen or seen.add(inst))]

        # ========== 阶段2：预注册（重试3次） ==========
        preregister_stats = None
        last_prereg_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            if not storage or not subscribed_instruments:
                last_prereg_error = f"第{attempt}次: storage不可用或subscribed_instruments为空"
                logging.warning("[PreRegister] %s", last_prereg_error)
                continue
            try:
                prereg_start = _time.perf_counter()
                logging.info("[PreRegister] 第%d次尝试: 开始预注册 %d 个合约...", attempt, len(subscribed_instruments))
                preregister_stats = storage.ensure_registered_instruments(subscribed_instruments)
                prereg_elapsed = _time.perf_counter() - prereg_start
                logging.info(
                    "[PreRegister] 第%d次尝试成功 (耗时=%.3fs): 配置=%d, 已注册=%d, 缺失=%d, 新建=%d, 失败=%d",
                    attempt, prereg_elapsed,
                    preregister_stats['configured_count'],
                    preregister_stats['registered_count'],
                    preregister_stats['missing_count'],
                    preregister_stats['created_count'],
                    preregister_stats['failed_count'],
                )
                if preregister_stats['failed_count'] > 0:
                    logging.warning(
                        "[PreRegister] ⚠️ 预注册完成但有 %d 个合约注册失败（将在tick到达时惰性注册）",
                        preregister_stats['failed_count'],
                    )
                last_prereg_error = None
                break
            except Exception as prereg_e:
                last_prereg_error = f"第{attempt}次: {type(prereg_e).__name__}: {prereg_e}"
                logging.warning("[PreRegister] 预注册异常: %s", last_prereg_error)
        else:
            error_detail = (
                f"预注册经{MAX_RETRIES}次重试均失败，策略初始化终止。\n"
                f"合约数: {len(subscribed_instruments)}\n"
                f"最后错误: {last_prereg_error}\n"
                f"可能原因: storage未初始化、DB连接失败、register_instrument内部异常"
            )
            logging.error("[PreRegister] ❌ %s", error_detail)
            raise RuntimeError(error_detail)

        result = {
            'futures_list': selected_futures_list,
            'options_dict': selected_options_dict,
            'subscribed_instruments': subscribed_instruments,
            'source': 'output_files',
            'preregister_stats': preregister_stats,
        }

        # ========== 完成后通知 ==========
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            event_bus = get_global_event_bus()
            if event_bus:
                event_data = {
                    'futures_count': len(selected_futures_list),
                    'options_count': self._count_option_contracts(selected_options_dict),
                    'total_instruments': len(subscribed_instruments),
                    'preregister_stats': preregister_stats,
                    'source': 'output_files',
                }
                event_bus.publish('InstrumentsLoadAndPreregisterCompleted', event_data)
                logging.info(
                    "[Init-Load+PreRegister] ✅ 已发布InstrumentsLoadAndPreregisterCompleted事件: "
                    "期货=%d, 期权=%d, 共=%d",
                    event_data['futures_count'],
                    event_data['options_count'],
                    event_data['total_instruments'],
                )
        except Exception as event_e:
            logging.warning("[Init-Load+PreRegister] 发布完成事件失败（不影响流程）: %s", event_e)

        return result

    @staticmethod
    def _normalize_instruments(instrument_ids: List[str]) -> List[str]:
        """去重、去空、去交易所前缀"""
        seen = set()
        result = []
        for inst_id in instrument_ids or []:
            normalized = str(inst_id or '').strip()
            if '.' in normalized:
                _, normalized = normalized.split('.', 1)
            if normalized and normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
        return result

    @staticmethod
    def _normalize_options_dict(options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """规范化期权字典（去重、去空、去交易所前缀）"""
        result = {}
        for underlying, option_ids in (options_dict or {}).items():
            normalized_underlying = str(underlying or '').strip()
            if '.' in normalized_underlying:
                _, normalized_underlying = normalized_underlying.split('.', 1)
            normalized_ids = []
            seen = set()
            for opt_id in option_ids or []:
                normalized_opt = str(opt_id or '').strip()
                if '.' in normalized_opt:
                    _, normalized_opt = normalized_opt.split('.', 1)
                if normalized_opt and normalized_opt not in seen:
                    seen.add(normalized_opt)
                    normalized_ids.append(normalized_opt)
            if normalized_underlying and normalized_ids:
                result[normalized_underlying] = normalized_ids
        return result

    @staticmethod
    def _count_option_contracts(options_dict: Dict[str, List[str]]) -> int:
        """计算期权合约总数"""
        return sum(len(v) for v in (options_dict or {}).values())

    @staticmethod
    def _derive_underlying_futures(options_dict: Dict[str, List[str]]) -> List[str]:
        """从期权字典推导标的期货列表"""
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        underlying_set = set()
        for underlying in (options_dict or {}).keys():
            clean = str(underlying or '').strip()
            if '.' in clean:
                _, clean = clean.split('.', 1)
            try:
                parsed = SubscriptionManager.parse_future(clean)
                if parsed.get('product') and parsed.get('year_month'):
                    underlying_set.add(f"{parsed['product']}{parsed['year_month']}")
            except (ValueError, Exception):
                underlying_set.add(clean)
        return sorted(underlying_set)

    # ========================================================================
    # 品种与合约查询
    # ========================================================================
    
    def get_active_instruments_by_product(self, product: str) -> List[str]:
        """
        获取指定品种的所有活跃合约 (DuckDB 版本)
        
        Args:
            product: 品种代码（如 'IF', 'IO'）
            
        Returns:
            List[str]: 合约 ID 列表
        """
        instrument_ids = []
        normalized_product = str(product or '').strip()
        if not normalized_product:
            return instrument_ids
        
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                
                futures = ds.query(
                    "SELECT instrument_id FROM futures_instruments "
                    "WHERE product=? ORDER BY instrument_id",
                    [normalized_product]
                )
                if hasattr(futures, 'num_rows') and futures.num_rows > 0:
                    for row in futures.read_all().to_pylist():
                        instrument_ids.append(row.get('instrument_id'))
                
                options = ds.query(
                    "SELECT instrument_id FROM option_instruments "
                    "WHERE product=? ORDER BY instrument_id",
                    [normalized_product]
                )
                if hasattr(options, 'num_rows') and options.num_rows > 0:
                    for row in options.read_all().to_pylist():
                        instrument_ids.append(row.get('instrument_id'))
        except Exception as e:
            logging.error("[QueryService] get_active_instruments_by_product failed: %s", e)
        
        return instrument_ids
    
    def get_active_instruments_by_products(self, products: List[str]) -> List[str]:
        """
        获取多个品种的所有活跃合约
        
        Args:
            products: 品种代码列表（如 ['IF', 'IH', 'IC']）
            
        Returns:
            List[str]: 合约 ID 列表
        """
        all_instrument_ids = []
        for product in products:
            instrument_ids = self.get_active_instruments_by_product(product.strip())
            all_instrument_ids.extend(instrument_ids)
        return all_instrument_ids
    
    def get_current_month_contracts(self, product: str) -> List[str]:
        """
        获取指定品种的当月合约
        
        Args:
            product: 品种代码
            
        Returns:
            List[str]: 当月合约 ID 列表
        """
        current_month = datetime.now().strftime('%y%m')
        
        all_instruments = self.get_active_instruments_by_product(product)
        current_month_contracts = []
        
        # ✅ 使用 params_service 元数据获取年月，而非正则
        from ali2026v3_trading.params_service import get_params_service
        ps = get_params_service()
        for inst_id in all_instruments:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta and meta.get('year_month') == current_month:
                current_month_contracts.append(inst_id)
        
        return current_month_contracts
    
    def get_next_month_contracts(self, product: str) -> List[str]:
        """
        获取指定品种的下月合约
        
        Args:
            product: 品种代码
            
        Returns:
            List[str]: 下月合约 ID 列表
        """
        next_month = self._storage._get_next_year_month()
        
        all_instruments = self.get_active_instruments_by_product(product)
        next_month_contracts = []
        
        # ✅ 使用 params_service 元数据获取年月，而非正则
        from ali2026v3_trading.params_service import get_params_service
        ps = get_params_service()
        for inst_id in all_instruments:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta and meta.get('year_month') == next_month:
                next_month_contracts.append(inst_id)
        
        return next_month_contracts
    
    # ========================================================================
    # 基础数据查询
    # ========================================================================
    
    # 注意：K 线查询功能已迁移到 DataService (data_service.py)
    # - get_kline_range(): 时间范围查询（Arrow 格式）
    # - get_latest_klines(): 最新 N 条 K 线
    # - get_kline_count(): K 线总数
    # - get_kline_stats(): 统计数据
    # 请使用 DataService 进行高性能 K 线查询
    
    # ========================================================================
    # 期权链查询
    # ========================================================================
    
    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """
        根据期货合约代码获取期权链（唯一入口）。
        
        Args:
            future_instrument_id: 期货合约代码（如 'IF2603'）
            
        Returns:
            Dict: 期权链信息 {'future': {...}, 'options': [...]}
        """
        info = self._storage._get_instrument_info(future_instrument_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货合约：{future_instrument_id}")
        
        future_id = self._get_info_internal_id(info)
        
        options = []
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                # 只走 underlying_future_id 主关联
                result = ds.query(
                    "SELECT internal_id, instrument_id, option_type, strike_price "
                    "FROM option_instruments "
                    "WHERE underlying_future_id=? "
                    "ORDER BY strike_price, option_type",
                    [future_id]
                )
                if hasattr(result, 'num_rows') and result.num_rows > 0:
                    options = [
                        {
                            'internal_id': row.get('internal_id'),
                            'instrument_id': row.get('instrument_id'),
                            'option_type': row.get('option_type'),
                            'strike_price': row.get('strike_price')
                        }
                        for row in result.read_all().to_pylist()
                    ]
        except Exception as e:
            logging.error("[QueryService] get_option_chain_for_future failed: %s", e)

        return {
            'future': {'internal_id': future_id, 'instrument_id': future_instrument_id},
            'options': options
        }
    
    # ✅ 删除get_option_chain_by_future_id，统一使用get_option_chain_for_future
    
    # ========================================================================
    # 高级查询（最新数据）
    # ========================================================================
    
    # ========================================================================
    # ID 迁移与序列管理
    # ========================================================================
    
    # 方法唯一修复#69-71：删除3个DEPRECATED方法体，已迁移到StorageMaintenanceService
    
    def _migrate_instrument_ids_to_global_namespace(self) -> int:
        """[DEPRECATED] 已迁移到 StorageMaintenanceService。
        此方法不再可用，调用将直接抛出异常。
        """
        raise NotImplementedError(
            "_migrate_instrument_ids_to_global_namespace is deprecated. "
            "Use StorageMaintenanceService instead."
        )
    
    # ========================================================================
    # 统计数据查询
    # ========================================================================
    
    # 注意：以下统计方法已被 DataService 的 get_kline_stats() 替代
    # 保留这些方法用于向后兼容，建议迁移到 DataService
    
    # ✅ 接口唯一：委托data_service.get_kline_count，不再独立SQL查询
    def get_kline_count(self, instrument_id: str) -> int:
        """
        获取 K 线总条数 (DuckDB 版本)
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            int: K 线总条数
        """
        try:
            if _HAS_DATA_SERVICE:
                return get_data_service().get_kline_count(instrument_id)
        except Exception as e:
            logging.error("[QueryService] get_kline_count failed: %s", e)
        return 0
    
    def get_tick_count(self, instrument_id: str) -> int:
        """
        获取 Tick 总条数 (DuckDB 版本)
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            int: Tick 总条数
        """
        info = self._storage._get_instrument_info(instrument_id)
        if not info:
            return 0
        
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                result = ds.query(
                    "SELECT COUNT(*) as cnt FROM ticks_raw WHERE instrument_id=?",
                    [instrument_id],
                    arrow=False  # 返回 pandas DataFrame
                )
                if result is not None and hasattr(result, 'iloc') and len(result) > 0:
                    return int(result.iloc[0]['cnt'])
        except Exception as e:
            logging.error("[QueryService] get_tick_count failed: %s", e)
        return 0
    
    # ✅ 接口唯一：委托data_service.get_kline_range，不再独立SQL查询
    def get_kline_range(self, instrument_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        获取 K 线数据的时间范围 (DuckDB 版本)
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            Tuple[Optional[str], Optional[str]]: (最早时间，最晚时间)
        """
        try:
            if _HAS_DATA_SERVICE:
                return get_data_service().get_kline_range(instrument_id)
        except Exception as e:
            logging.error("[QueryService] get_kline_range failed: %s", e)
        return (None, None)
    
    def get_tick_range(self, instrument_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        获取 Tick 数据的时间范围 (DuckDB 版本)
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            Tuple[Optional[str], Optional[str]]: (最早时间，最晚时间)
        """
        info = self._storage._get_instrument_info(instrument_id)
        if not info:
            return (None, None)
        
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                result = ds.query(
                    "SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts "
                    "FROM ticks_raw WHERE instrument_id=?",
                    [instrument_id]
                )
                if hasattr(result, 'num_rows') and result.num_rows > 0:
                    row = result.read_all().to_pylist()[0]
                    min_ts = row.get('min_ts')
                    max_ts = row.get('max_ts')
                    return (
                        str(min_ts) if min_ts else None,
                        str(max_ts) if max_ts else None
                    )
        except Exception as e:
            logging.error("[QueryService] get_tick_range failed: %s", e)
        return (None, None)
    
    # ========================================================================
    # 统计与摘要
    # ========================================================================
    
    def get_instrument_summary(self, instrument_id: str) -> Dict[str, Any]:
        """
        获取合约数据摘要统计
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            Dict[str, Any]: 合约摘要统计
        """
        info = self._storage._get_instrument_info(instrument_id)
        if not info:
            return {}
        
        kline_count = self.get_kline_count(instrument_id)
        tick_count = self.get_tick_count(instrument_id)
        kline_range = self.get_kline_range(instrument_id)
        tick_range = self.get_tick_range(instrument_id)
        
        return {
            'instrument_id': instrument_id,
            'internal_id': self._get_info_internal_id(info),
            'type': info['type'],
            'product': info.get('product'),
            'year_month': info.get('year_month'),
            'kline_count': kline_count,
            'tick_count': tick_count,
            'kline_first': kline_range[0],
            'kline_last': kline_range[1],
            'tick_first': tick_range[0],
            'tick_last': tick_range[1]
        }
    
    def get_all_instruments_summary(self) -> List[Dict[str, Any]]:
        """
        获取所有合约的摘要统计
        
        Returns:
            List[Dict[str, Any]]: 所有合约的摘要列表
        """
        summaries = []
        for instrument_id in self._storage._params_service.get_all_instrument_ids():
            summary = self.get_instrument_summary(instrument_id)
            if summary:
                summaries.append(summary)
        return summaries
    
    def get_storage_stats(self) -> Dict[str, Any]:
        tables = []
        total_size = 0
        
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                result = ds.query("""
                    SELECT table_name as name, estimated_size as size_bytes
                    FROM duckdb_tables()
                    ORDER BY estimated_size DESC
                """)
                
                for row in (result.read_all().to_pylist() if hasattr(result, 'to_pylist') else []):
                    tables.append({'name': row['name'], 'size_bytes': row.get('size_bytes', 0) or 0})
                    total_size += (row.get('size_bytes', 0) or 0)
            else:
                logging.warning("[QueryService] DataService not available for storage stats")
        except Exception as e:
            logging.error("[QueryService] Failed to get storage stats: %s", e)
        
        return {
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'tables': tables,
            'num_futures': len([k for k, v in self._get_cached_instruments().items() if v.get('type') == 'future']),
            'num_options': len([k for k, v in self._get_cached_instruments().items() if v.get('type') == 'option'])
        }
    
    # ========================================================================
    # 数据导出
    # ========================================================================
    
    def export_kline_to_csv(self, instrument_id: str, output_file: str,
                           start_time: Optional[str] = None,
                           end_time: Optional[str] = None) -> int:
        """
        导出 K 线数据到 CSV 文件（DuckDB 版本）
        
        Args:
            instrument_id: 合约代码
            output_file: 输出文件路径
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）
            
        Returns:
            int: 导出的记录数
        """
        try:
            # P1 Bug #51修复：使用get_data_service()而非直接构造DataService
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
            else:
                logging.error("导出失败：DataService不可用")
                return 0
            
            start_dt = datetime.fromisoformat(start_time) if start_time else None
            end_dt = datetime.fromisoformat(end_time) if end_time else None
            
            if not start_dt or not end_dt:
                stats = ds.get_kline_stats(instrument_id)
                if not stats:
                    logging.error("导出失败：合约不存在 %s", instrument_id)
                    return 0
                start_dt = stats.get('first_time')
                end_dt = stats.get('last_time')
            
            klines = ds.get_kline_range(instrument_id, start_dt, end_dt)
            
            df = klines.to_pandas()
            df.to_csv(output_file, index=False)
            
            logging.info("导出 K 线 %d 条到 %s", len(df), output_file)
            return len(df)
            
        except ImportError:
            logging.error("data_service 不可用，无法导出")
            return 0
        except Exception as e:
            logging.error("[QueryService] export_kline_to_csv failed: %s", e)
            return 0
    
    def export_tick_to_csv(self, instrument_id: str, output_file: str,
                          start_time: Optional[str] = None,
                          end_time: Optional[str] = None) -> int:
        """
        导出 Tick 数据到 CSV 文件（DuckDB 版本）
        
        Args:
            instrument_id: 合约代码
            output_file: 输出文件路径
            start_time: 开始时间（可选）
            end_time: 结束时间（可选）
            
        Returns:
            int: 导出的记录数
        """
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                
                start_dt = datetime.fromisoformat(start_time) if start_time else None
                end_dt = datetime.fromisoformat(end_time) if end_time else None
                
                # P1 Bug #52修复：None时使用默认时间范围，避免AttributeError
                if not start_dt or not end_dt:
                    stats = ds.get_kline_stats(instrument_id)
                    if not stats:
                        logging.error("导出失败：合约不存在 %s", instrument_id)
                        return 0
                    start_dt = start_dt or stats.get('min_time')
                    end_dt = end_dt or stats.get('max_time')
                
                result = ds.get_time_range(instrument_id, start_dt, end_dt)
                
                if hasattr(result, 'to_pandas'):
                    df = result.to_pandas()
                    df.to_csv(output_file, index=False)
                    logging.info("导出 Tick %d 条到 %s", len(df), output_file)
                    return len(df)
                elif hasattr(result, 'to_pylist'):
                    rows = result.read_all().to_pylist()
                    with open(output_file, 'w', newline='', encoding='utf-8') as f:
                        if rows:
                            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                            writer.writeheader()
                            for row in rows:
                                writer.writerow(row)
                    logging.info("导出 Tick %d 条到 %s", len(rows), output_file)
                    return len(rows)
            else:
                logging.error("DataService 不可用，无法导出")
                return 0
        except Exception as e:
            logging.error("[QueryService] export_tick_to_csv failed: %s", e)
            return 0
    
    def _diagnose_contract(self, instrument_id: str, is_future: bool) -> Dict:
        """
        诊断单个合约的状态 (DuckDB 版本)
        
        Returns:
            dict: {
                'subscribed': bool,
                'subscribe_error': str,
                'enqueued': bool,
                'enqueue_error': str,
                'received': bool,
                'receive_error': str,
                'stored': bool,
                'store_error': str,
                'kline_table': str,
                'tick_table': str,
                'kline_count': int,
                'tick_count': int,
            }
        """
        result = {
            'subscribed': False,
            'subscribe_error': '',
            'enqueued': False,
            'enqueue_error': '',
            'received': False,
            'receive_error': '',
            'stored': False,
            'store_error': '',
            'kline_table': '',
            'tick_table': '',
            'kline_count': 0,
            'tick_count': 0,
        }
        
        try:
            info = self._storage._get_instrument_info(instrument_id)
            
            if not info:
                result['subscribe_error'] = f'合约 {instrument_id} 不存在'
                return result
            
            internal_id = self._get_info_internal_id(info)
            # ✅ 统一使用 klines_raw 和 ticks_raw 表
            
            result['kline_table'] = 'klines_raw'
            result['tick_table'] = 'ticks_raw'
            
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                
                # subscriptions 表已废弃，跳过订阅状态检查
                
                kline_count = self.get_kline_count(instrument_id)
                tick_count = self.get_tick_count(instrument_id)
                result['kline_count'] = kline_count
                result['tick_count'] = tick_count
                
                if kline_count > 0 or tick_count > 0:
                    result['enqueued'] = True
                    result['received'] = True
                    result['stored'] = True
                else:
                    result['enqueue_error'] = '数据表为空或不存在'
            
        except Exception as e:
            result['store_error'] = f'诊断过程出错：{str(e)}'
        
        return result


# ============================================================================
# 存储维护服务（从 storage_maintenance_service.py 迁移）
# ============================================================================

class StorageMaintenanceService:
    """
    存储服务 - 数据自愈能力
    
    职责:
    - 启动时执行元数据表检查
    - 孤儿记录清理
    - 空表检测与清理
    - 订阅标准化检查
    - **新增：2605 合约日志诊断（每 30 秒输出）**
    
    设计原则:
    - 轻量级：只执行必要的检查
    - 非侵入：不影响正常业务流程
    - 自动化：启动时自动执行
    """
    
    MAINTENANCE_VERSION = 20260403
    
    # 2605 合约诊断配置
    DIAGNOSTIC_CONTRACTS_2605 = [
        {'exchange': 'CFFEX', 'future': 'IF2605', 'option': 'IO2605-C-4500'},  # CFFEX 沪深 300 股指期权
        {'exchange': 'SHFE', 'future': 'CU2605', 'option': 'cu2605C100000'},  # SHFE 铜期权 (小写 +C+ 行权价)
        {'exchange': 'DCE', 'future': 'M2605', 'option': 'm2605-C-2950'},  # DCE 豆粕期权 (小写)
        {'exchange': 'CZCE', 'future': 'MA2605', 'option': 'MA605C3000'},  # CZCE PTA 期权 (605 表示 2605)
        {'exchange': 'INE', 'future': 'LU2605', 'option': 'sc2605C660'},  # INE 原油期权 (小写 +C+ 行权价)
        {'exchange': 'GFEX', 'future': 'LC2605', 'option': 'lc2605-C-16000'},  # GFEX 工业硅期权
    ]
    
    def __init__(self, manager: Any):
        """初始化维护服务"""
        self.manager = manager
        self._sequence_lock = threading.Lock()
        logging.info("[StorageMaintenance] 服务已初始化")
    
    def run_startup_checks_fast_path(self, conn=None) -> None:
        """P1-3修复：startup checks fast path。
        
        正常重启时，如果 maintenance version 已是最新，只执行轻量健康检查
        （序列同步 + 缺表修复），跳过重维护逻辑。
        只有 maintenance version 变化时，才执行全量 startup checks。
        
        设计约束（修改必看十原则）：
        1. fast path 必须包含序列同步（轻量但关键，防止 ID 冲突）
        2. fast path 必须包含缺表修复（轻量但关键，防止表缺失）
        3. 重维护逻辑（orphan 恢复、FK 修复、订阅去重等）只在 version 变化时执行
        4. 新增迁移脚本、缺表修复、索引补建时，必须声明是否允许在启动热路径执行
        """
        try:
            logging.info("[StorageMaintenance] fast path 启动检查开始...")
            
            if not _HAS_DATA_SERVICE:
                logging.warning("[StorageMaintenance] DataService 不可用，跳过维护检查")
                return
            
            ds = get_data_service()
            
            # 轻量检查：序列同步（关键，防止 ID 冲突）
            self.sync_instrument_id_sequence()
            
            # 轻量检查：缺表修复（关键，防止表缺失）
            repaired_tables = 0
            if self.manager is not None and hasattr(self.manager, '_repair_missing_instrument_tables'):
                try:
                    repaired_tables = self.manager._repair_missing_instrument_tables()
                except Exception as e:
                    logging.warning("[StorageMaintenance] _repair_missing_instrument_tables failed: %s", e)
            
            # 检查 maintenance version：匹配则跳过重维护
            current = self.get_kv_value('storage_maintenance_version')
            if current == str(self.MAINTENANCE_VERSION):
                logging.info(
                    "[StorageMaintenance] fast path: maintenance version 匹配 (%s)，跳过重维护 (repaired_tables=%d)",
                    current, repaired_tables,
                )
                return
            
            # version 不匹配：执行全量 startup checks
            logging.info(
                "[StorageMaintenance] fast path: maintenance version 不匹配 (current=%s, expected=%s)，执行全量检查",
                current, self.MAINTENANCE_VERSION,
            )
            self.run_startup_checks(conn)
        except Exception as exc:
            logging.warning("[StorageMaintenance] fast path 失败，降级到全量检查: %s", exc)
            try:
                self.run_startup_checks(conn)
            except Exception as fallback_exc:
                logging.warning("[StorageMaintenance] 全量检查也失败: %s", fallback_exc)

    def run_startup_checks(self, conn=None) -> None:
        """
        启动时执行必要检查 (DuckDB 版本)
        
        Args:
            conn: 数据库连接（已废弃，保留参数用于兼容）
            
        注意：不抛出异常，避免影响系统启动
        """
        try:
            logging.info("[StorageMaintenance] 开始启动检查...")
            
            if not _HAS_DATA_SERVICE:
                logging.warning("[StorageMaintenance] DataService 不可用，跳过维护检查")
                return
            
            ds = get_data_service()
            
            self.sync_instrument_id_sequence()

            repaired_tables = 0
            if self.manager is not None and hasattr(self.manager, '_repair_missing_instrument_tables'):
                try:
                    repaired_tables = self.manager._repair_missing_instrument_tables()
                except Exception as e:
                    logging.warning("[StorageMaintenance] _repair_missing_instrument_tables failed: %s", e)

            current = self.get_kv_value('storage_maintenance_version')
            if current == str(self.MAINTENANCE_VERSION):
                logging.info("[StorageMaintenance] 已是最新版本，跳过其余检查（repaired_tables=%d）", repaired_tables)
                return
            
            recovered_orphans = self.recover_orphan_option_metadata()
            seeded_option_products = self.ensure_option_product_catalog()
            repaired_option_fk = self.repair_option_underlying_product_references()
            exchange_updates = self.backfill_metadata_exchange()
            dedup_subs = 0
            normalized_subs = 0
            dropped_empty_tables = 0
            
            self.set_kv_value('storage_maintenance_version', self.MAINTENANCE_VERSION)
            
            logging.info(
                "[StorageMaintenance] 完成：repaired_tables=%d, recovered_orphans=%d, seeded_option_products=%d, repaired_option_fk=%d, exchange_updates=%d, normalized_subs=%d, dedup_subs=%d, dropped_empty_tables=%d",
                repaired_tables,
                recovered_orphans,
                seeded_option_products,
                repaired_option_fk,
                exchange_updates,
                normalized_subs,
                dedup_subs,
                dropped_empty_tables,
            )
        except Exception as exc:
            logging.warning("[StorageMaintenance] 因运行时锁定或维护失败而跳过：%s", exc)
    
    def run_periodic_diagnostic(self, conn=None) -> None:
        """
        运行 2605 合约日志诊断（手动调用，每 30 秒）(DuckDB 版本)
        
        Args:
            conn: 数据库连接（已废弃，保留参数用于兼容）
        
        使用方式：
            # 在策略主循环中每 30 秒调用一次
            if time.time() - last_diagnostic_time >= 30:
                maintenance.run_periodic_diagnostic()
                last_diagnostic_time = time.time()
        """
        try:
            logging.info("\n" + "="*80)
            logging.info("📊 [2605 合约诊断] 开始检查 (每 30 秒自动输出)")
            logging.info("="*80)
            
            total_futures_subscribed = 0
            total_futures_received = 0
            total_options_subscribed = 0
            total_options_received = 0
            
            for idx, contract in enumerate(self.DIAGNOSTIC_CONTRACTS_2605, 1):
                exchange = contract['exchange']
                future_id = contract['future']
                option_id = contract['option']
                
                future_status = self._diagnose_contract(future_id, is_future=True)
                if future_status['subscribed']:
                    total_futures_subscribed += 1
                if future_status['received']:
                    total_futures_received += 1
                
                option_status = None
                if option_id:
                    option_status = self._diagnose_contract(option_id, is_future=False)
                    if option_status['subscribed']:
                        total_options_subscribed += 1
                    if option_status['received']:
                        total_options_received += 1
                
                logging.info(f"\n[{idx}] {exchange} - {future_id} (期货)")
                
                if future_status['subscribed']:
                    logging.info(f"  订阅：[OK] 成功")
                else:
                    logging.error(f"  订阅：[FAIL] 失败")
                    if future_status['subscribe_error']:
                        logging.error(f"    原因：{future_status['subscribe_error']}")
                
                if future_status['enqueued']:
                    logging.info(f"  入队：[OK] 成功")
                else:
                    logging.error(f"  入队：[FAIL] 失败")
                    if future_status['enqueue_error']:
                        logging.error(f"    原因：{future_status['enqueue_error']}")
                
                has_data = future_status['received'] and future_status['kline_count'] > 0
                if has_data:
                    logging.info(f"  接收：[OK] 成功 ({future_status['kline_count']}条 K 线，{future_status['tick_count']}个 Tick)")
                elif future_status['received']:
                    logging.warning(f"  接收：[WARN] 无数据 ({future_status['kline_count']}条 K 线，{future_status['tick_count']}个 Tick)")
                else:
                    logging.error(f"  接收：[FAIL] 失败 ({future_status['kline_count']}条 K 线，{future_status['tick_count']}个 Tick)")
                    if future_status['receive_error']:
                        logging.error(f"    原因：{future_status['receive_error']}")
                
                if future_status['stored']:
                    logging.info(f"  落库：[OK] 成功 (K 线表：{future_status['kline_table']}, Tick 表：{future_status['tick_table']})")
                else:
                    logging.error(f"  落库：[FAIL] 失败")
                    if future_status['store_error']:
                        logging.error(f"    原因：{future_status['store_error']}")
                
                if option_id and option_status:
                    logging.info(f"\n[{idx}] {exchange} - {option_id} (期权)")
                    
                    if option_status['subscribed']:
                        logging.info(f"  订阅：[OK] 成功")
                    else:
                        logging.error(f"  订阅：[FAIL] 失败")
                        if option_status['subscribe_error']:
                            logging.error(f"    原因：{option_status['subscribe_error']}")
                    
                    if option_status['enqueued']:
                        logging.info(f"  入队：[OK] 成功")
                    else:
                        logging.error(f"  入队：[FAIL] 失败")
                        if option_status['enqueue_error']:
                            logging.error(f"    原因：{option_status['enqueue_error']}")
                    
                    has_data = option_status['received'] and option_status['kline_count'] > 0
                    if has_data:
                        logging.info(f"  接收：[OK] 成功 ({option_status['kline_count']}条 K 线，{option_status['tick_count']}个 Tick)")
                    elif option_status['received']:
                        logging.warning(f"  接收：[WARN] 无数据 ({option_status['kline_count']}条 K 线，{option_status['tick_count']}个 Tick)")
                    else:
                        logging.error(f"  接收：[FAIL] 失败 ({option_status['kline_count']}条 K 线，{option_status['tick_count']}个 Tick)")
                        if option_status['receive_error']:
                            logging.error(f"    原因：{option_status['receive_error']}")
                    
                    if option_status['stored']:
                        logging.info(f"  落库：[OK] 成功 (K 线表：{option_status['kline_table']}, Tick 表：{option_status['tick_table']})")
                    else:
                        logging.error(f"  落库：[FAIL] 失败")
                        if option_status['store_error']:
                            logging.error(f"    原因：{option_status['store_error']}")
                elif option_id:
                    logging.info(f"\n[{idx}] {exchange} - {option_id} (期权)")
                    logging.warning(f"  ❌ 期权合约不存在")
            
            logging.info("\n" + "-"*80)
            logging.info("📋 汇总统计:")
            total_options_count = len(self.DIAGNOSTIC_CONTRACTS_2605)
            logging.info(f"  期货：{total_futures_subscribed}/{total_options_count} 已订阅，{total_futures_received}/{total_options_count} 有数据")
            logging.info(f"  期权：{total_options_subscribed}/{total_options_count} 已订阅，{total_options_received}/{total_options_count} 有数据")
            logging.info("="*80 + "\n")
            
        except Exception as exc:
            logging.error(f"[2605 合约诊断] 执行失败：{exc}", exc_info=True)
    
    # subscriptions 表已废弃，相关方法已移除：
    # - deduplicate_subscriptions()
    # - normalize_subscription_types()
    # - _deduplicate_subscriptions_by_target_type()
    
    def ensure_instrument_id_sequence(self) -> int:
        """确保全局 instrument_id_sequence 存在并返回当前 next_id (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE:
            return 1
        try:
            ds = get_data_service()
            
            ds.query("""
                CREATE TABLE IF NOT EXISTS instrument_id_sequence (
                    name TEXT PRIMARY KEY,
                    next_id INTEGER NOT NULL
                )
            """)
            
            result = ds.query("SELECT next_id FROM instrument_id_sequence WHERE name = 'global'")
            rows = _result_to_pylist(result)
            if rows:
                row = rows[0]
                if row.get('next_id') is not None:
                    return int(row['next_id'])
            
            rows = _result_to_pylist(ds.query("""
                SELECT COALESCE(MAX(max_id), 0) + 1 AS next_id
                FROM (
                    SELECT MAX(internal_id) AS max_id FROM futures_instruments
                    UNION ALL
                    SELECT MAX(internal_id) AS max_id FROM option_instruments
                )
            """))
            next_id = int(rows[0].get('next_id', 0) or 0) if rows else 1
            next_id = next_id or 1
            
            ds.query(
                "INSERT INTO instrument_id_sequence (name, next_id) VALUES ('global', ?)",
                [next_id]
            )
            return next_id
        except Exception as e:
            logging.error("[StorageMaintenance] ensure_instrument_id_sequence failed: %s", e)
            return 1
    
    def reserve_next_global_id(self) -> int:
        """从统一序列中预留一个新的全局 internal_id (DuckDB 版本)
        
        线程安全：通过 _sequence_lock 保证读-改原子性。
        异常安全：失败时抛出 RuntimeError 而非返回默认值，避免 ID 冲突。
        """
        if not _HAS_DATA_SERVICE:
            raise RuntimeError("[StorageMaintenance] DataService unavailable, cannot reserve global ID")
        with self._sequence_lock:
            try:
                ds = get_data_service()
                next_id = self.ensure_instrument_id_sequence()
                ds.query(
                    "UPDATE instrument_id_sequence SET next_id = ? WHERE name = 'global'",
                    [next_id + 1]
                )
                return next_id
            except Exception as e:
                logging.error("[StorageMaintenance] reserve_next_global_id failed: %s", e)
                raise RuntimeError(f"[StorageMaintenance] reserve_next_global_id failed: {e}") from e
    
    def sync_instrument_id_sequence(self) -> None:
        """把全局序列推进到当前所有 instrument.id 的上界之后 (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE:
            return
        try:
            ds = get_data_service()
            next_id = self.ensure_instrument_id_sequence()
            
            rows = _result_to_pylist(ds.query("""
                SELECT COALESCE(MAX(max_id), 0) + 1 AS next_id
                FROM (
                    SELECT MAX(internal_id) AS max_id FROM futures_instruments
                    UNION ALL
                    SELECT MAX(internal_id) AS max_id FROM option_instruments
                )
            """))
            expected_next_id = int(rows[0].get('next_id', 0) or 0) if rows else 1
            expected_next_id = expected_next_id or 1
            
            if expected_next_id != next_id:
                ds.query(
                    "UPDATE instrument_id_sequence SET next_id = ? WHERE name = 'global'",
                    [expected_next_id]
                )
        except Exception as e:
            logging.error("[StorageMaintenance] sync_instrument_id_sequence failed: %s", e)
    
    def get_kv_value(self, key: str = None) -> Optional[str]:
        """获取 KV 存储值 (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE or not key:
            return None
        try:
            ds = get_data_service()
            rows = _result_to_pylist(ds.query("SELECT value FROM app_kv_store WHERE key=?", [key]))
            if rows:
                return str(rows[0].get('value'))
        except Exception as e:
            logging.error("[StorageMaintenance] get_kv_value failed: %s", e)
        return None
    
    def set_kv_value(self, key: str = None, value: Any = None) -> None:
        """设置 KV 存储值 (DuckDB 版本)"""
        if not _HAS_DATA_SERVICE or not key:
            return
        try:
            ds = get_data_service()
            ds.query(
                """
                INSERT INTO app_kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
                """,
                [key, str(value), time.time()]
            )
        except Exception as e:
            logging.error("[StorageMaintenance] set_kv_value failed: %s", e)
    
    def recover_orphan_option_metadata(self) -> int:
        """恢复孤儿期权元数据 (DuckDB 统一表版本)"""
        # ✅ Group A收口：不再使用LEGACYF0000占位符，历史数据已在迁移时处理
        # if self.manager is not None and hasattr(self.manager, 'ensure_legacy_placeholder_roots'):
        #     try:
        #         self.manager.ensure_legacy_placeholder_roots()
        #     except Exception as e:
        #         logging.warning("[StorageMaintenance] ensure_legacy_placeholder_roots failed: %s", e)
        
        # ⚠️ DEPRECATED: 此方法已废弃
        # 旧版本使用 tick_option_data 分表存储期权 Tick 数据，
        # 现已统一迁移到 ticks_raw 表。此方法保留仅为向后兼容，
        # 实际不再执行任何恢复操作。
        logging.warning("[StorageMaintenance] recover_orphan_options is deprecated, using unified ticks_raw table")
        return 0
    
    def backfill_metadata_exchange(self) -> int:
        """回填元数据交易所信息
        
        P2 Bug #111修复：明确返回值含义为"待更新行数"而非"已更新行数"
        DuckDB不支持changes()，因此先COUNT待更新行数，再执行UPDATE
        """
        if not _HAS_DATA_SERVICE:
            return 0
        ds = get_data_service()
        
        # 第一步：统计futures_instruments待更新行数
        result1 = ds.query("""
            SELECT COUNT(*) as cnt FROM futures_instruments
            WHERE COALESCE(exchange, '') IN ('', 'AUTO') AND product != 'LEGACY'
        """)
        rows = _result_to_pylist(result1)
        futures_count = int(rows[0].get('cnt', 0)) if rows else 0
        
        # 第二步：执行UPDATE
        if futures_count > 0:
            ds.query(
                """
                UPDATE futures_instruments
                SET exchange = (
                    SELECT fp.exchange FROM future_products fp
                    WHERE fp.product = futures_instruments.product
                )
                WHERE COALESCE(exchange, '') IN ('', 'AUTO')
                  AND product != 'LEGACY'
            """)
        
        # 第三步：统计option_instruments待更新行数
        result2 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE COALESCE(exchange, '') IN ('', 'AUTO') AND product != 'LEGACY'
        """)
        rows = _result_to_pylist(result2)
        option_count = int(rows[0].get('cnt', 0)) if rows else 0
        
        # 第四步：执行UPDATE
        if option_count > 0:
            ds.query(
                """
                UPDATE option_instruments
                SET exchange = (
                    SELECT op.exchange FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                )
                WHERE COALESCE(exchange, '') IN ('', 'AUTO')
                  AND product != 'LEGACY'
                """)
        
        # P2 Bug #111修复：返回待更新行数总和（因为DuckDB不支持changes()）
        return futures_count + option_count

    def repair_option_underlying_product_references(self) -> int:
        """修复 option_instruments.underlying_product 指向旧期货品种或缺失占位符的问题。
        
        P2 Bug #112修复：明确返回值含义为"待更新行数"而非"已更新行数"
        DuckDB不支持changes()，因此先COUNT待更新行数，再执行UPDATE
        """
        if not _HAS_DATA_SERVICE:
            return 0
        ds = get_data_service()
        updated = 0

        # 第一步：统计第一批待更新行数
        r1 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE EXISTS (SELECT 1 FROM option_products op WHERE UPPER(op.product) = UPPER(option_instruments.product))
              AND NOT EXISTS (SELECT 1 FROM option_products op_bad WHERE UPPER(op_bad.product) = UPPER(option_instruments.underlying_product))
        """)
        rows = _result_to_pylist(r1)
        count1 = int(rows[0].get('cnt', 0)) if rows else 0
        
        # 第二步：执行第一批UPDATE
        if count1 > 0:
            ds.query(
                """
                UPDATE option_instruments
                SET underlying_product = (
                    SELECT op.product
                    FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                    ORDER BY op.product
                    LIMIT 1
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM option_products op
                    WHERE UPPER(op.product) = UPPER(option_instruments.product)
                )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM option_products op_bad
                    WHERE UPPER(op_bad.product) = UPPER(option_instruments.underlying_product)
                  )
                """
            )
        updated += count1

        # 第三步：统计第二批待更新行数
        r2 = ds.query("""
            SELECT COUNT(*) as cnt FROM option_instruments
            WHERE COALESCE(product, '') = 'LEGACY' AND COALESCE(underlying_product, '') != 'LEGACY'
        """)
        rows = _result_to_pylist(r2)
        count2 = int(rows[0].get('cnt', 0)) if rows else 0
        
        # 第四步：执行第二批UPDATE
        if count2 > 0:
            ds.query(
                """
                UPDATE option_instruments
                SET underlying_product = 'LEGACY'
                WHERE COALESCE(product, '') = 'LEGACY'
                  AND COALESCE(underlying_product, '') != 'LEGACY'
                """
            )
        updated += count2

        # P2 Bug #112修复：返回待更新行数总和（因为DuckDB不支持changes()）
        return max(updated, 0)

    @staticmethod
    def _get_option_format_template(exchange: str) -> str:
        """按交易所返回期权合约格式模板。"""
        # 使用标准化函数，不直接变形
        exchange_upper = _normalize_code(exchange)
        if exchange_upper == 'CFFEX':
            return 'YYYYMM-C-XXXX'
        return '{product}{year_month}{option_type}{strike_price}'

    def ensure_option_product_catalog(self) -> int:
        """补齐并激活 option_products 中缺失的真实期权品种配置。"""

        try:
            from ali2026v3_trading.config_service import ExchangeConfig
        except Exception as exc:
            logging.warning("[StorageMaintenance] 加载 ExchangeConfig 失败，跳过 option_products 补齐: %s", exc)
            return 0

        updated = 0
        ds = self.manager._data_service if hasattr(self.manager, '_data_service') else None
        if not ds:
            return 0

        option_products = ExchangeConfig().option_products
        for option_product, product_meta in option_products.items():
            if not isinstance(product_meta, tuple) or len(product_meta) < 2:
                continue

            underlying_product, exchange = product_meta[:2]
            # 使用标准化函数，不直接变形
            normalized_product = _normalize_code(option_product)
            normalized_underlying = _normalize_code(underlying_product)
            normalized_exchange = _normalize_code(exchange)
            format_template = self._get_option_format_template(normalized_exchange)

            result = ds.query(
                "SELECT product, exchange, underlying_product, format_template, is_active "
                "FROM option_products WHERE UPPER(product)=? ORDER BY product LIMIT 1",
                (normalized_product,)
            )
            row = None
            rows = _result_to_pylist(result)
            if rows:
                row = rows[0]

            if row is None:
                try:
                    ds.query(
                        "INSERT INTO option_products "
                        "(product, exchange, underlying_product, format_template, tick_size, contract_size, is_active) "
                        "VALUES (?, ?, ?, ?, 1.0, 1.0, 1)",
                        (normalized_product, normalized_exchange, normalized_underlying, format_template),
                    )
                except Exception as exc:
                    logging.warning("[StorageMaintenance] 跳过 option_products 插入 %s: %s", normalized_product, exc)
                    continue
                updated += 1
            continue

        return updated
    
    def drop_empty_instrument_tables(self) -> int:
        """删除无数据的合约记录（统一表版本：DELETE而非DROP TABLE）"""
        deleted = 0
        ds = self.manager._data_service if hasattr(self.manager, '_data_service') else None
        if not ds:
            return 0

        # 查找kline_data和tick表中无数据的instrument_id
        try:
            registered_rows = _result_to_pylist(ds.query(
                "SELECT internal_id, instrument_id FROM futures_instruments UNION ALL SELECT internal_id, instrument_id FROM option_instruments"
            ))
            all_ids = {int(row['internal_id']) for row in registered_rows if row.get('internal_id') is not None}

            kline_ids = {
                int(row['internal_id'])
                for row in _result_to_pylist(ds.query("SELECT DISTINCT internal_id FROM klines_raw"))
                if row.get('internal_id') is not None
            }

            tick_instrument_ids = set()
            if _table_exists(ds, 'ticks_raw'):
                tick_instrument_ids = {
                    str(row['instrument_id'])
                    for row in _result_to_pylist(ds.query("SELECT DISTINCT instrument_id FROM ticks_raw"))
                    if row.get('instrument_id') is not None
                }

            has_data_ids = set(kline_ids)
            for row in registered_rows:
                internal_id = row.get('internal_id')
                instrument_id = row.get('instrument_id')
                if internal_id is None or instrument_id is None:
                    continue
                if str(instrument_id) in tick_instrument_ids:
                    has_data_ids.add(int(internal_id))

            empty_ids = all_ids - has_data_ids
            
            for internal_id in empty_ids:
                # 删除元数据记录
                ds.query("DELETE FROM futures_instruments WHERE internal_id=?", [internal_id])
                ds.query("DELETE FROM option_instruments WHERE internal_id=?", [internal_id])
                # subscriptions 表已废弃，DELETE 操作已移除
                deleted += 1
        except Exception as e:
            logging.error("[StorageMaintenance] drop_empty_instrument_tables failed: %s", e)
        return deleted
    
    def ensure_legacy_placeholder_roots(self) -> None:
        """确保遗留占位符根存在"""
        # ✅ Group A收口：不再使用LEGACYF0000占位符，此函数已废弃
        logging.warning("[QueryService] ensure_legacy_placeholder_roots is deprecated, LEGACY placeholder removed")
        return


# ============================================================================
# 合约辅助工具函数（从 market_data_service.py 迁移）
# ============================================================================

def resolve_subscribe_flag(params: Any, flag_name: str, legacy_flag: str, default: bool) -> bool:
    """
    解析订阅标志
    
    Args:
        params: 参数对象
        flag_name: 新标志名
        legacy_flag: 旧标志名（兼容）
        default: 默认值
    
    Returns:
        bool: 订阅标志值
    """
    try:
        # 优先使用新标志
        value = getattr(params, flag_name, None)
        if value is not None:
            return bool(value)
        # 兼容旧标志
        value = getattr(params, legacy_flag, default)
        return bool(value)
    except Exception:
        return default


def is_symbol_current_or_next(symbol: str, params: Any) -> bool:
    """
    检查合约是否为当前或下月合约
    
    Args:
        symbol: 合约代码（如 IF2603）
        params: 参数对象（包含 month_mapping）
    
    Returns:
        bool: 是否为当前或下月合约
    """
    try:
        month_mapping = getattr(params, 'month_mapping', {})
        if not month_mapping:
            return True
        
        # ✅ 使用 params_service 元数据获取品种，而非正则
        from ali2026v3_trading.params_service import get_params_service
        ps = get_params_service()
        meta = ps.get_instrument_meta_by_id(symbol)
        if not meta:
            return True
        
        product = meta.get('product', '').upper()
        if product in month_mapping:
            specified_months = month_mapping[product]
            if isinstance(specified_months, (list, tuple)) and len(specified_months) >= 2:
                return symbol in [specified_months[0], specified_months[1]]
        return True
    except Exception:
        return True


def is_real_month_contract(instrument_id: str) -> bool:
    """
    判断是否为具体的月合约（全量模式，不过滤）
    
    ⚠️  警告：此方法已被废弃，不再进行任何过滤！
    
    原过滤逻辑:
    1. 排除 888, 999, HOT, IDX 等非具体合约
    2. 只保留年月结尾的合约
    
    Returns:
        bool: 始终返回 True（全量模式）
    """
    # ✅ 全量模式：直接返回 True，不做任何过滤
    return True


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'LightKLine',  # 轻量 K 线数据结构
    '_KlineAggregator',  # K 线聚合器
    'QueryService',
    'StorageMaintenanceService',
    'resolve_subscribe_flag',  # 订阅标志解析
    'is_symbol_current_or_next',  # 当月/下月合约判断
    'is_real_month_contract',  # 真实月份合约判断
]
