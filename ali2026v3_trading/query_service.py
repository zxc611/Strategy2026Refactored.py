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
        self._idx_lock = threading.Lock()

    def _get_params_service(self):
        """通过公共单例获取ParamsService，避免穿透storage._params_service私有属性"""
        try:
            from ali2026v3_trading.params_service import get_params_service
            return get_params_service()
        except Exception:
            return None

    def _get_cached_instruments(self) -> Dict[str, Dict[str, Any]]:
        ps = self._get_params_service()
        return ps.get_all_instrument_cache() if ps else {}

    def _ensure_internal_id_index(self) -> Dict[int, str]:
        """M23-Bug2修复：确保反向索引已构建，O(1)查找（线程安全）"""
        with self._idx_lock:
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
            if self._storage:
                ps = self._get_params_service()
                if ps:
                    info = ps.get_instrument_meta_by_id(normalized_id)
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
            ps = self._get_params_service()
            return ps.get_all_instrument_ids() if ps else []

        registered_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids:
            normalized_id = str(instrument_id).strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)

            # ✅ 统一为params_service缓存查询（唯一权威源）
            ps = self._get_params_service()
            if ps and ps.get_instrument_meta_by_id(normalized_id):
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
        """从合约配置文件加载合约列表并确保合约注册到DB（供on_init调用）。

        设计约束：
        1. 合约列表唯一来源：TXT配置文件，不存在任何回退机制，失败即终止初始化
        2. 品种ID已内置于配置文件，初始化前完整读取+ID匹配+新表创建
        3. 重试3次快速失败：每步骤最多重试3次，3次均失败则抛RuntimeError
        4. 预注册有失败合约则阻断初始化，杜绝带病运行
        5. 完成后通知：发布InstrumentsLoadAndPreregisterCompleted事件

        Args:
            storage: InstrumentDataManager实例（strategy_core_service.storage）
            params: 参数对象
        """
        import time as _time
        MAX_RETRIES = 3

        # ========== 阶段1：从TXT合约配置文件加载合约列表（重试3次） ==========
        selected_futures_list: List[str] = []
        selected_options_dict: Dict[str, List[str]] = {}
        futures_metadata: Dict[str, Dict] = {}
        options_metadata: Dict[str, Dict] = {}
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
                    futures_metadata = file_result.get('futures_metadata', {})
                    options_metadata = file_result.get('options_metadata', {})
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

        # ========== 验证标的期货完整性（禁止补齐回退） ==========
        derived_futures = self._derive_underlying_futures(selected_options_dict)
        if derived_futures:
            existing_futures = set(selected_futures_list)
            missing_futures = sorted(item for item in derived_futures if item not in existing_futures)
            if missing_futures:
                error_detail = (
                    f"合约配置文件验证: 期权标的期货缺失 %d 个，策略初始化终止。\n"
                    f"缺失合约: %s\n"
                    f"当前期货清单(%d个): %s\n"
                    f"解决方案: 在 %s 中添加缺失的标的期货合约后重启策略"
                ) % (
                    len(missing_futures), missing_futures,
                    len(selected_futures_list), selected_futures_list[:20],
                    self._futures_file_path,
                )
                logging.error("[Init-VerifyFutures] ❌ %s", error_detail)
                raise RuntimeError(error_detail)

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
                    error_detail = (
                        f"预注册完成但有 %d 个合约注册失败，策略初始化终止。杜绝带病运行。\n"
                        f"失败合约将被记录在WARNING日志中，请检查合约格式和DB连接。"
                    )
                    logging.error("[PreRegister] %s", error_detail)
                    raise RuntimeError(error_detail)
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

        # ========== 阶段3：硬验证 — 逐合约确认已写入DB并可查询 ==========
        MISSING_RETRY_MAX = 3
        for verify_attempt in range(1, MISSING_RETRY_MAX + 1):
            still_missing = []
            for inst_id in subscribed_instruments:
                try:
                    info = storage._get_instrument_info(str(inst_id).strip())
                    if info is None:
                        still_missing.append(inst_id)
                except Exception:
                    still_missing.append(inst_id)

            if not still_missing:
                logging.info(
                    "[VerifyPreRegister] 第%d次验证通过: %d个合约全部可查询DB",
                    verify_attempt, len(subscribed_instruments),
                )
                break

            logging.warning(
                "[VerifyPreRegister] 第%d次验证发现 %d/%d 个合约缺失，重试注册...",
                verify_attempt, len(still_missing), len(subscribed_instruments),
            )
            for inst_id in still_missing:
                try:
                    storage.register_instrument(
                        instrument_id=str(inst_id).strip(),
                        exchange=self.infer_exchange_from_id(inst_id),
                    )
                except Exception as reg_e:
                    logging.warning("[VerifyPreRegister] 补注册失败 %s: %s", inst_id, reg_e)
        else:
            final_missing = []
            for inst_id in subscribed_instruments:
                try:
                    if storage._get_instrument_info(str(inst_id).strip()) is None:
                        final_missing.append(inst_id)
                except Exception:
                    final_missing.append(inst_id)
            if final_missing:
                error_detail = (
                    f"预注册完成但 %d/%d 个合约仍不可查询DB，策略初始化终止。\n"
                    f"缺失合约(前10): %s\n"
                    f"可能原因: register_instrument静默失败、DB写入后不可读、缓存未刷新"
                ) % (len(final_missing), len(subscribed_instruments), final_missing[:10])
                logging.error("[VerifyPreRegister] ❌ %s", error_detail)
                raise RuntimeError(error_detail)

        result = {
            'futures_list': selected_futures_list,
            'options_dict': selected_options_dict,
            'subscribed_instruments': subscribed_instruments,
            'source': 'output_files',
            'preregister_stats': preregister_stats,
            'futures_metadata': futures_metadata,
            'options_metadata': options_metadata,
        }

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
        """从期权字典推导标的期货列表（从具体期权合约ID解析标的期货）"""
        from ali2026v3_trading.subscription_manager import SubscriptionManager
        underlying_set = set()
        OPTION_TO_FUTURE_MAP = {'MO': 'IM', 'IO': 'IF', 'HO': 'IH'}
        for option_ids in (options_dict or {}).values():
            for opt_id in option_ids:
                try:
                    parsed = SubscriptionManager.parse_option(str(opt_id).strip())
                    opt_product = parsed.get('product', '')
                    year_month = parsed.get('year_month', '')
                    if opt_product and year_month:
                        future_product = OPTION_TO_FUTURE_MAP.get(opt_product, opt_product)
                        future_id = f"{future_product}{year_month}"
                        underlying_set.add(future_id)
                except Exception:
                    continue
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
        ps = self._get_params_service()
        all_ids = ps.get_all_instrument_ids() if ps else []
        for instrument_id in all_ids:
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



from ali2026v3_trading.maintenance_service import StorageMaintenanceService

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
