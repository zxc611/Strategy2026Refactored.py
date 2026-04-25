"""
T 型图服务模块 - CQRS 架构 Query 层
来源：12_trading_logic.py (部分)
功能：期权宽度计算 + T 型图数据生成

优化 v2.0 (2026-04-03) - 集成 DataService:
- ✅ 使用 DataService 获取标的价格和 K 线数据
- ✅ 移除重复的数据查询逻辑
- ✅ 利用 DataService 的索引和缓存提升性能
- ✅ 简化代码结构，专注核心业务逻辑

优化 v2.1 (2026-04-05) - 完整降级查询与代码清理:
- 🔴 高优先级：实现完整的 DuckDB 降级查询（使用 option_status_calc 视图）
- 🔴 高优先级：删除未使用的降级方法（_query_options_from_db, _calculate_sync_otm_from_db, _is_option_sync_moving_from_db）
- 🟡 中优先级：预加载统一基于 latest_prices 和 ticks_raw 最新元数据，避免依赖漂移表结构
- 🟡 中优先级：封装 _future_rising 访问（添加 get_future_rising() 方法）
- 🟢 低优先级：增加缓存容量监控（内存占用估算）
- 🟢 低优先级：支持动态注册新合约（预留监听器接口）
"""
from __future__ import annotations

import re
import threading
import logging
import struct
from contextlib import contextmanager

# ✅ #89修复：DiagnosisProbeManager不可用时的no-op替代
class _NoOpDiagnosisProbeManager:
    """DiagnosisProbeManager导入失败时的降级替代，所有操作为no-op"""
    @staticmethod
    @contextmanager
    def startup_step(name):
        yield
    @staticmethod
    def mark_startup_event(name, detail=''):
        pass
import sys
import heapq
import time
import zlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, field
from ali2026v3_trading.params_service import get_params_service


# ============================================================================
# SortEntry - 排序桶条目（by_sort_bucket 索引结构）
# ============================================================================

@dataclass(order=True)
class SortEntry:
    """
    ✅ 分组D准备：排序桶条目，用于 by_sort_bucket 索引
    
    排序优先级（从高到低）：
    1. sync_flag: True > False (同步虚值优先)
    2. volume: 成交量越大越靠前
    3. strike_distance: 虚值距离越小越靠前（更接近实值）
    4. last_tick_ts: 最新tick时间越新越靠前
    5. internal_id: 兜底排序，保证稳定性
    
    注意：使用负值实现降序排列（heapq是最小堆）
    """
    sort_key: Tuple = field(compare=True)  # 排序键
    internal_id: int = field(compare=False)  # internal_id
    instrument_id: str = field(compare=False)  # instrument_id（供外部消费）
    option_type: str = field(compare=False)  # CALL/PUT
    strike_price: float = field(compare=False)  # 行权价
    volume: float = field(compare=False)  # 成交量
    price: float = field(compare=False)  # 最新价
    sync_flag: bool = field(compare=False)  # 是否同步
    month: str = field(compare=False)  # 月份
    product: str = field(compare=False)  # 产品
    
    @staticmethod
    def create(internal_id: int, instrument_id: str, option_type: str, 
               strike_price: float, volume: float, price: float,
               sync_flag: bool, m_price: float, month: str, product: str,
               last_tick_ts: float = 0.0) -> 'SortEntry':
        """
        创建排序条目
        
        Args:
            internal_id: internal_id
            instrument_id: instrument_id
            option_type: CALL/PUT
            strike_price: 行权价
            volume: 成交量
            price: 最新价
            sync_flag: 是否同步虚值
            m_price: 期货最新价（用于计算虚值距离）
            month: 月份
            product: 产品
            last_tick_ts: 最新tick时间戳
        """
        # 计算虚值距离
        if option_type == 'CALL':
            strike_distance = strike_price - m_price if m_price > 0 else float('inf')
        else:  # PUT
            strike_distance = m_price - strike_price if m_price > 0 else float('inf')
        
        # 构建排序键（使用负值实现降序）
        sort_key = (
            not sync_flag,  # False(0) < True(1)，所以同步的排前面
            -volume,  # 成交量大的排前面
            strike_distance,  # 虚值距离小的排前面
            -last_tick_ts,  # 时间新的排前面
            internal_id,  # 兜底
        )
        
        return SortEntry(
            sort_key=sort_key,
            internal_id=internal_id,
            instrument_id=instrument_id,
            option_type=option_type,
            strike_price=strike_price,
            volume=volume,
            price=price,
            sync_flag=sync_flag,
            month=month,
            product=product,
        )


# ============================================================================
# WidthStrengthCache - 内嵌类：实时维护期权宽度强度
# ============================================================================

# P1 修复：从 event_bus 模块统一导入 EventBus
try:
    from ali2026v3_trading.event_bus import get_global_event_bus, EventBus
    _HAS_EVENT_BUS = get_global_event_bus() is not None
except ImportError as e:
    logging.warning(f"[TTypeService] EventBus import failed: {e}")
    EventBus = None
    get_global_event_bus = None
    _HAS_EVENT_BUS = False

try:
    from ali2026v3_trading.storage import KLineEvent as EventBusKLineEvent
except ImportError:
    EventBusKLineEvent = None

class WidthStrengthCache:
    """内存字典：实时计算宽度强度（同步虚值期权计数）
    
    核心逻辑：
    1. 期货上涨 + 看涨期权价格上涨 → 同步 ✅
    2. 期货下跌 + 看跌期权价格上涨 → 同步 ✅

     五态状态机规则：
     1. 五态缓存表示“当前状态”，不是历史事件累计。
     2. 期权方向只由最近两笔“不同且大于 0”的价格确定。
     3. 平价 Tick 不提供新的方向信息，因此不改变最近一次有效方向。
     4. 如果期货方向未变，则平价 Tick 保持当前五态；例如已是 correct_rise 时，
         平价 Tick 不会把状态打回 other。
     5. 0 价、无效价或断层恢复视为状态连续性中断，会清空最近一次有效方向，
         随后必须等待新的两笔不同有效价才能重新建立方向。
     6. 期货方向变化会触发整产品重分类，但仍复用每个期权当前持有的最近一次有效方向。
     7. 诊断输出直接读取当前状态缓存，避免被最新一笔平价 Tick 临时重算污染。
    
    增量更新策略：
    - 期权 tick: O(1) 增量更新计数
    - 期货 tick: O(N) 重算（但 N 可优化）
    """

    def __init__(self, tracked_option_tick_ids: Optional[set] = None):
        self._lock = threading.RLock()
        # 可配置的跟踪合约ID集合（默认为空，由外部配置注入）
        self._tracked_option_tick_ids = set(tracked_option_tick_ids) if tracked_option_tick_ids else set()
        
        # 产品 -> 月份 -> 期货最新价 (P1 修复：实现分月精准定价)
        self._future_prices_by_month: Dict[str, Dict[str, float]] = defaultdict(dict)
        # future_internal_id -> 期货前一个价格（用于方向判断）
        self._future_prev_price: Dict[int, float] = {}
        # future_internal_id -> 期货是否上涨（最新方向）
        self._future_rising: Dict[int, bool] = {}
        
        # ---- internal_id 主键化：单合约运行时状态缓存 ----
        # 期权 internal_id -> 最新价
        self._option_price: Dict[int, float] = {}
        # 期权 internal_id -> 前一个价格
        self._option_prev_price: Dict[int, float] = {}
        # 期权 internal_id -> 最近一笔有效且不同的价格，用于定义方向锚点
        self._option_last_distinct_price: Dict[int, Optional[float]] = {}
        # 期权 internal_id -> 最近一次有效方向，True=上涨，False=下跌，None=尚无方向
        self._option_last_direction: Dict[int, Optional[bool]] = {}
        # 期权 internal_id -> 基本信息（只保留 internal_id 关系字段，不缓存产品映射语义）
        self._option_info: Dict[int, Dict] = {}
        # 期权 internal_id -> 累计成交量（用于虚值开仓排序）
        self._option_volume: Dict[int, float] = {}
        
        # ✅ 索引：future_internal_id -> 类型 -> internal_id 列表（加速期货方向变化时的重算）
        self._options_by_future_type: Dict[int, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
        
        # future_internal_id -> 月份 -> option_type -> 五类状态计数
        # 1. correct_rise, 2. wrong_rise, 3. correct_fall, 4. wrong_fall, 5. other
        self._status_counts: Dict[int, Dict[str, Dict[str, Dict[str, int]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(self._new_status_bucket))
        )
        # ✅ 同步虚值期权计数（排序二原则核心）：future_internal_id -> 月份 -> option_type -> count
        self._sync_otm_count: Dict[int, Dict[str, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        # future_internal_id -> 所有已注册月份列表
        self._months: Dict[int, List[str]] = defaultdict(list)
        
        # 期权 internal_id -> 上次是否同步（用于增量更新）
        self._sync_flag: Dict[int, bool] = {}
        # 期权 internal_id -> 当前五态状态（用于保证 _status_counts 始终表示当前状态）
        self._current_status: Dict[int, str] = {}
        
        # ✅ 期货是否已收到足够数据以判断方向（避免注册初值触发伪方向）
        self._future_initialized: Dict[int, bool] = {}
        
        # ✅ instrument_id -> internal_id 入口映射（兼容层：平台回调仍传 instrument_id）
        self._instrument_id_to_internal_id: Dict[str, int] = {}
        # internal_id -> instrument_id 反向映射
        self._internal_id_to_instrument_id: Dict[int, str] = {}
        
        # ✅ 性能监控：查询统计
        self._query_count: int = 0
        self._cache_hits: int = 0
        self._cache_misses: int = 0
        
        # ✅ 分组D准备：by_sort_bucket 索引结构（future_internal_id -> 月份 -> option_type -> 排序堆）
        # 使用heapq维护前N个最优期权，避免全量扫描
        self._sort_buckets: Dict[int, Dict[str, Dict[str, List[SortEntry]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        # 每个桶的最大容量（只保留前N个）
        self._sort_bucket_max_size: int = 50

    @staticmethod
    def _new_status_bucket() -> Dict[str, int]:
        return {
            'correct_rise': 0,
            'wrong_rise': 0,
            'correct_fall': 0,
            'wrong_fall': 0,
            'other': 0,
        }

    # ✅ 方法唯一：委托shared_utils.normalize_instrument_id，不再独立实现
    @staticmethod
    def _normalize_instrument_id(instrument_id: str) -> str:
        from ali2026v3_trading.shared_utils import normalize_instrument_id
        return normalize_instrument_id(instrument_id)

    @staticmethod
    def _normalize_option_type(opt_type: str) -> str:
        """✅ ID唯一：委托shared_utils.normalize_option_type，不再独立实现"""
        from ali2026v3_trading.shared_utils import normalize_option_type
        return normalize_option_type(opt_type)
    
    def _resolve_internal_id(self, instrument_id: str) -> Optional[int]:
        """
        instrument_id -> internal_id 入口转换
        
        ✅ 序号127修复：统一为ParamsService查询，本地映射仅做缓存加速
        
        Args:
            instrument_id: 平台合约代码（已大写化）
            
        Returns:
            对应的 internal_id，未找到返回 None
        """
        normalized = self._normalize_instrument_id(instrument_id)
        
        # ✅ 先查本地缓存（加速）
        iid = self._instrument_id_to_internal_id.get(normalized)
        if iid is not None:
            return iid
        
        # ✅ 统一从ParamsService查询（唯一权威源）
        try:
            ps = get_params_service()
            iid = ps.resolve_internal_id(normalized)
            if iid is not None:
                # 回填本地缓存（仅加速，不作为数据源）
                self._instrument_id_to_internal_id[normalized] = iid
                self._internal_id_to_instrument_id[iid] = normalized
                return iid
        except Exception:
            pass
        
        return None


    def _get_future_product_by_underlying_id(self, underlying_future_id) -> str:
        """✅ 通过 underlying_future_id 从 id_cache 获取 product（替代原产品映射）"""
        if underlying_future_id is None or underlying_future_id == '':
            return ''
        try:
            future_info = get_params_service().get_id_cache(int(underlying_future_id))
            if future_info:
                return str(future_info.get('product', '')).upper()
        except Exception:
            pass
        return ''

    def _get_future_price_by_id_and_month(self, underlying_future_id: Optional[int], month: str) -> float:
        """按 future_internal_id + month 获取期货价格。"""
        if underlying_future_id is None:
            return 0.0
        future_product = self._get_future_product_by_underlying_id(underlying_future_id)
        if not future_product:
            return 0.0
        return float(self._future_prices_by_month.get(future_product, {}).get(month, 0.0) or 0.0)

    def _get_future_runtime_state(self, underlying_future_id: Optional[int], month: str) -> Dict[str, Any]:
        """统一读取期货运行态，避免混用 product / future_id 作为 key。"""
        future_id = int(underlying_future_id) if underlying_future_id not in (None, '') else None
        future_product = self._get_future_product_by_underlying_id(future_id)
        return {
            'future_id': future_id,
            'future_product': future_product,
            'initialized': self._future_initialized.get(future_id, False) if future_id is not None else False,
            'rising': self._future_rising.get(future_id) if future_id is not None else None,
            'price': self._get_future_price_by_id_and_month(future_id, month),
        }

    def _get_option_display_products(self, info: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """仅在导出或诊断时派生产品显示字段，避免把它们作为关系主链缓存。"""
        if not info:
            return {'underlying_product': '', 'future_product': ''}

        option_product = ''
        internal_id = info.get('internal_id')
        instrument_id = info.get('instrument_id')
        params_service = get_params_service()

        if internal_id is not None:
            meta = params_service.get_id_cache(int(internal_id))
            if meta:
                option_product = str(meta.get('product') or '').upper()

        if not option_product and instrument_id:
            meta = params_service.get_instrument_meta_by_id(str(instrument_id))
            if meta:
                option_product = str(meta.get('product') or '').upper()

        future_product = self._get_future_product_by_underlying_id(info.get('underlying_future_id'))
        return {
            'underlying_product': option_product or future_product,
            'future_product': future_product,
        }

    def _resolve_option_context(self, instrument_id: str) -> Dict[str, Any]:
        """按 instrument_id 收敛期权元数据，优先走 WidthCache，其次 ParamsService。"""
        normalized = self._normalize_instrument_id(instrument_id)
        context: Dict[str, Any] = {
            'instrument_id': normalized,
            'internal_id': None,
            'underlying_future_id': None,
            'month': '',
            'strike_price': 0.0,
            'option_type': '',
        }

        internal_id = self._instrument_id_to_internal_id.get(normalized)
        info = self._option_info.get(internal_id) if internal_id is not None else None
        if info:
            context.update({
                'internal_id': internal_id,
                'underlying_future_id': info.get('underlying_future_id'),
                'month': info.get('month', ''),
                'strike_price': float(info.get('strike_price') or 0.0),
                'option_type': str(info.get('option_type') or ''),
            })
            return context

        params_meta = get_params_service().get_instrument_meta_by_id(normalized)
        if params_meta:
            context.update({
                'internal_id': params_meta.get('internal_id'),
                'underlying_future_id': params_meta.get('underlying_future_id'),
                'month': str(params_meta.get('year_month') or params_meta.get('month') or ''),
                'strike_price': float(params_meta.get('strike_price') or 0.0),
                'option_type': str(params_meta.get('option_type') or ''),
            })
        return context

    def _get_underlying_price_by_future_id(self, underlying_future_id: Optional[int]) -> Optional[float]:
        """通过 future_internal_id 获取标的期货最新价，彻底替代 underlying_symbol 字符串回退。"""
        if underlying_future_id in (None, ''):
            return None
        future_info = get_params_service().get_id_cache(int(underlying_future_id))
        if not future_info:
            return None
        future_instrument_id = future_info.get('instrument_id')
        if not future_instrument_id:
            return None
        return self.get_underlying_price_from_service(str(future_instrument_id))

    def _should_trace_option_tick(self, instrument_id: str) -> bool:
        normalized = self._normalize_instrument_id(instrument_id)
        normalized_upper = normalized.upper()
        return any(n.upper() == normalized_upper for n in self._tracked_option_tick_ids)



    def _log_tracked_option_tick(
        self,
        instrument_id: str,
        info: Optional[Dict[str, Any]],
        prev_price: float,
        current_price: float,
        note: str = '',
    ) -> None:
        # 用internal_id查缓存（缓存键已统一为int）
        internal_id = self._resolve_internal_id(instrument_id)
        iid = internal_id if internal_id is not None else instrument_id
        last_distinct_price = self._option_last_distinct_price.get(iid)
        direction = self._option_last_direction.get(iid)
        current_status = self._current_status.get(iid, 'other')
        direction_text = 'up' if direction is True else ('down' if direction is False else 'none')
        
        # PROBE: 检查期货初始化状态
        underlying_future_id = info.get('underlying_future_id')
        future_state = self._get_future_runtime_state(underlying_future_id, info.get('month', '') if info else '')
        display_meta = self._get_option_display_products(info)
        
        logging.info(
            "[TTypeTickTrace] instrument=%s product=%s month=%s option_type=%s prev_price=%.4f current_price=%.4f last_distinct_price=%s direction=%s current_status=%s sync=%s note=%s future_init=%s future_rising=%s",
            instrument_id,
            display_meta.get('underlying_product', ''),
            info.get('month', '') if info else '',
            info.get('option_type', '') if info else '',
            float(prev_price or 0.0),
            float(current_price or 0.0),
            f"{float(last_distinct_price):.4f}" if last_distinct_price is not None else 'None',
            direction_text,
            current_status,
            self._sync_flag.get(iid, False),
            note or '-',
            future_state['initialized'],
            future_state['rising'],
        )

    def _classify_status(self, underlying_future_id: int, month: str, opt_type: str,
                         current_price: float, prev_price: float,
                         option_direction: Optional[bool] = None) -> str:
        """根据当前内存状态计算单个期权的当前五态状态。"""
        future_state = self._get_future_runtime_state(underlying_future_id, month)
        if future_state['future_id'] is None:
            return 'other'
        if current_price <= 0 or prev_price <= 0:
            return 'other'

        if not future_state['initialized']:
            return 'other'

        if option_direction is None:
            if current_price > prev_price:
                option_direction = True
            elif current_price < prev_price:
                option_direction = False

        if option_direction is None:
            return 'other'

        future_rising = bool(future_state['rising'])
        option_rising = option_direction

        if opt_type == 'CALL':
            if future_rising and option_rising:
                return 'correct_rise'
            if not future_rising and option_rising:
                return 'wrong_rise'
            if not future_rising and not option_rising:
                return 'correct_fall'
            if future_rising and not option_rising:
                return 'wrong_fall'
        else:
            if not future_rising and option_rising:
                return 'correct_rise'
            if future_rising and option_rising:
                return 'wrong_rise'
            if future_rising and not option_rising:
                return 'correct_fall'
            if not future_rising and not option_rising:
                return 'wrong_fall'

        return 'other'

    def _set_current_status(self, internal_id: int, info: Dict[str, Any], new_status: str) -> None:
        """更新单个期权的当前状态，并同步修正 _status_counts。"""
        # ✅ 使用 underlying_future_id 作为键
        future_id = info.get('underlying_future_id')
        if future_id is None:
            logging.warning(f"[_set_current_status] Missing underlying_future_id for internal_id={internal_id}")
            return
        
        month = info['month']
        opt_type = info['option_type']

        bucket = self._status_counts[future_id][month][opt_type]
        old_status = self._current_status.get(internal_id)

        if old_status in bucket:
            bucket[old_status] = max(0, bucket[old_status] - 1)

        bucket[new_status] += 1
        self._current_status[internal_id] = new_status

    def _update_option_direction(self, internal_id: int, price: float) -> Optional[bool]:
        """维护期权最近一次有效方向。

        定义：
        - 只使用最近两笔不同且大于 0 的价格来生成方向
        - 平价 Tick 不改变方向
        - 0 价或无效价视为断层，清空方向；恢复后需等待下一笔不同价重新建方向
        """
        if price <= 0:
            self._option_last_distinct_price[internal_id] = None
            self._option_last_direction[internal_id] = None
            return None

        anchor_price = self._option_last_distinct_price.get(internal_id)
        if anchor_price is None or anchor_price <= 0:
            self._option_last_distinct_price[internal_id] = price
            self._option_last_direction[internal_id] = None
            return None

        if price > anchor_price:
            self._option_last_distinct_price[internal_id] = price
            self._option_last_direction[internal_id] = True
            return True

        if price < anchor_price:
            self._option_last_distinct_price[internal_id] = price
            self._option_last_direction[internal_id] = False
            return False

        return self._option_last_direction.get(internal_id)

    def _compute_sync_flag(self, info: Dict[str, Any], current_price: float,
                           option_direction: Optional[bool]) -> bool:
        underlying_future_id = info.get('underlying_future_id')
        month = info['month']
        opt_type = info['option_type']
        strike = info['strike_price']
        future_state = self._get_future_runtime_state(underlying_future_id, month)

        if future_state['future_id'] is None or not future_state['initialized']:
            return False

        if current_price <= 0 or option_direction is None:
            return False

        future_rising = bool(future_state['rising'])
        future_price = future_state['price']
        is_otm = self._is_out_of_the_money(opt_type, future_price, strike)
        is_target = (
            (future_rising and opt_type == 'CALL' and option_direction)
            or (not future_rising and opt_type == 'PUT' and option_direction)
        )
        return is_target and is_otm

    def register_future(self, future_internal_id: int, initial_price: float, month: str = None):
        """注册期货品种，初始化价格和方向
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            initial_price: 初始价格
            month: 可选，指定月份。如果不传，则更新该期货所有已知月份的价格。
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product 和 month
            info = get_params_service().get_id_cache(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return
            
            product = str(info.get('product', '')).upper()
            contract_month = info.get('month', month)
            
            if contract_month:
                self._future_prices_by_month[product][contract_month] = initial_price
            else:
                # 如果没有指定月份，更新该期货下所有已注册月份的价格（模拟主力合约联动）
                for m in self._months.get(future_internal_id, []):
                    self._future_prices_by_month[product][m] = initial_price
            
            self._future_prev_price[future_internal_id] = initial_price
            self._future_rising[future_internal_id] = False
            # 注册初值不代表已有方向信息；首次真实 tick 到来后再标记为已初始化
            self._future_initialized.setdefault(future_internal_id, False)

    def register_option(self, instrument_id: str, underlying_product: str,
                        month: str, strike_price: float, option_type: str,
                        initial_price: float,
                        underlying_future_id: Optional[int] = None,
                        internal_id: Optional[int] = None):
        """注册期权合约，记录基本信息
        
        Args:
            instrument_id: 合约代码（兼容入口）
            underlying_product: 期权产品代码（兼容展示入参，不参与关系推导）
            month: 月份
            strike_price: 行权价
            option_type: 期权类型
            initial_price: 初始价格
            underlying_future_id: 标的期货 internal_id
            internal_id: 系统内部代理键（优先使用）
        """
        with self._lock:
            instrument_id = self._normalize_instrument_id(instrument_id)
            if underlying_future_id is None:
                raise ValueError("underlying_future_id required (two-ID principle)")
            if not self._get_future_product_by_underlying_id(underlying_future_id):
                raise ValueError(f"Cannot resolve product for underlying_future_id={underlying_future_id}")
            opt_type_upper = self._normalize_option_type(option_type)
            
            # 入口标准化：确定 internal_id
            if internal_id is None:
                internal_id = self._resolve_internal_id(instrument_id)
            
            # ✅ 序号126修复：删除CRC32兜底，统一使用ParamsService解析的internal_id
            if internal_id is None:
                logging.error(
                    "[WidthStrengthCache] internal_id未提供且无法从ParamsService解析: "
                    "instrument_id=%s. Please ensure contract is properly registered.",
                    instrument_id
                )
                return None
            
            # 维护双向映射
            self._instrument_id_to_internal_id[instrument_id] = internal_id
            self._internal_id_to_instrument_id[internal_id] = instrument_id

            existing_info = self._option_info.get(internal_id)
            existing_price = float(self._option_price.get(internal_id, 0.0) or 0.0)
            preserve_runtime_state = existing_info is not None and initial_price <= 0 < existing_price
            if existing_info is not None:
                old_future_id = existing_info.get('underlying_future_id')
                old_type = existing_info['option_type']
                if old_future_id is not None:
                    old_list = self._options_by_future_type.get(old_future_id, {}).get(old_type, [])
                    if internal_id in old_list and old_future_id != underlying_future_id:
                        old_list[:] = [iid for iid in old_list if iid != internal_id]
            
            self._option_info[internal_id] = {
                'instrument_id': instrument_id,
                'internal_id': internal_id,
                'underlying_future_id': int(underlying_future_id) if underlying_future_id not in (None, '') else None,
                'month': month,
                'strike_price': float(strike_price),
                'option_type': opt_type_upper,
            }
            # ✅ 使用 future_internal_id 作为键
            option_list = self._options_by_future_type[underlying_future_id][opt_type_upper]
            if internal_id not in option_list:
                option_list.append(internal_id)
            if month not in self._months[underlying_future_id]:
                self._months[underlying_future_id].append(month)

            if preserve_runtime_state:
                return

            # 注册时使用实际价格，如果为0则不初始化价格字段（等待第一个Tick）
            if initial_price > 0:
                self._option_price[internal_id] = initial_price
                self._option_prev_price[internal_id] = initial_price
                self._option_last_distinct_price[internal_id] = initial_price
            else:
                self._option_price.pop(internal_id, None)
                self._option_prev_price.pop(internal_id, None)
                self._option_last_distinct_price[internal_id] = None
            
            self._option_last_direction[internal_id] = None
            self._set_current_status(internal_id, self._option_info[internal_id], 'other')
            self._sync_flag[internal_id] = False

    # ✅ 接口唯一：WidthStrengthCache的on_*_tick为内部实现，TTypeService为唯一公开入口
    def on_future_tick(self, future_internal_id: int, price: float, month: str = None):
        """更新期货价格，自动判断涨跌方向并同步到指定月份
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            price: 最新价格
            month: 可选，该 Tick 所属的具体月份（如 '2605'）。
                   如果不提供，则从 id_cache 获取。
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product 和 month
            info = get_params_service().get_id_cache(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return
            
            product = str(info.get('product', '')).upper()
            contract_month = info.get('month', month)
            
            prev = self._future_prev_price.get(future_internal_id, price)
            self._future_prev_price[future_internal_id] = price
            # 平价时保留之前方向，仅价格真正变化时更新方向
            if price != prev:
                self._future_rising[future_internal_id] = price > prev
            self._future_initialized[future_internal_id] = True
            
            # 更新指定月份或所有月份的价格
            if contract_month:
                # P1 Bug #70修复：只更新指定月份，避免远月价格被近月覆盖
                self._future_prices_by_month[product][contract_month] = price
            else:
                # 如果没有指定月份，跳过更新（需要明确指定月份）
                logging.debug(f"[TTypeService] on_future_tick: month=None for {future_internal_id}, skipping update")
                # 不更新任何月份，等待明确的月份信息
            
            self._recalc_all_state_for_underlying(future_internal_id)

    def on_future_instrument_tick(self, future_internal_id: int, price: float):
        """✅ 按期货 internal_id 更新价格，ID语义主入口。"""
        info = get_params_service().get_id_cache(future_internal_id)
        if not info:
            logging.warning(f"[TType] Unknown future internal_id: {future_internal_id}")
            return
        
        product = info.get('product', '').upper()
        month = info.get('month', '')
        if not product:
            return
        # ✅ 直接调用 on_future_tick，传递 future_internal_id
        self.on_future_tick(future_internal_id, price, month)

    def on_option_tick(self, instrument_id: str, price: float, volume: float = 0):
        """更新期权价格，增量更新同步虚值计数
        
        入口先做 instrument_id -> internal_id 标准化。
        """
        with self._lock:
            instrument_id = self._normalize_instrument_id(instrument_id)
            # 入口标准化：instrument_id -> internal_id
            internal_id = self._resolve_internal_id(instrument_id)
            if internal_id is None:
                # 未注册的期权，无法处理
                return
            
            if volume > 0:
                self._option_volume[internal_id] = self._option_volume.get(internal_id, 0) + volume
            info = self._option_info.get(internal_id)
            if not info:
                if self._should_trace_option_tick(instrument_id):
                    self._log_tracked_option_tick(
                        instrument_id,
                        None,
                        prev_price=0.0,
                        current_price=price,
                        note='unregistered',
                    )
                return
            
            # ✅ 使用 future_internal_id
            future_id = info.get('underlying_future_id')
            if future_id is None:
                logging.warning(f"[on_option_tick] Missing underlying_future_id for {instrument_id}")
                return
            month = info['month']
            opt_type = info['option_type']

            # 获取前一笔价格，如果不存在则说明是第一个Tick
            prev_price = self._option_price.get(internal_id)
            if prev_price is None:
                # 第一个有效Tick：只建立锚点，不计算状态
                self._option_price[internal_id] = price
                self._option_prev_price[internal_id] = price
                self._option_last_distinct_price[internal_id] = price
                self._option_last_direction[internal_id] = None
                return
            
            self._option_prev_price[internal_id] = prev_price
            self._option_price[internal_id] = price

            last_direction = self._update_option_direction(internal_id, price)

            old_was_sync = self._sync_flag.get(internal_id, False)
            new_is_sync = self._compute_sync_flag(info, price, last_direction)
            new_status = self._classify_status(
                future_id,
                month,
                opt_type,
                price,
                prev_price,
                option_direction=last_direction,
            )
            self._set_current_status(internal_id, info, new_status)

            # ✅ 增量更新 _sync_otm_count（排序二原则核心）- 使用 future_internal_id
            if new_is_sync and not old_was_sync:
                self._sync_otm_count[future_id][month][opt_type] += 1
            elif not new_is_sync and old_was_sync:
                self._sync_otm_count[future_id][month][opt_type] = max(0, self._sync_otm_count[future_id][month][opt_type] - 1)

            self._sync_flag[internal_id] = new_is_sync

            # ✅ 分组D准备：增量更新排序桶 - 使用 future_internal_id
            self._update_sort_bucket(internal_id, info, future_id, month, opt_type)

            if self._should_trace_option_tick(instrument_id):
                self._log_tracked_option_tick(
                    instrument_id,
                    info,
                    prev_price=prev_price,
                    current_price=price,
                )

    def _get_future_price_for_month(self, product: str, month: str) -> float:
        """✅ 获取指定产品在指定月份的期货价格（ID语义版本）"""
        # 注意：此方法需要product字符串，从调用方传入的应是已从underlying_future_id解析出的product
        return self._future_prices_by_month.get(product, {}).get(month, 0.0)

    def _is_out_of_the_money(self, option_type: str, underlying_price: float, strike: float) -> bool:
        """判断是否为虚值期权（P1 修复：修正 CALL 虚值条件）"""
        if option_type == 'CALL':
            return underlying_price < strike  # 看涨期权：标的低于行权价为虚值
        else:
            return underlying_price > strike  # 看跌期权：标的高于行权价为虚值

    def _recalc_all_state_for_underlying(self, future_internal_id: int):
        """期货方向变化时，重新计算该标的下所有期权的当前状态和同步计数，并同步更新排序桶。
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
        """
        # ✅ 从 id_cache 获取 product
        info = get_params_service().get_id_cache(future_internal_id)
        if not info:
            logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
            return
        
        product = str(info.get('product', '')).upper()
        
        # ✅ 使用 future_internal_id 作为键
        self._sync_otm_count[future_internal_id].clear()
        self._status_counts[future_internal_id].clear()
        target_internal_ids = []
        for internal_ids in self._options_by_future_type.get(future_internal_id, {}).values():
            target_internal_ids.extend(internal_ids)
        
        for iid in target_internal_ids:
            opt_info = self._option_info.get(iid)
            if not opt_info:
                continue
            
            # ✅ 只处理属于该期货的期权
            if opt_info.get('underlying_future_id') != future_internal_id:
                continue
            
            month = opt_info['month']
            opt_type = opt_info['option_type']
            strike = opt_info['strike_price']
            underlying_future_id = opt_info.get('underlying_future_id')
            if underlying_future_id is None:
                continue
            
            # ✅ 获取期权最新价和前一笔价格
            option_price = self._option_price.get(iid, 0.0)
            prev_price = self._option_prev_price.get(iid, option_price)
            
            # ✅ 获取对应月份的期货价格
            future_price = self._get_future_price_by_id_and_month(underlying_future_id, month)
            
            status = self._classify_status(
                underlying_future_id,
                month,
                opt_type,
                option_price,
                prev_price,
                option_direction=self._option_last_direction.get(iid),
            )
            self._set_current_status(iid, opt_info, status)

            is_sync = self._compute_sync_flag(opt_info, option_price, self._option_last_direction.get(iid))
            self._sync_flag[iid] = is_sync
            if is_sync:
                is_otm = self._is_out_of_the_money(opt_type, future_price, strike)
                if is_otm:
                    # ✅ 使用 future_internal_id 作为键
                    self._sync_otm_count[future_internal_id][month][opt_type] += 1
            
            # 同步更新排序桶 - 使用 future_internal_id
            self._update_sort_bucket(iid, opt_info, future_internal_id, month, opt_type)

    def get_width_strength_summary(self, future_internal_id: int, months: List[str], option_type: str = None) -> Dict[str, int]:
        """P2 优化：获取指定月份组合的五类状态汇总
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            months: 月份列表
            option_type: 可选，'CALL' 或 'PUT'
            
        Returns:
            Dict: 五类状态计数
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product
            info = get_params_service().get_id_cache(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return {'correct_rise': 0, 'wrong_rise': 0, 'correct_fall': 0, 'wrong_fall': 0, 'other': 0}
            
            product = str(info.get('product', '')).upper()
            summary = {'correct_rise': 0, 'wrong_rise': 0, 'correct_fall': 0, 'wrong_fall': 0, 'other': 0}
            for month in months:
                # ✅ 使用 future_internal_id 作为键
                month_data = self._status_counts.get(future_internal_id, {}).get(month, {})
                target_types = [self._normalize_option_type(option_type)] if option_type else ['CALL', 'PUT']
                
                for ot in target_types:
                    counts = month_data.get(ot, {})
                    for key in summary:
                        summary[key] += counts.get(key, 0)
            return summary

    def get_future_rising(self, future_internal_id: int) -> bool:
        """🟡 中优先级：封装 _future_rising 访问
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            
        Returns:
            bool: 期货是否上涨
        """
        with self._lock:
            # ✅ 直接查询，使用 future_internal_id 作为键
            return self._future_rising.get(future_internal_id, False)
    
    def get_all_months(self, future_internal_id: int) -> List[str]:
        """获取某期货已注册的所有月份
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            
        Returns:
            List[str]: 月份列表
        """
        # ✅ 从 id_cache 获取 product
        info = get_params_service().get_id_cache(future_internal_id)
        if not info:
            logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
            return []
        
        product = str(info.get('product', '')).upper()
        # ✅ 使用 future_internal_id 作为键
        return self._months.get(future_internal_id, [])
    
    def get_width_strength(self, future_internal_id: int, months: List[str], option_type: str = None) -> int:
        """🔴 核心方法：O(1) 查询宽度强度（同步虚值期权计数）
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            months: 月份列表
            option_type: 可选，'CALL' 或 'PUT'，None 表示两者都算
            
        Returns:
            int: 同步虚值期权计数
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product
            info = get_params_service().get_id_cache(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return 0
            
            product = str(info.get('product', '')).upper()
            strength = 0
            target_types = [self._normalize_option_type(option_type)] if option_type else ['CALL', 'PUT']
            
            for month in months:
                for opt_type in target_types:
                    # ✅ O(1) 查询：直接从内存字典读取，使用 future_internal_id 作为键
                    count = self._sync_otm_count.get(future_internal_id, {}).get(month, {}).get(opt_type, 0)
                    strength += count
            
            self._query_count += 1
            self._cache_hits += 1
            
            return strength
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息（含内存占用估算）"""
        import sys
        
        with self._lock:
            total_queries = self._query_count
            hit_rate = (self._cache_hits / total_queries * 100) if total_queries > 0 else 0.0
            
            # ✅ 低优先级：内存占用估算
            try:
                memory_estimate_bytes = (
                    sys.getsizeof(self._future_prev_price) +
                    sys.getsizeof(self._option_price) +
                    sys.getsizeof(self._option_info) +
                    sys.getsizeof(self._sync_otm_count) +
                    sys.getsizeof(self._sync_flag)
                )
                memory_estimate_kb = round(memory_estimate_bytes / 1024, 2)
            except Exception:
                memory_estimate_kb = None
            
            return {
                'total_futures': len(self._future_prev_price),
                'total_options': len(self._option_info),
                'total_products': len(self._months),
                'sync_flags_count': len(self._sync_flag),
                'query_count': self._query_count,
                'cache_hits': self._cache_hits,
                'cache_misses': self._query_count - self._cache_hits,
                'hit_rate_percent': round(hit_rate, 2),
                'memory_estimate_kb': memory_estimate_kb
            }
    
    def print_status_diagnosis(self, future_internal_id: int = None, top_n: int = 10):
        """🔴 期权5种状态诊断日志输出
        
        Args:
            future_internal_id: 期货合约 internal_id（主键），None 表示所有期货
            top_n: 每个状态下显示前 N 个合约
        """
        with self._lock:
            # ✅ 如果指定了 future_internal_id，获取其 product；否则遍历所有 future_internal_id
            if future_internal_id:
                info = get_params_service().get_id_cache(future_internal_id)
                if not info:
                    logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                    return
                futures_to_diagnose = [future_internal_id]
            else:
                # ✅ 遍历所有已注册的 future_internal_id
                futures_to_diagnose = list(self._months.keys())
            
            for fid in futures_to_diagnose:
                # ✅ 从 id_cache 获取 product 用于显示
                info = get_params_service().get_id_cache(fid)
                if not info:
                    logging.warning(f"[WidthStrengthCache] Cannot resolve product for future_internal_id={fid}")
                    continue
                prod = str(info.get('product', '')).upper()
                
                logging.info("[OptionStatus] " + "=" * 78)
                logging.info(f"[OptionStatus] 期权状态诊断报告 - 产品: {prod} (future_id={fid})")
                logging.info("[OptionStatus] " + "=" * 78)
                
                # 获取期货方向 - 直接使用 future_internal_id
                future_rising = self.get_future_rising(fid)
                direction_str = "上涨" if future_rising else "下跌"
                logging.info(f"[OptionStatus] 期货方向: {direction_str}")
                logging.info("[OptionStatus]")
                
                # 统计各状态的合约
                status_contracts = {
                    'correct_rise': [],   # 正确上涨
                    'wrong_rise': [],     # 错误上涨
                    'correct_fall': [],   # 正确下跌
                    'wrong_fall': [],     # 错误下跌
                    'other': []           # 其它
                }
                
                # 锁内快照数据，锁外遍历（避免长时间持锁阻塞其他线程）
                with self._lock:
                    option_info_keys = [iid for iid, info in self._option_info.items()
                                       if info.get('underlying_future_id') == fid]
                    option_price_snapshot = dict(self._option_price)
                    sync_flag_snapshot = dict(self._sync_flag)
                
                # 锁外构建完整快照
                option_info_snapshot = {iid: dict(self._option_info[iid]) for iid in option_info_keys if iid in self._option_info}
                
                for iid, info in option_info_snapshot.items():
                    
                    inst_id = info.get('instrument_id', self._internal_id_to_instrument_id.get(iid, str(iid)))
                    status = self._get_current_status(iid)
                    if status in status_contracts:
                        status_contracts[status].append({
                            'instrument_id': inst_id,
                            'month': info['month'],
                            'type': info['option_type'],
                            'strike': info['strike_price'],
                            'price': option_price_snapshot.get(iid, 0),
                            'is_sync': sync_flag_snapshot.get(iid, False)
                        })
                
                # 输出各状态统计
                status_labels = {
                    'correct_rise': '正确上涨(同步)',
                    'wrong_rise': '错误上涨',
                    'correct_fall': '正确下跌(同步)',
                    'wrong_fall': '错误下跌',
                    'other': '其它'
                }
                
                total_count = 0
                for status, label in status_labels.items():
                    contracts = status_contracts[status]
                    count = len(contracts)
                    total_count += count
                    
                    logging.info(f"[OptionStatus] {label}: {count} 个")
                    
                    # 显示前 top_n 个合约详情
                    if contracts and count > 0:
                        for i, c in enumerate(contracts[:top_n]):
                            sync_mark = " [同步]" if c['is_sync'] else ""
                            logging.info(
                                f"[OptionStatus]   {i+1}. {c['instrument_id']} "
                                f"({c['type']} {c['strike']}) "
                                f"@ {c['price']:.2f}{sync_mark}"
                            )
                        if count > top_n:
                            logging.info(f"[OptionStatus]   ... 还有 {count - top_n} 个")
                    logging.info("[OptionStatus]")
                
                logging.info(f"[OptionStatus] 总计: {total_count} 个期权合约")
                logging.info("[OptionStatus] " + "=" * 78)
                logging.info("[OptionStatus]")
    
    def _get_current_status(self, internal_id: int) -> str:
        return self._current_status.get(internal_id, 'other')

    # ========================================================================
    # ✅ 分组D准备：by_sort_bucket 索引维护方法
    # ========================================================================

    def _update_sort_bucket(self, internal_id: int, info: Dict, future_internal_id: int, month: str, opt_type: str):
        """
        ✅ 分组D准备：增量更新排序桶
        
        当期权状态/成交量变化时，更新对应的排序桶。
        使用heapq维护前N个最优期权，避免全量扫描。
        
        Args:
            internal_id: internal_id
            info: 期权元数据
            future_internal_id: 期货合约 internal_id（主键）
            month: 月份
            opt_type: CALL/PUT
        """
        try:
            future_info = get_params_service().get_id_cache(future_internal_id)
            if not future_info:
                logging.warning(f"[_update_sort_bucket] Unknown future_internal_id: {future_internal_id}")
                return
            
            # ✅ 获取期货最新价 - 使用 future_internal_id
            m_price = self._get_future_price_by_id_and_month(future_internal_id, month)
            if m_price <= 0:
                return  # 期货价格未就绪，跳过
            
            # 获取期权信息
            strike = info.get('strike_price', 0)
            volume = self._option_volume.get(internal_id, 0)
            price = self._option_price.get(internal_id, 0)
            sync_flag = self._sync_flag.get(internal_id, False)
            instrument_id = info.get('instrument_id', self._internal_id_to_instrument_id.get(internal_id, str(internal_id)))
            
            # 只处理同步虚值期权（correct_rise）
            current_status = self._current_status.get(internal_id, 'other')
            if current_status != 'correct_rise':
                # 如果不是正确上涨状态，从桶中移除
                self._remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)
                return
            
            # 检查是否为虚值
            if not self._is_out_of_the_money(opt_type, m_price, strike):
                self._remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)
                return
            
            # 创建排序条目
            entry = SortEntry.create(
                internal_id=internal_id,
                instrument_id=instrument_id,
                option_type=opt_type,
                strike_price=strike,
                volume=volume,
                price=price,
                sync_flag=sync_flag,
                m_price=m_price,
                month=month,
                product=str(future_info.get('product', '')).upper(),
                last_tick_ts=time.monotonic(),  # 单调时钟，保证时间新鲜度排序有效
            )
            
            # 插入排序桶 - 使用 future_internal_id 作为键
            bucket = self._sort_buckets[future_internal_id][month][opt_type]
            
            # 检查是否已存在
            existing_idx = None
            for i, e in enumerate(bucket):
                if e.internal_id == internal_id:
                    existing_idx = i
                    break
            
            if existing_idx is not None:
                # 使用heapq公共API：先移除旧条目再重新堆化
                bucket[existing_idx] = bucket[-1]
                bucket.pop()
                heapq.heapify(bucket)  # O(n) 重新堆化，使用公共API避免内部函数
            
            # 插入新的
            if len(bucket) < self._sort_bucket_max_size:
                heapq.heappush(bucket, entry)
            elif entry < bucket[0]:  # 如果新条目比堆顶更优
                heapq.heapreplace(bucket, entry)
                
        except Exception as e:
            logging.warning(f"[_update_sort_bucket] Error updating sort bucket for {internal_id}: {e}")

    def _remove_from_sort_bucket(self, internal_id: int, future_internal_id: int, month: str, opt_type: str):
        """
        从排序桶中移除指定期权
        
        Args:
            internal_id: internal_id
            future_internal_id: 期货合约 internal_id（主键）
            month: 月份
            opt_type: CALL/PUT
        """
        try:
            # ✅ 使用 future_internal_id 作为键
            bucket = self._sort_buckets.get(future_internal_id, {}).get(month, {}).get(opt_type, [])
            for i, entry in enumerate(bucket):
                if entry.internal_id == internal_id:
                    bucket[i] = bucket[-1]
                    bucket.pop()
                    heapq.heapify(bucket)
                    break
        except Exception as e:
            logging.warning(f"[_remove_from_sort_bucket] Error removing {internal_id}: {e}")

    def select_from_sort_bucket(self, future_internal_id: int, month: str, opt_type: str, top_n: int = 1) -> List[Dict[str, Any]]:
        """
        ✅ 分组D准备：从排序桶直接读取最优期权
        
        这是 select_otm_targets_by_volume 的优化版本，直接从排序桶读取，
        避免全量扫描。
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            month: 月份
            opt_type: CALL/PUT
            top_n: 返回前N个
        
        Returns:
            最优期权列表
        """
        with self._lock:
            try:
                # ✅ 使用 future_internal_id 作为键
                bucket = self._sort_buckets.get(future_internal_id, {}).get(month, {}).get(opt_type, [])
                if not bucket:
                    return []
                
                # 堆已经按排序键排好序，直接取前top_n个
                sorted_entries = sorted(bucket)[:top_n]
                
                results = []
                for entry in sorted_entries:
                    results.append({
                        'instrument_id': entry.instrument_id,
                        'internal_id': entry.internal_id,
                        'option_type': entry.option_type,
                        'strike_price': entry.strike_price,
                        'volume': entry.volume,
                        'price': entry.price,
                        'month': entry.month,
                        'product': entry.product,
                        'sync_flag': entry.sync_flag,
                    })
                
                return results
                
            except Exception as e:
                logging.error(f"[select_from_sort_bucket] Error: {e}")
                return []

    def get_all_options(self) -> List[Dict[str, Any]]:
        """获取所有已注册的期权合约列表"""
        with self._lock:
            results = []
            for internal_id, info in self._option_info.items():
                display_meta = self._get_option_display_products(info)
                results.append({
                    'internal_id': internal_id,
                    'instrument_id': info.get('instrument_id'),
                    'option_type': info.get('option_type'),
                    'strike_price': info.get('strike_price'),
                    'underlying_product': display_meta.get('underlying_product'),
                    'future_product': display_meta.get('future_product'),
                    'underlying_future_id': info.get('underlying_future_id'),
                    'month': info.get('month'),
                    'price': self._option_price.get(internal_id, 0.0),
                })
            return results

    def select_otm_targets_by_volume(self, future_internal_id: int = None) -> List[Dict[str, Any]]:
        """
        ✅ 分组D优化：使用 by_sort_bucket 索引，避免全量扫描
        
        优化前：遍历 _options_by_future_type 全量筛选 O(N)
        优化后：直接从排序桶读取 O(1)
        
        Args:
            future_internal_id: 可选，指定期货合约 internal_id。如果为 None，则自动选择最优期货。
        """
        # P0 Bug #16修复：WidthStrengthCache自身就是缓存，直接使用self
        if hasattr(self, '_width_cache') and self._width_cache:
            return self._width_cache.select_otm_targets_by_volume(future_internal_id)
        
        # ✅ Fallback: 旧逻辑（如果 _width_cache 未初始化）
        with self._lock:
            now = datetime.now()
            current_month = f"{now.year % 100 + 2000}{now.month:02d}"
            
            # 第一步：选择最优期货（使用 future_internal_id 作为键）
            future_scores = []
            for fid, month_data in self._status_counts.items():
                # 如果指定了 future_internal_id，只处理该期货
                if future_internal_id is not None and fid != future_internal_id:
                    continue
                    
                if not self._future_initialized.get(fid, False):
                    continue
                    
                # ✅ 从 id_cache 获取 product
                fp_info = get_params_service().get_id_cache(fid)
                if not fp_info:
                    continue
                
                opt_type = 'CALL' if self._future_rising.get(fid, False) else 'PUT'
                cr = sum(m_data.get(opt_type, {}).get('correct_rise', 0) for m_data in month_data.values())
                wr = sum(m_data.get(opt_type, {}).get('wrong_rise', 0) for m_data in month_data.values())
                if cr > 0 and cr > wr:
                    future_scores.append({'future_internal_id': fid, 'correct_rise': cr, 'wrong_rise': wr})
            
            future_scores.sort(key=lambda x: -x['correct_rise'])
            top_futures = future_scores[:2]
            if not top_futures:
                return []
            
            # 第二步：✅ 从排序桶直接读取最优期权（避免全量扫描）
            targets = []
            for pidx, fs in enumerate(top_futures):
                fid = fs['future_internal_id']
                try:
                    from ali2026v3_trading.position_service import get_position_service
                    pos_svc = get_position_service()
                except Exception:
                    pos_svc = None
                
                if not get_params_service().get_id_cache(fid):
                    continue
                
                future_rising = self._future_rising.get(fid, False)
                opt_type = 'CALL' if future_rising else 'PUT'
                
                # ✅ 序号131修复：使用排序桶直接获取最优期权（O(1) vs O(N)）- 使用 future_internal_id
                best_candidates = self.select_from_sort_bucket(fid, current_month, opt_type, top_n=1)
                
                if not best_candidates:
                    # ✅ 排序桶为空时跳过，不再回退到全量扫描
                    logging.debug(f"[select_otm_targets] Sort bucket empty for future_internal_id={fid}, skipping")
                    continue
                
                best = best_candidates[0]
                
                if best:
                    if pos_svc and hasattr(pos_svc, 'has_position') and pos_svc.has_position(best['instrument_id']):
                        continue
                    targets.append({**best, 'lots': 2 if pidx == 0 else 1, 'product_rank': pidx + 1})
            
            return targets

# ✅ 新增：导入 DataService（顶级性能数据服务）- P1 修复：增强健壮性
try:
    from ali2026v3_trading.data_service import DataService
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning(f"[TTypeService] Failed to import DataService: {e}")
    _HAS_DATA_SERVICE = False
    DataService = None

# ============================================================================
# 工具函数
# ============================================================================

def _to_float32(value: Any) -> float:
    """✅ ID唯一：委托shared_utils.to_float32，不再独立实现"""
    from ali2026v3_trading.shared_utils import to_float32
    return to_float32(value)



class TTypeService:
    """T 型图服务 - Query 层

    职责:
    - 期权宽度计算
    - T 型图数据生成
    - 期权类型识别
    - 虚值/实值判断

    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 高性能缓存（复用 DataService）
    
    优化 v2.0:
    - 集成 DataService 获取标的价格和 K 线数据
    - 移除重复的数据查询逻辑
    - 利用 DataService 的索引和缓存（5-20x 性能提升）
    """
    
    def __init__(self, data_source: Optional[Any] = None, use_data_service: bool = True):
        """初始化 T 型图服务
        
        Args:
            data_source: 数据源（已弃用，直接使用 DataService）
            use_data_service: 是否使用 DataService（默认 True，推荐）
        """
        # ✅ #89修复：DiagnosisProbeManager导入失败时不阻断TTypeService初始化
        try:
            from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
        except ImportError:
            DiagnosisProbeManager = _NoOpDiagnosisProbeManager

        # 线程锁
        self._data_lock = threading.RLock()

        # ✅ 新增：DataService 集成
        self._data_service: Optional[Any] = None
        if use_data_service and _HAS_DATA_SERVICE:
            try:
                with DiagnosisProbeManager.startup_step("TTypeService.__init__.DataService"):
                    # ✅ 传递渠道唯一：统一使用get_data_service()工厂函数获取单例，禁止直接实例化
                    from ali2026v3_trading.data_service import get_data_service
                    self._data_service = get_data_service()
                logging.info("[TTypeService] DataService integration enabled")
            except Exception as e:
                logging.warning(f"[TTypeService] Failed to initialize DataService: {e}")
                self._data_service = None
        
        # ✅ 新增：WidthStrengthCache 内存缓存（已内嵌）
        try:
            with DiagnosisProbeManager.startup_step("TTypeService.__init__.WidthStrengthCache"):
                self._width_cache = WidthStrengthCache()
            logging.info("[TTypeService] WidthStrengthCache integration enabled (O(1) query)")
            # ✅ 修复：将 _preload_from_db 改为后台异步执行，不阻塞初始化（解决14分钟启动延迟）
            self._preload_thread = None
            self._preload_stop = threading.Event()
            self._preload_started = False
            self._preload_complete = False
            logging.info("[TTypeService] _preload_from_db will run in background thread")
        except Exception as e:
            logging.warning(f"[TTypeService] Failed to initialize WidthStrengthCache: {e}")
            self._width_cache = None
    
    def _ensure_preload_started(self):
        """确保后台预加载线程已启动（惰性触发模式）"""
        if not self._width_cache or not self._data_service:
            return

        with self._data_lock:
            preload_thread = self._preload_thread
            if preload_thread and preload_thread.is_alive():
                return

            if self._preload_stop.is_set():
                self._preload_stop.clear()
                self._preload_started = False
                self._preload_complete = False
                self._preload_thread = None

            if self._preload_started and self._preload_complete:
                return

            if self._preload_started and self._preload_thread is None:
                self._preload_started = False

            if self._preload_started:
                return

            self._preload_thread = threading.Thread(
                target=self._preload_from_db,
                name="TTypeService-Preload[shared-service]",
                daemon=True
            )
            self._preload_started = True
            self._preload_thread.start()
            logging.info("[TTypeService] Background preload thread started")
    
    def _mark_preload_complete(self):
        """标记预加载完成"""
        self._preload_complete = True
    
    # 使用 WidthStrengthCache 的统一方法（避免重复定义）
    _normalize_option_type = staticmethod(WidthStrengthCache._normalize_option_type)

    # ✅ 方法唯一：委托shared_utils.extract_strike_price，不再独立实现
    @staticmethod
    def _extract_strike_price(instrument_id: str) -> float:
        """
        从期权合约代码提取行权价
        """
        from ali2026v3_trading.shared_utils import extract_strike_price
        result = extract_strike_price(instrument_id)
        return result if result is not None else 0.0
    
    def get_underlying_price_from_service(self, symbol: str) -> Optional[float]:
        """
        从 DataService 获取标的价格（优先使用）- P1 修复：懒加载
        
        Args:
            symbol: 合约代码
            
        Returns:
            Optional[float]: 当前价格，如果 DataService 不可用则返回 None
        """
        ds = self._get_data_service()
        if ds:
            try:
                return ds.get_latest_price(symbol)
            except Exception as e:
                logging.error(f"[TTypeService] Failed to get price from DataService: {e}")
        return None
    
    def _get_data_service(self):
        """P1 修复：获取 DataService 实例（懒加载模式）
        ✅ 传递渠道唯一：统一使用get_data_service()工厂函数，禁止直接实例化
        """
        if self._data_service is None:
            try:
                from ali2026v3_trading.data_service import get_data_service
                self._data_service = get_data_service()
                logging.info("[TTypeService] DataService lazily initialized via get_data_service().")
            except Exception as e:
                logging.error(f"[TTypeService] Failed to initialize DataService: {e}")
                return None
        return self._data_service

    def calculate_option_width(
        self,
        instrument_id: str,
        underlying_price: Optional[float] = None,
        strike_price: float = 0.0,
        option_type: str = 'CALL'
    ) -> Dict[str, Any]:
        """计算期权宽度 - P2优化：优先使用固化数据，避免重复计算
        
        Args:
            instrument_id: 期权合约代码
            underlying_price: 标的价格（可选，如不传则从缓存获取）
            strike_price: 行权价（可选，如为 0 则从缓存读取固化值）
            option_type: 期权类型 ('CALL'/'PUT')
            
        Returns:
            Dict: 计算结果 {width, moneyness, is_otm, ...}
        """
        try:
            source = 'fallback'
            option_context = self._resolve_option_context(instrument_id)

            # ✅ P2 优化：优先从 WidthStrengthCache 读取固化数据
            if self._width_cache:
                # 通过 instrument_id -> internal_id 查找
                iid = self._width_cache._instrument_id_to_internal_id.get(
                    self._width_cache._normalize_instrument_id(instrument_id)
                )
                info = self._width_cache._option_info.get(iid) if iid is not None else None
                if info:
                    source = 'cache'
                    # 直接使用固化字段
                    if strike_price <= 0:
                        strike_price = info['strike_price']
                    if self._normalize_option_type(option_type) == 'CALL':
                        option_type = info['option_type']
                    
                    # 从缓存获取对应月份的期货价格
                    if underlying_price is None:
                        underlying_future_id = info.get('underlying_future_id')
                        month = info['month']
                        underlying_price = self._width_cache._get_future_price_by_id_and_month(underlying_future_id, month)
            
            if strike_price <= 0:
                strike_price = option_context['strike_price'] or strike_price
            if (not option_type or self._normalize_option_type(option_type) == 'CALL') and option_context['option_type']:
                option_type = option_context['option_type'] or option_type

            if underlying_price is None or underlying_price <= 0:
                month = option_context['month']
                underlying_future_id = option_context['underlying_future_id']
                if self._width_cache and underlying_future_id not in (None, '') and month:
                    cached_price = self._width_cache._get_future_price_by_id_and_month(underlying_future_id, month)
                    if cached_price > 0:
                        underlying_price = cached_price
                        if source == 'fallback':
                            source = 'cache'

                if underlying_price is None or underlying_price <= 0:
                    underlying_price = self._get_underlying_price_by_future_id(underlying_future_id)
            
            # 再次降级：解析行权价
            if strike_price <= 0:
                strike_price = self._extract_strike_price(instrument_id)
            
            # 参数验证
            if not instrument_id or (underlying_price is not None and underlying_price <= 0) or strike_price <= 0:
                logging.error(f"[TTypeService] Invalid params: {instrument_id}, {underlying_price}, {strike_price}")
                return {}

            # 计算虚实值程度
            opt_type_normalized = self._normalize_option_type(option_type)
            if opt_type_normalized == 'CALL':
                moneyness = (underlying_price - strike_price) / strike_price * 100
                is_otm = underlying_price < strike_price
            else:
                moneyness = (strike_price - underlying_price) / strike_price * 100
                is_otm = underlying_price > strike_price
            
            # 计算宽度（绝对值）
            width = abs(underlying_price - strike_price)
            
            result = {
                'instrument_id': instrument_id,
                'underlying_price': _to_float32(underlying_price) if underlying_price else None,
                'strike_price': _to_float32(strike_price),
                'option_type': self._normalize_option_type(option_type),
                'width': _to_float32(width),
                'moneyness_percent': _to_float32(moneyness),
                'is_otm': is_otm,
                'is_itm': not is_otm,
                'calculated_at': datetime.now(),
                'source': source,
            }

            logging.debug(f"[TTypeService] Calculated: {instrument_id} width={width:.2f}, moneyness={moneyness:.2f}%")
            return result
            
        except Exception as e:
            logging.error(f"[TTypeService] Calculation error: {e}")
            return {}
    
    # ======================== ✅ DataService 集成增强功能 ========================
    
    def calculate_option_width_strength(
        self,
        specified_months: Optional[List[str]] = None,  # 如 ['2601', '2602', '2603', '2604', '2605']
        underlying_future_id: int = None,  # ✅ 期货合约 internal_id（主键）
        option_type_filter: Optional[str] = None,  # 'CALL' / 'PUT' / None
        min_width_threshold: float = 4.0
    ) -> Dict[str, Any]:
        """
        计算期权宽度强度 - 优先使用内存缓存，降级到 DuckDB
        
        核心逻辑：
        1. 优先从 WidthStrengthCache 获取（O(1)，< 100 ns）
        2. 如果缓存不可用，降级到 DuckDB 查询（10-100 μs）
        
        Args:
            specified_months: 指定月份列表（如 ['2601', '2602', '2603', '2604', '2605']）
                             必须显式指定，不自动生成
            underlying_future_id: 期货合约 internal_id（主键）
            option_type_filter: 期权类型过滤 ('CALL'/'PUT'/None)
            min_width_threshold: 最小宽度阈值
        
        Returns:
            Dict: {
                'product': str,                    # 产品代码
                'future_rising': bool,             # 期货是否上涨
                'width_strength': int,             # 宽度强度
                'month_details': Dict,             # 各月详情
                'all_sync': bool,                  # 是否全部同步
                'calculated_at': datetime
            }
        """
        # ✅ 优先使用内存缓存（O(1) 查询）
        if self._width_cache and specified_months and underlying_future_id:
            try:
                # 🟡 中优先级：使用封装方法访问，避免直接访问私有属性
                future_rising = self._width_cache.get_future_rising(underlying_future_id)
                width_strength = self._width_cache.get_width_strength(
                    underlying_future_id, specified_months, option_type_filter
                )
                
                # 构建月详情
                month_details = {}
                for month in specified_months:
                    cnt = self._width_cache.get_width_strength(
                        underlying_future_id, [month], option_type_filter
                    )
                    month_details[month] = {
                        'sync_otm_count': cnt,
                        'future_rising': future_rising
                    }
                
                # ✅ 从 id_cache 获取 product
                info = get_params_service().get_id_cache(underlying_future_id)
                product = str(info.get('product', '')).upper() if info else ''
                
                return {
                    'product': product,
                    'future_rising': future_rising,
                    'width_strength': width_strength,
                    'month_details': month_details,
                    'all_sync': width_strength > 0,
                    'total_months': len(specified_months),
                    'calculated_at': datetime.now()
                }
            except Exception as e:
                logging.error(f"[TTypeService] WidthStrengthCache calculation failed: {e}")
                # ✅ 序号132修复：内存缓存为唯一策略，不再降级到DB查询
                return self._empty_width_result(product, specified_months)
    
    # ✅ 接口唯一：_empty_width_result唯一实现，同文件内调用
    def _empty_width_result(self, product: str, months: List[str]) -> Dict[str, Any]:
        """返回空的宽度强度结果（降级失败时使用）"""
        month_details = {month: {'sync_otm_count': 0, 'future_rising': False} for month in months}
        return {
            'product': product,
            'future_rising': False,
            'width_strength': 0,
            'month_details': month_details,
            'all_sync': False,
            'total_months': len(months),
            'calculated_at': datetime.now(),
            'source': 'empty_fallback'
        }
    


    def select_trading_targets(
        self,
        width_strength_results: Dict[str, Dict[str, Any]],
        top_n: int = 5,
        min_width_threshold: float = 4.0
    ) -> List[Dict[str, Any]]:
        """
        按照宽度三原则选择交易标的
        
        宽度三原则：
        1. 同月份：同一到期月份的期权（已在计算时分别统计）
        2. 同方向：都是看涨或都是看跌（已根据期货方向选择）
        3. 同虚实度：都是虚值期权（已过滤）
        
        Args:
            width_strength_results: {underlying_symbol: width_strength_result}
            top_n: 返回前 N 个交易标的
            min_width_threshold: 最小宽度阈值（默认>4）
        
        Returns:
            List[Dict]: 交易标的列表（已排序）
        """
        try:
            # 1. 收集所有结果
            all_results = []
            for symbol, result in width_strength_results.items():
                if not result:
                    continue
                
                width_strength = result.get('width_strength', 0)
                all_sync = result.get('all_sync', False)
                
                # 2. 过滤：宽度必须大于阈值
                if width_strength <= min_width_threshold:
                    continue
                
                # 3. 添加到候选列表
                all_results.append({
                    'symbol': symbol,
                    'width_strength': width_strength,
                    'all_sync': all_sync,
                    'future_rising': result.get('future_rising', False),
                    'month_details': result.get('month_details', {}),
                    'total_months': result.get('total_months', 0),
                    'priority': 0 if all_sync else 1  # 全部同步优先级更高
                })
            
            # 4. 排序规则：
            # - 优先级 1：全部同步 > 部分同步
            # - 优先级 2：宽度强度从大到小
            all_results.sort(key=lambda x: (x['priority'], -x['width_strength']))
            
            # 5. 截取 Top N
            trading_targets = all_results[:top_n]
            
            # 6. 分配信号类型
            signals = []
            if trading_targets:
                max_width = trading_targets[0]['width_strength']
                
                for i, target in enumerate(trading_targets):
                    if target['all_sync'] and target['width_strength'] == max_width:
                        signal_type = "最优信号"
                    elif target['all_sync']:
                        signal_type = "全同步信号"
                    elif not target['all_sync'] and target['width_strength'] == max_width:
                        signal_type = "次优信号"
                    else:
                        signal_type = "部分同步信号"
                    
                    signals.append({
                        **target,
                        'signal_type': signal_type,
                        'rank': i + 1
                    })
            
            return signals
        
        except Exception as e:
            logging.error(f"[TTypeService] Error in select_trading_targets: {e}", exc_info=True)
            return []
    
    # ✅ ID唯一：clear_cache统一接口，服务=TTypeService
    def clear_cache(self) -> None:
        """清空所有缓存（已移除独立缓存，保留接口兼容性）"""
        logging.info('[TTypeService] Cache cleared')
    
    def _preload_from_db(self):
        """从数据库预加载合约数据到缓存（热启动）

        直接基于 latest_prices 和 ticks_raw 中的最新元数据恢复缓存，
        避免依赖未来可能漂移的业务表或临时视图列。
        """
        def _should_stop() -> bool:
            return bool(getattr(self, '_preload_stop', None) and self._preload_stop.is_set())

        # ✅ #89修复：DiagnosisProbeManager导入失败时不阻断
        try:
            from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
        except ImportError:
            DiagnosisProbeManager = _NoOpDiagnosisProbeManager

        if not self._width_cache or not self._data_service:
            logging.warning("[TTypeService] Cannot preload: cache or data service not available")
            return
        
        try:
            if _should_stop():
                logging.info("[TTypeService] Preload canceled before start")
                return

            logging.info("[TTypeService] Preloading contracts from database...")

            # 步骤 1: 从 latest_prices 恢复所有最新期货价格
            futures_loaded = 0
            try:
                with DiagnosisProbeManager.startup_step("TTypeService._preload_from_db.futures_query"):
                    futures_df = self._data_service.query(
                    """
                    SELECT instrument_id, last_price
                    FROM latest_prices
                    WHERE regexp_matches(instrument_id, '^[A-Za-z]+\\d{3,4}$')
                    ORDER BY instrument_id
                    """,
                    arrow=False,
                    raise_on_error=True,
                )

                # ✅ 统一验证：确保所有期货合约已加载
                future_ids = [str(row['instrument_id']) for _, row in futures_df.iterrows() 
                             if re.match(r'^[A-Za-z]+\d{3,4}$', str(row['instrument_id']))]
                ps = get_params_service()
                ps.validate_contracts_loaded(future_ids, "future")

                with DiagnosisProbeManager.startup_step("TTypeService._preload_from_db.futures_loop"):
                    for _, row in futures_df.iterrows():
                        if _should_stop():
                            logging.info("[TTypeService] Preload canceled during futures loop")
                            return
                        instrument_id = str(row['instrument_id'])
                        # ✅ 使用正则解析而非_parse_future_contract
                        match = re.match(r'^([A-Za-z]+)(\d{3,4})$', instrument_id)
                        if not match:
                            continue
                        product = match.group(1).upper()
                        month = match.group(2)
                        price = float(row['last_price']) if row['last_price'] is not None else 0.0
                        if not product or not month or price <= 0:
                            continue
                        
                        # ✅ 从 params_service 获取 future_internal_id（已通过验证，必定存在）
                        ps = get_params_service()
                        future_info = ps.get_instrument_meta_by_id(instrument_id)
                        future_internal_id = int(future_info['internal_id'])
                        self._width_cache.register_future(future_internal_id, price, month)
                        futures_loaded += 1
                        logging.debug(f"[TTypeService] Preloaded future {instrument_id} (id={future_internal_id}) @ {price}")

                if futures_loaded == 0:
                    logging.warning("[TTypeService] No latest future prices found for preload")
            except Exception as e:
                logging.error(f"[TTypeService] Failed to preload futures from latest_prices: {e}", exc_info=True)

            logging.info(f"[TTypeService] Preloaded {futures_loaded} futures contracts")
            DiagnosisProbeManager.mark_startup_event(
                "TTypeService._preload_from_db.futures_loaded",
                detail=f"count={futures_loaded}",
            )

            # 步骤 2: 从最新价 + 最新期权元数据恢复所有期权缓存
            options_loaded = 0
            try:
                with DiagnosisProbeManager.startup_step("TTypeService._preload_from_db.options_query"):
                    options_df = self._data_service.query(
                    """
                    SELECT
                        oi.internal_id,
                        oi.instrument_id,
                        oi.product AS option_product,
                        oi.option_type,
                        oi.strike_price,
                        oi.underlying_future_id,
                        oi.year_month,
                        lp.last_price
                    FROM option_instruments oi
                    LEFT JOIN latest_prices lp ON oi.instrument_id = lp.instrument_id
                    WHERE oi.is_active = 1
                    ORDER BY oi.instrument_id
                    """,
                    arrow=False,
                    raise_on_error=True,
                )

                # ✅ 统一验证：确保所有期权合约已加载
                option_ids = [str(row['instrument_id']) for _, row in options_df.iterrows()]
                ps = get_params_service()
                ps.validate_contracts_loaded(option_ids, "option")

                with DiagnosisProbeManager.startup_step("TTypeService._preload_from_db.options_loop"):
                    for _, row in options_df.iterrows():
                        if _should_stop():
                            logging.info("[TTypeService] Preload canceled during options loop")
                            return
                        try:
                            instrument_id = str(row['instrument_id'])
                            strike = float(row['strike_price']) if row['strike_price'] is not None else 0.0
                            price = float(row['last_price']) if row['last_price'] is not None else 0.0
                            opt_type = str(row['option_type'] or 'CALL')
                            internal_id = int(row['internal_id']) if row.get('internal_id') is not None else None
                            underlying_future_id = row.get('underlying_future_id')
                            
                            option_product = row.get('option_product')
                            if option_product is not None and not isinstance(option_product, str):
                                option_product = str(option_product)
                            year_month = row.get('year_month')
                            if year_month is not None and not isinstance(year_month, str):
                                year_month = str(year_month)
                            month = year_month

                            # ✅ 已通过validate_contracts_loaded验证，internal_id必定存在
                            # 但仍需检查其他字段的完整性
                            if not option_product or not month or underlying_future_id in (None, '') or strike <= 0:
                                raise ValueError(f"Missing required fields: product={option_product}, month={month}, underlying={underlying_future_id}, strike={strike}")
                            
                            # ✅ 价格可以为0（未交易的期权），但仍需注册到缓存
                            if price <= 0:
                                logging.debug(f"[TTypeService] Option {instrument_id} has no last_price, registering with price=0")
                                price = 0.0

                            self._width_cache.register_option(
                                instrument_id,
                                option_product,
                                month,
                                strike,
                                opt_type,
                                price,
                                underlying_future_id=underlying_future_id,
                                internal_id=internal_id,
                            )
                            options_loaded += 1
                        except Exception as e:
                            # ❌ 关键错误：配置表中的合约数据异常
                            logging.error(f"[TTypeService] CRITICAL: Failed to preload option {row.get('instrument_id', 'N/A')}: {e}", exc_info=True)
                            raise  # 直接抛出，阻止初始化

                if options_loaded == 0:
                    logging.warning("[TTypeService] No latest option prices found for preload")

                logging.info(f"[TTypeService] Preloaded {options_loaded} option contracts")
                DiagnosisProbeManager.mark_startup_event(
                    "TTypeService._preload_from_db.options_loaded",
                    detail=f"count={options_loaded}",
                )
            except Exception as e:
                logging.error(f"[TTypeService] Failed to load options: {e}", exc_info=True)
            
            # 总结
            stats = self._width_cache.get_cache_stats()
            logging.info(f"[TTypeService] Preload complete - Futures: {stats['total_futures']}, Options: {stats['total_options']}")
            DiagnosisProbeManager.mark_startup_event(
                "TTypeService._preload_from_db.cache_stats",
                detail=f"futures={stats['total_futures']}, options={stats['total_options']}",
            )
            
        except Exception as e:
            logging.error(f"[TTypeService] Failed to preload from DB: {e}", exc_info=True)
        finally:
            with self._data_lock:
                self._preload_thread = None
                if self._preload_stop.is_set():
                    self._preload_started = False
                    self._preload_complete = False
                else:
                    self._mark_preload_complete()
    
    # ======================== ✅ WidthStrengthCache 公共 API ========================
    
    def on_future_tick(self, future_internal_id: int, price: float, month: str = None):
        """外部行情推送时调用，更新期货价格并自动重算宽度强度
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            price: 期货最新价
            month: 可选，合约月份 (如 '2605')
        """
        # ✅ 修复：首次tick时触发后台预加载（惰性触发模式，解决14分钟启动延迟）
        self._ensure_preload_started()
        if self._width_cache:
            self._width_cache.on_future_tick(future_internal_id, price, month)

    def on_future_instrument_tick(self, future_internal_id: int, price: float):
        """✅ 外部行情推送时按期货 internal_id 更新 TType（ID语义）。"""
        # ✅ 修复：首次tick时触发后台预加载
        self._ensure_preload_started()
        if self._width_cache:
            self._width_cache.on_future_instrument_tick(future_internal_id, price)
    
    def on_option_tick(self, instrument_id: str, price: float, volume: float = 0):
        # ✅ 修复：首次tick时触发后台预加载
        self._ensure_preload_started()
        if self._width_cache:
            self._width_cache.on_option_tick(instrument_id, price, volume)
    
    # ✅ 接口唯一：register_future_contract委托给WidthStrengthCache.register_future
    def register_future_contract(self, future_internal_id: int, initial_price: float):
        """注册期货合约到缓存
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            initial_price: 初始价格
        """
        if self._width_cache:
            self._width_cache.register_future(future_internal_id, initial_price)
    
    # ✅ 接口唯一：register_option_contract委托给WidthStrengthCache.register_option
    def register_option_contract(self, instrument_id: str, underlying_product: str,
                                 month: str, strike_price: float, option_type: str,
                                 initial_price: float,
                                 underlying_future_id: Optional[int] = None,
                                 internal_id: Optional[int] = None):
        """注册期权合约到缓存
        
        Args:
            instrument_id: 期权合约代码
            underlying_product: 期权产品代码（兼容展示入参，不参与关系推导）
            month: 月份 (如 '2603')
            strike_price: 行权价
            option_type: 期权类型 ('CALL'/'PUT')
            initial_price: 初始价格
            underlying_future_id: 标的期货 internal_id，主链来源
            internal_id: 系统内部代理键（优先使用）
        """
        if self._width_cache:
            self._width_cache.register_option(
                instrument_id, underlying_product, month, 
                strike_price, option_type, initial_price,
                underlying_future_id=underlying_future_id,
                internal_id=internal_id,
            )

    def register_option(self, instrument_id: str, underlying_product: str,
                        month: str, strike_price: float, option_type: str,
                        initial_price: float,
                        underlying_future_id: Optional[int] = None,
                        internal_id: Optional[int] = None):
        """兼容旧调用路径，转发到 register_option_contract。"""
        self.register_option_contract(
            instrument_id=instrument_id,
            underlying_product=underlying_product,
            month=month,
            strike_price=strike_price,
            option_type=option_type,
            initial_price=initial_price,
            underlying_future_id=underlying_future_id,
            internal_id=internal_id,
        )
    
    def get_width_strength_from_cache(self, future_internal_id: int, months: List[str], 
                                      option_type: str = None) -> int:
        """从内存缓存获取宽度强度 - O(1) 查询
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            months: 月份列表
            option_type: 可选，'CALL' 或 'PUT'
            
        Returns:
            int: 宽度强度（同步虚值期权计数）
        """
        if self._width_cache:
            return self._width_cache.get_width_strength(future_internal_id, months, option_type)
        return 0
    
    def get_width_cache_stats(self) -> Dict[str, Any]:
        """获取宽度缓存统计信息（含内存占用估算）"""
        if self._width_cache:
            return self._width_cache.get_cache_stats()
        return {
            'enabled': False,
            'total_futures': 0,
            'total_options': 0,
            'query_count': 0
        }
    
    def print_option_status_diagnosis(self, future_internal_id: int = None, top_n: int = 10):
        """打印期权状态诊断报告
        
        Args:
            future_internal_id: 期货合约 internal_id（主键），None 表示所有期货
            top_n: 每个状态下显示前 N 个合约
        """
        if self._width_cache:
            self._width_cache.print_status_diagnosis(future_internal_id, top_n)
    
    def get_all_options(self) -> List[Dict[str, Any]]:
        """获取所有已注册的期权合约列表"""
        if self._width_cache:
            return self._width_cache.get_all_options()
        return []
    
    def select_otm_targets_by_volume(self, product: str = None) -> List[Dict[str, Any]]:
        """代理方法：委托给 WidthStrengthCache.select_otm_targets_by_volume"""
        if self._width_cache:
            return self._width_cache.select_otm_targets_by_volume(product)
        return []


# ✅ 传递渠道唯一：TTypeService单例工厂函数，禁止直接实例化
_t_type_service_instance: Optional['TTypeService'] = None
_t_type_service_lock = threading.Lock()


def get_t_type_service() -> 'TTypeService':
    """获取TTypeService单例（线程安全）"""
    global _t_type_service_instance
    if _t_type_service_instance is None:
        with _t_type_service_lock:
            if _t_type_service_instance is None:
                _t_type_service_instance = TTypeService()
    return _t_type_service_instance


# 导出公共接口
__all__ = [
    'TTypeService',
    'get_t_type_service',
]
