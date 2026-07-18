# MODULE_ID: M1-050
"""
storage_snapshot_mixin.py — 快照/信号/KV写入Mixin
包含: _StorageSnapshotMixin
"""

from infra.shared_utils import InitPhase, requires_phase, CHINA_TZ
from data.query_service import _KlineAggregator
import logging
import threading
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

from infra.serialization_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)


class StorageSnapshotService:

    def __init__(self, facade=None):
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


    def _batch_insert_with_fallback(self, table_name: str, fields: Tuple[str, ...], rows: List[Any],  # [R22-P2-TS19]
                                     batch_limit: int = 100, caller: str = '') -> None:
        if not rows:
            return
        field_str = ', '.join(fields)
        placeholder_str = ', '.join(['?'] * len(fields))
        update_fields = ', '.join([f"{f} = excluded.{f}" for f in fields[1:]])
        for batch_start in range(0, len(rows), batch_limit):
            batch = rows[batch_start:batch_start + batch_limit]
            flat_values = [v for row in batch for v in row]
            multi_placeholder = ', '.join([f'({placeholder_str})'] * len(batch))
            # R5-E-09修复: _DuckDBConnectionContextManager包装器+连接池context manager支持
            # R5-E-10修复: _TimedDuckDBConnection查询级30s超时保护+连接级10s超时保护
            try:
                self._data_service.query(f"""
                    INSERT INTO {table_name}
                    ({field_str})
                    VALUES {multi_placeholder}
                    ON CONFLICT(timestamp) DO UPDATE SET
                    {update_fields}
                """, flat_values, raise_on_error=True)
            except Exception as e:
                logging.error("[R5-E-02] %s batch insert failed, fallback to row-by-row: %s", caller, e, exc_info=True)
                for row in batch:
                    try:
                        self._data_service.query(f"""
                            INSERT INTO {table_name}
                            ({field_str})
                            VALUES ({placeholder_str})
                            ON CONFLICT(timestamp) DO UPDATE SET
                            {update_fields}
                        """, list(row), raise_on_error=True)
                    except Exception as _row_err:
                        logging.error("[R5-E-02] 单行写入失败(数据可能丢失): %s", _row_err, exc_info=True)

    def _save_depth_batch_impl(self, depth_list: List[Dict[str, Any]]):
        grouped: Dict[str, List[tuple]] = {}
        for d in depth_list:
            try:
                internal_id = self.register_instrument(d['instrument_id'])
            except ValueError as e:
                logging.debug("_save_depth_batch_impl register failed: %s", e)
                continue

            info = self._get_info_by_id(internal_id)
            if not info:
                continue

            table_name = info.get('tick_table', 'ticks_raw')
            self._validate_table_name(table_name)

            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            values = [d.get('ts') or d.get('timestamp'),
                      d.get('last_price', 0.0),
                      d.get('volume', 0),
                      d.get('open_interest', 0)]

            for i in range(1, 6):
                bid_key = f'bid_price{i}'
                ask_key = f'ask_price{i}'
                if bid_key in d:
                    fields.append(bid_key)
                    values.append(d[bid_key])
                if ask_key in d:
                    fields.append(ask_key)
                    values.append(d[ask_key])

            key = (table_name, tuple(fields))
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tuple(values))

        for (table_name, fields), rows in grouped.items():
            if not rows:
                continue
            self._batch_insert_with_fallback(table_name, fields, rows, caller='_save_depth_batch_impl')

    @requires_phase(InitPhase.READY)
    def save_depth_batch(self, depth_list: List[Dict[str, Any]]) -> None:
        if not isinstance(depth_list, list):
            logging.error("save_depth_batch 参数不是列表：%s", type(depth_list))
            return
        if not depth_list:
            return

        valid_list = []
        for d in depth_list:
            if not isinstance(d, dict):
                continue
            required = ['ts', 'instrument_id', 'last_price']
            if any(field not in d for field in required):
                continue
            price = d.get('last_price')
            if price is not None and (not isinstance(price, (int, float)) or price <= 0):
                continue
            d_copy = d.copy()
            ts = self._to_timestamp(d_copy.get('ts'))
            if ts is None:
                continue
            d_copy['ts'] = ts
            valid_list.append(d_copy)

        if valid_list:
            self._enqueue_write('_save_depth_batch_impl', valid_list)

    def _save_signal_impl(self, signal: Dict[str, Any]):
        try:
            internal_id = self.register_instrument(signal['instrument_id'])
        except ValueError as e:
            logging.debug("_save_signal_impl register failed: %s", e)
            return

        info = self._get_info_by_id(internal_id)
        if not info:
            logging.error("_save_signal_impl 找不到合约信息：%s", signal['instrument_id'])
            return

        key = f"signal:{signal['ts']}:{signal['instrument_id']}:{signal['strategy_name']}"
        # SER-P1-12修复: signal高频写入路径使用json_dumps统一接口，sort_keys=False避免排序开销
        self._save_kv_impl(key, json_dumps(signal, sort_keys=False))
        strategy_key = f"strategy_signals:{signal.get('strategy_name', 'unknown')}:{signal['ts']}"
        self._save_kv_impl(strategy_key, json_dumps(signal, sort_keys=False))

    @requires_phase(InitPhase.READY)
    def save_signal(self, signal: Dict[str, Any]) -> None:
        if not self._validate_signal(signal):
            return
        signal = signal.copy()
        ts = self._to_timestamp(signal.get('ts'))
        if ts is None:
            logging.error("save_signal 时间戳转换失败：%s", signal.get('ts'))
            return
        signal['ts'] = ts
        self._enqueue_write('_save_signal_impl', signal)

    def _save_underlying_snapshot_impl(self, data: Dict[str, Any]):
        key = f"underlying_snapshot:{data.get('underlying')}:{data.get('expiration')}:{data.get('ts')}"
        # SER-P1-12修复: 快照写入使用json_dumps统一接口，sort_keys=False避免排序开销
        self._save_kv_impl(key, json_dumps(data, sort_keys=False))
        logging.debug("标的物快照已保存：%s %s", data.get('underlying'), data.get('expiration'))

    @requires_phase(InitPhase.READY)
    def save_underlying_snapshot(self, data: Dict[str, Any]) -> None:
        if not self._validate_underlying(data):
            return
        data = data.copy()
        ts = self._to_timestamp(data.get('ts'))
        if ts is None:
            logging.error("save_underlying_snapshot 时间戳转换失败：%s", data.get('ts'))
            return
        data['ts'] = ts
        self._enqueue_write('_save_underlying_snapshot_impl', data)

    def _save_option_snapshot_batch_impl(self, data_list: List[Dict[str, Any]]):
        grouped: Dict[str, List[tuple]] = {}
        for d in data_list:
            try:
                internal_id = self.register_instrument(d['instrument_id'])
            except ValueError as e:
                logging.debug("_save_option_snapshot_batch_impl register failed: %s", e)
                continue

            info = self._get_info_by_id(internal_id)
            if not info:
                continue

            table_name = info.get('tick_table', 'ticks_raw')
            self._validate_table_name(table_name)

            fields = ['timestamp', 'last_price', 'volume', 'open_interest']
            values = [d.get('ts') or d.get('timestamp'),
                      d.get('last_price', 0.0),
                      d.get('volume', 0),
                      d.get('open_interest', 0)]

            if 'bid_price1' in d:
                fields.append('bid_price1')
                values.append(d['bid_price1'])
            if 'ask_price1' in d:
                fields.append('ask_price1')
                values.append(d['ask_price1'])

            option_fields = ['implied_volatility', 'delta', 'gamma', 'theta', 'vega']
            for field in option_fields:
                if field in d:
                    fields.append(field)
                    values.append(d[field])

            key = (table_name, tuple(fields))
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(tuple(values))

        for (table_name, fields), rows in grouped.items():
            if not rows:
                continue
            self._batch_insert_with_fallback(table_name, fields, rows, caller='_save_option_snapshot_batch_impl')

    @requires_phase(InitPhase.READY)
    def save_option_snapshot_batch(self, data_list: List[Dict[str, Any]]) -> None:
        if not isinstance(data_list, list):
            logging.error("save_option_snapshot_batch 参数不是列表：%s", type(data_list))
            return
        if not data_list:
            return

        valid_list = []
        for d in data_list:
            if not isinstance(d, dict):
                continue
            required = ['ts', 'instrument_id', 'last_price']
            if any(field not in d for field in required):
                continue
            strike = d.get('strike')
            if strike is not None and (not isinstance(strike, (int, float)) or strike <= 0):
                continue
            d_copy = d.copy()
            ts = self._to_timestamp(d_copy.get('ts'))
            if ts is None:
                continue
            d_copy['ts'] = ts
            valid_list.append(d_copy)

        if valid_list:
            self._enqueue_write('_save_option_snapshot_batch_impl', valid_list)

    def _save_kv_impl(self, key: str, payload: str):
        try:
            self._data_service.query(
                """
                INSERT OR REPLACE INTO app_kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                [key, payload, datetime.now(CHINA_TZ).isoformat()],
                raise_on_error=True,
            )
        except Exception as e:
            logging.error(f"_save_kv_impl 保存失败 key={key}: {e}")
            raise

    def save(self, key: str, data: Any, async_mode: bool = True) -> bool:
        if not key or not isinstance(key, str):
            logging.error("save 失败：key 无效：%s", key)
            return False

        try:
            # SER-P1-12修复: 通用save路径使用json_dumps统一接口
            # SER-04修复: 包装版本号，确保payload格式可演进
            payload = json_dumps({"__kv_version__": "1.0", "data": data})

            if async_mode:
                return self._enqueue_write('_save_kv_impl', key, payload)
            else:
                self._save_kv_impl(key, payload)
                return True
        except Exception as e:
            logging.error(f"save 异常：{e}")
            return False

    def _save_aggregator_states(self):
        """BP-17：持久化K线聚合器状态到KV store"""
        try:
            states = {}
            with self._agg_lock:
                for (instrument, period), agg in self._aggregators.items():
                    state = agg.to_state_dict()
                    if state:
                        states[f"{instrument}:{period}"] = state
            if states:
                self.save('kline_aggregator_states', states, async_mode=False)
                logging.info("[BP-17] 已持久化 %d 个K线聚合器状态", len(states))
        except Exception as e:
            logging.warning("[BP-17] K线聚合器状态持久化失败: %s", e)

    def _restore_aggregator_states(self):
        """BP-17：从KV store恢复K线聚合器状态"""
        try:
            states = self.load('kline_aggregator_states')
            if not states:
                return
            restored = 0
            with self._agg_lock:
                for key, state in states.items():
                    instrument = state.get('instrument_id', '')
                    period = state.get('period', '')
                    if not instrument or not period:
                        continue
                    agg_key = (instrument, period)
                    if agg_key not in self._aggregators:
                        self._aggregators[agg_key] = _KlineAggregator.from_state_dict(state)
                        restored += 1
            if restored > 0:
                logging.info("[BP-17] 已恢复 %d 个K线聚合器状态", restored)
        except Exception as e:
            logging.warning("[BP-17] K线聚合器状态恢复失败: %s", e)

    def _update_external_kline_timestamp(self, instrument_id: str, period: str, ts: float) -> None:
        key = (instrument_id, period)
        with self._ext_kline_lock:
            self._last_ext_kline[key] = ts

    def _is_ext_kline_missing(self, instrument: str, period: str, current_time: float) -> bool:
        # FIX-M8-1: 原代码在_ext_kline_load_in_progress=True时直接return False
        # 阻止所有合成K线生成。但历史加载可能完全失败(success=0, failed=250)，
        # 此时_last_ext_kline为空，应允许合成K线作为唯一K线来源。
        # 修复: 仅在已有外部K线数据(last_time不为None)时，加载期间跳过合成K线
        # 当last_time为None时，无论是否在加载中，都允许合成K线
        key = (instrument, period)
        with self._ext_kline_lock:
            last_time = self._last_ext_kline.get(key)
            if self._ext_kline_load_in_progress and last_time is not None:
                # 已有外部K线且正在加载新数据时，跳过合成K线避免重复
                return False

        if last_time is None:
            return True

        try:
            minutes = int(period.replace('min', ''))
        except (ValueError, AttributeError) as e:
            logging.error("无效周期格式：%s, 错误：%s", period, e)
            return True

        timeout = minutes * 60 * self._kline_missing_timeout_multiplier
        if (current_time - last_time) > timeout:
            logging.info("[K线兜底] 外部K线超时 %.0f秒(阈值%.0f秒)，启用合成K线写入：%s %s",
                        current_time - last_time, timeout, instrument, period)
            return True
        return False


_StorageSnapshotMixin = StorageSnapshotService
