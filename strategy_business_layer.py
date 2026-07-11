"""
StrategyCoreService 业务层 — 从strategy_core_service.py拆分
职责: 交易执行、订单/持仓服务惰性初始化、生态系统互斥、影子策略推送
"""
from __future__ import annotations

import collections
import logging
import threading
import time
from ali2026v3_trading.infra.shared_utils import generate_prefixed_id  # R9-3
from typing import Any, Dict, List, Optional

from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState


try:
    from ali2026v3_trading.config.config_service import get_param_provider
except Exception:
    def get_param_provider():
        return None


def _normalize_signal_source(signal_source: Any) -> str:
    src = str(signal_source or '').upper()
    if src in ('A', 'B', 'C'):
        return src
    if src == 'AUTO':
        return 'C'
    return 'legacy'


class StrategyBusinessLayer:
    def __init__(self, provider):
        self._provider = provider
        try:
            _pp = get_param_provider()
            _params = _pp.get_params('_default') if _pp else {}
            _src = _params.get('tl_signal_source', None)
            _src = _normalize_signal_source(_src)
            if _src != 'legacy':
                provider._signal_source = _src
            else:
                from ali2026v3_trading.config.tvf_param_loader import SIGNAL_SOURCE
                provider._signal_source = _normalize_signal_source(SIGNAL_SOURCE)
        except Exception:
            from ali2026v3_trading.config.tvf_param_loader import SIGNAL_SOURCE
            provider._signal_source = _normalize_signal_source(SIGNAL_SOURCE)

    # ========== 服务惰性初始化 ==========

    def ensure_order_service(self) -> None:
        provider = self._provider
        if provider._order_service is not None:
            return
        with provider._order_service_init_lock:
            if provider._order_service is not None:
                return
            try:
                from ali2026v3_trading.order.order_service import get_order_service
                provider._order_service = get_order_service()
                _store = getattr(provider, '_state_store', None)
                if _store:
                    _store.set_ref('_order_service', provider._order_service)
                logging.debug("[StrategyBusinessLayer] OrderService initialized (singleton)")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.warning("[StrategyBusinessLayer] Failed to initialize OrderService: %s", e)
                provider._order_service = None

    def ensure_position_service(self) -> None:
        provider = self._provider
        if provider._position_service is not None:
            return
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            from ali2026v3_trading.position.position_service import get_position_service
            scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
            risk_svc = get_risk_service(None, scope_id=scope_id)
            provider._position_service = get_position_service(risk_service=risk_svc, scope_id=scope_id)
            _store = getattr(provider, '_state_store', None)
            if _store:
                _store.set_ref('_position_service', provider._position_service)
            logging.debug("[StrategyBusinessLayer] PositionService initialized (scope=%s)", scope_id)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[StrategyBusinessLayer] Failed to initialize PositionService: %s", e)
            provider._position_service = None

    def ensure_hft_engine(self) -> None:
        provider = self._provider
        if provider._hft_engine is not None:
            return
        try:
            from ali2026v3_trading.strategy.hft_enhancements import get_hft_engine
            provider._hft_engine = get_hft_engine()
            provider._ensure_order_service()
            if provider._order_service:
                provider._order_service.enable_hft_enhancements()
            _store = getattr(provider, '_state_store', None)
            if _store:
                _store.set_ref('_hft_engine', provider._hft_engine)
            logging.debug("[StrategyBusinessLayer] HFT增强引擎已初始化(七大竞争优势-分散架构)")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[StrategyBusinessLayer] HFT增强引擎初始化失败: %s", e)
            provider._hft_engine = None

    def ensure_snapshot_collector(self, symbol: str = "") -> None:
        """初始化市场快照采集器 — 覆盖18策略(6主策略×3变体)"""
        provider = self._provider
        if provider._snapshot_collector is not None:
            return
        try:
            from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
            sym = symbol or provider.strategy_id
            provider._snapshot_collector = MarketSnapshotCollector(symbol=sym)
            # 注入到信号服务
            if provider._signal_service and hasattr(provider._signal_service, 'set_snapshot_collector'):
                provider._signal_service.set_snapshot_collector(provider._snapshot_collector)
            # 注入到订单服务
            provider._ensure_order_service()
            if provider._order_service and hasattr(provider._order_service, 'set_snapshot_collector'):
                provider._order_service.set_snapshot_collector(provider._snapshot_collector)
            logging.debug("[StrategyBusinessLayer] MarketSnapshotCollector已初始化(18策略覆盖), symbol=%s", sym)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[StrategyBusinessLayer] MarketSnapshotCollector初始化失败: %s", e)
            provider._snapshot_collector = None

    def get_snapshot_collector(self):
        """获取市场快照采集器（惰性初始化）"""
        provider = self._provider
        if provider._snapshot_collector is None:
            self.ensure_snapshot_collector()
        return provider._snapshot_collector

    # ========== 订单/成交回调 ==========

    def on_order(self, order_data: Any, *args, **kwargs) -> None:
        provider = self._provider
        try:
            if isinstance(order_data, dict):
                oid = str(order_data.get("order_id", ""))
                sts = order_data.get("status", "")
            else:
                oid = str(getattr(order_data, "order_id", ""))
                sts = getattr(order_data, "status", "")
            logging.info("[StrategyBusinessLayer.on_order] OrderID=%s, Status=%s", oid, sts)
            # FIX-R34-MEMO: PythonGO平台规范 - OrderData.memo字段包含我们发送订单时填入的内部ID(ORD_xxx)
            # 在所有其他处理之前，优先使用memo建立platform_id→internal_id映射，
            # 这样on_trade_update和后续方法都能通过oid找到正确的内部订单
            self._build_mapping_from_memo(provider, oid, order_data)
            if provider._order_service:
                provider._order_service.on_trade_update(order_data)
            self._ensure_platform_id_mapping(provider, oid, order_data)
            _is_filled = sts in ('全部成交', '部成部撤', '部分成交', '部成部撤还在队列中', '部分撤单还在队列中', 'FILLED', 'ALL_FILLED', '全成', 'PARTIAL_FILLED')
            if _is_filled:
                self._on_order_filled_trigger_position_update(order_data, oid)
            _is_failed = sts in ('已撤销', 'CANCELLED', 'FAILED', 'REJECTED', '取消', '废单', '拒绝')
            if _is_failed:
                self._reset_closing_flag_on_order_failure(oid)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StrategyBusinessLayer.on_order] Error: %s", e)

    def _build_mapping_from_memo(self, provider: Any, oid: str, order_data: Any = None) -> None:
        """FIX-R34-MEMO: 使用OrderData/TradeData的memo字段(内部ID)直接建立platform_id→internal_id映射

        PythonGO平台规范: OrderData.memo和TradeData.memo包含发送订单时填入的memo参数，
        我们将其设置为内部order_id(ORD_xxx)。通过memo可以直接找到内部订单，无需依赖
        不可靠的instrument匹配。
        """
        if not oid or not provider._order_service or not order_data:
            return
        try:
            _osvc = provider._order_service
            _oid_str = str(oid)
            # 如果映射已存在，无需重复建立
            if getattr(_osvc, '_platform_id_to_order_id', {}).get(_oid_str):
                return
            # 从回调对象提取memo字段（支持dict和object两种格式）
            if isinstance(order_data, dict):
                _memo = order_data.get('memo', '')
            else:
                _memo = getattr(order_data, 'memo', '')
            if not _memo:
                return
            _memo_str = str(_memo)
            # 只处理以ORD_开头的memo（我们的内部ID格式）
            if not _memo_str.startswith('ORD_'):
                return
            _order = _osvc.get_order(_memo_str)
            if not _order:
                return
            with _osvc._lock:
                _order['platform_order_id'] = _oid_str
                _osvc._platform_id_to_order_id[_oid_str] = _memo_str
            logging.info("[R34-MEMO-MAP] memo直接映射: platform_id=%s → internal_id=%s inst=%s action=%s",
                         _oid_str, _memo_str, _order.get('instrument_id', ''), _order.get('action', ''))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[R34-MEMO-MAP] memo映射建立失败: %s", e)

    def _ensure_platform_id_mapping(self, provider: Any, oid: str, order_data: Any = None) -> None:
        if not oid or not provider._order_service:
            return
        try:
            _osvc = provider._order_service
            _oid_str = str(oid)
            if getattr(_osvc, '_platform_id_to_order_id', {}).get(_oid_str):
                return
            _inst_from_callback = getattr(order_data, 'instrument_id', '') if order_data else ''
            _order = _osvc.get_order(oid)
            if _order:
                _existing_pid = _order.get('platform_order_id', '')
                _is_real_pid = _existing_pid and not str(_existing_pid).startswith('ORD_') and _existing_pid != _order.get('order_id', '')
                if _is_real_pid:
                    return
                with _osvc._lock:
                    _order['platform_order_id'] = _oid_str
                    _osvc._platform_id_to_order_id[_oid_str] = _order.get('order_id', oid)
                logging.info("[R28-PID-MAP] _ensure补建映射: platform_id=%s → internal_id=%s", _oid_str, _order.get('order_id', oid))
                return
            _order_by_pid = _osvc.get_order_by_platform_id(oid)
            if _order_by_pid:
                return
            with _osvc._lock:
                from datetime import datetime as _dt
                _now_ts = __import__('time').time()
                for _ioid, _o in getattr(_osvc, '_orders_by_id', {}).items():
                    _o_pid = _o.get('platform_order_id', '')
                    _is_internal_pid = (not _o_pid or str(_o_pid).startswith('ORD_') or _o_pid == _ioid)
                    if not _is_internal_pid:
                        continue
                    _o_status = _o.get('status', '')
                    # FIX-R33-CANCEL-MATCH: 允许匹配最近120秒内的CANCELLED订单，
                    # 因为平台回调返回成交时，原订单可能已被cancel_order标记为CANCELLED
                    if _o_status in ('CANCELLED', 'FAILED', 'ORPHANED', '已撤销', '部成部撤'):
                        if _o_status in ('CANCELLED', '已撤销'):
                            _o_ts = _o.get('updated_at') or _o.get('created_at')
                            _o_ts_f = 0.0
                            if isinstance(_o_ts, _dt):
                                _o_ts_f = _o_ts.timestamp()
                            else:
                                try:
                                    _o_ts_f = float(_o_ts) if _o_ts else 0.0
                                except (ValueError, TypeError):
                                    _o_ts_f = 0.0
                            if _now_ts - _o_ts_f > 120:
                                continue
                        else:
                            continue
                    if _inst_from_callback and _o.get('instrument_id') != _inst_from_callback:
                        continue
                    # FIX-R38-ACTION-EMPTY: 跳过action为空的订单，这些通常是重启恢复的旧订单，
                    # 其direction可能不正确，匹配它们会导致错单
                    if not _o.get('action', ''):
                        continue
                    _o['platform_order_id'] = _oid_str
                    _osvc._platform_id_to_order_id[_oid_str] = _ioid
                    logging.info("[R28-PID-MAP] _ensure模糊匹配补建: platform_id=%s → internal_id=%s inst=%s action=%s",
                                 _oid_str, _ioid, _o.get('instrument_id', ''), _o.get('action', ''))
                    return
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[R28-PID-MAP] 补建映射失败: %s", e)

    def _reset_closing_flag_on_order_failure(self, oid: str) -> None:
        if not oid:
            return
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            _osvc = get_order_service()
            if not _osvc:
                return
            _order = _osvc.get_order(oid)
            if not _order:
                _order = _osvc.get_order_by_platform_id(oid)
            if not _order:
                return
            _action = _order.get('action', '')
            _inst = _order.get('instrument_id', '')
            if _action != 'CLOSE':
                if not _action:
                    logging.warning("[R30-CLOSING-RESET] oid=%s action为空，无法确认订单类型，跳过_closing重置", oid)
                return
            if not _inst:
                return
            _traded_vol = _order.get('traded_volume', 0) or 0
            if _traded_vol > 0:
                logging.info("[R30-CLOSING-RESET] CLOSE订单已部分/全部成交(traded=%d)，不重置_closing: oid=%s inst=%s",
                             _traded_vol, oid, _inst)
                return
            _internal_oid = _order.get('order_id', '')
            _platform_oid = str(oid)
            from ali2026v3_trading.position.position_service import get_position_service
            _sid = str(getattr(self._provider, 'strategy_id', '') or 'global')
            _pos_svc = get_position_service(scope_id=_sid)
            if not _pos_svc:
                return
            _reset_count = 0
            with _pos_svc._get_instrument_lock(_inst):
                _pos_dict = _pos_svc.positions.get(_inst, {})
                for _rec in _pos_dict.values():
                    if not getattr(_rec, '_closing', False):
                        continue
                    _rec_closing_oid = getattr(_rec, 'closing_order_id', '')
                    _rec_pos_id = getattr(_rec, 'position_id', '')
                    # FIX-R37-UNIQUE-CLOSE(A3): 扩展匹配条件，包含所有 PENDING_ 变体前缀
                    # 原 _reset_closing_flag 只匹配 PENDING_{pos_id}，遗漏了
                    # PENDING_EMERGENCY_{pos_id} 和 PENDING_RISK_REDUCE_{pos_id}，
                    # 导致紧急平仓/熔断减仓订单失败时 _closing 标志无法重置，持仓卡死。
                    _pending_variants = (
                        f"PENDING_{_rec_pos_id}",
                        f"PENDING_EMERGENCY_{_rec_pos_id}",
                        f"PENDING_RISK_REDUCE_{_rec_pos_id}",
                    )
                    if _rec_closing_oid and (_rec_closing_oid in (_internal_oid, _platform_oid) or
                                              _rec_closing_oid in _pending_variants or
                                              _rec_closing_oid.startswith('PARTIAL_')):
                        _rec_close_method = getattr(_rec, 'close_method', '')
                        _non_reset_methods = ('risk_reduce', 'emergency_close', 'spring_straddle_abort')
                        if _rec_close_method in _non_reset_methods:
                            _rec.closing_order_id = 'CANNOT_CLOSE'
                            _reset_count += 1
                            logging.warning("[R30-CLOSING-RESET] close_method=%s不可重置_closing，设CANNOT_CLOSE: inst=%s pos_id=%s",
                                            _rec_close_method, _inst, _rec_pos_id)
                        else:
                            _rec._closing = False
                            _rec.closing_order_id = ''
                            _reset_count += 1
                            logging.debug("[R30-CLOSING-RESET] 精确匹配closing_order_id重置: inst=%s pos_id=%s closing_oid=%s",
                                          _inst, _rec_pos_id, _rec_closing_oid)
            if _reset_count > 0:
                logging.warning("[R30-CLOSING-RESET] CLOSE订单失败 oid=%s inst=%s, 精确重置%d个持仓_closing标志",
                               oid, _inst, _reset_count)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _reset_err:
            logging.debug("[R30-CLOSING-RESET] 重置_closing异常: %s", _reset_err)

    def _on_order_filled_trigger_position_update(self, order_data: Any, oid: str) -> None:
        provider = self._provider
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            _osvc = get_order_service()
            if not _osvc:
                return
            order = _osvc.get_order(oid)
            if not order:
                order = _osvc.get_order_by_platform_id(oid)
            if not order:
                order = self._find_order_by_instrument_and_action(_osvc, order_data, oid)
            if not order:
                logging.warning("[R28-ON_ORDER_FILL] oid=%s 未找到对应订单，跳过持仓更新", oid)
                return
            _action = order.get('action', '')
            _direction = order.get('direction', '')
            _instrument_id = order.get('instrument_id', '')
            _volume = order.get('volume', 0)
            _price = order.get('price', 0.0)
            _traded_volume = order.get('traded_volume', 0) or _volume
            if not _instrument_id or _traded_volume <= 0:
                return
            if not _action:
                # FIX-R37-ACTION-EMPTY: 重启恢复的旧订单action为空是预期行为，降为DEBUG避免噪音
                logging.debug("[R32-ACTION-EMPTY] oid=%s action为空，跳过on_order持仓更新，交由on_trade fallback处理", oid)
                return
            _prev_traded = order.get('_prev_traded_volume', 0)
            _incremental_volume = _traded_volume - _prev_traded
            if _incremental_volume <= 0:
                logging.debug("[R7-DEDUP] oid=%s traded_volume=%d prev=%d 无增量成交，跳过", oid, _traded_volume, _prev_traded)
                return
            order['_prev_traded_volume'] = _traded_volume
            logging.info("[R28-ON_ORDER_FILL] 订单成交触发持仓更新: oid=%s action=%s inst=%s dir=%s incremental=%d/%d price=%.2f",
                         oid, _action, _instrument_id, _direction, _incremental_volume, _traded_volume, _price)
            import types
            trade_obj = types.SimpleNamespace(
                instrument_id=_instrument_id,
                exchange=order.get('exchange', ''),
                direction='0' if _direction in ('BUY', '0') else '1',
                offset='0' if _action == 'OPEN' else ('3' if _action == 'CLOSE_TODAY' else '1'),
                price=_price,
                volume=_incremental_volume,
                order_id=oid,
                trade_id=f"ON_ORDER_FILL_{oid}_{_traded_volume}",
                # FIX-R35-MEMO: 添加memo字段(内部ID)，确保position_command_service.on_trade
                # 能通过memo直接查找订单，避免offset字段不正确导致平仓被误判为开仓
                memo=order.get('order_id', ''),
            )
            with self._provider._trade_ids_lock:
                self._provider._processed_trade_ids[f"ON_ORDER_FILL_{oid}"] = True
            self.update_position_from_trade(trade_obj)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[R28-ON_ORDER_FILL] 持仓更新失败: %s", e)

    def _find_order_by_instrument_and_action(self, _osvc: Any, order_data: Any, oid: str) -> Any:
        try:
            # FIX-R34-MEMO: 优先使用memo字段(内部ID)直接查找订单，比instrument匹配更可靠
            if order_data is not None:
                if isinstance(order_data, dict):
                    _memo = str(order_data.get('memo', '') or '')
                else:
                    _memo = str(getattr(order_data, 'memo', '') or '')
                if _memo.startswith('ORD_'):
                    _memo_order = _osvc.get_order(_memo)
                    if _memo_order:
                        _existing_pid = _memo_order.get('platform_order_id', '')
                        _need_update = (not _existing_pid
                                        or str(_existing_pid) == _memo
                                        or str(_existing_pid).startswith('ORD_'))
                        if _need_update and oid:
                            with _osvc._lock:
                                _memo_order['platform_order_id'] = str(oid)
                                _osvc._platform_id_to_order_id[str(oid)] = _memo
                        logging.info("[R34-MEMO-FIND] memo直接查找成功: memo=%s platform_id=%s inst=%s action=%s",
                                     _memo, oid, _memo_order.get('instrument_id', ''), _memo_order.get('action', ''))
                        return _memo_order

            # FIX-R33-DICT: 支持dict和object两种格式获取instrument_id
            if isinstance(order_data, dict):
                _inst = order_data.get('instrument_id', '')
            else:
                _inst = getattr(order_data, 'instrument_id', '')
            if not _inst:
                logging.warning("[R33-FIND] oid=%s order_data无instrument_id，无法匹配", oid)
                return None
            # FIX-R34-DIR-OFFSET: PythonGO平台规范 - OrderData有direction和offset字段
            # offset=0(OPEN)→匹配action=OPEN的订单; offset=1(CLOSE)→匹配action=CLOSE的订单
            # 避免CLOSE订单被错误匹配到OPEN交易，导致持仓被错误更新
            if isinstance(order_data, dict):
                _trade_offset = str(order_data.get('offset', '') or '')
                _trade_direction = str(order_data.get('direction', '') or '')
            else:
                _trade_offset = str(getattr(order_data, 'offset', '') or '')
                _trade_direction = str(getattr(order_data, 'direction', '') or '')
            _expected_action = ''
            if _trade_offset in ('0', 'OPEN', 'open', '开'):
                _expected_action = 'OPEN'
            elif _trade_offset in ('1', 'CLOSE', 'close', '平'):
                _expected_action = 'CLOSE'
            _now_dt = datetime.now(CHINA_TZ)
            _candidates = []
            _oid_str = str(oid)
            for internal_oid, o in getattr(_osvc, '_orders_by_id', {}).items():
                _status = o.get('status', '')
                _is_recent_cancelled = False
                if _status in ('CANCELLED', '已撤销'):
                    _ua = o.get('updated_at')
                    if _ua and isinstance(_ua, datetime):
                        try:
                            _is_recent_cancelled = (_now_dt - _ua).total_seconds() < 120
                        except (TypeError, ValueError):
                            _is_recent_cancelled = True
                # FIX-R32-PID-MAP: 原条件 not o.get('platform_order_id') 过于严格，
                # 当platform_order_id被设置为内部ID(如ORD_xxx)作为fallback时，无法匹配。
                # 修改为：匹配没有platform_order_id、或platform_order_id是内部ID、
                # 或platform_order_id不等于当前oid的订单
                _pid = o.get('platform_order_id', '')
                _is_internal_pid = (not _pid or _pid == internal_oid or str(_pid).startswith('ORD_'))
                if (o.get('instrument_id') == _inst
                    and _is_internal_pid
                    and (_status not in ('CANCELLED', 'FAILED', 'ORPHANED', '已撤销', '部成部撤') or _is_recent_cancelled)):
                    _candidates.append(o)
            # FIX-R34-DIR-OFFSET: 如果能从offset推断出action，优先匹配相同action的订单
            if _expected_action and _candidates:
                _filtered = [o for o in _candidates if o.get('action', '') == _expected_action]
                if _filtered:
                    _candidates = _filtered
                    logging.info("[R34-DIR-OFFSET-FIND] offset=%s→action=%s, 过滤后剩余%d个候选订单",
                                 _trade_offset, _expected_action, len(_candidates))
            if not _candidates:
                logging.warning("[R33-FIND] oid=%s inst=%s offset=%s 未找到匹配订单(含CANCELLED 120s内)",
                               oid, _inst, _trade_offset)
                return None
            from datetime import datetime as _dt
            def _safe_ts(o):
                _ts = o.get('updated_at') or o.get('created_at')
                if isinstance(_ts, _dt):
                    return _ts.timestamp()
                try:
                    return float(_ts) if _ts else 0.0
                except (ValueError, TypeError):
                    return 0.0
            _matched = max(_candidates, key=_safe_ts)
            _matched['platform_order_id'] = _oid_str
            _osvc._platform_id_to_order_id[_oid_str] = _matched.get('order_id', '')
            logging.info("[R28-PID-MAP] 补建映射+匹配订单: platform_id=%s → internal_id=%s inst=%s action=%s offset=%s",
                         _oid_str, _matched.get('order_id', ''), _inst, _matched.get('action', ''), _trade_offset)
            return _matched
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logging.debug("[R28-PID-MAP] 匹配失败: %s", e)
            return None

    def on_trade(self, trade_data: Any, *args, **kwargs) -> None:
        try:
            if isinstance(trade_data, dict):
                _tid = trade_data.get('trade_id', '') or ''
                _inst = trade_data.get('instrument_id', '') or ''
                _oid = str(trade_data.get('order_id', ''))
            else:
                _tid = getattr(trade_data, 'trade_id', '') or ''
                _inst = getattr(trade_data, 'instrument_id', '') or ''
                _oid = str(getattr(trade_data, 'order_id', ''))
            logging.info("[StrategyBusinessLayer.on_trade] trade_id=%s inst=%s", _tid, _inst)
            with self._provider._trade_ids_lock:
                if _oid and f"ON_ORDER_FILL_{_oid}" in self._provider._processed_trade_ids:
                    logging.debug("[R28-ON_TRADE-DEDUP] order_id=%s 已通过on_order处理，跳过", _oid)
                    return
            self.update_position_from_trade(trade_data)
            with self._provider._trade_ids_lock:
                if _oid and _tid in self._provider._processed_trade_ids:
                    self._provider._processed_trade_ids[f"ON_ORDER_FILL_{_oid}"] = True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StrategyBusinessLayer.on_trade] Error: %s", e)

    def update_position_from_trade(self, trade_data: Any) -> None:
        provider = self._provider
        try:
            trade_id = (trade_data.get('trade_id', None) if isinstance(trade_data, dict)
                        else getattr(trade_data, 'trade_id', None)) or str(id(trade_data))
            # FIX-UPDATE-TIMELY-09: 将_processed_trade_ids写入前移到风控检查之前，
            # 避免并发场景下两个线程同时通过dedup检查后都执行持仓更新导致重复处理
            # 如果风控阻断，再从_processed_trade_ids中移除
            with provider._trade_ids_lock:
                if trade_id in provider._processed_trade_ids:
                    return
                # 提前标记为已处理，防止并发重复进入
                provider._processed_trade_ids[trade_id] = True
                if len(provider._processed_trade_ids) > provider.MAX_PROCESSED_TRADE_IDS:
                    remove_count = len(provider._processed_trade_ids) - int(provider.MAX_PROCESSED_TRADE_IDS * 0.8)
                    for _ in range(remove_count):
                        provider._processed_trade_ids.popitem(last=False)
                # FIX-UPDATE-TIMELY-09-ASSERT: 断言trade_id已成功写入_processed_trade_ids
                assert trade_id in provider._processed_trade_ids, \
                    f"trade_id未成功写入_processed_trade_ids: {trade_id}"

            # [R28-C011修复] 先风控检查再更新持仓，防止中间状态暴露
            from ali2026v3_trading.strategy_judgment.causal_chain_utils import CyclicDependencyGuard
            _guard = CyclicDependencyGuard.get_instance()
            if not _guard.enter("position_update_from_trade"):
                logging.warning("[R28-C011] 拒绝在循环依赖上下文中更新持仓, trade_id=%s", trade_id)
                # FIX-UPDATE-TIMELY-09: 循环依赖拒绝时回滚_processed_trade_ids标记
                with provider._trade_ids_lock:
                    provider._processed_trade_ids.pop(trade_id, None)
                return
            try:
                # 步骤1: 风控前置检查（在持仓更新前）
                risk_svc = getattr(provider, '_risk_service', None)
                if risk_svc is not None and hasattr(risk_svc, 'check_before_trade'):
                    try:
                        _td = trade_data
                        _is_dict_td = isinstance(_td, dict)
                        trade_instrument = _td.get('instrument_id', '') if _is_dict_td else getattr(_td, 'instrument_id', '')
                        trade_direction = _td.get('direction', '') if _is_dict_td else getattr(_td, 'direction', '')
                        trade_volume = _td.get('volume', 0) if _is_dict_td else getattr(_td, 'volume', 0)
                        trade_price = _td.get('price', 0.0) if _is_dict_td else getattr(_td, 'price', 0.0)
                        risk_level = risk_svc.check_before_trade(
                            instrument_id=trade_instrument,
                            direction=trade_direction,
                            volume=trade_volume,
                            price=trade_price,
                        )
                        if risk_level is not None and hasattr(risk_level, 'blocked') and risk_level.blocked:
                            logging.warning("[R28-C011] 风控阻断持仓更新: trade_id=%s instrument=%s", trade_id, trade_instrument)
                            # FIX-UPDATE-TIMELY-09: 风控阻断时回滚_processed_trade_ids标记，
                            # 允许后续重试(如风控条件变化后)
                            with provider._trade_ids_lock:
                                provider._processed_trade_ids.pop(trade_id, None)
                            return
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as risk_e:
                        logging.debug("[R28-C011] 风控前置检查异常(允许继续): %s", risk_e)

                # 步骤2: 风控通过后更新持仓（_processed_trade_ids已在前面提前写入）
                from ali2026v3_trading.position.position_service import get_position_service
                scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
                pos_svc = get_position_service(scope_id=scope_id)
                pos_svc.on_trade(trade_data)
            finally:
                _guard.exit("position_update_from_trade")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            # R13-P2-API-12修复: 不再静默吞掉异常，改为warning级别记录
            logging.warning("[StrategyBusinessLayer.update_position_from_trade] 持仓更新失败: %s", e, exc_info=True)

    # ========== 交易周期 ==========

    def execute_option_trading_cycle(self) -> None:
        provider = self._provider
        if not provider._initialized:
            now_ts = time.time()
            if not hasattr(self, '_last_init_incomplete_log_ts') or now_ts - self._last_init_incomplete_log_ts >= 300:
                self._last_init_incomplete_log_ts = now_ts
                logging.info("[StrategyBusinessLayer.execute_option_trading_cycle] R24-P1-CF-02: 初始化未完成，跳过交易周期")
            return
        if not provider._is_running or provider._is_paused:
            # FIX-P0-22: 原G2守卫完全静默，导致交易周期"消失"无法排查
            # 添加DEBUG级别日志（避免INFO刷屏），记录跳过原因
            logging.debug("[OptionTrading] R24-P1-CF-02 跳过交易周期: _is_running=%s _is_paused=%s",
                          provider._is_running, provider._is_paused)
            return
        if provider._state == StrategyState.DEGRADED:
            logging.info("[OptionTrading] 策略处于DEGRADED状态，跳过交易周期")
            return
        if not provider._trading_lock.acquire(blocking=False):
            # FIX-P0-22: 原G4守卫完全静默，锁竞争时无法定位
            # 添加DEBUG日志与连续失败计数器，超过阈值告警
            if not hasattr(self, '_trading_lock_fail_count'):
                self._trading_lock_fail_count = 0
            self._trading_lock_fail_count += 1
            logging.debug("[OptionTrading] R24-P1-CF-02 跳过交易周期: _trading_lock获取失败(连续第%d次)",
                          self._trading_lock_fail_count)
            if self._trading_lock_fail_count >= 10:
                logging.warning("[OptionTrading] _trading_lock连续%d次获取失败，可能存在锁泄漏",
                                self._trading_lock_fail_count)
            return
        # FIX-P0-22: 锁获取成功后重置失败计数器
        if hasattr(self, '_trading_lock_fail_count'):
            self._trading_lock_fail_count = 0
        try:
            # R24-P2-AT-01修复: 添加交易周期耗时记录
            _cycle_start = time.time()
            t_type = getattr(provider, 't_type_service', None)
            if not t_type:
                # FIX-P0-22: 原G5守卫完全静默
                logging.debug("[OptionTrading] R24-P1-CF-02 跳过交易周期: t_type_service=None")
                return
            signal_source = _normalize_signal_source(getattr(provider, '_signal_source', 'legacy'))
            if signal_source in ('A', 'B', 'C'):
                targets = t_type.select_otm_targets_signal_sources(signal_source=signal_source)
            else:
                targets = t_type.select_otm_targets_by_volume()
            if not targets:
                # FIX-R6: targets为空时输出节流warning日志，避免完全静默
                # 每5分钟输出一次，包含诊断信息
                if not hasattr(provider, '_empty_targets_last_log'):
                    provider._empty_targets_last_log = 0.0
                _now = time.time()
                if _now - provider._empty_targets_last_log >= 300:
                    provider._empty_targets_last_log = _now
                    try:
                        _wc = getattr(t_type, '_width_cache', None) or getattr(t_type, 'width_cache', None)
                        _tier_info = {}
                        _init_count = 0
                        if _wc:
                            _init_count = sum(1 for v in getattr(_wc, '_future_initialized', {}).values() if v)
                            _sc = getattr(_wc, '_status_counts', {})
                            _fr_map = getattr(_wc, '_future_rising', {})
                            _tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
                            for fid, md in _sc.items():
                                try:
                                    # FIX-P1-16: 原代码_months = list(md.values())数据结构错误
                                    # md结构: Dict[month, Dict[opt_type, Dict[status, int]]]
                                    # compute_correct_up_pct期望List[Dict]每个元素含'cr'/'wr'/'cf'/'wf'键
                                    # 参考width_cache_types.py:421 _build_future_score_rows的正确实现
                                    _opt_type = 'CALL' if _fr_map.get(fid, False) else 'PUT'
                                    _raw_months = list(md.keys()) if isinstance(md, dict) else []
                                    _months = _wc._get_scoring_months(fid, _raw_months) if hasattr(_wc, '_get_scoring_months') else _raw_months
                                    _month_list = []
                                    for _mth in _months:
                                        _counts = md.get(_mth, {}).get(_opt_type, {})
                                        _cr = _counts.get('correct_rise', 0)
                                        _wr = _counts.get('wrong_rise', 0)
                                        _cf = _counts.get('correct_fall', 0)
                                        _wf = _counts.get('wrong_fall', 0)
                                        _other = _counts.get('other', 0)
                                        _total = _cr + _wr + _cf + _wf + _other
                                        _month_list.append({'cr': _cr, 'wr': _wr, 'cf': _cf, 'wf': _wf, 'other': _other, 'total': _total})
                                    # FIX-P1-17: 原代码调用_compute_month_weights方法不存在，正确方法名为resolve_month_weights
                                    _w = _wc.resolve_month_weights(len(_month_list)) if hasattr(_wc, 'resolve_month_weights') else tuple(1.0 for _ in _month_list)
                                    _cup = _wc.compute_correct_up_pct(_month_list, _w) if _month_list else 0.0
                                    _cov = _wc.compute_coverage(_month_list, _w) if _month_list else 0.0
                                    _wilson = 0.0
                                    _nr = _wc.compute_noise_ratio(_month_list, _w) if _month_list else 0.0
                                    _t = _wc.determine_tier(_cov, _wilson, _cup, _nr)
                                    _tier_counts[_t] = _tier_counts.get(_t, 0) + 1
                                except Exception:
                                    pass
                            _tier_info = _tier_counts
                        logging.warning(
                            "[OptionTrading] R24-P1-CF-02: targets为空，跳过交易周期 "
                            "(initialized_futures=%d, tier_counts=%s, signal_source=%s)",
                            _init_count, _tier_info, signal_source
                        )
                    except Exception as _diag_e:
                        logging.warning("[OptionTrading] targets为空（诊断失败: %s）", _diag_e)
                return
            open_reason = provider._resolve_open_reason()
            for t in targets:
                t['open_reason'] = t.get('open_reason', '') or open_reason
            # FIX-P0-1: 在targets过滤阶段排除trading=False合约，根因修复重复下单失败
            # 证据: 2小时内15条下单失败ERROR+15条TRADING-BLOCK+109条幂等拦截+161条订单延迟
            # 根因: trading=False的合约未被过滤，每30秒交易周期重复提交
            _trading_blocked_instruments = set()
            try:
                _order_svc = getattr(provider, '_order_service', None)
                if _order_svc and hasattr(_order_svc, '_platform_insert_order'):
                    _platform_self = getattr(_order_svc._platform_insert_order, '__self__', None)
                    if _platform_self is not None and getattr(_platform_self, 'trading', True) is False:
                        _trading_blocked_instruments.add('__GLOBAL_TRADING_FALSE__')
            except (ValueError, KeyError, TypeError, AttributeError):
                pass
            if _trading_blocked_instruments:
                logging.warning("[FIX-P0-1] 检测到全局trading=False，跳过本次交易周期(避免重复下单失败)")
                return
            # FIX-P0-1-2: 逐合约检查trading状态，过滤trading=False的合约
            _pre_filter_count = len(targets)
            _filtered_targets = []
            for t in targets:
                _inst = t.get('instrument_id', '')
                _action = t.get('action', '') or 'OPEN'
                if _action in ('OPEN', 'open') and _inst:
                    try:
                        _order_svc = getattr(provider, '_order_service', None)
                        if _order_svc and hasattr(_order_svc, '_consecutive_failures'):
                            _consec = getattr(_order_svc, '_consecutive_failures', 0)
                            _threshold = getattr(_order_svc, '_circuit_breaker_threshold', 5)
                            if _consec >= _threshold:
                                _filtered_targets.append(t)
                                continue
                        _recent_fail_key = f'_p01_recent_fail_{_inst}_{_action}'
                        _recent_fail_ts = getattr(provider, _recent_fail_key, 0)
                        if _recent_fail_ts and (time.time() - _recent_fail_ts) < 300:
                            continue
                    except (ValueError, KeyError, TypeError, AttributeError):
                        pass
                _filtered_targets.append(t)
            if _filtered_targets:
                targets = _filtered_targets
            if _pre_filter_count > len(targets):
                logging.info("[FIX-P0-1] 过滤trading=False/近期失败合约: %d→%d (排除%d)",
                             _pre_filter_count, len(targets), _pre_filter_count - len(targets))
            if not provider._check_ecosystem_exclusion(targets):
                logging.info("[OptionTrading] 互斥规则或跨策略风控阻断, 跳过本次周期")
                return
            # ✅ P0-5修复: 交易前风控检查
            # R13-TYP-02修复: signal dict必须包含action字段，否则断路器平仓豁免永远不触发
            # R13-P1-BIZ-06修复: signal_id链路贯通 — signal→risk_check→order→position
            try:
                from ali2026v3_trading.risk.risk_service import get_risk_service
                scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
                rs = get_risk_service(None, scope_id=scope_id)
                passed_targets = []
                for t in targets:
                    # P1-52修复: 使用generate_prefixed_id统一ID生成
                    from ali2026v3_trading.infra.shared_utils import generate_prefixed_id
                    _sig_id = t.get('signal_id', '') or generate_prefixed_id('SIG', 12)
                    signal = {
                        'symbol': t.get('instrument_id', ''),
                        'direction': t.get('direction', 'BUY'),
                        'price': t.get('price', 0.0),
                        # CHAIN-BUG-1 fix: 风控按下单手数(lots)评估，而非市场成交量(volume)
                        'volume': t.get('lots', 1),
                        'is_valid': True,
                        # R13-P0-API-03修复: action字段默认为OPEN，断路器平仓豁免逻辑依赖此字段
                        'action': t.get('action', '') or 'OPEN',
                        'account_id': t.get('account_id', 'default'),
                        'amount': t.get('amount', 0),
                        'signal_id': _sig_id,  # R13-P1-BIZ-06修复: signal_id贯穿全链路
                    }
                    t['signal_id'] = _sig_id  # R13-P1-BIZ-06修复: 回写到target供后续order使用
                    check_result = rs.check_before_trade(signal)
                    if check_result.is_block:
                        logging.warning("[OptionTrading] 风控阻断: %s %s reason=%s type=%s result=%s message=%s",
                                        t.get('instrument_id', ''), t.get('direction', ''), check_result.reason,
                                        type(check_result).__name__, check_result.result, getattr(check_result, 'message', ''))
                        continue
                    passed_targets.append(t)
                if not passed_targets:
                    logging.info("[OptionTrading] 所有target均被风控阻断，跳过本次周期")
                    return
                targets = passed_targets
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                # R10-P0-11修复: 风控检查异常时fail-safe阻断交易，而非"继续交易"
                # 与P0-R10-10叠加修复：安全元层异常→阻断，外层也必须阻断
                # R10-P2-02: 关键异常日志添加exc_info=True
                logging.error(
                    "[R10-P0-11] 风控检查异常，fail-safe阻断交易: %s", e,
                    exc_info=True,
                )
                return
            # P-30/R7-M-15修复: 健康检查CRITICAL时暂停新开仓（手册12.1节）
            # 双重检查: (1) _health_pause_new_open标志 (2) 实时health查询
            if provider._health_pause_new_open:
                _pause_ts = getattr(provider, '_health_pause_new_open_ts', 0) or 0
                _pause_elapsed = time.time() - _pause_ts
                if _pause_elapsed > 300:
                    provider._health_pause_new_open = False
                    provider._health_pause_new_open_ts = 0
                    logging.warning("[OptionTrading] _health_pause_new_open超时自动恢复(暂停%.0fs)", _pause_elapsed)
                else:
                    logging.warning("[OptionTrading] _health_pause_new_open=True(健康异常, 已暂停%.0fs/300s), 暂停新开仓", _pause_elapsed)
                    return
            try:
                health = provider.get_health_status()
                if health.get('health') in ('CRITICAL', 'DEGRADED'):
                    provider._health_pause_new_open = True
                    provider._health_pause_new_open_ts = time.time()
                    logging.warning("[OptionTrading] 系统健康状态=%s, 暂停新开仓",
                                    health.get('health'))
                    return
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[OptionTrading] 健康检查异常: %s", e)
            provider._ensure_order_service()
            if provider._order_service:
                order_ids = provider._order_service.execute_by_ranking(targets)
                if order_ids:
                    logging.info("[OptionTrading] 下单完成: %d 笔 reason=%s", len(order_ids), open_reason)
            provider._feed_shadow_engine(targets, open_reason)
            # R14-P1-DEAD-08修复: 在决策循环中调用governance checker，手册14节治理检测
            try:
                from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
                _eco = get_strategy_ecosystem()
                if _eco and hasattr(_eco, 'run_governance_checks'):
                    _gov_result = _eco.run_governance_checks()
                    if _gov_result.get('any_triggered', False):
                        logging.warning("[OptionTrading] R14-P1-DEAD-08: 治理检测触发降级: %s",
                                        _gov_result.get('degradation_reasons', []))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _gov_err:
                logging.warning("[R22-EP-P1] governance check跳过(合规检查可能放行违规交易): %s", _gov_err)
            # ✅ P1-9修复: 集成box_spring_strategy的箱体弹簧扫描
            # R13-API-01修复: 传递完整tick参数(high/low/volume/timestamp)，否则箱体检测失效
            # R15-P1-PERF-02修复: 使用全局单例get_box_spring_strategy()，避免绕过单例创建多实例
            try:
                from ali2026v3_trading.strategy.box_spring_strategy_impl import get_box_spring_strategy
                bss = get_box_spring_strategy()
            except ImportError as _bss_ie:
                logging.warning("[P1-FIX] box_spring_strategy导入失败, 箱体弹簧扫描不可用: %s", _bss_ie)
                bss = None
            try:  # R23-P0-FIX: 补充缺失的try块，修复SyntaxError
                if bss is not None:
                    for t in targets:
                        inst_id = t.get('instrument_id', '')
                        if inst_id:
                            # R13-API-01修复: 传递完整tick参数，箱体检测依赖high>0 and low>0条件
                            bss.on_tick(
                                instrument_id=inst_id,
                                price=t.get('price', 0.0),
                                high=t.get('high', 0.0),  # R13-API-01: 补充high参数
                                low=t.get('low', 0.0),    # R13-API-01: 补充low参数
                                volume=t.get('volume', 0),  # R13-API-01: 补充volume参数
                                timestamp=t.get('timestamp'),  # R13-API-01: 补充timestamp参数
                            )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[R22-EP-P1-17] [StrategyBusinessLayer] box_spring_strategy tick error: %s", e)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            # R10-P2-02: 关键异常日志添加exc_info=True
            logging.error("[StrategyBusinessLayer.execute_option_trading_cycle] Error: %s", e, exc_info=True)
        finally:
            # R24-P2-AT-01修复: 交易周期耗时记录
            _cycle_elapsed = time.time() - _cycle_start if '_cycle_start' in dir() else -1
            if _cycle_elapsed > 1.0:
                logging.warning("[R24-P2-AT-01] 交易周期耗时过长: %.2fs", _cycle_elapsed)
            provider._trading_lock.release()

    def _trading_cycle(self) -> None:
        self.execute_option_trading_cycle()

    # ========== 生态系统互斥 ==========

    def _check_ecosystem_exclusion(self, targets: list) -> bool:
        provider = self._provider
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            spm = getattr(provider, '_state_param_manager', None)
            current_state = spm.get_current_state() if spm else 'other'
            # FIX-R23: 直接使用硬编码映射，避免eco.__getattr__抛出AttributeError阻断交易
            _state_map = {
                'correct_trending': 'master',
                'correct_trending_defensive': 'master',
                'incorrect_reversal': 'divergence',
                'incorrect_reversal_defensive': 'divergence',
                'other': 'other',
                'spring': 'spring',
                'arbitrage': 'arbitrage',
                'market_making': 'market_making',
            }
            strategy_id = _state_map.get(current_state, 'master')
            # FIX-R23: 直接读取_active_strategy属性，避免check_mutual_exclusion被__getattr__拦截
            _active_strategy = getattr(eco, '_active_strategy', 'master')
            if _active_strategy == 'other' and strategy_id in ('master', 'divergence'):
                for t in targets:
                    direction = t.get('direction', 'BUY')
                    logging.warning("[OptionTrading] 互斥阻断: strategy=%s dir=%s reason=%s blocked in other state",
                                  strategy_id, direction, strategy_id)
                    return False
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.error("[R16-P1-4.2] StrategyBusinessLayer._check_ecosystem_exclusion Error: %s, 临时暂停新开仓300秒", e)
            provider._health_pause_new_open = True
            provider._health_pause_new_open_ts = time.time()
            return False

    # ========== 影子策略引擎 ==========

    def _feed_shadow_engine(self, targets: list, open_reason: str) -> None:
        """V7新增：将交易信号批量推送到影子策略监控引擎(优化：单次锁获取)"""
        provider = self._provider
        try:
            se = getattr(provider, '_shadow_engine', None)
            if se is None:
                from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine
                se = get_shadow_strategy_engine()
                provider._shadow_engine = se
            if hasattr(se, 'are_params_locked') and not se.are_params_locked():
                logging.warning("[R16-P1-15] 影子策略参数未锁定，使用未验证参数")
            spm = getattr(provider, '_state_param_manager', None)
            market_state = spm.get_current_state() if spm else 'unknown'
            # R13-API-05: Determine strategy_group from state_param_manager
            strategy_group = "s2_resonance"
            try:
                _STRATEGY_ID_TO_SHADOW_GROUP = {
                    'master': 's2_resonance', 'reverse': 's2_resonance', 'other': 's2_resonance',
                    'hft': 's1_hft', 'box': 's3_box', 'spring': 's4_spring',
                    'arbitrage': 's5_arbitrage', 'market_making': 's6_market_making',
                }
                # FIX-R23: 直接使用硬编码映射，避免eco.__getattr__抛出AttributeError
                _state_map = {
                    'correct_trending': 'master',
                    'correct_trending_defensive': 'master',
                    'incorrect_reversal': 'divergence',
                    'incorrect_reversal_defensive': 'divergence',
                    'other': 'other',
                    'spring': 'spring',
                    'arbitrage': 'arbitrage',
                    'market_making': 'market_making',
                }
                _sid = _state_map.get(market_state, 'master')
                strategy_group = _STRATEGY_ID_TO_SHADOW_GROUP.get(_sid, 's2_resonance')
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.warning("[R22-EP-P1] StrategyBusinessLayer exception swallowed")
                pass
            for t in targets:
                se.process_signal(
                    market_state=market_state,
                    instrument_id=t.get('instrument_id', ''),
                    signal_direction=t.get('direction', ''),
                    price=t.get('price', 0.0),
                    quantity=t.get('volume', 0),
                    signal_strength=t.get('signal_strength', 0.0),
                    strategy_group=strategy_group,  # R13-API-05: pass strategy_group
                )
            if not se.is_shadow_mode():
                logging.warning("[StrategyBusinessLayer] 影子策略隔离性验证失败")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[StrategyBusinessLayer._feed_shadow_engine] Error: %s", e)
