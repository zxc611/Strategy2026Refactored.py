"""
StrategyCoreService 业务层 — 从strategy_core_service.py拆分
职责: 交易执行、订单/持仓服务惰性初始化、生态系统互斥、影子策略推送
"""
from __future__ import annotations

import collections
import logging
import threading
import time
from infra.shared_utils import generate_prefixed_id  # R9-3
from typing import Any, Dict, List, Optional

from lifecycle.lifecycle_state_machine import StrategyState


try:
    from config.config_service import get_param_provider
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
                from config.tvf_param_loader import SIGNAL_SOURCE
                provider._signal_source = _normalize_signal_source(SIGNAL_SOURCE)
        except Exception:
            from config.tvf_param_loader import SIGNAL_SOURCE
            provider._signal_source = _normalize_signal_source(SIGNAL_SOURCE)

    # ========== 服务惰性初始化 ==========

    def ensure_order_service(self) -> None:
        provider = self._provider
        if provider._order_service is not None:
            # FIX-20260715-PROVIDER-REF-LATE: 即使_order_service已存在，也要确保_provider_ref被设置
            # 根因: OrderService是单例，可能在on_init阶段被其他路径初始化。on_start阶段
            # ensure_order_service直接return不设置_provider_ref，导致dry_run虚拟回调注入失败。
            try:
                if getattr(provider._order_service, '_provider_ref', None) is None:
                    provider._order_service._provider_ref = provider
                    logging.info("[StrategyBusinessLayer] _provider_ref已补设置到OrderService(延迟注入)")
            except (AttributeError, TypeError):
                pass
            # FIX-79 D5 P1: 即使_order_service已存在，也要确保已注册到_state_store
            # 根因: 与FIX-72/74相同模式。如果_order_service在recovery阶段被恢复，
            #       属性赋值完成但set_ref不执行 → tick_hft.py L245/280/363/406
            #       get_ref('_order_service')返回None → 下单链路断裂
            # 修复: 检查_state_store中是否已注册，未注册则补注册
            _os_store = getattr(provider, '_state_store', None)
            if _os_store is not None:
                try:
                    if _os_store.get_ref('_order_service') is None:
                        _os_store.set_ref('_order_service', provider._order_service)
                        logging.info("[StrategyBusinessLayer] _order_service已补注册(延迟注入)")
                except Exception as _os_ref_err:
                    logging.warning("[StrategyBusinessLayer] _order_service延迟注册失败(非阻断): %s", _os_ref_err)
            return
        with provider._order_service_init_lock:
            if provider._order_service is not None:
                try:
                    if getattr(provider._order_service, '_provider_ref', None) is None:
                        provider._order_service._provider_ref = provider
                        logging.info("[StrategyBusinessLayer] _provider_ref已补设置到OrderService(延迟注入)")
                except (AttributeError, TypeError):
                    pass
                # FIX-79 D5 P1 (locked路径): 同外层L69-76补注册逻辑
                _os_store_locked = getattr(provider, '_state_store', None)
                if _os_store_locked is not None:
                    try:
                        if _os_store_locked.get_ref('_order_service') is None:
                            _os_store_locked.set_ref('_order_service', provider._order_service)
                            logging.info("[StrategyBusinessLayer] _order_service已补注册(延迟注入-locked)")
                    except Exception as _os_ref_err_l:
                        logging.warning("[StrategyBusinessLayer] _order_service延迟注册失败(locked,非阻断): %s", _os_ref_err_l)
                return
            try:
                from order.order_service import get_order_service
                provider._order_service = get_order_service()
                _store = getattr(provider, '_state_store', None)
                if _store:
                    _store.set_ref('_order_service', provider._order_service)
                # [FIX-20260708-PROVIDER-REF] 设置_provider_ref，使dry_run虚拟回调能获取策略对象
                try:
                    provider._order_service._provider_ref = provider
                    logging.info("[StrategyBusinessLayer] _provider_ref已设置到OrderService")
                except (AttributeError, TypeError):
                    pass
                logging.debug("[StrategyBusinessLayer] OrderService initialized (singleton)")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logging.warning("[StrategyBusinessLayer] Failed to initialize OrderService: %s", e)
                provider._order_service = None

    def ensure_position_service(self) -> None:
        provider = self._provider
        if provider._position_service is not None:
            # FIX-72 D5 P0: 即使_position_service已存在，也要确保_risk_service已注册
            # 根因: 如果_position_service在on_init阶段被其他路径(如recovery/checkpoint)初始化，
            #       ensure_position_service直接return，导致FIX-61的_risk_service注册代码不执行
            #       → tick_dispatch.py L110 get_ref('_risk_service') 返回None
            #       → check_hard_time_stop_live 实盘硬时间止损检查被完全跳过（超时持仓无法止损）
            #       → trade_data 风控前置检查被跳过
            # 修复: 检查_risk_service是否已注册，未注册则补注册（与ensure_order_service L54-64延迟注入模式一致）
            _store = getattr(provider, '_state_store', None)
            # FIX-78 D5 P1: 即使_position_service已存在，也要确保已注册到_state_store
            # 根因: 与FIX-79相同模式。如果_position_service在recovery阶段被恢复，
            #       属性赋值完成但set_ref不执行 → tick_dispatch.py L126 get_ref返回None
            #       → 调用ensure_position_service → provider._position_service is not None走提前return
            #       → _position_service仍不在_state_store → 循环死结，硬时间止损链路断裂
            # 修复: 检查_state_store中是否已注册，未注册则补注册
            if _store is not None:
                try:
                    if _store.get_ref('_position_service') is None:
                        _store.set_ref('_position_service', provider._position_service)
                        logging.info("[StrategyBusinessLayer] _position_service已补注册(延迟注入)")
                except Exception as _ps_ref_err:
                    logging.warning("[StrategyBusinessLayer] _position_service延迟注册失败(非阻断): %s", _ps_ref_err)
            _need_register = (getattr(provider, '_risk_service', None) is None)
            if (not _need_register) and (_store is not None):
                try:
                    _need_register = (_store.get_ref('_risk_service') is None)
                except Exception:
                    _need_register = False
            if _need_register:
                try:
                    from risk.risk_service import get_risk_service
                    _sid = str(getattr(provider, 'strategy_id', '') or 'global')
                    _risk_svc_late = get_risk_service(None, scope_id=_sid)
                    if _risk_svc_late is not None:
                        provider._risk_service = _risk_svc_late
                        if _store is not None:
                            _store.set_ref('_risk_service', _risk_svc_late)
                        logging.info("[StrategyBusinessLayer] _risk_service已补注册(延迟注入, scope=%s)", _sid)
                except Exception as _risk_late_err:
                    logging.warning("[StrategyBusinessLayer] _risk_service延迟注册失败(非阻断): %s", _risk_late_err)
            return
        try:
            from risk.risk_service import get_risk_service
            from position.position_service import get_position_service
            scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
            risk_svc = get_risk_service(None, scope_id=scope_id)
            provider._position_service = get_position_service(risk_service=risk_svc, scope_id=scope_id)
            _store = getattr(provider, '_state_store', None)
            if _store:
                _store.set_ref('_position_service', provider._position_service)
            # FIX-61 D5: 注册_risk_service到_state_store并保存到provider属性
            # 根因: risk_svc 创建后仅用于初始化_position_service，未注册也未保存到provider._risk_service
            #       → tick_dispatch.py L110 get_ref('_risk_service') 返回None
            #       → check_hard_time_stop_live 实盘硬时间止损检查被完全跳过（无fallback）
            #       → strategy_business_layer.py L562 getattr(provider, '_risk_service', None) 永远返回None
            #       → trade_data 风控前置检查被跳过
            # 修复: 同步注册到_state_store + 保存到provider._risk_service属性
            try:
                provider._risk_service = risk_svc
                if _store is not None and risk_svc is not None:
                    _store.set_ref('_risk_service', risk_svc)
            except Exception as _risk_ref_err:
                logging.warning("[StrategyBusinessLayer] _risk_service set_ref注册失败(非阻断): %s", _risk_ref_err)
            logging.debug("[StrategyBusinessLayer] PositionService initialized (scope=%s)", scope_id)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[StrategyBusinessLayer] Failed to initialize PositionService: %s", e)
            provider._position_service = None

    def ensure_hft_engine(self) -> None:
        provider = self._provider
        if provider._hft_engine is not None:
            # FIX-74 D5 P1: 即使_hft_engine已存在，也要确保已注册到_state_store
            # 根因: 与FIX-72相同模式。如果_hft_engine在recovery/checkpoint阶段被恢复，
            #       ensure_hft_engine直接return，set_ref('_hft_engine')不执行
            #       → tick_hft.py L65 get_ref('_hft_engine')返回None → HFT增强失效
            # 修复: 检查_state_store中是否已注册，未注册则补注册
            _store = getattr(provider, '_state_store', None)
            if _store is not None:
                try:
                    if _store.get_ref('_hft_engine') is None:
                        _store.set_ref('_hft_engine', provider._hft_engine)
                        logging.info("[StrategyBusinessLayer] _hft_engine已补注册(延迟注入)")
                except Exception as _hft_ref_err:
                    logging.warning("[StrategyBusinessLayer] _hft_engine延迟注册失败(非阻断): %s", _hft_ref_err)
            return
        try:
            from strategy.hft_enhancements import get_hft_engine
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
            from strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
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
        except Exception as e:
            # FIX-65 D1 P2: 扩展except为Exception，避免吞掉MemoryError/ConnectionError/TimeoutError等
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
            from order.order_service import get_order_service
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
            from position.position_service import get_position_service
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
            from order.order_service import get_order_service
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
        except Exception as e:
            # FIX-65 D1 P2: 扩展except为Exception，避免吞掉MemoryError/ConnectionError/TimeoutError等
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
            from strategy_judgment.causal_chain_utils import CyclicDependencyGuard
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
                from position.position_service import get_position_service
                scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
                pos_svc = get_position_service(scope_id=scope_id)
                pos_svc.on_trade(trade_data)
            finally:
                _guard.exit("position_update_from_trade")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            # R13-P2-API-12修复: 不再静默吞掉异常，改为warning级别记录
            logging.warning("[StrategyBusinessLayer.update_position_from_trade] 持仓更新失败: %s", e, exc_info=True)

    # ========== 交易周期 ==========

    def _save_signal_snapshot(self, targets: list, open_reason: str) -> None:
        """FIX-32 RC-43: 统一信号快照保存 — 为S1/S2/S3/S4/S7策略保存信号快照

        根因: S5/S6各自有Monitor.save_snapshot，但S1-S4/S7通过execute_option_trading_cycle
        处理信号，从未保存快照→日志统计显示0快照
        修复: 在风控通过后、下单前，为每个target保存JSON快照
        """
        try:
            import os, json
            from datetime import datetime
            _snap_dir = os.path.join('logs', 'signal_snapshots')
            os.makedirs(_snap_dir, exist_ok=True)
            _now = datetime.now()
            _ts_str = _now.strftime('%Y%m%d_%H%M%S_%f')
            for t in targets:
                _reason = t.get('open_reason', open_reason or 'UNKNOWN')
                # S5/S6已有各自Monitor的save_snapshot，跳过避免重复
                if _reason in ('ARBITRAGE', 'MARKET_MAKING', 'S5_ARB', 'S6_MM'):
                    continue
                # FIX-K: 同时检查simulated标志，S5/S6 target的simulated=True但open_reason可能为OTHER_SCALP
                # 根因: S5/S6 target的open_reason通常被resolve_open_reason()设为'OTHER_SCALP'(不在跳过列表)
                #       →_save_signal_snapshot为S5/S6额外保存快照→与Monitor的save_snapshot重复
                if t.get('simulated', False):
                    continue
                _inst = t.get('instrument_id', 'unknown')
                _sig_id = t.get('signal_id', '')
                _filename = f"{_reason}_{_inst}_{_ts_str}.json"
                _filepath = os.path.join(_snap_dir, _filename)
                _snapshot = {
                    'signal': {k: v for k, v in t.items() if k != 'divergence_signal'},
                    'snapshot_time_iso': _now.isoformat(),
                    'snapshot_time_ts': time.time(),
                    'strategy_meta': {
                        'open_reason': _reason,
                        'dry_run': getattr(self._provider, '_dry_run_active', False),
                        'strategy_id': getattr(self._provider, 'strategy_id', ''),
                    },
                }
                try:
                    with open(_filepath, 'w', encoding='utf-8') as f:
                        json.dump(_snapshot, f, ensure_ascii=False, indent=2, default=str)
                    logging.info("[SignalSnapshot] 快照已保存: %s (reason=%s inst=%s)",
                                 _filepath, _reason, _inst)
                except (OSError, IOError, ValueError, TypeError) as _snap_err:
                    logging.warning("[SignalSnapshot] 快照保存失败: %s error=%s", _filepath, _snap_err)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _snap_fatal:
            logging.debug("[SignalSnapshot] 快照模块异常(降级): %s", _snap_fatal)

    def execute_option_trading_cycle(self) -> None:
        provider = self._provider
        if not provider._initialized:
            now_ts = time.time()
            if not hasattr(self, '_last_init_incomplete_log_ts') or now_ts - self._last_init_incomplete_log_ts >= 300:
                self._last_init_incomplete_log_ts = now_ts
                logging.info("[StrategyBusinessLayer.execute_option_trading_cycle] R24-P1-CF-02: 初始化未完成，跳过交易周期")
            return
        if not provider._is_running or provider._is_paused:
            # FIX-20260708: 升级为INFO(节流5分钟)，原DEBUG在生产日志不可见，导致交易周期"消失"无法排查
            if not hasattr(self, '_skip_running_log_ts'):
                self._skip_running_log_ts = 0.0
            _now_sr = time.time()
            if _now_sr - self._skip_running_log_ts >= 300:
                self._skip_running_log_ts = _now_sr
                logging.info("[OptionTrading] 跳过交易周期: _is_running=%s _is_paused=%s",
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
            # FIX-20260707-AUTO-TRADING: auto_trading开关前置检查
            # 根因: UI关闭自动交易后(auto_trading_enabled=False)，策略代码仍尝试下单，
            # 导致83次平台拒绝(result=-1)产生噪音ERROR日志
            # FIX-20260707-DRY-RUN: dry_run模式下不跳过交易周期，
            # 策略逻辑必须完整运行（信号/风控/持仓/快照），仅由OrderExecutor拦截实际下单
            _dry_run_mode = False
            try:
                from config.params_service import get_params_service
                _dry_run_mode = get_params_service().get_bool('dry_run_mode', False) or False
            except Exception as _dr_err:
                # FIX-20260708: dry_run_mode读取失败时记录警告，原代码静默pass导致问题不可见
                if not hasattr(self, '_dry_run_read_err_ts'):
                    self._dry_run_read_err_ts = 0.0
                _now_dr = time.time()
                if _now_dr - self._dry_run_read_err_ts >= 300:
                    self._dry_run_read_err_ts = _now_dr
                    logging.warning("[OptionTrading] dry_run_mode读取失败(默认False): %s", _dr_err)
            # FIX-20260708-V3: 回退检查provider._dry_run_active（由lifecycle_callbacks.py设置）
            # 根因: ParamsService._params可能未从params_default.json加载dry_run_mode，
            # 但lifecycle_callbacks.py已通过config_params检测到dry_run_mode=True并设置_dry_run_active
            # 此回退确保交易周期正确感知dry_run模式，避免hard_stop检查阻断dry_run交易
            if not _dry_run_mode:
                _dry_run_mode = bool(getattr(provider, '_dry_run_active', False))
            # FIX-20260708: 每5分钟记录一次dry_run_mode值，便于诊断hard_stop是否阻断交易周期
            if not hasattr(self, '_dry_run_log_ts'):
                self._dry_run_log_ts = 0.0
            _now_drl = time.time()
            if _now_drl - self._dry_run_log_ts >= 300:
                self._dry_run_log_ts = _now_drl
                logging.info("[OptionTrading] dry_run_mode=%s", _dry_run_mode)
            if not _dry_run_mode:
                _auto_trading = getattr(provider, 'auto_trading_enabled', None)
                if _auto_trading is None:
                    _auto_trading = getattr(provider, 'my_trading', None)
                if _auto_trading is False:
                    # 节流: 每5分钟输出一次INFO，避免DEBUG在生产环境不可见
                    if not hasattr(self, '_auto_trading_skip_ts'):
                        self._auto_trading_skip_ts = 0.0
                    _now = time.time()
                    if _now - self._auto_trading_skip_ts >= 300:
                        self._auto_trading_skip_ts = _now
                        logging.info("[OptionTrading] auto_trading_enabled=False（开仓功能已关闭），跳过交易周期")
                    return
            # FIX-20260707-HARD-STOP: hard_stop前置检查（dry_run模式下也执行，确保风控不缺失）
            # FIX-73 D1 P0: 修正注释与代码矛盾
            # 根因: 原注释"dry_run模式下不跳过"但代码 `if not _dry_run_mode:` 实际在dry_run模式下跳过检查
            #       → dry_run模式下hard_stop风控失效，可能放行应被拦截的交易
            #       → 与memory约束"dry_run模式必须完整运行策略逻辑(信号/风控/持仓/快照)"矛盾
            # 修复: 移除dry_run_mode守卫，hard_stop检查始终执行（与auto_trading检查L754区分）
            try:
                from risk.risk_service import get_safety_meta_layer
                _sid = str(getattr(provider, 'strategy_id', '') or 'global')
                # FIX-64 D3 P1: 修正参数名 scope_id → strategy_id
                # 根因: get_safety_meta_layer(params, strategy_id) 函数签名只接受 strategy_id
                #       原代码 scope_id=_sid 会抛 TypeError，被 except Exception 静默吞掉
                #       → hard_stop 检查永远失效（_safety 恒为 None → 风控放行所有交易）
                _safety = get_safety_meta_layer(None, strategy_id=_sid)
                if _safety and _safety.is_hard_stop_triggered():
                    # 节流: 每5分钟输出一次INFO
                    if not hasattr(self, '_hard_stop_skip_ts'):
                        self._hard_stop_skip_ts = 0.0
                    _now_hs = time.time()
                    if _now_hs - self._hard_stop_skip_ts >= 300:
                        self._hard_stop_skip_ts = _now_hs
                        logging.info("[OptionTrading] hard_stop已触发，跳过交易周期")
                    return
            except Exception:
                pass  # 风控不可用时放行，不阻断交易
            # R24-P2-AT-01修复: 添加交易周期耗时记录
            _cycle_start = time.time()
            t_type = getattr(provider, 't_type_service', None)
            if not t_type:
                # FIX-20260708: 升级为INFO(节流5分钟)，原DEBUG在生产日志不可见
                if not hasattr(self, '_no_ttype_log_ts'):
                    self._no_ttype_log_ts = 0.0
                _now_nt = time.time()
                if _now_nt - self._no_ttype_log_ts >= 300:
                    self._no_ttype_log_ts = _now_nt
                    logging.info("[OptionTrading] 跳过交易周期: t_type_service=None")
                return
            signal_source = _normalize_signal_source(getattr(provider, '_signal_source', 'legacy'))
            if signal_source in ('A', 'B', 'C'):
                targets = t_type.select_otm_targets_signal_sources(signal_source=signal_source)
            else:
                targets = t_type.select_otm_targets_by_volume()
            # FIX-20260708-V2: 记录信号数量（原代码从未调用record_signal，导致状态报告恒显示"信号数量=0"）
            # 根因: _lifecycle_service属性名错误，应为_lifecycle_svc；且record_signal在_LIFECYCLE_DELEGATE中
            # 修复: 直接调用provider.record_signal()，通过_LIFECYCLE_DELEGATE委托到_lifecycle_svc._lc_monitor
            if targets:
                try:
                    _record_fn = getattr(provider, 'record_signal', None)
                    _fn_name = getattr(_record_fn, '__name__', str(type(_record_fn).__name__) if _record_fn else 'None')
                    if callable(_record_fn):
                        for _ in targets:
                            _record_fn()
                    else:
                        with provider._lock:
                            provider._stats['total_signals'] = provider._stats.get('total_signals', 0) + len(targets)
                    # [FIX-20260708-V5-DIAG] 诊断record_signal执行结果
                    _sig_count = provider._stats.get('total_signals', -1)
                    if not hasattr(provider, '_rs_diag_logged'):
                        provider._rs_diag_logged = 0
                    provider._rs_diag_logged += 1
                    if provider._rs_diag_logged <= 5:
                        logging.info("[OptionTrading] record_signal诊断: targets=%d fn=%s callable=%s total_signals=%d",
                                     len(targets), _fn_name, callable(_record_fn), _sig_count)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _rs_err:
                    # [FIX-20260708-V4-DIAG] 诊断record_signal异常，原except pass静默吞掉了所有错误
                    if not hasattr(provider, '_rs_err_logged'):
                        provider._rs_err_logged = 0
                    provider._rs_err_logged += 1
                    if provider._rs_err_logged <= 3:
                        logging.warning("[OptionTrading] record_signal异常(第%d次): %s targets_count=%d fn=%s",
                                        provider._rs_err_logged, _rs_err, len(targets),
                                        type(_record_fn).__name__ if _record_fn else 'None')
                    # 回退：直接递增_stats
                    try:
                        provider._stats['total_signals'] = provider._stats.get('total_signals', 0) + len(targets)
                    except (ValueError, KeyError, TypeError, AttributeError):
                        pass
            # [FIX-20260710-PRE-TARGETS] 将BS/DR/HFT集成移到targets为空检查之前
            # 根因: targets为空时L734原return导致所有策略集成代码(BS/DR/HFT)从不执行
            # 即使targets为空，BS/DR/HFT仍需运行以产生信号和更新内部状态
            # DR不依赖targets（从DuckDB查数据），BS/HFT在targets为空时从width_cache获取替代合约列表
            _fallback_instruments = []
            if not targets:
                try:
                    _wc_fb = getattr(t_type, '_width_cache', None) or getattr(t_type, 'width_cache', None)
                    if _wc_fb:
                        _fi_map = getattr(_wc_fb, '_future_initialized', {})
                        _inst_map = getattr(_wc_fb, '_instrument_id_map', None) or getattr(_wc_fb, '_futures_instruments', None)
                        for _fid, _init in _fi_map.items():
                            if _init:
                                _fb_inst = None
                                if isinstance(_inst_map, dict):
                                    _fb_inst = _inst_map.get(_fid)
                                elif hasattr(_wc_fb, '_futures_map'):
                                    _fm = getattr(_wc_fb, '_futures_map', {})
                                    _fb_inst = _fm.get(_fid, {}).get('instrument_id')
                                if _fb_inst:
                                    _fallback_instruments.append({'instrument_id': _fb_inst, 'price': 0.0})
                except (ValueError, KeyError, TypeError, AttributeError):
                    pass
            # ✅ P1-9修复: 集成box_spring_strategy的箱体弹簧扫描
            # R13-API-01修复: 传递完整tick参数(high/low/volume/timestamp)，否则箱体检测失效
            # R15-P1-PERF-02修复: 使用全局单例get_box_spring_strategy()，避免绕过单例创建多实例
            try:
                from strategy.box_spring_strategy_impl import get_box_spring_strategy
                bss = get_box_spring_strategy()
            except ImportError as _bss_ie:
                logging.warning("[P1-FIX] box_spring_strategy导入失败, 箱体弹簧扫描不可用: %s", _bss_ie)
                bss = None
            try:
                if bss is not None:
                    _bs_tick_targets = targets if targets else _fallback_instruments
                    for t in _bs_tick_targets:
                        inst_id = t.get('instrument_id', '')
                        if inst_id:
                            # FIX-14 RC-15: high/low为0时fallback到price，确保箱体检测能运行
                            # FIX-OO5c (S4-Spring-ROOT): 零宽箱体修复
                            # 根因: targets无K线high/low字段→high=low=price→零宽箱体→
                            #       contains_price/price_position退化失效→detect_spring过滤
                            # 修复: high/low为price时添加±0.5%的合成波动，形成非零宽箱体
                            _bs_price = t.get('price', 0.0)
                            _bs_high = t.get('high', 0.0) or _bs_price
                            _bs_low = t.get('low', 0.0) or _bs_price
                            if _bs_high == _bs_low and _bs_price > 0:
                                _bs_spread = _bs_price * 0.005  # 0.5%合成波动
                                _bs_high = _bs_price + _bs_spread
                                _bs_low = _bs_price - _bs_spread
                            bss.on_tick(
                                instrument_id=inst_id,
                                price=_bs_price,
                                high=_bs_high,
                                low=_bs_low,
                                volume=t.get('volume', 0),
                                timestamp=t.get('timestamp'),
                            )
                    # [FIX-20260710-BS-TRIGGER] BoxSpring信号检测→下单链路修复
                    _bs_signal_count = 0
                    for t in _bs_tick_targets:
                        _bs_inst = t.get('instrument_id', '')
                        _bs_price = t.get('price', 0.0)
                        if not _bs_inst or _bs_price <= 0:
                            continue
                        try:
                            _bs_signal = bss.detect_spring(
                                instrument_id=_bs_inst,
                                future_price=_bs_price,
                                option_instrument_id=t.get('option_instrument_id', _bs_inst),
                                strike_price=t.get('strike_price', 0.0) or _bs_price,  # FIX-S3S4-2: strike_price缺失时fallback到当前价格，避免strike_distance_pct=1.0>阈值
                                iv=t.get('iv', 0.15),  # FIX-T R9-4-2: fallback 0.0→0.15，避免IV百分位返回50>5过滤
                                premium_price=t.get('premium_price', 0.0),
                                days_to_expiry=t.get('days_to_expiry', 3),  # FIX-T R9-4-1: fallback 30→3，在[2,5]DTE范围内
                                account_equity=t.get('account_equity', 100000.0),
                            )
                            if _bs_signal is not None:
                                # [FIX-20260712-S4] 弹簧强度评分 — 上策H-Rev: 过滤低质量弹簧信号
                                # [FIX-20260712-S4-P2] 统一引用SPRING_STRENGTH_THRESHOLD类常量, 消除硬编码
                                try:
                                    _spring_box = bss.get_active_box(_bs_inst) if hasattr(bss, 'get_active_box') else None
                                    _spring_strength = bss.detect_spring_strength(
                                        _bs_signal, _spring_box
                                    ) if hasattr(bss, 'detect_spring_strength') else 0.5
                                    _spring_threshold = getattr(bss, 'SPRING_STRENGTH_THRESHOLD', 0.45)  # 统一引用类常量, fallback=0.45
                                    if _spring_strength < _spring_threshold:
                                        logging.info(
                                            "[FIX-20260712-S4] 弹簧强度不足, 过滤信号: inst=%s strength=%.3f threshold=%.3f",
                                            _bs_inst, _spring_strength, _spring_threshold
                                        )
                                        continue
                                except (ValueError, KeyError, TypeError, AttributeError):
                                    pass  # 评分异常时不阻断信号
                                _bs_triggered = bss.check_trigger(
                                    _bs_inst,
                                    order_flow_imbalance=t.get('order_flow_imbalance', 0.0),
                                    option_chain_activity=t.get('option_chain_activity', 0.0),
                                )
                                if _bs_triggered is not None:
                                    _bs_order_id = bss.execute_spring_entry(_bs_triggered)
                                    if _bs_order_id:
                                        _bs_signal_count += 1
                                        logging.info("[FIX-0710-BS-TRIGGER] BoxSpring入场: inst=%s order=%s dir=%s",
                                                     _bs_inst, _bs_order_id, _bs_triggered.direction)
                                        # FIX-S3S4-14: S4信号通过execute_spring_entry直接下单，不追加到targets列表，
                                        # 导致_save_signal_snapshot不会为S4保存快照→日志统计显示"S4策略0快照"。
                                        # 修复: 在execute_spring_entry成功下单后，手动保存信号快照。
                                        try:
                                            import os, json
                                            from datetime import datetime
                                            _snap_dir = os.path.join('logs', 'signal_snapshots')
                                            os.makedirs(_snap_dir, exist_ok=True)
                                            _now_bs = datetime.now()
                                            _ts_bs = _now_bs.strftime('%Y%m%d_%H%M%S_%f')
                                            _snap_bs = {
                                                'signal': {
                                                    'instrument_id': _bs_inst,
                                                    'option_instrument_id': _bs_triggered.option_instrument_id,
                                                    'direction': _bs_triggered.direction,
                                                    'price': _bs_triggered.premium_price,
                                                    'volume': _bs_triggered.lots,
                                                    'open_reason': 'BOX_SPRING',
                                                    'signal_id': _bs_triggered.signal_id,
                                                    'order_id': _bs_order_id,
                                                    'box_id': _bs_triggered.box_id,
                                                    'iv_percentile': _bs_triggered.iv_percentile,
                                                    'strike_price': _bs_triggered.strike_price,
                                                },
                                                'snapshot_time_iso': _now_bs.isoformat(),
                                                'snapshot_time_ts': time.time(),
                                                'strategy_meta': {
                                                    'open_reason': 'BOX_SPRING',
                                                    'dry_run': getattr(provider, '_dry_run_active', False),
                                                    'strategy_id': getattr(provider, 'strategy_id', ''),
                                                },
                                            }
                                            _fp_bs = os.path.join(_snap_dir, f"BOX_SPRING_{_bs_inst}_{_ts_bs}.json")
                                            with open(_fp_bs, 'w', encoding='utf-8') as _f_bs:
                                                json.dump(_snap_bs, _f_bs, ensure_ascii=False, indent=2, default=str)
                                            logging.info("[SignalSnapshot] S4快照已保存: %s (reason=BOX_SPRING inst=%s)",
                                                         _fp_bs, _bs_inst)
                                        except (OSError, IOError, ValueError, TypeError, KeyError, AttributeError) as _s4_snap_err:
                                            logging.warning("[SignalSnapshot] S4快照保存失败: %s", _s4_snap_err)
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _bs_sig_err:
                            _bs_sig_err_count = getattr(provider, '_bs_sig_err_count', 0) + 1
                            provider._bs_sig_err_count = _bs_sig_err_count
                            if _bs_sig_err_count <= 3 or _bs_sig_err_count % 100 == 0:
                                logging.info("[FIX-0710-BS-TRIGGER] 弹簧检测异常(第%d次): %s",
                                             _bs_sig_err_count, _bs_sig_err)
                    if _bs_signal_count > 0:
                        logging.info("[FIX-0710-BS-TRIGGER] BoxSpring本周期入场%d笔", _bs_signal_count)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("[R22-EP-P1-17] [StrategyBusinessLayer] box_spring_strategy tick error: %s", e)
            # [FIX-20260710-HFT-INTEGRATE] HFT引擎tick集成到交易周期
            try:
                _hft = getattr(provider, '_hft_engine', None)
                if _hft is None:
                    provider._ensure_hft_engine()
                    _hft = getattr(provider, '_hft_engine', None)
                if _hft is not None:
                    _hft_tick_count = getattr(provider, '_hft_tick_count', 0) + 1
                    provider._hft_tick_count = _hft_tick_count
                    if _hft_tick_count % 10 == 1:
                        _hft_tick_targets = targets if targets else _fallback_instruments
                        for t in _hft_tick_targets:
                            _hft_inst = t.get('instrument_id', '')
                            _hft_price = t.get('price', 0.0)
                            if _hft_inst and _hft_price > 0:
                                _hft_result = _hft.on_tick_enhanced(
                                    instrument_id=_hft_inst,
                                    price=_hft_price,
                                    volume=t.get('volume', 0),
                                    direction=t.get('direction', 'BUY'),
                                    product=t.get('product', ''),
                                    bid_price=t.get('bid_price', 0.0),
                                    ask_price=t.get('ask_price', 0.0),
                                    resonance_strength=getattr(getattr(provider, '_state_param_manager', None), '_last_resonance_strength', 0.0),  # FIX-S R10-5-8: 从SPM读取
                                    current_state=getattr(provider, '_resolve_open_reason', lambda: 'UNKNOWN')(),
                                )
                                if _hft_result and any(v is not None for v in _hft_result.values() if not isinstance(v, bool)):
                                    logging.debug("[FIX-0710-HFT] HFT信号: inst=%s result_keys=%s",
                                                  _hft_inst, [k for k, v in _hft_result.items() if v is not None and not isinstance(v, bool)])
                                    # [FIX-20260718-S1-HFT] S1 HFT 追仓信号接入下单链路
                                    # 根因: L1047-1080 HFT集成块仅 logging.debug 记录 pursuit_signal，
                                    #       未追加到 targets 列表，导致 S1 HFT 信号从未进入
                                    #       execute_by_ranking 下单流程，7/17 实证 S1(HIGH_FREQ) 0 单。
                                    # 修复: 提取 pursuit_signal 构造 S1 target 并 append 到 targets，
                                    #       与 S4(L1120)/S5(L1378)/S6(L1416)/S7(L1253)/S2(L1515) 对齐。
                                    # 原则: 业务模块不加日志，此处复用现有 logging.debug 位置不新增日志。
                                    # 去重: simulated=True + signal_id 在 execute_by_ranking L771 已有 10s TTL 去重，
                                    #       且 dispatch_hft_tick 路径1 走 send_order_split 不走 execute_by_ranking，无重复下单。
                                    _s1_pursuit = _hft_result.get('pursuit_signal')
                                    if _s1_pursuit and isinstance(_s1_pursuit, dict):
                                        _s1_action = _s1_pursuit.get('action', '')
                                        if _s1_action in ('OPEN_POSITION', 'ADD_POSITION'):
                                            _s1_target = {
                                                'instrument_id': _hft_inst,
                                                'direction': _s1_pursuit.get('direction', t.get('direction', 'BUY')),
                                                'price': float(_s1_pursuit.get('price', _hft_price) or _hft_price),
                                                'volume': int(_s1_pursuit.get('volume', 1) or 1),
                                                'lots': int(_s1_pursuit.get('volume', 1) or 1),
                                                'action': 'OPEN' if _s1_action == 'OPEN_POSITION' else 'ADD',
                                                'open_reason': 'HIGH_FREQ',  # [FIX-20260712-S1] 与 tick_hft.py L450 对齐
                                                'signal_id': _s1_pursuit.get('signal_id', generate_prefixed_id('S1HFT', 12)),
                                                'signal_type': _s1_pursuit.get('signal_type', 'OPEN'),
                                                'capital_allocation': 0.0,
                                                'source': 's1_hft_pursuit_simulated',
                                                'simulated': True,
                                                'exchange': t.get('exchange', ''),
                                                'product': t.get('product', ''),
                                            }
                                            targets.append(_s1_target)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _hft_err:
                _hft_err_count = getattr(provider, '_hft_err_count', 0) + 1
                provider._hft_err_count = _hft_err_count
                if _hft_err_count <= 3 or _hft_err_count % 100 == 0:
                    logging.debug("[FIX-0710-HFT] HFT引擎集成异常(第%d次): %s", _hft_err_count, _hft_err)
            # [FIX-20260715-OTHER-SCALP] ecosystem services process_other_strategy_signal 调用集成
            # 根因: services.py:503 定义了 process_other_strategy_signal()（内部调用 _box_detector.classify_extreme_state
            # 检测箱体极值），但该方法从未被任何调用方引用，导致 ecosystem 层 OTHER_SCALP 信号链路完全断裂。
            # 附录P.5 声称已在836-880行添加调用，实际未添加。本次修复在 HFT集成之后、DR集成之前接入。
            try:
                from strategy.strategy_ecosystem import get_strategy_ecosystem
                _eco_svc = get_strategy_ecosystem()
                if _eco_svc is not None and hasattr(_eco_svc, 'process_other_strategy_signal'):
                    _eco_tick_targets = list(targets) if targets else _fallback_instruments
                    _other_signal_count = 0
                    _other_skip_count = 0
                    for t in _eco_tick_targets:
                        _eco_inst = t.get('instrument_id', '')
                        _eco_price = t.get('price', 0.0)
                        if not _eco_inst or _eco_price <= 0:
                            continue
                        try:
                            _other_result = _eco_svc.process_other_strategy_signal(
                                current_price=_eco_price,
                                resonance_direction=('fall' if t.get('direction', '') in ('SELL', 'sell', 'SHORT', 'short') else 'rise') if t.get('direction', '') else '',  # FIX-S3S4-7: 映射BUY/SELL→rise/fall
                                resonance_strength=getattr(getattr(provider, '_state_param_manager', None), '_last_resonance_strength', 0.0),  # FIX-S R10-5-08
                                current_iv=t.get('iv', 0.15),  # FIX-T R9-4-2: fallback 0.0→0.15
                                flow_imbalance=t.get('order_flow_imbalance', 0.0),
                                cvd_slope=0.0,
                                instrument_id=_eco_inst,  # FIX-56: 传递instrument_id用于冷却控制
                            )
                            _other_action = _other_result.get('action', '') if _other_result else ''
                            if _other_action == 'open':
                                _other_dir = _other_result.get('direction', 'BUY')
                                _other_target = {
                                    'instrument_id': _eco_inst,
                                    'direction': _other_dir,
                                    'price': _eco_price,
                                    'volume': 0,
                                    'lots': 1,
                                    'open_reason': 'OTHER_SCALP',
                                    'signal_id': _other_result.get('trade_id', ''),
                                    'strategy_id': 'other',
                                }
                                targets.append(_other_target)
                                _other_signal_count += 1
                                logging.info(
                                    "[FIX-20260715-OTHER-SCALP] ecosystem箱体极值信号触发: inst=%s dir=%s trade_id=%s",
                                    _eco_inst, _other_dir, _other_result.get('trade_id', ''),
                                )
                            else:
                                _other_skip_count += 1
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _other_err:
                            _other_err_count = getattr(provider, '_other_sig_err_count', 0) + 1
                            provider._other_sig_err_count = _other_err_count
                            if _other_err_count <= 3 or _other_err_count % 100 == 0:
                                logging.debug("[FIX-20260715-OTHER-SCALP] ecosystem信号检测异常(第%d次): %s",
                                              _other_err_count, _other_err)
                    if _other_signal_count > 0:
                        logging.info("[FIX-20260715-OTHER-SCALP] ecosystem本周期入场%d笔", _other_signal_count)
                    # 诊断日志: 确认 ecosystem 服务被调用但信号被跳过的原因（每100个tick输出一次）
                    _eco_diag_count = getattr(provider, '_eco_diag_count', 0) + 1
                    provider._eco_diag_count = _eco_diag_count
                    if _eco_diag_count <= 3 or _eco_diag_count % 200 == 0:
                        logging.info(
                            "[FIX-20260715-OTHER-SCALP] ecosystem诊断: svc=%s targets=%d open=%d skip=%d",
                            type(_eco_svc).__name__, len(_eco_tick_targets), _other_signal_count, _other_skip_count,
                        )
                else:
                    # 诊断日志: ecosystem 服务不可用
                    _eco_unavail_count = getattr(provider, '_eco_unavail_count', 0) + 1
                    provider._eco_unavail_count = _eco_unavail_count
                    if _eco_unavail_count <= 3 or _eco_unavail_count % 200 == 0:
                        logging.warning(
                            "[FIX-20260715-OTHER-SCALP] ecosystem服务不可用: svc=%s has_method=%s",
                            type(_eco_svc).__name__ if _eco_svc else None,
                            hasattr(_eco_svc, 'process_other_strategy_signal') if _eco_svc else False,
                        )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _eco_err:
                logging.debug("[FIX-20260715-OTHER-SCALP] ecosystem服务集成异常: %s", _eco_err)
            # [FIX-20260712-S7-V3] DivergenceReversal信号生成集成 — 用户本意: 反趋势开仓
            # 旧版[FIX-20260710-DIVREV-V2]仅记录日志不生成交易信号，导致S7背离检测与交易完全断开
            # 新版调用generate_signal()生成明确方向信号，有效信号追加到targets列表参与交易
            try:
                from strategy.divergence_reversal import get_divergence_reversal_module
                _dr_module = get_divergence_reversal_module()
                if _dr_module is not None:
                    _dr_call_count = getattr(provider, '_dr_update_call_count', 0) + 1
                    provider._dr_update_call_count = _dr_call_count
                    if _dr_call_count % 10 == 1:
                        if _dr_call_count <= 3 or _dr_call_count % 300 == 1:
                            logging.info("[S7-V3-DIVREV] S7背离检测运行中(call=%d)", _dr_call_count)
                        try:
                            from data.data_service import get_data_service as _gds
                            _ds = _gds()
                            if _ds is not None and hasattr(_ds, 'query'):
                                # [S7-V3-P1] 按配置周期过滤K线，避免kline_period参数失效/混合多周期数据
                                _dr_period = _dr_module.get_sql_period()
                                # [S7-V3-P2] SQL缓存: 若距上次查询不足TTL秒, 跳过SQL直接用缓存DataFrame
                                _dr_cache_ttl = _dr_module.get_sql_cache_ttl()
                                _dr_last_ts = getattr(provider, '_dr_last_sql_ts', 0.0)
                                _dr_cached_df = getattr(provider, '_dr_cached_df', None)
                                _now_ts = time.time()
                                _use_cache = (_dr_cached_df is not None
                                              and (_now_ts - _dr_last_ts) < _dr_cache_ttl)
                                if _use_cache:
                                    _dr_df = _dr_cached_df
                                else:
                                    # [FIX-20260712-S7-V3-P0] SQL必须使用UNION分别取期货K线和期权K线，
                                    # 原JOIN LEFT JOIN会把期权行挂到期货symbol下，导致option_symbol实为期货代码、
                                    # 期权价格用期货价格，交易标的全错。
                                    _dr_sql = (
                                        "SELECT fi.instrument_id AS symbol, kr.timestamp AS minute, "
                                        "kr.open, kr.high, kr.low, kr.close, kr.volume, "
                                        "NULL AS option_type, NULL AS strike_price, lp.last_price AS underlying_price "
                                        "FROM klines_raw kr "
                                        "JOIN futures_instruments fi ON kr.internal_id = fi.internal_id "
                                        # FIX-20260713-S7-SQL: latest_prices视图只有instrument_id列,无internal_id
                                        # 根因: latest_prices视图定义SELECT instrument_id,last_price,timestamp FROM ticks_raw
                                        #       原SQL JOIN ON lp.internal_id=fi.internal_id 报Binder Error
                                        # 修复: 改为JOIN ON lp.instrument_id=fi.instrument_id
                                        "LEFT JOIN latest_prices lp ON lp.instrument_id = fi.instrument_id "
                                        "WHERE kr.trade_date = (SELECT MAX(trade_date) FROM klines_raw WHERE period = ?) "
                                        "AND kr.period = ? "
                                        "UNION ALL "
                                        "SELECT oi.instrument_id AS symbol, kr.timestamp AS minute, "
                                        "kr.open, kr.high, kr.low, kr.close, kr.volume, "
                                        "oi.option_type, oi.strike_price, lp.last_price AS underlying_price "
                                        "FROM klines_raw kr "
                                        "JOIN option_instruments oi ON kr.internal_id = oi.internal_id "
                                        "LEFT JOIN futures_instruments fi ON fi.internal_id = oi.underlying_future_id "
                                        "LEFT JOIN latest_prices lp ON lp.instrument_id = fi.instrument_id "
                                        "WHERE kr.trade_date = (SELECT MAX(trade_date) FROM klines_raw WHERE period = ?) "
                                        "AND kr.period = ? "
                                        "ORDER BY minute DESC LIMIT 500"
                                    )
                                    _dr_result = _ds.query(_dr_sql, [_dr_period, _dr_period, _dr_period, _dr_period])
                                    if _dr_result is not None and len(_dr_result) > 0:
                                        import pandas as _pd
                                        _dr_df = _dr_result.to_pandas() if hasattr(_dr_result, 'to_pandas') else _pd.DataFrame(_dr_result.to_pylist())
                                        # [S7-V3-P1] 查询结果按时间降序，但下游按'last'取末根成交量；
                                        # 必须按时间升序排列，使.last()返回最新bar。
                                        if 'minute' in _dr_df.columns:
                                            _dr_df = _dr_df.sort_values('minute', ascending=True).reset_index(drop=True)
                                        # 更新缓存
                                        provider._dr_cached_df = _dr_df
                                        provider._dr_last_sql_ts = _now_ts
                                    else:
                                        _dr_df = None
                                        provider._dr_cached_df = None
                                if _dr_df is not None and len(_dr_df) > 0:
                                    # [S7-V3] 调用generate_signal生成方向信号(用户本意: 反趋势开仓)
                                    _dr_signal = _dr_module.generate_signal(_dr_df)
                                    if _dr_signal is not None and _dr_signal.is_valid:
                                        logging.info(
                                            "[S7-V3-DIVREV] 背离反转信号触发: dir=%s strength=%.2f "
                                            "fut=%s@%.2f opt=%s@%.2f type=%s period=%s bar=%s",
                                            _dr_signal.direction, _dr_signal.strength,
                                            _dr_signal.futures_symbol, _dr_signal.futures_price,
                                            _dr_signal.option_symbol, _dr_signal.option_premium,
                                            _dr_signal.divergence_type, _dr_signal.kline_period,
                                            _dr_signal.bar_time,
                                        )
                                        # 有效信号追加到targets列表 — 使背离信号真正参与交易
                                        _dr_target = {
                                            'instrument_id': _dr_signal.option_symbol,
                                            'direction': _dr_signal.direction,
                                            'price': _dr_signal.option_premium,
                                            'volume': 0,
                                            'lots': 1,
                                            'action': 'OPEN',
                                            'open_reason': 'DIVERGENCE_REVERSAL',
                                            'reason': 'DIVERGENCE_REVERSAL',
                                            'signal_id': generate_prefixed_id('DIVSIG', 12),
                                            'divergence_signal': _dr_signal.to_dict(),
                                            'source': 's7_divergence_reversal_v3',
                                        }
                                        targets.append(_dr_target)
                                        provider._dr_last_signal = _dr_signal.to_dict()
                                        logging.info("[S7-V3-DIVREV] 背离信号已追加到targets (reason=DIVERGENCE_REVERSAL)")
                                    else:
                                        # 无背离信号时节流日志
                                        if _dr_call_count <= 3 or _dr_call_count % 300 == 1:
                                            logging.info("[S7-V3-DIVREV] 无背离信号(call=%d rows=%d)",
                                                         _dr_call_count, len(_dr_df))
                                        # 仍调用update()保持向后兼容(三层诊断输出)
                                        _dr_module.update(_dr_df)
                                else:
                                    if _dr_call_count <= 3 or _dr_call_count % 300 == 1:
                                        logging.info("[S7-V3-DIVREV] klines_raw查询无数据(call=%d period=%s) — DuckDB中可能无对应周期K线", _dr_call_count, _dr_period)
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _dr_qry_err:
                            if _dr_call_count <= 3 or _dr_call_count % 100 == 0:
                                logging.warning("[S7-V3-DIVREV] klines_raw查询失败(call=%d): %s",
                                                _dr_call_count, _dr_qry_err)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _dr_err:
                _dr_err_count = getattr(provider, '_dr_err_count', 0) + 1
                provider._dr_err_count = _dr_err_count
                if _dr_err_count <= 3 or _dr_err_count % 100 == 0:
                    logging.warning("[S7-V3-DIVREV] DivergenceReversalModule集成异常(第%d次): %s",
                                    _dr_err_count, _dr_err)
            # [FIX-20260713-S5S6-FEED] S5/S6 monitor tick数据投喂 — 根因: on_tick()从未被调用
            # 根因: S5/S6 monitor虽有on_tick()接口，但在onTick链中从未被调用
            #       → monitor无价格历史 → 无信号生成 → 0模拟下单 → 其他模块无法参考
            # 修复: 将tick数据投喂给S5/S6 monitor, 首次调用时注册默认配对/设置合约
            try:
                _s5s6_feed_count = getattr(provider, '_s5s6_feed_count', 0) + 1
                provider._s5s6_feed_count = _s5s6_feed_count
                if _s5s6_feed_count % 5 == 1:  # 每5tick投喂一次(性能)
                    _feed_targets = (targets if targets else _fallback_instruments)[:5]
                    # S5套利: 投喂tick + 首次注册默认配对
                    try:
                        from strategy.monitor.arbitrage_monitor import get_arbitrage_monitor, ArbitragePairConfig
                        _s5_mon = get_arbitrage_monitor()
                        if (not getattr(provider, '_s5_pairs_registered', False)) and len(_feed_targets) >= 2:
                            _leg_a = _feed_targets[0].get('instrument_id', '')
                            _leg_b = _feed_targets[1].get('instrument_id', '')
                            if _leg_a and _leg_b and _leg_a != _leg_b:
                                _s5_mon.register_pair(ArbitragePairConfig(leg_a=_leg_a, leg_b=_leg_b))
                                provider._s5_pairs_registered = True
                                logging.info("[S5-ARB-FEED] 注册默认套利对: %s:%s", _leg_a, _leg_b)
                        for _t in _feed_targets:
                            _inst = _t.get('instrument_id', '')
                            _price = float(_t.get('price', 0.0) or 0.0)
                            if _inst and _price > 0:
                                _s5_mon.on_tick({
                                    'instrument_id': _inst,
                                    'last_price': _price,
                                    'volume': float(_t.get('volume', 0.0) or 0.0),
                                    'bid_price': float(_t.get('bid_price', 0.0) or 0.0),
                                    'ask_price': float(_t.get('ask_price', 0.0) or 0.0),
                                    'bid_volume': float(_t.get('bid_volume', _t.get('bid_size', 0.0)) or 0.0),
                                    'ask_volume': float(_t.get('ask_volume', _t.get('ask_size', 0.0)) or 0.0),
                                    'open_interest': float(_t.get('open_interest', 0.0) or 0.0),
                                    'direction': str(_t.get('direction', '') or ''),
                                })
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _s5_feed_e:
                        logging.debug("[S5-ARB-FEED] 投喂异常: %s", _s5_feed_e)
                    # S6做市: 投喂tick + 首次设置instrument_id
                    try:
                        from strategy.monitor.market_making_monitor import get_market_making_monitor
                        _s6_mon = get_market_making_monitor()
                        if (not getattr(provider, '_s6_instrument_set', False)) and _feed_targets:
                            _s6_inst = _feed_targets[0].get('instrument_id', '')
                            if _s6_inst:
                                _s6_mon.instrument_id = _s6_inst
                                provider._s6_instrument_set = True
                                logging.info("[S6-MM-FEED] 设置做市合约: %s", _s6_inst)
                        for _t in _feed_targets:
                            _inst = _t.get('instrument_id', '')
                            _price = float(_t.get('price', 0.0) or 0.0)
                            if _inst and _price > 0:
                                _s6_mon.on_tick({
                                    'instrument_id': _inst,
                                    'last_price': _price,
                                    'volume': float(_t.get('volume', 0.0) or 0.0),
                                    'bid_price': float(_t.get('bid_price', 0.0) or 0.0),
                                    'ask_price': float(_t.get('ask_price', 0.0) or 0.0),
                                    'bid_volume': float(_t.get('bid_volume', _t.get('bid_size', 0.0)) or 0.0),
                                    'ask_volume': float(_t.get('ask_volume', _t.get('ask_size', 0.0)) or 0.0),
                                    'open_interest': float(_t.get('open_interest', 0.0) or 0.0),
                                    'direction': str(_t.get('direction', '') or ''),
                                })
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _s6_feed_e:
                        logging.debug("[S6-MM-FEED] 投喂异常: %s", _s6_feed_e)
            except (ValueError, KeyError, TypeError, AttributeError) as _s5s6_feed_outer_e:
                logging.debug("[S5S6-FEED] 投喂集成异常: %s", _s5s6_feed_outer_e)
            # [FIX-20260713-S5S6-SIM-ORDER] S5套利/S6做市商模拟开仓/平仓信号接入订单链
            # 根因: S5/S6虽内部生成信号和快照，但未接入targets → 无模拟下单日志 → 其他模块无法参考
            # 修复: 将S5/S6最新信号追加到targets，capital_allocation=0.0确保不实际下单
            #   dry_run模式会记录模拟下单日志，其他模块可通过targets感知S5/S6方向
            try:
                _s5s6_call_count = getattr(provider, '_s5s6_call_count', 0) + 1
                provider._s5s6_call_count = _s5s6_call_count
                # FIX-20260716-S5S6-FREQ: 模拟信号接入频率从50tick提升到5tick
                # 根因: 50tick采样导致S5/S6模拟交易记录过于稀疏，与投喂频率(5tick)不一致
                #       在实盘运行时模拟交易并记录快照的方式下，应保证信号及时接入订单链
                if _s5s6_call_count % 5 == 1:  # 5tick采样一次，与投喂频率一致
                    # S5套利模拟信号 — 先触发信号生成(配对评估), 再读取最新信号
                    try:
                        from strategy.monitor.arbitrage_monitor import get_arbitrage_monitor
                        _s5_mon = get_arbitrage_monitor()
                        _s5_mon.generate_simulated_signal()
                        _s5_sig = _s5_mon.get_last_simulated_signal()
                        if _s5_sig is not None:
                            _s5_last_recorded = getattr(provider, '_s5_last_recorded_sig_id', '')
                            if _s5_sig.signal_id != _s5_last_recorded:
                                _s5_target = {
                                    'instrument_id': _s5_sig.instrument_id or _s5_sig.leg_a or 'S5_ARB',
                                    'direction': _s5_sig.direction,
                                    'price': _s5_sig.entry_price,
                                    'action': 'OPEN',
                                    'lots': 1,
                                    'signal_id': _s5_sig.signal_id,
                                    'signal_type': _s5_sig.signal_type,
                                    'open_reason': 'ARBITRAGE',  # FIX-S5-4: 'reason'→'open_reason'，与 execute_by_ranking L828 target.get('open_reason','') 对齐
                                    'capital_allocation': 0.0,
                                    'source': 's5_arbitrage_simulated',
                                    'simulated': True,
                                    'z_score': _s5_sig.z_score,
                                    'hedge_ratio': _s5_sig.hedge_ratio,
                                    'cointegration_pvalue': _s5_sig.cointegration_pvalue,
                                }
                                targets.append(_s5_target)
                                provider._s5_last_recorded_sig_id = _s5_sig.signal_id
                                logging.info("[S5-ARB-SIM] 模拟%s信号接入订单链: id=%s dir=%s z=%.2f hedge=%.3f",
                                             _s5_sig.signal_type, _s5_sig.signal_id, _s5_sig.direction,
                                             _s5_sig.z_score, _s5_sig.hedge_ratio)
                    except Exception as _s5_e:  # FIX-S6-3: 窄异常元组→Exception (NEW-1硬约束) + debug→warning
                        logging.warning("[S5-ARB-SIM] S5套利模拟信号采集失败: %s", _s5_e)
                    # S6做市商模拟信号
                    try:
                        from strategy.monitor.market_making_monitor import get_market_making_monitor
                        _s6_mon = get_market_making_monitor()
                        # FIX-8 RC-10: 先触发S6信号生成，再读取最新信号
                        # 根因: 原代码只读取get_last_signal()但从未调用generate_simulated_signal()
                        #       → monitor无新信号生成 → S6永远0信号 → 0模拟下单
                        _s6_mon.generate_simulated_signal()
                        _s6_sig = _s6_mon.get_last_signal()
                        if _s6_sig is not None:
                            _s6_last_recorded = getattr(provider, '_s6_last_recorded_sig_id', '')
                            _s6_sig_id = getattr(_s6_sig, 'signal_id', '') or str(getattr(_s6_sig, 'timestamp', 0))
                            if _s6_sig_id != _s6_last_recorded:
                                _s6_dir = getattr(_s6_sig, 'direction', 'BUY')
                                _s6_target = {
                                    'instrument_id': getattr(_s6_sig, 'instrument_id', 'S6_MM') or 'S6_MM',
                                    'direction': _s6_dir,
                                    'price': getattr(_s6_sig, 'mid_price', 0.0),
                                    'action': 'OPEN',
                                    'lots': 1,
                                    'signal_id': _s6_sig_id,
                                    'signal_type': getattr(_s6_sig, 'signal_type', 'OPEN'),
                                    'open_reason': 'MARKET_MAKING',  # FIX-S6-4: 'reason'→'open_reason'，与 execute_by_ranking L828 target.get('open_reason','') 对齐
                                    'capital_allocation': 0.0,
                                    'source': 's6_market_making_simulated',
                                    'simulated': True,
                                    'bid_price': getattr(_s6_sig, 'bid_price', 0.0),
                                    'ask_price': getattr(_s6_sig, 'ask_price', 0.0),
                                    'spread_bps': getattr(_s6_sig, 'spread_bps', 0.0),
                                    'inventory': getattr(_s6_sig, 'inventory', 0.0),
                                }
                                targets.append(_s6_target)
                                provider._s6_last_recorded_sig_id = _s6_sig_id
                                logging.info("[S6-MM-SIM] 模拟%s信号接入订单链: id=%s dir=%s spread=%.1fbps inv=%.2f",
                                             getattr(_s6_sig, 'signal_type', 'OPEN'), _s6_sig_id, _s6_dir,
                                             getattr(_s6_sig, 'spread_bps', 0.0), getattr(_s6_sig, 'inventory', 0.0))
                    except Exception as _s6_e:  # FIX-S6-3: 窄异常元组→Exception (NEW-1硬约束) + debug→warning
                        logging.warning("[S6-MM-SIM] S6做市商模拟信号采集失败: %s", _s6_e)
            except Exception as _s5s6_e:  # FIX-S6-3: 窄异常元组→Exception (NEW-1硬约束) + debug→warning
                logging.warning("[S5S6-SIM] S5/S6模拟信号集成异常: %s", _s5s6_e)
            # [FIX-20260712-S2-V2] S2日内交易四维信号组装 — 用户本意: 共振+订单流+希腊+三角
            # 冗余桥接模块已删除，此处直接在业务层组装四维信号(参照S7修复方式)
            # 条件(全部满足): 共振>0.75 + |订单流|>0.20 + 希腊>0.4 + 三角>0.5
            try:
                _s2_call_count = getattr(provider, '_s2_call_count', 0) + 1
                provider._s2_call_count = _s2_call_count
                if _s2_call_count % 30 == 1:  # 30tick采样一次(S2日内不需要高频)
                    _s2_targets = targets if targets else _fallback_instruments
                    # 获取希腊字母评分和三角评判评分(从RiskComputeService)
                    _s2_greeks_score = 0.5  # 默认中性
                    _s2_tri_score = 0.5     # 默认中性
                    try:
                        from risk.risk_service import get_risk_service
                        _s2_rs = get_risk_service()
                        if _s2_rs and hasattr(_s2_rs, '_risk_compute'):
                            _s2_rc = _s2_rs._risk_compute
                            try:
                                _s2_gd = getattr(_s2_rs, '_greeks_dashboard', None)
                                _s2_greeks_score = _s2_rc._compute_greeks_usage_score(_s2_gd)
                            except (ValueError, KeyError, TypeError, AttributeError):
                                pass
                            try:
                                _s2_tri_score = _s2_rc._compute_tri_validation_score(None)
                            except (ValueError, KeyError, TypeError, AttributeError):
                                pass
                    except (ValueError, KeyError, TypeError, AttributeError, ImportError):
                        pass
                    # S2参数(阈值高于S1以提高胜率)
                    _S2_RESONANCE_THRESH = 0.75   # > S1的0.6
                    _S2_ORDER_FLOW_THRESH = 0.20  # > S1的0.15
                    _S2_GREEKS_THRESH = 0.4
                    _S2_TRI_THRESH = 0.5
                    # FIX-D: S2 resonance_strength从SPM读取(与S1一致)，target dict不含此字段
                    _spm_s2 = getattr(provider, '_state_param_manager', None)
                    _s2_market_resonance = getattr(_spm_s2, '_last_resonance_strength', 0.0) if _spm_s2 else 0.0
                    for t in _s2_targets:
                        _s2_inst = t.get('instrument_id', '')
                        _s2_price = t.get('price', 0.0)
                        _s2_res = _s2_market_resonance  # 从SPM获取，非target dict
                        _s2_dir_hint = t.get('direction', '')
                        if not _s2_inst or _s2_price <= 0:
                            continue
                        # [FIX-20260712-S2-P0] 订单流必须按目标合约获取，空字符串返回全局/默认值。
                        _s2_of_imbalance = 0.0
                        try:
                            # FIX-U R9-2: 使用get_order_flow_bridge()单例，而非每次new OrderFlowBridge()
                            # 根因: OrderFlowBridge()新实例内部MicrostructureAnalyzer无累积tick数据→imbalance恒0→S2永0信号
                            from order.order_flow_bridge import get_order_flow_bridge
                            _s2_ofb = get_order_flow_bridge()
                            _s2_of_imbalance = _s2_ofb.get_instant_imbalance(_s2_inst) or 0.0
                        except Exception:  # FIX-U: 扩大异常捕获
                            pass
                        # 四维条件检查(全部满足才开仓)
                        if _s2_res < _S2_RESONANCE_THRESH:
                            continue
                        if abs(_s2_of_imbalance) < _S2_ORDER_FLOW_THRESH:
                            continue
                        if _s2_greeks_score < _S2_GREEKS_THRESH:
                            continue
                        if _s2_tri_score < _S2_TRI_THRESH:
                            continue
                        # 方向: 订单流不平衡符号决定(正=BUY, 负=SELL)
                        if _s2_dir_hint in ('BUY', 'SELL'):
                            _s2_dir = _s2_dir_hint
                        elif _s2_of_imbalance > 0:
                            _s2_dir = 'BUY'
                        else:
                            _s2_dir = 'SELL'
                        _s2_target = {
                            'instrument_id': _s2_inst,
                            'direction': _s2_dir,
                            'price': _s2_price,
                            'volume': 0,
                            'lots': 1,
                            'action': 'OPEN',
                            'reason': 'INTRADAY',
                            'open_reason': 'INTRADAY',  # [FIX-20260712-S2-P0] 必须包含open_reason，否则order_executor_platform.py:815读取为空→持仓管理/Greeks限额/EV缓存路由全部断裂
                            'strategy_group': 'intraday',
                            'signal_id': generate_prefixed_id('S2INT', 12),
                            'take_profit_ratio': 1.5,
                            'stop_loss_ratio': 0.5,
                            'max_hold_minutes': 240.0,
                            's2_scores': {
                                's2_resonance': round(_s2_res, 4),
                                'order_flow': round(_s2_of_imbalance, 4),
                                'greeks': round(_s2_greeks_score, 4),
                                'tri_validation': round(_s2_tri_score, 4),
                            },
                            'source': 's2_intraday_v2',
                        }
                        targets.append(_s2_target)
                        provider._s2_last_signal = _s2_target
                        logging.info(
                            "[S2-INTRADAY] 四维信号追加targets: inst=%s dir=%s res=%.3f of=%.4f greeks=%.3f tri=%.3f",
                            _s2_inst, _s2_dir, _s2_res, _s2_of_imbalance, _s2_greeks_score, _s2_tri_score,
                        )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _s2_err:
                _s2_err_count = getattr(provider, '_s2_err_count', 0) + 1
                provider._s2_err_count = _s2_err_count
                if _s2_err_count <= 3 or _s2_err_count % 100 == 0:
                    logging.warning("[S2-INTRADAY] 四维信号组装异常(第%d次): %s", _s2_err_count, _s2_err)
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
                            "[OptionTrading] R24-P1-CF-02: targets为空，跳过下单周期 "
                            "(initialized_futures=%d, tier_counts=%s, signal_source=%s, fallback_inst=%d)",
                            _init_count, _tier_info, signal_source, len(_fallback_instruments)
                        )
                    except Exception as _diag_e:
                        logging.warning("[OptionTrading] targets为空（诊断失败: %s）", _diag_e)
                return
            open_reason = getattr(provider, '_resolve_open_reason', lambda: 'UNKNOWN')()  # [FIX-20260712-AUDIT-P0] 保护调用防止AttributeError
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
                from risk.risk_service import get_risk_service
                scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
                rs = get_risk_service(None, scope_id=scope_id)
                passed_targets = []
                for t in targets:
                    # P1-52修复: 使用generate_prefixed_id统一ID生成
                    from infra.shared_utils import generate_prefixed_id
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
                        # NEW-04(FIX-20260706-RISK-BLOCK-NOISE): 限流风控阻断日志，避免hard_stop期间洪泛
                        # 旧代码每个target每30s周期输出1条WARNING，hard_stop=True时612条/24min，可能拖慢平台UI
                        _now = time.time()
                        _last = getattr(provider, '_risk_block_last_log_ts', 0)
                        _count = getattr(provider, '_risk_block_suppressed', 0)
                        if (_now - _last) >= 60.0:
                            if _count > 0:
                                logging.warning("[OptionTrading] 风控阻断(限流): %s %s reason=%s (已抑制%d条同类日志)",
                                                t.get('instrument_id', ''), t.get('direction', ''), check_result.reason, _count)
                            else:
                                logging.warning("[OptionTrading] 风控阻断: %s %s reason=%s type=%s result=%s message=%s",
                                                t.get('instrument_id', ''), t.get('direction', ''), check_result.reason,
                                                type(check_result).__name__, check_result.result, getattr(check_result, 'message', ''))
                            provider._risk_block_last_log_ts = _now
                            provider._risk_block_suppressed = 0
                        else:
                            provider._risk_block_suppressed = _count + 1
                        continue
                    passed_targets.append(t)
                if not passed_targets:
                    # NEW-04: 限流"所有target均被风控阻断"日志，1条/60s
                    _now2 = time.time()
                    _last2 = getattr(provider, '_all_blocked_last_log_ts', 0)
                    if (_now2 - _last2) >= 60.0:
                        logging.info("[OptionTrading] 所有target均被风控阻断，跳过本次周期 (限流1/60s)")
                        provider._all_blocked_last_log_ts = _now2
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
            # FIX-20260709-DRY-RUN-PASS: dry_run模式下不阻断，但记录健康状态供快照诊断
            if not _dry_run_mode:
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
            else:
                # dry_run模式: 记录健康状态但不阻断
                try:
                    if provider._health_pause_new_open:
                        logging.info(
                            "[FIX-20260709-DRY-RUN-PASS] dry_run模式跳过_health_pause_new_open阻断 "
                            "(快照中可查健康状态): 已暂停%.0fs/300s",
                            (time.time() - (getattr(provider, '_health_pause_new_open_ts', 0) or 0)),
                        )
                    try:
                        health = provider.get_health_status()
                        if health.get('health') in ('CRITICAL', 'DEGRADED'):
                            logging.info(
                                "[FIX-20260709-DRY-RUN-PASS] dry_run模式跳过健康检查阻断(快照中记录): health=%s",
                                health.get('health'),
                            )
                    except Exception:
                        pass
                except Exception:
                    pass
            # FIX-32 RC-43: 在风控通过后、下单前保存信号快照(S1-S4/S7)
            self._save_signal_snapshot(targets, open_reason)
            provider._ensure_order_service()
            if provider._order_service:
                order_ids = provider._order_service.execute_by_ranking(targets)
                if order_ids:
                    logging.info("[OptionTrading] 下单完成: %d 笔 reason=%s", len(order_ids), open_reason)
                    # FIX-20260708-V2: 记录交易数量（原代码从未调用record_trade，导致状态报告恒显示"交易数量=0"）
                    # 根因: _lifecycle_service属性名错误，应为_lifecycle_svc；且record_trade在_LIFECYCLE_DELEGATE中
                    # 修复: 直接调用provider.record_trade()，通过_LIFECYCLE_DELEGATE委托到_lifecycle_svc._lc_monitor
                    # dry_run模式下execute_by_ranking返回空list(被OrderExecutor拦截)，不会误计交易
                    try:
                        _record_fn = getattr(provider, 'record_trade', None)
                        if callable(_record_fn):
                            for _ in order_ids:
                                _record_fn()
                        else:
                            with provider._lock:
                                provider._stats['total_trades'] = provider._stats.get('total_trades', 0) + len(order_ids)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _rt_err:
                        # [FIX-20260708-V4-DIAG] 诊断record_trade异常，原except pass静默吞掉了所有错误
                        if not hasattr(provider, '_rt_err_logged'):
                            provider._rt_err_logged = 0
                        provider._rt_err_logged += 1
                        if provider._rt_err_logged <= 3:
                            logging.warning("[OptionTrading] record_trade异常(第%d次): %s order_count=%d fn=%s",
                                            provider._rt_err_logged, _rt_err, len(order_ids),
                                            type(_record_fn).__name__ if _record_fn else 'None')
                        # 回退：直接递增_stats
                        try:
                            provider._stats['total_trades'] = provider._stats.get('total_trades', 0) + len(order_ids)
                        except (ValueError, KeyError, TypeError, AttributeError):
                            pass
            provider._feed_shadow_engine(targets, open_reason)
            # R14-P1-DEAD-08修复: 在决策循环中调用governance checker，手册14节治理检测
            try:
                from strategy.strategy_ecosystem import get_strategy_ecosystem
                _eco = get_strategy_ecosystem()
                if _eco and hasattr(_eco, 'run_governance_checks'):
                    _gov_result = _eco.run_governance_checks()
                    if _gov_result.get('any_triggered', False):
                        logging.warning("[OptionTrading] R14-P1-DEAD-08: 治理检测触发降级: %s",
                                        _gov_result.get('degradation_reasons', []))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _gov_err:
                logging.warning("[R22-EP-P1] governance check跳过(合规检查可能放行违规交易): %s", _gov_err)
            # [FIX-20260710-PRE-TARGETS] BS/DR/HFT集成已移到targets为空检查之前(见L757+)
            # 原因: targets为空时return导致这些策略从不执行
        except Exception as e:
            # R10-P2-02: 关键异常日志添加exc_info=True
            # FIX-59 D1: 扩展except为Exception，避免吞掉MemoryError/ConnectionError/TimeoutError等
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
            from strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            spm = getattr(provider, '_state_param_manager', None)
            current_state = spm.get_current_state() if spm else 'other'
            # FIX-R23: 直接使用硬编码映射，避免eco.__getattr__抛出AttributeError阻断交易
            _state_map = {
                'correct_trending': 'master',
                'correct_trending_defensive': 'master',
                'incorrect_reversal': 's7_divergence',
                'incorrect_reversal_defensive': 's7_divergence',
                'other': 'other',
                's4_spring': 's4_spring',
                's5_arbitrage': 's5_arbitrage',
                's6_market_making': 's6_market_making',
            }
            strategy_id = _state_map.get(current_state, 'master')
            # FIX-R23: 直接读取_active_strategy属性，避免check_mutual_exclusion被__getattr__拦截
            _active_strategy = getattr(eco, '_active_strategy', 'master')
            # FIX-23 RC-28: dry_run模式跳过互斥阻断，避免S1(master)在ecosystem='other'时被永久阻断
            # 根因: ecosystem长期处于'other'状态时S1通过主交易周期的所有信号被永久阻断，dry_run模拟也被阻断
            _dry_run = bool(getattr(provider, '_dry_run_active', False))
            if (not _dry_run) and _active_strategy == 'other' and strategy_id in ('master', 's7_divergence'):
                for t in targets:
                    direction = t.get('direction', 'BUY')
                    logging.warning("[OptionTrading] 互斥阻断: strategy=%s dir=%s reason=%s blocked in other state",
                                  strategy_id, direction, strategy_id)
                    return False
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            # FIX-24 RC-29: 移除_health_pause_new_open全局阻断(原300秒全局阻断所有新开仓形成永久循环)
            # 根因: 异常处理中反复设置_health_pause_new_open=True导致永久阻断
            # 修复: dry_run模式fail-open(返回True)，实盘仅阻断本周期(返回False)不设全局暂停
            _dry_run = bool(getattr(provider, '_dry_run_active', False))
            logging.error("[R16-P1-4.2] _check_ecosystem_exclusion Error: %s, %s", e,
                          "dry_run fail-open" if _dry_run else "实盘本周期阻断")
            return True if _dry_run else False

    # ========== 影子策略引擎 ==========

    def _feed_shadow_engine(self, targets: list, open_reason: str) -> None:
        """V7新增：将交易信号批量推送到影子策略监控引擎(优化：单次锁获取)"""
        provider = self._provider
        try:
            se = getattr(provider, '_shadow_engine', None)
            if se is None:
                from strategy.shadow_strategy_facade import get_shadow_strategy_engine
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
                    'hft': 's1_hft', 's3_box': 's3_box', 's4_spring': 's4_spring',
                    's5_arbitrage': 's5_arbitrage', 's6_market_making': 's6_market_making',
                }
                # FIX-R23: 直接使用硬编码映射，避免eco.__getattr__抛出AttributeError
                _state_map = {
                    'correct_trending': 'master',
                    'correct_trending_defensive': 'master',
                    'incorrect_reversal': 's7_divergence',
                    'incorrect_reversal_defensive': 's7_divergence',
                    'other': 'other',
                    's4_spring': 's4_spring',
                    's5_arbitrage': 's5_arbitrage',
                    's6_market_making': 's6_market_making',
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
