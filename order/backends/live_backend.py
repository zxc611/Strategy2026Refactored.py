# [M1-46-LIVE] 生产下单执行后端
# MODULE_ID: M1-46-LIVE
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""
LiveExecutionBackend — 生产下单执行后端

迁移自 _OrderExecutorPlatformMethods 的生产分支方法:
- _execute_platform_insert (生产分支 L216-399) -> execute()
- _build_platform_insert_params (L996-1064) -> build_params()
- _invoke_platform_insert_with_timeout (L1068-1118) -> invoke_insert()
- _invoke_platform_cancel_with_timeout (L1122-1168) -> invoke_cancel()
- bind_platform_apis (L864-908) -> bind_platform_apis()

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.2
迁移映射: 第四部分 项 5, 12, 13, 14, 15
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict

from infra.shared_utils import CHINA_TZ
from order.order_base import OrderResult
from order.execution_backend_base import OrderExecutionBackend


class LiveExecutionBackend(OrderExecutionBackend):
    """生产下单后端 — 调用实盘平台 API

    特点:
    - supports_dry_run() 返回 False (走完整 _pre_send_checks 校验)
    - 调用 svc._platform_insert_order 实际下单
    - 包含 R37-TRADING-RECOVERY 平仓恢复重试逻辑
    - 包含 R33-PID 内部 ID 忽略逻辑
    - 包含断路器恢复逻辑
    """

    def __init__(self, orchestrator: Any = None):
        super().__init__(orchestrator=orchestrator)

    def supports_dry_run(self) -> bool:
        return False

    def execute(self, ctx: Any) -> OrderResult:
        """阶段2: 平台下单 — 构建参数 + 超时调用 + 结果归一化

        迁移自 _OrderExecutorPlatformMethods._execute_platform_insert (L20-449)
        仅保留生产分支 (L216-399)，dry_run 分支已迁移到 DryRunExecutionBackend
        """
        svc = self._svc

        try:
            ctx.order_id = svc._generate_order_id()

            ctx.order = {
                'order_id': ctx.order_id,
                'instrument_id': ctx.instrument_id,
                'exchange': ctx.exchange,
                'volume': ctx.volume,
                'price': ctx.price,
                'direction': ctx.direction,
                'action': ctx.action,
                'status': 'SUBMITTED',
                'traded_volume': 0,
                'signal_id': ctx.signal_id,
                'open_reason': ctx.open_reason,
                'decision_score': ctx.decision_score,
                'position_scale': ctx.position_scale,
                'decision_action': ctx.decision_action,
                'dimension_scores': ctx.dimension_scores or {},
                'dimension_weights': ctx.dimension_weights or {},
                'created_at': datetime.now(CHINA_TZ),
                'updated_at': datetime.now(CHINA_TZ),
            }

            signed_order = svc.authenticator.generate_signed_request(ctx.order)
            if not signed_order:
                ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'sign_failed', '请求签名失败'
                return ctx
            ctx.order = signed_order

            _slippage_bps = svc._estimate_slippage(ctx.instrument_id, ctx.price, int(ctx.volume))
            logging.info("[R16-P0-005] 滑点估算: instrument=%s slippage=%.2fbps", ctx.instrument_id, _slippage_bps)

            with svc._lock:
                if ctx.idempotent_key in svc._order_idempotent_set:
                    if ctx.action in ('CLOSE', 'close'):
                        svc._order_idempotent_set.discard(ctx.idempotent_key)
                        # FIX-R28-LOG-NOISE: 同一合约5分钟内仅打印1次CLOSE-RETRY日志
                        _r28_inst = ctx.instrument_id
                        _r28_log_key = f'_r28_close_retry_logged_{_r28_inst}'
                        _r28_last_ts = getattr(svc, _r28_log_key, 0)
                        if (time.time() - _r28_last_ts) > 300:
                            logging.warning("[R28-CLOSE-RETRY] CLOSE订单幂等key已移除，允许重试: %s", ctx.idempotent_key)
                            setattr(svc, _r28_log_key, time.time())
                    else:
                        logging.warning("[R27-P0-RC-03] 幂等去重拦截(锁内): %s", ctx.idempotent_key)
                        ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'idempotent_duplicate', f'幂等去重拦截: {ctx.idempotent_key}'
                        return ctx

                svc._order_idempotent_set.add(ctx.idempotent_key)
                svc._persist_idempotent_key(ctx.idempotent_key)

                if len(svc._orders_by_id) >= svc._MAX_ORDERS_TRACKED:
                    _forced_remove = [oid for oid, o in svc._orders_by_id.items()
                                     if o['status'] in ('FILLED', 'CANCELLED', 'FAILED', 'ORPHANED', '全部成交', '已撤销', '部成部撤', 'ALL_FILLED')]
                    if _forced_remove:
                        for _foid in _forced_remove:
                            _fo = svc._orders_by_id[_foid]
                            _fa = _fo.get('action', '')
                            # FIX-R28: CLOSE订单的key含signal_id，同步构造以正确移除
                            if _fa in ('CLOSE', 'close'):
                                _fo_sig = _fo.get('signal_id', '')
                                _fo_sig_suffix = f"_{_fo_sig}" if _fo_sig else ""
                                svc._order_idempotent_set.discard(
                                    f"{_fo.get('instrument_id', '')}_{_fo.get('direction', '')}_{_fa}_{_fo.get('volume', '')}_{round(_fo.get('price', 0), 4)}{_fo_sig_suffix}")
                            else:
                                _fo_sig = _fo.get('signal_id', '')
                                _fo_sig_suffix = f"_{_fo_sig}" if _fo_sig else ""
                                svc._order_idempotent_set.discard(
                                    f"{_fo.get('instrument_id', '')}_{_fo.get('direction', '')}_{_fa}_{_fo.get('volume', '')}_{round(_fo.get('price', 0), 4)}{_fo_sig_suffix}")
                            del svc._orders_by_id[_foid]
                    else:
                        svc._order_idempotent_set.discard(ctx.idempotent_key)
                        ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'capacity_exceeded', f'订单追踪容量超限({svc._MAX_ORDERS_TRACKED})'
                        return ctx

                svc._orders_by_id[ctx.order_id] = ctx.order
                svc._stats['total_orders'] += 1

            svc._wal_write(ctx.order_id, 'PENDING', ctx.order)
            svc._append_order_state(ctx.order_id, 'SUBMITTED', ctx.order)

            if svc._platform_insert_order and callable(svc._platform_insert_order):
                try:
                    from config.config_service import resolve_product_exchange
                    if not ctx.exchange:
                        ctx.exchange = resolve_product_exchange(ctx.instrument_id)

                    filtered_params = self.build_params(
                        order_id=ctx.order_id, instrument_id=ctx.instrument_id,
                        exchange=ctx.exchange, volume=ctx.volume,
                        price=ctx.price, direction=ctx.direction, action=ctx.action,
                    )

                    result = self.invoke_insert(filtered_params)

                    if result is None or (isinstance(result, int) and result < 0):
                        _trading_flag = getattr(svc._platform_insert_order, '__self__', None)
                        _is_trading = getattr(_trading_flag, 'trading', 'N/A') if _trading_flag else 'N/A'
                        # FIX-R37-TRADING-RECOVERY: 当trading=False时尝试恢复trading=True并重试一次
                        # FIX-R37-CLOSE-ONLY: 仅CLOSE订单允许恢复trading（必须平仓），OPEN订单尊重风控决策
                        if _is_trading is False and _trading_flag is not None:
                            _allow_recovery = ctx.action in ('CLOSE', 'close')
                            if _allow_recovery:
                                # FIX-R37-LOG-NOISE: 同一合约5分钟内仅打印1次RECOVERY日志
                                _recovery_log_key = f'_r37_recovery_logged_{ctx.instrument_id}'
                                _last_recovery_log_ts = getattr(svc, _recovery_log_key, 0)
                                _should_log_recovery = (time.time() - _last_recovery_log_ts) > 300
                                try:
                                    setattr(_trading_flag, 'trading', True)
                                    if _should_log_recovery:
                                        logging.warning("[R37-TRADING-RECOVERY] 检测到trading=False，已恢复trading=True，重试平仓: inst=%s dir=%s",
                                                        ctx.instrument_id, ctx.direction)
                                        setattr(svc, _recovery_log_key, time.time())
                                    result = self.invoke_insert(filtered_params)
                                except Exception as _recover_err:
                                    # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
                                    if _should_log_recovery:
                                        logging.warning("[R37-TRADING-RECOVERY] 恢复trading=True失败: %s", _recover_err)
                            else:
                                logging.warning("[R37-TRADING-BLOCK] trading=False且为OPEN订单，尊重风控决策不恢复: inst=%s dir=%s",
                                                ctx.instrument_id, ctx.direction)
                        if result is None or (isinstance(result, int) and result < 0):
                            # FIX-ERROR-LOG-NOISE: 同一合约5分钟内仅打印1次下单失败ERROR
                            _err_log_key = f'_order_fail_logged_{ctx.instrument_id}_{ctx.direction}'
                            _err_last_ts = getattr(svc, _err_log_key, 0)
                            if (time.time() - _err_last_ts) > 300:
                                if _is_trading is False:
                                    logging.debug("[OrderService] 平台下单返回失败(非交易时段，预期行为): result=%s %s %s trading=%s platform_func=%s",
                                                  result, ctx.instrument_id, ctx.direction, _is_trading,
                                                  getattr(svc._platform_insert_order, '__name__', str(type(svc._platform_insert_order))))
                                else:
                                    logging.error("[OrderService] 平台下单返回失败: result=%s %s %s trading=%s platform_func=%s",
                                                  result, ctx.instrument_id, ctx.direction, _is_trading,
                                                  getattr(svc._platform_insert_order, '__name__', str(type(svc._platform_insert_order))))
                                setattr(svc, _err_log_key, time.time())
                            # FIX-P0-1: trading=False时标记失败时间戳，供策略层过滤
                            if _is_trading is False:
                                try:
                                    _provider_ref = getattr(svc, '_provider_ref', None)
                                    if _provider_ref is not None:
                                        _fail_key = f'_p01_recent_fail_{ctx.instrument_id}_{ctx.action}'
                                        setattr(_provider_ref, _fail_key, time.time())
                                except Exception:
                                    # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
                                    pass
                            with svc._lock:
                                ctx.order['status'] = 'FAILED'
                                ctx.order['updated_at'] = datetime.now(CHINA_TZ)
                                svc._remove_order_and_idempotent_key(ctx.order_id, ctx.order)
                                svc._stats['failed_orders'] += 1
                                svc._consecutive_failures += 1
                            svc._wal_write(ctx.order_id, 'FAILED', ctx.order)
                            svc._append_order_state(ctx.order_id, 'FAILED', ctx.order)
                            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'platform_rejected', f'平台下单返回失败: result={result}'
                            return ctx

                    _platform_order_id = svc._normalize_platform_result(result).get('order_id')

                    # FIX-R33-PID: 忽略以ORD_开头的platform_order_id（内部ID被平台回传），
                    # 只接受真实的平台ID（如int或非ORD_开头的字符串）
                    if _platform_order_id and not str(_platform_order_id).startswith('ORD_'):
                        with svc._lock:
                            ctx.order['platform_order_id'] = _platform_order_id
                            svc._platform_id_to_order_id[str(_platform_order_id)] = ctx.order_id
                    elif _platform_order_id:
                        logging.warning("[R33-PID] 平台返回内部ID作为order_id，忽略: %s internal_id=%s",
                                       _platform_order_id, ctx.order_id)

                    logging.info("[OrderService] 平台下单成功: %s %s %s %d@%.2f", ctx.order_id, ctx.instrument_id, ctx.direction, int(ctx.volume), ctx.price)

                    svc._consecutive_failures = 0
                    svc._circuit_breaker_open = False
                    svc._circuit_breaker_half_open = False
                    svc._circuit_breaker_opened_at = 0.0

                    svc._wal_write(ctx.order_id, 'CONFIRMED', ctx.order)
                    svc._append_order_state(ctx.order_id, 'CONFIRMED', ctx.order)
                    svc._wal_delete(ctx.order_id)

                except Exception as e:
                    # 实时回调路径硬约束: except Exception (合并 TimeoutError 与一般异常处理)
                    # 原 TimeoutError 单独 handler 已合并以严格符合 NEW-1 硬约束
                    _is_timeout = isinstance(e, TimeoutError) or 'timeout' in str(e).lower()
                    if _is_timeout:
                        logging.error("[R27-P0-DR-01] 平台下单超时: %s %s", ctx.instrument_id, e)
                        _order_status = 'TIMEOUT'
                        _reject_code = 'platform_timeout'
                        _reject_msg = f'平台下单超时: {e}'
                    else:
                        logging.error("[OrderService] 平台下单失败: %s %s", ctx.instrument_id, e)
                        _order_status = 'FAILED'
                        _reject_code = 'platform_error'
                        _reject_msg = f'平台下单失败: {e}'
                    with svc._lock:
                        ctx.order['status'] = _order_status
                        ctx.order['updated_at'] = datetime.now(CHINA_TZ)
                        if _is_timeout:
                            ctx.order['timeout_reason'] = str(e)
                        # FIX-R28: 超时后移除idempotent_key，允许重试平仓
                        svc._remove_order_and_idempotent_key(ctx.order_id, ctx.order)
                        svc._stats['failed_orders'] += 1
                        svc._consecutive_failures += 1
                        if svc._consecutive_failures >= svc._circuit_breaker_threshold:
                            svc._circuit_breaker_open = True
                            svc._circuit_breaker_opened_at = time.time()
                    svc._wal_write(ctx.order_id, _order_status, ctx.order)
                    svc._append_order_state(ctx.order_id, _order_status, ctx.order)
                    ctx.rejected, ctx.reject_code, ctx.reject_message = True, _reject_code, _reject_msg
                    return ctx

            else:
                logging.info("[OrderService] 模拟下单: %s %s %s %d@%.2f", ctx.order_id, ctx.instrument_id[:3] + '***', ctx.direction[:1], int(ctx.volume), ctx.price)

        except Exception as e:
            # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
            logging.error("[OrderService] Send order error: %s", e)
            with svc._lock:
                try:
                    if ctx.order_id and ctx.order_id in svc._orders_by_id:
                        svc._remove_order_and_idempotent_key(ctx.order_id, svc._orders_by_id.get(ctx.order_id, {}))
                except Exception:
                    # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                svc._stats['failed_orders'] += 1
                svc._consecutive_failures += 1
                if svc._consecutive_failures >= svc._circuit_breaker_threshold:
                    svc._circuit_breaker_open = True
            try:
                if ctx.order_id:
                    svc._wal_write(ctx.order_id, 'FAILED', {'order_id': ctx.order_id, 'instrument_id': ctx.instrument_id, 'direction': ctx.direction, 'volume': ctx.volume, 'price': ctx.price})
                    svc._append_order_state(ctx.order_id, 'FAILED', {'order_id': ctx.order_id, 'status': 'FAILED'})
            except Exception:
                # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
            ctx.rejected, ctx.reject_code, ctx.reject_message = True, 'send_error', f'下单异常: {e}'
            return ctx

        # 委托编排层后置持久化
        return self._orchestrator._post_send_persist(ctx)

    def build_params(self, *, order_id: str, instrument_id: str, exchange: str,
                     volume: float, price: float, direction: str,
                     action: str) -> Dict[str, Any]:
        """构建平台下单参数 — 迁移自 _build_platform_insert_params (L996-1064)"""
        svc = self._svc

        _order_direction = 'buy' if direction == 'BUY' else ('sell' if direction == 'SELL' else direction.lower())
        _offset = '0' if action == 'OPEN' else ('1' if action in ('CLOSE', 'close') else ('3' if action == 'CLOSE_TODAY' else action))

        all_params = {
            'exchange': exchange,
            'instrument_id': instrument_id,
            'volume': int(volume),
            'price': price,
            'order_direction': _order_direction,
            'offset': _offset,
            'order_type': 'GFD',
            'investor': '',
            'hedgeflag': '1',
            'memo': order_id,
        }

        if svc._platform_insert_order_params:
            mapped_params = {}
            for key, value in all_params.items():
                if key in svc._platform_insert_order_params:
                    mapped_params[key] = value
            if 'offset' not in mapped_params and 'offset' in all_params:
                mapped_params['offset'] = all_params['offset']
            return mapped_params
        return all_params

    def invoke_insert(self, filtered_params: Dict[str, Any]) -> Any:
        """超时控制下单 — 迁移自 _invoke_platform_insert_with_timeout (L1068-1118)"""
        svc = self._svc
        result_holder: Dict[str, Any] = {}
        error_holder: Dict[str, BaseException] = {}
        done = threading.Event()

        def _target() -> None:
            try:
                result_holder['result'] = svc._platform_insert_order(**filtered_params)
            except BaseException as exc:
                error_holder['error'] = exc
            finally:
                done.set()

        worker = threading.Thread(target=_target, name='OrderServicePlatformInsert', daemon=True)
        worker.start()
        timeout_seconds = max(float(svc._platform_submit_timeout_seconds), 0.01)
        if not done.wait(timeout_seconds):
            raise TimeoutError(f'platform insert timeout after {timeout_seconds:.2f}s')
        if 'error' in error_holder:
            raise error_holder['error']
        return result_holder.get('result')

    def invoke_cancel(self, platform_id: str) -> None:
        """超时控制撤单 — 迁移自 _invoke_platform_cancel_with_timeout (L1122-1168)"""
        svc = self._svc
        error_holder: Dict[str, BaseException] = {}
        done = threading.Event()

        def _target() -> None:
            try:
                svc._platform_cancel_order(platform_id)
            except BaseException as exc:
                error_holder['error'] = exc
            finally:
                done.set()

        worker = threading.Thread(target=_target, name='OrderServicePlatformCancel', daemon=True)
        worker.start()
        cancel_timeout = svc._operation_timeouts.get('cancel', 2.0)
        if not done.wait(cancel_timeout):
            raise TimeoutError(f'[R33-P1-12] platform cancel timeout after {cancel_timeout:.2f}s, platform_id={platform_id}')
        if 'error' in error_holder:
            raise error_holder['error']

    def bind_platform_apis(self, insert_order_func, cancel_order_func) -> None:
        """绑定平台下单/撤单API — 迁移自 bind_platform_apis (L864-908)"""
        svc = self._svc
        svc._platform_insert_order = insert_order_func
        svc._platform_cancel_order = cancel_order_func
        svc._platform_insert_order_params = set()

        if insert_order_func and callable(insert_order_func):
            svc._platform_api_ready = True
            try:
                sig = inspect.signature(insert_order_func)
                has_var_keyword = any(
                    p.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
                    for p in sig.parameters.values()
                )
                if has_var_keyword:
                    svc._platform_insert_order_params = set()
                    logging.info("[OrderService] 平台下单API接受*args/**kwargs，跳过参数过滤 %s", list(sig.parameters.keys()))
                else:
                    svc._platform_insert_order_params = set(sig.parameters.keys())
                    logging.info("[OrderService] 平台下单API参数签名: %s", list(sig.parameters.keys()))
            except Exception as e:
                # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
                logging.warning("[OrderService] 无法检测平台API签名: %s", e)

        logging.info("[OrderService] 平台下单/撤单API已绑定")


__all__ = ['LiveExecutionBackend']
