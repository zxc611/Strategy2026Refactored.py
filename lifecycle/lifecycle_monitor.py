# MODULE_ID: M1-124
"""lifecycle_monitor.py — 状态查询/性能监控/健康检查（从strategy_lifecycle_mixin.py拆分）
职责: get_state, is_running, is_paused, is_trading, get_uptime, record_tick/trade/signal/error,
      get_stats, health_check, _should_probe_t_type_future, _log_t_type_future_probe
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from infra.shared_utils import CHINA_TZ
from lifecycle.lifecycle_state_machine import StrategyState


# [P2-10-LM] 职责: 策略生命周期状态查询+性能统计+基础健康检查
# 与strategy/strategy_monitoring_layer.py的StrategyMonitoringLayer分工: LM=状态查询, SML=风控层健康检查+紧急停止
class LifecycleMonitor:
    def __init__(self, provider):
        self.p = provider

    def get_state(self) -> StrategyState:
        return self.p._state

    def is_running(self) -> bool:
        return self.p._is_running and not self.p._is_paused

    def is_paused(self) -> bool:
        return self.p._is_paused

    def is_trading(self) -> bool:
        from infra.scheduler_service import is_market_open
        if is_market_open():
            return True
        return self.p._is_trading

    def get_uptime(self) -> float:
        start_time = self.p._stats.get('start_time')
        if not start_time:
            return 0.0
        elapsed = (datetime.now(CHINA_TZ) - start_time).total_seconds()
        return elapsed

    def record_tick(self) -> None:
        with self.p._lock:
            self.p._stats['total_ticks'] += 1

    def record_trade(self) -> None:
        with self.p._lock:
            self.p._stats['total_trades'] += 1

    def record_signal(self, target: Optional[Dict[str, Any]] = None) -> None:
        """记录信号数量，可选传递target信息用于信号统计和诊断。

        FIX-P0-1-RC13 (2026-07-23): 原record_signal只递增计数器不传递target内容，
        导致五态排序结果被丢弃。现增加target参数，支持:
        - 按signal_source(A/B/C)统计信号分布
        - 按tier统计信号质量分布
        - 记录前N个信号的关键字段用于诊断
        """
        with self.p._lock:
            self.p._stats['total_signals'] += 1
            # FIX-P0-1-RC13: 信号源统计
            # FIX-E2 (2026-07-23): 使用普通dict替代defaultdict，避免序列化风险和意外键创建
            if target:
                _signal_stats = self.p._stats.setdefault('signal_stats', {
                    'by_source': {},                # {'A': N, 'B': N, 'C': N}
                    'by_tier': {},                  # {1: N, 2: N, 3: N, 4: N}
                    'by_direction': {},             # {'BUY': N, 'SELL': N}
                    'last_targets': [],             # 最近N个target快照
                    'first_seen_at': time.time(),
                })
                _ss = _signal_stats
                _src = target.get('signal_source', 'unknown')
                _tier = target.get('tier', 4)
                _dir = target.get('direction', 'unknown')
                _ss['by_source'][_src] = _ss['by_source'].get(_src, 0) + 1
                _ss['by_tier'][_tier] = _ss['by_tier'].get(_tier, 0) + 1
                _ss['by_direction'][_dir] = _ss['by_direction'].get(_dir, 0) + 1
                # 保留最近20个target快照用于诊断
                if len(_ss['last_targets']) < 20:
                    _ss['last_targets'].append({
                        'instrument_id': target.get('option_instrument_id', target.get('instrument_id', '')),
                        'future_id': target.get('underlying_future_instrument_id', ''),
                        'source': _src,
                        'tier': _tier,
                        'direction': _dir,
                        'rank': target.get('product_rank', 0),
                        'correct_up_pct': target.get('correct_up_pct', 0.0),
                        'premium': target.get('premium_price', target.get('price', 0.0)),
                    })
                # 首次信号时记录关键信息
                _total = self.p._stats['total_signals']
                if _total <= 5 or _total % 500 == 0:
                    _inst = target.get('option_instrument_id', target.get('instrument_id', 'N/A'))
                    logging.info(
                        "[SignalRecord] #%d inst=%s src=%s tier=%s dir=%s rank=%d "
                        "correct_up=%.3f premium=%.4f",
                        _total, _inst, _src, _tier, _dir,
                        target.get('product_rank', 0),
                        target.get('correct_up_pct', 0.0),
                        target.get('premium_price', target.get('price', 0.0)),
                    )

    def record_error(self, error_message: str) -> None:
        with self.p._lock:
            self.p._stats['errors_count'] += 1
            self.p._stats['last_error_time'] = datetime.now(CHINA_TZ)
            self.p._stats['last_error_message'] = error_message

    def get_stats(self) -> Dict[str, Any]:
        with self.p._lock:
            uptime = self.get_uptime()
            ticks_per_second = self.p._stats['total_ticks'] / uptime if uptime > 0 else 0
            state_val = getattr(self.p, '_state', None)
            state_str = state_val.value if state_val is not None and hasattr(state_val, 'value') else 'UNKNOWN'
            return {
                'service_name': 'StrategyCoreService',
                **self.p._stats,
                'uptime_seconds': uptime,
                'ticks_per_second': ticks_per_second,
                'state': state_str,
                'is_running': getattr(self.p, '_is_running', False),
                'is_paused': getattr(self.p, '_is_paused', False)
            }

    def health_check(self) -> Dict[str, Any]:
        p = self.p
        issues = []
        status = 'healthy'
        if p._state == StrategyState.ERROR:
            issues.append("Strategy in ERROR state")
            status = 'unhealthy'
        if p._state == StrategyState.DEGRADED:
            issues.append("Strategy in DEGRADED state: API未就绪或订阅部分失败")
            status = 'degraded' if status == 'healthy' else status
        uptime = self.get_uptime()
        if uptime > 60:
            error_rate = p._stats['errors_count'] / uptime
            if error_rate > 0.1:
                issues.append(f"High error rate: {error_rate*60:.2f}/min")
                status = 'warning' if status == 'healthy' else 'unhealthy'
        e2e = p._e2e_counters
        if uptime > 30 and e2e['configured_instruments'] > 0:
            if e2e['first_tick_received'] == 0:
                issues.append("E2E: 运行30秒后仍未收到任何Tick")
                status = 'warning' if status == 'healthy' else status
            if e2e['preregistered_instruments'] == 0:
                issues.append("E2E: 未完成任何合约预注册")
                status = 'warning' if status == 'healthy' else status
            if e2e['kline_persisted_count'] == 0 and uptime > 60:
                issues.append("E2E: 运行60秒后仍无K线数据落盘")
                status = 'warning' if status == 'healthy' else status
        return {
            'status': status,
            'issues': issues,
            'strategy_id': p.strategy_id,
            'state': p._state.value,
            'uptime_seconds': uptime,
            'e2e_counters': e2e,
            'e2e_shard_enqueued': dict(p._e2e_shard_enqueued),
            'e2e_shard_persisted': dict(p._e2e_shard_persisted),
            'timestamp': datetime.now(CHINA_TZ).isoformat()
        }

    @staticmethod
    def _should_probe_t_type_future(product: str) -> bool:
        return str(product or '') in {'AL', 'IH'}

    def _log_t_type_future_probe(self, phase: str, instrument_id: str, product: str, month: str, last_price: float) -> None:
        if not self._should_probe_t_type_future(product):
            return
        logging.info(
            "[TTypeFutureProbe] phase=%s instrument=%s product=%s month=%s price=%s",
            phase, instrument_id, product, month, last_price,
        )