"""
shadow_strategy_pnl.py - PnL and evaluation mixin (Facade)

Responsibilities:
- ShadowStrategyPnLMixin: Degradation, health, stats (inherits Metrics from shadow_strategy_pnl_metrics)

Source: split from _shadow_services.py ShadowMetricsService+ShadowDegradationService per doc 12.3.4
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.strategy._shadow_strategy_pnl_metrics import ShadowStrategyPnLMetricsService, ShadowStrategyPnLMetricsMixin
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.serialization_utils import json_dumps

logger = get_logger(__name__)  # R9-5


class ShadowStrategyPnLService(ShadowStrategyPnLMetricsService):
    """PnL and evaluation service (from ShadowStrategyPnLMixin)"""

    def __init__(self, facade=None):
        super().__init__()
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # === Degradation methods from ShadowDegradationService ===
    # RESIDUAL: self._engine = engine  (original line 623, kept as-is)

    def is_degradation_active(self) -> bool:
        with self._lock:
            _active = self._degradation_active
            if _active:
                logger.debug("[R23-SM-08-FIX] 降级激活: degradation_active=%s ev_pause=%s "
                             "alpha_decay_rate=%s consecutive_losses=%s",
                             _active, self._absolute_ev_pause,
                             getattr(self, '_alpha_decay_rate', None),
                             getattr(self, '_consecutive_losses', None))
            return _active
    def is_absolute_ev_paused(self) -> bool:
        with self._lock:
            _paused = self._absolute_ev_pause
            if _paused:
                logger.debug("[R23-SM-08-FIX] 绝对EV暂停: ev_pause=%s degradation_active=%s "
                             "current_ev=%s ev_threshold=%s",
                             _paused, self._degradation_active,
                             getattr(self, '_current_expected_value', None),
                             getattr(self, '_ev_pause_threshold', None))
            return _paused
    def clear_degradation(self) -> None:
        with self._lock:
            self._degradation_active = False
            logger.info("[ShadowStrategyEngine] Alpha衰减降级已清除")
    def clear_absolute_ev_pause(self) -> None:
        with self._lock:
            self._absolute_ev_pause = False
            logger.info("[ShadowStrategyEngine] 绝对期望值暂停已解除")
    def set_trades_max_len(self, max_len: int) -> None:
        """设置交易记录上限（运行时可配置）"""
        if max_len > 0:
            self._trades_max_len = max_len
            logger.info("[ShadowStrategyEngine] R13-P2-LOG-16: 交易记录上限设为%d", max_len)
    def record_governance_degradation(self, reasons: List[str]) -> None:
        """记录治理引擎触发的降级事件到影子引擎日志。"""
        with self._lock:
            self._stats['governance_degradations'] = self._stats.get('governance_degradations', 0) + 1
            event = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'type': 'governance_degradation',
                'reasons': reasons,
                'alpha_ratio': self.get_alpha_ratio(),
            }
            self._jsonl_logger.info(json_dumps(event))
            logger.critical(
                "[ShadowStrategyEngine] 治理引擎降级事件已记录: %s",
                '; '.join(reasons)
            )
    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            # R10-P1-03: _last_alpha_metrics读路径在锁内，线程安全
            alpha_ratio = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            master_ev = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            last_alpha_ts = self._last_alpha_metrics.timestamp if self._last_alpha_metrics else None
            # P-32修复: 三态健康检查 OK/WARNING/CRITICAL（手册12.1节）
            if self._absolute_ev_pause:
                status = "CRITICAL"
            elif self._degradation_active or alpha_ratio < 1.0:
                status = "WARNING"
            else:
                status = "OK"
            result = {
                'component': 'shadow_strategy_engine',
                'status': status,
                'params_locked': self._params_locked,
                'degradation_active': self._degradation_active,
                'absolute_ev_pause': self._absolute_ev_pause,
                'shadow_a_trades': self._stats['shadow_a_trades'],
                'shadow_b_trades': self._stats['shadow_b_trades'],
                'master_trades': self._stats['master_trades'],
                'alpha_ratio': alpha_ratio,
                'master_expected_value': master_ev,
                'last_alpha_calculation': last_alpha_ts,
            }
            # P1-R10-05修复: 组件检查异常传播到返回值
            if status in ('CRITICAL', 'WARNING'):
                result['error_propagation'] = True
                result['action_required'] = (
                    'halt_new_open' if status == 'CRITICAL' else 'review_and_confirm'
                )
            return result
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['alpha_ratio'] = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            stats['master_expected_value'] = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            stats['degradation_active'] = self._degradation_active
            stats['absolute_ev_pause'] = self._absolute_ev_pause
            _signal_svc = None
            if hasattr(self, '_facade') and self._facade is not None:
                _signal_svc = getattr(self._facade, '_signal_service', None)
                if _signal_svc is None:
                    _signal_svc = getattr(self._facade, '__dict__', {}).get('_signal_service')
            if _signal_svc is not None:
                stats['master_pnl_sum'] = getattr(_signal_svc, '_master_pnl_sum', 0.0)
                stats['shadow_a_pnl_sum'] = getattr(_signal_svc, '_shadow_a_pnl_sum', 0.0)
                stats['shadow_b_pnl_sum'] = getattr(_signal_svc, '_shadow_b_pnl_sum', 0.0)
                stats['is_shadow_mode'] = getattr(_signal_svc, '_is_shadow_mode', False)
            else:
                stats['master_pnl_sum'] = self._master_pnl_sum
                stats['shadow_a_pnl_sum'] = self._shadow_a_pnl_sum
                stats['shadow_b_pnl_sum'] = self._shadow_b_pnl_sum
                stats['is_shadow_mode'] = self._is_shadow_mode
            stats['paper_account_equity'] = self._paper_account['current_equity']
            stats['paper_account_equity_masked'] = self._mask_sensitive_value(self._paper_account['current_equity'])
            return stats

__all__ = ['ShadowStrategyPnLService', 'ShadowStrategyPnLMixin']
ShadowStrategyPnLMixin = ShadowStrategyPnLService