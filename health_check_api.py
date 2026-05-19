import logging
import json
import time
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


class StructuredJsonlLogger:
    def __init__(self, log_dir: str = "logs"):
        self._log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._signal_log_path = os.path.join(log_dir, "signals.jsonl")
        self._order_log_path = os.path.join(log_dir, "orders.jsonl")

    def log_signal(self, signal: Dict[str, Any]) -> None:
        record = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "signal",
            "signal_id": signal.get("signal_id", ""),
            "instrument_id": signal.get("instrument_id", ""),
            "direction": signal.get("direction", ""),
            "strength": signal.get("strength", 0.0),
            "estimated_plr": signal.get("estimated_plr", 0.0),
            "decision_score": signal.get("decision_score", 0.0),
            "position_scale": signal.get("position_scale", 1.0),
            "filtered": signal.get("filtered", False),
            "filter_reason": signal.get("filter_reason", ""),
        }
        with open(self._signal_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def log_order(self, order: Dict[str, Any]) -> None:
        record = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "order",
            "order_id": order.get("order_id", ""),
            "instrument_id": order.get("instrument_id", ""),
            "direction": order.get("direction", ""),
            "price": order.get("price", 0.0),
            "volume": order.get("volume", 0),
            "order_type": order.get("order_type", ""),
            "status": order.get("status", ""),
            "filled_volume": order.get("filled_volume", 0),
            "remaining_volume": order.get("remaining_volume", 0),
        }
        with open(self._order_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def log_close(self, close_record: Dict[str, Any]) -> None:
        record = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "close",
            "position_id": close_record.get("position_id", ""),
            "instrument_id": close_record.get("instrument_id", ""),
            "pnl": close_record.get("pnl", 0.0),
            "close_reason": close_record.get("close_reason", ""),
            "hold_time_minutes": close_record.get("hold_time_minutes", 0.0),
        }
        with open(self._order_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


class HealthCheckAPI:
    # ✅ 健康检查阈值参数化（P1-D组后续完善）
    DEFAULT_SIGNAL_STALE_SECONDS = 300.0
    DEFAULT_ORDER_STALE_SECONDS = 600.0
    DEFAULT_DAILY_DRAWDOWN_THRESHOLD = 0.05
    DEFAULT_MAX_OPEN_POSITIONS = 10

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}
        self._component_status: Dict[str, str] = {
            "data_feeds": "unknown",
            "greeks_calculator": "unknown",
            "state_diagnosis": "unknown",
            "signal_generator": "unknown",
            "risk_manager": "unknown",
            "order_executor": "unknown",
        }
        self._open_position_count: int = 0
        self._last_signal_time: Optional[float] = None
        self._last_order_time: Optional[float] = None

        # 从ConfigService统一配置中心加载阈值
        self._signal_stale_seconds = self.DEFAULT_SIGNAL_STALE_SECONDS
        self._order_stale_seconds = self.DEFAULT_ORDER_STALE_SECONDS
        try:
            from ali2026v3_trading.config_service import ConfigService
            cs = ConfigService()
            self._signal_stale_seconds = cfg.get(
                'signal_stale_seconds',
                getattr(cs.trading, 'strategy_heartbeat_interval_seconds', self.DEFAULT_SIGNAL_STALE_SECONDS) * 10
            )
            self._order_stale_seconds = cfg.get(
                'order_stale_seconds',
                getattr(cs.trading, 'strategy_heartbeat_interval_seconds', self.DEFAULT_ORDER_STALE_SECONDS) * 20
            )
        except Exception:
            pass

        self._alert_thresholds = {
            "l1_circuit_breaker": cfg.get('l1_circuit_breaker', True),
            "daily_drawdown": cfg.get('daily_drawdown_threshold', self.DEFAULT_DAILY_DRAWDOWN_THRESHOLD),
            "max_open_positions": cfg.get('max_open_positions', self.DEFAULT_MAX_OPEN_POSITIONS),
        }

    def update_component_status(self, component: str, status: str) -> None:
        if component in self._component_status:
            self._component_status[component] = status

    def update_open_position_count(self, count: int) -> None:
        self._open_position_count = count

    def update_last_signal_time(self, timestamp: float = None) -> None:
        self._last_signal_time = timestamp if timestamp is not None else time.time()

    def update_last_order_time(self, timestamp: float = None) -> None:
        self._last_order_time = timestamp if timestamp is not None else time.time()

    def get_health_status(self) -> Dict[str, Any]:
        now = time.time()
        signal_stale = False
        if self._last_signal_time is not None:
            signal_stale = (now - self._last_signal_time) > self._signal_stale_seconds

        order_stale = False
        if self._last_order_time is not None:
            order_stale = (now - self._last_order_time) > self._order_stale_seconds
        
        overall = "healthy"
        if any(s != "ok" for s in self._component_status.values()):
            overall = "degraded"
        if signal_stale or order_stale:
            overall = "degraded"
        if self._open_position_count > self._alert_thresholds["max_open_positions"]:
            overall = "warning"
        
        return {
            "overall_status": overall,
            "component_status": dict(self._component_status),
            "open_position_count": self._open_position_count,
            "last_signal_time": self._last_signal_time,
            "last_order_time": self._last_order_time,
            "signal_stale": signal_stale,
            "order_stale": order_stale,
            "alert_thresholds": dict(self._alert_thresholds),
            "timestamp": now,
        }

    def check_l1_alerts(self, daily_drawdown_pct: float = 0.0) -> List[str]:
        alerts = []
        if daily_drawdown_pct > self._alert_thresholds["daily_drawdown"]:
            alerts.append(f"L1_ALERT: daily_drawdown={daily_drawdown_pct:.2%} exceeds threshold={self._alert_thresholds['daily_drawdown']:.2%}")
        if self._open_position_count > self._alert_thresholds["max_open_positions"]:
            alerts.append(f"L1_ALERT: open_positions={self._open_position_count} exceeds max={self._alert_thresholds['max_open_positions']}")
        return alerts
