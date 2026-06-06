"""
信号服务模块 - CQRS 架构 Command 层
来源：12_trading_logic.py (部分)
功能：信号生成 + 冷却管理 + 信号历史
行数：~300 行 (-25% vs 原 400 行)

优化 v1.1 (2026-03-16):
- ✅ 集成 EventBus 发布信号事件
- ✅ 添加数据源适配层接口
- ✅ 性能基准测试支持

优化 v2.0 (2026-05-12):
- ✅ 集成HFT增强: 信号时序滤波(Kalman/EMA)
- ✅ 共振强度平滑值+变化速度双重条件
"""
from __future__ import annotations

import threading
import logging
import math
import uuid
import os
import json
from datetime import datetime
import time
from typing import Any, Dict, List, Optional, Callable, Tuple, Deque
from collections import deque

from ali2026v3_trading.shared_utils import SignalType, VALID_SIGNAL_TYPES, OPEN_SIGNAL_TYPES, CLOSE_SIGNAL_TYPES
from ali2026v3_trading.signal_history_service import SignalHistoryService
from ali2026v3_trading.signal_filter_chain import SignalFilterChain
from ali2026v3_trading.cooldown_manager import CooldownManager

try:
    from ali2026v3_trading.serialization_utils import json_dumps
except ImportError:
    json_dumps = None

try:
    from ali2026v3_trading.audit_log_utils import structured_audit_log as _structured_audit_log  # R27-CP-05-FIX
except ImportError:
    _structured_audit_log = None

try:
    from ali2026v3_trading.causal_chain_utils import CausalChainTracker
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False

# 导入 EventBus（可选依赖）
try:
    from ali2026v3_trading.event_bus import EventBus, get_global_event_bus
    _HAS_EVENT_BUS = True
except ImportError as e:
    logging.warning("[SignalService] Failed to import EventBus: %s", e)
    _HAS_EVENT_BUS = False
    EventBus = None


class SignalState:
    """R23-SM-02-FIX: 信号状态机 — 6阶段生命周期"""
    GENERATED = 'GENERATED'
    FILTERED = 'FILTERED'
    SCORED = 'SCORED'
    EXECUTED = 'EXECUTED'
    CONFIRMED = 'CONFIRMED'
    COMPLETED = 'COMPLETED'
    EXPIRED = 'EXPIRED'
    REJECTED = 'REJECTED'

SIGNAL_STATE_TRANSITIONS = {
    SignalState.GENERATED: [SignalState.FILTERED, SignalState.EXPIRED, SignalState.REJECTED],
    SignalState.FILTERED: [SignalState.SCORED, SignalState.EXPIRED, SignalState.REJECTED],
    SignalState.SCORED: [SignalState.EXECUTED, SignalState.EXPIRED, SignalState.REJECTED],
    SignalState.EXECUTED: [SignalState.CONFIRMED, SignalState.EXPIRED],
    SignalState.CONFIRMED: [SignalState.COMPLETED],
    SignalState.COMPLETED: [],
    SignalState.EXPIRED: [],
    SignalState.REJECTED: [],
}


class SignalService:
    """信号服务 - Command 层
    
    职责:
    - 信号生成（开仓/平仓）
    - 冷却时间管理
    - 信号历史记录
    - 信号过滤
    
    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 事件驱动
    """
    
    SIGNAL_HISTORY_MAX_LEN = 1000
    DEFAULT_COOLDOWN_SECONDS = 60.0
    CLEANUP_INTERVAL_SECONDS = 300
    _CONFIG_COOLDOWN_KEY = 'signal_cooldown_sec'  # R16-P0-CFG-01修复: 与config_params.py键名对齐(去掉多余的s)
    _CONFIG_CLEANUP_KEY = 'signal_cleanup_interval_seconds'
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE_START = 15
    MARKET_CLOSE_MINUTE_END = 20
    DEFAULT_ORDER_FLOW_CONSISTENCY = 0.5

    # SIG-02修复: 信号延迟预算阈值（P50/P99）
    LATENCY_BUDGET_MS = {
        'signal_generation': {'p50': 20.0, 'p99': 50.0},   # T1→T2 信号生成
        'event_bus_publish': {'p50': 2.0, 'p99': 10.0},     # T2→T3 信号发布
        'schedule_cycle': {'p50': 10.0, 'p99': 30.0},       # T3→T4 交易周期
        'risk_check': {'p50': 15.0, 'p99': 50.0},           # T4→T6 风控检查
        'order_submit': {'p50': 3.0, 'p99': 10.0},          # T6→T7 订单提交
    }
    SIGNAL_HALF_LIFE_MS = 5000.0  # 信号半衰期（毫秒），高频策略约5秒

    def __init__(self, event_bus: Optional[EventBus] = None, default_order_flow_consistency: float = 0.5):
        """初始化信号服务

        Args:
            event_bus: 事件总线实例（可选，用于发布信号事件）
        """
        # 信号历史
        self._history_service = SignalHistoryService(max_history=self.SIGNAL_HISTORY_MAX_LEN)
        self._signal_history = self._history_service._history

        # 冷却时间管理（存储信号时刻，非冷却结束时刻）
        self._cooldown_times: Dict[str, float] = {}
        self._cooldown_durations: Dict[str, float] = {}
        self._last_cleanup = time.time()
        self._DEDUP_HARD_LIMIT = 1000  # PF-05修复: 冷却缓存硬上限
        self._DEDUP_EVICT_COUNT = 500   # PF-05修复: 超限时淘汰数量

        # R4-P-04修复: 信号短时TTL去重缓存（5s内相同instrument_id+signal_type的信号去重）
        self._signal_dedup_cache: Dict[str, float] = {}
        self._DEDUP_TTL_SECONDS = 5.0
        self._DEDUP_CACHE_HARD_LIMIT = 500

        # 信号统计
        self._stats = {
            'total_signals': 0,
            'filtered_signals': 0,
            'plr_filtered': 0,
            'mode_filtered': 0,
            'cooldown_filtered': 0,
            'decision_filtered': 0,
            'emitted_signals': 0,
            'dedup_filtered': 0,
            'last_signal_time': None
        }

        # 线程锁
        self._lock = threading.RLock()

        # R27-P0-DR-10修复: 信号队列背压保护——使用BoundedQueue限制待处理信号数
        from collections import deque as _deque
        self._signal_queue_max_size = 2000
        self._signal_queue: _deque = _deque(maxlen=self._signal_queue_max_size)
        self._signal_drop_count: int = 0

        # R23-SM-02-FIX: 信号状态追踪
        self._signal_states: Dict[str, str] = {}
        # R23-FR-05-FIX: 信号过期管理
        self._signal_max_age_sec: float = 180.0
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _params = get_cached_params()
            if isinstance(_params, dict) and 'signal_max_age_sec' in _params:
                self._signal_max_age_sec = float(_params['signal_max_age_sec'])
        except Exception:
            pass

        self._default_cooldown_seconds = self.DEFAULT_COOLDOWN_SECONDS
        self._cleanup_interval = self.CLEANUP_INTERVAL_SECONDS
        self._default_order_flow_consistency = default_order_flow_consistency

        try:
            from ali2026v3_trading.config_params import get_cached_params
            _params = get_cached_params()
            if 'default_order_flow_consistency' in _params:
                self._default_order_flow_consistency = _params['default_order_flow_consistency']
        except Exception:
            pass

        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            _cooldown_cfg = _cfg.get(self._CONFIG_COOLDOWN_KEY, None) if hasattr(_cfg, 'get') else None
            _cleanup_cfg = _cfg.get(self._CONFIG_CLEANUP_KEY, None) if hasattr(_cfg, 'get') else None
            if _cooldown_cfg is not None:
                self._default_cooldown_seconds = float(_cooldown_cfg)
                logging.info("[SignalService] 冷却时间从配置读取: %.1fs", self._default_cooldown_seconds)
            if _cleanup_cfg is not None:
                self._cleanup_interval = float(_cleanup_cfg)
                logging.info("[SignalService] 清理间隔从配置读取: %.1fs", self._cleanup_interval)
        except Exception:
            pass
        
        # EventBus 集成
        self._event_bus = event_bus or (get_global_event_bus() if _HAS_EVENT_BUS else None)

        # PLR过滤
        self._min_estimated_plr: float = 0.0
        self._plr_filter_enabled: bool = False

        self._hft_signal_filter = None
        self._hft_filter_enabled = False
        self._filter_chain = SignalFilterChain(self)
        self._cooldown_mgr = CooldownManager()
        self._decision_score_filter_enabled = True  # R3-D-01修复: 默认开启决策评分过滤（11维度评分系统真正生效）

        # P1-4修复: 根据ConfigService配置自动启用HFT过滤
        # R4-P-08修复: 默认根据策略类型决定是否启用HFT过滤（HFT策略默认启用，非HFT默认不启用）
        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            if getattr(_cfg, 'trading', None) and getattr(_cfg.trading, 'enable_hft_filter', None):
                self.enable_hft_filter()
            else:
                _strategy_type = getattr(_cfg, 'strategy_type', 'normal') if hasattr(_cfg, 'strategy_type') else 'normal'
                if _strategy_type in ('hft', 'high_frequency'):
                    self.enable_hft_filter()
                    logging.info("[R4-P-08] HFT过滤根据策略类型自动启用: %s", _strategy_type)
        except Exception:
            pass

        # ✅ P1-5修复: 集成AdaptiveSignalThreshold
        try:
            self._adaptive_threshold = AdaptiveSignalThreshold()
            logging.info("[SignalService] AdaptiveSignalThreshold已集成")
        except Exception as e:
            logging.warning("[SignalService] AdaptiveSignalThreshold集成失败: %s", e)
            self._adaptive_threshold = None

        self._log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs'
        )
        self._daily_report_generated: Dict[str, bool] = {}

        # ✅ 集成结构化JSONL日志（L-P1-1）
        try:
            from ali2026v3_trading.health_check_api import StructuredJsonlLogger
            self._structured_logger = StructuredJsonlLogger(log_dir=self._log_dir)
            logging.info("[SignalService] 结构化JSONL日志已集成")
        except Exception as e:
            logging.warning("[SignalService] 结构化日志集成失败: %s", e)
            self._structured_logger = None

    def enable_hft_filter(self, threshold: float = 0.6, use_kalman: bool = True) -> None:
        self._filter_chain.enable_hft_filter(threshold, use_kalman)
        self._hft_filter_enabled = True
        self._hft_signal_filter = self._filter_chain._hft_signal_filter

    def transition_signal_state(self, signal_id: str, new_state: str) -> bool:
        return self._filter_chain.transition_signal_state(signal_id, new_state, self._signal_states, SIGNAL_STATE_TRANSITIONS)

    def expire_stale_signals(self) -> int:
        return self._filter_chain.expire_stale_signals(self._history_service.get_recent(n=len(self._signal_history)), self._signal_states, self._signal_max_age_sec, self._lock)

    def enable_plr_filter(self, min_estimated_plr: float = 2.0) -> None:
        self._filter_chain.enable_plr_filter(min_estimated_plr)
        self._min_estimated_plr = min_estimated_plr
        self._plr_filter_enabled = True

    def disable_plr_filter(self) -> None:
        self._filter_chain.disable_plr_filter()
        self._plr_filter_enabled = False

    def filter_with_hft(self, instrument_id: str, resonance_strength: float) -> Optional[Dict[str, Any]]:
        return self._filter_chain.filter_with_hft(instrument_id, resonance_strength)

    def generate_signal(
        self,
        instrument_id: str,
        signal_type: str,
        price: float,
        volume: float,
        reason: str = '',
        priority: int = 0,
        cooldown_seconds: Optional[float] = None,
        estimated_plr: float = 0.0,
        signal_strength: float = 0.0,
        days_to_expiry: int = 999,
        correlation_id: str = "",
        tick: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """生成交易信号
        
        Args:
            instrument_id: 合约代码
            signal_type: 信号类型 ('BUY'/'SELL'/'CLOSE_LONG'/'CLOSE_SHORT')
            price: 价格
            volume: 手数
            reason: 原因描述
            priority: 优先级 (0-10)
            cooldown_seconds: 冷却时间（秒）
            estimated_plr: 预估盈亏比（0.0表示未计算）
            
        Returns:
            Optional[Dict]: 信号对象，被过滤返回 None
        """
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_FILTER_CHAIN_SIGNAL'):
            from ali2026v3_trading.signal_generator import SignalGenerator, SignalContext
            _gen = SignalGenerator(self)
            _ctx = SignalContext(
                instrument_id=instrument_id, signal_type=signal_type,
                price=price, volume=volume, reason=reason, priority=priority,
                cooldown_seconds=cooldown_seconds, estimated_plr=estimated_plr,
                signal_strength=signal_strength, days_to_expiry=days_to_expiry,
                correlation_id=correlation_id, tick=tick,
            )
            _result = _gen.generate_signal(_ctx)
            if _result.rejected:
                return None
            return _result.signal
        from ali2026v3_trading.signal_generator import SignalGenerator, SignalContext
        _gen = SignalGenerator(self)
        _ctx = SignalContext(
            instrument_id=instrument_id, signal_type=signal_type,
            price=price, volume=volume, reason=reason, priority=priority,
            cooldown_seconds=cooldown_seconds, estimated_plr=estimated_plr,
            signal_strength=signal_strength, days_to_expiry=days_to_expiry,
            correlation_id=correlation_id, tick=tick,
        )
        _result = _gen.generate_signal(_ctx)
        if _result.rejected:
            return None
        return _result.signal

    def _make_cooldown_key(self, instrument_id: str, signal_type: str = '') -> str:
        return self._cooldown_mgr.make_cooldown_key(instrument_id, signal_type)

    def _is_in_cooldown(self, cooldown_key: str, cooldown_seconds: float) -> bool:
        return self._cooldown_mgr.is_in_cooldown(cooldown_key, cooldown_seconds)
    
    def get_signal_history(
        self,
        instrument_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """获取信号历史
        
        Args:
            instrument_id: 合约代码（可选）
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 信号历史列表
        """
        with self._lock:
            history = self._history_service.get_recent(n=len(self._signal_history))
        
        if instrument_id:
            history = [s for s in history if s['instrument_id'] == instrument_id]
        
        return history[-limit:]
    
    def set_cooldown(
        self,
        instrument_id: str,
        cooldown_seconds: float,
        signal_type: str = ''
    ) -> None:
        self._cooldown_mgr.set_cooldown(instrument_id, cooldown_seconds, signal_type)
        self._cooldown_times = self._cooldown_mgr.cooldown_times
        self._cooldown_durations = self._cooldown_mgr.cooldown_durations
    
    def clear_cooldown(self, instrument_id: str) -> None:
        self._cooldown_mgr.clear_cooldown(instrument_id)
    
    def reset_signal_history(self) -> None:
        """重置信号历史"""
        with self._lock:
            self._history_service.clear()
            logging.info("[SignalService] Signal history reset")
    
    # ========================================================================
    # 性能基准测试支持
    # ========================================================================
    
    def run_benchmark(self, iterations: int = 100, cooldown_enabled: bool = False) -> Dict[str, Any]:
        """运行性能基准测试
        
        Args:
            iterations: 测试迭代次数
            cooldown_enabled: 是否启用冷却（会影响测试结果）
            
        Returns:
            Dict: 基准测试结果 {avg_latency, total_time, signals_per_second, ...}
        """
        import statistics
        
        latencies = []
        start_total = time.time()
        original_cooldown = self._default_cooldown_seconds
        
        # 临时关闭冷却以获得最佳性能数据
        if not cooldown_enabled:
            self._default_cooldown_seconds = 0.0
        
        try:
            for i in range(iterations):
                test_instrument = f"TEST{str(i).zfill(3)}"
                test_price = 3500.0 + (i % 10)
                
                start = time.time()
                signal = self.generate_signal(
                    instrument_id=test_instrument,
                    signal_type='BUY',
                    price=test_price,
                    volume=1,
                    reason=f'Benchmark signal {i}'
                )
                elapsed = time.time() - start
                
                if signal:  # 只记录成功信号的延迟
                    latencies.append(elapsed)
            
            total_time = time.time() - start_total
            
            # 计算统计信息
            results = {
                'iterations': iterations,
                'successful_signals': len(latencies),
                'filtered_signals': iterations - len(latencies),
                'total_time_seconds': total_time,
                'avg_latency_ms': statistics.mean(latencies) * 1000 if latencies else 0,
                'min_latency_ms': min(latencies) * 1000 if latencies else 0,
                'max_latency_ms': max(latencies) * 1000 if latencies else 0,
                'median_latency_ms': statistics.median(latencies) * 1000 if latencies else 0,
                'stddev_latency_ms': statistics.stdev(latencies) * 1000 if len(latencies) > 1 else 0,
                'signals_per_second': len(latencies) / total_time if total_time > 0 and latencies else 0,
                'success_rate': len(latencies) / max(1, iterations)
            }
            
            logging.info("[SignalService] Benchmark completed: %d/%d signals, avg latency=%.3fms, %.1f signals/s",
                        len(latencies), iterations, results['avg_latency_ms'], results['signals_per_second'])
            
            return results
            
        finally:
            # 恢复原始冷却设置
            self._default_cooldown_seconds = original_cooldown
    
    # ✅ ID唯一：get_stats统一接口，返回值含service_name="SignalService"
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息
        
        Returns:
            Dict: 统计数据
        """
        with self._lock:
            # 定期清理过期冷却条目防止内存泄漏
            now = time.time()
            if now - self._last_cleanup > self._cleanup_interval:
                self._last_cleanup = now
                expired = [k for k, v in self._cooldown_times.items() 
                          if now - v > self._default_cooldown_seconds]
                for k in expired:
                    del self._cooldown_times[k]
                    self._cooldown_durations.pop(k, None)
                if expired:
                    logging.debug("[SignalService] 清理%d个过期冷却条目", len(expired))
            _stats_copy = dict(self._stats)
            _total = _stats_copy.get('total_signals', 0)
            _filtered = _stats_copy.get('filtered_signals', 0)
            _emitted = _stats_copy.get('emitted_signals', 0)
            _invariant_ok = (_total == _filtered + _emitted)
            if not _invariant_ok and _total > 0:
                logging.warning("[R26-P0-DI-04] 信号统计不自洽: total=%d != filtered=%d + emitted=%d (diff=%d)",
                              _total, _filtered, _emitted, _total - _filtered - _emitted)
            # R23-FR-05-FIX: 定期清理过期信号
            self.expire_stale_signals()
            return {
                'service_name': 'SignalService',
                **_stats_copy,
                'stats_invariant_ok': _invariant_ok,
                'history_size': self._history_service.get_statistics()['total_signals'],
                'active_cooldowns': len([
                    k for k, v in self._cooldown_times.items()
                    if time.time() - v < self._default_cooldown_seconds
                ])
            }
    
    def validate_signal(
        self,
        signal: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """验证信号有效性
        
        Args:
            signal: 信号对象
            
        Returns:
            Tuple[bool, str]: (是否有效，消息)
        """
        # 必填字段检查
        required_fields = ['instrument_id', 'signal_type', 'price', 'volume']
        for field in required_fields:
            if field not in signal:
                return False, f"Missing required field: {field}"
        
        # 信号类型检查
        valid_types = VALID_SIGNAL_TYPES
        if signal['signal_type'] not in valid_types:
            return False, f"Invalid signal type: {signal['signal_type']}"
        
        # 价格和手数检查
        if signal['price'] <= 0 or signal['volume'] <= 0:
            return False, "Price and volume must be positive"
        
        return True, "Valid signal"

    def generate_daily_signal_report(self, date: Optional[str] = None) -> Optional[str]:
        if date is None:
            date = datetime.now(CHINA_TZ).strftime("%Y-%m-%d")

        if self._daily_report_generated.get(date):
            return None

        with self._lock:
            date_signals = [
                s for s in self._history_service.get_recent(n=len(self._signal_history))
                if isinstance(s.get('generated_at'), datetime) and s['generated_at'].strftime("%Y-%m-%d") == date
            ]

        if not date_signals:
            return None

        buy_signals = [s for s in date_signals if s.get('signal_type') in ('BUY',)]
        sell_signals = [s for s in date_signals if s.get('signal_type') in ('SELL',)]
        close_long = [s for s in date_signals if s.get('signal_type') == 'CLOSE_LONG']
        close_short = [s for s in date_signals if s.get('signal_type') == 'CLOSE_SHORT']

        lines = [
            "=" * 80,
            f"当日信号明细报告 - {date}",
            "=" * 80,
            f"总信号数: {len(date_signals)}",
            f"  买入信号: {len(buy_signals)}",
            f"  卖出信号: {len(sell_signals)}",
            f"  平多信号: {len(close_long)}",
            f"  平空信号: {len(close_short)}",
            "-" * 80,
            "信号明细:",
        ]

        for i, s in enumerate(date_signals, 1):
            ts = s['generated_at'].strftime('%H:%M:%S') if isinstance(s.get('generated_at'), datetime) else str(s.get('generated_at', ''))
            lines.extend([
                f"【信号 {i}】",
                f"  时间: {ts}",
                f"  合约: {s.get('instrument_id', '')}",
                f"  类型: {s.get('signal_type', '')}",
                f"  价格: {s.get('price', 0)}",
                f"  数量: {s.get('volume', 0)}",
                f"  原因: {s.get('reason', '')}",
                f"  信号ID: {s.get('signal_id', '')}",
            ])

        lines.extend([
            "=" * 80,
            f"报告生成时间: {datetime.now(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
        ])

        report = "\n".join(lines)

        try:
            os.makedirs(self._log_dir, exist_ok=True)
            report_file = os.path.join(self._log_dir, f"signal_daily_report_{date}.txt")
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            json_file = os.path.join(self._log_dir, f"signal_daily_report_{date}.json")
            serializable_signals = []
            for s in date_signals:
                entry = dict(s)
                if isinstance(entry.get('generated_at'), datetime):
                    entry['generated_at'] = entry['generated_at'].isoformat()
                serializable_signals.append(entry)
            report_data = {
                'date': date,
                'total': len(date_signals),
                'buy': len(buy_signals),
                'sell': len(sell_signals),
                'close_long': len(close_long),
                'close_short': len(close_short),
                'signals': serializable_signals,
            }
            with open(json_file, 'w', encoding='utf-8') as f:
                if json_dumps is not None:
                    f.write(json_dumps(report_data, indent=2))
                else:
                    def _fallback_default(obj):
                        """P1-11修复: 与serialization_utils.json_default_serializer一致的NaN/Infinity可逆处理"""
                        import math as _math
                        import datetime as _dt
                        import decimal as _decimal
                        if isinstance(obj, float):
                            if _math.isnan(obj):
                                return {"__special__": "NaN"}
                            if _math.isinf(obj):
                                return {"__special__": "Infinity" if obj > 0 else "-Infinity"}
                        if isinstance(obj, (_dt.datetime, _dt.date)):
                            return obj.isoformat()
                        if isinstance(obj, _decimal.Decimal):
                            return {"__decimal__": str(obj)}
                        return str(obj)
                    # SER-P1-12修复: else分支使用原生json.dump+_fallback_default
                    json.dump(report_data, f, indent=2, default=_fallback_default, ensure_ascii=False)
            self._daily_report_generated[date] = True
            logging.info("[SignalService] 日信号报告已生成: %s", report_file)
        except Exception as e:
            logging.warning("[SignalService] 生成日信号报告失败: %s", e)

        return report

    def check_market_close_and_report(self) -> Optional[str]:
        now = datetime.now(CHINA_TZ)
        current_time = now.time()
        from datetime import time as dt_time
        close_time = dt_time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE_START)
        check_end = dt_time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE_END)
        if close_time <= current_time <= check_end:
            today = now.strftime("%Y-%m-%d")
            return self.generate_daily_signal_report(today)
        return None

    def _collect_decision_dimensions(self, instrument_id: str) -> Dict[str, Any]:
        kwargs = {}
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            if hasattr(rs, 'get_greeks_dashboard'):
                try:
                    gd = rs.get_greeks_dashboard(instrument_id)
                    if isinstance(gd, dict):
                        kwargs['greeks_dashboard'] = gd
                except Exception:
                    pass
            if hasattr(rs, 'params') and isinstance(rs.params, dict):
                try:
                    kwargs['consecutive_losses'] = int(rs.params.get('_consecutive_losses', 0))
                    kwargs['current_pnl'] = float(rs.params.get('_current_pnl', 0.0))
                    kwargs['drawdown_pct'] = float(rs.params.get('_drawdown_pct', 0.0))
                except Exception:
                    pass
        except Exception:
            pass
        try:
            from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
            _sse = get_shadow_strategy_engine()
            if _sse and hasattr(_sse, 'alpha_ratio'):
                kwargs['alpha_ratio'] = _sse.alpha_ratio
        except Exception:
            pass
        try:
            from ali2026v3_trading.state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            if spm and hasattr(spm, 'current_state'):
                kwargs['hmm_state'] = str(spm.current_state)
        except Exception:
            pass
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            se = get_strategy_ecosystem()
            if se and hasattr(se, 'cross_correlation'):
                kwargs['cross_correlation'] = se.cross_correlation
        except Exception:
            pass
        return kwargs

    def apply_decision_score_filter(self, signal: Dict[str, Any], state_strength: float,
                                     order_flow_consistency: float,
                                     hmm_state: Optional[str] = None,
                                     cr_output: Optional[Any] = None,
                                     greeks_dashboard: Optional[Dict[str, Any]] = None,
                                     consecutive_losses: int = 0,
                                     current_pnl: float = 0.0,
                                     drawdown_pct: float = 0.0,
                                     alpha_ratio: Optional[float] = None,
                                     cross_correlation: Optional[float] = None,
                                     tri_validation_score: Optional[float] = None,
                                     slippage_source: str = 'LIVE') -> Dict[str, Any]:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            result = rs.compute_decision_score(
                state_strength, order_flow_consistency,
                hmm_state=hmm_state, cr_output=cr_output,
                greeks_dashboard=greeks_dashboard,
                consecutive_losses=consecutive_losses,
                current_pnl=current_pnl, drawdown_pct=drawdown_pct,
                alpha_ratio=alpha_ratio,
                cross_correlation=cross_correlation,
                tri_validation_score=tri_validation_score,
                slippage_source=slippage_source,
            )
            if result["action"] == "no_open_wait":
                signal["filtered"] = True
                signal["filter_reason"] = f"decision_score_low: score={result['decision_score']:.2f}"
            signal["decision_score"] = result["decision_score"]
            signal["position_scale"] = result["position_scale"]
            signal["decision_action"] = result["action"]
            signal["dimension_scores"] = result.get("dimension_scores", {})
            signal["dimension_weights"] = result.get("dimension_weights", {})
        except Exception as e:
            logging.debug("[SignalService] apply_decision_score_filter error: %s", e)
        return signal


_signal_service_instance: Optional['SignalService'] = None
_signal_service_lock = threading.Lock()


def get_signal_service(**kwargs) -> 'SignalService':
    global _signal_service_instance
    with _signal_service_lock:
        if _signal_service_instance is None:
            _signal_service_instance = SignalService(**kwargs)
    return _signal_service_instance


# 导出公共接口
__all__ = ['SignalService', 'KalmanFilter1D', 'EMASignalFilter', 'SignalTimingFilter', 'get_signal_service']


class KalmanFilter1D:
    def __init__(self, process_variance: float = 1e-4, measurement_variance: float = 1e-2):
        self._x = 0.0
        self._p = 1.0
        self._q = process_variance
        self._r = measurement_variance
        self._velocity = 0.0
        self._prev_x = 0.0
        self._initialized = False
        self._lock = threading.Lock()

    def update(self, measurement: float) -> Tuple[float, float]:
        with self._lock:
            if not self._initialized:
                self._x = measurement
                self._prev_x = measurement
                self._initialized = True
                return self._x, 0.0
            self._p += self._q
            k = self._p / (self._p + self._r)
            self._prev_x = self._x
            self._x = self._x + k * (measurement - self._x)
            self._p = (1 - k) * self._p
            self._velocity = self._x - self._prev_x
            return self._x, self._velocity

    def get_state(self) -> Tuple[float, float]:
        with self._lock:
            return self._x, self._velocity

    def reset(self) -> None:
        with self._lock:
            self._x = 0.0
            self._p = 1.0
            self._velocity = 0.0
            self._prev_x = 0.0
            self._initialized = False


class EMASignalFilter:
    def __init__(self, fast_period: int = 5, slow_period: int = 20):
        self._fast_alpha = 2.0 / (fast_period + 1)
        self._slow_alpha = 2.0 / (slow_period + 1)
        self._fast_ema: Optional[float] = None
        self._slow_ema: Optional[float] = None
        self._velocity = 0.0
        self._prev_fast: Optional[float] = None
        self._lock = threading.Lock()

    def update(self, value: float) -> Tuple[float, float, float]:
        with self._lock:
            if self._fast_ema is None:
                self._fast_ema = value
                self._slow_ema = value
                self._prev_fast = value
                return self._fast_ema, self._slow_ema, 0.0
            self._prev_fast = self._fast_ema
            self._fast_ema = self._fast_alpha * value + (1 - self._fast_alpha) * self._fast_ema
            self._slow_ema = self._slow_alpha * value + (1 - self._slow_alpha) * self._slow_ema
            self._velocity = self._fast_ema - self._prev_fast
            return self._fast_ema, self._slow_ema, self._velocity

    def is_bullish_crossover(self) -> bool:
        with self._lock:
            if self._fast_ema is None or self._slow_ema is None:
                return False
            return self._fast_ema > self._slow_ema and self._velocity > 0

    def get_state(self) -> Tuple[float, float, float]:
        with self._lock:
            return self._fast_ema or 0.0, self._slow_ema or 0.0, self._velocity


class SignalTimingFilter:
    def __init__(self, threshold: float = 0.6, use_kalman: bool = True,
                 kalman_process_var: float = 1e-4, kalman_measure_var: float = 1e-2,
                 ema_fast_period: int = 5, ema_slow_period: int = 20):
        self._threshold = threshold
        self._use_kalman = use_kalman
        self._kalman = KalmanFilter1D(kalman_process_var, kalman_measure_var)
        self._ema = EMASignalFilter(ema_fast_period, ema_slow_period)
        self._filters: Dict[str, KalmanFilter1D] = {}
        self._ema_filters: Dict[str, EMASignalFilter] = {}
        self._lock = threading.Lock()
        self._stats = {
            'total_inputs': 0,
            'filtered_noise': 0,
            'passed_signals': 0,
        }

    def filter_signal(self, instrument_id: str, raw_strength: float) -> Dict[str, Any]:
        self._stats['total_inputs'] += 1
        with self._lock:
            if instrument_id not in self._filters:
                self._filters[instrument_id] = KalmanFilter1D()
                self._ema_filters[instrument_id] = EMASignalFilter()
            kf = self._filters[instrument_id]
            ema_f = self._ema_filters[instrument_id]
        smoothed, velocity = kf.update(raw_strength)
        fast_ema, slow_ema, ema_vel = ema_f.update(raw_strength)
        if self._use_kalman:
            effective_value = smoothed
            effective_velocity = velocity
        else:
            effective_value = fast_ema
            effective_velocity = ema_vel
        threshold_crossed = effective_value >= self._threshold
        velocity_positive = effective_velocity > 0
        ema_confirmed = ema_f.is_bullish_crossover()
        passed = threshold_crossed and velocity_positive
        if not passed and raw_strength >= self._threshold:
            self._stats['filtered_noise'] += 1
        if passed:
            self._stats['passed_signals'] += 1
        return {
            'instrument_id': instrument_id,
            'raw_strength': raw_strength,
            'smoothed_value': effective_value,
            'velocity': effective_velocity,
            'threshold_crossed': threshold_crossed,
            'velocity_positive': velocity_positive,
            'ema_confirmed': ema_confirmed,
            'signal_passed': passed,
            'fast_ema': fast_ema,
            'slow_ema': slow_ema,
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            'service_name': 'SignalTimingFilter',
            **self._stats,
            'filter_ratio': (self._stats['filtered_noise'] / max(1, self._stats['total_inputs'])),
            'pass_ratio': (self._stats['passed_signals'] / max(1, self._stats['total_inputs'])),
            'tracked_instruments': len(self._filters),
        }


class AdaptiveSignalThreshold:
    """自适应信号阈值：基于近期信号通过率和Sharpe动态调整阈值

    原理：通过率过高→阈值过低→噪声多→上调；Sharpe过低→阈值过低→上调
    """

    def __init__(self, initial_threshold: float = 0.3,
                 min_threshold: float = 0.15, max_threshold: float = 0.6,
                 adaptation_rate: float = 0.05,
                 target_pass_rate: float = 0.4):
        self._threshold = initial_threshold
        self._min_threshold = min_threshold
        self._max_threshold = max_threshold
        self._adaptation_rate = adaptation_rate
        self._target_pass_rate = target_pass_rate
        self._recent_passes: Deque[bool] = deque(maxlen=100)
        self._recent_pnls: Deque[float] = deque(maxlen=50)

    @property
    def threshold(self) -> float:
        return self._threshold

    def record_signal(self, passed: bool, pnl: float = 0.0) -> None:
        self._recent_passes.append(passed)
        if passed:
            self._recent_pnls.append(pnl)
        if len(self._recent_passes) >= 20:
            self._adapt()

    def _adapt(self) -> None:
        pass_rate = sum(1 for p in self._recent_passes if p) / len(self._recent_passes)  # [R22-TS-P1-05] 显式bool→int转换
        adjustment = (pass_rate - self._target_pass_rate) * self._adaptation_rate
        if len(self._recent_pnls) >= 10:
            avg_pnl = sum(self._recent_pnls) / len(self._recent_pnls)
            if avg_pnl < 0:
                adjustment += self._adaptation_rate * 0.5
        self._threshold = max(self._min_threshold,
                              min(self._max_threshold, self._threshold + adjustment))

    def get_stats(self) -> Dict[str, Any]:
        pass_rate = sum(1 for p in self._recent_passes if p) / max(1, len(self._recent_passes))  # [R22-TS-P1-05] 显式bool→int转换
        return {
            'service_name': 'AdaptiveSignalThreshold',
            'current_threshold': round(self._threshold, 4),
            'pass_rate': round(pass_rate, 4),
            'target_pass_rate': self._target_pass_rate,
        }
