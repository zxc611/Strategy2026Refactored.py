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
import uuid
import os
import json
from datetime import datetime
import time
from typing import Any, Dict, List, Optional, Callable, Tuple, Deque
from collections import deque

# 导入 EventBus（可选依赖）
try:
    from ali2026v3_trading.event_bus import EventBus, get_global_event_bus
    _HAS_EVENT_BUS = True
except ImportError as e:
    logging.warning(f"[SignalService] Failed to import EventBus: {e}")
    _HAS_EVENT_BUS = False
    EventBus = None


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
    _CONFIG_COOLDOWN_KEY = 'signal_cooldown_seconds'
    _CONFIG_CLEANUP_KEY = 'signal_cleanup_interval_seconds'
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE_START = 15
    MARKET_CLOSE_MINUTE_END = 20
    DEFAULT_ORDER_FLOW_CONSISTENCY = 0.5

    def __init__(self, event_bus: Optional[EventBus] = None, default_order_flow_consistency: float = 0.5):
        """初始化信号服务

        Args:
            event_bus: 事件总线实例（可选，用于发布信号事件）
        """
        # 信号历史
        self._signal_history: deque = deque(maxlen=self.SIGNAL_HISTORY_MAX_LEN)

        # 冷却时间管理（存储信号时刻，非冷却结束时刻）
        self._cooldown_times: Dict[str, float] = {}
        self._cooldown_durations: Dict[str, float] = {}
        self._last_cleanup = time.time()

        # 信号统计
        self._stats = {
            'total_signals': 0,
            'filtered_signals': 0,
            'emitted_signals': 0,
            'last_signal_time': None
        }

        # 线程锁
        self._lock = threading.RLock()

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
        self._decision_score_filter_enabled = False

        # P1-4修复: 根据ConfigService配置自动启用HFT过滤
        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            if getattr(_cfg, 'trading', None) and getattr(_cfg.trading, 'enable_hft_filter', False):
                self.enable_hft_filter()
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
        self._hft_signal_filter = SignalTimingFilter(threshold=threshold, use_kalman=use_kalman)
        self._hft_filter_enabled = True
        logging.info("[SignalService] HFT信号时序滤波器已启用 threshold=%.2f", threshold)

    def enable_plr_filter(self, min_estimated_plr: float = 2.0) -> None:
        self._min_estimated_plr = min_estimated_plr
        self._plr_filter_enabled = True
        logging.info("[SignalService] PLR过滤已启用 min_estimated_plr=%.2f", min_estimated_plr)

    def disable_plr_filter(self) -> None:
        self._plr_filter_enabled = False
        logging.info("[SignalService] PLR过滤已禁用")

    def filter_with_hft(self, instrument_id: str, resonance_strength: float) -> Optional[Dict[str, Any]]:
        if not self._hft_filter_enabled or not self._hft_signal_filter:
            return None
        return self._hft_signal_filter.filter_signal(instrument_id, resonance_strength)

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
        if not instrument_id or signal_type not in ('BUY', 'SELL', 'CLOSE_LONG', 'CLOSE_SHORT'):
            logging.error(f"[SignalService] Invalid signal params: {instrument_id}, {signal_type}")
            return None
        
        with self._lock:
            self._stats['total_signals'] += 1
            
            # PLR过滤
            if self._plr_filter_enabled and self._min_estimated_plr > 0:
                if signal_type in ('BUY', 'SELL') and estimated_plr < self._min_estimated_plr:
                    self._stats['filtered_signals'] += 1
                    logging.debug(f"[SignalService] Signal filtered (PLR): {instrument_id} {signal_type} estimated_plr={estimated_plr:.2f} < {self._min_estimated_plr:.2f}")
                    return None
            
            # ModeEngine信号过滤（信号强度+time_decay）
            try:
                from ali2026v3_trading.mode_engine import ModeEngine
                _me = ModeEngine.get_instance()
                _passed, _reason = _me.filter_signal_by_mode(
                    signal_type, estimated_plr=estimated_plr,
                    signal_strength=signal_strength, days_to_expiry=days_to_expiry,
                )
                if not _passed:
                    self._stats['filtered_signals'] += 1
                    logging.debug(f"[SignalService] Signal filtered (ModeEngine): {instrument_id} {signal_type} {_reason}")
                    return None
            except Exception:
                pass
            
            # 冷却检查
            effective_cooldown = cooldown_seconds if cooldown_seconds is not None else self._default_cooldown_seconds
            cooldown_key = f"{instrument_id}_{signal_type}" if signal_type else instrument_id
            if self._is_in_cooldown(cooldown_key, effective_cooldown):
                self._stats['filtered_signals'] += 1
                logging.debug(f"[SignalService] Signal filtered (cooldown): {instrument_id} {signal_type}")
                return None
            
            # ✅ 集成决策分数过滤（P0-10修复）
            if self._decision_score_filter_enabled:
                try:
                    signal = self.apply_decision_score_filter(
                        {'signal_id': '', 'instrument_id': instrument_id, 'signal_type': signal_type,
                         'price': price, 'volume': volume, 'reason': reason, 'priority': priority,
                         'estimated_plr': estimated_plr, 'filtered': False, 'filter_reason': ''},
                        state_strength=signal_strength,
                        order_flow_consistency=self._default_order_flow_consistency,
                    )
                    if signal.get('filtered'):
                        self._stats['filtered_signals'] += 1
                        logging.info("[SignalService] Signal filtered (decision_score): %s %s %s",
                                     instrument_id, signal_type, signal.get('filter_reason', ''))
                        return None
                except Exception as e:
                    logging.debug("[SignalService] decision_score_filter error: %s", e)

            # ✅ P1-4修复: 集成HFT信号过滤
            if self._hft_filter_enabled and self._hft_signal_filter is not None:
                try:
                    hft_result = self.filter_with_hft(instrument_id, signal_strength)
                    if hft_result is not None and not hft_result.get('signal_passed', True):
                        self._stats['hft_filtered'] = self._stats.get('hft_filtered', 0) + 1
                        logging.info("[SignalService] HFT过滤阻断: %s %s", instrument_id, hft_result.get('reason', ''))
                        return None
                except Exception as e:
                    logging.debug("[SignalService] HFT过滤异常: %s", e)

            # ✅ P1-5修复: 集成AdaptiveSignalThreshold自适应阈值
            if self._adaptive_threshold is not None:
                try:
                    current_threshold = self._adaptive_threshold.threshold
                    if signal_strength < current_threshold:
                        self._adaptive_threshold.record_signal(passed=False, pnl=0.0)
                        self._stats['adaptive_filtered'] = self._stats.get('adaptive_filtered', 0) + 1
                        logging.info("[SignalService] 自适应阈值过滤: strength=%.2f < threshold=%.2f",
                                     signal_strength, current_threshold)
                        return None
                    self._adaptive_threshold.record_signal(passed=True, pnl=0.0)
                except Exception as e:
                    logging.debug("[SignalService] AdaptiveThreshold异常: %s", e)

            # 生成信号
            # 统一使用 UUID 生成唯一 ID（避免时间戳冲突）
            signal = {
                'signal_id': f"SIG_{uuid.uuid4().hex[:12]}",
                'instrument_id': instrument_id,
                'signal_type': signal_type,
                'price': price,
                'volume': volume,
                'reason': reason,
                'priority': priority,
                'estimated_plr': estimated_plr,
                'generated_at': datetime.now(),
                'status': 'EMITTED'
            }
            
            # 记录历史
            self._signal_history.append(signal)
            
            # 更新冷却时间
            self._cooldown_times[cooldown_key] = time.time()
            
            # 更新统计
            self._stats['emitted_signals'] += 1
            self._stats['last_signal_time'] = datetime.now()
        
        # 发布信号事件（如果 EventBus 可用）
        if self._event_bus:
            try:
                from ali2026v3_trading.event_bus import SignalEvent as EventBusSignalEvent
                signal_event = EventBusSignalEvent(
                    instrument_id=instrument_id,
                    signal_type=signal_type,
                    price=price,
                    volume=volume,
                    reason=reason
                )
                self._event_bus.publish(signal_event, async_mode=True)
            except Exception as e:
                logging.error(f"[SignalService] Failed to publish signal event: {e}")
        
        logging.info(f"[SignalService] Signal generated: {signal['signal_id']} {instrument_id} {signal_type} "
                    f"@{price} x{volume}")

        # ✅ 集成结构化JSONL日志记录（L-P1-1）
        if self._structured_logger is not None:
            try:
                self._structured_logger.log_signal({
                    "signal_id": signal['signal_id'],
                    "instrument_id": instrument_id,
                    "direction": "buy" if signal_type in ('BUY', 'CLOSE_SHORT') else "sell",
                    "strength": signal_strength,
                    "estimated_plr": estimated_plr,
                    "decision_score": 0.0,
                    "position_scale": 1.0,
                    "filtered": False,
                    "filter_reason": "",
                })
            except Exception as e:
                logging.debug("[SignalService] StructuredLogger error: %s", e)

        if signal_type in ('CLOSE_LONG', 'CLOSE_SHORT'):
            if self._event_bus:
                try:
                    self._event_bus.publish('signal.close', {
                        'instrument_id': instrument_id,
                        'signal_type': signal_type,
                        'price': price,
                        'signal_id': signal['signal_id'],
                    })
                except Exception as e:
                    logging.debug("[SignalService] publish close signal event failed: %s", e)
        
        return signal
    
    def _is_in_cooldown(self, cooldown_key: str, cooldown_seconds: float) -> bool:
        last_signal_time = self._cooldown_times.get(cooldown_key, 0)
        effective_cooldown = self._cooldown_durations.get(cooldown_key, cooldown_seconds)
        elapsed = time.time() - last_signal_time
        return elapsed < effective_cooldown
    
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
            history = list(self._signal_history)
        
        if instrument_id:
            history = [s for s in history if s['instrument_id'] == instrument_id]
        
        return history[-limit:]
    
    def set_cooldown(
        self,
        instrument_id: str,
        cooldown_seconds: float,
        signal_type: str = ''
    ) -> None:
        cooldown_key = f"{instrument_id}_{signal_type}" if signal_type else instrument_id
        with self._lock:
            self._cooldown_times[cooldown_key] = time.time()
            self._cooldown_durations[cooldown_key] = cooldown_seconds
            logging.info(f"[SignalService] Cooldown set for {cooldown_key}: {cooldown_seconds}s")
    
    def clear_cooldown(self, instrument_id: str) -> None:
        with self._lock:
            if instrument_id in self._cooldown_times:
                del self._cooldown_times[instrument_id]
                self._cooldown_durations.pop(instrument_id, None)
                logging.debug(f"[SignalService] Cooldown cleared for {instrument_id}")
    
    def reset_signal_history(self) -> None:
        """重置信号历史"""
        with self._lock:
            self._signal_history.clear()
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
            
            logging.info(f"[SignalService] Benchmark completed: {len(latencies)}/{iterations} signals, "
                        f"avg latency={results['avg_latency_ms']:.3f}ms, "
                        f"{results['signals_per_second']:.1f} signals/s")
            
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
                    logging.debug(f"[SignalService] 清理{len(expired)}个过期冷却条目")
            return {
                'service_name': 'SignalService',  # ✅ ID唯一：统一标识服务来源
                **self._stats,
                'history_size': len(self._signal_history),
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
        valid_types = ('BUY', 'SELL', 'CLOSE_LONG', 'CLOSE_SHORT')
        if signal['signal_type'] not in valid_types:
            return False, f"Invalid signal type: {signal['signal_type']}"
        
        # 价格和手数检查
        if signal['price'] <= 0 or signal['volume'] <= 0:
            return False, "Price and volume must be positive"
        
        return True, "Valid signal"

    def generate_daily_signal_report(self, date: Optional[str] = None) -> Optional[str]:
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        if self._daily_report_generated.get(date):
            return None

        with self._lock:
            date_signals = [
                s for s in self._signal_history
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
            f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
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
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'date': date,
                    'total': len(date_signals),
                    'buy': len(buy_signals),
                    'sell': len(sell_signals),
                    'close_long': len(close_long),
                    'close_short': len(close_short),
                    'signals': serializable_signals,
                }, f, ensure_ascii=False, indent=2, default=str)
            self._daily_report_generated[date] = True
            logging.info("[SignalService] 日信号报告已生成: %s", report_file)
        except Exception as e:
            logging.warning("[SignalService] 生成日信号报告失败: %s", e)

        return report

    def check_market_close_and_report(self) -> Optional[str]:
        now = datetime.now()
        current_time = now.time()
        from datetime import time as dt_time
        close_time = dt_time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE_START)
        check_end = dt_time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE_END)
        if close_time <= current_time <= check_end:
            today = now.strftime("%Y-%m-%d")
            return self.generate_daily_signal_report(today)
        return None

    def apply_decision_score_filter(self, signal: Dict[str, Any], state_strength: float,
                                     order_flow_consistency: float) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            result = rs.compute_decision_score(state_strength, order_flow_consistency)
            if result["action"] == "no_open_wait":
                signal["filtered"] = True
                signal["filter_reason"] = f"decision_score_low: score={result['decision_score']:.2f}"
            signal["decision_score"] = result["decision_score"]
            signal["position_scale"] = result["position_scale"]
            signal["decision_action"] = result["action"]
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
        pass_rate = sum(self._recent_passes) / len(self._recent_passes)
        adjustment = (pass_rate - self._target_pass_rate) * self._adaptation_rate
        if len(self._recent_pnls) >= 10:
            avg_pnl = sum(self._recent_pnls) / len(self._recent_pnls)
            if avg_pnl < 0:
                adjustment += self._adaptation_rate * 0.5
        self._threshold = max(self._min_threshold,
                              min(self._max_threshold, self._threshold + adjustment))

    def get_stats(self) -> Dict[str, Any]:
        pass_rate = sum(self._recent_passes) / max(1, len(self._recent_passes))
        return {
            'service_name': 'AdaptiveSignalThreshold',
            'current_threshold': round(self._threshold, 4),
            'pass_rate': round(pass_rate, 4),
            'target_pass_rate': self._target_pass_rate,
        }
