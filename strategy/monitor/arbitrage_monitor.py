# MODULE_ID: S5-ARB-MONITOR
"""S5套利策略实盘模拟监控模块

背景：
- S5套利策略当前在HFT通道中直接下单（tick_hft.py:handle_arbitrage_signal），
  本模块改为实盘模拟方式，不实际下单，只生成信号快照供其他策略参考方向。
- S5不参与资金分配（capital_allocation=0）。
- S5只为监控套利行为，让策略能够正确选择方向。
- 信号和快照需要保存下来。

功能：
- 3层诊断：HFT信号源诊断、主周期捕获诊断、STANDBY阻止诊断
- 价差收敛逻辑：跟踪deviation收敛过程
- 实盘模拟：生成开仓/平仓信号+快照保存，不实际下单
- 不参与资金分配
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

# 使用已有的ali2026v3_trading模块（与tick_hft.py保持一致）
from ali2026v3_trading.infra._helpers import get_logger  # R9-5 统一日志工厂
from ali2026v3_trading.infra.shared_utils import (
    CHINA_TZ as _CHINA_TZ,  # P2-13: 统一CHINA_TZ
    generate_prefixed_id,  # R9-3: 统一前缀ID生成
)
from ali2026v3_trading.infra.resilience import (
    safe_divide,  # 安全除法
    stable_std,  # 数值稳定标准差
)

__all__ = ['ArbitrageMonitor', 'SimulatedArbitrageSignal']

logger = get_logger(__name__)  # R9-5


# ============================================================================
# 数据结构定义
# ============================================================================

@dataclass
class SimulatedArbitrageSignal:
    """模拟套利信号数据结构（实盘模拟，不下单）

    字段对齐 tick_hft.handle_arbitrage_signal 的 arbitrage_signal 字典，
    并补充价差收敛与质量评估字段。
    """
    signal_id: str
    instrument_id: str
    direction: str  # 'BUY' / 'SELL'
    signal_type: str  # 'OPEN_LONG' / 'OPEN_SHORT' / 'CLOSE_LONG' / 'CLOSE_SHORT'
    deviation_bps: float
    confidence: float
    entry_price: float
    spread_convergence_rate: float
    quality_score: float
    timestamp: float
    # 实盘模拟标记：True表示不实际下单，仅生成快照
    simulated: bool = True
    # S5不参与资金分配的标记
    capital_allocation: float = 0.0
    # 附加诊断信息
    hft_consumed: bool = False
    source: str = 's5_arbitrage_monitor'

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# ArbitrageMonitor 主类
# ============================================================================

class ArbitrageMonitor:
    """S5套利策略实盘模拟监控模块

    功能：
    - 3层诊断：HFT信号源诊断、主周期捕获诊断、STANDBY阻止诊断
    - 价差收敛逻辑：跟踪deviation收敛过程
    - 实盘模拟：生成开仓/平仓信号+快照保存，不实际下单
    - 不参与资金分配

    重要约束：
    - 本模块不调用 OrderService.send_order
    - 本模块不调用 route_capital
    - 本模块只生成信号快照供其他策略参考方向
    """

    # S5套利策略固定配置
    STRATEGY_ID = 's5_arbitrage'
    STRATEGY_TYPE = 'arbitrage'
    # S5不参与资金分配
    CAPITAL_ALLOCATION = 0.0
    # 信号快照保存目录（相对于项目根目录）
    SNAPSHOT_SUBDIR = os.path.join('logs', 'arbitrage_monitor')
    # 价差收敛跟踪窗口（秒）
    CONVERGENCE_WINDOW_SEC = 60.0
    # 收敛样本最大数量
    MAX_CONVERGENCE_SAMPLES = 300
    # 历史信号保留数量
    MAX_HISTORY_SIGNALS = 500

    def __init__(self, base_dir: Optional[str] = None) -> None:
        """初始化套利监控模块

        Args:
            base_dir: 项目根目录（包含logs/子目录）。
                      为None时自动从__file__向上查找包含logs/的目录，
                      兜底使用当前工作目录。
        """
        self._base_dir = base_dir or self._detect_base_dir()
        self._snapshot_dir = os.path.join(self._base_dir, self.SNAPSHOT_SUBDIR)
        # 确保快照目录存在
        os.makedirs(self._snapshot_dir, exist_ok=True)

        # 线程锁：保护共享状态
        self._lock = threading.RLock()

        # 最近一次接收到的HFT套利信号（来自on_arbitrage_signal回调）
        self._last_hft_signal: Optional[Dict[str, Any]] = None
        self._last_hft_signal_ts: float = 0.0

        # 价差收敛跟踪：每个instrument_id维护一个deviation时间序列
        # 结构: {instrument_id: [{'ts': float, 'deviation': float, 'price': float}, ...]}
        self._deviation_history: Dict[str, List[Dict[str, Any]]] = {}

        # 最近一次tick数据（用于诊断和快照）
        self._last_tick: Optional[Dict[str, Any]] = None
        self._last_tick_ts: float = 0.0

        # 生成的模拟信号历史
        self._simulated_signals: List[SimulatedArbitrageSignal] = []

        # 诊断统计
        self._diag_stats: Dict[str, int] = {
            'layer1_hft_signal_total': 0,
            'layer1_hft_signal_passed': 0,
            'layer2_main_capture_total': 0,
            'layer2_main_capture_passed': 0,
            'layer3_standby_block_total': 0,
            'layer3_standby_block_blocked': 0,
            'simulated_signals_generated': 0,
            'snapshots_saved': 0,
        }

        logging.info("[S5-ARB-MONITOR] 初始化完成 base_dir=%s snapshot_dir=%s capital_allocation=%.2f",
                     self._base_dir, self._snapshot_dir, self.CAPITAL_ALLOCATION)

    # ------------------------------------------------------------------
    # 基础工具方法
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_base_dir() -> str:
        """从__file__向上查找项目根目录（包含ali2026v3_trading包的目录）

        优先策略：找到包含 ali2026v3_trading/__init__.py 的目录作为项目根。
        兜底：使用当前工作目录。
        """
        try:
            current = os.path.dirname(os.path.abspath(__file__))
        except (ValueError, OSError):
            return os.getcwd()
        # 最多向上查找6层（monitor→strategy→ali2026v3_trading→项目根）
        for _ in range(6):
            # 项目根目录的标志：包含 ali2026v3_trading 包
            pkg_init = os.path.join(current, 'ali2026v3_trading', '__init__.py')
            if os.path.isfile(pkg_init):
                return current
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
        # 兜底：使用当前工作目录
        return os.getcwd()

    @staticmethod
    def _now_iso() -> str:
        """当前北京时间ISO格式字符串"""
        try:
            return datetime.now(_CHINA_TZ).isoformat(timespec='milliseconds')
        except (ValueError, OSError):
            return datetime.now().isoformat(timespec='milliseconds')

    def _append_deviation_sample(self, instrument_id: str, deviation: float, price: float) -> None:
        """向价差收敛历史追加一个样本，超出窗口或数量时裁剪"""
        if not instrument_id:
            return
        now = time.time()
        samples = self._deviation_history.setdefault(instrument_id, [])
        samples.append({'ts': now, 'deviation': float(deviation), 'price': float(price)})
        # 按时间窗口裁剪
        cutoff = now - self.CONVERGENCE_WINDOW_SEC
        # 保留窗口内样本
        fresh = [s for s in samples if s['ts'] >= cutoff]
        # 数量上限裁剪
        if len(fresh) > self.MAX_CONVERGENCE_SAMPLES:
            fresh = fresh[-self.MAX_CONVERGENCE_SAMPLES:]
        self._deviation_history[instrument_id] = fresh

    # ------------------------------------------------------------------
    # 3层诊断方法
    # ------------------------------------------------------------------

    def diagnose_layer1_hft_signal(self) -> Dict[str, Any]:
        """第1层诊断：HFT通道是否产生arbitrage_signal

        检查点：
        - on_arbitrage_signal是否被回调过
        - 信号是否在有效期内（TTL=60s，对齐tick_hft._ARBITRAGE_SIGNAL_TTL_SEC）
        - confidence是否达到阈值(>=0.6，对齐handle_arbitrage_signal)

        Returns:
            诊断结果字典，包含 passed/instrument_id/deviation_bps/confidence/reason 字段。
            断言：返回值非None（由调用方在集成时校验）。
        """
        with self._lock:
            self._diag_stats['layer1_hft_signal_total'] += 1
            result: Dict[str, Any] = {
                'layer': 1,
                'passed': False,
                'instrument_id': '',
                'deviation_bps': 0.0,
                'confidence': 0.0,
                'reason': '',
                'timestamp': time.time(),
            }
            sig = self._last_hft_signal
            if sig is None:
                result['reason'] = 'no_hft_arbitrage_signal_received'
                logging.info("[S5-ARB-L1] 诊断未通过: 未收到HFT套利信号")
                return result
            # TTL检查
            elapsed = time.time() - self._last_hft_signal_ts
            if elapsed > self.CONVERGENCE_WINDOW_SEC:
                result['reason'] = f'hft_signal_expired(elapsed={elapsed:.1f}s)'
                logging.info("[S5-ARB-L1] 诊断未通过: HFT信号已过期 elapsed=%.1fs", elapsed)
                return result
            confidence = float(sig.get('confidence', 0.0))
            deviation = float(sig.get('deviation_bps', 0.0))
            instrument_id = str(sig.get('instrument_id', ''))
            result['instrument_id'] = instrument_id
            result['deviation_bps'] = deviation
            result['confidence'] = confidence
            if confidence < 0.6:
                result['reason'] = f'confidence_below_threshold({confidence:.2f}<0.60)'
                logging.info("[S5-ARB-L1] 诊断未通过: confidence=%.2f < 0.60 instr=%s",
                             confidence, instrument_id)
                return result
            result['passed'] = True
            result['reason'] = 'hft_signal_valid'
            self._diag_stats['layer1_hft_signal_passed'] += 1
            logging.info("[S5-ARB-L1] 诊断通过: instr=%s dev=%.1fbps conf=%.2f",
                         instrument_id, deviation, confidence)
            return result

    def diagnose_layer2_main_capture(self) -> Dict[str, Any]:
        """第2层诊断：hft_consumed=True的信号是否被主交易周期捕获

        检查点（对齐strategy_config_layer.py:232-248的ARBITRAGE分支条件）：
        - HFT信号是否被消费(hft_consumed=True)
        - 主周期触发条件: confidence>=0.8 且 deviation>100bps 且 not hft_consumed

        Returns:
            诊断结果字典，包含 passed/captured/hft_consumed/reason 字段。
        """
        with self._lock:
            self._diag_stats['layer2_main_capture_total'] += 1
            result: Dict[str, Any] = {
                'layer': 2,
                'passed': False,
                'captured': False,
                'hft_consumed': False,
                'confidence': 0.0,
                'deviation_bps': 0.0,
                'reason': '',
                'timestamp': time.time(),
            }
            sig = self._last_hft_signal
            if sig is None:
                result['reason'] = 'no_hft_signal_to_check'
                logging.info("[S5-ARB-L2] 诊断未通过: 无HFT信号可检查")
                return result
            hft_consumed = bool(sig.get('hft_consumed', False))
            confidence = float(sig.get('confidence', 0.0))
            deviation = abs(float(sig.get('deviation_bps', 0.0)))
            result['hft_consumed'] = hft_consumed
            result['confidence'] = confidence
            result['deviation_bps'] = deviation
            # 主周期捕获条件：高置信度+大deviation+未被HFT消费
            captured = (confidence >= 0.8) and (deviation > 100.0) and (not hft_consumed)
            result['captured'] = captured
            if captured:
                result['passed'] = True
                result['reason'] = 'main_cycle_captured_arbitrage'
                self._diag_stats['layer2_main_capture_passed'] += 1
                logging.info("[S5-ARB-L2] 诊断通过: 主周期捕获套利 conf=%.2f dev=%.1fbps",
                             confidence, deviation)
            else:
                # 给出未捕获的具体原因
                if hft_consumed:
                    result['reason'] = 'hft_already_consumed_signal'
                elif confidence < 0.8:
                    result['reason'] = f'confidence_below_main_threshold({confidence:.2f}<0.80)'
                elif deviation <= 100.0:
                    result['reason'] = f'deviation_below_main_threshold({deviation:.1f}<=100bps)'
                else:
                    result['reason'] = 'unknown_not_captured'
                logging.info("[S5-ARB-L2] 诊断未通过: %s hft_consumed=%s conf=%.2f dev=%.1fbps",
                             result['reason'], hft_consumed, confidence, deviation)
            return result

    def diagnose_layer3_standby_block(self, slot_state: Optional[str] = None) -> Dict[str, Any]:
        """第3层诊断：STANDBY槽位是否阻止开仓

        检查点（对齐strategy_ecosystem._models.SlotState）：
        - 槽位状态是否为STANDBY（此时阻止开仓）
        - STANDBY只能转ACTIVE，不能直接开仓

        Args:
            slot_state: S5套利策略槽位状态字符串。
                        为None时默认视为'active'（不阻止）。

        Returns:
            诊断结果字典，包含 passed/blocked/slot_state/reason 字段。
        """
        with self._lock:
            self._diag_stats['layer3_standby_block_total'] += 1
            state = str(slot_state or 'active').lower()
            result: Dict[str, Any] = {
                'layer': 3,
                'passed': True,
                'blocked': False,
                'slot_state': state,
                'reason': '',
                'timestamp': time.time(),
            }
            if state == 'standby':
                result['blocked'] = True
                result['passed'] = False
                result['reason'] = 'slot_in_standby_blocks_open'
                self._diag_stats['layer3_standby_block_blocked'] += 1
                logging.info("[S5-ARB-L3] 诊断未通过: 槽位STANDBY阻止开仓")
            elif state in ('inactive', 'retired', 'handover'):
                result['blocked'] = True
                result['passed'] = False
                result['reason'] = f'slot_state_{state}_blocks_open'
                self._diag_stats['layer3_standby_block_blocked'] += 1
                logging.info("[S5-ARB-L3] 诊断未通过: 槽位状态%s阻止开仓", state)
            else:
                result['reason'] = 'slot_allows_open'
                logging.info("[S5-ARB-L3] 诊断通过: 槽位状态=%s 允许开仓", state)
            return result

    # ------------------------------------------------------------------
    # 价差收敛逻辑
    # ------------------------------------------------------------------

    def track_spread_convergence(self, instrument_id: str) -> Dict[str, Any]:
        """跟踪deviation的收敛过程

        基于self._deviation_history中instrument_id对应的时间序列，
        计算收敛速率（bps/秒）和收敛趋势。

        收敛速率定义：
            (首个样本deviation - 末个样本deviation) / 时间跨度(秒)
            正值表示价差在收敛（deviation在减小），负值表示发散。

        Args:
            instrument_id: 合约ID

        Returns:
            收敛分析字典，包含 convergence_rate/trend/sample_count/time_span 字段。
        """
        with self._lock:
            result: Dict[str, Any] = {
                'instrument_id': instrument_id,
                'convergence_rate': 0.0,
                'trend': 'unknown',
                'sample_count': 0,
                'time_span_sec': 0.0,
                'start_deviation': 0.0,
                'end_deviation': 0.0,
            }
            samples = self._deviation_history.get(instrument_id, [])
            if not samples:
                result['trend'] = 'no_data'
                logging.info("[S5-ARB-CONV] 价差收敛跟踪无数据 instr=%s", instrument_id)
                return result
            # 裁剪到时间窗口内
            now = time.time()
            cutoff = now - self.CONVERGENCE_WINDOW_SEC
            fresh = [s for s in samples if s['ts'] >= cutoff]
            if len(fresh) < 2:
                result['trend'] = 'insufficient_samples'
                result['sample_count'] = len(fresh)
                logging.info("[S5-ARB-CONV] 价差收敛样本不足 instr=%s count=%d",
                             instrument_id, len(fresh))
                return result
            start = fresh[0]
            end = fresh[-1]
            time_span = end['ts'] - start['ts']
            start_dev = float(start['deviation'])
            end_dev = float(end['deviation'])
            # 收敛速率：deviation减小为正（收敛），增大为负（发散）
            rate = safe_divide(start_dev - end_dev, time_span, default=0.0)
            result['convergence_rate'] = rate
            result['trend'] = 'converging' if rate > 0 else ('diverging' if rate < 0 else 'flat')
            result['sample_count'] = len(fresh)
            result['time_span_sec'] = time_span
            result['start_deviation'] = start_dev
            result['end_deviation'] = end_dev
            logging.info("[S5-ARB-CONV] 价差收敛分析 instr=%s rate=%.4fbps/s trend=%s samples=%d span=%.1fs",
                         instrument_id, rate, result['trend'], len(fresh), time_span)
            return result

    def evaluate_arbitrage_quality(self, instrument_id: str,
                                   volume: float = 0.0) -> Dict[str, Any]:
        """评估套利信号质量（基于价差收敛速度、波动率、成交量）

        质量评分(quality_score)合成逻辑：
            - 收敛分量(40%): |convergence_rate| 归一化到[0,1]，收敛趋势额外加权
            - 波动率分量(30%): 1 - min(volatility/阈值, 1)，波动率越低质量越高
            - 成交量分量(30%): min(volume/阈值, 1)，成交量越高质量越高

        Args:
            instrument_id: 合约ID
            volume: 当前成交量（可选）

        Returns:
            质量评估字典，包含 quality_score/convergence_rate/volatility/volume 字段。
        """
        with self._lock:
            conv = self.track_spread_convergence(instrument_id)
            conv_rate = float(conv.get('convergence_rate', 0.0))
            trend = conv.get('trend', 'unknown')
            # 收敛分量：速率绝对值归一化（假设10bps/s为满分）
            conv_component = min(abs(conv_rate) / 10.0, 1.0)
            # 收敛趋势额外加权（发散时减分）
            if trend == 'converging':
                conv_component = min(conv_component * 1.2, 1.0)
            elif trend == 'diverging':
                conv_component = conv_component * 0.5
            # 波动率分量：基于deviation序列的标准差
            samples = self._deviation_history.get(instrument_id, [])
            deviations = [float(s['deviation']) for s in samples] if samples else []
            volatility = stable_std(deviations) if deviations else 0.0
            # 波动率阈值50bps，超过则质量下降
            vol_component = 1.0 - min(volatility / 50.0, 1.0)
            # 成交量分量：阈值1000手
            vol_value = float(volume) if volume and volume > 0 else 0.0
            volume_component = min(vol_value / 1000.0, 1.0)
            # 合成质量评分
            quality_score = (
                conv_component * 0.4
                + vol_component * 0.3
                + volume_component * 0.3
            )
            quality_score = max(0.0, min(1.0, quality_score))
            result: Dict[str, Any] = {
                'instrument_id': instrument_id,
                'quality_score': round(quality_score, 4),
                'convergence_rate': round(conv_rate, 4),
                'trend': trend,
                'volatility': round(volatility, 4),
                'volume': vol_value,
                'components': {
                    'convergence': round(conv_component, 4),
                    'volatility': round(vol_component, 4),
                    'volume': round(volume_component, 4),
                },
            }
            logging.info("[S5-ARB-QUALITY] 套利质量评估 instr=%s quality=%.4f conv_rate=%.4f vol=%.4f volume=%.1f",
                         instrument_id, quality_score, conv_rate, volatility, vol_value)
            return result

    # ------------------------------------------------------------------
    # 实盘模拟（生成信号+保存快照，不实际下单）
    # ------------------------------------------------------------------

    def generate_simulated_signal(self, action: str = 'OPEN') -> Optional[SimulatedArbitrageSignal]:
        """生成模拟开仓/平仓信号（实盘模拟，不实际下单）

        重要：本方法不调用OrderService.send_order，不调用route_capital。
        仅生成SimulatedArbitrageSignal并保存快照，供其他策略参考方向。

        Args:
            action: 'OPEN' 生成开仓信号，'CLOSE' 生成平仓信号

        Returns:
            模拟信号对象；无有效HFT信号源时返回None。
        """
        with self._lock:
            sig = self._last_hft_signal
            if sig is None:
                logging.info("[S5-ARB-SIM] 无法生成模拟信号: 无HFT套利信号源")
                return None
            instrument_id = str(sig.get('instrument_id', ''))
            if not instrument_id:
                logging.info("[S5-ARB-SIM] 无法生成模拟信号: instrument_id为空")
                return None
            direction = str(sig.get('direction', 'BUY')).upper()
            if direction not in ('BUY', 'SELL'):
                direction = 'BUY'
            deviation = float(sig.get('deviation_bps', 0.0))
            confidence = float(sig.get('confidence', 0.0))
            entry_price = float(sig.get('entry_price', 0.0))
            # 价差收敛速率
            conv = self.track_spread_convergence(instrument_id)
            conv_rate = float(conv.get('convergence_rate', 0.0))
            # 质量评分
            quality = self.evaluate_arbitrage_quality(instrument_id)
            quality_score = float(quality.get('quality_score', 0.0))
            # 信号类型
            if action.upper() == 'CLOSE':
                signal_type = 'CLOSE_LONG' if direction == 'BUY' else 'CLOSE_SHORT'
                # 平仓方向反向
                close_direction = 'SELL' if direction == 'BUY' else 'BUY'
                sim_direction = close_direction
            else:
                signal_type = 'OPEN_LONG' if direction == 'BUY' else 'OPEN_SHORT'
                sim_direction = direction
            signal = SimulatedArbitrageSignal(
                signal_id=generate_prefixed_id('S5ARB', 12),
                instrument_id=instrument_id,
                direction=sim_direction,
                signal_type=signal_type,
                deviation_bps=deviation,
                confidence=confidence,
                entry_price=entry_price,
                spread_convergence_rate=conv_rate,
                quality_score=quality_score,
                timestamp=time.time(),
                simulated=True,
                capital_allocation=self.CAPITAL_ALLOCATION,
                hft_consumed=bool(sig.get('hft_consumed', False)),
                source='s5_arbitrage_monitor',
            )
            # 保存到历史
            self._simulated_signals.append(signal)
            if len(self._simulated_signals) > self.MAX_HISTORY_SIGNALS:
                self._simulated_signals = self._simulated_signals[-self.MAX_HISTORY_SIGNALS:]
            self._diag_stats['simulated_signals_generated'] += 1
            logging.info("[S5-ARB-SIM] 生成模拟信号 id=%s instr=%s type=%s dir=%s dev=%.1fbps conf=%.2f quality=%.4f (不实际下单)",
                         signal.signal_id, instrument_id, signal_type, sim_direction,
                         deviation, confidence, quality_score)
            # 保存快照
            self.save_snapshot(signal)
            return signal

    def save_snapshot(self, signal: SimulatedArbitrageSignal) -> str:
        """保存信号快照到日志和数据存储

        快照保存为JSON格式到 logs/arbitrage_monitor/ 目录。
        保存后断言文件存在。

        Args:
            signal: 模拟信号对象

        Returns:
            快照文件绝对路径
        """
        with self._lock:
            # 文件名: S5ARB_<signal_id>_<timestamp>.json
            safe_ts = datetime.now(_CHINA_TZ).strftime('%Y%m%d_%H%M%S_%f')
            filename = f"S5ARB_{signal.signal_id}_{safe_ts}.json"
            filepath = os.path.join(self._snapshot_dir, filename)
            # 构建快照内容
            snapshot: Dict[str, Any] = {
                'signal': signal.to_dict(),
                'snapshot_time_iso': self._now_iso(),
                'snapshot_time_ts': time.time(),
                'monitor_stats': dict(self._diag_stats),
                'strategy_meta': {
                    'strategy_id': self.STRATEGY_ID,
                    'strategy_type': self.STRATEGY_TYPE,
                    'capital_allocation': self.CAPITAL_ALLOCATION,
                    'simulated': True,
                    'note': 'S5套利策略实盘模拟，不参与资金分配，不实际下单',
                },
            }
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
                self._diag_stats['snapshots_saved'] += 1
                logging.info("[S5-ARB-SNAPSHOT] 快照已保存 file=%s signal_id=%s",
                             filepath, signal.signal_id)
            except (OSError, IOError, ValueError, TypeError) as e:
                logging.warning("[S5-ARB-SNAPSHOT] 快照保存失败 file=%s error=%s", filepath, e)
                raise
            # 断言文件存在（满足"信号快照保存后断言文件存在"要求）
            assert os.path.exists(filepath), f"快照文件保存后不存在: {filepath}"
            return filepath

    # ------------------------------------------------------------------
    # 集成接口
    # ------------------------------------------------------------------

    def on_tick(self, tick_data: Dict[str, Any]) -> None:
        """tick数据回调

        从tick中提取instrument_id/price/deviation，更新价差收敛历史。
        本方法不触发下单，仅更新监控状态。

        Args:
            tick_data: tick数据字典，需包含 instrument_id/last_price 字段，
                       可选 deviation_bps/volume 字段。
        """
        if not tick_data or not isinstance(tick_data, dict):
            return
        try:
            instrument_id = str(tick_data.get('instrument_id', ''))
            if not instrument_id:
                return
            price = float(tick_data.get('last_price', 0.0) or tick_data.get('price', 0.0) or 0.0)
            deviation = float(tick_data.get('deviation_bps', 0.0) or 0.0)
            volume = float(tick_data.get('volume', 0.0) or 0.0)
            with self._lock:
                self._last_tick = dict(tick_data)
                self._last_tick_ts = time.time()
                if deviation != 0.0 or price > 0:
                    self._append_deviation_sample(instrument_id, deviation, price)
            logging.debug("[S5-ARB-TICK] tick更新 instr=%s price=%.4f dev=%.2fbps vol=%.1f",
                          instrument_id, price, deviation, volume)
        except (ValueError, KeyError, TypeError) as e:
            logging.debug("[S5-ARB-TICK] tick处理异常: %s", e)

    def on_arbitrage_signal(self, signal: Dict[str, Any]) -> None:
        """套利信号回调（从HFT通道接收）

        接收来自tick_hft.handle_arbitrage_signal的套利信号，
        更新内部状态并跟踪价差收敛。本方法不触发下单。

        Args:
            signal: 套利信号字典，需包含 instrument_id/direction/deviation_bps/confidence 字段。
        """
        if not signal or not isinstance(signal, dict):
            return
        try:
            instrument_id = str(signal.get('instrument_id', ''))
            direction = str(signal.get('direction', ''))
            deviation = float(signal.get('deviation_bps', 0.0))
            confidence = float(signal.get('confidence', 0.0))
            entry_price = float(signal.get('entry_price', 0.0))
            with self._lock:
                self._last_hft_signal = dict(signal)
                self._last_hft_signal_ts = time.time()
                # 追加到价差收敛历史
                self._append_deviation_sample(instrument_id, deviation, entry_price)
            logging.info("[S5-ARB-SIG] 接收HFT套利信号 instr=%s dir=%s dev=%.1fbps conf=%.2f price=%.4f",
                         instrument_id, direction, deviation, confidence, entry_price)
        except (ValueError, KeyError, TypeError) as e:
            logging.warning("[S5-ARB-SIG] 套利信号处理异常: %s", e)

    def get_monitoring_report(self) -> Dict[str, Any]:
        """获取监控报告

        汇总3层诊断结果、价差收敛分析、最近模拟信号和统计信息。

        Returns:
            监控报告字典。断言返回值非None。
        """
        with self._lock:
            # 执行3层诊断
            layer1 = self.diagnose_layer1_hft_signal()
            layer2 = self.diagnose_layer2_main_capture()
            layer3 = self.diagnose_layer3_standby_block()
            # 价差收敛分析（基于最近HFT信号的instrument_id）
            instrument_id = ''
            if self._last_hft_signal is not None:
                instrument_id = str(self._last_hft_signal.get('instrument_id', ''))
            convergence = self.track_spread_convergence(instrument_id) if instrument_id else {
                'instrument_id': '', 'convergence_rate': 0.0, 'trend': 'no_signal', 'sample_count': 0,
            }
            # 最近模拟信号
            recent_signals = [s.to_dict() for s in self._simulated_signals[-10:]]
            report: Dict[str, Any] = {
                'strategy_id': self.STRATEGY_ID,
                'strategy_type': self.STRATEGY_TYPE,
                'capital_allocation': self.CAPITAL_ALLOCATION,
                'report_time_iso': self._now_iso(),
                'report_time_ts': time.time(),
                'diagnostics': {
                    'layer1_hft_signal': layer1,
                    'layer2_main_capture': layer2,
                    'layer3_standby_block': layer3,
                },
                'spread_convergence': convergence,
                'recent_simulated_signals': recent_signals,
                'stats': dict(self._diag_stats),
                'last_hft_signal_ts': self._last_hft_signal_ts,
                'last_tick_ts': self._last_tick_ts,
            }
            logging.info("[S5-ARB-REPORT] 监控报告生成 L1=%s L2=%s L3=%s signals=%d",
                         layer1.get('passed'), layer2.get('passed'), layer3.get('passed'),
                         len(recent_signals))
            return report

    # ------------------------------------------------------------------
    # 辅助查询方法
    # ------------------------------------------------------------------

    def get_last_simulated_signal(self) -> Optional[SimulatedArbitrageSignal]:
        """获取最近一次生成的模拟信号"""
        with self._lock:
            if not self._simulated_signals:
                return None
            return self._simulated_signals[-1]

    def get_stats(self) -> Dict[str, int]:
        """获取诊断统计"""
        with self._lock:
            return dict(self._diag_stats)

    @classmethod
    def get_instance(cls) -> 'ArbitrageMonitor':
        """获取全局ArbitrageMonitor单例（类方法接口，兼容tick_hft.py调用）

        FIX-20260711-S5S6-CHAIN: 添加此类方法，使tick_hft.py中的
        ArbitrageMonitor.get_instance()调用能正常工作。
        """
        return get_arbitrage_monitor()


# ============================================================================
# 模块级单例（可选使用，与health_monitor等模块保持一致）
# ============================================================================

_monitor_instance: Optional[ArbitrageMonitor] = None
_monitor_lock = threading.Lock()


def get_arbitrage_monitor() -> ArbitrageMonitor:
    """获取全局ArbitrageMonitor单例"""
    global _monitor_instance
    with _monitor_lock:
        if _monitor_instance is None:
            _monitor_instance = ArbitrageMonitor()
        return _monitor_instance
