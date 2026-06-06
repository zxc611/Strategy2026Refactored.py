"""risk_audit_utils.py - 风控审计/告警/报告工具函数

从risk_service.py提取的模块级辅助函数和类。
通过risk_service.py re-export保持向后兼容。
"""

from __future__ import annotations

import copy
import json
import logging
import math
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
from enum import Enum, auto
from dataclasses import dataclass, field

import numpy as np

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_jsonl_append_line
from ali2026v3_trading import config_params
from ali2026v3_trading.resilience_utils import (
    stable_sum, stable_mean, stable_variance, stable_std,
    safe_divide, KahanSummation, approx_equal, approx_less,
    PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
)
from ali2026v3_trading.shared_utils import safe_int, safe_float

_CHINA_TZ = timezone(timedelta(hours=8))


def safe_get(obj: Any, attr: str, default: Any = None, target_type: type = float) -> Any:
    """统一安全属性获取入口（方法唯一修复：公开为safe_get，并行接口为便捷别名）"""
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return target_type(val) if target_type == int else float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_%s] Error getting %s: %s", target_type.__name__, attr, e)
        return default


def safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    return safe_get(obj, attr, default, float)


def safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    return safe_get(obj, attr, default, int)


_risk_service_instances: Dict[str, RiskService] = {}
_risk_service_lock = threading.Lock()



def _normalize_risk_scope_id(scope_id: Optional[str], strategy: Any = None) -> str:
    if scope_id:
        return str(scope_id)
    strategy_id = None
    if isinstance(strategy, dict):
        strategy_id = strategy.get('strategy_id')
    elif strategy is not None:
        strategy_id = getattr(strategy, 'strategy_id', None)
    return str(strategy_id or 'default')



class AlertLevel(Enum):
    """OPS-05修复: 告警级别枚举 — P0/P1/P2分类"""
    P0 = "P0"  # 紧急: 系统级故障，需立即处理（如: 硬停止触发、断路器熔断）
    P1 = "P1"  # 严重: 业务级异常，需30分钟内响应（如: 大额亏损、数据异常）
    P2 = "P2"  # 一般: 需关注但不紧急（如: 限频触发、参数偏离）


# OPS-05修复: 告警级别→日志级别映射
_ALERT_LEVEL_LOG_MAP = {
    AlertLevel.P0: logging.CRITICAL,
    AlertLevel.P1: logging.ERROR,
    AlertLevel.P2: logging.WARNING,
}

# OPS-05修复: 告警级别→默认超时秒数（超时后升级）
_ALERT_LEVEL_TIMEOUT_SEC = {
    AlertLevel.P0: 300.0,   # P0: 5分钟未处理则升级
    AlertLevel.P1: 1800.0,  # P1: 30分钟未处理则升级
    AlertLevel.P2: 0.0,     # P2: 不自动升级
}



class AlertDeduplicator:
    """OPS-07修复: 告警去重器 — 在时间窗口内抑制重复告警

    防止同一告警在短时间内重复触发（告警风暴）。
    相同key的告警在window_sec内只放行第一次。
    """

    def __init__(self, window_sec: float = 60.0):
        self._window_sec = window_sec
        self._recent_alerts: Dict[str, float] = {}  # key → last_alert_timestamp
        self._lock = threading.Lock()

    def should_alert(self, key: str) -> bool:
        """检查该key的告警是否应放行（去重后）

        Returns:
            True: 应发送告警; False: 在窗口内已发送过，抑制
        """
        now = time.time()
        with self._lock:
            last_ts = self._recent_alerts.get(key, 0.0)
            if now - last_ts < self._window_sec:
                return False
            self._recent_alerts[key] = now
            return True

    def reset(self, key: str = None) -> None:
        """重置去重状态"""
        with self._lock:
            if key:
                self._recent_alerts.pop(key, None)
            else:
                self._recent_alerts.clear()


# 全局告警去重器实例
_alert_deduplicator: Optional[AlertDeduplicator] = None
_alert_deduplicator_lock = threading.Lock()



def get_alert_deduplicator() -> AlertDeduplicator:
    global _alert_deduplicator
    with _alert_deduplicator_lock:
        if _alert_deduplicator is None:
            _alert_deduplicator = AlertDeduplicator()
        return _alert_deduplicator


# OPS-06修复: 告警升级跟踪
_alert_escalation_tracker: Dict[str, Dict[str, Any]] = {}
_alert_escalation_lock = threading.Lock()



def _check_alert_escalation() -> None:
    """OPS-06修复: 检查P0/P1告警是否超时未处理，自动升级"""
    now = time.time()
    with _alert_escalation_lock:
        to_escalate = []
        for key, info in _alert_escalation_tracker.items():
            level = info.get('level')
            if level is None:
                continue
            timeout = _ALERT_LEVEL_TIMEOUT_SEC.get(level, 0.0)
            if timeout <= 0:
                continue
            if info.get('escalated', False):
                continue
            elapsed = now - info.get('timestamp', 0.0)
            if elapsed > timeout:
                to_escalate.append(key)

        for key in to_escalate:
            info = _alert_escalation_tracker[key]
            info['escalated'] = True
            info['escalated_at'] = now
            logging.critical(
                "[OPS-06] P0告警超时升级! key=%s level=%s elapsed=%.0fs "
                "已自动升级通知 — 需立即处理!",
                key, info.get('level', ''), now - info.get('timestamp', 0.0),
            )
            # 触发告警回调
            try:
                RiskService._fire_alert('ALERT_ESCALATION', {
                    'alert_key': key,
                    'original_level': str(info.get('level', '')),
                    'elapsed_sec': now - info.get('timestamp', 0.0),
                })
            except Exception:
                logging.warning("[R22-EP-P1] RiskService exception swallowed")
                pass



def alert(level: AlertLevel, key: str, message: str,
          detail: Optional[Dict[str, Any]] = None) -> bool:
    """OPS-05/06/07修复: 分级告警函数

    Args:
        level: 告警级别 (P0/P1/P2)
        key: 告警唯一标识（用于去重和升级跟踪）
        message: 告警消息
        detail: 告警详情字典

    Returns:
        True: 告警已发送; False: 被去重抑制
    """
    # OPS-07: 去重检查
    dedup = get_alert_deduplicator()
    if not dedup.should_alert(key):
        logging.debug("[OPS-07] 告警去重抑制: key=%s", key)
        return False

    # OPS-05: 按级别记录日志
    log_level = _ALERT_LEVEL_LOG_MAP.get(level, logging.WARNING)
    logging.log(
        log_level,
        "[OPS-05] [%s] %s key=%s detail=%s",
        level.value, message, key, detail or {},
    )

    # OPS-06: P0/P1告警注册升级跟踪
    if level in (AlertLevel.P0, AlertLevel.P1):
        with _alert_escalation_lock:
            _alert_escalation_tracker[key] = {
                'level': level,
                'message': message,
                'timestamp': time.time(),
                'escalated': False,
                'detail': detail,
            }
        # 立即检查是否有需要升级的告警
        _check_alert_escalation()

    # 触发告警回调
    try:
        RiskService._fire_alert(f'ALERT_{level.value}', {
            'key': key,
            'level': level.value,
            'message': message,
            'detail': detail or {},
        })
    except Exception:
        logging.warning("[R22-EP-P1] RiskService exception swallowed")
        pass

    return True


# OPS-15修复: 操作审计日志
_audit_log_path: Optional[str] = None
_audit_log_lock = threading.Lock()



def _get_audit_log_path() -> str:
    global _audit_log_path
    if _audit_log_path is None:
        _audit_log_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'operations_audit.jsonl'
        )
        os.makedirs(os.path.dirname(_audit_log_path), exist_ok=True)
    return _audit_log_path



def operations_audit_log(action: str, operator: str = "unknown",
                         result: str = "", detail: Any = None) -> None:
    """OPS-15修复: 操作审计日志 — 记录谁在何时做了什么

    Args:
        action: 操作类型（如 EMERGENCY_STOP, EMERGENCY_DEGRADE, CONFIRM_RESUME 等）
        operator: 操作人标识
        result: 操作结果（success/partial/failed）
        detail: 操作详情
    """
    record = {
        'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'action': action,
        'operator': operator,
        'result': result,
        'detail': detail if detail is not None else {},
    }
    try:
        with _audit_log_lock:
            with open(_get_audit_log_path(), 'a', encoding='utf-8') as f:
                safe_jsonl_append_line(f, record)
    except Exception as e:
        logging.warning("[OPS-15] 审计日志写入失败: %s", e)


# R24-P0-TR-02修复: 不可否认性机制 — hash chain
_audit_chain_lock = threading.Lock()
_audit_chain_last_hash = 'genesis_00000000'


def audit_chain_append(event_type: str, payload: dict) -> str:
    """R24-P0-TR-02: 不可否认性hash chain — 每个审计事件链接到前一个事件的hash"""
    global _audit_chain_last_hash
    import hashlib as _hl
    record = {
        'timestamp': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'event_type': event_type,
        'payload': payload,
        'prev_hash': _audit_chain_last_hash,
    }
    _raw = f"{record['timestamp']}:{event_type}:{_audit_chain_last_hash}"
    _new_hash = _hl.sha256(_raw.encode()).hexdigest()[:16]
    record['hash'] = _new_hash
    with _audit_chain_lock:
        _audit_chain_last_hash = _new_hash
    # 写入hash chain日志
    try:
        _chain_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'audit_chain.jsonl')
        os.makedirs(os.path.dirname(_chain_path), exist_ok=True)
        with open(_chain_path, 'a', encoding='utf-8') as f:
            safe_jsonl_append_line(f, record)
    except Exception as e:
        logging.warning("[R24-P0-TR-02] audit_chain write failed: %s", e)
    return _new_hash


# R27-CP-05-FIX: structured_audit_log已提取至audit_log_utils.py，消除config_params↔risk_service双向依赖
from ali2026v3_trading.audit_log_utils import structured_audit_log  # R27-CP-05-FIX


# R24-P0-TR-04修复: 系统状态快照机制
_state_snapshot_lock = threading.Lock()


def save_state_snapshot(snapshot_data: dict, tag: str = '') -> None:
    """R24-P0-TR-04: 保存系统状态快照，支持时序重建"""
    import time as _time
    record = {
        'snapshot_time': _get_tz_aware_now().isoformat(),  # P1-R11-12修复: 使用UTC时区
        'tag': tag,
        'data': snapshot_data,
    }
    try:
        with _state_snapshot_lock:
            _snap_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'state_snapshots')
            os.makedirs(_snap_dir, exist_ok=True)
            _fname = f"snapshot_{_get_tz_aware_now().strftime('%Y%m%d_%H%M%S')}_{tag}.json"  # P1-R11-12修复: 使用UTC时区
            with open(os.path.join(_snap_dir, _fname), 'w', encoding='utf-8') as f:
                f.write(json_dumps(record))
    except Exception as e:
        logging.warning("[R24-P0-TR-04] State snapshot save failed: %s", e)


# R24-P0-TR-09修复: 交易所要求报告生成

def generate_exchange_report(trades: list, output_path: str = None) -> str:
    """R24-P0-TR-09: 生成交易所要求的交易报告（时间戳/方向/价格/数量）"""
    import csv
    if output_path is None:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'exchange_report.csv')
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'instrument_id', 'direction', 'price', 'volume', 'order_id', 'signal_id'])
            for t in trades:
                writer.writerow([
                    t.get('timestamp', ''),
                    t.get('instrument_id', ''),
                    t.get('direction', ''),
                    t.get('price', ''),
                    t.get('volume', ''),
                    t.get('order_id', ''),
                    t.get('signal_id', ''),
                ])
        logging.info("[R24-P0-TR-09] Exchange report generated: %s (%d trades)", output_path, len(trades))
        return output_path
    except Exception as e:
        logging.error("[R24-P0-TR-09] Exchange report generation failed: %s", e)
        return ''


# ============================================================================
# P0-4修复：简化SPAN保证金模拟器
# ============================================================================


class SimplifiedSPAN:
    """简化SPAN保证金模拟器 (基于16情景风险阵列)
    
    用于回测中模拟组合保证金，避免固定比例保证金低估套利/做市策略资金占用。
    """
    
    SCENARIOS = [
        (0, 0),      # 基准
        (+1, 0), (-1, 0), (+2, 0), (-2, 0), (+3, 0), (-3, 0),  # 价格变动
        (0, +1), (0, -1), (0, +2), (0, -2),  # IV变动
        (+1, +1), (+1, -1), (-1, +1), (-1, -1),  # 联合变动
    ]
    
    def __init__(self, price_sigma: float = 0.01, iv_sigma: float = 0.25,
                 min_base_margin: float = 1000.0,
                 capital_scale: str = 'medium'):
        self.price_sigma = price_sigma
        self.iv_sigma = iv_sigma
        # R15-P1-BIZ-01修复: min_base_margin按capital_scale动态调整默认值
        _scale_margin_map = {'small': 500.0, 'medium': 1000.0, 'large': 5000.0}
        if min_base_margin == 1000.0 and capital_scale in _scale_margin_map:
            min_base_margin = _scale_margin_map[capital_scale]
        # P2-R8-05修复: 最低保证金参数化，不再硬编码1000元
        self.min_base_margin = min_base_margin
    
    def calc_margin(self, positions: List[Dict], underlying_price: float, current_iv: float) -> float:
        """计算组合保证金
        
        Args:
            positions: 持仓列表，每个包含 {'quantity', 'current_value', 'delta', 'gamma', 'vega'}
            underlying_price: 标的价格
            current_iv: 当前IV
        
        Returns:
            保证金金额
        """
        max_loss = 0.0
        
        for price_shift_sigma, iv_shift_pct in self.SCENARIOS:
            price_new = underlying_price * (1 + price_shift_sigma * self.price_sigma)
            iv_new = current_iv * (1 + iv_shift_pct * self.iv_sigma)
            
            portfolio_pnl = 0.0
            for pos in positions:
                # 简化PnL近似：Delta * dS + 0.5 * Gamma * dS^2 + Vega * dIV
                dS = price_new - underlying_price
                dIV = iv_new - current_iv
                
                delta = pos.get('delta', 0.0)
                gamma = pos.get('gamma', 0.0)
                vega = pos.get('vega', 0.0)
                
                pnl = delta * dS + 0.5 * gamma * dS**2 + vega * dIV
                if np.isfinite(pnl):  # NP-P2-12: portfolio_pnl累加添加isfinite检查
                    portfolio_pnl += pnl * pos.get('quantity', 0)
                if abs(portfolio_pnl) > 1e12:  # NP-P2-12: 溢出检测
                    logging.warning("[NP-P2-12] portfolio_pnl overflow detected: %.2e", portfolio_pnl)
                    break
            
            max_loss = min(max_loss, portfolio_pnl)
        
        # 净保证金 = 最大亏损 + 最低保证金
        # P2-R8-05修复: 使用参数化min_base_margin替代硬编码1000.0
        return abs(max_loss) + self.min_base_margin
    
    def calc_margin_ratio(self, positions: List[Dict], underlying_price: float,
                          current_iv: float, total_position_value: float,
                          equity: float = 0.0) -> float:
        """计算保证金比率

        INV-05/INV-CAP-03修复: 增加equity参数，保证金超过权益时返回1.0并告警

        Args:
            positions: 持仓列表
            underlying_price: 标的价格
            current_iv: 当前IV
            total_position_value: 持仓总价值
            equity: 账户权益（INV-CAP-03新增）

        Returns:
            保证金/持仓价值比率
        """
        if total_position_value <= 0:
            return 0.20  # 默认20%

        margin = self.calc_margin(positions, underlying_price, current_iv)
        # INV-05/INV-CAP-03修复: 保证金占用超过权益时告警
        if equity > 0 and margin > equity:
            logging.critical(
                "[SimplifiedSPAN] INV-CAP-03: margin(%.2f) > equity(%.2f), "
                "账户资不抵债，保证金比率钳制为1.0",
                margin, equity,
            )
            return 1.0
        return margin / total_position_value



def calculate_var_historical(returns: List[float], confidence_level: float = 0.95) -> float:
    """计算历史VaR(Value at Risk)
    
    手册第268行要求: "历史滚动VaR(95%)"
    
    Args:
        returns: 收益率序列
        confidence_level: 置信水平，默认95%
    
    Returns:
        VaR值(正值表示潜在损失)
    """
    if not returns or len(returns) < 10:
        return 0.0
    
    returns_arr = np.array(returns)
    sorted_returns = np.sort(returns_arr)
    index = safe_int((1.0 - confidence_level) * len(sorted_returns))
    index = max(0, min(index, len(sorted_returns) - 1))
    
    var_value = -sorted_returns[index]
    return float(var_value)



def calculate_var_rolling(returns: List[float], window: int = 252, confidence_level: float = 0.95) -> List[float]:
    """计算滚动VaR
    
    Args:
        returns: 收益率序列
        window: 滚动窗口大小
        confidence_level: 置信水平
    
    Returns:
        滚动VaR序列
    """
    if not returns or len(returns) < window:
        return [calculate_var_historical(returns, confidence_level)] if returns else [0.0]
    
    rolling_vars = []
    for i in range(window, len(returns) + 1):
        window_returns = returns[i - window:i]
        var_val = calculate_var_historical(window_returns, confidence_level)
        rolling_vars.append(var_val)
    
    return rolling_vars



def check_circuit_breaker_auto_recovery(risk_service_instance, cooldown_sec: float = 300.0) -> bool:
    """R23-SM-06-FIX: 风控熔断自动恢复检查
    
    熔断后经cooldown_sec秒冷却期，若风险指标回归正常则自动解除熔断。
    Args:
        risk_service_instance: RiskService实例
        cooldown_sec: 冷却期(秒)，默认300秒(5分钟)
    Returns:
        是否执行了恢复
    """
    safety = getattr(risk_service_instance, '_safety_meta_layer', None)
    if safety is None:
        return False
    try:
        if hasattr(safety, 'is_circuit_breaker_active') and safety.is_circuit_breaker_active():
            breaker_time = getattr(safety, '_circuit_breaker_activated_at', 0.0)
            if breaker_time > 0 and (time.time() - breaker_time) > cooldown_sec:
                if hasattr(safety, 'reset_circuit_breaker'):
                    safety.reset_circuit_breaker()
                    logging.info("[R23-SM-06-FIX] 风控熔断自动恢复: 冷却期%.1fs已过，熔断已解除", cooldown_sec)
                    return True
                else:
                    logging.warning("[R23-SM-06-FIX] SafetyMetaLayer缺少reset_circuit_breaker方法，无法自动恢复")
    except Exception as e:
        logging.warning("[R23-SM-06-FIX] 熔断自动恢复检查异常: %s", e)
    return False


def validate_bid_ask_spread_quality(bars: List[Dict], max_spread_bps: float = 50.0,
                                     min_valid_ratio: float = 0.95) -> Dict[str, Any]:
    """P0-R8-12修复: P0-Q2 bid_ask_spread数据质量门验证

    手册23.1节要求: bid_ask_spread不得超过max_spread_bps，
    且至少min_valid_ratio的Bar满足条件。

    Args:
        bars: Bar数据列表，每个元素应含'bid_price','ask_price'或'spread_bps'
        max_spread_bps: 最大允许spread(bp)
        min_valid_ratio: 最低有效比例
    Returns:
        {'passed': bool, 'valid_ratio': float, 'max_observed_bps': float, 'n_bars': int}
    """
    if not bars:
        return {'passed': False, 'valid_ratio': 0.0, 'max_observed_bps': 0.0, 'n_bars': 0}
    n_valid = 0
    max_observed = 0.0
    for bar in bars:
        spread_bps = bar.get('spread_bps', 0.0)
        if spread_bps <= 0 and 'bid_price' in bar and 'ask_price' in bar:
            bid = bar['bid_price']
            ask = bar['ask_price']
            if bid > 0:
                spread_bps = abs(ask - bid) / bid * 10000.0
        max_observed = max(max_observed, spread_bps)
        if spread_bps <= max_spread_bps:
            n_valid += 1
    valid_ratio = n_valid / len(bars)
    return {
        'passed': valid_ratio >= min_valid_ratio,
        'valid_ratio': valid_ratio,
        'max_observed_bps': max_observed,
        'n_bars': len(bars),
    }


def validate_tick_timestamp_uniqueness(tick_df_or_list, tolerance_ms: float = 1.0) -> Dict[str, Any]:
    """P0-R8-12修复: P0-Q3 分钟Bar内Tick时间戳唯一性质量门验证

    手册23.1节要求: 同一分钟Bar内不应有重复Tick时间戳(容差tolerance_ms)。

    Args:
        tick_df_or_list: Tick数据(pandas DataFrame或列表)，需含'timestamp'列
        tolerance_ms: 重复判定容差(毫秒)
    Returns:
        {'passed': bool, 'n_total': int, 'n_duplicate': int, 'duplicate_ratio': float}
    """
    timestamps = []
    if hasattr(tick_df_or_list, 'iloc'):
        timestamps = tick_df_or_list['timestamp'].tolist() if 'timestamp' in tick_df_or_list.columns else []
    elif isinstance(tick_df_or_list, (list, tuple)):
        timestamps = [t.get('timestamp', t) if isinstance(t, dict) else t for t in tick_df_or_list]
    n_total = len(timestamps)
    if n_total < 2:
        return {'passed': True, 'n_total': n_total, 'n_duplicate': 0, 'duplicate_ratio': 0.0}
    sorted_ts = sorted(timestamps)
    n_duplicate = 0
    for i in range(1, len(sorted_ts)):
        diff = abs(sorted_ts[i] - sorted_ts[i - 1])
        diff_ms = diff.total_seconds() * 1000 if hasattr(diff, 'total_seconds') else float(diff) * 1000
        if diff_ms <= tolerance_ms:
            n_duplicate += 1
    duplicate_ratio = n_duplicate / n_total
    return {
        'passed': duplicate_ratio < 0.01,
        'n_total': n_total,
        'n_duplicate': n_duplicate,
        'duplicate_ratio': duplicate_ratio,
    }


def validate_option_metadata_integrity(options: List[Dict], required_fields: Tuple[str, ...] = (
        'symbol', 'strike', 'expiry', 'option_type', 'underlying')) -> Dict[str, Any]:
    """P0-R8-12修复: P0-Q4 期权元数据完整性质量门验证

    手册23.1节要求: 每个期权合约必须包含完整的required_fields字段，
    且字段值类型和范围正确。

    Args:
        options: 期权合约数据列表
        required_fields: 必需字段名元组
    Returns:
        {'passed': bool, 'n_total': int, 'n_valid': int, 'missing_fields': Dict[str, int]}
    """
    if not options:
        return {'passed': False, 'n_total': 0, 'n_valid': 0, 'missing_fields': {}}
    n_valid = 0
    missing_counts: Dict[str, int] = {}
    for opt in options:
        is_valid = True
        for f in required_fields:
            val = opt.get(f)
            if val is None or val == '' or val == 0:
                missing_counts[f] = missing_counts.get(f, 0) + 1
                is_valid = False
        if is_valid:
            n_valid += 1
    return {
        'passed': n_valid == len(options),
        'n_total': len(options),
        'n_valid': n_valid,
        'missing_fields': missing_counts,
    }


def validate_depth_imbalance_quality(depth_data: List[Dict], max_imbalance_ratio: float = 3.0,
                                       min_valid_ratio: float = 0.90) -> Dict[str, Any]:
    """P0-R8-12补齐: 盘口深度不平衡质量门验证

    检查bid/ask深度比是否在合理范围内，过滤异常盘口数据。

    Args:
        depth_data: 盘口数据列表，每个元素含'bid_volume','ask_volume'或'imbalance_ratio'
        max_imbalance_ratio: 最大允许不平衡比
        min_valid_ratio: 最低有效比例
    Returns:
        {'passed': bool, 'valid_ratio': float, 'max_observed_ratio': float, 'n_records': int}
    """
    if not depth_data:
        return {'passed': False, 'valid_ratio': 0.0, 'max_observed_ratio': 0.0, 'n_records': 0}
    n_valid = 0
    max_observed = 0.0
    for rec in depth_data:
        ratio = rec.get('imbalance_ratio', 0.0)
        if ratio <= 0:
            bid_vol = rec.get('bid_volume', 0.0)
            ask_vol = rec.get('ask_volume', 0.0)
            if ask_vol > 0 and bid_vol > 0:
                ratio = max(bid_vol / ask_vol, ask_vol / bid_vol)
            elif bid_vol > 0 or ask_vol > 0:
                ratio = 999.0  # R27-P2-08-FIX: 用有限值替代inf防止JSON序列化失败
        max_observed = max(max_observed, ratio)
        if ratio <= max_imbalance_ratio:
            n_valid += 1
    valid_ratio = n_valid / len(depth_data)
    return {
        'passed': valid_ratio >= min_valid_ratio,
        'valid_ratio': valid_ratio,
        'max_observed_ratio': max_observed,
        'n_records': len(depth_data),
    }


def validate_volume_quality(bars: List[Dict], min_avg_volume: float = 100.0,
                              zero_volume_max_pct: float = 0.05) -> Dict[str, Any]:
    """P0-R8-12补齐: 成交量质量门验证

    检查Bar成交量是否充足且零成交量Bar比例在阈值内。

    Args:
        bars: Bar数据列表，每个元素含'volume'
        min_avg_volume: 最低平均成交量
        zero_volume_max_pct: 零成交量Bar最大允许比例
    Returns:
        {'passed': bool, 'avg_volume': float, 'zero_volume_pct': float, 'n_bars': int}
    """
    if not bars:
        return {'passed': False, 'avg_volume': 0.0, 'zero_volume_pct': 0.0, 'n_bars': 0}
    volumes = [bar.get('volume', 0) for bar in bars]
    avg_vol = sum(volumes) / len(volumes)
    n_zero = sum(1 for v in volumes if v <= 0)
    zero_pct = n_zero / len(bars)
    return {
        'passed': avg_vol >= min_avg_volume and zero_pct <= zero_volume_max_pct,
        'avg_volume': avg_vol,
        'zero_volume_pct': zero_pct,
        'n_bars': len(bars),
    }
