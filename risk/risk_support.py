# MODULE_ID: M1-209-217-219-222
# [M1-33] Risk-Position Bridge Interface
# [M1-34] RPN风险优先数矩阵
# [M1-39] 安全元审计日志
# [M1-40] 安全元层核心逻辑

"""
risk_support.py - 风控支持模块（合并5个小碎模块）

合并来源：
- _utils.py (28行): 风控内部工具
- risk_priority_matrix.py (171行): RPN风险优先数矩阵
- risk_position_bridge.py (382行): Risk-Position桥接接口
- safety_meta_audit.py (301行): 安全元审计日志
- safety_meta_layer.py (117行): 安全元层核心逻辑

作者：CodeArts 代码智能体
版本：v2.1
"""

from __future__ import annotations

import json
import logging
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from ali2026v3_trading.infra.shared_utils import safe_float, safe_int, safe_get_float, safe_get_int  # P1-20: 统一类型转换
from ali2026v3_trading.infra.shared_utils import atomic_replace_file
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_jsonl_append_line

logger = logging.getLogger(__name__)  # R9-5


# =============================================================================
# Part 1: _utils.py - 风控内部工具
# =============================================================================

def _get_tz_aware_now() -> datetime:
    return datetime.now(_CHINA_TZ)


# P1-21: api_version统一从infra.shared_utils导入
from ali2026v3_trading.infra.shared_utils import api_version


def structured_audit_log(event_type: str, action: str, details: Dict[str, Any] = None,
                         severity: str = "INFO") -> None:
    try:
        from ali2026v3_trading.risk.risk_service import audit_chain_append
        audit_chain_append(event_type, {'action': action, 'details': details or {}, 'severity': severity})
    except ImportError:
        logging.debug("[structured_audit_log] audit_chain unavailable, skipping log for %s/%s", event_type, action)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[structured_audit_log] log failed: %s", e)


# =============================================================================
# Part 2: risk_priority_matrix.py - RPN风险优先数矩阵
# =============================================================================

"""
Phase0增强: RPN风险优先数矩阵阵定量风险评估

RPN = 概率(1-5) × 影响(1-5) × 可检测量1-5)

CRITICAL(_6) > HIGH(_4) > MEDIUM(_5) > LOW(<15)

"""


@dataclass
class RiskItem:
    name: str
    probability: int
    impact: int
    detectability: int
    description: str = ''
    mitigation: str = ''
    trigger_phase: str = ''


    @property
    def rpn(self) -> int:
        return self.probability * self.impact * self.detectability


    @property
    def severity(self) -> str:
        _rpn = self.rpn
        if _rpn >= 36:
            return 'CRITICAL'
        if _rpn >= 24:
            return 'HIGH'
        if _rpn >= 15:
            return 'MEDIUM'
        return 'LOW'



class RiskPriorityMatrix:
    """RPN风险优先数矩阵


    10项风险按RPN降序排序，CRITICAL级风险必须先缓解才能进入下一Phase

    """


    def __init__(self):
        self._risks: List[RiskItem] = []
        self._init_default_risks()


    def _init_default_risks(self) -> None:
        self._risks = [
            RiskItem('拆分引入新BUG', 3, 4, 3, '每Sprint后运行全量回撤单元测试+契约测试', 'Phase 1-4'),
            RiskItem('循环依赖再现', 3, 4, 3, '每Sprint后运行import_graph DAG验证;CI/CD阻断', 'Phase 2-3'),
            RiskItem('测试覆盖不足', 4, 3, 3, '拆分前先补测量覆盖率≥80%再拆;先写测试再重构', 'Phase 1-4'),
            RiskItem('人员技能不足', 3, 3, 4, '提前进行技能矩阵评估关键Sprint配备备份人员', 'Phase 1-4'),
            RiskItem('_LifecycleMixin MRO断裂', 2, 5, 3, '拆分前确认所有调用方已迁移到类方法委托A/B灰度验证', 'Phase 2-Sprint 6'),
            RiskItem('WAL/JSONL格式兼容灾', 3, 3, 3, '拆分前归档现有WAL;新格式向下兼容旧格式读取;格式版本号', 'Phase 2-Sprint 4'),
            RiskItem('双写不一凁', 3, 4, 2, '6个月双写过渡期定期对账脚本+差异告警', 'Phase 2'),
            RiskItem('Phase并行冲突', 2, 3, 4, 'Phase 3和Phase 4操作不同模块,可部分并行但需协调', 'Phase 3-4'),
            RiskItem('可观测性缺陷', 3, 5, 1, 'Phase 0先部署可观测性基础设施(Sprint 0独立前置)', '全程'),
            RiskItem('Facade性能退避', 2, 3, 2, '委托方法用例slots+描述符缓存优化性能基线对比', 'Phase 2'),
        ]


    def add_risk(self, risk: RiskItem) -> None:
        self._risks.append(risk)


    def get_sorted_risks(self) -> List[RiskItem]:
        return sorted(self._risks, key=lambda r: r.rpn, reverse=True)


    def get_critical_risks(self) -> List[RiskItem]:
        return [r for r in self._risks if r.severity == 'CRITICAL']


    def get_risk_by_name(self, name: str) -> Optional[RiskItem]:
        for r in self._risks:
            if r.name == name:
                return r
        return None


    def to_report(self) -> str:
        lines = ['# RPN风险优先数矩阵\n']
        lines.append('| # | 风险 | 概率 | 影响 | 可检测量| RPN | 严重复| 触发Phase |')
        lines.append('|---|------|:--:|:--:|:----:|:---:|:------:|----------|')
        for i, r in enumerate(self.get_sorted_risks(), 1):
            lines.append(f'| {i} | {r.name} | {r.probability} | {r.impact} | {r.detectability} | **{r.rpn}** | **{r.severity}** | {r.trigger_phase} |')
        return '\n'.join(lines)


# =============================================================================
# Part 3: risk_position_bridge.py - Risk-Position Bridge Interface
# =============================================================================

"""
Risk-Position Bridge Interface _Phase 1 architectural refactoring.

Breaks the circular dependency between risk_service.py and position_service.py

by introducing a protocol-based interface layer.

Before: position_service _risk_service (9 direct refs)

        risk_service _position_service (6 lazy imports)

After:  position_service _RiskPositionBridge (protocol)

        risk_service _PositionBridge (protocol)

        Both protocols defined here, no circular import.

"""


class BridgeRiskLevel(Enum):
    PASS = "PASS"
    BLOCK = "BLOCK"
    WARNING = "WARNING"



@dataclass(slots=True)
class BridgeRiskResponse:
    level: BridgeRiskLevel = BridgeRiskLevel.PASS
    reason: str = ""
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)



@dataclass(slots=True)
class BridgePositionLimit:
    account_id: str = ""
    limit_amount: float = 0.0
    effective_until: Optional[float] = None



class RiskPositionBridge:
    """Interface that position_service uses to communicate with risk_service.

    This breaks the direct dependency on RiskService class.
    RiskService implements this interface; position_service depends only on this.

    """


    def check_position_limit(self, account_id: str, required_amount: float) -> BridgeRiskResponse:
        raise NotImplementedError


    def set_position_limit(self, account_id: str, limit_amount: float,
                           effective_until: Optional[float] = None) -> None:
        raise NotImplementedError


    def get_position_limit(self, account_id: str) -> Optional[BridgePositionLimit]:
        raise NotImplementedError


    def get_safety_meta_layer(self, params: Any = None, strategy_id: str = "") -> Any:
        raise NotImplementedError


    def check_cross_instrument_limit(self, account_id: str, instrument_id: str,
                                      requested_amount: float) -> BridgeRiskResponse:
        raise NotImplementedError



class PositionBridge:
    """Interface that risk_service uses to communicate with position_service.

    This breaks the lazy import dependency on PositionService class.
    PositionService implements this interface; risk_service depends only on this.

    """


    def get_position_service(self, scope_id: Optional[str] = None) -> Any:
        raise NotImplementedError


    def get_cross_strategy_risk_guard(self) -> Any:
        raise NotImplementedError


    def get_position_limit_config(self, account_id: str) -> Optional[Any]:
        raise NotImplementedError



class RiskBridgeAdapter(RiskPositionBridge):
    """Adapter wrapping an existing RiskService instance as a RiskPositionBridge."""


    def __init__(self, risk_service: Any):
        self._risk_service = risk_service


    def check_position_limit(self, account_id: str, required_amount: float) -> BridgeRiskResponse:
        if self._risk_service is None:
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="no_risk_service")
        try:
            result = self._risk_service._check_position_limit(account_id, required_amount)
            if hasattr(result, 'is_pass') and result.is_pass:
                return BridgeRiskResponse(level=BridgeRiskLevel.PASS)
            elif hasattr(result, 'is_block') and result.is_block:
                return BridgeRiskResponse(
                    level=BridgeRiskLevel.BLOCK,
                    reason=getattr(result, 'reason', ''),
                    message=getattr(result, 'message', '')
                )
            return BridgeRiskResponse(
                level=BridgeRiskLevel.WARNING,
                reason=getattr(result, 'reason', ''),
                message=getattr(result, 'message', '')
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("RiskBridgeAdapter.check_position_limit error: %s", e)
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="adapter_error")


    def set_position_limit(self, account_id: str, limit_amount: float,
                           effective_until: Optional[float] = None) -> None:
        if self._risk_service is not None:
            self._risk_service.set_position_limit(account_id, limit_amount, effective_until)


    def get_position_limit(self, account_id: str) -> Optional[BridgePositionLimit]:
        if self._risk_service is None:
            return None
        try:
            pl = self._risk_service.get_position_limit(account_id)
            if pl is None:
                return None
            return BridgePositionLimit(
                account_id=getattr(pl, 'account_id', account_id),
                limit_amount=getattr(pl, 'limit_amount', 0.0),
                effective_until=getattr(pl, 'effective_until', None),
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("RiskBridgeAdapter.get_position_limit error: %s", e)
            return None


    def get_safety_meta_layer(self, params: Any = None, strategy_id: str = "") -> Any:
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            return get_safety_meta_layer(params=params, strategy_id=strategy_id)
        except ImportError:
            return None


    def check_cross_instrument_limit(self, account_id: str, instrument_id: str,
                                      requested_amount: float) -> BridgeRiskResponse:
        if self._risk_service is None:
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="no_risk_service")
        try:
            result = self._risk_service.check_cross_instrument_limit(
                account_id, instrument_id, requested_amount
            )
            if hasattr(result, 'is_pass') and result.is_pass:
                return BridgeRiskResponse(level=BridgeRiskLevel.PASS)
            return BridgeRiskResponse(
                level=BridgeRiskLevel.BLOCK,
                reason=getattr(result, 'reason', ''),
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("RiskBridgeAdapter.check_cross_instrument_limit error: %s", e)
            return BridgeRiskResponse(level=BridgeRiskLevel.PASS, reason="adapter_error")



class PositionBridgeAdapter(PositionBridge):
    """Adapter wrapping an existing PositionService instance as a PositionBridge."""


    def __init__(self, position_service: Any = None):
        self._position_service = position_service


    def get_position_service(self, scope_id: Optional[str] = None) -> Any:
        if self._position_service is not None:
            return self._position_service
        try:
            from ali2026v3_trading.position.position_service import get_position_service
            return get_position_service(scope_id=scope_id)
        except ImportError:
            return None


    def get_cross_strategy_risk_guard(self) -> Any:
        try:
            from ali2026v3_trading.position.position_service import get_cross_strategy_risk_guard
            return get_cross_strategy_risk_guard()
        except ImportError:
            return None


    def get_position_limit_config(self, account_id: str) -> Optional[Any]:
        if self._position_service is None:
            return None
        try:
            return getattr(self._position_service, '_position_limits', {}).get(account_id)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return None


# =============================================================================
# Part 4: safety_meta_audit.py - 安全元审计日志
# =============================================================================

# 风控专用审计：职责边界断路器状态持久化+合规审批记录。与config/config_logging.AuditLogger(日志审计)、param_pool/l1_quantification/meta_audit_engine(参数审计)分工厂


class CircuitBreakerStateStore:
    """断路器状态持久化服务 _审计与状态存在

    负责：断路器状态的save/load/跨日重置逻辑

    """


    def __init__(self, params: Any = None):
        self.params = params
        self._lock = threading.RLock()
        self._circuit_breaker_state_path: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".circuit_breaker_state.json"
        )


    def save_state(self, trading_paused_until: float, pause_reason: str,
                   circuit_breaker_calm_until: float, circuit_breaker_shadow_mode: bool,
                   circuit_breaker_shadow_until: float, circuit_breaker_activated_at: float,
                   state_data: Dict[str, Any]) -> None:
        """持久化断路器状态到JSON文件"""
        try:
            now = time.time()
            state = {
                "trading_paused_until": trading_paused_until,
                "pause_reason": pause_reason,
                "circuit_breaker_calm_until": circuit_breaker_calm_until,
                "circuit_breaker_shadow_mode": circuit_breaker_shadow_mode,
                "circuit_breaker_shadow_until": circuit_breaker_shadow_until,
                "circuit_breaker_activated_at": circuit_breaker_activated_at,
                "daily_hard_stop_triggered": state_data.get("daily_hard_stop_triggered", False),
                "daily_new_open_blocked": state_data.get("daily_new_open_blocked", False),
                "daily_start_equity": state_data.get("daily_start_equity"),
                "daily_peak_equity": state_data.get("daily_peak_equity", 0.0),
                "daily_drawdown": state_data.get("daily_drawdown", 0.0),
                "prev_5day_avg_profit": state_data.get("prev_5day_avg_profit", 0.0),
                "current_date": state_data.get("current_date"),
                "saved_at": now,
            }
            # P2-22修复: 使用 atomic_replace_file 替代内联 os.replace
            _result = atomic_replace_file(self._circuit_breaker_state_path, json_dumps(state))
            if not _result['success']:
                raise RuntimeError(_result.get('error', 'atomic_replace_file failed'))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] 断路器状态持久化失败: %s", e)


    def load_state(self) -> Dict[str, Any]:
        result = {}
        try:
            if not os.path.exists(self._circuit_breaker_state_path):
                return result
            with open(self._circuit_breaker_state_path, "r", encoding="utf-8") as f:
                state = json_loads(f.read())  # R5-2
            now = time.time()
            # FIX-R37-CB-ATTR: 初始化断路器属性默认值，避免条件分支未设置时AttributeError
            self._trading_paused_until = state.get("trading_paused_until", 0.0)
            self._pause_reason = state.get("pause_reason", "")
            self._circuit_breaker_calm_until = state.get("circuit_breaker_calm_until", 0.0)
            self._circuit_breaker_shadow_mode = False
            self._circuit_breaker_shadow_until = 0.0

            if state.get("trading_paused_until", 0) > now:
                self._trading_paused_until = state["trading_paused_until"]
                self._pause_reason = state.get("pause_reason", "")
            if state.get("circuit_breaker_calm_until", 0) > now:
                self._circuit_breaker_calm_until = state["circuit_breaker_calm_until"]
            if state.get("circuit_breaker_shadow_mode", False):
                if state.get("circuit_breaker_shadow_until", 0) > now:
                    self._circuit_breaker_shadow_mode = True
                    self._circuit_breaker_shadow_until = state["circuit_breaker_shadow_until"]
            self._circuit_breaker_activated_at = state.get("circuit_breaker_activated_at", 0.0)
            saved_date = state.get("current_date")
            today = _get_tz_aware_now().strftime("%Y-%m-%d")
            if saved_date and saved_date == today:
                result = {
                    "daily_hard_stop_triggered": state.get("daily_hard_stop_triggered", False),
                    "daily_new_open_blocked": state.get("daily_new_open_blocked", False),
                    "daily_start_equity": state.get("daily_start_equity"),
                    "daily_peak_equity": state.get("daily_peak_equity", 0.0),
                    "daily_drawdown": state.get("daily_drawdown", 0.0),
                    "prev_5day_avg_profit": state.get("prev_5day_avg_profit", 0.0),
                    "current_date": saved_date,
                }
            else:
                _saved_hard_stop = state.get("daily_hard_stop_triggered", False)
                if _saved_hard_stop:
                    result["daily_hard_stop_triggered"] = True
                    # NEW-01修复(FIX-20260706-STATE-SPLIT): 跨日时new_open_blocked必须与hard_stop同步
                    # 旧代码无条件置为False导致hard_stop=True但new_open_blocked=False的状态分裂
                    # 对齐safety_meta_equity.py:_update_equity_metrics的跨日逻辑
                    result["daily_new_open_blocked"] = True
                    logging.warning(
                        "[P0-R9-08] DR-08: 断路器hard_stop跨日保留(%s -> %s), new_open_blocked同步保留, 需人工confirm_daily_resume()恢复。"
                        "恢复方式: 在策略实例上调用 confirm_daily_resume(caller_id='manual') 或执行 "
                        "python -c \"from ali2026v3_trading.risk.risk_support import get_safety_meta_layer; get_safety_meta_layer().confirm_daily_resume('manual')\"",
                        saved_date, today,
                    )
                else:
                    result["daily_hard_stop_triggered"] = False
                    result["daily_new_open_blocked"] = False
                logging.info(
                    "[SafetyMetaLayer] DR-08: 断路器状态跨日(%s -> %s), 日回撤状态已重置",
                    saved_date, today,
                )
            logging.info(
                "[SafetyMetaLayer] 断路器状态已恢复: paused_until=%.1f calm_until=%.1f shadow=%s hard_stop=%s new_open_blocked=%s",
                self._trading_paused_until, self._circuit_breaker_calm_until,
                self._circuit_breaker_shadow_mode,
                result.get("daily_hard_stop_triggered", False), result.get("daily_new_open_blocked", False),
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] 断路器状态恢复失败 %s", e)
        return result


    def write_compliance_audit(self, caller_id: str, approval_context: Optional[Dict[str, Any]],
                               daily_drawdown: float) -> None:
        """写入合规审批记录到JSONL审计日志"""
        try:
            audit_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
            os.makedirs(audit_dir, exist_ok=True)
            audit_path = os.path.join(audit_dir, 'compliance_audit.jsonl')
            audit_entry = {
                'event': 'confirm_daily_resume',
                'timestamp': _get_tz_aware_now().isoformat(),
                'caller_id': caller_id,
                'approval_context': approval_context or {},
                'previous_state': {
                    'daily_hard_stop_triggered': True,
                    'daily_drawdown': daily_drawdown,
                },
                'new_state': {
                    'daily_hard_stop_triggered': False,
                    'daily_new_open_blocked': False,
                },
            }
            with open(audit_path, 'a', encoding='utf-8') as f:
                safe_jsonl_append_line(f, audit_entry)
            logging.info("[SafetyMetaLayer] CMP-P1-02: 合规审批记录已写入%s", audit_path)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] CMP-P1-02: 合规审批记录写入失败: %s", e)


# =============================================================================
# Part 5: safety_meta_layer.py - 安全元层核心逻辑
# =============================================================================

"""
安全元层模块 _从risk_circuit_breaker.py拆分

职责: SafetyMetaLayer核心逻辑（日回撤硬停止、断路器、保证金追保存

"""


class SafetyMetaLayer:
    def __init__(self, params: Optional[Dict[str, Any]] = None, strategy_id: str = 'global'):
        self._params = params or {}
        self._strategy_id = strategy_id
        self._daily_start_equity: float = 0.0
        self._hard_stop_triggered: bool = False
        self._daily_resume_confirmed: bool = False
        self._circuit_breaker_active: bool = False
        self._circuit_breaker_until: Optional[float] = None


    def set_daily_start_equity(self, equity: float) -> None:
        self._daily_start_equity = equity
        self._hard_stop_triggered = False
        self._daily_resume_confirmed = False


    def check_daily_drawdown(self, current_equity: float) -> bool:
        if self._daily_start_equity <= 0:
            return False
        hard_stop_pct = self._params.get('daily_loss_hard_stop_pct', 0.05)
        dd = (self._daily_start_equity - current_equity) / self._daily_start_equity
        if dd >= hard_stop_pct:
            self._hard_stop_triggered = True
            return True
        return False


    def is_hard_stop_triggered(self) -> bool:
        return self._hard_stop_triggered


    def confirm_daily_resume(self, caller_id: str = "unknown") -> bool:
        auto_callers = frozenset({'timer', 'scheduler', 'auto', 'cron', 'periodic', 'heartbeat'})
        if caller_id.lower() in auto_callers:
            logging.warning("[SafetyMetaLayer] 自动来源'%s'调用confirm_daily_resume，已拒绝", caller_id)
            return False
        self._hard_stop_triggered = False
        self._daily_resume_confirmed = True
        return True


    def activate_circuit_breaker(self, pause_sec: float = 180.0) -> None:
        self._circuit_breaker_active = True
        self._circuit_breaker_until = time.time() + pause_sec


    def is_circuit_breaker_active(self) -> bool:
        if self._circuit_breaker_active and self._circuit_breaker_until is not None:
            if time.time() >= self._circuit_breaker_until:
                self._circuit_breaker_active = False
        return self._circuit_breaker_active


# =============================================================================
# 导出
# =============================================================================

__all__ = [
    # _utils.py
    "_get_tz_aware_now",
    "api_version",
    "structured_audit_log",
    "safe_float",
    "safe_int",
    "safe_get_float",
    "safe_get_int",
    # risk_priority_matrix.py
    "RiskItem",
    "RiskPriorityMatrix",
    # risk_position_bridge.py
    "BridgeRiskLevel",
    "BridgeRiskResponse",
    "BridgePositionLimit",
    "RiskPositionBridge",
    "PositionBridge",
    "RiskBridgeAdapter",
    "PositionBridgeAdapter",
    # safety_meta_audit.py
    "CircuitBreakerStateStore",
    # safety_meta_layer.py
    "SafetyMetaLayer",
]