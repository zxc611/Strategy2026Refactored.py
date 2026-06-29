"""
StrategyCoreService 监控层 — 从strategy_core_service.py拆分
职责: 健康检查、紧急停止、日回撤硬停止、诊断任务、回归测试
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ali2026v3_trading.infra.shared_utils import CHINA_TZ

try:
    from ali2026v3_trading.strategy_judgment.causal_chain_utils import (
        CyclicDependencyGuard, ParamIsolationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False


# [P2-10-SML] 职责: 风控层健康检查+紧急停止+日回撤硬停止+诊断任务
# 与lifecycle/lifecycle_monitor.py的LifecycleMonitor分工: SML=风控层, LM=状态查询
class StrategyMonitoringLayer:
    _AUTO_CALLER_IDS = frozenset({'timer', 'scheduler', 'auto', 'cron', 'periodic', 'heartbeat'})

    def __init__(self, provider):
        self._provider = provider

    def get_health_status(self) -> Dict[str, Any]:
        from ali2026v3_trading.infra.health_monitor import HealthCheckAggregator
        _aggregator = HealthCheckAggregator(self._provider)
        return _aggregator.aggregate()

    def confirm_daily_resume(self, caller_id: str = "unknown") -> bool:
        if caller_id.lower() in self._AUTO_CALLER_IDS:
            logging.warning(
                "[R10-P1-11] confirm_daily_resume被自动来源'%s'调用，已拒绝。"
                "手册9.6节要求仅通过人工确认恢复，换日不自动重置。", caller_id)
            return False
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self._provider, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(
                params=getattr(self._provider, 'params', None),
                strategy_id=_sid
            )
            if safety:
                result = safety.confirm_daily_resume(caller_id=caller_id)
                if result:
                    logging.critical("[StrategyMonitoringLayer] 人工确认恢复交易：日回撤硬停止已解除 caller_id=%s", caller_id)
                    self._provider._health_pause_new_open = False
                return result
            else:
                logging.error("[StrategyMonitoringLayer] confirm_daily_resume失败: SafetyMetaLayer不可用")
                return False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.error("[StrategyMonitoringLayer] confirm_daily_resume异常: %s", e)
            return False

    def is_hard_stop_triggered(self) -> bool:
        try:
            from ali2026v3_trading.risk.risk_service import get_safety_meta_layer
            _sid = str(getattr(self._provider, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(
                params=getattr(self._provider, 'params', None),
                strategy_id=_sid
            )
            if safety:
                return safety.is_hard_stop_triggered()
            return False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[StrategyMonitoringLayer] is_hard_stop_triggered error: %s", e)
            return False

    def check_position_risk(self) -> None:
        """持仓风控检查 — 从StrategyCoreService.check_position_risk迁移
        
        CC-04修复: 移除CyclicDependencyGuard，因为check_position_risk是定时调度调用，
        不存在真正的递归循环。之前的enter/exit模式在enter返回False时直接return
        而不调用exit，导致栈泄漏，后续所有调用被误判为循环依赖。
        """
        provider = self._provider
        if not provider._is_running:
            return
        _param_guard = ParamIsolationGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _param_guard:
            _params = getattr(provider, 'params', {})
            try:
                _param_hash = str(hash(frozenset(_params.items() if _params else {})))
            except TypeError:
                _param_hash = str(hash(str(sorted(_params.items(), key=lambda x: str(x))) if _params else ''))
            _param_guard.register_param_source("strategy_params", "strategy_core_service", _param_hash)
        try:
            provider._ensure_position_service()
            if provider._position_service:
                provider._position_service.check_all_positions()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[StrategyMonitoringLayer.check_position_risk] Error: %s", e, exc_info=True)


    def emergency_stop(self, caller_id: str = "unknown", reason: str = "") -> Dict[str, Any]:
        """OPS-03修复: 紧急一键停止 — 暂停策略 + 撤销挂单 + 平所有仓位 + 通知

        操作步骤：
        1. 暂停策略（停止接收新行情和新交易）
        2. 撤销所有未成交挂单
        3. 市价平所有持仓
        4. 触发通知（告警回调）
        5. 记录审计日志

        Args:
            caller_id: 操作人标识
            reason: 紧急停止原因

        Returns:
            dict: {paused, cancelled_orders, close_result, errors, timestamp}
        """
        provider = self._provider
        result = {
            'paused': False,
            'cancelled_orders': 0,
            'close_result': None,
            'errors': [],
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'caller_id': caller_id,
            'reason': reason,
        }
        logging.critical(
            "[OPS-03] emergency_stop 触发! caller_id=%s reason=%s",
            caller_id, reason,
        )

        # Step 1: 暂停策略
        try:
            provider._is_paused = True
            provider._is_trading = False
            provider._health_pause_new_open = True
            provider._health_pause_new_open_ts = time.time()
            try:
                if hasattr(provider, '_lifecycle_manager') and provider._lifecycle_manager is not None:
                    from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState
                    provider._lifecycle_manager.transition_to(StrategyState.STOPPED)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _lsm_err:
                logging.warning("[OPS-03] lifecycle state sync failed: %s", _lsm_err)
            result['paused'] = True
            logging.critical("[OPS-03] 策略已暂停")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            result['errors'].append(f"pause failed: {e}")

        # Step 2+3: 撤销挂单 + 平所有仓位
        try:
            provider._ensure_order_service()
            if provider._order_service:
                close_result = provider._order_service.emergency_close_all_positions(
                    caller_id=caller_id,
                )
                result['cancelled_orders'] = close_result.get('cancelled_orders', 0)
                result['close_result'] = close_result
            else:
                result['errors'].append("OrderService不可用")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            result['errors'].append(f"emergency_close_all_positions failed: {e}")
            logging.error("[OPS-03] 紧急平仓失败: %s", e)

        # Step 4: 触发通知
        try:
            from ali2026v3_trading.risk.risk_service import RiskService
            RiskService._fire_alert('EMERGENCY_STOP', {
                'caller_id': caller_id,
                'reason': reason,
                'strategy_id': provider.strategy_id,
                'timestamp': result['timestamp'],
            })
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            result['errors'].append(f"alert failed: {e}")

        # Step 5: 审计日志
        try:
            from ali2026v3_trading.risk.risk_service import operations_audit_log
            operations_audit_log(
                action='EMERGENCY_STOP',
                operator=caller_id,
                result='success' if not result['errors'] else 'partial',
                detail=result,
            )
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.warning("[R22-EP-P1] StrategyMonitoringLayer exception swallowed")
            pass

        logging.critical(
            "[OPS-03] emergency_stop 完成: paused=%s, cancelled=%d, errors=%d",
            result['paused'], result['cancelled_orders'], len(result['errors']),
        )
        return result

    def run_regression_check(self, test_cases: Optional[List[Dict[str, Any]]] = None,
                              test_func: Optional[Callable] = None) -> Dict[str, Any]:
        """UPG-P1-13修复: 运行策略逻辑回归测试

        使用shared_utils.StrategyRegressionTest框架，验证策略逻辑变更后
        关键场景的输出仍然符合预期。

        Args:
            test_cases: 自定义测试用例列表，每项格式:
                {
                    'name': str,           # 测试用例名称
                    'input': Any,          # 输入数据
                    'expected': Any,       # 期望输出
                    'tolerance': float,    # 浮点数容差(可选)
                    'comparator': Callable # 自定义比较函数(可选)
                }
                如果为None，使用内置默认测试用例。
            test_func: 被测试的策略逻辑函数
                如果为None，使用默认的信号生成测试函数。

        Returns:
            Dict: 回归测试报告 {
                passed: int,
                failed: int,
                total: int,
                details: list,
                test_name: str,
            }
        """
        provider = self._provider
        from ali2026v3_trading.infra.shared_utils import StrategyRegressionTest

        tester = StrategyRegressionTest("strategy_core_regression")

        if test_cases:
            for case in test_cases:
                tester.register(
                    case_name=case.get('name', 'unnamed'),
                    input_data=case.get('input'),
                    expected_output=case.get('expected'),
                    tolerance=case.get('tolerance', 0.0),
                    comparator=case.get('comparator'),
                )
        else:
            tester.register(
                case_name="state_initializing",
                input_data={'action': 'get_state'},
                expected_output='INITIALIZING',
            )
            tester.register(
                case_name="health_check_returns_dict",
                input_data={'action': 'health_check'},
                expected_output=None,
                comparator=lambda actual, expected: isinstance(actual, dict),
            )

        if test_func is not None:
            func = test_func
        else:
            def _default_test_func(input_data):
                action = input_data.get('action', '') if isinstance(input_data, dict) else ''
                if action == 'get_state':
                    return str(provider._state)
                elif action == 'health_check':
                    return provider.get_health_status()
                elif action == 'is_running':
                    return provider._is_running
                else:
                    return None
            func = _default_test_func

        report = tester.run_all(func)

        if report['failed'] > 0:
            logging.warning(
                "UPG-P1-13: 策略回归测试 %d/%d 通过, %d 失败",
                report['passed'], report['total'], report['failed'],
            )
        else:
            logging.info(
                "UPG-P1-13: 策略回归测试全部通过 (%d/%d)",
                report['passed'], report['total'],
            )


class EcosystemMonitoringService:
    def __init__(self, provider=None):
        self._provider = provider

    def get_health_status(self) -> Dict[str, Any]:
        if self._provider is not None:
            return StrategyMonitoringLayer(self._provider).get_health_status()
        return {'healthy': True, 'source': 'EcosystemMonitoringService'}

    def confirm_daily_resume(self, caller_id: str = "unknown") -> bool:
        if self._provider is not None:
            return StrategyMonitoringLayer(self._provider).confirm_daily_resume(caller_id)
        return False

        return report

MonitoringService = EcosystemMonitoringService
MonitoringMixin = EcosystemMonitoringService
