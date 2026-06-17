# MODULE_ID: M1-274
"""
StrategyCoreService 监控层 — 从strategy_core_service.py拆分
职责: 健康检查、紧急停止、日回撤硬停止、诊断任务、回归测试
+ EcosystemMonitoringService (从strategy_ecosystem/_monitoring.py迁入)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ali2026v3_trading.infra.logging_utils import get_logger
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
        """持仓风控检查 — 从StrategyCoreService.check_position_risk迁移"""
        provider = self._provider
        if not provider._is_running:
            return
        if not provider._trading_lock.acquire(blocking=False):
            logging.info("[PositionRisk] 交易锁被占用，跳过本次风控检查")
            return
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _cyclic_guard and not _cyclic_guard.enter("check_position_risk"):
            logging.warning("[CC-04/CC-11] Cyclic call detected in check_position_risk, skipping")
            provider._trading_lock.release()
            return
        _param_guard = ParamIsolationGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _param_guard:
            _params = getattr(provider, 'params', {})
            _param_guard.register_param_source("strategy_params", "strategy_core_service", str(hash(frozenset(_params.items() if _params else {}))))
        try:
            provider._ensure_position_service()
            if provider._position_service:
                provider._position_service.check_all_positions()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            # R10-P2-02: 关键异常日志添加exc_info=True
            logging.error("[StrategyMonitoringLayer.check_position_risk] Error: %s", e, exc_info=True)
        finally:
            if _cyclic_guard:
                _cyclic_guard.exit("check_position_risk")
            provider._trading_lock.release()

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

        return report


# ============================================================
# EcosystemMonitoringService (从 strategy_ecosystem/_monitoring.py 迁入)
# 职责: 持仓聚合、健康状态、统计查询
# ============================================================

from ali2026v3_trading.strategy.strategy_ecosystem._models import (
    SlotState,
    StrategySlot,
)

_eco_logger = get_logger(__name__ + '.ecosystem')


class EcosystemMonitoringService:
    """持仓聚合、健康状态、统计查询相关方法（原MonitoringService）"""

    def get_strategy_slots(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                'master': self._master.to_dict(),
                'reverse': self._reverse.to_dict(),
                'other': self._other.to_dict(),
                'spring': self._spring.to_dict(),
                'arbitrage': self._arbitrage.to_dict(),
                'market_making': self._market_making.to_dict(),
            }

    # DFG-P1-14修复: 多策略持仓聚合视图
    def aggregate_position_view(self) -> Dict[str, Any]:
        """DFG-P1-14修复: 聚合所有策略的持仓数据，解决多策略持仓聚合数据流断裂"""
        with self._lock:
            _by_strategy = {}
            _by_instrument: Dict[str, Dict[str, Any]] = {}
            _total_positions = 0
            _net_direction = 0  # 正=净多, 负=净空

            for _sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making'):
                _slot = self._get_slot(_sid)
                if _slot is None:
                    continue
                _slot_positions = {
                    'strategy_id': _sid,
                    'strategy_type': _slot.strategy_type,
                    'state': _slot.state,
                    'position_count': _slot.position_count,
                    'total_pnl': round(_slot.total_pnl, 4),
                    'expected_value': round(_slot.expected_value, 6),
                    'paused': _slot.paused,
                    'frozen': _slot.frozen,
                    'last_direction': _slot.last_direction,
                }
                _by_strategy[_sid] = _slot_positions
                _total_positions += _slot.position_count

                # 按方向聚合净敞口
                if _slot.last_direction in ('long', 'BUY', 'buy'):
                    _net_direction += _slot.position_count
                elif _slot.last_direction in ('short', 'SELL', 'sell'):
                    _net_direction -= _slot.position_count

                # 按合约聚合（从交易记录中提取）
                _trades = self._get_trades(_sid)
                if _trades:
                    for _t in _trades:
                        if _t.is_open and _t.instrument_id:
                            _inst = _t.instrument_id
                            if _inst not in _by_instrument:
                                _by_instrument[_inst] = {
                                    'instrument_id': _inst,
                                    'strategies': [],
                                    'total_open_count': 0,
                                    'directions': set(),
                                }
                            _by_instrument[_inst]['strategies'].append(_sid)
                            _by_instrument[_inst]['total_open_count'] += 1
                            if _t.direction:
                                _by_instrument[_inst]['directions'].add(_t.direction)

            # 转换set为list以便JSON序列化
            for _inst_data in _by_instrument.values():
                _inst_data['directions'] = list(_inst_data['directions'])
                _inst_data['strategy_count'] = len(_inst_data['strategies'])

            return {
                'total_positions': _total_positions,
                'by_strategy': _by_strategy,
                'by_instrument': _by_instrument,
                'net_direction': _net_direction,
                'active_strategy': self._active_strategy,
                'capital_allocations': self.get_capital_allocations(),
            }

    # DFG-P1-14修复: 多策略持仓聚合数据通过EventBus发布
    def publish_aggregate_position(self) -> None:
        """DFG-P1-14修复: 发布多策略持仓聚合数据到EventBus"""
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _view = self.aggregate_position_view()
                _bus.publish('position.aggregate_view', {
                    'type': 'position.aggregate_view',
                    'total_positions': _view.get('total_positions', 0),
                    'net_direction': _view.get('net_direction', 0),
                    'by_strategy': _view.get('by_strategy', {}),
                    'by_instrument': _view.get('by_instrument', {}),
                }, async_mode=True)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass

    def get_ecosystem_health_status(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            if self._ev_cache is not None and (now - self._ev_cache_ts) < self._ev_cache_ttl:
                ev_check = self._ev_cache
            else:
                try:
                    ev_check = self.check_absolute_ev_bottomline()
                    if ev_check is not None:
                        self._ev_cache_ts = now
                        self._ev_cache = ev_check
                    else:
                        ev_check = self._ev_cache if self._ev_cache is not None else {}
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    ev_check = self._ev_cache if self._ev_cache is not None else {}

            any_paused = self._master.paused or self._reverse.paused or self._other.paused or self._spring.paused

            # 集成E12反向策略伪独立检测
            e12_result = None
            e12_error = False
            if self._e12_detector is not None:
                try:
                    master_trades = list(self._master_trades)
                    reverse_trades = list(self._reverse_trades)
                    e12_result = self._e12_detector.detect(master_trades, reverse_trades)
                    if e12_result and e12_result.get('e12_triggered'):
                        _eco_logger.critical("[StrategyEcosystem] E12告警: 反向策略与主策略相关性过高 correlation=%.2f",
                                        e12_result.get('correlation', 0.0))
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    _eco_logger.warning("[StrategyEcosystem] E12检测异常: %s", e, exc_info=True)
                    e12_error = True
                    e12_result = {'e12_triggered': False, 'error': str(e)}

            # 集成E11量化来源检查
            e11_result = None
            e11_error = False
            if self._e11_checker is not None:
                try:
                    param_sources = {
                        'master_params': 'backtest',
                        'reverse_params': 'backtest',
                        'other_params': 'backtest',
                    }
                    e11_result = self._e11_checker.check(param_sources)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    _eco_logger.warning("[StrategyEcosystem] E11检测异常: %s", e, exc_info=True)
                    e11_error = True
                    e11_result = {'e11_triggered': False, 'error': str(e)}

            # P1-R10-05修复: 组件检测异常时降级为DEGRADED
            if any_paused:
                health_status = 'CRITICAL'
            elif e12_error or e11_error:
                health_status = 'DEGRADED'
            else:
                health_status = 'OK'

            return {
                'component': 'strategy_ecosystem',
                'status': health_status,
                'active_strategy': self._active_strategy,
                'strategies': {
                    'master': {'state': self._master.state, 'paused': self._master.paused},
                    'reverse': {'state': self._reverse.state, 'paused': self._reverse.paused},
                    'other': {'state': self._other.state, 'paused': self._other.paused},
                    'spring': {'state': self._spring.state, 'paused': self._spring.paused},
                },
                'capital_allocations': self.get_capital_allocations(),
                'ev_check': ev_check,
                'box_detector': self._box_detector.get_health_status(),
                'governance': {
                    'e12_reverse_independence': e12_result,
                    'e11_quant_source': e11_result,
                },
            }

    def get_ecosystem_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['active_strategy'] = self._active_strategy
            stats['master_pnl'] = self._master.total_pnl
            stats['reverse_pnl'] = self._reverse.total_pnl
            stats['other_pnl'] = self._other.total_pnl
            stats['spring_pnl'] = self._spring.total_pnl
            stats['master_ev'] = self._master.expected_value
            stats['reverse_ev'] = self._reverse.expected_value
            stats['other_ev'] = self._other.expected_value
            stats['spring_ev'] = self._spring.expected_value
            stats['lineage_entries'] = sum(len(v) for v in self._strategy_lineage.values())
            stats['param_evolution_entries'] = len(self._strategy_param_evolution)
            stats['strategy_doc_links'] = dict(self._strategy_doc_links)
            stats['l2_dataset_enforced'] = self._l2_dataset_enforced
            stats['l2_dataset_id'] = self._l2_dataset_id
            stats['capital_scale_upgrade_enabled'] = self._capital_scale_upgrade_enabled
            stats['retired_strategy_count'] = len(self._retired_strategies)
            stats['revival_request_count'] = len(self._revival_requests)
            try:
                from ali2026v3_trading.infra.performance_monitor import PathCounter
                perf = PathCounter.get_stats()
                stats['performance_monitor'] = {
                    'total_paths': perf.get('total_paths', 0),
                    'uptime_seconds': round(perf.get('uptime_seconds', 0), 1),
                }
            except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
                logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
                pass
            return stats

    def validate_position_consistency(self) -> Dict[str, Any]:
        """INV-10/INV-CON-01: 验证策略持仓之和等于PositionService总持仓"""
        with self._lock:
            strategy_total = sum(
                self._get_slot(sid).position_count
                for sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making')
                if self._get_slot(sid) is not None
            )
            pos_service_total = 0
            try:
                pos_svc = self._get_position_service()
                if pos_svc is not None and hasattr(pos_svc, 'positions'):
                    pos_service_total = len(pos_svc.positions) if pos_svc.positions else 0
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                _eco_logger.debug("[INV-10] 获取PositionService持仓失败: %s", e)
                pos_service_total = -1

            consistent = strategy_total == pos_service_total if pos_service_total >= 0 else True
            if not consistent:
                _eco_logger.warning(
                    "[INV-10] 持仓一致性违反: 策略持仓之和=%d, PositionService总持仓=%d",
                    strategy_total, pos_service_total,
                )
            return {
                'consistent': consistent,
                'strategy_position_sum': strategy_total,
                'position_service_total': pos_service_total,
            }


# Backward-compatible aliases
MonitoringService = EcosystemMonitoringService
MonitoringMixin = EcosystemMonitoringService
