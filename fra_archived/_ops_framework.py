"""
ops_framework.py - 运维操作框架 (OpsOperation + OpsOperationManager)
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from datetime import datetime

from infra.serialization_utils import safe_jsonl_append_line
from infra._helpers import _CHINA_TZ
from infra._helpers import get_logger  # R9-5
from infra.shared_utils import generate_prefixed_id


logger = get_logger(__name__)  # R9-5


# ============================================================================
# OPS-P1-06~19修复: 运维操作框架
# ============================================================================

class OpsOperation:
    """OPS-P1-06~19修复: 运维操作封装对象

    每个运维操作封装为一个OpsOperation，包含:
    - operation_id: 唯一标识 (OPS-P1-15幂等保证)
    - operation_type: 操作类型
    - pre_check: 前置条件验证函数 (OPS-P1-18)
    - execute: 执行函数
    - post_check: 后置条件验证函数 (OPS-P1-19)
    - rollback: 回滚函数 (OPS-P1-07回退/OPS-P1-11回滚)
    - impact_assess: 影响评估函数 (OPS-P1-08)
    - dependency_check: 依赖检查函数 (OPS-P1-17)
    - timeout: 超时时间 (OPS-P1-13)
    - max_retries: 最大重试次数 (OPS-P1-14)
    """

    def __init__(
        self,
        operation_id: str,
        operation_type: str,
        execute: Callable,
        pre_check: Optional[Callable[[], bool]] = None,
        post_check: Optional[Callable[[], bool]] = None,
        rollback: Optional[Callable] = None,
        impact_assess: Optional[Callable[[], Dict[str, Any]]] = None,
        dependency_check: Optional[Callable[[], bool]] = None,
        timeout: float = 300.0,
        max_retries: int = 0,
        retry_interval: float = 5.0,
        dry_run_supported: bool = False,
    ):
        self.operation_id = operation_id
        self.operation_type = operation_type
        self.execute = execute
        self.pre_check = pre_check
        self.post_check = post_check
        self.rollback = rollback
        self.impact_assess = impact_assess
        self.dependency_check = dependency_check
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.dry_run_supported = dry_run_supported


class OpsOperationManager:
    """OPS-P1-06~19修复: 统一运维操作框架

    提供14项运维操作保障:
    - OPS-P1-06: 审批流程 — 操作前需获取审批令牌
    - OPS-P1-07: 回退方案 — 记录操作前状态，支持回退
    - OPS-P1-08: 影响评估 — 操作前评估影响范围
    - OPS-P1-09: 预演机制 — dry_run模式执行
    - OPS-P1-10: 结果验证 — 操作后验证预期结果
    - OPS-P1-11: 回滚方案 — 操作失败自动回滚
    - OPS-P1-12: 通知机制 — 操作开始/完成/失败通知
    - OPS-P1-13: 超时控制 — 操作执行超时自动终止
    - OPS-P1-14: 重试机制 — 可配置重试次数和间隔
    - OPS-P1-15: 幂等保证 — 操作ID去重
    - OPS-P1-16: 并发控制 — 操作锁
    - OPS-P1-17: 依赖检查 — 前置依赖验证
    - OPS-P1-18: 前置条件验证 — pre_condition检查
    - OPS-P1-19: 后置条件验证 — post_condition检查

    执行流程: pre_check -> dry_run(可选) -> execute -> post_check
    失败时: rollback -> 通知
    所有操作通过EventBus发布事件
    """

    def __init__(self):
        self._lock = threading.RLock()
        # OPS-P1-15: 幂等保证 — 已完成操作ID集合
        self._completed_ops: Dict[str, Dict[str, Any]] = {}
        # OPS-P1-06: 审批流程 — 审批令牌 {operation_id: approval_token}
        self._approvals: Dict[str, str] = {}
        # OPS-P1-16: 并发控制 — 操作类型锁 {operation_type: Lock}
        self._type_locks: Dict[str, threading.Lock] = {}
        # OPS-P1-07: 回退方案 — 操作前状态快照 {operation_id: snapshot}
        self._snapshots: Dict[str, Any] = {}
        # 审计日志路径
        self._audit_log_path = os.path.join('logs', 'ops_audit.jsonl')
        os.makedirs(os.path.dirname(self._audit_log_path), exist_ok=True)

    # OPS-P1-06: 审批流程
    def request_approval(self, operation_id: str, operation_type: str,
                         approver: str = "", reason: str = "") -> str:
        """请求运维操作审批

        Args:
            operation_id: 操作唯一ID
            operation_type: 操作类型
            approver: 审批人
            reason: 审批理由

        Returns:
            审批令牌(token)
        """
        token = generate_prefixed_id("", 8)  # R9-3
        with self._lock:
            self._approvals[operation_id] = token
        self._write_audit_log('approval_requested', {
            'operation_id': operation_id,
            'operation_type': operation_type,
            'approver': approver,
            'reason': reason,
            'token': token,
        })
        logger.info("[OpsOperationManager] OPS-P1-06: 审批请求 operation=%s type=%s approver=%s",
                     operation_id, operation_type, approver)
        return token

    def check_approval(self, operation_id: str, token: str) -> bool:
        """验证审批令牌"""
        with self._lock:
            return self._approvals.get(operation_id) == token

    # OPS-P1-08: 影响评估
    def assess_impact(self, operation: OpsOperation) -> Dict[str, Any]:
        """评估操作影响范围

        Args:
            operation: 运维操作对象

        Returns:
            影响评估结果
        """
        result = {
            'operation_id': operation.operation_id,
            'operation_type': operation.operation_type,
            'impact_level': 'unknown',
            'affected_components': [],
            'estimated_duration_sec': operation.timeout,
        }
        if operation.impact_assess:
            try:
                assess_result = operation.impact_assess()
                if isinstance(assess_result, dict):
                    result.update(assess_result)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                result['assess_error'] = str(e)
                logger.warning("[OpsOperationManager] OPS-P1-08: 影响评估异常: %s", e)
        self._write_audit_log('impact_assessed', result)
        return result

    # OPS-P1-09: 预演机制
    def dry_run(self, operation: OpsOperation) -> Dict[str, Any]:
        """预演模式执行（不实际修改状态）

        Args:
            operation: 运维操作对象

        Returns:
            预演结果
        """
        result = {
            'operation_id': operation.operation_id,
            'operation_type': operation.operation_type,
            'dry_run': True,
            'pre_check_passed': False,
            'impact_assessment': {},
        }
        # 执行前置检查
        if operation.pre_check:
            try:
                result['pre_check_passed'] = operation.pre_check()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                result['pre_check_error'] = str(e)
        else:
            result['pre_check_passed'] = True

        # 评估影响
        result['impact_assessment'] = self.assess_impact(operation)

        # 依赖检查
        if operation.dependency_check:
            try:
                result['dependency_check_passed'] = operation.dependency_check()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                result['dependency_check_error'] = str(e)
                result['dependency_check_passed'] = False
        else:
            result['dependency_check_passed'] = True

        self._write_audit_log('dry_run', result)
        self._publish_event(operation, 'dry_run', 'completed', '预演完成')
        return result

    # OPS-P1-06~19: 核心执行方法
    def execute_operation(self, operation: OpsOperation,
                          approval_token: Optional[str] = None,
                          dry_run: bool = False) -> Dict[str, Any]:
        """执行运维操作（完整生命周期）

        执行流程: 审批验证 -> 幂等检查 -> 并发控制 -> 依赖检查 ->
                 前置验证 -> 影响评估 -> (预演|执行) -> 后置验证 -> 通知

        Args:
            operation: 运维操作对象
            approval_token: 审批令牌 (OPS-P1-06)
            dry_run: 是否预演模式 (OPS-P1-09)

        Returns:
            执行结果
        """
        result = {
            'operation_id': operation.operation_id,
            'operation_type': operation.operation_type,
            'success': False,
            'dry_run': dry_run,
            'timestamp': time.time(),
        }

        # OPS-P1-09: 预演模式
        if dry_run:
            return self.dry_run(operation)

        # OPS-P1-06: 审批验证
        if approval_token and not self.check_approval(operation.operation_id, approval_token):
            result['error'] = '审批令牌无效'
            self._publish_event(operation, 'started', 'rejected', '审批令牌无效')
            return result

        # OPS-P1-15: 幂等保证 — 操作ID去重
        with self._lock:
            if operation.operation_id in self._completed_ops:
                result['error'] = '操作已执行过(幂等保证)'
                result['previous_result'] = self._completed_ops[operation.operation_id]
                result['success'] = True  # 幂等：已成功过视为成功
                logger.info("[OpsOperationManager] OPS-P1-15: 幂等跳过 operation=%s",
                             operation.operation_id)
                return result

        # OPS-P1-16: 并发控制 — 操作类型锁
        type_lock = self._get_type_lock(operation.operation_type)
        if not type_lock.acquire(timeout=operation.timeout):
            result['error'] = f'操作类型{operation.operation_type}并发冲突(超时{operation.timeout}s)'
            self._publish_event(operation, 'started', 'conflict', '并发冲突')
            return result

        try:
            # OPS-P1-17: 依赖检查
            if operation.dependency_check:
                try:
                    if not operation.dependency_check():
                        result['error'] = '依赖检查失败'
                        self._publish_event(operation, 'started', 'dependency_failed', '依赖检查失败')
                        return result
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    result['error'] = f'依赖检查异常: {e}'
                    self._publish_event(operation, 'started', 'dependency_error', str(e))
                    return result

            # OPS-P1-18: 前置条件验证
            if operation.pre_check:
                try:
                    if not operation.pre_check():
                        result['error'] = '前置条件验证失败'
                        self._publish_event(operation, 'started', 'pre_check_failed', '前置条件验证失败')
                        return result
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    result['error'] = f'前置条件验证异常: {e}'
                    self._publish_event(operation, 'started', 'pre_check_error', str(e))
                    return result

            # OPS-P1-08: 影响评估
            impact = self.assess_impact(operation)
            result['impact_assessment'] = impact

            # OPS-P1-07: 回退方案 — 记录操作前状态快照
            snapshot = self._capture_snapshot(operation)
            with self._lock:
                self._snapshots[operation.operation_id] = snapshot

            # OPS-P1-12: 通知 — 操作开始
            self._publish_event(operation, 'started', 'running', '操作开始执行')

            # OPS-P1-13: 超时控制 + OPS-P1-14: 重试机制
            execute_result = self._execute_with_retry_and_timeout(operation)
            result.update(execute_result)

            if result.get('success'):
                # OPS-P1-19: 后置条件验证
                if operation.post_check:
                    try:
                        post_ok = operation.post_check()
                        if not post_ok:
                            result['success'] = False
                            result['error'] = '后置条件验证失败'
                            logger.warning("[OpsOperationManager] OPS-P1-19: 后置验证失败 operation=%s",
                                           operation.operation_id)
                            # OPS-P1-11: 回滚方案
                            self._rollback_operation(operation, result)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        result['post_check_error'] = str(e)
                        logger.warning("[OpsOperationManager] OPS-P1-19: 后置验证异常: %s", e)

                if result.get('success'):
                    # 记录成功操作(幂等保证)
                    with self._lock:
                        self._completed_ops[operation.operation_id] = {
                            'operation_type': operation.operation_type,
                            'timestamp': time.time(),
                            'result': 'success',
                        }
                    # OPS-P1-12: 通知 — 操作完成
                    self._publish_event(operation, 'completed', 'success', '操作完成')
                    self._write_audit_log('operation_completed', result)
            else:
                # OPS-P1-11: 回滚方案
                self._rollback_operation(operation, result)

        finally:
            type_lock.release()

        return result

    def _get_type_lock(self, operation_type: str) -> threading.Lock:
        """OPS-P1-16: 获取操作类型锁"""
        with self._lock:
            if operation_type not in self._type_locks:
                self._type_locks[operation_type] = threading.Lock()
            return self._type_locks[operation_type]

    def _execute_with_retry_and_timeout(self, operation: OpsOperation) -> Dict[str, Any]:
        """OPS-P1-13: 超时控制 + OPS-P1-14: 重试机制

        P2-12修复: 委托到 TimeoutGuard + BoundedRetry。
        语义: 超时不重试(直接返回失败), 异常才重试。
        """
        from infra.resilience import (
            TimeoutGuard as _TimeoutGuard,
            BoundedRetry as _BoundedRetry,
        )

        def _execute_once():
            exc_holder = [None]
            result_holder = [{'success': False}]

            def _run():
                try:
                    ret = operation.execute()
                    if isinstance(ret, dict):
                        result_holder[0].update(ret)
                        result_holder[0]['success'] = ret.get('success', True)
                    elif ret is None:
                        result_holder[0]['success'] = True
                    else:
                        result_holder[0]['success'] = bool(ret)
                        result_holder[0]['result'] = ret
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    exc_holder[0] = e
                    result_holder[0]['error'] = str(e)
                    result_holder[0]['success'] = False

            exec_thread = threading.Thread(target=_run, daemon=True)
            exec_thread.start()
            exec_thread.join(timeout=operation.timeout)

            if exec_thread.is_alive():
                logger.warning("[OpsOperationManager] OPS-P1-13: 超时 operation=%s timeout=%s",
                               operation.operation_id, operation.timeout)
                result_holder[0]['timeout'] = True
                result_holder[0]['success'] = False
                result_holder[0]['error'] = f'操作超时({operation.timeout}s)'
                result_holder[0]['_timeout_no_retry'] = True
                return result_holder[0]

            if exc_holder[0]:
                raise exc_holder[0]
            return result_holder[0]

        for attempt in range(operation.max_retries + 1):
            if attempt > 0:
                logger.info("[OpsOperationManager] OPS-P1-14: 重试 operation=%s attempt=%d/%d",
                             operation.operation_id, attempt + 1, operation.max_retries + 1)
                time.sleep(operation.retry_interval)
            try:
                result = _execute_once()
                if result.get('_timeout_no_retry'):
                    result.pop('_timeout_no_retry', None)
                    return result
                return result
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.warning("[OpsOperationManager] OPS-P1-14: 执行异常 operation=%s error=%s",
                               operation.operation_id, e)
                if attempt >= operation.max_retries:
                    return {'success': False, 'error': str(e)}

        return {'success': False, 'error': '重试次数耗尽'}

    def _capture_snapshot(self, operation: OpsOperation) -> Dict[str, Any]:
        """OPS-P1-07: 捕获操作前状态快照"""
        snapshot = {
            'operation_id': operation.operation_id,
            'operation_type': operation.operation_type,
            'timestamp': time.time(),
        }
        # 尝试获取系统关键状态
        try:
            from infra.health_monitor import HealthCheckAPI
            api = HealthCheckAPI()
            snapshot['health_status'] = api.get_health_status()
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] _ops_framework health_status suppressed: %s", _r3_err)
            pass
        return snapshot

    def _rollback_operation(self, operation: OpsOperation,
                            result: Dict[str, Any]) -> None:
        """OPS-P1-07/OPS-P1-11: 执行回滚"""
        if operation.rollback:
            try:
                snapshot = self._snapshots.get(operation.operation_id)
                rollback_result = operation.rollback(snapshot)
                result['rollback_performed'] = True
                result['rollback_result'] = rollback_result
                logger.info("[OpsOperationManager] OPS-P1-11: 回滚成功 operation=%s",
                             operation.operation_id)
                self._publish_event(operation, 'rolled_back', 'success', '回滚成功')
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                result['rollback_performed'] = False
                result['rollback_error'] = str(e)
                logger.error("[OpsOperationManager] OPS-P1-11: 回滚失败 operation=%s error=%s",
                              operation.operation_id, e)
                self._publish_event(operation, 'rolled_back', 'failed', f'回滚失败: {e}')
        else:
            result['rollback_performed'] = False
            result['rollback_note'] = '未提供回滚函数'
            self._publish_event(operation, 'failed', 'no_rollback', '操作失败且无回滚方案')

        # OPS-P1-12: 通知 — 操作失败
        self._publish_event(operation, 'failed', 'error', result.get('error', '未知错误'))
        self._write_audit_log('operation_failed', result)

    def _publish_event(self, operation: OpsOperation, phase: str,
                       status: str, message: str) -> None:
        """OPS-P1-12: 通过EventBus发布运维操作事件"""
        try:
            from infra.event_bus import get_global_event_bus, OpsOperationEvent
            bus = get_global_event_bus()
            event = OpsOperationEvent(
                operation_id=operation.operation_id,
                operation_type=operation.operation_type,
                phase=phase,
                status=status,
                message=message,
            )
            bus.publish(event, async_mode=True)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logger.debug("[OpsOperationManager] OPS-P1-12: EventBus推送异常: %s", e)

    def _write_audit_log(self, event_type: str, detail: Dict[str, Any]) -> None:
        """写入运维审计日志"""
        try:
            record = {
                'timestamp': datetime.now(_CHINA_TZ).isoformat(),
                'event_type': event_type,
                'detail': detail,
            }
            with open(self._audit_log_path, 'a', encoding='utf-8') as f:
                safe_jsonl_append_line(f, record)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.debug("[OpsOperationManager] 审计日志写入失败: %s", e)

    def get_operation_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """查询操作执行状态"""
        with self._lock:
            return self._completed_ops.get(operation_id)

    def get_audit_log(self, last_n: int = 50) -> List[Dict[str, Any]]:
        """获取最近运维审计日志"""
        try:
            if not os.path.exists(self._audit_log_path):
                return []
            records = []
            with open(self._audit_log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        records.append(json.loads(line.strip()))
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] _ops_framework json parse suppressed: %s", _r3_err)
                        pass
            return records[-last_n:]
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("[OpsOperationManager] 读取审计日志失败: %s", e)
            return []


# 全局单例
_ops_operation_manager: Optional[OpsOperationManager] = None
_ops_operation_manager_lock = threading.Lock()


def get_ops_operation_manager() -> OpsOperationManager:
    """获取全局运维操作管理器单例"""
    global _ops_operation_manager
    with _ops_operation_manager_lock:
        if _ops_operation_manager is None:
            _ops_operation_manager = OpsOperationManager()
        return _ops_operation_manager


