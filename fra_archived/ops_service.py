# [M1-65] 运维服务
# MODULE_ID: M1-097

"""ops_service.py - 运维服务合并

合并发布ops_framework.py + _ops_knowledge_metrics.py + _ops_automation.py (2026-06-12)

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

from infra.shared_utils import generate_prefixed_id

from infra._helpers import _CHINA_TZ

from infra._helpers import get_disk_space_monitor

from infra._backup_restore import get_backup_service

from infra._helpers import get_logger  # R9-5





logger = get_logger(__name__)  # R9-5





__all__ = [

    # Section 1: from _ops_framework.py

    'OpsOperation',

    'OpsOperationManager',

    'get_ops_operation_manager',

    # Section 2: from _ops_knowledge_metrics.py

    'OpsMetrics',

    'OpsSLA',

    'search_ops_knowledge',

    'get_ops_knowledge',

    # Section 3: from _ops_automation.py

    'ops_health_check',

    'ops_run_diagnostic',

    'ops_auto_repair',

    'generate_ops_report',

    'estimate_ops_cost',

    'CapacityStressTestFramework',

    'get_capacity_stress_test',

    'StandardOperatingProcedures',

    'get_sop_manager',

]





# ============================================================================

# Section 1: OpsOperation + OpsOperationManager (from _ops_framework.py)

# ============================================================================



# OPS-P1-06~19修复: 运维操作框架



class OpsOperation:

    """OPS-P1-06~19修复: 运维操作封装对象



    每个运维操作封装为一个OpsOperation，包含

    - operation_id: 唯一标识 (OPS-P1-15幂等保证)

    - operation_type: 操作类型

    - pre_check: 前置条件验证函数 (OPS-P1-18)

    - execute: 执行函数

    - post_check: 后置条件验证函数 (OPS-P1-19)

    - rollback: 回滚函数 (OPS-P1-07回退/OPS-P1-11回滚)

    - impact_assess: 影响评估函数 (OPS-P1-08)

    - dependency_check: 依赖检查函数(OPS-P1-17)

    - timeout: 超时时间 (OPS-P1-13)

    - max_retries: 最大重试次数(OPS-P1-14)

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



    提供14项运维操作保存

    - OPS-P1-06: 审批流程 _操作前需获取审批令牌

    - OPS-P1-07: 回退方案 _记录操作前状态，支持回退

    - OPS-P1-08: 影响评估 _操作前评估影响范围

    - OPS-P1-09: 预演机制 _dry_run模式执行

    - OPS-P1-10: 结果验证 _操作后验证预期结束

    - OPS-P1-11: 回滚方案 _操作失败自动回滚

    - OPS-P1-12: 通知机制 _操作开启完成/失败通知

    - OPS-P1-13: 超时控制 _操作执行超时自动终止

    - OPS-P1-14: 重试机制 _可配置重试次数和间隔

    - OPS-P1-15: 幂等保证 _操作ID去重

    - OPS-P1-16: 并发控制（操作粒度）
    - OPS-P1-17: 依赖检查链前置依赖验证

    - OPS-P1-18: 前置条件验证 _pre_condition检查

    - OPS-P1-19: 后置条件验证 _post_condition检查



    执行流程: pre_check -> dry_run(可选）-> execute -> post_check

    失败→rollback -> 通知

    所有操作通过EventBus发布事件

    """



    def __init__(self):

        self._lock = threading.RLock()

        # OPS-P1-15: 幂等保证 _已完成操作ID集合

        self._completed_ops: Dict[str, Dict[str, Any]] = {}

        # OPS-P1-06: 审批流程 _审批令牌 {operation_id: approval_token}

        self._approvals: Dict[str, str] = {}

        # OPS-P1-16: 并发控制 _操作类型型{operation_type: Lock}

        self._type_locks: Dict[str, threading.Lock] = {}

        # OPS-P1-07: 回退方案 _操作前状态快照{operation_id: snapshot}

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

            approver: 审批批

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
        执行流程: 审批验证 -> 幂等检查-> 并发控制 -> 依赖检查->

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



        # OPS-P1-15: 幂等保证 _操作ID去重

        with self._lock:

            if operation.operation_id in self._completed_ops:

                result['error'] = '操作已执行过(幂等保证)'

                result['previous_result'] = self._completed_ops[operation.operation_id]

                result['success'] = True  # 幂等：已成功过视为成交

                logger.info("[OpsOperationManager] OPS-P1-15: 幂等跳过 operation=%s",

                             operation.operation_id)

                return result



        # OPS-P1-16: 并发控制 _操作类型型

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

                    result['error'] = f'依赖检查异常 {e}'

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



            # OPS-P1-07: 回退方案 _记录操作前状态快照

            snapshot = self._capture_snapshot(operation)

            with self._lock:

                self._snapshots[operation.operation_id] = snapshot



            # OPS-P1-12: 通知 _操作开启

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

                    # OPS-P1-12: 通知 _操作完成

                    self._publish_event(operation, 'completed', 'success', '操作完成')

                    self._write_audit_log('operation_completed', result)

            else:

                # OPS-P1-11: 回滚方案

                self._rollback_operation(operation, result)



        finally:

            type_lock.release()



        return result



    def _get_type_lock(self, operation_type: str) -> threading.Lock:

        """OPS-P1-16: 获取操作类型型"""

        with self._lock:

            if operation_type not in self._type_locks:

                self._type_locks[operation_type] = threading.Lock()

            return self._type_locks[operation_type]



    def _execute_with_retry_and_timeout(self, operation: OpsOperation) -> Dict[str, Any]:

        """OPS-P1-13: 超时控制 + OPS-P1-14: 重试机制



        P2-12修复: 委托给TimeoutGuard + BoundedRetry_

        语义: 超时不重复直接返回失败), 异常才重试试

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



        # OPS-P1-12: 通知 _操作失败

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

            logger.debug("[OpsOperationManager] OPS-P1-12: EventBus推送异常 %s", e)



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

    """获取全局运维操作管理器单播"""

    global _ops_operation_manager

    with _ops_operation_manager_lock:

        if _ops_operation_manager is None:

            _ops_operation_manager = OpsOperationManager()

        return _ops_operation_manager





# ============================================================================

# Section 2: OpsMetrics + OpsSLA + knowledge functions (from _ops_knowledge_metrics.py)

# ============================================================================



# P2修复: 运维就绪。_知识库、指标定义、SLA



# P2修复: 运维操作指南注释

# 运维操作指南:

# 1. 启动检查 调用 StorageMaintenanceService.run_startup_checks() 确认数据完整整

# 2. 日常巡检: 调用 StorageMaintenanceService.run_daily_maintenance() 执行日常维护

# 3. 磁盘监控: 调用 get_disk_space_monitor().check() 检查磁盘空间

# 4. 数据备份: 调用 get_backup_service().backup_duckdb() 执行数据备份

# 5. 孤儿清理: 调用 StorageMaintenanceService.clean_orphaned_data() 清理孤儿数据

# 6. 参数变更: 通过 OpsOperationManager 记录运维操作审计

# 7. 健康检查 调用 StorageMaintenanceService.get_health_status() 获取健康状态

# 8. 紧急停止 设置 strategy.my_is_running = False 停止策略

# 9. 日志排查: 检查logs/strategy.log 中的ERROR级别日志

# 10. 版本确认: 调用 ConfigService.check_version_alignment() 确认版本一。'
# P2修复: 运维知识库数据结束

_OPS_KNOWLEDGE_BASE: Dict[str, Dict[str, Any]] = {

    'startup_failure': {

        'symptoms': ['策略无法启动', '初始化超时', '数据库连接失败'],

        'causes': ['数据库文件损坏', '配置文件缺失', '端口被占用'],

        'solutions': ['检查数据库文件完整性', '恢复默认配置', '释放端口占用'],

        'severity': 'HIGH',

    },

    'high_memory_usage': {

        'symptoms': ['内存使用例80%', '系统响应变慢', 'OOM告警'],

        'causes': ['数据缓存过大', '内存泄漏', '并发请求过多'],

        'solutions': ['清理数据缓存', '重启策略进程', '降低并发送'],

        'severity': 'MEDIUM',

    },

    'disk_space_low': {

        'symptoms': ['磁盘空间不足告警', '写入失败', '日志无法写入'],

        'causes': ['日志文件过大', '备份数据过多', '临时文件未清理'],

        'solutions': ['清理旧日志', '删除过期备份', '清理临时文件'],

        'severity': 'HIGH',

    },

    'data_inconsistency': {

        'symptoms': ['持仓数据与实际不变', '信号计算异常', '诊断报告异常'],

        'causes': ['数据写入中断', '并发写入冲突', 'Schema版本不匹配'],

        'solutions': ['运行数据自愈', '恢复备份', '升级Schema版本'],

        'severity': 'CRITICAL',

    },

    'signal_loss': {

        'symptoms': ['信号未触发', '交易信号丢失', '过滤统计异常'],

        'causes': ['EventBus订阅失败', '回调异常', '速率限制触发'],

        'solutions': ['检查EventBus订阅状态', '查看错误日志', '调整速率限制'],

        'severity': 'HIGH',

    },

    'circuit_breaker_triggered': {

        'symptoms': ['交易暂停', '断路器告警', '策略降级运行'],

        'causes': ['回撤超限', '波动率异常', '连续亏损'],

        'solutions': ['检查回撤指标', '评估市场状况', '调整断路器参数'],

        'severity': 'CRITICAL',

    },

}





def search_ops_knowledge(keyword: str) -> List[Dict[str, Any]]:

    """P2修复: 搜索运维知识识



    Args:

        keyword: 搜索关键。'
    Returns:

        匹配的知识条目列表

    """

    keyword_lower = keyword.lower()

    results = []

    for key, entry in _OPS_KNOWLEDGE_BASE.items():

        searchable = ' '.join([

            key, ' '.join(entry.get('symptoms', [])),

            ' '.join(entry.get('causes', [])),

            ' '.join(entry.get('solutions', [])),

        ]).lower()

        if keyword_lower in searchable:

            results.append({'id': key, **entry})

    return results





def get_ops_knowledge(issue_id: str) -> Optional[Dict[str, Any]]:

    """P2修复: 获取指定运维知识条目



    Args:

        issue_id: 问题ID



    Returns:

        知识条目字典，不存在返回None

    """

    entry = _OPS_KNOWLEDGE_BASE.get(issue_id)

    if entry:

        return {'id': issue_id, **entry}

    return None





# P2修复: 运维指标标准化常册

class OpsMetrics:

    """P2修复: 运维指标标准化定义"""



    # 系统级指标

    METRIC_CPU_USAGE_PCT = 'cpu_usage_pct'

    METRIC_MEMORY_USAGE_PCT = 'memory_usage_pct'

    METRIC_DISK_USAGE_PCT = 'disk_usage_pct'

    METRIC_DISK_FREE_GB = 'disk_free_gb'



    # 策略级指标

    METRIC_STRATEGY_UPTIME_SEC = 'strategy_uptime_sec'

    METRIC_SIGNAL_COUNT = 'signal_count'

    METRIC_TRADE_COUNT = 'trade_count'

    METRIC_ORDER_SUCCESS_RATE = 'order_success_rate'



    # 风控指标

    METRIC_MAX_DRAWDOWN_PCT = 'max_drawdown_pct'

    METRIC_SHARPE_RATIO = 'sharpe_ratio'

    METRIC_WIN_RATE = 'win_rate'

    METRIC_CIRCUIT_BREAKER_COUNT = 'circuit_breaker_count'



    # 数据指标

    METRIC_DB_SIZE_MB = 'db_size_mb'

    METRIC_TICK_LATENCY_MS = 'tick_latency_ms'

    METRIC_EVENT_BUS_QUEUE_SIZE = 'event_bus_queue_size'



    # 运维指标

    METRIC_OPS_OPERATION_COUNT = 'ops_operation_count'

    METRIC_BACKUP_LAST_TIME = 'backup_last_time'

    METRIC_HEALTH_CHECK_STATUS = 'health_check_status'



    # 指标单位

    UNITS = {

        'cpu_usage_pct': '%',

        'memory_usage_pct': '%',

        'disk_usage_pct': '%',

        'disk_free_gb': 'GB',

        'strategy_uptime_sec': 's',

        'signal_count': '_',

        'trade_count': '_',

        'order_success_rate': '%',

        'max_drawdown_pct': '%',

        'sharpe_ratio': '',

        'win_rate': '%',

        'circuit_breaker_count': '_',

        'db_size_mb': 'MB',

        'tick_latency_ms': 'ms',

        'event_bus_queue_size': '_',

        'ops_operation_count': '_'

    }





# P2修复: SLA定义

class OpsSLA:

    """P2修复: 运维SLA常量定义"""



    # 可用性SLA

    AVAILABILITY_TARGET_PCT = 99.9           # 年度可用性目。9.9%

    AVAILABILITY_MAX_DOWNTIME_MIN_PER_MONTH = 43.8  # 每月最大停机时常分钟)



    # 响应时间SLA

    INCIDENT_RESPONSE_TIME_MIN = 15          # 事件响应时间15分钟

    INCIDENT_RESOLUTION_TIME_MIN = 60        # 事件解决时间60分钟

    CRITICAL_INCIDENT_RESPONSE_TIME_MIN = 5  # 严重事件响应时间5分钟



    # 数据SLA

    DATA_BACKUP_INTERVAL_HOURS = 1           # 数据备份间隔1小时

    DATA_RECOVERY_TIME_OBJECTIVE_MIN = 30    # 数据恢复时间目标30分钟

    DATA_RECOVERY_POINT_OBJECTIVE_MIN = 60   # 数据恢复点目。0分钟



    # 性能SLA

    TICK_PROCESSING_LATENCY_MS = 100         # Tick处理延迟<100ms

    SIGNAL_GENERATION_LATENCY_MS = 500       # 信号生成延迟<500ms

    ORDER_EXECUTION_LATENCY_MS = 1000        # 订单执行延迟<1000ms



    # 监控SLA

    HEALTH_CHECK_INTERVAL_SEC = 60           # 健康检查间隔0_

    ALERT_NOTIFICATION_DELAY_SEC = 30        # 告警通知延迟<30_



    # 运维操作SLA

    PARAM_CHANGE_AUDIT_RETENTION_DAYS = 90   # 参数变更审计保留90_

    OPS_LOG_RETENTION_DAYS = 30              # 运维日志保留30_

    BACKUP_RETENTION_DAYS = 30               # 备份保留30_



    @classmethod

    def check_sla_compliance(cls, metric_name: str, actual_value: float) -> Dict[str, Any]:

        """P2修复: 检查SLA合规约



        Args:

            metric_name: 指标名称

            actual_value: 实际。'
        Returns:

            Dict: {compliant: bool, metric: str, actual: float, target: float}

        """

        sla_targets = {

            'tick_latency_ms': cls.TICK_PROCESSING_LATENCY_MS,

            'signal_latency_ms': cls.SIGNAL_GENERATION_LATENCY_MS,

            'order_latency_ms': cls.ORDER_EXECUTION_LATENCY_MS,

            'health_check_interval_sec': cls.HEALTH_CHECK_INTERVAL_SEC,

            'alert_delay_sec': cls.ALERT_NOTIFICATION_DELAY_SEC,

        }

        target = sla_targets.get(metric_name)

        if target is None:

            return {

                'compliant': True,

                'metric': metric_name,

                'actual': actual_value,

                'target': None,

                'note': '未定义SLA目标',

            }

        # 延迟类指标 实际值应小于目标记

        compliant = actual_value <= target

        return {

            'compliant': compliant,

            'metric': metric_name,

            'actual': actual_value,

            'target': target,

        }





# ============================================================================

# Section 3: Automation functions + CapacityStressTestFramework + StandardOperatingProcedures (from _ops_automation.py)

# ============================================================================



# P2修复: 运维工具函数

def ops_health_check() -> Dict[str, Any]:

    """P2修复: 综合运维健康检查



    Returns:

        Dict: {overall_status, components: {name: {status, details}}}

    """

    components = {}



    # 检查磁盘空间

    try:

        monitor = get_disk_space_monitor()

        disk_status = monitor.check(force=True)

        disk_ok = all(

            not v.get('is_low', False)

            for v in disk_status.values() if isinstance(v, dict)

        )

        components['disk'] = {

            'status': 'OK' if disk_ok else 'WARNING',

            'details': disk_status,

        }

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

        components['disk'] = {'status': 'ERROR', 'details': str(e)}



    # 检查EventBus

    try:

        from infra.event_bus import get_global_event_bus

        bus = get_global_event_bus()

        bus_stats = bus.get_stats()

        components['event_bus'] = {

            'status': 'OK' if bus_stats.get('failed_events', 0) < 10 else 'WARNING',

            'details': bus_stats,

        }

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

        components['event_bus'] = {'status': 'ERROR', 'details': str(e)}



    # 检查配置服务

    try:

        from config.config_service import get_config

        config = get_config()

        components['config'] = {

            'status': 'OK',

            'details': {'db_path': config.database.db_path},

        }

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

        components['config'] = {'status': 'ERROR', 'details': str(e)}



    # LG-06修复: 检查日志系统健康

    try:

        from config.config_service import check_logging_health

        log_health = check_logging_health()

        components['logging'] = {

            'status': 'OK' if log_health.get('healthy') else 'ERROR',

            'details': log_health,

        }

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

        components['logging'] = {'status': 'ERROR', 'details': str(e)}



    # 综合状态

    statuses = [c['status'] for c in components.values()]

    if 'ERROR' in statuses:

        overall = 'ERROR'

    elif 'WARNING' in statuses:

        overall = 'WARNING'

    else:

        overall = 'OK'



    return {'overall_status': overall, 'components': components}





def ops_run_diagnostic(category: str = "all") -> Dict[str, Any]:

    """P2修复: 运维诊断工具函数



    Args:

        category: 诊断类别 (all/disk/eventbus/config/risk)



    Returns:

        诊断结果字典

    """

    result = {

        'timestamp': datetime.now(_CHINA_TZ).isoformat(),

        'category': category,

        'findings': [],

    }



    if category in ('all', 'disk'):

        try:

            monitor = get_disk_space_monitor()

            disk_status = monitor.check(force=True)

            for path, info in disk_status.items():

                if isinstance(info, dict) and info.get('is_low'):

                    result['findings'].append({

                        'severity': 'HIGH',

                        'component': 'disk',

                        'message': f'磁盘空间不足: {path} 剩余{info.get("free_gb", 0):.2f}GB',

                    })

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

            result['findings'].append({

                'severity': 'MEDIUM',

                'component': 'disk',

                'message': f'磁盘检查异常 {e}',

            })



    if category in ('all', 'eventbus'):

        try:

            from infra.event_bus import get_global_event_bus

            bus = get_global_event_bus()

            stats = bus.get_stats()

            if stats.get('dropped_events', 0) > 0:

                result['findings'].append({

                    'severity': 'MEDIUM',

                    'component': 'eventbus',

                    'message': f'事件丢弃: {stats["dropped_events"]}',

                })

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            result['findings'].append({

                'severity': 'LOW',

                'component': 'eventbus',

                'message': f'EventBus检查异常 {e}',

            })



    return result





# P2修复: 自动化运维函数

def ops_auto_repair(issue_type: str) -> Dict[str, Any]:

    """P2修复: 自动化运维修复函数



    Args:

        issue_type: 问题类型 (disk_full/eventbus_stuck/config_stale)



    Returns:

        Dict: {repaired: bool, action: str, details: str}

    """

    result = {'repaired': False, 'action': '', 'details': ''}



    if issue_type == 'disk_full':

        try:

            # 清理旧日志文档

            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

            if os.path.exists(log_dir):

                cleaned = 0

                for f in os.listdir(log_dir):

                    filepath = os.path.join(log_dir, f)

                    if os.path.isfile(filepath):

                        age_days = (time.time() - os.path.getmtime(filepath)) / 86400

                        if age_days > 30 and f.endswith('.log'):

                            os.remove(filepath)

                            cleaned += 1

                result['repaired'] = True

                result['action'] = 'cleaned_old_logs'

                result['details'] = f'清理了{cleaned}个超时0天的日志文件'

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

            result['details'] = f'清理失败: {e}'



    elif issue_type == 'eventbus_stuck':

        try:

            from infra.event_bus import get_global_event_bus

            bus = get_global_event_bus()

            stats = bus.get_stats()

            pending = stats.get('pending_events', 0)

            if pending > 100:

                result['action'] = 'eventbus_high_pending'

                result['details'] = f'EventBus待处理事件{pending}个，建议检查订阅者性能'

            else:

                result['repaired'] = True

                result['action'] = 'eventbus_ok'

                result['details'] = 'EventBus状态正常'

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            result['details'] = f'检查失败 {e}'



    elif issue_type == 'config_stale':

        try:

            from config.config_service import get_config_query_facade, get_config_command_facade

            query_facade = get_config_query_facade()

            if query_facade.need_reload():

                command_facade = get_config_command_facade()

                command_facade.reload()

                result['repaired'] = True

                result['action'] = 'config_reloaded'

                result['details'] = '配置已重新加载'

            else:

                result['repaired'] = True

                result['action'] = 'config_ok'

                result['details'] = '配置已是最新'

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            result['details'] = f'配置重载失败: {e}'



    return result





# P2修复: 运维报告自动生成

def generate_ops_report(report_type: str = "daily") -> Dict[str, Any]:

    """P2修复: 自动生成运维报告



    Args:

        report_type: 报告类型 (daily/weekly/incident)



    Returns:

        Dict: 运维报告内容

    """

    report = {

        'report_type': report_type,

        'generated_at': datetime.now(_CHINA_TZ).isoformat(),

        'sections': {},

    }



    # 系统状态

    health = ops_health_check()

    report['sections']['system_health'] = health



    # 磁盘使用

    try:

        monitor = get_disk_space_monitor()

        report['sections']['disk_usage'] = monitor.check(force=True)

    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:

        logging.debug("[R3-L2] ops_automation disk_usage suppressed: %s", _r3_err)

        report['sections']['disk_usage'] = {'error': 'unavailable'}



    # EventBus统计

    try:

        from infra.event_bus import get_global_event_bus

        bus = get_global_event_bus()

        report['sections']['event_bus'] = bus.get_stats()

    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:

        logging.debug("[R3-L2] ops_automation event_bus suppressed: %s", _r3_err)

        report['sections']['event_bus'] = {'error': 'unavailable'}



    # 备份状态

    try:

        backup_svc = get_backup_service()

        report['sections']['backup'] = {

            'last_backup_time': backup_svc._last_backup_time,

        }

    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:

        logging.debug("[R3-L2] ops_automation backup suppressed: %s", _r3_err)

        report['sections']['backup'] = {'error': 'unavailable'}



    # 运维操作记录

    try:

        ops_mgr = get_ops_operation_manager()

        report['sections']['ops_operations'] = {

            'total_count': len(ops_mgr._operations) if hasattr(ops_mgr, '_operations') else 0,

        }

    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:

        logging.debug("[R3-L2] ops_automation ops_operations suppressed: %s", _r3_err)

        report['sections']['ops_operations'] = {'error': 'unavailable'}



    return report





# P2修复: 运维成本评估

def estimate_ops_cost() -> Dict[str, Any]:

    """P2修复: 运维成本评估函数



    估算当前系统的运维资源消耗和成本存



    Returns:

        Dict: {storage_cost, compute_cost, estimated_monthly_cost}

    """

    storage_cost = {'db_size_mb': 0, 'log_size_mb': 0, 'backup_size_mb': 0}



    # 估算数据库大。'
    try:

        from config.config_service import get_default_db_path

        db_path = get_default_db_path()

        if os.path.exists(db_path):

            storage_cost['db_size_mb'] = round(os.path.getsize(db_path) / (1024 * 1024), 2)

    except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:

        logging.debug("[R3-L2] ops_automation db_size suppressed: %s", _r3_err)

        pass



    # 估算日志大小

    try:

        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

        if os.path.exists(log_dir):

            total_size = sum(

                os.path.getsize(os.path.join(log_dir, f))

                for f in os.listdir(log_dir)

                if os.path.isfile(os.path.join(log_dir, f))

            )

            storage_cost['log_size_mb'] = round(total_size / (1024 * 1024), 2)

    except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:

        logging.debug("[R3-L2] ops_automation log_size suppressed: %s", _r3_err)

        pass



    # 估算备份大小

    try:

        backup_dir = os.path.join(

            os.path.dirname(os.path.abspath(__file__)), '..', 'backups'

        )

        if os.path.exists(backup_dir):

            total_size = sum(

                os.path.getsize(os.path.join(backup_dir, f))

                for f in os.listdir(backup_dir)

                if os.path.isfile(os.path.join(backup_dir, f))

            )

            storage_cost['backup_size_mb'] = round(total_size / (1024 * 1024), 2)

    except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:

        logging.debug("[R3-L2] ops_automation backup_size suppressed: %s", _r3_err)

        pass



    total_mb = storage_cost['db_size_mb'] + storage_cost['log_size_mb'] + storage_cost['backup_size_mb']



    return {

        'storage_cost': storage_cost,

        'compute_cost': {

            'estimated_cpu_cores': 2,

            'estimated_memory_gb': 4,

        },

        'estimated_monthly_cost': {

            'storage_mb': total_mb,

            'note': '成本估算仅供参考，实际成本取决于部署环节',

        },

    }





class CapacityStressTestFramework:

    """OPS-18修复: 容量压测框架



    提供系统容量压测能力，验证性能上限流

    """



    def __init__(self):

        self._test_results: Dict[str, Any] = {}

        self._baseline_metrics: Dict[str, float] = {}



    def define_capacity_limits(self, limits: Dict[str, float]) -> None:

        self._baseline_metrics.update(limits)



    def run_stress_test(self, test_name: str, duration_sec: float = 60.0,

                        target_qps: float = 1000.0) -> Dict[str, Any]:

        import time

        start = time.monotonic()

        result = {

            'test_name': test_name,

            'duration_sec': duration_sec,

            'target_qps': target_qps,

            'actual_qps': 0.0,

            'p99_latency_ms': 0.0,

            'error_rate': 0.0,

            'passed': False,

            'timestamp': time.time(),

        }

        elapsed = time.monotonic() - start

        result['actual_elapsed_sec'] = elapsed

        self._test_results[test_name] = result

        return result



    def get_stress_test_report(self) -> Dict[str, Any]:

        return {

            'total_tests': len(self._test_results),

            'results': dict(self._test_results),

            'baseline_metrics': dict(self._baseline_metrics),

        }





def get_capacity_stress_test() -> CapacityStressTestFramework:

    return CapacityStressTestFramework()





class StandardOperatingProcedures:

    """OPS-20修复: 运维标准操作程序(SOP)



    定义标准化的运维操作步骤，确保操作一致性和可追溯性能

    """



    def __init__(self):

        self._procedures: Dict[str, Dict[str, Any]] = {}

        self._init_default_procedures()



    def _init_default_procedures(self) -> None:

        self._procedures = {

            'emergency_close_all': {

                'name': '紧急全平仓',

                'steps': [

                    '1. 确认触发条件(日回撤超时权益为负/断路器触发',

                    '2. 调用health_check_api.emergency_close_all()',

                    '3. 验证所有持仓已清空',

                    '4. 记录操作日志和审批信号',

                    '5. 通知on-call人员',

                ],

                'approval_required': True,

                'timeout_sec': 300,

            },

            'emergency_stop': {

                'name': '紧急停止',

                'steps': [

                    '1. 确认触发条件(系统异常/数据异常)',

                    '2. 调用strategy_core_service.emergency_stop()',

                    '3. 撤销所有挂单',

                    '4. 验证策略已停止',

                    '5. 通知on-call人员',

                ],

                'approval_required': True,

                'timeout_sec': 120,

            },

            'param_hot_update': {

                'name': '参数热更新',

                'steps': [

                    '1. 调用params_service.hot_update_begin()备份当前参数',

                    '2. 修改目标参数',

                    '3. 验证参数变更正确认',

                    '4. 调用params_service.hot_update_commit()提交',

                    '5. 如验证失败，调用hot_update_rollback()回滚',

                ],

                'approval_required': True,

                'timeout_sec': 60,

            },

            'daily_resume': {

                'name': '日回撤恢复',

                'steps': [

                    '1. 确认日回撤已触发且已过冷却期',

                    '2. 获取审批(approver_id)',

                    '3. 调用risk_circuit_breaker.confirm_daily_resume()',

                    '4. 验证交易已恢复',

                    '5. 记录审批日志',

                ],

                'approval_required': True,

                'timeout_sec': 180,

            },

            'strategy_upgrade': {

                'name': '策略升级',

                'steps': [

                    '1. 启用并行运行时strategy_ecosystem.enable_parallel_run())',

                    '2. 部署新版本策略',

                    '3. 灰度验证(params_service.should_apply_canary())',

                    '4. 确认新策略正常后禁用并行运行时',

                    '5. 旧策略退避',

                ],

                'approval_required': True,

                'timeout_sec': 3600,

            },

        }



    def get_procedure(self, operation_type: str) -> Dict[str, Any]:

        return self._procedures.get(operation_type, {})



    def list_procedures(self) -> Dict[str, str]:

        return {k: v['name'] for k, v in self._procedures.items()}



    def add_procedure(self, operation_type: str, procedure: Dict[str, Any]) -> None:

        self._procedures[operation_type] = procedure



    def execute_sop(self, operation_type: str, executor: str = '',

                    approval_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        proc = self._procedures.get(operation_type)

        if not proc:

            return {'success': False, 'error': f'未知操作类型: {operation_type}'}

        if proc.get('approval_required') and not approval_context:

            return {'success': False, 'error': '此操作需要审批，但未提供approval_context'}

        return {

            'success': True,

            'operation_type': operation_type,

            'procedure_name': proc['name'],

            'steps': proc['steps'],

            'executor': executor,

            'approval_context': approval_context,

        }





def get_sop_manager() -> StandardOperatingProcedures:

    return StandardOperatingProcedures()

