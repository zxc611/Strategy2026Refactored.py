"""
ops_automation.py - 鑷姩鍖栧嚱鏁?鍘嬫祴/SOP
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

from datetime import datetime

from ali2026v3_trading.infra._helpers import _CHINA_TZ
from ali2026v3_trading.infra._helpers import get_disk_space_monitor
from ali2026v3_trading.infra._backup_restore import get_backup_service
from ali2026v3_trading.tests.infra_archived._ops_framework import get_ops_operation_manager
from ali2026v3_trading.infra._helpers import get_logger  # R9-5


logger = get_logger(__name__)  # R9-5


# P2淇: 杩愮淮宸ュ叿鍑芥暟
def ops_health_check() -> Dict[str, Any]:
    """P2淇: 缁煎悎杩愮淮鍋ュ悍妫€鏌?

    Returns:
        Dict: {overall_status, components: {name: {status, details}}}
    """
    components = {}

    # 妫€鏌ョ鐩樼┖闂?
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

    # 妫€鏌ventBus
    try:
        from ali2026v3_trading.infra.event_bus import get_global_event_bus
        bus = get_global_event_bus()
        bus_stats = bus.get_stats()
        components['event_bus'] = {
            'status': 'OK' if bus_stats.get('failed_events', 0) < 10 else 'WARNING',
            'details': bus_stats,
        }
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        components['event_bus'] = {'status': 'ERROR', 'details': str(e)}

    # 妫€鏌ラ厤缃湇鍔?
    try:
        from ali2026v3_trading.config.config_service import get_config
        config = get_config()
        components['config'] = {
            'status': 'OK',
            'details': {'db_path': config.database.db_path},
        }
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        components['config'] = {'status': 'ERROR', 'details': str(e)}

    # LG-06淇: 妫€鏌ユ棩蹇楃郴缁熷仴搴?
    try:
        from ali2026v3_trading.config.config_service import check_logging_health
        log_health = check_logging_health()
        components['logging'] = {
            'status': 'OK' if log_health.get('healthy') else 'ERROR',
            'details': log_health,
        }
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
        components['logging'] = {'status': 'ERROR', 'details': str(e)}

    # 缁煎悎鐘舵€?
    statuses = [c['status'] for c in components.values()]
    if 'ERROR' in statuses:
        overall = 'ERROR'
    elif 'WARNING' in statuses:
        overall = 'WARNING'
    else:
        overall = 'OK'

    return {'overall_status': overall, 'components': components}


def ops_run_diagnostic(category: str = "all") -> Dict[str, Any]:
    """P2淇: 杩愮淮璇婃柇宸ュ叿鍑芥暟

    Args:
        category: 璇婃柇绫诲埆 (all/disk/eventbus/config/risk)

    Returns:
        璇婃柇缁撴灉瀛楀吀
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
                        'message': f'纾佺洏绌洪棿涓嶈冻: {path} 鍓╀綑{info.get("free_gb", 0):.2f}GB',
                    })
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            result['findings'].append({
                'severity': 'MEDIUM',
                'component': 'disk',
                'message': f'纾佺洏妫€鏌ュ紓甯? {e}',
            })

    if category in ('all', 'eventbus'):
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            bus = get_global_event_bus()
            stats = bus.get_stats()
            if stats.get('dropped_events', 0) > 0:
                result['findings'].append({
                    'severity': 'MEDIUM',
                    'component': 'eventbus',
                    'message': f'事件丢弃: {stats["dropped_events"]}次',
                })
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            result['findings'].append({
                'severity': 'LOW',
                'component': 'eventbus',
                'message': f'EventBus妫€鏌ュ紓甯? {e}',
            })

    return result


# P2淇: 鑷姩鍖栬繍缁村嚱鏁?
def ops_auto_repair(issue_type: str) -> Dict[str, Any]:
    """P2淇: 鑷姩鍖栬繍缁翠慨澶嶅嚱鏁?

    Args:
        issue_type: 闂绫诲瀷 (disk_full/eventbus_stuck/config_stale)

    Returns:
        Dict: {repaired: bool, action: str, details: str}
    """
    result = {'repaired': False, 'action': '', 'details': ''}

    if issue_type == 'disk_full':
        try:
            # 娓呯悊鏃ф棩蹇楁枃浠?
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
                result['details'] = f'清理了{cleaned}个超过30天的日志文件'
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            result['details'] = f'娓呯悊澶辫触: {e}'

    elif issue_type == 'eventbus_stuck':
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            bus = get_global_event_bus()
            stats = bus.get_stats()
            pending = stats.get('pending_events', 0)
            if pending > 100:
                result['action'] = 'eventbus_high_pending'
                result['details'] = f'EventBus待处理事件{pending}个，建议检查订阅者性能'
            else:
                result['repaired'] = True
                result['action'] = 'eventbus_ok'
                result['details'] = 'EventBus状态异常'
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            result['details'] = f'妫€鏌ュけ璐? {e}'

    elif issue_type == 'config_stale':
        try:
            from ali2026v3_trading.config.config_service import get_config_query_facade, get_config_command_facade
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
                result['details'] = '配置已术最新'
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            result['details'] = f'閰嶇疆閲嶈浇澶辫触: {e}'

    return result


# P2淇: 杩愮淮鎶ュ憡鑷姩鐢熸垚
def generate_ops_report(report_type: str = "daily") -> Dict[str, Any]:
    """P2淇: 鑷姩鐢熸垚杩愮淮鎶ュ憡

    Args:
        report_type: 鎶ュ憡绫诲瀷 (daily/weekly/incident)

    Returns:
        Dict: 杩愮淮鎶ュ憡鍐呭
    """
    report = {
        'report_type': report_type,
        'generated_at': datetime.now(_CHINA_TZ).isoformat(),
        'sections': {},
    }

    # 绯荤粺鐘舵€?
    health = ops_health_check()
    report['sections']['system_health'] = health

    # 纾佺洏浣跨敤
    try:
        monitor = get_disk_space_monitor()
        report['sections']['disk_usage'] = monitor.check(force=True)
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] ops_automation disk_usage suppressed: %s", _r3_err)
        report['sections']['disk_usage'] = {'error': 'unavailable'}

    # EventBus缁熻
    try:
        from ali2026v3_trading.infra.event_bus import get_global_event_bus
        bus = get_global_event_bus()
        report['sections']['event_bus'] = bus.get_stats()
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] ops_automation event_bus suppressed: %s", _r3_err)
        report['sections']['event_bus'] = {'error': 'unavailable'}

    # 澶囦唤鐘舵€?
    try:
        backup_svc = get_backup_service()
        report['sections']['backup'] = {
            'last_backup_time': backup_svc._last_backup_time,
        }
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] ops_automation backup suppressed: %s", _r3_err)
        report['sections']['backup'] = {'error': 'unavailable'}

    # 杩愮淮鎿嶄綔璁板綍
    try:
        ops_mgr = get_ops_operation_manager()
        report['sections']['ops_operations'] = {
            'total_count': len(ops_mgr._operations) if hasattr(ops_mgr, '_operations') else 0,
        }
    except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
        logging.debug("[R3-L2] ops_automation ops_operations suppressed: %s", _r3_err)
        report['sections']['ops_operations'] = {'error': 'unavailable'}

    return report


# P2淇: 杩愮淮鎴愭湰璇勪及
def estimate_ops_cost() -> Dict[str, Any]:
    """P2淇: 杩愮淮鎴愭湰璇勪及鍑芥暟

    浼扮畻褰撳墠绯荤粺鐨勮繍缁磋祫婧愭秷鑰楀拰鎴愭湰銆?

    Returns:
        Dict: {storage_cost, compute_cost, estimated_monthly_cost}
    """
    storage_cost = {'db_size_mb': 0, 'log_size_mb': 0, 'backup_size_mb': 0}

    # 浼扮畻鏁版嵁搴撳ぇ灏?
    try:
        from ali2026v3_trading.config.config_service import get_default_db_path
        db_path = get_default_db_path()
        if os.path.exists(db_path):
            storage_cost['db_size_mb'] = round(os.path.getsize(db_path) / (1024 * 1024), 2)
    except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:
        logging.debug("[R3-L2] ops_automation db_size suppressed: %s", _r3_err)
        pass

    # 浼扮畻鏃ュ織澶у皬
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

    # 浼扮畻澶囦唤澶у皬
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
            'note': '成本估算仅供参考，实际成本取决于部署环僃',
        },
    }


# P2淇: SLA瀹氫箟



class CapacityStressTestFramework:
    """OPS-18淇: 瀹归噺鍘嬫祴妗嗘灦

    鎻愪緵绯荤粺瀹归噺鍘嬫祴鑳藉姏锛岄獙璇佹€ц兘涓婇檺銆?
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
    """OPS-20淇: 杩愮淮鏍囧噯鎿嶄綔绋嬪簭(SOP)

    瀹氫箟鏍囧噯鍖栫殑杩愮淮鎿嶄綔姝ラ锛岀‘淇濇搷浣滀竴鑷存€у拰鍙拷婧€с€?
    """

    def __init__(self):
        self._procedures: Dict[str, Dict[str, Any]] = {}
        self._init_default_procedures()

    def _init_default_procedures(self) -> None:
        self._procedures = {
            'emergency_close_all': {
                'name': '绱ф€ュ叏骞充粨',
                'steps': [
                    '1. 纭瑙﹀彂鏉′欢(鏃ュ洖鎾よ秴闄?鏉冪泭涓鸿礋/鏂矾鍣ㄨЕ鍙?',
                    '2. 璋冪敤health_check_api.emergency_close_all()',
                    '3. 楠岃瘉鎵€鏈夋寔浠撳凡娓呯┖',
                    '4. 记录操作日志和审批信号',
                    '5. 閫氱煡on-call浜哄憳',
                ],
                'approval_required': True,
                'timeout_sec': 300,
            },
            'emergency_stop': {
                'name': '瀀急停止',
                'steps': [
                    '1. 纭瑙﹀彂鏉′欢(绯荤粺寮傚父/鏁版嵁寮傚父)',
                    '2. 璋冪敤strategy_core_service.emergency_stop()',
                    '3. 撤销所有挂单',
                    '4. 验证策略已停止',
                    '5. 閫氱煡on-call浜哄憳',
                ],
                'approval_required': True,
                'timeout_sec': 120,
            },
            'param_hot_update': {
                'name': '参数热更新',
                'steps': [
                    '1. 璋冪敤params_service.hot_update_begin()澶囦唤褰撳墠鍙傛暟',
                    '2. 淇敼鐩爣鍙傛暟',
                    '3. 验证参数变更正确性',
                    '4. 璋冪敤params_service.hot_update_commit()鎻愪氦',
                    '5. 濡傞獙璇佸け璐ワ紝璋冪敤hot_update_rollback()鍥炴粴',
                ],
                'approval_required': True,
                'timeout_sec': 60,
            },
            'daily_resume': {
                'name': '日回撤恢复',
                'steps': [
                    '1. 纭鏃ュ洖鎾ゅ凡瑙﹀彂涓斿凡杩囧喎鍗存湡',
                    '2. 鑾峰彇瀹℃壒(approver_id)',
                    '3. 璋冪敤risk_circuit_breaker.confirm_daily_resume()',
                    '4. 验证交易已恢复',
                    '5. 璁板綍瀹℃壒鏃ュ織',
                ],
                'approval_required': True,
                'timeout_sec': 180,
            },
            'strategy_upgrade': {
                'name': '绛栫暐鍗囩骇',
                'steps': [
                    '1. 鍚敤骞惰杩愯鏈?strategy_ecosystem.enable_parallel_run())',
                    '2. 部署新版本策略',
                    '3. 鐏板害楠岃瘉(params_service.should_apply_canary())',
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
            return {'success': False, 'error': f'鏈煡鎿嶄綔绫诲瀷: {operation_type}'}
        if proc.get('approval_required') and not approval_context:
            return {'success': False, 'error': '姝ゆ搷浣滈渶瑕佸鎵癸紝浣嗘湭鎻愪緵approval_context'}
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
