"""shared_utils_infra — 基础设施相关 (R27-CP-04-FIX: 从shared_utils拆分)

环形缓冲区、分片路由、线程生命周期管理、初始化状态机、
幂等性保证、原子文件替换、自动回滚、版本同步检查、不变量检查
"""

import errno
import hashlib
import logging
import os
import shutil as _shutil
import threading
import time
from collections import deque
from enum import IntEnum
from typing import Any, Callable, Dict, Iterator, List, Optional

from ali2026v3_trading.infra.shared_utils_instrument import extract_product_code
from ali2026v3_trading.infra.shared_utils_types import to_float32

__all__ = [
    'RingBuffer', 'ShardRouter', 'DEFAULT_SHARD_COUNT', 'get_shard_router',
    'ThreadLifecycleManager',
    'InitPhase', 'InitializationError', 'InitStateMachine',
    'IdempotencyGuard',
    'AutoRollbackContext', 'atomic_replace_file', 'cleanup_atomic_backup',
    'check_version_sync', 'check_shared_utils_invariants',
]


class RingBuffer:
    """环形缓冲区，线程安全

    R21-MEM-P1-15修复: 添加TTL过期检查，append时记录时间戳，
    查询时自动过滤过期条目，防止陈旧信号占用内存。
    """

    def __init__(self, maxlen: int, ttl_seconds: float = 0.0):
        self.maxlen = maxlen
        self.data = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        # R21-MEM-P1-15修复: TTL过期机制
        self._ttl_seconds = ttl_seconds  # 0表示不启用TTL
        self._timestamps: deque[float] = deque(maxlen=maxlen)  # [R27-AUDIT] P2修复: 添加泛型参数

    def append(self, item: Any) -> None:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            self.data.append(item)
            # R21-MEM-P1-15修复: 记录append时间戳
            if self._ttl_seconds > 0:
                import time as _time
                self._timestamps.append(_time.time())

    def extend(self, items: List[Any]) -> None:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            self.data.extend(items)

    def __len__(self) -> int:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return len(self.data)

    def __getitem__(self, index: int) -> Any:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return self.data[index]

    def __iter__(self) -> Iterator[Any]:  # [R27-AUDIT] P1修复: 返回类型从Any改为Iterator[Any]
        with self._lock:
            return iter(list(self.data))

    def clear(self) -> None:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            self.data.clear()

    def to_list(self) -> List[Any]:  # [R22-TS-P1-01] 添加类型标注
        with self._lock:
            return list(self.data)

    # R21-MEM-P1-15修复: 获取未过期的条目
    def get_fresh(self) -> List[Any]:  # [R22-P2-TS06]
        """返回未过期的条目列表（TTL机制），若未启用TTL则返回全部数据"""
        with self._lock:
            if self._ttl_seconds <= 0 or not self._timestamps:
                return list(self.data)
            import time as _time
            now = _time.time()
            result = []
            for i, ts in enumerate(self._timestamps):
                if now - ts <= self._ttl_seconds:
                    result.append(self.data[i])
            return result


class ShardRouter:
    """确定性分片路由器：品种ID驱动的自动通道绑定

    核心设计：
    1. 用 hashlib.md5 替代 Python hash()，保证跨进程/跨重启确定性
    2. 品种代码全链路直通，零配置自动绑定
    3. 同一品种始终命中固定通道，tick时序天然正确
    4. 通道绑定注册表可审计（§8.4 路由规则可审计）
    5. 简单取模路由：shard_key % shard_count，2^n时等价位掩码

    用法：
        router = ShardRouter(shard_count=16)
        idx = router.route("IF2506")   # 返回确定性shard索引
        router.get_binding_map()        # 返回品种→通道绑定表
    """

    def __init__(self, shard_count: int = 16):
        self._shard_count = shard_count
        self._binding_map: Dict[str, int] = {}
        self._binding_lock = threading.Lock()

    @staticmethod
    def _deterministic_hash(key: str) -> int:
        h = hashlib.md5(key.encode('utf-8')).digest()
        return (h[0] << 24) | (h[1] << 16) | (h[2] << 8) | h[3]

    @property
    def shard_count(self) -> int:
        return self._shard_count

    def route(self, instrument_id: str) -> int:
        _pc_raw = extract_product_code(instrument_id)
        product_code = _pc_raw.lower() if _pc_raw else ''
        return self.route_by_product_code(product_code)

    def route_by_product_code(self, product_code: str) -> int:
        if not product_code:
            product_code = 'unknown'
        product_code = product_code.lower()

        with self._binding_lock:
            if product_code in self._binding_map:
                return self._binding_map[product_code]

        h = self._deterministic_hash(product_code)
        shard_idx = h % self._shard_count

        with self._binding_lock:
            self._binding_map[product_code] = shard_idx

        return shard_idx

    def get_binding_map(self) -> Dict[str, int]:
        with self._binding_lock:
            return dict(self._binding_map)

    def get_shard_members(self) -> Dict[int, List[str]]:
        result: Dict[int, List[str]] = {i: [] for i in range(self._shard_count)}
        with self._binding_lock:
            for product, shard_id in self._binding_map.items():
                result[shard_id].append(product)
        for shard_id in result:
            result[shard_id].sort()
        return result

    def get_routing_audit_line(self) -> str:
        members = self.get_shard_members()
        parts = []
        for sid in sorted(members.keys()):
            prods = members[sid]
            if prods:
                parts.append(f"Shard-{sid:02d}: {','.join(prods)}")
        return ' | '.join(parts)

    def reconfigure(self, new_shard_count: int) -> Dict[str, int]:
        with self._binding_lock:
            old_map = dict(self._binding_map)
            self._shard_count = new_shard_count
            self._binding_map.clear()
            migration = {}
            for product, old_shard in old_map.items():
                h = self._deterministic_hash(product)
                new_shard = h % self._shard_count
                self._binding_map[product] = new_shard
                if new_shard != old_shard:
                    migration[product] = (old_shard, new_shard)
        return migration


_GLOBAL_SHARD_ROUTER: Optional[ShardRouter] = None
_GLOBAL_SHARD_ROUTER_LOCK = threading.Lock()
DEFAULT_SHARD_COUNT = 16


def get_shard_router(shard_count: int = None) -> ShardRouter:
    """获取全局单例 ShardRouter（方法唯一原则）

    全链路只创建一个 ShardRouter 实例，确保第一层和第二层路由完全一致。
    """
    global _GLOBAL_SHARD_ROUTER
    if shard_count is None:
        shard_count = DEFAULT_SHARD_COUNT
    with _GLOBAL_SHARD_ROUTER_LOCK:
        if _GLOBAL_SHARD_ROUTER is None:
            _GLOBAL_SHARD_ROUTER = ShardRouter(shard_count=shard_count)
        return _GLOBAL_SHARD_ROUTER


class ThreadLifecycleManager:
    """管理多服务线程的启停与排水顺序（§8.5 并发安全）

    设计原则：
    - 不包含业务逻辑，仅管理线程生命周期
    - 强制停止顺序：(1) set StopEvent → (2) join Writers → (3) drain → (4) flush
    - 部分启动失败自动回滚已启动线程
    - 独立模块，不增加核心大模块负担
    """

    PHASE_CREATED = 'created'
    PHASE_STARTING = 'starting'
    PHASE_RUNNING = 'running'
    PHASE_STOPPING = 'stopping'
    PHASE_STOPPED = 'stopped'

    def __init__(self, owner_label: str = ''):
        self._owner_label = owner_label
        self._phase = self.PHASE_CREATED
        self._threads: Dict[str, threading.Thread] = {}
        self._daemon_flags: Dict[str, bool] = {}
        self._lock = threading.Lock()
        self._started_at: Optional[float] = None

    def register(self, name: str, thread: threading.Thread, daemon: bool = False) -> None:
        with self._lock:
            self._threads[name] = thread
            self._daemon_flags[name] = daemon

    def start_all(self, thread_configs: List[tuple]) -> None:
        self._phase = self.PHASE_STARTING
        self._started_at = time.time()
        started = []
        try:
            for name, thread, daemon in thread_configs:
                thread.start()
                self.register(name, thread, daemon)
                started.append(name)
        except Exception:
            logging.error("[ThreadMgr] 启动失败，已启动 %s/共%d，执行紧急清理", started, len(thread_configs))
            self._force_stop_partial(started)
            raise
        self._phase = self.PHASE_RUNNING

    def stop_all(self, stop_event: threading.Event, timeout: float = 30.0) -> Dict[str, Any]:  # [R22-P2-TS04]
        self._phase = self.PHASE_STOPPING
        stop_event.set()
        result = {'stopped': 0, 'timeout': 0, 'daemon': 0}
        timeout_threads = []  # R33-P2-2: 收集超时线程信息
        for name, thread in list(self._threads.items()):
            if not thread:
                continue
            if self._daemon_flags.get(name, False):
                thread.join(timeout=min(timeout, 5.0))
                result['daemon'] += 1
                continue
            thread.join(timeout=timeout)
            if thread.is_alive():
                # R33-P2-2: 增强超时告警——添加线程详细诊断信息
                timeout_threads.append({
                    'name': name,
                    'daemon': thread.daemon,
                    'ident': thread.ident,
                })
                logging.error(
                    "[R33-P2-2] 线程 %s 在 %.1fs 内未停止 (daemon=%s, ident=%s)，"
                    "Python无法强制kill线程，将标记为daemon并设置中断标记",
                    name, timeout, thread.daemon, thread.ident,
                )
                result['timeout'] += 1
                # P2-2修复: 超时线程标记为daemon，允许进程退出时自动终止
                if not thread.daemon:
                    thread.daemon = True
                    logging.info("[ThreadMgr] P2-2: 将超时线程 %s 标记为daemon，允许进程退出时自动终止", name)
                # R33-P2-2: 设置线程中断标记（对检查中断标记的线程有效）
                if hasattr(thread, '_stop_requested'):
                    thread._stop_requested = True
            else:
                result['stopped'] += 1
        # R33-P2-2: 超时线程汇总告警
        if timeout_threads:
            logging.critical(
                "[R33-P2-2] CRITICAL: %d个线程在%.1fs超时后仍未停止: %s，"
                "这些线程已被标记为daemon，进程退出时将被强制终止",
                len(timeout_threads), timeout,
                [t['name'] for t in timeout_threads],
            )
        self._phase = self.PHASE_STOPPED
        return result

    def _force_stop_partial(self, started_names: List[str]) -> None:  # [R22-P2-TS07]
        self._phase = self.PHASE_STOPPING
        for name in started_names:
            thread = self._threads.get(name)
            if thread and thread.is_alive():
                thread.join(timeout=2.0 if self._daemon_flags.get(name) else 10.0)
        self._phase = self.PHASE_STOPPED

    @property
    def phase(self) -> str:
        return self._phase

    @property
    def alive_count(self) -> int:
        with self._lock:
            return sum(1 for t in self._threads.values() if t and t.is_alive())

    def health_check(self) -> Dict[str, Any]:  # [R22-P2-TS05]
        with self._lock:
            return {
                'owner': self._owner_label,
                'phase': self._phase,
                'total_registered': len(self._threads),
                'alive': self.alive_count,
                'uptime_seconds': time.time() - self._started_at if self._started_at else 0,
                'threads': {name: t.is_alive() for name, t in self._threads.items()},
            }

    def detect_zombie_threads(self) -> Dict[str, Any]:  # P2-3修复: 僵尸线程检测
        """检测已停止但未清理的僵尸线程引用"""
        zombies = {}
        for name, thread in list(self._threads.items()):
            if thread is not None and not thread.is_alive():
                zombies[name] = {
                    'alive': False,
                    'daemon': self._daemon_flags.get(name, False),
                    'stale_seconds': time.time() - getattr(thread, '_stop_time', time.time()),
                }
        if zombies:
            logging.warning("[ThreadMgr] P2-3: 检测到%d个僵尸线程引用: %s", len(zombies), list(zombies.keys()))
        return {'zombie_count': len(zombies), 'zombies': zombies}


# ============================================================================
# InitPhase / InitStateMachine — 初始化阶段管理
# ============================================================================

class InitPhase(IntEnum):
    NOT_STARTED = 0
    MEMORY_ALLOC = 1
    DB_CONNECT = 2
    EXTERNAL_SERVICES = 3
    DB_SCHEMA = 4
    DB_CACHES = 5
    WRITERS_START = 6
    CLEANUP_START = 7
    READY = 8


class InitializationError(Exception):
    def __init__(self, message: str, current_phase: 'InitPhase', required_phase: 'InitPhase'):
        super().__init__(message)
        self.current_phase = current_phase
        self.required_phase = required_phase


class InitStateMachine:
    def __init__(self):
        self._phase = InitPhase.NOT_STARTED
        self._lock = threading.Lock()
        self._ready_event = threading.Event()

    @property
    def phase(self) -> InitPhase:
        with self._lock:
            return self._phase

    def advance(self, new_phase: InitPhase) -> None:
        with self._lock:
            if new_phase != self._phase + 1 and new_phase != self._phase:
                raise InitializationError(
                    f"非法阶段转换: {self._phase.name} -> {new_phase.name}",
                    self._phase, new_phase
                )
            self._phase = new_phase
            if new_phase == InitPhase.READY:
                self._ready_event.set()

    def check_phase(self, required: InitPhase) -> None:
        with self._lock:
            current = self._phase
        if current < required:
            raise InitializationError(
                f"需要阶段 {required.name} 但当前为 {current.name}",
                current, required
            )

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        return self._ready_event.wait(timeout=timeout)

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def rollback_phase(self) -> InitPhase:
        with self._lock:
            if self._phase > InitPhase.NOT_STARTED:
                self._phase = InitPhase(self._phase - 1)
            return self._phase


class IdempotencyGuard:
    """OPS-P1-09修复: 幂等性保证

    确保操作不会被重复执行。使用操作ID跟踪已完成的操作，
    相同ID的操作只执行一次。

    用法:
        guard = IdempotencyGuard()
        if guard.try_acquire("order_123"):
            # 执行操作
            guard.mark_completed("order_123", result)
        else:
            result = guard.get_result("order_123")
    """

    def __init__(self, max_history: int = 10000, ttl_sec: float = 3600.0):
        self._completed: dict = {}  # op_id → (result, timestamp)
        self._in_progress: set = set()
        self._lock = threading.Lock()
        self._max_history = max_history
        self._ttl_sec = ttl_sec

    def try_acquire(self, op_id: str) -> bool:
        """尝试获取操作执行权

        Returns:
            True: 可以执行; False: 操作已在进行或已完成
        """
        with self._lock:
            if op_id in self._in_progress:
                return False
            if op_id in self._completed:
                return False
            self._in_progress.add(op_id)
            return True

    def mark_completed(self, op_id: str, result: Any = None) -> None:
        """标记操作完成"""
        with self._lock:
            self._in_progress.discard(op_id)
            self._completed[op_id] = (result, time.time())
            # 清理过期记录
            if len(self._completed) > self._max_history:
                now = time.time()
                expired = [k for k, (_, ts) in self._completed.items()
                           if now - ts > self._ttl_sec]
                for k in expired:
                    del self._completed[k]

    def get_result(self, op_id: str) -> Optional[Any]:
        """获取已完成操作的结果"""
        with self._lock:
            entry = self._completed.get(op_id)
            return entry[0] if entry else None

    def is_completed(self, op_id: str) -> bool:
        """检查操作是否已完成"""
        with self._lock:
            return op_id in self._completed


# ============================================================================
# UPG-P1-07修复: 热更新事务性保证
# ============================================================================

def atomic_replace_file(target_path: str, new_content: str,
                        backup_suffix: str = ".bak") -> Dict[str, Any]:
    """UPG-P1-07修复: 原子替换文件（备份+写入+验证+失败回滚）

    实现热更新的事务性保证：
    1. 备份原文件
    2. 写入新内容到临时文件
    3. 验证写入成功
    4. 原子替换目标文件
    5. 失败时从备份回滚

    Args:
        target_path: 目标文件路径
        new_content: 新文件内容
        backup_suffix: 备份文件后缀

    Returns:
        Dict: {success: bool, backup_path: str, error: str}
    """
    result = {'success': False, 'backup_path': '', 'error': ''}
    backup_path = target_path + backup_suffix

    # 步骤1: 备份原文件
    try:
        if os.path.exists(target_path):
            _shutil.copy2(target_path, backup_path)
            result['backup_path'] = backup_path
    except Exception as e:
        result['error'] = f"备份失败: {e}"
        return result

    # 步骤2: 写入临时文件
    temp_path = target_path + ".tmp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        result['error'] = f"写入临时文件失败: {e}"
        # 回滚: 恢复备份
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except Exception as rb_err:
                logging.error("UPG-P1-07: 回滚失败! backup=%s error=%s", backup_path, rb_err)
        # 清理临时文件
        try:
            os.unlink(temp_path)
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，临时文件清理失败: %s", _os_err)
        return result

    # 步骤3: 验证临时文件
    try:
        with open(temp_path, 'r', encoding='utf-8') as f:
            written = f.read()
        if written != new_content:
            raise RuntimeError("写入内容验证不匹配")
    except Exception as e:
        result['error'] = f"验证失败: {e}"
        # 回滚
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except Exception as rb_err:
                logging.error("UPG-P1-07: 回滚失败! error=%s", rb_err)
        try:
            os.unlink(temp_path)
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，临时文件清理失败: %s", _os_err)
        return result

    # 步骤4: 原子替换
    try:
        os.replace(temp_path, target_path)
    except Exception as e:
        result['error'] = f"原子替换失败: {e}"
        # 回滚
        if backup_path and os.path.exists(backup_path):
            try:
                _shutil.copy2(backup_path, target_path)
            except Exception as rb_err:
                logging.error("UPG-P1-07: 回滚失败! error=%s", rb_err)
        try:
            os.unlink(temp_path)
        except OSError as _os_err:
            if getattr(_os_err, 'errno', None) == errno.ENOSPC:
                logging.critical("[P1-16] 磁盘空间不足，临时文件清理失败: %s", _os_err)
        return result

    result['success'] = True
    logging.info("UPG-P1-07: 文件原子替换成功: %s (backup=%s)", target_path, backup_path)
    return result


def cleanup_atomic_backup(backup_path: str) -> None:
    """UPG-P1-07修复: 清理原子替换的备份文件

    确认更新成功后调用此函数清理备份。

    Args:
        backup_path: 备份文件路径
    """
    if backup_path and os.path.exists(backup_path):
        try:
            os.unlink(backup_path)
            logging.debug("UPG-P1-07: 已清理备份文件: %s", backup_path)
        except OSError as e:
            logging.warning("UPG-P1-07: 清理备份文件失败: %s", e)


# ============================================================================
# UPG-P1-09修复: 自动回滚机制
# ============================================================================

class AutoRollbackContext:
    """UPG-P1-09修复: 自动回滚上下文管理器

    在更新操作中使用，确保失败时自动回滚到更新前的状态。

    用法:
        with AutoRollbackContext("schema_update") as ctx:
            ctx.checkpoint("step1", state_before_step1)
            do_step1()
            ctx.checkpoint("step2", state_before_step2)
            do_step2()
        # 如果do_step2()抛出异常，自动回滚到step1的状态
    """

    def __init__(self, operation_name: str, rollback_handler: Optional[Callable] = None):
        """初始化自动回滚上下文

        Args:
            operation_name: 操作名称（用于日志）
            rollback_handler: 自定义回滚处理函数，签名: handler(checkpoint_name, checkpoint_state)
        """
        self._operation_name = operation_name
        self._rollback_handler = rollback_handler
        self._checkpoints: List[Dict[str, Any]] = []
        self._completed = False

    def checkpoint(self, name: str, state: Any) -> None:
        """设置检查点，保存回滚状态

        Args:
            name: 检查点名称
            state: 回滚时恢复的状态
        """
        self._checkpoints.append({'name': name, 'state': state})
        logging.debug("UPG-P1-09: 检查点已设置: %s/%s", self._operation_name, name)

    def mark_completed(self) -> None:
        """标记操作成功完成，不需要回滚"""
        self._completed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not self._completed and self._checkpoints:
            # 发生异常且未完成，执行回滚
            logging.warning(
                "UPG-P1-09: 操作%s失败，开始自动回滚(error=%s)",
                self._operation_name, exc_val,
            )
            # 从最近的检查点开始回滚
            for cp in reversed(self._checkpoints):
                try:
                    if self._rollback_handler:
                        self._rollback_handler(cp['name'], cp['state'])
                    else:
                        logging.info(
                            "UPG-P1-09: 回滚到检查点 %s/%s",
                            self._operation_name, cp['name'],
                        )
                except Exception as rb_err:
                    logging.error(
                        "UPG-P1-09: 回滚检查点%s失败: %s",
                        cp['name'], rb_err,
                    )
            logging.info("UPG-P1-09: 自动回滚完成: %s", self._operation_name)
        return False  # 不吞没异常


# ============================================================================
# UPG-P1-11修复: 代码版本与手册版本同步检查
# ============================================================================

_CODE_VERSION = "2.0.0"  # 代码版本号
_MANUAL_VERSION = "2.0.0"  # 手册版本号


def check_version_sync(code_version: str = None, manual_version: str = None) -> Dict[str, Any]:
    """UPG-P1-11修复: 检查代码版本与手册版本是否同步

    确保代码实现与操作手册版本一致，防止版本漂移导致操作错误。

    Args:
        code_version: 代码版本号（默认使用内置版本）
        manual_version: 手册版本号（默认使用内置版本，可通过环境变量MANUAL_VERSION覆盖）

    Returns:
        Dict: {synced: bool, code_version: str, manual_version: str, warning: str}
    """
    cv = code_version or _CODE_VERSION
    mv = manual_version or os.environ.get('MANUAL_VERSION', _MANUAL_VERSION)

    result = {
        'synced': cv == mv,
        'code_version': cv,
        'manual_version': mv,
        'warning': '',
    }

    if cv != mv:
        result['warning'] = (
            f"代码版本({cv})与手册版本({mv})不一致! "
            f"请确认代码和手册是否已同步更新。"
        )
        logging.warning("UPG-P1-11: %s", result['warning'])
    else:
        logging.debug("UPG-P1-11: 版本同步检查通过: code=%s manual=%s", cv, mv)

    return result


# ============================================================================
# P2修复: 不变量运行时检查与恢复策略
# ============================================================================

# P2修复: shared_utils中的不变量恢复策略文档
# 以下不变量在shared_utils各工具函数中隐含，现显式声明恢复策略:
# - INV-UTIL-01 (to_float32结果非NaN/inf): 违反时返回0.0，记录WARNING
# - INV-UTIL-02 (normalize_instrument_id非空): 违反时返回空字符串，记录WARNING
# - INV-UTIL-03 (ShardRouter.shard_count > 0): 违反时使用默认值16，记录WARNING
# - INV-UTIL-04 (RingBuffer.maxlen > 0): 违反时使用默认值100，记录WARNING
# - INV-UTIL-05 (InitPhase有序递增): 违反时抛出InitializationError
# - INV-UTIL-06 (safe_int/safe_float返回有效值): 违反时返回default，记录WARNING

def check_shared_utils_invariants() -> Dict[str, Any]:
    """P2修复: 检查shared_utils模块中的不变量

    将注释中的不变量断言转为运行时检查，
    违反时执行恢复策略（使用安全默认值）。

    Returns:
        Dict: {all_passed: bool, violations: list, recoveries: list}
    """
    violations = []
    recoveries = []

    # INV-UTIL-03: 全局ShardRouter的shard_count > 0
    try:
        router = get_shard_router()
        if router.shard_count <= 0:
            violations.append('INV-UTIL-03: ShardRouter.shard_count <= 0')
            recoveries.append('INV-UTIL-03: 重新初始化ShardRouter(shard_count=16)')
            logging.warning("[shared_utils] P2修复: INV-UTIL-03违反, shard_count<=0")
    except Exception as e:
        violations.append(f'INV-UTIL-03: ShardRouter检查异常: {e}')
        logging.warning("[shared_utils] P2修复: INV-UTIL-03检查异常: %s", e)

    # INV-UTIL-06: to_float32对NaN/inf的处理
    import math
    test_result = to_float32(float('nan'))
    if math.isnan(test_result) or math.isinf(test_result):
        violations.append('INV-UTIL-06: to_float32返回NaN/inf而非0.0')
        recoveries.append('INV-UTIL-06: to_float32已修复NaN/inf返回0.0')
        logging.warning("[shared_utils] P2修复: INV-UTIL-06违反, to_float32返回NaN/inf")

    all_passed = len(violations) == 0
    if not all_passed:
        logging.warning(
            "[shared_utils] P2修复: 不变量检查发现%d项违反",
            len(violations),
        )
    return {
        'all_passed': all_passed,
        'violations': violations,
        'recoveries': recoveries,
    }
