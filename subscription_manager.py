"""
subscription_manager_v2.py - 企业级订阅管理器 (临时优化版)

基于 order_flow_analyzer.py 的设计思想重构:
1. 重试队列 + 退避策略 (应对平台API限流/超时)
2. 异步落盘防止状态丢失 (JSONL WAL文件)
3. 告警回调集成 (订阅失败率超阈值通知)
4. 配置对象替代硬编码 (灵活适配不同场景)

⚠️ 这是临时验证文件,待实证通过后合并到 subscription_manager.py
"""

import atexit
import json
import logging
import os
import random
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from ali2026v3_trading.shared_utils import safe_float, normalize_instrument_id

import pyarrow as pa

logger = logging.getLogger(__name__)


def classify_registered_instruments(storage) -> Tuple[List[str], Dict[int, List[str]]]:
    """按 underlying_future_id 对已注册合约做轻量分组，避免初始化阶段逐合约阻塞等待。"""
    futures_instruments: List[str] = []
    option_instruments: Dict[int, List[str]] = {}

    if not storage:
        return futures_instruments, option_instruments

    from ali2026v3_trading.params_service import get_params_service

    registered_ids = storage.get_registered_instrument_ids() or []
    logger.debug("[InitServices] 已注册合约数量: %d", len(registered_ids))
    params_service = get_params_service()

    for inst_id in registered_ids:
        if SubscriptionManager.is_option(inst_id):
            try:
                meta = params_service.get_instrument_meta_by_id(inst_id) if params_service else None
                underlying_future_id = meta.get('underlying_future_id') if meta else None
                if not underlying_future_id:
                    logger.debug("[InitServices] 跳过缺少 underlying_future_id 的期权: %s", inst_id)
                    continue
                option_instruments.setdefault(int(underlying_future_id), []).append(inst_id)
            except Exception as exc:
                logger.debug("[InitServices] 期权解析失败: %s - %s", inst_id, exc)
        else:
            futures_instruments.append(inst_id)

    return futures_instruments, option_instruments


# ========== EventBus 安全导入（从 event_bus 模块统一导入）==========
try:
    from ali2026v3_trading.event_bus import get_global_event_bus, EventBus
    _HAS_EVENT_BUS = get_global_event_bus() is not None
except ImportError as e:
    logger.warning(f"[SubscriptionManagerV2] EventBus import failed: {e}")
    EventBus = None
    get_global_event_bus = None
    _HAS_EVENT_BUS = False


# ========== 工具函数 ==========

def inst_get(inst: Any, *keys: str, default: Any = '') -> Any:
    """兼容 dict / object 两种返回结构读取字段"""
    if isinstance(inst, dict):
        for k in keys:
            if k in inst and inst.get(k) not in (None, ''):
                return inst.get(k)
        return default
    for k in keys:
        try:
            val = getattr(inst, k, None)
        except (AttributeError, TypeError) as e:
            logger.debug(f"[SubscriptionManagerV2] Failed to get attribute {k}: {e}")
            val = None
        if val not in (None, ''):
            return val
    return default


# ========== 配置对象 (替代硬编码) ==========

@dataclass
class SubscriptionConfig:
    """订阅管理器配置 - 借鉴 OrderFlowConfig 设计"""
    
    # 重试策略
    max_retries: int = 3                          # 最大重试次数
    retry_base_delay: float = 1.0                 # 基础延迟(秒)
    retry_max_delay: float = 60.0                 # 最大延迟(秒)
    backoff_strategy: str = "exponential"         # 退避策略: exponential/linear/random_jitter
    
    # 批量写入
    db_batch_size: int = 100                      # DuckDB批量大小
    handshake_timeout: float = 10.0               # 握手超时(秒)
    throttle_window: float = 1.0                  # 节流窗口(秒)
    
    # 异步落盘 (WAL保护)
    enable_wal: bool = True                       # 启用WAL文件
    wal_path: str = "subscription_wal.jsonl"      # WAL文件路径
    wal_max_size_mb: int = 50                     # WAL最大文件大小(MB)
    wal_max_rotations: int = 3                    # WAL轮转数量
    recover_on_start: bool = True                 # 启动时恢复WAL
    
    # 重试队列
    retry_queue_max_size: int = 500               # 重试队列最大容量
    retry_interval: float = 5.0                   # 重试线程间隔(秒)
    max_event_age_seconds: int = 1800             # 事件最大存活时间(30分钟)
    
    # 清理线程配置 (借鉴 OrderFlowAnalyzer)
    enable_cleanup_thread: bool = True            # 启用独立清理线程
    cleanup_interval: float = 60.0                # 清理检查间隔(秒)
    
    # 告警回调
    alert_callback: Optional[Callable[[int, str], None]] = None
    alert_threshold: int = 10                     # 累计失败超过此值触发告警
    failure_rate_threshold: float = 0.1           # 失败率超过10%触发告警
    
    # 监控
    enable_stats: bool = True                     # 启用统计信息
    stats_report_interval: float = 60.0           # 统计报告间隔(秒)



# ========== 企业级订阅管理器 ==========

class SubscriptionManager:
    """
    企业级订阅管理器 - 融合订单流设计的健壮性特性
    
    核心改进:
    1. 重试队列 + 指数退避 → 应对平台API限流
    2. WAL异步落盘 → 进程崩溃后恢复订阅状态
    3. 告警回调 → 实时监控订阅健康度
    4. 配置驱动 → 灵活适配回测/实盘不同场景
    """
    
    def __init__(self, data_manager: Any, config: SubscriptionConfig = None):
        self._config = config or SubscriptionConfig()
        self.data_manager = data_manager
        
        # 线程安全锁
        self._lock = threading.RLock()
        self._tick_lock = threading.RLock()
        self._retry_lock = threading.Lock()
        self._wal_lock = threading.Lock()
        
        # 订阅状态
        self._subscriptions: Dict[str, Dict] = {}
        self.t_type_service = None
        
        # 订阅成功跟踪：用实际收到作为成功标准（品种维度）
        self._subscription_success = {
            'total_subscribed': 0,        # 分母：订阅合约总数
            'total_products': 0,          # 分母：订阅品种总数
            'kline_received': 0,          # 分子：收到K线的合约数
            'tick_received': 0,           # 分子：收到Tick的合约数
            'kline_products': set(),      # 已收到K线的品种集合（如 AU, CU, AL）
            'tick_products': set(),       # 已收到Tick的品种集合
            'kline_instruments': set(),   # 已收到K线的合约集合
            'tick_instruments': set(),    # 已收到Tick的合约集合
            'subscribed_products': set(), # 已订阅的品种集合
            'subscribe_time': {},         # {inst_id: subscribe_timestamp}
        }
        self._subscription_success_lock = threading.Lock()
        self._submgr_tick_summary_interval = 60.0
        self._submgr_tick_last_output_time = 0.0
        self._submgr_tick_accum_count = 0
        self._submgr_tick_accum_lock = threading.Lock()
        
        # 重试队列 (task_data, retry_count, next_retry_time, enqueue_time)
        self._retry_queue: deque = deque(maxlen=self._config.retry_queue_max_size)
        self._dropped_count = 0
        self._last_alert_count = 0
        self._total_subscriptions = 0
        self._total_failures = 0
        
        # 合约元数据未找到警告的去重追踪（避免日志洪水）
        self._missing_metadata_warning_limit: int = 100  # 最多跟踪100个不同合约
        self._missing_metadata_warnings: deque = deque(maxlen=self._missing_metadata_warning_limit)
        
        # WAL文件管理
        self._wal_file = None
        self._async_thread: Optional[threading.Thread] = None
        self._stop_async = threading.Event()
        self._retry_thread: Optional[threading.Thread] = None
        self._stop_retry = threading.Event()
        self._cleanup_thread: Optional[threading.Thread] = None
        self._stop_cleanup = threading.Event()
        
        # 初始化
        self._init_wal()
        self._bg_threads_started = False
        atexit.register(self._atexit_cleanup)
        
        logger.info(
            "[SubscriptionManagerV2] Initialized: retries=%d, backoff=%s, wal=%s",
            self._config.max_retries, 
            self._config.backoff_strategy,
            "ON" if self._config.enable_wal else "OFF"
        )
    
    # ========================================================================
    # WAL文件管理 (异步落盘防止状态丢失)
    # ========================================================================
    
    def _init_wal(self):
        """初始化WAL文件"""
        if not self._config.enable_wal:
            return
        
        try:
            os.makedirs(os.path.dirname(self._config.wal_path) if os.path.dirname(self._config.wal_path) else '.', exist_ok=True)
            self._wal_file = open(self._config.wal_path, 'a', encoding='utf-8')
            logger.info("[SubscriptionManagerV2] WAL enabled: %s", self._config.wal_path)
            
            # P1 Bug #36修复：注册atexit关闭WAL文件，防止进程退出时文件泄漏
            atexit.register(self._close_wal)
            
            # 启动时恢复
            if self._config.recover_on_start:
                self._recover_from_wal()
        except Exception as e:
            logger.error("[SubscriptionManagerV2] WAL init failed: %s", e)
            self._wal_file = None
    
    def _rotate_wal(self):
        """WAL文件轮转"""
        if not self._config.enable_wal or not self._wal_file:
            return
        
        try:
            if not os.path.exists(self._config.wal_path):
                return
            
            size_mb = os.path.getsize(self._config.wal_path) / (1024 * 1024)
            if size_mb >= self._config.wal_max_size_mb:
                base, ext = os.path.splitext(self._config.wal_path)
                for i in range(self._config.wal_max_rotations - 1, 0, -1):
                    old = f"{base}.{i}{ext}"
                    new = f"{base}.{i+1}{ext}"
                    if os.path.exists(old):
                        os.rename(old, new)
                if os.path.exists(self._config.wal_path):
                    os.rename(self._config.wal_path, f"{base}.1{ext}")
                logger.info("[SubscriptionManagerV2] Rotated WAL: %s", self._config.wal_path)
        except Exception as e:
            logger.error("[SubscriptionManagerV2] WAL rotation failed: %s", e)
    
    def _write_wal_async(self, record: dict):
        """异步写入WAL - 使用队列缓冲"""
        if not self._config.enable_wal:
            return
        
        with self._wal_lock:
            try:
                # 直接写入内存队列,由后台线程批量落盘
                if not hasattr(self, '_wal_queue'):
                    self._wal_queue = deque(maxlen=1000)
                self._wal_queue.append(record)
            except Exception as e:
                logger.error("[SubscriptionManagerV2] WAL queue write failed: %s", e)
    
    def _recover_from_wal(self):
        """从WAL恢复订阅状态"""
        if not self._config.enable_wal or not os.path.exists(self._config.wal_path):
            return
        
        try:
            recovered = 0
            with open(self._config.wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        task_type = record.get('type')
                        
                        if task_type == 'subscribe':
                            instrument_id = record.get('instrument_id')
                            if instrument_id:
                                # 重新加入重试队列
                                with self._retry_lock:
                                    if len(self._retry_queue) < self._config.retry_queue_max_size:
                                        self._retry_queue.append((record, 0, time.time(), time.time()))
                                        recovered += 1
                                    else:
                                        logger.error("[SubscriptionManagerV2] Retry queue full during WAL recovery, dropping recovered task")
                                        self._dropped_count += 1
                                        self._check_alert()
                    except Exception as e:
                        logger.error(f"[SubscriptionManagerV2] Failed to process WAL record: {e}")
            
            logger.info("[SubscriptionManagerV2] Recovered %d subscription tasks from WAL", recovered)
            
            # 恢复后重命名文件（如果失败则保留原文件）
            try:
                # Windows上可能因文件锁定导致重命名失败，尝试多种方式
                recovered_path = self._config.wal_path + ".recovered"
                if os.path.exists(recovered_path):
                    try:
                        os.remove(recovered_path)
                    except Exception:
                        pass
                os.rename(self._config.wal_path, recovered_path)
            except OSError as e:
                # Windows上文件锁定时重命名失败是常见情况，不影响功能
                if e.winerror == 32:  # ERROR_SHARING_VIOLATION
                    logger.warning(f"[SubscriptionManagerV2] WAL file locked by another process, skipping rename")
                else:
                    logger.warning(f"[SubscriptionManagerV2] WAL rename failed: {e}")
            except Exception as e:
                logger.warning(f"[SubscriptionManagerV2] WAL rename failed: {e}")
        except Exception as e:
            logger.error("[SubscriptionManagerV2] WAL recovery failed: %s", e)
    
    def _close_wal(self):
        """P1 Bug #36修复：关闭WAL文件，防止进程退出时文件泄漏"""
        if self._wal_file:
            try:
                self._wal_file.flush()
                self._wal_file.close()
                logger.info("[SubscriptionManagerV2] WAL file closed")
            except Exception as e:
                logger.warning("[SubscriptionManagerV2] WAL close error: %s", e)
            finally:
                self._wal_file = None
    
    # ========================================================================
    # 后台线程管理
    # ========================================================================
    
    def start(self):
        """显式启动后台线程（构造函数不再自动启动）"""
        self._start_background_threads()

    def _start_background_threads(self):
        """启动后台线程"""
        if self._bg_threads_started:
            return
        self._bg_threads_started = True
        # 异步WAL写入线程
        if self._config.enable_wal:
            self._stop_async.clear()
            self._async_thread = threading.Thread(
                target=self._wal_writer_loop, 
                name="SubAsyncWriter[shared-service]", 
                daemon=True
            )
            self._async_thread.start()
        
        # 重试线程
        self._stop_retry.clear()
        self._retry_thread = threading.Thread(
            target=self._retry_loop,
            name="SubRetry[shared-service]",
            daemon=True
        )
        self._retry_thread.start()
        
        # 清理线程 (独立于重试逻辑)
        if self._config.enable_cleanup_thread:
            self._stop_cleanup.clear()
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                name="SubCleanup[shared-service]",
                daemon=True
            )
            self._cleanup_thread.start()
            logger.info(
                "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
                "Cleanup thread started (interval=%.1fs)",
                self._config.cleanup_interval
            )

    def ensure_background_threads(self) -> None:
        """确保后台线程处于运行状态，供 stop 后重启复用。"""
        with self._lock:
            need_async = bool(
                self._config.enable_wal and (self._async_thread is None or not self._async_thread.is_alive())
            )
            need_retry = bool(self._retry_thread is None or not self._retry_thread.is_alive())
            need_cleanup = bool(
                self._config.enable_cleanup_thread and (self._cleanup_thread is None or not self._cleanup_thread.is_alive())
            )

            if not (need_async or need_retry or need_cleanup):
                return

            rearm_details = []
            if need_async:
                rearm_details.append(f"async_writer(alive={self._async_thread is not None and self._async_thread.is_alive()})")
            if need_retry:
                rearm_details.append(f"retry(alive={self._retry_thread is not None and self._retry_thread.is_alive()})")
            if need_cleanup:
                rearm_details.append(f"cleanup(alive={self._cleanup_thread is not None and self._cleanup_thread.is_alive()})")

            logger.info(
                "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
                "Rearming background threads (rearm_reason=ensure-alive, close_policy=process-lifetime, "
                "details=%s)", ", ".join(rearm_details)
            )
            self._start_background_threads()

    def stop_background_threads(self, join_timeout: float = 2.0) -> None:
        self._stop_async.set()
        self._stop_retry.set()
        self._stop_cleanup.set()

        logger.info(
            "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
            "Stopping background threads (close_reason=strategy-stop, close_policy=graceful-join)"
        )

        for t in [self._async_thread, self._retry_thread, self._cleanup_thread]:
            if t and t.is_alive():
                t.join(timeout=join_timeout)
                if t.is_alive():
                    logger.warning(
                        "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
                        "Thread %s did not exit within timeout (close_policy=graceful-failed)", t.name
                    )
                else:
                    logger.info(
                        "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
                        "Thread %s exited cleanly (close_policy=graceful-join)", t.name
                    )
    
    def _wal_writer_loop(self):
        """WAL写入线程 - 批量异步落盘"""
        while not self._stop_async.is_set():
            try:
                # 批量取出记录
                batch = []
                with self._wal_lock:
                    if hasattr(self, '_wal_queue'):
                        while self._wal_queue and len(batch) < 50:
                            batch.append(self._wal_queue.popleft())
                
                # 批量写入文件
                if batch and self._wal_file:
                    self._rotate_wal()
                    for record in batch:
                        self._wal_file.write(json.dumps(record, ensure_ascii=False) + '\n')
                    self._wal_file.flush()
                
                time.sleep(0.05)  # 50ms批次间隔
            except Exception as e:
                logger.error("[SubscriptionManagerV2] WAL writer error: %s", e)
                time.sleep(0.1)
    
    def _retry_loop(self):
        """重试线程 - 定期处理失败任务"""
        while not self._stop_retry.is_set():
            time.sleep(self._config.retry_interval)
            try:
                self._process_retry_queue()
            except Exception as e:
                logger.error("[SubscriptionManagerV2] Retry loop error: %s", e)
    
    def _atexit_cleanup(self):
        """进程退出清理"""
        self.close()
    
    def close(self):
        """关闭所有资源"""
        self.stop_background_threads(join_timeout=2.0)
        
        if self._wal_file:
            try:
                self._wal_file.flush()
                self._wal_file.close()
                self._wal_file = None
            except Exception as e:
                logger.error(f"[SubscriptionManagerV2] Operation failed: {e}", exc_info=True)
        
        logger.info("[SubscriptionManagerV2] Closed")
    
    # ========================================================================
    # 退避策略实现
    # ========================================================================
    
    def _calc_backoff_delay(self, retry_count: int) -> float:
        """计算退避延迟"""
        base = self._config.retry_base_delay
        max_delay = self._config.retry_max_delay
        
        if self._config.backoff_strategy == "exponential":
            delay = base * (2 ** retry_count)
        elif self._config.backoff_strategy == "linear":
            delay = base * (retry_count + 1)
        elif self._config.backoff_strategy == "random_jitter":
            delay = base * (1 + random.random())
        else:
            delay = base * (2 ** retry_count)
        
        return min(delay, max_delay)
    
    # ========================================================================
    # 重试队列管理
    # ========================================================================
    
    def _enqueue_for_retry(self, task_data: dict, retry_count: int = 0):
        """将失败任务加入重试队列"""
        now = time.time()
        delay = self._calc_backoff_delay(retry_count)
        
        with self._retry_lock:
            if len(self._retry_queue) >= self._config.retry_queue_max_size:
                logger.error("[SubscriptionManagerV2] Retry queue full, dropping task")
                self._dropped_count += 1
                # WAL保护: 溢出任务也落盘,防止永久丢失
                self._write_wal_async({
                    'type': 'queue_overflow',
                    'instrument_id': task_data.get('instrument_id'),
                    'task_type': task_data.get('type'),
                    'timestamp': now
                })
                self._check_alert()
                return
            
            next_retry = now + delay
            self._retry_queue.append((task_data, retry_count + 1, next_retry, now))
            
            # 写入WAL
            self._write_wal_async({
                'type': 'retry_enqueue',
                'instrument_id': task_data.get('instrument_id'),
                'retry_count': retry_count + 1,
                'timestamp': now
            })
    
    def _process_retry_queue(self):
        """处理重试队列 (已移除过期检查,由独立清理线程负责)"""
        if not self._retry_queue:
            return
        
        now = time.time()
        success_count = 0
        still_pending = []
        
        with self._retry_lock:
            while self._retry_queue:
                task, count, next_time, enq_time = self._retry_queue[0]
                
                # 检查是否到达重试时间
                if next_time > now:
                    break
                
                self._retry_queue.popleft()
                
                # 执行重试
                try:
                    task_type = task.get('type')
                    if task_type == 'subscribe':
                        self._retry_subscribe(task)
                        success_count += 1
                    elif task_type == 'handshake':
                        self._retry_handshake(task)
                        success_count += 1
                except Exception as e:
                    logger.error("[SubscriptionManagerV2] Retry failed: %s", e)
                    if count < self._config.max_retries:
                        still_pending.append((task, count, now + self._calc_backoff_delay(count), enq_time))
                    else:
                        logger.error("[SubscriptionManagerV2] Task dropped after %d retries", count)
                        self._dropped_count += 1
                        self._total_failures += 1
                        # WAL保护: 最终丢弃的任务也落盘
                        self._write_wal_async({
                            'type': 'final_drop',
                            'instrument_id': task.get('instrument_id'),
                            'task_type': task_type,
                            'retry_count': count,
                            'timestamp': now
                        })
        
        # 放回未处理的任务
        # P1 Bug #37修复：使用优先队列按next_retry_time排序，到期任务不被未到期任务阻塞
        with self._retry_lock:
            # still_pending是元组列表: (task, count, next_retry_time, enq_time)
            # 按next_retry_time（索引2）排序，最早重试的排前面
            still_pending_sorted = sorted(still_pending, key=lambda x: x[2])
            for task_tuple in still_pending_sorted:
                if len(self._retry_queue) >= self._config.retry_queue_max_size:
                    logger.error("[SubscriptionManagerV2] Retry queue full when returning pending tasks, dropping task")
                    self._dropped_count += 1
                    self._check_alert()
                    continue
                self._retry_queue.append(task_tuple)
        
        if success_count > 0:
            logger.debug("[SubscriptionManagerV2] Retried %d tasks successfully", success_count)
        
        # 检查告警
        self._check_alert()
    
    def _do_subscribe(self, instrument_id: str, data_type: str) -> None:
        """
        统一订阅方法（内部使用）
        
        ✅ 序号122修复：统一为data_manager.subscribe，删除_platform_subscribe回退
        
        Args:
            instrument_id: 合约ID
            data_type: 数据类型 ('tick', 'kline'等)
        """
        subscribe_method = getattr(self.data_manager, 'subscribe', None)
        if subscribe_method and callable(subscribe_method):
            subscribe_method(instrument_id, data_type)
        else:
            raise AttributeError("InstrumentDataManager 缺少 subscribe 方法")
    
    def _retry_subscribe(self, task: dict):
        instrument_id = task.get('instrument_id')
        data_type = task.get('data_type', 'tick')
        
        logger.info("[SubscriptionManagerV2] Retrying subscribe: %s", instrument_id)
        self._do_subscribe(instrument_id, data_type)
    
    def _retry_handshake(self, task: dict):
        """重试握手"""
        underlying = task.get('underlying')
        expiration = task.get('expiration')
        
        logger.info("[SubscriptionManagerV2] Retrying handshake: %s_%s", underlying, expiration)
        # TODO: 调用实际握手逻辑
        self._handshake(underlying, expiration)
    
    # ========================================================================
    # 独立清理线程 (借鉴 OrderFlowAnalyzer 设计)
    # ========================================================================
    
    def _cleanup_expired_events(self):
        """清理重试队列中超过最大存活时间的事件"""
        now = time.time()
        with self._retry_lock:
            new_queue = deque(maxlen=self._config.retry_queue_max_size)
            expired_count = 0
            for task, cnt, next_time, enq_time in self._retry_queue:
                if now - enq_time > self._config.max_event_age_seconds:
                    logger.warning("[SubscriptionManagerV2] Expired task dropped: %s", task.get('instrument_id'))
                    self._dropped_count += 1
                    self._total_failures += 1
                    # WAL保护: 清理事件也落盘,可追溯
                    self._write_wal_async({
                        'type': 'expired_drop',
                        'instrument_id': task.get('instrument_id'),
                        'task_type': task.get('type'),
                        'age_seconds': now - enq_time,
                        'timestamp': now
                    })
                    expired_count += 1
                    continue
                new_queue.append((task, cnt, next_time, enq_time))
            self._retry_queue = new_queue
            
            if expired_count > 0:
                logger.info("[SubscriptionManagerV2] Cleaned %d expired tasks", expired_count)
                self._check_alert()
    
    def _cleanup_loop(self):
        """定期清理过期事件 (独立线程,不阻塞重试逻辑)"""
        while not self._stop_cleanup.is_set():
            time.sleep(self._config.cleanup_interval)
            try:
                self._cleanup_expired_events()
            except Exception as e:
                logger.error("[SubscriptionManagerV2] Cleanup error: %s", e)
    
    # ========================================================================
    # 告警回调集成
    # ========================================================================
    
    def _check_alert(self):
        """检查是否触发告警（✅ P1修复：加锁保护共享计数器）"""
        if not self._config.alert_callback:
            return
        
        # ✅ 加锁读取共享计数器
        with self._lock:
            dropped_count = self._dropped_count
            total_failures = self._total_failures
            total_subscriptions = self._total_subscriptions
        
        # 检查绝对失败数
        if (dropped_count - self._last_alert_count) >= self._config.alert_threshold:
            self._config.alert_callback(
                dropped_count,
                f"Subscription dropped tasks threshold exceeded: {dropped_count}"
            )
            self._last_alert_count = dropped_count
        
        # 检查失败率
        if total_subscriptions > 0:
            failure_rate = total_failures / total_subscriptions
            if failure_rate > self._config.failure_rate_threshold:
                self._config.alert_callback(
                    int(failure_rate * 100),
                    f"Subscription failure rate too high: {failure_rate:.2%}"
                )
    
    # ========================================================================
    # 合约解析与分类 (保留原逻辑)
    # ========================================================================
    
    @staticmethod
    def _strip_exchange_prefix(instrument_id: str) -> str:
        """剥离交易所前缀（统一使用正则）
        
        Args:
            instrument_id: 合约ID（可能带交易所前缀，如 'CFFEX.IF2603'）
        
        Returns:
            str: 纯净的合约ID（如 'IF2603'）
        """
        # 直接使用正则提取合约ID，避免二次标准化
        import re
        match = re.search(r'([A-Za-z]+\d+.*?)$', instrument_id)
        return match.group(1) if match else instrument_id
    
    @staticmethod
    def _normalize_product_code(product: str) -> str:
        """标准化产品代码，用于内部比较和分类
        
        Args:
            product: 产品代码字符串
        
        Returns:
            str: 标准化的产品代码（SHFE品种小写，其他大写）
        """
        if not product:
            return ''
        
        return str(product)
    
    @staticmethod
    def parse_future(instrument_id: str) -> Dict[str, Any]:
        """解析期货合约"""
        clean_id = SubscriptionManager._strip_exchange_prefix(instrument_id)
        match = re.match(r'^([A-Za-z]+)(\d{3,4})$', clean_id)
        if not match:
            raise ValueError(f"无法解析期货：{instrument_id}")
        
        raw_product = match.group(1)
        year_month = match.group(2)
        
        # 使用标准化函数
        product = SubscriptionManager._normalize_product_code(raw_product)
        
        return {'product': product, 'year_month': year_month}
    
    @staticmethod
    def is_option(instrument_id: str) -> bool:
        """判断是否为期权"""
        try:
            SubscriptionManager.parse_option(normalize_instrument_id(instrument_id))
            return True
        except (ValueError, Exception):
            return False
    
    @staticmethod
    def parse_option(instrument_id: str) -> Dict[str, Any]:
        r"""
        解析期权合约(单一正则同时匹配连字符和紧凑格式, 品种ID直通)
        
        修复标准#125: 一种正则解析所有格式, 原样ID直通不改变格式.
        统一正则 r'^([A-Za-z]+)(\d{3,4})-?([CP])-?(\d+(?:\.\d+)?)$'
        同时匹配连字符格式(CU2603-C-5000)和紧凑格式(CU2603C5000).
        """
        clean_id = SubscriptionManager._strip_exchange_prefix(instrument_id)
        
        # 单一正则: 同时匹配连字符格式和紧凑格式
        # - 连字符: CU2603-C-5000 (-分隔, 行权价支持小数)
        # - 紧凑: CU2603C5000 (无分隔, 行权价仅整数)
        # -? 使连字符可选, \d{3,4} 覆盖两种年月位数
        match = re.match(r'^([A-Za-z]+)(\d{3,4})-?([CP])-?(\d+(?:\.\d+)?)$', clean_id)
        if match:
            product = match.group(1)  # 直通: 保持原始大小写
            year_month_raw = match.group(2)
            option_type = match.group(3)
            strike_price = float(match.group(4))
            
            # 年月归一化(3位转4位, 仅紧凑格式可能出现3位)
            year_month = SubscriptionManager._normalize_option_year_month(year_month_raw)
            
            # 格式标记: 通过原始ID中是否含-判断(不改变ID, 仅标记来源格式)
            fmt = 'dash' if '-' in clean_id else 'compact'
            
            return {
                'product': product,
                'year_month': year_month,
                'option_type': option_type,
                'strike_price': strike_price,
                'format': fmt,
            }
        
        raise ValueError(f"无法解析期权: {instrument_id}")
    
    @staticmethod
    def _normalize_option_year_month(year_month_raw: str) -> str:
        """归一化期权年月"""
        normalized = normalize_instrument_id(year_month_raw)
        if len(normalized) == 4:
            return normalized
        if len(normalized) != 3 or not normalized.isdigit():
            raise ValueError(f"非法期权月份编码：{year_month_raw}")
        
        current_year = datetime.now().year % 100
        year_digit = int(normalized[0])
        month_digits = normalized[1:]
        
        current_decade = (current_year // 10) * 10
        candidate_years = []
        for decade_offset in (-10, 0, 10):
            candidate_year = current_decade + decade_offset + year_digit
            if 0 <= candidate_year <= 99:
                candidate_years.append(candidate_year)
        
        resolved_year = min(candidate_years, key=lambda year: (abs(year - current_year), -year))
        return f"{resolved_year:02d}{month_digits}"
    
    # ✅ 接口唯一：classify_instruments唯一实现，query_service两处已委托此处
    @staticmethod
    def classify_instruments(instrument_ids: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
        """分类合约
        
        options_dict 的 key 统一为 canonical underlying 标识 (product+year_month)，
        如 'IO2605', 'al2605'，与 ParamsService._extract_canonical_underlying 输出一致。
        """
        futures_list = []
        options_dict = {}
        
        for inst_id in instrument_ids:
            try:
                parsed = SubscriptionManager.parse_option(inst_id)
                # 统一 key 语义为 product+year_month
                underlying = f"{parsed['product']}{parsed['year_month']}"
                if underlying not in options_dict:
                    options_dict[underlying] = []
                options_dict[underlying].append(inst_id)
            except ValueError:
                futures_list.append(inst_id)
        
        return futures_list, options_dict
    
    # ========================================================================
    # 订阅核心逻辑 (增强版)
    # ========================================================================
    
    def set_t_type_service(self, t_type_service: Any) -> None:
        """注入TTypeService"""
        self.t_type_service = t_type_service
        logger.info("[SubscriptionManagerV2] TTypeService injected")
    
    def _ensure_instruments_loaded(self) -> bool:
        """
        确保合约数据已从配置文件加载到数据库（启动时必须完成）
        
        ⚠️ 关键设计：
        - 合约元数据必须在 Storage 初始化前从配置文件完整加载
        - 这个检查只验证配置是否已加载，不负责实际加载
        - 实际加载由 strategy_core_service.on_init() 调用 ensure_products_with_retry 完成
        - 如果配置未加载，返回 False 触发上层重试
        - 只有策略正常运行后，才可能从平台进行增量更新
        
        Returns:
            bool: 配置是否已加载完成
        """
        try:
            from ali2026v3_trading.data_service import get_data_service
            ds = get_data_service()
            
            # 检查是否标记为已加载
            if getattr(ds, '_products_loaded', False):
                logger.debug("[SubscriptionManagerV2] Products already marked as loaded")
                return True
            
            # 检查数据库是否有合约数据（配置加载的结果）
            futures_count = ds.query("SELECT COUNT(*) as cnt FROM futures_instruments").to_pylist()[0]['cnt']
            options_count = ds.query("SELECT COUNT(*) as cnt FROM option_instruments").to_pylist()[0]['cnt']
            
            if futures_count > 0 or options_count > 0:
                logger.debug(
                    f"[SubscriptionManagerV2] Database has instrument data: "
                    f"futures={futures_count}, options={options_count}"
                )
                # 标记为已加载（如果还没标记）
                ds._products_loaded = True
                return True
            
            # 数据库为空，说明配置尚未加载
            logger.warning(
                "[SubscriptionManagerV2] Instrument database is empty - "
                "ensure_products_with_retry must be called first from strategy_core_service.on_init()"
            )
            return False
            
        except Exception as e:
            logger.error(f"[SubscriptionManagerV2._ensure_instruments_loaded] Error: {e}")
            return False
    
    def subscribe_all_instruments(self, futures_list: List[str], 
                                   options_dict: Dict[str, List[str]]) -> int:
        """
        全量订阅 (增强版 - 带重试和WAL保护)
        
        Returns:
            bool: 是否全部成功
        """
        started_at = time.perf_counter()
        total_count = len(futures_list) + sum(len(opts) for opts in options_dict.values())
        self._total_subscriptions = total_count
        self.ensure_background_threads()
        
        # ✅ 在订阅前检查数据库是否为空，如果为空则从平台加载合约数据
        if not futures_list and not options_dict:
            logger.warning("[SubscriptionManagerV2] No instruments to subscribe, checking database...")
            loaded = self._ensure_instruments_loaded()
            if loaded:
                logger.info("[SubscriptionManagerV2] Instruments loaded from platform, retrying subscription")
                # 重新获取合约列表（需要从外部传入或重新查询）
                # 注意：这里需要调用方重新传递参数，因为订阅管理器不维护品种配置
        
        logger.info(
            "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
            "Starting bulk subscription: %d instruments", total_count
        )
        
        success_count = 0
        failed_tasks = []
        
        # 订阅期货
        for inst_id in futures_list:
            try:
                self._subscribe_single_with_retry(inst_id, 'tick')
                self._subscribe_single_with_retry(inst_id, 'kline_1min')
                success_count += 1
                
                # ✅ 环节1: 订阅成功探针
                from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                DiagnosisProbeManager.on_subscribe(inst_id, 'future', True)
            except Exception as e:
                logger.error("[SubscriptionManagerV2] Subscribe failed: %s - %s", inst_id, e)
                failed_tasks.append({
                    'type': 'subscribe',
                    'instrument_id': inst_id,
                    'data_type': 'tick',
                    'error': str(e)
                })
                
                # ✅ 环节1: 订阅失败探针
                from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                DiagnosisProbeManager.on_subscribe(inst_id, 'future', False, str(e))
        
        # 订阅期权
        for underlying, option_ids in options_dict.items():
            try:
                # 握手
                if option_ids:
                    parsed = self.parse_option(option_ids[0])
                    expiration = parsed['year_month']
                    if not self._handshake_with_retry(underlying, expiration):
                        logger.error("[SubscriptionManagerV2] Handshake failed: %s_%s", underlying, expiration)
                        failed_tasks.append({
                            'type': 'handshake',
                            'underlying': underlying,
                            'expiration': expiration
                        })
                        continue
                
                # 订阅期权合约
                for opt_id in option_ids:
                    try:
                        self._subscribe_single_with_retry(opt_id, 'tick')
                        success_count += 1
                        
                        # ✅ 环节1: 期权订阅成功探针
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_subscribe(opt_id, 'option', True)
                    except Exception as e:
                        logger.error("[SubscriptionManagerV2] Option subscribe failed: %s - %s", opt_id, e)
                        failed_tasks.append({
                            'type': 'subscribe',
                            'instrument_id': opt_id,
                            'data_type': 'tick',
                            'error': str(e)
                        })
                        
                        # ✅ 环节1: 期权订阅失败探针
                        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
                        DiagnosisProbeManager.on_subscribe(opt_id, 'option', False, str(e))
            except Exception as e:
                logger.error("[SubscriptionManagerV2] Option batch failed: %s - %s", underlying, e)
        
        # P1 Bug #38修复：累加失败计数，而非覆盖
        self._total_failures += len(failed_tasks)
        
        # 发布完成事件
        if _HAS_EVENT_BUS and get_global_event_bus and success_count > 0:
            try:
                eb = get_global_event_bus()
                from ali2026v3_trading.event_bus import SubscriptionCompletedEvent
                eb.publish(SubscriptionCompletedEvent(success_count, self._total_failures))
                logger.info("[SubscriptionManagerV2] Published completion event")
            except Exception as e:
                logger.warning("[SubscriptionManagerV2] Failed to publish event: %s", e)
        
        elapsed = time.perf_counter() - started_at
        logger.info(
            "[SubscriptionManagerV2] Bulk subscription completed: success=%d, failed=%d, total=%d, time=%.3fs",
            success_count, self._total_failures, total_count, elapsed
        )
        
        
        return total_count
    
    def _subscribe_single_with_retry(self, instrument_id: str, data_type: str):
        try:
            self._do_subscribe(instrument_id, data_type)
        except Exception as e:
            # 加入重试队列
            self._enqueue_for_retry({
                'type': 'subscribe',
                'instrument_id': instrument_id,
                'data_type': data_type
            })
            logger.warning("[SubscriptionManagerV2] 订阅失败已入重试队列: %s (%s)", instrument_id, data_type)
    
    def _handshake_with_retry(self, underlying: str, expiration: str) -> bool:
        """握手 (带重试)"""
        try:
            return self._handshake(underlying, expiration)
        except Exception as e:
            logger.error("[SubscriptionManagerV2] Handshake error: %s", e)
            self._enqueue_for_retry({
                'type': 'handshake',
                'underlying': underlying,
                'expiration': expiration
            })
            return False
    
    def _handshake(self, underlying: str, expiration: str) -> bool:
        """握手协议 (未实现 - 返回True以兼容现有调用方)"""
        logger.debug("[SubscriptionManagerV2] Handshake not implemented for %s/%s", underlying, expiration)
        return True
    
    # ========================================================================
    # 查询接口
    # ========================================================================
    
    def unsubscribe_all(self) -> bool:
        """取消所有订阅"""
        with self._lock:
            success_count = 0
            failed_count = 0
            
            for key, sub_info in list(self._subscriptions.items()):
                option_ids = sub_info.get('option_ids', [])
                for option_id in option_ids:
                    try:
                        unsubscribe_method = getattr(self.data_manager, 'unsubscribe', None)
                        if unsubscribe_method and callable(unsubscribe_method):
                            unsubscribe_method(option_id, 'tick')
                        success_count += 1
                    except Exception as e:
                        failed_count += 1
                        logger.warning("[SubscriptionManagerV2] Unsubscribe failed: %s - %s", option_id, e)
                
                del self._subscriptions[key]
            
            logger.info("[SubscriptionManagerV2] Unsubscribe completed: success=%d, failed=%d", 
                       success_count, failed_count)
            return failed_count == 0
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计"""
        with self._lock:
            total_options = sum(
                len(sub_info.get('option_ids', []))
                for sub_info in self._subscriptions.values()
            )
            
            return {
                'total_subscriptions': len(self._subscriptions),
                'total_option_contracts': total_options,
                'underlyings': list(self._subscriptions.keys()),
                'retry_queue_size': len(self._retry_queue),
                'dropped_tasks': self._dropped_count,
                'total_subscribed': self._total_subscriptions,
                'total_failures': self._total_failures,
                'failure_rate': self._total_failures / max(self._total_subscriptions, 1)
            }
    
    def on_tick(self, instrument_id: str, last_price: float, volume: float = 0) -> None:
        """Tick 路由入口：先查 metadata 决定期权/期货，再转发到 TTypeService。
        
        改造后不再每笔 Tick 先 parse_option/parse_future 再路由，
        而是先查 instrument_id -> internal_id -> meta，
        由 metadata 中的 type 字段决定走哪条路径。
        """
        if not instrument_id or last_price in (None, ''):
            return
        
        with self._tick_lock:
            if not self._bg_threads_started:
                self._start_background_threads()
            return self._on_tick_impl(instrument_id, last_price, volume)

    def _on_tick_impl(self, instrument_id: str, last_price: float, volume: float = 0) -> None:
        """on_tick内部实现（在_tick_lock内执行）"""
        normalized_id = self._strip_exchange_prefix(str(instrument_id).strip())
        
        diag_on = False
        try:
            from ali2026v3_trading.diagnosis_service import is_monitored_contract
            diag_on = is_monitored_contract(normalized_id)
        except Exception:
            logger.debug(f"[SubscriptionManagerV2] Failed to check monitored contract for {normalized_id}")
        if diag_on:
            now = time.time()
            with self._submgr_tick_accum_lock:
                self._submgr_tick_accum_count += 1
                if now - self._submgr_tick_last_output_time >= self._submgr_tick_summary_interval:
                    logging.info(f"[SUBMGR_TICK_ALL_SUMMARY] ticks={self._submgr_tick_accum_count} "
                                f"sample=({instrument_id} -> {normalized_id} P={last_price} V={volume})")
                    self._submgr_tick_accum_count = 0
                    self._submgr_tick_last_output_time = now
        
        try:
            # 优先从 ParamsService metadata 路由
            routed = False
            try:
                from ali2026v3_trading.params_service import get_params_service
                ps = get_params_service()
                # ✅ Group A收口：优先使用规范化ID查找，仅当两ID不同时才回退
                meta = ps.get_instrument_meta_by_id(normalized_id)
                if not meta and normalized_id != instrument_id:
                    meta = ps.get_instrument_meta_by_id(instrument_id)
                if meta:
                    inst_type = meta.get('type', '')
                    internal_id = meta.get('internal_id')
                    # ✅ P0修复：原子引用t_type_service，避免并发替换导致None引用
                    tts = self.t_type_service
                    if inst_type == 'option' and tts:
                        tts.on_option_tick(normalized_id, float(last_price), volume)
                        routed = True
                    elif inst_type == 'future' and tts:
                        # ✅ 两ID原则：传递 internal_id 而非 instrument_id
                        if internal_id is not None:
                            tts.on_future_instrument_tick(int(internal_id), float(last_price))
                        else:
                            logging.warning(f"[SubMgr] Missing internal_id for future {normalized_id}")
                        routed = True
            except Exception as e:
                err_key = str(e)
                if not hasattr(self, '_op_error_circuit'):
                    self._op_error_circuit = {}
                now = time.time()
                if err_key not in self._op_error_circuit:
                    self._op_error_circuit[err_key] = {'count': 0, 'first_seen': now, 'last_logged': 0}
                self._op_error_circuit[err_key]['count'] += 1
                if self._op_error_circuit[err_key]['count'] <= 3 or now - self._op_error_circuit[err_key]['last_logged'] > 300:
                    logger.error(f"[SubscriptionManagerV2] Operation failed: {e}", exc_info=True)
                    self._op_error_circuit[err_key]['last_logged'] = now
                elif self._op_error_circuit[err_key]['count'] % 1000 == 0:
                    logger.warning(f"[SubscriptionManagerV2] Operation failed '{err_key}' suppressed (count={self._op_error_circuit[err_key]['count']})")
            
            # metadata miss：配置文件未覆盖的合约tick，记录告警，不做回退路由
            # 设计约束：合约配置文件是订阅唯一来源，无增量注册/降级路由回退
            if not routed:
                should_warn = False
                with self._lock:
                    already_warned = False
                    for item in self._missing_metadata_warnings:
                        if item == normalized_id:
                            already_warned = True
                            break
                    
                    if not already_warned:
                        if len(self._missing_metadata_warnings) < self._missing_metadata_warning_limit:
                            self._missing_metadata_warnings.append(normalized_id)
                            should_warn = True
                    
                    if should_warn:
                        logger.warning(
                            "[SubscriptionManagerV2] Contract metadata not found for: %s. "
                            "Please ensure contract is properly registered.", normalized_id
                        )
                    else:
                        logger.debug(
                            "[SubscriptionManagerV2] Contract metadata not found for: %s (suppressed, "
                            "too many unique missing contracts)", normalized_id
                        )
                return
        except Exception as exc:
            logger.error("[SubscriptionManagerV2] Tick processing failed: %s - %s", normalized_id, exc)

    # ========================================================================
    # 订阅成功跟踪：用实际收到作为成功标准
    # ========================================================================
    
    @staticmethod
    def _extract_product(instrument_id: str) -> str:
        """从合约ID提取品种代码
        
        Examples:
            au2605C1032 -> AU
            al2605C25200 -> AL
            cu2605 -> CU
            HO2605-C-2800 -> HO
        """
        import re
        if not instrument_id:
            return ''
        # 匹配品种代码：字母前缀
        m = re.match(r'^([A-Za-z]+)', instrument_id)
        if m:
            return m.group(1).upper()
        return instrument_id[:2].upper() if len(instrument_id) >= 2 else instrument_id.upper()
    
    def record_subscription(self, instrument_ids: List[str]) -> None:
        """记录订阅合约列表（分母）"""
        with self._subscription_success_lock:
            now = time.time()
            products = set()
            for inst_id in instrument_ids:
                self._subscription_success['subscribe_time'][inst_id] = now
                product = self._extract_product(inst_id)
                if product:
                    products.add(product)
            self._subscription_success['total_subscribed'] = len(instrument_ids)
            self._subscription_success['subscribed_products'] = products
            self._subscription_success['total_products'] = len(products)
    
    def record_kline_received(self, instrument_id: str) -> None:
        """记录收到K线（分子：平台返回过K线的合约）"""
        with self._subscription_success_lock:
            if instrument_id not in self._subscription_success['kline_instruments']:
                self._subscription_success['kline_instruments'].add(instrument_id)
                self._subscription_success['kline_received'] += 1
                product = self._extract_product(instrument_id)
                if product:
                    self._subscription_success['kline_products'].add(product)
    
    def record_tick_received(self, instrument_id: str) -> None:
        """记录收到Tick（分子：平台推送过Tick的合约）"""
        with self._subscription_success_lock:
            if instrument_id not in self._subscription_success['tick_instruments']:
                self._subscription_success['tick_instruments'].add(instrument_id)
                self._subscription_success['tick_received'] += 1
                product = self._extract_product(instrument_id)
                if product:
                    self._subscription_success['tick_products'].add(product)
    
    def get_subscription_success_rate(self) -> Dict[str, Any]:
        """获取订阅成功率统计
        
        Returns:
            {
                'total_subscribed': 分母（合约数）,
                'total_products': 分母（品种数）,
                'kline_received': K线分子（合约数）,
                'tick_received': Tick分子（合约数）,
                'kline_products': K线分子（品种数）,
                'tick_products': Tick分子（品种数）,
                'kline_rate': K线成功率（合约维度）,
                'tick_rate': Tick成功率（合约维度）,
                'kline_product_rate': K线成功率（品种维度）,
                'tick_product_rate': Tick成功率（品种维度）,
            }
        """
        with self._subscription_success_lock:
            total = self._subscription_success['total_subscribed']
            total_products = self._subscription_success['total_products']
            kline_count = self._subscription_success['kline_received']
            tick_count = self._subscription_success['tick_received']
            kline_products = len(self._subscription_success['kline_products'])
            tick_products = len(self._subscription_success['tick_products'])
            
            kline_rate = kline_count / total if total > 0 else 0.0
            tick_rate = tick_count / total if total > 0 else 0.0
            kline_product_rate = kline_products / total_products if total_products > 0 else 0.0
            tick_product_rate = tick_products / total_products if total_products > 0 else 0.0
            
            all_subscribed = set(self._subscription_success['subscribe_time'].keys())
            kline_missing = list(all_subscribed - self._subscription_success['kline_instruments'])
            tick_missing = list(all_subscribed - self._subscription_success['tick_instruments'])
            
            kline_missing_products = list(self._subscription_success['subscribed_products'] - self._subscription_success['kline_products'])
            tick_missing_products = list(self._subscription_success['subscribed_products'] - self._subscription_success['tick_products'])
            
            return {
                'total_subscribed': total,
                'total_products': total_products,
                'kline_received': kline_count,
                'tick_received': tick_count,
                'kline_products': kline_products,
                'tick_products': tick_products,
                'kline_rate': kline_rate,
                'tick_rate': tick_rate,
                'kline_product_rate': kline_product_rate,
                'tick_product_rate': tick_product_rate,
                'kline_missing_count': len(kline_missing),
                'tick_missing_count': len(tick_missing),
                'kline_missing': kline_missing[:50],
                'tick_missing': tick_missing[:50],
                'kline_missing_products': kline_missing_products,
                'tick_missing_products': tick_missing_products,
            }
    
    def log_subscription_success_summary(self) -> None:
        """输出订阅成功率摘要日志"""
        stats = self.get_subscription_success_rate()
        
        logger.info("=" * 80)
        logger.info("[订阅成功率统计] 分母=%d合约 / %d品种", stats['total_subscribed'], stats['total_products'])
        logger.info("-" * 80)
        
        # 合约维度
        logger.info(
            "[合约维度] K线: %d/%d = %.1f%% | Tick: %d/%d = %.1f%%",
            stats['kline_received'], stats['total_subscribed'], stats['kline_rate'] * 100,
            stats['tick_received'], stats['total_subscribed'], stats['tick_rate'] * 100
        )
        
        # 品种维度
        logger.info(
            "[品种维度] K线: %d/%d = %.1f%% | Tick: %d/%d = %.1f%%",
            stats['kline_products'], stats['total_products'], stats['kline_product_rate'] * 100,
            stats['tick_products'], stats['total_products'], stats['tick_product_rate'] * 100
        )
        
        # 未收到数据的品种
        if stats['tick_missing_products']:
            logger.warning("[Tick未收到品种] %s", ', '.join(stats['tick_missing_products'][:20]))
        
        if stats['kline_missing_products']:
            logger.warning("[K线未收到品种] %s", ', '.join(stats['kline_missing_products'][:20]))
        
        # 未收到数据的合约样例
        if stats['tick_missing_count'] > 0 and stats['tick_missing']:
            logger.warning("[Tick未收到合约] 前20个:")
            for inst in stats['tick_missing'][:20]:
                logger.warning(f"  {inst}")
            if stats['tick_missing_count'] > 20:
                logger.warning(f"  ... 还有 {stats['tick_missing_count'] - 20} 个")
        
        logger.info("=" * 80)


# ========== 测试代码 ==========

if __name__ == "__main__":
    # 示例: 使用告警回调
    def alert_handler(count, msg):
        print(f"[ALERT] {msg}: {count}")
    
    config = SubscriptionConfig(
        alert_callback=alert_handler,
        alert_threshold=5,
        enable_wal=True,
        backoff_strategy="exponential"
    )
    
    # Mock DataManager
    class MockDataManager:
        def subscribe(self, inst_id, data_type):
            print(f"  Subscribing: {inst_id} ({data_type})")
        
        def unsubscribe(self, inst_id, data_type):
            print(f"  Unsubscribing: {inst_id} ({data_type})")
    
    # P2 Bug #39修复：使用正确的类名SubscriptionManager
    manager = SubscriptionManager(MockDataManager(), config)
    
    # 模拟订阅
    futures = ['RB2405', 'CU2406']
    options = {'RB2405': ['RB2405-C-3500', 'RB2405-P-3500']}
    
    success = manager.subscribe_all_instruments(futures, options)
    print(f"\nSubscription result: {success}")
    
    # 查看统计
    stats = manager.get_subscription_stats()
    print(f"\nStats: {json.dumps(stats, indent=2)}")
    
    manager.close()
