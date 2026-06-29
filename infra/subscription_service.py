# [M1-64] 订阅服务
# MODULE_ID: M1-118

from __future__ import annotations
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
from ali2026v3_trading.infra.serialization_utils import json_dumps
from ali2026v3_trading.infra.shared_utils import safe_float, normalize_instrument_id, CHINA_TZ
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

# 跨平台文件锁支持

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False
try:
    import msvcrt
    _HAS_MSVCRT = True
except ImportError:
    _HAS_MSVCRT = False
"""订阅管理器- 配置与工具函数"""

logger = get_logger(__name__)  # R9-5

def _acquire_file_lock(f):
    """EC-P1-05修复: 跨进程文件锁"""
    try:
        if _HAS_FCNTL:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        elif _HAS_MSVCRT:
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
    except (OSError, IOError) as e:
        logger.warning("[EC-P1-05] 文件锁获取失败 %s，继续执行但跨进程安全无保障", e)
def _release_file_lock(f):
    """EC-P1-05修复: 释放跨进程文件锁"""
    try:
        if _HAS_FCNTL:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        elif _HAS_MSVCRT:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
    except (OSError, IOError) as e:
        logger.warning("[EC-P1-05] 文件锁释放失败 %s", e)
def classify_registered_instruments(storage) -> Tuple[List[str], Dict[int, List[str]]]:
    """_underlying_future_id 对已注册合约做轻量分组，避免初始化阶段逐合约阻塞等待待"""
    futures_instruments: List[str] = []
    option_instruments: Dict[int, List[str]] = {}
    if not storage:
        return futures_instruments, option_instruments

    from ali2026v3_trading.config.params_service import get_params_service
    registered_ids = storage.get_registered_instrument_ids() or []
    logger.info("[InitServices] 已注册合约数据 %d", len(registered_ids))
    params_service = get_params_service()
    for inst_id in registered_ids:
        if SubscriptionInstrumentService.is_option(inst_id):
            try:
                meta = params_service.get_instrument_meta_by_id(inst_id) if params_service else None
                underlying_future_id = meta.get('underlying_future_id') if meta else None
                if not underlying_future_id:
                    logger.warning("[InitServices] 跳过缺少 underlying_future_id 的期权 %s", inst_id)
                    continue

                option_instruments.setdefault(int(underlying_future_id), []).append(inst_id)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
                logger.warning("[InitServices] 期权解析失败: %s - %s", inst_id, exc)
        else:
            futures_instruments.append(inst_id)
    return futures_instruments, option_instruments

# ========== EventBus 安全导入（从 event_bus 模块统一导入口=========

try:
    from ali2026v3_trading.infra.event_bus import get_global_event_bus, EventBus
    _HAS_EVENT_BUS = get_global_event_bus() is not None
except ImportError as e:
    logger.warning("[SubscriptionManagerV2] EventBus import failed: %s", e)  # R13-P2-LOG-01修复
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
            logger.debug("[SubscriptionManagerV2] Failed to get attribute %s: %s", k, e)  # R13-P2-LOG-01修复
            val = None
        if val not in (None, ''):
            return val

    return default

# ========== 配置对象 (替代硬编码 ==========

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

@dataclass(slots=True)
class SubscriptionConfig:
    """订阅管理器配置- 借鉴 OrderFlowConfig 设计"""

    # 重试策略
    max_retries: int = 3                          # 最大重试次数
    retry_base_delay: float = 1.0                 # 基础延迟(_
    retry_max_delay: float = 60.0                 # 最大延迟(秒)
    backoff_strategy: str = "exponential"         # 退避策略 exponential/linear/random_jitter

    # 批量写入
    db_batch_size: int = 100                      # DuckDB批量大小
    throttle_window: float = 1.0                  # 节流窗口(_

    # 异步落盘 (WAL保护)
    enable_wal: bool = True                       # 启用WAL文件
    wal_path: str = os.path.join(_MODULE_DIR, "subscription_wal.jsonl")
    wal_max_size_mb: int = 50                     # WAL最大文件大。MB)
    wal_max_rotations: int = 3                    # WAL轮转数量
    recover_on_start: bool = True                 # 启动时恢复WAL

    # 重试队列
    retry_queue_max_size: int = 500               # 重试队列最大容灾
    retry_interval: float = 5.0                   # 重试线程间隔(_
    max_event_age_seconds: int = 1800             # 事件最大存活时常30分钟)

    # 清理线程配置 (借鉴 OrderFlowAnalyzer)
    enable_cleanup_thread: bool = True            # 启用独立清理线程
    cleanup_interval: float = 60.0                # 清理检查间隔离

    # R14-P1-LOG-13修复: alert_callback默认非None(空日志回撤
    alert_callback: Optional[Callable[[int, str], None]] = None  # 初始化时设为期default_alert_callback
    alert_threshold: int = 10                     # 累计失败超过此值触发告警
    failure_rate_threshold: float = 0.1           # 失败率超时0%触发告警

    # 监控
    enable_stats: bool = True                     # 启用统计信息
    stats_report_interval: float = 60.0           # 统计报告间隔(_

# ========== 企业级订阅管理器 ==========

"""订阅管理器- 合约解析 (_SubscriptionInstrumentMixin)"""

class SubscriptionInstrumentService:
    @staticmethod
    def _strip_exchange_prefix(instrument_id: str) -> str:
        """剥离交易所前缀（统一使用正则）'
        Args:
            instrument_id: 合约ID（可能带交易所前缀，如 'CFFEX.IF2603'_
        Returns:
            str: 纯净的合约ID（如 'IF2603'_
        """

        # 直接使用正则提取合约ID，避免二次标准化
        import re
        match = re.search(r'([A-Za-z]+\d+.*?)$', instrument_id)
        return match.group(1) if match else instrument_id

    @staticmethod
    def _normalize_product_code(product: str) -> str:
        """标准化产品代码，用于内部比较和分析
        Args:
            product: 产品代码字符串
        Returns:
            str: 标准化的产品代码（SHFE品种小写，其他大写）'
        """
        if not product:
            return ''

        return str(product)

    @staticmethod
    def parse_future(instrument_id: str) -> Dict[str, Any]:
        """解析期货合约"""
        clean_id = SubscriptionInstrumentService._strip_exchange_prefix(instrument_id)
        match = re.match(r'^([A-Za-z]+)(\d{3,4})$', clean_id)
        if not match:
            raise ValueError(f"无法解析期货：{instrument_id}")

        raw_product = match.group(1)
        year_month = match.group(2)
        if len(year_month) == 3:
            year_month = '2' + year_month

        # 使用标准化函数
        product = SubscriptionInstrumentService._normalize_product_code(raw_product)
        return {'product': product, 'year_month': year_month}

    @staticmethod
    def is_option(instrument_id: str) -> bool:
        """判断是否为期期"""
        try:
            SubscriptionInstrumentService.parse_option(normalize_instrument_id(instrument_id))
            return True

        except (ValueError, Exception):
            return False

    @staticmethod
    def parse_option(instrument_id: str) -> Dict[str, Any]:
        r"""
        解析期权合约(支持标准格式、连字符格式、紧凑格式、MS迷你格式)
        支持格式:
        - 标准: CU2603C5000, IO2606C4000
        - 连字符: CU2603-C-5000
        - CZCE迷你: SR607MSC4700, SR607MSP4700 (MSC=Mini Call, MSP=Mini Put)
        - DCE迷你: c2607-MS-C-2040, m2607-MS-P-2400
        """
        clean_id = SubscriptionInstrumentService._strip_exchange_prefix(instrument_id)

        ms_compact = re.match(r'^([A-Za-z]+)(\d{3,4})MS([CP])(\d+)$', clean_id)
        if ms_compact:
            product = ms_compact.group(1)
            year_month_raw = ms_compact.group(2)
            option_type = ms_compact.group(3)
            strike_price = float(ms_compact.group(4))
            year_month = SubscriptionInstrumentService._normalize_option_year_month(year_month_raw)
            return {
                'product': product,
                'year_month': year_month,
                'option_type': option_type,
                'strike_price': strike_price,
                'format': 'ms_compact',
            }

        ms_dash = re.match(r'^([A-Za-z]+)(\d{3,4})-MS-([CP])-?(\d+(?:\.\d+)?)$', clean_id)
        if ms_dash:
            product = ms_dash.group(1)
            year_month_raw = ms_dash.group(2)
            option_type = ms_dash.group(3)
            strike_price = float(ms_dash.group(4))
            year_month = SubscriptionInstrumentService._normalize_option_year_month(year_month_raw)
            return {
                'product': product,
                'year_month': year_month,
                'option_type': option_type,
                'strike_price': strike_price,
                'format': 'ms_dash',
            }

        match = re.match(r'^([A-Za-z]+)(\d{3,4})-?([CP])-?(\d+(?:\.\d+)?)$', clean_id)
        if match:
            product = match.group(1)
            year_month_raw = match.group(2)
            option_type = match.group(3)
            strike_price = float(match.group(4))
            year_month = SubscriptionInstrumentService._normalize_option_year_month(year_month_raw)
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
        """归一化期权年）"""
        normalized = normalize_instrument_id(year_month_raw)
        if len(normalized) == 4:
            return normalized

        if len(normalized) != 3 or not normalized.isdigit():
            raise ValueError(f"非法期权月份编码：{year_month_raw}")

        current_year = datetime.now(CHINA_TZ).year % 100
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

    # _接口唯一：classify_instruments唯一实现，query_service两处已委托此。'
    @staticmethod
    def classify_instruments(instrument_ids: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
        """分类合约
        options_dict _key 统一。canonical underlying 标识 (product+year_month)_
        _'IO2605', 'al2605'，与 ParamsService._extract_canonical_underlying 输出一致。'
        """
        futures_list = []
        options_dict = {}
        for inst_id in instrument_ids:
            try:
                parsed = SubscriptionInstrumentService.parse_option(inst_id)

                # 统一 key 语义。product+year_month
                underlying = f"{parsed['product']}{parsed['year_month']}"
                if underlying not in options_dict:
                    options_dict[underlying] = []
                options_dict[underlying].append(inst_id)
            except ValueError:
                futures_list.append(inst_id)
        return futures_list, options_dict

    # ========================================================================
    # 订阅核心逻辑 (增强强
    # ========================================================================
_SubscriptionInstrumentMixin = SubscriptionInstrumentService

"""订阅管理器- WAL管理 (_SubscriptionWALMixin)"""

logger = get_logger(__name__)  # R9-5

class SubscriptionWALService:
    def __init__(self, facade=None, config=None):
        self._facade = facade
        self._config = config if config is not None else SubscriptionConfig()
        self._retry_queue: deque = deque(maxlen=self._config.retry_queue_max_size)
        self._retry_lock = threading.Lock()
        self._wal_lock = threading.Lock()
        self._bg_threads_started = False
        self._stop_async = threading.Event()
        self._stop_retry = threading.Event()
        self._stop_cleanup = threading.Event()
        self._stop_watchdog = threading.Event()
        self._dropped_count = 0
        self._wal_file = None
        self._async_thread = None
        self._cleanup_thread = None
        self._retry_thread = None
        self._watchdog_thread = None
        self._last_alert_count = 0
        self._lock = threading.Lock()
        self._total_failures = 0
        self._total_subscriptions = 0
        self._wal_queue = deque()
        self._last_tick_time = {}
        self._tick_stale_instruments = {}
        self._watchdog_interval_sec = 5.0
        self._tick_timeout_sec = 10.0
        self._event_bus = None

    def __getattr__(self, name):
        # 递归保护: 防止facade双向委托导致无限递归
        if '__getattr__recursing' in self.__dict__:
            raise AttributeError(name)

        self.__dict__['__getattr__recursing'] = True
        try:
            _facade = self.__dict__.get('_facade')
            if _facade is not None:
                try:
                    return getattr(_facade, name)

                except AttributeError:
                    pass
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        finally:
            self.__dict__.pop('__getattr__recursing', None)

    def _init_wal(self):
        """初始化WAL文件"""
        if not self._config.enable_wal:
            return

        try:
            _wal_dir = os.path.dirname(os.path.abspath(self._config.wal_path))
            os.makedirs(_wal_dir, exist_ok=True)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[SubscriptionManagerV2] WAL directory creation failed: %s", e)
            self._wal_file = None
            return

        try:
            self._wal_file = open(self._config.wal_path, 'a', encoding='utf-8', buffering=1)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[SubscriptionManagerV2] WAL file open failed: %s", e)
            self._wal_file = None
            return

        try:
            logger.info("[SubscriptionManagerV2] WAL enabled: %s", self._config.wal_path)
            atexit.register(self._close_wal)
            if self._config.recover_on_start:
                self._recover_from_wal()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.error("[SubscriptionManagerV2] WAL post-init failed: %s", e)
            self._close_wal()
            return

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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[SubscriptionManagerV2] WAL rotation failed: %s", e)

    # R15-P0-RES-10修复: WAL心跳方法

    def _wal_heartbeat(self):
        """R15-P0-RES-10修复: 定期写入心跳记录，重启时检测异常退避"""
        if not self._config.enable_wal:
            return

        try:
            self._write_wal_async({'type': 'heartbeat', 'ts': time.time()})
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.debug("[SubscriptionManagerV2] R15-P0-RES-10: _wal_heartbeat failed: %s", e)

    def _check_last_heartbeat(self):
        """R15-P0-RES-10修复: 重启时检测最后心跳时间判断异常退避"""
        if not self._config.enable_wal or not os.path.exists(self._config.wal_path):
            return False

        try:
            last_heartbeat_ts = None
            with open(self._config.wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        if record.get('type') == 'heartbeat':
                            last_heartbeat_ts = record.get('ts')
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[_sub_wal] 数据解析降级: %s", _r3_err)
                        continue

            if last_heartbeat_ts is not None:
                gap = time.time() - last_heartbeat_ts
                if gap > 120:
                    logger.warning("[SubscriptionManagerV2] R15-P0-RES-10: 检测到异常退避心跳间隔%.0f_120)", gap)
                    return True

            return False

        except (ValueError, KeyError, TypeError, AttributeError) as e:
            logger.debug("[SubscriptionManagerV2] R15-P0-RES-10: _check_last_heartbeat failed: %s", e)
            return False

    def _write_wal_async(self, record: Dict[str, Any]):  # [R22-P2-TS27]
        """异步写入WAL - 使用队列缓冲
        EC-P1-05: 已有进程级锁保护 - self._wal_lock (threading.Lock)_
        在。write_wal_async和。wal_writer_loop中均使用with self._wal_lock保护。'
        满足进程内线程安全；跨进程场景需依赖文件系统计待规约束
        """
        if not self._config.enable_wal:
            return

        with self._wal_lock:
            try:
                # 直接写入内存队列,由后台线程批量落。
                # R13-P2-LOG-08修复: 已在。_init__中显式初始化化wal_queue，无需hasattr检查
                self._wal_queue.append(record)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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

                    # R15-P1-DATA-07修复: WAL读取时验证CRC32校验验
                    _crc_verify_ok = True
                    if '\t#crc:' in line:
                        _parts = line.rsplit('\t#crc:', 1)
                        _data_part = _parts[0]
                        _crc_hex = _parts[1]
                        try:
                            import binascii as _ba
                            _expected = _ba.crc32(_data_part.encode('utf-8')) & 0xFFFFFFFF
                            if f'{_expected:08x}' != _crc_hex:
                                logger.warning("R15-P1-DATA-07: WAL CRC校验失败 line=%s", _data_part[:80])
                                _crc_verify_ok = False
                            line = _data_part
                        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                            logging.debug("[R3-L2] _recover_from_wal suppressed: %s", _r3_err)
                            pass
                    try:
                        record = json.loads(line)
                        if not _crc_verify_ok:
                            logger.warning("R15-P1-DATA-07: CRC校验失败的记录被跳过: type=%s", record.get('type'))
                            continue

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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        logger.error("[SubscriptionManagerV2] Failed to process WAL record: %s", e)  # R13-P2-LOG-01修复
            logger.info("[SubscriptionManagerV2] Recovered %d subscription tasks from WAL", recovered)

            # 恢复后重命名文件（如果失败则保留原文件）'
            try:
                # Windows上可能因文件锁定导致重命名失败，尝试多种方式
                recovered_path = os.path.splitext(self._config.wal_path)[0] + ".recovered"  # EC-P2-01: 用os.path.splitext替代硬拼。'
                if os.path.exists(recovered_path):
                    try:
                        os.remove(recovered_path)
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                        pass
                os.rename(self._config.wal_path, recovered_path)
            except OSError as e:
                # Windows上文件锁定时重命名失败是常见情况，不影响功能
                if getattr(e, 'winerror', None) == 32:  # ERROR_SHARING_VIOLATION
                    logger.warning("[SubscriptionManagerV2] WAL file locked by another process, skipping rename")  # R13-P2-LOG-01修复
                else:
                    logger.warning("[SubscriptionManagerV2] WAL rename failed: %s", e)  # R13-P2-LOG-01修复
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.warning("[SubscriptionManagerV2] WAL rename failed: %s", e)  # R13-P2-LOG-01修复
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.error("[SubscriptionManagerV2] WAL recovery failed: %s", e)

    def _close_wal(self):
        """P1 Bug #36修复：关闭WAL文件，防止进程退出时文件泄漏"""
        if self._wal_file:
            try:
                self._wal_file.flush()
                self._wal_file.close()
                logger.info("[SubscriptionManagerV2] WAL file closed")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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

        # R27-P0-DR-05修复: 启动tick看门狗线程
        self._stop_watchdog.clear()
        self._watchdog_thread = threading.Thread(
            target=self._tick_watchdog_loop,
            name="TickWatchdog[shared-service]",
            daemon=True
        )
        self._watchdog_thread.start()
        logger.info("[R27-P0-DR-05] Tick看门狗线程已启动(interval=%.1fs, timeout=%.1fs)",
                     self._watchdog_interval_sec, self._tick_timeout_sec)

    def ensure_background_threads(self) -> None:
        """确保后台线程处于运行状态，_stop 后重启复用例"""
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

    def stop_background_threads(self, join_timeout: float = 2.0, silent: bool = False) -> None:
        """优雅停止所有后台线程
        Args:
            join_timeout: 线程等待超时（秒。'
            silent: 为True时抑制日志输出（用于atexit清理理
        """
        self._stop_async.set()
        self._stop_retry.set()
        self._stop_cleanup.set()
        self._stop_watchdog.set()  # R27-P0-DR-05修复: 停止看门面
        if not silent:
            logger.info(
                "[SubscriptionManagerV2][owner_scope=shared-service][source_type=shared-service] "
                "Stopping background threads (close_reason=strategy-stop, close_policy=graceful-join)"
            )

        # R27-P1修复: join列表补充看门狗线程，防止策略重启时旧看门狗不停止
        for t in [self._async_thread, self._retry_thread, self._cleanup_thread, self._watchdog_thread]:
            if t and t.is_alive():
                t.join(timeout=join_timeout)
                if not silent:
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
                    # R13-P2-LOG-08修复: 已在。_init__中显式初始化化wal_queue，无需hasattr检查
                    while self._wal_queue and len(batch) < 50:
                        batch.append(self._wal_queue.popleft())

                # 批量写入文件
                if batch and self._wal_file:
                    self._rotate_wal()
                    _acquire_file_lock(self._wal_file)
                    try:
                        for record in batch:
                            # R15-P1-DATA-07修复: WAL写入添加CRC32校验验
                            _line = json_dumps(record)
                            import binascii as _ba
                            _crc = _ba.crc32(_line.encode('utf-8')) & 0xFFFFFFFF
                            self._wal_file.write(f'{_line}\t#crc:{_crc:08x}\n')
                        self._wal_file.flush()
                        os.fsync(self._wal_file.fileno())  # R14-P1-LOG-15修复: WAL写入加fsync确保持久化
                    finally:
                        _release_file_lock(self._wal_file)
                time.sleep(0.05)  # 50ms批次间隔
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, IOError) as e:
                logger.error("[SubscriptionManagerV2] WAL writer error: %s", e)
                time.sleep(0.1)

    def _retry_loop(self):
        """重试线程 - 定期处理失败任务"""
        while not self._stop_retry.is_set():
            time.sleep(self._config.retry_interval)
            try:
                self._process_retry_queue()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.error("[SubscriptionManagerV2] Retry loop error: %s", e)

    # R27-P0-DR-05修复: tick看门狗循环节检测tick断流并告警

    def _tick_watchdog_loop(self):
        """定期检查所有已订阅合约的tick是否断流，超时则触发告警"""
        while not self._stop_watchdog.is_set():
            self._stop_watchdog.wait(timeout=self._watchdog_interval_sec)
            if self._stop_watchdog.is_set():
                break

            try:
                now = time.time()
                stale_instruments = []
                with self._lock:
                    for inst_id, last_t in self._last_tick_time.items():
                        if now - last_t > self._tick_timeout_sec:
                            stale_instruments.append((inst_id, now - last_t))

                    # 清理已恢复的合约
                    self._tick_stale_instruments = {
                        k: v for k, v in self._tick_stale_instruments.items()
                        if now - self._last_tick_time.get(k, now) > self._tick_timeout_sec
                    }
                for inst_id, stale_sec in stale_instruments:
                    if inst_id not in self._tick_stale_instruments:
                        self._tick_stale_instruments[inst_id] = now
                        logger.warning(
                            "[R27-P0-DR-05] Tick断流告警: instrument=%s _.1f秒无tick(阈值%.1fs)",
                            inst_id, stale_sec, self._tick_timeout_sec
                        )
                        if hasattr(self, '_event_bus') and self._event_bus is not None:
                            try:
                                self._event_bus.publish(type('TickStaleEvent', (), {
                                    'instrument_id': inst_id,
                                    'stale_seconds': stale_sec,
                                    'timeout_seconds': self._tick_timeout_sec,
                                })())
                            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                                logging.debug("[R3-L2] unknown suppressed: %s", _r3_err)
                                pass
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.error("[R27-P0-DR-05] 看门狗循环异常 %s", e)

    def _atexit_cleanup(self):
        """进程退出清理- 静默模式避免测试退出时I/O错误"""
        try:
            # 在atexit时，stdout/stderr可能已关闭，使用静默模式
            import sys
            if sys.stderr is None or sys.stderr.closed:
                return

            self.close(silent=True)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            # atexit阶段忽略所有错误，避免进程退出异常
            logging.debug("[_sub_wal] 线程操作降级: %s", _r3_err)

    def close(self, silent: bool = False):
        """关闭所有资源
        Args:
            silent: 为True时抑制日志输出（用于atexit清理理
        """
        self.stop_background_threads(join_timeout=2.0, silent=silent)
        if self._wal_file:
            try:
                self._wal_file.flush()
                self._wal_file.close()
                self._wal_file = None
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                if not silent:
                    logger.error("[SubscriptionManagerV2] WAL close failed: %s", e, exc_info=True)
        if not silent:
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

    def _enqueue_for_retry(self, task_data: Dict[str, Any], retry_count: int = 0):  # [R22-P2-TS28]
        """将失败任务加入重试队列"""
        now = time.time()
        delay = self._calc_backoff_delay(retry_count)

        # 兼容 facade 模式: _facade 上被显式覆盖 _retry_queue，则使用 facade 的实现
        _facade = self.__dict__.get('_facade')
        if _facade is not None and '_retry_queue' in _facade.__dict__:
            _retry_queue = _facade.__dict__['_retry_queue']
        else:
            _retry_queue = self._retry_queue
        with self._retry_lock:
            if len(_retry_queue) >= self._config.retry_queue_max_size:
                logger.error("[SubscriptionManagerV2] Retry queue full, dropping task")
                self._dropped_count += 1

                # WAL保护: 溢出任务也落的防止永久丢失
                self._write_wal_async({
                    'type': 'queue_overflow',
                    'instrument_id': task_data.get('instrument_id'),
                    'task_type': task_data.get('type'),
                    'timestamp': now
                })
                self._check_alert()
                return

            next_retry = now + delay
            _retry_queue.append((task_data, retry_count + 1, next_retry, now))

            # 写入WAL
            self._write_wal_async({
                'type': 'retry_enqueue',
                'instrument_id': task_data.get('instrument_id'),
                'retry_count': retry_count + 1,
                'timestamp': now
            })

    def _process_retry_queue(self):
        """处理重试队列 (已移除过期检查由独立清理线程负载"""
        if not self._retry_queue:
            return

        now = time.time()
        success_count = 0
        still_pending = []
        with self._retry_lock:
            while self._retry_queue:
                task, count, next_time, enq_time = self._retry_queue[0]

                # 检查是否到达重试时常
                if next_time > now:
                    break

                self._retry_queue.popleft()

                # 执行重试
                try:
                    task_type = task.get('type')
                    if task_type == 'subscribe':
                        self._retry_subscribe(task)
                        success_count += 1
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logger.error("[SubscriptionManagerV2] Retry failed: %s", e)
                    if count < self._config.max_retries:
                        still_pending.append((task, count, now + self._calc_backoff_delay(count), enq_time))
                    else:
                        logger.error("[SubscriptionManagerV2] Task dropped after %d retries", count)
                        self._dropped_count += 1
                        self._total_failures += 1

                        # WAL保护: 最终丢弃的任务也落。'
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
            # still_pending是元组列表 (task, count, next_retry_time, enq_time)
            # 按next_retry_time（索的）排序，最早重试的排前。'
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

    def _retry_subscribe(self, task: Dict[str, Any]):  # [R22-P2-TS29]
        instrument_id = task.get('instrument_id')
        data_type = task.get('data_type', 'tick')
        logger.info("[SubscriptionManagerV2] Retrying subscribe: %s", instrument_id)
        self._do_subscribe(instrument_id, data_type)

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

                    # WAL保护: 清理事件也落的可追单
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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.error("[SubscriptionManagerV2] Cleanup error: %s", e)

    # ========================================================================
    # 告警回调集成
    # ========================================================================

    def _check_alert(self):
        """检查是否触发告警（_P1修复：加锁保护共享计数器从 # R17-P2-DOC-05"""
        if not self._config.alert_callback:
            return

        # _加锁读取共享计数据
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
_SubscriptionWALMixin = SubscriptionWALService

"""订阅管理器- 核心订阅逻辑 (_SubscriptionCoreMixin)"""

logger = get_logger(__name__)  # R9-5

class SubscriptionCoreService:
    def __init__(self, facade=None):
        self._facade = facade
        self.data_manager = getattr(facade, 'data_manager', None) if facade else None
        self._subscriptions: Dict[str, Any] = {}
        self._lock = threading.Lock()
        # tick 去重与统计
        self._tick_dedup_drop_count = 0
        self._last_tick_seq: Dict[str, int] = {}
        self._last_tick_seq_ts: Dict[str, float] = {}
        self._shard_locks: Dict[str, threading.Lock] = {}
        self._shard_locks_guard = threading.Lock()
        # tick 累积统计（监控合约）
        self._submgr_tick_accum_lock = threading.Lock()
        self._submgr_tick_accum_count = 0
        self._submgr_tick_last_output_time = 0.0
        self._submgr_tick_summary_interval = 60.0
        # metadata 缺失告警
        self._missing_metadata_warnings: List[str] = []
        self._missing_metadata_warning_limit = 200
        # 订阅成功跟踪
        self._subscription_success_lock = threading.Lock()
        self._subscription_success: Dict[str, Any] = {
            'subscribe_time': {},
            'total_subscribed': 0,
            'subscribed_products': set(),
            'total_products': 0,
            'kline_instruments': set(),
            'kline_received': 0,
            'kline_products': set(),
            'tick_instruments': set(),
            'tick_received': 0,
            'tick_products': set(),
        }
        # 错误熔断
        self._op_error_circuit: Dict[str, Any] = {}
        # 统计
        self._retry_queue: List[Any] = []
        self._dropped_count = 0
        self._total_subscriptions = 0
        self._total_failures = 0
        # t_type_service 引用（由外部注入或通过facade委托）
        self.t_type_service = None

    def _get_shard_lock(self, instrument_id: str) -> threading.Lock:
        """获取分片锁（按 instrument_id 哈希分片，减少锁争用）"""
        shard_key = instrument_id
        with self._shard_locks_guard:
            lock = self._shard_locks.get(shard_key)
            if lock is None:
                lock = threading.Lock()
                self._shard_locks[shard_key] = lock
            return lock

    def __getattr__(self, name):
        # 递归保护: 防止facade双向委托导致无限递归
        if '__getattr__recursing' in self.__dict__:
            raise AttributeError(name)

        self.__dict__['__getattr__recursing'] = True
        try:
            _facade = self.__dict__.get('_facade')
            if _facade is not None:
                try:
                    return getattr(_facade, name)

                except AttributeError:
                    pass
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        finally:
            self.__dict__.pop('__getattr__recursing', None)

    def _do_subscribe(self, instrument_id: str, data_type: str) -> None:
        """
        统一订阅方法（内部使用）'
        _序号122修复：统一为data_manager.subscribe，删除除platform_subscribe回退
        Args:
            instrument_id: 合约ID
            data_type: 数据类型 ('tick', 'kline'_
        """

        # R13-P1-API-02修复: data_manager为None时入队等待，避免bind_platform_apis()前崩溃
        if self.data_manager is None:
            logger.warning("[SubscriptionManagerV2] data_manager未绑定，订阅请求入队等待: %s", instrument_id)
            self._enqueue_pending_subscription(instrument_id, data_type)
            return

        if not self._is_subscribe_ready():
            logger.warning("[SubscriptionManagerV2] subscribe API未就绪，订阅请求入队等待: %s", instrument_id)
            self._enqueue_pending_subscription(instrument_id, data_type)
            return

        subscribe_method = getattr(self.data_manager, 'subscribe', None)
        if subscribe_method and callable(subscribe_method):
            result = subscribe_method(instrument_id, data_type)
            if result is None and not self._is_subscribe_ready():
                logger.warning("[SubscriptionManagerV2] subscribe API调用未生效，订阅请求回队等待: %s", instrument_id)
                self._enqueue_pending_subscription(instrument_id, data_type)
        else:
            raise AttributeError("InstrumentDataManager 缺少 subscribe 方法")

    def _enqueue_pending_subscription(self, instrument_id: str, data_type: str) -> None:
        if not hasattr(self, '_pending_subscriptions'):
            self._pending_subscriptions = []
        task = {'instrument_id': instrument_id, 'data_type': data_type}
        if task not in self._pending_subscriptions:
            self._pending_subscriptions.append(task)

    def _is_subscribe_ready(self) -> bool:
        if self.data_manager is None:
            return False
        subscribe_method = getattr(self.data_manager, 'subscribe', None)
        if not callable(subscribe_method):
            return False
        is_bound_method = getattr(self.data_manager, 'is_subscribe_api_bound', None)
        if callable(is_bound_method):
            try:
                return bool(is_bound_method())
            except Exception:
                return False
        return True

    def bind_data_manager(self, data_manager: Any) -> None:
        self.data_manager = data_manager
        self._drain_pending_subscriptions()

    def _drain_pending_subscriptions(self) -> None:
        if not self._is_subscribe_ready():
            return
        pending = list(getattr(self, '_pending_subscriptions', []) or [])
        if not pending:
            return
        self._pending_subscriptions = []
        for task in pending:
            try:
                self._do_subscribe(task.get('instrument_id'), task.get('data_type', 'tick'))
            except Exception as exc:
                logger.warning("[SubscriptionManagerV2] Pending subscribe replay failed: %s - %s", task.get('instrument_id'), exc)
                self._enqueue_pending_subscription(task.get('instrument_id'), task.get('data_type', 'tick'))

    def set_t_type_service(self, t_type_service: Any) -> None:
        """注入TTypeService"""
        self.t_type_service = t_type_service
        logger.info("[SubscriptionManagerV2] TTypeService injected")

    def _ensure_instruments_loaded(self) -> bool:
        """
        确保合约数据已从配置文件加载到数据库（启动时必须完成交
        ⚠️ 关键设计量
        - 合约元数据必须在 Storage 初始化前从配置文件完整加载
        - 这个检查只验证配置是否已加载，不负责实际加载
        - 实际加载器strategy_core_service.on_init() 调用 ensure_products_with_retry 完成
        - 如果配置未加载，返回 False 触发上层重试
        - 只有策略正常运行后，才可能从平台进行增量更新
        Returns:
            bool: 配置是否已加载完成
        """
        try:
            from ali2026v3_trading.data.data_service import get_data_service
            ds = get_data_service()

            # 检查是否标记为已加载
            if getattr(ds, '_products_loaded', False):
                logger.debug("[SubscriptionManagerV2] Products already marked as loaded")
                return True

            # 检查数据库是否有合约数据（配置加载的结果）'
            futures_count = ds.query("SELECT COUNT(*) as cnt FROM futures_instruments").to_pylist()[0]['cnt']
            options_count = ds.query("SELECT COUNT(*) as cnt FROM option_instruments").to_pylist()[0]['cnt']
            if futures_count > 0 or options_count > 0:
                logger.debug(
                    f"[SubscriptionManagerV2] Database has instrument data: "
                    f"futures={futures_count}, options={options_count}"
                )

                # 标记为已加载（如果还没标记）'
                ds._products_loaded = True
                # P1-9c修复：确保ParamsService缓存也同步加载
                try:
                    from ali2026v3_trading.config.params_service import get_params_service
                    _ps = get_params_service()
                    if not _ps._instrument_id_to_internal_id:
                        _ps.load_caches_from_db(ds)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _ps_err:
                    logger.warning("[SubscriptionManagerV2] ParamsService缓存同步加载失败: %s", _ps_err)
                return True

            # 数据库为空，说明配置尚未加载
            logger.warning(
                "[SubscriptionManagerV2] Instrument database is empty - "
                "ensure_products_with_retry must be called first from strategy_core_service.on_init()"
            )
            return False

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.error("[SubscriptionManagerV2._ensure_instruments_loaded] Error: %s", e)  # R13-P2-LOG-01修复
            return False

    def subscribe_all_instruments(self, futures_list: List[str],
                                   options_dict: Dict[str, List[str]]) -> int:
        """
        全量订阅 (增强强- 带重试和WAL保护)
        Returns:
            bool: 是否全部成功
        """

        # RES-P2-09: 合约订阅容量检查（告警不阻断，确保全量订阅）
        from ali2026v3_trading.config.config_params import CAPACITY_LIMITS
        _max_instruments = CAPACITY_LIMITS.get('max_instruments', 5000)
        _total_instruments = len(futures_list) + sum(len(opts) for opts in options_dict.values())
        if _total_instruments >= _max_instruments:
            logging.warning("[RES-P2-09] 合约订阅超过建议上限: %d/%d，继续订阅但需关注性能", _total_instruments, _max_instruments)

        # 空列表检查：无合约时直接返回0，避免后续空转
        if not futures_list and not options_dict:
            logger.warning("[SubscriptionManagerV2] No instruments to subscribe (futures_list and options_dict are empty)")
            return 0

        started_at = time.perf_counter()
        total_count = len(futures_list) + sum(len(opts) for opts in options_dict.values())
        self._total_subscriptions = total_count
        self.ensure_background_threads()

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

                # _环节1: 订阅成功探针
                from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                DiagnosisProbeManager.on_subscribe(inst_id, 'future', True)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                logger.error("[SubscriptionManagerV2] Subscribe failed: %s - %s", inst_id, e)
                failed_tasks.append({
                    'type': 'subscribe',
                    'instrument_id': inst_id,
                    'data_type': 'tick',
                    'error': str(e)
                })

                # _环节1: 订阅失败探针
                from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                DiagnosisProbeManager.on_subscribe(inst_id, 'future', False, str(e))

        # 订阅期权
        for underlying, option_ids in options_dict.items():
            try:
                # 握手
                if option_ids:
                    logger.debug("[SubscriptionManagerV2] Handshake removed: all subscriptions via _do_subscribe only")

                # 订阅期权合约
                for opt_id in option_ids:
                    try:
                        self._subscribe_single_with_retry(opt_id, 'tick')
                        self._subscribe_single_with_retry(opt_id, 'kline_1min')
                        success_count += 1

                        # _环节1: 期权订阅成功探针
                        from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                        DiagnosisProbeManager.on_subscribe(opt_id, 'option', True)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                        logger.error("[SubscriptionManagerV2] Option subscribe failed: %s - %s", opt_id, e)
                        failed_tasks.append({
                            'type': 'subscribe',
                            'instrument_id': opt_id,
                            'data_type': 'tick',
                            'error': str(e)
                        })

                        # _环节1: 期权订阅失败探针
                        from ali2026v3_trading.infra.health_monitor import DiagnosisProbeManager
                        DiagnosisProbeManager.on_subscribe(opt_id, 'option', False, str(e))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
                # R13-P1-API-05修复: 期权批次订阅失败时返回False阻断，而非仅log
                logger.error("[SubscriptionManagerV2] Option batch failed: %s - %s", underlying, e)
                return False

        # P1 Bug #38修复：累加失败计数，而非覆盖
        self._total_failures += len(failed_tasks)

        # P1-8修复：记录订阅合约列表（分母），使订阅成功率统计可用
        _all_instrument_ids = list(futures_list)
        for _opt_ids in options_dict.values():
            _all_instrument_ids.extend(_opt_ids)
        if _all_instrument_ids:
            self.record_subscription(_all_instrument_ids)

        # 发布完成事件
        if _HAS_EVENT_BUS and get_global_event_bus and success_count > 0:
            try:
                eb = get_global_event_bus()
                from ali2026v3_trading.infra.event_bus import SubscriptionCompletedEvent
                eb.publish(SubscriptionCompletedEvent(success_count, self._total_failures))
                logger.info("[SubscriptionManagerV2] Published completion event")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            # 加入重试队列
            self._enqueue_for_retry({
                'type': 'subscribe',
                'instrument_id': instrument_id,
                'data_type': data_type
            })
            logger.warning("[SubscriptionManagerV2] 订阅失败已入重试队列: %s (%s)", instrument_id, data_type)

    # ========================================================================
    # 查询接口
    # ========================================================================

    def unsubscribe_all(self) -> bool:
        """取消所有订单"""
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
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
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

    def on_tick(self, instrument_id: str, last_price: float, volume: float = 0,
                is_replay: bool = False) -> None:
        """Tick 路由入口：先。metadata 决定期权/期货，再转发送TTypeService_
        改造后不再每笔 Tick _parse_option/parse_future 再路由，
        而是先查 instrument_id -> internal_id -> meta_
        _metadata 中的 type 字段决定走哪条路径径
        R23-ID-04-FIX: 添加is_replay参数，回放tick直接跳过处理
        """
        if is_replay:
            self._tick_dedup_drop_count += 1
            return

        # R24-P0-IV-02修复: volume NaN/Inf/负值过滤
        import math as _math
        if not isinstance(volume, (int, float)) or _math.isnan(volume) or _math.isinf(volume) or volume < 0:
            volume = 0

        # R25-P2-IV-ext修复: last_price NaN/Inf/负值过滤（防止绕过滤process_tick直接调用时源头污染）'
        if (not isinstance(last_price, (int, float))
                or _math.isnan(last_price) or _math.isinf(last_price) or last_price <= 0):
            return

        if not instrument_id or last_price in (None, ''):
            return

        # R15-P0-PERF-01修复: 使用分片锁替代全局。tick_lock
        with self._get_shard_lock(instrument_id):
            # R15-P0-DATA-03修复: tick去重，基于(instrument_id, last_price, volume)三元组
            # 修复: 原逻辑用volume当序列号做去重，导致相同volume的tick被误判为重复而丢弃
            # 修复: 三元组去重在盘口静止时仍会误判，增加时间维度（5秒内相同三元组才丢弃）
            _now = time.monotonic()
            tick_dedup_key = (instrument_id, last_price, volume)
            last_dedup = self._last_tick_seq.get(instrument_id)
            _last_dedup_ts = self._last_tick_seq_ts.get(instrument_id, 0.0)
            if last_dedup == tick_dedup_key and (_now - _last_dedup_ts) < 1.0:
                self._tick_dedup_drop_count += 1
                if self._tick_dedup_drop_count % 10000 == 1:
                    logger.warning("[R15-P0-DATA-03] tick去重丢弃: instrument_id=%s total_dropped=%d",
                                   instrument_id, self._tick_dedup_drop_count)
                return

            self._last_tick_seq[instrument_id] = tick_dedup_key
            self._last_tick_seq_ts[instrument_id] = _now
            if not self._bg_threads_started:
                self._start_background_threads()
            return self._on_tick_impl(instrument_id, last_price, volume)

    def _on_tick_impl(self, instrument_id: str, last_price: float, volume: float = 0) -> None:
        """on_tick内部实现（在。tick_lock内执行）"""
        normalized_id = self._strip_exchange_prefix(str(instrument_id).strip())
        diag_on = False
        try:
            from ali2026v3_trading.infra.health_monitor import is_monitored_contract
            diag_on = is_monitored_contract(normalized_id)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] _sub_core is_monitored_contract suppressed: %s", _r3_err)
            logger.debug("[SubscriptionManagerV2] Failed to check monitored contract for %s", normalized_id)  # R13-P2-LOG-01修复
        if diag_on:
            now = time.time()
            with self._submgr_tick_accum_lock:
                self._submgr_tick_accum_count += 1
                if now - self._submgr_tick_last_output_time >= self._submgr_tick_summary_interval:
                    logging.info("[SUBMGR_TICK_ALL_SUMMARY] ticks=%d sample=(%s -> %s P=%s V=%s)",
                                self._submgr_tick_accum_count,
                                instrument_id, normalized_id, last_price, volume)  # R13-P2-LOG-01修复
                    self._submgr_tick_accum_count = 0
                    self._submgr_tick_last_output_time = now
        try:
            # 优先先ParamsService metadata 路由
            routed = False
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                ps = get_params_service()

                # _Group A收口：优先使用规范化ID查找，仅当两ID不同时才回退
                meta = ps.get_instrument_meta_by_id(normalized_id)
                if not meta and normalized_id != instrument_id:
                    meta = ps.get_instrument_meta_by_id(instrument_id)
                if meta:
                    inst_type = meta.get('type', '')
                    internal_id = meta.get('internal_id')

                    # _P0修复：原子引用t_type_service，避免并发替换导致None引用  # R17-P2-DOC-03: P0修复标记保留用于追溯
                    tts = self.t_type_service
                    if tts is None:
                        try:
                            from ali2026v3_trading.data.t_type_service import get_t_type_service
                            tts = get_t_type_service()
                            if tts is not None:
                                self.t_type_service = tts
                        except Exception:
                            pass
                    if inst_type == 'option' and tts:
                        tts.on_option_tick(normalized_id, float(last_price), volume)
                        routed = True
                    elif inst_type == 'future' and tts:
                        # _两ID原则：传。internal_id 而非 instrument_id
                        if internal_id is not None:
                            tts.on_future_instrument_tick(int(internal_id), float(last_price))
                        else:
                            logging.warning("[SubMgr] Missing internal_id for future %s", normalized_id)  # R13-P2-LOG-01修复
                        routed = True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                err_key = str(e)
                if not hasattr(self, '_op_error_circuit'):
                    self._op_error_circuit = {}
                now = time.time()
                if err_key not in self._op_error_circuit:
                    self._op_error_circuit[err_key] = {'count': 0, 'first_seen': now, 'last_logged': 0}
                self._op_error_circuit[err_key]['count'] += 1
                if self._op_error_circuit[err_key]['count'] <= 3 or now - self._op_error_circuit[err_key]['last_logged'] > 300:
                    logger.error("[SubscriptionManagerV2] Operation failed: %s", e, exc_info=True)  # R13-P2-LOG-01修复
                    self._op_error_circuit[err_key]['last_logged'] = now
                elif self._op_error_circuit[err_key]['count'] % 1000 == 0:
                    logger.warning("[SubscriptionManagerV2] Operation failed '%s' suppressed (count=%d)",
                                  err_key, self._op_error_circuit[err_key]['count'])  # R13-P2-LOG-01修复

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

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as exc:
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
        """记录收到K线（分子：平台返回过K线的合约定"""
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
                'total_subscribed': 分母（合约数据
                'total_products': 分母（品种数据
                'kline_received': K线分子（合约数）,
                'tick_received': Tick分子（合约数据
                'kline_products': K线分子（品种数）,
                'tick_products': Tick分子（品种数据
                'kline_rate': K线成功率（合约维度）,
                'tick_rate': Tick成功率（合约维度量
                'kline_product_rate': K线成功率（品种维度）,
                'tick_product_rate': Tick成功率（品种维度量
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
            "[合约维度] K_ %d/%d = %.1f%% | Tick: %d/%d = %.1f%%",
            stats['kline_received'], stats['total_subscribed'], stats['kline_rate'] * 100,
            stats['tick_received'], stats['total_subscribed'], stats['tick_rate'] * 100
        )

        # 品种维度
        logger.info(
            "[品种维度] K_ %d/%d = %.1f%% | Tick: %d/%d = %.1f%%",
            stats['kline_products'], stats['total_products'], stats['kline_product_rate'] * 100,
            stats['tick_products'], stats['total_products'], stats['tick_product_rate'] * 100
        )

        # 未收到数据的品种
        # FIX-P1-1: 夜盘品种过滤 — 夜盘仅SHFE/DCE/CZCE/INE/GFEX有交易，其余品种Tick/K线缺失属正常
        _night_session_exchanges = {'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX'}
        _night_session_products = set()
        try:
            from ali2026v3_trading.config.params_service import get_params_service
            _ps = get_params_service()
            if _ps:
                for _pid in (stats.get('tick_missing_products', []) + stats.get('kline_missing_products', [])):
                    _meta = _ps.get_instrument_meta_by_id(_pid + '2609') if _ps else None
                    if not _meta:
                        _meta = _ps.get_instrument_meta_by_id(_pid + '2607') if _ps else None
                    if _meta and _meta.get('exchange') in _night_session_exchanges:
                        _night_session_products.add(_pid)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError):
            pass
        _is_night_session = False
        try:
            from ali2026v3_trading.infra.market_time_service import MarketTimeService
            _mts = MarketTimeService()
            _is_night_session = _mts.is_night_session()
        except (ImportError, ValueError, KeyError, TypeError, AttributeError):
            try:
                _now_hour = datetime.now().hour
                _is_night_session = _now_hour >= 21 or _now_hour < 3
            except (ValueError, TypeError):
                pass
        if stats['tick_missing_products']:
            _filtered_tick_missing = stats['tick_missing_products']
            if _is_night_session and _night_session_products:
                _filtered_tick_missing = [
                    p for p in stats['tick_missing_products']
                    if p in _night_session_products
                ]
            elif _is_night_session:
                _filtered_tick_missing = stats['tick_missing_products'][:5]
            if _filtered_tick_missing:
                logger.warning("[Tick未收到品种] %s%s",
                               ', '.join(_filtered_tick_missing[:20]),
                               f' (夜盘过滤: {len(stats["tick_missing_products"])-len(_filtered_tick_missing)}个非夜盘品种已忽略)' if _is_night_session and len(_filtered_tick_missing) < len(stats['tick_missing_products']) else '')
        if stats['kline_missing_products']:
            _filtered_kline_missing = stats['kline_missing_products']
            if _is_night_session and _night_session_products:
                _filtered_kline_missing = [
                    p for p in stats['kline_missing_products']
                    if p in _night_session_products
                ]
            elif _is_night_session:
                _filtered_kline_missing = stats['kline_missing_products'][:5]
            if _filtered_kline_missing:
                logger.warning("[K线未收到品种] %s%s",
                               ', '.join(_filtered_kline_missing[:20]),
                               f' (夜盘过滤: {len(stats["kline_missing_products"])-len(_filtered_kline_missing)}个非夜盘品种已忽略)' if _is_night_session and len(_filtered_kline_missing) < len(stats['kline_missing_products']) else '')

        # 未收到数据的合约样例
        if stats['tick_missing_count'] > 0 and stats['tick_missing']:
            logger.info("[Tick未收到合约] 缺失合约数: %d (前5: %s)", stats['tick_missing_count'], ', '.join(str(i) for i in stats['tick_missing'][:5]))
        logger.info("=" * 80)
_SubscriptionCoreMixin = SubscriptionCoreService

class SubscriptionManager:
    """订阅管理器- Facade组合（消灭Mixin继承承
    通过组合持有3个Service实例，__getattr__委托实现零破坏性变更集
    DEPRECATED: Merged into subscription_service.py on 2026-06-12.
    """

    def __init__(self, data_manager=None, config=None):
        self.data_manager = data_manager
        self._config = config
        self._core_service = SubscriptionCoreService(self)
        self._instrument_service = SubscriptionInstrumentService()
        self._wal_service = SubscriptionWALService(self, config=config)

    def __getattr__(self, name):
        if '__getattr__recursing' in self.__dict__:
            raise AttributeError(name)

        self.__dict__['__getattr__recursing'] = True
        try:
            _ws = self.__dict__.get('_wal_service')
            if _ws is not None:
                try:
                    return getattr(_ws, name)
                except AttributeError:
                    pass
            _is = self.__dict__.get('_instrument_service')
            if _is is not None:
                try:
                    return getattr(_is, name)
                except AttributeError:
                    pass
            _cs = self.__dict__.get('_core_service')
            if _cs is not None:
                try:
                    return getattr(_cs, name)
                except AttributeError:
                    pass
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        finally:
            self.__dict__.pop('__getattr__recursing', None)
    @staticmethod
    def is_option(instrument_id: str) -> bool:
        return SubscriptionInstrumentService.is_option(instrument_id)

    @staticmethod
    def parse_option(instrument_id):
        return SubscriptionInstrumentService.parse_option(instrument_id)

    @staticmethod
    def parse_future(instrument_id):
        return SubscriptionInstrumentService.parse_future(instrument_id)

    def bind_data_manager(self, data_manager):
        self.data_manager = data_manager
        self._core_service.bind_data_manager(data_manager)

# ========== 测试代码 ==========

if __name__ == "__main__":
    # 示例: 使用告警回调

    def alert_handler(count, msg):
        pass
__all__ = [
    'SubscriptionConfig', 'SubscriptionInstrumentService', 'SubscriptionWALService',
    'SubscriptionCoreService', 'SubscriptionManager',
    '_acquire_file_lock', '_release_file_lock',
    'classify_registered_instruments', 'inst_get'
]
