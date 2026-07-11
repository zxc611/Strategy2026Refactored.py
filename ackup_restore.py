"""
backup_restore.py - 备份/恢复/值班 (DuckDBBackupService, OnCallManager, DuckDBRestoreService)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from typing import Any, Dict, List, Optional

from datetime import datetime

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads  # R4-4: 统一json_loads
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ
from ali2026v3_trading.infra._helpers import get_logger  # R9-5


logger = get_logger(__name__)  # R9-5


# ============================================================================
# DR-P1-01修复: DuckDB定期备份机制
# DR-P1-03修复: 备份异地存储标记(backup_manifest.json)
# ============================================================================
class DuckDBBackupService:
    """DuckDB定期备份服务

    DR-P1-01: 每小时备份一次，保留最近24个备份
    DR-P1-03: 生成backup_manifest.json记录备份完整性
    """

    BACKUP_INTERVAL_SEC = 3600.0  # 1小时
    MAX_BACKUPS = 24
    REMOTE_BACKUP_ENABLED = False  # DR-P1-03: 异地存储标记，需手动配置后启用

    def __init__(self, db_path: Optional[str] = None,
                 backup_dir: Optional[str] = None):
        self._db_path = db_path
        self._backup_dir = backup_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', 'backups'
        )
        self._manifest_path = os.path.join(self._backup_dir, 'backup_manifest.json')
        self._last_backup_time: float = 0.0
        self._lock = threading.Lock()
        os.makedirs(self._backup_dir, exist_ok=True)

    def _ensure_db_path(self) -> str:
        if self._db_path and os.path.exists(self._db_path):
            return self._db_path
        try:
            from ali2026v3_trading.data.storage_core import _get_default_db_path
            return _get_default_db_path()
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            logging.debug("[R3-L2] backup_restore _ensure_db_path import suppressed: %s", _r3_err)
            default = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), '..', 'trading_data.duckdb'
            )
            return default

    def backup_duckdb(self, force: bool = False) -> Optional[str]:
        """执行DuckDB备份

        DR-P1-01: 每小时执行一次，保留最近24个备份
        DR-P1-03: 记录backup_manifest.json

        Returns:
            备份文件路径，跳过时返回None
        """
        now = time.time()
        if not force and (now - self._last_backup_time) < self.BACKUP_INTERVAL_SEC:
            return None

        db_path = self._ensure_db_path()
        if not os.path.exists(db_path):
            logger.warning("[DR-P1-01] DuckDB文件不存在，跳过备份: %s", db_path)
            return None

        timestamp = datetime.fromtimestamp(now).strftime('%Y%m%d_%H%M%S')
        backup_filename = f"ticks_{timestamp}.duckdb"
        backup_path = os.path.join(self._backup_dir, backup_filename)

        try:
            _conn = None
            try:
                from ali2026v3_trading.data.data_access import get_data_access
                _da_bak = get_data_access()
                from ali2026v3_trading.data.db_adapter import connect
                _conn = connect(db_path, read_only=True)
                _conn.execute(f"EXPORT DATABASE '{backup_path}' (FORMAT PARQUET)")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, IOError):
                if _conn:
                    try:
                        _conn.execute(f"COPY (SELECT 1) TO '{backup_path}' (FORMAT PARQUET)")
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _r3_err:
                        logging.debug("[R3-L2] backup_restore COPY fallback suppressed: %s", _r3_err)
                        pass
                import shutil as _shutil
                _shutil.copy2(db_path, backup_path)
            finally:
                if _conn:
                    try:
                        _conn.close()
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        logging.debug("[R3-L2] backup_restore conn.close suppressed: %s", _r3_err)
                        pass

            with self._lock:
                self._last_backup_time = now

            self._record_backup_manifest(backup_path)
            self._rotate_backups()
            logger.info("[DR-P1-01] DuckDB备份完成: %s", backup_path)
            return backup_path
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[DR-P1-01] DuckDB备份失败: %s", e)
            return None

    def _record_backup_manifest(self, backup_path: str) -> None:
        """DR-P1-03: 记录备份到backup_manifest.json"""
        import hashlib
        try:
            file_size = os.path.getsize(backup_path)
            sha256_hash = hashlib.sha256()
            with open(backup_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256_hash.update(chunk)
            file_hash = sha256_hash.hexdigest()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("[DR-P1-03] 计算备份哈希失败: %s", e)
            file_size = 0
            file_hash = ''

        entry = {
            'file_path': backup_path,
            'file_size': file_size,
            'hash': file_hash,
            'created_at': datetime.now(_CHINA_TZ).isoformat(),
        }

        manifest = []
        if os.path.exists(self._manifest_path):
            try:
                with open(self._manifest_path, 'r', encoding='utf-8') as f:
                    manifest = json_loads(f.read())  # R4-4: 统一json_loads
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] backup_restore manifest read suppressed: %s", _r3_err)
                manifest = []

        manifest.append(entry)
        with open(self._manifest_path, 'w', encoding='utf-8') as f:
            f.write(json_dumps(manifest, indent=2))
        logger.info("[DR-P1-03] 备份清单已更新: %s (size=%d, hash=%s...)", backup_path, file_size, file_hash[:16])

    def verify_backup_integrity(self, backup_file: str) -> bool:
        """DR-P1-03: 验证备份文件完整性"""
        if not os.path.exists(backup_file):
            logger.warning("[DR-P1-03] 备份文件不存在: %s", backup_file)
            return False
        if not os.path.exists(self._manifest_path):
            logger.warning("[DR-P1-03] 备份清单不存在，无法验证")
            return False
        try:
            import hashlib
            with open(self._manifest_path, 'r', encoding='utf-8') as f:
                manifest = json_loads(f.read())  # R4-4: 统一json_loads
            target_entry = None
            for entry in manifest:
                if entry.get('file_path') == backup_file:
                    target_entry = entry
                    break
            if not target_entry:
                logger.warning("[DR-P1-03] 备份清单中未找到: %s", backup_file)
                return False
            expected_hash = target_entry.get('hash', '')
            if not expected_hash:
                return True
            sha256_hash = hashlib.sha256()
            with open(backup_file, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256_hash.update(chunk)
            actual_hash = sha256_hash.hexdigest()
            if actual_hash != expected_hash:
                logger.error("[DR-P1-03] 备份完整性校验失败: %s", backup_file)
                return False
            logger.info("[DR-P1-03] 备份完整性校验通过: %s", backup_file)
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[DR-P1-03] 验证备份完整性异常: %s", e)
            return False

    def _rotate_backups(self) -> None:
        """DR-P1-01: 保留最近MAX_BACKUPS个备份，删除旧的"""
        try:
            backup_files = sorted(
                [os.path.join(self._backup_dir, f) for f in os.listdir(self._backup_dir)
                 if f.startswith('ticks_') and f.endswith('.duckdb')],
                key=os.path.getmtime,
            )
            while len(backup_files) > self.MAX_BACKUPS:
                oldest = backup_files.pop(0)
                os.remove(oldest)
                logger.info("[DR-P1-01] 删除旧备份: %s", oldest)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("[DR-P1-01] 备份轮转失败: %s", e)


# 全局单例
_backup_service: Optional[DuckDBBackupService] = None
_backup_service_lock = threading.Lock()


def get_backup_service() -> DuckDBBackupService:
    global _backup_service
    with _backup_service_lock:
        if _backup_service is None:
            _backup_service = DuckDBBackupService()
        return _backup_service


class OnCallManager:
    """R20-P1-OPS-01修复: On-call轮值管理"""

    def __init__(self):
        self._schedule = {}
        self._escalation_timeout_sec = 300
        self._current_oncall = None

    def set_oncall_schedule(self, schedule: dict) -> None:
        self._schedule = schedule

    def get_current_oncall(self) -> Optional[dict]:
        return self._current_oncall

    def update_oncall_rotation(self, role: str, person: str, contact: str, shift_start: str, shift_end: str) -> None:
        self._schedule[role] = {
            'person': person,
            'contact': contact,
            'shift_start': shift_start,
            'shift_end': shift_end,
        }
        logger.info("On-call rotation updated: role=%s, person=%s", role, person)

    def check_escalation_needed(self, alert_time: float) -> Optional[dict]:
        if alert_time and (time.time() - alert_time) > self._escalation_timeout_sec:
            return {'action': 'escalate', 'timeout_sec': self._escalation_timeout_sec}
        return None


_oncall_manager: Optional[OnCallManager] = None
_oncall_lock = threading.Lock()


def get_oncall_manager() -> OnCallManager:
    global _oncall_manager
    with _oncall_lock:
        if _oncall_manager is None:
            _oncall_manager = OnCallManager()
        return _oncall_manager


class DuckDBRestoreService:
    """R20-P1-OPS-02修复: DuckDB恢复服务"""

    MAX_RESTORE_ATTEMPTS = 3

    def __init__(self):
        self._backup_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', 'backups'
        )

    def list_available_backups(self) -> list:
        if not os.path.exists(self._backup_dir):
            return []
        return sorted(
            [f for f in os.listdir(self._backup_dir)
             if f.startswith('ticks_') and f.endswith('.duckdb')],
            key=lambda f: os.path.getmtime(os.path.join(self._backup_dir, f)),
            reverse=True,
        )

    def restore_from_backup(self, backup_filename: str, target_db_path: str) -> dict:
        result = {'success': False, 'backup_file': backup_filename, 'target': target_db_path, 'error': None}
        backup_path = os.path.join(self._backup_dir, backup_filename)
        if not os.path.exists(backup_path):
            result['error'] = f'Backup file not found: {backup_path}'
            return result
        try:
            if os.path.exists(target_db_path):
                corrupted_path = target_db_path + '.corrupted'
                shutil.move(target_db_path, corrupted_path)
                logger.warning("Moved current DB to corrupted: %s", corrupted_path)
            shutil.copy2(backup_path, target_db_path)
            logger.info("Restored DB from backup: %s -> %s", backup_path, target_db_path)
            result['success'] = True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            result['error'] = str(e)
            logger.error("Restore failed: %s", e)
        return result

    def auto_restore_latest(self, target_db_path: str) -> dict:
        backups = self.list_available_backups()
        if not backups:
            return {'success': False, 'error': 'No backups available'}
        for attempt, backup in enumerate(backups[:self.MAX_RESTORE_ATTEMPTS]):
            result = self.restore_from_backup(backup, target_db_path)
            if result['success']:
                return result
            logger.warning("Restore attempt %d/%d failed: %s", attempt + 1, self.MAX_RESTORE_ATTEMPTS, result['error'])
        return {'success': False, 'error': 'All restore attempts failed'}


_restore_service: Optional[DuckDBRestoreService] = None
_restore_lock = threading.Lock()


def get_restore_service() -> DuckDBRestoreService:
    global _restore_service
    with _restore_lock:
        if _restore_service is None:
            _restore_service = DuckDBRestoreService()
        return _restore_service


