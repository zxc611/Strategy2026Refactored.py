"""ConfigQueryFacade — 配置读操作门面(P0-S3)
ISP遵守：18个纯读消费者通过此Facade访问配置，不含写操作。
所有方法委托给ConfigService单例。
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class ConfigQueryFacade:
    """配置读操作门面 — 所有方法委托给ConfigService单例，禁止写操作"""

    def __init__(self):
        from ali2026v3_trading.config_service import ConfigService
        self._service = ConfigService()

    def get_config_data(self) -> Dict[str, Any]:
        return self._service.get_config_data()

    def need_reload(self) -> bool:
        return self._service.need_reload()

    def get_snapshot_info(self) -> Optional[Dict[str, Any]]:
        return self._service.get_snapshot_info()

    def load_checkpoint_safe(self, path: str) -> Optional[Dict[str, Any]]:
        return self._service.load_checkpoint_safe(path)

    def to_dict(self) -> Dict[str, Any]:
        return self._service.to_dict()

    def diff_config(self, other_env: str = 'production') -> Dict[str, Any]:
        return self._service.diff_config(other_env)

    def check_version_alignment(self) -> Dict[str, Any]:
        return self._service.check_version_alignment()

    def get_cascade_threshold_grid(self) -> Dict[str, Any]:
        return self._service.get_cascade_threshold_grid()

    def get_plr_thresholds(self) -> Dict[str, Any]:
        return self._service.get_plr_thresholds()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._service, name)


class ConfigCommandFacade:
    """配置写操作门面 — 所有方法委托给ConfigService单例，禁止自行实现
    线程安全：reload()内部_sync_config_data()持锁原子替换_config_data
    """

    def __init__(self):
        from ali2026v3_trading.config_service import ConfigService
        self._service = ConfigService()

    def reload(self) -> bool:
        return self._service.reload()

    def create_config_snapshot(self, snapshot_id: str) -> bool:
        return self._service.create_config_snapshot(snapshot_id)

    def rollback_config_snapshot(self, snapshot_id: str) -> bool:
        return self._service.rollback_config_snapshot(snapshot_id)

    def save_to_file(self, filepath: str) -> bool:
        return self._service.save_to_file(filepath)

    def save_checkpoint_safe(self, path: str) -> bool:
        return self._service.save_checkpoint_safe(path)


def get_config_query_facade() -> ConfigQueryFacade:
    return ConfigQueryFacade()


def get_config_command_facade() -> ConfigCommandFacade:
    return ConfigCommandFacade()


__all__ = [
    'ConfigQueryFacade',
    'ConfigCommandFacade',
    'get_config_query_facade',
    'get_config_command_facade',
]