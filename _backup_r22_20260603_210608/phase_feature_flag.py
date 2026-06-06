"""
Phase0增强: PhaseFeatureFlag — 每Phase回滚/降级策略的Feature Flag控制
灰度发布: 默认关闭，逐步灰度开启，支持即时切换回旧代码路径
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional


class PhaseFeatureFlag:
    """重构Phase的Feature Flag控制

    每个Sprint的Feature Flag默认关闭(False)
    灰度发布流程:
    1. 代码部署(Feature Flag=False, 使用旧代码路径)
    2. 灰度开启(Feature Flag=True, 部分流量走新代码)
    3. 全量开启(确认无问题后全量切换)
    4. 回滚(Feature Flag=False, 即时切换回旧代码)
    """

    _flags: Dict[str, bool] = {
        'USE_PIPELINED_SEND_ORDER': False,
        'USE_FILTER_CHAIN_SIGNAL': False,
        'USE_BUILDER_INIT': False,
        'USE_HEALTH_AGGREGATOR': False,
        'USE_STRATEGY_OPEN_STRATEGY': False,
        'USE_COMPLIANCE_RULE_ENGINE': False,
        'USE_PARAM_TABLE_PROVIDER': False,
        'USE_RISK_CHECK_ENGINE': False,
        'USE_VALIDATION_REGISTRY': False,
        'USE_STRATEGY_THREE_LAYER': False,
    }
    _lock = threading.RLock()
    _change_log: list = []

    @classmethod
    def is_enabled(cls, flag_name: str) -> bool:
        with cls._lock:
            return cls._flags.get(flag_name, False)

    @classmethod
    def enable(cls, flag_name: str) -> None:
        with cls._lock:
            old = cls._flags.get(flag_name, False)
            cls._flags[flag_name] = True
            cls._change_log.append((flag_name, old, True))
            logging.info("[FeatureFlag] %s: %s -> True", flag_name, old)

    @classmethod
    def disable(cls, flag_name: str) -> None:
        with cls._lock:
            old = cls._flags.get(flag_name, False)
            cls._flags[flag_name] = False
            cls._change_log.append((flag_name, old, False))
            logging.info("[FeatureFlag] %s: %s -> False", flag_name, old)

    @classmethod
    def disable_all(cls) -> None:
        with cls._lock:
            for k in cls._flags:
                cls._flags[k] = False
            logging.critical("[FeatureFlag] 全局紧急回滚: 所有Feature Flag已关闭")

    @classmethod
    def get_all(cls) -> Dict[str, bool]:
        with cls._lock:
            return dict(cls._flags)

    @classmethod
    def get_change_log(cls) -> list:
        with cls._lock:
            return list(cls._change_log)