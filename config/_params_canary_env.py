# [M1-93] 参数金丝雀环境
# MODULE_ID: M1-004

"""参数服务 - 金丝雀/环境/归档"""



from __future__ import annotations



import json

import os

import time

import logging

from datetime import datetime

from typing import Any, Dict, List, Optional



from ali2026v3_trading.infra.shared_utils import atomic_replace_file, CHINA_TZ  # R9-1

from ali2026v3_trading.infra.serialization_utils import json_dumps





_CANARY_DEPLOYMENT_CONFIG: Dict[str, Any] = {

    'enabled': False,                # 是否启用灰度发布

    'canary_ratio': 0.1,             # 灰度比例(10%)

    'canary_instruments': [],         # 灰度品种列表

    'canary_duration_sec': 3600,      # 灰度观察者者

    'rollback_on_error_rate': 0.05,   # 错误率超时%自动回滚

    'metrics_to_monitor': [           # 灰度期间监控的指标
        'sharpe_ratio', 'max_drawdown', 'win_rate', 'profit_factor',

    ],

}





def get_canary_config() -> Dict[str, Any]:

    """P2修复: 获取灰度发布配置"""

    return dict(_CANARY_DEPLOYMENT_CONFIG)





def update_canary_config(**kwargs) -> None:

    """P2修复: 更新灰度发布配置



    Args:

        **kwargs: 要更新的配置源
    """

    for key, value in kwargs.items():

        if key in _CANARY_DEPLOYMENT_CONFIG:

            _CANARY_DEPLOYMENT_CONFIG[key] = value

        else:

            logging.warning("[ParamsService] P2修复: 未知灰度配置源 %s", key)





def should_apply_canary(instrument_id: str = "") -> bool:

    """P2修复: 判断是否应对指定品种应用灰度策略



    Args:

        instrument_id: 品种ID



    Returns:

        True表示该品种应走灰度路由
    """

    config = get_canary_config()

    if not config['enabled']:

        return False

    canary_list = config.get('canary_instruments', [])

    if not canary_list:

        # 无指定品种列表时，按比例灰度

        from ali2026v3_trading.infra.shared_utils import compute_content_hash

        hash_val = int(compute_content_hash(instrument_id), 16)  # R5-3

        return (hash_val % 100) < (config['canary_ratio'] * 100)

    return instrument_id in canary_list





# P2修复: 热更新灰度能。'
def hot_update_with_canary(params: Dict[str, Any],

                            updates: Dict[str, Any],

                            instrument_id: str = "") -> Dict[str, Any]:

    """P2修复: 带灰度能力的热更新


    根据灰度配置决定是否将更新应用到参数据


    Args:

        params: 参数字典

        updates: 更新内容

        instrument_id: 品种ID（用于灰度判断）'
    Returns:

        Dict: {applied: bool, canary: bool, updated_keys: list}

    """

    is_canary = should_apply_canary(instrument_id)

    if not is_canary:

        logging.info(

            "[ParamsService] P2修复: 热更新跳过非灰度品种 instrument=%s",

            instrument_id,

        )

        return {'applied': False, 'canary': False, 'updated_keys': []}



    updated_keys = []

    for key, value in updates.items():

        if key in params:

            params[key] = value

            updated_keys.append(key)



    logging.info(

        "[ParamsService] P2修复: 灰度热更新应。instrument=%s keys=%s",

        instrument_id, updated_keys,

    )

    return {'applied': True, 'canary': True, 'updated_keys': updated_keys}





# P2修复: 多环境配置差异管理
_ENV_CONFIG_PROFILES: Dict[str, Dict[str, Any]] = {

    'development': {

        'log_level': 'DEBUG',

        'max_position_limit': 10,

        'enable_auto_stop_loss': True,

    },

    'testing': {

        'log_level': 'INFO',

        'max_position_limit': 50,

        'enable_auto_stop_loss': True,

    },

    'production': {

        'log_level': 'WARNING',

        'max_position_limit': 100,

        'enable_auto_stop_loss': True,

    },

}





def get_env_profile(env_name: str) -> Dict[str, Any]:

    """P2修复: 获取环境配置档案



    Args:

        env_name: 环境名称 (development/testing/production)



    Returns:

        环境配置字典

    """

    return dict(_ENV_CONFIG_PROFILES.get(env_name, {}))





def diff_env_profiles(env1: str, env2: str) -> Dict[str, Any]:

    """P2修复: 对比两个环境的配置差。'
    Args:

        env1: 第一个环境名称
        env2: 第二个环境名称


    Returns:

        Dict: {only_in_env1, only_in_env2, value_diffs}

    """

    p1 = _ENV_CONFIG_PROFILES.get(env1, {})

    p2 = _ENV_CONFIG_PROFILES.get(env2, {})

    k1, k2 = set(p1.keys()), set(p2.keys())

    return {

        'only_in_env1': sorted(k1 - k2),

        'only_in_env2': sorted(k2 - k1),

        'value_diffs': {

            k: {'env1': p1[k], 'env2': p2[k]}

            for k in k1 & k2 if p1[k] != p2[k]

        },

    }





# P2修复: 数据归档策略

def archive_params(params: Dict[str, Any],

                   archive_dir: Optional[str] = None,

                   label: str = "") -> Optional[str]:

    """P2修复: 归档参数快照到磁盘


    Args:

        params: 要归档的参数

        archive_dir: 归档目录（默认为项目logs/param_archives_
        label: 归档标签（如版本号）'
    Returns:

        归档文件路径，失败返回None

    """

    import json as _json

    try:

        if archive_dir is None:

            archive_dir = os.path.join(

                os.path.dirname(os.path.abspath(__file__)), 'logs', 'param_archives'

            )

        if not os.path.exists(archive_dir):

            os.makedirs(archive_dir, exist_ok=True)



        timestamp = datetime.now(CHINA_TZ).strftime('%Y%m%d_%H%M%S')

        label_suffix = f"_{label}" if label else ""

        archive_file = os.path.join(

            archive_dir, f"params_{timestamp}{label_suffix}.json"

        )

        atomic_replace_file(archive_file, json_dumps(params, indent=2))  # R9-1

        logging.info("[ParamsService] P2修复: 参数归档完成 path=%s", archive_file)

        return archive_file

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

        logging.error("[ParamsService] P2修复: 参数归档失败: %s", e)

        return None





def list_param_archives(archive_dir: Optional[str] = None,

                        limit: int = 20) -> List[str]:

    """P2修复: 列出参数归档文件



    Args:

        archive_dir: 归档目录

        limit: 返回数量限制



    Returns:

        归档文件路径列表

    """

    try:

        if archive_dir is None:

            archive_dir = os.path.join(

                os.path.dirname(os.path.abspath(__file__)), 'logs', 'param_archives'

            )

        if not os.path.exists(archive_dir):

            return []

        files = sorted(

            [os.path.join(archive_dir, f) for f in os.listdir(archive_dir)

             if f.startswith('params_') and f.endswith('.json')],

            reverse=True,

        )

        return files[:limit]

    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

        logging.warning("[R22-EP-P1] ParamsService exception swallowed")

        return []





# P2修复: 代码分支管理策略文档注释

# 分支管理策略:

# - main: 生产分支，只接受经过测试的合并请。'
# - develop: 开发分支，日常开发在此分支进程
# - feature/*: 功能分支，从develop拉出，完成后合并回develop

# - hotfix/*: 紧急修复分支，从main拉出，修复后合并回main和develop

# - release/*: 发布分支，从develop拉出，测试通过后合并回main

# 版本号规约 MAJOR.MINOR.PATCH (语义化版本

# - MAJOR: 不兼容的API变更

# - MINOR: 向后兼容的功能新回
# - PATCH: 向后兼容的问题修复
