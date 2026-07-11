# [M1-92] 参数迁移
# MODULE_ID: M1-007

"""参数服务 - 参数迁移"""



from __future__ import annotations



import logging

from typing import Any, Dict, List, Optional





_param_migration_history: List[Dict[str, Any]] = []

_PARAM_MIGRATION_HISTORY_MAX = 100





def migrate_params_rename(params: Dict[str, Any],

                           old_key: str, new_key: str,

                           version: str = "") -> Dict[str, Any]:

    """UPG-P1-02修复: 参数重命名迁移


    将旧参数名映射到新参数名，保留旧参数值
    如果新参数名已存在，则不覆盖率


    Args:

        params: 参数字典（将被原地修改）

        old_key: 旧参数名

        new_key: 新参数名

        version: 迁移版本号（用于日志记录录


    Returns:

        Dict: 迁移结果 {migrated: bool, old_key: str, new_key: str, value: Any}

    """

    result = {'migrated': False, 'old_key': old_key, 'new_key': new_key, 'value': None}



    if old_key in params and new_key not in params:

        value = params.pop(old_key)

        params[new_key] = value

        result['migrated'] = True

        result['value'] = value

        logging.info(

            "UPG-P1-02: 参数重命名迁移 %s _%s (value=%r, version=%s)",

            old_key, new_key, value, version,

        )

    elif old_key in params and new_key in params:

        # 新旧参数都存在，保留新参数值，删除旧参数
        old_value = params.pop(old_key)

        result['value'] = params[new_key]

        logging.debug(

            "UPG-P1-02: 参数重命名跳过新参数已存在): %s _%s "

            "(old_value=%r, new_value=%r)",

            old_key, new_key, old_value, params[new_key],

        )

    else:

        logging.debug("UPG-P1-02: 参数重命名跳过旧参数不存在): %s", old_key)



    # 记录迁移历史

    _record_param_migration('rename', {

        'old_key': old_key, 'new_key': new_key,

        'version': version, 'result': result,

    })



    return result





def migrate_params_default_change(params: Dict[str, Any],

                                   key: str, old_default: Any, new_default: Any,

                                   version: str = "",

                                   force_update: bool = False) -> Dict[str, Any]:

    """UPG-P1-02修复: 参数默认值变更迁移


    当参数的默认值发生变更时，将使用旧默认值的参数更新为新默认值起
    如果用户已自定义该参数值（不等于旧默认值），则不覆盖率


    Args:

        params: 参数字典（将被原地修改）'
        key: 参数据
        old_default: 旧默认证
        new_default: 新默认证
        version: 迁移版本号
        force_update: 是否强制更新（即使值不等于旧默认值也更新新


    Returns:

        Dict: 迁移结果 {migrated: bool, key: str, old_value: Any, new_value: Any}

    """

    result = {'migrated': False, 'key': key, 'old_value': None, 'new_value': None}



    current_value = params.get(key)

    result['old_value'] = current_value



    if current_value is None:

        # 参数不存在，设置新默认证
        params[key] = new_default

        result['migrated'] = True

        result['new_value'] = new_default

        logging.info(

            "UPG-P1-02: 参数默认值迁移新增): %s = %r (version=%s)",

            key, new_default, version,

        )

    elif force_update or current_value == old_default:

        # 当前值等于旧默认值，更新为新默认证
        params[key] = new_default

        result['migrated'] = True

        result['new_value'] = new_default

        logging.info(

            "UPG-P1-02: 参数默认值迁移 %s %r _%r (version=%s)",

            key, current_value, new_default, version,

        )

    else:

        # 用户已自定义，不覆盖

        result['new_value'] = current_value

        logging.debug(

            "UPG-P1-02: 参数默认值迁移跳过用户自定义: %s = %r (old_default=%r)",

            key, current_value, old_default,

        )



    _record_param_migration('default_change', {

        'key': key, 'old_default': old_default, 'new_default': new_default,

        'version': version, 'result': result,

    })



    return result





def migrate_params_type_change(params: Dict[str, Any],

                                key: str, old_type: str, new_type: str,

                                type_converter: Optional[Callable[[Any], Any]] = None,

                                version: str = "") -> Dict[str, Any]:

    """UPG-P1-02修复: 参数类型转换迁移



    将参数值从旧类型转换为新类型型


    Args:

        params: 参数字典（将被原地修改）'
        key: 参数据
        old_type: 旧类型名称（_'str', 'int', 'float', 'bool'_
        new_type: 新类型名称
        type_converter: 自定义类型转换函数，签名 converter(old_value) -> new_value

        version: 迁移版本号


    Returns:

        Dict: 迁移结果 {migrated: bool, key: str, old_value: Any, new_value: Any, error: str}

    """

    result = {'migrated': False, 'key': key, 'old_value': None, 'new_value': None, 'error': ''}



    if key not in params:

        logging.debug("UPG-P1-02: 参数类型迁移跳过(参数不存在: %s", key)

        return result



    old_value = params[key]

    result['old_value'] = old_value



    # 内置类型转换换
    _builtin_converters = {

        ('str', 'int'): lambda v: int(v),

        ('str', 'float'): lambda v: float(v),

        ('str', 'bool'): lambda v: v.lower() in ('true', '1', 'yes'),

        ('int', 'float'): lambda v: float(v),

        ('int', 'str'): lambda v: str(v),

        ('float', 'str'): lambda v: str(v),

        ('float', 'int'): lambda v: int(v),

        ('bool', 'str'): lambda v: str(v),

        ('bool', 'int'): lambda v: 1 if v else 0,  # [R22-TS-P1-06] 显式bool→int转换(非int()隐式)

    }



    converter = type_converter or _builtin_converters.get((old_type, new_type))



    if converter is None:

        result['error'] = f"无内置转换器: {old_type}→{new_type}，请提供type_converter参数"

        logging.warning("UPG-P1-02: %s", result['error'])

        return result



    try:

        new_value = converter(old_value)

        params[key] = new_value

        result['migrated'] = True

        result['new_value'] = new_value

        logging.info(

            "UPG-P1-02: 参数类型迁移: %s %s(%r) _%s(%r) (version=%s)",

            key, old_type, old_value, new_type, new_value, version,

        )

    except (ValueError, TypeError) as e:

        result['error'] = str(e)

        logging.warning(

            "UPG-P1-02: 参数类型迁移失败: %s %s(%r) _%s: %s",

            key, old_type, old_value, new_type, e,

        )



    _record_param_migration('type_change', {

        'key': key, 'old_type': old_type, 'new_type': new_type,

        'version': version, 'result': result,

    })



    return result





def apply_param_migration_plan(params: Dict[str, Any],

                                migration_plan: List[Dict[str, Any]]) -> Dict[str, Any]:

    """UPG-P1-02修复: 执行参数迁移计划



    按顺序执行一组参数迁移操作，支持重命令默认值变换类型转换换


    Args:

        params: 参数字典（将被原地修改）'
        migration_plan: 迁移计划列表，每项格式

            {

                'action': 'rename'|'default_change'|'type_change',

                'version': '迁移版本号,

                # rename: old_key, new_key

                # default_change: key, old_default, new_default, force_update(可。'
                # type_change: key, old_type, new_type, type_converter(可。

            }



    Returns:

        Dict: 迁移报告 {total: int, migrated: int, skipped: int, failed: int, details: list}

    """

    report = {'total': len(migration_plan), 'migrated': 0, 'skipped': 0, 'failed': 0, 'details': []}



    for step in migration_plan:

        action = step.get('action', '')

        version = step.get('version', '')



        try:

            if action == 'rename':

                result = migrate_params_rename(

                    params, step['old_key'], step['new_key'], version,

                )

            elif action == 'default_change':

                result = migrate_params_default_change(

                    params, step['key'], step['old_default'],

                    step['new_default'], version,

                    step.get('force_update', False),

                )

            elif action == 'type_change':

                result = migrate_params_type_change(

                    params, step['key'], step['old_type'],

                    step['new_type'], step.get('type_converter'), version,

                )

            else:

                result = {'migrated': False, 'error': f"未知迁移操作: {action}"}

                report['failed'] += 1



            if result.get('migrated'):

                report['migrated'] += 1

            elif result.get('error'):

                report['failed'] += 1

            else:

                report['skipped'] += 1



            report['details'].append(result)



        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            report['failed'] += 1

            report['details'].append({'action': action, 'error': str(e), 'migrated': False})

            logging.error("UPG-P1-02: 迁移步骤执行异常: action=%s error=%s", action, e)



    logging.info(

        "UPG-P1-02: 迁移计划执行完成: total=%d migrated=%d skipped=%d failed=%d",

        report['total'], report['migrated'], report['skipped'], report['failed'],

    )



    return report





def _record_param_migration(action: str, detail: Dict[str, Any]) -> None:

    """UPG-P1-02修复: 记录参数迁移历史"""

    global _param_migration_history

    _param_migration_history.append({

        'action': action,

        'detail': detail,

        'timestamp': time.time(),

    })

    if len(_param_migration_history) > _PARAM_MIGRATION_HISTORY_MAX:

        _param_migration_history = _param_migration_history[-_PARAM_MIGRATION_HISTORY_MAX:]





def get_param_migration_history(limit: int = 20) -> List[Dict[str, Any]]:

    """UPG-P1-02修复: 获取参数迁移历史记录



    Args:

        limit: 返回的最大记录数



    Returns:

        List[Dict]: 迁移历史记录列表

    """

    return _param_migration_history[-limit:]





# ============================================================================

# UPG-P1-12修复: 参数默认值兼容性检查
# ============================================================================



def check_default_value_compatibility(

    old_defaults: Dict[str, Any],

    new_defaults: Dict[str, Any],

    current_params: Optional[Dict[str, Any]] = None,

) -> Dict[str, Any]:

    """UPG-P1-12修复: 检查参数默认值变更的兼容灾


    在升级参数默认值之前，检查变更是否会导致兼容性问题：

    1. 新增参数（old中不存在在 安全

    2. 删除参数（new中不存在在 需要确认
    3. 默认值变换- 需要评估影子
    4. 类型变更 - 需要评估影子


    Args:

        old_defaults: 旧默认值字典
        new_defaults: 新默认值字典
        current_params: 当前运行时参数（可选，用于检查用户自定义值）'
    Returns:

        Dict: 兼容性报警{

            compatible: bool,

            added_keys: list,       # 新增参数

            removed_keys: list,     # 删除参数

            changed_defaults: list, # 默认值变换
            type_changes: list,     # 类型变更

            warnings: list,         # 警告

            breaking_changes: list, # 破坏性变换
        }

    """

    old_keys = set(old_defaults.keys())

    new_keys = set(new_defaults.keys())



    added_keys = sorted(new_keys - old_keys)

    removed_keys = sorted(old_keys - new_keys)

    changed_defaults = []

    type_changes = []

    warnings = []

    breaking_changes = []



    # 检查共有参数的默认值变换
    for key in sorted(old_keys & new_keys):

        old_val = old_defaults[key]

        new_val = new_defaults[key]



        # 类型变更检查
        old_type = type(old_val).__name__

        new_type = type(new_val).__name__

        if old_type != new_type:

            type_changes.append({

                'key': key,

                'old_type': old_type,

                'new_type': new_type,

                'old_value': old_val,

                'new_value': new_val,

            })

            # 类型变更通常是破坏性的

            breaking_changes.append(

                f"参数 {key} 类型变更: {old_type}→{new_type} "

                f"({old_val!r}→{new_val!r})"

            )



        # 默认值变更检查
        elif old_val != new_val:

            change_info = {

                'key': key,

                'old_default': old_val,

                'new_default': new_val,

                'impact': 'low',

            }



            # 评估影响级别

            # 安全关键参数的默认值变更影响高

            _safety_critical_params = {

                'close_stop_loss_ratio', 'max_risk_ratio',

                'circuit_breaker_pause_sec', 'signal_cooldown_sec',

                'max_net_delta_pct', 'max_net_gamma_pct',

            }

            if key in _safety_critical_params:

                change_info['impact'] = 'high'

                breaking_changes.append(

                    f"安全关键参数 {key} 默认值变换 {old_val!r}→{new_val!r}"

                )

            # 数值类参数变更幅度检查
            elif isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):

                if old_val != 0:

                    change_pct = abs((new_val - old_val) / old_val) * 100

                    if change_pct > 50:

                        change_info['impact'] = 'high'

                        warnings.append(

                            f"参数 {key} 默认值变化幅度大: {change_pct:.1f}% "

                            f"({old_val!r}→{new_val!r})"

                        )

                    elif change_pct > 10:

                        change_info['impact'] = 'medium'

                        warnings.append(

                            f"参数 {key} 默认值变换 {change_pct:.1f}% "

                            f"({old_val!r}→{new_val!r})"

                        )



            changed_defaults.append(change_info)



            # 检查当前运行时参数是否受影子
            if current_params and key in current_params:

                current_val = current_params[key]

                if current_val == old_val:

                    warnings.append(

                        f"参数 {key} 当前值等于旧默认值，升级后将自动变更"

                    )



    # 删除参数检查
    for key in removed_keys:

        breaking_changes.append(f"参数 {key} 已删除（旧默认值 {old_defaults[key]!r}）")

        if current_params and key in current_params:

            warnings.append(

                f"参数 {key} 已删除但当前运行时仍存在，需确认是否仍需。")



    compatible = len(breaking_changes) == 0



    report = {

        'compatible': compatible,

        'added_keys': added_keys,

        'removed_keys': removed_keys,

        'changed_defaults': changed_defaults,

        'type_changes': type_changes,

        'warnings': warnings,

        'breaking_changes': breaking_changes,

    }



    if not compatible:

        logging.warning(

            "UPG-P1-12: 参数默认值兼容性检查发送%d 个破坏性变换",

            len(breaking_changes),

        )

        for bc in breaking_changes:

            logging.warning("  UPG-P1-12: %s", bc)

    else:

        logging.info(

            "UPG-P1-12: 参数默认值兼容性检查通过 "

            "(added=%d, removed=%d, changed=%d, type_changed=%d)",

            len(added_keys), len(removed_keys),

            len(changed_defaults), len(type_changes),

        )



    return report





# ============================================================================

# P2修复: 升级迁移 _版本标记、迁移测试、灰度配置、归档策略
# ============================================================================



# P2修复: 参数服务版本号标记（增加版本号覆盖率数
_PARAMS_SERVICE_VERSION = "2.0.0"  # P2修复: 参数服务模块版本

_PARAMS_DATA_VERSION = "1.0"       # P2修复: 参数数据格式版本

_PARAMS_API_VERSION = "1.1"        # P2修复: 参数API版本





def test_migration(migration_plan: List[Dict[str, Any]],

                   test_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

    """P2修复: 迁移脚本测试函数



    在测试参数副本上执行迁移计划，验证迁移是否正确，

    不影响实际运行参数据


    Args:

        migration_plan: 迁移计划列表

        test_params: 测试参数（默认使用空字典。'
    Returns:

        Dict: {success: bool, report: dict, original_params: dict, migrated_params: dict}

    """

    import copy as _copy

    params = _copy.deepcopy(test_params or {})  # R21-MEM-P2-08修复: deepcopy必要，测试参数可能含嵌套dict需独立修改

    original = _copy.deepcopy(params)  # R21-MEM-P2-08修复: deepcopy必要，需保留原始快照用于对比



    try:

        report = apply_param_migration_plan(params, migration_plan)

        success = report['failed'] == 0

        if not success:

            logging.warning(

                "[ParamsService] P2修复: 迁移测试发现%d项失败",

                report['failed'],

            )

        return {

            'success': success,

            'report': report,

            'original_params': original,

            'migrated_params': params,

        }

    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

        logging.error("[ParamsService] P2修复: 迁移测试异常: %s", e)

        return {

            'success': False,

            'report': {'error': str(e)},

            'original_params': original,

            'migrated_params': params,

        }





def verify_rollback(original_params: Dict[str, Any],

                    backup_params: Dict[str, Any]) -> Dict[str, Any]:

    """P2修复: 回滚路径验证函数



    验证备份参数是否可以正确恢复到原始状态机


    Args:

        original_params: 原始参数

        backup_params: 备份参数



    Returns:

        Dict: {can_rollback: bool, missing_keys: list, extra_keys: list, value_diffs: list}

    """

    import copy as _copy

    original_keys = set(original_params.keys())

    backup_keys = set(backup_params.keys())



    missing_keys = sorted(original_keys - backup_keys)

    extra_keys = sorted(backup_keys - original_keys)

    value_diffs = []



    for key in sorted(original_keys & backup_keys):

        if original_params[key] != backup_params[key]:

            value_diffs.append({

                'key': key,

                'original': original_params[key],

                'backup': backup_params[key],

            })



    can_rollback = len(missing_keys) == 0 and len(value_diffs) == 0



    if not can_rollback:

        logging.warning(

            "[ParamsService] P2修复: 回滚验证失败 missing=%d extra=%d diffs=%d",

            len(missing_keys), len(extra_keys), len(value_diffs),

        )



    return {

        'can_rollback': can_rollback,

        'missing_keys': missing_keys,

        'extra_keys': extra_keys,

        'value_diffs': value_diffs,

    }







