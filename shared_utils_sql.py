"""shared_utils_sql — SQL安全相关 (R27-CP-04-FIX: 从shared_utils拆分)

SQL注入防护: 标识符与值清理函数
"""

import re

__all__ = ['sanitize_sql_identifier', 'sanitize_sql_value']


def sanitize_sql_identifier(identifier: str) -> str:
    """清理SQL标识符（表名、列名）以防止注入。

    仅允许字母、数字、下划线和点号字符，且必须以字母或下划线开头。

    Args:
        identifier: 待清理的SQL标识符

    Returns:
        str: 验证通过的标识符

    Raises:
        ValueError: 标识符包含非法字符时抛出
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


def sanitize_sql_value(value) -> str:
    """清理SQL值，通过转义单引号防止注入。

    Args:
        value: 待清理的SQL值，None返回'NULL'

    Returns:
        str: 转义后的SQL值字符串（含引号）
    """
    if value is None:
        return 'NULL'
    s = str(value)
    s = s.replace("'", "''")
    return f"'{s}'"
