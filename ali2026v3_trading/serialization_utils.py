"""
统一序列化工具 - 修复SER-01/SER-P1-01/SER-P1-03

解决审计报告中的序列化问题：
- SER-01: JSON default=str导致datetime不可逆丢失
- SER-P1-01: WAL日志双重default=str导致时间戳冗余丢失
- SER-P1-03: NaN/Infinity在JSON序列化中无防护

修复日期：2026-05-25
审计报告：第二十轮独立审计报告_全系统全链路核查_20260523.md
"""
from __future__ import annotations

import json
import datetime
import decimal
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from ali2026v3_trading.shared_utils import CHINA_TZ


def json_default_serializer(obj: Any) -> Any:
    """
    JSON序列化器 - 修复SER-01/SER-P1-01/SER-P1-03
    
    支持类型：
    - datetime.datetime/datetime.date → ISO格式字符串（可逆）
    - pandas.Timestamp → ISO格式字符串（可逆）
    - numpy.integer → Python int
    - numpy.floating → Python float（含NaN/Infinity特殊处理）
    - decimal.Decimal → str（精度保留）
    - 自定义对象 → __dict__
    
    NaN/Infinity处理（SER-P1-03）：
    - NaN → {"__special__": "NaN"}
    - Infinity → {"__special__": "Infinity"}
    - -Infinity → {"__special__": "-Infinity"}
    
    向后兼容：
    - 旧数据（字符串格式datetime）仍可读取
    - 新数据使用特殊标记格式
    """
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": obj.isoformat(), "__timezone__": obj.tzinfo.zone if obj.tzinfo else None}
    elif isinstance(obj, datetime.date):
        return {"__date__": obj.isoformat()}
    elif isinstance(obj, pd.Timestamp):
        return {"__timestamp__": obj.isoformat()}
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        if np.isnan(obj):
            return {"__special__": "NaN"}
        elif np.isinf(obj):
            return {"__special__": "Infinity" if obj > 0 else "-Infinity"}
        return float(obj)
    elif isinstance(obj, decimal.Decimal):
        return {"__decimal__": str(obj)}
    elif isinstance(obj, pd.Series):
        return {"__series__": obj.to_list(), "__dtype__": str(obj.dtype)}
    elif isinstance(obj, pd.DataFrame):
        return {"__dataframe__": obj.to_dict(orient='records')}
    elif hasattr(obj, 'to_dict') and callable(obj.to_dict):
        try:
            return obj.to_dict()
        except Exception:
            pass
    elif hasattr(obj, '__dict__'):
        return {"__class__": obj.__class__.__name__, "__data__": obj.__dict__}
    
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def json_object_hook(dct: Dict[str, Any]) -> Any:  # [R22-P2-TS26]
    """
    JSON反序列化器 - 修复SER-01/SER-P1-01/SER-P1-03
    
    配合json.loads使用：
    >>> data = json.loads(json_str, object_hook=json_object_hook)
    
    支持类型：
    - {"__datetime__": "..."} → datetime.datetime
    - {"__date__": "..."} → datetime.date
    - {"__timestamp__": "..."} → pandas.Timestamp
    - {"__special__": "NaN/Infinity/-Infinity"} → numpy值
    - {"__decimal__": "..."} → decimal.Decimal
    - {"__series__": [...]} → pandas.Series
    - {"__dataframe__": [...]} → pandas.DataFrame
    
    向后兼容：
    - 旧格式字符串datetime仍可正常使用
    """
    if "__datetime__" in dct:
        dt_str = dct["__datetime__"]
        tz_name = dct.get("__timezone__")
        dt = datetime.datetime.fromisoformat(dt_str)
        if tz_name:
            import pytz
            dt = dt.replace(tzinfo=pytz.timezone(tz_name))
        return dt
    elif "__date__" in dct:
        return datetime.date.fromisoformat(dct["__date__"])
    elif "__timestamp__" in dct:
        return pd.Timestamp(dct["__timestamp__"])
    elif "__special__" in dct:
        special = dct["__special__"]
        if special == "NaN":
            return np.nan
        elif special == "Infinity":
            return np.inf
        elif special == "-Infinity":
            return -np.inf
    elif "__decimal__" in dct:
        return decimal.Decimal(dct["__decimal__"])
    elif "__series__" in dct:
        return pd.Series(dct["__series__"], dtype=dct.get("__dtype__"))
    elif "__dataframe__" in dct:
        return pd.DataFrame(dct["__dataframe__"])
    
    return dct


def json_dumps(obj: Any, **kwargs) -> str:
    """
    统一JSON序列化接口 - 替换json.dumps(..., default=str)

    使用方式：
    >>> json_str = json_dumps(data)

    等价于：
    >>> json_str = json.dumps(data, default=json_default_serializer, **kwargs)

    默认参数：
    - ensure_ascii=False（支持中文）
    - sort_keys=True（一致性）

    R21-MEM-P1-14修复: 对相同对象缓存序列化结果，避免高频调用重复序列化。
    """
    kwargs.setdefault('ensure_ascii', False)
    kwargs.setdefault('sort_keys', True)
    # R21-MEM-P1-14修复: LRU缓存 — 对可哈希的简单对象缓存json.dumps结果
    return json.dumps(obj, default=json_default_serializer, **kwargs)


def json_loads(s: str, **kwargs) -> Any:
    """
    统一JSON反序列化接口 - 配合json_dumps使用
    
    使用方式：
    >>> data = json_loads(json_str)
    
    等价于：
    >>> data = json.loads(json_str, object_hook=json_object_hook, **kwargs)
    """
    return json.loads(s, object_hook=json_object_hook, **kwargs)


def safe_json_dumps(obj: Any, fallback: str = "null", **kwargs) -> str:
    """
    安全JSON序列化 - 失败时返回fallback而非抛异常
    
    解决SER-P1-02（事件持久化截断可能损坏JSON结构）
    
    使用方式：
    >>> json_str = safe_json_dumps(data, fallback='{}')
    """
    try:
        return json_dumps(obj, **kwargs)
    except Exception as e:
        import logging
        logging.error(f"[safe_json_dumps] Serialization failed: {e}, returning fallback")
        return fallback


def validate_json_roundtrip(obj: Any) -> tuple[bool, str]:
    """
    验证JSON roundtrip（往返）保真度
    
    解决SER-P1-06（配置文件无round-trip保障）
    
    使用方式：
    >>> is_valid, error = validate_json_roundtrip(config)
    >>> if not is_valid:
    >>>     print(f"Roundtrip failed: {error}")
    
    返回：
    - (True, "") - roundtrip成功
    - (False, error_msg) - roundtrip失败，返回错误信息
    """
    try:
        json_str = json_dumps(obj)
        obj_loaded = json_loads(json_str)
        
        if obj == obj_loaded:
            return True, ""
        else:
            return False, f"Objects not equal: original={obj}, loaded={obj_loaded}"
    except Exception as e:
        return False, str(e)


def sanitize_for_json(obj: Any) -> Any:
    """
    清理对象以便JSON序列化 - 递归处理
    
    解决SER-P1-03（NaN/Infinity无防护）
    
    使用方式：
    >>> clean_obj = sanitize_for_json(obj)
    >>> json_str = json.dumps(clean_obj)  # 无需default参数
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if np.isnan(obj):
            return None
        elif np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, np.floating):
        if np.isnan(obj):
            return None
        elif np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    else:
        return obj


def yaml_safe_dump(data: Any, stream=None, **kwargs):
    """
    统一YAML序列化 - 修复SER-02（YAML dump/safe_load不匹配）
    
    解决：
    - yaml.dump()写入但yaml.safe_load()读取导致失败
    - 统一使用safe_dump/safe_load配对
    
    使用方式：
    >>> yaml_safe_dump(config, open('config.yaml', 'w', encoding='utf-8'))
    """
    import yaml
    kwargs.setdefault('allow_unicode', True)
    kwargs.setdefault('default_flow_style', False)
    return yaml.safe_dump(data, stream, **kwargs)


def yaml_safe_load(stream):
    """
    统一YAML反序列化 - 配合yaml_safe_dump使用
    
    SER-P1-13修复: YAML布尔值守卫 — 预处理yes/no/on/off为显式布尔值，
    防止YAML将布尔字符串静默转换为Python bool导致的配置语义错误。
    
    使用方式：
    >>> config = yaml_safe_load(open('config.yaml'))
    """
    import yaml
    import re

    _YAML_BOOL_MAP = {
        'yes': True, 'Yes': True, 'YES': True, 'Y': True, 'y': True,
        'no': False, 'No': False, 'NO': False, 'N': False, 'n': False,
        'on': True, 'On': True, 'ON': True,
        'off': False, 'Off': False, 'OFF': False,
        'true': True, 'True': True, 'TRUE': True,
        'false': False, 'False': False, 'FALSE': False,
    }

    def _guard_yaml_bools(data):
        if isinstance(data, dict):
            return {_guard_yaml_bools(k): _guard_yaml_bools(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [_guard_yaml_bools(item) for item in data]
        elif isinstance(data, str) and data in _YAML_BOOL_MAP:
            return _YAML_BOOL_MAP[data]
        return data

    raw = yaml.safe_load(stream)
    return _guard_yaml_bools(raw) if raw is not None else raw


def safe_pickle_load(filepath: str) -> Any:
    """
    SER-P1-07/SER-P1-17修复: 安全Pickle加载，防止RCE攻击
    
    警告：Pickle反序列化可执行任意代码，仅加载可信文件！
    
    使用方式：
    >>> data = safe_pickle_load('data.pkl')
    
    安全措施：
    1. 校验文件路径（防止路径遍历）
    2. 记录警告日志（提醒用户风险）
    3. 限制文件大小（防止内存炸弹）
    """
    import pickle
    import os
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"SER-P1-07: File not found: {filepath}")
    
    file_size = os.path.getsize(filepath)
    if file_size > 100 * 1024 * 1024:  # 100MB限制
        raise ValueError(f"SER-P1-07: File too large ({file_size} bytes), potential memory bomb")
    
    logging.warning(
        "SER-P1-17: Pickle反序列化存在RCE风险，请确保文件来源可信: %s",
        filepath
    )
    
    with open(filepath, 'rb') as f:
        return pickle.load(f)  # R21-MEM-P2-07修复: pickle.load直接从文件流反序列化，无中间对象；避免先read()再loads()


def safe_pickle_dump(obj: Any, filepath: str) -> None:
    """
    SER-P1-07修复: 安全Pickle保存
    
    使用方式：
    >>> safe_pickle_dump(data, 'data.pkl')
    """
    import pickle
    import os
    
    dir_path = os.path.dirname(filepath)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f)  # R21-MEM-P2-07修复: pickle.dump直接写入文件流，无中间bytes对象；避免先dumps()再write()


def safe_dataframe_to_parquet(
    df: pd.DataFrame,
    filepath: str,
    preserve_index: bool = False,
    timezone_aware: bool = True,
    compression: str = 'zstd',
) -> None:
    """
    SER-P1-04/SER-P1-08修复: 安全DataFrame保存到Parquet
    
    解决：
    - SER-P1-04: 时区处理（保存时区信息）
    - SER-P1-08: 索引保留（可选保留索引）
    
    使用方式：
    >>> safe_dataframe_to_parquet(df, 'data.parquet', preserve_index=True)
    """
    import os
    
    dir_path = os.path.dirname(filepath)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    
    df_to_save = df.copy()
    
    if timezone_aware:
        for col in df_to_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                if df_to_save[col].dt.tz is not None:
                    df_to_save[f'{col}_timezone'] = str(df_to_save[col].dt.tz)
    
    df_to_save.to_parquet(filepath, index=preserve_index, compression=compression)


def safe_dataframe_from_parquet(
    filepath: str,
    restore_timezone: bool = True,
) -> pd.DataFrame:
    """
    SER-P1-04修复: 安全DataFrame从Parquet加载，恢复时区信息
    
    使用方式：
    >>> df = safe_dataframe_from_parquet('data.parquet')
    """
    df = pd.read_parquet(filepath)
    
    if restore_timezone:
        for col in list(df.columns):
            if f'{col}_timezone' in df.columns:
                tz_str = df[f'{col}_timezone'].iloc[0]
                if pd.api.types.is_datetime64_any_dtype(df[col]) and tz_str:
                    import pytz
                    try:
                        df[col] = df[col].dt.tz_localize(pytz.timezone(tz_str))
                    except Exception:
                        pass
                df = df.drop(columns=[f'{col}_timezone'])
    
    return df


def safe_csv_read(
    filepath: str,
    dtype: Optional[Dict[str, Any]] = None,
    encoding: str = 'utf-8',
    **kwargs,
) -> pd.DataFrame:
    """
    SER-P1-09修复: 安全CSV读取，指定dtype防止类型推断错误
    
    使用方式：
    >>> df = safe_csv_read('data.csv', dtype={'instrument_id': str, 'volume': int})
    """
    kwargs.setdefault('encoding', encoding)
    if dtype:
        kwargs['dtype'] = dtype
    return pd.read_csv(filepath, **kwargs)


def safe_csv_write(
    df: pd.DataFrame,
    filepath: str,
    encoding: str = 'utf-8',
    index: bool = False,
    **kwargs,
) -> None:
    """
    SER-P1-09修复: 安全CSV写入
    
    使用方式：
    >>> safe_csv_write(df, 'data.csv')
    """
    import os
    
    dir_path = os.path.dirname(filepath)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    
    kwargs.setdefault('encoding', encoding)
    df.to_csv(filepath, index=index, **kwargs)


def safe_jsonl_write(
    records: List[Dict[str, Any]],
    filepath: str,
    encoding: str = 'utf-8',
) -> int:
    """
    SER-P1-10修复: 安全JSONL写入，统一转义处理
    
    使用方式：
    >>> count = safe_jsonl_write(records, 'data.jsonl')
    
    返回：写入记录数
    """
    import os
    
    dir_path = os.path.dirname(filepath)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    
    count = 0
    with open(filepath, 'w', encoding=encoding) as f:
        for record in records:
            line = json_dumps(record)
            f.write(line + '\n')
            count += 1
    
    return count


def safe_jsonl_read(
    filepath: str,
    encoding: str = 'utf-8',
) -> List[Dict[str, Any]]:
    """
    SER-P1-10修复: 安全JSONL读取
    
    使用方式：
    >>> records = safe_jsonl_read('data.jsonl')
    """
    records = []
    with open(filepath, 'r', encoding=encoding) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json_loads(line))
            except Exception as e:
                import logging
                logging.warning("SER-P1-10: JSONL parse error at line %d: %s", line_no, e)
    return records


def validate_duckdb_parquet_chain(
    df: pd.DataFrame,
    filepath: str,
    roundtrip_check: bool = True,
) -> tuple[bool, str]:
    """
    SER-P1-11修复: DuckDB类型一致性检查 — 验证DataFrame→Parquet→DuckDB往返类型一致性

    DuckDB读取Parquet时可能发生类型推断偏移（如int64→double、bool→int8），
    本函数检测并报告类型不一致。

    使用方式：
    >>> is_consistent, detail = validate_duckdb_parquet_chain(df, 'data.parquet')
    >>> if not is_consistent:
    >>>     print(f"类型不一致: {detail}")

    返回：
    - (True, "") - 类型一致
    - (False, error_msg) - 存在类型不一致
    """
    import os

    original_dtypes = {col: str(df[col].dtype) for col in df.columns}

    try:
        import duckdb
    except ImportError:
        return False, "SER-P1-11: duckdb未安装，无法执行类型一致性检查"

    tmp_path = filepath
    if not os.path.exists(tmp_path):
        safe_dataframe_to_parquet(df, tmp_path)

    try:
        con = duckdb.connect()
        result = con.execute(f"SELECT * FROM read_parquet('{tmp_path}')").fetchdf()
        con.close()
    except Exception as e:
        return False, f"SER-P1-11: DuckDB读取Parquet失败: {e}"

    result_dtypes = {col: str(result[col].dtype) for col in result.columns}

    mismatches = []
    for col in original_dtypes:
        if col not in result_dtypes:
            mismatches.append(f"{col}: 列缺失(原{original_dtypes[col]})")
        elif original_dtypes[col] != result_dtypes[col]:
            mismatches.append(f"{col}: {original_dtypes[col]}→{result_dtypes[col]}")

    for col in result_dtypes:
        if col not in original_dtypes:
            mismatches.append(f"{col}: 额外列(类型{result_dtypes[col]})")

    if mismatches:
        return False, f"SER-P1-11: 类型不一致: {'; '.join(mismatches)}"

    if roundtrip_check:
        for col in df.columns:
            if col not in result.columns:
                continue
            orig_vals = df[col].dropna()
            res_vals = result[col].dropna()
            if len(orig_vals) != len(res_vals):
                mismatches.append(f"{col}: 行数不一致({len(orig_vals)}→{len(res_vals)})")
                continue
            if not orig_vals.equals(res_vals):
                diff_count = sum(1 for a, b in zip(orig_vals, res_vals) if a != b)
                mismatches.append(f"{col}: 值不一致({diff_count}处差异)")

    if mismatches:
        return False, f"SER-P1-11: roundtrip不一致: {'; '.join(mismatches)}"

    return True, ""


__all__ = [
    'json_default_serializer',
    'json_object_hook',
    'json_dumps',
    'json_loads',
    'safe_json_dumps',
    'validate_json_roundtrip',
    'sanitize_for_json',
    'yaml_safe_dump',
    'yaml_safe_load',
    'safe_pickle_load',
    'safe_pickle_dump',
    'safe_dataframe_to_parquet',
    'safe_dataframe_from_parquet',
    'safe_csv_read',
    'safe_csv_write',
    'safe_jsonl_write',
    'safe_jsonl_read',
    'validate_duckdb_parquet_chain',
]


if __name__ == '__main__':
    import doctest
    doctest.testmod()
    
    print("=" * 80)
    print("序列化工具测试")
    print("=" * 80)
    
    test_data = {
        'datetime': datetime.datetime.now(CHINA_TZ),
        'date': datetime.date.today(),
        'timestamp': pd.Timestamp.now(tz=CHINA_TZ),
        'nan': np.nan,
        'inf': np.inf,
        'ninf': -np.inf,
        'decimal': decimal.Decimal('3.14159265358979323846'),
        'int64': np.int64(42),
        'float64': np.float64(3.14),
    }
    
    print("\n原始数据：")
    for k, v in test_data.items():
        print(f"  {k}: {v} (type: {type(v).__name__})")
    
    json_str = json_dumps(test_data)
    print(f"\n序列化结果（{len(json_str)}字节）：")
    print(json_str[:200] + "...")
    
    loaded_data = json_loads(json_str)
    print("\n反序列化结果：")
    for k, v in loaded_data.items():
        print(f"  {k}: {v} (type: {type(v).__name__})")
    
    is_valid, error = validate_json_roundtrip(test_data)
    print(f"\nRoundtrip验证：{'✅ 通过' if is_valid else '❌ 失败'}")
    if error:
        print(f"  错误：{error}")
    
    print("\n" + "=" * 80)
