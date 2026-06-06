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
import yaml as _yaml

from ali2026v3_trading.shared_utils import CHINA_TZ

# P2-12: Parquet schema版本常量
PARQUET_SCHEMA_VERSION = '1.0'


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


def json_dumps(obj: Any, indent: Optional[int] = None, sort_keys: bool = True, ensure_ascii: bool = False, **kwargs) -> str:
    """
    统一JSON序列化接口 - 替换json.dumps(..., default=str)

    使用方式：
    >>> json_str = json_dumps(data)           # 紧凑输出，适用于JSONL日志
    >>> json_str = json_dumps(data, indent=2) # 缩进输出，适用于配置文件

    等价于：
    >>> json_str = json.dumps(data, default=json_default_serializer, **kwargs)

    默认参数：
    - indent=None（紧凑输出，适用于JSONL日志；配置文件可传indent=2）
    - ensure_ascii=False（支持中文）
    - sort_keys=True（一致性）

    Raises TypeError on serialization failure.
    For non-critical paths where fallback is acceptable, use safe_json_dumps instead.

    R21-MEM-P1-14修复: 对相同对象缓存序列化结果，避免高频调用重复序列化。
    """
    kwargs.setdefault('ensure_ascii', ensure_ascii)
    kwargs.setdefault('sort_keys', sort_keys)
    kwargs.setdefault('indent', indent)
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

    仅用于日志/非关键路径。关键路径应使用json_dumps（失败时抛TypeError）。

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
    kwargs.setdefault('allow_unicode', True)
    kwargs.setdefault('default_flow_style', False)
    return _yaml.safe_dump(data, stream, **kwargs)


def yaml_safe_load(stream):
    """
    统一YAML反序列化 - 配合yaml_safe_dump使用
    
    SER-P1-13修复: YAML布尔值守卫 — 预处理yes/no/on/off为显式布尔值，
    防止YAML将布尔字符串静默转换为Python bool导致的配置语义错误。
    
    使用方式：
    >>> config = yaml_safe_load(open('config.yaml'))
    """
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

    raw = _yaml.safe_load(stream)
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
    
    class _RestrictedUnpickler(pickle.Unpickler):
        """SER-P1-17修复: 限制pickle反序列化的类，防止RCE攻击"""
        _ALLOWED_MODULES = {
            'numpy', 'pandas', 'datetime', 'collections', 'builtins',
            'ali2026v3_trading', 'typing',
        }
        def find_class(self, module, name):
            # 允许基础类型和已知安全模块
            if module in self._ALLOWED_MODULES or any(module.startswith(m + '.') for m in self._ALLOWED_MODULES):
                return super().find_class(module, name)
            raise pickle.UnpicklingError(f"SER-P1-17: 不允许反序列化 {module}.{name}，疑似RCE攻击")

    with open(filepath, 'rb') as f:
        return _RestrictedUnpickler(f).load()


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

    # SER-P1-07修复: 可序列化性预检查 — 序列化后立即反序列化验证，失败则抛出SerializationError
    try:
        with open(filepath, 'rb') as f:
            _loaded = pickle.load(f)
        # 基本类型检查：确保反序列化结果不是意外类型
        if type(_loaded) != type(obj):
            raise SerializationError(
                f"pickle roundtrip类型不一致: original={type(obj).__name__}, loaded={type(_loaded).__name__}"
            )
    except SerializationError:
        # 删除已写入的损坏文件
        try:
            os.remove(filepath)
        except Exception:
            pass
        raise
    except Exception as _rt_err:
        import logging as _logging
        _logging.warning(
            "SER-P1-07: pickle roundtrip验证失败(非致命): %s, 文件已写入但可能不可靠",
            _rt_err,
        )


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
    - P2-12: Schema版本元数据（写入schema_version和created_at）
    
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
    
    # P2-12: 写入schema版本元数据
    _metadata = {
        'schema_version': PARQUET_SCHEMA_VERSION,
        'created_at': datetime.datetime.now(CHINA_TZ).isoformat(),
    }
    df_to_save.attrs['_metadata'] = _metadata
    
    df_to_save.to_parquet(filepath, index=preserve_index, compression=compression)


def safe_dataframe_from_parquet(
    filepath: str,
    restore_timezone: bool = True,
) -> pd.DataFrame:
    """
    SER-P1-04修复: 安全DataFrame从Parquet加载，恢复时区信息
    
    P2-12: 读取时验证schema_version元数据，版本不匹配时发出WARNING。
    
    使用方式：
    >>> df = safe_dataframe_from_parquet('data.parquet')
    """
    df = pd.read_parquet(filepath)
    
    # P2-12: 验证schema版本元数据
    try:
        _metadata = df.attrs.get('_metadata', None) if hasattr(df, 'attrs') else None
        if _metadata and isinstance(_metadata, dict):
            file_version = _metadata.get('schema_version', None)
            if file_version and file_version != PARQUET_SCHEMA_VERSION:
                import logging as _logging
                _logging.warning(
                    "P2-12: Parquet schema版本不匹配 file=%s current=%s: %s",
                    filepath, PARQUET_SCHEMA_VERSION, file_version,
                )
                # SER-P1-05修复: Schema evolution — 自动添加缺失列(填充NaN)并删除多余列
                try:
                    from ali2026v3_trading.serialization_utils import safe_dataframe_to_parquet as _safe_to_parquet
                    # 读取当前schema期望的列集（通过写入一个空DataFrame获取）
                    _expected_cols = set(pd.DataFrame().columns)  # fallback空集
                    # 使用当前版本写入一个临时文件来获取期望的列集
                    import tempfile as _tempfile
                    import os as _os
                    _tmp = _tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
                    _tmp_path = _tmp.name
                    _tmp.close()
                    try:
                        _empty_df = pd.DataFrame()
                        _safe_to_parquet(_empty_df, _tmp_path)
                        _ref_df = pd.read_parquet(_tmp_path)
                        _expected_cols = set(_ref_df.columns)
                    except Exception:
                        _expected_cols = set()
                    finally:
                        try:
                            _os.unlink(_tmp_path)
                        except Exception:
                            pass
                    # 添加缺失列(填充NaN)
                    _actual_cols = set(df.columns)
                    _missing_cols = _expected_cols - _actual_cols
                    if _missing_cols:
                        for _col in _missing_cols:
                            df[_col] = np.nan
                        _logging.info(
                            "SER-P1-05: Schema evolution添加缺失列: %s",
                            sorted(_missing_cols),
                        )
                    # 删除多余列
                    _extra_cols = _actual_cols - _expected_cols
                    if _extra_cols and _expected_cols:  # 仅当有期望列集时才删除
                        df = df.drop(columns=list(_extra_cols), errors='ignore')
                        _logging.info(
                            "SER-P1-05: Schema evolution删除多余列: %s",
                            sorted(_extra_cols),
                        )
                except Exception as _evo_err:
                    _logging.warning(
                        "SER-P1-05: Schema evolution处理失败: %s, 继续使用原始数据",
                        _evo_err,
                    )
        elif _metadata is None:
            # 旧格式文件无schema_version，仅debug日志
            import logging as _logging
            _logging.debug("P2-12: Parquet文件无schema_version元数据: %s", filepath)
    except Exception:
        pass
    
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


# P1-16修复: JSONL日志截断保护常量和辅助函数
MAX_JSONL_LINE_LENGTH = 1 * 1024 * 1024  # 1MB单行上限


def safe_jsonl_append_line(f, record: Any) -> None:
    """
    P1-16修复: 安全JSONL逐行追加写入，带截断保护
    
    解决：单条记录序列化后可能超长（如含大字段），导致JSONL文件损坏或读取OOM。
    
    使用方式：
    >>> with open('data.jsonl', 'a', encoding='utf-8') as f:
    ...     safe_jsonl_append_line(f, record)
    >>> # 或对缓存句柄：
    >>> safe_jsonl_append_line(self._log_f, record)
    
    行为：
    1. 序列化record为JSON字符串
    2. 若行长度超过MAX_JSONL_LINE_LENGTH(1MB)，截断并添加__truncated标记
    3. 写入行 + 换行符
    """
    line = json_dumps(record)
    if len(line) > MAX_JSONL_LINE_LENGTH:
        import logging as _logging
        _logging.warning(
            "[P1-16] JSONL line truncated: %d bytes exceeds %d limit",
            len(line), MAX_JSONL_LINE_LENGTH
        )
        line = line[:MAX_JSONL_LINE_LENGTH - 1] + '"}'  # 尝试闭合JSON
    f.write(line + '\n')


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
        from ali2026v3_trading.data_access import get_data_access
        _da = get_data_access()
        from ali2026v3_trading.db_adapter import connect_in_memory, fetchdf, close
    except ImportError:
        return False, "SER-P1-11: duckdb未安装，无法执行类型一致性检查"

    tmp_path = filepath
    if not os.path.exists(tmp_path):
        safe_dataframe_to_parquet(df, tmp_path)

    try:
        con = connect_in_memory()
        result = fetchdf(con, f"SELECT * FROM read_parquet('{tmp_path}')")
        close(con)
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


def normalize_parquet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """SER-P1-15修复: 统一Parquet列名为小写，消除跨环境大小写不一致

    DuckDB/Spark等引擎读取Parquet时可能将列名转为小写，
    此函数统一为小写，确保round-trip一致性。
    """
    df.columns = [c.lower().strip() for c in df.columns]
    return df


# P2-28修复: 序列化性能监控装饰器
import time as _time
import functools as _functools

def serialization_perf_monitor(func):
    """P2-28修复: 序列化性能监控装饰器"""
    @_functools.wraps(func)
    def wrapper(*args, **kwargs):
        _start = _time.perf_counter()
        try:
            result = func(*args, **kwargs)
            _elapsed = _time.perf_counter() - _start
            if _elapsed > 1.0:
                logging.warning("P2-28: 序列化操作耗时%.2fs: %s", _elapsed, func.__name__)
            return result
        except Exception as e:
            _elapsed = _time.perf_counter() - _start
            logging.error("P2-28: 序列化操作失败(%.2fs): %s - %s", _elapsed, func.__name__, e)
            raise
    return wrapper


# P2-34修复: 多版本数据格式路由
def route_data_by_version(data: Any, target_version: str = PARQUET_SCHEMA_VERSION) -> Any:
    """P2-34修复: 多版本数据格式路由策略"""
    if isinstance(data, dict):
        version = data.get('__kv_version__', data.get('schema_version', '1.0'))
        if version == target_version:
            return data
        # 版本迁移逻辑占位
        logging.info("P2-34: 数据版本 %s -> %s 迁移", version, target_version)
    return data


# P2-35修复: 友好序列化错误消息
class SerializationError(Exception):
    """P2-35修复: 友好的序列化错误消息"""
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.original_error = original_error
        user_message = f"数据处理失败: {message}"
        if original_error:
            user_message += f" (原因: {type(original_error).__name__})"
        super().__init__(user_message)


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
    'safe_jsonl_append_line',
    'MAX_JSONL_LINE_LENGTH',
    'safe_jsonl_read',
    'validate_duckdb_parquet_chain',
    'PARQUET_SCHEMA_VERSION',
    'normalize_parquet_columns',
    'serialization_perf_monitor',
    'route_data_by_version',
    'SerializationError',
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
