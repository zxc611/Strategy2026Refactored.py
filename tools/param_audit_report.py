"""参数三源审计工具 — YAML配置 vs Python代码默认值一致性检查

三源定义:
  1. YAML参数源: config/*.yaml + param_pool/*.yaml 中的参数键/默认值
  2. 代码默认值源: config/config_params.py DEFAULT_PARAM_TABLE 中的参数键/默认值
  3. 属性矩阵源: config/params.yaml parameter_attributes + param_pool/parameter_attribute_matrix.yaml

本工具对比源1(YAML)和源2(代码默认值)，报告不一致项。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# 项目根目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


from ali2026v3_trading.infra.serialization_utils import yaml_safe_load

def _yaml_safe_load_file(path: str) -> Dict[str, Any]:
    """安全加载YAML文件，失败返回空字典"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml_safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[WARN] 加载YAML失败 {path}: {e}")
        return {}


def _extract_yaml_params_from_cascade_config(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 cascade_config.yaml 提取参数键和值"""
    result = {}
    # gate1/gate2/gate3 参数
    for gate_prefix in ("gate1_", "gate2_", "gate3_"):
        for key, val in data.items():
            if key.startswith(gate_prefix) and isinstance(val, dict):
                # 如 gate1_profit_ratio: {min: 1.8, range: [...], ...}
                if "min" in val:
                    result[key] = val["min"]
                elif "max" in val:
                    result[key] = val["max"]
    # scoring 权重
    for scoring_key in ("scoring", "scoring_small"):
        scoring = data.get(scoring_key, {})
        if isinstance(scoring, dict):
            for k, v in scoring.items():
                if k == "description":
                    continue
                if isinstance(v, (int, float)):
                    result[k] = v
    # data_quality
    dq = data.get("data_quality", {})
    if isinstance(dq, dict):
        for k, v in dq.items():
            if isinstance(v, (int, float)):
                result[k] = v
    # estimation
    est = data.get("estimation", {})
    if isinstance(est, dict):
        for k, v in est.items():
            if isinstance(v, (int, float)):
                result[k] = v
    # sigmoid
    sig = data.get("sigmoid", {})
    if isinstance(sig, dict):
        for k, v in sig.items():
            if k == "range":
                continue
            if isinstance(v, (int, float)):
                result[k] = v
    # alpha_decay
    ad = data.get("alpha_decay", {})
    if isinstance(ad, dict):
        for k, v in ad.items():
            if isinstance(v, (int, float)):
                result[k] = v
    return result


def _extract_yaml_params_from_threshold_grid(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 cascade_threshold_grid.yaml 提取参数键"""
    result = {}
    # grid 定义
    for key, val in data.items():
        if key.endswith("_grid") and isinstance(val, dict):
            # 如 gate1_min_profit_ratio_grid: {values: [...], count: 17}
            pass  # grid参数不提取默认值
        # strategy_preset_thresholds
    presets = data.get("strategy_preset_thresholds", {})
    if isinstance(presets, dict):
        for strategy, params in presets.items():
            if isinstance(params, dict):
                for k, v in params.items():
                    # 高频策略的预设值作为参考默认值
                    if strategy == "high_freq" and isinstance(v, (int, float)):
                        result[k] = v
    return result


def _extract_yaml_params_from_params(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 config/params.yaml 提取参数键和默认值

    params.yaml 包含:
    - 顶层分组(shadow_strategy, risk, backtest, judgment)
    - parameter_attributes 下每个参数的 default 字段
    """
    result = {}
    # 顶层分组参数
    for section_key in ("shadow_strategy", "risk", "backtest", "judgment"):
        section = data.get(section_key, {})
        if isinstance(section, dict):
            for k, v in section.items():
                if isinstance(v, (int, float, bool, str)):
                    result[k] = v
    # parameter_attributes — 最权威的参数默认值来源
    pa = data.get("parameter_attributes", {})
    if isinstance(pa, dict):
        for param_name, attr in pa.items():
            if isinstance(attr, dict) and "default" in attr:
                result[param_name] = attr["default"]
    return result


def _extract_yaml_params_from_tvf(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 param_pool/param_configs.yaml 的 tvf_params section 提取参数键和默认值"""
    result = {}
    tvf = data.get("tvf_params", data)
    for k in ("tvf_enabled", "tvf_sigmoid_scale"):
        if k in tvf:
            result[k] = tvf[k]
    # layer_weights
    lw = tvf.get("layer_weights", {})
    if isinstance(lw, dict):
        for k, v in lw.items():
            if isinstance(v, (int, float)):
                result[f"tvf_l{k[1]}_weight" if k.startswith("l") else k] = v
    # l1_tri_validation
    l1 = tvf.get("l1_tri_validation", {})
    if isinstance(l1, dict):
        for k, v in l1.items():
            if k == "inner_weights":
                if isinstance(v, dict):
                    for ik, iv in v.items():
                        if isinstance(iv, (int, float)):
                            result[f"tvf_l1_inner_{ik}_weight"] = iv
            elif isinstance(v, (int, float)):
                result[f"tvf_{k}"] = v
    # l2_order_flow
    l2 = tvf.get("l2_order_flow", {})
    if isinstance(l2, dict):
        for k, v in l2.items():
            if k == "inner_weights":
                if isinstance(v, dict):
                    for ik, iv in v.items():
                        if isinstance(iv, (int, float)):
                            result[f"tvf_l2_inner_{ik}_weight"] = iv
            elif isinstance(v, (int, float)):
                result[f"tvf_{k}"] = v
    # l3_greeks
    l3 = tvf.get("l3_greeks", {})
    if isinstance(l3, dict):
        for k, v in l3.items():
            if k == "inner_weights":
                if isinstance(v, dict):
                    for ik, iv in v.items():
                        if isinstance(iv, (int, float)):
                            result[f"tvf_l3_inner_{ik}_weight"] = iv
            elif isinstance(v, (int, float)):
                result[f"tvf_{k}"] = v
    return result


def _extract_yaml_params_from_attribute_matrix(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 param_pool/param_configs.yaml 的 parameter_attributes section 提取参数默认值"""
    result = {}
    pam = data.get("parameter_attributes", data)
    for param_name, attr in pam.items():
        if isinstance(attr, dict) and "default" in attr:
            result[param_name] = attr["default"]
    return result


def _extract_yaml_params_from_judgment_scoring(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 config/judgment_scoring_config.yaml 提取参数"""
    result = {}
    jsc = data.get("judgment_scoring_coefficients", {})
    if isinstance(jsc, dict):
        for k, v in jsc.items():
            if isinstance(v, (int, float)):
                result[k] = v
    return result


def _extract_yaml_params_from_plr(data: Dict[str, Any]) -> Dict[str, Any]:
    """从 param_pool/param_configs.yaml 的 plr_thresholds section 提取参数"""
    result = {}
    plr = data.get("plr_thresholds", data.get("plr_thresholds", {}))
    if isinstance(plr, dict):
        for size, params in plr.items():
            if isinstance(params, dict):
                for k, v in params.items():
                    if isinstance(v, (int, float, bool)):
                        result[f"plr_{size}_{k}"] = v
    return result


def collect_yaml_params() -> Dict[str, Any]:
    """收集所有YAML文件中的参数键和默认值"""
    config_dir = _PROJECT_ROOT / "config"
    param_pool_dir = _PROJECT_ROOT / "param_pool"

    all_yaml_params = {}

    # 1. config/cascade_config.yaml
    data = _yaml_safe_load_file(str(config_dir / "cascade_config.yaml"))
    params = _extract_yaml_params_from_cascade_config(data)
    all_yaml_params.update(params)

    # 2. config/cascade_threshold_grid.yaml
    data = _yaml_safe_load_file(str(config_dir / "cascade_threshold_grid.yaml"))
    params = _extract_yaml_params_from_threshold_grid(data)
    all_yaml_params.update(params)

    # 3. config/params.yaml — 最权威的YAML参数源
    data = _yaml_safe_load_file(str(config_dir / "params.yaml"))
    params = _extract_yaml_params_from_params(data)
    all_yaml_params.update(params)

    # 4. param_pool/tvf_params.yaml
    data = _yaml_safe_load_file(str(param_pool_dir / "param_configs.yaml"))
    params = _extract_yaml_params_from_tvf(data)
    all_yaml_params.update(params)

    # 5. param_pool/parameter_attribute_matrix.yaml
    data = _yaml_safe_load_file(str(param_pool_dir / "param_configs.yaml"))
    params = _extract_yaml_params_from_attribute_matrix(data)
    all_yaml_params.update(params)

    # 6. config/judgment_scoring_config.yaml
    data = _yaml_safe_load_file(str(config_dir / "judgment_scoring_config.yaml"))
    params = _extract_yaml_params_from_judgment_scoring(data)
    all_yaml_params.update(params)

    # 7. param_pool/plr_thresholds.yaml
    data = _yaml_safe_load_file(str(param_pool_dir / "param_configs.yaml"))
    params = _extract_yaml_params_from_plr(data)
    all_yaml_params.update(params)

    return all_yaml_params


def collect_code_defaults() -> Dict[str, Any]:
    """收集Python代码中的默认参数值 (DEFAULT_PARAM_TABLE + CENTRALIZED_DEFAULTS)"""
    code_params = {}

    # 尝试从config_params导入DEFAULT_PARAM_TABLE
    try:
        # 添加项目根目录到sys.path
        if str(_PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(_PROJECT_ROOT))
        if str(_PROJECT_ROOT.parent) not in sys.path:
            sys.path.insert(0, str(_PROJECT_ROOT.parent))

        from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE
        for k, v in DEFAULT_PARAM_TABLE.items():
            # 跳过不可序列化的值
            if isinstance(v, (int, float, bool, str, list, tuple, type(None))):
                code_params[k] = v
    except Exception as e:
        print(f"[WARN] 无法导入DEFAULT_PARAM_TABLE: {e}")
        # 回退: 直接解析config_params.py
        code_params = _parse_default_param_table_from_file()

    return code_params


def _parse_default_param_table_from_file() -> Dict[str, Any]:
    """回退方案: 直接解析config_params.py文件提取DEFAULT_PARAM_TABLE的键"""
    result = {}
    config_params_path = _PROJECT_ROOT / "config" / "config_params.py"
    if not config_params_path.exists():
        return result

    try:
        content = config_params_path.read_text(encoding="utf-8")
        # 提取 DEFAULT_PARAM_TABLE = { ... } 中的字符串键
        in_table = False
        for line in content.splitlines():
            stripped = line.strip()
            if "DEFAULT_PARAM_TABLE = {" in stripped:
                in_table = True
                continue
            if in_table:
                if stripped.startswith("}"):
                    break
                # 匹配 "key": value 模式
                if stripped.startswith('"') and ":" in stripped:
                    key_part = stripped.split(":")[0].strip().strip('"').strip("'")
                    if key_part and not key_part.startswith("_"):
                        result[key_part] = "<parsed_from_file>"
    except Exception as e:
        print(f"[WARN] 解析config_params.py失败: {e}")

    return result


def _normalize_value(val: Any) -> str:
    """将值归一化为可比较的字符串"""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, float):
        # 处理浮点精度: 保留4位小数比较
        return f"{val:.4f}"
    if isinstance(val, list):
        return str(sorted(val) if all(isinstance(x, (int, float)) for x in val) else val)
    return str(val)


def _values_match(yaml_val: Any, code_val: Any) -> bool:
    """比较两个值是否匹配（考虑类型差异）"""
    # None/null 比较
    if yaml_val is None and code_val is None:
        return True
    if yaml_val is None or code_val is None:
        return False

    # 布尔比较
    if isinstance(yaml_val, bool) and isinstance(code_val, bool):
        return yaml_val == code_val
    if isinstance(yaml_val, bool) or isinstance(code_val, bool):
        # bool vs int 的特殊处理
        if isinstance(yaml_val, bool):
            return int(yaml_val) == code_val
        if isinstance(code_val, bool):
            return yaml_val == int(code_val)

    # 数值比较 (int vs float)
    if isinstance(yaml_val, (int, float)) and isinstance(code_val, (int, float)):
        return abs(float(yaml_val) - float(code_val)) < 1e-6

    # 字符串比较
    if isinstance(yaml_val, str) and isinstance(code_val, str):
        return yaml_val == code_val

    # 列表比较
    if isinstance(yaml_val, list) and isinstance(code_val, list):
        return _normalize_value(yaml_val) == _normalize_value(code_val)

    return _normalize_value(yaml_val) == _normalize_value(code_val)


def run_audit() -> Dict[str, Any]:
    """执行参数三源审计

    Returns:
        dict 包含:
        - yaml_params: YAML中的参数名列表
        - code_defaults: 代码中的参数名列表
        - in_both: 两个源都有的参数名列表
        - yaml_only: 仅YAML中存在的参数名列表
        - code_only: 仅代码中存在的参数名列表
        - value_mismatches: 值不一致的参数列表
          [{param: str, yaml_value: Any, code_value: Any}, ...]
    """
    yaml_params = collect_yaml_params()
    code_defaults = collect_code_defaults()

    yaml_keys = set(yaml_params.keys())
    code_keys = set(code_defaults.keys())

    in_both = sorted(yaml_keys & code_keys)
    yaml_only = sorted(yaml_keys - code_keys)
    code_only = sorted(code_keys - yaml_keys)

    # 比较值
    value_mismatches = []
    for key in in_both:
        yv = yaml_params[key]
        cv = code_defaults[key]
        if not _values_match(yv, cv):
            value_mismatches.append({
                "param": key,
                "yaml_value": yv,
                "code_value": cv,
            })

    # 按参数名排序
    value_mismatches.sort(key=lambda x: x["param"])

    return {
        "yaml_params": sorted(yaml_keys),
        "code_defaults": sorted(code_keys),
        "in_both": in_both,
        "yaml_only": yaml_only,
        "code_only": code_only,
        "value_mismatches": value_mismatches,
    }


if __name__ == "__main__":
    result = run_audit()

    print("=" * 70)
    print("参数三源审计报告")
    print("=" * 70)

    print(f"\nYAML参数总数: {len(result['yaml_params'])}")
    print(f"代码默认值总数: {len(result['code_defaults'])}")
    print(f"两源共有参数: {len(result['in_both'])}")
    print(f"仅YAML存在: {len(result['yaml_only'])}")
    print(f"仅代码存在: {len(result['code_only'])}")
    print(f"值不一致: {len(result['value_mismatches'])}")

    if result["yaml_only"]:
        print(f"\n--- 仅YAML存在的参数 ({len(result['yaml_only'])}个) ---")
        for p in result["yaml_only"][:50]:
            print(f"  {p}")
        if len(result["yaml_only"]) > 50:
            print(f"  ... 还有 {len(result['yaml_only']) - 50} 个")

    if result["code_only"]:
        print(f"\n--- 仅代码存在的参数 ({len(result['code_only'])}个) ---")
        for p in result["code_only"][:50]:
            print(f"  {p}")
        if len(result["code_only"]) > 50:
            print(f"  ... 还有 {len(result['code_only']) - 50} 个")

    if result["value_mismatches"]:
        print(f"\n--- 值不一致的参数 ({len(result['value_mismatches'])}个) ---")
        for m in result["value_mismatches"]:
            yv = m["yaml_value"]
            cv = m["code_value"]
            # 截断过长的值显示
            yv_str = str(yv)[:60] + "..." if len(str(yv)) > 60 else str(yv)
            cv_str = str(cv)[:60] + "..." if len(str(cv)) > 60 else str(cv)
            print(f"  {m['param']}: YAML={yv_str} | Code={cv_str}")
    else:
        print("\n所有共有参数值一致 ✓")

    print("\n" + "=" * 70)
    print("审计完成")
