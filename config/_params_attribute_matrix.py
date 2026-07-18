# [M1-91] 参数属性矩阵验证
# MODULE_ID: M1-003

"""参数服务 - 属性矩阵验验(AttributeMatrixMixin)"""



from __future__ import annotations



import ast

import logging

import os

from typing import Any, Dict, List, Optional



from infra.serialization_utils import yaml_safe_load





class AttributeMatrixService:

    """参数服务 - 属性矩阵验证（从AttributeMatrixMixin重构为独立Service_"""



    def __init__(self, params_service=None):

        self._params_service = params_service



    def __getattr__(self, name):

        if self._params_service is not None and hasattr(self._params_service, name):

            return getattr(self._params_service, name)

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def load_attribute_matrix(self, yaml_path: Optional[str] = None) -> Dict[str, Any]:

        """加载 parameter_attribute_matrix.yaml 并校验当前参数


        YAML为主源, JSON仅作fallback。计划统一到YAML单一。
        Returns:

            校验报告 dict，含 violations/warnings/checked_count

        """

        if yaml_path is None:

            # 优先查找参数池子目录，回退到模块根目录

            _base_dir = os.path.dirname(os.path.abspath(__file__))

            _project_dir = os.path.dirname(_base_dir)

            _pool_path = os.path.join(_project_dir, 'param_pool', 'param_configs.yaml')

            _root_path = os.path.join(_base_dir, 'param_configs.yaml')

            yaml_path = _pool_path if os.path.exists(_pool_path) else _root_path



        try:

            import yaml as _yaml

        except ImportError:

            logging.warning("[ParamsService] PyYAML not installed, cannot load attribute matrix")

            return {'violations': [], 'warnings': ['PyYAML not installed'], 'checked_count': 0}



        try:

            with open(yaml_path, 'r', encoding='utf-8') as f:

                _raw = yaml_safe_load(f) or {}

            self._attribute_matrix = _raw.get('parameter_attributes', _raw)

            self._attribute_matrix_loaded = True

            logging.info("[ParamsService] Loaded attribute matrix from %s (%d top-level keys)",

                         yaml_path, len(self._attribute_matrix))

        except FileNotFoundError:

            logging.debug("[ParamsService] Attribute matrix config missing: %s; using empty validation result", yaml_path)

            return {'violations': [], 'warnings': ['Attribute matrix config missing'], 'checked_count': 0}

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

            logging.info("[ParamsService] Failed to load attribute matrix, using empty validation result: %s", e)

            return {'violations': [], 'warnings': [f'Load failed: {e}'], 'checked_count': 0}



        return self.validate_with_attribute_matrix()



    def validate_with_attribute_matrix(self) -> Dict[str, Any]:

        """基于 attribute_matrix 校验当前 _params 中的所有参数


        校验验

        1. 类型校验 (type)

        2. 范围校验 (range)

        3. 依赖约束 (dependencies)

        4. 互斥规则 (mutual_exclusion)

        5. L1_safety 层运行时只读校验 (editable=runtime_readonly)



        Returns:

            {'violations': [...], 'warnings': [...], 'checked_count': int}

        """

        if not self._attribute_matrix:

            return {'violations': [], 'warnings': ['Attribute matrix not loaded'], 'checked_count': 0}



        violations = []

        warnings = []

        checked = 0



        with self._lock:

            params_snapshot = dict(self._params)



        for key, attr in self._attribute_matrix.items():

            if not isinstance(attr, dict):

                continue

            if key in ('mutual_exclusion', 'dependencies', 'route_priority'):

                continue



            value = params_snapshot.get(key)

            if value is None:

                continue



            checked += 1

            expected_type = attr.get('type')

            value_range = attr.get('range')

            editable = attr.get('editable')

            layer = attr.get('layer', '')



            if expected_type and not self._check_type(value, expected_type):

                violations.append(

                    f"TYPE_VIOLATION | {key}: expected {expected_type}, got {type(value).__name__} = {value!r}"

                )



            if value_range and isinstance(value_range, list) and len(value_range) == 2:

                if isinstance(value, (int, float)):

                    lo, hi = value_range

                    if not (lo <= value <= hi):

                        violations.append(

                            f"RANGE_VIOLATION | {key}: {value} not in [{lo}, {hi}]"

                        )



            if editable == 'runtime_readonly' and layer == 'L1_safety':

                warnings.append(

                    f"SAFETY_READONLY | {key}={value} (layer={layer}, should not be modified at runtime)"

                )



            source = attr.get('source')

            if source == 'intuition':

                warnings.append(

                    f"INTUTION_LOCK | {key}={value}: source=intuition, 不可锁定为生产值（铁律：无量化来源的参数不得锁定）"

                )



        dep_section = self._attribute_matrix.get('dependencies', {})

        if isinstance(dep_section, dict):

            for param_key, dep_info in dep_section.items():

                if not isinstance(dep_info, dict):

                    continue

                constraint = dep_info.get('constraint', '')

                val = params_snapshot.get(param_key)

                requires_key = dep_info.get('requires', '')

                req_val = params_snapshot.get(requires_key)

                if val is not None and req_val is not None and constraint:

                    ok = self._check_dependency_constraint(param_key, val, requires_key, req_val, constraint)

                    if not ok:

                        violations.append(

                            f"DEPENDENCY_VIOLATION | {constraint} | {param_key}={val}, {requires_key}={req_val}"

                        )



        mutex_section = self._attribute_matrix.get('mutual_exclusion', {})

        if isinstance(mutex_section, dict):

            for rule in mutex_section.get('rules', []) if isinstance(mutex_section.get('rules'), list) else []:

                if isinstance(rule, list) and len(rule) >= 2:

                    active = [k for k in rule if params_snapshot.get(k) in (True, 1, 'true')]

                    if len(active) > 1:

                        violations.append(

                            f"MUTEX_VIOLATION | {active} cannot be active simultaneously"

                        )

            mutex_list = mutex_section if isinstance(mutex_section, list) else []

            for item in mutex_list:

                if isinstance(item, list) and len(item) >= 2:

                    active = [k for k in item if params_snapshot.get(k) in (True, 1, 'true')]

                    if len(active) > 1:

                        violations.append(

                            f"MUTEX_VIOLATION | {active} cannot be active simultaneously"

                        )



        if violations:

            logging.error("[ParamsService] Attribute matrix validation FAILED: %d violations",

                          len(violations))

            for v in violations:

                logging.error("  %s", v)

        if warnings:

            logging.warning("[ParamsService] Attribute matrix warnings: %d", len(warnings))

            for w in warnings:

                logging.warning("  %s", w)

        if not violations and not warnings:

            logging.info("[ParamsService] Attribute matrix validation PASSED (%d params checked)", checked)



        return {'violations': violations, 'warnings': warnings, 'checked_count': checked}



    def get_attribute_matrix(self) -> Dict[str, Any]:

        with self._lock:

            return dict(self._attribute_matrix)



    def get_route_priority(self) -> Dict[str, int]:

        with self._lock:

            rp = self._attribute_matrix.get('route_priority', {})

            return {k: v for k, v in rp.items() if isinstance(v, int)}



    # ========================================================================

    # 动态参数表达式

    # ========================================================================



    _SAFE_BUILTINS = {

        'min': min, 'max': max, 'abs': abs,

        'round': round, 'pow': pow,

    }

    

    _ALLOWED_EXPR_CHARS = frozenset('0123456789.+-*/() %')



    _SAFE_BIN_OPS = {

        ast.Add: lambda a, b: a + b,

        ast.Sub: lambda a, b: a - b,

        ast.Mult: lambda a, b: a * b,

        ast.Div: lambda a, b: a / b,

        ast.Mod: lambda a, b: a % b,

        ast.Pow: lambda a, b: a ** b,

        ast.FloorDiv: lambda a, b: a // b,

    }



    _SAFE_UNARY_OPS = {

        ast.USub: lambda a: -a,

        ast.UAdd: lambda a: +a,

    }



    @classmethod

    def _safe_eval_ast(cls, expr: str, namespace: Dict[str, Any]) -> Any:

        tree = ast.parse(expr, mode='eval')

        return cls._eval_ast_node(tree.body, namespace)



    @classmethod

    def _eval_ast_node(cls, node: ast.AST, namespace: Dict[str, Any]) -> Any:

        if isinstance(node, ast.Constant):

            return node.value

        if isinstance(node, ast.Name):

            if node.id in namespace:

                return namespace[node.id]

            raise NameError(f"name '{node.id}' is not defined")

        if isinstance(node, ast.BinOp):

            op_func = cls._SAFE_BIN_OPS.get(type(node.op))

            if op_func is None:

                raise ValueError(f"Unsupported operator: {type(node.op).__name__}")

            return op_func(cls._eval_ast_node(node.left, namespace),

                           cls._eval_ast_node(node.right, namespace))

        if isinstance(node, ast.UnaryOp):

            op_func = cls._SAFE_UNARY_OPS.get(type(node.op))

            if op_func is None:

                raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

            return op_func(cls._eval_ast_node(node.operand, namespace))

        if isinstance(node, ast.Call):

            raise ValueError('Function calls not allowed in dynamic expressions')  # P1-30: safe-eval security enhancement

        if isinstance(node, ast.Attribute):

            raise ValueError("Attribute access not allowed in dynamic expressions")

        raise ValueError(f"Unsupported AST node: {type(node).__name__}")



    _DANGEROUS_KEYWORDS = frozenset({

        'import', 'exec', 'eval', 'compile', 'open', 'input',

        '__import__', 'globals', 'locals', 'getattr', 'setattr',

        'delattr', 'hasattr', 'dir', 'vars', 'type',

    })



    @classmethod

    def _validate_expr_safe(cls, expr: str) -> bool:

        if not expr or not isinstance(expr, str):

            return False

        allowed = cls._ALLOWED_EXPR_CHARS | frozenset('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_')

        if not all(c in allowed for c in expr):

            return False

        expr_lower = expr.lower()

        for kw in cls._DANGEROUS_KEYWORDS:

            if kw in expr_lower:

                return False

        return True



    def eval_dynamic_param(self, key: str, context: Optional[Dict[str, Any]] = None) -> Any:

        """根据动态表达式计算参数据


        当参数的 attribute_matrix 中定义了 `dynamic_expr` 时，

        根据运行时context（如实时波动率、时间、持仓比例等）计算参数值值
        若计算失败或表达式不存在，降级返回静默default 值起


        dynamic_expr 示例（在 YAML 中定义）_
            signal_cooldown_sec:

              dynamic_expr: "base * (1 + (iv_percentile - 50) / 100)"

              dynamic_depends: [base, iv_percentile]



        Args:

            key: 参数键名

            context: 运行时变量字典，_{'iv_percentile': 65, 'volatility': 0.25}



        Returns:

            计算后的参数值；无动态表达式则返回当前静态由
        """

        if not self._attribute_matrix_loaded:

            return self.get(key)



        attr = self._attribute_matrix.get(key)

        if not isinstance(attr, dict):

            return self.get(key)



        dynamic_expr = attr.get('dynamic_expr')

        if not dynamic_expr:

            return self.get(key)



        depends = attr.get('dynamic_depends', [])

        eval_ns: Dict[str, Any] = dict(self._SAFE_BUILTINS)



        with self._lock:

            params_snapshot = dict(self._params)



        for dep_key in depends:

            if dep_key in (context or {}):

                eval_ns[dep_key] = context[dep_key]

            elif dep_key in params_snapshot:

                eval_ns[dep_key] = params_snapshot[dep_key]

            else:

                logging.warning(

                    "[ParamsService] eval_dynamic_param: missing dep '%s' for '%s', "

                    "falling back to static value", dep_key, key,

                )

                return self.get(key)



        try:

            if not self._validate_expr_safe(dynamic_expr):

                logging.error(

                    "[ParamsService] eval_dynamic_param REJECTED unsafe expr for '%s': %r",

                    key, dynamic_expr,

                )

                return self.get(key)

            result = self._safe_eval_ast(dynamic_expr, eval_ns)

            logging.info(

                "[ParamsService] eval_dynamic_param OK for '%s': expr=%r result=%r",

                key, dynamic_expr, result,

            )

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error(

                "[ParamsService] eval_dynamic_param FAILED for '%s': expr=%r error=%s",

                key, dynamic_expr, e,

            )

            return self.get(key)



        value_range = attr.get('range')

        if value_range and isinstance(value_range, list) and len(value_range) == 2:

            if isinstance(result, (int, float)):

                lo, hi = value_range

                if not (lo <= result <= hi):

                    logging.warning(

                        "[ParamsService] eval_dynamic_param CLAMPED for '%s': "

                        "%r not in [%s, %s]", key, result, lo, hi,

                    )

                    result = max(lo, min(hi, result))



        expected_type = attr.get('type')

        if expected_type == 'int' and isinstance(result, float):

            result = int(round(result))



        return result



    @staticmethod

    def _check_type(value: Any, expected: str) -> bool:

        type_map = {'int': int, 'float': (int, float), 'bool': bool, 'str': str}

        py_type = type_map.get(expected)

        if py_type is None:

            return True

        if expected == 'float':

            return isinstance(value, (int, float))

        if expected == 'bool':

            return isinstance(value, bool)

        return isinstance(value, py_type)



    @staticmethod

    def _check_dependency_constraint(key1: str, val1: Any, key2: str, val2: Any,

                                      constraint: str) -> bool:

        try:

            if 'calm_period >= pause_sec' in constraint:

                return float(val1) >= float(val2)

            if 'stop_profit_ratio * (1 - max_loss_pct) > 1.0' in constraint:

                return float(val1) * (1 - float(val2)) > 1.0

        except (TypeError, ValueError):

            return False

        return True



    # ========================================================================

    # 输出控制

    # ========================================================================





AttributeMatrixMixin = AttributeMatrixService

