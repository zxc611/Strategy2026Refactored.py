# MODULE_ID: M2-459
import pytest
import ast
import threading
from unittest.mock import MagicMock, patch
from ali2026v3_trading.config._params_attribute_matrix import AttributeMatrixService, AttributeMatrixMixin


class TestAttributeMatrixServiceInit:
    def test_init_no_params_service(self):
        svc = AttributeMatrixService()
        assert svc._params_service is None

    def test_init_with_params_service(self):
        mock_ps = MagicMock()
        svc = AttributeMatrixService(params_service=mock_ps)
        assert svc._params_service is mock_ps

    def test_getattr_delegation(self):
        mock_ps = MagicMock()
        mock_ps.custom_method.return_value = 42
        svc = AttributeMatrixService(params_service=mock_ps)
        assert svc.custom_method() == 42

    def test_getattr_no_params_service(self):
        svc = AttributeMatrixService()
        with pytest.raises(AttributeError):
            svc.nonexistent_attr


class TestCheckType:
    def test_int_type(self):
        assert AttributeMatrixService._check_type(5, 'int') is True

    def test_int_type_float_value(self):
        assert AttributeMatrixService._check_type(5.0, 'int') is False

    def test_float_type_int_value(self):
        assert AttributeMatrixService._check_type(5, 'float') is True

    def test_float_type_float_value(self):
        assert AttributeMatrixService._check_type(5.0, 'float') is True

    def test_bool_type(self):
        assert AttributeMatrixService._check_type(True, 'bool') is True

    def test_bool_type_int_not_bool(self):
        assert AttributeMatrixService._check_type(1, 'bool') is False

    def test_str_type(self):
        assert AttributeMatrixService._check_type("hello", 'str') is True

    def test_unknown_type(self):
        assert AttributeMatrixService._check_type(5, 'custom') is True


class TestCheckDependencyConstraint:
    def test_calm_period_constraint_pass(self):
        result = AttributeMatrixService._check_dependency_constraint(
            'calm_period', 30, 'pause_sec', 20, 'calm_period >= pause_sec'
        )
        assert result is True

    def test_calm_period_constraint_fail(self):
        result = AttributeMatrixService._check_dependency_constraint(
            'calm_period', 10, 'pause_sec', 20, 'calm_period >= pause_sec'
        )
        assert result is False

    def test_stop_profit_constraint_pass(self):
        result = AttributeMatrixService._check_dependency_constraint(
            'stop_profit_ratio', 2.0, 'max_loss_pct', 0.3,
            'stop_profit_ratio * (1 - max_loss_pct) > 1.0'
        )
        assert result is True

    def test_stop_profit_constraint_fail(self):
        result = AttributeMatrixService._check_dependency_constraint(
            'stop_profit_ratio', 1.0, 'max_loss_pct', 0.5,
            'stop_profit_ratio * (1 - max_loss_pct) > 1.0'
        )
        assert result is False

    def test_unknown_constraint(self):
        result = AttributeMatrixService._check_dependency_constraint(
            'key1', 1, 'key2', 2, 'unknown_constraint'
        )
        assert result is True


class TestSafeEvalAst:
    def test_constant(self):
        result = AttributeMatrixService._safe_eval_ast("42", {})
        assert result == 42

    def test_name_lookup(self):
        result = AttributeMatrixService._safe_eval_ast("x + y", {"x": 3, "y": 7})
        assert result == 10

    def test_arithmetic(self):
        result = AttributeMatrixService._safe_eval_ast("2 * 3 + 4", {})
        assert result == 10

    def test_division(self):
        result = AttributeMatrixService._safe_eval_ast("10 / 3", {})
        assert abs(result - 3.333) < 0.01

    def test_modulo(self):
        result = AttributeMatrixService._safe_eval_ast("10 % 3", {})
        assert result == 1

    def test_power(self):
        result = AttributeMatrixService._safe_eval_ast("2 ** 3", {})
        assert result == 8

    def test_unary_neg(self):
        result = AttributeMatrixService._safe_eval_ast("-5", {})
        assert result == -5

    def test_floor_div(self):
        result = AttributeMatrixService._safe_eval_ast("10 // 3", {})
        assert result == 3

    def test_name_error(self):
        with pytest.raises(NameError):
            AttributeMatrixService._safe_eval_ast("undefined_var", {})

    def test_function_call_rejected(self):
        with pytest.raises(ValueError, match="Function calls not allowed"):
            AttributeMatrixService._safe_eval_ast("print(1)", {})

    def test_attribute_access_rejected(self):
        with pytest.raises(ValueError, match="Attribute access not allowed"):
            AttributeMatrixService._safe_eval_ast("x.y", {"x": MagicMock()})

    def test_builtin_min_rejected(self):
        with pytest.raises(ValueError, match="Function calls not allowed"):
            AttributeMatrixService._safe_eval_ast("min(3, 5)", {"min": min})

    def test_builtin_max_rejected(self):
        with pytest.raises(ValueError, match="Function calls not allowed"):
            AttributeMatrixService._safe_eval_ast("max(3, 5)", {"max": max})


class TestValidateExprSafe:
    def test_valid_expr(self):
        assert AttributeMatrixService._validate_expr_safe("base * (1 + x / 100)") is True

    def test_empty_expr(self):
        assert AttributeMatrixService._validate_expr_safe("") is False

    def test_none_expr(self):
        assert AttributeMatrixService._validate_expr_safe(None) is False

    def test_unsafe_chars_semicolon(self):
        assert AttributeMatrixService._validate_expr_safe("x; rm -rf /") is False

    def test_unsafe_chars_bracket(self):
        assert AttributeMatrixService._validate_expr_safe("x[0]") is False

    def test_unsafe_keyword_import(self):
        assert AttributeMatrixService._validate_expr_safe("import os") is False

    def test_unsafe_keyword_exec(self):
        assert AttributeMatrixService._validate_expr_safe("exec('code')") is False

    def test_unsafe_keyword_eval(self):
        assert AttributeMatrixService._validate_expr_safe("eval('1+1')") is False


class TestValidateWithAttributeMatrix:
    def test_no_matrix_loaded(self):
        svc = AttributeMatrixService()
        svc._attribute_matrix = {}
        result = svc.validate_with_attribute_matrix()
        assert result['violations'] == []
        assert 'Attribute matrix not loaded' in result['warnings']

    def test_type_violation(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'my_param': 'not_an_int'}
        svc._attribute_matrix = {
            'my_param': {'type': 'int', 'range': [0, 100]},
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert any('TYPE_VIOLATION' in v for v in result['violations'])

    def test_range_violation(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'my_param': 150}
        svc._attribute_matrix = {
            'my_param': {'type': 'int', 'range': [0, 100]},
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert any('RANGE_VIOLATION' in v for v in result['violations'])

    def test_safety_readonly_warning(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'safety_param': 5}
        svc._attribute_matrix = {
            'safety_param': {'type': 'int', 'editable': 'runtime_readonly', 'layer': 'L1_safety'},
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert any('SAFETY_READONLY' in w for w in result['warnings'])

    def test_intuition_lock_warning(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'gut_param': 0.5}
        svc._attribute_matrix = {
            'gut_param': {'type': 'float', 'source': 'intuition'},
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert any('INTUTION_LOCK' in w for w in result['warnings'])

    def test_dependency_violation(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'calm_period': 5, 'pause_sec': 30}
        svc._attribute_matrix = {
            'dependencies': {
                'calm_period': {'requires': 'pause_sec', 'constraint': 'calm_period >= pause_sec'},
            },
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert any('DEPENDENCY_VIOLATION' in v for v in result['violations'])

    def test_mutex_violation(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'flag_a': True, 'flag_b': True}
        svc._attribute_matrix = {
            'mutual_exclusion': {'rules': [['flag_a', 'flag_b']]},
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert any('MUTEX_VIOLATION' in v for v in result['violations'])

    def test_no_violations(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._params = {'good_param': 50}
        svc._attribute_matrix = {
            'good_param': {'type': 'int', 'range': [0, 100]},
        }
        svc._attribute_matrix_loaded = True
        result = svc.validate_with_attribute_matrix()
        assert result['violations'] == []
        assert result['checked_count'] == 1


class TestLoadAttributeMatrix:
    def test_missing_yaml_file(self):
        svc = AttributeMatrixService()
        result = svc.load_attribute_matrix(yaml_path='/nonexistent/path.yaml')
        assert result['violations'] == []
        assert result['checked_count'] == 0


class TestEvalDynamicParam:
    def _make_svc_with_get(self):
        mock_ps = MagicMock()
        mock_ps.get.return_value = 0
        svc = AttributeMatrixService(params_service=mock_ps)
        return svc

    def test_no_matrix_loaded(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = False
        svc._lock = threading.Lock()
        svc._params = {'my_param': 42}
        svc._params_service.get.return_value = 42
        result = svc.eval_dynamic_param('my_param')
        assert result == 42

    def test_no_dynamic_expr(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = True
        svc._lock = threading.Lock()
        svc._params = {'my_param': 42}
        svc._attribute_matrix = {'my_param': {'type': 'int'}}
        svc._params_service.get.return_value = 42
        result = svc.eval_dynamic_param('my_param')
        assert result == 42

    def test_dynamic_expr_evaluation(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = True
        svc._lock = threading.Lock()
        svc._params = {'base': 10, 'iv_percentile': 65}
        svc._attribute_matrix = {
            'signal_cooldown_sec': {
                'dynamic_expr': 'base * (1 + (iv_percentile - 50) / 100)',
                'dynamic_depends': ['base', 'iv_percentile'],
            },
        }
        svc._params_service.get.return_value = 10
        result = svc.eval_dynamic_param('signal_cooldown_sec')
        assert result == 11.5

    def test_dynamic_expr_missing_dep(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = True
        svc._lock = threading.Lock()
        svc._params = {}
        svc._attribute_matrix = {
            'my_param': {
                'dynamic_expr': 'base * 2',
                'dynamic_depends': ['base'],
            },
        }
        svc._params_service.get.return_value = 0
        result = svc.eval_dynamic_param('my_param')
        assert result == 0

    def test_dynamic_expr_unsafe_rejected(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = True
        svc._lock = threading.Lock()
        svc._params = {'x': 1}
        svc._attribute_matrix = {
            'my_param': {
                'dynamic_expr': 'x[0]',
                'dynamic_depends': ['x'],
            },
        }
        svc._params_service.get.return_value = 0
        result = svc.eval_dynamic_param('my_param')
        assert result == 0

    def test_dynamic_expr_with_clamping(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = True
        svc._lock = threading.Lock()
        svc._params = {'base': 200}
        svc._attribute_matrix = {
            'my_param': {
                'dynamic_expr': 'base',
                'dynamic_depends': ['base'],
                'range': [0, 100],
            },
        }
        svc._params_service.get.return_value = 100
        result = svc.eval_dynamic_param('my_param')
        assert result == 100

    def test_dynamic_expr_int_coercion(self):
        svc = self._make_svc_with_get()
        svc._attribute_matrix_loaded = True
        svc._lock = threading.Lock()
        svc._params = {'base': 10}
        svc._attribute_matrix = {
            'my_param': {
                'dynamic_expr': 'base * 1.5',
                'dynamic_depends': ['base'],
                'type': 'int',
            },
        }
        svc._params_service.get.return_value = 15
        result = svc.eval_dynamic_param('my_param')
        assert result == 15


class TestGetAttributeMatrix:
    def test_returns_copy(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._attribute_matrix = {'key': 'value'}
        result = svc.get_attribute_matrix()
        assert result == {'key': 'value'}
        result['key'] = 'modified'
        assert svc._attribute_matrix['key'] == 'value'


class TestGetRoutePriority:
    def test_with_route_priority(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._attribute_matrix = {'route_priority': {'param_a': 1, 'param_b': 2, 'param_c': 'invalid'}}
        result = svc.get_route_priority()
        assert result == {'param_a': 1, 'param_b': 2}

    def test_no_route_priority(self):
        svc = AttributeMatrixService()
        svc._lock = threading.Lock()
        svc._attribute_matrix = {}
        result = svc.get_route_priority()
        assert result == {}


class TestAttributeMatrixMixinAlias:
    def test_alias(self):
        assert AttributeMatrixMixin is AttributeMatrixService