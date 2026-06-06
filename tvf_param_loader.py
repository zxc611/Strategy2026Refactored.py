"""
TVF参数加载器 — 从参数池配置文件加载六维仓位调整因子参数

支持:
  - 从YAML配置文件加载参数
  - 参数校验 (权重和检查、阈值范围检查)
  - 动态重载 (运行时更新参数)
  - 多模式支持 (small/medium/large可配置不同参数)
"""

import logging
import os
from typing import Any, Dict, Optional, Tuple

from ali2026v3_trading.serialization_utils import yaml_safe_load

try:
    import yaml
except ImportError:
    yaml = None

logger = logging.getLogger(__name__)


TVF_DEFAULT_PARAMS = {
    "tvf_l1_weight": 0.40,
    "tvf_l2_weight": 0.35,
    "tvf_l3_weight": 0.25,
    "tvf_sortino_threshold": 1.5,
    "tvf_calmar_threshold": 0.8,
    "tvf_sharpe_threshold": 1.2,
    "tvf_gamma_threshold": 0.05,
    "tvf_theta_threshold": -0.02,
    "tvf_vega_threshold": 0.10,
    "tvf_sortino_scale": 0.50,
    "tvf_calmar_scale": 0.30,
    "tvf_sharpe_scale": 0.40,
    "tvf_ofi_scale": 3.00,
    "tvf_smf_scale": 2.00,
    "tvf_gamma_scale": 0.02,
    "tvf_theta_scale": 0.01,
    "tvf_vega_scale": 0.05,
    "tvf_l1_inner_sortino_weight": 0.40,
    "tvf_l1_inner_calmar_weight": 0.30,
    "tvf_l1_inner_sharpe_weight": 0.30,
    "tvf_l2_inner_ofi_weight": 0.40,
    "tvf_l2_inner_cvd_weight": 0.30,
    "tvf_l2_inner_smf_weight": 0.30,
    "tvf_l3_inner_delta_weight": 0.30,
    "tvf_l3_inner_gamma_weight": 0.25,
    "tvf_l3_inner_theta_weight": 0.25,
    "tvf_l3_inner_vega_weight": 0.20,
}


class TVFParamLoader:

    _params: Dict[str, Any] = {}
    _loaded: bool = False

    def __new__(cls):
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('tvf_param_loader')
        _inst = _registry.get('instance')
        if _inst is None:
            _inst = super().__new__(cls)
            _registry.set('instance', _inst)
        return _inst

    @classmethod
    def get_instance(cls) -> 'TVFParamLoader':
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('tvf_param_loader')
        _inst = _registry.get('instance')
        if _inst is None:
            _inst = cls()
        return _inst

    @classmethod
    def reset(cls):
        from ali2026v3_trading.singleton_registry import SingletonRegistry
        _registry = SingletonRegistry.get_registry('tvf_param_loader')
        _registry.remove('instance')
        cls._params = {}
        cls._loaded = False

    def load(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """加载TVF参数配置文件

        Args:
            config_path: 配置文件路径，默认使用参数池目录下的tvf_params.yaml

        Returns:
            加载的参数字典
        """
        if config_path is None:
            _base_dir = os.path.dirname(os.path.abspath(__file__))
            _pool_path = os.path.join(_base_dir, "param_pool", "tvf_params.yaml")
            _root_path = os.path.join(_base_dir, "tvf_params.yaml")
            config_path = _pool_path if os.path.exists(_pool_path) else _root_path

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._params = yaml_safe_load(f)
            self._loaded = True
            logger.info('[TVFParamLoader] 参数加载成功: %s', config_path)

            # 校验参数
            self._validate_params()

        except FileNotFoundError:
            logger.warning('[TVFParamLoader] 配置文件不存在: %s，使用默认值', config_path)
            self._params = self._default_params()
            self._loaded = True
        except Exception as e:
            logger.error('[TVFParamLoader] 参数加载失败: %s', e)
            self._params = self._default_params()
            self._loaded = True

        return self._params

    def reload(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """重新加载参数（运行时热更新）"""
        logger.info('[TVFParamLoader] 重新加载参数')
        return self.load(config_path)

    def get_params(self) -> Dict[str, Any]:
        """获取当前加载的参数"""
        if not self._loaded:
            self.load()
        return self._params

    def apply_to_mode_config(self, config) -> 'ModeConfig':
        """将TVF参数应用到ModeConfig

        注意: ModeConfig是frozen的dataclass，需要创建新实例
        """
        from ali2026v3_trading.mode_engine import ModeConfig

        params = self.get_params()

        new_kwargs = {
            'tvf_enabled': params.get('tvf_enabled', True),  # R26-P0-CD-02修复: 三源统一为True(YAML=True, Code=True)
            'tvf_l1_weight': params['layer_weights']['l1_risk_return'],
            'tvf_l2_weight': params['layer_weights']['l2_market_micro'],
            'tvf_l3_weight': params['layer_weights']['l3_option_greeks'],
            'tvf_sortino_threshold': params['l1_tri_validation']['sortino_tri_center'],
            'tvf_calmar_threshold': params['l1_tri_validation']['calmar_tri_center'],
            'tvf_sharpe_threshold': params['l1_tri_validation']['sharpe_tri_center'],
            'tvf_sortino_scale': params['l1_tri_validation']['sortino_tri_scale'],
            'tvf_calmar_scale': params['l1_tri_validation']['calmar_tri_scale'],
            'tvf_sharpe_scale': params['l1_tri_validation']['sharpe_tri_scale'],
            'tvf_ofi_scale': params['l2_order_flow']['ofi_scale'],
            'tvf_smf_scale': params['l2_order_flow']['smf_scale'],
            'tvf_gamma_threshold': params['l3_greeks']['gamma_threshold'],
            'tvf_theta_threshold': params['l3_greeks']['theta_threshold'],
            'tvf_vega_threshold': params['l3_greeks']['vega_threshold'],
            'tvf_gamma_scale': params['l3_greeks']['gamma_scale'],
            'tvf_theta_scale': params['l3_greeks']['theta_scale'],
            'tvf_vega_scale': params['l3_greeks']['vega_scale'],
            'tvf_l1_inner_sortino_weight': params['l1_tri_validation']['inner_weights']['sortino'],
            'tvf_l1_inner_calmar_weight': params['l1_tri_validation']['inner_weights']['calmar'],
            'tvf_l1_inner_sharpe_weight': params['l1_tri_validation']['inner_weights']['sharpe'],
            'tvf_l2_inner_ofi_weight': params['l2_order_flow']['inner_weights']['ofi'],
            'tvf_l2_inner_cvd_weight': params['l2_order_flow']['inner_weights']['cvd_divergence'],
            'tvf_l2_inner_smf_weight': params['l2_order_flow']['inner_weights']['smart_money_flow'],
            'tvf_l3_inner_delta_weight': params['l3_greeks']['inner_weights']['delta'],
            'tvf_l3_inner_gamma_weight': params['l3_greeks']['inner_weights']['gamma'],
            'tvf_l3_inner_theta_weight': params['l3_greeks']['inner_weights']['theta'],
            'tvf_l3_inner_vega_weight': params['l3_greeks']['inner_weights']['vega'],
        }

        import dataclasses
        original_fields = {f.name: getattr(config, f.name) for f in dataclasses.fields(config)}

        original_fields.update(new_kwargs)

        return ModeConfig(**original_fields)

    def _validate_params(self) -> None:
        """校验参数合法性"""
        params = self._params
        errors = []
        warnings = []

        # R24-P2-IV-04修复: 检查必要嵌套key是否存在，给出更明确的错误信息
        _REQUIRED_NESTED_KEYS = {
            'layer_weights': ['l1_risk_return', 'l2_market_micro', 'l3_option_greeks'],
            'l1_tri_validation': ['sortino_tri_scale', 'calmar_tri_scale', 'sharpe_tri_scale'],
            'l2_order_flow': ['ofi_scale', 'smf_scale'],
            'l3_greeks': ['delta_scale', 'gamma_scale', 'theta_scale', 'vega_scale'],
        }
        for _group, _keys in _REQUIRED_NESTED_KEYS.items():
            if _group not in params:
                errors.append(f'缺少必要嵌套key: {_group}')
            else:
                for _k in _keys:
                    if _k not in params[_group]:
                        warnings.append(f'嵌套key {_group}.{_k} 缺失，将使用默认值0')

        # 1. 校验层间权重和
        lw = params.get('layer_weights', {})
        l1_w = lw.get('l1_risk_return', 0)
        l2_w = lw.get('l2_market_micro', 0)
        l3_w = lw.get('l3_option_greeks', 0)
        layer_sum = l1_w + l2_w + l3_w
        if abs(layer_sum - 1.0) > 1e-6:
            errors.append(f'层间权重和不等于1.0: {layer_sum}')

        # 2. 校验L1层内权重和
        l1_inner = params.get('l1_tri_validation', {}).get('inner_weights', {})
        l1_inner_sum = sum(l1_inner.values())
        if abs(l1_inner_sum - 1.0) > 1e-6:
            errors.append(f'L1层内权重和不等于1.0: {l1_inner_sum}')

        # 3. 校验L2层内权重和
        l2_inner = params.get('l2_order_flow', {}).get('inner_weights', {})
        l2_inner_sum = sum(l2_inner.values())
        if abs(l2_inner_sum - 1.0) > 1e-6:
            errors.append(f'L2层内权重和不等于1.0: {l2_inner_sum}')

        # 4. 校验L3层内权重和
        l3_inner = params.get('l3_greeks', {}).get('inner_weights', {})
        l3_inner_sum = sum(l3_inner.values())
        if abs(l3_inner_sum - 1.0) > 1e-6:
            errors.append(f'L3层内权重和不等于1.0: {l3_inner_sum}')

        # 5. 校验scale值必须为正
        scales = [
            ('sortino_tri_scale', params.get('l1_tri_validation', {}).get('sortino_tri_scale', 0)),
            ('calmar_tri_scale', params.get('l1_tri_validation', {}).get('calmar_tri_scale', 0)),
            ('sharpe_tri_scale', params.get('l1_tri_validation', {}).get('sharpe_tri_scale', 0)),
            ('ofi_scale', params.get('l2_order_flow', {}).get('ofi_scale', 0)),
            ('smf_scale', params.get('l2_order_flow', {}).get('smf_scale', 0)),
            ('gamma_scale', params.get('l3_greeks', {}).get('gamma_scale', 0)),
            ('theta_scale', params.get('l3_greeks', {}).get('theta_scale', 0)),
            ('vega_scale', params.get('l3_greeks', {}).get('vega_scale', 0)),
        ]
        for name, value in scales:
            if value <= 0:
                errors.append(f'{name}必须为正数: {value}')

        # 6. 校验阈值合理性
        if params.get('l1_tri_validation', {}).get('sortino_tri_center', 0) < 0:
            warnings.append('sortino_tri_center为负数，可能导致异常行为')

        if params.get('l3_greeks', {}).get('gamma_threshold', 0) < 0:
            warnings.append('gamma_threshold为负数，可能导致异常行为')

        # 输出校验结果
        if errors:
            for e in errors:
                logger.error('[TVFParamLoader] 参数校验错误: %s', e)
            raise ValueError(f'TVF参数校验失败: {errors}')

        if warnings:
            for w in warnings:
                logger.warning('[TVFParamLoader] 参数校验警告: %s', w)

        logger.info('[TVFParamLoader] 参数校验通过')

    @staticmethod
    def _default_params() -> Dict[str, Any]:
        p = TVF_DEFAULT_PARAMS
        return {
            'tvf_enabled': True,  # R26-P0-CD-02修复: 三源统一为True
            'layer_weights': {
                'l1_risk_return': p['tvf_l1_weight'],
                'l2_market_micro': p['tvf_l2_weight'],
                'l3_option_greeks': p['tvf_l3_weight'],
            },
            'l1_tri_validation': {
                'sortino_tri_center': p['tvf_sortino_threshold'],
                'calmar_tri_center': p['tvf_calmar_threshold'],
                'sharpe_tri_center': p['tvf_sharpe_threshold'],
                'sortino_tri_scale': p['tvf_sortino_scale'],
                'calmar_tri_scale': p['tvf_calmar_scale'],
                'sharpe_tri_scale': p['tvf_sharpe_scale'],
                'inner_weights': {
                    'sortino': p['tvf_l1_inner_sortino_weight'],
                    'calmar': p['tvf_l1_inner_calmar_weight'],
                    'sharpe': p['tvf_l1_inner_sharpe_weight'],
                },
            },
            'l2_order_flow': {
                'ofi_scale': p['tvf_ofi_scale'],
                'smf_scale': p['tvf_smf_scale'],
                'inner_weights': {
                    'ofi': p['tvf_l2_inner_ofi_weight'],
                    'cvd_divergence': p['tvf_l2_inner_cvd_weight'],
                    'smart_money_flow': p['tvf_l2_inner_smf_weight'],
                },
            },
            'l3_greeks': {
                'gamma_threshold': p['tvf_gamma_threshold'],
                'theta_threshold': p['tvf_theta_threshold'],
                'vega_threshold': p['tvf_vega_threshold'],
                'gamma_scale': p['tvf_gamma_scale'],
                'theta_scale': p['tvf_theta_scale'],
                'vega_scale': p['tvf_vega_scale'],
                'inner_weights': {
                    'delta': p['tvf_l3_inner_delta_weight'],
                    'gamma': p['tvf_l3_inner_gamma_weight'],
                    'theta': p['tvf_l3_inner_theta_weight'],
                    'vega': p['tvf_l3_inner_vega_weight'],
                },
            },
        }


def get_tvf_param_loader() -> TVFParamLoader:
    """获取TVF参数加载器单例"""
    return TVFParamLoader.get_instance()


def load_tvf_params(config_path: Optional[str] = None) -> Dict[str, Any]:
    """便捷函数：加载TVF参数"""
    loader = get_tvf_param_loader()
    return loader.load(config_path)
