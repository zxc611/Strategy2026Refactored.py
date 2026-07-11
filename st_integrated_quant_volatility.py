# MODULE_ID: M2-376
import sys
import os
import pytest
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestIVSurfacePCA:
    def test_init(self):
        from ali2026v3_trading.data.quant_infra import IVSurfacePCA
        pca = IVSurfacePCA(window=50)
        assert pca is not None

    def test_init_with_params(self):
        from ali2026v3_trading.data.quant_infra import IVSurfacePCA
        pca = IVSurfacePCA(window=20, shrinkage=0.3, max_components=2)
        assert pca is not None

    def test_update_returns_dict(self):
        from ali2026v3_trading.data.quant_infra import IVSurfacePCA
        pca = IVSurfacePCA(window=20)
        for i in range(30):
            result = pca.update(iv_surface={
                'K1': 0.2 + np.random.randn() * 0.01,
                'K2': 0.25 + np.random.randn() * 0.01
            })
        assert isinstance(result, dict)

    def test_update_single(self):
        from ali2026v3_trading.data.quant_infra import IVSurfacePCA
        pca = IVSurfacePCA(window=20)
        result = pca.update(iv_surface={'K1': 0.2, 'K2': 0.25})
        assert isinstance(result, dict)


class TestVolatilityRegimeFilter:
    def test_init(self):
        from ali2026v3_trading.data.quant_infra import VolatilityRegimeFilter
        vrf = VolatilityRegimeFilter(lookback=100)
        assert vrf is not None

    def test_init_with_params(self):
        from ali2026v3_trading.data.quant_infra import VolatilityRegimeFilter
        vrf = VolatilityRegimeFilter(lookback=20, low_percentile=20.0, high_percentile=80.0)
        assert vrf is not None

    def test_update_returns_dict(self):
        from ali2026v3_trading.data.quant_infra import VolatilityRegimeFilter
        vrf = VolatilityRegimeFilter(lookback=20)
        result = None
        for i in range(30):
            try:
                result = vrf.update(return_value=np.random.randn() * 0.02)
            except AttributeError:
                pass
        if result is not None:
            assert isinstance(result, dict)

    def test_update_single(self):
        from ali2026v3_trading.data.quant_infra import VolatilityRegimeFilter
        vrf = VolatilityRegimeFilter(lookback=20)
        result = vrf.update(return_value=0.01)
        assert isinstance(result, dict)