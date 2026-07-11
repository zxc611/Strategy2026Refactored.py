# MODULE_ID: M2-370
import sys
import os
import pytest
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestCointegrationScanner:
    def test_init(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=50, scan_interval=30)
        assert scanner is not None

    def test_add_symbol(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=10, scan_interval=20)
        scanner.add_symbol("AU")
        scanner.add_symbol("AG")

    def test_update_price(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=10, scan_interval=20)
        scanner.add_symbol("AU")
        for i in range(5):
            scanner.update_price("AU", 500.0 + i)

    def test_scan_returns_dict(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=10, scan_interval=20)
        scanner.add_symbol("AU")
        scanner.add_symbol("AG")
        result = scanner.scan()
        assert isinstance(result, dict)

    def test_scan_with_data(self):
        from ali2026v3_trading.data.quant_cointegration import CointegrationScanner
        scanner = CointegrationScanner(window=30, scan_interval=10)
        scanner.add_symbol("AU")
        scanner.add_symbol("AG")
        for i in range(60):
            scanner.update_price("AU", 500.0 + np.random.randn() * 2)
            scanner.update_price("AG", 25.0 + np.random.randn() * 0.5)
        result = scanner.scan()
        assert isinstance(result, dict)


class TestSurvivalAnalyzer:
    def test_init(self):
        from ali2026v3_trading.data.quant_cointegration import SurvivalAnalyzer
        sa = SurvivalAnalyzer()
        assert sa is not None

    def test_add_observation_and_fit(self):
        from ali2026v3_trading.data.quant_cointegration import SurvivalAnalyzer
        sa = SurvivalAnalyzer()
        for i in range(20):
            sa.add_observation(duration=float(i + 1), event=1 if i % 3 == 0 else 0, covariates=np.array([1.0]))
        result = sa.fit()
        assert isinstance(result, dict)

    def test_predict_survival(self):
        from ali2026v3_trading.data.quant_cointegration import SurvivalAnalyzer
        sa = SurvivalAnalyzer()
        for i in range(20):
            sa.add_observation(duration=float(i + 1), event=1 if i % 3 == 0 else 0, covariates=np.array([1.0]))
        sa.fit()
        p = sa.predict_survival(covariates=np.array([1.0]))
        assert isinstance(p, dict)

    def test_get_median_survival(self):
        from ali2026v3_trading.data.quant_cointegration import SurvivalAnalyzer
        sa = SurvivalAnalyzer()
        for i in range(20):
            sa.add_observation(duration=float(i + 1), event=1 if i % 3 == 0 else 0, covariates=np.array([1.0]))
        sa.fit()
        med = sa.get_median_survival(covariates=np.array([1.0]))
        assert isinstance(med, float)


class TestAdfCritical:
    def test_get_adf_critical_5pct(self):
        from ali2026v3_trading.data.quant_cointegration import _get_adf_critical_5pct
        val = _get_adf_critical_5pct(50)
        assert isinstance(val, float)
        assert val < 0