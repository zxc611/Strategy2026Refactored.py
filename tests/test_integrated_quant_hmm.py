# MODULE_ID: M2-371
import sys
import os
import pytest
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestAdaptiveHMM:
    def test_init(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=3, update_interval=50)
        assert hmm is not None

    def test_init_with_params(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=2, update_interval=10, var_floor_ratio=0.02)
        assert hmm is not None

    def test_update_single(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=2, update_interval=100)
        result = hmm.update(0.001)
        assert isinstance(result, dict)

    def test_update_multiple(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=3, update_interval=10)
        for i in range(50):
            result = hmm.update(np.random.randn() * 0.01)
            assert isinstance(result, dict)

    def test_run_em_if_needed(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=2, update_interval=10)
        for i in range(20):
            hmm.update(np.random.randn())
        hmm.run_em_if_needed()

    def test_update_returns_state_info(self):
        from ali2026v3_trading.data.quant_hmm import AdaptiveHMM
        hmm = AdaptiveHMM(n_states=2, update_interval=100)
        result = hmm.update(0.005)
        assert isinstance(result, dict)