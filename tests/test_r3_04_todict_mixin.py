# MODULE_ID: M2-529
"""R3-4 断言测试: P2-27 ToDictMixin提取 — 14个dataclass继承ToDictMixin，删除简单to_dict，MarketSnapshot保留自定义

验证项:
1. ToDictMixin存在于shared_utils_types并可用
2. 14个dataclass继承ToDictMixin
3. MarketSnapshot不继承ToDictMixin（保留自定义to_dict）
4. 简单to_dict方法已从14个类中删除
5. 继承ToDictMixin的类to_dict()运行时行为正确
"""
import sys
import os
import re
import pytest
import importlib
import numpy as np

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


@pytest.fixture(autouse=True)
def _reload_market_snapshot_collector():
    _mod_name = 'ali2026v3_trading.strategy_judgment.market_snapshot_collector'
    _saved = sys.modules.get(_mod_name)
    if _saved is not None and hasattr(_saved, '__spec__'):
        importlib.reload(_saved)
    elif _saved is not None:
        del sys.modules[_mod_name]
        import ali2026v3_trading.strategy_judgment.market_snapshot_collector
    yield
    if _saved is not None and _saved is not sys.modules.get(_mod_name):
        sys.modules[_mod_name] = _saved


class TestToDictMixin:
    """R3-4: ToDictMixin提取断言测试"""

    # 14个应继承ToDictMixin的dataclass名
    EXPECTED_MIXIN_CLASSES = [
        "StrategyStateSnapshot",
        "HFTSpecificState",
        "ResonanceSpecificState",
        "BoxSpecificState",
        "SpringSpecificState",
        "ArbitrageSpecificState",
        "MarketMakingSpecificState",
        "EcosystemState",
        "ShadowAlphaState",
        "SafetyMetaState",
        "CrossStrategyGreeks",
        "LifeExpectancySnapshot",
        "CyclePredictionSnapshot",
        "RiskDimensionScores",
    ]

    def test_todict_mixin_exists_and_importable(self):
        """验证ToDictMixin可从shared_utils_types导入"""
        from ali2026v3_trading.infra.shared_utils import ToDictMixin
        assert ToDictMixin is not None
        assert hasattr(ToDictMixin, 'to_dict')

    def test_14_classes_inherit_todict_mixin(self):
        """验证14个dataclass都继承了ToDictMixin"""
        from ali2026v3_trading.infra.shared_utils import ToDictMixin
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import (
            StrategyStateSnapshot, HFTSpecificState, ResonanceSpecificState,
            BoxSpecificState, SpringSpecificState, ArbitrageSpecificState,
            MarketMakingSpecificState, EcosystemState, ShadowAlphaState,
            SafetyMetaState, CrossStrategyGreeks, LifeExpectancySnapshot,
            CyclePredictionSnapshot, RiskDimensionScores,
        )
        for cls in [
            StrategyStateSnapshot, HFTSpecificState, ResonanceSpecificState,
            BoxSpecificState, SpringSpecificState, ArbitrageSpecificState,
            MarketMakingSpecificState, EcosystemState, ShadowAlphaState,
            SafetyMetaState, CrossStrategyGreeks, LifeExpectancySnapshot,
            CyclePredictionSnapshot, RiskDimensionScores,
        ]:
            assert issubclass(cls, ToDictMixin), f"{cls.__name__} 未继承ToDictMixin"

    def test_market_snapshot_not_inherit_mixin(self):
        """验证MarketSnapshot不继承ToDictMixin（保留自定义to_dict）"""
        from ali2026v3_trading.infra.shared_utils import ToDictMixin
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshot
        assert not issubclass(MarketSnapshot, ToDictMixin), "MarketSnapshot不应继承ToDictMixin"

    def test_simple_to_dict_removed_from_14_classes(self):
        """验证14个类中简单to_dict方法已删除（源码中不应有'return asdict(self)'）"""
        source_path = os.path.join(
            _project_root, "strategy_judgment", "market_snapshot_collector.py"
        )
        with open(source_path, 'r', encoding='utf-8') as f:
            source = f.read()

        # 在14个类的范围内不应有 "return asdict(self)" 的简单to_dict
        # MarketSnapshot的自定义to_dict可以包含asdict但不应该是简单的return asdict(self)
        simple_to_dict_pattern = r'def to_dict\(self\)[^:]*:\s*return asdict\(self\)'
        matches = re.findall(simple_to_dict_pattern, source)
        assert len(matches) == 0, f"发现{len(matches)}处简单to_dict方法未删除: {matches}"

    def test_todict_mixin_runtime_behavior(self):
        """验证继承ToDictMixin的类to_dict()运行时返回正确dict"""
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import StrategyStateSnapshot

        obj = StrategyStateSnapshot(strategy_id='test_id', strategy_type='hft')
        result = obj.to_dict()
        assert isinstance(result, dict), "to_dict()应返回dict"
        assert result['strategy_id'] == 'test_id'
        assert result['strategy_type'] == 'hft'

    def test_market_snapshot_custom_todict_works(self):
        """验证MarketSnapshot自定义to_dict仍然正常工作"""
        from ali2026v3_trading.strategy_judgment.market_snapshot_collector import (
            MarketSnapshot, SnapshotTrigger,
        )
        snap = MarketSnapshot(
            snapshot_id='snap_001',
            timestamp=np.datetime64('2025-01-01'),
            symbol='TEST',
            trigger=SnapshotTrigger.SIGNAL_GENERATED,
        )
        result = snap.to_dict()
        assert isinstance(result, dict), "MarketSnapshot.to_dict()应返回dict"
        # 自定义to_dict应将Enum转为value
        assert result['trigger'] == SnapshotTrigger.SIGNAL_GENERATED.value, "Enum应转为value"
