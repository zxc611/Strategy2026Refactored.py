# MODULE_ID: M2-532
"""R3轮端到端断言验证测试 — 软集成治理"""
import ast
import os
import re
import sys
import pytest

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


class TestR3_1_NoBareExceptInProduction:
    """R3-1: 生产代码中无bare except Exception"""

    def test_no_bare_except_in_strategy_judgment(self):
        """strategy_judgment/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'strategy_judgment'))
        assert count == 0, f"R3-1失败: strategy_judgment/仍有{count}处bare except Exception"

    def test_no_bare_except_in_param_pool(self):
        """param_pool/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'param_pool'))
        assert count == 0, f"R3-1失败: param_pool/仍有{count}处bare except Exception"

    def test_no_bare_except_in_risk(self):
        """risk/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'risk'))
        assert count == 0, f"R3-1失败: risk/仍有{count}处bare except Exception"

    def test_no_bare_except_in_strategy(self):
        """strategy/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'strategy'))
        assert count == 0, f"R3-1失败: strategy/仍有{count}处bare except Exception"

    def test_no_bare_except_in_governance(self):
        """governance/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'governance'))
        assert count == 0, f"R3-1失败: governance/仍有{count}处bare except Exception"

    def test_no_bare_except_in_evaluation(self):
        """evaluation/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'evaluation'))
        assert count == 0, f"R3-1失败: evaluation/仍有{count}处bare except Exception"

    def test_no_bare_except_in_lifecycle(self):
        """lifecycle/ 目录无bare except Exception"""
        count = _count_bare_except(os.path.join(_PROJECT_ROOT, 'lifecycle'))
        assert count == 0, f"R3-1失败: lifecycle/仍有{count}处bare except Exception"

    def test_total_bare_except_is_zero(self):
        """全项目生产代码bare except Exception总数为0"""
        dirs = ['strategy_judgment', 'param_pool', 'risk', 'strategy', 'governance', 'evaluation', 'position', 'order', 'lifecycle']
        total = sum(_count_bare_except(os.path.join(_PROJECT_ROOT, d)) for d in dirs)
        assert total == 0, f"R3-1失败: 全项目仍有{total}处bare except Exception"


class TestR3_2_ComponentFailurePolicy:
    """R3-2: ComponentFailurePolicy分层策略已实现"""

    def test_policy_enum_exists(self):
        """ComponentFailurePolicy枚举存在"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert 'class ComponentFailurePolicy' in src, "R3-2失败: ComponentFailurePolicy不存在"

    def test_block_policy_exists(self):
        """BLOCK策略存在"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert 'ComponentFailurePolicy.BLOCK' in src, "R3-2失败: BLOCK策略不存在"

    def test_degrade_policy_exists(self):
        """DEGRADE策略存在"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert 'ComponentFailurePolicy.DEGRADE' in src, "R3-2失败: DEGRADE策略不存在"

    def test_critical_components_config(self):
        """CRITICAL_COMPONENTS配置包含关键组件"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert 'E-04_governance' in src, "R3-2失败: E-04不在CRITICAL_COMPONENTS"
        assert 'E-09_chicory_eviction' in src, "R3-2失败: E-09不在CRITICAL_COMPONENTS"
        assert 'E-10_marquee_threshold' in src, "R3-2失败: E-10不在CRITICAL_COMPONENTS"

    def test_e04_is_block_policy(self):
        """E-04 governance使用BLOCK策略"""
        src = open(os.path.join(_PROJECT_ROOT, 'strategy_judgment', 'judgment_scoring_helpers.py'), encoding='utf-8').read()
        assert '"E-04_governance": ComponentFailurePolicy.BLOCK' in src, "R3-2失败: E-04未使用BLOCK策略"


def _count_bare_except(directory):
    """统计目录中bare except Exception的数量"""
    count = 0
    if not os.path.isdir(directory):
        return 0
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if 'backup' not in d.lower() and d != '__pycache__']
        for f in files:
            if not f.endswith('.py'):
                continue
            fp = os.path.join(root, f)
            try:
                src = open(fp, encoding='utf-8').read()
            except (OSError, UnicodeDecodeError):
                continue
            for line in src.split('\n'):
                s = line.strip()
                if s.startswith('#'):
                    continue
                if re.search(r'except\s+Exception\b', s):
                    count += 1
    return count


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])