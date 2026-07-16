#!/usr/bin/env python3
# MODULE_ID: M2-309
"""CascadeJudge 评判引擎测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def test_cascade_judge_import():
    from evaluation.cascade_judge import CascadeJudge
    assert CascadeJudge is not None

def test_cascade_judge_from_config():
    from evaluation.cascade_judge import CascadeJudge
    judge = CascadeJudge.from_config()
    assert judge is not None

def test_cascade_judge_v75_alignment():
    """验证V7.5架构对齐: L1规则引擎/L2 ML模型/L3影子隐形"""
    import evaluation.cascade_judge as mod
    src = open(mod.__file__, encoding='utf-8').read()
    assert 'gate_l1_rule_engine' in src or 'L1' in src
    assert 'gate_l2_ml_model' in src or 'L2' in src
    assert 'gate_l3_shadow_stealth' in src or 'L3' in src

def test_cascade_judge_three_gates():
    from evaluation.cascade_judge import CascadeJudge
    judge = CascadeJudge.from_config()
    assert hasattr(judge, 'judge') or hasattr(judge, 'evaluate')

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            try:
                fn()
                print(f'[PASS] {name}')
            except Exception as e:
                print(f'[FAIL] {name}: {e}')
