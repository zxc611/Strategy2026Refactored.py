# MODULE_ID: M2-284
import sys, os, ast, glob
sys.path.insert(0, r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo')
os.chdir(r'C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading')

print('=== 第3组端到端断言验证 (R3-1~R3-5) ===')
PASS = 0
FAIL = 0

def count_except_exception(directory):
    count = 0
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.endswith('.py') and f != '__init__.py':
                fpath = os.path.join(root, f)
                try:
                    with open(fpath, 'r', encoding='utf-8') as fh:
                        src = fh.read()
                    tree = ast.parse(src)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.ExceptHandler):
                            if node.type is None:
                                count += 1
                            elif isinstance(node.type, ast.Name) and node.type.id == 'Exception':
                                count += 1
                            elif isinstance(node.type, ast.Attribute) and node.type.attr == 'Exception':
                                count += 1
                except SyntaxError:
                    pass
    return count

# R3-1: 异常分层策略制定与实施
try:
    jdg_count = count_except_exception('strategy_judgment')
    assert jdg_count == 0, f'strategy_judgment/仍有{jdg_count}处except Exception'
    print(f'R3-1 PASS: strategy_judgment/中except Exception数量={jdg_count}（已全部改造）')
    PASS += 1
except AssertionError as e:
    print(f'R3-1 FAIL: {e}')
    FAIL += 1

# R3-2: judgment_scoring_helpers.py 全量改造
try:
    with open('strategy_judgment/judgment_scoring_helpers.py', 'r', encoding='utf-8') as f:
        src = f.read()
    tree = ast.parse(src)
    bare_count = 0
    except_exception_count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            if node.type is None:
                bare_count += 1
            elif isinstance(node.type, ast.Name) and node.type.id == 'Exception':
                except_exception_count += 1
    assert bare_count == 0, f'仍有{bare_count}处bare except'
    assert except_exception_count == 0, f'仍有{except_exception_count}处except Exception'
    print('R3-2 PASS: judgment_scoring_helpers.py已全量改造（0 bare except, 0 except Exception）')
    PASS += 1
except AssertionError as e:
    print(f'R3-2 FAIL: {e}')
    FAIL += 1

# R3-3: _judgment_services.py 全量改造
try:
    with open('strategy_judgment/_judgment_services.py', 'r', encoding='utf-8') as f:
        src = f.read()
    tree = ast.parse(src)
    bare_count = 0
    except_exception_count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            if node.type is None:
                bare_count += 1
            elif isinstance(node.type, ast.Name) and node.type.id == 'Exception':
                except_exception_count += 1
    assert bare_count == 0, f'仍有{bare_count}处bare except'
    assert except_exception_count == 0, f'仍有{except_exception_count}处except Exception'
    print('R3-3 PASS: _judgment_services.py已全量改造（0 bare except, 0 except Exception）')
    PASS += 1
except AssertionError as e:
    print(f'R3-3 FAIL: {e}')
    FAIL += 1

# R3-4: param_pool/ 关键路径改造
try:
    pp_count = count_except_exception('param_pool')
    assert pp_count == 0, f'param_pool/仍有{pp_count}处except Exception'
    print(f'R3-4 PASS: param_pool/中except Exception数量={pp_count}（已全部改造）')
    PASS += 1
except AssertionError as e:
    print(f'R3-4 FAIL: {e}')
    FAIL += 1

# R3-5: 建立CI lint规则禁止关键路径bare except
try:
    assert os.path.exists('.ci/lint_no_bare_except.py'), '.ci/lint_no_bare_except.py不存在'
    with open('.ci/lint_no_bare_except.py', 'r', encoding='utf-8') as f:
        lint_src = f.read()
    assert 'CRITICAL_PATHS' in lint_src, 'lint脚本未定义CRITICAL_PATHS'
    assert 'ast' in lint_src, 'lint脚本未使用AST分析'
    print('R3-5 PASS: CI lint规则已建立')
    PASS += 1
except AssertionError as e:
    print(f'R3-5 FAIL: {e}')
    FAIL += 1

print(f'\n第3组结果: {PASS} PASS / {FAIL} FAIL')