# MODULE_ID: M2-546
"""R6断言测试: 残留json.load→json_loads + 全项目json.load零残留验证

验证:
1. meta_audit_passport.py 不再有json.load(f)
2. ts_result_executor.py 不再有json.load(f)
3. 全项目活跃代码零json.load残留
"""
import os

_BASE = os.path.join(os.path.dirname(__file__), '..')


def _read(rel_path: str) -> str:
    with open(os.path.join(_BASE, rel_path), 'r', encoding='utf-8') as f:
        return f.read()


def test_meta_audit_passport_no_json_load():
    """meta_audit_passport.py不应有json.load(f)"""
    src = _read('param_pool/l1_quantification/meta_audit_passport.py')
    assert 'json.load(' not in src, "meta_audit_passport.py不应有json.load()"


def test_ts_result_writer_no_json_load():
    """ts_result_writer.py不应有json.load(f)"""
    src = _read('param_pool/ts/ts_result_writer.py')
    assert 'json.load(' not in src, "ts_result_writer.py不应有json.load()"


def test_ts_result_writer_has_json_loads():
    """ts_result_writer.py应导入json_loads"""
    src = _read('param_pool/ts/ts_result_writer.py')
    assert 'json_loads' in src, "ts_result_writer.py应导入json_loads"


def test_global_no_json_load_in_active_code():
    """全项目活跃代码（排除backup/tests/__pycache__/archive）零json.load残留"""
    import re
    active_dirs = ['config', 'governance', 'infra', 'order', 'param_pool',
                   'position', 'risk', 'signal', 'strategy', 'strategy_judgment',
                   'lifecycle', 'evaluation']
    violations = []
    for subdir in active_dirs:
        dirpath = os.path.join(_BASE, subdir)
        if not os.path.isdir(dirpath):
            continue
        for root, dirs, files in os.walk(dirpath):
            # 排除backup和。_pycache__
            dirs[:] = [d for d in dirs if not d.startswith('_backup_') and d != '__pycache__']
            for fname in files:
                if not fname.endswith('.py'):
                    continue
                fpath = os.path.join(root, fname)
                with open(fpath, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if 'json.load(' in line:
                            violations.append(f"{os.path.relpath(fpath, _BASE)}:{i}")
    # 也检查根目录的py文件
    for fname in os.listdir(_BASE):
        if fname.endswith('.py') and fname != '__init__.py':
            fpath = os.path.join(_BASE, fname)
            with open(fpath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    if 'json.load(' in line:
                        violations.append(f"{fname}:{i}")
    assert len(violations) == 0, f"仍有json.load残留: {violations}"


def test_global_no_json_dump_in_active_code():
    """全项目活跃代码零json.dump残留"""
    active_dirs = ['config', 'governance', 'infra', 'order', 'param_pool',
                   'position', 'risk', 'signal', 'strategy', 'strategy_judgment',
                   'lifecycle', 'evaluation']
    violations = []
    for subdir in active_dirs:
        dirpath = os.path.join(_BASE, subdir)
        if not os.path.isdir(dirpath):
            continue
        for root, dirs, files in os.walk(dirpath):
            dirs[:] = [d for d in dirs if not d.startswith('_backup_') and d != '__pycache__']
            for fname in files:
                if not fname.endswith('.py'):
                    continue
                fpath = os.path.join(root, fname)
                with open(fpath, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f, 1):
                        if 'json.dump(' in line:
                            violations.append(f"{os.path.relpath(fpath, _BASE)}:{i}")
    for fname in os.listdir(_BASE):
        if fname.endswith('.py') and fname != '__init__.py':
            fpath = os.path.join(_BASE, fname)
            with open(fpath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    if 'json.dump(' in line:
                        violations.append(f"{fname}:{i}")
    assert len(violations) == 0, f"仍有json.dump残留: {violations}"
