"""参数池 — 英文路径主目录

此目录与 参数池/ (中文路径镜像) 保持同步。
修改参数文件时，请确保两个目录保持一致。

R26-P0-CD-09: Linux环境下中文路径可能失败，__init__.py 中已注册双向映射。
"""

import os as _os
import logging as _logging

_pkg_dir = _os.path.dirname(_os.path.abspath(__file__))
_en_pool_dir = _pkg_dir  # 本目录 (param_pool)
_cn_pool_dir = _os.path.join(_os.path.dirname(_pkg_dir), '参数池')


def _check_param_pool_consistency():
    """检查 param_pool/ 与 参数池/ 目录一致性，不同步时发出WARNING"""
    if not _os.path.isdir(_cn_pool_dir):
        return

    cn_files = set()
    en_files = set()

    for root, _dirs, files in _os.walk(_cn_pool_dir):
        rel = _os.path.relpath(root, _cn_pool_dir)
        for f in files:
            cn_files.add(_os.path.join(rel, f) if rel != '.' else f)

    for root, _dirs, files in _os.walk(_en_pool_dir):
        rel = _os.path.relpath(root, _en_pool_dir)
        for f in files:
            en_files.add(_os.path.join(rel, f) if rel != '.' else f)

    only_in_cn = cn_files - en_files
    only_in_en = en_files - cn_files

    if only_in_cn or only_in_en:
        _logging.warning(
            "P2-11: param_pool/ 与 参数池/ 目录不同步 — "
            "仅参数池: %s, 仅param_pool: %s",
            sorted(only_in_cn)[:5], sorted(only_in_en)[:5],
        )


try:
    _check_param_pool_consistency()
except Exception:
    pass
