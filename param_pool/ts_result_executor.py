# MODULE_ID: M1-193
﻿# [M1-97] 结果执行器
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
#!/usr/bin/env python3
"""ts_result_executor — 执行轮次子模块 (从ts_result_writer.py拆分)

包含:
  - _execute_round: 执行一轮参数扫描
"""
from __future__ import annotations

import itertools
import json  # R6-5: 保留用于json.JSONDecodeError
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads
from ali2026v3_trading.infra.shared_utils import atomic_replace_file  # R9-1
import logging
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from ali2026v3_trading.param_pool._param_grids import (
    PARAM_DEFAULTS,
)
from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
    MAX_WORKERS,
)
from ali2026v3_trading.param_pool.ts.ts_backtest_strategies import (
    _prepare_df_for_subprocess,
    _worker_init,
    _worker_task,
)

logger = get_logger(__name__)  # R9-5


def _execute_round(
    round_name: str,
    param_grid: Dict[str, List],
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    task_id_offset: int = 0,
    fixed_params: Optional[Dict[str, float]] = None,
    resume: bool = False,
) -> Tuple[List[dict], int]:
    """执行一轮参数扫描，每个任务包含九策略回测

    P1-裂缝51：增加checkpoint_db记录已完成组合，支持--resume断点续传。
    """
    _run_id = time.strftime("%Y%m%d_%H%M%S")
    all_keys = sorted(set(list(param_grid.keys()) + (list(fixed_params.keys()) if fixed_params else [])))
    grid_keys, grid_values = zip(*param_grid.items())
    grid_combos = [dict(zip(grid_keys, v)) for v in itertools.product(*grid_values)]

    completed_task_ids = set()
    if resume:
        try:
            checkpoint_dir = os.path.dirname(os.path.abspath(__file__))
            existing_checkpoints = sorted(
                [f for f in os.listdir(checkpoint_dir) if f.startswith(f"checkpoint_{round_name}_") and f.endswith(".json")],
                reverse=True
            )
            if existing_checkpoints:
                checkpoint_path = os.path.join(checkpoint_dir, existing_checkpoints[0])
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    completed_task_ids = set(json_loads(f.read()).get("completed_ids", []))  # R6-5
                _run_id = existing_checkpoints[0].replace(f"checkpoint_{round_name}_", "").replace(".json", "")
                logger.info("[P1-裂缝51] 从checkpoint恢复: %d个已完成任务, file=%s", len(completed_task_ids), existing_checkpoints[0])
            else:
                logger.info("[P1-裂缝51] 未找到已有checkpoint文件, 从头开始")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("[P1-裂缝51] 加载checkpoint失败: %s", e)

    tasks = []
    task_id = task_id_offset
    for grid_params in grid_combos:
        full_params = dict(PARAM_DEFAULTS)
        if fixed_params:
            full_params.update(fixed_params)
        full_params.update(grid_params)

        if task_id not in completed_task_ids:
            tasks.append({"id": task_id, "params": full_params, "train": True})
        task_id += 1
        if task_id not in completed_task_ids:
            tasks.append({"id": task_id, "params": full_params, "train": False})
        task_id += 1

    logger.info("[%s] %d 组合 × 2(train+test) × 18策略 = %d 回测", round_name, len(grid_combos), len(tasks) * 18)

    results: List[dict] = []
    train_data_slim = _prepare_df_for_subprocess(train_data)
    test_data_slim = _prepare_df_for_subprocess(test_data)
    _essential_cols = ['minute', 'open', 'high', 'low', 'close', 'volume', 'amount',
                       'open_interest', 'symbol', 'product', 'iv', 'vwap']
    for _slim_df in (train_data_slim, test_data_slim):
        if _slim_df is not None and isinstance(_slim_df, pd.DataFrame):
            _keep_cols = [c for c in _essential_cols if c in _slim_df.columns]
            _extra_cols = [c for c in _slim_df.columns if c not in _essential_cols]
            if _extra_cols:
                logger.debug("[R21-CC-P1-10] 精简传输列: 移除%s, 保留%d/%d列",
                             _extra_cols[:5], len(_keep_cols), len(_slim_df.columns))
                _slim_df = _slim_df[_keep_cols + _extra_cols[:5]]
    if MAX_WORKERS > 1:
        _failed_count = 0
        _failure_threshold = max(1, len(tasks) // 2)
        try:
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=_worker_init,
                initargs=(train_data_slim, test_data_slim),
                mp_context=multiprocessing.get_context('spawn'),
                max_tasks_per_child=50,
            ) as executor:
                futures = {executor.submit(_worker_task, task): task for task in tasks}
                for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                    try:
                        task_result = future.result()
                        if task_result:
                            results.append(task_result)
                        else:
                            _failed_count += 1
                            logger.warning("[R21-CC03] 任务返回空结果: task_id=%s", futures[future].get('id', '?'))
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
        except TypeError:
            logger.warning("[R21-CC-P2-02] Python<3.11不支持max_tasks_per_child，降级运行")
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=_worker_init,
                initargs=(train_data_slim, test_data_slim),
                mp_context=multiprocessing.get_context('spawn'),
            ) as executor:
                futures = {executor.submit(_worker_task, task): task for task in tasks}
                for future in tqdm(as_completed(futures), total=len(tasks), desc=f"{round_name}进度"):
                    try:
                        task_result = future.result()
                        if task_result:
                            results.append(task_result)
                        else:
                            _failed_count += 1
                            logger.warning("[R21-CC03] 任务返回空结果: task_id=%s", futures[future].get('id', '?'))
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        _failed_count += 1
                        logger.error("[R21-CC03] 任务执行异常(%d/%d): %s", _failed_count, len(tasks), e)
                        if _failed_count >= _failure_threshold:
                            logger.critical("[R21-CC03] 失败任务超过阈值(%d/%d)，中断主流程!", _failed_count, _failure_threshold)
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise RuntimeError(f"ProcessPoolExecutor失败率过高({_failed_count}/{len(tasks)}), 中断回测")
    else:
        _worker_init(train_data_slim, test_data_slim)
        _st_failed_count = 0
        _st_failure_threshold = max(1, len(tasks) // 2)
        for task in tqdm(tasks, desc=f"{round_name}进度"):
            try:
                task_result = _worker_task(task)
                if task_result:
                    results.append(task_result)
                    completed_task_ids.add(task["id"])
                    if len(results) % 10 == 0:
                        try:
                            checkpoint_path = os.path.join(
                                os.path.dirname(os.path.abspath(__file__)),
                                f"checkpoint_{round_name}_{_run_id}.json"
                            )
                            atomic_replace_file(checkpoint_path, json_dumps({"completed_ids": list(completed_task_ids)}))  # R9-1
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _e:
                            logger.warning("[BACKTEST] inner flow exception: %s", _e)
                else:
                    _st_failed_count += 1
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _st_err:
                logger.error("[R22-P1-ST-01] 单线程回测任务异常: %s", _st_err)
                _st_failed_count += 1
            if _st_failed_count >= _st_failure_threshold:
                logger.error("[R22-P1-ST-01] 单线程失败率过高(%d/%d), 中断回测", _st_failed_count, len(tasks))
                break

    return results, task_id