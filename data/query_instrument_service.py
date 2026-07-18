# MODULE_ID: M1-038
"""
query_instrument_service.py - 合约注册/预注册/分类 服务

从 query_service.py 拆分出的合约管理相关功能

核心功能:
1. 合约分类与推断
2. 合约注册与缓存查询
3. 合约预注册与加载
4. 品种与合约查询
5. 期权链查询

重构说明 (2026-06-11):
- _QueryInstrumentMixin → InstrumentQueryService（服务提取+Facade组合，消灭Mixin）
- 构造函数显式接收 storage / params_service，消除隐式self依赖
"""

from __future__ import annotations

import logging
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from infra.shared_utils import CHINA_TZ as _CHINA_TZ


try:
    from data.data_service import DataService, get_data_service
    _HAS_DATA_SERVICE = True
except ImportError as e:
    logging.warning("[QueryService] Failed to import DataService: %s", e)
    _HAS_DATA_SERVICE = False
    DataService = None
    get_data_service = None


class InstrumentQueryService:
    """
    合约注册/预注册/分类 服务

    职责：
    1. 合约分类与推断
    2. 合约注册与缓存查询
    3. 合约预注册与加载
    4. 品种与合约查询
    5. 期权链查询
    """

    def __init__(self, storage, params_service=None):
        self._storage = storage
        self._params_service = params_service

    def _get_params_service(self):
        if self._params_service is not None:
            return self._params_service
        try:
            from config.params_service import get_params_service
            return get_params_service()
        except Exception:
            return None

    def _get_info_internal_id(self, info):
        if info is None:
            return None
        internal_id = info.get('internal_id')
        return int(internal_id) if internal_id is not None else None

    # ========================================================================
    # 合约分类与推断
    # ========================================================================

    def classify_instruments(self, instrument_ids: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
        """
        分类合约 ID 列表为期货和期权

        Args:
            instrument_ids: 合约 ID 列表

        Returns:
            Tuple[List[str], Dict[str, List[str]]]: (futures_list, options_dict)
            - futures_list: 期货合约列表
            - options_dict: 期权合约字典 {underlying: [option_ids]}
        """
        # ✅ 委托给 subscription_manager 的统一解析函数
        from infra.subscription_service import SubscriptionManager
        return SubscriptionManager.classify_instruments(instrument_ids)

    # ✅ 删除classify_instruments_static，统一使用实例方法

    def infer_exchange_from_id(self, instrument_id: str) -> str:
        """
        从合约 ID 推断交易所（唯一方法）。'
        Args:
            instrument_id: 合约 ID（如 'IF2603', 'CU2406'）

        Returns:
            str: 交易所代码（如 'CFFEX', 'SHFE'）

        ✅ 修复标准：统一为params_service缓存查询+config_service降级
        """
        normalized_id = str(instrument_id or '').strip()
        if not normalized_id:
            return 'UNKNOWN'

        # ✅ 第一优先级：params_service缓存查询（唯一权威源）'
        try:
            if self._storage:
                ps = self._get_params_service()
                if ps:
                    info = ps.get_instrument_meta_by_id(normalized_id)
                    if info:
                        exchange = str(info.get('exchange') or '').strip()
                        if exchange and exchange.upper() != 'AUTO':
                            return exchange
        except Exception as cache_error:
            logging.warning("[QueryService] Failed to get exchange from cache: %s", cache_error)

        # ✅ 第二优先级：config_service降级解析
        try:
            from config.config_exchange import resolve_product_exchange

            exchange = resolve_product_exchange(normalized_id)
            if not exchange:
                logging.debug("[QueryService] resolve_product_exchange returned None for %s", normalized_id)
                return 'UNKNOWN'

            # 验证返回值，防止 AUTO 污染
            exchange_str = str(exchange).strip()
            if exchange_str.upper() == 'AUTO':
                logging.warning("[QueryService] resolve_product_exchange returned AUTO for %s, using UNKNOWN", normalized_id)
                return 'UNKNOWN'

            return exchange_str

        except ImportError as e:
            logging.warning("[QueryService] config_service not available: %s", e)
            return 'UNKNOWN'
        except (KeyError, AttributeError, ValueError) as e:
            # 数据缺失或格式错误，返回 UNKNOWN 标记
            logging.debug("[QueryService] resolve_product_exchange failed for %s: %s", normalized_id, e)
            return 'UNKNOWN'
        except Exception as exc:
            logging.error("[QueryService] resolve_product_exchange failed for %s: %s", normalized_id, exc, exc_info=True)
            raise

    # ========================================================================
    # 合约注册与缓存查询
    # ========================================================================

    def get_all_subscribed_instrument_ids(self) -> List[str]:
        ps = self._get_params_service()
        if ps is None:
            return []
        all_ids = ps.get_all_instrument_ids()
        return [x for x in (all_ids or []) if x]

    @staticmethod
    def _is_option_id(instrument_id: str) -> bool:
        import re as _re_opt
        return bool(_re_opt.compile(r'[CP]\d{3,}').search(str(instrument_id)))

    def get_registered_instrument_ids(self, instrument_ids: Optional[List[str]] = None) -> List[str]:
        """
        获取已注册的合约 ID 列表，可选按给定 instrument_ids 过滤。'
        Args:
            instrument_ids: 可选的合约 ID 列表进行过滤

        Returns:
            List[str]: 已注册的合约 ID 列表

        ✅ 修复标准：统一为params_service单一路径
        """
        if not instrument_ids:
            ps = self._get_params_service()
            return ps.get_all_instrument_ids() if ps else []

        registered_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids:
            normalized_id = str(instrument_id).strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)

            # ✅ 统一为params_service缓存查询（唯一权威源）'
            ps = self._get_params_service()
            if ps and ps.get_instrument_meta_by_id(normalized_id):
                registered_ids.append(normalized_id)

        return registered_ids

    def ensure_registered_instruments(self, instrument_ids: List[str]) -> Dict[str, int]:
        """
        比对给定合约列表与已注册缓存/数据库，只为缺失合约创建内部 ID。

        Args:
            instrument_ids: 需要确保已注册的合约 ID 列表

        Returns:
            Dict[str, int]: 统计信息字典
        """
        normalized_ids: List[str] = []
        seen = set()
        for instrument_id in instrument_ids or []:
            normalized_id = str(instrument_id).strip()
            if not normalized_id or normalized_id in seen:
                continue
            seen.add(normalized_id)
            normalized_ids.append(normalized_id)

        registered_ids = set(self.get_registered_instrument_ids(normalized_ids))
        missing_ids = [instrument_id for instrument_id in normalized_ids if instrument_id not in registered_ids]

        created_count = 0
        failed_count = 0
        _DB_BATCH_SIZE = 500
        _DB_BATCH_PAUSE_SEC = 0.3
        for _db_batch_start in range(0, len(missing_ids), _DB_BATCH_SIZE):
            _db_batch_end = min(_db_batch_start + _DB_BATCH_SIZE, len(missing_ids))
            for instrument_id in missing_ids[_db_batch_start:_db_batch_end]:
                try:
                    self._storage.register_instrument(
                        instrument_id=instrument_id,
                        exchange=self.infer_exchange_from_id(instrument_id),
                    )
                    created_count += 1
                except ValueError as e:
                    failed_count += 1
                    logging.warning("预注册合约失败 %s: %s", instrument_id, e)
            if _db_batch_end < len(missing_ids):
                import time as _db_batch_time
                logging.info(
                    "[PreRegister] DB batch pause: %d/%d instruments registered",
                    _db_batch_end, len(missing_ids),
                )
                _db_batch_time.sleep(_DB_BATCH_PAUSE_SEC)

        result = {
            'configured_count': len(normalized_ids),
            'registered_count': len(registered_ids),
            'missing_count': len(missing_ids),
            'created_count': created_count,
            'failed_count': failed_count,
        }

        # 发布预注册完成事件
        try:
            from infra.event_bus import get_global_event_bus
            event_bus = get_global_event_bus()
            if event_bus:
                event_bus.publish('PreRegisterCompleted', result)
                logging.info("[QueryService] 预注册完成，已发布PreRegisterCompleted事件")
        except Exception as e:
            logging.warning("[QueryService] 发布预注册事件失败: %s", e)

        return result

    def load_and_preregister_instruments(self, storage: Any, params: Any) -> Dict[str, Any]:
        """从合约配置文件加载合约列表并确保合约注册到DB（供on_init调用）。'
        设计约束：
        1. 合约列表唯一来源：TXT配置文件，不存在任何回退机制，失败即终止初始化
        2. 品种ID已内置于配置文件，初始化前完整读取+ID匹配+新表创建
        3. 重试3次快速失败：每步骤最多重试3次，3次均失败则抛RuntimeError
        4. 预注册有失败合约则阻断初始化，杜绝带病运行
        5. 完成后通知：发布InstrumentsLoadAndPreregisterCompleted事件

        Args:
            storage: InstrumentDataManager实例（strategy_core_service.storage）
            params: 参数对象
        """
        import time as _time
        MAX_RETRIES = 3

        self._futures_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'subscription_futures_fixed.txt',
        )

        # ========== 阶段1：从TXT合约配置文件加载合约列表（重试3次） ==========
        selected_futures_list: List[str] = []
        selected_options_dict: Dict[str, List[str]] = {}
        futures_metadata: Dict[str, Dict] = {}
        options_metadata: Dict[str, Dict] = {}
        last_load_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                from config.params_service import get_params_service
                ps = get_params_service()
                class _TempParams:
                    future_instruments = []
                    option_instruments = {}
                temp_params = _TempParams()
                file_result = ps.load_instrument_list(temp_params, source='output_files')
                if file_result and (file_result.get('futures_list') or file_result.get('options_dict')):
                    selected_futures_list = self._normalize_instruments(file_result['futures_list'])
                    selected_options_dict = self._normalize_options_dict(file_result['options_dict'])
                    futures_metadata = file_result.get('futures_metadata', {})
                    options_metadata = file_result.get('options_metadata', {})
                    _deferred_from_file = file_result.get('deferred_from_file', [])
                    _deferred_meta_from_file = file_result.get('deferred_meta_from_file', {})
                    _overflow_count = file_result.get('deferred_count', 0)
                    _raw_total = len(selected_futures_list) + self._count_option_contracts(selected_options_dict) + _overflow_count
                    _deferred_options = {}
                    if _deferred_from_file:
                        for _opt_id in _deferred_from_file:
                            _product = ''.join([c for c in _opt_id if c.isalpha()])[:2]
                            if _product:
                                _deferred_options.setdefault(_product, []).append(_opt_id)
                        for _opt_id, _meta in _deferred_meta_from_file.items():
                            options_metadata[_opt_id] = _meta
                        logging.warning(
                            "[Init-Load] 合约数 %d，延迟加载期权 %d 个到on_start后",
                            _raw_total, _overflow_count,
                        )
                    logging.info(
                        "[Init-Load] 第%d次尝试成功: 从合约配置文件加载 %d 期货, %d 期权 (延迟=%d)",
                        attempt, len(selected_futures_list),
                        self._count_option_contracts(selected_options_dict), _overflow_count,
                    )
                    last_load_error = None
                    break
                else:
                    last_load_error = f"第{attempt}次: load_instrument_list返回空结果"
                    logging.warning("[Init-Load] %s", last_load_error)
            except Exception as e:
                last_load_error = f"第{attempt}次: {type(e).__name__}: {e}"
                logging.warning("[Init-Load] 合约配置文件加载异常: %s", last_load_error)
        else:
            error_detail = (
                f"合约配置文件加载经{MAX_RETRIES}次重试均失败，策略初始化终止。\n"
                f"最后错误: {last_load_error}\n"
                f"请检查文件是否存在且非空:\n"
                f"  - demo/subscription_futures_fixed.txt\n"
                f"  - demo/subscription_options_fixed.txt\n"
                f"  - ensure_products_with_retry是否已在步骤1中成功执行"
            )
            logging.error("[Init-Load] ❌ %s", error_detail)
            raise RuntimeError(error_detail)

        if not selected_futures_list and not selected_options_dict:
            error_detail = (
                f"合约配置文件加载成功但规范化后为空（期货=0, 期权=0），策略初始化终止。\n"
                f"可能原因: TXT文件内容格式错误，所有合约被。normalize过滤掉"
            )
            logging.error("[Init-Load] ❌ %s", error_detail)
            raise RuntimeError(error_detail)

        # ========== 验证标的期货完整性（禁止补齐回退） ==========
        derived_futures = self._derive_underlying_futures(selected_options_dict)
        if derived_futures:
            # R36-P0-FIX: 归一化existing_futures，将CZCE的3位年月格式(如CF607)转为4位(如CF2607)
            # 因为_derive_underlying_futures使用4位年月格式，但CZCE期货文件使用3位年月格式
            existing_futures = set()
            for fut_id in selected_futures_list:
                existing_futures.add(fut_id)
                # 尝试将3位年月转为4位（CZCE格式：CF607 -> CF2607）
                import re as _re
                _m = _re.match(r'^([A-Za-z]+)(\d{3})$', fut_id)
                if _m:
                    existing_futures.add(f"{_m.group(1)}2{_m.group(2)}")
            missing_futures = sorted(item for item in derived_futures if item not in existing_futures)
            if missing_futures:
                error_detail = (
                    f"合约配置文件验证: 期权标的期货缺失 %d 个，策略初始化终止。\n"
                    f"缺失合约: %s\n"
                    f"当前期货清单(%d个): %s\n"
                    f"解决方案: 在 %s 中添加缺失的标的期货合约后重启策略"
                ) % (
                    len(missing_futures), missing_futures,
                    len(selected_futures_list), selected_futures_list[:20],
                    self._futures_file_path,
                )
                logging.error("[Init-VerifyFutures] ❌ %s", error_detail)
                raise RuntimeError(error_detail)

        # 构建完整订阅列表
        subscribe_list = list(selected_futures_list)
        for option_ids in selected_options_dict.values():
            subscribe_list.extend(option_ids or [])
        seen = set()
        subscribed_instruments = [inst for inst in subscribe_list if not (inst in seen or seen.add(inst))]
        _deferred_instruments = []
        if _overflow_count > 0:
            _all_options_flat = []
            for _uk, _uopts in _deferred_options.items():
                _all_options_flat.extend(_uopts or [])
            _deferred_instruments = _all_options_flat


        # ========== 阶段2：DB预注册（on_init阶段必须完成，DLL 16287上限仅影响C++订阅，不影响DB注册） ==========
        # DB预注册（ensure_registered_instruments）仅写入DuckDB的instruments_registry表，
        # 不创建C++合约对象，不受StrategyLib.dll 16287上限限制。
        # C++订阅（subscribe_instrument）才受16287上限限制，在on_start阶段分区处理。
        _DEFER_DB_REGISTRATION = False
        preregister_stats = None

        if _DEFER_DB_REGISTRATION:
            logging.info(
                "[PreRegister] 延迟注册模式: 跳过on_init阶段DB预注册，将在on_start订阅时按需注册 (合约数=%d)",
                len(subscribed_instruments),
            )
            preregister_stats = {
                'configured_count': len(subscribed_instruments),
                'registered_count': 0,
                'missing_count': len(subscribed_instruments),
                'created_count': 0,
                'failed_count': 0,
            }
        else:
            _PREREG_BATCH_SIZE = 500
            _PREREG_BATCH_PAUSE_SEC = 0.5
            last_prereg_error = None
            total_created = 0
            total_failed = 0
            total_registered = 0
            total_missing = 0

            for batch_start in range(0, len(subscribed_instruments), _PREREG_BATCH_SIZE):
                batch_end = min(batch_start + _PREREG_BATCH_SIZE, len(subscribed_instruments))
                batch_ids = subscribed_instruments[batch_start:batch_end]
                batch_num = batch_start // _PREREG_BATCH_SIZE + 1
                total_batches = (len(subscribed_instruments) + _PREREG_BATCH_SIZE - 1) // _PREREG_BATCH_SIZE
                for attempt in range(1, MAX_RETRIES + 1):
                    if not storage or not batch_ids:
                        last_prereg_error = f"批次{batch_num}: storage不可用或batch_ids为空"
                        logging.warning("[PreRegister] %s", last_prereg_error)
                        continue
                    try:
                        prereg_start = _time.perf_counter()
                        logging.info(
                            "[PreRegister] 批次%d/%d: 开始预注册 %d 个合约 (总进度 %d/%d)...",
                            batch_num, total_batches, len(batch_ids), batch_end, len(subscribed_instruments),
                        )
                        batch_stats = storage.ensure_registered_instruments(batch_ids)
                        prereg_elapsed = _time.perf_counter() - prereg_start
                        logging.info(
                            "[PreRegister] 批次%d/%d成功 (耗时=%.3fs): 配置=%d, 已注册=%d, 缺失=%d, 新建=%d, 失败=%d",
                            batch_num, total_batches, prereg_elapsed,
                            batch_stats['configured_count'],
                            batch_stats['registered_count'],
                            batch_stats['missing_count'],
                            batch_stats['created_count'],
                            batch_stats['failed_count'],
                        )
                        if batch_stats['failed_count'] > 0:
                            error_detail = (
                                f"批次{batch_num}预注册完成但有 %d 个合约注册失败，策略初始化终止。杜绝带病运行。"
                            ) % batch_stats['failed_count']
                            logging.error("[PreRegister] %s", error_detail)
                            raise RuntimeError(error_detail)
                        total_created += batch_stats['created_count']
                        total_failed += batch_stats['failed_count']
                        total_registered += batch_stats['registered_count']
                        total_missing += batch_stats['missing_count']
                        last_prereg_error = None
                        break
                    except Exception as prereg_e:
                        last_prereg_error = f"批次{batch_num}第{attempt}次: {type(prereg_e).__name__}: {prereg_e}"
                        logging.warning("[PreRegister] 预注册异常: %s", last_prereg_error)
                else:
                    error_detail = (
                        f"批次{batch_num}预注册经{MAX_RETRIES}次重试均失败，策略初始化终止。\n"
                        f"批次合约数: {len(batch_ids)}\n"
                        f"最后错误: {last_prereg_error}"
                    )
                    logging.error("[PreRegister] %s", error_detail)
                    raise RuntimeError(error_detail)
                if batch_end < len(subscribed_instruments):
                    _time.sleep(_PREREG_BATCH_PAUSE_SEC)

            preregister_stats = {
                'configured_count': len(subscribed_instruments),
                'registered_count': total_registered,
                'missing_count': total_missing,
                'created_count': total_created,
                'failed_count': total_failed,
            }
            logging.info(
                "[PreRegister] 全部完成: 配置=%d, 已注册=%d, 缺失=%d, 新建=%d, 失败=%d",
                preregister_stats['configured_count'],
                preregister_stats['registered_count'],
                preregister_stats['missing_count'],
                preregister_stats['created_count'],
                preregister_stats['failed_count'],
            )

        # ========== 阶段3：硬验证（延迟注册模式下跳过） ==========
        if _DEFER_DB_REGISTRATION:
            logging.info("[VerifyPreRegister] 延迟注册模式: 跳过硬验证，将在on_start订阅时按需验证")
        else:
            MISSING_RETRY_MAX = 3
            _VERIFY_BATCH_SIZE = 500
            _VERIFY_BATCH_PAUSE_SEC = 0.2
            for verify_attempt in range(1, MISSING_RETRY_MAX + 1):
                still_missing = []
                for _vb_start in range(0, len(subscribed_instruments), _VERIFY_BATCH_SIZE):
                    for inst_id in subscribed_instruments[_vb_start:_vb_start + _VERIFY_BATCH_SIZE]:
                        try:
                            info = storage._get_instrument_info(str(inst_id).strip())
                            if info is None:
                                still_missing.append(inst_id)
                        except Exception:
                            still_missing.append(inst_id)
                    _vb_end = min(_vb_start + _VERIFY_BATCH_SIZE, len(subscribed_instruments))
                    if _vb_end < len(subscribed_instruments):
                        _time.sleep(_VERIFY_BATCH_PAUSE_SEC)

                if not still_missing:
                    logging.info(
                        "[VerifyPreRegister] 第%d次验证通过: %d个合约全部可查询DB",
                        verify_attempt, len(subscribed_instruments),
                    )
                    break

                logging.warning(
                    "[VerifyPreRegister] 第%d次验证发现 %d/%d 个合约缺失，重试注册...",
                    verify_attempt, len(still_missing), len(subscribed_instruments),
                )
                for _vr_start in range(0, len(still_missing), _VERIFY_BATCH_SIZE):
                    for inst_id in still_missing[_vr_start:_vr_start + _VERIFY_BATCH_SIZE]:
                        try:
                            storage.register_instrument(
                                instrument_id=str(inst_id).strip(),
                                exchange=self.infer_exchange_from_id(inst_id),
                            )
                        except Exception as reg_e:
                            logging.warning("[VerifyPreRegister] 补注册失败 %s: %s", inst_id, reg_e)
                    _vr_end = min(_vr_start + _VERIFY_BATCH_SIZE, len(still_missing))
                    if _vr_end < len(still_missing):
                        _time.sleep(_VERIFY_BATCH_PAUSE_SEC)
            else:
                final_missing = []
                for _vf_start in range(0, len(subscribed_instruments), _VERIFY_BATCH_SIZE):
                    for inst_id in subscribed_instruments[_vf_start:_vf_start + _VERIFY_BATCH_SIZE]:
                        try:
                            if storage._get_instrument_info(str(inst_id).strip()) is None:
                                final_missing.append(inst_id)
                        except Exception:
                            final_missing.append(inst_id)
                    _vf_end = min(_vf_start + _VERIFY_BATCH_SIZE, len(subscribed_instruments))
                    if _vf_end < len(subscribed_instruments):
                        _time.sleep(_VERIFY_BATCH_PAUSE_SEC)
                if final_missing:
                    error_detail = (
                        f"预注册完成但 %d/%d 个合约仍不可查询DB，策略初始化终止。\n"
                        f"缺失合约(前10): %s\n"
                        f"可能原因: register_instrument静默失败、DB写入后不可读、缓存未刷新"
                    ) % (len(final_missing), len(subscribed_instruments), final_missing[:10])
                    logging.error("[VerifyPreRegister] ❌ %s", error_detail)
                    raise RuntimeError(error_detail)

        result = {
            'futures_list': list(selected_futures_list),
            'options_dict': {k: list(v) for k, v in selected_options_dict.items()},
            'subscribed_instruments': list(subscribed_instruments),
            'source': 'output_files',
            'preregister_stats': preregister_stats,
            'futures_metadata': dict(futures_metadata),
            'options_metadata': dict(options_metadata),
            'total_futures': len(selected_futures_list),
            'total_options': self._count_option_contracts(selected_options_dict),
            'total_instruments': len(subscribed_instruments),
            'instruments_in_db': True,
            'deferred_instruments': list(_deferred_instruments),
            'deferred_options': {k: list(v) for k, v in _deferred_options.items()},
            'deferred_count': _overflow_count,

        }
        del selected_futures_list, selected_options_dict, subscribed_instruments
        del futures_metadata, options_metadata


        return result

    @staticmethod
    def _normalize_instruments(instrument_ids: List[str]) -> List[str]:
        """去重、去空、去交易所前缀"""
        seen = set()
        result = []
        for inst_id in instrument_ids or []:
            normalized = str(inst_id or '').strip()
            if '.' in normalized:
                _, normalized = normalized.split('.', 1)
            if normalized and normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
        return result

    @staticmethod
    def _normalize_options_dict(options_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """规范化期权字典（去重、去空、去交易所前缀）"""
        result = {}
        for underlying, option_ids in (options_dict or {}).items():
            normalized_underlying = str(underlying or '').strip()
            if '.' in normalized_underlying:
                _, normalized_underlying = normalized_underlying.split('.', 1)
            normalized_ids = []
            seen = set()
            for opt_id in option_ids or []:
                normalized_opt = str(opt_id or '').strip()
                if '.' in normalized_opt:
                    _, normalized_opt = normalized_opt.split('.', 1)
                if normalized_opt and normalized_opt not in seen:
                    seen.add(normalized_opt)
                    normalized_ids.append(normalized_opt)
            if normalized_underlying and normalized_ids:
                result[normalized_underlying] = normalized_ids
        return result

    @staticmethod
    def _count_option_contracts(options_dict: Dict[str, List[str]]) -> int:
        """计算期权合约总数"""
        return sum(len(v) for v in (options_dict or {}).values())

    @staticmethod
    def _derive_underlying_futures(options_dict: Dict[str, List[str]]) -> List[str]:
        """从期权字典推导标的期货列表（从具体期权合约ID解析标的期货）"""
        from infra.subscription_service import SubscriptionManager
        underlying_set = set()
        OPTION_TO_FUTURE_MAP = {'MO': 'IM', 'IO': 'IF', 'HO': 'IH'}
        for option_ids in (options_dict or {}).values():
            for opt_id in option_ids:
                try:
                    parsed = SubscriptionManager.parse_option(str(opt_id).strip())
                    opt_product = parsed.get('product', '')
                    year_month = parsed.get('year_month', '')
                    if opt_product and year_month:
                        future_product = OPTION_TO_FUTURE_MAP.get(opt_product, opt_product)
                        future_id = f"{future_product}{year_month}"
                        underlying_set.add(future_id)
                except Exception:
                    continue
        return sorted(underlying_set)

    # ========================================================================
    # 品种与合约查询
    # ========================================================================

    def get_active_instruments_by_product(self, product: str) -> List[str]:
        """
        获取指定品种的所有活跃合约 (DuckDB 版本)

        Args:
            product: 品种代码（如 'IF', 'IO'）

        Returns:
            List[str]: 合约 ID 列表
        """
        instrument_ids = []
        normalized_product = str(product or '').strip()
        if not normalized_product:
            return instrument_ids

        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()

                futures = ds.query(
                    "SELECT instrument_id FROM futures_instruments "
                    "WHERE product=? ORDER BY instrument_id",
                    [normalized_product]
                )
                if hasattr(futures, 'num_rows') and futures.num_rows > 0:
                    for row in futures.read_all().to_pylist():
                        instrument_ids.append(row.get('instrument_id'))

                options = ds.query(
                    "SELECT instrument_id FROM option_instruments "
                    "WHERE product=? ORDER BY instrument_id",
                    [normalized_product]
                )
                if hasattr(options, 'num_rows') and options.num_rows > 0:
                    for row in options.read_all().to_pylist():
                        instrument_ids.append(row.get('instrument_id'))
        except Exception as e:
            logging.error("[QueryService] get_active_instruments_by_product failed: %s", e)

        return instrument_ids

    def get_active_instruments_by_products(self, products: List[str]) -> List[str]:
        """
        获取多个品种的所有活跃合约

        Args:
            products: 品种代码列表（如 ['IF', 'IH', 'IC']）

        Returns:
            List[str]: 合约 ID 列表
        """
        all_instrument_ids = []
        for product in products:
            instrument_ids = self.get_active_instruments_by_product(product.strip())
            all_instrument_ids.extend(instrument_ids)
        return all_instrument_ids

    def get_current_month_contracts(self, product: str) -> List[str]:
        """
        获取指定品种的当月合约

        Args:
            product: 品种代码

        Returns:
            List[str]: 当月合约 ID 列表
        """
        current_month = datetime.now(_CHINA_TZ).strftime('%y%m')

        all_instruments = self.get_active_instruments_by_product(product)
        current_month_contracts = []

        # ✅ 使用 params_service 元数据获取年月，而非正则
        from config.params_service import get_params_service
        ps = get_params_service()
        for inst_id in all_instruments:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta and meta.get('year_month') == current_month:
                current_month_contracts.append(inst_id)

        return current_month_contracts

    def get_next_month_contracts(self, product: str) -> List[str]:
        """
        获取指定品种的下月合约

        Args:
            product: 品种代码

        Returns:
            List[str]: 下月合约 ID 列表
        """
        next_month = self._storage._get_next_year_month()

        all_instruments = self.get_active_instruments_by_product(product)
        next_month_contracts = []

        # ✅ 使用 params_service 元数据获取年月，而非正则
        from config.params_service import get_params_service
        ps = get_params_service()
        for inst_id in all_instruments:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta and meta.get('year_month') == next_month:
                next_month_contracts.append(inst_id)

        return next_month_contracts

    # ========================================================================
    # 期权链查询
    # ========================================================================

    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """
        根据期货合约代码获取期权链（唯一入口）。'
        Args:
            future_instrument_id: 期货合约代码（如 'IF2603'）

        Returns:
            Dict: 期权链信息 {'future': {...}, 'options': [...]}
        """
        info = self._storage._get_instrument_info(future_instrument_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货合约：{future_instrument_id}")

        future_id = self._get_info_internal_id(info)

        options = []
        try:
            if _HAS_DATA_SERVICE:
                ds = get_data_service()
                # 只走 underlying_future_id 主关联
                result = ds.query(
                    "SELECT internal_id, instrument_id, option_type, strike_price "
                    "FROM option_instruments "
                    "WHERE underlying_future_id=? "
                    "ORDER BY strike_price, option_type",
                    [future_id]
                )
                if hasattr(result, 'num_rows') and result.num_rows > 0:
                    options = [
                        {
                            'internal_id': row.get('internal_id'),
                            'instrument_id': row.get('instrument_id'),
                            'option_type': row.get('option_type'),
                            'strike_price': row.get('strike_price')
                        }
                        for row in result.read_all().to_pylist()
                    ]
        except Exception as e:
            logging.error("[QueryService] get_option_chain_for_future failed: %s", e)

        return {
            'future': {'internal_id': future_id, 'instrument_id': future_instrument_id},
            'options': options
        }

    # ✅ 删除get_option_chain_by_future_id，统一使用get_option_chain_for_future

    # ========================================================================
    # ID 迁移与序列管理
    # ========================================================================

    # 方法唯一修复#69-71：删除3个DEPRECATED方法体，已迁移到StorageMaintenanceService

    def _migrate_instrument_ids_to_global_namespace(self) -> int:
        """[DEPRECATED] 已迁移到 StorageMaintenanceService。

        UPG-P1-05修复: 添加migration_guide
        Migration guide: replace _migrate_instrument_ids_to_global_namespace()
        → StorageMaintenanceService.migrate_instrument_ids_to_global_namespace()
        """
        import warnings
        warnings.warn(
            "_migrate_instrument_ids_to_global_namespace is deprecated. "
            "Migration guide: Use StorageMaintenanceService.migrate_instrument_ids_to_global_namespace() instead. "
            "Import: from infra.maintenance_service import StorageMaintenanceService",
            DeprecationWarning,
            stacklevel=2,
        )
        raise NotImplementedError(
            "_migrate_instrument_ids_to_global_namespace is deprecated. "
            "Use StorageMaintenanceService instead."
        )


_QueryInstrumentMixin = InstrumentQueryService
