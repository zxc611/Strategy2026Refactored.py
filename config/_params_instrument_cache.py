# [M1-90] 合约缓存管理
# MODULE_ID: M1-006

"""参数服务 - 合约缓存管理 (InstrumentCacheMixin)"""



from __future__ import annotations



import json  # R5-1: 保留用于json.JSONDecodeError

from ali2026v3_trading.infra.serialization_utils import json_loads

from ali2026v3_trading.infra.shared_utils import normalize_instrument_id

import os

import logging

import threading

from typing import Any, Dict, List, Optional





class InstrumentCacheService:

    """参数服务 - 合约缓存管理（从InstrumentCacheMixin重构为独立Service_"""



    def __init__(self, params_service=None):

        self._params_service = params_service

        self._lock = threading.RLock()



    def __getattr__(self, name):

        if self._params_service is not None and hasattr(self._params_service, name):

            return getattr(self._params_service, name)

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def init_instrument_cache(self) -> None:

        """

        初始化合约缓存容器
        

        _ID三层架构 - 第二层: 运行时缓存
        - 5.1.2 instrument_id_to_internal_id: instrument_id -> internal_id

        - 5.1.1 instrument_meta_by_id: internal_id -> InstrumentMeta

        """

        # _ID三层架构缓存（唯一缓存结构建
        self._instrument_id_to_internal_id: Dict[str, int] = {}  # instrument_id -> internal_id

        self._instrument_meta_by_id: Dict[int, Dict[str, Any]] = {}  # internal_id -> InstrumentMeta

        self._product_cache: Dict[str, Dict[str, Any]] = {}  # product -> product_info

        self._column_cache: Dict[str, List[str]] = {}  # table_name -> column_names

        self._column_cache_lock = threading.Lock()

    

    def _normalize_id_field(self, info: Dict[str, Any]) -> Dict[str, Any]:

        """ID字段标准化：删除旧id字段（统一使用internal_id_"""

        if 'id' in info:

            del info['id']

        return info



    def cache_instrument_info(self, instrument_id: str, info: Dict[str, Any]) -> Dict[str, Any]:

        """

        写入缓存并强强internal_id 全局唯一，禁止静默覆盖
        

        _ID三层架构：同时更新两层运行时缓存

        - 5.1.2 instrument_id_to_internal_id: instrument_id -> internal_id

        - 5.1.1 instrument_meta_by_id: internal_id -> InstrumentMeta

        

        Args:

            instrument_id: 合约代码

            info: 合约信息

            

        Returns:

            缓存后的合约信息

            

        Raises:

            RuntimeError: 检测到 internal_id 冲突时抛。'
        """

        with self._lock:

            cached_info = dict(info)

            # A-01修复：标准化instrument_id，确保键格式一致
            # 避免数据库存储带前缀（SHFE.au2501）而平台推送不带前缀（au2501）导致查找失败
            normalized_instrument_id = normalize_instrument_id(instrument_id)
            cached_info['instrument_id'] = normalized_instrument_id



            # _统一ID标准化入口
            self._normalize_id_field(cached_info)



            cid = cached_info.get('internal_id')

            if cid is None:

                logging.warning(

                    "[ParamsService] internal_id=None, 跳过缓存: instrument_id=%s",

                    instrument_id

                )

                return cached_info

            

            # _ID三层架构 - 检查internal_id冲突

            existing = self._instrument_meta_by_id.get(cid)

            if existing and existing.get('instrument_id') != instrument_id:

                raise RuntimeError(

                    "检测到重复 internal_id=%s: %s _%s 冲突" % (

                        cid,

                        existing.get('instrument_id'),

                        instrument_id,

                    )

                )

            
            
            # _更新ID三层架构缓存（在锁内存 仅写入新架构2个字典
            # 5.1.2: instrument_id -> internal_id

            self._instrument_id_to_internal_id[normalized_instrument_id] = cid

            

            # 5.1.1: internal_id -> InstrumentMeta

            self._instrument_meta_by_id[cid] = cached_info

            

            return cached_info

    

    # ========================================================================

    # _ID三层架构 - 新增API

    # ========================================================================

    

    def get_internal_id(self, instrument_id: str) -> Optional[int]:

        """

        _5.1.2: 通过 instrument_id 获取 internal_id

        

        Args:

            instrument_id: 平台原始合约代码（如 'IH2605'_
            

        Returns:

            internal_id: 系统内部代理键，如果不存在则返回 None

        """

        with self._lock:  # _双重检查锁。'
            if not self._instrument_id_to_internal_id:

                self._lazy_load_from_db()

            return self._instrument_id_to_internal_id.get(instrument_id)

    

    def get_instrument_meta(self, internal_id: int) -> Optional[Dict[str, Any]]:

        """

        _5.1.1: 通过 internal_id 获取 InstrumentMeta

        

        Args:

            internal_id: 系统内部代理理
            

        Returns:

            InstrumentMeta: 合约元数据，如果不存在则返回 None

        """

        with self._lock:

            if not self._instrument_meta_by_id:

                self._lazy_load_from_db()

            return self._instrument_meta_by_id.get(internal_id)

    

    def get_instrument_meta_by_id(self, instrument_id: str) -> Optional[Dict[str, Any]]:

        """

        _便捷方法：通过 instrument_id 直接获取 InstrumentMeta

        

        Args:

            instrument_id: 平台原始合约代码

            

        Returns:

            InstrumentMeta: 合约元数据，如果不存在则返回 None

        """

        internal_id = self.get_internal_id(instrument_id)

        if internal_id is None:

            return None

        return self.get_instrument_meta(internal_id)

    

    def validate_contracts_loaded(self, contract_ids: List[str], contract_type: str = "contract") -> List[str]:

        """

        _强制验证：确保配置表中的所有合约都已加载到缓存

        

        Args:

            contract_ids: 需要验证的合约ID列表

            contract_type: 合约类型描述（用于错误提示）'
        Returns:

            failed_contracts: 加载失败的合约列表
            

        Raises:

            RuntimeError: 如果有合约加载失败
        """

        # 确保缓存已加载（使用新架构字典判断）

        if not self._instrument_id_to_internal_id:

            self._lazy_load_from_db()

        

        failed = []

        for contract_id in contract_ids:

            iid = self._instrument_id_to_internal_id.get(contract_id)

            if iid is None:

                failed.append(contract_id)

        

        if failed:

            error_msg = (

                f"[ParamsService] CRITICAL: {len(failed)} {contract_type}(s) failed to load: "

                f"{failed[:10]}{'...' if len(failed) > 10 else ''}. "

                f"Strategy cannot start without complete contract metadata."

            )

            logging.error(error_msg)

            raise RuntimeError(error_msg)

        

        return []



    def _lazy_load_from_db(self) -> None:

        """惰性从数据库加载合约缓存（仅当缓存为空时执行）"""

        try:

            from ali2026v3_trading.data.data_service import get_existing_data_service

            ds = get_existing_data_service()

            if ds is None:

                logging.debug("[ParamsService] DataService尚未初始化，跳过惰性加载")
                return

            

            if hasattr(ds, '_products_loaded') and not ds._products_loaded:

                logging.warning(

                    "[ParamsService] ⚠️  品种配置尚未加载（_products_loaded=False），"

                    "跳过惰性加载以避免空缓存，"

                    "请确保先调用 ensure_products_with_retry() / ds.mark_products_loaded()"

                )

                return

            

            if not hasattr(ds, '_get_connection'):

                logging.warning("[ParamsService] DataService实例不完整，跳过惰性加载")
                return

            

            self.load_caches_from_db(ds)

            logging.info("[ParamsService] 惰性加载缓存完成：%d 个合并", len(self._instrument_id_to_internal_id))

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            logging.error("[ParamsService] 惰性加载缓存失败 %s", e)

    

    def get_id_cache(self, internal_id: int) -> Optional[Dict[str, Any]]:

        """[DEPRECATED] 请使。get_instrument_meta(internal_id)



        UPG-P1-05修复: 添加migration_guide参数

        """

        import warnings

        warnings.warn(

            "get_id_cache is deprecated, use get_instrument_meta instead. "

            "Migration guide: replace get_id_cache(id) _get_instrument_meta(id), "

            "return type unchanged (Optional[Dict]).",

            DeprecationWarning,

            stacklevel=2,

        )

        return self.get_instrument_meta(internal_id)

    

    def get_product_cache(self, product: str) -> Optional[Dict[str, Any]]:

        """获取品种信息"""

        with self._lock:

            return self._product_cache.get(product)

    

    def clear_instrument_cache(self) -> None:

        """

        BP-9修复：原子替换模式——先构建新缓存，再一次性替换，避免清空窗口期权
        如果重新加载失败，保留旧缓存（宁用过期数据不丢数据）_
        """

        with self._lock:

            old_id_map = dict(self._instrument_id_to_internal_id)

            old_meta_map = dict(self._instrument_meta_by_id)

            old_product_cache = dict(self._product_cache)



        try:

            self._rebuild_instrument_cache()

            with self._lock:

                new_count = len(self._instrument_meta_by_id)

            logging.info("[ParamsService] 合约缓存原子替换完成: %d _%d", len(old_meta_map), new_count)

            # DFG-P1-08修复: 缓存重建后通过EventBus发布数据格式变更事件

            try:

                from ali2026v3_trading.infra.event_bus import get_global_event_bus, DataFormatChangedEvent

                _bus = get_global_event_bus()

                if _bus is not None:

                    _event = DataFormatChangedEvent(

                        source='ParamsService.clear_instrument_cache',

                        old_version=str(len(old_meta_map)),

                        new_version=str(new_count),

                        affected_tables=['futures_instruments', 'option_instruments'],

                        change_description=f'合约缓存原子替换: {len(old_meta_map)}→{new_count}',

                    )

                    _bus.publish(_event, async_mode=True)

            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                logging.warning("[R22-EP-P1] ParamsService EventBus publish failed: %s", _r3_err)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            with self._lock:

                self._instrument_id_to_internal_id = old_id_map

                self._instrument_meta_by_id = old_meta_map

                self._product_cache = old_product_cache

            logging.error("[ParamsService] 合约缓存重新加载失败，保留旧缓存: %s", e)



    def _rebuild_instrument_cache(self) -> None:

        """构建全新的合约缓存并原子替换（内部方法，复用load_caches_from_db_"""

        try:

            from ali2026v3_trading.data.data_service import get_data_service

            ds = get_data_service()

            if ds is None:

                logging.warning("[ParamsService] DataService不可用，跳过缓存重建")

                return

            if not hasattr(ds, '_get_connection'):

                logging.warning("[ParamsService] DataService实例不完整，跳过缓存重建")

                return

            self.load_caches_from_db(ds)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            logging.error("[ParamsService] 缓存重建失败: %s", e)

            raise



    def get_all_instrument_cache(self) -> Dict[str, Dict[str, Any]]:

        """返回当前合约缓存的深拷贝副本（基于新架构）_"""

        import copy

        with self._lock:

            # 从新架构构建等效的instrument_cache格式

            result = {}

            for instrument_id, internal_id in self._instrument_id_to_internal_id.items():

                meta = self._instrument_meta_by_id.get(internal_id)

                if meta:

                    result[instrument_id] = meta

            return {k: dict(v) if isinstance(v, dict) else v for k, v in result.items()}  # R21-MEM-P1-04修复: 浅拷贝替代deepcopy



    def get_all_instrument_ids(self) -> List[str]:

        """返回当前缓存中的全部合约代码（基于新架构）_"""

        with self._lock:

            return list(self._instrument_id_to_internal_id.keys())

    

    # ========================================================================

    # 合约列表加载与缓存（strategy_core_service.py 依赖赖
    # ========================================================================

    

    def load_instrument_list(self, params: Any, source: str = 'param_cache') -> Optional[Dict[str, Any]]:
        """加载合约列表（从参数缓存或输出文件）
        Args:
            params: 参数对象（dict或对象）
            source: 加载来源，'param_cache' 或 'output_files'
        Returns:
            Dict: {'futures_list': [...], 'options_dict': {...}} 或 None
        """

        try:

            futures_metadata = {}

            options_metadata = {}

            # 从params中读取合约列表
            if isinstance(params, dict):

                futures_list = params.get('future_instruments', [])

                options_dict = params.get('option_instruments', {})

                futures_metadata = params.get('futures_metadata', {})

                options_metadata = params.get('options_metadata', {})

            else:

                futures_list = getattr(params, 'future_instruments', [])

                options_dict = getattr(params, 'option_instruments', {})

                futures_metadata = getattr(params, 'futures_metadata', {})

                options_metadata = getattr(params, 'options_metadata', {})

            

            # 如果source是output_files，尝试从文件加载（带3次重试）'
            if source == 'output_files':

                # P0修复：合约配置文件加载增量次重试机。'
                max_retries = 3

                retry_delay = 1.0  # 每次重试间隔1_
                

                futures_metadata = {}

                options_metadata = {}

                _total_loaded = 0

                for attempt in range(1, max_retries + 1):

                    futures_from_file = []

                    options_from_file = []
                    _futures_meta = {}
                    _options_meta = {}
                    _fixed_config_files = [
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subscription_futures_fixed.txt'),
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subscription_options_fixed.txt'),
                    ]
                    load_success = True
                    for fixed_config_file in _fixed_config_files:
                        if not os.path.exists(fixed_config_file):

                            continue

                        try:

                            is_futures_file = 'futures' in os.path.basename(fixed_config_file)

                            with open(fixed_config_file, 'r', encoding='utf-8') as f:

                                for line in f:

                                    line = line.strip()

                                    if not line or line.startswith('#'):

                                        continue

                                    parts = line.split('|')

                                    contract_part = parts[0]

                                    if '.' in contract_part:

                                        contract = contract_part.split('.', 1)[1]

                                    else:

                                        contract = contract_part

                                    if is_futures_file:

                                        futures_from_file.append(contract)

                                        _total_loaded += 1

                                        internal_id = int(parts[1]) if len(parts) > 1 else None

                                        product = parts[2] if len(parts) > 2 else None

                                        year_month = parts[3] if len(parts) > 3 else None
                                        if internal_id is not None:
                                            _futures_meta[contract] = {
                                                'internal_id': internal_id,
                                                'product': product,
                                                'year_month': year_month,
                                            }
                                    else:
                                        options_from_file.append(contract)
                                        internal_id = int(parts[1]) if len(parts) > 1 else None
                                        underlying_future_id = int(parts[2]) if len(parts) > 2 else None
                                        product = parts[3] if len(parts) > 3 else None
                                        underlying_product = parts[4] if len(parts) > 4 else None
                                        year_month = parts[5] if len(parts) > 5 else None
                                        option_type = parts[6] if len(parts) > 6 else None
                                        strike_price = float(parts[7]) if len(parts) > 7 else None
                                        if internal_id is not None:
                                            _options_meta[contract] = {
                                                'internal_id': internal_id,
                                                'underlying_future_id': underlying_future_id,
                                                'product': product,
                                                'underlying_product': underlying_product,
                                                'year_month': year_month,
                                                'option_type': option_type,
                                                'strike_price': strike_price,
                                            }
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                            logging.warning(f"[ParamsService] 加载固定配置文件失败 (attempt {attempt}/{max_retries}): {e}")
                            load_success = False

                            break

                    

                    if load_success and (futures_from_file or options_from_file):

                        futures_metadata = _futures_meta

                        options_metadata = _options_meta

                        if attempt > 1:

                            logging.info(f"[ParamsService] 配置文件加载成功 (retry {attempt-1})")

                        break

                    elif attempt < max_retries:

                        logging.warning(f"[ParamsService] 配置文件加载失败，{retry_delay}s后重复({attempt}/{max_retries})...")

                        import threading

                        threading.Event().wait(retry_delay)  # [R22-P2-TIME06] 替代time.sleep以便可中断
                    else:

                        error_details = []

                        for fixed_config_file in _fixed_config_files:

                            if os.path.exists(fixed_config_file):

                                try:

                                    with open(fixed_config_file, 'r', encoding='utf-8') as f:

                                        content = f.read()

                                        error_details.append(f"  - {os.path.basename(fixed_config_file)}: exists, size={len(content)} bytes")

                                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:

                                    error_details.append(f"  - {os.path.basename(fixed_config_file)}: exists but unreadable: {e}")

                            else:

                                error_details.append(f"  - {os.path.basename(fixed_config_file)}: NOT FOUND at {fixed_config_file}")

                        

                        logging.error(

                            "[ParamsService] CRITICAL: Contract configuration loading failed after %d retries.\n"

                            "  File status:\n"

                            "%s\n"

                            "  Action required:\n"

                            "    - Verify subscription_futures_fixed.txt and subscription_options_fixed.txt exist and are valid\n"

                            "    - Check file permissions and disk space\n"

                            "    - Ensure no other process is locking these files",

                            max_retries, "\n".join(error_details)

                        )



                if futures_from_file:

                    futures_list = futures_from_file

                if options_from_file:

                    options_dict = {}

                    for opt in options_from_file:

                        product = ''.join([c for c in opt if c.isalpha()])[:2]

                        if product:

                            if product not in options_dict:

                                options_dict[product] = []

                            options_dict[product].append(opt)

                    logging.info(f"[ParamsService] 从固定配置文件加载：{len(futures_list)} 期货，{len(options_from_file)} 期权")
                if not futures_list and not options_dict:

                    config_dir = self.get_str('config_dir', '.arts')

                    futures_file = os.path.join(config_dir, 'futures_list.json')

                    options_file = os.path.join(config_dir, 'options_dict.json')

                    

                    if os.path.exists(futures_file):

                        with open(futures_file, 'r', encoding='utf-8') as f:

                            futures_list = json_loads(f.read())  # R5-1

                    if os.path.exists(options_file):

                        with open(options_file, 'r', encoding='utf-8') as f:

                            options_dict = json_loads(f.read())  # R5-1

            

            if futures_list or options_dict:

                result = {

                    'futures_list': list(futures_list) if futures_list else [],

                    'options_dict': dict(options_dict) if options_dict else {},

                    'futures_metadata': futures_metadata,
                    'options_metadata': options_metadata,
                }
                return result

            logging.error(

                "[ParamsService.load_instrument_list] No instruments loaded "

                "(futures=%d, options=%d, source=%s)",

                len(futures_list), len(options_dict), source

            )

            raise ValueError(

                f"[ParamsService.load_instrument_list] No instruments loaded "

                f"(futures={len(futures_list)}, options={len(options_dict)}, source={source})"

            )

        except ValueError:

            raise

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.error(f"[ParamsService.load_instrument_list] Error: {e}")

            raise RuntimeError(

                f"[ParamsService.load_instrument_list] Failed to load instrument list: {e}"

            ) from e

    

    def cache_instrument_list(self, params: Any, futures_list: List[str], 

                              options_dict: Dict[str, List[str]], source: str = 'param_cache') -> None:

        """

        缓存合约列表到参数对手
        

        Args:

            params: 参数对象（dict或对象）'
            futures_list: 期货合约列表

            options_dict: 期权合约字典

            source: 缓存来源标识

        """

        try:

            if isinstance(params, dict):

                params['future_instruments'] = list(futures_list)

                params['option_instruments'] = dict(options_dict)

                params['_instrument_list_source'] = source

            else:

                setattr(params, 'future_instruments', list(futures_list))

                setattr(params, 'option_instruments', dict(options_dict))

                setattr(params, '_instrument_list_source', source)

            logging.debug(f"[ParamsService.cache_instrument_list] Cached {len(futures_list)} futures, {len(options_dict)} option groups from {source}")

        except (ValueError, KeyError, TypeError, AttributeError) as e:

            logging.warning(f"[ParamsService.cache_instrument_list] Error: {e}")

    

    def cache_column_names(self, table_name: str, columns: List[str]) -> None:

        """缓存表列表"""

        with self._column_cache_lock:

            self._column_cache[table_name] = columns

    

    def get_cached_column_names(self, table_name: str) -> Optional[List[str]]:

        """获取缓存的表列名"""

        with self._column_cache_lock:

            return self._column_cache.get(table_name)

    

    def load_caches_from_db(self, data_service) -> None:

        """

        从数据库加载合约缓存

        

        _恢复设计：从合约配置表直接加载所有合约品种，创建元数据表

        """

        from ali2026v3_trading.data.data_service import get_data_service

        ds = data_service or get_data_service()



        temp_instrument_id_to_internal_id: Dict[str, int] = {}

        temp_instrument_meta_by_id: Dict[int, Dict[str, Any]] = {}

        temp_product_cache: Dict[str, Dict[str, Any]] = {}



        def _cache_temp_instrument_info(instrument_id: str, info: Dict[str, Any]) -> None:

            """复用例normalize_id_field进行ID标准准"""

            cached_info = dict(info)

            # FIX-P0-08: 标准化instrument_id，与cache_instrument_info保持一致
            # 避免DB存储带前缀(SHFE.au2501)而查找用标准化key(au2501)导致查找失败
            normalized_instrument_id = normalize_instrument_id(instrument_id)
            cached_info['instrument_id'] = normalized_instrument_id



            # _复用例normalize_id_field统一ID标准准
            self._normalize_id_field(cached_info)

            cid = cached_info.get('internal_id')



            if cid is None:

                logging.warning(

                    "[ParamsService] internal_id=None, 跳过ID缓存: instrument_id=%s",

                    instrument_id

                )

                return



            existing = temp_instrument_meta_by_id.get(cid)

            if existing and existing.get('instrument_id') != instrument_id:

                raise RuntimeError(

                    "检测到重复 internal_id=%s: %s _%s 冲突" % (

                        cid,

                        existing.get('instrument_id'),

                        instrument_id,

                    )

                )



            temp_instrument_id_to_internal_id[normalized_instrument_id] = cid

            temp_instrument_meta_by_id[cid] = cached_info

        

        def _rows(result):

            if result is None:

                return []

            if hasattr(result, 'read_all'):

                result = result.read_all()

            if hasattr(result, 'to_pylist'):

                return result.to_pylist()

            if hasattr(result, 'to_dict'):

                return result.to_dict('records')

            if hasattr(result, '__iter__'):

                return list(result)

            return []

        

        # _步骤1：加载所有期货合约（无任何过滤）'
        futures_loaded = 0

        try:

            futures_rows = _rows(ds.query("SELECT internal_id, instrument_id, product, exchange, year_month, product_code, shard_key FROM futures_instruments"))

            

            for row in futures_rows:

                try:

                    product_code = row.get('product_code') or row['product'].lower()

                    shard_key = row.get('shard_key')

                    if shard_key is None:

                        from ali2026v3_trading.infra.shared_utils import ShardRouter

                        shard_key = ShardRouter._deterministic_hash(product_code)

                    info = {

                        'internal_id': row['internal_id'],

                        'type': 'future',

                        'product': row['product'],

                        'exchange': row['exchange'],

                        'year_month': row['year_month'],

                        'product_code': product_code,

                        'shard_key': shard_key,

                    }

                    _cache_temp_instrument_info(row['instrument_id'], info)

                    futures_loaded += 1

                except RuntimeError as e:

                    logging.error(f"[ParamsService] 期货合约缓存失败: {e}")

                    continue

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            logging.warning(f"[ParamsService] 查询期货合约失败: {e}")

        

        logging.info(f"[ParamsService] 期货合约加载完成: {futures_loaded}")

        

        # _步骤2：加载所有期权合约（无任何过滤）'
        options_loaded = 0

        try:

            options_rows = _rows(ds.query("SELECT internal_id, instrument_id, product, exchange, underlying_future_id, underlying_product, year_month, option_type, strike_price, product_code, shard_key FROM option_instruments"))

            

            for row in options_rows:

                try:

                    product_code = row.get('product_code') or row['product'].lower()

                    shard_key = row.get('shard_key')

                    if shard_key is None:

                        from ali2026v3_trading.infra.shared_utils import ShardRouter

                        shard_key = ShardRouter._deterministic_hash(product_code)

                    info = {

                        'internal_id': row['internal_id'],

                        'type': 'option',

                        'product': row['product'],

                        'exchange': row['exchange'],

                        'underlying_future_id': row['underlying_future_id'],

                        'underlying_product': row['underlying_product'],

                        'year_month': row['year_month'],

                        'option_type': row['option_type'],

                        'strike_price': row['strike_price'],

                        'product_code': product_code,

                        'shard_key': shard_key,

                    }

                    _cache_temp_instrument_info(row['instrument_id'], info)

                    options_loaded += 1

                except RuntimeError as e:

                    logging.error(f"[ParamsService] 期权合约缓存失败: {e}")

                    continue

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:

            logging.warning(f"[ParamsService] 查询期权合约失败: {e}")

        

        logging.info(f"[ParamsService] 期权合约加载完成: {options_loaded}")

        logging.info(f"[ParamsService] 总合约数: {futures_loaded + options_loaded} _(期货={futures_loaded}, 期权={options_loaded})")

        

        # _步骤3：加载所有品种元数据（不限制活跃状态）'
        try:

            for row in _rows(ds.query("SELECT product, exchange, format_template, tick_size, contract_size FROM future_products")):

                temp_product_cache[row['product']] = {'type': 'future', **dict(row)}

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, IOError) as e:

            logging.warning(f"[ParamsService] 查询期货品种元数据失败 {e}")

        

        try:

            for row in _rows(ds.query("SELECT product, exchange, underlying_product, format_template, tick_size, contract_size FROM option_products")):

                temp_product_cache[row['product']] = {'type': 'option', **dict(row)}

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError, IOError) as e:

            logging.warning(f"[ParamsService] 查询期权品种元数据失败 {e}")



        with self._lock:

            self._instrument_id_to_internal_id = temp_instrument_id_to_internal_id

            self._instrument_meta_by_id = temp_instrument_meta_by_id

            self._product_cache = temp_product_cache

        
        if len(self._instrument_id_to_internal_id) == 0:
            logging.error("[ParamsService] load_caches_from_db loaded 0 instruments! DB may be empty or query failed")

        logging.info(f"加载缓存：{len(self._instrument_id_to_internal_id)}个合约，{len(self._product_cache)}个品种")





InstrumentCacheMixin = InstrumentCacheService

    # ========================================================================

    # 参数加载

    # ========================================================================



