# MODULE_ID: M1-049
"""
storage_query_instrument.py — StorageInstrumentService
合约信息查询 + 合约注册/删除/批量 + 期权链查询

重构说明 (2026-06-11):
- _StorageQueryInstrumentMixin → StorageInstrumentService（服务提取+Facade组合，消灭Mixin）
- 构造函数显式接收 base_service (StorageQueryBaseService)，组合替代继承
"""

from infra.subscription_service import SubscriptionManager, inst_get
from infra.health_monitor import DiagnosisProbeManager
import logging
import time
from typing import List, Dict, Optional, Any

from infra.shared_utils import InitPhase, requires_phase


class StorageInstrumentService:

    def __init__(self, base_service):
        self._base = base_service

    def __getattr__(self, name):
        delegated = [
            '_lock', '_params_service', '_data_service', '_maintenance_service',
            '_runtime_missing_warned', '_assigned_ids', 'max_retries', 'retry_delay',
            'batch_size', '_TABLE_NAME_PATTERN',
            '_ensure_exchange_map_loaded', '_validate_table_name',
            '_execute_with_retry', '_parse_future', '_parse_option_with_dash',
            '_make_instrument_info', '_get_info_internal_id',
            'infer_exchange_from_id', '_validate_tick', '_validate_kline',
            '_get_instrument_info', '_get_info_by_id',
            'get_registered_instrument_ids', 'load',
            '_cache_to_params_service', '_cache_alias_instrument_mapping',
            '_warn_runtime_unregistered', '_get_registered_instrument_info',
            '_get_registered_internal_id', '_query_rows', 'connection', '_q',
            '_to_timestamp', '_normalize_tick_fields', '_validate_signal',
            '_validate_underlying', '_json_default', '_load_caches',
            '_init_kv_store', '_load_exchange_map_from_config',
            '_resolve_kline_provider', '_estimate_kline_count',
            '_normalize_kline_period', '_get_next_id',
            '_create_future_tables_by_id', '_create_option_tables_by_id',
            '_extract_table_suffix_id',
        ]
        if name in delegated:
            return getattr(self._base, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def get_option_chain_by_future_id(self, future_id: int) -> Dict:
        info = self._get_info_by_id(future_id)
        if not info or info['type'] != 'future':
            raise ValueError(f"无效的期货ID: {future_id}")
        future_instrument = info.get('instrument_id')
        if not future_instrument:
            rows = self._data_service.query("SELECT instrument_id FROM futures_instruments WHERE internal_id=?", [future_id]).to_pylist()
            row = rows[0] if rows else None
            if row:
                future_instrument = row['instrument_id']
        if not future_instrument:
            raise ValueError(f"未找到期货合约ID: {future_id}")

        options = self._data_service.query("""
            SELECT internal_id, instrument_id, option_type, strike_price
            FROM option_instruments
            WHERE underlying_future_id=?
            ORDER BY strike_price, option_type
        """, [future_id]).to_pylist()

        normalized_options = []
        for row in options:
            option_internal_id = int(row['internal_id'])
            normalized_options.append({
                'internal_id': option_internal_id,
                'instrument_id': row['instrument_id'],
                'option_type': row['option_type'],
                'strike_price': row['strike_price'],
            })

        return {
            'future': {'internal_id': future_id, 'instrument_id': future_instrument},
            'options': normalized_options
        }

    def get_option_chain_for_future(self, future_instrument_id: str) -> Dict:
        """
        根据期货合约代码获取期权链（委托给QueryService）。'
        Args:
            future_instrument_id: 期货合约代码

        Returns:
            Dict: 期权链信息
        """
        from data.query_service import QueryService
        qs = QueryService(self)
        return qs.get_option_chain_for_future(future_instrument_id)

    @requires_phase(InitPhase.DB_SCHEMA)
    def query_kline(self, instrument_id: str, start_time: str, end_time: str,
                    limit: Optional[int] = None) -> List[Dict]:
        internal_id = self.register_instrument(instrument_id)
        return self.query_kline_by_id(internal_id, start_time, end_time, limit)

    @requires_phase(InitPhase.DB_SCHEMA)
    def query_tick(self, instrument_id: str, start_time: str, end_time: str,
                   limit: Optional[int] = None) -> List[Dict]:
        internal_id = self.register_instrument(instrument_id)
        return self.query_tick_by_id(internal_id, start_time, end_time, limit)

    @requires_phase(InitPhase.DB_SCHEMA)
    def get_option_chain(self, ts: float, underlying: str, expiration: str) -> List[Dict[str, Any]]:
        try:
            rows = self._data_service.query(
                "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                [underlying, expiration]
            ).to_pylist()

            if not rows:
                logging.warning(f"[Storage] 期货合约未注册: {underlying}{expiration}，无法查询期权链")
                return []

            future_instrument_id = rows[0]['instrument_id']
            chain = self.get_option_chain_for_future(future_instrument_id)
            return chain.get('options', [])
        except Exception as e:
            logging.error("[R22-EP-P1] get_option_chain failed(数据库异常→空列表，调用方无法区分无数据/故障): %s", e, exc_info=True)
            return []

    def get_latest_underlying(self, underlying: str, expiration: str) -> Optional[Dict[str, Any]]:
        logging.debug("get_latest_underlying called: %s %s", underlying, expiration)
        return None

    @staticmethod
    def load_all_instruments(strategy_instance=None, params=None, logger=None):
        import logging as stdlib_logging

        if logger is None:
            logger = stdlib_logging.getLogger(__name__)

        futures_list = []
        options_dict = {}

        try:
            if strategy_instance is not None:
                try:
                    instruments = getattr(strategy_instance, 'subscribed_instruments', {})
                    for inst_id, inst_info in instruments.items():
                        try:
                            product = inst_get(inst_info, 'product', 'Product', 'underlying_product')
                            instrument_id = inst_get(inst_info, 'instrument_id', 'InstrumentID', '_instrument_id')

                            if not instrument_id:
                                continue

                            is_option = False
                            try:
                                SubscriptionManager.parse_option(instrument_id)
                                is_option = True
                            except Exception as _parse_err:
                                logging.debug("[Storage] 合约解析失败(已降级): %s", _parse_err)

                            if is_option:
                                underlying_future_id = inst_get(inst_info, 'underlying_future_id')
                                if underlying_future_id:
                                    try:
                                        from data.data_service import get_data_service
                                        ds = get_data_service()
                                        future_rows = ds.query(
                                            "SELECT instrument_id FROM futures_instruments WHERE internal_id = ?",
                                            [underlying_future_id]
                                        ).to_pylist()
                                        if future_rows and len(future_rows) > 0:
                                            future_instrument_id = future_rows[0]['instrument_id']
                                        else:
                                            continue
                                    except Exception:
                                        continue

                                    if future_instrument_id not in options_dict:
                                        options_dict[future_instrument_id] = []
                                    options_dict[future_instrument_id].append({
                                        'instrument_id': instrument_id,
                                        'product': product,
                                        'underlying_future_id': underlying_future_id,
                                        'exchange': inst_get(inst_info, 'exchange', 'ExchangeID'),
                                        'year_month': inst_get(inst_info, 'year_month', 'DeliveryMonth'),
                                        'strike_price': inst_get(inst_info, 'strike_price', 'StrikePrice'),
                                        'option_type': inst_get(inst_info, 'option_type', 'OptionsType')
                                    })
                            else:
                                futures_list.append({
                                    'instrument_id': instrument_id,
                                    'product': product,
                                    'exchange': inst_get(inst_info, 'exchange', 'ExchangeID'),
                                    'year_month': inst_get(inst_info, 'year_month', 'DeliveryMonth')
                                })
                        except Exception as e:
                            logger.debug("跳过合约处理失败：%s", e)
                except Exception as e:
                    logger.warning("从策略实例获取合约失败：%s", e)

            if not futures_list and not options_dict and params is not None:
                try:
                    configured_instruments = getattr(params, 'instrument_ids', [])
                    for inst_id in configured_instruments:
                        try:
                            if isinstance(inst_id, str):
                                is_option = False
                                opt_parsed = None
                                try:
                                    opt_parsed = SubscriptionManager.parse_option(inst_id)
                                    is_option = True
                                except Exception as _parse_err:
                                    logging.debug("[Storage] 合约解析失败(已降级): %s", _parse_err)

                                if is_option and opt_parsed:
                                    underlying = opt_parsed.get('underlying', '')
                                    if not underlying:
                                        underlying = SubscriptionManager.parse_future(inst_id).get('product', inst_id)
                                    if underlying not in options_dict:
                                        options_dict[underlying] = []
                                    options_dict[underlying].append({
                                        'instrument_id': inst_id,
                                        'underlying': underlying
                                    })
                                else:
                                    futures_list.append({
                                        'instrument_id': inst_id
                                    })
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug("从 params 获取合约失败：%s", e)

            logger.info("加载完成：%d 个期货，%d 个品种期权", len(futures_list), len(options_dict))
            return futures_list, options_dict

        except Exception as e:
            logger.error("[R22-EP-P1] load_all_instruments 异常(数据库异常→空结果，调用方无法区分无数据/故障): %s", e, exc_info=True)
            return [], {}

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
    def ensure_registered_instruments(self, instrument_ids: List[str]) -> Dict[str, int]:
        from data.query_service import QueryService

        return QueryService(self).ensure_registered_instruments(instrument_ids)

    @requires_phase(InitPhase.EXTERNAL_SERVICES)
    def register_instrument(self, instrument_id: str, exchange: str = "AUTO",
                            expire_date: Optional[str] = None,
                            listing_date: Optional[str] = None) -> int:
        normalized_instrument_id = str(instrument_id or '').strip()
        if not normalized_instrument_id:
            raise ValueError("instrument_id 不能为空")

        if str(exchange or 'AUTO').strip() == 'AUTO':
            exchange = self.infer_exchange_from_id(normalized_instrument_id)

        existing_meta = self._params_service.get_instrument_meta_by_id(normalized_instrument_id)
        if existing_meta:
            return existing_meta.get('internal_id')

        # 查询数据库
        info = self._get_instrument_info(normalized_instrument_id)
        if info:
            return self._get_info_internal_id(info)

        # 注册新合约（加锁保证唯一性）'
        with self._lock:
            from infra.shared_utils import ShardRouter
            existing_meta = self._params_service.get_instrument_meta_by_id(normalized_instrument_id)
            if existing_meta:
                return existing_meta.get('internal_id')
            normalized_upper = str(normalized_instrument_id).strip().upper()
            rows = self._data_service.query("SELECT product, exchange, format_template, is_active FROM future_products WHERE is_active=1").to_pylist()
            _fp_matched_inactive = False
            for prod in rows:
                product_key = str(prod['product'] or '').strip()
                if normalized_upper.startswith(product_key.upper()):
                    try:
                        parsed = self._parse_future(normalized_instrument_id)
                        # 生成新ID
                        new_id = self._get_next_id()
                        kline_table = f"kline_future_{new_id}"
                        tick_table = f"tick_future_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        self._data_service.query("""
                            INSERT INTO futures_instruments
                            (internal_id, exchange, instrument_id, product, year_month, format,
                             expire_date, listing_date, kline_table, tick_table, product_code, shard_key, is_active)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [new_id, exchange, normalized_instrument_id, parsed['product'], parsed['year_month'],
                              prod['format_template'], expire_date, listing_date, kline_table, tick_table,
                              parsed['product'].lower(), ShardRouter._deterministic_hash(parsed['product'].lower()), 1], raise_on_error=True)
                        self._create_future_tables_by_id(new_id)
                        # 更新缓存
                        info = self._make_instrument_info(
                            new_id,
                            normalized_instrument_id,
                            'future',
                            product=parsed['product'],
                            year_month=parsed['year_month'],
                            kline_table=kline_table,
                            tick_table=tick_table,
                        )
                        self._assigned_ids.add(new_id)  # v1吸收：跟踪已分配ID
                        self._cache_to_params_service(normalized_instrument_id, info)  # v1吸收：双向缓存同步
                        logging.info(f"注册期货合约：{normalized_instrument_id} -> ID={new_id}")

                        # DiagnosisProbeManager already imported at module level
                        DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'future')

                        return new_id
                    except ValueError:
                        continue

            # FIX-P0-14: 活跃品种定义过滤 - is_active=1 阻碍新合约注册
            # 若活跃产品中无匹配，回退查询非活跃产品并告警
            if not _fp_matched_inactive:
                _inactive_rows = self._data_service.query("SELECT product, exchange, format_template FROM future_products WHERE is_active=0").to_pylist()
                for prod in _inactive_rows:
                    product_key = str(prod['product'] or '').strip()
                    if normalized_upper.startswith(product_key.upper()):
                        _fp_matched_inactive = True
                        logging.warning(
                            "[FIX-P0-14] 期货品种 %s 被标记为 is_active=0(非活跃), 仍尝试注册合约 %s",
                            product_key, normalized_instrument_id,
                        )
                        try:
                            parsed = self._parse_future(normalized_instrument_id)
                            new_id = self._get_next_id()
                            kline_table = f"kline_future_{new_id}"
                            tick_table = f"tick_future_{new_id}"
                            self._validate_table_name(kline_table)
                            self._validate_table_name(tick_table)
                            self._data_service.query("""
                                INSERT INTO futures_instruments
                                (internal_id, exchange, instrument_id, product, year_month, format,
                                 expire_date, listing_date, kline_table, tick_table, product_code, shard_key, is_active)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [new_id, exchange, normalized_instrument_id, parsed['product'], parsed['year_month'],
                                  prod['format_template'], expire_date, listing_date, kline_table, tick_table,
                                  parsed['product'].lower(), ShardRouter._deterministic_hash(parsed['product'].lower()), 1], raise_on_error=True)
                            self._create_future_tables_by_id(new_id)
                            info = self._make_instrument_info(
                                new_id, normalized_instrument_id, 'future',
                                product=parsed['product'], year_month=parsed['year_month'],
                                kline_table=kline_table, tick_table=tick_table,
                            )
                            self._assigned_ids.add(new_id)
                            self._cache_to_params_service(normalized_instrument_id, info)
                            logging.info(f"注册期货合约(非活跃品种回退)：{normalized_instrument_id} -> ID={new_id}")
                            DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'future')
                            return new_id
                        except ValueError:
                            continue

            # 尝试注册为期权
            rows = self._data_service.query("SELECT product, exchange, underlying_product, format_template, is_active FROM option_products WHERE is_active=1").to_pylist()
            _op_matched_inactive = False
            for prod in rows:
                product_key = str(prod['product'] or '').strip()
                if normalized_upper.startswith(product_key.upper()):
                    try:
                        parsed = self._parse_option_with_dash(normalized_instrument_id)
                        # 查找标的期货 ID（如果没有，先注册期货）'
                        new_future_id = None
                        rows = self._data_service.query("SELECT internal_id, instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                       [prod['underlying_product'], parsed['year_month']]).to_pylist()
                        row = rows[0] if rows else None
                        if not row:
                            expected_future_id = f"{prod['underlying_product']}{parsed['year_month']}"
                            logging.error(
                                f"[Storage] 标的期货未注册，无法注册期权 {normalized_instrument_id}。"
                                f"请先注册期货合约: {expected_future_id}"
                            )
                            return -1  # 注册失败

                        # 使用数据库中已存在的原始instrument_id（ID直通）'
                        new_future_id = row['internal_id']
                        logging.info("标的期货已存在：ID=%d", new_future_id)
                        new_id = self._get_next_id()
                        kline_table = f"kline_option_{new_id}"
                        tick_table = f"tick_option_{new_id}"
                        self._validate_table_name(kline_table)
                        self._validate_table_name(tick_table)

                        opt_product_code = parsed['product'].lower()
                        opt_shard_key = ShardRouter._deterministic_hash(opt_product_code)
                        self._data_service.query("""
                            INSERT INTO option_instruments
                            (internal_id, exchange, instrument_id, product, underlying_future_id, underlying_product,
                            year_month, option_type, strike_price, format,
                            expire_date, listing_date, kline_table, tick_table, product_code, shard_key, is_active)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [new_id, exchange, normalized_instrument_id, parsed['product'], new_future_id, prod['product'], parsed['year_month'],
                              parsed['option_type'], parsed['strike_price'], prod['format_template'],
                              expire_date, listing_date, kline_table, tick_table,
                              opt_product_code, opt_shard_key, 1], raise_on_error=True)
                        self._create_option_tables_by_id(new_id)
                        # 更新缓存
                        info = self._make_instrument_info(
                            new_id,
                            normalized_instrument_id,
                            'option',
                            product=parsed['product'],
                            year_month=parsed['year_month'],
                            option_type=parsed['option_type'],
                            strike_price=parsed['strike_price'],
                            underlying_future_id=new_future_id,
                            kline_table=kline_table,
                            tick_table=tick_table,
                        )
                        self._assigned_ids.add(new_id)  # v1吸收：跟踪已分配ID
                        self._cache_to_params_service(normalized_instrument_id, info)  # v1吸收：双向缓存同步
                        logging.info(f"注册期权合约：{normalized_instrument_id} -> ID={new_id}")

                        # DiagnosisProbeManager already imported at module level
                        DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'option')

                        return new_id
                    except ValueError:
                        # 期权解析失败，尝试注册为期权标的期货（如 ho2605 -> IH2605）
                        try:
                            # 方法唯一修复：统一使用例parse_future，不再内联正则
                            future_parsed = self._parse_future(normalized_instrument_id)
                            underlying_prod = str(prod.get('underlying_product') or '').strip()
                            if underlying_prod and future_parsed.get('year_month'):
                                rows = self._data_service.query(
                                    "SELECT instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                    [underlying_prod, future_parsed['year_month']]
                                ).to_pylist()
                                if rows:
                                    future_instrument_id = rows[0]['instrument_id']  # 使用数据库中的原始值
                                    # 检查是否已注册
                                    existing = self._get_instrument_info(future_instrument_id)
                                    if existing:
                                            self._cache_alias_instrument_mapping(normalized_instrument_id, existing)
                                            canonical_internal_id = self._get_info_internal_id(existing)
                                            logging.info("复用期权标的期货主记录：%s -> ID=%d (主记录=%s)", normalized_instrument_id, canonical_internal_id, future_instrument_id)
                                            # DiagnosisProbeManager already imported at module level
                                            DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', canonical_internal_id, 'future')
                                            return canonical_internal_id
                        except Exception as _e:
                            logging.info("[Storage] 期权标的期货回退注册失败: %s", _e)  # R26-P2-DF-05修复: debug→info
                        continue

            # FIX-P0-14: 期权活跃品种定义过滤回退 - is_active=0 的期权品种也尝试注册
            if not _op_matched_inactive:
                _inactive_opt_rows = self._data_service.query("SELECT product, exchange, underlying_product, format_template FROM option_products WHERE is_active=0").to_pylist()
                for prod in _inactive_opt_rows:
                    product_key = str(prod['product'] or '').strip()
                    if normalized_upper.startswith(product_key.upper()):
                        _op_matched_inactive = True
                        logging.warning(
                            "[FIX-P0-14] 期权品种 %s 被标记为 is_active=0(非活跃), 仍尝试注册合约 %s",
                            product_key, normalized_instrument_id,
                        )
                        try:
                            parsed = self._parse_option_with_dash(normalized_instrument_id)
                            _rows = self._data_service.query("SELECT internal_id, instrument_id FROM futures_instruments WHERE product=? AND year_month=?",
                                       [prod['underlying_product'], parsed['year_month']]).to_pylist()
                            _row = _rows[0] if _rows else None
                            if not _row:
                                logging.error(
                                    "[FIX-P0-14] 标的期货未注册，无法注册期权(非活跃品种回退) %s",
                                    normalized_instrument_id,
                                )
                                return -1
                            new_future_id = _row['internal_id']
                            new_id = self._get_next_id()
                            kline_table = f"kline_option_{new_id}"
                            tick_table = f"tick_option_{new_id}"
                            self._validate_table_name(kline_table)
                            self._validate_table_name(tick_table)
                            opt_product_code = parsed['product'].lower()
                            opt_shard_key = ShardRouter._deterministic_hash(opt_product_code)
                            self._data_service.query("""
                                INSERT INTO option_instruments
                                (internal_id, exchange, instrument_id, product, underlying_future_id, underlying_product,
                                year_month, option_type, strike_price, format,
                                expire_date, listing_date, kline_table, tick_table, product_code, shard_key, is_active)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [new_id, exchange, normalized_instrument_id, parsed['product'], new_future_id, prod['product'], parsed['year_month'],
                                  parsed['option_type'], parsed['strike_price'], prod['format_template'],
                                  expire_date, listing_date, kline_table, tick_table,
                                  opt_product_code, opt_shard_key, 1], raise_on_error=True)
                            self._create_option_tables_by_id(new_id)
                            info = self._make_instrument_info(
                                new_id, normalized_instrument_id, 'option',
                                product=parsed['product'], year_month=parsed['year_month'],
                                option_type=parsed['option_type'], strike_price=parsed['strike_price'],
                                underlying_future_id=new_future_id, kline_table=kline_table, tick_table=tick_table,
                            )
                            self._assigned_ids.add(new_id)
                            self._cache_to_params_service(normalized_instrument_id, info)
                            logging.info(f"注册期权合约(非活跃品种回退)：{normalized_instrument_id} -> ID={new_id}")
                            DiagnosisProbeManager.on_register(normalized_instrument_id, 'storage_register', new_id, 'option')
                            return new_id
                        except ValueError:
                            continue

            raise ValueError(f"无法识别合约品种：{normalized_instrument_id}")


    @requires_phase(InitPhase.DB_SCHEMA)
    def delete_instrument(self, instrument_id: str) -> None:
        info = self._get_instrument_info(instrument_id)
        if not info:
            logging.warning(f"合约不存在，无法删除：{instrument_id}")
            return

        internal_id = self._get_info_internal_id(info)

        if info['type'] == 'future':
            rows = self._data_service.query(
                "SELECT instrument_id FROM option_instruments WHERE underlying_future_id=?",
                [internal_id],
            ).to_pylist()
            dependent_options = [row['instrument_id'] for row in rows]
            for option_instrument_id in dependent_options:
                self.delete_instrument(option_instrument_id)

        kline_table = info.get('kline_table')
        tick_table = info.get('tick_table')
        if kline_table:
            self._validate_table_name(kline_table)
            self._data_service.query(f"DROP TABLE IF EXISTS {kline_table}", raise_on_error=True)
        if tick_table:
            self._validate_table_name(tick_table)
            self._data_service.query(f"DROP TABLE IF EXISTS {tick_table}", raise_on_error=True)
        delete_internal_id = int(internal_id)
        if info['type'] == 'future':
            self._data_service.query("DELETE FROM futures_instruments WHERE instrument_id=?", [instrument_id], raise_on_error=True)
        else:
            self._data_service.query("DELETE FROM option_instruments WHERE instrument_id=?", [instrument_id], raise_on_error=True)
        logging.info(f"已删除合约 {instrument_id} (ID={internal_id})")

    def batch_add_future_instruments(self, instruments: List[Dict]) -> None:
        try:
            for inst in instruments:
                exchange = inst.get('exchange', 'AUTO')
                instrument_id = inst.get('instrument_id')
                if not instrument_id:
                    continue
                try:
                    self.register_instrument(instrument_id, exchange,
                                           expire_date=inst.get('expire_date'),
                                           listing_date=inst.get('listing_date'))
                except Exception as e:
                    logging.warning("批量导入期货失败 %s: %s", instrument_id, e)
            logging.info("批量导入 %d 个期货合约", len(instruments))
        except Exception as e:
            logging.error("批量导入期货失败：%s", e)
            raise

    def batch_add_option_instruments(self, instruments: List[Dict]) -> None:
        try:
            for inst in instruments:
                exchange = inst.get('exchange', 'AUTO')
                instrument_id = inst.get('instrument_id')
                if not instrument_id:
                    continue
                try:
                    self.register_instrument(instrument_id, exchange,
                                           expire_date=inst.get('expire_date'),
                                           listing_date=inst.get('listing_date'))
                except Exception as e:
                    logging.warning("批量导入期权失败 %s: %s", instrument_id, e)
            logging.info("批量导入 %d 个期权合约", len(instruments))
        except Exception as e:
            logging.error("批量导入期权失败：%s", e)
            raise


_StorageQueryInstrumentMixin = StorageInstrumentService
