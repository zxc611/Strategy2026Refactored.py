# MODULE_ID: M1-130
"""
product_initializer.py — 品种初始化模块

从 config_service.py 提取，解决 SRP 违反问题。
ensure_products_with_retry 处理品种加载和DB建表，不属于配置服务职责。

职责:
- 解析期货/期权配置文件
- 预验证期权->期货映射关系
- 写入DB并验证ID完整性
- 刷新ParamsService缓存

调用方: strategy_lifecycle_mixin.py (on_init)
"""

import logging
import os
import time
from typing import Any, Dict, Optional, Tuple


_PRODUCT_PARAMS: Dict[str, Dict[str, Any]] = {
    'IO': {'tick_size': 0.2, 'contract_size': 100, 'exchange': 'CFFEX', 'type': 'option'},
    'HO': {'tick_size': 0.2, 'contract_size': 100, 'exchange': 'CFFEX', 'type': 'option'},
    'MO': {'tick_size': 0.2, 'contract_size': 100, 'exchange': 'CFFEX', 'type': 'option'},

    'IF': {'tick_size': 0.2, 'contract_size': 200.0, 'exchange': 'CFFEX', 'type': 'future', 'price_limit_pct': 0.10, 'margin_ratio': 0.12},
    'IC': {'tick_size': 0.2, 'contract_size': 200.0, 'exchange': 'CFFEX', 'type': 'future', 'price_limit_pct': 0.10, 'margin_ratio': 0.12},
    'IH': {'tick_size': 0.2, 'contract_size': 300.0, 'exchange': 'CFFEX', 'type': 'future', 'price_limit_pct': 0.10, 'margin_ratio': 0.12},
    'IM': {'tick_size': 0.2, 'contract_size': 200.0, 'exchange': 'CFFEX', 'type': 'future', 'price_limit_pct': 0.10, 'margin_ratio': 0.12},
    'TS': {'tick_size': 0.005, 'contract_size': 10000.0, 'exchange': 'CFFEX', 'type': 'future'},
    'TF': {'tick_size': 0.005, 'contract_size': 10000.0, 'exchange': 'CFFEX', 'type': 'future'},
    'T':  {'tick_size': 0.005, 'contract_size': 10000.0, 'exchange': 'CFFEX', 'type': 'future'},
    'CU': {'tick_size': 10.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.06},
    'AL': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.06},
    'ZN': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.06},
    'AU': {'tick_size': 0.02, 'contract_size': 1000, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.07},
    'AG': {'tick_size': 1.0, 'contract_size': 15, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.07},
    'RB': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.07},
    'RU': {'tick_size': 5.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'option', 'price_limit_pct': 0.07},
    'MA': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'TA': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'OI': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'RM': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'SA': {'tick_size': 1.0, 'contract_size': 20, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'FG': {'tick_size': 1.0, 'contract_size': 20, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'SR': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'CF': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'option', 'price_limit_pct': 0.05},
    'AP': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'option'},
    'CJ': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'option'},
    'SF': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'option'},
    'SM': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'option'},
    'UR': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'option'},
    'M':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'option', 'price_limit_pct': 0.07},
    'Y':  {'tick_size': 2.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'option', 'price_limit_pct': 0.07},
    'P':  {'tick_size': 2.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'option', 'price_limit_pct': 0.07},
    'A':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'option', 'price_limit_pct': 0.07},
    'L':  {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'option'},
    'V':  {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'option'},
    'PP': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'option'},
    'EB': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'option'},
    'I':  {'tick_size': 0.5, 'contract_size': 100, 'exchange': 'DCE', 'type': 'option', 'price_limit_pct': 0.07},
    'EG': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'option'},
    'C':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'option'},
    'CS': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'option'},
    # EX-P2-01: 补充SHFE/DCE/CZCE/INE主要品种tick_size配置（期货类型）'
    # EX-P2-02: 同时添加price_limit_pct涨跌停幅度
    'CU_F': {'tick_size': 10.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.10},
    'AL_F': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.10},
    'ZN_F': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.10},
    'PB_F': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.10},
    'NI_F': {'tick_size': 10.0, 'contract_size': 1, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.12},
    'SN_F': {'tick_size': 10.0, 'contract_size': 1, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.12},
    'SS_F': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.10},
    'AU_F': {'tick_size': 0.02, 'contract_size': 1000, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.08},
    'AG_F': {'tick_size': 1.0, 'contract_size': 15, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.08},
    'RB_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'RU_F': {'tick_size': 5.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'HC_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'WR_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'BU_F': {'tick_size': 2.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'SP_F': {'tick_size': 2.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'FU_F': {'tick_size': 5.0, 'contract_size': 30, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'BC_F': {'tick_size': 10.0, 'contract_size': 5, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.06, 'margin_ratio': 0.10},
    'NR_F': {'tick_size': 5.0, 'contract_size': 10, 'exchange': 'SHFE', 'type': 'future', 'price_limit_pct': 0.07, 'margin_ratio': 0.10},
    'I_F':  {'tick_size': 0.5, 'contract_size': 100, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'J_F':  {'tick_size': 0.5, 'contract_size': 100, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'JM_F': {'tick_size': 0.5, 'contract_size': 60, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'M_F':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'Y_F':  {'tick_size': 2.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'A_F':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'P_F':  {'tick_size': 2.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'C_F':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'CS_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'L_F':  {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'V_F':  {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'PP_F': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'EG_F': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'EB_F': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'B_F':  {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'FB_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'RR_F': {'tick_size': 1.0, 'contract_size': 20, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'JD_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'LH_F': {'tick_size': 1.0, 'contract_size': 16, 'exchange': 'DCE', 'type': 'future', 'price_limit_pct': 0.07},
    'CF_F': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'SR_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'TA_F': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'MA_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'OI_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'FG_F': {'tick_size': 1.0, 'contract_size': 20, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'SA_F': {'tick_size': 1.0, 'contract_size': 20, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'RM_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'AP_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'CJ_F': {'tick_size': 5.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'PF_F': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'SF_F': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'SM_F': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'UR_F': {'tick_size': 1.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'SH_F': {'tick_size': 1.0, 'contract_size': 30, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'PX_F': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    'PK_F': {'tick_size': 2.0, 'contract_size': 5, 'exchange': 'CZCE', 'type': 'future', 'price_limit_pct': 0.05},
    # EX-P2-01: 补充INE能源交易中心品种
    'SC_F': {'tick_size': 0.1, 'contract_size': 1000, 'exchange': 'INE', 'type': 'future', 'price_limit_pct': 0.05},
    'NR_F': {'tick_size': 5.0, 'contract_size': 10, 'exchange': 'INE', 'type': 'future', 'price_limit_pct': 0.07},
    'LU_F': {'tick_size': 1.0, 'contract_size': 10, 'exchange': 'INE', 'type': 'future', 'price_limit_pct': 0.07},
    'BC_F': {'tick_size': 10.0, 'contract_size': 5, 'exchange': 'INE', 'type': 'future', 'price_limit_pct': 0.06},
    'EC_F': {'tick_size': 1.0, 'contract_size': 30, 'exchange': 'INE', 'type': 'future', 'price_limit_pct': 0.05},
}

_DEFAULT_FUTURE_SPEC = {'tick_size': 0.2, 'contract_size': 1.0, 'price_limit_pct': 0.10, 'margin_ratio': 0.12}  # EX-P2-06: 添加margin_ratio
_DEFAULT_OPTION_SPEC = {'tick_size': 1.0, 'contract_size': 1.0, 'price_limit_pct': 0.10, 'margin_ratio': 0.10}  # EX-P2-06: 添加margin_ratio


def get_product_params(product: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = product.upper().rstrip('0123456789')
    while p and p not in _PRODUCT_PARAMS and len(p) > 1:
        p = p[:-1]
    base = dict(_PRODUCT_PARAMS.get(p, _DEFAULT_FUTURE_SPEC))
    if overrides:
        base.update(overrides)
    return base


def _get_option_underlying_product(option_product: str) -> str:
    """期权品种 -> 标的期货品种映射"""
    from ali2026v3_trading.config.config_service import ExchangeConfig
    config = ExchangeConfig()
    entry = config.option_products.get(option_product)
    return entry[0] if entry else option_product


def _reset_config_instrument_catalog(data_service) -> None:
    """按配置全集重建合约元数据，避免历史upsert遗留冲突和配置外污染。"""
    conn = data_service.get_connection()
    try:
        conn.execute("BEGIN")
        conn.execute("""CREATE TABLE IF NOT EXISTS instruments_registry (
            instrument_id VARCHAR PRIMARY KEY, product VARCHAR, exchange VARCHAR,
            year_month VARCHAR, internal_id INTEGER, option_type VARCHAR,
            strike_price DOUBLE, underlying_future_id INTEGER,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS future_products (
            product VARCHAR PRIMARY KEY, exchange VARCHAR, format_template VARCHAR,
            tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS option_products (
            product VARCHAR PRIMARY KEY, exchange VARCHAR, underlying_product VARCHAR,
            format_template VARCHAR, tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS futures_instruments (
            internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE,
            product VARCHAR, exchange VARCHAR, year_month VARCHAR, is_active BOOLEAN)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS option_instruments (
            internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE,
            product VARCHAR, exchange VARCHAR, underlying_future_id INTEGER,
            underlying_product VARCHAR, year_month VARCHAR, option_type VARCHAR,
            strike_price DOUBLE, is_active BOOLEAN)""")
        for table_name in (
            'instruments_registry',
            'option_instruments',
            'futures_instruments',
            'option_products',
            'future_products',
        ):
            conn.execute(f"DELETE FROM {table_name}")
        conn.execute("DROP SEQUENCE IF EXISTS instrument_id_seq")
        conn.execute("CREATE SEQUENCE instrument_id_seq START 1")
        conn.execute("COMMIT")
        logging.info("[ensure_products] 已按配置全集清空元数据并重建instrument_id_seq")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        data_service._return_connection(conn)


def _rebuild_instruments_registry(data_service) -> Dict[str, int]:
    conn = data_service.get_connection()
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM instruments_registry")
        conn.execute("""
            INSERT INTO instruments_registry (
                instrument_id, product, exchange, year_month, internal_id,
                option_type, strike_price, underlying_future_id, registered_at
            )
            SELECT instrument_id, product, exchange, year_month, CAST(internal_id AS INTEGER),
                   NULL, NULL, NULL, CURRENT_TIMESTAMP
            FROM futures_instruments
        """)
        conn.execute("""
            INSERT INTO instruments_registry (
                instrument_id, product, exchange, year_month, internal_id,
                option_type, strike_price, underlying_future_id, registered_at
            )
            SELECT instrument_id, product, exchange, year_month, CAST(internal_id AS INTEGER),
                   option_type, strike_price, underlying_future_id, CURRENT_TIMESTAMP
            FROM option_instruments
        """)
        conn.execute("COMMIT")
        row = conn.execute("""
            SELECT COUNT(*) total,
                   SUM(CASE WHEN internal_id IS NULL THEN 1 ELSE 0 END) null_internal,
                   SUM(CASE WHEN option_type IS NOT NULL AND underlying_future_id IS NULL THEN 1 ELSE 0 END) null_underlying
            FROM instruments_registry
        """).fetchone()
        return {
            'registry_total': int(row[0] or 0),
            'registry_null_internal': int(row[1] or 0),
            'registry_null_underlying': int(row[2] or 0),
        }
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        data_service._return_connection(conn)


def _insert_rows_via_arrow(conn, view_name: str, table_name: str, columns, rows) -> None:
    if not rows:
        return
    import pyarrow as pa

    records = [dict(zip(columns, row)) for row in rows]
    table = pa.Table.from_pylist(records)
    conn.register(view_name, table)
    try:
        conn.execute(
            f"INSERT INTO {table_name} ({', '.join(columns)}) "
            f"SELECT {', '.join(columns)} FROM {view_name}"
        )
    finally:
        try:
            conn.unregister(view_name)
        except Exception:
            pass


def _rebuild_config_instruments(data_service, futures_parsed, options_parsed, future_products_set, option_products_set) -> Tuple[int, int]:
    """一次事务批量重建配置合约，保证期货/期权internal_id全局唯一。"""
    from ali2026v3_trading.infra.shared_utils import ShardRouter

    future_product_rows = []
    for product in sorted(future_products_set):
        exchange = next((f[2] for f in futures_parsed if f[1] == product), 'CFFEX')
        params = get_product_params(product)
        future_product_rows.append((
            product, exchange, '{product}{year_month}',
            params.get('tick_size', 0.2), params.get('contract_size', 1.0), True,
        ))

    option_product_rows = []
    for product, exchange, underlying_product in sorted(option_products_set):
        option_product_rows.append((
            product, exchange, underlying_product,
            '{product}{year_month}-{option_type}-{strike_price}', 0.2, 1.0, True,
        ))

    future_rows = []
    future_id_map = {}
    next_internal_id = 1
    for instrument_id, product, exchange, year_month in futures_parsed:
        product_code = product.lower() if product else None
        shard_key = ShardRouter._deterministic_hash(product_code) if product_code else None
        future_rows.append((
            next_internal_id, instrument_id, product, exchange, year_month,
            '{product}{year_month}', None, None,
            'klines_raw', 'ticks_raw', product_code, shard_key, True,
        ))
        future_id_map[(product, year_month)] = next_internal_id
        next_internal_id += 1

    option_rows = []
    _missing_underlying_count = 0
    for instrument_id, product, exchange, year_month, option_type, strike_price, underlying_future_key in options_parsed:
        # FIX-P0-16: underlying_future_key 直接字典访问导致 KeyError 中断所有期权注册
        underlying_future_id = future_id_map.get(underlying_future_key)
        if underlying_future_id is None:
            _missing_underlying_count += 1
            if _missing_underlying_count <= 10 or _missing_underlying_count % 100 == 0:
                logging.error(
                    "[FIX-P0-16] 期权 %s 的标的期货 %s 未在 future_id_map 中, 跳过该期权(累计%d个)",
                    instrument_id, underlying_future_key, _missing_underlying_count,
                )
            continue
        underlying_product = _get_option_underlying_product(product)
        product_code = product.lower() if product else None
        shard_key = ShardRouter._deterministic_hash(product_code) if product_code else None
        option_rows.append((
            next_internal_id, instrument_id, product, exchange,
            underlying_future_id, underlying_product, year_month, option_type, strike_price,
            '{product}{year_month}-{option_type}-{strike_price}', None, None,
            'klines_raw', 'ticks_raw', product_code, shard_key, True,
        ))
        next_internal_id += 1

    conn = data_service.get_connection()
    try:
        conn.execute("BEGIN")
        conn.execute("""CREATE TABLE IF NOT EXISTS instruments_registry (
            instrument_id VARCHAR PRIMARY KEY, product VARCHAR, exchange VARCHAR,
            year_month VARCHAR, internal_id INTEGER, option_type VARCHAR,
            strike_price DOUBLE, underlying_future_id INTEGER,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS future_products (
            product VARCHAR PRIMARY KEY, exchange VARCHAR, format_template VARCHAR,
            tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS option_products (
            product VARCHAR PRIMARY KEY, exchange VARCHAR, underlying_product VARCHAR,
            format_template VARCHAR, tick_size DOUBLE, contract_size DOUBLE, is_active BOOLEAN)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS futures_instruments (
            internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE,
            product VARCHAR, exchange VARCHAR, year_month VARCHAR, is_active BOOLEAN)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS option_instruments (
            internal_id BIGINT PRIMARY KEY, instrument_id VARCHAR UNIQUE,
            product VARCHAR, exchange VARCHAR, underlying_future_id INTEGER,
            underlying_product VARCHAR, year_month VARCHAR, option_type VARCHAR,
            strike_price DOUBLE, is_active BOOLEAN)""")
        for table_name in (
            'instruments_registry',
            'option_instruments',
            'futures_instruments',
            'option_products',
            'future_products',
        ):
            conn.execute(f"DELETE FROM {table_name}")
        conn.execute("DROP SEQUENCE IF EXISTS instrument_id_seq")
        conn.execute(f"CREATE SEQUENCE instrument_id_seq START {next_internal_id}")

        _insert_rows_via_arrow(
            conn, '_future_product_rows', 'future_products',
            ['product', 'exchange', 'format_template', 'tick_size', 'contract_size', 'is_active'],
            future_product_rows,
        )
        _insert_rows_via_arrow(
            conn, '_option_product_rows', 'option_products',
            ['product', 'exchange', 'underlying_product', 'format_template', 'tick_size', 'contract_size', 'is_active'],
            option_product_rows,
        )
        _insert_rows_via_arrow(
            conn, '_future_instrument_rows', 'futures_instruments',
            ['internal_id', 'instrument_id', 'product', 'exchange', 'year_month',
             'format', 'expire_date', 'listing_date', 'kline_table', 'tick_table', 'product_code', 'shard_key', 'is_active'],
            future_rows,
        )
        _insert_rows_via_arrow(
            conn, '_option_instrument_rows', 'option_instruments',
            ['internal_id', 'instrument_id', 'product', 'exchange',
             'underlying_future_id', 'underlying_product', 'year_month', 'option_type', 'strike_price',
             'format', 'expire_date', 'listing_date', 'kline_table', 'tick_table', 'product_code', 'shard_key', 'is_active'],
            option_rows,
        )
        conn.execute("COMMIT")
        logging.info(
            "[ensure_products] 配置全集批量重建完成: 期货=%d, 期权=%d, next_internal_id=%d",
            len(future_rows), len(option_rows), next_internal_id,
        )
        return len(future_rows), len(option_rows)
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        data_service._return_connection(conn)


def ensure_products_with_retry(data_service, max_retries: int = 5) -> Dict[str, int]:
    """
    三机制根因修复 + 期货建表时同时赋映射ID

    流程：
    1. 阶段1（纯内存）：解析配置文件，明示式映射，预验证所有期权->期货关系
    2. 阶段2（写DB）：期货 upsert 分配 ID -> 同时建立映射 -> 期权 upsert 使用已验证的 underlying_future_id
    3. 阶段3（硬断言）：验证 0 个 NULL，否则初始化失败

    机制1 - 内存预验证：写DB前验证每个期权的 (underlying_product, year_month) 在期货集合中存在
    机制2 - 硬断言替代warning：underlying_future_id 为 NULL 则初始化失败
    机制3 - 删除回填SQL：不再需要 underlying_product + year_month 的字符串匹配回填
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # ali2026v3_trading 根目录
    config_dir = os.path.join(base_dir, 'config')
    futures_file = os.path.join(config_dir, 'subscription_futures_fixed.txt')
    options_file = os.path.join(config_dir, 'subscription_options_fixed.txt')
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            errors = []

            # ========== 阶段1：纯内存解析 + 预验证（不碰DB） ==========

            # 1A. 解析期货配置文件 -> 内存列表
            futures_parsed = []
            future_products_set = set()
            if os.path.exists(futures_file):
                with open(futures_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if '|' in line:
                            first_field = line.split('|')[0]
                            parts = first_field.split('.')
                            instrument_id = parts[-1] if len(parts) > 1 else first_field
                            exchange = parts[0] if len(parts) > 1 else 'CFFEX'
                        else:
                            parts = line.split('.')
                            instrument_id = parts[-1] if len(parts) > 1 else line
                            exchange = parts[0] if len(parts) > 1 else 'CFFEX'
                        try:
                            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
                            parsed = SubscriptionManager.parse_future(instrument_id)
                            product = parsed.get('product', '')
                            year_month = parsed.get('year_month', '')
                            if not product or not year_month:
                                errors.append(f"期货合约格式错误: {instrument_id}")
                                continue
                        except (ValueError, KeyError, TypeError):
                            errors.append(f"期货合约格式错误: {instrument_id}")
                            continue
                        futures_parsed.append((instrument_id, product, exchange, year_month))
                        future_products_set.add(product)
            else:
                raise RuntimeError(f"期货配置文件不存在: {futures_file}")

            # 1B. 解析期权配置文件 -> 内存列表 + 明示式映射
            options_parsed = []
            option_products_set = set()
            if os.path.exists(options_file):
                with open(options_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if '|' in line:
                            first_field = line.split('|')[0]
                            parts = first_field.split('.')
                            instrument_id = parts[-1] if len(parts) > 1 else first_field
                            exchange = parts[0] if len(parts) > 1 else 'CFFEX'
                        else:
                            parts = line.split('.')
                            instrument_id = parts[-1] if len(parts) > 1 else line
                            exchange = parts[0] if len(parts) > 1 else 'CFFEX'
                        try:
                            from ali2026v3_trading.infra.subscription_service import SubscriptionManager
                            parsed_opt = SubscriptionManager.parse_option(instrument_id)
                            product = parsed_opt['product']
                            year_month = parsed_opt['year_month']
                            option_type = 'C' if parsed_opt['option_type'].upper() in ('C', 'CALL') else 'P'
                            strike_price = float(parsed_opt['strike_price'])
                        except (ValueError, KeyError, TypeError) as _parse_err:
                            errors.append(f"期权合约格式错误: {instrument_id} ({_parse_err})")
                            continue
                        underlying_product = _get_option_underlying_product(product)
                        underlying_future_key = (underlying_product, year_month)
                        options_parsed.append((instrument_id, product, exchange, year_month,
                                              option_type, strike_price, underlying_future_key))
                        option_products_set.add((product, exchange, underlying_product))
            else:
                raise RuntimeError(f"期权配置文件不存在: {options_file}")

            # 1B-defensive: 验证config_exchange中声明的期权品种在TXT中都有合约
            try:
                from ali2026v3_trading.config.config_exchange import ExchangeConfig
                _ec = ExchangeConfig()
                _declared_option_products = set(_ec.option_products.keys())
                _loaded_option_products = {p[0] for p in option_products_set}
                _missing_in_txt = _declared_option_products - _loaded_option_products
                if _missing_in_txt:
                    logging.warning(
                        "[ensure_products] 配置声明但TXT缺失的期权品种: %s — 这些品种将无法订阅tick数据",
                        sorted(_missing_in_txt))
            except Exception as _e:
                logging.debug("[ensure_products] 期权品种完整性检查跳过: %s", _e)

            if errors:
                logging.error("[ensure_products] 配置解析错误: %s", errors[:10])

            # 1C. 预验证：每个期权的 (underlying_product, year_month) 必须在期货集合中存在
            future_ids = {(f[1], f[3]) for f in futures_parsed}
            missing_futures = set()
            for opt in options_parsed:
                underlying_future_key = opt[6]
                if underlying_future_key not in future_ids:
                    missing_futures.add(underlying_future_key)
            if missing_futures:
                affected = sum(1 for o in options_parsed if o[6] in missing_futures)
                raise RuntimeError(
                    f"ID完整性违约：{len(missing_futures)} 个标的期货不在配置中"
                    f"（{affected} 个期权受影响），缺失：{sorted(missing_futures)}"
                )

            # ========== 阶段2：写DB（所有关系已在内存中验证完毕） ==========

            futures_loaded, options_loaded = _rebuild_config_instruments(
                data_service, futures_parsed, options_parsed, future_products_set, option_products_set
            )

            # ========== 阶段3：硬断言核查（0 个 NULL，否则初始化失败） ==========

            conn = data_service.get_connection()
            try:
                null_future_count = conn.execute(
                    "SELECT COUNT(*) FROM option_instruments WHERE underlying_future_id IS NULL"
                ).fetchone()[0]
                if null_future_count > 0:
                    raise RuntimeError(
                        f"ID完整性违约：{null_future_count} 个期权合约的 underlying_future_id 为 NULL，初始化终止"
                    )

                invalid_future_count = conn.execute("""
                    SELECT COUNT(*)
                    FROM option_instruments oi
                    LEFT JOIN futures_instruments fi ON fi.internal_id = oi.underlying_future_id
                    WHERE oi.underlying_future_id IS NOT NULL
                      AND fi.internal_id IS NULL
                """).fetchone()[0]
                if invalid_future_count > 0:
                    raise RuntimeError(
                        f"ID完整性违约：{invalid_future_count} 个期权合约的 underlying_future_id 指向无效期货，初始化终止"
                    )

                duplicate_internal_id_count = conn.execute("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT internal_id FROM (
                            SELECT internal_id FROM futures_instruments
                            UNION ALL
                            SELECT internal_id FROM option_instruments
                        ) all_ids
                        GROUP BY internal_id HAVING COUNT(*) > 1
                    ) dup
                """).fetchone()[0]
                if duplicate_internal_id_count > 0:
                    raise RuntimeError(
                        f"ID完整性违约：{duplicate_internal_id_count} 组internal_id重复，初始化终止"
                    )
            finally:
                data_service._return_connection(conn)

            registry_result = _rebuild_instruments_registry(data_service)
            if registry_result['registry_total'] != futures_loaded + options_loaded:
                raise RuntimeError(
                    f"registry重建数量错误：{registry_result['registry_total']} != {futures_loaded + options_loaded}"
                )
            if registry_result['registry_null_internal'] or registry_result['registry_null_underlying']:
                raise RuntimeError(f"registry完整性违约：{registry_result}")

            coverage_result = data_service.ensure_config_coverage_for_today()
            expected_total = futures_loaded + options_loaded
            if coverage_result['ticks_raw_count'] != expected_total or coverage_result['klines_raw_count'] != expected_total:
                raise RuntimeError(
                    f"覆盖落库不完整：expected={expected_total}, coverage={coverage_result}"
                )

            # CHECKPOINT 确保持久化
            checkpoint_conn = None
            try:
                checkpoint_conn = data_service.get_connection()
                checkpoint_conn.execute("CHECKPOINT")
                logging.info("[ensure_products] 合约数据已持久化（CHECKPOINT）")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as checkpoint_err:
                logging.warning(f"[ensure_products] CHECKPOINT失败: {checkpoint_err}")
            finally:
                if checkpoint_conn is not None:
                    data_service._return_connection(checkpoint_conn)

            # 刷新 ParamsService 缓存
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                get_params_service().clear_instrument_cache()
                logging.info("[ensure_products] ParamsService 合约缓存已刷新（原子替换）")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as cache_err:
                logging.warning(f"[ensure_products] 清空 ParamsService 缓存失败: {cache_err}")

            # 验证：至少加载了一些合约
            if futures_loaded == 0 and options_loaded == 0:
                raise RuntimeError("合约表加载后仍为空")

            # P1-15修复：先加载ParamsService缓存，再标记品种配置已加载完成
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                ps = get_params_service()
                ps.load_caches_from_db(data_service)
                all_cache = ps.get_all_instrument_cache()
                futures_cached = sum(1 for k in all_cache if '-' not in k)
                options_cached = sum(1 for k in all_cache if '-' in k)
                logging.info(
                    f"[ensure_products] ParamsService 缓存已加载: "
                    f"期货={futures_cached}, 期权={options_cached}"
                )
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as load_err:
                logging.error(f"[ensure_products] ParamsService 缓存加载失败: {load_err}")
                raise RuntimeError(f"ParamsService 缓存加载失败，策略无法继续初始化: {load_err}")

            # 标记品种配置已加载完成（必须在load_caches_from_db之后）
            try:
                data_service.mark_products_loaded()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as mark_err:
                logging.warning(f"[ensure_products] 标记品种加载状态失败: {mark_err}")

            logging.info(
                f"[ensure_products] 合约初始化完成(第{attempt}次): "
                f"期货={futures_loaded}, 期权={options_loaded}, "
                f"underlying_future_id为NULL=0, invalid=0"
            )
            return {
                'future_added': futures_loaded,
                'option_added': options_loaded,
                'future_existing': 0,
                'option_existing': 0,
                'null_future_count': 0,
                'invalid_future_count': 0
            }

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            last_error = e
            logging.warning("[ensure_products] 第%d次尝试失败: %s", attempt, e)
            if attempt < max_retries:
                time.sleep(0.5 * attempt)  # R23-P2-15标记: P2级阻塞重试

    raise RuntimeError(f"[ensure_products] 合约初始化失败，已重试{max_retries}次，最后错误: {last_error}。策略无法继续初始化。")
