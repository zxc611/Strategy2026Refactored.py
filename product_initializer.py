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
from typing import Dict


def _get_option_underlying_product(option_product: str) -> str:
    """期权品种 -> 标的期货品种映射"""
    from ali2026v3_trading.config_service import ExchangeConfig
    config = ExchangeConfig()
    entry = config.option_products.get(option_product)
    return entry[0] if entry else option_product


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
    base_dir = os.path.dirname(os.path.abspath(__file__))
    futures_file = os.path.join(base_dir, 'subscription_futures_fixed.txt')
    options_file = os.path.join(base_dir, 'subscription_options_fixed.txt')
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
                            from ali2026v3_trading.subscription_manager import SubscriptionManager
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
                            from ali2026v3_trading.subscription_manager import SubscriptionManager
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

            futures_loaded = 0
            options_loaded = 0

            # 2A. upsert 期货品种 + 合约，建立 instrument_id -> internal_id 映射
            future_id_map = {}
            for product in sorted(future_products_set):
                exchange = next((f[2] for f in futures_parsed if f[1] == product), 'CFFEX')
                data_service.upsert_future_product(
                    product=product, exchange=exchange,
                    format_template='{product}{year_month}',
                    tick_size=0.2, contract_size=1.0, is_active=True
                )

            for instrument_id, product, exchange, year_month in futures_parsed:
                internal_id = data_service.upsert_future_instrument(
                    instrument_id=instrument_id, product=product,
                    exchange=exchange, year_month=year_month, is_active=True
                )
                future_id_map[(product, year_month)] = internal_id
                futures_loaded += 1

            # 2B. upsert 期权品种 + 合约，underlying_future_id 从已验证的映射中获取
            for product_key in sorted(option_products_set):
                product, exchange, underlying_product = product_key
                fmt = '{product}{year_month}-{option_type}-{strike_price}'
                data_service.upsert_option_product(
                    product=product, exchange=exchange,
                    underlying_product=underlying_product,
                    format_template=fmt, tick_size=0.2,
                    contract_size=1.0, is_active=True
                )

            for instrument_id, product, exchange, year_month, option_type, strike_price, underlying_future_key in options_parsed:
                underlying_future_id = future_id_map[underlying_future_key]
                underlying_product = _get_option_underlying_product(product)
                data_service.upsert_option_instrument(
                    instrument_id=instrument_id, product=product,
                    exchange=exchange, underlying_future_id=underlying_future_id,
                    underlying_product=underlying_product, year_month=year_month,
                    option_type=option_type, strike_price=strike_price, is_active=True
                )
                options_loaded += 1

            # ========== 阶段3：硬断言核查（0 个 NULL，否则初始化失败） ==========

            conn = data_service._get_connection()

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

            # CHECKPOINT 确保持久化
            try:
                conn.execute("CHECKPOINT")
                logging.info("[ensure_products] 合约数据已持久化（CHECKPOINT）")
            except Exception as checkpoint_err:
                logging.warning(f"[ensure_products] CHECKPOINT失败: {checkpoint_err}")

            # 刷新 ParamsService 缓存
            try:
                from ali2026v3_trading.params_service import get_params_service
                get_params_service().clear_instrument_cache()
                logging.info("[ensure_products] ParamsService 合约缓存已刷新（原子替换）")
            except Exception as cache_err:
                logging.warning(f"[ensure_products] 清空 ParamsService 缓存失败: {cache_err}")

            # 验证：至少加载了一些合约
            if futures_loaded == 0 and options_loaded == 0:
                raise RuntimeError("合约表加载后仍为空")

            # 标记品种配置已加载完成，允许后续访问 params_service
            try:
                data_service.mark_products_loaded()
            except Exception as mark_err:
                logging.warning(f"[ensure_products] 标记品种加载状态失败: {mark_err}")

            # 显式加载 ParamsService 缓存，确保 validate_contracts_loaded 能通过
            try:
                from ali2026v3_trading.params_service import get_params_service
                ps = get_params_service()
                ps.load_caches_from_db(data_service)
                all_cache = ps.get_all_instrument_cache()
                futures_cached = len([k for k in all_cache if '-' not in k])
                options_cached = len([k for k in all_cache if '-' in k])
                logging.info(
                    f"[ensure_products] ParamsService 缓存已加载: "
                    f"期货={futures_cached}, 期权={options_cached}"
                )
            except Exception as load_err:
                logging.error(f"[ensure_products] ParamsService 缓存加载失败: {load_err}")
                raise RuntimeError(f"ParamsService 缓存加载失败，策略无法继续初始化: {load_err}")

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

        except Exception as e:
            last_error = e
            logging.warning("[ensure_products] 第%d次尝试失败: %s", attempt, e)
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    raise RuntimeError(f"[ensure_products] 合约初始化失败，已重试{max_retries}次，最后错误: {last_error}。策略无法继续初始化。")
