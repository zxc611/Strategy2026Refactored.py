"""
配置服务模块 - CQRS 架构 Configuration 层
来源：分散的配置文件（quant_trading_system_T型图/config.py + 06_params.py 配置类）
功能：统一配置管理、路径配置、交易参数、输出配置
行数：~300 行

优化 v1.0 (2026-03-16):
- ✅ 集中化配置管理
- ✅ 类型安全和验证
- ✅ 环境变量支持
- ✅ 热重载支持
- ✅ 多环境配置
"""
from __future__ import annotations

import os
import sys
import json
import copy
import re
import logging
import time
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta


# ============================================================================
# 自定义日志 Handler - 确保实时写入磁盘
# ============================================================================

class FlushHandler(logging.Handler):
    """自定义 Handler，确保每次日志写入后立即 flush 到磁盘"""
    def emit(self, record):
        try:
            # 获取所有 FileHandler 并 flush
            for handler in logging.getLogger().handlers:
                if isinstance(handler, RotatingFileHandler) and hasattr(handler, 'stream'):
                    handler.stream.flush()
                    os.fsync(handler.stream.fileno())  # 强制同步到磁盘
        except Exception:
            pass  # flush 失败不影响日志记录


# ============================================================================
# 路径配置
# ============================================================================

@dataclass
class PathConfig:
    """路径配置"""
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent)
    db_path: str = ""
    state_file: str = ""
    log_dir: str = ""
    config_dir: str = ""
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.db_path:
            self.db_path = str(self.project_root / "trading_data.db")
        if not self.state_file:
            self.state_file = str(self.project_root / "runtime_state.json")
        if not self.log_dir:
            self.log_dir = str(self.project_root / "auto_logs")
        if not self.config_dir:
            self.config_dir = str(self.project_root / ".arts")
    
    def to_dict(self) -> Dict[str, str]:
        """转换为字典"""
        return {
            "project_root": str(self.project_root),
            "db_path": self.db_path,
            "state_file": self.state_file,
            "log_dir": self.log_dir,
            "config_dir": self.config_dir
        }


# ============================================================================
# 交易所和合约配置
# ============================================================================

@dataclass
class ExchangeConfig:
    """交易所配置"""
    exchanges: List[str] = field(default_factory=lambda: ["CFFEX", "SHFE", "DCE", "CZCE"])
    simulated_instruments: Dict[str, List[str]] = field(default_factory=lambda: {
        "CFFEX": ["IF2406", "IO2406C4000"],
        "SHFE": ["rb2410"],
        "DCE": ["m2409"]
    })
    depth_levels: int = 5
    
    # ========== 新增：自动生成配置 ==========
    # 是否启用自动生成逻辑
    enable_auto_generation: bool = True
    
    # 是否启用期货品种
    enable_futures: bool = True
    
    # 是否启用期权品种
    enable_options: bool = True
    
    # 期权品种配置 (格式：{期权品种代码：(标的期货，交易所)})
    option_products: Dict[str, tuple] = field(default_factory=lambda: {
        # ========== 中金所 (CFFEX) - 股指期权 ==========
        "IO": ("IF", "CFFEX"),   # 沪深 300 股指期权
        "HO": ("IH", "CFFEX"),   # 上证 50 股指期权
        "MO": ("IM", "CFFEX"),   # 中证 1000 股指期权
        "EO": ("IM", "CFFEX"),   # 中证 1000 股指期权
        
        # ========== 上期所 (SHFE) - 商品期权 ==========
        "CU": ("CU", "SHFE"),   # 铜期权
        "AL": ("AL", "SHFE"),   # 铝期权
        "ZN": ("ZN", "SHFE"),   # 锌期权
        "AU": ("AU", "SHFE"),   # 黄金期权
        "AG": ("AG", "SHFE"),   # 白银期权
        "RB": ("RB", "SHFE"),   # 螺纹钢期权
        "RU": ("RU", "SHFE"),   # 橡胶期权
        
        # ========== 郑商所 (CZCE) - 商品期权 ==========
        "MA": ("MA", "CZCE"),   # 甲醇期权
        "TA": ("TA", "CZCE"),   # PTA 期权
        "OI": ("OI", "CZCE"),   # 菜籽油期权
        "RM": ("RM", "CZCE"),   # 菜籽粕期权
        "SA": ("SA", "CZCE"),   # 纯碱期权
        "FG": ("FG", "CZCE"),   # 玻璃期权
        "SR": ("SR", "CZCE"),   # 白糖期权
        "CF": ("CF", "CZCE"),   # 棉花期权
        "AP": ("AP", "CZCE"),   # 苹果期权
        "CJ": ("CJ", "CZCE"),   # 红枣期权
        "SF": ("SF", "CZCE"),   # 硅铁期权
        "SM": ("SM", "CZCE"),   # 锰硅期权
        "UR": ("UR", "CZCE"),   # 尿素期权
        
        # ========== 大商所 (DCE) - 商品期权 ==========
        "M": ("M", "DCE"),     # 豆粕期权
        "Y": ("Y", "DCE"),     # 豆油期权
        "P": ("P", "DCE"),     # 棕榈油期权
        "A": ("A", "DCE"),     # 黄大豆 1 号期权
        "L": ("L", "DCE"),     # 聚乙烯期权
        "V": ("V", "DCE"),     # 聚氯乙烯期权
        "PP": ("PP", "DCE"),   # 聚丙烯期权
        "EB": ("EB", "DCE"),   # 苯乙烯期权
        "I": ("I", "DCE"),     # 铁矿石期权
        "EG": ("EG", "DCE"),   # 乙二醇期权
        "C": ("C", "DCE"),     # 玉米期权
        "CS": ("CS", "DCE"),   # 玉米淀粉期权
        
        # ========== 能源中心 (INE) - 商品期权 ==========
        "SC": ("SC", "INE"),   # 原油期权
        
        # ========== 广期所 (GFEX) - 商品期权 ==========
        "SI": ("SI", "GFEX"),   # 工业硅期权
        "LC": ("LC", "GFEX"),   # 碳酸锂期权
    })
    
    # 品种到交易所的统一映射（单一数据源，覆盖期货和期权）
    product_exchanges: Dict[str, str] = field(default_factory=lambda: {
        # 中金所期货/期权
        "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX",
        "IO": "CFFEX", "HO": "CFFEX", "MO": "CFFEX", "EO": "CFFEX",
        # 上期所期货/期权
        "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE", "RB": "SHFE", "AU": "SHFE", "AG": "SHFE",
        "NI": "SHFE", "SN": "SHFE", "PB": "SHFE", "SS": "SHFE", "WR": "SHFE",
        "RU": "SHFE", "NR": "SHFE", "HC": "SHFE", "BU": "SHFE", "SP": "SHFE", "FU": "SHFE", "BR": "SHFE",
        # 能源中心
        "BC": "INE", "LU": "INE", "SC": "INE", "EC": "INE",
        # 大商所
        "M": "DCE", "Y": "DCE", "A": "DCE", "JM": "DCE", "I": "DCE",
        "C": "DCE", "CS": "DCE", "JD": "DCE", "L": "DCE", "V": "DCE", "PP": "DCE",
        "EG": "DCE", "PG": "DCE", "J": "DCE", "P": "DCE", "EB": "DCE", "B": "DCE", "RR": "DCE", "LH": "DCE",
        # 郑商所
        "CF": "CZCE", "SR": "CZCE", "MA": "CZCE", "TA": "CZCE", "RM": "CZCE", "OI": "CZCE",
        "SA": "CZCE", "PF": "CZCE", "FG": "CZCE", "SF": "CZCE", "SM": "CZCE", "AP": "CZCE", "CJ": "CZCE", "UR": "CZCE", "PX": "CZCE", "SH": "CZCE", "PR": "CZCE", "PK": "CZCE",
        # 广期所
        "LC": "GFEX", "SI": "GFEX",
    })
    
    # 期货品种开关 (格式：{品种代码：是否启用})
    futures_switches: Dict[str, bool] = field(default_factory=lambda: {
        # ========== 中金所 (CFFEX) ==========
        "IF": True,   # 沪深 300 股指期货
        "IH": True,   # 上证 50 股指期货
        "IC": True,   # 中证 500 股指期货
        "IM": True,   # 中证 1000 股指期货
        "T": True,    # 10 年期国债期货
        "TF": True,   # 5 年期国债期货
        "TS": True,   # 2 年期国债期货
        "TL": True,   # 30 年期国债期货
        
        # ========== 上期所 (SHFE) ==========
        "CU": True,   # 铜
        "AL": True,   # 铝
        "ZN": True,   # 锌
        "PB": True,   # 铅
        "NI": True,   # 镍
        "SN": True,   # 锡
        "AU": True,   # 黄金
        "AG": True,   # 白银
        "RB": True,   # 螺纹钢
        "HC": True,   # 热轧卷板
        "SS": True,   # 不锈钢
        "RU": True,   # 橡胶
        "BU": True,   # 沥青
        "SP": True,   # 纸浆
        "FU": True,   # 燃油
        "LU": True,   # 低硫燃料油
        "WR": False,  # 线材 (不活跃)
        
        # ========== 郑商所 (CZCE) ==========
        "MA": True,   # 甲醇
        "TA": True,   # PTA
        "PF": True,   # 短纤
        "UR": True,   # 尿素
        "OI": True,   # 菜籽油
        "RM": True,   # 菜籽粕
        "CF": True,   # 棉花
        "SR": True,   # 白糖
        "AP": True,   # 苹果
        "CJ": True,   # 红枣
        "SA": True,   # 纯碱
        "FG": True,   # 玻璃
        "SF": True,   # 硅铁
        "SM": True,   # 锰硅
        "RI": False,  # 早籼稻 (不活跃)
        "LR": False,  # 晚籼稻 (不活跃)
        "JR": False,  # 粳稻 (不活跃)
        "WH": False,  # 强麦 (不活跃)
        "PM": False,  # 普麦 (不活跃)
        
        # ========== 大商所 (DCE) ==========
        "M": True,    # 豆粕
        "Y": True,    # 豆油
        "P": True,    # 棕榈油
        "A": True,    # 黄大豆 1 号
        "B": True,    # 黄大豆 2 号
        "L": True,    # 聚乙烯 (LLDPE)
        "V": True,    # 聚氯乙烯 (PVC)
        "PP": True,   # 聚丙烯
        "EB": True,   # 苯乙烯
        "EG": True,   # 乙二醇
        "PG": True,   # 液化石油气 (LPG)
        "I": True,    # 铁矿石
        "J": True,    # 焦炭
        "JM": True,   # 焦煤
        "C": True,    # 玉米
        "CS": True,   # 玉米淀粉
        "JD": True,   # 鸡蛋
        "BB": False,  # 纤维板 (不活跃)
        "RR": False,  # 粳米 (不活跃)
        
        # ========== 能源中心 (INE) ==========
        "SC": True,   # 原油
        "BC": True,   # 国际铜
        
        # ========== 广期所 (GFEX) ==========
        "SI": True,   # 工业硅
        "LC": True,   # 碳酸锂
    })
    
    def get_instruments_for_exchange(self, exchange: str) -> List[str]:
        """获取指定交易所的模拟合约"""
        return self.simulated_instruments.get(exchange, [])
    
    def get_all_simulated_instruments(self) -> List[str]:
        """获取所有模拟合约"""
        instruments = []
        for inst_list in self.simulated_instruments.values():
            instruments.extend(inst_list)
        return instruments
    
    # ========== 新增：期货/期权开关管理方法 ==========
    
    def set_future_switch(self, product_code: str, enabled: bool):
        """
        设置单个期货品种的开关状态
        
        Args:
            product_code: 品种代码 (如 'IF', 'M')
            enabled: True=启用，False=关闭
        """
        self.futures_switches[product_code] = enabled
        logging.info(f"[Config] 期货品种开关：{product_code} = {'启用' if enabled else '关闭'}")
    
    def get_enabled_futures(self) -> List[str]:
        """
        获取所有启用的期货品种列表
        
        Returns:
            List[str]: 启用的品种代码列表
        """
        return [code for code, enabled in self.futures_switches.items() if enabled]
    
    def get_disabled_futures(self) -> List[str]:
        """
        获取所有关闭的期货品种列表
        
        Returns:
            List[str]: 关闭的品种代码列表
        """
        return [code for code, enabled in self.futures_switches.items() if not enabled]
    
    def is_future_enabled(self, product_code: str) -> bool:
        """
        判断某个期货品种是否启用
        
        Args:
            product_code: 品种代码
        
        Returns:
            bool: 是否启用
        """
        if not self.enable_futures:
            return False
        return self.futures_switches.get(product_code, False)
    
    def should_generate_option(self, option_product: str, underlying: str) -> bool:
        """
        判断某个期权品种是否应该生成 (基于标的期货的开关状态)
        
        Args:
            option_product: 期权品种代码
            underlying: 标的期货品种代码
        
        Returns:
            bool: 是否应该生成
        """
        # 全局期权开关
        if not self.enable_options:
            return False
        
        # 检查标的期货是否启用
        if not self.is_future_enabled(underlying):
            return False
        
        return True
    
    def get_enabled_options(self) -> List[str]:
        """
        获取所有启用的期权品种列表
        
        Returns:
            List[str]: 启用的期权品种代码列表
        """
        enabled = []
        for opt_code, (underlying, exchange) in self.option_products.items():
            if self.is_future_enabled(underlying):
                enabled.append(opt_code)
        return enabled
    
    def get_disabled_options(self) -> List[str]:
        """
        获取所有关闭的期权品种列表
        
        Returns:
            List[str]: 关闭的期权品种代码列表
        """
        disabled = []
        for opt_code, (underlying, exchange) in self.option_products.items():
            if not self.is_future_enabled(underlying):
                disabled.append(opt_code)
        return disabled
    
    def get_generation_config(self) -> Dict[str, Any]:
        """
        获取完整的生成配置信息
        
        Returns:
            Dict: 配置字典
        """
        return {
            'enable_auto_generation': self.enable_auto_generation,
            'enable_futures': self.enable_futures,
            'enable_options': self.enable_options,
            'enabled_futures': self.get_enabled_futures(),
            'disabled_futures': self.get_disabled_futures(),
            'futures_count': len(self.get_enabled_futures()),
            'enabled_options': self.get_enabled_options(),
            'disabled_options': self.get_disabled_options(),
            'options_count': len(self.get_enabled_options()),
        }


# ============================================================================
# 品种表初始化：从实盘配置文件增量加载品种（5次重试，失败硬报错）
# ============================================================================
def _get_option_underlying_product(option_product: str) -> str:
    """从ExchangeConfig获取期权对应的标的期货品种（单一数据源）"""
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
    import re as _re, logging, time
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
                        parts = line.split('.')
                        instrument_id = parts[-1] if len(parts) > 1 else line
                        exchange = parts[0] if len(parts) > 1 else 'CFFEX'
                        # ✅ 方法唯一修复：统一使用SubscriptionManager.parse_option，不再内联正则
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
                        # ✅ ID直通：underlying_product通过映射表获取，key为(product,year_month)元组供后续DB查询
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
                # underlying_future_id 必定存在（1C 已验证）
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
                logging.info("[ensure_products] ParamsService 合约缓存已清空，后续将从最新配置重载")
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

            # ✅ 关键修复：显式加载 ParamsService 缓存，确保 validate_contracts_loaded 能通过
            try:
                from ali2026v3_trading.params_service import get_params_service
                ps = get_params_service()
                ps.load_caches_from_db(data_service)
                # ✅ 传递渠道唯一：通过公共API获取缓存，不直接访问私有属性
                all_cache = ps.get_all_instrument_cache()
                futures_cached = len([k for k in all_cache if '-' not in k])
                options_cached = len([k for k in all_cache if '-' in k])
                logging.info(
                    f"[ensure_products] ✅ ParamsService 缓存已加载: "
                    f"期货={futures_cached}, 期权={options_cached}"
                )
            except Exception as load_err:
                logging.error(f"[ensure_products] ❌ ParamsService 缓存加载失败: {load_err}")
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


# ============================================================================
# 交易参数配置
# ============================================================================

@dataclass
class TradingConfig:
    """交易参数配置"""
    # 订单相关
    default_volume: int = 1
    max_position_limit: int = 1000
    order_timeout_seconds: float = 30.0
    
    # 风控相关
    max_daily_loss: float = 10000.0
    max_single_order_volume: int = 100
    enable_auto_stop_loss: bool = True
    stop_loss_threshold: float = 5000.0
    
    # 行情相关
    tick_update_interval_ms: int = 500
    kline_check_interval_ms: int = 1000
    market_data_cache_ttl_seconds: int = 60
    
    # 策略相关
    strategy_heartbeat_interval_seconds: float = 30.0
    strategy_max_heartbeat_failures: int = 3
    enable_incremental_diagnosis: bool = True


# ============================================================================
# 输出配置
# ============================================================================

class OutputConfig:
    """输出配置类
    
    负责管理和缓存输出相关的参数配置，避免重复访问 self.params
    """
    
    def __init__(self, params: Optional[Any] = None):
        """初始化输出配置
        
        Args:
            params: 参数对象（可选）
        """
        # 输出模式：open_debug|close_debug|trade|none
        self.mode = str(getattr(params, "output_mode", "debug")).lower()
        if self.mode == "debug":
            self.mode = "close_debug"
        
        # 交易模式静默开关
        self.trade_quiet = bool(getattr(params, "trade_quiet", True))
        
        # 诊断输出开关
        self.diagnostic_enabled = bool(getattr(params, "diagnostic_output", True))
        
        # 调试输出开关
        self.debug_enabled = bool(getattr(params, "debug_output", False))
        
        # 综合调试开关
        self.combined_debug_enabled = (
            self.diagnostic_enabled and 
            (self.debug_enabled or self.mode == "close_debug")
        )
        
        # 是否为交易类模式
        self.is_trade_like = self.mode in ("trade", "open_debug")
    
    def should_output(
        self,
        is_trade_msg: bool = False,
        is_force_msg: bool = False,
        is_trade_table: bool = False,
        is_diag_msg: bool = False
    ) -> bool:
        """判断是否应该输出消息（增加空值保护）
            
        Args:
            is_trade_msg: 是否交易消息
            is_force_msg: 是否强制消息
            is_trade_table: 是否交易表格
            is_diag_msg: 是否诊断消息
            
        Returns:
            bool: 是否应该输出
        """
        if not isinstance(is_trade_msg, bool) or not isinstance(is_force_msg, bool):
            return True # 默认允许输出以防逻辑错误
                
        # 交易模式且要求静默，且非交易表格，则拦截
        if self.is_trade_like and self.trade_quiet and not is_trade_table:
            return False
        
        # 诊断消息且诊断关闭，且非交易/强制消息，则拦截
        if is_diag_msg and not self.diagnostic_enabled and not is_trade_msg and not is_force_msg:
            return False
        
        # 交易模式下，仅输出交易或强制信息
        if self.is_trade_like and not is_trade_msg and not is_force_msg:
            return False
        
        # 非交易模式，若调试开关关闭且非强制，跳过输出
        if not is_trade_msg and not is_force_msg and not self.combined_debug_enabled:
            return False
        
        return True


# ============================================================================
# 日志配置
# ============================================================================

@dataclass
class LoggingConfig:
    """日志配置"""
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_date_format: str = "%Y-%m-%d %H:%M:%S"
    log_to_console: bool = True
    log_to_file: bool = True
    log_file_prefix: str = "strategy"
    log_rotation_days: int = 7
    log_backup_count: int = 10
    enable_async_logging: bool = True
    log_queue_size: int = 1000


# ============================================================================
# 数据库配置
# ============================================================================

@dataclass
class DatabaseConfig:
    """数据库配置"""
    db_type: str = "sqlite"
    db_path: str = ""
    connection_pool_size: int = 5
    connection_timeout: float = 30.0
    enable_connection_pooling: bool = True
    auto_commit: bool = False
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.db_path:
            path_config = PathConfig()
            self.db_path = path_config.db_path


# ============================================================================
# 性能监控配置
# ============================================================================

@dataclass
class PerformanceConfig:
    """性能监控配置"""
    enable_performance_monitoring: bool = True
    metrics_collection_interval_seconds: int = 60
    enable_memory_profiling: bool = False
    enable_cpu_profiling: bool = False
    performance_report_interval_minutes: int = 15
    alert_on_high_cpu_usage: bool = True
    high_cpu_threshold_percent: float = 80.0
    alert_on_high_memory_usage: bool = True
    high_memory_threshold_mb: int = 500


# ============================================================================
# 主配置类
# ============================================================================

class ConfigService:
    """配置服务 - 统一配置管理中心
    
    职责:
    - 集中管理所有配置
    - 支持从文件加载配置
    - 支持环境变量覆盖
    - 支持配置热重载
    - 提供类型安全的配置访问
    
    设计原则:
    - Singleton 模式
    - 配置与代码分离
    - 支持多环境
    - 类型安全
    """
    
    _instance: Optional['ConfigService'] = None
    _config_data: Dict[str, Any] = {}
    _init_lock = threading.Lock()  # P1 Bug #54修复：单例初始化锁
    
    def __new__(cls) -> 'ConfigService':
        """P1 Bug #54修复：标准双重检查锁定单例模式"""
        if cls._instance is None:
            with cls._init_lock:
                # 第二次检查，防止多线程同时通过第一次检查
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化配置服务"""
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = True
        self._config_file_path: Optional[Path] = None
        self._last_modified_time: float = 0.0
        
        # 初始化各子配置
        self.paths = PathConfig()
        self.exchanges = ExchangeConfig()
        self.trading = TradingConfig()
        self.logging = LoggingConfig()
        self.database = DatabaseConfig()
        self.performance = PerformanceConfig()
        
        # 尝试加载配置文件
        self._load_config_file()
        
        # 应用环境变量覆盖
        self._apply_environment_overrides()
    
    def _load_config_file(self, config_path: Optional[str] = None) -> None:
        """加载配置文件
        
        Args:
            config_path: 配置文件路径（可选）
        """
        if config_path:
            path = Path(config_path)
        else:
            path = Path(self.paths.config_dir) / "config.json"
        
        if not path.exists():
            return
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
            
            self._config_file_path = path
            self._last_modified_time = path.stat().st_mtime
            
            # 更新配置
            self._update_from_dict(file_config)
            
        except Exception as e:
            logging.error(f"[ConfigService] Failed to load config file: {e}")
            raise RuntimeError(f"Config load failed: {e}") from e
    
    def _update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """从字典更新配置
        
        Args:
            config_dict: 配置字典
        """
        # 更新路径配置
        if 'paths' in config_dict:
            for key, value in config_dict['paths'].items():
                if hasattr(self.paths, key):
                    setattr(self.paths, key, value)
        
        # 更新交易所配置
        if 'exchanges' in config_dict:
            for key, value in config_dict['exchanges'].items():
                if hasattr(self.exchanges, key):
                    setattr(self.exchanges, key, value)
        
        # 更新交易配置
        if 'trading' in config_dict:
            for key, value in config_dict['trading'].items():
                if hasattr(self.trading, key):
                    setattr(self.trading, key, value)
        
        # 更新日志配置
        if 'logging' in config_dict:
            for key, value in config_dict['logging'].items():
                if hasattr(self.logging, key):
                    setattr(self.logging, key, value)
        
        # 更新数据库配置
        if 'database' in config_dict:
            for key, value in config_dict['database'].items():
                if hasattr(self.database, key):
                    setattr(self.database, key, value)
        
        # 更新性能配置
        if 'performance' in config_dict:
            for key, value in config_dict['performance'].items():
                if hasattr(self.performance, key):
                    setattr(self.performance, key, value)
    
    def _apply_environment_overrides(self) -> None:
        """应用环境变量覆盖"""
        # 数据库路径
        if 'TRADING_DB_PATH' in os.environ:
            self.database.db_path = os.environ['TRADING_DB_PATH']
        
        # 日志级别
        if 'LOG_LEVEL' in os.environ:
            self.logging.log_level = os.environ['LOG_LEVEL']
        
        # 输出模式
        if 'OUTPUT_MODE' in os.environ:
            # 注意：OutputConfig 需要 params 对象，这里不直接设置
            pass
        
        # 性能监控
        if 'ENABLE_PERFORMANCE_MONITORING' in os.environ:
            value = os.environ['ENABLE_PERFORMANCE_MONITORING'].lower()
            self.performance.enable_performance_monitoring = value in ('true', '1', 'yes')
    
    def reload(self) -> bool:
        """重新加载配置
        
        Returns:
            bool: 是否成功重新加载
        """
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            if current_mtime > self._last_modified_time:
                self._load_config_file(str(self._config_file_path))
                return True
        return False
    
    def need_reload(self) -> bool:
        """检查是否需要重新加载
        
        Returns:
            bool: 是否需要重新加载
        """
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            return current_mtime > self._last_modified_time
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """将所有配置转换为字典
        
        Returns:
            Dict: 配置字典
        """
        return {
            'paths': self.paths.to_dict(),
            'exchanges': {
                'exchanges': self.exchanges.exchanges,
                'simulated_instruments': self.exchanges.simulated_instruments,
                'depth_levels': self.exchanges.depth_levels
            },
            'trading': {
                'default_volume': self.trading.default_volume,
                'max_position_limit': self.trading.max_position_limit,
                'order_timeout_seconds': self.trading.order_timeout_seconds,
                'max_daily_loss': self.trading.max_daily_loss,
                'max_single_order_volume': self.trading.max_single_order_volume,
                'enable_auto_stop_loss': self.trading.enable_auto_stop_loss,
                'stop_loss_threshold': self.trading.stop_loss_threshold,
                'tick_update_interval_ms': self.trading.tick_update_interval_ms,
                'kline_check_interval_ms': self.trading.kline_check_interval_ms,
                'market_data_cache_ttl_seconds': self.trading.market_data_cache_ttl_seconds,
                'strategy_heartbeat_interval_seconds': self.trading.strategy_heartbeat_interval_seconds,
                'strategy_max_heartbeat_failures': self.trading.strategy_max_heartbeat_failures,
                'enable_incremental_diagnosis': self.trading.enable_incremental_diagnosis
            },
            'logging': {
                'log_level': self.logging.log_level,
                'log_format': self.logging.log_format,
                'log_date_format': self.logging.log_date_format,
                'log_to_console': self.logging.log_to_console,
                'log_to_file': self.logging.log_to_file,
                'log_file_prefix': self.logging.log_file_prefix,
                'log_rotation_days': self.logging.log_rotation_days,
                'log_backup_count': self.logging.log_backup_count,
                'enable_async_logging': self.logging.enable_async_logging,
                'log_queue_size': self.logging.log_queue_size
            },
            'database': {
                'db_type': self.database.db_type,
                'db_path': self.database.db_path,
                'connection_pool_size': self.database.connection_pool_size,
                'connection_timeout': self.database.connection_timeout,
                'enable_connection_pooling': self.database.enable_connection_pooling,
                'auto_commit': self.database.auto_commit
            },
            'performance': {
                'enable_performance_monitoring': self.performance.enable_performance_monitoring,
                'metrics_collection_interval_seconds': self.performance.metrics_collection_interval_seconds,
                'enable_memory_profiling': self.performance.enable_memory_profiling,
                'enable_cpu_profiling': self.performance.enable_cpu_profiling,
                'performance_report_interval_minutes': self.performance.performance_report_interval_minutes,
                'alert_on_high_cpu_usage': self.performance.alert_on_high_cpu_usage,
                'high_cpu_threshold_percent': self.performance.high_cpu_threshold_percent,
                'alert_on_high_memory_usage': self.performance.alert_on_high_memory_usage,
                'high_memory_threshold_mb': self.performance.high_memory_threshold_mb
            }
        }
    
    def save_to_file(self, config_path: str) -> bool:
        """保存配置到文件
        
        Args:
            config_path: 配置文件路径
        
        Returns:
            bool: 是否成功保存
        """
        try:
            path = Path(config_path)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2)
            
            self._config_file_path = path
            self._last_modified_time = path.stat().st_mtime
            return True
        except Exception as e:
            logging.error(f"[ConfigService] Failed to save config file: {e}")
            raise RuntimeError(f"Config save failed: {e}") from e


# ============================================================================
# P2 功能恢复：Params 相关（从 06_params.py 恢复）
# ============================================================================

def Field(default=None, title="", **kwargs):
    """
    字段定义辅助函数
    
    Args:
        default: 默认值
        title: 字段标题
        **kwargs: 其他参数
        
    Returns:
        默认值
    """
    return default


class DebugConfig:
    """调试配置"""
    
    def __init__(self, params: Any):
        """初始化调试配置"""
        self.output_enabled = bool(getattr(params, "debug_output_enabled", False))
        self.throttle_map = getattr(params, "debug_throttle_map", None)
        if not isinstance(self.throttle_map, dict):
            self.throttle_map = {}
        
        # 参数表调试节流间隔（秒）
        self.param_table_interval = float(self.throttle_map.get("param_table", 60.0))
    
    def should_log_param_table(self, last_timestamp: float, current_time: float) -> bool:
        """
        判断是否应该输出参数表调试日志
        
        Args:
            last_timestamp: 上次日志时间戳
            current_time: 当前时间戳
            
        Returns:
            bool: 是否应该输出日志
        """
        if not self.output_enabled:
            return False
        return (current_time - last_timestamp) >= self.param_table_interval


import json as _json
import time as _time
import threading as _threading

# ============================================================================
# 参数缓存管理（模块级，线程安全 + TTL 自动过期）
# ============================================================================

_param_table_cache = None
_param_table_cache_timestamp = 0.0  # 缓存时间戳（用于 TTL 检查）
_param_table_lock = _threading.Lock()
CACHE_TTL = 300.0  # 5 分钟 TTL（秒）

def get_cached_params() -> dict:
    """获取缓存的参数表（线程安全 + 懒加载 + TTL 自动过期）。
    
    Returns:
        dict: 参数表字典
    """
    global _param_table_cache, _param_table_cache_timestamp
    
    now = time.time()
    
    # 使用锁防止并发加载
    with _param_table_lock:
        # ✅ 检查 TTL 是否过期
        if (_param_table_cache is not None and 
            now - _param_table_cache_timestamp < CACHE_TTL):
            return _param_table_cache
        
        # 懒加载：首次调用或 TTL 过期时从 DEFAULT_PARAM_TABLE 加载
        _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)  # 创建深拷贝，防止共享可变对象
        _param_table_cache_timestamp = now
        logging.info(f"[config_service.get_cached_params] 参数表已{'刷新' if _param_table_cache else '加载'}，包含 {len(_param_table_cache)} 个参数 (TTL={CACHE_TTL}s)")
        return _param_table_cache

def reset_param_cache() -> None:
    """重置参数缓存（供参数修改后重新加载使用，线程安全）。"""
    global _param_table_cache
    with _param_table_lock:
        _param_table_cache = None
        logging.info("[config_service.reset_param_cache] 参数缓存已重置，下次访问时将重新加载")


def update_cached_params(updates: Dict[str, Any], sync_default_table: bool = True) -> dict:
    """更新参数缓存，并可选择同步到默认参数表以跨 TTL 生效。"""
    global _param_table_cache, _param_table_cache_timestamp

    normalized_updates = dict(updates or {})
    if not normalized_updates:
        return get_cached_params()

    with _param_table_lock:
        if _param_table_cache is None:
            _param_table_cache = copy.deepcopy(DEFAULT_PARAM_TABLE)
        for key, value in normalized_updates.items():
            _param_table_cache[key] = value
            if sync_default_table:
                DEFAULT_PARAM_TABLE[key] = copy.deepcopy(value)
        _param_table_cache_timestamp = time.time()

    logging.info(
        "[config_service.update_cached_params] 已更新参数缓存字段：%s",
        ', '.join(sorted(normalized_updates.keys())),
    )
    return _param_table_cache


def build_exchange_mapping(custom_mapping: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """合并默认映射与外部映射，统一返回交易所映射表（数据源：ExchangeConfig.product_exchanges）。"""
    merged = dict(ExchangeConfig().product_exchanges)
    for product, exchange in (custom_mapping or {}).items():
        product_key = str(product or "")
        exchange_value = str(exchange or "")
        if product_key and exchange_value:
            merged[product_key] = exchange_value
    return merged


def resolve_product_exchange(
    product_or_instrument: Optional[str],
    exchange_mapping: Optional[Dict[str, Any]] = None,
    default_exchange: str = "CFFEX",
) -> str:
    """根据品种代码或合约代码解析交易所。
    
    方法唯一修复#14：品种提取委托SubscriptionManager.parse_future/parse_option，
    不再使用内联正则。
    """
    from ali2026v3_trading.subscription_manager import SubscriptionManager
    normalized_mapping = build_exchange_mapping(exchange_mapping)
    token = str(product_or_instrument or "")
    # 委托SubscriptionManager提取品种代码
    product_code = token  # 默认：输入本身就是品种代码
    try:
        if SubscriptionManager.is_option(token):
            parsed = SubscriptionManager.parse_option(token)
            product_code = parsed.get('product', token)
        else:
            parsed = SubscriptionManager.parse_future(token)
            product_code = parsed.get('product', token)
    except (ValueError, KeyError):
        pass  # 输入可能本身就是品种代码，保持原值
    result = None
    for key in normalized_mapping:
        if str(key).upper() == product_code.upper():
            result = normalized_mapping[key]
            break
    return result or str(default_exchange or "CFFEX")


def make_platform_future_id(product: str, year_month: str) -> str:
    """
    根据品种和年月构造期货合约ID。
    
    ⚠️ 仅用于新合约注册场景，不应用于已有合约的ID处理。
    已有合约应从元数据获取原始ID。
    
    Args:
        product: 品种代码，如 'cu', 'IF'
        year_month: 年月，如 '2605', '202605', '6M05'
        
    Returns:
        期货合约ID，如 'cu2605', 'IF2605'
    """
    product = str(product or '').strip()
    year_month = str(year_month or '').strip()
    if not product or not year_month:
        return ''
    
    # 归一化year_month: 支持 '202605' -> '2605', '6M05' -> '2605'
    if len(year_month) == 6 and year_month.isdigit():
        year_month = year_month[2:]  # '202605' -> '2605'
    elif len(year_month) == 4 and 'M' in year_month.upper():
        # '6M05' -> '2605'
        m = re.match(r'(\d)M(\d{2})', year_month, re.IGNORECASE)
        if m:
            year_month = f'{m.group(1)}{m.group(2)}'
    
    return f'{product}{year_month}'


# ============================================================================
# DEFAULT_PARAM_TABLE - 默认参数表（内嵌）
# ============================================================================

def _load_default_products_from_config() -> Dict[str, str]:
    """从配置文件加载默认品种列表（回退路径）"""
    import re as _re
    base_dir = os.path.dirname(os.path.abspath(__file__))
    futures_file = os.path.join(base_dir, 'subscription_futures_fixed.txt')
    options_file = os.path.join(base_dir, 'subscription_options_fixed.txt')
    
    future_products_set = set()
    option_products_set = set()
    
    # 从期货配置文件提取品种
    if os.path.exists(futures_file):
        with open(futures_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('.')
                instrument_id = parts[-1] if len(parts) > 1 else line
                try:
                    from ali2026v3_trading.subscription_manager import SubscriptionManager
                    parsed = SubscriptionManager.parse_future(instrument_id)
                    future_products_set.add(parsed.get('product', ''))
                except (ValueError, KeyError, TypeError):
                    pass
    
    # 从期权配置文件提取品种
    if os.path.exists(options_file):
        with open(options_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('.')
                instrument_id = parts[-1] if len(parts) > 1 else line
                # ✅ 方法唯一修复：统一使用SubscriptionManager.is_option判断+parse_option提取品种
                try:
                    from ali2026v3_trading.subscription_manager import SubscriptionManager
                    if SubscriptionManager.is_option(instrument_id):
                        parsed = SubscriptionManager.parse_option(instrument_id)
                        option_products_set.add(parsed['product'])
                except (ValueError, KeyError):
                    pass
    
    return {
        'future_products': ','.join(sorted(future_products_set)) if future_products_set else 'IF,IH,IC,IM',
        'option_products': ','.join(sorted(option_products_set)) if option_products_set else 'IO,HO,MO'
    }

# 从配置文件加载默认品种（回退路径）
_default_products = _load_default_products_from_config()

DEFAULT_PARAM_TABLE = {
    "max_kline": 200,
    "kline_style": "M1",
    "subscribe_options": True,
    "debug_output": True,
    "diagnostic_output": True,
    "api_key": "pk_test_default_api_key_for_pythongo",
    "infini_api_key": "sk_test_default_api_secret_for_pythongo",
    "access_key": "pk_test_default_api_key_for_pythongo",
    "access_secret": "sk_test_default_api_secret_for_pythongo",
    "run_profile": "full",
    "enable_scheduler": True,
    "use_tick_kline_generator": True,
    "backtest_tick_mode": False,
    "exchange": "CFFEX",
    "future_product": "IF",
    "option_product": "IO",
    "auto_load_history": True,
    "load_history_options": True,
    "load_all_products": True,
    "exchanges": "CFFEX,SHFE,DCE,CZCE,INE,GFEX",
    "future_products": _default_products['future_products'],
    "option_products": _default_products['option_products'],
    "future_instruments": [],
    "option_instruments": {},
    "instrument_cache_source": "",
    "include_future_products_for_options": True,
    "subscription_batch_size": 10,
    "subscription_interval": 1,
    "subscription_fetch_on_subscribe": True,
    "subscription_fetch_count": 5,
    "subscription_fetch_for_options": False,
    "rate_limit_min_interval_sec": 1,
    "rate_limit_per_instrument": 2,
    "rate_limit_global_per_min": 60,
    "rate_limit_window_sec": 120,
    "subscription_backoff_factor": 1.0,
    "subscribe_only_current_next_options": True,
    "subscribe_only_current_next_futures": True,
    "enable_doc_examples": False,
    "pause_unsubscribe_all": True,
    "pause_force_stop_scheduler": True,
    "pause_on_stop": False,
    "history_minutes": 1440,
    "history_load_batch_size": 200,
    "history_load_max_batch_size": 50,
    "history_load_batch_delay_sec": 0.05,
    "history_load_request_delay_sec": 0.03,
    "log_file_path": "strategy_startup.log",
    "test_mode": False,
    "auto_start_after_init": False,
    "subscribe_only_specified_month_options": True,
    "subscribe_only_specified_month_futures": True,
    # ✅ 月份参数加载模式开关 (2026-03-27)
    # load_month_params_in_init: 是否在初始化时加载月份参数
    # - True: 初始化时加载月份参数 (默认值在配置中)
    # - False: 只在查询时动态加载月份参数 (推荐)
    "load_month_params_in_init": False,  # 默认关闭，只在查询时加载
    # 指定月参数：specified_month
    # 指定下月一：next_specified_month_1
    # 指定下月二：next_specified_month_2
    # 指定下月三：next_specified_month_3
    # 指定下月四：next_specified_month_4
    "specified_month": "",
    "next_specified_month_1": "",
    "next_specified_month_2": "",
    "next_specified_month_3": "",
    "next_specified_month_4": "",
    
    # ✅ 期权宽度计算配置 (2026-04-03)
    # option_width_month_count: 计算宽度时查询的月份数量（默认 2，范围 2-5）
    # - 2: 当月 + 下月一（默认）
    # - 3: 当月 + 下月一 + 下月二
    # - 4: 当月 + 下月一 + 下月二 + 下月三
    # - 5: 当月 + 下月一 + 下月二 + 下月三 + 下月四
    "option_width_month_count": 2,
    
    # option_width_min_threshold: 最小宽度阈值（默认 4.0）
    # - 只有宽度强度 > 阈值的标的才会被选为交易目标
    "option_width_min_threshold": 4.0,
    # 项目路径参数 (保留向后兼容)
    "project_root": str(Path(__file__).parent.parent),
    "workspace": str(Path(__file__).parent.parent),
    "month_mapping": {
        "IF": ["IF2602", "IF2603"],
        "IH": ["IH2602", "IH2603"],
        "IM": ["IM2602", "IM2603"],
        "CU": ["CU2603", "CU2604"],
        "AL": ["AL2603", "AL2604"],
        "ZN": ["ZN2603", "ZN2604"],
        "RB": ["RB2603", "RB2604"],
        "AU": ["AU2603", "AU2604"],
        "AG": ["AG2603", "AG2604"],
        "M": ["M2603", "M2605"],
        "Y": ["Y2603", "Y2605"],
        "A": ["A2603", "A2605"],
        "JM": ["JM2604", "JM2605"],
        "I": ["I2603", "I2605"],
        "J": ["J2604", "J2605"],
        "CF": ["CF2603", "CF2605"],
        "SR": ["SR2603", "SR2605"],
        "MA": ["MA2603", "MA2605"],
        "TA": ["TA2603", "TA2605"],
    },
    "signal_cooldown_sec": 0.0,
    "option_buy_lots_min": 1,
    "option_buy_lots_max": 100,
    "option_contract_multiplier": 10000,
    "position_limit_valid_hours_max": 720,
    "position_limit_default_valid_hours": 24,
    "position_limit_max_ratio": 0.2,
    "position_limit_min_amount": 1000,
    "option_order_price_type": "2",
    "option_order_time_condition": "3",
    "option_order_volume_condition": "1",
    "option_order_contingent_condition": "1",
    "option_order_force_close_reason": "0",
    "option_order_hedge_flag": "1",
    "option_order_min_volume": 1,
    "option_order_business_unit": "1",
    "option_order_is_auto_suspend": 0,
    "option_order_user_force_close": 0,
    "option_order_is_swap": 0,
    "close_take_profit_ratio": 1.5,
    "close_overnight_check_time": "14:58",
    "close_daycut_time": "15:58",
    "close_max_hold_days": 3,
    "close_overnight_loss_threshold": -0.5,
    "close_overnight_profit_threshold": 4.0,
    "close_max_chase_attempts": 5,
    "close_chase_interval_seconds": 2,
    "close_chase_task_timeout_seconds": 30,
    "close_delayed_timeout_seconds": 30,
    "close_delayed_max_retries": 3,
    "close_order_price_type": "2",
    "output_mode": "debug",
    "force_debug_on_start": True,
    "ui_window_width": 260,
    "ui_window_height": 240,
    "ui_font_large": 11,
    "ui_font_small": 10,
    "enable_output_mode_ui": True,
    "daily_summary_hour": 15,
    "daily_summary_minute": 1,
    "trade_quiet": True,
    "print_start_snapshots": False,
    "trade_debug_allowlist": "",
    "debug_disable_categories": "",
    "debug_throttle_seconds": 0.0,
    "debug_throttle_map": {},
    "use_param_overrides_in_debug": False,
    "param_override_table": "",
    "param_edit_limit_per_month": 1,
    "backtest_params": {},
    "ignore_otm_filter": False,
    "allow_minimal_signal": False,
    "min_option_width": 1,
    "async_history_load": True,
    "manual_trade_limit_per_half": 1,
    "morning_afternoon_split_hour": 12,
    "account_id": "",
    "kline_max_age_sec": 0,
    "signal_max_age_sec": 180,
    "top3_rows": 3,
    "lots_min": 5,
    "history_load_max_workers": 4,
    "option_sync_tolerance": 0.5,
    "option_sync_allow_flat": True,
    "index_option_prefixes": "",
    "option_group_exchanges": "",
    "czce_year_future_window": 10,
    "czce_year_past_window": 10,
    "exchange_mapping": dict(ExchangeConfig().product_exchanges),
}

# ============================================================================
# 加载外部期权参数配置 (auto_generated_option_params.json)
# ============================================================================

def _normalize_option_params_payload(payload: Any, source_path: str) -> Dict[str, Dict[str, Any]]:
    """兼容新旧 JSON 结构，提取有效的品种参数映射。"""
    if not isinstance(payload, dict):
        logging.warning(
            f"[config_service] 期权参数文件顶层不是对象，已忽略: {source_path} ({type(payload).__name__})"
        )
        return {}

    candidate = payload
    for key in ('option_params_detail', 'option_params', 'products', 'data', 'params'):
        nested = candidate.get(key)
        if isinstance(nested, dict):
            candidate = nested
            break

    normalized: Dict[str, Dict[str, Any]] = {}
    ignored_keys: List[str] = []
    for product, params in candidate.items():
        product_key = str(product)
        if not product_key:
            continue
        if not isinstance(params, dict):
            ignored_keys.append(product_key)
            continue

        normalized_params = dict(params)
        if 'strike_count' not in normalized_params:
            near_count = len(normalized_params.get('near_month_strikes') or [])
            far_count = len(normalized_params.get('far_month_strikes') or [])
            normalized_params['strike_count'] = max(near_count, far_count, 0)
        normalized[product_key] = normalized_params

    if ignored_keys:
        preview = ', '.join(sorted(ignored_keys)[:5])
        suffix = '...' if len(ignored_keys) > 5 else ''
        logging.info(
            f"[config_service] 期权参数文件存在 {len(ignored_keys)} 个旧版/非品种字段，已自动忽略: {preview}{suffix}"
        )

    return normalized

def load_option_params_from_file() -> Dict[str, Any]:
    """
    从 auto_generated_option_params.json 加载期权参数配置
    
    Returns:
        包含期权参数的字典，文件不存在或解析失败时返回空字典
    """
    import json
    import os
    
    # 可能的配置文件路径（按优先级排序）
    possible_paths = [
        # 1. 当前工作目录
        'auto_generated_option_params.json',
        # 2. 项目根目录（父目录）
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'auto_generated_option_params.json'),
        # 3. 绝对路径（如果当前文件在 ali2026v3_trading 中）
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'auto_generated_option_params.json'),
    ]
    
    for file_path in possible_paths:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                data = _normalize_option_params_payload(raw_data, file_path)
                if not data:
                    logging.warning(f"[config_service] 期权参数文件未提取到有效品种配置: {file_path}")
                    continue
                
                logging.info(f"[config_service] 成功加载期权参数配置: {file_path}")
                logging.info(f"[config_service] 包含 {len(data)} 个品种的期权参数")
                
                # 统计总行权价数量
                total_strikes = sum(int((p or {}).get('strike_count', 0) or 0) for p in data.values())
                logging.info(f"[config_service] 总行权价组合数: {total_strikes}")
                
                return data
            except json.JSONDecodeError as e:
                logging.error(f"[config_service] 期权参数文件 JSON 解析失败: {file_path} - {e}")
            except Exception as e:
                logging.error(f"[config_service] 加载期权参数文件失败: {file_path} - {e}")
    
    # 文件不存在或都加载失败
    logging.warning("[config_service] 未找到 auto_generated_option_params.json，使用默认期权参数")
    return {}


def merge_option_params_to_default():
    """
    将外部期权参数合并到 DEFAULT_PARAM_TABLE
    在模块初始化时自动调用
    """
    option_params = load_option_params_from_file()
    
    if option_params:
        # 将期权参数保存到 DEFAULT_PARAM_TABLE
        DEFAULT_PARAM_TABLE["option_params_detail"] = option_params
        
        # 同时更新 option_products 列表（从文件中的品种）
        products_from_file = list(option_params.keys())
        if products_from_file:
            # 合并现有品种和文件中的品种（去重）
            current_products = set(DEFAULT_PARAM_TABLE.get("option_products", "").split(","))
            current_products = {p.strip() for p in current_products if p.strip()}
            new_products = current_products.union(set(products_from_file))
            DEFAULT_PARAM_TABLE["option_products"] = ",".join(sorted(new_products))
            
            logging.info(f"[config_service] 已合并期权品种列表: {len(products_from_file)} 个品种")
            logging.info(f"[config_service] 最终期权品种: {DEFAULT_PARAM_TABLE['option_products']}")


# 模块初始化时自动加载并合并期权参数
try:
    merge_option_params_to_default()
except Exception as e:
    logging.warning(f"[config_service] 合并期权参数时出错: {e}")




# ============================================================================
# 全局配置实例
# ============================================================================

# ✅ 传递渠道唯一：模块级变量通过get_config()工厂函数获取，禁止直接使用config_service
_config_service_instance: Optional['ConfigService'] = None
_config_service_lock = threading.Lock()


# ============================================================================
# 便捷函数
# ============================================================================

def get_config() -> ConfigService:
    """获取全局配置实例（工厂函数，线程安全单例）
    
    Returns:
        ConfigService: 配置服务实例
    """
    global _config_service_instance
    if _config_service_instance is None:
        with _config_service_lock:
            if _config_service_instance is None:
                _config_service_instance = ConfigService()
    return _config_service_instance

# 向后兼容：模块级变量委托给get_config()，首次访问时初始化
class _ConfigServiceProxy:
    """✅ 传递渠道唯一：代理类，确保所有访问走get_config()工厂函数"""
    def __getattr__(self, name):
        return getattr(get_config(), name)
    def __repr__(self):
        return repr(get_config())

config_service = _ConfigServiceProxy()


def reload_config() -> bool:
    """重新加载配置
    
    Returns:
        bool: 是否成功重新加载
    """
    return config_service.reload()


def get_default_db_path() -> str:
    """获取默认数据库路径
    
    Returns:
        str: 数据库路径
    """
    return config_service.database.db_path


def get_project_root() -> Path:
    """获取项目根目录
    
    Returns:
        Path: 项目根目录
    """
    return config_service.paths.project_root


# 导出公共接口
__all__ = [
    'ConfigService',
    'PathConfig',
    'ExchangeConfig',
    'TradingConfig',
    'OutputConfig',
    'LoggingConfig',
    'DatabaseConfig',
    'PerformanceConfig',
    'config_service',
    'get_config',
    'reload_config',
    'get_default_db_path',
    'get_project_root',
    # Unified configuration functions (新增)
    'setup_logging',
    'setup_paths',
    'get_paths',
    'ServiceContainer',  # Service container for dependency management
]


# ============================================================================
# 统一配置函数 - Bootstrap 优化集成
# ============================================================================

# Logging initialization flag
# Use module-level variable AND check root logger handlers to prevent duplicate init
_LOG_INITIALIZED = False

# Path configuration cache
_PATHS_CONFIGURED = False
_CACHED_PATHS = None


def setup_logging():
    """Configure logging system, ensuring it's initialized only once.
    
    This function:
    1. Sets up root logger with INFO level
    2. Adds PlatformLogHandler for platform console output
    3. Adds RotatingFileHandler for file logging
    4. Prevents duplicate initialization using both flag and handler inspection
    
    Returns:
        bool: True if logging was configured, False if already initialized
    """
    global _LOG_INITIALIZED
    
    # Double-check: flag AND actual handler presence
    if _LOG_INITIALIZED:
        return False
    
    # Also check if root logger already has our handlers (defensive programming)
    root_logger = logging.getLogger()
    has_rotating_handler = any(
        isinstance(h, RotatingFileHandler)
        for h in root_logger.handlers
    )
    if has_rotating_handler:
        # Already initialized by previous call
        _LOG_INITIALIZED = True  # Sync flag with reality
        return False
    
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Standard log format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add PlatformLogHandler (for InfiniTrader platform console)
        has_platform_handler = any(
            getattr(h, '_is_platform_handler', False)
            for h in root_logger.handlers
        )
        
        if not has_platform_handler:
            try:
                # Import write_log directly to avoid circular dependency with t_type_bootstrap
                from pythongo.infini import write_log
                
                # Create a simple handler that calls write_log
                class SimplePlatformHandler(logging.Handler):
                    def emit(self, record):
                        msg = self.format(record)
                        try:
                            write_log(msg)
                        except Exception:
                            pass  # Ignore errors in logging
                
                platform_handler = SimplePlatformHandler()
                platform_handler._is_platform_handler = True
                platform_handler.setLevel(logging.INFO)
                platform_handler.setFormatter(formatter)
                root_logger.addHandler(platform_handler)
            except ImportError:
                # write_log not available, skip
                pass
        
        # Add FileHandler (for strategy.log file)
        has_file_handler = any(
            isinstance(h, RotatingFileHandler)
            for h in root_logger.handlers
        )
        
        if not has_file_handler:
            # Use absolute path for logging
            current_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(current_dir, 'logs')
            log_file = os.path.join(log_dir, 'strategy.log')
            
            # Ensure directory exists
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,
                encoding='utf-8',
                delay=False  # 立即打开文件，而不是等到第一次写入
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
            # 添加 FlushHandler 确保实时写入
            flush_handler = FlushHandler()
            flush_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(flush_handler)
        
        # Note: Do NOT reset LogConfig._configured as it may cause conflicts
        # The platform's LogConfig should manage its own configuration
        # We only configure the root logger with our handlers
        
        _LOG_INITIALIZED = True
        logging.info('[config_service] Logging system initialized: platform + file')
        return True
        
    except Exception as e:
        # Fallback: at least configure basic logging
        try:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            _LOG_INITIALIZED = True
            logging.warning('[config_service] Using fallback logging configuration')
            return True
        except Exception:
            return False


def setup_paths():
    """Add ali2026v3_trading directory to sys.path once.
    
    This function:
    1. Adds ali2026v3_trading directory
    2. Ensures paths are added only once
    
    Returns:
        dict: {
            'ali2026_dir': str,      # ali2026v3_trading directory
        }
    """
    global _PATHS_CONFIGURED, _CACHED_PATHS
    
    if _PATHS_CONFIGURED:
        return _CACHED_PATHS
    
    # Compute path
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)  # ali2026v3_trading directory
    
    paths_to_add = [
        current_dir,       # ali2026v3_trading directory
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    _CACHED_PATHS = {
        'ali2026_dir': current_dir,
    }
    
    _PATHS_CONFIGURED = True
    
    logging.info(f'[config_service] Paths configured: {_CACHED_PATHS}')
    
    return _CACHED_PATHS


def get_paths():
    """Get cached path configuration.
    
    Returns:
        dict: Path configuration dictionary
    """
    if not _PATHS_CONFIGURED:
        return setup_paths()
    return _CACHED_PATHS


# Auto-initialize when this module is imported
# Note: Disabled to prevent duplicate initialization
# Call setup_logging() and setup_paths() explicitly where needed
# setup_logging()  # Called by t_type_bootstrap.py
# setup_paths()    # Called by t_type_bootstrap.py


# ============================================================================
# ServiceContainer - 服务容器（从 t_type_bootstrap.py 迁移至此）
# ============================================================================

class ServiceContainer:
    """服务容器 - 完整的依赖管理"""
    def __init__(self):
        self._services = {}  # 服务实例
        self._dependencies = {  # 服务依赖关系
            'event_bus': [],
            'config': [],
            'params': [],
            'storage': ['event_bus'],
            'market_data': ['storage', 'event_bus'],
            'analytics': ['market_data'],
            'risk': ['market_data'],
            'order': ['risk'],
            'signal': ['analytics'],
            't_type': ['signal'],
            'diagnosis': ['t_type'],
            'ui': ['diagnosis'],
            'strategy_core': ['storage', 'event_bus', 'config', 'params']
        }
        self._initialization_order = [
            'event_bus', 'config', 'params', 'storage',
            'market_data', 'analytics', 'risk', 'order',
            'signal', 't_type', 'diagnosis', 'ui', 'strategy_core'
        ]
    
    def register(self, name: str, service: Any) -> None:
        """注册服务"""
        self._services[name] = service
        logging.info(f"[服务容器] 注册服务：{name}")
    
    def get(self, name: str) -> Any:
        """获取服务"""
        return self._services.get(name)
    
    def initialize_all(self) -> bool:
        """按依赖顺序初始化所有服务"""
        logging.info("[服务容器] 开始初始化...")
        logging.info(f"[服务容器] 服务初始化顺序：{self._initialization_order}")
        
        # 检查循环依赖
        if self._check_circular_dependencies():
            logging.error("[服务容器] 检测到循环依赖!")
            return False
        
        # 按顺序初始化服务
        for service_name in self._initialization_order:
            if service_name in self._services:
                service = self._services[service_name]
                # 检查依赖是否满足
                deps = self._dependencies.get(service_name, [])
                missing_deps = [d for d in deps if d not in self._services]
                if missing_deps:
                    logging.error(f"[服务容器] 服务 {service_name} 依赖于 {missing_deps}，但它们未注册")
                    return False
                logging.info(f"[服务容器] 初始化服务：{service_name}")
        logging.info("[服务容器] 所有服务初始化成功")
        return True
    
    def _check_circular_dependencies(self) -> bool:
        """检查循环依赖"""
        visited = set()
        rec_stack = set()
        
        def visit(node):
            if node in rec_stack:
                return True  # 发现循环
            if node in visited:
                return False
            visited.add(node)
            rec_stack.add(node)
            for dep in self._dependencies.get(node, []):
                if visit(dep):
                    return True
            rec_stack.remove(node)
            return False
        
        for service_name in self._dependencies:
            if visit(service_name):
                return True
        return False

    def create_and_register_all_services(self) -> bool:
        """创建并注册所有服务（按依赖顺序）

        Returns:
            bool: 是否成功
        """
        try:
            logging.info("[服务容器] 开始创建和注册所有服务...")

            # 1. 创建事件总线
            # ✅ 传递渠道唯一：统一使用get_global_event_bus()工厂函数，禁止降级直接实例化
            from ali2026v3_trading.event_bus import get_global_event_bus
            event_bus = get_global_event_bus()
            self.register('event_bus', event_bus)
            logging.info("[服务容器] 已注册 event_bus")

            # 2. 创建配置服务
            config = get_cached_params()
            self.register('config', config)
            logging.info("[服务容器] 已注册 config")

            # 3. 创建参数服务
            self.register('params', config)
            logging.info("[服务容器] 已注册 params")

            # 4. 创建存储服务
            from ali2026v3_trading.storage import get_instrument_data_manager
            storage = get_instrument_data_manager()
            self.register('storage', storage)
            logging.info("[服务容器] 已注册 storage")

            # 5. 创建信号服务
            from ali2026v3_trading.signal_service import SignalService
            signal_service = SignalService(event_bus=event_bus)
            self.register('signal', signal_service)
            logging.info("[服务容器] 已注册 signal")

            # 6. 创建订单服务
            from ali2026v3_trading.order_service import OrderService
            order_service = OrderService(event_bus=event_bus)
            self.register('order', order_service)
            logging.info("[服务容器] 已注册 order")

            # 7. 初始化所有服务
            success = self.initialize_all()

            if success:
                logging.info("[服务容器] 所有服务创建和初始化成功")
            else:
                logging.error("[服务容器] 服务初始化失败")

            return success

        except Exception as e:
            logging.error(f"[服务容器] 创建和注册服务失败: {e}")
            return False
