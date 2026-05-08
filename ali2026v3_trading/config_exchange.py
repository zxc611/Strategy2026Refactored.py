"""
交易所与合约配置模块 - 从 config_service.py 拆分
职责：交易所配置、品种映射、合约解析工具
"""
from __future__ import annotations

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from ali2026v3_trading.shared_utils import normalize_year_month


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

    enable_auto_generation: bool = True
    enable_futures: bool = True
    enable_options: bool = True

    option_products: Dict[str, tuple] = field(default_factory=lambda: {
        "IO": ("IF", "CFFEX"),
        "HO": ("IH", "CFFEX"),
        "MO": ("IM", "CFFEX"),
        "EO": ("IM", "CFFEX"),
        "CU": ("CU", "SHFE"),
        "AL": ("AL", "SHFE"),
        "ZN": ("ZN", "SHFE"),
        "AU": ("AU", "SHFE"),
        "AG": ("AG", "SHFE"),
        "RB": ("RB", "SHFE"),
        "RU": ("RU", "SHFE"),
        "MA": ("MA", "CZCE"),
        "TA": ("TA", "CZCE"),
        "OI": ("OI", "CZCE"),
        "RM": ("RM", "CZCE"),
        "SA": ("SA", "CZCE"),
        "FG": ("FG", "CZCE"),
        "SR": ("SR", "CZCE"),
        "CF": ("CF", "CZCE"),
        "AP": ("AP", "CZCE"),
        "CJ": ("CJ", "CZCE"),
        "SF": ("SF", "CZCE"),
        "SM": ("SM", "CZCE"),
        "UR": ("UR", "CZCE"),
        "M": ("M", "DCE"),
        "Y": ("Y", "DCE"),
        "P": ("P", "DCE"),
        "A": ("A", "DCE"),
        "L": ("L", "DCE"),
        "V": ("V", "DCE"),
        "PP": ("PP", "DCE"),
        "EB": ("EB", "DCE"),
        "I": ("I", "DCE"),
        "EG": ("EG", "DCE"),
        "C": ("C", "DCE"),
        "CS": ("CS", "DCE"),
        "SC": ("SC", "INE"),
        "SI": ("SI", "GFEX"),
        "LC": ("LC", "GFEX"),
    })

    product_exchanges: Dict[str, str] = field(default_factory=lambda: {
        "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX",
        "IO": "CFFEX", "HO": "CFFEX", "MO": "CFFEX", "EO": "CFFEX",
        "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE", "RB": "SHFE", "AU": "SHFE", "AG": "SHFE",
        "NI": "SHFE", "SN": "SHFE", "PB": "SHFE", "SS": "SHFE", "WR": "SHFE",
        "RU": "SHFE", "NR": "SHFE", "HC": "SHFE", "BU": "SHFE", "SP": "SHFE", "FU": "SHFE", "BR": "SHFE",
        "BC": "INE", "LU": "INE", "SC": "INE", "EC": "INE",
        "M": "DCE", "Y": "DCE", "A": "DCE", "JM": "DCE", "I": "DCE",
        "C": "DCE", "CS": "DCE", "JD": "DCE", "L": "DCE", "V": "DCE", "PP": "DCE",
        "EG": "DCE", "PG": "DCE", "J": "DCE", "P": "DCE", "EB": "DCE", "B": "DCE", "RR": "DCE", "LH": "DCE",
        "CF": "CZCE", "SR": "CZCE", "MA": "CZCE", "TA": "CZCE", "RM": "CZCE", "OI": "CZCE",
        "SA": "CZCE", "PF": "CZCE", "FG": "CZCE", "SF": "CZCE", "SM": "CZCE", "AP": "CZCE", "CJ": "CZCE", "UR": "CZCE", "PX": "CZCE", "SH": "CZCE", "PR": "CZCE", "PK": "CZCE",
        "LC": "GFEX", "SI": "GFEX",
    })

    futures_switches: Dict[str, bool] = field(default_factory=lambda: {
        "IF": True, "IH": True, "IC": True, "IM": True,
        "T": True, "TF": True, "TS": True, "TL": True,
        "CU": True, "AL": True, "ZN": True, "PB": True, "NI": True, "SN": True,
        "AU": True, "AG": True, "RB": True, "HC": True, "SS": True,
        "RU": True, "BU": True, "SP": True, "FU": True, "LU": True, "WR": False,
        "MA": True, "TA": True, "PF": True, "UR": True, "OI": True, "RM": True,
        "CF": True, "SR": True, "AP": True, "CJ": True, "SA": True, "FG": True,
        "SF": True, "SM": True, "RI": False, "LR": False, "JR": False, "WH": False, "PM": False,
        "M": True, "Y": True, "P": True, "A": True, "B": True,
        "L": True, "V": True, "PP": True, "EB": True, "EG": True, "PG": True,
        "I": True, "J": True, "JM": True, "C": True, "CS": True, "JD": True,
        "BB": False, "RR": False,
        "SC": True, "BC": True,
        "SI": True, "LC": True,
    })

    def get_instruments_for_exchange(self, exchange: str) -> List[str]:
        return self.simulated_instruments.get(exchange, [])

    def get_all_simulated_instruments(self) -> List[str]:
        instruments = []
        for inst_list in self.simulated_instruments.values():
            instruments.extend(inst_list)
        return instruments

    def set_future_switch(self, product_code: str, enabled: bool):
        self.futures_switches[product_code] = enabled
        logging.info(f"[Config] 期货品种开关：{product_code} = {'启用' if enabled else '关闭'}")

    def get_enabled_futures(self) -> List[str]:
        return [code for code, enabled in self.futures_switches.items() if enabled]

    def get_disabled_futures(self) -> List[str]:
        return [code for code, enabled in self.futures_switches.items() if not enabled]

    def is_future_enabled(self, product_code: str) -> bool:
        if not self.enable_futures:
            return False
        return self.futures_switches.get(product_code, False)

    def should_generate_option(self, option_product: str, underlying: str) -> bool:
        if not self.enable_options:
            return False
        if not self.is_future_enabled(underlying):
            return False
        return True

    def get_enabled_options(self) -> List[str]:
        enabled = []
        for opt_code, (underlying, exchange) in self.option_products.items():
            if self.is_future_enabled(underlying):
                enabled.append(opt_code)
        return enabled

    def get_disabled_options(self) -> List[str]:
        disabled = []
        for opt_code, (underlying, exchange) in self.option_products.items():
            if not self.is_future_enabled(underlying):
                disabled.append(opt_code)
        return disabled

    def get_generation_config(self) -> Dict[str, Any]:
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


def _get_option_underlying_product(option_product: str) -> str:
    from ali2026v3_trading.product_initializer import _get_option_underlying_product as _impl
    return _impl(option_product)


def ensure_products_with_retry(data_service, max_retries: int = 5) -> Dict[str, int]:
    from ali2026v3_trading.product_initializer import ensure_products_with_retry as _impl
    return _impl(data_service, max_retries)


def build_exchange_mapping(custom_mapping: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
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
    from ali2026v3_trading.subscription_manager import SubscriptionManager
    normalized_mapping = build_exchange_mapping(exchange_mapping)
    token = str(product_or_instrument or "")
    product_code = token
    try:
        if SubscriptionManager.is_option(token):
            parsed = SubscriptionManager.parse_option(token)
            product_code = parsed.get('product', token)
        else:
            parsed = SubscriptionManager.parse_future(token)
            product_code = parsed.get('product', token)
    except (ValueError, KeyError):
        pass
    result = None
    for key in normalized_mapping:
        if str(key).upper() == product_code.upper():
            result = normalized_mapping[key]
            break
    return result or str(default_exchange or "CFFEX")


def make_platform_future_id(product: str, year_month: str) -> str:
    product = str(product or '').strip()
    year_month = str(year_month or '').strip()
    if not product or not year_month:
        return ''
    year_month = normalize_year_month(year_month)
    return f'{product}{year_month}'
