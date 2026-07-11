# [M1-85] 交易所与合约配置
"""
交易所与合约配置模块 - 从 config_service.py 拆分
职责：交易所配置、品种映射、合约解析工具
"""
from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

from ali2026v3_trading.infra.shared_utils import normalize_year_month, CHINA_TZ
from ali2026v3_trading.infra.subscription_service import SubscriptionManager
import re


@dataclass(slots=True)
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

        "CU": ("CU", "SHFE"),
        "AL": ("AL", "SHFE"),
        "ZN": ("ZN", "SHFE"),
        "AU": ("AU", "SHFE"),
        "AG": ("AG", "SHFE"),
        "RB": ("RB", "SHFE"),
        "RU": ("RU", "SHFE"),
        "NI": ("NI", "SHFE"),
        "SN": ("SN", "SHFE"),
        "PB": ("PB", "SHFE"),
        "SS": ("SS", "SHFE"),
        "HC": ("HC", "SHFE"),
        "BU": ("BU", "SHFE"),
        "SP": ("SP", "SHFE"),
        "FU": ("FU", "SHFE"),
        "BR": ("BR", "SHFE"),
        "AD": ("AD", "SHFE"),
        "AO": ("AO", "SHFE"),
        "OP": ("OP", "SHFE"),
        "NR": ("NR", "INE"),
        "BC": ("BC", "INE"),
        "LU": ("LU", "INE"),
        "SC": ("SC", "INE"),
        "EC": ("EC", "INE"),
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
        "PF": ("PF", "CZCE"),
        "PX": ("PX", "CZCE"),
        "SH": ("SH", "CZCE"),
        "PR": ("PR", "CZCE"),
        "PK": ("PK", "CZCE"),
        "CY": ("CY", "CZCE"),
        "JR": ("JR", "CZCE"),
        "PL": ("PL", "CZCE"),
        "PM": ("PM", "CZCE"),
        "RI": ("RI", "CZCE"),
        "RS": ("RS", "CZCE"),
        "WH": ("WH", "CZCE"),
        "ZC": ("ZC", "CZCE"),
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
        "JM": ("JM", "DCE"),
        "J": ("J", "DCE"),
        "JD": ("JD", "DCE"),
        "PG": ("PG", "DCE"),
        "BZ": ("BZ", "DCE"),
        "BB": ("BB", "DCE"),
        "FB": ("FB", "DCE"),
        "LH": ("LH", "DCE"),
        "LG": ("LG", "DCE"),
        "RR": ("RR", "DCE"),
        "SI": ("SI", "GFEX"),
        "LC": ("LC", "GFEX"),
        "PD": ("PD", "GFEX"),
        "PS": ("PS", "GFEX"),
        "PT": ("PT", "GFEX"),
    })

    product_exchanges: Dict[str, str] = field(default_factory=lambda: {
        "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX",
        "IO": "CFFEX", "HO": "CFFEX", "MO": "CFFEX",
        "T": "CFFEX", "TF": "CFFEX", "TL": "CFFEX", "TS": "CFFEX",
        "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE", "RB": "SHFE", "AU": "SHFE", "AG": "SHFE",
        "NI": "SHFE", "SN": "SHFE", "PB": "SHFE", "SS": "SHFE", "WR": "SHFE",
        "RU": "SHFE", "NR": "INE", "HC": "SHFE", "BU": "SHFE", "SP": "SHFE", "FU": "SHFE", "BR": "SHFE",
        "AD": "SHFE", "AO": "SHFE", "OP": "SHFE",
        "BC": "INE", "LU": "INE", "SC": "INE", "EC": "INE",
        "M": "DCE", "Y": "DCE", "A": "DCE", "JM": "DCE", "I": "DCE",
        "C": "DCE", "CS": "DCE", "JD": "DCE", "L": "DCE", "V": "DCE", "PP": "DCE",
        "EG": "DCE", "PG": "DCE", "J": "DCE", "P": "DCE", "EB": "DCE", "B": "DCE", "RR": "DCE", "LH": "DCE",
        "BB": "DCE", "BZ": "DCE", "FB": "DCE", "LG": "DCE",
        "CF": "CZCE", "SR": "CZCE", "MA": "CZCE", "TA": "CZCE", "RM": "CZCE", "OI": "CZCE",
        "SA": "CZCE", "PF": "CZCE", "FG": "CZCE", "SF": "CZCE", "SM": "CZCE", "AP": "CZCE", "CJ": "CZCE", "UR": "CZCE", "PX": "CZCE", "SH": "CZCE", "PR": "CZCE", "PK": "CZCE",
        "CY": "CZCE", "JR": "CZCE", "PL": "CZCE", "PM": "CZCE", "RI": "CZCE", "RS": "CZCE", "WH": "CZCE", "ZC": "CZCE",
        "LC": "GFEX", "SI": "GFEX", "PD": "GFEX", "PS": "GFEX", "PT": "GFEX",
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
    try:
        from ali2026v3_trading.product_initializer import _get_option_underlying_product as _impl
        return _impl(option_product)
    except (ImportError, AttributeError) as e:
        logging.warning(f"[config_exchange._get_option_underlying_product] 导入失败: {e}")
        m = re.match(r'^[A-Za-z]+', option_product)
        return m.group(0) if m else ''


def ensure_products_with_retry(data_service, max_retries: int = 5) -> Dict[str, int]:
    try:
        from ali2026v3_trading.lifecycle.product_initializer import ensure_products_with_retry as _impl
        return _impl(data_service, max_retries)
    except (ImportError, AttributeError) as e:
        logging.error("[config_exchange.ensure_products_with_retry] 导入失败: %s", e)
        raise RuntimeError(f"config_exchange代理导入失败，策略无法继续初始化: {e}")


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
    except (ValueError, KeyError) as e:
        logging.warning(f"[resolve_product_exchange] 解析合约失败 token={token}: {e}")
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


month_mapping = {
    "IF": [1, 2, 3, 6, 9, 12],
    "IH": [1, 2, 3, 6, 9, 12],
    "IC": [1, 2, 3, 6, 9, 12],
    "IM": [1, 2, 3, 6, 9, 12],
    "T": [3, 6, 9, 12],
    "TF": [3, 6, 9, 12],
    "TS": [3, 6, 9, 12],
    "TL": [3, 6, 9, 12],
}


def _add_calendar_months(year: int, month: int, offset: int) -> Tuple[int, int]:
    total_months = year * 12 + (month - 1) + offset
    next_year, month_index = divmod(total_months, 12)
    return next_year, month_index + 1


def get_runtime_scoring_months(reference_date: Optional[date] = None, count: int = 5) -> List[str]:
    """Return the runtime scoring month sequence.

    The trading desk currently defines:
    - 7月为当月
    - 8月为下月

    More generally, once the current calendar month passes the configured
    delivery day, the front scoring month rolls to the next calendar month.
    The remaining slots are filled by the next available quarter months.
    """
    if count <= 0:
        return []

    if reference_date is None:
        reference_date = datetime.now(CHINA_TZ).date()

    front_year = reference_date.year
    front_month = reference_date.month
    delivery_day = delivery_month_rules.get("CZCE", {}).get("delivery_day", 15)
    if reference_date.day > int(delivery_day):
        front_year, front_month = _add_calendar_months(front_year, front_month, 1)

    months: List[Tuple[int, int]] = []

    def _append_unique(year: int, month: int) -> None:
        item = (year, month)
        if item not in months:
            months.append(item)

    _append_unique(front_year, front_month)
    next_year, next_month = _add_calendar_months(front_year, front_month, 1)
    _append_unique(next_year, next_month)

    scan_year, scan_month = front_year, front_month
    while len(months) < count:
        scan_year, scan_month = _add_calendar_months(scan_year, scan_month, 1)
        if scan_month in {3, 6, 9, 12}:
            _append_unique(scan_year, scan_month)

    return [f"{year % 100:02d}{month:02d}" for year, month in months[:count]]

delivery_month_rules = {
    "CFFEX": {"delivery_day": 15, "delivery_month_offset": 0},
    "SHFE": {"delivery_day": 15, "delivery_month_offset": 0},
    "DCE": {"delivery_day": 15, "delivery_month_offset": 0},
    "CZCE": {"delivery_day": 15, "delivery_month_offset": 0},
    "INE": {"delivery_day": 15, "delivery_month_offset": 0},
    "GFEX": {"delivery_day": 15, "delivery_month_offset": 0},
}

tick_size_by_product = {
    "IF": 0.2, "IH": 0.2, "IC": 0.2, "IM": 0.2,
    "IO": 0.1, "HO": 0.1, "MO": 0.1,
    "T": 0.005, "TF": 0.005, "TS": 0.005, "TL": 0.005,
    "CU": 10, "AL": 5, "ZN": 5, "PB": 5, "NI": 10,
    "AU": 0.02, "AG": 1, "RB": 1, "HC": 1, "SS": 5,
    "RU": 5, "BU": 2, "SP": 2, "FU": 1, "LU": 1,
    "MA": 1, "TA": 2, "PF": 2, "UR": 1, "OI": 1,
    "CF": 5, "SR": 1, "AP": 1, "CJ": 5, "SA": 1,
    "FG": 1, "SF": 2, "SM": 1, "M": 1, "Y": 2,
    "P": 2, "A": 1, "B": 1, "L": 1, "V": 5,
    "PP": 1, "EB": 1, "EG": 1, "PG": 1, "I": 0.5,
    "J": 0.5, "JM": 0.5, "C": 1, "CS": 1, "JD": 0.5,
    "SC": 0.1, "BC": 0.1, "SI": 5, "LC": 10,
}

exchange_trading_sessions = {
    "CFFEX": {"day": [(9, 15), (11, 30)], "night": []},
    "SHFE": {"day": [(9, 0), (11, 30)], "night": [(21, 0), (1, 0)]},
    "DCE": {"day": [(9, 0), (11, 30)], "night": [(21, 0), (23, 0)]},
    "CZCE": {"day": [(9, 0), (11, 30)], "night": [(21, 0), (23, 30)]},
    "INE": {"day": [(9, 0), (11, 30)], "night": [(21, 0), (1, 0)]},
    "GFEX": {"day": [(9, 0), (11, 30)], "night": []},
}

rollover_days = {
    "CFFEX": 5,
    "SHFE": 5,
    "DCE": 5,
    "CZCE": 5,
    "INE": 5,
    "GFEX": 5,
}
