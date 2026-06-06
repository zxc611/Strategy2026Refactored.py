"""
Phase3-Sprint8: config_exchange_data.py — 从config_params.py提取的交易所/品种配置数据
包含: month_mapping, delivery_month_rules, tick_size_by_product, exchange_trading_sessions, rollover_days
"""
from __future__ import annotations

month_mapping = {
    "IF": ["IF2602", "IF2603", "IF2606", "IF2609", "IF2612"],
    "IH": ["IH2602", "IH2603", "IH2606", "IH2609", "IH2612"],
    "IM": ["IM2602", "IM2603", "IM2606", "IM2609", "IM2612"],
    "CU": ["CU2603", "CU2604", "CU2605", "CU2606", "CU2607"],
    "AL": ["AL2603", "AL2604", "AL2605", "AL2606", "AL2607"],
    "ZN": ["ZN2603", "ZN2604", "ZN2605", "ZN2606", "ZN2607"],
    "RB": ["RB2603", "RB2605", "RB2606", "RB2610", "RB2611"],
    "AU": ["AU2603", "AU2604", "AU2606", "AU2608", "AU2612"],
    "AG": ["AG2603", "AG2604", "AG2606", "AG2609", "AG2612"],
    "M":  ["M2603",  "M2605",  "M2607",  "M2608",  "M2609"],
    "Y":  ["Y2603",  "Y2605",  "Y2607",  "Y2608",  "Y2609"],
    "A":  ["A2603",  "A2605",  "A2607",  "A2609",  "A2611"],
    "JM": ["JM2604", "JM2605", "JM2606", "JM2609", "JM2612"],
    "I":  ["I2603",  "I2605",  "I2606",  "I2609",  "I2610"],
    "J":  ["J2604",  "J2605",  "J2606",  "J2609",  "J2612"],
    "CF": ["CF2603", "CF2605", "CF2607", "CF2609", "CF2611"],
    "SR": ["SR2603", "SR2605", "SR2607", "SR2609", "SR2611"],
    "MA": ["MA2603", "MA2605", "MA2606", "MA2609", "MA2612"],
    "TA": ["TA2603", "TA2605", "TA2606", "TA2609", "TA2612"],
}

delivery_month_rules = {
    "CFFEX": {"delivery_months": [1,2,3,4,5,6,7,8,9,10,11,12], "notice_day_offset": -3, "last_trade_day_offset": -1},
    "SHFE": {"delivery_months": [1,2,3,4,5,6,7,8,9,10,11,12], "notice_day_offset": -5, "last_trade_day_offset": -3},
    "DCE":  {"delivery_months": [1,2,3,4,5,6,7,8,9,10,11,12], "notice_day_offset": -5, "last_trade_day_offset": -3},
    "CZCE": {"delivery_months": [1,2,3,4,5,6,7,8,9,10,11,12], "notice_day_offset": -5, "last_trade_day_offset": -3},
    "GFEX": {"delivery_months": [1,2,3,4,5,6,7,8,9,10,11,12], "notice_day_offset": -5, "last_trade_day_offset": -3},
    "INE":  {"delivery_months": [1,2,3,4,5,6,7,8,9,10,11,12], "notice_day_offset": -5, "last_trade_day_offset": -3},
}

tick_size_by_product = {
    "IF": 0.2, "IC": 0.2, "IH": 0.2, "IM": 0.2,
    "rb": 1, "hc": 1, "i": 0.5, "j": 0.5, "jm": 0.5,
    "cu": 10, "al": 5, "zn": 5, "pb": 5, "ni": 10, "sn": 10, "au": 0.02, "ag": 1,
    "m": 1, "y": 2, "a": 1, "c": 1, "cs": 1, "p": 2,
    "SR": 1, "CF": 5, "TA": 2, "MA": 1, "FG": 1, "SA": 1,
    "sc": 0.1, "lu": 5, "fu": 1, "pg": 1, "bc": 0.1, "ec": 0.1,
}

exchange_trading_sessions = {
    "CFFEX": {"day_start": "09:30", "day_end": "15:00", "has_night": False},
    "SHFE": {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "02:30"},
    "DCE":  {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "02:30"},
    "CZCE": {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "23:30"},
    "INE":  {"day_start": "09:00", "day_end": "15:00", "has_night": True, "night_start": "21:00", "night_end": "02:30"},
    "GFEX": {"day_start": "09:00", "day_end": "15:00", "has_night": False},
}

rollover_days = {"CFFEX": 5, "SHFE": 5, "DCE": 5, "CZCE": 7, "INE": 5, "GFEX": 5}