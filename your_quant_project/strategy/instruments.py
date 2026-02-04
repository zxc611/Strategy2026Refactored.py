"""合约加载与归一化逻辑。"""
from __future__ import annotations

import json
import os
import re
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

try:
    from pythongo import infini  # type: ignore
except Exception:
    infini = None  # type: ignore

# [CRITICAL FIX] 默认月份映射表（从 Source Strategy20260105_3.py 同步）
# 必须包含 Source 中定义的所有键值对，尤其是 J, AU, AG 等
DEFAULT_MONTH_MAPPING = {
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
  "TA": ["TA2603", "TA2605"]
}

class InstrumentLoaderMixin:
    def _normalize_exchange_code(self, exchange: str) -> str:
        """规范化交易所代码，兼容别名。"""
        try:
            exch = (exchange or "").strip().upper()
            alias = {
                "CCFX": "CFFEX",
                "CFFE": "CFFEX",
                "SHFE_TEST": "SHFE",
                "DCE_TEST": "DCE",
                "CZCE_TEST": "CZCE",
            }
            return alias.get(exch, exch)
        except Exception:
            return (exchange or "").strip().upper()

    def _expand_czce_year_month(self, symbol: str) -> str:
        """将 CZCE 一位年份格式扩展为两位年份，兼容三/四位数字尾巴。"""
        try:
            m_two = re.match(r"^([A-Z]{1,})(\d{2})(\d{2})$", symbol)
            if m_two:
                prefix, yy, mm = m_two.group(1), int(m_two.group(2)), int(m_two.group(3))
                if 1 <= mm <= 12:
                    cur_y2 = datetime.now().year % 100
                    try:
                        future_window = int(getattr(self.params, "czce_year_future_window", 10) or 10)
                        past_window = int(getattr(self.params, "czce_year_past_window", 10) or 10)
                    except Exception:
                        future_window = 10
                        past_window = 10
                    if (cur_y2 - past_window) <= yy <= (cur_y2 + future_window):
                        return symbol
            m_three = re.match(r"^([A-Z]{1,})(\d)(\d{2})$", symbol)
            if m_three:
                return symbol
            m_four = re.match(r"^([A-Z]{1,})(\d)(\d{3})$", symbol)
            if m_four:
                prefix, y_digit, tail = m_four.group(1), int(m_four.group(2)), m_four.group(3)
                mm = tail[-2:]
                month = int(mm)
                if 1 <= month <= 12:
                    cur_y2 = datetime.now().year % 100
                    decade = (cur_y2 // 10) * 10
                    if y_digit > (cur_y2 % 10):
                        decade = max(0, decade - 10)
                    y2 = decade + y_digit
                    return f"{prefix}{y2:02d}{tail}"
            m_opt = re.match(r"^([A-Z]{1,})(\d)(\d{2})([CP])(\d+)$", symbol)
            if m_opt:
                prefix = m_opt.group(1)
                y_digit = int(m_opt.group(2))
                mm = m_opt.group(3)
                cp = m_opt.group(4)
                strike = m_opt.group(5)
                cur_y2 = datetime.now().year % 100
                decade = (cur_y2 // 10) * 10
                if y_digit > (cur_y2 % 10):
                    decade = max(0, decade - 10)
                y2 = decade + y_digit
                return f"{prefix}{y2:02d}{mm}{cp}{strike}"
            return symbol
        except Exception:
            return symbol

    def _normalize_future_id(self, instrument_id: str) -> str:
        """Normalize instrument ID by stripping exchange prefixes, removing separators, uppercasing, and expanding CZCE 一位年份格式。"""
        try:
            if not instrument_id:
                return ""
            normalized = str(instrument_id).upper()
            if "." in normalized:
                normalized = normalized.split(".")[-1]
            normalized = re.sub(r"^(CFFEX|SHFE|DCE|CZCE|GFEX)[_\-]+", "", normalized)
            normalized = re.sub(r"[\s_]+", "", normalized)
            normalized = re.sub(r"^(CFFEX|SHFE|DCE|CZCE|GFEX)", "", normalized)
            normalized = self._expand_czce_year_month(normalized)
            return normalized
        except Exception:
            return ""

    def _extract_month(self, instrument_id: str) -> Optional[int]:
        """提取合约月份（1-12）。优先识别CZCE样式（一位年+两位月）。"""
        s = self._normalize_future_id(instrument_id)
        match = re.search(r"[A-Za-z]+(\d)(\d{2})", s)
        if match:
            try:
                month = int(match.group(2))
                if 1 <= month <= 12:
                    return month
            except Exception:
                pass
        match = re.search(r"[A-Za-z]+(\d{2})(\d{1,2})", s)
        if match:
            try:
                month = int(match.group(2))
                if 1 <= month <= 12:
                    return month
            except Exception:
                pass
        return None

    def _extract_year(self, instrument_id: str) -> Optional[int]:
        """提取合约年份。优先识别CZCE样式（一位年+两位月）。"""
        s = self._normalize_future_id(instrument_id)
        match = re.search(r"[A-Za-z]+(\d)(\d{2})", s)
        if match:
            try:
                year_digit = int(match.group(1))
                now = datetime.now()
                current_year_last_digit = now.year % 10
                if current_year_last_digit >= year_digit:
                    return now.year - (current_year_last_digit - year_digit)
                return now.year - 10 + (year_digit - current_year_last_digit)
            except Exception:
                pass
        match = re.search(r"[A-Za-z]+(\d{2})(\d{1,2})", s)
        if match:
            try:
                year = int(match.group(1))
                return 2000 + year if year < 50 else 1900 + year
            except Exception:
                pass
        return None

    def _is_real_month_contract(self, instrument_id: str) -> bool:
        """判断是否为真实的月份合约代码，过滤掉 Main/Weighted 等综合合约。"""
        if not instrument_id:
            return False
        raw = str(instrument_id).upper()
        s = self._normalize_future_id(raw)
        if not s:
            return False
        bad_tags = (
            "MAIN", "INDEX", "WEIGHT", "WEIGHTED", "HOT", "CONT", "CONTINUOUS",
            "NEAR", "THIS", "NEXT", "000", "888", "999",
        )
        has_exch_prefix = re.match(r"^(CFFEX|SHFE|DCE|CZCE|GFEX)[_\-]", raw) is not None
        if any(tag in raw for tag in bad_tags) or (("_" in raw or "-" in raw) and not has_exch_prefix):
            return False
        if re.match(r"^[A-Z]{1,}\d{2}\d{1,2}$", s):
            return True
        if re.match(r"^[A-Z]{1,}\d\d{2}$", s):
            return True
        return False

    def _normalize_future_id_internal(self, instrument_id: str) -> str:
        """内部简化标准化（保留给少数场景使用，避免覆盖主逻辑）"""
        if not instrument_id:
            return ""
        return str(instrument_id).upper().strip()

    def get_price_tick(self, exchange: str, instrument_id: str) -> float:
        """获取合约最小变动价位"""
        # 类似 Source: 查 map -> cache -> 0.01
        inst_upper = self._normalize_future_id(instrument_id)
        if hasattr(self, "instrument_dict"):
             info = self.instrument_dict.get(inst_upper)
             if info:
                 try:
                     val = float(info.get("PriceTick", 0))
                     if val > 0: return val
                 except: pass
        return 0.01

    def _extract_product_code(self, instrument_id: str) -> str:
        """提取期货品种代码"""
        if not instrument_id: return ""
        normalized = self._normalize_future_id(str(instrument_id))
        match = re.match(r"^([A-Z]+)", normalized)
        return match.group(1).upper() if match else ""

    def _extract_future_symbol(self, option_symbol: str) -> Optional[str]:
        """从期权代码提取期货代码，增强支持商品期权（一位品种、一位年格式、各类分隔符）"""
        if not option_symbol:
            return None
        
        # 先进行归一化，去掉交易所前缀和点，并在大写模式下操作
        # 注意：_normalize_future_id 内部已经做了 .upper() 和 去前缀
        s_norm = self._normalize_future_id(option_symbol)
        
        # 股指映射表：IO->IF, HO->IH, MO->IM
        prefix_map = {"IO": "IF", "HO": "IH", "MO": "IM"}
        
        # 提取头部：品种代码 + (可选分隔符) + 数字串
        # 例如 SR601... -> SR, 601
        # IO2602... -> IO, 2602
        # C2601... -> C, 2601
        # M-2601 -> M, 2601 
        m = re.match(r"^([A-Za-z]+)([-_]?)(\d+)", s_norm)
        if not m:
            return None
            
        code = m.group(1)
        # sep = m.group(2)
        digits = m.group(3)
        
        year_str = ""
        month_str = ""
        
        if len(digits) == 4:
            # 两位年 + 两位月 (2601)
            year_str = digits[:2]
            month_str = digits[2:]
        elif len(digits) == 3:
            # 一位年 + 两位月 (601) -> 郑商所风格
            # 不进行年份补全，保留原一位年，防止生成错误的 CF2603 之类不存在的合约
            year_str = digits[:1]
            month_str = digits[1:]
            
        elif len(digits) >= 5:
             # 可能是 2601 + StrikePrice (如 26013000) 无分隔符情况
             # 假设前四位是年月
             year_str = digits[:2]
             month_str = digits[2:4]
        else:
            # 长度 1, 2？ 不足以构成年月
            return None

        # 校验月份
        try:
            m_val = int(month_str)
            if not (1 <= m_val <= 12):
                return None
        except Exception:
            return None
            
        # 映射品种代码 (HO->IH 等)
        mapped_code = prefix_map.get(code, code)
        
        # 再次归一化确保格式统一 (IH2601)
        return self._normalize_future_id(f"{mapped_code}{year_str}{str(month_str).zfill(2)}")

    def _map_future_to_option_prefix(self, future_code: str) -> str:
        """将期货合约代码转换为对应的期权合约代码前缀/标识。"""
        if not future_code:
            return ""
        m = re.match(r"^([A-Z]+)", future_code.upper())
        if not m:
            return ""
        f_prefix = m.group(1)
        reverse_map = {"IF": "IO", "IH": "HO", "IM": "MO"}
        return reverse_map.get(f_prefix, f_prefix)

    def _to_instrument_dict(self, obj: Any) -> Optional[dict]:
        """兼容 dict / InstrumentData 等对象，提取必要字段"""
        try:
            if obj is None:
                return None
            if isinstance(obj, dict):
                inst = dict(obj)
                try:
                    sp = inst.get("StrikePrice") or inst.get("strike_price") or inst.get("_strike_price") or inst.get("strikePrice")
                    if sp is not None:
                        inst["StrikePrice"] = sp
                    ot = inst.get("OptionType") or inst.get("option_type") or inst.get("call_put") or inst.get("_option_type") or inst.get("option_kind") or inst.get("_option_kind")
                    if ot is not None:
                        s = str(ot).upper()
                        if s in ("C", "CALL", "1"):
                            inst["OptionType"] = "C"
                        elif s in ("P", "PUT", "2"):
                            inst["OptionType"] = "P"
                        else:
                            inst["OptionType"] = s
                except Exception:
                    pass
                return inst

            def _safe_attr(o, name, default=""):
                try:
                    val = getattr(o, name, default)
                    return val if val is not None else default
                except Exception:
                    return default

            instrument_id = _safe_attr(obj, "instrument_id") or _safe_attr(obj, "_instrument_id")
            exchange_id = _safe_attr(obj, "exchange") or _safe_attr(obj, "_exchange") or _safe_attr(obj, "exchange_id")
            product_class_raw = _safe_attr(obj, "_product_type") or _safe_attr(obj, "product_type") or _safe_attr(obj, "product_class") or ""
            strike_price_val = _safe_attr(obj, "strike_price", None) or _safe_attr(obj, "_strike_price", None)
            option_type_val = _safe_attr(obj, "option_type", None) or _safe_attr(obj, "_option_type", None) or _safe_attr(obj, "call_put", None) or _safe_attr(obj, "option_kind", None)
            
            product_class = ""
            upper_pc = str(product_class_raw).upper()
            if upper_pc in ("1", "FUTURE", "FUTURES", "F", "I"):
                product_class = "1"
            elif upper_pc in ("2", "OPTION", "OPTIONS", "O", "OPT", "OPTN", "T", "9", "H"):
                product_class = "2"
            elif "期货" in str(product_class_raw):
                product_class = "1"
            elif "期权" in str(product_class_raw):
                product_class = "2"
            else:
                option_attrs = (
                    "option_type",
                    "_option_type",
                    "option_kind",
                    "_option_kind",
                    "strike_price",
                    "_strike_price",
                    "call_put",
                    "_call_put",
                )
                has_option_attr = any(_safe_attr(obj, attr, None) not in (None, "") for attr in option_attrs)
                if has_option_attr:
                    product_class = "2"
                elif instrument_id and re.search(r"[CP][-_]?\d", str(instrument_id)):
                    product_class = "2"
                else:
                    product_class = "1"
            return {
                "ExchangeID": exchange_id,
                "InstrumentID": instrument_id,
                "ProductClass": product_class,
                **({"StrikePrice": strike_price_val} if strike_price_val not in (None, "") else {}),
                **({"OptionType": ("C" if str(option_type_val).upper() in ("C", "CALL", "1") else ("P" if str(option_type_val).upper() in ("P", "PUT", "2") else str(option_type_val).upper()))} if option_type_val not in (None, "") else {}),
            }
        except Exception:
            return None

    # --- Missing Helpers Ported ---
    
    def _has_option_for_product(self, product_code: str) -> bool:
        """检查是否有对应期权（兼容不同分组）"""
        try:
            prod = str(product_code).upper()
            if not getattr(self, "option_instruments", None): return False
            # 1. 直接按 Product 查找
            if prod in self.option_instruments: return True
            
            # 2. 查找 future_to_option_map
            f2o = getattr(self, "future_to_option_map", {}) or {}
            opt_code = f2o.get(prod)
            if opt_code and opt_code in self.option_instruments: return True
            
            # 3. 遍历检查键的前缀
            # [Optimization] Performance Warning: This iterates ALL keys.
            # Only do this if strictly necessary. 
            # In 'OptionWidthCalculation', we call this per future.
            # Optimization: If self.option_instruments is huge, cache this result.
            cached = getattr(self, "_has_opt_cache", {})
            if prod in cached: return cached[prod]

            for k in self.option_instruments.keys():
                k_up = str(k).upper()
                if k_up.startswith(prod) or (prod in k_up): 
                    cached[prod] = True
                    if not hasattr(self, "_has_opt_cache"): self._has_opt_cache = {}
                    self._has_opt_cache[prod] = True
                    return True
            
            if not hasattr(self, "_has_opt_cache"): self._has_opt_cache = {}
            self._has_opt_cache[prod] = False
            return False

            return False
        except: return False

    def _get_option_group_options(self, future_id: str) -> List[Any]:
        """[Fix] 获取某个期货对应的期权合约列表，兼容 Mock Injection"""
        try:
            fut = self._normalize_future_id(future_id)
            if not fut: return []
            
            opts = getattr(self, "option_instruments", {})
            if not opts: return []
            
            # 1. 优先尝试直接用 future_id 查找 (Mock Injection 注入的就是这个: IF2602)
            if fut in opts: 
                return opts[fut]
            if future_id in opts:
                return opts[future_id]
                
            # 2. 尝试按 Product 查找 (InstrumentLoaderMixin 默认逻辑: options grouped by product)
            prod = self._extract_product_code(fut)
            
            # 2.1 Direct product match
            if prod in opts: return opts[prod]
            
            # 2.2 Map match (IF -> IO)
            f2o = getattr(self, "future_to_option_map", {}) or {}
            opt_prod = f2o.get(prod)
            if opt_prod and opt_prod in opts:
                return opts[opt_prod]
            
            return []
        except Exception: return []

    def _is_symbol_current(self, symbol: str) -> bool:
        """仅依据指定月判断是否为当前合约。"""
        try:
            if not symbol:
                return False
            future_upper = self._normalize_future_id(symbol)
            if not future_upper:
                return False

            eff_map = self._get_effective_month_mapping()
            if eff_map:
                m = re.match(r'^([A-Z]+)', future_upper)
                if m:
                    prod = m.group(1)
                    lst = eff_map.get(prod)
                    if isinstance(lst, list) and lst:
                        spec = (lst[0] or "").strip().upper()
                        if spec:
                            return future_upper == spec
                return False

            spec_global = (getattr(self.params, "specified_month", "") or "").strip().upper()
            if spec_global:
                return future_upper == spec_global

            # 未配置时放行（用于测试/调试）
            return True
        except Exception:
            return False

    def _get_next_month_id(self, future_id: str) -> Optional[str]:
        """只从参数表读取指定下月"""
        try:
            future_clean = self._normalize_future_id(future_id)
            product_code = self._extract_product_code(future_clean).upper()
            if not product_code:
                product_code = self._extract_product_code(future_id or "").upper()
            
            eff_map = self._get_effective_month_mapping()
            candidate: Optional[str] = None
            lst = eff_map.get(product_code)
            
            # 尝试通过 f2o 反向查找商品代码
            if not lst:
                # 假设 f2o 存在
                f2o = getattr(self, "future_to_option_map", {})
                for fut_code, opt_code in f2o.items():
                    if str(opt_code).strip().upper() == product_code:
                         lst = eff_map.get(str(fut_code).strip().upper())
                         if lst: break
            
            if isinstance(lst, list) and len(lst) >= 2:
                candidate = self._normalize_future_id((lst[1] or "").strip())
                if candidate: return candidate
            
            # 格式转换尝试 (CZCE/Single)
            if isinstance(lst, list) and len(lst) >= 2:
                candidate = (lst[1] or "").strip().upper()
                # 简化：直接尝试 normalize
                alt = self._normalize_future_id(candidate)
                if alt: return alt

            # 兜底
            nm_spec = (getattr(self.params, "next_specified_month", "") or "").strip().upper()
            if nm_spec and nm_spec.startswith(product_code):
                 return self._normalize_future_id(nm_spec)

            return None
        except: return None

    def _is_symbol_specified_or_next(self, symbol: str) -> bool:
        """带缓存判断合约是否为指定月/下月"""
        try:
            symbol_norm = self._normalize_future_id(symbol)
            if not symbol_norm: return False
            # 简单缓存
            if not hasattr(self, "_contract_spec_cache"): self._contract_spec_cache = {}
            if symbol_norm in self._contract_spec_cache: return self._contract_spec_cache[symbol_norm]
            
            res = self._is_symbol_specified_or_next_uncached(symbol_norm)
            self._contract_spec_cache[symbol_norm] = res
            return res
        except: return False

    def _is_symbol_current_or_next(self, symbol: str) -> bool:
        """兼容旧命名：判断合约是否为指定月/指定下月。"""
        return self._is_symbol_specified_or_next(symbol)

    def _is_symbol_specified_or_next_uncached(self, symbol_norm: str) -> bool:
        try:
            eff_map = self._get_effective_month_mapping()
            symbol_upper = symbol_norm.upper()
            
            # Simple direct match if map empty
            if not eff_map:
                sm = (getattr(self.params, "specified_month", "") or "").strip().upper()
                nm = (getattr(self.params, "next_specified_month", "") or "").strip().upper()
                if not sm and not nm: return False
                return symbol_upper in (sm, nm)

            symbol_prod = self._extract_product_code(symbol_upper)
            months = eff_map.get(symbol_prod)
            if months and len(months) >= 2:
                sm = (months[0] or "").strip().upper()
                nm = (months[1] or "").strip().upper()
                return symbol_upper == sm or symbol_upper == nm
            
            # Fallback global
            sm = (getattr(self.params, "specified_month", "") or "").strip().upper()
            nm = (getattr(self.params, "next_specified_month", "") or "").strip().upper()
            return symbol_upper in (sm, nm)
        except: return False

    def _get_effective_month_mapping(self) -> Dict[str, List[str]]:
        """获取生效的月份映射表"""
        # 简化版：优先 ParamTableMixin 的 load_param_table
        mapping = {}
        # 1. Start with params.month_mapping
        mm = getattr(self.params, "month_mapping", {})
        if isinstance(mm, dict):
            for k, v in mm.items():
                if isinstance(v, (list, tuple)) and len(v)>=2:
                    mapping[str(k).upper()] = [self._normalize_future_id(str(v[0])), self._normalize_future_id(str(v[1]))]

        # 2. Merge from param_table.json via loaded logic
        # 假设 ParamTableMixin 已经加载了 _param_override_cache 或者我们可以调用 self._load_param_table()
        # 这里为了稳健调用 param_table.py 的函数 (若 Mixin 正确混入)
        loaded = None
        if hasattr(self, "_load_param_table"):
             loaded = self._load_param_table() # 假定返回 dict
        
        if loaded:
            # 尝试从 loaded (dict) 中找 month_mapping
            mm_json = loaded.get("original_defaults", {}).get("month_mapping") or loaded.get("params", {}).get("month_mapping")
            if isinstance(mm_json, str):
                 try: mm_json = json.loads(mm_json)
                 except: pass
            if isinstance(mm_json, dict):
                 for k, v in mm_json.items():
                      if isinstance(v, (list, tuple)) and len(v)>=2:
                           mapping[str(k).upper()] = [self._normalize_future_id(str(v[0])), self._normalize_future_id(str(v[1]))]
        
        # 3. Fallback debug mapping
        if not mapping:
             dbg = self._get_debug_month_mapping()
             if dbg:
                  for k, v in dbg.items():
                       mapping[str(k).upper()] = [self._normalize_future_id(str(v[0])), self._normalize_future_id(str(v[1]))]
        
        return mapping

    def _get_debug_month_mapping(self):
        """Mock fallback mapping logic similar to original"""
        # 简单解析 debug_month_mapping 字符串参数
        raw = getattr(self.params, "debug_month_mapping", "")
        if not raw: return {}
        # ... logic ...
        return {} # Placeholder for simplicity

    def _normalize_instruments(self, res: Any) -> List[dict]:
        """将infini 返回的合约数据转换为统一的dict 列表"""
        if res is None:
            return []
        normalized: List[dict] = []
        try:
            if not isinstance(res, (list, tuple, set)):
                res = [res]
            for obj in res:
                if isinstance(obj, (list, tuple, set)):
                    for sub in obj:
                        inst = self._to_instrument_dict(sub)
                        if inst and inst.get("InstrumentID"):
                            normalized.append(inst)
                    continue
                inst = self._to_instrument_dict(obj)
                if inst and inst.get("InstrumentID"):
                    normalized.append(inst)
        except Exception:
            return []
        return normalized

    def _normalize_option_group_keys(self) -> None:
        """将已分组的期权键进行彻底归一化。"""
        try:
            if not self.option_instruments:
                return
            normalized: Dict[str, List[Dict[str, Any]]] = {}
            for key, opts in self.option_instruments.items():
                target_key = self._extract_future_symbol(str(key))
                if not target_key:
                    target_key = self._normalize_future_id(str(key))
                if target_key not in normalized:
                    normalized[target_key] = []
                normalized[target_key].extend(opts)
            if len(normalized) != len(self.option_instruments) or set(normalized.keys()) != set(self.option_instruments.keys()):
                diff_keys = set(self.option_instruments.keys()) - set(normalized.keys())
                if diff_keys:
                    self._debug(f"期权键已归一化修正: {list(diff_keys)} -> {list(normalized.keys())}")
                self.option_instruments = normalized
                self._debug("期权分组键已完成标准归一化")
        except Exception as e:
            self._debug(f"期权分组键归一化失败: {e}")

    # [Added Helpers for Calculation Mixin]
    def _safe_option_dict(self, option: Any) -> Optional[Dict[str, Any]]:
        """将期权对象安全转换为 dict。"""
        try:
            if isinstance(option, dict):
                return option
            return self._to_instrument_dict(option)
        except Exception:
            return None

    def _safe_option_id(self, option: Dict[str, Any]) -> str:
        """安全提取期权合约ID，兼容不同字段名。"""
        try:
            if not option:
                return ""
            for key in ("InstrumentID", "instrument_id", "InstrumentId", "symbol", "Symbol"):
                val = option.get(key)
                if val:
                    return str(val).strip().upper()
        except Exception:
            pass
        return ""

    def _safe_option_exchange(self, option: Dict[str, Any], fallback: str = "") -> str:
        """安全提取期权交易所代码。"""
        try:
            if option:
                for key in ("ExchangeID", "exchange_id", "ExchangeId", "exchange", "Exchange"):
                    val = option.get(key)
                    if val:
                        return self._normalize_exchange_code(str(val))
        except Exception:
            pass
        return self._normalize_exchange_code(fallback)

    def _get_option_type(self, option_symbol: str, option_dict: Optional[Dict[str, Any]] = None, exchange: str = "") -> Optional[str]:
        """获取期权类型（C=看涨，P=看跌）带缓存，支持多种格式"""
        try:
            # 检查缓存 (self.option_type_cache initialized in Strategy __init__ or Mixin)
            # Mixin might not have it initialized, so check hasattr
            if not hasattr(self, "option_type_cache"):
                self.option_type_cache = {}
            
            if option_symbol in self.option_type_cache:
                return self.option_type_cache[option_symbol]
            
            # 优先使用合约字典中的字段
            if option_dict:
                for key in ["OptionType", "OptionsType"]:
                    if option_dict.get(key):
                        val = str(option_dict[key]).upper().strip()
                        if val == '1' or val in ("CALL", "购", "认购", "C"):
                            self.option_type_cache[option_symbol] = "C"
                            return "C"
                        elif val == '2' or val in ("PUT", "沽", "认沽", "P"):
                            self.option_type_cache[option_symbol] = "P"
                            return "P"

            option_symbol_upper = option_symbol.upper()
            
            if "-C-" in option_symbol_upper or "_C_" in option_symbol_upper:
                self.option_type_cache[option_symbol] = "C"
                return "C"
            if "-P-" in option_symbol_upper or "_P_" in option_symbol_upper:
                self.option_type_cache[option_symbol] = "P"
                return "P"

            match = re.search(r"^[A-Z]{1,2}\d{3,4}([CP])\d+$", option_symbol_upper)
            if match:
                self.option_type_cache[option_symbol] = match.group(1)
                return match.group(1)

            match = re.search(r"[\d-]([CP])\d+", option_symbol_upper)
            if match:
                self.option_type_cache[option_symbol] = match.group(1)
                return match.group(1)
                
            return None
        except Exception:
            return None

    def _get_next_month_id(self, future_id: str) -> Optional[str]:
        """只从参数表读取指定下月；无配置直接返回 None"""
        try:
            future_clean = self._normalize_future_id(future_id)
            product_code = self._extract_product_code(future_clean).upper()
            if not product_code:
                product_code = self._extract_product_code(future_id or "").upper()

            eff_map = self._get_effective_month_mapping()
            candidate: Optional[str] = None
            lst = eff_map.get(product_code)
            if not lst:
                try:
                    f2o = getattr(self, "future_to_option_map", {}) or {}
                    for fut_code, opt_code in f2o.items():
                        if str(opt_code).strip().upper() == product_code:
                            lst = eff_map.get(str(fut_code).strip().upper())
                            if lst:
                                break
                except Exception:
                    pass
            if isinstance(lst, list) and len(lst) >= 2:
                candidate = self._normalize_future_id((lst[1] or "").strip())
                if candidate:
                    return candidate
            return None
        except Exception:
            return None

    def _log_option_month_pair_coverage(self) -> None:
        """检查指定月/指定下月期权分组是否齐全，输出缺失品种。"""
        try:
            eff_map = self._get_effective_month_mapping()
            if not eff_map:
                return
            missing = []
            for prod, pair in eff_map.items():
                if not isinstance(pair, list) or len(pair) < 2:
                    continue
                cm = self._normalize_future_id(pair[0])
                nm = self._normalize_future_id(pair[1])
                cm_ok = cm in self.option_instruments and bool(self.option_instruments.get(cm))
                nm_ok = nm in self.option_instruments and bool(self.option_instruments.get(nm))
                if not cm_ok or not nm_ok:
                    missing.append((prod, cm if not cm_ok else "", nm if not nm_ok else ""))
            if missing:
                self.output(f"[警告] 期权分组缺失(指定月/指定下月): {missing}")
        except Exception:
            pass

    def _get_commodity_option_exchange(self, product_code: str) -> Optional[str]:
        """根据品种代码获取商品期权对应的交易所"""
        PRODUCT_EXCHANGE_MAP = {
            "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE",
            "NI": "SHFE", "SN": "SHFE", "PB": "SHFE",
            "RB": "SHFE", "HC": "SHFE", "WR": "SHFE", "SS": "SHFE",
            "AU": "SHFE", "AG": "SHFE",
            "FU": "SHFE", "BU": "SHFE", "RU": "SHFE", "SP": "SHFE",
            "SC": "INE", "LU": "INE", "NR": "INE", "BC": "INE", "BR": "INE",
            "M": "DCE", "Y": "DCE", "A": "DCE", "P": "DCE",
            "C": "DCE", "CS": "DCE", "JD": "DCE", "L": "DCE",
            "V": "DCE", "PP": "DCE", "EB": "DCE", "EG": "DCE",
            "PG": "DCE", "LH": "DCE", "FB": "DCE", "BB": "DCE",
            "J": "DCE", "JM": "DCE", "I": "DCE",
            "MA": "CZCE", "FG": "CZCE", "TA": "CZCE",
            "CF": "CZCE", "SR": "CZCE", "OI": "CZCE", "RM": "CZCE",
            "ZC": "CZCE", "WH": "CZCE", "PM": "CZCE", "RI": "CZCE",
            "JR": "CZCE", "LR": "CZCE", "SF": "CZCE", "SM": "CZCE",
            "UR": "CZCE", "SA": "CZCE", "PK": "CZCE", "AP": "CZCE",
            "CY": "CZCE", "CJ": "CZCE", "PF": "CZCE",
        }
        exch = PRODUCT_EXCHANGE_MAP.get(product_code.upper())
        if exch:
            return exch
        try:
            prod = str(product_code).upper()
            if prod in self.future_symbol_to_exchange:
                return self.future_symbol_to_exchange.get(prod)
            for sym, ex in self.future_symbol_to_exchange.items():
                if str(sym).upper().startswith(prod):
                    return ex
        except Exception:
            pass
        return None

    def _is_commodity_option(self, product_code: str) -> bool:
        """判断是否为商品期权（非股指期权）"""
        index_options = {"IO", "HO", "MO", "EO"}
        try:
            extra = getattr(self.params, "index_option_prefixes", None)
            if extra:
                index_options = index_options.union({str(x).upper() for x in extra})
        except Exception:
            pass
        return product_code.upper() not in index_options

    def _has_option_for_product(self, product_code: str) -> bool:
        """检测该期货品种是否存在已加载的期权分组"""
        try:
            prefix = str(product_code).upper()
            for k, v in self.option_instruments.items():
                try:
                    if str(k).upper().startswith(prefix) and v:
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    def _is_index_option(self, product_code: str) -> bool:
        """判断是否为股指期权"""
        index_options = {"IO", "HO", "MO", "EO"}
        try:
            extra = getattr(self.params, "index_option_prefixes", None)
            if extra:
                index_options = index_options.union({str(x).upper() for x in extra})
        except Exception:
            pass
        return product_code.upper() in index_options

    def _build_option_groups_by_option_prefix(self) -> Dict[str, List[Dict[str, Any]]]:
        """按期权品种前缀+年月构建分组（如 IO2601、HO2601、M2505）。"""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        try:
            allowed_ex = {"CFFEX"}
            try:
                extra_ex = getattr(self.params, "option_group_exchanges", None)
                if extra_ex:
                    allowed_ex = {str(x).upper() for x in extra_ex}
                else:
                    allowed_ex = {"CFFEX", "INE"}
            except Exception:
                allowed_ex = {"CFFEX", "INE"}

            for _, options in self.option_instruments.items():
                for opt in options:
                    exchange = str(opt.get("ExchangeID", "")).strip().upper()
                    if exchange not in allowed_ex:
                        continue
                    oid = str(opt.get("InstrumentID", "")).upper()
                    m = re.search(r"([A-Za-z]{1,})(\d{2})(\d{1,2})", oid)
                    if not m:
                        m2 = re.search(r"([A-Za-z]{1,})(\d)(\d{2})", oid)
                        if not m2:
                            continue
                        prefix = m2.group(1).upper()
                        yy = m2.group(2)
                        mm = m2.group(3)
                        group_id = f"{prefix}{yy}{mm}"
                    else:
                        prefix = m.group(1).upper()
                        yy = m.group(2)
                        mm = m.group(3)
                        group_id = f"{prefix}{yy}{mm}"
                    groups.setdefault(group_id, []).append(opt)
        except Exception:
            pass
        return groups

    def _build_shfe_option_groups(self) -> Dict[str, List[Dict[str, Any]]]:
        """按SHFE期权品种前缀+年月构建分组（如 CU2601、RB2601）。"""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        try:
            shfe_options_count = 0
            for _, options in self.option_instruments.items():
                for opt in options:
                    exchange = str(opt.get("ExchangeID", "")).strip().upper()
                    if exchange != "SHFE":
                        continue
                    shfe_options_count += 1
                    oid = str(opt.get("InstrumentID", "")).upper()
                    m = re.search(r"([A-Za-z]{1,2})(\d{2})(\d{1,2})", oid)
                    if m:
                        prefix = m.group(1).upper()
                        yy = m.group(2)
                        mm = m.group(3)
                        group_id = f"{prefix}{yy}{mm}"
                        groups.setdefault(group_id, []).append(opt)
            # self._debug(f"[SHFE期权分组] 共处理{shfe_options_count}个SHFE期权，生成{len(groups)}个分组")
        except Exception:
            pass
        return groups

    def _build_dce_option_groups(self) -> Dict[str, List[Dict[str, Any]]]:
        """按DCE期权品种前缀+年月构建分组（如 M2601、Y2601）。"""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        try:
            dce_options_count = 0
            for _, options in self.option_instruments.items():
                for opt in options:
                    exchange = str(opt.get("ExchangeID", "")).strip().upper()
                    if exchange != "DCE":
                        continue
                    dce_options_count += 1
                    oid = str(opt.get("InstrumentID", "")).upper()
                    m = re.search(r"([A-Za-z]{1,2})(\d{2})(\d{1,2})", oid)
                    if m:
                        prefix = m.group(1).upper()
                        yy = m.group(2)
                        mm = m.group(3)
                        group_id = f"{prefix}{yy}{mm}"
                        groups.setdefault(group_id, []).append(opt)
            # self._debug(f"[DCE期权分组] 共处理{dce_options_count}个DCE期权，生成{len(groups)}个分组")
        except Exception:
            pass
        return groups

    def _build_czce_option_groups(self) -> Dict[str, List[Dict[str, Any]]]:
        """按CZCE期权品种前缀+年月构建分组（如 SR509、CF509）。"""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        try:
            czce_options_count = 0
            for _, options in self.option_instruments.items():
                for opt in options:
                    exchange = str(opt.get("ExchangeID", "")).strip().upper()
                    if exchange != "CZCE":
                        continue
                    czce_options_count += 1
                    oid = str(opt.get("InstrumentID", "")).upper()
                    m = re.search(r"([A-Za-z]{1,2})(\d{1})(\d{2})$", oid)
                    if m:
                        prefix = m.group(1).upper()
                        yy = m.group(2)
                        mm = m.group(3)
                        group_id = f"{prefix}{yy}{mm}"
                        groups.setdefault(group_id, []).append(opt)
            self._debug(f"[CZCE期权分组] 共处理{czce_options_count}个CZCE期权，生成{len(groups)}个分组")
        except Exception:
            pass
        return groups

    def _safe_option_dict(self, option: Any) -> Optional[Dict[str, Any]]:
        """将期权对象安全转换为 dict。"""
        try:
            if isinstance(option, dict):
                return option
            return self._to_instrument_dict(option)
        except Exception:
            return None

    def _safe_option_id(self, option: Dict[str, Any]) -> str:
        """安全提取期权合约ID，兼容不同字段名。"""
        try:
            if not option:
                return ""
            for key in ("InstrumentID", "instrument_id", "InstrumentId", "symbol", "Symbol"):
                val = option.get(key)
                if val:
                    return str(val).strip().upper()
        except Exception:
            pass
        return ""

    def _safe_option_exchange(self, option: Dict[str, Any], fallback: str = "") -> str:
        """安全提取期权交易所代码。"""
        try:
            if option:
                for key in ("ExchangeID", "exchange_id", "ExchangeId", "exchange", "Exchange"):
                    val = option.get(key)
                    if val:
                        return self._normalize_exchange_code(str(val))
        except Exception:
            pass
        return self._normalize_exchange_code(fallback)

    def _to_czce_single_year(self, code: str) -> Optional[str]:
        """将两位年格式转换为CZCE一位年格式（如 SR2601 -> SR601）。"""
        try:
            s = self._normalize_future_id(code)
            m = re.match(r"^([A-Z]{1,})(\d{1,2})(\d{1,2})$", s)
            if not m:
                return None
            prefix = m.group(1)
            yy = m.group(2)
            mm = m.group(3)
            if len(yy) == 1:
                return f"{prefix}{yy}{str(mm).zfill(2)}"
            return f"{prefix}{yy[-1]}{str(mm).zfill(2)}"
        except Exception:
            return None

    def _get_option_group_options(self, group_key: str) -> List[Dict[str, Any]]:
        """按分组键获取期权列表，兼容大小写/空格/映射/CZCE格式。"""
        try:
            if not isinstance(self.option_instruments, dict):
                return []
            key_raw = (group_key or "").strip().upper()
            if not key_raw:
                return []
            opts = self.option_instruments.get(key_raw)
            if opts:
                return list(opts)
            key_norm = self._normalize_future_id(key_raw)
            opts = self.option_instruments.get(key_norm)
            if opts:
                return list(opts)
            prefix_map = {"IO": "IF", "HO": "IH", "MO": "IM"}
            m = re.match(r"^([A-Z]+)(\d{2})(\d{1,2})$", key_norm)
            if m:
                prefix = m.group(1)
                mapped = prefix_map.get(prefix)
                if mapped:
                    alt = f"{mapped}{m.group(2)}{m.group(3)}"
                    opts = self.option_instruments.get(alt)
                    if opts:
                        return list(opts)
            czce_key = self._to_czce_single_year(key_norm)
            if czce_key:
                opts = self.option_instruments.get(czce_key)
                if opts:
                    return list(opts)
            try:
                groups_all = self._build_option_groups_by_option_prefix()
                opts = groups_all.get(key_raw) or groups_all.get(key_norm) or []
                return list(opts)
            except Exception:
                return []
        except Exception:
            return []

    def _cleanup_kline_cache_for_symbol(self, instrument_id: str) -> None:
        """清理某合约的K线缓存，避免无期权品种持续占用内存"""
        try:
            inst_upper = self._normalize_future_id(instrument_id)
            if not inst_upper:
                return
            keys_to_remove = []
            for key in list(self.kline_data.keys()):
                try:
                    if key.endswith(f"_{inst_upper}"):
                        keys_to_remove.append(key)
                except Exception:
                    continue
            for key in keys_to_remove:
                try:
                    del self.kline_data[key]
                except Exception:
                    pass
        except Exception:
            pass

    def _is_instrument_allowed(self, instrument_id: str, exchange: str = "") -> bool:
        """判断某合约是否允许进入主逻辑（需要合约已加载、在指定月、且有期权链）。"""
        try:
            if not instrument_id:
                return False
            if not getattr(self, "data_loaded", False) or not getattr(self, "_instruments_ready", False):
                return False
            inst_upper = self._normalize_future_id(instrument_id)
            if not inst_upper:
                return False

            if inst_upper in self.future_symbol_to_exchange or self._is_real_month_contract(inst_upper):
                if not self._is_symbol_current(inst_upper):
                    return False
                prod = self._extract_product_code(inst_upper)
                if prod and not self._has_option_for_product(prod):
                    return False
                return True

            fut_symbol = self._extract_future_symbol(inst_upper)
            if fut_symbol:
                if not self._is_symbol_current(fut_symbol):
                    return False
                prod = self._extract_product_code(fut_symbol)
                if prod and not self._has_option_for_product(prod):
                    return False
                return True

            return False
        except Exception:
            return False

    def _is_instrument_allowed_for_kline(self, instrument_id: str, exchange: str = "") -> bool:
        """判断某合约是否允许用于K线合成（允许指定月/指定下月）。"""
        try:
            if not instrument_id:
                return False
            if not getattr(self, "data_loaded", False) or not getattr(self, "_instruments_ready", False):
                return False
            inst_upper = self._normalize_future_id(instrument_id)
            if not inst_upper:
                return False

            if inst_upper in self.future_symbol_to_exchange or self._is_real_month_contract(inst_upper):
                if not self._is_symbol_specified_or_next(inst_upper):
                    return False
                prod = self._extract_product_code(inst_upper)
                if prod and not self._has_option_for_product(prod):
                    return False
                return True

            fut_symbol = self._extract_future_symbol(inst_upper)
            if fut_symbol:
                if not self._is_symbol_specified_or_next(fut_symbol):
                    return False
                prod = self._extract_product_code(fut_symbol)
                if prod and not self._has_option_for_product(prod):
                    return False
                return True

            return False
        except Exception:
            return False

    def _should_forward_to_position_manager(self, instrument_id: str, exchange: str = "") -> bool:
        """防止回报/行情通过后门进入平仓管理器（无期权链则阻断）。"""
        try:
            return self._is_instrument_allowed(instrument_id, exchange)
        except Exception:
            return False

    def _validate_option_availability(self) -> None:
        """数据预处理：校验期货合约是否有对应的期权数据，并标记无数据的合约以便跳过计算"""
        try:
            self.futures_without_options.clear()
            futures_source = []
            if hasattr(self, "future_instruments") and isinstance(self.future_instruments, list):
                futures_source.extend(self.future_instruments)
            if hasattr(self, "instruments") and isinstance(self.instruments, dict):
                for _, v in self.instruments.items():
                    if v.get("ProductClass") in ("1", "i", "I"):
                        futures_source.append(v)
            if not futures_source:
                return
            checked_ids = set()
            for info in futures_source:
                fid_raw = info.get("InstrumentID")
                if not fid_raw or fid_raw in checked_ids:
                    continue
                checked_ids.add(fid_raw)
                if info.get("product_type") == "option" or info.get("ProductClass") in ("2", "h", "H"):
                    continue
                fid_norm = self._normalize_future_id(fid_raw)
                if not fid_norm:
                    continue
                options_found = False
                if fid_norm in self.option_instruments and self.option_instruments[fid_norm]:
                    options_found = True
                if not options_found:
                    self.futures_without_options.add(fid_norm)
                    self._debug(f"[数据预检查] 期货 {fid_norm} 未找到对应的期权数据，将在宽度计算中间跳过")
            if self.futures_without_options:
                self.output(f"[预处理] 共发现 {len(self.futures_without_options)} 个期货合约无期权数据，已标记跳过。")
        except Exception as e:
            self.output(f"[预处理] 校验期权数据可用性出错: {e}")

    def load_all_instruments(self) -> None:
        """加载所有期货和期权合约"""
        self.output("[调试] load_all_instruments() 方法入口", force=True)
        try:
            # [CRITICAL FIX] 确保 month_mapping 始终有默认值
            # Refactored Strategy lacks the __init__ injection, so we do it here (Just-in-Time)
            if not getattr(self.params, "month_mapping", None):
                self.output("[Info] month_mapping 为空，应用 DEFAULT_MONTH_MAPPING")
                self.params.month_mapping = DEFAULT_MONTH_MAPPING.copy()

            self._option_fetch_failures = set()
            PRODUCT_EXCHANGE_MAP = {
                "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX",
                "IO": "CFFEX", "HO": "CFFEX", "MO": "CFFEX", "EO": "CFFEX",
                "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE",
                "NI": "SHFE", "SN": "SHFE", "PB": "SHFE",
                "RB": "SHFE", "HC": "SHFE", "WR": "SHFE", "SS": "SHFE",
                "AU": "SHFE", "AG": "SHFE",
                "FU": "SHFE", "BU": "SHFE", "RU": "SHFE", "SP": "SHFE",
                "M": "DCE", "Y": "DCE", "A": "DCE", "P": "DCE",
                "C": "DCE", "CS": "DCE", "JD": "DCE", "L": "DCE",
                "V": "DCE", "PP": "DCE", "EB": "DCE", "EG": "DCE",
                "PG": "DCE", "LH": "DCE", "FB": "DCE", "BB": "DCE",
                "J": "DCE", "JM": "DCE", "I": "DCE",
                "MA": "CZCE", "FG": "CZCE", "TA": "CZCE",
                "CF": "CZCE", "SR": "CZCE", "OI": "CZCE", "RM": "CZCE",
                "ZC": "CZCE", "WH": "CZCE", "PM": "CZCE", "RI": "CZCE",
                "JR": "CZCE", "LR": "CZCE", "SF": "CZCE", "SM": "CZCE",
                "UR": "CZCE", "SA": "CZCE", "PK": "CZCE", "AP": "CZCE",
                "CY": "CZCE", "CJ": "CZCE", "PF": "CZCE",
            }

            exchanges_str = getattr(self.params, "exchanges", "CFFEX") or "CFFEX"
            future_products_str = str(getattr(self.params, "future_products", "") or "")
            option_products_str = str(getattr(self.params, "option_products", "") or "")
            include_fut_as_opt = bool(getattr(self.params, "include_future_products_for_options", True))
            load_all = getattr(self.params, "load_all_products", True)
            self.output(f"=== load_all = {load_all} ===")

            exchanges = [self._normalize_exchange_code(e) for e in exchanges_str.split(",") if e.strip()]
            future_products = [p.strip() for p in future_products_str.split(",") if p.strip()]
            option_products = [p.strip() for p in option_products_str.split(",") if p.strip()]

            # [UI_RESTORE] Removed no_option_products filter to match Strategy20260105_3.py behavior (Cover Everything)

            if not future_products and not option_products:
                try:
                    mm = getattr(self.params, "month_mapping", {}) or {}
                    mm_keys = [str(k).strip().upper() for k in mm.keys() if str(k).strip()]
                    if mm_keys:
                        future_products = list(dict.fromkeys(mm_keys))
                        if include_fut_as_opt:
                            option_products = list(dict.fromkeys(option_products + future_products))
                except Exception:
                    pass
            if include_fut_as_opt:
                option_products = list(dict.fromkeys(option_products + future_products))
            try:
                mapping = getattr(self, "future_to_option_map", {}) or {}
                mapped_from_future = []
                for fp in future_products:
                    k = str(fp).strip().upper()
                    if not k:
                        continue
                    mapped = mapping.get(k) or k.lower()
                    if mapped and mapped not in option_products and mapped not in mapped_from_future:
                        mapped_from_future.append(mapped)
                effective_option_products = list(dict.fromkeys(option_products + mapped_from_future))
            except Exception:
                effective_option_products = option_products

            # [强制筛选] 始终仅拉取指定月/下月对应的期权品种
            try:
                eff_map = self._get_effective_month_mapping()
                allowed_prod = {str(k).upper() for k in eff_map.keys()} if isinstance(eff_map, dict) else set()

                # 若映射为空，尝试从指定月/下月直接推导品种
                if not allowed_prod:
                    sm = (getattr(self.params, "specified_month", "") or "").strip().upper()
                    nm = (getattr(self.params, "next_specified_month", "") or "").strip().upper()
                    for sym in (sm, nm):
                        if sym:
                            prod = self._extract_product_code(sym)
                            if prod:
                                allowed_prod.add(str(prod).upper())

                allowed_opt = set()
                for prod in allowed_prod:
                    try:
                        opt_prefix = self._map_future_to_option_prefix(prod)
                        if opt_prefix:
                            allowed_opt.add(str(opt_prefix).upper())
                    except Exception:
                        pass
                    allowed_opt.add(str(prod).upper())

                before_opts = list(effective_option_products)
                if allowed_opt:
                    effective_option_products = [p for p in effective_option_products if str(p).upper() in allowed_opt]
                else:
                    effective_option_products = []
                self._debug(f"[筛选] 期权品种仅保留指定月/下月对应品种: {before_opts} -> {effective_option_products}")
            except Exception:
                pass

            default_exchange = exchanges[0] if exchanges else "CFFEX"

            self._debug("=== 开始加载合约 ===")
            self._debug(f"交易所列表: {exchanges}")
            self._debug(f"期货品种列表: {future_products}")
            self._debug(f"期权品种列表: {effective_option_products} (include_future_products_for_options={include_fut_as_opt})")
            self._debug(f"load_all_products: {load_all}")

            fetched: List[dict] = []

            def _collect(res: Any, label: str) -> None:
                if res is None:
                    self._debug(f"{label} 返回 None")
                    return
                norm = self._normalize_instruments(res)
                self._debug(f"{label} 原始数量: {len(res) if hasattr(res, '__len__') else '未知'} 归一化后: {len(norm)}")
                if norm:
                    filtered_norm = []
                    expect_option = "期权" in label
                    for inst in norm:
                        product_class = str(inst.get("ProductClass", "") or "").strip()
                        inst_id = inst.get("InstrumentID", "")
                        normalized_id = str(inst_id or "").upper()

                        if expect_option:
                            is_option_like = (
                                product_class in ("2", "h", "H")
                                or inst.get("OptionType") not in (None, "")
                                or inst.get("StrikePrice") not in (None, "")
                                or "-C" in normalized_id
                                or "-P" in normalized_id
                            )
                            if not is_option_like:
                                key_skip = f"{label}:{inst_id}"
                                if key_skip not in self._non_option_return_logged:
                                    self._non_option_return_logged.add(key_skip)
                                    self._debug(f"过滤非期权返回: {inst_id} (label={label})")
                                continue

                        handled = False

                        if product_class in ("2", "h", "H"):
                            handled = True
                            future_symbol = self._extract_future_symbol(inst_id)
                            if future_symbol and self._is_symbol_specified_or_next(future_symbol.upper()):
                                filtered_norm.append(inst)
                            else:
                                # 仅订阅指定月/下月时，非目标期权直接静默跳过以节省资源
                                if getattr(self.params, "subscribe_only_specified_month_options", False) or \
                                   getattr(self.params, "subscribe_only_current_next_options", False):
                                    continue
                                self._debug(f"过滤期权(非指定月/指定下月或解析失败): {inst_id}")
                            continue

                        if product_class in ("1", "i", "I"):
                            handled = True
                            if self._is_symbol_specified_or_next(normalized_id):
                                filtered_norm.append(inst)
                            else:
                                # 仅订阅指定月/下月时，非目标期货静默跳过以节省资源
                                if getattr(self.params, "subscribe_only_specified_month_futures", False) or \
                                   getattr(self.params, "subscribe_only_current_next_futures", False):
                                    continue
                                self._debug(f"过滤期货(非指定月/指定下月): {inst_id}")
                            continue

                        option_hint = (
                            inst.get("OptionType") not in (None, "")
                            or inst.get("StrikePrice") not in (None, "")
                            or "-C" in normalized_id
                            or "-P" in normalized_id
                        )
                        if not handled and option_hint:
                            handled = True
                            future_symbol = self._extract_future_symbol(inst_id)
                            if future_symbol and self._is_symbol_specified_or_next(future_symbol.upper()):
                                filtered_norm.append(inst)
                            else:
                                if getattr(self.params, "subscribe_only_specified_month_options", False) or \
                                   getattr(self.params, "subscribe_only_current_next_options", False):
                                    continue
                                self._debug(f"过滤期权(推断类型) {inst_id}: 对应期货不在指定月/指定下月或无法解析")
                            continue

                        if not handled and self._is_real_month_contract(normalized_id):
                            handled = True
                            if self._is_symbol_specified_or_next(normalized_id):
                                filtered_norm.append(inst)
                            else:
                                if getattr(self.params, "subscribe_only_specified_month_futures", False) or \
                                   getattr(self.params, "subscribe_only_current_next_futures", False):
                                    continue
                                self._debug(f"过滤期货(推断类型) {inst_id}: 非指定月/指定下月")
                            continue

                        if not handled:
                            self._debug(f"过滤未知类型合约: {inst_id} ProductClass={product_class}")

                    fetched.extend(filtered_norm)
                    self._debug(f"{label} 归一化后已添加到fetched，当前fetched数量: {len(fetched)}")
                else:
                    self._debug(f"{label} 归一化后为空")

            def _fetch_by_product(exchange_code: str, product_code: str, category: str) -> bool:
                """按品种获取合约，若infini 返回空则回退到MarketCenter"""
                primary_res: Any = None
                try:
                    primary_res = infini.get_instruments_by_product(exchange=exchange_code, product_id=product_code)
                    if product_code == "CU" or (primary_res and len(primary_res) > 0 and str(exchange_code) == "SHFE"):
                         debug_ids = [str(x.get("InstrumentID")) for x in primary_res[:5]] if primary_res else []
                         self.output(f"[DEBUG_DIAGNOSE] infini.get_instruments_by_product('{exchange_code}', '{product_code}') -> len={len(primary_res) if primary_res else 0}, IDs={debug_ids}")
                    _collect(primary_res, f"获取{category} {exchange_code}.{product_code}")
                except Exception as e:
                    self._debug(f"获取{category} {exchange_code}.{product_code} 失败: {e}")

                if primary_res:
                    return True

                mc_by_prod = None
                if getattr(self, "market_center", None):
                    mc_by_prod = getattr(self.market_center, "get_instruments_by_product", None)
                
                if callable(mc_by_prod):
                    try:
                        mc_res = mc_by_prod(exchange=exchange_code, product_id=product_code)
                        _collect(mc_res, f"MarketCenter 获取{category} {exchange_code}.{product_code}")
                        if mc_res:
                            self.output(f"{category} {exchange_code}.{product_code} infini 返回空，已用 MarketCenter 兜底 {len(mc_res)} 个")
                            return True
                    except Exception as e:
                        self._debug(f"MarketCenter 获取{category} {exchange_code}.{product_code} 失败: {e}")

                mc_get_all = None
                if getattr(self, "market_center", None):
                    mc_get_all = getattr(self.market_center, "get_instruments", None)

                if callable(mc_get_all):
                    try:
                        mc_all = mc_get_all(exchange=exchange_code)
                        filtered = []
                        for inst in self._normalize_instruments(mc_all):
                            inst_id = str(inst.get("InstrumentID", "")).upper()
                            if inst_id.startswith(product_code.upper()):
                                filtered.append(inst)
                        _collect(filtered, f"MarketCenter 过滤{category} {exchange_code}.{product_code}")
                        if filtered:
                            self.output(f"{category} {exchange_code}.{product_code} 使用 MarketCenter.get_instruments 过滤获得 {len(filtered)} 个")
                            return True
                    except Exception as e:
                        self._debug(f"MarketCenter 过滤{category} {exchange_code}.{product_code} 失败: {e}")

                self._debug(f"{category} {exchange_code}.{product_code} 未获取到合约，已尝试 infini 和MarketCenter")
                return False

            if load_all:
                self._debug("load_all_products=True，尝试获取全部合约..")
                try:
                    get_all = getattr(infini, "get_instruments", None)
                    if callable(get_all):
                        try:
                            res = get_all(exchange=None)
                            self._debug("infini.get_instruments(exchange=None) 调用成功")
                        except TypeError:
                            res = get_all()
                            self._debug("infini.get_instruments() 调用成功")
                        _collect(res, "infini.get_instruments(全部交易所)")
                except Exception as e:
                    self.output(f"获取全部合约失败: {e}\n{traceback.format_exc()}")

                if not fetched:
                    self._debug("infini.get_instruments 未获取到合约，尝试使用MarketCenter...")
                    try:
                        mc_get_all = None
                        if getattr(self, "market_center", None):
                            mc_get_all = getattr(self.market_center, "get_instruments", None)
                        
                        if callable(mc_get_all):
                            try:
                                res = mc_get_all(exchange=None)
                                self._debug("MarketCenter.get_instruments(exchange=None) 调用成功")
                            except TypeError:
                                res = mc_get_all()
                                self._debug("MarketCenter.get_instruments() 调用成功")
                            _collect(res, "MarketCenter.get_instruments(全部交易所)")
                    except Exception as e:
                        self.output(f"MarketCenter 获取全部合约失败: {e}\n{traceback.format_exc()}")

                if fetched:
                    self._debug(f"全量获取成功，共获取 {len(fetched)} 个合约，跳过按品种拉取")
                    try:
                        all_products_for_coverage = list(dict.fromkeys(future_products + effective_option_products))
                        inst_ids_upper = [str(inst.get("InstrumentID", "")).upper() for inst in fetched if isinstance(inst, dict)]
                        missing_products = [p for p in all_products_for_coverage if not any(iid.startswith(p.upper()) for iid in inst_ids_upper)]
                        # 额外策略：对于option_prod，以及日志中显示的缺失品种 (AG, AL, AU, CU, J, RB, ZN)
                        # [Auto-Fix] 覆盖所有商品基线品种
                        target_commodities = {
                            "CU", "RB", "AL", "ZN", "AU", "AG",  # 上期所
                            "M", "Y", "A", "J", "JM", "I",       # 大商所
                            "CF", "SR", "MA", "TA"               # 郑商所
                        }
                        forced_products = [p for p in effective_option_products if p.upper() in target_commodities]
                        self.output(f"[DEBUG] 强制补拉列表计算完成，forced_products count: {len(forced_products)}")
                        products_to_fetch = list(set(missing_products + forced_products))
                        
                        if products_to_fetch:
                            self.output(f"全量获取后，执行补拉/强制刷新品种: {products_to_fetch}")
                            for prod in products_to_fetch:
                                target_ex = PRODUCT_EXCHANGE_MAP.get(prod.upper())
                                if target_ex and target_ex in exchanges:
                                    _fetch_by_product(target_ex, prod, "补拉合约")
                                else:
                                    for exch in exchanges:
                                        _fetch_by_product(exch, prod, "补拉合约")
                    except Exception as e:
                        self._debug(f"全量覆盖检查失败 {e}")
                else:
                    self._debug("全量获取失败或返回空，回退到按品种拉取")

            if not fetched:
                for fut_prod in future_products:
                    target_exchange = PRODUCT_EXCHANGE_MAP.get(fut_prod.upper())
                    if target_exchange:
                        if target_exchange not in exchanges:
                            self._debug(f"品种 {fut_prod} 的交易所 {target_exchange} 不在交易所列表中，跳过")
                            continue
                        _fetch_by_product(target_exchange, fut_prod, "期货合约")
                    else:
                        for exch in exchanges:
                            _fetch_by_product(exch, fut_prod, "期货合约")

            self.output("=== 开始加载期权合约 ===")
            self.output(f"期权品种列表: {effective_option_products}")
            for i, opt_prod in enumerate(effective_option_products):
                self.output(f"=== 开始加载期权品种 {i + 1}/{len(effective_option_products)}: {opt_prod} ===")
                target_exchange = PRODUCT_EXCHANGE_MAP.get(opt_prod.upper())
                self.output(f"  期权品种 {opt_prod} 对应的交易所: {target_exchange}")
                if target_exchange:
                    if target_exchange not in exchanges:
                        self._debug(f"品种 {opt_prod} 的交易所 {target_exchange} 不在交易所列表中，跳过")
                        continue
                    self.output(f"  调用 infini.get_instruments_by_product(exchange={target_exchange}, product_id={opt_prod})")
                    ok = _fetch_by_product(target_exchange, opt_prod, "期权合约")
                    if not ok:
                        self._option_fetch_failures.add(str(opt_prod).upper())
                    self.output(f"  获取 {target_exchange}.{opt_prod} 完成，当前fetched数量: {len(fetched)}")
                else:
                    ok = False
                    for exch in exchanges:
                        ok = _fetch_by_product(exch, opt_prod, "期权合约")
                        if ok:
                            break
                    if not ok:
                        self._option_fetch_failures.add(str(opt_prod).upper())
                self.output(f"=== 期权品种 {opt_prod} 加载完成，当前fetched数量: {len(fetched)} ===")
            self.output("=== 期权合约加载完成 ===")

            all_instruments = fetched or []
            self.output(f"=== 归一化前，fetched: {len(fetched) if fetched else 0} ===")
            try:
                _ = iter(all_instruments)
                self.output("=== 归一化成功，all_instruments 是可迭代的 ===")
            except TypeError:
                all_instruments = []
                self.output("=== 归一化失败，all_instruments 不是可迭代的 ===")

            if not all_instruments:
                if getattr(self.params, "test_mode", False):
                    try:
                        self.output("未获取到任何合约，启用本地演示数据回退（test_mode=True）")
                        now = datetime.now()
                        y2 = str(now.year)[2:]
                        cur_mon = now.month
                        next_mon = 1 if cur_mon == 12 else cur_mon + 1
                        cur_mon_str = f"{cur_mon:02d}"
                        next_mon_str = f"{next_mon:02d}"
                        demo_futures = [
                            ("CFFEX", f"IF{y2}{cur_mon_str}"),
                            ("CFFEX", f"IH{y2}{cur_mon_str}"),
                            ("CFFEX", f"IC{y2}{cur_mon_str}"),
                            ("CFFEX", f"IM{y2}{cur_mon_str}"),
                            ("SHFE", f"CU{y2}{cur_mon_str}"),
                            ("SHFE", f"RB{y2}{cur_mon_str}"),
                            ("SHFE", f"AG{y2}{cur_mon_str}"),
                            ("DCE", f"M{y2}{cur_mon_str}"),
                            ("CZCE", f"SR{y2}{cur_mon_str}"),
                        ]
                        synthetic_futures = []
                        synthetic_options = []
                        for exch, fut in demo_futures:
                            fut_cur = fut
                            fut_next = f"{fut[:-2]}{next_mon_str}"
                            synthetic_futures.append({"ExchangeID": exch, "InstrumentID": fut_cur, "ProductClass": "1"})
                            synthetic_futures.append({"ExchangeID": exch, "InstrumentID": fut_next, "ProductClass": "1"})
                            for fid in (fut_cur, fut_next):
                                key = f"{exch}_{fid}"
                                series = [
                                    self._to_light_kline({"open": 100.0, "high": 101.0, "low": 99.5, "close": 100.0, "volume": 1000}),
                                    self._to_light_kline({"open": 101.0, "high": 102.0, "low": 100.5, "close": 101.0, "volume": 1200}),
                                ]
                                self.kline_data[key] = {"generator": None, "data": series}

                            prefix_map = {"IF": "IO", "IH": "HO", "IC": "MO", "IM": "EO"}
                            prod = re.match(r"^([A-Za-z]{1,})\d{2}\d{1,2}$", fut_cur)
                            prod_code = prod.group(1).upper() if prod else fut_cur[:2].upper()
                            opt_prefix = prefix_map.get(prod_code, prod_code)

                            base_strike_cur = 105.0
                            base_strike_next = 106.0
                            opt_ids_cur = [
                                f"{opt_prefix}{y2}{cur_mon_str}C{int(base_strike_cur)}",
                                f"{opt_prefix}{y2}{cur_mon_str}C{int(base_strike_cur + 2)}",
                            ]
                            opt_ids_next = [
                                f"{opt_prefix}{y2}{next_mon_str}C{int(base_strike_next)}",
                                f"{opt_prefix}{y2}{next_mon_str}C{int(base_strike_next + 2)}",
                            ]

                            for oid in opt_ids_cur:
                                synthetic_options.append({
                                    "ExchangeID": exch,
                                    "InstrumentID": oid,
                                    "ProductClass": "2",
                                    "OptionType": "C",
                                    "StrikePrice": float(re.search(r"C(\d+(?:\.\d+)?)", oid).group(1)),
                                })
                                key = f"{exch}_{oid}"
                                series = [
                                    self._to_light_kline({"open": 5.0, "high": 5.2, "low": 4.9, "close": 5.0, "volume": 500}),
                                    self._to_light_kline({"open": 5.1, "high": 5.4, "low": 5.0, "close": 5.3, "volume": 600}),
                                ]
                                self.kline_data[key] = {"generator": None, "data": series}

                            for oid in opt_ids_next:
                                synthetic_options.append({
                                    "ExchangeID": exch,
                                    "InstrumentID": oid,
                                    "ProductClass": "2",
                                    "OptionType": "C",
                                    "StrikePrice": float(re.search(r"C(\d+(?:\.\d+)?)", oid).group(1)),
                                })
                                key = f"{exch}_{oid}"
                                series = [
                                    self._to_light_kline({"open": 4.0, "high": 4.2, "low": 3.9, "close": 4.0, "volume": 450}),
                                    self._to_light_kline({"open": 4.1, "high": 4.3, "low": 4.0, "close": 4.2, "volume": 520}),
                                ]
                                self.kline_data[key] = {"generator": None, "data": series}

                        all_instruments = synthetic_futures + synthetic_options
                        self.output(f"本地演示数据生成完成：期货{len(synthetic_futures)}，期权{len(synthetic_options)}")
                    except Exception as e:
                        self.output(f"本地演示数据生成失败: {e}")
                else:
                    self.output("未获取到任何合约，请检查exchange/future_product/option_product 设置")
                    self.output("=== 合约数据加载失败 ===")
                    return

            self._debug(f"归一化后合约总数: {len(all_instruments)}")
            self.output(f"=== 归一化后合约总数: {len(all_instruments)} ===")
            self.output("=== 开始打印前10个合约样例 ===")
            self._debug("=== 前10个合约样例 ===")
            for i, inst in enumerate(all_instruments[:10]):
                exchange = inst.get("ExchangeID", "")
                inst_id = inst.get("InstrumentID", "")
                product_class = inst.get("ProductClass", "")
                self._debug(f"  {i + 1}. {exchange}.{inst_id} (ProductClass: {product_class})")
            self.output("=== 打印前10个合约样例完成 ===")

            futures_cnt = 0
            options_cnt = 0
            unknown_cnt = 0
            unknown_samples: List[str] = []
            product_class_counter: Dict[str, int] = {}
            unknown_instruments: List[str] = []

            for instrument in all_instruments:
                if not isinstance(instrument, dict):
                    continue

                product_class = instrument.get("ProductClass", "")
                product_class_counter[product_class] = product_class_counter.get(product_class, 0) + 1
                inst_id = instrument.get("InstrumentID")
                if not inst_id:
                    continue

                if "ExchangeID" not in instrument or not instrument["ExchangeID"]:
                    instrument["ExchangeID"] = instrument.get("Exchange", default_exchange)

                try:
                    instrument["ExchangeID"] = self._normalize_exchange_code(instrument.get("ExchangeID", ""))
                except Exception:
                    pass

                if product_class in ("1", "i", "I"):
                    futures_cnt += 1
                    self.future_instruments.append(instrument)
                    try:
                        sym = inst_id.upper()
                        exch_val = self._normalize_exchange_code(instrument.get("ExchangeID", ""))
                        if sym and exch_val:
                            self.future_symbol_to_exchange[sym] = exch_val
                    except Exception:
                        pass
                elif product_class in ("2", "h", "H"):
                    options_cnt += 1
                    future_symbol = self._extract_future_symbol(inst_id)
                    self._debug(f"[期权加载] 期权合约: {inst_id}, ProductClass: {product_class}, 交易所: {instrument.get('ExchangeID', '')}, 提取的期货代码: {future_symbol}")
                    if future_symbol:
                        if future_symbol not in self.option_instruments:
                            self.option_instruments[future_symbol] = []
                        self.option_instruments[future_symbol].append(instrument)
                        self._debug(f"[期权加载] 期权已添加到option_instruments: {future_symbol}, 交易所: {instrument.get('ExchangeID', '')}")
                    else:
                        self._debug(f"[期权加载] 期权合约无法提取期货代码: {inst_id}, ProductClass: {product_class}, 交易所: {instrument.get('ExchangeID', '')}")

                    exchange = instrument.get("ExchangeID", "")
                    option_id = inst_id
                    if exchange and option_id:
                        key = f"{exchange}_{option_id}"
                        if key not in self.kline_data:
                            self.kline_data[key] = {"generator": None, "data": []}
                else:
                    unknown_cnt += 1
                    if self.params.debug_output and len(unknown_samples) < 5:
                        unknown_samples.append(str(product_class))
                        unknown_instruments.append(str(instrument)[:200])

            self._debug(f"分类统计:期货: {futures_cnt} 期权: {options_cnt} 未识别 {unknown_cnt}")
            self.output(f"=== 分类完成，期货 {futures_cnt} 期权: {options_cnt} 未识别 {unknown_cnt} ===")
            if unknown_samples:
                self._debug(f"未识别ProductClass 样例: {unknown_samples}")
                self._debug(f"未识别合约样例 {unknown_instruments}")
            if product_class_counter:
                self._debug(f"ProductClass 直方图 {product_class_counter}")

            if self.future_instruments:
                self._debug("=== 已加载的期货合约列表 ===")
                for i, future in enumerate(self.future_instruments):
                    exchange = future.get("ExchangeID", "")
                    inst_id = future.get("InstrumentID", "")
                    product_class = future.get("ProductClass", "")
                    self._debug(f"  {i + 1}. {exchange}.{inst_id} (ProductClass: {product_class})")
                self._debug(f"=== 共加载{len(self.future_instruments)} 个期货合约 ===")

            self._normalize_option_group_keys()
            self._log_option_month_pair_coverage()

            try:
                before_groups = len(self.option_instruments)
                self.option_instruments = {k: v for k, v in self.option_instruments.items() if v}
                if len(self.option_instruments) != before_groups:
                    self._debug(f"清理空期权分组: {before_groups} -> {len(self.option_instruments)}")
            except Exception:
                pass

            try:
                filter_specified_futures = self._resolve_subscribe_flag(
                    "subscribe_only_specified_month_futures",
                    "subscribe_only_current_next_futures",
                    False,
                )
                if filter_specified_futures:
                    before_fut = len(self.future_instruments)
                    self.future_instruments = [
                        f for f in self.future_instruments
                        if self._is_symbol_specified_or_next(str(f.get("InstrumentID", "")).upper())
                    ]
                    self._debug(f"过滤期货至指定月/指定下月: {before_fut} 到{len(self.future_instruments)}")
                    allowed = {self._normalize_future_id(str(f.get("InstrumentID", ""))) for f in self.future_instruments}
                    if allowed:
                        before_opt_groups = len(self.option_instruments)
                        self.option_instruments = {k: v for k, v in self.option_instruments.items() if self._normalize_future_id(k) in allowed}
                        self._debug(f"过滤期权分组至指定月/指定下月: {before_opt_groups} 到{len(self.option_instruments)}")
            except Exception as e:
                self._debug(f"指定月/指定下月过滤失败: {e}")

            try:
                if getattr(self.params, "subscribe_options", True):
                    before_fut = len(self.future_instruments)
                    self.future_instruments = [
                        f for f in self.future_instruments
                        if self._has_option_for_product(self._extract_product_code(str(f.get("InstrumentID", ""))))
                    ]
                    if len(self.future_instruments) != before_fut:
                        self._debug(f"剔除无期权链期货: {before_fut} -> {len(self.future_instruments)}")
            except Exception as e:
                self._debug(f"剔除无期权链期货失败: {e}")

            if self._option_fetch_failures:
                self.output(f"[警告] 期权合约加载失败品种: {sorted(self._option_fetch_failures)}")

            self.data_loaded = True
            self.output("=== 合约数据加载完成 ===")

            self._validate_option_availability()

            self._debug("=== 期权映射关系 ===")
            for future_symbol, options in self.option_instruments.items():
                self._debug(f"  {future_symbol}: {len(options)} 个期权")
                if len(options) <= 3:
                    for opt in options:
                        self._debug(f"    - {opt.get('ExchangeID', '')}.{opt.get('InstrumentID', '')}")
            self._debug("=== 期货代码和期权代码对应关系 ===")
            for future in self.future_instruments:
                exchange = future.get("ExchangeID", "")
                future_id = future.get("InstrumentID", "")
                if future_id:
                    future_id_upper = future_id.upper()
                    options = self.option_instruments.get(future_id_upper, [])
                    if options:
                        self._debug(f"  期货: {exchange}.{future_id} -> 期权数量: {len(options)}")
                        for opt in options[:3]:
                            opt_id = opt.get("InstrumentID", "")
                            self._debug(f"    期权: {exchange}.{opt_id}")
            self._debug("=== 各交易所期权统计 ===")
            exchange_stats = {}
            for _, options in self.option_instruments.items():
                for opt in options:
                    exchange = opt.get("ExchangeID", "")
                    if exchange not in exchange_stats:
                        exchange_stats[exchange] = 0
                    exchange_stats[exchange] += 1
            for exchange, count in exchange_stats.items():
                self._debug(f"  {exchange}: {count} 个期权")
        except Exception as e:
            self.output(f"加载合约失败: {e}\n{traceback.format_exc()}")
            self.output("=== 合约数据加载失败 ===")
