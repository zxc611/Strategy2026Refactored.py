"""数据验证与预处理。"""
from __future__ import annotations

import re
from typing import Any, List, Optional
from datetime import datetime


class ValidationMixin:
    def validate_environment(self) -> bool:
        """环境自检，确保必要模块可用"""
        try:
            self._ensure_market_center()
            if not getattr(self, "market_center", None):
                self.output("MarketCenter 未初始化，已降级继续运行（部分功能不可用）")
                # 允许在无 MarketCenter 环境继续运行，避免阻断启动/暂停/停止
                return True
            
            # [Added] Pre-Flight Data Check
            if hasattr(self, "option_instruments"):
                opt_count = sum(len(v) for v in self.option_instruments.values())
                self.output(f"[Pre-Check] Loaded {len(self.future_instruments)} Futures, {opt_count} Options across {len(self.option_instruments)} Groups.", force=True)
                if opt_count == 0:
                     self.output("[Pre-Check] WARNING: No options loaded! Width calculation will verify logic but yield nothing.", force=True)
            
            # [Added] Check Parameters
            if hasattr(self, "params"):
                 sm = getattr(self.params, "specified_month", "N/A")
                 nm = getattr(self.params, "next_specified_month", "N/A")
                 self.output(f"[Pre-Check] Params: Specified={sm}, Next={nm}", force=True)

            return True
        except Exception as e:
            self.output(f"环境自检异常: {e}")
            return False

    def _is_instrument_allowed(self, instrument_id: str, exchange: str = "") -> bool:
        """检查合约是否在允许列表中。"""
        try:
            if not instrument_id:
                return False

            base_id = self._normalize_future_id(instrument_id)
            if "_" in base_id:
                base_id = base_id.split("_")[-1]

            if base_id in self.future_symbol_to_exchange:
                return True

            fut = self._extract_future_symbol(base_id)
            if fut and fut in self.future_symbol_to_exchange:
                return True

            return False
        except Exception:
            return False

    def _validate_instruments_and_map(self) -> None:
        """验证并建立映射关系：期权 -> 归属的期货。"""
        try:
            valid_futures = []
            for f in self.future_instruments:
                fid = f.get("InstrumentID")
                eid = f.get("ExchangeID")
                if fid and eid:
                    valid_futures.append(f)
                    base_id = self._normalize_future_id(fid)
                    self.future_symbol_to_exchange[base_id] = eid

            self.future_instruments = valid_futures

            valid_options = {}
            for future_symbol in self.option_instruments:
                opts = self.option_instruments[future_symbol]
                checked_opts = []
                for o in opts:
                    if o.get("InstrumentID") and o.get("ExchangeID"):
                        checked_opts.append(o)
                if checked_opts:
                    valid_options[future_symbol] = checked_opts
            self.option_instruments = valid_options

            self.output(f"合约验证完成: 期货 {len(self.future_instruments)} 个, 期权组 {len(self.option_instruments)} 组")

            self._align_month_mapping_to_loaded_futures()

        except Exception as e:
            self.output(f"合约验证出错: {e}")

    def _normalize_future_id(self, instrument_id: str) -> str:
        """标准化期货合约代码（去除交易所前缀等）"""
        if not instrument_id:
            return ""
        try:
            if "|" in instrument_id:
                return instrument_id.split("|")[-1]
            if "_" in instrument_id:
                parts = instrument_id.split("_")
                if len(parts) == 2 and len(parts[0]) <= 5:
                    return parts[1]
            return instrument_id
        except Exception:
            return str(instrument_id)



    def _extract_product_code(self, symbol: str) -> Optional[str]:
        """提取品种代码（如 ru2501 -> ru）"""
        try:
            if not symbol:
                return None
            m = re.match(r"^([a-zA-Z]+)", symbol)
            if m:
                return m.group(1)
            return None
        except Exception:
            return None

    def _has_option_for_product(self, product_code: str) -> bool:
        """检查该品种是否有对应的期权配置"""
        try:
            if not product_code:
                return False
            norm_prod = product_code.upper()
            self._build_option_product_cache_once()
            return norm_prod in self._option_product_cache
        except Exception:
            return False

    def _build_option_product_cache_once(self) -> None:
        """构建期权品种缓存，避免重复遍历"""
        if hasattr(self, "_option_product_cache") and self._option_product_cache:
            return
        self._option_product_cache = set()
        for fsym in self.option_instruments.keys():
            prod = self._extract_product_code(self._normalize_future_id(fsym))
            if prod:
                self._option_product_cache.add(prod.upper())

    def _normalize_option_group_keys(self) -> None:
        """标准化 option_instruments 的键，确保后续查找匹配"""
        if getattr(self, "_normalized_keys_done", False):
            return
        try:
            new_map = {}
            for k, v in self.option_instruments.items():
                kn = self._normalize_future_id(k)
                if kn:
                    new_map[kn] = v
                    new_map[kn.upper()] = v
                    new_map[k] = v
            self.option_instruments = new_map
            self._normalized_keys_done = True
        except Exception:
            pass

    def _is_real_month_contract(self, instrument_id: str) -> bool:
        """判断是否为具体的月合约（排除主力、指数等连续合约）"""
        try:
            if not instrument_id:
                return False
            fid = self._normalize_future_id(instrument_id)
            if "888" in fid or "999" in fid or "HOT" in fid.upper() or "IDX" in fid.upper():
                return False
            if re.search(r"\d{3,4}$", fid):
                return True
            return False
        except Exception:
            return False

    def _parse_contract_year_month(self, instrument_id: str) -> Optional[int]:
        """解析合约的年份和月份，返回 YYYYMM 整数"""
        try:
            m = re.search(r"(\d{3,4})$", instrument_id)
            if not m:
                return None
            digits = m.group(1)
            current_year = datetime.now().year
            if len(digits) == 4:
                year_part = int(digits[:2]) + 2000
                month_part = int(digits[2:])
                return year_part * 100 + month_part
            elif len(digits) == 3:
                y1 = int(digits[0])
                month_part = int(digits[1:])
                y_candidate = (current_year // 10) * 10 + y1
                if abs(y_candidate - current_year) > 5:
                    if y_candidate < current_year:
                        y_candidate += 10
                    else:
                        y_candidate -= 10
                return y_candidate * 100 + month_part
            return None
        except Exception:
            return None

    def _expand_czce_year_month(self, instrument_id: str) -> str:
        """郑商所三位年份补全为四位（用于API请求）"""
        try:
            if len(instrument_id) > 6 or (not re.search(r"[A-Z]{2}\d{3}$", instrument_id, re.IGNORECASE)):
                return instrument_id
            m = re.match(r"^([A-Z]+)(\d)(\d{2})$", instrument_id, re.IGNORECASE)
            if m:
                prod = m.group(1)
                y1 = int(m.group(2))
                mm = m.group(3)
                curr_y = datetime.now().year
                base_y = (curr_y // 10) * 10 + y1
                if base_y < curr_y - 1:
                    base_y += 10
                return f"{prod}{base_y % 100:02d}{mm}"
            return instrument_id
        except Exception:
            return instrument_id

    def _ensure_market_center(self) -> None:
        """确保 self.market_center 可用。"""
        if hasattr(self, "market_center") and self.market_center:
            return

        try:
            import pythongo

            mc_cls = getattr(pythongo, "MarketCenter", None)
            if mc_cls:
                self.market_center = mc_cls()
                return
        except Exception:
            pass

        try:
            from pythongo.core import MarketCenter

            self.market_center = MarketCenter()
            return
        except Exception:
            pass

        try:
            from pythongo import MarketCenter

            self.market_center = MarketCenter()
            return
        except Exception:
            self.output("严重错误: 无法导入 MarketCenter，行情功能将失效")

