# [M1-44-04] 订单流数据分析

from ali2026v3_trading.order.order_flow_data_structures import MicrostructureConfig, FootprintBar, ProductMicroData

# ProductMicroData的高级分析方法（续）



    # ========================================================================

    # 锚定VWAP（带LRU淘汰。'
    # ========================================================================

    def get_anchor_vwap(self, anchor_time: float, initial_price: float = None,

                        initial_volume: float = None) -> float:

        with self._lock:

            # 如果锚点不存在，创建新锚。

            if anchor_time not in self._anchor_vwap:

                # LRU淘汰：如果超过最大数量，删除最旧的锚点

                if len(self._anchor_vwap) >= self._anchor_vwap_maxsize:

                    oldest = self._anchor_vwap_order.pop(0)

                    del self._anchor_vwap[oldest]

                

                if initial_price is not None and initial_volume is not None:

                    self._anchor_vwap[anchor_time] = (initial_price * initial_volume, initial_volume)

                else:

                    self._anchor_vwap[anchor_time] = (0.0, 0.0)

                self._anchor_vwap_order.append(anchor_time)

            

            cum_pv, cum_vol = self._anchor_vwap[anchor_time]

            if cum_vol == 0:

                return 0.0

            return cum_pv / cum_vol



    # ========================================================================

    # 最优执行评估（增加除零保护。

    def evaluate_execution(self, order_price: float, order_volume: float, side: str) -> Dict:

        with self._lock:

            if not self._bids or not self._asks:

                return {'error': 'no_depth'}

                

            try:

                best_bid = self._bids[0][0] if self._bids else 0.0

                best_ask = self._asks[0][0] if self._asks else float('inf')

                

                MIN_PRICE_THRESHOLD = 1e-6  # NP-P2-26: 极小值保存

                if best_bid == 0.0 or best_ask == float('inf') or best_bid < MIN_PRICE_THRESHOLD or best_ask < MIN_PRICE_THRESHOLD:

                    return {'error': 'invalid_depth'}

                    

                mid_price = (best_bid + best_ask) / 2



                # 计算最小0秒的VWAP（作为基准）'
                now = time.time()

                cutoff = now - self.config.vwap_lookback_seconds

                cum_pv = 0.0

                cum_vol = 0.0

                trades_count = 0

                

                for ts, p, v, _ in self._trades:

                    if ts >= cutoff:

                        cum_pv += p * v

                        cum_vol += v

                        trades_count += 1

                        

                # 修复：添加除零保存

                vwap_60 = cum_pv / cum_vol if cum_vol > 0 else mid_price



                # 理想价格：买方用ask，卖方用bid

                if side == 'buy':

                    ideal_price = best_ask

                else:

                    ideal_price = best_bid



                # 计算滑点

                if side == 'buy':

                    slippage = order_price - ideal_price

                else:

                    slippage = ideal_price - order_price

                    

                # 修复：添加除零保存

                slippage_bps = (slippage / ideal_price) * 10000 if ideal_price > 0 else 0



                # 与VWAP比较（增加除零保护）'
                if side == 'buy':

                    vwap_slippage = order_price - vwap_60

                else:

                    vwap_slippage = vwap_60 - order_price

                    

                # 修复：添加除零保存

                vwap_bps = (vwap_slippage / vwap_60) * 10000 if vwap_60 > 0 else 0



                # 效率评级

                if abs(slippage_bps) <= MicrostructureConfig.SLIPPAGE_THRESHOLD_BPS:

                    efficiency = 'excellent'

                elif abs(slippage_bps) <= 2:

                    efficiency = 'good'

                elif abs(slippage_bps) <= 10:

                    efficiency = 'acceptable'

                else:

                    efficiency = 'poor'



                return {

                    'order_price': order_price,

                    'mid_price': mid_price,

                    'vwap_60': vwap_60,

                    'ideal_price': ideal_price,

                    'slippage_bps': round(slippage_bps, 2),

                    'vwap_slippage_bps': round(vwap_bps, 2),

                    'efficiency': efficiency,

                    'timestamp': now,

                    'recent_trades': trades_count

                }

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logger.error(f"Execution evaluation error: {e}")

                return {'error': f'calculation_error: {str(e)}'}



    # ========================================================================

    # 综合评估（修复CVD变化率除零）'
    # ========================================================================

    def get_composite_assessment(self, lookback_seconds: int = 300,

                                 footprint_period: str = '5min',

                                 price_func: Optional[Callable] = None) -> Dict:

        """

        综合评估订单流信号，输出交易建议

        price_func: 可选，用于获取当前价格的函数（如果未提供则内部获取消

        """

        with self._lock:

            score = 0.0

            reasons = []

            details = {}



            try:

                # 1. CVD 背离（_分）'
                cvd_div = self.check_cvd_divergence(lookback_seconds)

                if cvd_div:

                    details['cvd_divergence'] = cvd_div

                    if cvd_div['divergence'] == 'bullish':

                        score += 2.0

                        reasons.append("CVD bullish divergence")

                    elif cvd_div['divergence'] == 'bearish':

                        score -= 2.0

                        reasons.append("CVD bearish divergence")



                # 2. 即时订单流不平衡。1~1，映射到 -1.5 ~ 1.5_

                imb = self.calc_instant_imbalance(depth_levels=5)

                details['instant_imbalance'] = imb

                imb_score = imb * MicrostructureConfig.IMBALANCE_SCORE_MULTIPLIER

                score += imb_score

                reasons.append(f"order_book imbalance={imb:.2f}")



                # 3. 锚定VWAP偏离（锚定最小分钟频

                anchor_time = time.time() - 300

                vwap = self.get_anchor_vwap(anchor_time)

                

                # 获取当前价格

                current_price = 0.0

                if price_func:

                    current_price = price_func(self)

                else:

                    if self._bids and self._asks:

                        current_price = (self._bids[0][0] + self._asks[0][0]) / 2

                    elif len(self._trades) > 0:

                        current_price = self._trades[-1][1]

                        

                if vwap > 0 and current_price > 0:

                    deviation_pct = (current_price - vwap) / vwap * 100

                    details['vwap_deviation_pct'] = deviation_pct

                    if deviation_pct > MicrostructureConfig.DEVIATION_THRESHOLD:

                        score += 1.0

                        reasons.append(f"price above VWAP by {deviation_pct:.2f}%")

                    elif deviation_pct < -MicrostructureConfig.DEVIATION_THRESHOLD:

                        score -= 1.0

                        reasons.append(f"price below VWAP by {abs(deviation_pct):.2f}%")

                    else:

                        reasons.append(f"price near VWAP ({deviation_pct:.2f}%)")



                # 4. Footprint 支撑/阻力识别（最小根K线内成交量密集区。'
                footprint_bars = self.get_footprint_bars(footprint_period)

                if footprint_bars and current_price > 0:

                    vol_profile = {}

                    for bar in footprint_bars[-5:]:

                        for price_int, vol in bar.price_volume.items():

                            vol_profile[price_int] = vol_profile.get(price_int, 0) + vol

                    if vol_profile:

                        poc_price = max(vol_profile, key=vol_profile.get)

                        details['footprint_poc'] = poc_price

                        details['footprint_volume'] = vol_profile[poc_price]

                        if current_price > poc_price:

                            score += MicrostructureConfig.SCORE_INCREMENT

                            reasons.append(f"price above footprint POC={poc_price}")

                        elif current_price < poc_price:

                            score -= MicrostructureConfig.SCORE_INCREMENT

                            reasons.append(f"price below footprint POC={poc_price}")



                # 5. 价格动量（基于CVD变化率，增加epsilon防除零）'
                if len(self._cvd_history) >= 10:

                    recent_data = [(t, c) for t, c in self._cvd_history][-10:]

                    if len(recent_data) >= 2:

                        start_cvd = recent_data[0][1]

                        end_cvd = recent_data[-1][1]

                        # R5-5: 使用safe_divide替代手动epsilon除法保护

                        cvd_change = safe_divide(end_cvd - start_cvd, abs(start_cvd), default=0.0, min_denominator=1e-6)

                        cvd_change = max(-1, min(1, cvd_change))

                        score += cvd_change * 1.0

                        reasons.append(f"CVD change rate={cvd_change:.2f}")



                # 6. 订单流失衡指标OFI（_.5分，基于逐笔成交买方/卖方力量对比。'
                ofi_10s = self.calc_ofi(lookback_seconds=10)

                ofi_60s = self.calc_ofi(lookback_seconds=60)

                details['ofi_10s'] = ofi_10s

                details['ofi_60s'] = ofi_60s

                ofi_weighted = ofi_10s * MicrostructureConfig.OFI_SHORT_TERM_WEIGHT + ofi_60s * MicrostructureConfig.OFI_LONG_TERM_WEIGHT

                ofi_score = ofi_weighted * MicrostructureConfig.IMBALANCE_SCORE_MULTIPLIER

                score += ofi_score

                reasons.append(f"OFI(10s)={ofi_10s:.2f}, OFI(60s)={ofi_60s:.2f}")



                # 限制分数范围 -10..10

                final_score = max(-10.0, min(10.0, score))



                # 信号判定

                if final_score >= 5:

                    signal = "strong_buy"

                    confidence = min(1.0, (final_score - 5) / 5 + MicrostructureConfig.BASE_CONFIDENCE)

                elif final_score >= 2:

                    signal = "buy"

                    confidence = MicrostructureConfig.MODERATE_CONFIDENCE + (final_score - 2) / 3 * MicrostructureConfig.CONFIDENCE_ADJUSTMENT

                elif final_score <= -5:

                    signal = "strong_sell"

                    confidence = min(1.0, (-final_score - 5) / 5 + MicrostructureConfig.BASE_CONFIDENCE)

                elif final_score <= -2:

                    signal = "sell"

                    confidence = MicrostructureConfig.MODERATE_CONFIDENCE + (-final_score - 2) / 3 * MicrostructureConfig.CONFIDENCE_ADJUSTMENT

                else:

                    signal = "neutral"

                    confidence = MicrostructureConfig.BASE_CONFIDENCE



                return {

                    'product': self.product,

                    'score': round(final_score, 2),

                    'signal': signal,

                    'confidence': round(confidence, 2),

                    'details': details,

                    'reason': '; '.join(reasons),

                    'timestamp': time.time()

                }

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logger.error(f"Composite assessment error: {e}")

                return {

                    'product': self.product,

                    'score': 0.0,

                    'signal': 'neutral',

                    'confidence': 0.0,

                    'details': {},

                    'reason': f'calculation_error: {str(e)}',

                    'timestamp': time.time()

                }



    # ========================================================================

    # 快照导出

    # ========================================================================

    def get_snapshot(self) -> Dict:

        with self._lock:

            return {

                'product': self.product,

                'cvd': self.cvd,

                'depth_timestamp': self._depth_timestamp,

                'trade_count': len(self._trades),

                'price_history_len': len(self._price_history),

                'cvd_history_len': len(self._cvd_history),

                'footprint_bars': {k: len(v) for k, v in self._footprint_bars.items()},

                'anchor_vwap_count': len(self._anchor_vwap)

            }

