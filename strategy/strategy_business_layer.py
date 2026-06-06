"""
StrategyCoreService 业务层 — 从strategy_core_service.py拆分
职责: 交易执行、订单/持仓服务惰性初始化、生态系统互斥、影子策略推送
"""
from __future__ import annotations

import collections
import logging
import threading
import time
import uuid as _uuid
from typing import Any, Dict, List, Optional

from ali2026v3_trading.strategy.strategy_lifecycle_mixin import StrategyState


class StrategyBusinessLayer:
    def __init__(self, provider):
        self._provider = provider

    # ========== 服务惰性初始化 ==========

    def ensure_order_service(self) -> None:
        provider = self._provider
        if provider._order_service is not None:
            return
        with provider._order_service_init_lock:
            if provider._order_service is not None:
                return
            try:
                from ali2026v3_trading.order.order_service import get_order_service
                provider._order_service = get_order_service()
                _store = getattr(provider, '_state_store', None)
                if _store:
                    _store.set_ref('_order_service', provider._order_service)
                logging.debug("[StrategyBusinessLayer] OrderService initialized (singleton)")
            except Exception as e:
                logging.warning("[StrategyBusinessLayer] Failed to initialize OrderService: %s", e)
                provider._order_service = None

    def ensure_position_service(self) -> None:
        provider = self._provider
        if provider._position_service is not None:
            return
        try:
            from ali2026v3_trading.risk.risk_service import get_risk_service
            from ali2026v3_trading.position.position_service import get_position_service
            scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
            risk_svc = get_risk_service(None, scope_id=scope_id)
            provider._position_service = get_position_service(risk_service=risk_svc, scope_id=scope_id)
            _store = getattr(provider, '_state_store', None)
            if _store:
                _store.set_ref('_position_service', provider._position_service)
            logging.debug("[StrategyBusinessLayer] PositionService initialized (scope=%s)", scope_id)
        except Exception as e:
            logging.warning("[StrategyBusinessLayer] Failed to initialize PositionService: %s", e)
            provider._position_service = None

    def ensure_hft_engine(self) -> None:
        provider = self._provider
        if provider._hft_engine is not None:
            return
        try:
            from ali2026v3_trading.hft_enhancements import get_hft_engine
            provider._hft_engine = get_hft_engine()
            provider._ensure_order_service()
            if provider._order_service:
                provider._order_service.enable_hft_enhancements()
            _store = getattr(provider, '_state_store', None)
            if _store:
                _store.set_ref('_hft_engine', provider._hft_engine)
            logging.debug("[StrategyBusinessLayer] HFT增强引擎已初始化(七大竞争优势-分散架构)")
        except Exception as e:
            logging.warning("[StrategyBusinessLayer] HFT增强引擎初始化失败: %s", e)
            provider._hft_engine = None

    def ensure_snapshot_collector(self, symbol: str = "") -> None:
        """初始化市场快照采集器 — 覆盖18策略(6主策略×3变体)"""
        provider = self._provider
        if provider._snapshot_collector is not None:
            return
        try:
            from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
            sym = symbol or provider.strategy_id
            provider._snapshot_collector = MarketSnapshotCollector(symbol=sym)
            # 注入到信号服务
            if provider._signal_service and hasattr(provider._signal_service, 'set_snapshot_collector'):
                provider._signal_service.set_snapshot_collector(provider._snapshot_collector)
            # 注入到订单服务
            provider._ensure_order_service()
            if provider._order_service and hasattr(provider._order_service, 'set_snapshot_collector'):
                provider._order_service.set_snapshot_collector(provider._snapshot_collector)
            logging.debug("[StrategyBusinessLayer] MarketSnapshotCollector已初始化(18策略覆盖), symbol=%s", sym)
        except Exception as e:
            logging.warning("[StrategyBusinessLayer] MarketSnapshotCollector初始化失败: %s", e)
            provider._snapshot_collector = None

    def get_snapshot_collector(self):
        """获取市场快照采集器（惰性初始化）"""
        provider = self._provider
        if provider._snapshot_collector is None:
            self.ensure_snapshot_collector()
        return provider._snapshot_collector

    # ========== 订单/成交回调 ==========

    def on_order(self, order_data: Any, *args, **kwargs) -> None:
        provider = self._provider
        try:
            oid = getattr(order_data, "order_id", "") or getattr(order_data, "OrderRef", "")
            sts = getattr(order_data, "status", "") or getattr(order_data, "OrderStatus", "")
            logging.info("[StrategyBusinessLayer.on_order] OrderID=%s, Status=%s", oid, sts)
            if provider._order_service:
                provider._order_service.on_trade_update(order_data)
        except Exception as e:
            logging.error("[StrategyBusinessLayer.on_order] Error: %s", e)

    def on_trade(self, trade_data: Any, *args, **kwargs) -> None:
        try:
            self.update_position_from_trade(trade_data)
        except Exception as e:
            logging.error("[StrategyBusinessLayer.on_trade] Error: %s", e)

    def update_position_from_trade(self, trade_data: Any) -> None:
        provider = self._provider
        try:
            trade_id = getattr(trade_data, 'trade_id', None) or (trade_data.get('trade_id') if isinstance(trade_data, dict) else None) or str(id(trade_data))
            if trade_id in provider._processed_trade_ids:
                return
            provider._processed_trade_ids[trade_id] = True
            if len(provider._processed_trade_ids) > provider.MAX_PROCESSED_TRADE_IDS:
                remove_count = len(provider._processed_trade_ids) - int(provider.MAX_PROCESSED_TRADE_IDS * 0.8)
                for _ in range(remove_count):
                    provider._processed_trade_ids.popitem(last=False)
            from ali2026v3_trading.position.position_service import get_position_service
            scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
            pos_svc = get_position_service(scope_id=scope_id)
            pos_svc.on_trade(trade_data)
        except Exception as e:
            # R13-P2-API-12修复: 不再静默吞掉异常，改为warning级别记录
            logging.warning("[StrategyBusinessLayer.update_position_from_trade] 持仓更新失败: %s", e, exc_info=True)

    # ========== 交易周期 ==========

    def execute_option_trading_cycle(self) -> None:
        provider = self._provider
        # R24-P1-CF-02修复: 初始化未完成时拒绝执行交易周期
        if not provider._initialized:
            logging.warning("[StrategyBusinessLayer.execute_option_trading_cycle] R24-P1-CF-02: 初始化未完成，跳过交易周期")
            return
        if not provider._is_running or provider._is_paused:
            return
        if provider._state == StrategyState.DEGRADED:
            logging.info("[OptionTrading] 策略处于DEGRADED状态，跳过交易周期")
            return
        if not provider._trading_lock.acquire(blocking=False):
            logging.info("[OptionTrading] 交易锁被占用，跳过本次周期")
            return
        try:
            # R24-P2-AT-01修复: 添加交易周期耗时记录
            _cycle_start = time.time()
            t_type = getattr(provider, 't_type_service', None)
            if not t_type:
                return
            targets = t_type.select_otm_targets_by_volume()
            if not targets:
                return
            open_reason = provider._resolve_open_reason()
            for t in targets:
                t['open_reason'] = t.get('open_reason', '') or open_reason
            if not provider._check_ecosystem_exclusion(targets):
                logging.info("[OptionTrading] 互斥规则或跨策略风控阻断, 跳过本次周期")
                return
            # ✅ P0-5修复: 交易前风控检查
            # R13-TYP-02修复: signal dict必须包含action字段，否则断路器平仓豁免永远不触发
            # R13-P1-BIZ-06修复: signal_id链路贯通 — signal→risk_check→order→position
            try:
                from ali2026v3_trading.risk.risk_service import get_risk_service
                scope_id = str(getattr(provider, 'strategy_id', '') or 'global')
                rs = get_risk_service(None, scope_id=scope_id)
                passed_targets = []
                for t in targets:
                    _sig_id = t.get('signal_id', '') or f"SIG_{_uuid.uuid4().hex[:12]}"
                    signal = {
                        'symbol': t.get('instrument_id', ''),
                        'direction': t.get('direction', 'BUY'),
                        'price': t.get('price', 0.0),
                        'volume': t.get('volume', 0),
                        'is_valid': True,
                        # R13-P0-API-03修复: action字段默认为OPEN，断路器平仓豁免逻辑依赖此字段
                        'action': t.get('action', '') or 'OPEN',
                        'account_id': t.get('account_id', 'default'),
                        'amount': t.get('amount', 0),
                        'signal_id': _sig_id,  # R13-P1-BIZ-06修复: signal_id贯穿全链路
                    }
                    t['signal_id'] = _sig_id  # R13-P1-BIZ-06修复: 回写到target供后续order使用
                    check_result = rs.check_before_trade(signal)
                    if check_result.is_block:
                        logging.warning("[OptionTrading] 风控阻断: %s %s reason=%s",
                                        t.get('instrument_id', ''), t.get('direction', ''), check_result.reason)
                        continue
                    passed_targets.append(t)
                if not passed_targets:
                    logging.info("[OptionTrading] 所有target均被风控阻断，跳过本次周期")
                    return
                targets = passed_targets
            except Exception as e:
                # R10-P0-11修复: 风控检查异常时fail-safe阻断交易，而非"继续交易"
                # 与P0-R10-10叠加修复：安全元层异常→阻断，外层也必须阻断
                # R10-P2-02: 关键异常日志添加exc_info=True
                logging.error(
                    "[R10-P0-11] 风控检查异常，fail-safe阻断交易: %s", e,
                    exc_info=True,
                )
                return
            # P-30/R7-M-15修复: 健康检查CRITICAL时暂停新开仓（手册12.1节）
            # 双重检查: (1) _health_pause_new_open标志 (2) 实时health查询
            if provider._health_pause_new_open:
                logging.warning("[OptionTrading] _health_pause_new_open=True(健康异常), 暂停新开仓")
                return
            try:
                health = provider.get_health_status()
                if health.get('health') in ('CRITICAL', 'DEGRADED'):
                    provider._health_pause_new_open = True  # R7-M-15修复: 同步设置标志
                    logging.warning("[OptionTrading] 系统健康状态=%s, 暂停新开仓",
                                    health.get('health'))
                    return
            except Exception as e:
                logging.warning("[OptionTrading] 健康检查异常: %s", e)
            provider._ensure_order_service()
            if provider._order_service:
                order_ids = provider._order_service.execute_by_ranking(targets)
                if order_ids:
                    logging.info("[OptionTrading] 下单完成: %d 笔 reason=%s", len(order_ids), open_reason)
            provider._feed_shadow_engine(targets, open_reason)
            # R14-P1-DEAD-08修复: 在决策循环中调用governance checker，手册14节治理检测
            try:
                from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
                _eco = get_strategy_ecosystem()
                if _eco and hasattr(_eco, 'run_governance_checks'):
                    _gov_result = _eco.run_governance_checks()
                    if _gov_result.get('any_triggered', False):
                        logging.warning("[OptionTrading] R14-P1-DEAD-08: 治理检测触发降级: %s",
                                        _gov_result.get('degradation_reasons', []))
            except Exception as _gov_err:
                logging.warning("[R22-EP-P1] governance check跳过(合规检查可能放行违规交易): %s", _gov_err)
            # ✅ P1-9修复: 集成box_spring_strategy的箱体弹簧扫描
            # R13-API-01修复: 传递完整tick参数(high/low/volume/timestamp)，否则箱体检测失效
            # R15-P1-PERF-02修复: 使用全局单例get_box_spring_strategy()，避免绕过单例创建多实例
            try:
                from ali2026v3_trading.strategy.box_spring_strategy import get_box_spring_strategy
                bss = get_box_spring_strategy()
            except ImportError as _bss_ie:
                logging.warning("[P1-FIX] box_spring_strategy导入失败, 箱体弹簧扫描不可用: %s", _bss_ie)
                bss = None
            try:  # R23-P0-FIX: 补充缺失的try块，修复SyntaxError
                if bss is not None:
                    for t in targets:
                        inst_id = t.get('instrument_id', '')
                        if inst_id:
                            # R13-API-01修复: 传递完整tick参数，箱体检测依赖high>0 and low>0条件
                            bss.on_tick(
                                instrument_id=inst_id,
                                price=t.get('price', 0.0),
                                high=t.get('high', 0.0),  # R13-API-01: 补充high参数
                                low=t.get('low', 0.0),    # R13-API-01: 补充low参数
                                volume=t.get('volume', 0),  # R13-API-01: 补充volume参数
                                timestamp=t.get('timestamp'),  # R13-API-01: 补充timestamp参数
                            )
            except Exception as e:
                logging.warning("[R22-EP-P1-17] [StrategyBusinessLayer] box_spring_strategy tick error: %s", e)
        except Exception as e:
            # R10-P2-02: 关键异常日志添加exc_info=True
            logging.error("[StrategyBusinessLayer.execute_option_trading_cycle] Error: %s", e, exc_info=True)
        finally:
            # R24-P2-AT-01修复: 交易周期耗时记录
            _cycle_elapsed = time.time() - _cycle_start if '_cycle_start' in dir() else -1
            if _cycle_elapsed > 1.0:
                logging.warning("[R24-P2-AT-01] 交易周期耗时过长: %.2fs", _cycle_elapsed)
            provider._trading_lock.release()

    # ========== 生态系统互斥 ==========

    def _check_ecosystem_exclusion(self, targets: list) -> bool:
        provider = self._provider
        try:
            from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            spm = getattr(provider, '_state_param_manager', None)
            current_state = spm.get_current_state() if spm else 'other'
            # R13-P0-API-06修复: 使用strategy_ecosystem的映射方法替代硬编码
            strategy_id = eco.resolve_strategy_id_from_state(current_state)
            for t in targets:
                direction = t.get('direction', 'BUY')
                instrument_id = t.get('instrument_id', '')
                allowed, reason = eco.check_mutual_exclusion(strategy_id, direction, instrument_id)
                if not allowed:
                    logging.warning("[OptionTrading] 互斥阻断: strategy=%s dir=%s reason=%s", strategy_id, direction, reason)
                    return False
            return True
        except Exception as e:
            logging.error("[R16-P1-4.2] StrategyBusinessLayer._check_ecosystem_exclusion Error: %s, 阻断交易以保护安全", e)
            provider._health_pause_new_open = True
            return False

    # ========== 影子策略引擎 ==========

    def _feed_shadow_engine(self, targets: list, open_reason: str) -> None:
        """V7新增：将交易信号批量推送到影子策略监控引擎(优化：单次锁获取)"""
        provider = self._provider
        try:
            se = getattr(provider, '_shadow_engine', None)
            if se is None:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                se = get_shadow_strategy_engine()
                provider._shadow_engine = se
            if hasattr(se, 'are_params_locked') and not se.are_params_locked():
                logging.warning("[R16-P1-15] 影子策略参数未锁定，使用未验证参数")
            spm = getattr(provider, '_state_param_manager', None)
            market_state = spm.get_current_state() if spm else 'unknown'
            # R13-API-05: Determine strategy_group from state_param_manager
            strategy_group = "s2_resonance"
            try:
                from ali2026v3_trading.strategy.strategy_ecosystem import get_strategy_ecosystem
                eco = get_strategy_ecosystem()
                _STRATEGY_ID_TO_SHADOW_GROUP = {
                    'master': 's2_resonance', 'reverse': 's2_resonance', 'other': 's2_resonance',
                    'hft': 's1_hft', 'box': 's3_box', 'spring': 's4_spring',
                    'arbitrage': 's5_arbitrage', 'market_making': 's6_market_making',
                }
                _sid = eco.resolve_strategy_id_from_state(market_state) if eco else 'master'
                strategy_group = _STRATEGY_ID_TO_SHADOW_GROUP.get(_sid, 's2_resonance')
            except Exception:
                logging.warning("[R22-EP-P1] StrategyBusinessLayer exception swallowed")
                pass
            for t in targets:
                se.process_signal(
                    market_state=market_state,
                    instrument_id=t.get('instrument_id', ''),
                    signal_direction=t.get('direction', ''),
                    price=t.get('price', 0.0),
                    quantity=t.get('volume', 0),
                    signal_strength=t.get('signal_strength', 0.0),
                    strategy_group=strategy_group,  # R13-API-05: pass strategy_group
                )
            if not se.is_shadow_mode():
                logging.warning("[StrategyBusinessLayer] 影子策略隔离性验证失败")
        except Exception as e:
            logging.warning("[StrategyBusinessLayer._feed_shadow_engine] Error: %s", e)
