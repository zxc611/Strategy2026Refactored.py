"""
ProductionQuantSystem.py - 量化核心系统 V3 门面

5文件结构：
- quant_infra.py    : 基础设施原语（NumpyRingBuffer+日志限流）
- quant_platform.py : 平台层（ExchangeTime+TickAggregator+AtomicState+HealthMonitor）
- quant_core.py     : 6个核心量化模块（趋势+PCA+HMM+波动率+协整+生存分析）
- quant_services.py : 服务层（持久化+热配置+Numba+单例管理）
- ProductionQuantSystem.py : 门面类+模块级单例+主程序入口

主程序入口用法：
    python -m ali2026v3_trading.ProductionQuantSystem --config config.json --symbols IF,IH,IM
"""
from __future__ import annotations

import gc
import logging
import os
import threading
import time
import weakref
import argparse
import sys
from typing import Any, Dict, List, Optional

from .infra.serialization_utils import json_dumps, json_loads, json_default_serializer

try:
    from .data.quant_infra import NumpyRingBuffer
    from .data.quant_platform import ExchangeTime, TickAggregator, AtomicSystemState, SystemHealthMonitor
    from .data.quant_trend_scorer import MultiPeriodTrendScorer
    from .data.quant_volatility import IVSurfacePCA, VolatilityRegimeFilter
    from .data.quant_hmm import AdaptiveHMM
    from .data.quant_cointegration import CointegrationScanner, SurvivalAnalyzer
    from .data.quant_services import (
        LightweightPersistence, HotConfigManager, numba_helper, HAS_NUMBA,
    )
    from .infra.singleton_registry import SingletonRegistry
except ImportError:
    from data.quant_infra import NumpyRingBuffer
    from data.quant_platform import ExchangeTime, TickAggregator, AtomicSystemState, SystemHealthMonitor
    from data.quant_trend_scorer import MultiPeriodTrendScorer
    from data.quant_volatility import IVSurfacePCA, VolatilityRegimeFilter
    from data.quant_hmm import AdaptiveHMM
    from data.quant_cointegration import CointegrationScanner, SurvivalAnalyzer
    from data.quant_services import (
        LightweightPersistence, HotConfigManager, numba_helper, HAS_NUMBA,
    )
    from infra.singleton_registry import SingletonRegistry
from collections import deque


class ProductionQuantSystem:

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or {}

        # R13-P2-DEAD-04修复: 添加shutdown标志
        self._shutdown_requested: bool = False

        # P1-01修复: 统一健康检查入口到HealthCheckAggregator
        try:
            from ali2026v3_trading.infra.health_monitor import HealthCheckAPI
            from ali2026v3_trading.infra.health_monitor import HealthCheckAggregator
            self.health_check_api = HealthCheckAPI(config=cfg)
            self.health_check_aggregator = HealthCheckAggregator
            logging.info("[ProductionQuantSystem] 健康检查API和Aggregator已集成")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[ProductionQuantSystem] 健康检查集成失败: %s", e)
            self.health_check_api = None
            self.health_check_aggregator = None

        # R24-P2-IV-08修复: 验证config中关键数值参数的合理性
        _adx_period = cfg.get('adx_period', 14)
        if not isinstance(_adx_period, (int, float)) or _adx_period <= 0:
            logging.warning("[R24-P2-IV-08] adx_period=%s 无效，回退到默认值14", _adx_period)
            _adx_period = 14

        self.trend_scorer = MultiPeriodTrendScorer(
            periods=tuple(cfg.get('trend_periods', (5, 20, 60))),
            weights=tuple(cfg.get('trend_weights', (0.2, 0.5, 0.3))),
            adx_period=_adx_period,
        )

        self.iv_pca = IVSurfacePCA(
            window=cfg.get('pca_window', 120),
            shrinkage=cfg.get('pca_shrinkage', 0.5),
            var_explained_threshold=cfg.get('pca_var_threshold', 0.90),
            max_components=cfg.get('pca_max_components', 3),
            refit_interval=cfg.get('pca_refit_interval', 10),
            ewma_alpha=cfg.get('pca_ewma_alpha', 0.05),
        )

        self.hmm = AdaptiveHMM(
            n_states=cfg.get('hmm_n_states', 3),
            update_interval=cfg.get('hmm_update_interval', 100),
            var_floor_ratio=cfg.get('hmm_var_floor_ratio', 0.01),
            min_variance=cfg.get('hmm_min_variance', 1e-10),
        )

        self.vol_filter = VolatilityRegimeFilter(
            lookback=cfg.get('vol_lookback', 100),
            low_percentile=cfg.get('vol_low_pct', 25.0),
            high_percentile=cfg.get('vol_high_pct', 75.0),
            min_hold_ticks=cfg.get('vol_min_hold', 20),
            ewma_alpha=cfg.get('vol_ewma_alpha', 0.05),
        )

        self.coint_scanner = CointegrationScanner(
            window=cfg.get('coint_window', 120),
            scan_interval=cfg.get('coint_scan_interval', 50),
        )

        self.survival = SurvivalAnalyzer(
            max_iterations=cfg.get('survival_max_iter', 10),
            convergence_tol=cfg.get('survival_tol', 1e-3),
            lm_damping=cfg.get('survival_lm_damping', 1e-4),
        )

        self.exchange_time = ExchangeTime(
            exchange=cfg.get('exchange', 'DCE'),
            night_end_hour=cfg.get('night_end_hour', 23),
        )

        self.tick_aggregator = TickAggregator(
            interval_sec=cfg.get('bar_interval_sec', 300),
            vol_threshold=cfg.get('bar_vol_threshold', 0),
            exchange_time=self.exchange_time,
            volume_mode=cfg.get('volume_mode', 'delta'),
        )

        self.atomic_state = AtomicSystemState()
        self.health_monitor = SystemHealthMonitor(
            latency_thresholds=cfg.get('latency_thresholds'),
        )

        self.persistence = LightweightPersistence(
            db_dir=cfg.get('persistence_dir'),
            snapshot_interval_ms=cfg.get('snapshot_interval_ms', 5000),
        )

        self.hot_config = HotConfigManager(
            config_path=cfg.get('hot_config_path'),
            poll_interval=cfg.get('config_poll_interval', 5.0),
        )

        self._initialized = False
        self._symbols: List[str] = []
        self._weak_self: Optional[weakref.ref] = None
        self._singleton_mgr = SingletonRegistry  # P1-19修复: 迁移到SingletonRegistry
        self._crm = None
        self._imbalance_window = cfg.get('imbalance_window', 20)
        self._signed_volume_history: deque = deque(maxlen=self._imbalance_window)
        self._last_close: Optional[float] = None

    def initialize(self, symbols: Optional[List[str]] = None) -> None:
        if symbols:
            self._symbols = symbols
            for sym in symbols:
                self.coint_scanner.add_symbol(sym)
        self._initialized = True
        self._weak_self = weakref.ref(self)
        self._singleton_mgr.register_singleton('quant_system', self, self.shutdown)  # P1-19修复: SingletonRegistry
        if self.hot_config._config_path:
            self.hot_config.start_watching()
        numba_helper.warmup()
        logging.info("[ProductionQuantSystem] Initialized with %d symbols, numba=%s",
                     len(self._symbols), HAS_NUMBA)

    def update_tick(self, symbol: str, high: float, low: float,
                    close: float, return_value: float,
                    iv_surface: Optional[Dict[str, float]] = None,
                    timestamp_ms: Optional[int] = None,
                    cum_volume: int = 0) -> Dict[str, Any]:
        if not self._initialized:
            return {}
        t0 = time.perf_counter()
        # R27-P0-DR-04修复: 每个策略模块调用加异常隔离，防止单策略崩溃影响全局
        trend_result = {}
        try:
            trend_result = self.trend_scorer.update(high, low, close)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-04] trend_scorer异常隔离: %s", e)
        vol_result = {}
        try:
            vol_result = self.vol_filter.update(return_value)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-04] vol_filter异常隔离: %s", e)
        hmm_result = {}
        try:
            hmm_result = self.hmm.update(return_value)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-04] hmm异常隔离: %s", e)
        iv_result = {}
        try:
            iv_result = self.iv_pca.update(iv_surface) if iv_surface else {}
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-04] iv_pca异常隔离: %s", e)
        try:
            self.coint_scanner.update_price(symbol, close)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-04] coint_scanner异常隔离: %s", e)
        try:
            completed_bar = self.tick_aggregator.update_tick(close, cum_volume, timestamp_ms)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            completed_bar = None
            logging.warning("[R27-P0-DR-04] tick_aggregator异常隔离: %s", e)
        try:
            self.hmm.run_em_if_needed()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[R27-P0-DR-04] hmm EM异常隔离: %s", e)

        if self._last_close is not None and cum_volume > 0:
            tick_vol = max(cum_volume - getattr(self, '_last_cum_volume', 0), 1)
            sign = 1.0 if close >= self._last_close else -1.0
            self._signed_volume_history.append(sign * tick_vol)
        self._last_close = close
        self._last_cum_volume = cum_volume

        imbalance = 0.0
        if len(self._signed_volume_history) >= 3:
            total_vol = sum(abs(v) for v in self._signed_volume_history)
            net_vol = sum(self._signed_volume_history)
            imbalance = net_vol / total_vol if total_vol > 0 else 0.0
            imbalance = max(-1.0, min(1.0, imbalance))

        # R10-P1-10修复: CRM.update()失败时升级为warning日志并加重试机制
        _crm_max_retries = 3
        for _crm_attempt in range(_crm_max_retries):
            try:
                if self._crm is None:
                    import importlib  # R21-CC-P2-03修复: 动态导入 — 每次tick都可能触发，应缓存模块引用
                    crm_module = importlib.import_module('ali2026v3_trading.param_pool.optimization.cycle_sharpe')
                    self._crm = crm_module.get_cycle_resonance_module()
                hmm_label = hmm_result.get('state_label', 'NORMAL') if isinstance(hmm_result, dict) else 'NORMAL'
                hmm_posterior = tuple(hmm_result.get('posterior', [0.33, 0.34, 0.33])) if isinstance(hmm_result, dict) else (0.33, 0.34, 0.33)
                trend_scores = tuple(trend_result.get('period_scores', [0.0, 0.0, 0.0])) if isinstance(trend_result, dict) else (0.0, 0.0, 0.0)
                trend_dirs = tuple(1.0 if s > 0 else (-1.0 if s < 0 else 0.0) for s in trend_scores)
                strength = trend_result.get('strength', 0.0) if isinstance(trend_result, dict) else 0.0
                self._crm.update(
                    hmm_state=hmm_label,
                    hmm_posterior=hmm_posterior,
                    trend_scores=trend_scores,
                    trend_directions=trend_dirs,
                    strength=strength,
                    imbalance=imbalance,
                )
                break  # 成功则跳出重试循环
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                if _crm_attempt < _crm_max_retries - 1:
                    logging.warning("[R10-P1-10] CRM update失败(第%d次), 将重试: %s", _crm_attempt + 1, e)
                else:
                    logging.warning("[R10-P1-10] CRM update失败(已重试%d次), 后续状态诊断可能基于过期CRM状态: %s",
                                    _crm_max_retries, e)

        self.atomic_state.capture(
            trend=trend_result, vol_regime=vol_result,
            hmm_state=hmm_result, iv_pca=iv_result,
            current_bar=self.tick_aggregator.get_current_bar(),
        )
        # P2-R11-15: 性能计数器为全局变量，多策略共享。建议：使用per-strategy计数器或标签化metrics。
        elapsed_us = (time.perf_counter() - t0) * 1e6
        self.health_monitor.record_latency('system_update_tick', elapsed_us)

        # ✅ 集成健康检查API更新（L-P1-2）
        if self.health_check_api is not None:
            try:
                self.health_check_api.update_component_status("data_feeds", "ok")
                self.health_check_api.update_component_status("signal_generator", "ok")
                self.health_check_api.update_last_signal_time()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug("[ProductionQuantSystem] HealthCheckAPI update error: %s", e)

        # ✅ 集成结构化JSONL日志（L-P1-1）
        if self.structured_logger is not None:
            try:
                self.structured_logger.log_signal({
                    "signal_id": f"tick_{symbol}_{int(time.time()*1000)}",
                    "instrument_id": symbol,
                    "direction": "buy" if (trend_result.get('strength', 0) if isinstance(trend_result, dict) else 0) > 0 else "sell",
                    "strength": trend_result.get('strength', 0.0) if isinstance(trend_result, dict) else 0.0,
                    "estimated_plr": 0.0,
                    "decision_score": 0.0,
                    "position_scale": 1.0,
                    "filtered": False,
                    "filter_reason": "",
                })
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.debug("[ProductionQuantSystem] StructuredLogger error: %s", e)

        return {
            'trend': trend_result, 'volatility_regime': vol_result,
            'hmm_state': hmm_result, 'iv_pca': iv_result,
            'completed_bar': completed_bar,
        }

    # R30-ARCH-02修复: PQS分析结果桥接到策略决策链路
    def publish_analysis_to_strategy(self, analysis_result: Dict[str, Any]) -> None:
        """R30-ARCH-02: 将量化分析结果通过EventBus发布，供策略主链路消费

        发布事件:
        - 'pqs.trend_updated': 趋势评分变化
        - 'pqs.hmm_state_changed': HMM状态转移
        - 'pqs.vol_regime_changed': 波动率区间切换
        - 'pqs.analysis_snapshot': 完整分析快照

        Args:
            analysis_result: update_tick()返回的分析结果字典
        """
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is None or getattr(_bus, '_shutdown', False):
                return
            _bus.publish('pqs.analysis_snapshot', analysis_result)
            trend = analysis_result.get('trend', {})
            if isinstance(trend, dict) and trend.get('strength', 0) != 0:
                _bus.publish('pqs.trend_updated', trend)
            hmm = analysis_result.get('hmm_state', {})
            if isinstance(hmm, dict) and hmm.get('state_label'):
                _bus.publish('pqs.hmm_state_changed', hmm)
            vol = analysis_result.get('volatility_regime', {})
            if isinstance(vol, dict) and vol.get('regime'):
                _bus.publish('pqs.vol_regime_changed', vol)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[R30-ARCH-02] PQS EventBus发布失败: %s", e)

    def get_latest_analysis(self) -> Dict[str, Any]:
        """R30-ARCH-02: 获取最新分析结果快照（供策略直接查询）"""
        return self.atomic_state.get_snapshot() if self._initialized else {}

    def periodic_scan(self) -> Dict[str, Any]:
        return {'cointegration': self.coint_scanner.scan()}

    def persist_state(self) -> None:
        if not self._initialized:
            return
        state = self.atomic_state.get_snapshot()
        self.persistence.save_snapshot('quant_state', state)
        self.persistence.save('last_atomic_version', state.get('version', 0))
        self.persistence.save('symbols', self._symbols)
        self.persistence.save('hmm_state', self.hmm._build_result())
        self.persistence.save('vol_regime', self.vol_filter._build_result())

    def restore_state(self) -> Dict[str, Any]:
        restored = {}
        symbols = self.persistence.load('symbols')
        if symbols:
            self._symbols = symbols
            for sym in symbols:
                self.coint_scanner.add_symbol(sym)
            restored['symbols'] = symbols
        hmm_state = self.persistence.load('hmm_state')
        if hmm_state:
            restored['hmm_state'] = hmm_state
        # P1-17修复: 补充读取last_atomic_version和vol_regime（与persist_state对齐）'
        last_atomic_version = self.persistence.load('last_atomic_version')
        if last_atomic_version is not None:
            restored['last_atomic_version'] = last_atomic_version
        vol_regime = self.persistence.load('vol_regime')
        if vol_regime is not None:
            restored['vol_regime'] = vol_regime
        return restored

    def get_system_status(self) -> Dict[str, Any]:
        status = {
            'initialized': self._initialized,
            'symbols': self._symbols,
            'atomic_version': self.atomic_state.version,
            'atomic_age_ms': self.atomic_state.age_ms,
            'health': self.health_monitor.get_health_report(),
            'numba_available': HAS_NUMBA,
            'hot_config': self.hot_config.get_all(),
            'modules': {
                'trend_scorer': 'MultiPeriodTrendScorer',
                'iv_pca': 'IVSurfacePCA',
                'hmm': 'AdaptiveHMM',
                'vol_filter': 'VolatilityRegimeFilter',
                'coint_scanner': 'CointegrationScanner',
                'survival': 'SurvivalAnalyzer',
                'exchange_time': 'ExchangeTime',
                'tick_aggregator': 'TickAggregator',
                'atomic_state': 'AtomicSystemState',
                'health_monitor': 'SystemHealthMonitor',
                'ring_buffer': 'NumpyRingBuffer',
                'persistence': 'LightweightPersistence',
                'hot_config': 'HotConfigManager',
                'numba_jit': 'NumbaJITHelper',
                'singleton_mgr': 'SingletonManager/SingletonRegistry',
            },
        }

        # ✅ 集成健康检查API状态（L-P1-2）
        if self.health_check_api is not None:
            try:
                health_status = self.health_check_api.get_health_status()
                status['health_check_api'] = health_status
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                status['health_check_api'] = {'error': str(e)}

        return status

    def shutdown(self) -> None:
        self._initialized = False
        self.hot_config.stop_watching()
        self.persistence.close()
        self.hmm._em_running = False
        if self.hmm._em_thread is not None and self.hmm._em_thread.is_alive():
            self.hmm._em_thread.join(timeout=2.0)
        # R22-RES-04修复: 扩展shutdown覆盖更多子服务
        for _attr in ('tick_aggregator', 'health_monitor', 'atomic_state'):
            _svc = getattr(self, _attr, None)
            if _svc is not None and hasattr(_svc, 'stop'):
                try:
                    _svc.stop()
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _stop_err:
                    logging.warning("[R22-RES-04] %s.stop()失败: %s", _attr, _stop_err)
            elif _svc is not None and hasattr(_svc, 'close'):
                try:
                    _svc.close()
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _close_err:
                    logging.warning("[R22-RES-04] %s.close()失败: %s", _attr, _close_err)
        # R22-RES-04-残留修复: 清理health_check_api和structured_logger
        if hasattr(self, 'health_check_api') and self.health_check_api is not None:
            try:
                if hasattr(self.health_check_api, 'stop_push'):
                    self.health_check_api.stop_push()
                    logging.debug("[R22-RES-04-修复] health_check_api.stop_push()已调用")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _hca_err:
                logging.warning("[R22-RES-04] health_check_api.stop_push()失败: %s", _hca_err)
        if hasattr(self, 'structured_logger') and self.structured_logger is not None:
            try:
                if hasattr(self.structured_logger, 'close'):
                    self.structured_logger.close()
                    logging.debug("[R22-RES-04-修复] structured_logger.close()已调用")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _sl_err:
                logging.warning("[R22-RES-04] structured_logger.close()失败: %s", _sl_err)
        # R33-P0-03修复: EventBus线程池生命周期管理 — 在系统shutdown时主动关闭
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _eb = get_global_event_bus()
            if hasattr(_eb, 'shutdown'):
                _eb.shutdown(wait=False)
                logging.debug("[R33-P0-03] event_bus.shutdown()已调用")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _eb_err:
            logging.warning("[R33-P0-03] event_bus.shutdown()失败: %s", _eb_err)
        logging.info("[ProductionQuantSystem] Shutdown complete")


_quant_system: Optional[ProductionQuantSystem] = None
_quant_system_lock = threading.Lock()
_quant_system_ref: Optional[weakref.ref] = None


def get_production_quant_system(config: Optional[Dict[str, Any]] = None) -> ProductionQuantSystem:
    global _quant_system, _quant_system_ref
    if _quant_system is not None:
        obj = _quant_system_ref() if _quant_system_ref else None
        if obj is not None:
            return obj
        _quant_system = None
    with _quant_system_lock:
        if _quant_system is None:
            _quant_system = ProductionQuantSystem(config)
            _quant_system_ref = weakref.ref(_quant_system)
    return _quant_system


def shutdown_quant_system() -> None:
    global _quant_system, _quant_system_ref
    with _quant_system_lock:
        if _quant_system is not None:
            _quant_system.shutdown()
            _quant_system = None
            _quant_system_ref = None
    SingletonRegistry.cleanup_all_singletons()  # P1-19修复: 迁移到SingletonRegistry
    SingletonRegistry.reset_all()
    gc.collect()


# ============================================================================
# 主程序入口
# ============================================================================

def _load_config_from_file(config_path: str) -> Dict[str, Any]:
    """从JSON文件加载配置"""
    from ali2026v3_trading.infra.serialization_utils import json_loads
    with open(config_path, 'r', encoding='utf-8') as f:
        return json_loads(f.read())  # R5-1


def _build_config_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    """从命令行参数构建配置字典，优先使用ConfigService统一配置中心"""
    cfg: Dict[str, Any] = {}

    # 1. 尝试从统一配置中心加载
    try:
        from ali2026v3_trading.config.config_service import get_config_service
        cs = get_config_service()
        cfg['log_dir'] = cs.paths.log_dir
        cfg['exchange'] = cs.exchanges.exchanges[0] if cs.exchanges.exchanges else 'DCE'
        cfg['persistence_dir'] = cs.paths.db_path
        cfg['hot_config_path'] = str(cs.paths.config_dir / "hot_config.json")
        cfg['snapshot_interval_ms'] = cs.trading.tick_update_interval_ms
        cfg['bar_interval_sec'] = 300
    except (ImportError, ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.debug("[ProductionQuantSystem] ConfigService加载失败: %s", e)

    # 2. 从配置文件覆盖
    if args.config:
        try:
            file_cfg = _load_config_from_file(args.config)
            cfg.update(file_cfg)
            logging.info("[ProductionQuantSystem] 配置文件已加载: %s", args.config)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[ProductionQuantSystem] 配置文件加载失败: %s", e)

    # 3. 命令行参数直接覆盖
    if args.symbols:
        cfg['symbols'] = args.symbols.split(',')
    if args.log_dir:
        cfg['log_dir'] = args.log_dir
    if args.exchange:
        cfg['exchange'] = args.exchange

    return cfg


def main() -> int:
    """ProductionQuantSystem 主程序入口

    用法示例:
        python -m ali2026v3_trading.ProductionQuantSystem --symbols IF,IH,IM
        python -m ali2026v3_trading.ProductionQuantSystem --config quant_config.json
    """
    parser = argparse.ArgumentParser(
        description='ProductionQuantSystem V3 - 量化核心系统主程序',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s --symbols IF,IH,IM --exchange CFFEX
  %(prog)s --config config/quant.json --log-dir logs/quant
        """
    )
    parser.add_argument('--config', '-c', type=str, default='',
                        help='配置文件路径（JSON格式）')
    parser.add_argument('--symbols', '-s', type=str, default='',
                        help='交易品种列表，逗号分隔，如 IF,IH,IM')
    parser.add_argument('--exchange', '-e', type=str, default='',
                        help='交易所代码，如 CFFEX/DCE/SHFE')
    parser.add_argument('--log-dir', '-l', type=str, default='',
                        help='日志输出目录')
    parser.add_argument('--mode', '-m', type=str, default='run',
                        choices=['run', 'status', 'scan'],
                        help='运行模式: run=正常运行, status=查看状态, scan=一次性扫描')
    parser.add_argument('--duration', '-d', type=int, default=0,
                        help='运行时长（秒），0表示一直运行直到Ctrl+C')
    parser.add_argument('--param-version', type=str, default='',
                        help='参数版本号（如v1.2），对应环境变量PARAM_VERSION，手册15节要求')

    args = parser.parse_args()

    if args.param_version:
        os.environ.setdefault('PARAM_VERSION', args.param_version)
        logging.info("[ProductionQuantSystem] --param-version=%s 已设置到PARAM_VERSION环境变量", args.param_version)

    # P2-05修复: 委托统一日志入口, 不再独立basicConfig
    try:
        from ali2026v3_trading.config.config_logging import setup_logging
        setup_logging()
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )

    # 构建配置
    cfg = _build_config_from_args(args)

    # 获取或创建系统实例
    pqs = get_production_quant_system(cfg)

    # 初始化品种
    symbols = cfg.get('symbols', [])
    if symbols:
        pqs.initialize(symbols=symbols)
    else:
        pqs.initialize()

    logging.info("[ProductionQuantSystem] 系统已启动，模式=%s，品种=%s",
                 args.mode, symbols or '未指定')

    try:
        if args.mode == 'status':
            status = pqs.get_system_status()
            print(json_dumps(status, indent=2))
            return 0

        elif args.mode == 'scan':
            result = pqs.periodic_scan()
            print(json_dumps(result, indent=2))
            return 0

        elif args.mode == 'run':
            # 持久化状态
            pqs.persist_state()

            # 主循环
            start_time = time.time()
            scan_interval = 60.0  # 每秒扫描一次
            last_scan = 0.0

            while True:
                now = time.time()
                # R13-P2-DEAD-04修复: 检查shutdown标志
                if hasattr(pqs, '_shutdown_requested') and pqs._shutdown_requested:
                    logging.info("[ProductionQuantSystem] 收到shutdown请求，退出主循环")
                    break

                # 周期性扫描
                if now - last_scan >= scan_interval:
                    pqs.periodic_scan()
                    pqs.persist_state()
                    last_scan = now
                    logging.info("[ProductionQuantSystem] 周期性扫描完成，运行时间=%.0fs",
                                 now - start_time)

                # 检查运行时长
                if args.duration > 0 and (now - start_time) >= args.duration:
                    logging.info("[ProductionQuantSystem] 达到运行时长限制(%ds)，正常退出", args.duration)
                    break

                time.sleep(1.0)

    except KeyboardInterrupt:
        logging.info("[ProductionQuantSystem] 收到中断信号，正在优雅关闭...")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.error("[ProductionQuantSystem] 运行时错误: %s", e, exc_info=True)
        return 1
    finally:
        pqs.shutdown()
        shutdown_quant_system()
        logging.info("[ProductionQuantSystem] 系统已关闭")

    return 0


if __name__ == '__main__':
    sys.exit(main())
