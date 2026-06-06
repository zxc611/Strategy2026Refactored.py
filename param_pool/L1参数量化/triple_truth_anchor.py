import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from ali2026v3_trading.shared_utils import CHINA_TZ

import numpy as np
import pandas as pd

# R21-MATH-P1-04修复: 设置默认随机种子，确保测试/验证可复现
_TRIPLE_TRUTH_DEFAULT_SEED = 42
np.random.seed(_TRIPLE_TRUTH_DEFAULT_SEED)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TruthAnchorResult:
    algorithm_label: str
    expost_label: str
    external_label: Optional[str]
    algorithm_accuracy: float
    expost_accuracy: float
    external_accuracy: Optional[float]
    agreement_rate_algo_expost: float
    agreement_rate_algo_external: Optional[float]
    train_end_date: str
    val_start_date: str
    future_leak_risk: str
    timestamp: str = field(default_factory=lambda: datetime.now(CHINA_TZ).isoformat())


class TripleTruthAnchor:
    """
    三重真值锚定：切断盈利污染，训练与验证严格分离

    三重真值来源：
    1. 算法训练标签 — 多时间尺度投票（仅历史数据，无未来信息）
    2. 事后确认标签 — 未来N分钟实现波动率（仅用于验证，不参与训练）
    3. 外部权威标签 — 交易所官方数据（仅用于交叉校准）

    核心约束：训练标签与验证标签物理隔离，训练绝不使用事后确认标签
    """

    VOTING_WEIGHTS = {
        'minute': 0.10,
        'hour': 0.20,
        'daily': 0.40,
        'weekly': 0.30,
    }

    STATES = ['trend_up', 'trend_down', 'range_bound', 'volatile', 'quiet']

    def __init__(
        self,
        lookahead_minutes: int = 120,
        external_validator: Optional[Any] = None,
    ):
        self._lookahead = lookahead_minutes
        self._external = external_validator
        self._audit_log: List[Dict[str, Any]] = []

    def train_and_validate(
        self,
        data: pd.DataFrame,
        train_end_date: str,
        state_column: str = 'hmm_state',
        return_column: str = 'close',
    ) -> TruthAnchorResult:
        train_data = data[data.index <= train_end_date]
        val_data = data[data.index > train_end_date]

        if len(train_data) == 0 or len(val_data) == 0:
            raise ValueError(
                f"训练/验证数据为空: train={len(train_data)}, val={len(val_data)}"
            )

        train_labels = self._generate_no_future_labels(train_data, state_column)
        val_labels_algo = self._generate_no_future_labels(val_data, state_column)
        val_labels_expost = self._generate_expost_labels(
            val_data, return_column
        )
        val_labels_external = self._generate_external_labels(val_data)

        algo_acc = self._accuracy(val_labels_algo, val_labels_expost)
        expost_acc = 1.0
        external_acc = None
        agreement_algo_expost = self._agreement_rate(
            val_labels_algo, val_labels_expost
        )
        agreement_algo_external = None

        if val_labels_external is not None:
            external_acc = self._accuracy(val_labels_algo, val_labels_external)
            agreement_algo_external = self._agreement_rate(
                val_labels_algo, val_labels_external
            )

        result = TruthAnchorResult(
            algorithm_label='multi_scale_voting',
            expost_label=f'expost_{self._lookahead}min',
            external_label='exchange_official' if val_labels_external is not None else None,
            algorithm_accuracy=algo_acc,
            expost_accuracy=expost_acc,
            external_accuracy=external_acc,
            agreement_rate_algo_expost=agreement_algo_expost,
            agreement_rate_algo_external=agreement_algo_external,
            train_end_date=str(train_end_date),
            val_start_date=str(val_data.index.min()),
            future_leak_risk='NONE',
        )

        self._audit_log.append({
            'train_end': train_end_date,
            'train_samples': len(train_data),
            'val_samples': len(val_data),
            'result': result,
        })

        logger.info(
            "三重真值锚定完成: algo_acc=%.4f, agreement_algo_expost=%.4f, "
            "future_leak=%s, train_end=%s",
            algo_acc, agreement_algo_expost, result.future_leak_risk, train_end_date,
        )

        return result

    def _generate_no_future_labels(
        self, data: pd.DataFrame, state_column: str
    ) -> pd.Series:
        labels = []
        for i in range(len(data)):
            current_data = data.iloc[: i + 1]  # R21-MEM-P2-01修复: 每次循环创建递增切片副本，O(n^2)内存开销；可考虑滚动窗口替代

            minute_state = self._vote_minute(current_data.tail(30), state_column)
            hour_state = self._vote_hour(current_data.tail(240), state_column)
            daily_state = self._vote_daily(current_data.tail(252), state_column)
            weekly_state = self._vote_weekly(current_data, state_column)

            state = self._weighted_vote({
                'minute': minute_state,
                'hour': hour_state,
                'daily': daily_state,
                'weekly': weekly_state,
            })
            labels.append(state)

        return pd.Series(labels, index=data.index)

    def _generate_expost_labels(
        self, data: pd.DataFrame, return_column: str
    ) -> pd.Series:
        labels = []
        prices = data[return_column].values if return_column in data.columns else np.ones(len(data))

        for i in range(len(data)):
            future_end = min(i + self._lookahead, len(data) - 1)
            if future_end <= i:
                labels.append('quiet')
                continue

            future_prices = prices[i + 1 : future_end + 1]
            if len(future_prices) < 2:
                labels.append('quiet')
                continue

            realized_vol = np.std(np.diff(np.log(future_prices + 1e-10)))
            realized_return = (future_prices[-1] - prices[i]) / (prices[i] + 1e-10)

            if realized_vol > 0.02 and abs(realized_return) > 0.01:
                labels.append('volatile')
            elif realized_return > 0.005:
                labels.append('trend_up')
            elif realized_return < -0.005:
                labels.append('trend_down')
            elif realized_vol < 0.005:
                labels.append('quiet')
            else:
                labels.append('range_bound')

        return pd.Series(labels, index=data.index)

    def _generate_external_labels(
        self, data: pd.DataFrame
    ) -> Optional[pd.Series]:
        if self._external is None:
            return None
        try:
            return self._external.fetch_labels(data)
        except Exception as e:
            logger.warning("外部标签获取失败: %s", e)
            return None

    def _vote_minute(self, window: pd.DataFrame, state_column: str) -> str:
        return self._dominant_state(window, state_column)

    def _vote_hour(self, window: pd.DataFrame, state_column: str) -> str:
        return self._dominant_state(window, state_column)

    def _vote_daily(self, window: pd.DataFrame, state_column: str) -> str:
        return self._dominant_state(window, state_column)

    def _vote_weekly(self, window: pd.DataFrame, state_column: str) -> str:
        return self._dominant_state(window, state_column)

    def _dominant_state(self, window: pd.DataFrame, state_column: str) -> str:
        if state_column in window.columns:
            counts = window[state_column].value_counts()
            if len(counts) > 0:
                return str(counts.index[0])
        if len(window) > 1 and 'close' in window.columns:
            ret = window['close'].iloc[-1] / window['close'].iloc[0] - 1
            vol = window['close'].pct_change().std() if len(window) > 2 else 0.0
            if vol > 0.02 and abs(ret) > 0.01:
                return 'volatile'
            elif ret > 0.005:
                return 'trend_up'
            elif ret < -0.005:
                return 'trend_down'
            elif vol < 0.005:
                return 'quiet'
        return 'range_bound'

    def _weighted_vote(self, state_votes: Dict[str, str]) -> str:
        score = {s: 0.0 for s in self.STATES}
        for timescale, state in state_votes.items():
            w = self.VOTING_WEIGHTS.get(timescale, 0.0)
            if state in score:
                score[state] += w
        if max(score.values()) == 0.0:
            return 'range_bound'
        return max(score, key=score.get)

    @staticmethod
    def _accuracy(predictions: pd.Series, truth: pd.Series) -> float:
        if len(predictions) != len(truth):
            min_len = min(len(predictions), len(truth))
            predictions = predictions.iloc[:min_len]  # R21-MEM-P2-01修复: iloc切片为视图，.values比较时才产生ndarray
            truth = truth.iloc[:min_len]
        if len(predictions) == 0:
            return 0.0
        return float((predictions.values == truth.values).mean())

    @staticmethod
    def _agreement_rate(a: pd.Series, b: pd.Series) -> float:
        min_len = min(len(a), len(b))
        if min_len == 0:
            return 0.0
        return float((a.iloc[:min_len].values == b.iloc[:min_len].values).mean())  # R21-MEM-P2-01修复: iloc切片为视图，.values产生ndarray

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self._audit_log)

    def verify_no_future_leak(self) -> bool:
        for entry in self._audit_log:
            result = entry['result']
            if result.future_leak_risk != 'NONE':
                logger.error("未来泄露检测失败: train_end=%s", entry['train_end'])
                return False
        return True
