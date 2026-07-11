# [M1-44-02] 订单流分析器-核心

import threading
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.order.order_flow_data_structures import MicrostructureConfig, FootprintBar, ProductMicroData


try:
    from ali2026v3_trading.infra.shared_utils import extract_product_code
    _HAS_EXTRACT_PRODUCT = True
except ImportError:
    _HAS_EXTRACT_PRODUCT = False


class MicrostructureAnalyzer:

    """市场微观结构分析构- 高阶订单流工厂(修复复"""



    def __init__(self, config: MicrostructureConfig = None):

        self._config = config or MicrostructureConfig()

        self._lock = threading.RLock()



        # 品种数据容器

        self._products: Dict[str, ProductMicroData] = {}



        logger.info("Fixed MicrostructureAnalyzer initialized (memory leak fixed, improved divergence detection)")



    # ========================================================================

    # 辅助方法

    # ========================================================================

    # _方法唯一：委托shared_utils.extract_product_code，不再独立实现

    @staticmethod

    def _extract_product(instrument_id: str) -> str:

        """提取品种代码"""

        from ali2026v3_trading.infra.shared_utils import extract_product_code

        return extract_product_code(instrument_id)



    def _get_or_create(self, product: str) -> ProductMicroData:

        with self._lock:

            if product not in self._products:

                # 统一价格精度量位小。'
                self._products[product] = ProductMicroData(

                    product, 

                    self._config

                )

            return self._products[product]



    # ========================================================================

    # 数据输入接口

    # ========================================================================

    def on_trade(self, instrument_id: str, price: float, volume: int, direction: str,

                 timestamp: Optional[float] = None, product: Optional[str] = None):

        """逐笔成交 - 修复标准：统一为显式传入product参数

        

        Args:

            product: 必须显式传入品种代码，不再从instrument_id提取

        """

        if volume <= 0:

            return

        

        # _修复：强制要求显式传入product参数

        if product is None:

            logger.warning(f"on_trade requires explicit product parameter. instrument_id={instrument_id}")

            return

        

        if timestamp is None:

            timestamp = time.time()

        data = self._get_or_create(product)

        data.add_trade(price, volume, direction, timestamp)



        # _修复：内存更新为唯一路径，持久化由定时批量完成（删除实时DuckDB写入口

        # 注意：OrderFlowAnalyzer仅负责内存分析，持久化由DataService定时批量完成



    def on_depth(self, instrument_id: str, bids: List[Tuple[float, int]],

                 asks: List[Tuple[float, int]], timestamp: Optional[float] = None, product: Optional[str] = None):

        """订单簿快照- 修复标准：统一为显式传入product参数

        

        Args:

            product: 必须显式传入品种代码，不再从instrument_id提取

        """

        # _修复：强制要求显式传入product参数

        if product is None:

            logger.warning(f"on_depth requires explicit product parameter. instrument_id={instrument_id}")

            return

        

        if timestamp is None:

            timestamp = time.time()

        data = self._get_or_create(product)

        data.update_depth(bids, asks, timestamp)



    # ========================================================================

    # 查询接口

    # ========================================================================

    def get_cvd(self, product: str) -> float:

        with self._lock:

            data = self._products.get(product)

            return data.cvd if data else 0.0



    def get_cvd_divergence(self, product: str, lookback_seconds: int = None) -> Optional[Dict]:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return None

            # P2 Bug #78修复：在锁内完成全部操作，避免data引用悬空

            return data.check_cvd_divergence(lookback_seconds)



    def get_instant_imbalance(self, product: str, depth_levels: int = None) -> float:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return 0.0

            return data.calc_instant_imbalance(depth_levels)



    def get_ofi(self, product: str, lookback_seconds: int = 60) -> float:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return 0.0

            return data.calc_ofi(lookback_seconds)



    def get_multi_timeframe_ofi(self, product: str, timeframes: List[int] = None) -> Dict[int, float]:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return {}

            return data.calc_multi_timeframe_ofi(timeframes)



    def get_footprint(self, product: str, period: str = '1min') -> List[FootprintBar]:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return []

            return data.get_footprint_bars(period)



    def anchor_vwap(self, product: str, anchor_time: float,

                    anchor_price: float = None, anchor_volume: float = None) -> float:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return 0.0

            return data.get_anchor_vwap(anchor_time, anchor_price, anchor_volume)



    def get_execution_efficiency(self, product: str, order_price: float,

                                 order_volume: float, side: str) -> Dict:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return {'error': 'product not found'}

            return data.evaluate_execution(order_price, order_volume, side)



    def get_composite_assessment(self, product: str, lookback_seconds: int = 300,

                                 footprint_period: str = '5min') -> Dict[str, Any]:

        with self._lock:

            data = self._products.get(product)

            if not data:

                return self._empty_assessment(product)

            return data.get_composite_assessment(lookback_seconds, footprint_period, self._get_current_price)



    def _get_current_price(self, data: ProductMicroData) -> float:

        with data._lock:

            # 修复：原来错误地使用例self._asks，现在正确使。data._asks

            if data._bids and data._asks:  # 修复：应为data._bids和data._asks

                return (data._bids[0][0] + data._asks[0][0]) / 2

            if len(data._trades) > 0:

                return data._trades[-1][1]

            return 0.0



    def _empty_assessment(self, product: str) -> Dict:

        return {

            'product': product,

            'score': 0.0,

            'signal': 'neutral',

            'confidence': 0.0,

            'details': {},

            'reason': 'no data',

            'timestamp': time.time()

        }



    def export_snapshot(self) -> Dict:

        with self._lock:

            return {prod: data.get_snapshot() for prod, data in self._products.items()}



    def clear(self):

        with self._lock:

            self._products.clear()



    def get_products(self) -> List[str]:

        with self._lock:

            return list(self._products.keys())





# ============================================================================

# 使用示例

# ============================================================================

if __name__ == "__main__":

    import random

    

    print("=== 市场微观结构分析构- 配置对象象===\n")

    

    # 使用自定义配置

    config = MicrostructureConfig(

        large_order_threshold=50,

        max_history_size=10000,

        price_precision=2,

        footprint_maxlen=1000,

        anchor_vwap_maxsize=100,

        cvd_lookback_seconds=600,

        imbalance_depth_levels=5,

        vwap_lookback_seconds=60

    )

    msa = MicrostructureAnalyzer(config)



    # 测试期权合约处理

    print("测试期权合约产品提取:")

    print(f"IF2605-C-4500 -> {msa._extract_product('IF2605-C-4500')}")  # 应该输出 IF

    print(f"IF2605 -> {msa._extract_product('IF2605')}")  # 应该输出 IF

    print(f"cu2305 -> {msa._extract_product('cu2305')}")  # 应该输出 cu

    print(f"SR409C6200 -> {msa._extract_product('SR409C6200')}")  # 应该输出 SR

    print(f"m2405-P-2900 -> {msa._extract_product('m2405-P-2900')}")  # 应该输出 m



    # 模拟数据

    print("\n模拟交易数据...")

    base_price = 4120.0

    for i in range(200):

        price_noise = random.uniform(-3, 3)

        trend = 0.01 * i

        price = base_price + trend + price_noise

        volume = random.randint(1, 30)

        direction = 'buy' if random.random() > 0.45 else 'sell'

        ts = time.time() - 200 + i * 1.0

        # 显式传递product参数来避免期权合约提取问被

        msa.on_trade('IF2605', price, volume, direction, ts, product='IF')



    # 模拟订单播

    print("更新订单簿数据..")

    msa.on_depth('IF2605',

                 bids=[(4120, 150), (4119, 200), (4118, 100)],

                 asks=[(4121, 100), (4122, 80), (4123, 120)],

                 product='IF')



    # 综合评估

    print("\n=== 综合评估 ===")

    assessment = msa.get_composite_assessment('IF', lookback_seconds=300, footprint_period='1min')

    print(f"产品: {assessment['product']}")

    print(f"评分: {assessment['score']}")

    print(f"信号: {assessment['signal']}")

    print(f"置信号 {assessment['confidence']}")

    print(f"原因: {assessment['reason']}")

    print(f"详情: {assessment['details']}")



    # 其他功能测试

    print("\n=== 其他功能测试 ===")

    print(f"CVD: {msa.get_cvd('IF'):.2f}")

    

    cvd_div = msa.get_cvd_divergence('IF')

    if cvd_div:

        print(f"CVD背离: {cvd_div}")

    else:

        print("CVD背离: 无明显背离")

        

    print(f"即时不平台 {msa.get_instant_imbalance('IF'):.3f}")

    print(f"锚定VWAP(10秒前): {msa.anchor_vwap('IF', time.time() - 10):.2f}")

    

    exec_eval = msa.get_execution_efficiency('IF', 4121.5, 10, 'buy')

    print(f"执行效率: {exec_eval}")

    

    print(f"\n跟踪产品: {msa.get_products()}")

    

    snapshot = msa.export_snapshot()

    print(f"快照信息: {snapshot['IF']}")

    

    print("\n=== 修复点验验===")

    print("_修复了期权合约产品代码提取错误，支持多种格式")

    print("_on_trade _on_depth 支持显式传入 product 参数")

    print("_锚定VWAP字典已限制最小00个锚点，防止内存泄漏")

    print("_CVD背离检测改用线性回归趋势，减少噪声影响")

    print("_evaluate_execution增加除零保护")

    print("_CVD变化率计算增加epsilon防除集")

    print("_修复复。get_current_price 方法中的 data._asks 引用错误")

    print("_新增MicrostructureConfig配置对象，替代硬编码")


