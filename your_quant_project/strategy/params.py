"""策略参数定义。"""
from typing import Dict, Any

try:
    from pythongo.base import BaseParams, Field
except ImportError:
    # 兼容本地开发或 IDE 环境
    class BaseParams:
        pass
    def Field(default=None, title="", **kwargs):
        return default


class Params(BaseParams):
    max_kline: int = Field(default=200, title="K线缓存长度(V17)[CLASS_MATCH]")
    kline_request_count: int = Field(default=6, title="单次请求K线数量（默认6）")
    kline_style: str = Field(default="M1", title="K线周期")
    subscribe_options: bool = Field(default=True, title="是否订阅期权行情")
    debug_output: bool = Field(default=True, title="是否输出调试信息")
    diagnostic_output: bool = Field(default=True, title="诊断/测试输出开关（交易/回测自动关闭）")
    api_key: str = Field(default="", title="通用API密钥（可选，映射到环境变量API_KEY）")
    infini_api_key: str = Field(default="", title="Infini行情密钥（可选，映射到环境变量INFINI_API_KEY）")
    access_key: str = Field(default="", title="访问密钥 AccessKey（平台提供）")
    access_secret: str = Field(default="", title="访问密钥 AccessSecret（平台提供）")
    run_profile: str = Field(default="full", title="运行预设(full|lite)")
    history_load_max_workers: int = Field(default=16, title="加载历史K线的最大并发线程数")
    enable_scheduler: bool = Field(default=True, title="是否启用定时任务（回测可关闭）")
    use_tick_kline_generator: bool = Field(default=True, title="是否启用Tick合成K线")
    backtest_tick_mode: bool = Field(default=False, title="回测模式：仅用Tick驱动K线（跳过历史K线拉取）")
    exchange: str = Field(default="CFFEX", title="默认交易所，用于查询合约")
    future_product: str = Field(default="IF", title="期货品种，用于查询合约")
    option_product: str = Field(default="IO", title="期权品种，用于查询合约")

    # [New Requirements 2026-02-04]
    calculation_interval: int = Field(default=60, title="策略主循环/计算触发间隔(秒) [User Required: 60s]")
    kline_duration_seconds: int = Field(default=60, title="K线时间范围(秒)，用于拉取限制和计算超时")
    option_width_threshold: float = Field(default=4.0, title="期权宽度阈值，小于等于此值不交易")
    debug_output_interval: int = Field(default=180, title="收市调试状态下宽度排行输出间隔(秒)")
    open_debug_output_interval: int = Field(default=60, title="开盘调试状态下宽度排行输出间隔(秒)")
    trade_output_interval: int = Field(default=900, title="交易状态下宽度排行输出间隔(秒)")
    # Note: User request wording for 8 seemed swapped, assuming logical mapping:
    # "K线时间长度" -> kline_duration_seconds (default 60s)
    # "期权宽度阀值" -> option_width_threshold (default 4)
    auto_load_history: bool = Field(default=True, title="启动后自动加载历史K线")
    load_history_options: bool = Field(default=True, title="加载历史K线时是否包含期权")
    load_all_products: bool = Field(default=False, title="是否加载全部品种（忽略产品过滤）")
    exchanges: str = Field(default="CFFEX,SHFE,DCE,CZCE", title="交易所列表（逗号分隔）")
    future_products: str = Field(default="IF,IH,IM,CU,AL,ZN,RB,AU,AG,M,Y,A,JM,I,J,CF,SR,MA,TA", title="期货品种列表（逗号分隔）")
    option_products: str = Field(default="IO", title="期权品种列表（逗号分隔）")
    include_future_products_for_options: bool = Field(default=True, title="将期货品种一并尝试作为期权品种加载（覆盖商品期权）")
    subscription_batch_size: int = Field(default=10, title="订阅批次大小")
    subscription_interval: float = Field(default=0.2, title="订阅批次间隔(秒)")
    subscription_backoff_factor: float = Field(default=1.0, title="订阅批次退避因子")
    subscribe_only_current_next_options: bool = Field(default=True, title="仅订阅指定月/指定下月期权（旧字段名）")
    subscribe_only_current_next_futures: bool = Field(default=True, title="仅订阅指定月/指定下月期货（旧字段名，仅限CFFEX IF/IH/IC）")
    enable_doc_examples: bool = Field(default=False, title="启用说明文档示例")
    pause_unsubscribe_all: bool = Field(default=True, title="暂停时退订所有行情")
    pause_force_stop_scheduler: bool = Field(default=True, title="暂停时强制停止调度器（resume时重启）")
    pause_on_stop: bool = Field(default=False, title="平台 on_stop 回调是否按暂停处理")
    history_minutes: int = Field(default=1440, title="历史K线拉取回看分钟数")
    log_file_path: str = Field(default="strategy_startup.log", title="本地日志文件路径")
    test_mode: bool = Field(default=False, title="测试模式：忽略开盘时间门控")
    auto_start_after_init: bool = Field(default=False, title="初始化后自动触发 on_start，避免卡在初始化状态")
    # 指定月/指定下月新增参数，兼容旧字段
    subscribe_only_specified_month_options: bool = Field(default=True, title="仅订阅指定月/指定下月期权")
    subscribe_only_specified_month_futures: bool = Field(default=True, title="仅订阅指定月/指定下月期货")
    specified_month: str = Field(default="", title="指定月合约代码")
    next_specified_month: str = Field(default="", title="指定下月合约代码")
    month_mapping: Dict[str, Any] = Field(default_factory=dict, title="品种指定月/指定下月映射")
    # 计算与信号参数
    allow_minimal_signal: bool = Field(default=False, title="允许微弱信号")
    signal_cooldown: int = Field(default=60, title="信号冷却时间(秒)")
    lots_min: int = Field(default=1, title="手数最小值")
    width_threshold: float = Field(default=4.0, title="交易信号宽度阈值")
    debug_output_interval: int = Field(default=180, title="收市调试模式下输出间隔(秒)")
    lots_max: int = Field(default=100, title="手数最大值")

    # 开仓/风控参数
    option_buy_lots_min: int = Field(default=1, title="期权买入开仓最小手数")
    option_buy_lots_max: int = Field(default=100, title="期权买入开仓最大手数")
    option_contract_multiplier: float = Field(default=10000, title="期权合约乘数（价格*乘数*手数）")
    position_limit_valid_hours_max: int = Field(default=720, title="开仓资金限额可设置的最大有效小时数")
    position_limit_default_valid_hours: int = Field(default=24, title="开仓资金限额默认有效小时数")
    position_limit_max_ratio: float = Field(default=0.2, title="开仓资金限额占总资金比例上限")
    position_limit_min_amount: float = Field(default=1000, title="开仓资金限额最小金额")
    option_order_price_type: str = Field(default="2", title="期权开仓委托价类型")
    option_order_time_condition: str = Field(default="3", title="期权开仓时间条件")
    option_order_volume_condition: str = Field(default="1", title="期权开仓成交量条件")
    option_order_contingent_condition: str = Field(default="1", title="期权开仓触发条件")
    option_order_force_close_reason: str = Field(default="0", title="期权开仓强平原因")
    option_order_hedge_flag: str = Field(default="1", title="期权开仓投机/套保标记")
    option_order_min_volume: int = Field(default=1, title="期权开仓最小成交量")
    option_order_business_unit: str = Field(default="1", title="期权开仓业务单元")
    option_order_is_auto_suspend: int = Field(default=0, title="期权开仓是否自动挂起")
    option_order_user_force_close: int = Field(default=0, title="期权开仓是否用户强平")
    option_order_is_swap: int = Field(default=0, title="期权开仓是否互换单")
    # 平仓参数
    close_take_profit_ratio: float = Field(default=1.5, title="止盈倍数（开仓价*倍数）")
    close_overnight_check_time: str = Field(default="14:58", title="隔夜仓检查时间(HH:MM)")
    close_daycut_time: str = Field(default="15:58", title="日内平仓时间(HH:MM)")
    close_max_hold_days: int = Field(default=3, title="最大持仓天数(>=则平仓)")
    close_overnight_loss_threshold: float = Field(default=-0.5, title="隔夜亏损平仓阈值(收益率)")
    close_overnight_profit_threshold: float = Field(default=4.0, title="隔夜盈利平仓阈值(收益率)")
    close_max_chase_attempts: int = Field(default=5, title="追单最大次数")
    close_chase_interval_seconds: int = Field(default=2, title="追单间隔秒数")
    close_chase_task_timeout_seconds: int = Field(default=30, title="追单任务超时秒数")
    close_delayed_timeout_seconds: int = Field(default=30, title="延迟平仓超时秒数")
    close_delayed_max_retries: int = Field(default=3, title="延迟平仓最大重试次数")
    close_order_price_type: str = Field(default="2", title="平仓委托价类型")

    output_mode: str = Field(default="debug", title="输出模式(open_debug|close_debug|trade|none)")
    auto_trading_enabled: bool = Field(default=False, title="自动交易开关")
    auto_trading: bool = Field(default=False, title="自动交易开关(兼容字段)")
    force_debug_on_start: bool = Field(default=True, title="启动时强制开启调试输出")
    ui_window_width: int = Field(default=320, title="UI窗口宽度")
    ui_window_height: int = Field(default=360, title="UI窗口高度")
    ui_font_large: int = Field(default=11, title="UI大字体字号")
    ui_font_small: int = Field(default=10, title="UI小字体字号")
    enable_output_mode_ui: bool = Field(default=True, title="是否启用输出模式UI")
    daily_summary_hour: int = Field(default=15, title="日终汇总小时")
    daily_summary_minute: int = Field(default=1, title="日终汇总分钟")
    trade_quiet: bool = Field(default=True, title="交易模式下减少输出")
    print_start_snapshots: bool = Field(default=False, title="启动时打印快照")

    def get(self, key, default=None):
        return getattr(self, key, default)

    trade_debug_allowlist: str = Field(default="", title="交易调试白名单(逗号分隔)")
    debug_disable_categories: str = Field(default="", title="禁用调试类别(逗号分隔)")
    debug_throttle_seconds: float = Field(default=0.0, title="调试输出节流时间(秒)")
    debug_throttle_map: Dict[str, float] = Field(default_factory=dict, title="调试节流映射")
    use_param_overrides_in_debug: bool = Field(default=False, title="调试模式下使用参数覆盖")
    param_override_table: str = Field(default="", title="参数覆盖表路径")
    param_edit_limit_per_month: int = Field(default=1, title="每月参数修改次数限制")
