# MODULE_ID: M1-002
"""配置层无依赖常量 — R1-2修复: 提取CACHE_TTL到无依赖模块，打破config_params↔strategy_config循环导入"""
import os


CACHE_TTL: float = 60.0  # R23-FR-01-FIX: 缓存TTL从00秒缩短至60秒，极端行情下减少过期数据风险
# R17-P2-CFG-02: 可配置化，建议通过环境变量CONFIG_CACHE_TTL_SEC覆盖

# R2-4/P2-21修复: 统一日志目录常量，消除8+处硬编码
DEFAULT_LOG_DIR: str = os.environ.get('DEFAULT_LOG_DIR', 'logs')
