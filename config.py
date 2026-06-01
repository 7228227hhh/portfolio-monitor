"""
全局配置
"""
from datetime import datetime

# ===== Kafka =====
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "option.metrics.realtime"

# ===== 数据采集 =====

# 监控的6个标的
SYMBOLS = ["BTC", "ETH", "BNB", "XRP", "DOGE", "SOL"]

# 数据拉取间隔（秒）
FETCH_INTERVAL = 5

# ===== 仪表盘 =====
DASHBOARD_REFRESH_MS = 1000

# ===== Gamma组合预设 =====
DEFAULT_COMBO_CONFIG = {
    "name": "Short Straddle + OTM Protection",
    "legs": [
        {"type": "SHORT", "option": "ATM_STRADDLE", "ratio": 1},
        {"type": "LONG",  "option": "OTM_PUT_90",    "ratio": 2},
        {"type": "LONG",  "option": "OTM_CALL_110",  "ratio": 2},
    ]
}

# ===== DolphinDB（预留） =====
DOLPHINDB_HOST = "localhost"
DOLPHINDB_PORT = 8848
DOLPHINDB_USER = "admin"
DOLPHINDB_PASS = "123456"