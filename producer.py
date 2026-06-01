"""
Kafka生产者
定时拉取数据 → 计算指标 → 发送到Kafka
"""
import json
import time
import signal
import sys
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    SYMBOLS, FETCH_INTERVAL,
)
from data_fetcher import fetch_options_data_mock,fetch_options_data  # 有接口后换成 fetch_options_data
from greeks_calculator import calculate_all_metrics
from gamma_combo import build_combo_positions, calculate_net_greeks

running = True


def signal_handler(sig, frame):
    global running
    print("\n[Producer] 收到停止信号，正在退出...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_kafka_producer() -> KafkaProducer:
    """创建Kafka生产者"""
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type="gzip",
                linger_ms=50,
                batch_size=32768,
                max_request_size=1048576,
            )
            print(f"[Producer] Kafka连接成功: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[Producer] Kafka未就绪，第{attempt+1}次重试...")
            time.sleep(2)
        except Exception as e:
            print(f"[Producer] Kafka连接异常: {e}，第{attempt+1}次重试...")
            time.sleep(2)
    raise RuntimeError("无法连接Kafka，请确认docker-compose已启动")


def safe_float(value, default=None):
    """安全转换float，处理nan"""
    if value is None:
        return default
    try:
        if isinstance(value, float) and (value != value):  # nan check
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def produce_loop():
    """主生产循环"""
    producer = create_kafka_producer()
    print(f"[Producer] 开始采集数据，标的: {SYMBOLS}，间隔: {FETCH_INTERVAL}s")

    while running:
        try:
            ts = datetime.now().isoformat()
            print(f"\n[{ts}] 拉取数据...")

            # ============================================
            # 有真实接口后替换下面这行
            df_all = fetch_options_data(SYMBOLS)
            # df_all = fetch_options_data(SYMBOLS)
            # ============================================

            for symbol in SYMBOLS:
                df_sym = df_all[df_all["underlying"] == symbol]

                if df_sym.empty:
                    print(f"  [{symbol}] 无数据，跳过")
                    continue

                metrics = calculate_all_metrics(df_sym) #布尔索引
                positions = build_combo_positions(df_sym) #布尔索引
                combo_greeks = calculate_net_greeks(positions)

                message = {
                    "ts": ts,
                    "symbol": symbol,
                    "spot": safe_float(metrics["spot"]),
                    "metrics": {
                        "atm_iv": safe_float(metrics["atm_iv"]),
                        "skew_25d": safe_float(metrics["skew_25d"]),
                        "term_slope": safe_float(metrics["term_slope"]),
                        "gex": safe_float(metrics["gex"], 0),
                        "dix": safe_float(metrics["dix"], 0),
                    },
                    "combo_greeks": {
                        "net_delta": safe_float(combo_greeks["net_delta"], 0),
                        "net_gamma": safe_float(combo_greeks["net_gamma"], 0),
                        "net_vega": safe_float(combo_greeks["net_vega"], 0),
                        "net_theta": safe_float(combo_greeks["net_theta"], 0),
                    },
                }

                producer.send(KAFKA_TOPIC, key=symbol.encode(), value=message)

                iv_str = f"IV={metrics['atm_iv']:.4f}" if safe_float(metrics["atm_iv"]) is not None else "IV=N/A"
                print(f"  [{symbol}] Spot={metrics['spot']:.1f}, {iv_str}, "
                      f"GEX={metrics['gex']:.1e}, 组合Δ={combo_greeks['net_delta']:.4f}")

            for _ in range(FETCH_INTERVAL):
                if not running:
                    break
                time.sleep(1)#这段的作用是让循环每秒钟执行一次休眠，程序每fetch_interval间隔后重新执行一次


        except Exception as e:
            print(f"[Producer] 错误: {e}")
            import traceback
            traceback.print_exc() #打印完整调用栈+异常类型+异常消息
            time.sleep(3)

    producer.flush() #flush的特点：1.阻塞等待：直到所有待发送消息都被确认发送完成 2.不关闭连接：flush 后 Producer 仍然可用，可以继续发送消息 3.确保消息送达：保证调用前所有 send 的消息都真正到了 Broker
    producer.close()
    print("[Producer] 已停止")


if __name__ == "__main__":
    produce_loop()