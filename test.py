import pandas as pd
import numpy as np
from datetime import datetime
import json
import requests
from curl_cffi import requests as rq
import threading
import asyncio
import time
import sys

# 初始化全局变量
web_started = False
BTC_current_spot_price = None
ETH_current_spot_price = None
BNB_current_spot_price = None
XRP_current_spot_price = None
DOGE_current_spot_price = None
SOL_current_spot_price = None

spot_price_ready = threading.Event()


def start_websocket_thread():
    global web_started
    if web_started:
        return
    web_started = True

    def run_loop():
        global _loop
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
        _loop.run_until_complete(subscribe_spot_price())  # 改用 run_until_complete

    # 创建并启动线程 - 注意缩进，在 run_loop 定义之后
    thread = threading.Thread(target=run_loop, daemon=False)
    thread.start()
    return thread


async def subscribe_spot_price():
    """订阅实时价格（WebSocket）"""
    from websockets_proxy import Proxy, proxy_connect

    uri = "wss://stream.binance.com:9443/ws"
    proxy = Proxy.from_url("http://127.0.0.1:1080")

    async with proxy_connect(uri, proxy=proxy) as ws:
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": [
                "btcusdt@trade", "ethusdt@trade", "bnbusdt@trade",
                "solusdt@trade", "xrpusdt@trade", "dogeusdt@trade"
            ],
            "id": 1
        }
        await ws.send(json.dumps(subscribe_msg))
        print("WebSocket: 已订阅合约实时价格")

        async for message in ws:
            global data
            data = json.loads(message)
            if 'p' in data:
                global BTC_current_spot_price, ETH_current_spot_price
                global BNB_current_spot_price, XRP_current_spot_price
                global DOGE_current_spot_price, SOL_current_spot_price

                s = data['s']
                p = float(data['p'])

                if s == 'BTCUSDT':
                    BTC_current_spot_price = p
                elif s == 'ETHUSDT':
                    ETH_current_spot_price = p
                elif s == 'BNBUSDT':
                    BNB_current_spot_price = p
                elif s == 'XRPUSDT':
                    XRP_current_spot_price = p
                elif s == 'DOGEUSDT':
                    DOGE_current_spot_price = p
                elif s == 'SOLUSDT':
                    SOL_current_spot_price = p

                # 检查是否六个价格都已就绪
                if all(v is not None for v in [
                    BTC_current_spot_price, ETH_current_spot_price,
                    BNB_current_spot_price, XRP_current_spot_price,
                    DOGE_current_spot_price, SOL_current_spot_price
                ]):
                    if not spot_price_ready.is_set():
                        spot_price_ready.set()
                        print("所有价格已就绪！")


# 启动 WebSocket 线程
if __name__ == "__main__":
    start_websocket_thread()

    # 等待价格就绪
    spot_price_ready.wait()
    print("可以开始交易了...")

    # 保持主线程运行，这行必须要加
    while True:
        time.sleep(1)
        print(data)