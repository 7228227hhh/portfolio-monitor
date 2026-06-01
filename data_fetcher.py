"""
数据采集模块
封装币安期权API + WebSocket现货价格，统一输出DataFrame格式
"""
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

# ===== 你的原始逻辑，封装进函数 =====

# 全局变量（WebSocket现货价格）
BTC_current_spot_price = None
ETH_current_spot_price = None
BNB_current_spot_price = None
XRP_current_spot_price = None
DOGE_current_spot_price = None
SOL_current_spot_price = None
spot_price_ready = threading.Event()
_ws_started = False
_loop = None

# 静态数据缓存（期权合约基本信息）
_static_data = {}


def _load_static_data():
    """加载币安期权合约静态信息（只执行一次）"""
    global _static_data
    if _static_data:
        return _static_data

    info_url = "https://eapi.binance.com/eapi/v1/exchangeInfo"
    info_resp = requests.get(info_url)
    exchange_info = info_resp.json()

    for opt in exchange_info['optionSymbols']:
        _static_data[opt['symbol']] = {
            'strike': float(opt['strikePrice']),
            'expiryDate': opt['expiryDate'],
            'OptionType': opt['side'],
            'underlying': opt['underlying']
        }
    return _static_data


def _extract_underlying_from_symbol(symbol: str) -> str:
    """
    从期权symbol提取底层资产。
    例: BTC-260626-140000-C → BTC
    """
    return symbol.split('-')[0]


def start_websocket_thread():
    """在后台线程启动asyncio事件循环（只启动一次）"""
    global _ws_started
    if _ws_started:
        return
    _ws_started = True

    def run_loop():
        global _loop
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
        _loop.run_until_complete(subscribe_spot_price())

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()


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


def get_spot_price(symbol: str, timeout: int = 30) -> float:
    """
    获取最新现货价格。

    Args:
        symbol: 交易对，如 'BTCUSDT' 或 'BTC'
        timeout: 等待WebSocket就绪的超时时间（秒）
    """
    global BTC_current_spot_price, ETH_current_spot_price
    global BNB_current_spot_price, XRP_current_spot_price
    global DOGE_current_spot_price, SOL_current_spot_price

    if not symbol.endswith('USDT'):
        symbol_ws = f"{symbol.upper()}USDT"
    else:
        symbol_ws = symbol.upper()

    # 启动 WebSocket（只启动一次）
    if not spot_price_ready.is_set():
        start_websocket_thread()
        print("等待WebSocket连接...")
        spot_price_ready.wait(timeout)

    prefix = symbol_ws[:3]
    price_map = {
        'BTC': lambda: BTC_current_spot_price,
        'ETH': lambda: ETH_current_spot_price,
        'BNB': lambda: BNB_current_spot_price,
        'XRP': lambda: XRP_current_spot_price,
        'DOG': lambda: DOGE_current_spot_price,
        'SOL': lambda: SOL_current_spot_price,
    }

    getter = price_map.get(prefix)
    if getter is None:
        print(f"不支持的交易对: {symbol}")
        return None

    price = getter()
    if price is not None:
        return price

    # 等待最多50秒
    for _ in range(50):
        time.sleep(1)
        price = getter()
        if price is not None:
            return price

    print(f"错误: 等待50秒后仍未获取到 {symbol} 价格")
    return None


def fetch_options_data(symbols: list = None) -> pd.DataFrame:
    """
    从币安API拉取期权数据（你原来的完整逻辑）。

    Args:
        symbols: 标的列表（未使用，币安一次返回全部，后续按symbol过滤即可）

    Returns:
        标准化的DataFrame
    """
    # 1. 加载静态数据
    static_data = _load_static_data()

    # 2. 拉取mark数据
    print("正在获取币安期权数据...")
    mark_url = "https://eapi.binance.com/eapi/v1/mark"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    mark_resp = requests.get(mark_url, headers=headers, timeout=10)
    mark_data = mark_resp.json()
    print(f"获取到 {len(mark_data)} 个期权合约")

    # 3. 转换成DataFrame
    records = []
    now = datetime.now()

    for item in mark_data:
        symbol = item['symbol']
        if symbol not in static_data:
            continue

        static = static_data[symbol]
        expiry_ms = int(static['expiryDate'])
        expiry_date = datetime.fromtimestamp(expiry_ms / 1000)
        days_left = (expiry_date - now).total_seconds() / 86400

        if days_left < 1:
            continue

        # 提取底层资产用于获取现货价格
        underlying = _extract_underlying_from_symbol(symbol)
        spot = get_spot_price(underlying)

        records.append({
            'symbol': symbol,
            'underlying': underlying,
            'strike': static['strike'],
            'expiry_date': expiry_date,
            'days_left': days_left,
            'T': days_left / 365.0,
            'implied_vol': float(item.get('markIV', 0)),
            'delta': float(item.get('delta', 0)),
            'gamma': float(item.get('gamma', 0)),
            'vega': float(item.get('vega', 0)),
            'theta': float(item.get('theta', 0)),
            'option_type': static['OptionType'][0],  # 'C' or 'P'
            'mark_price': float(item.get('markPrice', 0)),
            'spot': spot if spot is not None else np.nan,
        })

    df = pd.DataFrame(records)

    # 4. 计算 log_moneyness
    df['log_moneyness'] = np.log(df['strike'] / df['spot'])

    print(f"清洗后: {len(df)} 个合约")
    return df

# ===== 兼容 mock 模式（当API不可用时） =====

def fetch_options_data_mock(symbols: list = None) -> pd.DataFrame:
    """
    模拟数据生成器（开发调试用，使用scipy BSM近似）
    当真实API不可用时使用。
    """
    from scipy.stats import norm

    if symbols is None:
        # 从config导入默认标的
        try:
            from config import SYMBOLS as default_symbols
            symbols = default_symbols
        except ImportError:
            symbols = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE"]

    np.random.seed(int(datetime.now().timestamp()) % 10000)

    spots = {
        "BTC": 73500, "ETH": 3200, "SOL": 180,
        "BNB": 620, "XRP": 0.55, "DOGE": 0.12
    }

    all_data = []

    for symbol in symbols:
        S = spots.get(symbol, 100)
        for days in [7, 14, 30, 60, 90]:
            T_val = days / 365.0
            expiry = pd.Timestamp.now() + pd.Timedelta(days=days)
            strikes = np.linspace(S * 0.8, S * 1.2, 9)
            atm_iv = 0.45 + np.random.normal(0, 0.02)

            for K in strikes:
                moneyness = np.log(K / S)
                iv = atm_iv + 0.15 * (moneyness ** 2) - 0.05 * moneyness
                iv = max(iv, 0.05)

                d1_val = (np.log(S / K) + (0.0 + 0.5 * iv ** 2) * T_val) / (iv * np.sqrt(T_val))
                call_delta = norm.cdf(d1_val)
                put_delta = call_delta - 1
                gamma_val = norm.pdf(d1_val) / (S * iv * np.sqrt(T_val))
                vega_val = S * norm.pdf(d1_val) * np.sqrt(T_val) / 100
                theta_val = -(S * norm.pdf(d1_val) * iv) / (2 * np.sqrt(T_val)) / 365

                all_data.append({
                    "symbol": f"{symbol}-{int(K)}-C",
                    "underlying": symbol,
                    "strike": K, "expiry_date": expiry,
                    "days_left": days, "T": T_val, "implied_vol": iv,
                    "delta": call_delta, "gamma": gamma_val,
                    "vega": vega_val, "theta": theta_val,
                    "option_type": "C",
                    "mark_price": max(S * call_delta * 0.02, 0.001),
                    "spot": S, "log_moneyness": moneyness,
                })
                all_data.append({
                    "symbol": f"{symbol}-{int(K)}-P",
                    "underlying": symbol,
                    "strike": K, "expiry_date": expiry,
                    "days_left": days, "T": T_val, "implied_vol": iv,
                    "delta": put_delta, "gamma": gamma_val,
                    "vega": vega_val, "theta": theta_val,
                    "option_type": "P",
                    "mark_price": max(S * abs(put_delta) * 0.02, 0.001),
                    "spot": S, "log_moneyness": moneyness,
                })

    return pd.DataFrame(all_data)