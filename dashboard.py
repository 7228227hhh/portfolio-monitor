# dashboard.py - 简化版（删除侧边栏实时快照和刷新频率）
"""
仪表盘模块 - 消费Kafka消息并实时可视化监控指标
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from kafka import KafkaConsumer
import json
import threading
import time
from datetime import datetime
from collections import deque
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SYMBOLS

# 页面配置
st.set_page_config(
    page_title="期权指标实时监控仪表盘",
    page_icon="📊",
    layout="wide"
)

# ========== 全局数据存储 ==========
HISTORY_SIZE = 100

# 每个标的最新数据
latest_data = {symbol: {} for symbol in SYMBOLS}

# 每个标的历史数据
history_data = {symbol: deque(maxlen=HISTORY_SIZE) for symbol in SYMBOLS}#这里history用deque存储，一次存储的长度是maxlen=HISTORY_SIZE

# 数据锁
data_lock = threading.Lock()

# 消费者状态
consumer_running = False

# 刷新计数器
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0

# 固定刷新频率（秒）
REFRESH_RATE = 2


# ========== 辅助函数 ==========
def format_spot_price(spot: float) -> str:
    """根据价格量级智能格式化现货价格"""
    if spot is None or (isinstance(spot, float) and spot != spot): #逻辑运算符里面and在or之前，spot!=spot是为了排除Nan
        return "N/A"
    if spot >= 1000:
        return f"${spot:,.2f}"#这里面,是指的千分位，.2f即保留两位小数字
    elif spot >= 1:
        return f"${spot:,.4f}"
    elif spot >= 0.001:
        return f"${spot:.6f}"
    else:
        return f"${spot:.8f}"


def format_greek(value: float) -> str:
    """格式化Greek值"""
    if value is None or (isinstance(value, float) and value != value):
        return "N/A"
    if abs(value) >= 1000:
        return f"{value:.2e}"
    elif abs(value) >= 1:
        return f"{value:.4f}"
    else:
        return f"{value:.6f}"


def get_latest(symbol, key, default=None):
    """安全获取最新数据"""
    with data_lock:#这个是with语句在线程锁的应用，锁竞争机制：多个线程抢一个锁，导致部分线程被迫等待的情况,with data_lock就是尝试获取锁，这个写法就是额外在函数外写一个threading.Lock(),作为全局锁，保护所有函数内的读写操作
        data = latest_data.get(symbol, {})
        if key == 'metrics':
            return data.get('metrics', {})
        elif key == 'combo_greeks':
            return data.get('combo_greeks', {})
        else:
            return data.get(key, default)


# ========== Kafka 消费者线程 ==========
def start_consumer():
    """启动Kafka消费者线程"""
    global consumer_running

    def consume():
        global consumer_running
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, #Kafka的集群地址（broker的地址，这里面只用了一个）
                auto_offset_reset='latest', #没有偏移量的时候从哪里开始读，偏移量可以理解成书的页码
                enable_auto_commit=True,#是否自动提交偏移量
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),#消息值反序列化：把kafka种的字节数据给转化成python对象
                key_deserializer=lambda x: x.decode('utf-8') if x else None,#消息键反序列化
            )
            """注意，Kafka 会把你 Producer 生产的数据保存下来，而且是自动的、持久化到磁盘的保存。你今天 Producer 发出去的数据，Kafka 默认会帮你在服务器上保留一周（7天）。

⚙️ Kafka 是如何存储数据的？
当你启动 Producer 发送消息时，Kafka 并不会直接把数据扔给 Consumer 就删除，而是以追加的方式写入到硬盘上的日志文件（log文件）里。这个行为就像你在一个本子上不断往后面记录新内容，不会去改动前面的字。

为了让你更清楚地了解 Kafka 的“自动保存”，我把它总结成了一个表格：

存储特性	具体说明	为什么重要
自动持久化	Producer 发送的消息会被立刻写入磁盘，而不只是在内存里停留。	断电/重启不丢数据：即使服务器宕机或重启，你之前发送的数据依然存在。
存储路径	数据会保存在 Broker 配置文件 server.properties 中 log.dirs 参数指定的文件夹下。	方便管理：你可以找到数据文件，也可以将目录配置到有足够空间的大硬盘上。
默认保存时长	默认保存 168 小时，也就是 7 天。	自动清理：7 天前的数据会被自动删除，你不用操心硬盘被写满。
存储上限	单个日志段文件的默认大小是 1GB (log.segment.bytes=1073741824)。	防止文件过大：当一个文件达到 1GB 后，Kafka 会关闭它并创建新文件来写入后续数据。
🧠 结合 Kafka 消费者工作原理
理解了这一点，就能明白你之前对消费者配置的疑问了：

数据持久存在：auto_offset_reset 配置之所以能生效，就是因为历史数据（只要没过保留期）还在硬盘里存着。

“滞后”的含义：监控工具中显示的 Lag（滞后），指的就是 Consumer 当前的 offset 和硬盘里最新一条消息的 offset 之间的差距。Lag 越大，说明消费者处理速度越跟不上生产速度。
这里面需要注意：
重新打开 Producer（生产者）不会导致 Consumer（消费者）从头开始接收。

只要你的 Consumer 使用的是相同的 group.id，并且 Kafka 中还保留着 offset = 50 这个位置，Consumer 重启后会从 51 接着读，绝对不会去读 earliest（也就是 0）。

下面我分三个关键点来解释为什么：

1. Producer 和 Consumer 是隔离的
Producer 只管往尾巴写数据（offset 不断增大）。

Consumer 只管根据自己记录的“书签”（offset）去读。

因果关系：Producer 重启、关掉、或者换个电脑运行，对 Consumer 的“书签”没有任何影响。Consumer 根本不关心 Producer 是否在线。

2. auto_offset_reset 只在一种情况下触发
auto_offset_reset 并不是每次重启都会触发的，它只在找不到提交的 offset 时才会生效。

什么时候找不到？

第一次启动：这个 group.id 从来没有连接过 Kafka，服务器上没有记录。
过期被删：Consumer 停机太久（比如停机了 10 天），Kafka 根据保留策略（默认 7 天）把 offset = 50 那条数据连同它所在的文件一起删掉了。服务器发现“你上次读到 50？但这个位置已经没东西了”，于是触发重置。
            """

            consumer_running = True
            print(f"[Dashboard] ✅ Kafka消费者已启动")

            for msg in consumer:
                symbol = msg.key
                data = msg.value

                with data_lock:
                    latest_data[symbol] = data
                    record = {
                        'ts': data.get('ts'),
                        'spot': data.get('spot'),
                        'atm_iv': data.get('metrics', {}).get('atm_iv'),
                        'skew_25d': data.get('metrics', {}).get('skew_25d'),
                        'gex': data.get('metrics', {}).get('gex'),
                        'dix': data.get('metrics', {}).get('dix'),
                        'net_delta': data.get('combo_greeks', {}).get('net_delta'),
                        'net_gamma': data.get('combo_greeks', {}).get('net_gamma'),
                        'net_vega': data.get('combo_greeks', {}).get('net_vega'),
                        'net_theta': data.get('combo_greeks', {}).get('net_theta'),
                    }
                    history_data[symbol].append(record)

                print(f"[Dashboard] 📥 收到 {symbol}: spot={data.get('spot')}")

        except Exception as e:
            print(f"[Dashboard] ❌ 消费者错误: {e}")
            consumer_running = False

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()


# ========== 缓存的数据获取函数 ==========
@st.cache_data(ttl=2, show_spinner=False)#这里ttl是time to live，缓存有效期，show_spinner=False代表不加载动画,这个装饰器作用于函数的返回值df
def get_history_df(symbol: str):
    """获取历史数据DataFrame（带缓存）"""
    with data_lock:
        data_list = list(history_data.get(symbol, []))

    if not data_list:
        return pd.DataFrame()

    df = pd.DataFrame(data_list)
    if df.empty:
        return pd.DataFrame()

    if 'ts' in df.columns:
        df['timestamp'] = pd.to_datetime(df['ts'])

    return df
"""这里装饰器的作用是：
df1=get_history_df("BTC")
df2=get_history_df("BTC")
如果df2和df1之间间隔在2秒以内，那么会从缓存里面直接读，如果超过2s就会重新调用并且缓存
"""

# ========== UI 组件 ==========
def render_sidebar():
    """渲染侧边栏（只显示连接状态）"""
    st.sidebar.title("📊 OptMonitor")
    st.sidebar.markdown("---")

    if consumer_running:
        st.sidebar.success("✅ Kafka 已连接")
    else:
        st.sidebar.warning("⏳ 连接中...")

    st.sidebar.markdown(f"**Topic:** `{KAFKA_TOPIC}`")
    st.sidebar.markdown(f"**Broker:** `{KAFKA_BOOTSTRAP_SERVERS}`")


def render_main_overview():
    """渲染主概览面板 - 6个标的卡片"""
    st.title("📊 期权指标实时监控")

    # 第一行：BTC, ETH, BNB
    col1, col2, col3 = st.columns(3)

    for symbol, col in zip(SYMBOLS[:3], [col1, col2, col3]):
        with col:
            spot = get_latest(symbol, 'spot')
            metrics = get_latest(symbol, 'metrics')

            if spot and spot == spot:
                iv = metrics.get('atm_iv', 0)
                skew = metrics.get('skew_25d', 0)
                gex = metrics.get('gex', 0)

                st.metric(
                    label=f"**{symbol}**",
                    value=format_spot_price(spot),
                    delta=f"IV: {iv:.1%}" if iv else None
                )
                st.caption(f"📐 偏度: {skew:.1%} | ⚡ GEX: {gex:.2e}")
            else:
                st.info(f"⏳ 等待 {symbol} 数据...")

    # 第二行：XRP, DOGE, SOL
    col4, col5, col6 = st.columns(3)

    for symbol, col in zip(SYMBOLS[3:], [col4, col5, col6]):
        with col:
            spot = get_latest(symbol, 'spot')
            metrics = get_latest(symbol, 'metrics')

            if spot and spot == spot:
                iv = metrics.get('atm_iv', 0)
                skew = metrics.get('skew_25d', 0)
                gex = metrics.get('gex', 0)

                st.metric(
                    label=f"**{symbol}**",
                    value=format_spot_price(spot),
                    delta=f"IV: {iv:.1%}" if iv else None
                )
                st.caption(f"📐 偏度: {skew:.1%} | ⚡ GEX: {gex:.2e}")
            else:
                st.info(f"⏳ 等待 {symbol} 数据...")


def render_price_trends(counter: int):
    """渲染所有标的的现货价格走势"""
    st.markdown("---")
    st.subheader("📈 现货价格走势")

    for symbol in SYMBOLS:
        df = get_history_df(symbol)

        if df.empty or 'spot' not in df.columns:
            st.info(f"⏳ 等待 {symbol} 价格数据...")
            continue

        valid_df = df[df['spot'].notna()].copy()
        if valid_df.empty:
            st.info(f"⏳ 等待 {symbol} 有效数据...")
            continue

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=valid_df['timestamp'],
            y=valid_df['spot'],
            mode='lines',
            name=symbol,
            line=dict(color='#00ff88', width=2)
        ))

        # 根据价格量级设置Y轴格式
        spot_price = valid_df['spot'].iloc[-1] if not valid_df.empty else 0
        if spot_price >= 1000:
            tick_format = '$,.0f'
        elif spot_price >= 1:
            tick_format = '$,.4f'
        else:
            tick_format = '$,.6f'

        fig.update_layout(
            title=f"{symbol} 价格走势",
            height=250,
            template="plotly_dark",
            yaxis=dict(tickformat=tick_format),
            margin=dict(l=0, r=0, t=40, b=0)
        )

        st.plotly_chart(fig, use_container_width=True, key=f"price_ts_{symbol}_{counter}")


def render_greeks_for_symbol(symbol: str, counter: int):
    """为单个标的渲染Greeks仪表盘"""
    combo = get_latest(symbol, 'combo_greeks')

    if not combo:
        st.info(f"⏳ 等待 {symbol} Greeks 数据...")
        return

    net_delta = combo.get('net_delta', 0)
    net_gamma = combo.get('net_gamma', 0)
    net_vega = combo.get('net_vega', 0)
    net_theta = combo.get('net_theta', 0)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        delta_color = "🟢" if net_delta > 0 else "🔴" if net_delta < 0 else "⚪"
        st.metric(
            label=f"{delta_color} Delta",
            value=format_greek(net_delta),
            help="价格每变动$1，组合价值的变化"
        )

    with col2:
        st.metric(
            label=f"🟡 Gamma",
            value=format_greek(net_gamma),
            help="价格每变动$1，Delta的变化量"
        )

    with col3:
        st.metric(
            label=f"🔵 Vega",
            value=format_greek(net_vega),
            help="波动率每变动1%，组合价值的变化"
        )

    with col4:
        st.metric(
            label=f"🟠 Theta",
            value=format_greek(net_theta),
            help="每天时间衰减带来的价值变化"
        )


def render_greeks_dashboard(counter: int):
    """渲染所有标的的Greeks仪表盘"""
    st.markdown("---")
    st.subheader("🎯 组合 Greeks (按标的)")

    for symbol in SYMBOLS:
        with st.expander(f"📈 {symbol} - Greeks详情", expanded=True):
            render_greeks_for_symbol(symbol, counter)

            # 添加Delta历史走势
            df = get_history_df(symbol)
            if not df.empty and 'net_delta' in df.columns:
                valid_df = df[df['net_delta'].notna()].copy()
                if not valid_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=valid_df['timestamp'],
                        y=valid_df['net_delta'],
                        mode='lines',
                        name='Delta',
                        line=dict(color='#00ff88', width=1.5)
                    ))
                    fig.update_layout(
                        title="Delta 走势",
                        height=200,
                        template="plotly_dark",
                        margin=dict(l=0, r=0, t=30, b=0)
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"delta_ts_{symbol}_{counter}")


def render_data_table(counter):
    """渲染实时数据表格"""
    st.markdown("---")
    st.subheader("📋 实时数据明细")

    table_data = []
    for symbol in SYMBOLS:
        spot = get_latest(symbol, 'spot')
        metrics = get_latest(symbol, 'metrics')
        combo = get_latest(symbol, 'combo_greeks')

        if spot and spot == spot:
            table_data.append({
                "标的": symbol,
                "现货": format_spot_price(spot),
                "ATM IV": f"{metrics.get('atm_iv', 0):.1%}" if metrics.get('atm_iv') and metrics.get(
                    'atm_iv') == metrics.get('atm_iv') else "N/A",
                "偏度": f"{metrics.get('skew_25d', 0):.1%}" if metrics.get('skew_25d') and metrics.get(
                    'skew_25d') == metrics.get('skew_25d') else "N/A",
                "GEX": f"{metrics.get('gex', 0):.2e}",
                "Delta": format_greek(combo.get('net_delta', 0)),
                "Gamma": format_greek(combo.get('net_gamma', 0)),
                "Vega": format_greek(combo.get('net_vega', 0)),
                "Theta": format_greek(combo.get('net_theta', 0)),
            })

    if table_data:
        st.dataframe(
            pd.DataFrame(table_data),
            use_container_width=True,
            hide_index=True,
            key=f"data_table_{counter}"
        )
    else:
        st.info("⏳ 等待数据...")


# ========== 主函数 ==========
def main():
    # 启动消费者
    if 'consumer_started' not in st.session_state:
        start_consumer()
        st.session_state.consumer_started = True
        time.sleep(1)

    # 侧边栏（只显示连接状态）
    render_sidebar()

    # 自动刷新容器
    placeholder = st.empty()

    while True:
        st.session_state.refresh_counter += 1
        counter = st.session_state.refresh_counter

        with placeholder.container():
            render_main_overview()
            render_price_trends(counter)
            render_greeks_dashboard(counter)
            render_data_table(counter)

            # 显示更新时间
            with data_lock:
                last_ts = None
                for symbol in SYMBOLS:
                    ts = latest_data.get(symbol, {}).get('ts')
                    if ts and (not last_ts or ts > last_ts):
                        last_ts = ts
            if last_ts:
                st.caption(f"📅 最后更新: {last_ts[11:19]}")

        time.sleep(REFRESH_RATE)


if __name__ == "__main__":
    main()