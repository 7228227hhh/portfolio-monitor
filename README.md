
---

### 📄 OptMonitor 技术手册

#### 📋 项目简介
**OptMonitor** 是一个基于 Python 的实时期权监控系统，专为加密货币期权（如币安合约）设计。系统通过 **WebSocket** 获取毫秒级现货价格，结合 **REST API** 拉取期权链数据，利用 **Kafka** 进行消息队列解耦，最终通过 **Streamlit** 实现交互式可视化仪表盘。

**核心功能：**
*   **多资产监控**：支持 BTC, ETH, BNB, SOL, XRP, DOGE 等主流币种。
*   **全市场指标**：实时计算 ATM 隐含波动率 (IV)、偏度 (Skew)、期限结构、Gamma Exposure (GEX)、Delta Exposure (DIX)。
*   **组合策略分析**：支持自定义期权组合（如 Straddle, Iron Condor），实时计算净 Greeks（Delta, Gamma, Vega, Theta）。
*   **高可用架构**：利用 Kafka 实现生产消费分离，支持多终端订阅。

#### 🏗️ 系统架构
系统采用典型的生产者-消费者模式，数据流如下：
```text
[现货 WS] + [期权 API] → [Producer] → [Kafka] → [Consumer] → [Dashboard]
  ↓                          ↓               ↓            ↓
实时价格               指标计算       消息队列      实时可视化
```

**模块清单：**

| 模块 | 文件 | 职责 |
| :--- | :--- | :--- |
| **配置中心** | `config.py` | 管理 Kafka 地址、标的列表、刷新间隔及组合策略配置。 |
| **数据采集** | `data_fetcher.py` | 双通道采集：WebSocket 获取现货 + REST API 获取期权全量数据。 |
| **指标引擎** | `greeks_calculator.py` | 核心算法：计算 GEX, DIX, IV, Skew 等市场情绪指标。 |
| **组合计算器** | `gamma_combo.py` | 基于预设策略计算组合的净风险敞口。 |
| **生产者** | `producer.py` | 定时任务调度，数据清洗后推送到 Kafka。 |
| **仪表盘** | `dashboard.py` | Streamlit 前端，消费 Kafka 消息并渲染图表。 |

#### 🚀 快速开始指南

**1. 环境准备**
*   Python 3.8+
*   Kafka 服务（需提前启动）
*   Docker Compose (可选，用于快速部署 Kafka)

**2. 依赖安装**
```bash
pip install -r requirements.txt
```
*关键依赖：* `streamlit`, `kafka-python`, `pandas`, `numpy`, `plotly`, `curl_cffi`, `websockets-proxy`, `scipy`

**3. 启动服务**
*   **启动 Kafka** (若使用 Docker)：
       ```bash
       docker-compose up -d
       ```
   *   **运行生产者** (数据采集与计算)：
       ```bash
       python producer.py
       ```
       *注：生产者默认每 5 秒拉取一次数据，发送至 Topic `option.metrics.realtime`。*
   *   **运行仪表盘**：
       ```bash
       streamlit run dashboard.py
       ```
       访问地址：[http://localhost:8501](http://localhost:8501)

#### 📊 核心指标解读

**1. 全市场指标 (Market Metrics)**

| 指标 | 含义 | 交易逻辑 |
| :--- | :--- | :--- |
| **Spot** | 现货价格 | 实时标的价格，作为定价基准。 |
| **ATM IV** | 平值隐含波动率 | 衡量市场对未来波动的预期，通常用于跨式策略定价。 |
| **Skew_25D** | 25-Delta 偏度 | `IV(25Δ Put) - IV(25Δ Call)`。正值越大，说明市场对下跌风险的担忧越重（"恐惧"）。 |
| **Term Slope** | 期限结构斜率 | `近期 ATM IV - 远期 ATM IV`。正值（Contango）利于卖方，负值（Backwardation）暗示短期事件风险。 |
| **GEX** | Gamma Exposure | 做市商的对冲压力指标。**正值**意味着做市商低买高卖，稳定市场；**负值**意味着追涨杀跌，放大波动。 |
| **DIX** | Delta Exposure | 市场整体的方向性偏好。 |

**2. 组合 Greeks (Combo Greeks)**
系统默认预设策略：**Short Straddle + OTM Protection** (卖出跨式 + 蝶式保护)
*   **Net Delta**：价格每变动 $1，组合价值的变化。
*   **Net Gamma**：价格每变动 $1，Delta 的变化量（加速度）。
*   **Net Vega**：波动率每变动 1%，组合价值的变化（卖权 Vega 为负）。
*   **Net Theta**：每天时间衰减带来的价值变化（卖权 Theta 为正，赚取时间价值）。

#### ⚙️ 配置与调试

**1. 代理配置 (关键)**
根据您提供的网页信息，如果您在本地使用了代理工具（如 Clash, V2Ray），请务必在 `data_fetcher.py` 中修改代理地址，否则无法连接币安 API：

```python
# 修改前/默认
# proxy = Proxy.from_url("http://127.0.0.1:7890")

# 修改后 (匹配您的本地代理端口)
proxy = Proxy.from_url("http://127.0.0.80")
```

**2. Kafka 持久化**
*   Kafka 默认保留消息 7 天。
*   消费者重启后，会从上次提交的 Offset 继续消费，确保数据不丢失。

**3. 常见问题排查**
*   **Q: Kafka 连接失败？**
    *   A: 检查 `config.py` 中的 `KAFKA_BOOTSTRAP_SERVERS` 是否为 `localhost:9092`，确认 Docker 容器或本地服务已启动。
*   **Q: Dashboard 价格不更新/显示 NaN？**
    *   A: 检查 Producer 日志是否有 `sent to kafka` 的打印；检查网络是否能访问 Binance API。
*   **Q: WebSocket 报错？**
    *   A: 重点检查代理配置是否正确，或者尝试更换 User-Agent。

#### 📂 项目结构
```text
opt_monitor/
├── config.py              # 全局配置
├── data_fetcher.py        # 数据采集
├── greeks_calculator.py   # 指标计算
├── gamma_combo.py         # 组合计算
├── producer.py            # 主程序入口
├── dashboard.py           # 可视化前端
└── requirements.txt       # 依赖列表
```

> **注意**：本项目目前为内部研究用途，代码仅供参考，不构成任何投资建议。市场有风险，交易需谨慎。