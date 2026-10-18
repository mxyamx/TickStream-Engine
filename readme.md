# TickStream Engine ⚡
> **Real-Time High-Frequency Market Infrastructure & Microstructure Anomaly Monitor**

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![WebSockets](https://img.shields.io/badge/WebSockets-Async-010101?style=for-the-badge&logo=socketdotio&logoColor=white)
![HTML5](https://img.shields.io/badge/UI-DRW--Inspired_Design-E34F26?style=for-the-badge&logo=html5&logoColor=white)

---

## 📌 Executive Summary

**TickStream Engine** is an event-driven, low-latency quantitative data pipeline designed to ingest, process, and analyze high-frequency trade ticks from cryptocurrency exchange feeds (`BTC/USDT` order flow). 

Built to handle fast market regimes, the microservice ingests asynchronous WebSocket market feeds, calculates dynamic sliding-window statistics ($O(1)$ time complexity), and executes real-time volatility anomaly detection using a multi-factor Z-Score filter with noise floor dampening. Processed telemetry is broadcast to an institutional, DRW-styled web dashboard over a full-duplex WebSocket stream.

---

## 📐 Mathematical Framework & Anomaly Signal

### 1. Dynamic Rolling Statistics
Given a stream of real-time execution price ticks $P = \{p_1, p_2, \dots, p_t\}$, the engine maintains a sliding window of size $N = 50$:

* **Rolling Mean ($\mu_t$):**
$$\mu_t = \frac{1}{N} \sum_{i=0}^{N-1} p_{t-i}$$

* **Rolling Sample Standard Deviation ($\sigma_t$):**
$$\sigma_t = \sqrt{\frac{1}{N-1} \sum_{i=0}^{N-1} (p_{t-i} - \mu_t)^2}$$

### 2. Standardized Z-Score Deviation
To measure price displacement relative to recent volatility:
$$Z_t = \frac{p_t - \mu_t}{\sigma_t + \epsilon}$$
*(where $\epsilon = 10^{-8}$ prevents division-by-zero during low-volatility liquidity freezes).*

### 3. Dual-Condition Noise Floor Filter
To eliminate false-positive volatility alerts during tight sideways consolidation, the state evaluator enforces both a normalized deviation threshold and a percentage change floor:

$$\text{State}(t) = \begin{cases} \text{VOLATILITY\_ALERT}, & \text{if } |Z_t| > 2.5 \quad \text{AND} \quad \frac{|p_t - \mu_t|}{\mu_t} \ge 0.0003 \ (0.03\%) \\ \text{NORMAL}, & \text{otherwise} \end{cases}$$

---

## 🏗 System Architecture & Telemetry Flow

```
   [ Exchange Market Data ]
             │ (Coinbase WS: btcusdt@trade)
             ▼
┌──────────────────────────────────────────────┐
│         Async Ingestion Layer                │
│  - Non-blocking asyncio event loop          │
│  - Network disconnect auto-reconnect logic   │
└────────────────────┬─────────────────────────┘
                     │ Raw Trade Ticks (JSON)
                     ▼
┌──────────────────────────────────────────────┐
│       Quantitative Processing Engine         │
│  - Ring buffer (collections.deque, maxlen=50)│
│  - O(1) sliding window updates               │
│  - Dual-condition Z-Score & floor filter    │
└────────────────────┬─────────────────────────┘
                     │ Processed Metric Payload
                     ▼
┌──────────────────────────────────────────────┐
│         FastAPI Broadcast Server             │
│  - Pub/Sub WebSocket Connection Manager      │
│  - Real-time client fan-out                  │
└────────────────────┬─────────────────────────┘
                     │ Telemetry Payload Streams
                     ▼
┌──────────────────────────────────────────────┐
│      DRW-Styled Editorial Dashboard          │
│  - Zero-dependency HTML5 / Vanilla CSS / JS  │
│  - Live Trade Stream buffer & state indicator│
└──────────────────────────────────────────────┘
```

---

## 🧠 Technical Hurdles & Engineering Solutions

### 1. False Positives in Low-Volatility Sideways Regimes
* **Issue:** In tight, low-volatility sideways markets, sample standard deviation ($\sigma_t$) shrinks toward zero. Consequently, tiny price moves (e.g., a $0.05 shift on an $80,000 asset) generated exaggerated Z-Scores ($|Z_t| > 3.0$), causing continuous false volatility alarms.
* **Solution:** Engineered a dual-condition signal filter. The state evaluator requires both $|Z_t| > 2.5$ **AND** an absolute price relative delta $\Delta \ge 0.03\%$. This preserves sensitivity to legitimate price breaks while suppressing microstructural noise.

### 2. Event-Loop Blocking Under High-Velocity Ticks
* **Issue:** Synchronous network or computation tasks within the WebSocket feed loop introduced frame drops and socket latency buildup under heavy order flow.
* **Solution:** Structured the ingestion pipeline around Python's native `asyncio` loop, leveraging `collections.deque(maxlen=50)` for constant-time $O(1)$ memory buffer ops and non-blocking socket message handlers.

---

## 🚀 Run it locally

### Prerequisites
* Python 3.10+
* Modern Web Browser (Chrome, Firefox, Safari)

### 1. Clone & Setup Environment
```bash
git clone https://github.com/mxyamx/TickStream-Engine.git
cd TickStream-Engine

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install fastapi uvicorn websockets
```

### 2. Run the Engine & Server
```bash
# Start FastAPI WebSocket Server & Ingestion Worker
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

### 3. Open Dashboard
Open `index.html` directly in your browser or serve it locally:
```bash
open index.html
```

---

## 📊 Live Metrics Payload Format

```json
{
  "symbol": "BTCUSDT",
  "latest_price": 81206.12,
  "mean_price": 81204.50,
  "z_score": 0.42,
  "status": "NORMAL",
  "timestamp": 1718000000000
}
```

---

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Networking & Web Framework:** FastAPI, Uvicorn, WebSockets, `asyncio`
* **Data Structures:** Python `collections.deque`
* **Frontend:** HTML5, CSS3 (DRW Design Tokens), Vanilla JavaScript (WebSocket API)

---
