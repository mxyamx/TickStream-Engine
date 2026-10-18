import asyncio
from collections import deque
import json
import os
import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import websockets

app = FastAPI(title="TickStream-Engine")

# Coinbase Advanced Trade / Pro Public WebSocket (Cloud-friendly, no geo-blocking)
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
RECENT_TRADES = deque(maxlen=50)
Z_SCORE_THRESHOLD = 2.5

connected_clients = set()


@app.get("/", response_class=HTMLResponse)
async def read_root():
    if os.path.exists("index.html"):
        with open("index.html", "r") as f:
            return f.read()
    return "<h3>Dashboard HTML template not found.</h3>"


async def coinbase_listener():
    """Background task that connects to Coinbase public feed and broadcasts anomalies."""
    print("Background Coinbase listener started...")
    
    # Subscription payload for Coinbase BTC-USD spot trades
    subscribe_message = json.dumps({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["matches"]
    })

    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL) as websocket:
                await websocket.send(subscribe_message)
                print("Connected to Coinbase WebSocket feed.")

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)

                    # Coinbase 'match' event structure
                    if data.get("type") == "match":
                        price = float(data["price"])
                        size = float(data["size"])
                        
                        trade = {
                            "price": price,
                            "quantity": size,
                        }

                        RECENT_TRADES.append(trade)

                        if len(RECENT_TRADES) == 50:
                            prices = np.array([t["price"] for t in RECENT_TRADES])
                            mean_price = np.mean(prices)
                            std_price = np.std(prices)
                            latest_price = prices[-1]

                            min_volatility_floor = 0.50
                            adjusted_std = max(std_price, min_volatility_floor)
                            z_score = (latest_price - mean_price) / adjusted_std
                            pct_change = (
                                abs(latest_price - mean_price) / mean_price * 100
                            )

                            is_alert = (
                                abs(z_score) > Z_SCORE_THRESHOLD
                                and pct_change > 0.03
                            )

                            payload = {
                                "status": (
                                    "VOLATILITY_ALERT" if is_alert else "NORMAL"
                                ),
                                "latest_price": round(latest_price, 2),
                                "mean_price": round(mean_price, 2),
                                "pct_change": round(pct_change, 4),
                                "z_score": round(z_score, 2),
                            }

                            if connected_clients:
                                message_json = json.dumps(payload)
                                await asyncio.gather(
                                    *[
                                        client.send_text(message_json)
                                        for client in connected_clients
                                    ]
                                )

        except Exception as e:
            print(f"Connection lost: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(coinbase_listener())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)