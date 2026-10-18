import asyncio
from collections import deque
import json
import os
import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import websockets

app = FastAPI(title="TickStream-Engine")

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
RECENT_TRADES = deque(maxlen=50)
Z_SCORE_THRESHOLD = 2.5

# Keep track of connected web clients (dashboards)
connected_clients = set()


@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serves the front-end HTML dashboard directly from the root URL."""
    if os.path.exists("index.html"):
        with open("index.html", "r") as f:
            return f.read()
    return "<h3>Dashboard HTML template not found.</h3>"


async def binance_listener():
    """Background task that listens to Binance and broadcasts alerts to web clients."""
    print("Background Binance listener started...")
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as websocket:
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)

                    trade = {
                        "price": float(data["p"]),
                        "quantity": float(data["q"]),
                        "time": data["T"],
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

                        # Package payload for web clients
                        payload = {
                            "status": (
                                "VOLATILITY_ALERT" if is_alert else "NORMAL"
                            ),
                            "latest_price": round(latest_price, 2),
                            "mean_price": round(mean_price, 2),
                            "pct_change": round(pct_change, 4),
                            "z_score": round(z_score, 2),
                        }

                        # Broadcast to all connected front-end dashboards
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
    """Automatically start the Binance pipeline when the web server boots up."""
    asyncio.create_task(binance_listener())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for front-end dashboards to connect to."""
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        connected_clients.remove(websocket)