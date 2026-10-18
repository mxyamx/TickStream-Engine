import asyncio
from collections import deque
import json
import numpy as np
import websockets

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
RECENT_TRADES = deque(maxlen=50)
Z_SCORE_THRESHOLD = 2.5  # Slightly raised to filter noise


async def connect_and_process():
    print("Connecting to Binance WebSocket stream with Noise Filter...")

    async with websockets.connect(BINANCE_WS_URL) as websocket:
        print(
            "Connected successfully! Filtering market noise and monitoring real spikes...\n"
        )

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

                # Calculate percentage change from the rolling mean
                pct_change = abs(latest_price - mean_price) / mean_price * 100

                # RULE: Must cross Z-Score threshold AND move by at least 0.03% (approx $25+ on an $80k asset)
                if (
                    abs(z_score) > Z_SCORE_THRESHOLD
                    and pct_change > 0.03
                ):
                    print(
                        f"🚨 [VOLATILITY ALERT!] Significant Move! "
                        f"Latest: ${latest_price:,.2f} | Change: {pct_change:.3f}% | Z-Score: {z_score:.2f}"
                    )
                else:
                    print(
                        f"[QUIET MARKET] Latest: ${latest_price:,.2f} | "
                        f"Mean: ${mean_price:,.2f} | Pct Change: {pct_change:.3f}%"
                    )


if __name__ == "__main__":
    try:
        asyncio.run(connect_and_process())
    except KeyboardInterrupt:
        print("\nStopped anomaly engine.")