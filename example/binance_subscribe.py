import asyncio
import msgspec
from nexus_ws import BinanceWSClient, BinanceStreamUrl

def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)

async def main():
    url = BinanceStreamUrl.SPOT
    client = BinanceWSClient(handler, url)

    await client.connect()
    client.subscribe_trade(["ethusdt"])
    await client.wait(timeout=10)

if __name__ == "__main__":
    asyncio.run(main())
