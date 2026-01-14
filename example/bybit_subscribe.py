import asyncio
import msgspec
import logging
from nexus_ws import BybitWSClient, BybitStreamUrl


def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)

logging.basicConfig(level=logging.DEBUG)

async def main():
    url = BybitStreamUrl.LINEAR
    client = BybitWSClient(handler, url)

    client.subscribe_trade(["BTCUSDT"])
    await client.wait(timeout=100)


if __name__ == "__main__":
    asyncio.run(main())
