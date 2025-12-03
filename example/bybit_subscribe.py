import asyncio
import msgspec
from nexus_ws import BybitWSClient, BybitStreamUrl


def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)


async def main():
    url = BybitStreamUrl.LINEAR
    client = BybitWSClient(handler, url)

    await client.subscribe_trade(["BTCUSDT"])
    await client.wait(timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
