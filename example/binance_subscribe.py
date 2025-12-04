import asyncio
import msgspec
from nexus_ws import BinanceWSClient, BinanceStreamUrl


def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)


async def main():
    url = BinanceStreamUrl.USD_M_FUTURES
    async with BinanceWSClient(handler, url) as client:
        client.subscribe_trade(["AIAUSDT"])
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
