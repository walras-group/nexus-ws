import asyncio
import msgspec
from nexus_ws import BinanceWSClient, BinanceStreamUrl


def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)


async def main():
    url = BinanceStreamUrl.USD_M_FUTURES
    client = BinanceWSClient(handler, url)

    await client.subscribe_partial_book_depth(["ethusdt"], levels=20, update_speed="100ms")
    await client.wait(timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
