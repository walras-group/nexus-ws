import asyncio
import msgspec
from nexus_ws import OkxStreamUrl, OkxWSClient

def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)


async def main():
    url = OkxStreamUrl.BUSINESS
    client = OkxWSClient(handler, url)

    client.subscribe_trade_all(["BTC-USDT-SWAP"])
    await client.wait(timeout=10)


if __name__ == "__main__":
    asyncio.run(main())
