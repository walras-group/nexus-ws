import asyncio
import os
import msgspec
from nexus_ws import BybitWSClient, BybitTestnetStreamUrl

API_KEY = os.getenv("BYBIT_API")
SECRET = os.getenv("BYBIT_SECRET")


def handler(raw: bytes):
    message = msgspec.json.decode(raw)
    print("Received message:", message)


async def main():
    url = BybitTestnetStreamUrl.PRIVATE
    client = BybitWSClient(handler, url, api_key=API_KEY, secret=SECRET)

    client.subscribe_execution()
    client.subscribe_orders()
    client.subscribe_position()
    client.subscribe_wallet()
    client.subscribe_fast_execution()

    await client.wait()
    
if __name__ == "__main__":
    asyncio.run(main())
