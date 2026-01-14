import picows
import hmac
import base64
import asyncio
from enum import Enum
from typing import Any, Callable, List, Literal, Dict


from .ws import WSClient, Listener


class OkxStreamUrl(Enum):
    PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
    BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"
    PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"


class OkxDemoStreamUrl(Enum):
    PUBLIC = "wss://wspap.okx.com:8443/ws/v5/public"
    BUSINESS = "wss://wspap.okx.com:8443/ws/v5/business"
    PRIVATE = "wss://wspap.okx.com:8443/ws/v5/private"


ORDER_BOOK_CHANNELS = Literal[
    "books", "books5", "bbo-tbt", "books-l2-tbt", "books50-l2-tbt"
]

INST_TYPE = Literal[
    "SPOT",
    "MARGIN",
    "SWAP",
    "FUTURES",
    "OPTION",
    "ANY",
]

CANDLESTICK_CHANNELS = Literal[
    "candle3M",
    "candle1M",
    "candle1W",
    "candle1D",
    "candle2D",
    "candle3D",
    "candle5D",
    "candle12H",
    "candle6H",
    "candle4H",
    "candle2H",
    "candle1H",
    "candle30m",
    "candle15m",
    "candle5m",
    "candle3m",
    "candle1m",
    "candle1s",
    "candle3Mutc",
    "candle1Mutc",
    "candle1Wutc",
    "candle1Dutc",
    "candle2Dutc",
    "candle3Dutc",
    "candle5Dutc",
    "candle12Hutc",
    "candle6Hutc",
]


def user_pong_callback(self: Listener, frame: picows.WSFrame) -> bool:
    self._log.debug("Pong received")
    return (
        frame.msg_type == picows.WSMsgType.TEXT
        and frame.get_payload_as_memoryview() == b"pong"
    )


class OkxWSClient(WSClient):
    def __init__(
        self,
        handler: Callable[..., Any],
        url: OkxStreamUrl,
        api_key: str | None = None,
        secret: str | None = None,
        passphrase: str | None = None,
        auto_reconnect_interval: int | None = None,
        max_subscriptions_per_client: int | None = None,
        max_clients: int | None = None,
    ):
        self._business = url == OkxStreamUrl.BUSINESS
        super().__init__(
            url.value,
            handler=handler,
            specific_ping_msg=b"ping",
            ping_idle_timeout=5,
            ping_reply_timeout=2,
            user_pong_callback=user_pong_callback,
            auto_reconnect_interval=auto_reconnect_interval,
            max_subscriptions_per_client=max_subscriptions_per_client,
            max_clients=max_clients,
        )

        self._api_key = api_key
        self._secret = secret
        self._passphrase = passphrase

        if (self._api_key is None) != (self._secret is None) or (
            self._api_key is None
        ) != (self._passphrase is None):
            raise ValueError(
                "api_key, secret, and passphrase must be provided together"
            )

        if self._api_key and url != OkxStreamUrl.PRIVATE:
            raise ValueError(
                "api_key, secret, and passphrase can only be used with private stream url"
            )

    def _get_auth_payload(self):
        if self._secret is None:
            raise ValueError("Secret is missing.")

        timestamp = int(self.timestamp())
        message = str(timestamp) + "GET" + "/users/self/verify"
        mac = hmac.new(
            bytes(self._secret, encoding="utf8"),
            bytes(message, encoding="utf-8"),
            digestmod="sha256",
        )
        d = mac.digest()
        sign = base64.b64encode(d)
        if self._api_key is None or self._passphrase is None or self._secret is None:
            raise ValueError("API Key, Passphrase, or Secret is missing.")
        arg = {
            "apiKey": self._api_key,
            "passphrase": self._passphrase,
            "timestamp": timestamp,
            "sign": sign.decode("utf-8"),
        }
        payload = {"op": "login", "args": [arg]}
        return payload

    async def auth(self, client_id: int | None = None):
        payload = self._get_auth_payload()
        self.send(payload, client_id=client_id)
        await asyncio.sleep(5)  # wait for auth to complete

    def _send_payload(
        self,
        params: List[Dict[str, Any]],
        op: Literal["subscribe", "unsubscribe"] = "subscribe",
        chunk_size: int = 100,
        client_id: int | None = None,
    ):
        # Split params into chunks of 100 if length exceeds 100
        params_chunks = [
            params[i : i + chunk_size] for i in range(0, len(params), chunk_size)
        ]

        for chunk in params_chunks:
            payload = {
                "op": op,
                "args": chunk,
            }
            self.send(payload, client_id=client_id)

    def _subscribe(self, params: List[Dict[str, Any]]):
        params = [param for param in params if param not in self._subscriptions]

        for param in params:
            self._log.debug(f"Subscribing to {param}...")

        if not params:
            return
        self._register_subscriptions(params)

    def subscribe_funding_rate(self, symbols: List[str]):
        params = [{"channel": "funding-rate", "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def subscribe_index_price(self, symbols: List[str]):
        params = [{"channel": "index-tickers", "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def subscribe_mark_price(self, symbols: List[str]):
        params = [{"channel": "mark-price", "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def subscribe_order_book(
        self,
        symbols: List[str],
        channel: ORDER_BOOK_CHANNELS,
    ):
        """
        /ws/v5/public
        https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel
        """
        params = [{"channel": channel, "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def subscribe_trade(self, symbols: List[str]):
        """
        /ws/v5/public
        https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-all-trades-channel
        """
        params = [{"channel": "trades", "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def subscribe_trade_all(self, symbols: List[str]):
        """
        /ws/v5/business
        https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-all-trades-channel
        """
        if not self._business:
            raise ValueError("subscribe_trade_all requires `BUSINESS` stream URL")
        params = [{"channel": "trades-all", "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def subscribe_candlesticks(
        self,
        symbols: List[str],
        channel: CANDLESTICK_CHANNELS,
    ):
        """
        /ws/v5/business
        https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-candlesticks-channel
        """
        if not self._business:
            raise ValueError("subscribe_candlesticks requires `BUSINESS` stream URL")
        params = [{"channel": channel, "instId": symbol} for symbol in symbols]
        self._subscribe(params)

    def _unsubscribe(self, params: List[Dict[str, Any]]):
        if not params:
            return
        removed = self._unregister_subscriptions(params)
        if not removed:
            return
        for client_id, client_params in removed.items():
            for param in client_params:
                self._log.debug(f"Unsubscribing from {param}...")
            self._send_payload(client_params, op="unsubscribe", client_id=client_id)

    def unsubscribe_funding_rate(self, symbols: List[str]):
        params = [{"channel": "funding-rate", "instId": symbol} for symbol in symbols]
        self._unsubscribe(params)

    def unsubscribe_index_price(self, symbols: List[str]):
        params = [{"channel": "index-tickers", "instId": symbol} for symbol in symbols]
        self._unsubscribe(params)

    def unsubscribe_mark_price(self, symbols: List[str]):
        params = [{"channel": "mark-price", "instId": symbol} for symbol in symbols]
        self._unsubscribe(params)

    def unsubscribe_order_book(
        self,
        symbols: List[str],
        channel: Literal[
            "books", "books5", "bbo-tbt", "books-l2-tbt", "books50-l2-tbt"
        ],
    ):
        params = [{"channel": channel, "instId": symbol} for symbol in symbols]
        self._unsubscribe(params)

    def unsubscribe_trade(self, symbols: List[str]):
        params = [{"channel": "trades", "instId": symbol} for symbol in symbols]
        self._unsubscribe(params)

    def unsubscribe_candlesticks(
        self,
        symbols: List[str],
        channel: CANDLESTICK_CHANNELS,
    ):
        params = [{"channel": channel, "instId": symbol} for symbol in symbols]
        self._unsubscribe(params)

    async def _resubscribe_for_client(
        self, client_id: int, subscriptions: List[Dict[str, Any]]
    ):
        if not subscriptions:
            return
        if self._api_key is not None:
            await self.auth(client_id=client_id)
        self._send_payload(subscriptions, client_id=client_id)

    def subscribe_orders(
        self,
        inst_types: INST_TYPE,
        inst_family: str | None = None,
        instId: str | None = None,
    ):
        """
        subscribe to orders

        Args:
            inst_types: instrument type
            inst_family: instrument family
            instId: instrument id
        """
        if self._api_key is None:
            raise ValueError("API key is required for subscribing to orders")

        param: Dict[str, Any] = {"channel": "orders", "instType": inst_types}
        if inst_family is not None:
            param["instFamily"] = inst_family
        if instId is not None:
            param["instId"] = instId

        self._subscribe([param])

    def subscribe_account(self, ccy: str | None = None):
        """
        subscribe to account

        Args:
            ccy: currency
        """
        if self._api_key is None:
            raise ValueError("API key is required for subscribing to account")

        param: Dict[str, Any] = {"channel": "account"}
        if ccy is not None:
            param["ccy"] = ccy

        self._subscribe([param])

    def subscribe_positions(
        self,
        inst_types: INST_TYPE,
        inst_family: str | None = None,
        instId: str | None = None,
    ):
        """
        subscribe to positions

        Args:
            inst_types: instrument type
            inst_family: instrument family
            instId: instrument id
        """
        if self._api_key is None:
            raise ValueError("API key is required for subscribing to positions")

        param: Dict[str, Any] = {"channel": "positions", "instType": inst_types}
        if inst_family is not None:
            param["instFamily"] = inst_family
        if instId is not None:
            param["instId"] = instId

        self._subscribe([param])

    def subscribe_balance_and_position(self):
        """
        subscribe to balance_and_position
        """
        if self._api_key is None:
            raise ValueError(
                "API key is required for subscribing to balance_and_position"
            )

        param: Dict[str, Any] = {"channel": "balance_and_position"}

        self._subscribe([param])
