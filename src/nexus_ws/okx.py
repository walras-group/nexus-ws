import picows
import msgspec
from enum import Enum
from typing import Any, Callable, List, Literal, Dict


from .ws import WSClient, Listener


class OkxStreamUrl(Enum):
    PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
    BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"


ORDER_BOOK_CHANNELS = Literal[
    "books", "books5", "bbo-tbt", "books-l2-tbt", "books50-l2-tbt"
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
    ):
        self._business = url == OkxStreamUrl.BUSINESS
        super().__init__(
            url.value,
            handler=handler,
            specific_ping_msg=b"ping",
            ping_idle_timeout=5,
            ping_reply_timeout=2,
            user_pong_callback=user_pong_callback,
        )

    def _send_payload(
        self,
        params: List[Dict[str, Any]],
        op: Literal["subscribe", "unsubscribe"] = "subscribe",
        chunk_size: int = 100,
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
            self.send(payload)

    def _subscribe(self, params: List[Dict[str, Any]], auth: bool = False):
        params = [param for param in params if param not in self._subscriptions]

        for param in params:
            self._subscriptions.append(param)
            self._log.debug(f"Subscribing to {param}...")

        if not params:
            return

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
        params_to_unsubscribe = [
            param for param in params if param in self._subscriptions
        ]

        for param in params_to_unsubscribe:
            self._subscriptions.remove(param)
            self._log.debug(f"Unsubscribing from {param}...")

        if params_to_unsubscribe:
            self._send_payload(params_to_unsubscribe, op="unsubscribe")

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

    def resubscribe(self):
        self._send_payload(self._subscriptions)
