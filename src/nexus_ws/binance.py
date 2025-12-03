from typing import List, Callable, Any, Literal
from enum import Enum
from .ws import WSClient


class BinanceStreamUrl(Enum):
    USDM_FUTURES = "wss://fstream.binancefuture.com/ws"
    COINM_FUTURES = "wss://dstream.binancefuture.com/ws"
    SPOT = "wss://stream.binance.com:9443/ws"


KLINE_INTERVAL = Literal[
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]

MARK_PRICE_INTERVAL = Literal["1s", "3s"]

CONTRACT_TYPE = Literal["perpetual", "current_quarter", "next_quarter"]


class BinanceWSClient(WSClient):
    def __init__(
        self,
        handler: Callable[..., Any],
        url: BinanceStreamUrl,
    ):
        super().__init__(
            url.value,
            handler=handler,
            enable_auto_ping=False,
        )

    def _send_payload(
        self, params: List[str], method: str = "SUBSCRIBE", chunk_size: int = 50
    ):
        params_chunks = [
            params[i : i + chunk_size] for i in range(0, len(params), chunk_size)
        ]

        for chunk in params_chunks:
            payload = {
                "method": method,
                "params": chunk,
                "id": self.timestamp_ms(),
            }
            self.send(payload)

    def _subscribe(self, params: List[str]):
        params = [param for param in params if param not in self._subscriptions]

        if not params:
            return

        for param in params:
            self._subscriptions.append(param)
            self._log.debug(f"Subscribing to {param}...")

        self._send_payload(params, method="SUBSCRIBE")

    def _unsubscribe(self, params: List[str]):
        params = [param for param in params if param in self._subscriptions]

        if not params:
            return

        for param in params:
            self._subscriptions.remove(param)
            self._log.debug(f"Unsubscribing from {param}...")

        self._send_payload(params, method="UNSUBSCRIBE")

    def subscribe_trade(self, symbols: List[str]):
        params = [f"{symbol.lower()}@trade" for symbol in symbols]
        self._subscribe(params)

    def subscribe_aggtrade(self, symbols: List[str]):
        params = [f"{symbol.lower()}@aggTrade" for symbol in symbols]
        self._subscribe(params)

    def subscribe_markprice(
        self, symbols: List[str], interval: MARK_PRICE_INTERVAL | None = None
    ):
        if interval == "1s":
            params = [f"{symbol.lower()}@markPrice@1s" for symbol in symbols]
        else:
            params = [f"{symbol.lower()}@markPrice" for symbol in symbols]
        self._subscribe(params)

    def subscribe_all_markprice(self, interval: MARK_PRICE_INTERVAL | None = None):
        if interval == "1s":
            params = ["!markPrice@arr@1s"]
        else:
            params = ["!markPrice@arr"]
        self._subscribe(params)

    def subscribe_kline(self, symbols: List[str], interval: KLINE_INTERVAL):
        params = [f"{symbol.lower()}@kline_{interval}" for symbol in symbols]
        self._subscribe(params)

    def subscribe_continuous_kline(
        self, pair: str, contract_type: CONTRACT_TYPE, interval: KLINE_INTERVAL
    ):
        params = [f"{pair.lower()}@continuousKline_{contract_type}_{interval}"]
        self._subscribe(params)

    def subscribe_mini_ticker(self, symbols: List[str]):
        params = [f"{symbol.lower()}@miniTicker" for symbol in symbols]
        self._subscribe(params)

    def subscribe_all_ticker(self):
        params = ["!ticker@arr"]
        self._subscribe(params)

    def subscribe_ticker(self, symbols: List[str]):
        params = [f"{symbol.lower()}@ticker" for symbol in symbols]
        self._subscribe(params)

    def resubscribe(self):
        self._send_payload(self._subscriptions)
