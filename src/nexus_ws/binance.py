import hashlib
import hmac
from enum import Enum
from typing import Any, Callable, Dict, List, Literal
from urllib.parse import urlencode

from .ws import WSClient


class BinanceStreamUrl(Enum):
    USD_M_FUTURES_PUBLIC = "wss://fstream.binance.com/public/ws"
    USD_M_FUTURES_MARKET = "wss://fstream.binance.com/market/ws"
    USD_M_FUTURES_PRIVATE = "wss://fstream.binance.com/private/ws"
    USD_M_FUTURES = "auto://binance-usd-m-futures"
    USD_M_FUTURES_TESTNET = "wss://fstream.binancefuture.com/ws"
    COIN_M_FUTURES = "wss://dstream.binance.com/ws"
    COIN_M_FUTURES_TESTNET = "wss://dstream.binancefuture.com/ws"
    SPOT = "wss://stream.binance.com:9443/ws"
    SPOT_TESTNET = "wss://demo-stream.binance.com/ws"
    PORTFOLIO_MARGIN = "wss://fstream.binance.com/pm/ws"


class BinanceWsApiUrl(Enum):
    SPOT = "wss://ws-api.binance.com:443/ws-api/v3"
    SPOT_TESTNET = "wss://demo-ws-api.binance.com/ws-api/v3"
    USD_M_FUTURE = "wss://ws-fapi.binance.com/ws-fapi/v1"
    USD_M_FUTURE_TESTNET = "wss://testnet.binancefuture.com/ws-fapi/v1"
    COIN_M_FUTURE = "wss://ws-dapi.binance.com/ws-dapi/v1"
    COIN_M_FUTURE_TESTNET = "wss://testnet.binancefuture.com/ws-dapi/v1"


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

PARTIAL_BOOK_DEPTH_LEVELS = Literal[5, 10, 20]
BOOK_DEPTH_UPDATE_SPEED = Literal["100ms", "250ms", "500ms"]
BINANCE_USD_M_FUTURES_ENDPOINT = Literal["public", "market", "private"]


USD_M_FUTURES_ENDPOINT_URLS: dict[BINANCE_USD_M_FUTURES_ENDPOINT, str] = {
    "public": BinanceStreamUrl.USD_M_FUTURES_PUBLIC.value,
    "market": BinanceStreamUrl.USD_M_FUTURES_MARKET.value,
    "private": BinanceStreamUrl.USD_M_FUTURES_PRIVATE.value,
}


class BinanceWSClient(WSClient):
    def __init__(
        self,
        handler: Callable[..., Any],
        url: BinanceStreamUrl,
        auto_reconnect_interval: int | None = None,
        max_subscriptions_per_client: int | None = None,
        max_clients: int | None = None,
    ):
        super().__init__(
            USD_M_FUTURES_ENDPOINT_URLS["market"]
            if url == BinanceStreamUrl.USD_M_FUTURES
            else url.value,
            handler=handler,
            enable_auto_ping=False,
            auto_reconnect_interval=auto_reconnect_interval,
            max_subscriptions_per_client=max_subscriptions_per_client,
            max_clients=max_clients,
        )
        self._auto_route_usd_m_futures = url == BinanceStreamUrl.USD_M_FUTURES
        self._client_urls: dict[int, str] = {}
        self._endpoint_clients: dict[BINANCE_USD_M_FUTURES_ENDPOINT, list[int]] = {
            "public": [],
            "market": [],
            "private": [],
        }

    def _url_for_client(self, client_id: int) -> str:
        return self._client_urls.get(client_id, self._url)

    def start(self):
        if self._auto_route_usd_m_futures and not self._client_subscriptions:
            self._started = True
            return None
        return super().start()

    def _send_payload(
        self,
        params: List[str],
        method: str = "SUBSCRIBE",
        chunk_size: int = 50,
        client_id: int | None = None,
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
            self.send(payload, client_id=client_id)

    def _get_client_id_for_endpoint(
        self, endpoint: BINANCE_USD_M_FUTURES_ENDPOINT
    ) -> int:
        for client_id in self._endpoint_clients[endpoint]:
            if (
                self._max_subscriptions_per_client is None
                or len(self._client_subscriptions[client_id])
                < self._max_subscriptions_per_client
            ):
                return client_id

        if (
            self._max_clients is not None
            and len(self._client_subscriptions) >= self._max_clients
        ):
            raise RuntimeError("Maximum number of websocket clients reached")

        client_id = self._next_client_id
        self._client_urls[client_id] = USD_M_FUTURES_ENDPOINT_URLS[endpoint]
        self._endpoint_clients[endpoint].append(client_id)
        self._ensure_client(client_id)
        return client_id

    def _register_endpoint_subscriptions(
        self,
        subscriptions: list[str],
        endpoint: BINANCE_USD_M_FUTURES_ENDPOINT,
    ) -> dict[int, list[str]]:
        assigned: dict[int, list[str]] = {}
        for subscription in subscriptions:
            if subscription in self._subscriptions:
                continue
            client_id = self._get_client_id_for_endpoint(endpoint)
            self._subscriptions.append(subscription)
            self._client_subscriptions[client_id].append(subscription)
            assigned.setdefault(client_id, []).append(subscription)
        return assigned

    def _subscribe(
        self,
        params: List[str],
        endpoint: BINANCE_USD_M_FUTURES_ENDPOINT | None = None,
    ):
        params = [param for param in params if param not in self._subscriptions]

        if not params:
            return

        for param in params:
            self._log.debug(f"Subscribing to {param}...")

        if self._auto_route_usd_m_futures:
            if endpoint is None:
                raise ValueError("Auto-routed Binance USD-M futures streams need an endpoint.")
            assigned = self._register_endpoint_subscriptions(params, endpoint)
        else:
            assigned = self._register_subscriptions(params)

        for client_id, client_params in assigned.items():
            if self._transports.get(client_id) is not None:
                self._send_payload(client_params, client_id=client_id)

    def _normalize_symbols(self, symbols: str | List[str]) -> List[str]:
        if isinstance(symbols, str):
            return [symbols]
        return symbols

    def _unsubscribe(self, params: List[str]):
        if not params:
            return

        removed = self._unregister_subscriptions(params)
        if not removed:
            return

        for client_id, client_params in removed.items():
            for param in client_params:
                self._log.debug(f"Unsubscribing from {param}...")
            self._send_payload(client_params, method="UNSUBSCRIBE", client_id=client_id)

    def subscribe_trade(self, symbols: str | List[str]):
        symbols = self._normalize_symbols(symbols)
        params = [f"{symbol.lower()}@trade" for symbol in symbols]
        self._subscribe(params, endpoint="public")

    def subscribe_aggtrade(self, symbols: str | List[str]):
        symbols = self._normalize_symbols(symbols)
        params = [f"{symbol.lower()}@aggTrade" for symbol in symbols]
        self._subscribe(params, endpoint="market")

    def subscribe_markprice(
        self, symbols: str | List[str], interval: MARK_PRICE_INTERVAL | None = None
    ):
        symbols = self._normalize_symbols(symbols)
        if interval == "1s":
            params = [f"{symbol.lower()}@markPrice@1s" for symbol in symbols]
        else:
            params = [f"{symbol.lower()}@markPrice" for symbol in symbols]
        self._subscribe(params, endpoint="market")

    def subscribe_all_markprice(self, interval: MARK_PRICE_INTERVAL | None = None):
        if interval == "1s":
            params = ["!markPrice@arr@1s"]
        else:
            params = ["!markPrice@arr"]
        self._subscribe(params, endpoint="market")

    def subscribe_kline(self, symbols: str | List[str], interval: KLINE_INTERVAL):
        symbols = self._normalize_symbols(symbols)
        params = [f"{symbol.lower()}@kline_{interval}" for symbol in symbols]
        self._subscribe(params, endpoint="market")

    def subscribe_continuous_kline(
        self, pair: str, contract_type: CONTRACT_TYPE, interval: KLINE_INTERVAL
    ):
        params = [f"{pair.lower()}@continuousKline_{contract_type}_{interval}"]
        self._subscribe(params, endpoint="market")

    def subscribe_mini_ticker(self, symbols: str | List[str]):
        symbols = self._normalize_symbols(symbols)
        params = [f"{symbol.lower()}@miniTicker" for symbol in symbols]
        self._subscribe(params, endpoint="market")

    def subscribe_all_mini_ticker(self):
        params = ["!miniTicker@arr"]
        self._subscribe(params, endpoint="market")

    def subscribe_book_ticker(self, symbols: str | List[str]):
        symbols = self._normalize_symbols(symbols)
        params = [f"{symbol.lower()}@bookTicker" for symbol in symbols]
        self._subscribe(params, endpoint="public")

    def subscribe_all_book_ticker(self):
        params = ["!bookTicker"]
        self._subscribe(params, endpoint="public")

    def subscribe_force_order(self, symbols: str | List[str]):
        symbols = self._normalize_symbols(symbols)
        params = [f"{symbol.lower()}@forceOrder" for symbol in symbols]
        self._subscribe(params, endpoint="market")

    def subscribe_all_force_order(self):
        params = ["!forceOrder@arr"]
        self._subscribe(params, endpoint="market")

    def subscribe_partial_book_depth(
        self,
        symbols: str | List[str],
        levels: PARTIAL_BOOK_DEPTH_LEVELS,
        update_speed: BOOK_DEPTH_UPDATE_SPEED,
    ):
        symbols = self._normalize_symbols(symbols)
        if update_speed == "250ms":
            params = [f"{symbol.lower()}@depth{levels}" for symbol in symbols]
        else:
            params = [
                f"{symbol.lower()}@depth{levels}@{update_speed}" for symbol in symbols
            ]
        self._subscribe(params, endpoint="public")

    def subscribe_diff_book_depth(
        self, symbols: str | List[str], update_speed: BOOK_DEPTH_UPDATE_SPEED
    ):
        symbols = self._normalize_symbols(symbols)
        if update_speed == "250ms":
            params = [f"{symbol.lower()}@depth" for symbol in symbols]
        else:
            params = [f"{symbol.lower()}@depth@{update_speed}" for symbol in symbols]
        self._subscribe(params, endpoint="public")

    def subscribe_user_data_stream(self, listen_key: str):
        self._subscribe([listen_key], endpoint="private")

    async def _resubscribe_for_client(self, client_id: int, subscriptions: List[str]):
        if not subscriptions:
            return
        self._send_payload(subscriptions, client_id=client_id)


class BinanceWSApiClient(WSClient):
    def __init__(
        self,
        handler: Callable[..., Any],
        url: BinanceStreamUrl,
        api_key: str | None = None,
        secret: str | None = None,
        auto_reconnect_interval: int | None = None,
        max_subscriptions_per_client: int | None = None,
        max_clients: int | None = None,
    ):
        super().__init__(
            url.value,
            handler=handler,
            enable_auto_ping=False,
            auto_reconnect_interval=auto_reconnect_interval,
            max_subscriptions_per_client=max_subscriptions_per_client,
            max_clients=max_clients,
        )
        self._api_key = api_key
        self._secret = secret

        if (self._api_key is None) != (self._secret is None):
            raise ValueError(
                "Both api_key and secret must be provided for authenticated endpoints."
            )

    def _generate_signature(self, query: str) -> str:
        signature = hmac.new(
            self._secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return signature

    def _send_payload(
        self,
        params: Dict[str, Any],
        method: str,
        client_id: int | None = None,
    ):
        query = urlencode(sorted(params.items()))
        signature = self._generate_signature(query)
        params["signature"] = signature

        payload = {
            "method": method,
            "params": params,
            "id": self.timestamp_ms(),
        }
        self.send(payload, client_id=client_id)

    def _subscribe(self, params: List[Any]):
        params = [param for param in params if param not in self._subscriptions]

        if not params:
            return

        for param in params:
            self._log.debug(f"Subscribing to {param}...")

        self._register_subscriptions(params)

    def subscribe_spot_user_data_stream(self):
        if self._url not in {
            BinanceWsApiUrl.SPOT.value,
            BinanceWsApiUrl.SPOT_TESTNET.value,
        }:
            raise ValueError(
                "Spot user data stream subscription is only supported on SPOT_WS_API and SPOT_WS_API_TESTNET endpoints."
            )
        param = {
            "apiKey": self._api_key,
            "timestamp": self.timestamp_ms(),
        }

        self._subscribe([(param, "userDataStream.subscribe.signature")])

    async def _resubscribe_for_client(self, client_id: int, subscriptions: List[str]):
        if not subscriptions:
            return
        for sub, method in subscriptions:
            self._send_payload(sub, method=method, client_id=client_id)
