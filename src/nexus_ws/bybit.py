import asyncio
import picows
import msgspec
import hmac
from enum import Enum
from typing import Any, Callable, List, Literal


from .ws import WSClient, Listener


class BybitStreamUrl(Enum):
    SPOT = "wss://stream.bybit.com/v5/public/spot"
    LINEAR = "wss://stream.bybit.com/v5/public/linear"
    INVERSE = "wss://stream.bybit.com/v5/public/inverse"
    OPTION = "wss://stream.bybit.com/v5/public/option"
    PRIVATE = "wss://stream.bybit.com/v5/private"

    @property
    def is_private(self) -> bool:
        return self == BybitStreamUrl.PRIVATE

    @property
    def is_public(self) -> bool:
        return not self.is_private


class BybitTestnetStreamUrl(Enum):
    SPOT = "wss://stream-testnet.bybit.com/v5/public/spot"
    LINEAR = "wss://stream-testnet.bybit.com/v5/public/linear"
    INVERSE = "wss://stream-testnet.bybit.com/v5/public/inverse"
    OPTION = "wss://stream-testnet.bybit.com/v5/public/option"
    PRIVATE = "wss://stream-testnet.bybit.com/v5/private"

    @property
    def is_private(self) -> bool:
        return self == BybitTestnetStreamUrl.PRIVATE

    @property
    def is_public(self) -> bool:
        return not self.is_private


KLINE_INTERVAL = Literal[
    "1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M"
]

ORDER_BOOK_DEPTH = Literal[1, 25, 50, 100, 200, 1000]
CONTRACT_TYPE = Literal["USDT", "USDC", "inverse"]

POSITION_TOPICS = Literal[
    "position", "position.linear", "position.inverse", "position.option"
]

EXECUTION_TOPICS = Literal[
    "execution", "execution.linear", "execution.inverse", "execution.option"
]

EXECUTION_FAST_TOPICS = Literal[
    "execution.fast",
    "execution.fast.linear",
    "execution.fast.inverse",
    "execution.fast.option",
]

ORDER_TOPICS = Literal["order", "order.linear", "order.inverse", "order.option"]


class BybitPingMsg(msgspec.Struct):
    op: str = ""
    ret_msg: str = ""

    @property
    def is_pong(self) -> bool:
        return self.ret_msg == "pong" or self.op == "pong"


def user_pong_callback(self: Listener, frame: picows.WSFrame) -> bool:
    if frame.msg_type != picows.WSMsgType.TEXT:
        self._log.debug(
            f"Received non-text frame for pong callback. ws_frame: {self._decode_frame(frame)}"
        )
        return False
    try:
        message = msgspec.json.decode(frame.get_payload_as_bytes(), type=BybitPingMsg)
        self._log.debug(f"Received pong message: {self._decode_frame(frame)}")
        return message.is_pong
    except msgspec.DecodeError:
        self._log.error(
            f"Failed to decode pong message. ws_frame: {self._decode_frame(frame)}"
        )
        return False


class BybitWSClient(WSClient):
    def __init__(
        self,
        handler: Callable[..., Any],
        url: BybitStreamUrl | BybitTestnetStreamUrl,
        api_key: str | None = None,
        secret: str | None = None,
        auto_reconnect_interval: int | None = None,
        max_subscriptions_per_client: int | None = None,
        max_clients: int | None = None,
    ):
        super().__init__(
            url.value,
            handler=handler,
            ping_idle_timeout=20,
            ping_reply_timeout=2,
            specific_ping_msg=msgspec.json.encode({"op": "ping"}),
            auto_ping_strategy="ping_when_idle",
            user_pong_callback=user_pong_callback,
            auto_reconnect_interval=auto_reconnect_interval,
            max_subscriptions_per_client=max_subscriptions_per_client,
            max_clients=max_clients,
        )

        self._api_key = api_key
        self._secret = secret

        # validate api_key and secret
        # api_key and secret must be all None or all not None
        if (self._api_key is None) != (self._secret is None):
            raise ValueError("api_key and secret must be provided together")

        if self._api_key and not url.is_private:
            raise ValueError(
                "api_key and secret can only be used with private stream url"
            )

    async def auth(self, client_id: int | None = None):
        self._log.debug("Authenticating...")
        self.send(self._get_auth_payload(), client_id=client_id)
        await asyncio.sleep(5)  # wait for auth response
        self._log.debug("Authentication payload sent.")

    def _generate_signature(self):
        if self._secret is None:
            raise ValueError("Secret key is not set for signature generation")

        expires = self.timestamp_ms() + 1_000
        signature = str(
            hmac.new(
                bytes(self._secret, "utf-8"),
                bytes(f"GET/realtime{expires}", "utf-8"),
                digestmod="sha256",
            ).hexdigest()
        )
        return signature, expires

    def _get_auth_payload(self):
        signature, expires = self._generate_signature()
        return {"op": "auth", "args": [self._api_key, expires, signature]}

    def _send_payload(
        self,
        params: List[str],
        chunk_size: int = 100,
        op: str = "subscribe",
        client_id: int | None = None,
    ):
        # Split params into chunks of 100 if length exceeds 100
        params_chunks = [
            params[i : i + chunk_size] for i in range(0, len(params), chunk_size)
        ]

        for chunk in params_chunks:
            payload = {"op": op, "args": chunk}
            self.send(payload, client_id=client_id)

    def _subscribe(self, topics: List[str]):
        topics = [topic for topic in topics if topic not in self._subscriptions]

        for topic in topics:
            self._log.debug(f"Subscribing to {topic}...")

        if not topics:
            return

        self._register_subscriptions(topics)

    def _unsubscribe(self, topics: List[str]):
        if not topics:
            return

        removed = self._unregister_subscriptions(topics)
        if not removed:
            return

        for client_id, client_topics in removed.items():
            for topic in client_topics:
                self._log.debug(f"Unsubscribing from {topic}...")
            self._send_payload(client_topics, op="unsubscribe", client_id=client_id)

    def subscribe_order_book(self, symbols: List[str], depth: ORDER_BOOK_DEPTH):
        """
        subscribe to orderbook

        Args:
            symbols: list of symbols to subscribe
            depth: order book depth (1, 25, 50, 100, 200, 1000)
                - for option, only support 25, 100,
                - for inverse/linear/spot, support 1,50,200,1000
        """
        topics = [f"orderbook.{depth}.{symbol}" for symbol in symbols]
        self._subscribe(topics)

    def subscribe_trade(self, symbols: List[str]):
        """subscribe to trade"""
        topics = [f"publicTrade.{symbol}" for symbol in symbols]
        self._subscribe(topics)

    def subscribe_ticker(self, symbols: List[str]):
        """subscribe to ticker"""
        topics = [f"tickers.{symbol}" for symbol in symbols]
        self._subscribe(topics)

    def subscribe_kline(self, symbols: List[str], interval: KLINE_INTERVAL):
        """subscribe to kline"""
        topics = [f"kline.{interval}.{symbol}" for symbol in symbols]
        self._subscribe(topics)

    def subscribe_all_liquidation(self, symbols: List[str]):
        """subscribe to all liquidation"""
        topics = [f"allLiquidation.{symbol}" for symbol in symbols]
        self._subscribe(topics)

    def subscribe_insurance_pool(self, contract_types: List[CONTRACT_TYPE]):
        """subscribe to insurance pool"""
        topics = [f"insurancePool.{contract_type}" for contract_type in contract_types]
        self._subscribe(topics)

    def subscribe_adl_alert(self, coins: List[CONTRACT_TYPE]):
        """subscribe to adl alert"""
        topics = [f"adlAlert.{coin}" for coin in coins]
        self._subscribe(topics)

    def subscribe_position(self, topic: POSITION_TOPICS = "position"):
        """subscribe to position updates"""
        self._subscribe([topic])

    def subscribe_execution(self, topic: EXECUTION_TOPICS = "execution"):
        """subscribe to execution updates"""
        self._subscribe([topic])

    def subscribe_fast_execution(self, topic: EXECUTION_FAST_TOPICS = "execution.fast"):
        """subscribe to fast execution updates"""
        self._subscribe([topic])

    def subscribe_orders(self, topic: ORDER_TOPICS = "order"):
        """subscribe to orders updates"""
        self._subscribe([topic])

    def subscribe_wallet(self):
        """subscribe to wallet updates"""
        self._subscribe(["wallet"])

    def subscribe_greeks(self):
        """subscribe to greeks updates"""
        self._subscribe(["greeks"])

    async def _resubscribe_for_client(self, client_id: int, subscriptions: List[str]):
        if not subscriptions:
            return
        if self._api_key is not None:
            await self.auth(client_id=client_id)
        self._send_payload(subscriptions, client_id=client_id)
