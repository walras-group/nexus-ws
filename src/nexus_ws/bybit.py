import picows
import msgspec
from enum import Enum
from typing import Any, Callable, List, Literal


from .ws import WSClient, Listener


class BybitStreamUrl(Enum):
    SPOT = "wss://stream.bybit.com/v5/public/spot"
    LINEAR = "wss://stream.bybit.com/v5/public/linear"
    INVERSE = "wss://stream.bybit.com/v5/public/inverse"
    OPTION = "wss://stream.bybit.com/v5/public/option"


KLINE_INTERVAL = Literal[
    "1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M"
]

ORDER_BOOK_DEPTH = Literal[1, 25, 50, 100, 200, 1000]
CONTRACT_TYPE = Literal["USDT", "USDC", "inverse"]


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
        url: BybitStreamUrl,
    ):
        super().__init__(
            url.value,
            handler=handler,
            ping_idle_timeout=20,
            ping_reply_timeout=2,
            specific_ping_msg=msgspec.json.encode({"op": "ping"}),
            auto_ping_strategy="ping_when_idle",
            user_pong_callback=user_pong_callback,
        )

    def _send_payload(
        self, params: List[str], chunk_size: int = 100, op: str = "subscribe"
    ):
        # Split params into chunks of 100 if length exceeds 100
        params_chunks = [
            params[i : i + chunk_size] for i in range(0, len(params), chunk_size)
        ]

        for chunk in params_chunks:
            payload = {"op": op, "args": chunk}
            self.send(payload)

    def _subscribe(self, topics: List[str]):
        topics = [topic for topic in topics if topic not in self._subscriptions]

        for topic in topics:
            self._subscriptions.append(topic)
            self._log.debug(f"Subscribing to {topic}...")

        if not topics:
            return
        # self._send_payload(topics)

    def _unsubscribe(self, topics: List[str]):
        topics = [topic for topic in topics if topic in self._subscriptions]

        for topic in topics:
            self._subscriptions.remove(topic)
            self._log.debug(f"Unsubscribing from {topic}...")

        if not topics:
            return
        self._send_payload(topics, op="unsubscribe")

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

    def resubscribe(self):
        self._send_payload(self._subscriptions)
