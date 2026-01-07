import asyncio
import logging
import time
import msgspec
from abc import ABC, abstractmethod
from typing import Any
from typing import Callable, Literal
from types import MethodType

from picows import (
    ws_connect,
    WSFrame,
    WSTransport,
    WSListener,
    WSMsgType,
    WSAutoPingStrategy,
)


class Listener(WSListener):
    """WebSocket listener implementation that handles connection events and message frames.

    Inherits from picows.WSListener to provide WebSocket event handling functionality.
    """

    def __init__(
        self,
        callback,
        logger: logging.Logger,
        specific_ping_msg=None,
        user_pong_callback: Callable[["Listener", WSFrame], bool] | None = None,
        *args,
        **kwargs,
    ):
        """Initialize the WebSocket listener.

        Args:
            logger: Logger instance for logging events
            specific_ping_msg: Optional custom ping message
        """
        super().__init__(*args, **kwargs)
        self._log = logger
        self._specific_ping_msg: bytes = specific_ping_msg
        self._callback = callback

        if user_pong_callback:
            self.is_user_specific_pong = MethodType(user_pong_callback, self)  # type: ignore

    def send_user_specific_ping(self, transport: WSTransport) -> None:
        """Send a custom ping message or default ping frame.

        Args:
            transport (picows.WSTransport): WebSocket transport instance
        """
        if self._specific_ping_msg:
            transport.send(WSMsgType.TEXT, self._specific_ping_msg)
            self._log.debug(
                f"Sent user specific ping: `{self._specific_ping_msg.decode()}`."
            )
        else:
            transport.send_ping()
            self._log.debug("Sent default ping.")

    def on_ws_connected(self, transport: WSTransport) -> None:
        """Called when WebSocket connection is established.

        Args:
            transport (picows.WSTransport): WebSocket transport instance
        """
        self._log.debug("Connected to Websocket...")

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        """Called when WebSocket connection is closed.

        Args:
            transport (picows.WSTransport): WebSocket transport instance
        """
        self._log.debug("Disconnected from Websocket.")

    def _decode_frame(self, frame: WSFrame) -> str:
        """Decode the payload of a WebSocket frame safely.

        Args:
            frame (picows.WSFrame): Received WebSocket frame

        Returns:
            str: Decoded payload as UTF-8 text or a placeholder for binary data
        """
        try:
            return frame.get_payload_as_utf8_text()
        except Exception:
            return f"<binary data: {len(frame.get_payload_as_bytes())} bytes>"

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        """Handle incoming WebSocket frames.

        Args:
            transport (picows.WSTransport): WebSocket transport instance
            frame (picows.WSFrame): Received WebSocket frame
        """
        try:
            match frame.msg_type:
                case WSMsgType.TEXT:
                    self._callback(frame.get_payload_as_bytes())
                    return
                case WSMsgType.CLOSE:
                    close_code = frame.get_close_code()
                    self._log.warning(
                        f"Received close frame. Close code: {str(close_code)}"
                    )
                    return
        except Exception as e:
            self._log.exception(
                f"Error processing message: {str(e)}\nws_frame: {self._decode_frame(frame)}"
            )


class WSClient(ABC):
    def __init__(
        self,
        url: str,
        handler: Callable[..., Any],
        specific_ping_msg: bytes | None = None,
        reconnect_interval: int = 1,
        ping_idle_timeout: int = 2,
        ping_reply_timeout: int = 1,
        auto_ping_strategy: Literal[
            "ping_when_idle", "ping_periodically"
        ] = "ping_when_idle",
        enable_auto_ping: bool = True,
        enable_auto_pong: bool = True,
        user_pong_callback: Callable[["Listener", WSFrame], bool] | None = None,
        auto_reconnect_interval: int | None = None,
    ):
        self._url = url
        self._specific_ping_msg = specific_ping_msg
        self._reconnect_interval = reconnect_interval
        self._ping_idle_timeout = ping_idle_timeout
        self._ping_reply_timeout = ping_reply_timeout
        self._enable_auto_pong = enable_auto_pong
        self._enable_auto_ping = enable_auto_ping
        self._user_pong_callback = user_pong_callback
        self._listener: WSListener | None = None
        self._transport = None
        self._subscriptions = []
        self._wait_task: asyncio.Task | None = None
        self._auto_reconnect_interval = auto_reconnect_interval
        self._auto_reconnect_task: asyncio.Task | None = None
        self._callback = handler
        if auto_ping_strategy == "ping_when_idle":
            self._auto_ping_strategy = WSAutoPingStrategy.PING_WHEN_IDLE
        elif auto_ping_strategy == "ping_periodically":
            self._auto_ping_strategy = WSAutoPingStrategy.PING_PERIODICALLY
        self._log = logging.getLogger(name=str(self.__class__.__name__))

    async def __aenter__(self):
        """Enter async context manager."""
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context manager."""
        await self.stop()

        # Suppress KeyboardInterrupt and CancelledError for graceful shutdown
        if exc_type in (KeyboardInterrupt, asyncio.CancelledError):
            self._log.debug("Shutting down gracefully...")
            return True  # Suppress these exceptions

        return False  # Don't suppress other exceptions

    def timestamp_ms(self) -> int:
        return int(time.time() * 1000)

    def timestamp(self) -> float:
        return time.time()

    @property
    def connected(self) -> bool:
        return self._transport is not None

    async def _connect(self):
        self._log.debug(f"Connecting to Websocket at {self._url}...")
        WSListenerFactory = lambda: Listener(  # noqa: E731
            self._callback,
            self._log,
            self._specific_ping_msg,
            self._user_pong_callback,
        )
        self._transport, self._listener = await ws_connect(
            WSListenerFactory,
            self._url,
            enable_auto_ping=self._enable_auto_ping,
            auto_ping_idle_timeout=self._ping_idle_timeout,
            auto_ping_reply_timeout=self._ping_reply_timeout,
            auto_ping_strategy=self._auto_ping_strategy,
            enable_auto_pong=self._enable_auto_pong,
        )
        self._log.info(f"Websocket connected successfully to {self._url}.")

    async def _wait(self):
        while True:
            try:
                await self._connect()
                await self.resubscribe()
                await self._transport.wait_disconnected()  # type: ignore
                self._log.debug("Websocket disconnected.")
            except asyncio.CancelledError:
                self._log.info("Websocket connection loop cancelled.")
                break
            except Exception as e:
                self._log.error(f"Connection error: {e}")
            finally:
                self._clean_up()

            self._log.warning(
                f"Websocket reconnecting in {self._reconnect_interval} seconds..."
            )
            await asyncio.sleep(self._reconnect_interval)

    async def wait(self, timeout: float | None = None):
        if timeout is None:
            await self._wait()
        else:
            try:
                await asyncio.wait_for(self._wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

    async def _auto_reconnect_loop(self):
        """Periodically disconnect to trigger reconnection (e.g., every 24 hours)."""
        while True:
            try:
                await asyncio.sleep(self._auto_reconnect_interval)
                self._log.info("Auto-reconnect triggered, disconnecting...")
                self.disconnect()
            except asyncio.CancelledError:
                self._log.info("Auto-reconnect loop cancelled.")
                break
            except Exception as e:
                self._log.error(f"Error in auto-reconnect loop: {e}")
    
    def disconnect(self):
        """Manually disconnect the websocket."""
        if self._transport:
            self._transport.disconnect()

    def start(self) -> asyncio.Task:
        """Start the internal wait loop as a background asyncio task."""
        if self._wait_task and not self._wait_task.done():
            self._log.debug("Websocket wait loop already running.")
            return self._wait_task
        self._wait_task = asyncio.create_task(self._wait())

        # Start auto-reconnect task if configured
        if self._auto_reconnect_interval and not (
            self._auto_reconnect_task and not self._auto_reconnect_task.done()
        ):
            self._auto_reconnect_task = asyncio.create_task(self._auto_reconnect_loop())
            self._log.info(
                f"Auto-reconnect enabled: will reconnect every {self._auto_reconnect_interval} seconds."
            )

        return self._wait_task

    async def stop(self) -> None:
        """Cancel the background wait loop if it is running."""
        # Cancel auto-reconnect task first
        if self._auto_reconnect_task:
            self._auto_reconnect_task.cancel()
            try:
                await self._auto_reconnect_task
            except asyncio.CancelledError:
                pass
            self._auto_reconnect_task = None

        # Cancel main wait loop
        if not self._wait_task:
            return
        task, self._wait_task = self._wait_task, None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            self._log.info("Websocket wait loop cancelled via stop().")

    def send(self, payload: dict):
        if not self.connected:
            self._log.warning(f"Websocket not connected. drop msg: {str(payload)}")
            return
        self._transport.send(WSMsgType.TEXT, msgspec.json.encode(payload))  # type: ignore

    def _clean_up(self):
        self._transport, self._listener = None, None

    @abstractmethod
    async def resubscribe(self):
        pass
