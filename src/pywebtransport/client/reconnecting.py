"""Client wrapper for automatic reconnection logic."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Self

from pywebtransport.client.client import WebTransportClient
from pywebtransport.events import EventEmitter
from pywebtransport.exceptions import ClientError, ConnectionError, TimeoutError
from pywebtransport.session import WebTransportSession
from pywebtransport.types import URL, EventType, SessionState
from pywebtransport.utils import get_logger

__all__: list[str] = ["ReconnectingClient"]

logger = get_logger(name=__name__)


class ReconnectingClient(EventEmitter):
    """A client that automatically reconnects based on the provided configuration."""

    def __init__(self, *, url: URL, client: WebTransportClient) -> None:
        """Initialize the reconnecting client."""
        self._config = client.config
        super().__init__(
            max_queue_size=self._config.max_event_queue_size,
            max_listeners=self._config.max_event_listeners,
            max_history=self._config.max_event_history_size,
        )
        self._url = url
        self._client = client
        self._session: WebTransportSession | None = None
        self._tg: asyncio.TaskGroup | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._closed = False
        self._is_initialized = False
        self._connected_event = asyncio.Event()
        self._crashed_exception: BaseException | None = None

    @property
    def is_connected(self) -> bool:
        """Check if the client is currently connected with a ready session."""
        return (
            self._session is not None
            and self._session.state == SessionState.CONNECTED
            and self._connected_event.is_set()
        )

    async def __aenter__(self) -> Self:
        """Enter the async context and start the reconnect loop."""
        if self._closed:
            raise ClientError(message="Client is already closed")
        if self._is_initialized:
            return self

        self._tg = asyncio.TaskGroup()
        await self._tg.__aenter__()

        self._reconnect_task = self._tg.create_task(coro=self._reconnect_loop())
        self._is_initialized = True
        logger.info("ReconnectingClient started for URL: %s", self._url)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit the async context and ensure the client is closed."""
        await self.close()
        if self._tg is not None:
            await self._tg.__aexit__(exc_type, exc_val, exc_tb)

    async def close(self) -> None:
        """Close the reconnecting client and all its resources."""
        if self._closed:
            return

        logger.info("Closing reconnecting client")
        self._closed = True
        self._connected_event.set()

        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        if self._session is not None:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning("Error closing session: %s", e)
            finally:
                self._session = None

        logger.info("Reconnecting client closed")

    async def get_session(self, *, wait_timeout: float = 5.0) -> WebTransportSession:
        """Get the current session and wait for a connection if necessary."""
        if self._closed:
            raise ClientError(message="Client is closed")

        if self._crashed_exception is not None:
            raise ClientError(message="Background reconnection task crashed") from self._crashed_exception

        if self._tg is None:
            raise ClientError(
                message=(
                    "ReconnectingClient has not been activated. It must be used as an "
                    "asynchronous context manager (`async with ...`)."
                )
            )

        async with asyncio.timeout(delay=wait_timeout):
            while True:
                await self._connected_event.wait()

                if self._closed:
                    raise ClientError(message="Client closed while waiting for session")

                if self._crashed_exception is not None:
                    raise ClientError(message="Background task crashed") from self._crashed_exception

                session = self._session
                if session is not None and not session.is_closed:
                    return session

                if self._reconnect_task is not None and self._reconnect_task.done():
                    if self._reconnect_task.cancelled():
                        raise ClientError(message="Reconnection task cancelled.")
                    if exc := self._reconnect_task.exception():
                        raise ClientError(message=f"Reconnection task failed: {exc}") from exc
                    raise ClientError(message="Reconnection task finished unexpectedly.")

                self._connected_event.clear()

    async def _reconnect_loop(self) -> None:
        """Manage the connection lifecycle with an exponential backoff retry strategy."""
        retry_count = 0
        max_retries = self._config.max_connection_retries if self._config.max_connection_retries >= 0 else float("inf")
        initial_delay = self._config.retry_delay
        backoff_factor = self._config.retry_backoff
        max_delay = self._config.max_retry_delay

        try:
            while not self._closed:
                try:
                    self._session = await self._client.connect(url=self._url)
                    logger.info("Successfully connected to %s", self._url)

                    self._connected_event.set()
                    await self.emit(
                        event_type=EventType.CONNECTION_ESTABLISHED,
                        data={"session": self._session, "attempt": retry_count + 1},
                    )
                    retry_count = 0

                    if self._session.state != SessionState.CLOSED:
                        await self._session.events.wait_for(event_type=EventType.SESSION_CLOSED)

                    self._connected_event.clear()

                    if not self._closed:
                        logger.warning("Connection to %s lost, attempting to reconnect...", self._url)
                        await self.emit(event_type=EventType.CONNECTION_LOST, data={"url": self._url})

                except (ConnectionError, TimeoutError, ClientError) as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error("Max retries (%d) exceeded for %s", max_retries, self._url)
                        await self.emit(
                            event_type=EventType.CONNECTION_FAILED,
                            data={"reason": "max_retries_exceeded", "last_error": str(e)},
                        )
                        break

                    delay = min(initial_delay * (backoff_factor ** (retry_count - 1)), max_delay)
                    logger.warning(
                        "Connection attempt %d failed for %s, retrying in %.1fs: %s",
                        retry_count,
                        self._url,
                        delay,
                        e,
                        exc_info=True,
                    )
                    await asyncio.sleep(delay=delay)

                finally:
                    if self._session is not None:
                        try:
                            await self._session.close()
                        except Exception as e:
                            logger.debug("Error closing old session during reconnect: %s", e)
                        finally:
                            self._session = None

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._crashed_exception = e
            logger.critical("Reconnection loop crashed: %s", e, exc_info=True)
            self._connected_event.set()
        finally:
            self._connected_event.set()
            logger.info("Reconnection loop finished.")
