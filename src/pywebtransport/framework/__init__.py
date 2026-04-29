"""Application framework for WebTransport services."""

from .app import ServerApp
from .middleware import (
    AuthHandlerProtocol,
    MiddlewareManager,
    MiddlewareProtocol,
    MiddlewareRejected,
    StatefulMiddlewareProtocol,
    create_auth_middleware,
    create_cors_middleware,
    create_rate_limit_middleware,
)
from .router import RequestRouter, SessionHandler

__all__: list[str] = [
    "AuthHandlerProtocol",
    "MiddlewareManager",
    "MiddlewareProtocol",
    "MiddlewareRejected",
    "RequestRouter",
    "ServerApp",
    "SessionHandler",
    "StatefulMiddlewareProtocol",
    "create_auth_middleware",
    "create_cors_middleware",
    "create_rate_limit_middleware",
]
