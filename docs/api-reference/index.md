# API Reference

Technical reference for the PyWebTransport public interface.

## Overview

The API is organized into three **hierarchical layers**: the **Application Framework** for high-level integration, the **Transport Layer** for protocol state management, and **Shared Primitives** for data structures and configuration.

## Application Framework

High-level entry points and declarative abstractions for endpoint routing and middleware.

| Module                        | Description                                                    | Key Components                                    |
| :---------------------------- | :------------------------------------------------------------- | :------------------------------------------------ |
| **[Client](client.md)**       | Client-side connectivity and endpoint orchestration.           | `WebTransportClient`                              |
| **[Server](server.md)**       | Server-side transport listener and connection management.      | `WebTransportServer`                              |
| **[Framework](framework.md)** | Declarative application abstractions, routing, and middleware. | `ServerApp`, `RequestRouter`, `MiddlewareManager` |

## Transport Layer

Low-level components managing the WebTransport protocol state machine, lifecycle, and I/O boundaries.

| Module                          | Description                                                | Key Components                                                              |
| :------------------------------ | :--------------------------------------------------------- | :-------------------------------------------------------------------------- |
| **[Session](session.md)**       | WebTransport session lifecycle and multiplexing control.   | `WebTransportSession`                                                       |
| **[Stream](stream.md)**         | Bidirectional and unidirectional stream I/O primitives.    | `WebTransportStream`, `WebTransportSendStream`, `WebTransportReceiveStream` |
| **[Connection](connection.md)** | Underlying QUIC connection state and transport parameters. | `WebTransportConnection`                                                    |
| **[Manager](manager.md)**       | Resource lifecycle management and concurrency control.     | `ConnectionManager`, `SessionManager`                                       |

## Shared Primitives

Cross-cutting types, exceptions, and configuration data classes used throughout the stack.

| Module                          | Description                                                | Key Components                              |
| :------------------------------ | :--------------------------------------------------------- | :------------------------------------------ |
| **[Configuration](config.md)**  | Immutable configuration data classes for endpoints.        | `ClientConfig`, `ServerConfig`              |
| **[Events](events.md)**         | Asynchronous event emission primitives.                    | `EventEmitter`, `Event`, `EventHandler`     |
| **[Types](types.md)**           | Type aliases, protocols, and enumerations.                 | `StreamId`, `SessionId`, `StreamState`      |
| **[Exceptions](exceptions.md)** | Protocol error hierarchy and exception handling.           | `WebTransportError`, `StreamError`          |
| **[Constants](constants.md)**   | Protocol constants, error codes, and default values.       | `ErrorCodes`                                |
| **[Utils](utils.md)**           | Shared, general-purpose utilities and operational helpers. | `generate_self_signed_cert`, `init_tracing` |
