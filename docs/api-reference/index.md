# API Reference

Technical reference for the PyWebTransport public interface.

## Overview

The API is organized into three **hierarchical layers**: the **Application Framework** for high-level integration, the **Transport Layer** for protocol state management, and **Shared Primitives** for data structures and configuration.

## Application Framework

High-level abstractions for application development, routing, and object-level transmission.

| Module                          | Description                                                     | Key Components                                              |
| :------------------------------ | :-------------------------------------------------------------- | :---------------------------------------------------------- |
| **[Client](client.md)**         | Client-side state orchestration and connectivity management.    | `WebTransportClient`, `ClientFleet`, `ReconnectingClient`   |
| **[Server](server.md)**         | Server-side application logic, request routing, and middleware. | `ServerApp`, `WebTransportServer`, `RequestRouter`          |
| **[Messaging](messaging.md)**   | Typed object transmission over streams and datagrams.           | `StructuredStream`, `StructuredDatagramTransport`           |
| **[Serializer](serializer.md)** | Pluggable serialization protocols for typed messaging.          | `JSONSerializer`, `MsgPackSerializer`, `ProtobufSerializer` |

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

| Module                          | Description                                                  | Key Components                          |
| :------------------------------ | :----------------------------------------------------------- | :-------------------------------------- |
| **[Configuration](config.md)**  | Immutable configuration data classes for endpoints.          | `ClientConfig`, `ServerConfig`          |
| **[Events](events.md)**         | Asynchronous event emission primitives.                      | `EventEmitter`, `Event`, `EventHandler` |
| **[Types](types.md)**           | Type aliases, protocols, and enumerations.                   | `StreamId`, `SessionId`, `StreamState`  |
| **[Exceptions](exceptions.md)** | Protocol error hierarchy and exception handling.             | `WebTransportError`, `StreamError`      |
| **[Constants](constants.md)**   | Protocol constants, error codes, and default values.         | `ErrorCodes`                            |
| **[Utils](utils.md)**           | Auxiliary utilities for timing and operational measurements. | `Timer`                                 |
