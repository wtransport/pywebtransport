# API Reference

Technical reference for the PyWebTransport public interface.

| Module                          | Description              | Key Components                                                              |
| :------------------------------ | :----------------------- | :-------------------------------------------------------------------------- |
| **[Client](client.md)**         | WebTransport client.     | `WebTransportClient`                                                        |
| **[Configuration](config.md)**  | Common configuration.    | `ClientConfig`, `ServerConfig`                                              |
| **[Connection](connection.md)** | WebTransport connection. | `WebTransportConnection`                                                    |
| **[Constants](constants.md)**   | Common constants.        | `ErrorCodes`                                                                |
| **[Events](events.md)**         | Event emitter.           | `Event`, `EventEmitter`, `EventHandler`                                     |
| **[Exceptions](exceptions.md)** | Common exceptions.       | `StreamError`, `WebTransportError`                                          |
| **[Server](server.md)**         | WebTransport server.     | `WebTransportServer`                                                        |
| **[Session](session.md)**       | WebTransport session.    | `WebTransportSession`                                                       |
| **[Stream](stream.md)**         | WebTransport stream.     | `WebTransportReceiveStream`, `WebTransportSendStream`, `WebTransportStream` |
| **[Types](types.md)**           | Common types.            | `SessionId`, `StreamId`, `StreamReceiveState`, `StreamSendState`            |
| **[Utils](utils.md)**           | General utilities.       | `generate_self_signed_cert`, `init_tracing`                                 |
