---
hide:
  - navigation
  - toc
---

<div style="text-align: center">
  <h1>
    <img src="assets/favicon.svg" alt="PyWebTransport" width="120" />
  </h1>
  <p>
    <em>An async-native WebTransport stack for Python</em>
  </p>
</div>

---

## Overview

**PyWebTransport** implements the WebTransport protocol over QUIC and HTTP/3. It provides a deterministic state machine for streams and datagrams, alongside a high-level application framework designed for standards compliance and strict concurrency safety.

## Features

- **Sans-I/O Architecture**: Powered by a unified, deterministic state machine decoupled from the I/O runtime.
- **Transport Primitives**: Full implementation of bidirectional streams, unidirectional streams, and unreliable datagrams.
- **Structured Concurrency**: Deterministic lifecycle management for connections and streams via asynchronous context managers.
- **Zero-Copy I/O**: End-to-end support for buffer protocols and `memoryview` to minimize data copying overhead.
- **Typed Messaging**: Integrated transmission of Python objects via pluggable serializers (`JSON`, `MsgPack`, `Protobuf`).
- **Application Framework**: Includes `ServerApp` with routing and middleware, plus a composable client suite for connection resilience and fleet management.

## Interoperability

**Infrastructure**

- [**Public Instance**](https://interop.wtransport.org): `https://interop.wtransport.org`, _Native Dual-Stack_
- [**Container Image**](https://github.com/wtransport/pywebtransport/pkgs/container/interop-server): `ghcr.io/wtransport/interop-server:latest`, _UDP Port 4433_

**Endpoints**

- **/echo**: Bidirectional stream and datagram reflection.
- **/stats**: Current session statistics and negotiated parameters.
- **/status**: Global server health and aggregate metrics.

## API Reference

- [**Full Reference**](api-reference/index.md): Comprehensive documentation organized into the **Application Framework**, **Transport Layer**, and **Shared Primitives**.

## Community

- [**GitHub**](https://github.com/wtransport/pywebtransport): Source code and issue tracker.
- [**PyPI**](https://pypi.org/project/pywebtransport/): Package distribution.

## License

Distributed under the terms of the Apache License 2.0. See [`LICENSE`](https://github.com/wtransport/pywebtransport/blob/main/LICENSE) for details.
