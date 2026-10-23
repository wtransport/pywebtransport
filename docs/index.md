---
hide:
  - navigation
  - toc
---

<div align="center">
  <h1>
    <img src="assets/favicon.svg" alt="PyWebTransport" width="120" />
  </h1>
  <p>
    <em>An async-native WebTransport stack</em>
  </p>
</div>

---

## Overview

**PyWebTransport** implements WebTransport over HTTP/3. It provides a deterministic state machine for streams and datagrams, and delivers peer-initiated connections, sessions, and streams through asynchronous iterators.

## Features

- **Sans-I/O Architecture**: Ownership-driven Rust state machine and autonomous threaded reactor.
- **Transport Primitives**: Full implementation of bidirectional streams, unidirectional streams, and unreliable datagrams.

## Interoperability

**Infrastructure**

- [**Public Instance**](https://interop.wtransport.org): `https://interop.wtransport.org`, _Native Dual-Stack_
- [**Container Image**](https://github.com/wtransport/pywebtransport/pkgs/container/interop-server): `ghcr.io/wtransport/interop-server:latest`, _UDP Port 4433_

**Endpoints**

- **/echo**: Bidirectional stream and datagram reflection.
- **/stats**: Current session statistics and negotiated parameters.
- **/webtransport/devious-baton**: Devious Baton protocol state transition validation.

## API Reference

- [**Full Reference**](api-reference/index.md): Technical reference for the PyWebTransport public interface.

## Community

- [**GitHub**](https://github.com/wtransport/pywebtransport): Source code and issue tracker.
- [**PyPI**](https://pypi.org/project/pywebtransport/): Package distribution.

## License

Distributed under the terms of the Apache License 2.0. See [`LICENSE`](https://github.com/wtransport/pywebtransport/blob/main/LICENSE) for details.
