<div align="center">
  <img
    src="https://raw.githubusercontent.com/wtransport/pywebtransport/main/docs/assets/favicon.svg"
    alt="PyWebTransport Logo"
    width="100"
  />

# PyWebTransport

_An async-native WebTransport stack for Python_

  <br />

[![PyPI version](https://badge.fury.io/py/pywebtransport.svg)](https://pypi.org/project/pywebtransport/)
[![Python Version](https://img.shields.io/pypi/pyversions/pywebtransport)](https://pypi.org/project/pywebtransport/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/wtransport/pywebtransport/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wtransport/pywebtransport/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/wtransport/pywebtransport/branch/main/graph/badge.svg)](https://codecov.io/gh/wtransport/pywebtransport)
[![Docs](https://app.readthedocs.org/projects/pywebtransport/badge/?version=latest)](https://python.wtransport.org/)

</div>

## Features

- **Sans-I/O Architecture**: Powered by a unified, deterministic state machine decoupled from the I/O runtime.
- **Transport Primitives**: Full implementation of bidirectional streams, unidirectional streams, and unreliable datagrams.
- **Structured Concurrency**: Deterministic lifecycle management for connections and streams via asynchronous context managers.
- **Zero-Copy I/O**: End-to-end support for buffer protocols and `memoryview` to minimize data copying overhead.
- **Typed Messaging**: Integrated transmission of Python objects via pluggable serializers (`JSON`, `MsgPack`, `Protobuf`).
- **Application Framework**: Includes `ServerApp` with routing and middleware, plus a composable client suite for connection resilience and fleet management.

## Installation

```bash
pip install pywebtransport
```

## Quick Start

### Server

```python
import asyncio

from pywebtransport import Event, ServerApp, ServerConfig, WebTransportSession, WebTransportStream
from pywebtransport.types import EventType
from pywebtransport.utils import generate_self_signed_cert

generate_self_signed_cert(hostname="localhost")

app = ServerApp(config=ServerConfig(certfile="localhost.crt", keyfile="localhost.key"))


@app.route(path="/")
async def echo_handler(session: WebTransportSession) -> None:
    async def on_datagram(event: Event) -> None:
        if isinstance(event.data, dict) and (data := event.data.get("data")):
            await session.send_datagram(data=b"ECHO: " + data)

    async def on_stream(event: Event) -> None:
        if isinstance(event.data, dict) and (stream := event.data.get("stream")):
            if isinstance(stream, WebTransportStream):
                asyncio.create_task(handle_stream(stream))

    session.events.on(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)
    session.events.on(event_type=EventType.STREAM_OPENED, handler=on_stream)

    try:
        await session.events.wait_for(event_type=EventType.SESSION_CLOSED)
    finally:
        session.events.off(event_type=EventType.DATAGRAM_RECEIVED, handler=on_datagram)
        session.events.off(event_type=EventType.STREAM_OPENED, handler=on_stream)


async def handle_stream(stream: WebTransportStream) -> None:
    try:
        data = await stream.read_all()
        await stream.write_all(data=b"ECHO: " + data, end_stream=True)
    except Exception:
        pass


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=4433)
```

### Client

```python
import asyncio
import ssl

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType


async def main() -> None:
    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://127.0.0.1:4433/")

        await session.send_datagram(data=b"Hello, Datagram!")

        event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
        if isinstance(event.data, dict) and (data := event.data.get("data")):
            print(f"Datagram: {data!r}")

        stream = await session.create_bidirectional_stream()
        await stream.write_all(data=b"Hello, Stream!", end_stream=True)

        response = await stream.read_all()
        print(f"Stream: {response!r}")

        await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

## Interoperability

**Infrastructure**

- [**Public Instance**](https://interop.wtransport.org): `https://interop.wtransport.org`, _Native Dual-Stack_
- [**Container Image**](https://github.com/wtransport/pywebtransport/pkgs/container/interop-server): `ghcr.io/wtransport/interop-server:latest`, _UDP Port 4433_

**Endpoints**

- **/echo**: Bidirectional stream and datagram reflection.
- **/stats**: Current session statistics and negotiated parameters.
- **/status**: Global server health and aggregate metrics.

## Sponsors

<div>
  <br />
  <a href="https://www.fastly.com/" target="_blank" rel="noopener noreferrer">
    <img
      src="https://raw.githubusercontent.com/wtransport/pywebtransport/main/docs/assets/sponsor-fastly.svg"
      alt="Fastly"
      width="110"
    />
  </a>
</div>

## License

Distributed under the terms of the Apache License 2.0. See [`LICENSE`](https://github.com/wtransport/pywebtransport/blob/main/LICENSE) for details.
