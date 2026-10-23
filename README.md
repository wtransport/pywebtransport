<div align="center">
  <img
    src="https://raw.githubusercontent.com/wtransport/pywebtransport/main/docs/assets/favicon.svg"
    alt="PyWebTransport Logo"
    width="100"
  />

# PyWebTransport

_An async-native WebTransport stack_

  <br />

[![PyPI version](https://badge.fury.io/py/pywebtransport.svg)](https://pypi.org/project/pywebtransport/)
[![Python Version](https://img.shields.io/pypi/pyversions/pywebtransport)](https://pypi.org/project/pywebtransport/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/wtransport/pywebtransport/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wtransport/pywebtransport/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/wtransport/pywebtransport/branch/main/graph/badge.svg)](https://codecov.io/gh/wtransport/pywebtransport)
[![Docs](https://app.readthedocs.org/projects/pywebtransport/badge/?version=latest)](https://python.wtransport.org/)

</div>

## Features

- **Sans-I/O Architecture**: Ownership-driven Rust state machine and autonomous threaded reactor.
- **Transport Primitives**: Full implementation of bidirectional streams, unidirectional streams, and unreliable datagrams.

## Installation

```bash
pip install pywebtransport
```

## Interoperability

**Infrastructure**

- [**Public Instance**](https://interop.wtransport.org): `https://interop.wtransport.org`, _Native Dual-Stack_
- [**Container Image**](https://github.com/wtransport/pywebtransport/pkgs/container/interop-server): `ghcr.io/wtransport/interop-server:latest`, _UDP Port 4433_

**Endpoints**

- **/echo**: Bidirectional stream and datagram reflection.
- **/stats**: Current session statistics and negotiated parameters.
- **/webtransport/devious-baton**: Devious Baton protocol state transition validation.

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
  &nbsp;&nbsp;&nbsp;&nbsp;
  <a href="https://osuosl.org/" target="_blank" rel="noopener noreferrer">
    <img
      src="https://raw.githubusercontent.com/wtransport/pywebtransport/main/docs/assets/sponsor-osuosl.png"
      alt="OSU Open Source Lab"
      width="90"
    />
  </a>
</div>

## License

Distributed under the terms of the Apache License 2.0. See [`LICENSE`](https://github.com/wtransport/pywebtransport/blob/main/LICENSE) for details.
