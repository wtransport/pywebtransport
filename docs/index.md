---
hide:
  - navigation
  - toc
---

<div style="text-align: center">
  <h1>
    <img src="assets/favicon.svg" alt="PyWebTransport Icon" width="120" />
  </h1>

  <p>
    <em>An async-native WebTransport stack for Python</em>
  </p>
</div>

---

## What is PyWebTransport?

This library enables low-latency, bidirectional communication over QUIC and HTTP/3.
It provides a complete, robust implementation of streams and datagrams
alongside a high-level application framework, with a focus on standards compliance,
performance, and reliability.

**Key Features:**

- **Pure Async**: Built entirely on `asyncio` for high-concurrency, non-blocking I/O operations.
- **Event Architecture**: Powered by a Sans-I/O unified state machine and a strictly typed `EventEmitter`.
- **Zero-Copy I/O**: End-to-end support for `memoryview` and buffer protocols to minimize data copying overhead.
- **Structured Messaging**: Transmission of typed Python objects via pluggable serializers (`JSON`, `MsgPack`, `Protobuf`).
- **High-Level Abstractions**: `ServerApp` with routing and middleware, plus `WebTransportClient` utilities for fleet management.
- **Protocol Completeness**: Implementation of bidirectional streams, unidirectional streams, and unreliable datagrams.
- **Resource Safety**: Async context managers for automatic connection, session, and stream lifecycle management.
- **Type-Safe & Tested**: Fully type-annotated API with comprehensive unit, integration, end-to-end, and benchmark coverage.

## Core Concepts

### Client and Server

The library provides two main entry points: `WebTransportClient` for creating client connections and `ServerApp` for building server applications with routing and middleware.

### Sessions

A `WebTransportSession` represents an active connection and is the primary interface for communication. Each session can manage multiple streams and datagrams.

### Streams

Streams provide reliable, ordered communication with automatic flow control. Use bidirectional streams for request-response patterns and unidirectional streams for one-way data transfer.

### Datagrams

Datagrams offer low-latency, unreliable, and out-of-order messaging, ideal for real-time applications where speed is more important than guaranteed delivery.

## Use Cases

### Real-time Interactivity

- **Live Collaboration:** Real-time document editing, whiteboarding, and design tools.
- **Instant Messaging:** Live chat, notifications, and signaling services.
- **Interactive Entertainment:** Multiplayer online gaming, virtual reality (VR), and live stream interactions.
- **Real-time Dashboards:** Financial trading platforms, monitoring panels, and live sports updates.

### High-Performance Data Streaming

- **Media Streaming:** Live video, audio feeds, and webcam broadcasting.
- **Large-Scale Data Transfer:** Bulk file uploads/downloads and data synchronization.
- **IoT & Telemetry:** Efficiently aggregating sensor data from a multitude of devices.

### AI & Machine Learning

- **Real-time Inference:** Stream live audio, video, or sensor data to models for analysis.
- **Distributed Training:** Efficiently exchange model gradients and parameters between nodes.
- **Data Augmentation:** Feed large datasets to augmentation pipelines with low latency.

### Modern Backend Architecture

- **Microservice Communication:** A low-latency, high-throughput message bus between services.
- **API Acceleration:** Reduce repetitive HTTP overhead with persistent connections.
- **Edge Computing:** Establish efficient data channels between edge nodes and the cloud.

## Interoperability

[https://interop.wtransport.org](https://interop.wtransport.org)

- **/echo**: Bidirectional stream and datagram reflection.
- **/status**: Global server health and aggregate metrics.
- **/stats**: Current session statistics and negotiated parameters.

## API Reference

- **[Complete API Reference](api-reference/index.md)** - All available APIs, including client, server, session, stream, datagrams, and configuration. The API is organized into layers (Application, Core, and Foundational) to help you find the right abstraction for your needs.

## Community

- **GitHub** - [Source code and issues](https://github.com/wtransport/pywebtransport)
- **PyPI** - [Package distribution](https://pypi.org/project/pywebtransport/)

## License

PyWebTransport is released under the Apache License 2.0. See [LICENSE](https://github.com/wtransport/pywebtransport/blob/main/LICENSE) for details.
