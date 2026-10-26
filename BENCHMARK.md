# Performance Benchmarks

This document reports the performance characteristics of the `PyWebTransport` library. The benchmarks quantify the implementation overhead of the protocol stack—covering connection establishment, stream multiplexing, and datagram processing—isolated from physical network latency constraints.

## 1. Test Environment

The test configuration detailed below serves as the reference environment for all measurements presented in this document.

| Component            | Specification                                      |
| :------------------- | :------------------------------------------------- |
| **Library Version**  | `PyWebTransport v0.21.1` (Ref: `HEAD`)             |
| **Python Runtime**   | CPython 3.12.10                                    |
| **Rust Compiler**    | rustc 1.97.1                                       |
| **Cryptography**     | `rustls` v0.23.46 (ring)                           |
| **Test Suite**       | `pytest-benchmark`                                 |
| **OS / Kernel**      | Debian 12.15 / Linux 6.1.0-54-amd64                |
| **CPU Architecture** | x86-64                                             |
| **CPU Scaling**      | Dual-threaded (GIL-bound Loop + Dedicated Reactor) |
| **vCPU Allocation**  | 4 Cores                                            |
| **Memory**           | 8 GB                                               |
| **Platform**         | OSU Open Source Lab OpenStack (Multi-Tenant)       |

## 2. Methodology

The following methodologies are enforced to ensure result reproducibility:

- **Garbage Collection**: Measurements incorporate Python garbage collection overhead to reflect production runtime characteristics.
- **Process Isolation**: Client and server processes are restarted for each test module.
- **Warm-up Phase**: Five warm-up rounds precede each timed measurement to exclude one-time initialization costs.
- **Measurement Metrics**:
  - **Latency**: Metrics include Minimum, Median (p50), and Maximum values.
  - **Throughput**: Reported as the mean sustained data transfer rate.
  - **Overhead**: Application logging is disabled (`CRITICAL` level) to eliminate I/O blocking.

## 3. Stream Throughput

This section details the sustained goodput over reliable WebTransport streams, utilizing a 100 MB payload per stream.

| Scenario     | Result (MB/s) |
| :----------- | :------------ |
| **Upload**   | `262.92` MB/s |
| **Download** | `255.75` MB/s |
| **Duplex**   | `258.30` MB/s |

## 4. Latency & RTT

This section measures the Round-Trip Time (RTT) for application-layer interactions under the methodology defined in Section 2.

| Metric                          | Min       | Median (p50) | Max       |
| :------------------------------ | :-------- | :----------- | :-------- |
| **Handshake** (Connect → Ready) | `3.92` ms | `4.24` ms    | `8.64` ms |
| **Request-Response** (64B)      | `0.78` ms | `0.89` ms    | `2.76` ms |
| **Request-Response** (1KB)      | `0.79` ms | `0.88` ms    | `1.42` ms |
| **Datagram RTT**                | `0.26` ms | `0.29` ms    | `1.20` ms |

## 5. Concurrency & Multiplexing

This section evaluates stream multiplexing within a single session and concurrent connection establishment.

| Metric              | Result         | Description                                                                |
| :------------------ | :------------- | :------------------------------------------------------------------------- |
| **RPC Throughput**  | `2,609.12` RPS | Measures Requests Per Second with 20 concurrent streams using 64KB payload |
| **Connection Rate** | `1,199.18` CPS | Measures Connections Per Second sustaining 50 concurrent handshakes        |

## 6. Datagram Performance

This section measures the packet processing rate for unreliable datagrams (HTTP/3 Datagrams).

| Metric        | Result          | Description                                                          |
| :------------ | :-------------- | :------------------------------------------------------------------- |
| **Send Rate** | `24,139.32` PPS | Tests utilize a 64-byte payload transmitted in a non-blocking burst. |

## 7. Resource Utilization

This section measures the system memory footprint per connection in a steady, idle state.

| Metric                         | Result     |
| :----------------------------- | :--------- |
| **Memory per Idle Connection** | `60.51` KB |
