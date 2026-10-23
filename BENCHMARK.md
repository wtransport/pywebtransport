# Performance Benchmarks

This document reports the performance characteristics of the `PyWebTransport` library. The benchmarks quantify the implementation overhead of the protocol stack—covering connection establishment, stream multiplexing, and datagram processing—isolated from physical network latency constraints.

## 1. Test Environment

The test configuration detailed below serves as the reference environment for all measurements presented in this document.

| Component            | Specification                                      |
| :------------------- | :------------------------------------------------- |
| **Library Version**  | `PyWebTransport v0.21.0` (Ref: `HEAD`)             |
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
| **Upload**   | `209.65` MB/s |
| **Download** | `219.28` MB/s |
| **Duplex**   | `230.11` MB/s |

## 4. Latency & RTT

This section measures the Round-Trip Time (RTT) for application-layer interactions under the methodology defined in Section 2.

| Metric                          | Min       | Median (p50) | Max       |
| :------------------------------ | :-------- | :----------- | :-------- |
| **Handshake** (Connect → Ready) | `4.47` ms | `4.78` ms    | `7.67` ms |
| **Request-Response** (64B)      | `0.82` ms | `0.89` ms    | `1.20` ms |
| **Request-Response** (1KB)      | `0.77` ms | `0.82` ms    | `1.22` ms |
| **Datagram RTT**                | `0.27` ms | `0.29` ms    | `0.55` ms |

## 5. Concurrency & Multiplexing

This section evaluates stream multiplexing within a single session and concurrent connection establishment.

| Metric              | Result         | Description                                                                |
| :------------------ | :------------- | :------------------------------------------------------------------------- |
| **RPC Throughput**  | `2,432.63` RPS | Measures Requests Per Second with 20 concurrent streams using 64KB payload |
| **Connection Rate** | `1,162.39` CPS | Measures Connections Per Second sustaining 50 concurrent handshakes        |

## 6. Datagram Performance

This section measures the packet processing rate for unreliable datagrams (HTTP/3 Datagrams).

| Metric        | Result          | Description                                                          |
| :------------ | :-------------- | :------------------------------------------------------------------- |
| **Send Rate** | `25,476.76` PPS | Tests utilize a 64-byte payload transmitted in a non-blocking burst. |

## 7. Resource Utilization

This section measures the system memory footprint per connection in a steady, idle state.

| Metric                         | Result     |
| :----------------------------- | :--------- |
| **Memory per Idle Connection** | `60.43` KB |
