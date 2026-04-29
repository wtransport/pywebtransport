# Performance Benchmarks

This document defines the performance characteristics of the `PyWebTransport` library. The benchmarks quantify the implementation overhead of the protocol stack—covering connection establishment, stream multiplexing, and datagram processing—isolated from physical network latency constraints.

## 1. Test Environment

The test configuration detailed below serves as the reference environment for all measurements presented in this document.

| Component            | Specification                                      |
| :------------------- | :------------------------------------------------- |
| **Library Version**  | `PyWebTransport v0.17.0` (Ref: `HEAD`)             |
| **Python Runtime**   | CPython 3.12.13                                    |
| **Rust Compiler**    | rustc 1.93.1                                       |
| **Event Loop**       | `uvloop` v0.22.1                                   |
| **Cryptography**     | `rustls` v0.23.36 (ring)                           |
| **Test Suite**       | `pytest-benchmark`                                 |
| **OS / Kernel**      | Debian 12.12 / Linux 6.1.0-41-amd64                |
| **CPU Architecture** | Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz          |
| **CPU Scaling**      | Dual-threaded (GIL-bound Loop + Dedicated Reactor) |
| **vCPU Allocation**  | 4 Cores                                            |
| **Memory**           | 8 GB                                               |
| **Hypervisor**       | VMware ESXi 7.0 Update 3                           |

## 2. Methodology

The following methodologies are enforced to ensure result reproducibility and statistical significance:

- **Event Loop Policy**: `uvloop` is mandated for all test cases.
- **Garbage Collection**: Measurements incorporate Python garbage collection overhead to reflect production runtime characteristics.
- **Process Isolation**: All test cases are executed in isolated processes, restarted between runs to ensure deterministic memory states.
- **Warm-up Phase**: A warm-up cycle precedes all measurements to stabilize branch prediction and internal caching effects.
- **Measurement Metrics**:
  - **Latency**: Metrics include Minimum, Median (p50), and Maximum values to accurately quantify network jitter and tail latency characteristics.
  - **Throughput**: Reported as the mean sustained data transfer rate.
  - **Overhead**: Application logging is disabled (`CRITICAL` level) to eliminate I/O blocking.

## 3. Stream Throughput

This section details the sustained goodput over reliable WebTransport streams, utilizing a 1 MB payload per stream.

| Scenario     | Result (MB/s) |
| :----------- | :------------ |
| **Upload**   | `28.13` MB/s  |
| **Download** | `33.32` MB/s  |
| **Duplex**   | `52.90` MB/s  |

## 4. Latency & RTT

This section measures the Round-Trip Time (RTT) for application-layer interactions under the methodology defined in Section 2.

| Metric                          | Min        | Median (p50) | Max        |
| :------------------------------ | :--------- | :----------- | :--------- |
| **Handshake** (Connect → Ready) | `6.38` ms  | `10.99` ms   | `21.42` ms |
| **Request-Response** (64B)      | `10.99` ms | `12.80` ms   | `21.36` ms |
| **Request-Response** (1KB)      | `11.65` ms | `12.97` ms   | `22.42` ms |
| **Datagram RTT**                | `10.87` ms | `12.12` ms   | `23.29` ms |

## 5. Concurrency & Multiplexing

This section evaluates connection scalability when handling concurrent flows on a single session.

| Metric              | Result       | Description                                                                 |
| :------------------ | :----------- | :-------------------------------------------------------------------------- |
| **RPC Throughput**  | `433.81` RPS | Measures Requests Per Second with 100 concurrent streams using 64KB payload |
| **Connection Rate** | `215.66` CPS | Measures Connections Per Second sustaining 50 concurrent handshakes         |

## 6. Datagram Performance

This section measures the packet processing rate for unreliable datagrams (HTTP/3 Datagrams).

| Metric        | Result          | Description                                                          |
| :------------ | :-------------- | :------------------------------------------------------------------- |
| **Send Rate** | `23,769.12` PPS | Tests utilize a 64-byte payload transmitted in a non-blocking burst. |

## 7. Resource Utilization

This section measures the system memory footprint per connection in a steady, idle state.

| Metric                         | Result     |
| :----------------------------- | :--------- |
| **Memory per Idle Connection** | `76.28` KB |
