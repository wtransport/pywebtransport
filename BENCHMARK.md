# Performance Benchmarks

This document defines the performance characteristics of the `PyWebTransport` library. The benchmarks quantify the implementation overhead of the protocol stack—covering connection establishment, stream multiplexing, and datagram processing—isolated from physical network latency constraints.

## 1. Test Environment

The test configuration detailed below serves as the reference environment for all measurements presented in this document.

| Component            | Specification                             |
| :------------------- | :---------------------------------------- |
| **Library Version**  | `PyWebTransport v0.13.0` (Ref: `HEAD`)    |
| **Python Runtime**   | CPython 3.12.12                           |
| **Event Loop**       | `uvloop` v0.22.1+                         |
| **Cryptography**     | OpenSSL 3.0.17                            |
| **Test Suite**       | `pytest-benchmark`                        |
| **OS / Kernel**      | Debian 12.12 / Linux 6.1.0-41-amd64       |
| **CPU Architecture** | Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz |
| **CPU Scaling**      | Single-threaded (GIL-bound)               |
| **vCPU Allocation**  | 4 Cores                                   |
| **Memory**           | 8 GB                                      |
| **Hypervisor**       | VMware ESXi 7.0 Update 3                  |

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
| **Upload**   | `4.88` MB/s   |
| **Download** | `5.31` MB/s   |
| **Duplex**   | `8.05` MB/s   |

## 4. Latency & RTT

This section measures the Round-Trip Time (RTT) for application-layer interactions under the methodology defined in Section 2.

| Metric                          | Min        | Median (p50) | Max        |
| :------------------------------ | :--------- | :----------- | :--------- |
| **Handshake** (Connect → Ready) | `12.61` ms | `23.68` ms   | `33.87` ms |
| **Request-Response** (64B)      | `23.07` ms | `27.99` ms   | `55.91` ms |
| **Request-Response** (1KB)      | `24.62` ms | `26.75` ms   | `30.39` ms |
| **Datagram RTT**                | `24.70` ms | `25.75` ms   | `28.80` ms |

## 5. Concurrency & Multiplexing

This section evaluates connection scalability when handling concurrent flows on a single session.

| Metric              | Result       | Description                                                                 |
| :------------------ | :----------- | :-------------------------------------------------------------------------- |
| **RPC Throughput**  | `35.59` RPS  | Measures Requests Per Second with 100 concurrent streams using 64KB payload |
| **Connection Rate** | `109.95` CPS | Measures Connections Per Second sustaining 50 concurrent handshakes         |

## 6. Datagram Performance

This section measures the packet processing rate for unreliable datagrams (HTTP/3 Datagrams).

| Metric        | Result         | Description                                                          |
| :------------ | :------------- | :------------------------------------------------------------------- |
| **Send Rate** | `9,755.58` PPS | Tests utilize a 64-byte payload transmitted in a non-blocking burst. |

## 7. Resource Utilization

This section measures the system memory footprint per connection in a steady, idle state.

| Metric                         | Result      |
| :----------------------------- | :---------- |
| **Memory per Idle Connection** | `125.01` KB |
