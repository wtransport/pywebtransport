# Known Issues

## Active

### [KI-003] Interoperability Failure with Web Browsers due to Protocol Draft Divergence

- **Status**: Blocked
- **Component**: Google Chrome, Microsoft Edge, Mozilla Firefox
- **Version**: All
- **Date Discovered**: 2025-12-15

**Summary**

WebTransport sessions cannot be established with current stable versions of web browsers. The connection is terminated immediately following the handshake. This phenomenon results from a Protocol Version Mismatch where the library tracks the latest IETF consensus while browsers typically implement older draft versions.

**Symptoms**

Browser clients report generic protocol errors such as `net::ERR_QUIC_PROTOCOL_ERROR`. Server-side logs indicate a peer-initiated close citing protocol violations, specifically `HTTP/2 frame received in a HTTP/3 connection` or unexpected transport parameters.

**Root Cause Analysis**

The fundamental cause is a divergence in the targeted protocol draft versions between the client and server. As the WebTransport specification evolves, breaking changes are introduced such as the removal of `SETTINGS_WT_MAX_SESSIONS` in favor of explicit flow control. When a strictly compliant server interacts with a client utilizing an outdated draft dialect, the handshake fails due to incompatible transport parameters or frame expectations.

**Impact Assessment**

- **Functionality**: Interoperability with browser-based clients is unavailable.
- **Compliance**: The library remains compliant with the targeted IETF draft.

**Current Status & Resolution**

- **Tracking**: [wtransport/pywebtransport#2](https://github.com/wtransport/pywebtransport/issues/2)
- **Strategy**: Classified as Blocked pending ecosystem updates. Resolution is contingent upon browser vendors updating their network stacks to align with the evolving IETF WebTransport standard.

---

### [KI-002] Protocol Compliance Gap due to Lack of `RESET_STREAM_AT` Support

- **Status**: Confirmed
- **Component**: aioquic
- **Version**: All
- **Date Discovered**: 2025-09-12

**Summary**

The current version of `pywebtransport` does not support the `RESET_STREAM_AT` frame. This limitation originates from the underlying `aioquic` library, which lacks support for the `draft-ietf-quic-reliable-stream-reset` extension. While standard `RESET_STREAM` functionality is unaffected, this prevents full compliance with newer WebTransport specifications.

**Symptoms**

This issue does not produce direct error logs or crashes. The symptom is a functional gap in protocol compliance where applications cannot utilize reliable reset semantics to ensure delivery of specific bytes before stream termination.

**Root Cause Analysis**

The WebTransport specification mandates support for `RESET_STREAM_AT` to ensure predictable data delivery prior to reset. This requires implementation at the QUIC transport layer and cannot be emulated at the application layer.

**Impact Assessment**

- **Functionality**: Standard stream termination works as expected. Scenarios requiring at-least-once semantics during abrupt stream termination are affected.
- **Compliance**: Prevents 100% compliance with the latest IETF drafts.

**Current Status & Resolution**

- **Tracking**: [aiortc/aioquic#596](https://github.com/aiortc/aioquic/issues/596)
- **Strategy**: No application-layer workaround is available. Monitoring upstream progress.

## Resolved

### [KI-001] Race Condition in aioquic Core on Connection Shutdown

- **Status**: Resolved
- **Component**: aioquic
- **Version**: < 1.3.0
- **Date Discovered**: 2025-09-17
- **Date Resolved**: 2025-10-23

**Summary**

On the server side, a benign `AssertionError` could be triggered in the `aioquic` dependency when a client connection closed rapidly. This stemmed from a non-idempotent event handler in `aioquic` versions prior to 1.3.0.

**Symptoms**

The server logs periodically show an `ERROR` level traceback involving `_Selector_datagram_transport._read_ready()` which terminates with `AssertionError: cannot call reset() more than once`.

**Root Cause Analysis**

A race condition existed between Application Layer Cleanup (`reset_stream`), Peer Stream Closure (`STOP_SENDING`), and Peer Connection Closure (`CONNECTION_CLOSE`). In older versions, the internal stream reset function was not idempotent, causing an assertion failure if multiple cleanup events arrived simultaneously.

**Impact Assessment**

- **Functionality**: None. Data transfer and lifecycle management were correct.
- **Operations**: Significant log noise could trigger false positives in monitoring systems.

**Current Status & Resolution**

- **Tracking**: [aiortc/aioquic#597](https://github.com/aiortc/aioquic/issues/597)
- **Strategy**: Fixed in upstream `aioquic` v1.3.0. Mandatory dependency updated to `aioquic >= 1.3.0` within `pywebtransport`.
