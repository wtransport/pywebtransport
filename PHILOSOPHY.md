# Implementation Philosophy

PyWebTransport is engineered as a **standards-compliant and deterministic** implementation of the WebTransport protocol. The design prioritizes protocol correctness, architectural purity, and long-term maintainability over transient compatibility.

## Core Tenets

- **Standard Authority**: IETF Drafts and formal RFCs serve as the exclusive technical basis for implementation decisions.
- **Non-Accommodation of Deviations**: The core implementation rejects compatibility patches or workarounds for non-compliant user agents. Strict adherence constitutes the foundation of an interoperable ecosystem.

### 1. Strict Adherence to Standards

The primary directive of the library is correctness. The transport layer is implemented in strict accordance with the official IETF WebTransport specifications.

**Active Draft Consensus**: During the active IETF draft stage, the library prioritizes the Working Group's (WG) latest consensus, ensuring forward compatibility and avoiding the implementation of features deprecated by WG resolution.

### 2. Architectural Integrity

As a foundational infrastructure component, PyWebTransport enforces a strict separation of concerns to ensure reliability and maintainability.

- **Decoupled Logic**: The core protocol logic is kept independent of the I/O runtime and application state. This isolation ensures the transport layer remains deterministic and verifiable in any execution environment.
- **Composable Design**: High-level abstractions and ecosystem integrations are implemented as optional, modular layers. This structure prevents feature bloat and ensures the core remains focused solely on transport efficiency.

### 3. Verification Strategy

Development practices prioritize protocol compliance over accommodation of implementation-specific behaviors.

- **E2E Verification**: End-to-end (E2E) execution between the native client and server implementations serves as the primary validation mechanism.
- **Correctness Over Compatibility**: While external suites like Web Platform Tests (WPT) provide reference value, protocol correctness takes precedence over passing tests that reflect divergent browser behaviors. The objective is to promote standard compliance rather than mask deviations.
