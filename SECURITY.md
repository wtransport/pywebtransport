# Security Policy

## Supported Versions

| Version           | Supported | Notes                                                       |
| :---------------- | :-------- | :---------------------------------------------------------- |
| **Latest Stable** | **Yes**   | Security updates are restricted to the latest PyPI release. |
| **< Latest**      | **No**    | Older versions are immediately End-of-Life (EOL).           |

## Reporting a Vulnerability

We advocate for **Responsible Disclosure**. If you discover a vulnerability, please report it privately.

### Reporting Process

- **GitHub Security Advisories (Preferred)**: Navigate to the **[Security tab](https://github.com/wtransport/pywebtransport/security)** and click **"Report a vulnerability"** to open an encrypted draft.

- **Email (Alternative)**: Send an encrypted message to `security@wtransport.org` with the subject `[SECURITY] PyWebTransport Vulnerability Report`.

### Report Contents

- **Description**: Technical details of the vulnerability.
- **Impact**: Potential consequences and attack vectors.
- **Reproduction**: Minimal code example or step-by-step guide.
- **Environment**: Versions of Python, PyWebTransport, and OS.

### Response SLA

- **Acknowledgment**: Within 48 hours.
- **Assessment**: Initial severity assessment within 5 business days.
- **Resolution**: Critical vulnerabilities aim to be patched within 30 days.

## Shared Responsibility Model

Security is a shared responsibility between the library maintainers and application developers.

### Library Responsibilities

- **Transport Security**: Enforcing TLS 1.3 encryption and certificate validation by default.
- **Protocol Compliance**: Mitigating protocol-level attacks (e.g., amplification, state exhaustion).
- **Dependency Management**: Monitoring upstream dependencies for security advisories.

### User Responsibilities

- **PKI Management**: Provisioning valid certificates from a trusted CA.
- **Authentication**: Implementing application-layer authentication logic.
- **Resource Governance**: Configuring connection, stream, and datagram limits to prevent DoS.
- **Input Sanitization**: Validating all data payloads before processing.

## Supply Chain Security

PyWebTransport enforces a **minimal-dependency philosophy**. We actively monitor runtime dependencies for CVEs, ensuring upstream patches trigger an immediate release.

## Disclosure Policy

Upon validating a vulnerability, we will:

1.  Collaborate with the reporter to verify the fix.
2.  Reserve a CVE identifier if applicable.
3.  Publish a security advisory on GitHub.
4.  Release a patched version to PyPI.
5.  Credit the reporter in the advisory and `CHANGELOG.md` (unless anonymity is requested).

---

**Note**: This project does not currently operate a financial Bug Bounty program.
