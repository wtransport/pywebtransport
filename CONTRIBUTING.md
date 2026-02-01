# Contributing to PyWebTransport

Thank you for your interest in contributing to PyWebTransport. This document defines the technical standards for contributors.

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Security Vulnerabilities

Please **do not** report security vulnerabilities via public GitHub issues. For reporting instructions and disclosure policies, consult the **[Security Policy](SECURITY.md)**.

## Developer Certificate of Origin (DCO)

To ensure legal compliance, we enforce the **[Developer Certificate of Origin (DCO) 1.1](https://developercertificate.org/)**. By contributing, you certify that you have the right to submit the patch. You **must** sign off every commit by adding a `Signed-off-by` line.

## Implementation Philosophy

Before contributing, review our **[Implementation Philosophy](PHILOSOPHY.md)**. All contributions must align with the architectural principles defined therein.

## Development Environment

### Prerequisites

- **Clang**: Required for C-bindings generation.
- **Git**: Version control.
- **pyenv**: Python version management.
- **rustup**: Rust toolchain management.
- **tox**: Automated testing suite.

### Setup

1.  **Fork and Clone**:

    ```bash
    git clone https://github.com/<your-username>/pywebtransport.git
    cd pywebtransport
    git remote add upstream https://github.com/wtransport/pywebtransport.git
    ```

2.  **Environment**:

    ```bash
    pyenv install
    python -m venv .venv
    source .venv/bin/activate
    ```

3.  **Dependencies**:
    ```bash
    pip install -r dev-requirements.txt
    pip install -e .
    ```

## Coding Standards

All contributions must adhere to the following style requirements:

- **formatting**: `black`, `cargo fmt`
- **imports**: `isort`
- **linting**: `flake8`, `clippy`
- **typing**: `mypy`
- **documentation**: Google-style docstrings and mandatory type hints for public APIs.

## Testing

Execute `pytest` to validate full-stack behavior including the underlying Rust engine, and `cargo test` for internal state machine correctness. Full matrix validation via `tox` is mandatory before submission.

**Requirement**: New features must include positive, negative, and edge-case tests.

## Commit & PR Process

1.  **Commit**: Sign-off is mandatory (`git commit -s`). Follow **Conventional Commits**.
2.  **Changelog**: Add a user-facing entry to `CHANGELOG.md` under `Unreleased`.
3.  **Submission**: Open a PR against `main` with technical rationale.

## Release Process

_(Maintainers Only)_

Releases follow **Semantic Versioning** and are automated via **Trusted Publishers (OIDC)**. Update the version source of truth and finalize `CHANGELOG.md` before merging to `main`.

## Protocol Compliance

Contributions must adhere to the IETF WebTransport specifications:

- **[WebTransport over HTTP/3](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/)**
- **[RFC 9000 (QUIC)](https://www.rfc-editor.org/rfc/rfc9000.txt)**
- **[RFC 9114 (HTTP/3)](https://www.rfc-editor.org/rfc/rfc9114.txt)**
- **[RFC 9297 (HTTP Datagrams)](https://www.rfc-editor.org/rfc/rfc9297.txt)**
