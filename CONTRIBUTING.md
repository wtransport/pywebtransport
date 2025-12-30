# Contributing to PyWebTransport

Thank you for your interest in contributing to PyWebTransport. This document provides technical guidelines and standards for contributors.

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Security Vulnerabilities

If you discover a security vulnerability, please **do not open a public issue**.

- **Preferred Method:** Use **GitHub Security Advisories** (Private Reporting) on this repository. This allows for secure, encrypted collaboration.
- **Alternative Method:** Email the Core Team at **admin@wtransport.org**.

All reports are handled with strict confidentiality and priority.

## Developer Certificate of Origin (DCO)

To ensure long-term legal compliance and clear copyright ownership, we enforce the **[Developer Certificate of Origin (DCO) 1.1](https://developercertificate.org/)**.

By contributing, you certify that you wrote the patch or have the right to contribute it as open-source software. You **must** sign off every commit by adding a `Signed-off-by` line. This requirement is enforced by our repository checks.

## Implementation Philosophy

Before contributing, please review our **[Implementation Philosophy](PHILOSOPHY.md)**. It outlines the core architectural principles and design goals that guide this project. All contributions must align with these standards.

## Getting Started

### Prerequisites

Ensure your environment includes:

- **Git**: Version control.
- **pyenv**: Python version management (required for local matrix testing).
- **tox**: Automated testing across Python versions.

We recommend installation via `pipx` for isolation.

### Development Setup

1.  **Fork and Clone**:
    Fork the repository on GitHub, then clone your fork locally and configure the upstream remote.

    ```bash
    # Clone your fork (replace <your-username> with your actual username)
    git clone https://github.com/<your-username>/pywebtransport.git
    cd pywebtransport

    # Add the official repository as 'upstream'
    git remote add upstream https://github.com/wtransport/pywebtransport.git
    ```

2.  **Python Version**:
    Install the recommended Python version using `pyenv` (which reads the `.python-version` file).

    ```bash
    pyenv install
    ```

3.  **Virtual Environment**:
    Create an isolated environment.

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

4.  **Dependencies**:
    Install exact development dependencies (locked via `pip-tools`) and the project in editable mode.

    ```bash
    pip install -r dev-requirements.txt
    pip install -e .
    ```

## Development Workflow

1.  **Branching**:
    Create a feature branch from `main`.

    ```bash
    git checkout -b feature/your-feature-name
    ```

2.  **Development**:
    Implement changes following the **Coding Standards** below.

3.  **Local Verification**:
    Run static analysis and unit tests (including quality checks and pytest).

    ```bash
    # Quality checks
    black src tests
    flake8 src tests
    mypy src tests

    # Unit tests
    pytest
    ```

4.  **Full Suite Verification**:
    **Mandatory:** Run the full test matrix before pushing to ensure compatibility across Python versions.

    ```bash
    tox
    ```

5.  **Commit (Signed)**:
    We enforce **Conventional Commits** and **DCO**. You must use the sign-off flag.

    ```bash
    git add .
    git commit -s -m "feat: implement bidirectional stream flow control"
    ```

## Coding Standards

Configurations are defined in `pyproject.toml` and `.flake8`.

- **Formatting**: Black
- **Linting**: flake8
- **Typing**: mypy (Strict mode)
- **Imports**: isort

### Type Hints

- **Mandatory:** Public APIs must have type hints.
- **Syntax:** Use Python 3.12+ syntax (e.g., generics, union types). Do not use legacy typing imports.

### Documentation

- **Mandatory:** Public APIs must have docstrings.
- **Style:** Concise summary line followed by detailed description if necessary.

```python
async def create_bidirectional_stream(self) -> WebTransportStream:
    """Create a new bidirectional WebTransport stream."""
```

## Testing

Tests are structurally mirrored in the `tests/` directory:

- **unit**: Isolated logic tests.
- **integration**: Component interaction.
- **e2e**: Full protocol compliance.
- **benchmark**: Performance regressions.

**Requirement:** New features must include positive, negative, and edge-case tests.

## Pull Request Process

1.  **Pre-check**: Ensure the full test automation passes and commits are signed.
2.  **Changelog**: Add a user-facing entry to `CHANGELOG.md` under the `Unreleased` section.
3.  **Submission**: Open a PR against the `main` branch of the official repository.
    - **Title**: Follow Conventional Commits.
    - **Description**: Technical rationale and implementation details.

## Release Process

_(Maintainers Only)_

We follow **Semantic Versioning**. Releases are automated via **Trusted Publishers (OIDC)**.

1.  Bump version in the source version file.
2.  Finalize `CHANGELOG.md`.
3.  Merge release PR to `main`.
4.  CI/CD pipeline triggers PyPI publication.

## Issue Reporting

- **Bugs**: Include Python version, OS, traceback, and a minimal reproduction script.
- **Features**: Reference relevant IETF specifications (RFC/Draft) and provide API design proposals.

## Protocol Compliance

Contributions must adhere to the following specifications:

- [WebTransport over HTTP/3 (draft-ietf-webtrans-http3-14)](https://www.ietf.org/archive/id/draft-ietf-webtrans-http3-14.txt)
- [RFC 9000 (QUIC)](https://www.rfc-editor.org/rfc/rfc9000.txt)
- [RFC 9114 (HTTP/3)](https://www.rfc-editor.org/rfc/rfc9114.txt)
- [RFC 9297 (HTTP Datagrams)](https://www.rfc-editor.org/rfc/rfc9297.txt)

---

Thank you for contributing to PyWebTransport! Your efforts help advance the WebTransport ecosystem in Python.

**The WTransport Authors**
