"""Unit tests for the pywebtransport.utils module."""

from pywebtransport.utils import generate_self_signed_cert, init_tracing


def test_generate_self_signed_cert() -> None:

    ca_path, cert_path, key_path = generate_self_signed_cert(hostname="localhost")

    assert isinstance(ca_path, str)
    assert isinstance(cert_path, str)
    assert isinstance(key_path, str)
    assert ca_path.endswith("_ca.crt")
    assert cert_path.endswith(".crt")
    assert key_path.endswith(".key")


def test_init_tracing() -> None:

    init_tracing()
