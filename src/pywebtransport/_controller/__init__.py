"""Endpoint control."""

from pywebtransport._controller.abi import ABI_VERSION as PYTHON_ABI_VERSION
from pywebtransport._pywebtransport import ABI_VERSION as NATIVE_ABI_VERSION

if PYTHON_ABI_VERSION != NATIVE_ABI_VERSION:
    raise RuntimeError(f"abi_version validate invalid actual={NATIVE_ABI_VERSION} expected=python_abi_version")

__all__: list[str] = []
