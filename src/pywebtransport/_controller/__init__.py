"""Threaded reactor concurrency controller and FFI runtime bindings."""

from pywebtransport._controller.abi import ABI_VERSION as _PY_ABI_VERSION
from pywebtransport._pywebtransport import ABI_VERSION as _NATIVE_ABI_VERSION

if _PY_ABI_VERSION != _NATIVE_ABI_VERSION:
    raise RuntimeError(f"abi_version validate invalid actual={_NATIVE_ABI_VERSION} expected=py_abi_version")

__all__: list[str] = []
