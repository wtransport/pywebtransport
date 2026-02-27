"""Low-level native protocol driver and FFI bindings."""

from pywebtransport._driver.abi import ABI_VERSION as _PY_ABI_VERSION
from pywebtransport._wtransport import ABI_VERSION as _NATIVE_ABI_VERSION

if _PY_ABI_VERSION != _NATIVE_ABI_VERSION:
    raise RuntimeError(f"ABI version mismatch: expected {_PY_ABI_VERSION}, got {_NATIVE_ABI_VERSION}.")

__all__: list[str] = []
