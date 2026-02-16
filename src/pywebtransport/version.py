"""Define the library version."""

from __future__ import annotations

import importlib.metadata

__all__: list[str] = ["__version__"]

__version__: str = importlib.metadata.version("pywebtransport")
