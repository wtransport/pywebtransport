"""Defines the semantic version number for the library."""

from __future__ import annotations

import importlib.metadata

__all__: list[str] = ["__version__"]

__version__ = importlib.metadata.version("pywebtransport")
