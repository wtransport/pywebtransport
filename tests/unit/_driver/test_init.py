"""Unit tests for the pywebtransport._driver.__init__ module."""

import importlib
import sys
from unittest.mock import patch

import pytest


class TestDriverInit:

    def test_abi_version_match(self) -> None:
        with patch("pywebtransport._driver.abi.ABI_VERSION", 1), patch("pywebtransport._wtransport.ABI_VERSION", 1):
            if "pywebtransport._driver" in sys.modules:
                driver_module = importlib.reload(sys.modules["pywebtransport._driver"])
            else:
                driver_module = importlib.import_module("pywebtransport._driver")

            assert driver_module.__all__ == []

    def test_abi_version_mismatch_raises_error(self) -> None:
        with patch("pywebtransport._driver.abi.ABI_VERSION", 1), patch("pywebtransport._wtransport.ABI_VERSION", 2):
            with pytest.raises(RuntimeError, match="ABI version mismatch: expected 1, got 2."):
                if "pywebtransport._driver" in sys.modules:
                    importlib.reload(sys.modules["pywebtransport._driver"])
                else:
                    importlib.import_module("pywebtransport._driver")

        if "pywebtransport._driver" in sys.modules:
            del sys.modules["pywebtransport._driver"]
