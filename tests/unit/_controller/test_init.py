"""Unit tests for the pywebtransport._controller.__init__ module."""

import importlib
import sys
from unittest.mock import patch

import pytest


class TestControllerInit:

    def test_abi_version_match(self) -> None:
        with (
            patch(target="pywebtransport._controller.abi.ABI_VERSION", new=2),
            patch(target="pywebtransport._pywebtransport.ABI_VERSION", new=2),
        ):
            if "pywebtransport._controller" in sys.modules:
                controller_module = importlib.reload(sys.modules["pywebtransport._controller"])
            else:
                controller_module = importlib.import_module(name="pywebtransport._controller")

            assert controller_module.__all__ == []

    def test_abi_version_mismatch_raises_error(self) -> None:
        with (
            patch(target="pywebtransport._controller.abi.ABI_VERSION", new=2),
            patch(target="pywebtransport._pywebtransport.ABI_VERSION", new=1),
        ):
            with pytest.raises(
                expected_exception=RuntimeError, match="abi_version validate invalid actual=1 expected=py_abi_version"
            ):
                if "pywebtransport._controller" in sys.modules:
                    importlib.reload(sys.modules["pywebtransport._controller"])
                else:
                    importlib.import_module(name="pywebtransport._controller")

        if "pywebtransport._controller" in sys.modules:
            del sys.modules["pywebtransport._controller"]
