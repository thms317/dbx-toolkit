"""Module for default tests."""

from dbx_toolkit.main import main


def test_default() -> None:
    """Default test: will always pass."""
    assert True


def test_main() -> None:
    """Test the main function."""
    assert main() is True
