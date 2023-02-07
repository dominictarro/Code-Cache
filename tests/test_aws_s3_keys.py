"""
Tests for the src.aws_.s3.keys module.

"""
from __future__ import annotations

from pathlib import Path

# Pytests
import pytest

from src.aws_.s3 import keys


@pytest.mark.parametrize(
    argnames=["key", "allow_separator", "expected"],
    argvalues=[
        ("key/year-2000/simple", True, True),
        ("key/year-2000/simple", False, False),
        ("key/year-2000/simple?", True, False),
        ("key/year-2000/simple?", False, False),
        ("~/key/year=2000/simple?", True, False),
        ("~/key/year=2000/simple?", False, False),
    ],
)
def test_safe_validator(
    key: Path | str, allow_separator: bool, expected: bool
):
    """Tests the lenient validator against the key, asserting the result
    equals `expected`.
    """
    validator = keys.S3KeyValidator(
        keys.S3KeyValidator.Level.SAFE, allow_separator=allow_separator
    )
    assert validator.is_valid(key) == expected


@pytest.mark.parametrize(
    argnames=["key", "allow_separator", "expected"],
    argvalues=[
        ("key/year-2000/simple", True, True),
        ("key/year-2000/simple", False, True),
        ("key/year-2000/simple?", True, True),
        ("key/year-2000/simple?", False, True),
        ("~/key/year-2000/simple?^", True, False),
        ("~/key/year-2000/simple?^", False, False),
    ],
)
def test_lenient_validator(
    key: Path | str, allow_separator: bool, expected: bool
):
    """Tests the lenient validator against the key, asserting the result
    equals `expected`.
    """
    validator = keys.S3KeyValidator(
        keys.S3KeyValidator.Level.LENIENT, allow_separator=allow_separator
    )
    assert validator.is_valid(key) == expected
