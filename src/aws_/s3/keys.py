"""
Code for working with AWS S3 keys.
"""
from __future__ import annotations

import enum
import string
from pathlib import Path
from typing import Callable


class S3KeyValidator:
    """Validates an S3 key according to Amazon's\
    [Object Key documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-key.html).
    """

    class Level(enum.Enum):
        """Validation levels from documentation.

        SAFE:       Characters deemed safe by documentation
        LENIENT:    Characters that may require special handling
        """

        SAFE = enum.auto()
        LENIENT = enum.auto()

    def __init__(
        self, level: Level | str = Level.SAFE, *, allow_separator: bool = True
    ) -> None:
        """Creates a validator at the given `level`.

        :param level:           Level or string value of Level, defaults to\
            Level.SAFE
        :param allow_separator: Allows '/' if `level` is level.SAFE

        NOTE `level` strings are case insensitive.
        """
        self.__level: S3KeyValidator.Level
        self.__validator: Callable[[str], bool]
        # Property setting protocol will be applied
        # __level and __validator will be set
        self.level = level

        self.allow_separator: bool = allow_separator

    @property
    def level(self) -> Level:
        return self.__level

    @level.setter
    def level(self, level: Level | str):
        """Sets the validator's level if it is acceptable.

        :param level:       Level to set the validator to
        :raises TypeError:  `level` is not a string or `Level`
        :raises ValueError: `level` does not exist
        """
        if not isinstance(level, self.Level):
            if not isinstance(level, str):
                raise TypeError(
                    "'level' must be a Level or string, not"
                    f"{type(level).__name__}"
                )
            try:
                level = self.Level._member_map_[level.upper()]
            except KeyError:
                raise ValueError(
                    f"Level '{level}' is not supported. Try any in "
                    f"{tuple(self.Level._member_names_)}"
                )
        self.__level = level
        self.__validator = (
            self._validate_safely
            if self.__level == self.Level.SAFE
            else self._validate_lenient
        )

    def _validate_safely(self, key: str) -> bool:
        """Validates the key with the strictest possible conditions.

        :param key:             Key to validate
        :param allow_separator: Allow forward slashes. Useful when key is a\
            full key
        :return:                The key meets safe conditions
        """
        alphabet = string.ascii_letters + string.digits + "!-_.*'()"
        if self.allow_separator:
            alphabet += "/"
        return all(c in alphabet for c in key)

    def _validate_lenient(self, key: str) -> bool:
        """Validates the key with the more lenient rules.

        :param key:             Key to validate
        :param allow_separator: Allow forward slashes. Useful when key is a\
            full key
        :return:                The key meets safe conditions
        """
        alphabet = string.ascii_letters + string.digits + "!-_.*'()&$@=;/:+ ,?"
        # TODO
        # ASCII character ranges 00-1F hex (0-31 decimal) and 7F (127 decimal)
        return all(c in alphabet for c in key)

    def is_valid(self, key: Path | str) -> bool:
        """Checks if the key at `self.level`."""
        key = key.as_posix() if isinstance(key, Path) else key
        return self.__validator(key)
