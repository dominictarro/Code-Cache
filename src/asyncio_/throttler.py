"""
A class for limiting concurrency.
"""
from __future__ import annotations

import functools
import threading
from typing import Dict, overload


class Throttler:
    """Limits function executions.
    
    Functions can be throttled as a group, causing them to share a
    `Semaphore`.
    """
    _instances: Dict[str, Throttler] = {}

    def __init__(self, concurrency_limit: int, group: str | None = None) -> None:
        """Initializes a `Throttler` and creates a `group` if one is defined. Errors if
        the initialization tries to override an existing group.
        """

        self.__concurrency_limit: int = concurrency_limit
        self.__semaphore: threading.Semaphore = threading.Semaphore(self.__concurrency_limit)
        self.__group: str | None = group
        if group is not None and group in type(self)._instances:
            raise ValueError(
                f"{type(self).__name__} already exists with name '{group}'"
            )
        type(self)._instances[group] = self

    @property
    def concurrency_limit(self) -> int:
        """Number of concurrent operations the throttler permits."""
        return self.__concurrency_limit

    @property
    def group(self) -> str:
        """Group the instance belongs to."""
        return self.__group

    @property
    def locked(self) -> bool:
        """Returns True if throttler cannot be acquired immediately."""
        return self.__semaphore.locked()

    @classmethod
    def get_group(cls, group: str) -> Throttler:
        """Gets the `Throttler` for the given group. Errors if one does not
        exist.
        """
        return Throttler._instances[group]

    def __call__(self, func):
        """Applies a wrapper that throttles the function's concurrent calls to
        the defined `concurrency_limit`.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwds):
            """Calls the function with the throttler's `Semaphore`."""
            with self.__semaphore:
            # with self.lock():
                return func(*args, **kwds)

        return wrapper


@overload
def throttler(concurrency_limit: int) -> Throttler:
    ...


@overload
def throttler(concurrency_limit: int, group: str) -> Throttler:
    ...


@overload
def throttler(group: str) -> Throttler:
    ...


def throttler(*, concurrency_limit = None, group = None):
    """A `Throttler` to limit function calls.

    :param concurrency_limit:   Maximum number of concurrent executions allowed
    :param group:               Name of the `Throttler` to assign or return
    :return:                    A `Throttler`

    If both are defined, then a new `Throttler` group is created. This will
    error if it attempts to overwrite an existing group.

    If only `concurrency_limit` is defined, then a standalone `Throttler` is
    created that cannot be referenced later.

    If only `group` is defined, then an existing `Throttler` is fetched. Errors
    if one does not exist with the name.
    """
    if concurrency_limit is not None and group is not None:
        obj = Throttler(concurrency_limit, group)
    elif concurrency_limit is not None:
        obj = Throttler(concurrency_limit, None)
    elif group is not None:
        obj = Throttler.get_group(group)
    else:
        raise ValueError(f"'group' or 'concurrency_limit' must be defined")
    return obj
