"""
A wrapper for working with `boto3.s3.ObjectSummary`.

More can be found at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#objectsummary
"""
from __future__ import annotations

import datetime


class ObjectSummary:
    """Wrapper for `boto3.s3.ObjectSummary`."""

    def __init__(self, obj) -> None:
        """Wraps a `boto.s3.ObjectSummary`."""
        self.obj = obj

    def __repr__(self) -> str:
        return f"@{self.obj!r}"

    @property
    def bucket_name(self) -> str:
        """Object's bucket name."""
        return self.obj.bucket_name

    @property
    def checksum_algorithm(self) -> str:
        """Object's checksum algorithm."""
        return self.obj.checksum_algorithm

    @property
    def key(self) -> str:
        """Object's key."""
        return self.obj.key

    @property
    def e_tag(self) -> str:
        """Object's ETag."""
        return self.obj.e_tag

    @property
    def last_modified(self) -> datetime.datetime:
        """Object's last modified datetime."""
        return self.obj.last_modified

    @property
    def size(self) -> int:
        """Object's size in bytes."""
        return self.obj.size

    @property
    def storage_class(self) -> int:
        """Object's storage class."""
        return self.obj.storage_class

    def get(self, **kwds) -> bytes:
        """Gets the object from S3.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
        """
        return self.obj.get(**kwds).get("Body").read()

    async def get_async(self, **kwds) -> bytes:
        """Gets the object from S3.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
        """
        return self.get(**kwds)
