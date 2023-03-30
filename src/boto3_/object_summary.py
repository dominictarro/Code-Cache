"""
A wrapper for working with `boto3.s3.ObjectSummary`.

More can be found at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#objectsummary
"""
from __future__ import annotations

import datetime


class ObjectSummary:
    """Wrapper for `boto3.s3.ObjectSummary`."""

    def __init__(self, obj) -> None:
        self.obj = obj

    def __repr__(self) -> str:
        return f"@{self.obj!r}"

    @property
    def bucket_name(self) -> str:
        return self.obj.bucket_name

    @property
    def checksum_algorithm(self) -> str:
        return self.obj.checksum_algorithm

    @property
    def key(self) -> str:
        return self.obj.key

    @property
    def e_tag(self) -> str:
        return self.obj.e_tag

    @property
    def last_modified(self) -> datetime.datetime:
        return self.obj.last_modified

    @property
    def size(self) -> int:
        return self.obj.size

    @property
    def storage_class(self) -> int:
        return self.obj.storage_class

    async def get(self, **kwds) -> bytes:
        """Gets the object from S3.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
        """
        return self.obj.get(**kwds).get("Body").read()
