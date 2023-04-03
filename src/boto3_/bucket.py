"""
Module for simplified S3 Bucket operations.
"""
from __future__ import annotations

import dataclasses as dc
from pathlib import Path
from typing import Any, Callable, Generator, TypeVar

# Installing boto3 (AWS's Python package)
#  - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
import boto3
from marshmallow import Schema, fields, post_load

from .object_summary import ObjectSummary

Boto3ObjectSummary = TypeVar("Boto3ObjectSummary")


@dc.dataclass(repr=True)
class S3Bucket:
    """S3 Bucket to perform operations with.

    :param name:            Name of the S3 Bucket
    :param bucket_folder:   Folder of the bucket to work within
    :param profile:         Local AWS profile to use

    1. Download & Install the AWS CLI
        - https://aws.amazon.com/cli/
    2. Create a "Named Profile"
        - https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html
        - Should have permission to access the 'name' S3 bucket
    3. Set 'profile' to the profile name you created
    """

    name: str
    bucket_folder: Path | None = None
    profile: str | None = None

    def __post_init__(self):
        """Creates more attributes using the passed."""
        self.session: boto3.Session = boto3.Session(profile_name=self.profile)
        self.s3 = self.session.resource("s3")
        self.bucket = self.s3.Bucket(self.name)

        if self.bucket_folder:
            self.bucket_folder = Path(self.bucket_folder)

    def _resolve_path(self, key: Path | str | None = None) -> str:
        """Resolves the path of `key` in `Bucket` given the `Bucket.bucket_folder`."""
        if self.bucket_folder:
            if key:
                return (self.bucket_folder / key).as_posix()
            else:
                return self.bucket_folder.as_posix()

        if key:
            return Path(key).as_posix()

        return ""

    def all(
        self,
        prefix: Path | str | None = None,
        caster: Callable[[Boto3ObjectSummary], Any] | None = None,
    ) -> Generator[Boto3ObjectSummary | ObjectSummary | Any, None, None]:
        """Yields all objects in the bucket with the prefix."""
        prefix = self._resolve_path(prefix)
        if not caster:
            caster = ObjectSummary
        yield from map(caster, self.bucket.objects.iterator(Prefix=prefix))

    def files(
        self,
        prefix: Path | str | None = None,
        caster: Callable[[Boto3ObjectSummary], Any] | None = None,
    ) -> Generator[Boto3ObjectSummary | ObjectSummary | Any, None, None]:
        """Yields files in the bucket with the prefix."""
        yield from filter(
            lambda o: o.key.endswith("/") is False,
            self.all(prefix, caster=caster),
        )

    def folders(
        self,
        prefix: Path | str | None = None,
        caster: Callable[[Boto3ObjectSummary], Any] | None = None,
    ) -> Generator[Boto3ObjectSummary | ObjectSummary | Any, None, None]:
        """Yields folders in the bucket with the prefix."""
        yield from filter(
            lambda o: o.key.endswith("/"), self.all(prefix, caster=caster)
        )


class S3BucketSchema(Schema):
    """Schema for `S3Bucket`."""

    name = fields.Str(required=True)
    bucket_folder = fields.Str(required=False)
    profile = fields.Str(required=False)

    @post_load
    def make_bucket(self, data, **kwds) -> S3Bucket:
        """Constructs an `S3Bucket` from a dictionary configuration."""
        return S3Bucket(**data)
