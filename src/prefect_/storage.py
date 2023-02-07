"""
Code for storing data with Prefect.
"""
from __future__ import annotations

from pathlib import Path

from prefect_aws import S3Bucket

bucket: S3Bucket = S3Bucket.load("aws-s3-bucket-borderlands")


def create_bucket_with_resolved_subpath(
    key: str, __bucket: S3Bucket | None = None
) -> S3Bucket:
    """Creates a child `S3Bucket` of `bucket` whose `bucket_folder` is within
    `bucket.bucket_folder`.

    :param key:     Child path from `bucket.bucket_folder`
    :param bucket:  `S3Bucket` to create a child from
    :return:        `S3Bucket` whose `bucket_folder` is\
        `bucket.bucket_folder`/`key`

    ## Example

    ```py
    bucket: S3Bucket = ...
    # bucket.bucket_folder == "base_folder/"
    child_bucket = create_bucket_with_resolved_subpath("child_folder", bucket)
    # child_bucket.bucket_folder == "base_folder/child_folder/"
    ```
    """
    if __bucket is None:
        __bucket = bucket
    new_bucket_folder: Path = __bucket._resolve_path(key)
    return __bucket.copy(update=dict(bucket_folder=new_bucket_folder))


def task_persistence_subfolder(bucket: S3Bucket, key: str | None = None):
    """Decorator to set the `task.result_storage` attribute to a subfolder of
    `bucket`. `key` will be relative to the existing `bucket.bucket_folder`.
    If no key is given, then `task.fn.__name__` will be used as the subfolder.

    NOTE This method does not alter the `result_serializer` or
    `persist_result` attributes.

    :param bucket:  `S3Bucket` to create a child folder for
    :param key:     Folder to resolve `bucket_folder` to
    :return:        Task whose `result_storage` outputs to a subfolder

    ## Examples

    ```py
    bucket: S3Bucket = ...
    # bucket.bucket_folder == "base_folder/"

    @task_persistence_subfolder(bucket)
    @task(result_serializer=PickleSerializer, persist_result=True)
    def persisted_task(data: dict) -> dict:
        '''A task whose return value is persisted in a subfolder of `bucket`.
        '''
        ...
    # Results will be persisted to "base_folder/persisted_task/"

    @task_persistence_subfolder(bucket, key="AlternativePersistedTask")
    @task(result_serializer=PickleSerializer, persist_result=True)
    def second_persisted_task(data: dict) -> dict:
        '''A task whose return value is persisted in a subfolder of `bucket`.
        '''
        ...
    # Results will be persisted to "base_folder/persisted_task/"
    ```
    """

    def decorator(task):
        """Decorates the task to assign a new `task.result_storage`."""

        task.result_storage = create_bucket_with_resolved_subpath(
            key or task.fn.__name__, __bucket=bucket
        )
        return task

    return decorator
