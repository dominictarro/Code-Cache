from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from prefect.filesystems import LocalFileSystem
from prefect.utilities.testing import prefect_test_harness

TESTS_PATH: Path = Path(__file__).parent


@pytest.fixture
def tests_path() -> Path:
    """Returns the path to the tests/ directory."""
    return TESTS_PATH


@pytest.fixture
def test_output_path(tests_path: Path) -> Path:
    """Returns the path to the tests/output/ directory."""
    # NOTE you may want to include tests/output/ in your .gitignore
    path = tests_path / "output"
    path.mkdir(exist_ok=True)
    return path


# Sets the test harness so prefect flow tests are not recorded in the Orion
# database.
#
# I use this at the session-level so that all flows are run within the same
# environment.
@pytest.fixture(autouse=True, scope="session")
def with_test_harness():
    """Sets the Prefect test harness for local pipeline testing."""
    with prefect_test_harness():
        yield


# NOTE, assumes "from package import blocks", where blocks is a module
# that contains a `bucket` variable. The `bucket` is a `prefect_aws.S3Bucket`
# that all other buckets inherit from.
#
# You can add more buckets, change the module, or change the bucket variable
# name.
#
# DELETE ME: PLACEHOLDER FOR FLAKE8
blocks = __package__
# NOTE, assumes you included the `create_bucket_with_resolved_subpath`
# function in your blocks module. You can remove that if you have not defined
# it or change the module if is not in `blocks`


@pytest.fixture(autouse=True, scope="function")
def with_mock_bucket(monkeypatch: MonkeyPatch, test_output_path: Path):
    """Creates a bucket that outputs to the 'tests/' directory's 'output/'."""
    # Update bucket so it dumps to local output folder
    fs = LocalFileSystem(basepath=str(test_output_path / "output"))

    # UPDATE ME: if not in blocks or named "bucket"
    monkeypatch.setattr(blocks, "bucket", fs)
    blocks.bucket.update_forward_refs()

    # While prefect_aws is moving to the `bucket_folder` attribute,
    # LocalFileSystem still uses `basepath`. Augment this "bucket"
    # building function to reflect that
    def create_bucket_with_resolved_subpath(
        key: str, __bucket: LocalFileSystem | None = None
    ) -> LocalFileSystem:
        """Creates a child `S3Bucket` of `bucket` whose `bucket_folder` is within
        `bucket.bucket_folder`.

        :param key:     Child path from `bucket.bucket_folder`
        :param bucket:  `S3Bucket` to create a child from
        :return:        `S3Bucket` whose `bucket_folder` is\
            `bucket.bucket_folder`/`key`
        """
        if __bucket is None:
            __bucket = fs
        new_bucket_folder: Path = __bucket._resolve_path(key)
        return __bucket.copy(update=dict(basepath=new_bucket_folder))

    # UPDATE ME (if not in blocks)
    monkeypatch.setattr(
        blocks,
        "create_bucket_with_resolved_subpath",
        create_bucket_with_resolved_subpath,
    )
