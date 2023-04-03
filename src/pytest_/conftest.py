"""
Pytest configuration.
"""
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from prefect.filesystems import LocalFileSystem
from prefect.testing.utilities import prefect_test_harness

from ..prefect_ import storage as blocks

TESTS_PATH: Path = Path(__file__).parent


@pytest.fixture
def tests_path() -> Path:
    """Returns the path to the tests/ directory."""
    return TESTS_PATH


@pytest.fixture
def mock_bucket_path(tests_path: Path) -> Path:
    """Returns the path to the tests/output/ directory."""
    # NOTE you may want to include tests/output/ in your .gitignore
    path = tests_path / "data/results"
    path.mkdir(exist_ok=True, parents=True)
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


@pytest.fixture(autouse=True, scope="function")
def with_mock_bucket(monkeypatch: MonkeyPatch, mock_bucket_path: Path):
    """Creates a bucket that outputs to the 'tests/' directory's 'output/'."""
    # Update bucket so it dumps to local output folder
    fs: LocalFileSystem = LocalFileSystem(
        _block_document_name="test-bucket",
        _is_anonymous=True,
        basepath=str(mock_bucket_path.absolute()),
    )
    fs._block_document_id = fs._save(
        "test-bucket",
        is_anonymous=True,
        overwrite=True,
    )

    # UPDATE ME: assumes the root bucket is named 'bucket'
    monkeypatch.setattr(blocks, "bucket", fs)

    fs_persist: LocalFileSystem = blocks.create_child_bucket(
        key="persistence",
        suffix="-persistence",
        parent=fs,
    )
    monkeypatch.setattr(blocks, "persistence", fs_persist)

    # UPDATE ME: does not update bucket instances with these names
    IGNORE = ("bucket", "persistence", "noaa_source_bucket")
    # Updates all other bucket instances
    for attr, value in blocks.__dict__.items():
        if attr in IGNORE or not isinstance(value, blocks.S3Bucket):
            continue
        tempfs = blocks.create_bucket_with_resolved_subpath(
            value.bucket_folder, fs
        )
        monkeypatch.setattr(
            blocks,
            attr,
            tempfs,
        )
