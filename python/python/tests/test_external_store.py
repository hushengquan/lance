# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

from typing import Dict, Optional, Tuple

import pytest
from lance.commit import CommitConflictError, ExternalManifestStore


class InMemoryExternalStore(ExternalManifestStore):
    """
    In-memory implementation of ExternalManifestStore for testing.

    This provides a simple thread-safe implementation using a dictionary.
    """

    def __init__(self):
        self.store: Dict[str, Dict[int, dict]] = {}

    def get(self, base_uri: str, version: int) -> str:
        if base_uri not in self.store:
            raise KeyError(f"Dataset {base_uri} not found")
        if version not in self.store[base_uri]:
            raise KeyError(f"Version {version} not found for {base_uri}")
        return self.store[base_uri][version]["path"]

    def get_latest_version(self, base_uri: str) -> Optional[Tuple[int, str]]:
        if base_uri not in self.store or not self.store[base_uri]:
            return None

        latest_version = max(self.store[base_uri].keys())
        path = self.store[base_uri][latest_version]["path"]
        return (latest_version, path)

    def put_if_not_exists(
        self, base_uri: str, version: int, path: str, size: int, e_tag: Optional[str]
    ) -> None:
        if base_uri not in self.store:
            self.store[base_uri] = {}

        if version in self.store[base_uri]:
            raise CommitConflictError(
                f"Version {version} already exists for {base_uri}"
            )

        self.store[base_uri][version] = {
            "path": path,
            "size": size,
            "e_tag": e_tag,
        }

    def put_if_exists(
        self, base_uri: str, version: int, path: str, size: int, e_tag: Optional[str]
    ) -> None:
        if base_uri not in self.store:
            raise KeyError(f"Dataset {base_uri} not found")
        if version not in self.store[base_uri]:
            raise KeyError(f"Version {version} does not exist for {base_uri}")

        self.store[base_uri][version] = {
            "path": path,
            "size": size,
            "e_tag": e_tag,
        }

    def delete(self, base_uri: str) -> None:
        if base_uri in self.store:
            del self.store[base_uri]


class TestExternalManifestStore:
    """Test suite for ExternalManifestStore."""

    def test_basic_operations(self):
        """Test basic get/put operations."""
        store = InMemoryExternalStore()

        # Initially empty
        assert store.get_latest_version("s3://bucket/dataset") is None

        # Put first version
        store.put_if_not_exists(
            "s3://bucket/dataset",
            1,
            "s3://bucket/dataset/_versions/1.manifest",
            1024,
            "etag123",
        )

        # Get it back
        path = store.get("s3://bucket/dataset", 1)
        assert path == "s3://bucket/dataset/_versions/1.manifest"

        # Get latest
        latest = store.get_latest_version("s3://bucket/dataset")
        assert latest == (1, "s3://bucket/dataset/_versions/1.manifest")

    def test_put_if_not_exists_conflict(self):
        """Test that put_if_not_exists raises CommitConflictError on duplicate."""
        store = InMemoryExternalStore()

        # Put first version
        store.put_if_not_exists(
            "s3://bucket/dataset",
            1,
            "s3://bucket/dataset/_versions/1.manifest",
            1024,
            None,
        )

        # Try to put same version again - should fail
        with pytest.raises(CommitConflictError):
            store.put_if_not_exists(
                "s3://bucket/dataset",
                1,
                "s3://bucket/dataset/_versions/1-retry.manifest",
                1024,
                None,
            )

    def test_put_if_exists(self):
        """Test put_if_exists for updating existing versions."""
        store = InMemoryExternalStore()

        # Put initial version
        store.put_if_not_exists(
            "s3://bucket/dataset",
            1,
            "s3://bucket/dataset/_versions/1-staging.manifest",
            1024,
            None,
        )

        # Update to final path
        store.put_if_exists(
            "s3://bucket/dataset",
            1,
            "s3://bucket/dataset/_versions/1.manifest",
            1024,
            "etag456",
        )

        # Verify update
        path = store.get("s3://bucket/dataset", 1)
        assert path == "s3://bucket/dataset/_versions/1.manifest"

    def test_put_if_exists_not_found(self):
        """Test that put_if_exists raises KeyError if version doesn't exist."""
        store = InMemoryExternalStore()

        with pytest.raises(KeyError):
            store.put_if_exists(
                "s3://bucket/dataset",
                1,
                "s3://bucket/dataset/_versions/1.manifest",
                1024,
                None,
            )

    def test_multiple_versions(self):
        """Test handling multiple versions."""
        store = InMemoryExternalStore()

        # Add multiple versions
        for version in range(1, 6):
            store.put_if_not_exists(
                "s3://bucket/dataset",
                version,
                f"s3://bucket/dataset/_versions/{version}.manifest",
                1024 * version,
                f"etag{version}",
            )

        # Get latest should return version 5
        latest = store.get_latest_version("s3://bucket/dataset")
        assert latest == (5, "s3://bucket/dataset/_versions/5.manifest")

        # Get specific versions
        for version in range(1, 6):
            path = store.get("s3://bucket/dataset", version)
            assert path == f"s3://bucket/dataset/_versions/{version}.manifest"

    def test_delete(self):
        """Test deleting a dataset."""
        store = InMemoryExternalStore()

        # Add some versions
        store.put_if_not_exists(
            "s3://bucket/dataset",
            1,
            "s3://bucket/dataset/_versions/1.manifest",
            1024,
            None,
        )
        store.put_if_not_exists(
            "s3://bucket/dataset",
            2,
            "s3://bucket/dataset/_versions/2.manifest",
            2048,
            None,
        )

        # Delete dataset
        store.delete("s3://bucket/dataset")

        # Should be gone
        assert store.get_latest_version("s3://bucket/dataset") is None

        with pytest.raises(KeyError):
            store.get("s3://bucket/dataset", 1)

    def test_multiple_datasets(self):
        """Test handling multiple independent datasets."""
        store = InMemoryExternalStore()

        # Add versions to two different datasets
        store.put_if_not_exists(
            "s3://bucket/dataset1",
            1,
            "s3://bucket/dataset1/_versions/1.manifest",
            1024,
            None,
        )
        store.put_if_not_exists(
            "s3://bucket/dataset2",
            1,
            "s3://bucket/dataset2/_versions/1.manifest",
            2048,
            None,
        )

        # They should be independent
        latest1 = store.get_latest_version("s3://bucket/dataset1")
        latest2 = store.get_latest_version("s3://bucket/dataset2")

        assert latest1 == (1, "s3://bucket/dataset1/_versions/1.manifest")
        assert latest2 == (1, "s3://bucket/dataset2/_versions/1.manifest")

        # Delete one shouldn't affect the other
        store.delete("s3://bucket/dataset1")
        assert store.get_latest_version("s3://bucket/dataset1") is None
        assert store.get_latest_version("s3://bucket/dataset2") is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
