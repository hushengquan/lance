# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Callable, Optional, Tuple

CommitLock = Callable[[int], AbstractContextManager]


class CommitConflictError(Exception):
    """Raised when a commit conflicts with an existing version."""

    pass


class ExternalManifestStore(ABC):
    """
    Abstract base class for external manifest stores.

    This interface allows business logic to implement custom external stores
    (e.g., DynamoDB, Redis, etcd) that can be used by both Python and Rust APIs
    to ensure atomic commits across both languages.

    The external store is expected to provide put-if-not-exists semantics for
    concurrent commit control.

    Example:
        >>> class MyRedisStore(ExternalManifestStore):
        ...     def __init__(self, redis_client):
        ...         self.redis = redis_client
        ...
        ...     def get(self, base_uri: str, version: int) -> str:
        ...         key = f"{base_uri}:v{version}"
        ...         path = self.redis.get(key)
        ...         if path is None:
        ...             raise KeyError(f"Version {version} not found")
        ...         return path.decode()
        ...
        ...     def get_latest_version(
        ...         self, base_uri: str) -> Optional[Tuple[int, str]]:
        ...         # Implementation details...
        ...         pass
        ...
        ...     def put_if_not_exists(
        ...         self,
        ...         base_uri: str,
        ...         version: int,
        ...         path: str,
        ...         size: int,
        ...         e_tag: Optional[str],
        ...     ) -> None:
        ...         key = f"{base_uri}:v{version}"
        ...         success = self.redis.setnx(key, path)
        ...         if not success:
        ...             raise CommitConflictError(f"Version {version} already exists")
        ...
        ...     def put_if_exists(
        ...         self,
        ...         base_uri: str,
        ...         version: int,
        ...         path: str,
        ...         size: int,
        ...         e_tag: Optional[str],
        ...     ) -> None:
        ...         key = f"{base_uri}:v{version}"
        ...         if not self.redis.exists(key):
        ...             raise KeyError(f"Version {version} does not exist")
        ...         self.redis.set(key, path)
        ...
        ...     def delete(self, base_uri: str) -> None:
        ...         pattern = f"{base_uri}:v*"
        ...         keys = self.redis.keys(pattern)
        ...         if keys:
        ...             self.redis.delete(*keys)
    """

    @abstractmethod
    def get(self, base_uri: str, version: int) -> str:
        """
        Get the manifest path for a specific version.

        Args:
            base_uri: The base URI of the dataset
            version: The version number

        Returns:
            The manifest path for the specified version

        Raises:
            KeyError: If the version does not exist
        """
        pass

    @abstractmethod
    def get_latest_version(self, base_uri: str) -> Optional[Tuple[int, str]]:
        """
        Get the latest version and its manifest path.

        Args:
            base_uri: The base URI of the dataset

        Returns:
            A tuple of (version, manifest_path) if exists, None otherwise
        """
        pass

    @abstractmethod
    def put_if_not_exists(
        self,
        base_uri: str,
        version: int,
        path: str,
        size: int,
        e_tag: Optional[str],
    ) -> None:
        """
        Put manifest info if the version doesn't exist.

        This method must provide atomic put-if-not-exists semantics.

        Args:
            base_uri: The base URI of the dataset
            version: The version number
            path: The manifest path
            size: The manifest file size in bytes
            e_tag: Optional ETag from object store

        Raises:
            CommitConflictError: If the version already exists
        """
        pass

    @abstractmethod
    def put_if_exists(
        self,
        base_uri: str,
        version: int,
        path: str,
        size: int,
        e_tag: Optional[str],
    ) -> None:
        """
        Update manifest info if the version already exists.

        This is used during manifest finalization to update the path
        from staging to final location.

        Args:
            base_uri: The base URI of the dataset
            version: The version number
            path: The new manifest path
            size: The manifest file size in bytes
            e_tag: Optional ETag from object store

        Raises:
            KeyError: If the version does not exist
        """
        pass

    @abstractmethod
    def delete(self, base_uri: str) -> None:
        """
        Delete all manifest information for a dataset.

        Args:
            base_uri: The base URI of the dataset
        """
        pass
