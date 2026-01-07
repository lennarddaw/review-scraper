"""Base storage interface for review data."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator

from src.models.review import Review


class BaseStorage(ABC):
    """
    Abstract base class for review storage backends.
    
    Defines the interface that all storage implementations must follow.
    """

    @abstractmethod
    async def save(self, reviews: list[Review]) -> int:
        """
        Save reviews to storage.

        Args:
            reviews: List of Review objects to save

        Returns:
            Number of reviews saved
        """
        pass

    @abstractmethod
    async def save_one(self, review: Review) -> bool:
        """
        Save a single review.

        Args:
            review: Review to save

        Returns:
            True if saved successfully
        """
        pass

    @abstractmethod
    async def load(self) -> list[Review]:
        """
        Load all reviews from storage.

        Returns:
            List of Review objects
        """
        pass

    @abstractmethod
    async def count(self) -> int:
        """
        Get total number of stored reviews.

        Returns:
            Review count
        """
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all stored reviews."""
        pass

    @abstractmethod
    async def exists(self, review_id: int) -> bool:
        """
        Check if a review exists.

        Args:
            review_id: Review ID to check

        Returns:
            True if exists
        """
        pass


class FileStorage(BaseStorage):
    """Base class for file-based storage backends."""

    def __init__(self, filepath: str | Path):
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    def _ensure_file_exists(self) -> None:
        """Create the file if it doesn't exist."""
        if not self.filepath.exists():
            self.filepath.touch()