"""JSON storage for review data - exports to training data format."""

import json
from pathlib import Path
from typing import Any

import aiofiles
from loguru import logger

from src.models.review import Review
from src.storage.base_storage import FileStorage


class JsonStorage(FileStorage):
    """
    JSON file storage for reviews.
    
    Exports reviews in the format: [{"id": 1, "text": "..."}, ...]
    This matches the training data format for AI pipelines.
    """

    def __init__(
        self,
        filepath: str | Path,
        pretty: bool = False,
        include_metadata: bool = False,
    ):
        """
        Initialize JSON storage.

        Args:
            filepath: Path to JSON file
            pretty: Use pretty-printed JSON (larger file size)
            include_metadata: Include full metadata, not just id/text
        """
        super().__init__(filepath)
        self.pretty = pretty
        self.include_metadata = include_metadata
        self._reviews: list[Review] = []
        self._loaded = False

    async def save(self, reviews: list[Review]) -> int:
        """
        Save reviews to JSON file.

        Args:
            reviews: Reviews to save

        Returns:
            Number of reviews saved
        """
        if not reviews:
            return 0

        # Convert to export format
        if self.include_metadata:
            data = [r.to_full_dict() for r in reviews]
        else:
            data = [r.to_export_dict() for r in reviews]

        # Write to file
        try:
            async with aiofiles.open(self.filepath, "w", encoding="utf-8") as f:
                if self.pretty:
                    content = json.dumps(data, ensure_ascii=False, indent=2)
                else:
                    content = json.dumps(data, ensure_ascii=False)
                await f.write(content)

            logger.info(f"Saved {len(reviews)} reviews to {self.filepath}")
            return len(reviews)

        except Exception as e:
            logger.error(f"Error saving to {self.filepath}: {e}")
            return 0

    async def save_one(self, review: Review) -> bool:
        """Save a single review (appends to existing data)."""
        await self._load_if_needed()
        self._reviews.append(review)
        count = await self.save(self._reviews)
        return count > 0

    async def append(self, reviews: list[Review]) -> int:
        """
        Append reviews to existing file.

        Args:
            reviews: Reviews to append

        Returns:
            Total number of reviews after append
        """
        await self._load_if_needed()
        self._reviews.extend(reviews)
        await self.save(self._reviews)
        return len(self._reviews)

    async def load(self) -> list[Review]:
        """Load reviews from JSON file."""
        if not self.filepath.exists():
            return []

        try:
            async with aiofiles.open(self.filepath, "r", encoding="utf-8") as f:
                content = await f.read()
                if not content.strip():
                    return []
                
                data = json.loads(content)
                
                # Convert back to Review objects
                reviews = []
                for item in data:
                    try:
                        review = Review(**item)
                        reviews.append(review)
                    except Exception as e:
                        logger.warning(f"Error parsing review: {e}")

                self._reviews = reviews
                self._loaded = True
                return reviews

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {self.filepath}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading {self.filepath}: {e}")
            return []

    async def _load_if_needed(self) -> None:
        """Load data if not already loaded."""
        if not self._loaded:
            await self.load()

    async def count(self) -> int:
        """Get review count."""
        await self._load_if_needed()
        return len(self._reviews)

    async def clear(self) -> None:
        """Clear all reviews."""
        self._reviews = []
        self._loaded = True
        if self.filepath.exists():
            self.filepath.unlink()

    async def exists(self, review_id: int) -> bool:
        """Check if a review ID exists."""
        await self._load_if_needed()
        return any(r.id == review_id for r in self._reviews)

    async def get_max_id(self) -> int:
        """Get the maximum review ID in storage."""
        await self._load_if_needed()
        if not self._reviews:
            return 0
        return max(r.id for r in self._reviews)


class JsonLinesStorage(FileStorage):
    """
    JSON Lines storage - one JSON object per line.
    
    Better for streaming large datasets and incremental writes.
    """

    def __init__(self, filepath: str | Path, include_metadata: bool = False):
        super().__init__(filepath)
        self.include_metadata = include_metadata

    async def save(self, reviews: list[Review]) -> int:
        """Save reviews as JSON Lines."""
        try:
            async with aiofiles.open(self.filepath, "w", encoding="utf-8") as f:
                for review in reviews:
                    if self.include_metadata:
                        data = review.to_full_dict()
                    else:
                        data = review.to_export_dict()
                    line = json.dumps(data, ensure_ascii=False)
                    await f.write(line + "\n")

            return len(reviews)

        except Exception as e:
            logger.error(f"Error saving JSONL to {self.filepath}: {e}")
            return 0

    async def save_one(self, review: Review) -> bool:
        """Append a single review."""
        try:
            async with aiofiles.open(self.filepath, "a", encoding="utf-8") as f:
                if self.include_metadata:
                    data = review.to_full_dict()
                else:
                    data = review.to_export_dict()
                line = json.dumps(data, ensure_ascii=False)
                await f.write(line + "\n")
            return True

        except Exception as e:
            logger.error(f"Error appending to {self.filepath}: {e}")
            return False

    async def load(self) -> list[Review]:
        """Load reviews from JSON Lines file."""
        if not self.filepath.exists():
            return []

        reviews = []
        try:
            async with aiofiles.open(self.filepath, "r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        reviews.append(Review(**data))
                    except Exception as e:
                        logger.warning(f"Error parsing line: {e}")

            return reviews

        except Exception as e:
            logger.error(f"Error loading {self.filepath}: {e}")
            return []

    async def count(self) -> int:
        """Count reviews (by counting lines)."""
        if not self.filepath.exists():
            return 0

        count = 0
        async with aiofiles.open(self.filepath, "r") as f:
            async for line in f:
                if line.strip():
                    count += 1
        return count

    async def clear(self) -> None:
        """Clear the file."""
        if self.filepath.exists():
            self.filepath.unlink()

    async def exists(self, review_id: int) -> bool:
        """Check if review exists (requires full scan)."""
        reviews = await self.load()
        return any(r.id == review_id for r in reviews)


def export_to_training_format(
    reviews: list[Review],
    output_path: str | Path,
    start_id: int = 1,
) -> int:
    """
    Export reviews to the exact training data format.
    
    Format: [{"id": 1, "text": "review text..."}, ...]
    
    Args:
        reviews: Reviews to export
        output_path: Output file path
        start_id: Starting ID for renumbering
        
    Returns:
        Number of reviews exported
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Renumber IDs sequentially
    data = []
    for i, review in enumerate(reviews, start=start_id):
        data.append({
            "id": i,
            "text": review.text,
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    logger.info(f"Exported {len(data)} reviews to {output_path}")
    return len(data)