"""Incremental storage with checkpoint support for resumable scraping."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import aiofiles
from loguru import logger
from pydantic import BaseModel

from src.models.review import Review
from src.storage.json_storage import JsonStorage


class ScrapeCheckpoint(BaseModel):
    """Checkpoint data for resuming a scrape."""

    source: str
    started_at: datetime
    updated_at: datetime
    total_reviews: int
    urls_completed: list[str]
    urls_pending: list[str]
    current_url: str | None = None
    current_page: int = 1
    last_review_id: int = 0
    errors: list[str] = []


class IncrementalStorage:
    """
    Storage with incremental saves and checkpoint support.
    
    Features:
    - Saves reviews in batches to avoid data loss
    - Maintains checkpoint for resume capability
    - Tracks progress across multiple URLs
    """

    def __init__(
        self,
        output_dir: str | Path,
        source_name: str,
        batch_size: int = 100,
    ):
        """
        Initialize incremental storage.

        Args:
            output_dir: Directory for output files
            source_name: Name of the source being scraped
            batch_size: Number of reviews before auto-save
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.source_name = source_name
        self.batch_size = batch_size
        
        # File paths
        self.data_file = self.output_dir / f"{source_name}_reviews.json"
        self.checkpoint_file = self.output_dir / f"{source_name}_checkpoint.json"
        
        # In-memory buffer
        self._buffer: list[Review] = []
        self._all_reviews: list[Review] = []
        self._checkpoint: ScrapeCheckpoint | None = None
        
        # Storage backend
        self._storage = JsonStorage(self.data_file)

    async def initialize(self, urls: list[str]) -> ScrapeCheckpoint:
        """
        Initialize or load checkpoint.

        Args:
            urls: List of URLs to scrape

        Returns:
            Checkpoint (new or resumed)
        """
        # Try to load existing checkpoint
        if self.checkpoint_file.exists():
            checkpoint = await self._load_checkpoint()
            if checkpoint:
                logger.info(f"Resuming from checkpoint: {checkpoint.total_reviews} reviews")
                self._checkpoint = checkpoint
                
                # Load existing reviews
                self._all_reviews = await self._storage.load()
                
                return checkpoint

        # Create new checkpoint
        self._checkpoint = ScrapeCheckpoint(
            source=self.source_name,
            started_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            total_reviews=0,
            urls_completed=[],
            urls_pending=urls.copy(),
        )
        await self._save_checkpoint()
        
        return self._checkpoint

    async def add_review(self, review: Review) -> None:
        """
        Add a review to the buffer.
        
        Automatically saves when buffer reaches batch_size.
        """
        self._buffer.append(review)
        
        if len(self._buffer) >= self.batch_size:
            await self.flush()

    async def add_reviews(self, reviews: list[Review]) -> None:
        """Add multiple reviews."""
        for review in reviews:
            await self.add_review(review)

    async def flush(self) -> int:
        """
        Flush buffer to storage.
        
        Returns:
            Number of reviews flushed
        """
        if not self._buffer:
            return 0

        # Add to all reviews
        self._all_reviews.extend(self._buffer)
        count = len(self._buffer)
        self._buffer = []

        # Save to file
        await self._storage.save(self._all_reviews)
        
        # Update checkpoint
        if self._checkpoint:
            self._checkpoint.total_reviews = len(self._all_reviews)
            self._checkpoint.updated_at = datetime.utcnow()
            if self._all_reviews:
                self._checkpoint.last_review_id = self._all_reviews[-1].id
            await self._save_checkpoint()

        logger.debug(f"Flushed {count} reviews (total: {len(self._all_reviews)})")
        return count

    async def mark_url_complete(self, url: str) -> None:
        """Mark a URL as completed."""
        if self._checkpoint:
            if url in self._checkpoint.urls_pending:
                self._checkpoint.urls_pending.remove(url)
            if url not in self._checkpoint.urls_completed:
                self._checkpoint.urls_completed.append(url)
            self._checkpoint.current_url = None
            await self._save_checkpoint()

    async def mark_url_started(self, url: str, page: int = 1) -> None:
        """Mark a URL as currently being scraped."""
        if self._checkpoint:
            self._checkpoint.current_url = url
            self._checkpoint.current_page = page
            await self._save_checkpoint()

    async def record_error(self, error: str) -> None:
        """Record an error in the checkpoint."""
        if self._checkpoint:
            self._checkpoint.errors.append(f"{datetime.utcnow().isoformat()}: {error}")
            await self._save_checkpoint()

    async def finalize(self) -> int:
        """
        Finalize the scrape - flush all data and clean up.
        
        Returns:
            Total number of reviews
        """
        await self.flush()
        
        # Clean up checkpoint file on successful completion
        if self._checkpoint and not self._checkpoint.urls_pending:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
                logger.info("Scrape complete - checkpoint removed")

        return len(self._all_reviews)

    async def get_pending_urls(self) -> list[str]:
        """Get list of URLs still to be scraped."""
        if self._checkpoint:
            return self._checkpoint.urls_pending.copy()
        return []

    async def get_progress(self) -> dict[str, Any]:
        """Get current progress information."""
        if not self._checkpoint:
            return {}

        total_urls = len(self._checkpoint.urls_completed) + len(self._checkpoint.urls_pending)
        completed_urls = len(self._checkpoint.urls_completed)

        return {
            "source": self.source_name,
            "total_reviews": self._checkpoint.total_reviews + len(self._buffer),
            "urls_completed": completed_urls,
            "urls_total": total_urls,
            "progress_percent": (completed_urls / total_urls * 100) if total_urls else 0,
            "current_url": self._checkpoint.current_url,
            "errors": len(self._checkpoint.errors),
        }

    async def _save_checkpoint(self) -> None:
        """Save checkpoint to file."""
        if not self._checkpoint:
            return

        try:
            async with aiofiles.open(self.checkpoint_file, "w") as f:
                data = self._checkpoint.model_dump(mode="json")
                await f.write(json.dumps(data, indent=2, default=str))
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    async def _load_checkpoint(self) -> ScrapeCheckpoint | None:
        """Load checkpoint from file."""
        try:
            async with aiofiles.open(self.checkpoint_file, "r") as f:
                content = await f.read()
                data = json.loads(content)
                return ScrapeCheckpoint(**data)
        except Exception as e:
            logger.warning(f"Error loading checkpoint: {e}")
            return None

    @property
    def total_reviews(self) -> int:
        """Get total review count including buffer."""
        return len(self._all_reviews) + len(self._buffer)