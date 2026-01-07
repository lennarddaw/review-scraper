"""Review processing pipeline - combines cleaning, deduplication, and filtering."""

from typing import AsyncIterator, Callable

from loguru import logger

from src.models.review import Review
from src.pipeline.cleaner import TextCleaner
from src.pipeline.deduplicator import Deduplicator


class ReviewPipeline:
    """
    Main pipeline for processing scraped reviews.
    
    Combines multiple processing steps:
    1. Text cleaning (normalize, fix encoding, etc.)
    2. Deduplication (remove exact and near-duplicates)
    3. Filtering (language, length, quality)
    4. Transformation (optional custom transforms)
    """

    def __init__(
        self,
        min_length: int = 20,
        max_length: int = 10000,
        dedupe: bool = True,
        clean: bool = True,
    ):
        """
        Initialize the pipeline.

        Args:
            min_length: Minimum review text length
            max_length: Maximum review text length
            dedupe: Enable deduplication
            clean: Enable text cleaning
        """
        self.min_length = min_length
        self.max_length = max_length
        self.dedupe = dedupe
        self.clean = clean

        self._cleaner = TextCleaner()
        self._deduplicator = Deduplicator()
        self._transforms: list[Callable[[Review], Review | None]] = []
        
        # Stats
        self._stats = {
            "input": 0,
            "cleaned": 0,
            "deduplicated": 0,
            "filtered_length": 0,
            "filtered_quality": 0,
            "output": 0,
        }

    def add_transform(self, transform: Callable[[Review], Review | None]) -> "ReviewPipeline":
        """
        Add a custom transform function.

        Transform functions receive a Review and return either:
        - A modified Review (or the same one)
        - None to filter out the review

        Args:
            transform: Transform function

        Returns:
            Self for chaining
        """
        self._transforms.append(transform)
        return self

    def process(self, reviews: list[Review]) -> list[Review]:
        """
        Process a list of reviews through the pipeline.

        Args:
            reviews: Input reviews

        Returns:
            Processed reviews
        """
        self._stats["input"] += len(reviews)
        result = []

        for review in reviews:
            processed = self._process_one(review)
            if processed:
                result.append(processed)

        self._stats["output"] += len(result)
        return result

    def _process_one(self, review: Review) -> Review | None:
        """Process a single review."""
        # Step 1: Clean text
        if self.clean:
            review.text = self._cleaner.clean(review.text)
            self._stats["cleaned"] += 1

        # Step 2: Length filter
        text_len = len(review.text)
        if text_len < self.min_length or text_len > self.max_length:
            self._stats["filtered_length"] += 1
            return None

        # Step 3: Deduplication
        if self.dedupe:
            if self._deduplicator.is_duplicate(review.text):
                self._stats["deduplicated"] += 1
                return None
            self._deduplicator.add(review.text)

        # Step 4: Custom transforms
        for transform in self._transforms:
            review = transform(review)
            if review is None:
                self._stats["filtered_quality"] += 1
                return None

        return review

    async def process_stream(
        self,
        reviews: AsyncIterator[Review],
    ) -> AsyncIterator[Review]:
        """
        Process reviews as an async stream.

        Args:
            reviews: Async iterator of reviews

        Yields:
            Processed reviews
        """
        async for review in reviews:
            processed = self._process_one(review)
            if processed:
                yield processed

    def get_stats(self) -> dict[str, int]:
        """Get processing statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset statistics."""
        for key in self._stats:
            self._stats[key] = 0

    def reset_deduplicator(self) -> None:
        """Reset the deduplicator state."""
        self._deduplicator.reset()


class PipelineBuilder:
    """Fluent builder for creating review pipelines."""

    def __init__(self):
        self._min_length = 20
        self._max_length = 10000
        self._dedupe = True
        self._clean = True
        self._transforms = []

    def min_length(self, length: int) -> "PipelineBuilder":
        """Set minimum text length."""
        self._min_length = length
        return self

    def max_length(self, length: int) -> "PipelineBuilder":
        """Set maximum text length."""
        self._max_length = length
        return self

    def no_dedupe(self) -> "PipelineBuilder":
        """Disable deduplication."""
        self._dedupe = False
        return self

    def no_clean(self) -> "PipelineBuilder":
        """Disable text cleaning."""
        self._clean = False
        return self

    def add_transform(self, transform: Callable[[Review], Review | None]) -> "PipelineBuilder":
        """Add a custom transform."""
        self._transforms.append(transform)
        return self

    def filter_rating(self, min_rating: float = None, max_rating: float = None) -> "PipelineBuilder":
        """Add a rating filter."""
        def rating_filter(review: Review) -> Review | None:
            if review.rating is None:
                return review
            if min_rating and review.rating < min_rating:
                return None
            if max_rating and review.rating > max_rating:
                return None
            return review
        
        self._transforms.append(rating_filter)
        return self

    def filter_negative(self) -> "PipelineBuilder":
        """Only keep negative reviews (rating <= 2)."""
        return self.filter_rating(max_rating=2.0)

    def filter_positive(self) -> "PipelineBuilder":
        """Only keep positive reviews (rating >= 4)."""
        return self.filter_rating(min_rating=4.0)

    def build(self) -> ReviewPipeline:
        """Build the pipeline."""
        pipeline = ReviewPipeline(
            min_length=self._min_length,
            max_length=self._max_length,
            dedupe=self._dedupe,
            clean=self._clean,
        )
        for transform in self._transforms:
            pipeline.add_transform(transform)
        return pipeline


# Pre-configured pipelines
def create_training_pipeline() -> ReviewPipeline:
    """Create a pipeline optimized for training data."""
    return (
        PipelineBuilder()
        .min_length(50)
        .max_length(5000)
        .build()
    )


def create_negative_review_pipeline() -> ReviewPipeline:
    """Create a pipeline for collecting negative reviews only."""
    return (
        PipelineBuilder()
        .min_length(30)
        .filter_negative()
        .build()
    )


# Aliases for backward compatibility
ReviewTransformer = ReviewPipeline
Pipeline = ReviewPipeline