"""Review data model."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Review(BaseModel):
    """
    Core review data model.
    
    The minimal export format is {id, text} to match the training data format.
    Additional metadata fields are available for filtering and analysis.
    """

    # Required fields (for export)
    id: int = Field(..., description="Unique review identifier")
    text: str = Field(..., description="Review text content")

    # Optional metadata (not exported by default)
    source: str | None = Field(default=None, description="Source platform (e.g., 'trustpilot')")
    source_url: str | None = Field(default=None, description="Original URL of the review")
    source_id: str | None = Field(default=None, description="Original ID from source platform")
    
    rating: float | None = Field(default=None, ge=0, le=5, description="Rating (0-5 scale)")
    title: str | None = Field(default=None, description="Review title/headline")
    author: str | None = Field(default=None, description="Author name/username")
    date: datetime | None = Field(default=None, description="Review date")
    
    product_name: str | None = Field(default=None, description="Product or business name")
    product_id: str | None = Field(default=None, description="Product identifier")
    category: str | None = Field(default=None, description="Product category")
    
    helpful_count: int | None = Field(default=None, ge=0, description="Helpful votes count")
    verified: bool | None = Field(default=None, description="Verified purchase/user")
    
    language: str | None = Field(default=None, description="Detected language code")
    scraped_at: datetime = Field(default_factory=datetime.utcnow, description="Scrape timestamp")

    @field_validator("text")
    @classmethod
    def clean_text(cls, v: str) -> str:
        """Clean and normalize review text."""
        if not v:
            raise ValueError("Review text cannot be empty")
        # Basic normalization (more thorough cleaning happens in pipeline)
        return v.strip()

    def to_export_dict(self) -> dict[str, Any]:
        """
        Convert to minimal export format {id, text}.
        
        This matches the target training data format.
        """
        return {
            "id": self.id,
            "text": self.text,
        }

    def to_full_dict(self) -> dict[str, Any]:
        """Convert to full dict including all metadata."""
        return self.model_dump(exclude_none=True)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class ReviewBatch(BaseModel):
    """A batch of reviews with metadata."""

    reviews: list[Review] = Field(default_factory=list)
    source: str | None = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    total_count: int = 0
    page: int | None = None

    def __len__(self) -> int:
        return len(self.reviews)

    def __iter__(self):
        return iter(self.reviews)

    def add(self, review: Review) -> None:
        """Add a review to the batch."""
        self.reviews.append(review)
        self.total_count = len(self.reviews)

    def extend(self, reviews: list[Review]) -> None:
        """Add multiple reviews to the batch."""
        self.reviews.extend(reviews)
        self.total_count = len(self.reviews)

    def to_export_list(self) -> list[dict[str, Any]]:
        """Convert all reviews to export format."""
        return [r.to_export_dict() for r in self.reviews]


class ReviewFactory:
    """Factory for creating Review objects with auto-incrementing IDs."""

    def __init__(self, start_id: int = 1):
        self._next_id = start_id

    def create(self, text: str, **kwargs) -> Review:
        """Create a new review with auto-assigned ID."""
        review = Review(id=self._next_id, text=text, **kwargs)
        self._next_id += 1
        return review

    def create_batch(self, texts: list[str], **common_kwargs) -> list[Review]:
        """Create multiple reviews from a list of texts."""
        return [self.create(text, **common_kwargs) for text in texts]

    @property
    def next_id(self) -> int:
        """Get the next ID that will be assigned."""
        return self._next_id

    def set_next_id(self, id: int) -> None:
        """Set the next ID (useful for resuming)."""
        self._next_id = id