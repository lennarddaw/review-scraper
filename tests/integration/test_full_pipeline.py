"""Tests for data models."""

import pytest
from datetime import datetime

from src.models.review import Review, ReviewFactory, ReviewBatch


class TestReview:
    """Tests for the Review model."""

    def test_create_review(self):
        """Test creating a basic review."""
        review = Review(id=1, text="Great product!")
        assert review.id == 1
        assert review.text == "Great product!"
        assert review.source is None

    def test_review_with_metadata(self):
        """Test creating a review with full metadata."""
        review = Review(
            id=1,
            text="Test review",
            source="trustpilot",
            rating=4.5,
            author="John",
        )
        assert review.rating == 4.5
        assert review.author == "John"
        assert review.source == "trustpilot"

    def test_export_dict(self):
        """Test export format."""
        review = Review(
            id=1,
            text="Test review",
            rating=5.0,
            author="John",
        )
        export = review.to_export_dict()
        assert export == {"id": 1, "text": "Test review"}
        assert "rating" not in export

    def test_full_dict(self):
        """Test full dict format."""
        review = Review(
            id=1,
            text="Test review",
            rating=5.0,
        )
        full = review.to_full_dict()
        assert "id" in full
        assert "text" in full
        assert "rating" in full

    def test_text_validation(self):
        """Test that empty text is rejected."""
        with pytest.raises(ValueError):
            Review(id=1, text="")


class TestReviewFactory:
    """Tests for the ReviewFactory."""

    def test_create_with_auto_id(self, review_factory):
        """Test auto-incrementing IDs."""
        r1 = review_factory.create(text="First review")
        r2 = review_factory.create(text="Second review")
        assert r1.id == 1
        assert r2.id == 2

    def test_create_batch(self, review_factory):
        """Test batch creation."""
        texts = ["Review 1", "Review 2", "Review 3"]
        reviews = review_factory.create_batch(texts, source="test")
        assert len(reviews) == 3
        assert all(r.source == "test" for r in reviews)


class TestReviewBatch:
    """Tests for ReviewBatch."""

    def test_add_review(self, sample_review):
        """Test adding reviews to batch."""
        batch = ReviewBatch()
        batch.add(sample_review)
        assert len(batch) == 1

    def test_export_list(self, sample_reviews):
        """Test exporting batch to list."""
        batch = ReviewBatch(reviews=sample_reviews)
        export = batch.to_export_list()
        assert len(export) == len(sample_reviews)
        assert all("id" in item and "text" in item for item in export)