"""Pytest configuration and fixtures."""

import pytest
from datetime import datetime

from src.models.review import Review, ReviewFactory


@pytest.fixture
def sample_review():
    """Create a sample review for testing."""
    return Review(
        id=1,
        text="This is a great product! I love it and would recommend to everyone.",
        source="test",
        rating=5.0,
        title="Great Product",
        author="Test User",
        date=datetime.utcnow(),
    )


@pytest.fixture
def sample_reviews():
    """Create a list of sample reviews."""
    factory = ReviewFactory()
    return [
        factory.create(
            text="Excellent product, fast shipping, great quality!",
            rating=5.0,
            source="test",
        ),
        factory.create(
            text="Terrible experience. Product broke after one day.",
            rating=1.0,
            source="test",
        ),
        factory.create(
            text="Average product, nothing special but works as expected.",
            rating=3.0,
            source="test",
        ),
        factory.create(
            text="Good value for money. Would buy again.",
            rating=4.0,
            source="test",
        ),
        factory.create(
            text="Worst purchase ever. Complete waste of money.",
            rating=1.0,
            source="test",
        ),
    ]


@pytest.fixture
def review_factory():
    """Create a review factory."""
    return ReviewFactory()