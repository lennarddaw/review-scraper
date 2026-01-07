"""Review validation for quality control."""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from loguru import logger


class ValidationLevel(str, Enum):
    """Validation result levels."""
    PASS = "pass"
    WARNING = "warning"
    FAIL = "fail"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    level: ValidationLevel
    message: str
    field: str | None = None
    value: Any = None

    @property
    def passed(self) -> bool:
        return self.level == ValidationLevel.PASS

    @property
    def failed(self) -> bool:
        return self.level == ValidationLevel.FAIL


class ReviewValidator:
    """
    Validates reviews for quality and completeness.
    
    Checks:
    - Text length and content
    - Rating validity
    - Required fields
    - Spam/quality indicators
    """

    def __init__(
        self,
        min_length: int = 10,
        max_length: int = 50000,
        require_rating: bool = False,
        check_spam: bool = True,
    ):
        self.min_length = min_length
        self.max_length = max_length
        self.require_rating = require_rating
        self.check_spam = check_spam

        # Spam patterns
        self._spam_patterns = [
            r"(?i)click\s+here",
            r"(?i)buy\s+now",
            r"(?i)free\s+money",
            r"(?i)make\s+\$\d+",
            r"(?i)work\s+from\s+home",
            r"https?://[^\s]+\s*https?://",  # Multiple URLs
        ]

    def validate(self, review) -> list[ValidationResult]:
        """
        Validate a review.

        Args:
            review: Review object to validate

        Returns:
            List of validation results
        """
        results = []

        # Check text
        results.extend(self._validate_text(review.text))

        # Check rating
        if self.require_rating and review.rating is None:
            results.append(ValidationResult(
                level=ValidationLevel.FAIL,
                message="Rating is required",
                field="rating",
            ))
        elif review.rating is not None:
            results.extend(self._validate_rating(review.rating))

        # Spam check
        if self.check_spam:
            results.extend(self._check_spam(review.text))

        return results

    def is_valid(self, review) -> bool:
        """Check if a review passes all validations."""
        results = self.validate(review)
        return not any(r.failed for r in results)

    def _validate_text(self, text: str) -> list[ValidationResult]:
        """Validate review text."""
        results = []

        if not text:
            results.append(ValidationResult(
                level=ValidationLevel.FAIL,
                message="Text is empty",
                field="text",
            ))
            return results

        length = len(text)

        if length < self.min_length:
            results.append(ValidationResult(
                level=ValidationLevel.FAIL,
                message=f"Text too short ({length} < {self.min_length})",
                field="text",
                value=length,
            ))
        elif length > self.max_length:
            results.append(ValidationResult(
                level=ValidationLevel.FAIL,
                message=f"Text too long ({length} > {self.max_length})",
                field="text",
                value=length,
            ))

        # Check for placeholder text
        placeholder_patterns = [
            r"^test\s*$",
            r"^lorem\s+ipsum",
            r"^n/a\s*$",
            r"^\.\s*$",
            r"^-+\s*$",
        ]
        for pattern in placeholder_patterns:
            if re.match(pattern, text, re.IGNORECASE):
                results.append(ValidationResult(
                    level=ValidationLevel.FAIL,
                    message="Text appears to be placeholder",
                    field="text",
                ))
                break

        return results

    def _validate_rating(self, rating: float) -> list[ValidationResult]:
        """Validate rating value."""
        results = []

        if rating < 0 or rating > 5:
            results.append(ValidationResult(
                level=ValidationLevel.FAIL,
                message=f"Rating out of range (0-5): {rating}",
                field="rating",
                value=rating,
            ))

        return results

    def _check_spam(self, text: str) -> list[ValidationResult]:
        """Check for spam indicators."""
        results = []

        for pattern in self._spam_patterns:
            if re.search(pattern, text):
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    message=f"Potential spam detected: {pattern}",
                    field="text",
                ))

        # Check for excessive caps
        if len(text) > 20:
            caps_ratio = sum(1 for c in text if c.isupper()) / len(text)
            if caps_ratio > 0.5:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    message=f"Excessive caps ({caps_ratio:.0%})",
                    field="text",
                ))

        # Check for repetitive characters
        if re.search(r"(.)\1{4,}", text):
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                message="Repetitive characters detected",
                field="text",
            ))

        return results


class BatchValidator:
    """Validates batches of reviews with statistics."""

    def __init__(self, validator: ReviewValidator | None = None):
        self.validator = validator or ReviewValidator()
        self._stats = {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0,
        }

    def validate_batch(self, reviews: list) -> tuple[list, list]:
        """
        Validate a batch of reviews.

        Returns:
            Tuple of (valid_reviews, invalid_reviews)
        """
        valid = []
        invalid = []

        for review in reviews:
            self._stats["total"] += 1
            results = self.validator.validate(review)

            if any(r.failed for r in results):
                self._stats["failed"] += 1
                invalid.append((review, results))
            else:
                self._stats["passed"] += 1
                if any(r.level == ValidationLevel.WARNING for r in results):
                    self._stats["warnings"] += 1
                valid.append(review)

        return valid, invalid

    def get_stats(self) -> dict[str, int]:
        """Get validation statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset statistics."""
        for key in self._stats:
            self._stats[key] = 0