"""Synthetic data generation for e2e tests."""

from __future__ import annotations

import os
import random
from typing import Any

_WORDLIST_DIR = os.path.join(os.path.dirname(__file__), "wordlists")


def _load_wordlist(filename: str) -> list[str]:
    """Load a wordlist file, one entry per line."""
    path = os.path.join(_WORDLIST_DIR, filename)
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


class DataGenerator:
    """Generate synthetic test data from wordlists."""

    CATEGORIES = ["tech", "science", "culture", "business", "sports"]
    STATUSES = [1, 2, 3]

    def __init__(self, seed: int = 42) -> None:
        self.rng = random.Random(seed)
        self._english_words: list[str] | None = None
        self._japanese_phrases: list[str] | None = None

    @property
    def english_words(self) -> list[str]:
        if self._english_words is None:
            self._english_words = _load_wordlist("english_words.txt")
        return self._english_words

    @property
    def japanese_phrases(self) -> list[str]:
        if self._japanese_phrases is None:
            self._japanese_phrases = _load_wordlist("japanese_phrases.txt")
        return self._japanese_phrases

    def _random_sentence(self, min_words: int = 5, max_words: int = 30) -> str:
        """Generate a random English sentence."""
        length = self.rng.randint(min_words, max_words)
        words = [self.rng.choice(self.english_words) for _ in range(length)]
        sentence = " ".join(words)
        return sentence[0].upper() + sentence[1:] + "."

    def _random_paragraph(self, min_sentences: int = 2, max_sentences: int = 5) -> str:
        """Generate a random paragraph."""
        count = self.rng.randint(min_sentences, max_sentences)
        return " ".join(self._random_sentence() for _ in range(count))

    def _random_mixed_content(self) -> str:
        """Generate mixed English + Japanese content."""
        parts = []
        for _ in range(self.rng.randint(1, 3)):
            parts.append(self._random_paragraph(1, 3))
        if self.japanese_phrases:
            jp_count = self.rng.randint(1, 3)
            for _ in range(jp_count):
                parts.append(self.rng.choice(self.japanese_phrases))
        self.rng.shuffle(parts)
        return " ".join(parts)

    def generate_articles(
        self,
        count: int = 100,
        *,
        mixed: bool = False,
    ) -> list[dict[str, Any]]:
        """Generate article rows."""
        rows: list[dict[str, Any]] = []
        for _i in range(count):
            title_words = [
                self.rng.choice(self.english_words) for _ in range(self.rng.randint(3, 8))
            ]
            title = " ".join(title_words).title()

            if mixed and self.japanese_phrases:
                content = self._random_mixed_content()
            else:
                content = self._random_paragraph(2, 6)

            deleted_at = None
            if self.rng.random() < 0.05:  # 5% soft-deleted
                deleted_at = "2024-01-01 00:00:00"

            rows.append(
                {
                    "title": title,
                    "content": content,
                    "status": self.rng.choice(self.STATUSES),
                    "category": self.rng.choice(self.CATEGORIES),
                    "enabled": 1,
                    "deleted_at": deleted_at,
                }
            )
        return rows

    def generate_products(self, count: int = 100) -> list[dict[str, Any]]:
        """Generate product rows."""
        rows: list[dict[str, Any]] = []
        for _ in range(count):
            name_words = [
                self.rng.choice(self.english_words) for _ in range(self.rng.randint(2, 5))
            ]
            rows.append(
                {
                    "name": " ".join(name_words).title(),
                    "description": self._random_paragraph(1, 3),
                    "status": self.rng.choice(self.STATUSES),
                    "category": self.rng.choice(self.CATEGORIES),
                    "enabled": 1,
                }
            )
        return rows

    def generate_edge_cases(self) -> list[dict[str, Any]]:
        """Generate edge case rows for boundary testing."""
        cases: list[dict[str, Any]] = [
            {"content": "", "status": 1, "enabled": 1},  # empty
            {"content": "a", "status": 1, "enabled": 1},  # single char
            {"content": "ab", "status": 1, "enabled": 1},  # minimum bigram
            {"content": "Hello World", "status": 1, "enabled": 1},  # basic
            {
                "content": "\uff28\uff45\uff4c\uff4c\uff4f\u3000\uff37\uff4f\uff52\uff4c\uff44",
                "status": 1,
                "enabled": 1,
            },  # fullwidth
            {"content": "\ufb01\ufb02\ufb03\ufb04", "status": 1, "enabled": 1},  # ligatures (NFKC)
            {
                "content": "caf\u00e9 r\u00e9sum\u00e9 na\u00efve",
                "status": 1,
                "enabled": 1,
            },  # accented
            {
                "content": "\u6771\u4eac\u30bf\u30ef\u30fc\u306f\u6771\u4eac\u90fd\u6e2f\u533a\u306b\u3042\u308a\u307e\u3059",  # noqa: E501
                "status": 1,
                "enabled": 1,
            },  # Japanese
            {
                "content": "\u5317\u4eac\u5e02\u662f\u4e2d\u534e\u4eba\u6c11\u5171\u548c\u56fd\u7684\u9996\u90fd",  # noqa: E501
                "status": 1,
                "enabled": 1,
            },  # Chinese
            {
                "content": "Hello \u4e16\u754c \u3053\u3093\u306b\u3061\u306f World",
                "status": 1,
                "enabled": 1,
            },  # mixed script
            {
                "content": "\U0001f389\U0001f38a\U0001f388 Party time! \U0001f973",
                "status": 1,
                "enabled": 1,
            },  # emoji
            {
                "content": "zero\u200bwidth\u200bjoin\u200btest",
                "status": 1,
                "enabled": 1,
            },  # zero-width
            {
                "content": "Robert'); DROP TABLE articles;--",
                "status": 1,
                "enabled": 1,
            },  # SQL injection
            {"content": "<script>alert('xss')</script>", "status": 1, "enabled": 1},  # XSS
            {"content": "a" * 10000, "status": 1, "enabled": 1},  # long repeated
        ]
        return cases

    def generate_large_content(self, size_bytes: int = 1_000_000) -> str:
        """Generate a large text content of approximately the given size."""
        parts = []
        current_size = 0
        while current_size < size_bytes:
            para = self._random_paragraph(3, 8)
            parts.append(para)
            current_size += len(para.encode("utf-8")) + 1  # +1 for newline
        return "\n".join(parts)
