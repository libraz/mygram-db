#!/usr/bin/env python3
"""Download Wikipedia CirrusSearch dumps and generate a MySQL seed dump for MygramDB demo.

Uses streaming download + lightweight extraction (avoids full JSON parse of huge docs).

Usage:
    python generate_dump.py --en-count 1000000 --ja-count 100000 --output seed.sql.gz
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import re
import subprocess
import sys

CIRRUSSEARCH_BASE = "https://dumps.wikimedia.org/other/cirrussearch/20251229"
EN_URL = f"{CIRRUSSEARCH_BASE}/enwiki-20251229-cirrussearch-content.json.gz"
JA_URL = f"{CIRRUSSEARCH_BASE}/jawiki-20251229-cirrussearch-content.json.gz"

MIN_TEXT_LEN_EN = 100
MIN_TEXT_LEN_JA = 50

# Regex to extract title and opening_text without full JSON parse
_TITLE_RE = re.compile(rb'"title"\s*:\s*"((?:[^"\\]|\\.)*)"')
_OPENING_RE = re.compile(rb'"opening_text"\s*:\s*"((?:[^"\\]|\\.)*)"')
_INDEX_RE = re.compile(rb'"index"')


def _decode_json_string(raw: bytes) -> str:
    """Decode a JSON string value (handle escapes)."""
    # Wrap in quotes and use json.loads to handle all escapes correctly
    try:
        return json.loads(b'"' + raw + b'"')
    except (json.JSONDecodeError, UnicodeDecodeError):
        return ""


def stream_articles(url: str, max_count: int, min_text_len: int) -> list[tuple[str, str]]:
    """Stream CirrusSearch dump, extracting title + opening_text with regex."""
    print(f"Streaming {url} ...", flush=True)
    print(f"  Target: {max_count:,} articles (min text: {min_text_len})", flush=True)

    articles: list[tuple[str, str]] = []
    skipped = 0

    curl_proc = subprocess.Popen(
        ["curl", "-s", "-L", url],
        stdout=subprocess.PIPE,
    )
    gunzip_proc = subprocess.Popen(
        ["gunzip", "-c"],
        stdin=curl_proc.stdout,
        stdout=subprocess.PIPE,
    )
    if curl_proc.stdout:
        curl_proc.stdout.close()

    assert gunzip_proc.stdout is not None
    is_index_line = True

    for raw_line in gunzip_proc.stdout:
        if is_index_line:
            is_index_line = False
            continue

        is_index_line = True

        # Fast regex extraction instead of full JSON parse
        title_m = _TITLE_RE.search(raw_line)
        opening_m = _OPENING_RE.search(raw_line)

        if not title_m or not opening_m:
            skipped += 1
            continue

        title = _decode_json_string(title_m.group(1))
        text = _decode_json_string(opening_m.group(1))

        if not title or len(text) < min_text_len:
            skipped += 1
            continue

        if title.startswith("Wikipedia:") or title.startswith("Template:"):
            skipped += 1
            continue

        articles.append((title, text))

        count = len(articles)
        if count % 100000 == 0:
            print(f"  Collected {count:,} articles (skipped {skipped:,})...", flush=True)

        if count >= max_count:
            break

    gunzip_proc.stdout.close()
    gunzip_proc.terminate()
    curl_proc.terminate()
    gunzip_proc.wait()
    curl_proc.wait()

    print(f"  Done: {len(articles):,} articles, {skipped:,} skipped", flush=True)
    return articles


def escape_sql(s: str) -> str:
    """Escape a string for SQL insertion."""
    return (
        s.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\x00", "")
        .replace("\x1a", "")
    )


def write_dump(
    en_articles: list[tuple[str, str]],
    ja_articles: list[tuple[str, str]],
    output_path: str,
    batch_size: int = 5000,
) -> None:
    """Write a gzipped SQL dump file."""
    print(f"\nWriting dump to {output_path} ...", flush=True)

    categories = [
        "tech", "science", "culture", "business", "sports",
        "history", "geography", "arts", "health", "education",
    ]
    rng = random.Random(42)

    open_fn = gzip.open if output_path.endswith(".gz") else open

    with open_fn(output_path, "wt", encoding="utf-8") as f:  # type: ignore[call-overload]
        f.write("-- MygramDB Demo Seed Data\n")
        f.write("-- Generated from Wikipedia CirrusSearch dumps (CC BY-SA 3.0)\n")
        f.write(f"-- English articles: {len(en_articles):,}\n")
        f.write(f"-- Japanese articles: {len(ja_articles):,}\n")
        f.write("--\n\n")

        f.write("SET NAMES utf8mb4;\n")
        f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
        f.write("SET UNIQUE_CHECKS = 0;\n")
        f.write("SET AUTOCOMMIT = 0;\n")
        f.write("SET sql_log_bin = 0;\n\n")
        f.write("USE mydb;\n\n")

        f.write("DROP TABLE IF EXISTS articles;\n")
        f.write("""CREATE TABLE articles (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    status INT NOT NULL DEFAULT 1,
    category VARCHAR(50),
    enabled TINYINT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_status (status),
    KEY idx_category (category),
    KEY idx_enabled (enabled),
    KEY idx_created_at (created_at),
    KEY idx_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n\n""")

        f.write(
            "ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content) "
            "WITH PARSER ngram;\n\n"
        )

        # Merge and shuffle
        all_articles: list[tuple[str, str, int, str, int, bool]] = []

        for title, text in en_articles:
            cat = rng.choice(categories)
            status = rng.choices([1, 2, 3], weights=[85, 10, 5])[0]
            deleted = rng.random() < 0.03
            all_articles.append((title, text, status, cat, 1, deleted))

        for title, text in ja_articles:
            cat = rng.choice(categories)
            status = rng.choices([1, 2, 3], weights=[85, 10, 5])[0]
            deleted = rng.random() < 0.03
            all_articles.append((title, text, status, cat, 1, deleted))

        rng.shuffle(all_articles)

        total = len(all_articles)
        for i in range(0, total, batch_size):
            batch = all_articles[i : i + batch_size]
            f.write(
                "INSERT INTO articles "
                "(title, content, status, category, enabled, deleted_at) VALUES\n"
            )
            values = []
            for title, content, status, cat, enabled, deleted in batch:
                title_esc = escape_sql(title[:255])
                content_esc = escape_sql(content)
                deleted_val = "'2024-01-01 00:00:00'" if deleted else "NULL"
                values.append(
                    f"('{title_esc}','{content_esc}',{status},'{cat}',{enabled},{deleted_val})"
                )
            f.write(",\n".join(values))
            f.write(";\n")

            done = min(i + batch_size, total)
            if done % 100000 == 0 or done == total:
                print(f"  Written {done:,} / {total:,} rows", flush=True)

        f.write("\nCOMMIT;\n")
        f.write("SET sql_log_bin = 1;\n")
        f.write("SET UNIQUE_CHECKS = 1;\n")
        f.write("SET FOREIGN_KEY_CHECKS = 1;\n")
        f.write("SET AUTOCOMMIT = 1;\n")

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nDump complete: {output_path} ({size_mb:.1f} MB)", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate MygramDB seed data from Wikipedia CirrusSearch dumps"
    )
    parser.add_argument("--en-count", type=int, default=1_000_000)
    parser.add_argument("--ja-count", type=int, default=100_000)
    parser.add_argument("--output", default="seed.sql.gz")
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()

    en_articles = stream_articles(EN_URL, args.en_count, MIN_TEXT_LEN_EN)
    ja_articles = stream_articles(JA_URL, args.ja_count, MIN_TEXT_LEN_JA)
    write_dump(en_articles, ja_articles, args.output, args.batch_size)

    print(f"\nDone! To load into MySQL:")
    print(f"  gunzip -c {args.output} | mysql -u root -p mydb")


if __name__ == "__main__":
    main()
