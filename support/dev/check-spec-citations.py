#!/usr/bin/env python3
"""Verify that every source citation in spec/ still resolves.

The specification cites the implementation so that a reader can check a claim
against the code instead of trusting the prose. A citation that no longer
resolves is worse than no citation: it looks like evidence while pointing at
something else.

Line numbers cannot carry that guarantee. They drift on every refactor that
adds or removes a line above the cited one, silently and in bulk, and nothing
about the stale result looks wrong. So citations name a file, and optionally a
symbol within it, and this script checks both.

Rules enforced:

  1. Every backticked repository path resolves to a tracked file.
  2. No text names a file together with a line anchor (`path:123` or
     `path:12-34`), and no backticked bare `:123` continuation appears. Line
     anchors are rejected outright rather than validated, because a line number
     that happens to be in range still says nothing about whether it points at
     the cited behavior.
  3. In the form `Symbol` (`path`), where the token is written the way this
     codebase writes a symbol, that symbol appears in that file as a whole
     word.

Rules 1 and 3 read only prose, because fenced blocks hold shell transcripts and
sample payloads whose paths are illustrative and whose backticks are literal
text. Rule 2 reads prose and fenced blocks alike, and needs no backticks, so an
anchor written as plain text inside a transcript is caught as well.

Rule 3 is the only rule that looks inside the cited file, and it applies to the
subset of citations that name a symbol in a shape this codebase uses for one.
Every other citation is checked for the existence of the file and nothing more:
a citation that names a live file but describes the wrong behavior in it still
passes.

Exit status is 0 when every citation resolves and 1 otherwise.
"""

import re
import subprocess
import sys
from pathlib import Path

# Directories whose Markdown is checked. The specification is normative, so its
# citations are the ones worth enforcing mechanically.
CHECKED_DIRS = ("spec",)

# Extensions that make a backticked token a repository path rather than prose.
SOURCE_EXTENSIONS = ("cpp", "h", "hpp", "json", "yaml", "yml", "md", "txt", "py", "sh", "cmake")

_EXT_ALT = "|".join(SOURCE_EXTENSIONS)

# A backticked path, with or without a line anchor. The anchor is tolerated here
# so that the path itself is still resolved; rule 2 reports the anchor.
CITATION = re.compile(rf"`([A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:{_EXT_ALT}))(?::\d+(?:-\d+)?)?`")

# A file name followed by a line anchor, with or without backticks around it.
# Requiring a known source extension is what keeps this off the host:port pairs,
# clock times and version strings that fill shell transcripts.
LINE_ANCHOR = re.compile(rf"[A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:{_EXT_ALT}):\d+(?:-\d+)?")

# A bare `:123` continuation, used to name a second line in a file already cited
# earlier in the same sentence. Only meaningful next to a line anchor, so it is
# rejected for the same reason.
CONTINUATION = re.compile(r"`(:\d+(?:-\d+)?)`")

# `Symbol` (`path`) — the symbol-anchored citation form.
SYMBOL_CITATION = re.compile(
    rf"`([A-Za-z_][A-Za-z0-9_:]*)(?:\(\))?`\s*\(\s*`([A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:{_EXT_ALT}))`"
)

# A backticked token in front of a citation is only treated as a symbol when it
# is written the way this codebase writes one. Response field names, enum
# *values*, protocol tokens and type words are also backticked and also sit next
# to citations, and resolving those against the file would be meaningless.
#
# The two exclusions that do the work: a name has to carry a lowercase letter,
# which drops the all-caps protocol tokens and enum values (`AUTH`, `MGIX`,
# `EQ`, `DECIMAL`), and an all-lowercase name has to be multi-word, which drops
# the bare state and type words (`disconnected`, `true`, `int`).
SYMBOL_SHAPE = re.compile(
    r"""^(?:
          (?:[A-Za-z_][A-Za-z0-9_]*::)+[A-Za-z_][A-Za-z0-9_]*  # Qualified::name
        | k[A-Z][A-Za-z0-9_]*                                  # kConstant
        | [A-Z][A-Za-z0-9_]*[a-z][A-Za-z0-9_]*                 # Type, BM25Stats, LRUCache
        | [a-z][a-z0-9]*(?:_[a-z0-9]+)+                        # snake_case_function
    )$""",
    re.VERBOSE,
)

# Fenced code blocks hold shell transcripts and sample payloads. Their paths are
# illustrative and their backticks are literal text, so only rule 2 reads them.
FENCE = re.compile(r"^\s*```")


def tracked_files(repo_root: Path) -> tuple[set[str], dict[str, list[str]]]:
    """Return the set of tracked paths and an index from basename to paths."""
    listing = subprocess.run(
        ["git", "ls-files"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split("\n")
    paths = {line for line in listing if line}
    by_basename: dict[str, list[str]] = {}
    for path in paths:
        by_basename.setdefault(path.rsplit("/", 1)[-1], []).append(path)
    return paths, by_basename


def check_document(
    path: Path,
    repo_root: Path,
    paths: set[str],
    by_basename: dict[str, list[str]],
    symbol_cache: dict[str, str],
) -> list[str]:
    """Return one message per citation in this document that does not resolve."""
    problems: list[str] = []
    in_fence = False

    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        if FENCE.match(line):
            in_fence = not in_fence
            continue

        where = f"{path.relative_to(repo_root)}:{lineno}"

        for match in LINE_ANCHOR.finditer(line):
            problems.append(
                f"{where}: `{match.group(0)}` carries a line anchor; cite the file, "
                f"and the symbol if the file is large"
            )

        if in_fence:
            continue

        for match in CITATION.finditer(line):
            cited = match.group(1)
            if cited in paths:
                continue
            if "/" in cited:
                problems.append(f"{where}: `{cited}` is not a tracked file")
                continue
            candidates = by_basename.get(cited.rsplit("/", 1)[-1], [])
            if not candidates:
                problems.append(f"{where}: `{cited}` matches no tracked file")
            elif len(candidates) > 1:
                problems.append(
                    f"{where}: `{cited}` is ambiguous ({', '.join(candidates)}); "
                    f"cite the full path"
                )
            else:
                problems.append(
                    f"{where}: `{cited}` is a bare filename; cite `{candidates[0]}`"
                )

        for match in CONTINUATION.finditer(line):
            problems.append(
                f"{where}: `{match.group(1)}` is a line anchor continuation; "
                f"cite the file again instead"
            )

        for match in SYMBOL_CITATION.finditer(line):
            symbol, cited = match.group(1), match.group(2)
            if not SYMBOL_SHAPE.match(symbol):
                continue
            if cited not in paths:
                continue  # Already reported by the path rule above.
            # A qualified name is written as it appears at the definition, which
            # for a member function is the unqualified name.
            needle = symbol.rsplit("::", 1)[-1]
            if cited not in symbol_cache:
                symbol_cache[cited] = (repo_root / cited).read_text(errors="replace")
            # Whole-word, so a longer identifier that merely contains the name
            # does not stand in for it.
            if not re.search(rf"\b{re.escape(needle)}\b", symbol_cache[cited]):
                problems.append(f"{where}: `{symbol}` does not appear in `{cited}`")

    return problems


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )
    paths, by_basename = tracked_files(repo_root)
    symbol_cache: dict[str, str] = {}

    documents = sorted(
        document
        for directory in CHECKED_DIRS
        for document in (repo_root / directory).rglob("*.md")
    )
    if not documents:
        print(f"no documents found under {', '.join(CHECKED_DIRS)}", file=sys.stderr)
        return 1

    problems: list[str] = []
    for document in documents:
        problems.extend(
            check_document(document, repo_root, paths, by_basename, symbol_cache)
        )

    if problems:
        for problem in problems:
            print(problem)
        print(
            f"\n{len(problems)} citation(s) do not resolve across {len(documents)} document(s)",
            file=sys.stderr,
        )
        return 1

    print(f"all citations resolve across {len(documents)} document(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
