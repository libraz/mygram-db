# MygramDB external surface specification

This directory is the normative description of everything a client, an operator or a
packager can observe about MygramDB: the TCP command set, the HTTP routes, the error
codes, the configuration keys, and the on-disk format versions the code accepts.

Where any other text in this repository disagrees with a file in `spec/`, that other
text is wrong. The authority order is:

`spec/` > `src/config/config-schema.json` > public signatures in installed headers >
`README*.md` and `docs/` > Doxygen comments on public headers > inline comments

## Contents

| File | Covers |
|---|---|
| `spec/tcp-commands.md` | TCP text protocol: commands, grammar, arity, limits, framing, auth, response formats |
| `spec/http-routes.md` | HTTP surface: routes, parameters, status codes, JSON shapes, and its divergences from TCP |
| `spec/error-codes.md` | Every defined error code, its meaning, and which surfaces emit it |
| `spec/config-keys.md` | Every configuration key, its type, range, default, and whether it is startup-only or runtime-mutable; CLI flags and environment variables |
| `spec/persistence-formats.md` | Dump container and index serialization layouts, and the version-acceptance policy |
| `spec/filter-semantics.md` | Per-type comparison rules for query filters and for `required_filters`, at each site that evaluates them, and where those sites disagree |
| `spec/surface.snapshot.txt` | Generated golden: the static surface rendered as deterministic text |
| `spec/response-shapes.snapshot.txt` | Generated golden: the responses a representative request set produces on both surfaces |

## The two goldens

The `.txt` files are generated, not hand-written. Do not edit them by hand.

`spec/surface.snapshot.txt` is what the server binary prints for `--print-surface`. It renders
the command table, the request limits, the route table, the error-code registry, the
configuration keys, the accepted format versions and the CLI flags — every part of the
surface that is fixed at build time.

`spec/response-shapes.snapshot.txt` records what a representative set of requests actually
returns on both protocols, with volatile values (timestamps, durations, allocator
figures, ports, paths) replaced by typed placeholders.

A diff in either file means the external surface moved. That is a decision, not an
inconvenience: confirm the change is intended, record it in the release notes, and only
then regenerate.

```
make surface-snapshot                                        # regenerate surface.snapshot.txt
MYGRAMDB_UPDATE_SNAPSHOT=1 ctest -R ResponseShapeSnapshot    # regenerate response-shapes.snapshot.txt
```

`make surface-snapshot` runs from the repository root and rebuilds the server binary first.
The `ctest` invocation runs from a configured build directory; the test it selects is
`ResponseShapeSnapshotTest.ResponseShapesMatchGolden`, which compares against the golden
unless `MYGRAMDB_UPDATE_SNAPSHOT` is set in the environment.

## Known divergences

Several of the documents carry a "Known divergences" section. Those record places where
two code paths, or a document and the code, currently disagree. They are part of the
description of what is, not a list of intentions. A divergence is resolved by making the
code agree with this specification and deleting the entry.

## Citations

Every claim in the Markdown files names the file that implements it, so the claim can be
re-checked against the implementation rather than taken on trust. Where the file is large
the claim also names the symbol, written as ``​`Symbol` (`path`)``.

Citations carry no line numbers. A line number drifts on any edit above the line it names,
silently and in bulk, and the stale result still looks like evidence. A file path and a
symbol name survive every refactor that does not rename them, and a rename is visible.

```
make spec-check     # verify that every citation still resolves
```

That check reads the tree directly and enforces three rules:

- Every backticked repository path in the prose resolves to a tracked file, named by its
  full path rather than by its basename.
- Nothing names a file together with a line number — not in the prose, not inside a fenced
  block, with or without backticks around it.
- Where a citation names a symbol and the token is written the way this codebase writes
  one, that symbol appears in the cited file as a whole word.

Only the third rule looks inside a file, and it reaches the small minority of citations
that name a symbol. A token spelled the way this specification spells an enum value or a
protocol token is left alone, because resolving one against a file would be meaningless.
Every remaining citation is checked for the existence of the cited file and nothing more.

So the check catches a path that was renamed or deleted and a symbol that was renamed. It
does not catch a citation that still names a live file but describes the wrong behavior in
it — that stays a matter of reading the code.

When a citation no longer resolves to the behavior described, the specification is stale
and fixing it takes priority over the change that made it stale.
