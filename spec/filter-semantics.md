# Filter Comparison Semantics

This file is normative. It describes the comparison rule that each filter-evaluating code
path in MygramDB applies today, per value type, and records where two paths that decide the
same question reach different answers. Every claim names the file that implements it, so the
document can be re-verified against the source; see `spec/README.md` for the citation rules
and the check that enforces them.

---

## 1. Two features, two contracts

MygramDB has two independent filtering features. They share a type vocabulary and a
configuration file, and nothing else. Neither one constrains the other.

| | Query filters | `required_filters` |
|---|---|---|
| Surface | `FILTER` clause on TCP, the `filters` object on HTTP | `tables[].required_filters` in the configuration file |
| Question answered | Which of the already-indexed documents does this query return? | Which rows are in the index at all? |
| Evaluated | Per request, in memory | Twice: once as SQL in the initial-load `SELECT`, once in C++ per binlog row event |
| Value comparison sites | `ApplyFilters`, `ApplyFiltersWithBitmap` (`src/server/search_pipeline.cpp`) | Both sites render one `RequiredFilterPredicate` (`src/mysql/required_filter_predicate.h`): `BuildInitialLoadSelectQuery` calls `SqlPredicate` (`src/loader/initial_loader.cpp`), `BinlogFilterEvaluator::CompareFilterValue` calls `Matches` (`src/mysql/binlog_filter_evaluator.cpp`) |
| Operator set | `=` `!=` `>` `>=` `<` `<=` (`src/query/query_parser.h`) | `=` `!=` `<` `>` `<=` `>=` `IS NULL` `IS NOT NULL` (`src/config/config-schema.json`) |
| Type declaration | Not declared per filter; the comparison is dispatched on the type the value was stored as | Declared per filter as `type` (`src/config/config-schema.json`) |

A document excluded by `required_filters` is not in the index, so no query filter can
return it. A document admitted by `required_filters` is subject to query filters
independently — the same column may be used by both, with different results, because the
comparison rules differ (sections 2 and 3).

Both features conjoin their conditions. Query filters are ANDed
(`src/server/search_pipeline.cpp`);
required filters are ANDed both in the emitted SQL (`src/loader/initial_loader.cpp`)
and in the evaluator (`src/mysql/binlog_filter_evaluator.cpp`).

---

## 2. Query filter comparison, per value type

### 2.1 The two paths and how one is chosen

Every call site routes through `ApplyFiltersWithBitmap`
(`src/server/search_pipeline.cpp`). That function selects between two evaluation
strategies:

| Path | Runs when | Mechanism |
|---|---|---|
| **Bitmap** | Every filter in the request uses `=` or `!=` | Serializes the filter value under each type interpretation, ORs the matching per-value Roaring bitmaps, then intersects (`=`) or subtracts (`!=`) that bitmap from the candidate set (`src/server/search_pipeline.cpp`) |
| **Fallback** | Any filter in the request uses `>`, `>=`, `<` or `<=` | Fetches each stored value and compares it with the filter value, dispatching on the stored value's type (`src/server/search_pipeline.cpp`) |

Selection is made by `AllFiltersHaveBitmapSupport`, which returns false as soon as any
filter carries an operator other than `EQ` or `NE`
(`src/server/search_pipeline.cpp`). The choice is per request, not per filter: one
ordering operator anywhere in the `FILTER` clause moves **every** filter in that request
onto the fallback path.

Two further conditions fall back — a null filter index
(`src/server/search_pipeline.cpp`) and a bitmap allocation failure. The first is not
reachable: `DocumentStore`
constructs a `FilterIndex` eagerly (`src/storage/document_store.cpp`) and replaces it
rather than clearing it (`src/storage/document_store_persistence.cpp`), so `GetFilterIndex()`
(`src/storage/document_store_retrieval.cpp`) never returns null.

### 2.2 Filter-value conversion

The filter value arrives as an unstructured string on both surfaces. It is converted once per
request, before the candidate loop, into every interpretation it admits
(`src/server/search_pipeline.cpp`). Each interpretation is produced by `ConvertFilterValue`
(`src/storage/filter_index.cpp`), the same function that converts column values at initial load
and in replication, called with the type named below:

| Interpretation | Converter type | Rule |
|---|---|---|
| `bool` | `boolean` | lowercased, then `1`/`true` is true and `0`/`false` is false; so `TRUE` and `True` are recognized |
| `double` | `double` | `std::from_chars`, whole string must be consumed; `nan` and `inf` are accepted |
| `int64_t` | `bigint` | `std::from_chars`, whole string must be consumed |
| `uint64_t` | `bigint_unsigned` | `std::from_chars`, whole string must be consumed |

An interpretation the converter refuses is absent. Because `std::from_chars` accepts neither a
leading `+` nor leading whitespace, a filter value written as `+5` has no numeric
interpretation.

**A value no interpretation of the stored type admits matches nothing under `=` and keeps
every candidate under `!=`, on both paths.** The ordering operators match nothing. This is the
rule for `FILTER flag = yes`, `FILTER flag != maybe`, `FILTER n != abc` and a negative value
against an unsigned column, whether or not another condition in the request moved it onto the
fallback path.

### 2.3 Comparison rule per stored type

The stored value's type is fixed by the column's configured `type` at ingest
(`src/mysql/rows_parser_filter.cpp`); the query does not declare one. Both paths
therefore behave as a dispatch on the stored type. Every citation in this table is
`src/server/search_pipeline.cpp` unless the cell names another file.

| Stored type | Configured `type` that produces it | Operators | Fallback rule | Bitmap rule (`=`/`!=` only) | Agree? |
|---|---|---|---|---|---|
| `std::string` | `string`, `varchar`, `text` | all six | Byte-exact lexicographic compare of the stored bytes against the raw filter value; no collation, no case folding (`src/server/search_pipeline.cpp`, `src/utils/comparison_utils.h`) | Key is `\x0B` + the raw bytes; equality is byte-exact (`src/storage/filter_index.cpp`, `src/server/search_pipeline.cpp`) | Yes |
| `bool` | `boolean` | all six (`<`/`>` order `false` before `true`) | Compares the stored bool against the bool interpretation; with none, the rule in 2.2 | A bool key is emitted only for a value with a bool interpretation (`1`, `true`, `0`, `false`, any case) | Yes |
| `double` | `double` | all six | `=`/`!=` compare the 64-bit object representation; `<` `>` `<=` `>=` compare numerically; requires the `double` interpretation | Key is `\x0C` + the little-endian object representation; equality is bit-exact (`src/storage/filter_index.cpp`) | Yes — by construction, see 2.4 |
| `TimeValue` | `time` | all six | Compares `seconds` against the `int64_t` interpretation; requires it, so `01:00:00` never matches | A `TimeValue` key is emitted only when the value parses as `int64_t` | Yes |
| `int8_t`, `int16_t`, `int32_t`, `int64_t` | `tinyint`, `smallint`, `int`/`mediumint`, `bigint`, and also `datetime`, `date`, `timestamp` (stored as an epoch-second `int64_t`) | all six | Widens the stored value to `int64_t` and compares against the `int64_t` interpretation; requires it | Keys for `int64_t` and every narrower signed width the value fits are all ORed in | Yes |
| `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t` | `tinyint_unsigned`, `smallint_unsigned`, `int_unsigned`/`mediumint_unsigned`, `bigint_unsigned` | all six | Widens the stored value to `uint64_t` and compares against the `uint64_t` interpretation; requires it, so a negative filter value never matches under `=` or an ordering operator | Keys for `uint64_t` and every narrower unsigned width the value fits are all ORed in | Yes |
| `std::monostate` (SQL NULL) | any | all six | Matches `!=` and nothing else | NULLs are never inserted into the index (`src/storage/filter_index.cpp`), so `=` cannot select them and `!=` cannot subtract them | Yes |

Consequences worth stating explicitly, because they follow from the table rather than from
any per-type special case:

- **Temporal query filters take epoch seconds only.** `datetime`, `date` and `timestamp`
  columns are stored as `int64_t` epoch seconds (`src/mysql/rows_parser_filter.cpp`),
  so a query filter value must parse as an integer. `FILTER created_at >= 2024-01-01` has no
  integer interpretation and matches nothing. This is the opposite of `required_filters`,
  which accepts ISO 8601 (section 3.2).
- **`time` query filters take seconds only**, for the same reason
  (`src/server/search_pipeline.cpp`) — again the opposite of `required_filters`,
  which additionally accepts `HH:MM:SS` (`src/mysql/required_filter_predicate.cpp`).
- **A filter naming an unindexed column returns no rows for `=` and all rows for `!=`.**
  On the fallback path the value reads back as absent and is treated as NULL
  (`src/server/search_pipeline.cpp`); on the bitmap path the column lookup fails
  and the union bitmap stays empty (`src/storage/filter_index.cpp`).
- **`kFilterValueEpsilon` decides nothing anywhere.** It is passed at
  `src/server/search_pipeline.cpp`, but that call is reached only for the four
  ordering operators, and `CompareDoubleValues` ignores its epsilon argument for those
  (`src/utils/comparison_utils.h`). `required_filters` does not use it either
  (section 3.2), so `kFilterValueEpsilon` (`src/utils/constants.h`) is passed but never
  consulted.

### 2.4 DOUBLE equality

`DOUBLE` equality is decided on the **stored object representation**, identically on both
paths, and this is a deliberate consequence of the bitmap index shape.

**Bitmap path.** The index is a map from a serialized value key to a Roaring bitmap
(`src/storage/filter_index.cpp`). A `double` serializes to the tag byte `\x0C`
followed by the little-endian byte image of the `double`
(`src/storage/filter_index.cpp`). Equality is a hash-map lookup on that key
(`src/storage/filter_index.cpp`), so it can only ever select values whose 64 bits are
identical to the filter value's 64 bits.

**Fallback path.** `DoubleValuesIdentical` `memcpy`s both operands into `uint64_t` and
compares the integers (`src/server/search_pipeline.cpp`). It is used for `EQ` and,
negated, for `NE` (`src/server/search_pipeline.cpp`). The ordering operators do
not go through it and compare numerically
(`src/server/search_pipeline.cpp`, `src/utils/comparison_utils.h`).

The two paths therefore agree, and both differ from IEEE 754 `==` in two places:

| Case | IEEE 754 `==` | Both query-filter paths |
|---|---|---|
| stored `-0.0`, filter value `0` or `0.0` | true | **false** — the sign bit differs, so the object representations differ, so the bitmap key differs and `DoubleValuesIdentical` returns false |
| stored `-0.0`, filter value `-0.0` | true | true — `std::from_chars` produces `-0.0` (`src/server/search_pipeline.cpp`) and the representations match |
| stored NaN, filter value NaN with the same payload | false | true — the comparison is on bits, not on IEEE equality |

The NaN row is unreachable through normal ingest. Stored `double` values come from a MySQL
`DOUBLE`, which cannot hold NaN or an infinity, so neither can ever become a stored filter
value. A query filter value of `nan` or `inf` does parse — `std::from_chars` accepts those
spellings (`src/storage/filter_index.cpp`) — so `FILTER x = nan` is a well-formed request that
matches nothing and `FILTER x != nan` is one that matches every non-NULL row.

Ordinary decimal values are affected by the exactness rule whenever the decimal is not
representable: a column storing the result of `0.1 + 0.2` is not matched by the filter value
`0.3`, on either path.

---

## 3. `required_filters` membership, per value type

### 3.1 One declaration, two renderings

The two evaluations are not two implementations. Each `required_filters` entry is resolved
once into a `RequiredFilterPredicate` (`src/mysql/required_filter_predicate.h`), which
holds one **comparison domain** and a target value already expressed in that domain. The SQL conjunct and the C++ verdict are
the only two things that can be obtained from it:

| Obtained by | Produces | Used at |
|---|---|---|
| `SqlPredicate(quoted_column)` (`src/mysql/required_filter_predicate.cpp`) | one `WHERE` conjunct | `BuildInitialLoadSelectQuery` (`src/loader/initial_loader.cpp`) |
| `Matches(value)` (`src/mysql/required_filter_predicate.cpp`) | membership for one decoded row value | `BinlogFilterEvaluator::CompareFilterValue` (`src/mysql/binlog_filter_evaluator.cpp`) |

Both dispatch over the same variant, each through an overload set with one function per
domain and no generic fallback (`src/mysql/required_filter_predicate.cpp`). A domain that can be rendered as SQL but
cannot decide membership, or the reverse, does not compile. Nothing else in the tree renders
a required-filter comparison or evaluates one.

**Resolution is where a filter can be refused.** `Resolve`
(`src/mysql/required_filter_predicate.cpp`) checks the operator against a fixed list, bounds the configured value at 1 MiB
(`src/mysql/required_filter_predicate.h`), and parses the value into the domain. Every
failure is an `Expected` error, and the two surfaces act on it differently:

- **Initial load**: the conjunct cannot be rendered, `BuildInitialLoadSelectQuery` returns an
  empty string, and the load fails with `kStorageSnapshotBuildFailed` — "Invalid identifier
  or required filter value in initial load query" (`src/loader/initial_loader.cpp`).
- **Replication**: the row is rejected (`src/mysql/binlog_filter_evaluator.cpp`).

The postures differ — fail-stop against fail-closed — but the outcome does not: under a
configuration that cannot be resolved, no row enters the index by either route, so the two
cannot disagree about one. A required-filter column missing from the row's filter map also
rejects the row (`src/mysql/binlog_filter_evaluator.cpp`).

Two literal encoders build the right-hand side. `EncodeMySQLStringLiteral` renders bytes as
`_utf8mb4 X'<hex>'`, a byte-exact character literal that keeps the column's implicit
collation (`src/utils/sql_utils.cpp`). `FormatMySQLDoubleLiteral` renders a double in
exponent form with enough digits to round-trip it, which also makes MySQL read it as an
approximate value rather than an exact `DECIMAL` (`src/utils/sql_utils.cpp`).
Numbers are always re-rendered from the parsed target, never copied from the configured
text, so no configured string reaches the server outside a literal encoder.

`IS NULL` and `IS NOT NULL` resolve to a domain that never reads the configured value at all
(`src/mysql/required_filter_predicate.cpp`): the SQL is the bare column followed by
the operator, and the verdict is the column's NULL state. They agree for every type.

### 3.2 Per-type table

`type` accepts exactly the nineteen values enumerated at
`src/config/config-schema.json`, mirrored in `IsSupportedFilterType`
(`src/config/config.cpp`). All nineteen accept all eight operators per the schema,
except `boolean`, which configuration load narrows to `=`, `!=`, `IS NULL` and `IS NOT NULL`
(`src/config/config.cpp`).

**`float`, `enum` and `set` are not filter types.** None of the three appears in the schema
enum, so a configuration naming one fails to load with `kConfigValidationError` (1002) and
the message `instance not found in required enum`. `ValidateFilterType`
(`src/config/config.cpp`) refuses them a second time with `kConfigInvalidValue`
(1004) and states the reason: a `FLOAT` column is rendered to six significant digits in a
result set while a binlog row image carries the whole stored value, and `ENUM`/`SET` labels
are not available from binlog row events without extra metadata, so in either case the
initial load and replication would filter the same row differently. A `DOUBLE` column with
`type: double` is the supported approximate-numeric filter. Both gates apply to the optional
`filters` list as well as to `required_filters` (`src/config/config.cpp`).

Each accepted type maps to one domain, and the domain fixes both renderings. Below, `V` is
the configured value and `` `c` `` the quoted column. Every citation in this table is
`src/mysql/required_filter_predicate.cpp` unless the cell names another file.

| Type | Domain | Initial-load SQL predicate emitted | Binlog evaluator rule |
|---|---|---|---|
| `string`, `varchar`, `text` | bytes | `` CAST(`c` AS BINARY) op _utf8mb4 X'<hex V>' `` | byte-exact lexicographic compare against `V` |
| `tinyint`…`bigint`, and every `_unsigned` width below `bigint_unsigned` | signed 64-bit | `` `c` op <decimal> ``, re-rendered from the parsed target | stored value widened to `int64_t`, `V` parsed as `int64_t` |
| `bigint_unsigned` | unsigned 64-bit | `` `c` op <decimal> `` | `uint64_t` compare, `V` parsed as `uint64_t` |
| `float` | single-precision — unreachable | **Rejected at configuration load** with `kConfigValidationError` (1002) from the schema enum and `kConfigInvalidValue` (1004) from `ValidateFilterType` (`src/config/config.cpp`); the domain is implemented in `Resolve`, `SqlRenderer` and `MembershipTest` and would emit `` `c` op CAST(<double literal> AS FLOAT) ``, but no configuration reaches it | not reached |
| `double` | IEEE double | `` `c` op <double literal> `` | exact `double` compare |
| `boolean` | 1/0 | `` `c` op 1 `` or `` `c` op 0 `` | `V` lowercased, then `1`/`true` → true and `0`/`false` → false |
| `datetime`, `date` | wall clock | bare-column bounds (below) against whole-second literals `_utf8mb4 X'<YYYY-MM-DD HH:MM:SS>'`, `V` resolved to an instant and written back in `mysql.datetime_timezone` | epoch compare against the epoch the column's wall clock was read as |
| `timestamp` | UTC epoch | bare-column bounds against whole-second literals written in UTC | epoch compare |
| `time` | seconds since midnight | bare-column bounds against clock literals `_utf8mb4 X'<[-]HH:MM:SS>'`, `V` resolved to seconds and written back as a clock | seconds compare against the decoded `TimeValue.seconds` |
| any type, `IS NULL` / `IS NOT NULL` | NULL state | `` `c` IS NULL `` / `` `c` IS NOT NULL `` | tests for `std::monostate` |

Two of the domains deliberately do extra work so that the server decides membership the way
the evaluator does:

- **Text is cast to `BINARY`** because the default `utf8mb4_0900_ai_ci` is case-insensitive
  and would admit rows the byte comparison rejects.
- **Temporal comparisons are bounds on the bare column.** The binlog decode keeps whole
  seconds, so with `x` the target in whole seconds the SQL is, from one builder
  (`RenderTruncatedRange`, `src/mysql/required_filter_predicate.cpp`):

  | Operator | SQL |
  |---|---|
  | `=` | `c >= L(x) AND c < L(x+1)` |
  | `!=` | `c < L(x) OR c >= L(x+1)` |
  | `<` / `<=` | `c < L(x)` / `c < L(x+1)` |
  | `>` / `>=` | `c >= L(x+1)` / `c >= L(x)` |

  `L` is the literal for an instant (`datetime`, `date`: wall clock in `mysql.datetime_timezone`;
  `timestamp`: wall clock in UTC, which is how the connection reads a `TIMESTAMP`). A `DATE`
  column compared against a datetime-shaped literal is promoted to midnight rather than the
  literal being truncated to a date. For `time`, whose decode truncates toward zero, a
  non-positive `x` uses `c > L(x-1)` for "at least `x`" and `c <= L(x-1)` for "below `x`", so
  `-10:00:00.500` is seen as `-10:00:00` exactly as the decode sees it. A bound past the end of
  the column's range is the constant it implies (`c IS NOT NULL` or `1 = 0`), and a NULL
  column satisfies no bound.

**The same threshold string means different instants on `datetime` and on `timestamp`.**
This is not a disagreement between the two evaluation sites — they agree with each other for
both types — but between the two types. A `TIMESTAMP` column's value is converted to an epoch
with a fixed `+00:00`, because the connection renders it in UTC; a `DATETIME` column's value
is converted with `mysql.datetime_timezone` (`src/mysql/rows_parser_filter.cpp`). The
configured threshold, however, is resolved with `mysql.datetime_timezone` in both cases
(`src/mysql/required_filter_predicate.cpp`). An ISO 8601 threshold on a `DATETIME` column
is therefore compared wall-clock against wall-clock — the offsets cancel and the setting has
no effect — while the same string on a `TIMESTAMP` column is shifted by
`mysql.datetime_timezone` before being compared against a true UTC epoch. The two coincide
only when `mysql.datetime_timezone` is `+00:00`.

The DDL validator independently requires the configured `type` to match the column's actual
declared type before any of this runs (`src/mysql/ddl_schema_validator.cpp`), so a `double`
filter cannot be pointed at a
`VARCHAR` column. `boolean` is accepted only against `tinyint(1)`
(`src/mysql/ddl_schema_validator.cpp`); `string`/`varchar`/`text` are accepted
against `char`, `varchar` and every `text` width (`IsFilterTypeCompatible`,
`src/mysql/ddl_schema_validator.cpp`). A string filter cannot be placed on a `BINARY`,
`VARBINARY` or `BLOB` column: every configured column, filters included, is refused with
one of those types before the type match runs, because the snapshot and binlog paths do not
share a lossless representation for binary strings (`ValidateConfiguredColumnEncoding`,
`src/mysql/ddl_schema_validator.cpp`).

### 3.3 What neither side can observe

**Trailing spaces on a `CHAR(N)` column are invisible to both.** MySQL strips them when
retrieving a `CHAR` value, so `` CAST(`c` AS BINARY) `` sees the stripped bytes; the binlog
row image is written unpadded for the same reason, so the decoder sees the stripped bytes
too. Measured on MySQL 8.4 for `CHAR(6)` in both `ascii` and `utf8mb4`, holding `'xy '`:
`HEX(c)` and `HEX(CAST(c AS BINARY))` are both `7879`, and the row image decodes to `'xy'`.
The two sides therefore agree, and the shared consequence is that a configured value ending
in a space never matches a `CHAR(N)` row on either surface. `CHAR` columns are reachable: the
DDL validator accepts `char` for the `string`/`varchar`/`text` filter types
(`src/mysql/ddl_schema_validator.cpp`).

### 3.4 A value that fails conversion

When a column's text cannot be converted to the configured type — a `TINYINT(1)` holding `2`,
a zero `TIMESTAMP`, a zero `DATE` or `DATETIME` — both ingest paths log a warning and omit
that filter from the document's filter map (`src/loader/initial_loader.cpp`,
`src/mysql/rows_parser_filter.cpp`). Membership is still decided by the same verdict on both
routes. The initial load's `WHERE` clause has already judged the row; replication reaches that
verdict with `RequiredFilterPredicate::MatchesUndecodedValue`
(`src/mysql/required_filter_predicate.cpp`), called when a required column is absent from a
row's filter map (`src/mysql/binlog_filter_evaluator.cpp`; row images are always `FULL`, so
absence means the value did not decode), given the column's raw text:

| Domain | Verdict for a non-NULL value that did not decode |
|---|---|
| `IS NULL` / `IS NOT NULL` | `IS NOT NULL` holds, `IS NULL` does not |
| `boolean` | the value is neither 0 nor 1, so `!=` holds and `=` does not |
| `timestamp` | the zero timestamp orders before every instant: `!=`, `<` and `<=` hold, the rest do not (its raw text is ordered like a `datetime`) |
| `datetime`, `date` | the server compares a zero or partly zero date as a datetime ordered by its components (`0000-00-00` before every real instant, `2024-00-00` before `2024-01-01`). The verdict applies the same half-open bounds the `WHERE` clause renders for target `x`: `<` is `< L(x)`, `<=` is `< L(x+1)`, `>` is `>= L(x+1)`, `>=` is `>= L(x)`, `=` is both `>= L(x)` and `< L(x+1)`, `!=` is either `< L(x)` or `>= L(x+1)`, where `L` is the whole-second literal. A value ordered between `L(x)` and `L(x+1)` therefore satisfies `<=`, `>=` and `!=` but not `<`, `>` or `=` (`2024-00-00 00:00:00` satisfies `<= 2023-12-31 23:59:59`) |
| every other type | no stored value fails to decode; the row is rejected |

The same row content is therefore in the index after a cold start and after the next update to
that row.

---

## 4. Known divergences

Each entry states both sides with citations. These are descriptions of what the code does,
not a list of intentions.

### 4.1 The initial-load text predicate is not sargable

`` CAST(`c` AS BINARY) `` wraps the column in a function, which prevents MySQL from using an
index on that column (`src/mysql/required_filter_predicate.cpp`). The wrap exists to make the
SQL agree with the evaluator, and it costs the index. See section 5.

### 4.2 Configuration does not check that a value is comparable under its declared type

The configured `value` is validated for presence, for compatibility with `IS NULL`, and for
emptiness — all three in `ParseRequiredFilterConfig` (`src/config/config.cpp`), each
rejection being `kConfigInvalidValue` (1004) — but not against the declared `type`. The
schema constrains `value` only to a string, a number or a boolean
(`src/config/config-schema.json`); it relates it to neither `type` nor `op`. A
filter declared `type: int` with `value: "abc"` loads successfully and fails later, at
`RequiredFilterPredicate::Resolve`, as an aborted initial load
(`src/mysql/required_filter_predicate.cpp`, `src/loader/initial_loader.cpp`).
Replication rejects every row under the same configuration, so the failure is consistent —
but it is reported at first load rather than at configuration parse.

---

## 5. Sargability and cost

An initial load runs one streaming `SELECT` per table
(`src/loader/initial_loader.cpp`). Whether MySQL
can serve its `WHERE` from an index is fixed by the shape of the predicate the domain emits.
Every predicate below is emitted by `src/mysql/required_filter_predicate.cpp`.

| Type | Left-hand side emitted | Right-hand side emitted | Index usable |
|---|---|---|---|
| any type, `IS NULL` / `IS NOT NULL` | bare column | — | Yes |
| `string`, `varchar`, `text` | `` CAST(`c` AS BINARY) `` | character literal | **No** — the column is wrapped in a function |
| `date`, `datetime`, `timestamp` | bare column | character literals, as range bounds | Yes — MySQL folds each constant to the column's temporal type |
| `time` | bare column | clock literals `_utf8mb4 X'<hex [-]HH:MM:SS>'`, as range bounds | Yes — same constant folding |
| all integer types, `double`, `boolean` | bare column | bare numeric literal | Yes |
| `float` | bare column | `CAST(<double literal> AS FLOAT)` | n/a — the domain is implemented but unreachable, because the type is refused at configuration load, so no predicate is ever emitted (§3.2) |

Three further properties bear on the cost of an initial load:

- **The one non-sargable case is the common one.** A `required_filters` entry on a status
  or category `VARCHAR` — the most common shape — forces a full scan of the table. Every
  temporal cutoff is sargable, so the choice of MySQL column type for a load-narrowing filter
  can decide whether the load scans or seeks.
- **The `SELECT` is always ordered by the primary key**
  (`src/loader/initial_loader.cpp`). With a non-sargable `WHERE` this is a scan plus
  either a primary-key-ordered traversal or a filesort of the surviving rows.
- **Only the columns actually needed are selected** — primary key, text source columns,
  required-filter columns and optional-filter columns, deduplicated in that order
  (`src/loader/initial_loader.cpp`) — so a covering index on those columns can serve
  the query where the predicate permits an index at all.

The evaluator has no comparable cost story: it runs per row event over values already
decoded from the event, in configuration order, short-circuiting on the first false
(`src/mysql/binlog_filter_evaluator.cpp`).
