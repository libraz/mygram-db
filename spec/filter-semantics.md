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
comparison rules differ (section 4).

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

### 2.2 Filter-value parsing

The filter value arrives as an unstructured string on both surfaces. It is parsed once per
request into every interpretation it admits, before the candidate loop
(`src/server/search_pipeline.cpp`):

| Interpretation | Rule |
|---|---|
| `bool` | true only for the exact byte strings `1` and `true`; every other string, including `TRUE` and `True`, yields false with no failure flag |
| `double` | `std::from_chars`, whole string must be consumed; sets `double_valid` |
| `int64_t` | `std::from_chars`, whole string must be consumed; sets `int64_valid` |
| `uint64_t` | `std::from_chars`, whole string must be consumed; sets `uint64_valid` |

All four interpretations are produced in `src/server/search_pipeline.cpp`.

Because `std::from_chars` accepts neither a leading `+` nor leading whitespace, a filter
value written as `+5` sets none of the numeric validity flags.

The bool interpretation carries no validity flag. Its default of `false` is what makes the
two paths disagree for boolean columns (section 4.1).

### 2.3 Comparison rule per stored type

The stored value's type is fixed by the column's configured `type` at ingest
(`src/mysql/rows_parser_filter.cpp`); the query does not declare one. Both paths
therefore behave as a dispatch on the stored type. Every citation in this table is
`src/server/search_pipeline.cpp` unless the cell names another file.

| Stored type | Configured `type` that produces it | Operators | Fallback rule | Bitmap rule (`=`/`!=` only) | Agree? |
|---|---|---|---|---|---|
| `std::string` | `string`, `varchar`, `text` | all six | Byte-exact lexicographic compare of the stored bytes against the raw filter value; no collation, no case folding (`src/server/search_pipeline.cpp`, `src/utils/comparison_utils.h`) | Key is `\x0B` + the raw bytes; equality is byte-exact (`src/storage/filter_index.cpp`, `src/server/search_pipeline.cpp`) | Yes |
| `bool` | `boolean` | all six (`<`/`>` order `false` before `true`) | Compares the stored bool against the parsed bool, which is `false` for any string outside `{1, true}` | A bool key is emitted only for `1`, `true`, `0`, `false`; any other value contributes no bool key | **No** — see 4.1 |
| `double` | `double` | all six | `=`/`!=` compare the 64-bit object representation; `<` `>` `<=` `>=` compare numerically | Key is `\x0C` + the little-endian object representation; equality is bit-exact (`src/storage/filter_index.cpp`) | Yes — by construction, see 2.4 |
| `TimeValue` | `time` | all six | Compares `seconds` against the `int64_t` interpretation; requires `int64_valid`, so `01:00:00` never matches | A `TimeValue` key is emitted only when the value parses as `int64_t` | Yes |
| `int8_t`, `int16_t`, `int32_t`, `int64_t` | `tinyint`, `smallint`, `int`/`mediumint`, `bigint`, and also `datetime`, `date`, `timestamp` (stored as an epoch-second `int64_t`) | all six | Widens the stored value to `int64_t` and compares against the `int64_t` interpretation; requires `int64_valid` | Keys for `int64_t` and every narrower signed width the value fits are all ORed in | Yes |
| `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t` | `tinyint_unsigned`, `smallint_unsigned`, `int_unsigned`/`mediumint_unsigned`, `bigint_unsigned` | all six | Widens the stored value to `uint64_t` and compares against the `uint64_t` interpretation; requires `uint64_valid`, so a negative filter value never matches | Keys for `uint64_t` and every narrower unsigned width the value fits are all ORed in | Yes |
| `std::monostate` (SQL NULL) | any | all six | Matches `!=` and nothing else | NULLs are never inserted into the index (`src/storage/filter_index.cpp`), so `=` cannot select them and `!=` cannot subtract them | Yes |

Consequences worth stating explicitly, because they follow from the table rather than from
any per-type special case:

- **Temporal query filters take epoch seconds only.** `datetime`, `date` and `timestamp`
  columns are stored as `int64_t` epoch seconds (`src/mysql/rows_parser_filter.cpp`),
  so a query filter value must parse as an integer. `FILTER created_at >= 2024-01-01` sets
  `int64_valid` false and matches nothing. This is the opposite of `required_filters`,
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
  (section 3.2), so `src/utils/constants.h` is now passed but never consulted.

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

The NaN row is unreachable through normal ingest. Filter values are converted by
`ParseNumeric<double>`, which rejects any non-finite result
(`src/utils/numeric_parse.h`), so neither NaN nor an infinity can ever become a stored
filter value. A query filter value of `nan` or `inf` does parse — `std::from_chars` accepts
those spellings and sets `double_valid` (`src/server/search_pipeline.cpp`) — so
`FILTER x = nan` is a well-formed request that matches nothing and `FILTER x != nan` is one
that matches every non-NULL row.

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
| `float` | none | **Rejected at configuration load** with `kConfigValidationError` (1002) from the schema enum and `kConfigInvalidValue` (1004) from `ValidateFilterType` (`src/config/config.cpp`); no predicate is resolved and no SQL is emitted | not reached |
| `double` | IEEE double | `` `c` op <double literal> `` | exact `double` compare |
| `boolean` | 1/0 | `` `c` op 1 `` or `` `c` op 0 `` | `V` lowercased, then `1`/`true` → true and `0`/`false` → false |
| `datetime`, `date` | wall clock | `` `c` op _utf8mb4 X'<YYYY-MM-DD HH:MM:SS>' ``, where the literal is `V` resolved to an instant and written back in `mysql.datetime_timezone` | epoch compare against the epoch the column's wall clock was read as |
| `timestamp` | UTC epoch | `` UNIX_TIMESTAMP(`c`) op <epoch> `` | epoch compare |
| `time` | seconds since midnight | `` `c` op _utf8mb4 X'<[-]HH:MM:SS>' ``, where the literal is `V` resolved to seconds and written back as a clock | seconds compare against the decoded `TimeValue.seconds` |
| any type, `IS NULL` / `IS NOT NULL` | NULL state | `` `c` IS NULL `` / `` `c` IS NOT NULL `` | tests for `std::monostate` |

Three of the domains deliberately do extra work so that the server decides membership the way
the evaluator does. Each rests on a measured MySQL 8.4 result:

- **Text is cast to `BINARY`** because the default `utf8mb4_0900_ai_ci` is case-insensitive
  and would admit rows the byte comparison rejects.
- **`TIMESTAMP` is reduced to `UNIX_TIMESTAMP()`** because the connection renders it in UTC
  and the binlog decodes it to the same UTC epoch.
- **A `datetime`/`date` target is re-rendered as a wall clock** because a bare epoch is not a
  datetime to MySQL. Written back in `mysql.datetime_timezone`, the offsets cancel on the SQL
  side exactly as they do in C++. A `DATE` column compared against a datetime-shaped literal
  is promoted to midnight rather than the literal being truncated to a date, so the
  comparison is on the same instants the evaluator compares.

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
against `char`, `varchar` and every `text` width.

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

---

## 4. Known divergences

Each entry states both sides with citations. These are descriptions of what the code does,
not a list of intentions.

### 4.1 Boolean query filters: the two query paths disagree on unrecognized value text

The fallback path parses the filter value into a bool with no validity flag: `bool_val` is
true only for the exact strings `1` and `true`, and silently `false` for everything else
(`src/server/search_pipeline.cpp`). It then compares that against the stored bool
(`src/server/search_pipeline.cpp`).

The bitmap path emits a bool key only for `1`, `true`, `0` and `false`
(`src/server/search_pipeline.cpp`); for any other value no bool key enters the
union, and no stored boolean can be selected.

For a filter value outside `{1, true, 0, false}` — for example `TRUE`, `True`, `yes` or `2`:

| Request | Path taken | Result |
|---|---|---|
| `FILTER flag = TRUE` | bitmap | matches nothing |
| `FILTER flag = TRUE AND score > 3` | fallback | matches rows where `flag` is **false** |
| `FILTER flag != TRUE` | bitmap | matches every candidate |
| `FILTER flag != TRUE AND score > 3` | fallback | matches rows where `flag` is **true** |

The comparison is byte-exact and case-sensitive in both places, so `TRUE` and `True` are
both outside the recognized set. Note that required-filter resolution lowercases before
matching the same token set (`src/mysql/required_filter_predicate.cpp`), and so does the
ingest converter (`src/mysql/rows_parser_filter.cpp`) — the query side is the only one
of the three that is case-sensitive.

### 4.2 The initial-load text predicate is not sargable

`` CAST(`c` AS BINARY) `` wraps the column in a function, which prevents MySQL from using an
index on that column (`src/mysql/required_filter_predicate.cpp`). The same applies to
`` UNIX_TIMESTAMP(`c`) `` for `timestamp` (`src/mysql/required_filter_predicate.cpp`).
Both wraps exist to make the SQL agree with the evaluator, and both cost the index. See
section 5.

### 4.3 A value that fails conversion is admitted by the initial load and rejected by replication

When a column's text cannot be converted to the configured type, both ingest paths log a
warning and omit that filter from the document's filter map
(`src/loader/initial_loader.cpp`, `src/mysql/rows_parser_filter.cpp`). The
consequences then differ:

- Initial load: membership was already decided by the SQL `WHERE`, which the row passed. The
  document is indexed, with that filter column absent.
- Replication: `EvaluateRequiredFilters` looks the column up in the filter map, does not
  find it, logs `required_filter_column_not_found` and rejects the row
  (`src/mysql/binlog_filter_evaluator.cpp`).

The same row content is therefore in the index after a cold start and removed from it after
the next update to that row. This is a disagreement about the *stored* value, not about the
predicate: the predicate is never reached, because the value never became one.

### 4.4 Configuration does not check that a value is comparable under its declared type

The configured `value` is validated for presence, for compatibility with `IS NULL`, and for
emptiness (`src/config/config-schema.json`), but not against the declared `type`. A
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
| `timestamp` | `` UNIX_TIMESTAMP(`c`) `` | integer literal | **No** — the column is wrapped in a function |
| `date`, `datetime` | bare column | character literal | Yes — MySQL folds the constant to the column's temporal type |
| `time` | bare column | character literal `'HH:MM:SS'` | Yes — same constant folding |
| all integer types, `double`, `boolean` | bare column | bare numeric literal | Yes |
| `float` | — | — | n/a — the type is refused at configuration load, so no predicate is ever emitted (§3.2) |

Three further properties bear on the cost of an initial load:

- **The two non-sargable cases are the common ones.** A `required_filters` entry on a status
  or category `VARCHAR` — the most common shape — forces a full scan of the table, and so
  does any `timestamp` cutoff. A `datetime` column carrying the same cutoff is sargable, so
  the choice of MySQL column type for a load-narrowing filter decides whether the load scans
  or seeks.
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
