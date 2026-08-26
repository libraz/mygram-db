# Filter Comparison Semantics

This file is normative. It describes the comparison rule that each filter-evaluating code
path in MygramDB applies today, per value type, and records where two paths that decide the
same question reach different answers. Every claim carries a repo-relative `file:line`
citation so the document can be mechanically re-verified against the source.

---

## 1. Two features, two contracts

MygramDB has two independent filtering features. They share a type vocabulary and a
configuration file, and nothing else. Neither one constrains the other.

| | Query filters | `required_filters` |
|---|---|---|
| Surface | `FILTER` clause on TCP, the `filters` object on HTTP | `tables[].required_filters` in the configuration file |
| Question answered | Which of the already-indexed documents does this query return? | Which rows are in the index at all? |
| Evaluated | Per request, in memory | Twice: once as SQL in the initial-load `SELECT`, once in C++ per binlog row event |
| Value comparison sites | `ApplyFilters`, `ApplyFiltersWithBitmap` (`src/server/search_pipeline.cpp:1217`, `src/server/search_pipeline.cpp:1321`) | `BuildInitialLoadSelectQuery`, `BinlogFilterEvaluator::CompareFilterValue` (`src/loader/initial_loader.cpp:563`, `src/mysql/binlog_filter_evaluator.cpp:51`) |
| Operator set | `=` `!=` `>` `>=` `<` `<=` (`src/query/query_parser.h:93-100`) | `=` `!=` `<` `>` `<=` `>=` `IS NULL` `IS NOT NULL` (`src/config/config-schema.json:216-220`) |
| Type declaration | Not declared per filter; the comparison is dispatched on the type the value was stored as | Declared per filter as `type` (`src/config/config-schema.json:202-215`) |

A document excluded by `required_filters` is not in the index, so no query filter can
return it. A document admitted by `required_filters` is subject to query filters
independently — the same column may be used by both, with different results, because the
comparison rules differ (section 4).

Both features conjoin their conditions. Query filters are ANDed
(`src/server/search_pipeline.cpp:1307-1310`, `src/server/search_pipeline.cpp:1340-1354`);
required filters are ANDed both in the emitted SQL (`src/loader/initial_loader.cpp:629-632`)
and in the evaluator (`src/mysql/binlog_filter_evaluator.cpp:36-48`).

---

## 2. Query filter comparison, per value type

### 2.1 The two paths and how one is chosen

Every call site routes through `ApplyFiltersWithBitmap`
(`src/server/search_pipeline.cpp:573`, `src/server/search_pipeline.cpp:944`,
`src/server/search_pipeline.cpp:2275`). That function selects between two evaluation
strategies:

| Path | Runs when | Mechanism |
|---|---|---|
| **Bitmap** | Every filter in the request uses `=` or `!=` | Serializes the filter value under each type interpretation, ORs the matching per-value Roaring bitmaps, then intersects (`=`) or subtracts (`!=`) that bitmap from the candidate set (`src/server/search_pipeline.cpp:1340-1354`) |
| **Fallback** | Any filter in the request uses `>`, `>=`, `<` or `<=` | Fetches each stored value and compares it with the filter value, dispatching on the stored value's type (`src/server/search_pipeline.cpp:1245-1316`) |

Selection is made by `AllFiltersHaveBitmapSupport`, which returns false as soon as any
filter carries an operator other than `EQ` or `NE`
(`src/server/search_pipeline.cpp:1115-1122`), checked at
`src/server/search_pipeline.cpp:1330`. The choice is per request, not per filter: one
ordering operator anywhere in the `FILTER` clause moves **every** filter in that request
onto the fallback path.

Two further conditions fall back — a null filter index
(`src/server/search_pipeline.cpp:1330`) and a bitmap allocation failure
(`src/server/search_pipeline.cpp:1336`, `src/server/search_pipeline.cpp:1343`,
`src/server/search_pipeline.cpp:1349`). The first is not reachable: `DocumentStore`
constructs a `FilterIndex` eagerly (`src/storage/document_store.cpp:82`) and replaces it
rather than clearing it (`src/storage/document_store.cpp:359`,
`src/storage/document_store_persistence.cpp:593`), so `GetFilterIndex()`
(`src/storage/document_store_retrieval.cpp:254-257`) never returns null.

### 2.2 Filter-value parsing

The filter value arrives as an unstructured string on both surfaces. It is parsed once per
request into every interpretation it admits, before the candidate loop
(`src/server/search_pipeline.cpp:1056-1096`):

| Interpretation | Rule | Implementation |
|---|---|---|
| `bool` | true only for the exact byte strings `1` and `true`; every other string, including `TRUE` and `True`, yields false with no failure flag | `src/server/search_pipeline.cpp:1060` |
| `double` | `std::from_chars`, whole string must be consumed; sets `double_valid` | `src/server/search_pipeline.cpp:1066-1073` |
| `int64_t` | `std::from_chars`, whole string must be consumed; sets `int64_valid` | `src/server/search_pipeline.cpp:1076-1083` |
| `uint64_t` | `std::from_chars`, whole string must be consumed; sets `uint64_valid` | `src/server/search_pipeline.cpp:1086-1092` |

Because `std::from_chars` accepts neither a leading `+` nor leading whitespace, a filter
value written as `+5` sets none of the numeric validity flags.

The bool interpretation carries no validity flag. Its default of `false` is what makes the
two paths disagree for boolean columns (section 4.2).

### 2.3 Comparison rule per stored type

The stored value's type is fixed by the column's configured `type` at ingest
(`src/mysql/rows_parser_filter.cpp:36-108`); the query does not declare one. Both paths
therefore behave as a dispatch on the stored type.

| Stored type | Configured `type` that produces it | Operators | Fallback rule | Bitmap rule (`=`/`!=` only) | Agree? |
|---|---|---|---|---|---|
| `std::string` | `string`, `varchar`, `text` | all six | Byte-exact lexicographic compare of the stored bytes against the raw filter value; no collation, no case folding (`src/server/search_pipeline.cpp:1270-1271`, `src/utils/comparison_utils.h:29-42`) | Key is `\x0B` + the raw bytes; equality is byte-exact (`src/storage/filter_index.cpp:224-229`, `src/server/search_pipeline.cpp:1153`) | Yes |
| `bool` | `boolean` | all six (`<`/`>` order `false` before `true`) | Compares the stored bool against the parsed bool, which is `false` for any string outside `{1, true}` (`src/server/search_pipeline.cpp:1272-1274`) | A bool key is emitted only for `1`, `true`, `0`, `false`; any other value contributes no bool key (`src/server/search_pipeline.cpp:1156-1160`) | **No** — see 4.2 |
| `double` | `float`, `double` | all six | `=`/`!=` compare the 64-bit object representation; `<` `>` `<=` `>=` compare numerically (`src/server/search_pipeline.cpp:1275-1286`, `src/server/search_pipeline.cpp:1106-1112`) | Key is `\x0C` + the little-endian object representation; equality is bit-exact (`src/storage/filter_index.cpp:230-235`) | Yes — by construction, see 2.4 |
| `TimeValue` | `time` | all six | Compares `seconds` against the `int64_t` interpretation; requires `int64_valid`, so `01:00:00` never matches (`src/server/search_pipeline.cpp:1287-1291`) | A `TimeValue` key is emitted only when the value parses as `int64_t` (`src/server/search_pipeline.cpp:1181`) | Yes |
| `int8_t`, `int16_t`, `int32_t`, `int64_t` | `tinyint`, `smallint`, `int`/`mediumint`, `bigint`, and also `datetime`, `date`, `timestamp` (stored as an epoch-second `int64_t`) | all six | Widens the stored value to `int64_t` and compares against the `int64_t` interpretation; requires `int64_valid` (`src/server/search_pipeline.cpp:1298-1302`) | Keys for `int64_t` and every narrower signed width the value fits are all ORed in (`src/server/search_pipeline.cpp:1166-1183`) | Yes |
| `uint8_t`, `uint16_t`, `uint32_t`, `uint64_t` | `tinyint_unsigned`, `smallint_unsigned`, `int_unsigned`/`mediumint_unsigned`, `bigint_unsigned` | all six | Widens the stored value to `uint64_t` and compares against the `uint64_t` interpretation; requires `uint64_valid`, so a negative filter value never matches (`src/server/search_pipeline.cpp:1292-1297`) | Keys for `uint64_t` and every narrower unsigned width the value fits are all ORed in (`src/server/search_pipeline.cpp:1186-1201`) | Yes |
| `std::monostate` (SQL NULL) | any | all six | Matches `!=` and nothing else (`src/server/search_pipeline.cpp:1254-1260`) | NULLs are never inserted into the index (`src/storage/filter_index.cpp:26-29`), so `=` cannot select them and `!=` cannot subtract them | Yes |

Consequences worth stating explicitly, because they follow from the table rather than from
any per-type special case:

- **Temporal query filters take epoch seconds only.** `datetime`, `date` and `timestamp`
  columns are stored as `int64_t` epoch seconds (`src/mysql/rows_parser_filter.cpp:83-101`),
  so a query filter value must parse as an integer. `FILTER created_at >= 2024-01-01` sets
  `int64_valid` false and matches nothing. This is the opposite of `required_filters`,
  which accepts ISO 8601 (section 3.2).
- **`time` query filters take seconds only**, for the same reason
  (`src/server/search_pipeline.cpp:1287-1291`) — again the opposite of `required_filters`,
  which additionally accepts `HH:MM:SS` (`src/mysql/binlog_filter_evaluator.cpp:271-287`).
- **A filter naming an unindexed column returns no rows for `=` and all rows for `!=`.**
  On the fallback path the value reads back as absent and is treated as NULL
  (`src/server/search_pipeline.cpp:1254-1260`); on the bitmap path the column lookup fails
  and the union bitmap stays empty (`src/storage/filter_index.cpp:143-146`).
- **`kFilterValueEpsilon` does not participate in query filtering.** It is passed at
  `src/server/search_pipeline.cpp:1285-1286`, but that call is reached only for the four
  ordering operators, and `CompareDoubleValues` ignores its epsilon argument for those
  (`src/utils/comparison_utils.h:61-68`). The constant is live only in the binlog evaluator
  (`src/mysql/binlog_filter_evaluator.cpp:136`).

### 2.4 DOUBLE equality

`DOUBLE` equality is decided on the **stored object representation**, identically on both
paths, and this is a deliberate consequence of the bitmap index shape.

**Bitmap path.** The index is a map from a serialized value key to a Roaring bitmap
(`src/storage/filter_index.cpp:23-51`). A `double` serializes to the tag byte `\x0C`
followed by the little-endian byte image of the `double`
(`src/storage/filter_index.cpp:230-235`). Equality is a hash-map lookup on that key
(`src/storage/filter_index.cpp:137-152`), so it can only ever select values whose 64 bits are
identical to the filter value's 64 bits.

**Fallback path.** `DoubleValuesIdentical` `memcpy`s both operands into `uint64_t` and
compares the integers (`src/server/search_pipeline.cpp:1106-1112`). It is used for `EQ` and,
negated, for `NE` (`src/server/search_pipeline.cpp:1279-1284`). The ordering operators do
not go through it and compare numerically
(`src/server/search_pipeline.cpp:1285-1286`, `src/utils/comparison_utils.h:61-68`).

The two paths therefore agree, and both differ from IEEE 754 `==` in two places:

| Case | IEEE 754 `==` | Both query-filter paths |
|---|---|---|
| stored `-0.0`, filter value `0` or `0.0` | true | **false** — the sign bit differs, so the object representations differ, so the bitmap key differs and `DoubleValuesIdentical` returns false |
| stored `-0.0`, filter value `-0.0` | true | true — `std::from_chars` produces `-0.0` (`src/server/search_pipeline.cpp:1068`) and the representations match |
| stored NaN, filter value NaN with the same payload | false | true — the comparison is on bits, not on IEEE equality |

The NaN row is unreachable through normal ingest. Filter values are converted by
`ParseNumeric<double>`, which rejects any non-finite result
(`src/utils/numeric_parse.h:60-62`), so neither NaN nor an infinity can ever become a stored
filter value. A query filter value of `nan` or `inf` does parse — `std::from_chars` accepts
those spellings and sets `double_valid` (`src/server/search_pipeline.cpp:1066-1073`) — so
`FILTER x = nan` is a well-formed request that matches nothing and `FILTER x != nan` is one
that matches every non-NULL row.

Ordinary decimal values are affected by the exactness rule whenever the decimal is not
representable: a column storing the result of `0.1 + 0.2` is not matched by the filter value
`0.3`, on either path.

---

## 3. `required_filters` membership, per value type

### 3.1 Shape of the two evaluations

**Initial load.** `BuildInitialLoadSelectQuery` emits one `WHERE` conjunct per required
filter (`src/loader/initial_loader.cpp:625-724`). The operator is re-validated against a
fixed allow list as it becomes SQL syntax (`src/loader/initial_loader.cpp:641-649`,
`src/loader/initial_loader.cpp:553-557`). If any conjunct cannot be rendered the function
returns an empty string and the load fails with
`kStorageSnapshotBuildFailed` — "Invalid identifier or required filter value in initial load
query" (`src/loader/initial_loader.cpp:249-253`).

Two literal encoders are involved. `EncodeMySQLStringLiteral` renders a value as
`_utf8mb4 X'<hex>'`, a byte-exact character literal that keeps the column's implicit
collation (`src/utils/sql_utils.cpp:50-69`). Numeric values are written bare after passing
`IsSafeSQLNumericLiteral`, which accepts an optional leading `+`/`-`, decimal digits and at
most one `.` — and nothing else (`src/loader/initial_loader.cpp:523-551`).

**Binlog.** `BinlogFilterEvaluator::CompareFilterValue`
(`src/mysql/binlog_filter_evaluator.cpp:51-269`) dispatches on the **decoded variant type**
of the row's column value, not on the configured `type`. The configured `type` is consulted
only to disambiguate `int64_t` (temporal versus integer,
`src/mysql/binlog_filter_evaluator.cpp:84`) and `uint64_t`
(`src/mysql/binlog_filter_evaluator.cpp:148`). The variant is itself chosen from the
configured `type` at decode time (`src/mysql/rows_parser_filter.cpp:36-108`), so the two
always correspond.

A required-filter column missing from the row's filter map rejects the row
(`src/mysql/binlog_filter_evaluator.cpp:38-46`); every parse failure of the configured value
also rejects the row (`src/mysql/binlog_filter_evaluator.cpp:113`,
`src/mysql/binlog_filter_evaluator.cpp:133`,
`src/mysql/binlog_filter_evaluator.cpp:178`,
`src/mysql/binlog_filter_evaluator.cpp:196`). The evaluator is fail-closed throughout; the
SQL side is fail-stop, aborting the whole load rather than dropping a row.

`IS NULL` and `IS NOT NULL` bypass all of the below on both sides: the SQL is the bare
column followed by the operator (`src/loader/initial_loader.cpp:651-654`) and the evaluator
tests for `std::monostate` (`src/mysql/binlog_filter_evaluator.cpp:69-74`). They agree for
every type.

### 3.2 Per-type table

`type` accepts exactly the twenty values enumerated at
`src/config/config-schema.json:205-214`, mirrored in `IsSupportedFilterType`. `enum` and
`set` are rejected with a dedicated message. All twenty accept all eight operators per the
schema, except `boolean`, which configuration load narrows to `=`, `!=`, `IS NULL` and
`IS NOT NULL`.

Below, *SQL predicate* shows what is emitted for a non-NULL operator; `V` is the configured
value string and `` `c` `` the quoted column.

| Type | Operators accepted | Initial-load SQL predicate emitted | Binlog evaluator rule | Agree? |
|---|---|---|---|---|
| `string` | all eight | `` CAST(`c` AS BINARY) op _utf8mb4 X'<hex V>' `` (`src/loader/initial_loader.cpp:659-660`, `src/loader/initial_loader.cpp:707-708`) | Byte-exact lexicographic compare of the decoded string against `V` (`src/mysql/binlog_filter_evaluator.cpp:138-143`) | Yes for `VARCHAR`/`TEXT`; **no** for `CHAR(N)` — see 4.4 |
| `varchar` | all eight | as `string` | as `string` | as `string` |
| `text` | all eight | as `string` | as `string` | as `string` |
| `tinyint` | all eight | `` `c` op V `` (`src/loader/initial_loader.cpp:663-665`, `src/loader/initial_loader.cpp:709-721`) | Widened to `int64_t`, `V` parsed with `from_chars` as `int64_t` (`src/mysql/binlog_filter_evaluator.cpp:225-260`) | Yes |
| `tinyint_unsigned` | all eight | as `tinyint` | as `tinyint` — the `uint8_t` value is widened to `int64_t`, losslessly (`src/mysql/binlog_filter_evaluator.cpp:241-243`) | Yes |
| `smallint`, `smallint_unsigned` | all eight | as `tinyint` | as `tinyint` | Yes |
| `mediumint`, `mediumint_unsigned` | all eight | as `tinyint` | as `tinyint` | Yes |
| `int`, `int_unsigned` | all eight | as `tinyint` | as `tinyint` | Yes |
| `bigint` | all eight | as `tinyint` | `int64_t` compare, `V` parsed as `int64_t` (`src/mysql/binlog_filter_evaluator.cpp:100-116`) | Yes |
| `bigint_unsigned` | all eight | as `tinyint` | `uint64_t` compare, `V` parsed as `uint64_t` (`src/mysql/binlog_filter_evaluator.cpp:145-164`) | Yes |
| `float` | all eight | `` `c` op V `` as a bare decimal literal, evaluated by MySQL's own numeric comparison (`src/loader/initial_loader.cpp:709-721`) | `\|stored - V\| < 1e-9` for `=`, `>= 1e-9` for `!=`; ordering operators compare numerically (`src/mysql/binlog_filter_evaluator.cpp:118-136`, `src/utils/comparison_utils.h:56-68`, `src/utils/constants.h:104`) | **No** — see 4.1 |
| `double` | all eight | as `float` | as `float` | **No** — see 4.1 |
| `boolean` | `=`, `!=`, `IS NULL`, `IS NOT NULL` | `` `c` op V `` via the numeric-literal branch; `V` must satisfy `IsSafeSQLNumericLiteral` (`src/loader/initial_loader.cpp:709-721`) | `V` lowercased, then `1`/`true` → true and `0`/`false` → false; anything else rejects the row (`src/mysql/binlog_filter_evaluator.cpp:202-224`) | **No** for `V` = `"true"`/`"false"` — see 4.3 |
| `date` | all eight | `` `c` op _utf8mb4 X'<hex V>' `` — `V` unmodified as a character literal (`src/loader/initial_loader.cpp:663-665`, `src/loader/initial_loader.cpp:668-671`, `src/loader/initial_loader.cpp:707-708`) | `V` parsed by `ParseDatetimeValue`, which accepts epoch seconds **or** ISO 8601 (`src/mysql/binlog_filter_evaluator.cpp:84-97`, `src/utils/datetime_converter.cpp:446-461`) | **No** for an epoch-second `V` — see 4.5 |
| `datetime` | all eight | as `date` | as `date`, interpreting an ISO 8601 `V` in `mysql.datetime_timezone` (`src/mysql/binlog_filter_evaluator.cpp:85`) | as `date` |
| `timestamp` | all eight | `` UNIX_TIMESTAMP(`c`) op <epoch> ``, where `<epoch>` is `V` resolved through `ParseDatetimeValue` in `mysql.datetime_timezone` at query-build time (`src/loader/initial_loader.cpp:661-662`, `src/loader/initial_loader.cpp:676-689`) | `V` resolved by the same `ParseDatetimeValue` call in the same timezone, compared against the UTC epoch the row decoded to (`src/mysql/binlog_filter_evaluator.cpp:84-97`) | Yes |
| `time` | all eight | `` `c` op _utf8mb4 X'<hex HH:MM:SS>' ``, where the literal is `V` resolved to seconds and re-rendered as a clock literal (`src/loader/initial_loader.cpp:690-706`, `src/utils/sql_utils.cpp:71-97`) | `V` resolved to seconds by the same `ParseTimeFilterSeconds` (accepts bare seconds or `HH:MM:SS`), compared against the decoded `TimeValue.seconds` (`src/mysql/binlog_filter_evaluator.cpp:183-200`, `src/mysql/binlog_filter_evaluator.cpp:271-287`) | Yes |

`timestamp` and `time` are the two types where the SQL side deliberately does extra work —
reducing the column to a UTC epoch and re-rendering the value as a clock literal
respectively — specifically so the server decides membership the way the evaluator does.

**The same threshold string means different instants on `datetime` and on `timestamp`.**
This is not a disagreement between the two evaluation sites — they agree with each other for
both types — but between the two types. A `TIMESTAMP` column's value is converted to an
epoch with a fixed `+00:00`, because the connection renders it in UTC; a `DATETIME` column's
value is converted with `mysql.datetime_timezone`
(`src/mysql/rows_parser_filter.cpp:83-101`). The configured threshold, however, is resolved
with `mysql.datetime_timezone` in both cases
(`src/mysql/binlog_filter_evaluator.cpp:85`, `src/loader/initial_loader.cpp:677`). An ISO
8601 threshold on a `DATETIME` column is therefore compared wall-clock against wall-clock —
the offsets cancel and the setting has no effect — while the same string on a `TIMESTAMP`
column is shifted by `mysql.datetime_timezone` before being compared against a true UTC
epoch. The two coincide only when `mysql.datetime_timezone` is `+00:00`.

The DDL validator independently requires the configured `type` to match the column's actual
declared type before any of this runs (`src/mysql/ddl_schema_validator.cpp:87-138`,
`src/mysql/ddl_schema_validator.cpp:241-248`), so a `double` filter cannot be pointed at a
`VARCHAR` column. `boolean` is accepted only against `tinyint(1)`
(`src/mysql/ddl_schema_validator.cpp:118-121`); `string`/`varchar`/`text` are accepted
against `char`, `varchar` and every `text` width
(`src/mysql/ddl_schema_validator.cpp:122-128`).

---

## 4. Known divergences

Each entry states both sides with citations. These are descriptions of what the code does,
not a list of intentions.

### 4.1 `float` and `double` required filters use different equality on each side

The binlog evaluator decides `=` with a fixed absolute tolerance: `CompareDoubleValues` is
called with `kFilterValueEpsilon`, defined as `1e-9`
(`src/mysql/binlog_filter_evaluator.cpp:136`, `src/utils/constants.h:104`), and returns
`std::abs(lhs - rhs) < epsilon` for `=` and `std::abs(lhs - rhs) >= epsilon` for `!=`
(`src/utils/comparison_utils.h:56-60`).

The initial-load SQL emits a bare decimal literal
(`src/loader/initial_loader.cpp:709-721`) and MySQL evaluates the equality itself, with no
tolerance.

A row whose column differs from the configured value by less than `1e-9` but is not equal to
it is therefore excluded by the initial load and admitted by replication. The value is a
fixed *absolute* epsilon, so its effect scales with the column's magnitude: for values near
`1e9` it is smaller than one unit in the last place and never fires; for values near zero it
covers a wide neighbourhood.

The same column reached through a query `FILTER` uses neither rule — query-filter equality
is on the object representation (section 2.4), which is stricter than both. All three sites
can disagree on the same `(column, value)` pair.

### 4.2 Boolean query filters: the two query paths disagree on unrecognized value text

The fallback path parses the filter value into a bool with no validity flag: `bool_val` is
true only for the exact strings `1` and `true`, and silently `false` for everything else
(`src/server/search_pipeline.cpp:1060`). It then compares that against the stored bool
(`src/server/search_pipeline.cpp:1272-1274`).

The bitmap path emits a bool key only for `1`, `true`, `0` and `false`
(`src/server/search_pipeline.cpp:1156-1160`); for any other value no bool key enters the
union, and no stored boolean can be selected.

For a filter value outside `{1, true, 0, false}` — for example `TRUE`, `True`, `yes` or `2`:

| Request | Path taken | Result |
|---|---|---|
| `FILTER flag = TRUE` | bitmap | matches nothing |
| `FILTER flag = TRUE AND score > 3` | fallback | matches rows where `flag` is **false** |
| `FILTER flag != TRUE` | bitmap | matches every candidate |
| `FILTER flag != TRUE AND score > 3` | fallback | matches rows where `flag` is **true** |

The comparison is byte-exact and case-sensitive in both places, so `TRUE` and `True` are
both outside the recognized set. Note that the required-filter evaluator lowercases before
matching the same token set (`src/mysql/binlog_filter_evaluator.cpp:203`), and so does the
ingest converter (`src/mysql/rows_parser_filter.cpp:73-82`) — the query side is the only one
of the three that is case-sensitive.

### 4.3 `boolean` required filter written as a quoted string aborts the initial load

`boolean` is not routed through `EncodeMySQLStringLiteral`; `requires_quoting` covers only
`string`, `varchar`, `text`, `datetime` and `date`
(`src/loader/initial_loader.cpp:668-671`), so a `boolean` value reaches the numeric-literal
branch and must satisfy `IsSafeSQLNumericLiteral`
(`src/loader/initial_loader.cpp:709-721`), which admits only an optional sign, digits and
one decimal point (`src/loader/initial_loader.cpp:523-551`). The string `true` fails, the
query builder returns an empty string, and the load aborts
(`src/loader/initial_loader.cpp:249-253`).

The binlog evaluator accepts the same value, case-insensitively
(`src/mysql/binlog_filter_evaluator.cpp:202-217`).

The divergence is reachable only when the value is written as a **quoted string**. A YAML or
JSON boolean literal is normalized to `1` or `0` at configuration load
(`src/config/config.cpp:652-653`), and the schema permits a bare boolean there
(`src/config/config-schema.json:221-224`), so `value: true` produces the predicate
`` `c` = 1 `` and works on both sides. Only `value: "true"` diverges.

### 4.4 `CHAR(N)` cannot express a trailing-space difference on the SQL side

The SQL predicate wraps the column in `CAST(... AS BINARY)`
(`src/loader/initial_loader.cpp:659-660`) precisely to force a byte comparison rather than a
collation-aware one — the default `utf8mb4_0900_ai_ci` is case-insensitive and would admit
rows the binlog path later rejects (`src/loader/initial_loader.cpp:63-68`). The cast
operates on the value MySQL retrieves, and MySQL strips trailing spaces when retrieving from
a `CHAR(N)` column, so a configured value that differs from the stored value only in
trailing spaces cannot be distinguished by the emitted predicate however byte-exact the
comparison is.

`CHAR` columns are reachable: the DDL validator accepts `char` for the
`string`/`varchar`/`text` filter types (`src/mysql/ddl_schema_validator.cpp:122-128`).

### 4.5 `datetime` and `date`: the binlog side accepts an epoch, the SQL side does not

`ParseDatetimeValue` tries an epoch-second reading first and only then an ISO 8601 one
(`src/utils/datetime_converter.cpp:446-461`), and the evaluator calls it for `datetime`,
`date` and `timestamp` (`src/mysql/binlog_filter_evaluator.cpp:84-97`). A configured value
of `1700000000` is therefore a valid instant on the binlog side.

The SQL side treats `datetime` and `date` as quoted types and passes the configured value
through unmodified as a character literal
(`src/loader/initial_loader.cpp:668-671`, `src/loader/initial_loader.cpp:707-708`), so the
emitted predicate compares a `DATETIME` column against the character string `'1700000000'`.
MySQL has no epoch-string reading for that comparison. `timestamp` does not have this
problem, because it resolves the value to an epoch at query-build time and compares against
`UNIX_TIMESTAMP()` (`src/loader/initial_loader.cpp:661-662`,
`src/loader/initial_loader.cpp:676-689`).

The reverse direction agrees: an ISO 8601 value is a valid literal for MySQL and is parsed
by `ParseDatetimeValue` on the binlog side.

### 4.6 The initial-load text predicate is not sargable

`` CAST(`c` AS BINARY) `` wraps the column in a function, which prevents MySQL from using an
index on that column (`src/loader/initial_loader.cpp:659-660`). The same applies to
`` UNIX_TIMESTAMP(`c`) `` for `timestamp` (`src/loader/initial_loader.cpp:661-662`). Both wraps
exist to make the SQL agree with the binlog evaluator, and both cost the index. See
section 5.

### 4.7 A value that fails conversion is admitted by the initial load and rejected by replication

When a column's text cannot be converted to the configured type, both ingest paths log a
warning and omit that filter from the document's filter map
(`src/loader/initial_loader.cpp:828-839`, `src/mysql/rows_parser_filter.cpp:126-136`). The
consequences then differ:

- Initial load: membership was already decided by the SQL `WHERE`, which the row passed. The
  document is indexed, with that filter column absent.
- Replication: `EvaluateRequiredFilters` looks the column up in the filter map, does not
  find it, logs `required_filter_column_not_found` and rejects the row
  (`src/mysql/binlog_filter_evaluator.cpp:38-46`).

The same row content is therefore in the index after a cold start and removed from it after
the next update to that row.

### 4.8 Configuration does not check that a numeric-typed value is numeric

The configured `value` is validated for presence, for compatibility with `IS NULL`, and for
emptiness (`src/config/config-schema.json:221-224`), but not against the declared `type`. A
filter declared `type: int` with `value: "abc"` loads successfully and fails later, at
`BuildInitialLoadSelectQuery`, as an aborted initial load
(`src/loader/initial_loader.cpp:709-721`, `src/loader/initial_loader.cpp:249-253`).

---

## 5. Sargability and cost

An initial load runs one streaming `SELECT` per table
(`src/loader/initial_loader.cpp:248`, `src/loader/initial_loader.cpp:259`). Whether MySQL
can serve its `WHERE` from an index is fixed by the shape of the predicate the query builder
emits, and that shape is decided by the filter's `type`.

| Type | Left-hand side emitted | Right-hand side emitted | Index usable |
|---|---|---|---|
| any type, `IS NULL` / `IS NOT NULL` | bare column (`src/loader/initial_loader.cpp:651-654`) | — | Yes |
| `string`, `varchar`, `text` | `` CAST(`c` AS BINARY) `` (`src/loader/initial_loader.cpp:659-660`) | character literal (`src/loader/initial_loader.cpp:707-708`) | **No** — the column is wrapped in a function |
| `timestamp` | `` UNIX_TIMESTAMP(`c`) `` (`src/loader/initial_loader.cpp:661-662`) | integer literal (`src/loader/initial_loader.cpp:676-689`) | **No** — the column is wrapped in a function |
| `date`, `datetime` | bare column (`src/loader/initial_loader.cpp:663-665`) | character literal (`src/loader/initial_loader.cpp:707-708`) | Yes — MySQL folds the constant to the column's temporal type |
| `time` | bare column (`src/loader/initial_loader.cpp:663-665`) | character literal `'HH:MM:SS'` (`src/loader/initial_loader.cpp:690-706`) | Yes — same constant folding |
| all integer types, `float`, `double`, `boolean` | bare column (`src/loader/initial_loader.cpp:663-665`) | bare numeric literal (`src/loader/initial_loader.cpp:709-721`) | Yes |

Three further properties bear on the cost of an initial load:

- **The two non-sargable cases are the common ones.** A `required_filters` entry on a status
  or category `VARCHAR` — the most common shape — forces a full scan of the table, and so
  does any `timestamp` cutoff. A `datetime` column carrying the same cutoff is sargable, so
  the choice of MySQL column type for a load-narrowing filter decides whether the load scans
  or seeks.
- **The `SELECT` is always ordered by the primary key**
  (`src/loader/initial_loader.cpp:727-731`). With a non-sargable `WHERE` this is a scan plus
  either a primary-key-ordered traversal or a filesort of the surviving rows.
- **Only the columns actually needed are selected** — primary key, text source columns,
  required-filter columns and optional-filter columns, deduplicated in that order
  (`src/loader/initial_loader.cpp:570-616`) — so a covering index on those columns can serve
  the query where the predicate permits an index at all.

The binlog evaluator has no comparable cost story: it runs per row event over values already
decoded from the event, in configuration order, short-circuiting on the first false
(`src/mysql/binlog_filter_evaluator.cpp:36-48`).
