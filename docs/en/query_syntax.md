# Query Syntax Guide

MygramDB supports a rich boolean query syntax for complex text search operations.

## Table of Contents

- [Basic Syntax](#basic-syntax)
- [Boolean Operators](#boolean-operators)
- [Complex Boolean Queries](#complex-boolean-queries)
- [Operator Precedence](#operator-precedence)
- [Quoted Phrases](#quoted-phrases)
- [Filter Conditions](#filter-conditions)
- [Sorting (ORDER BY)](#sorting-order-by)
- [Pagination (LIMIT/OFFSET)](#pagination-limitoffset)
- [Error Handling](#error-handling)
- [Performance Tips](#performance-tips)

---

## Basic Syntax

### Command Format

```
SEARCH <table> <query_expression> [FILTER ...] [SORT ...] [LIMIT ...] [OFFSET ...]
COUNT <table> <query_expression> [FILTER ...]
```

### Simple Term Search

```
SEARCH <table> <term>
```

Example:
```
SEARCH threads golang
```

### Response Format

**SEARCH Response:**
```
OK RESULTS <total_count> <id1> <id2> <id3> ...
```

Example:
```
OK RESULTS 3 101 205 387
```

**COUNT Response:**
```
OK COUNT <number>
```

Example:
```
OK COUNT 42
```

---

## Boolean Operators

### AND Search

Search for documents containing **all** specified terms.

```
SEARCH <table> term1 AND term2 AND term3
```

Example:
```
SEARCH threads golang AND tutorial
```

### OR Search

Search for documents containing **any** of the specified terms.

```
SEARCH <table> term1 OR term2 OR term3
```

Example:
```
SEARCH threads golang OR python OR rust
```

### NOT Search

Exclude documents containing specific terms.

```
SEARCH <table> term1 NOT term2
```

Example:
```
SEARCH threads tutorial NOT beginner
```

**Important:** NOT excludes documents from the result set. Use it carefully with large indexes.

---

## Complex Boolean Queries

### Parentheses for Precedence

Use parentheses to group expressions and control operator precedence:

```
SEARCH <table> (term1 OR term2) AND term3
```

Example:
```
SEARCH threads (golang OR python) AND tutorial
```

This finds documents containing "tutorial" AND either "golang" OR "python".

### Nested Expressions

You can nest multiple levels of parentheses:

```
SEARCH <table> ((term1 OR term2) AND term3) OR term4
```

Example:
```
SEARCH threads ((golang OR python) AND web) OR rust
```

This finds:
- Documents with "web" AND ("golang" OR "python")
- **OR** documents with "rust"

### Complex Query Examples

#### Find Go or Python tutorials, excluding beginner content

```
SEARCH threads (golang OR python) AND tutorial NOT beginner
```

#### Find database content about MySQL or PostgreSQL, excluding SQLite

```
SEARCH posts database AND (mysql OR postgresql) NOT sqlite
```

#### Find machine learning content in Python or R, excluding TensorFlow

```
SEARCH articles "machine learning" AND (python OR R) NOT tensorflow
```

---

## Operator Precedence

When no parentheses are used, operators have the following precedence (highest to lowest):

1. **NOT** (highest)
2. **AND** (medium)
3. **OR** (lowest)

### Precedence Examples

**Query:** `a OR b AND c`
**Parsed as:** `a OR (b AND c)`

**Query:** `NOT a AND b`
**Parsed as:** `(NOT a) AND b`

**Query:** `a AND b OR c AND d`
**Parsed as:** `(a AND b) OR (c AND d)`

**Best Practice:** Use parentheses to make intent explicit, even when not strictly necessary.

---

## Quoted Phrases

Use double quotes `"` or single quotes `'` for exact phrase matching:

```
SEARCH <table> "exact phrase"
SEARCH <table> 'machine learning'
```

### Escape Sequences

Supported escape sequences inside quoted strings:

- `\n` - Newline
- `\t` - Tab
- `\r` - Carriage return
- `\\` - Backslash
- `\"` - Double quote
- `\'` - Single quote

Example:
```
SEARCH articles "hello \"world\""
```

### Mixing Quotes with Operators

Quoted phrases can be combined with boolean operators:

```
SEARCH threads "web framework" AND (golang OR python)
SEARCH posts "machine learning" NOT "deep learning"
```

---

## Filter Conditions

Filter results by column values using the `FILTER` clause.

### Syntax

```
SEARCH <table> <query> FILTER <column> <operator> <value> [FILTER <col> <op> <val> ...]
```

Multiple filters can be specified (all must match - AND logic).

### Supported Operators

- `=` or `EQ` - Equal
- `!=` or `NE` - Not equal
- `>` or `GT` - Greater than
- `>=` or `GTE` - Greater than or equal
- `<` or `LT` - Less than
- `<=` or `LTE` - Less than or equal

### Examples

**Single filter:**
```
SEARCH articles tech FILTER status = 1
```

**Multiple filters:**
```
SEARCH articles tech FILTER status = 1 FILTER category = ai
```

**Comparison operators:**
```
SEARCH articles tech FILTER views > 1000
SEARCH articles tech FILTER created_at >= 2024-01-01
SEARCH articles tech FILTER priority != 0
```

**With boolean queries:**
```
SEARCH threads (golang OR python) AND tutorial FILTER status = published
```

### Filter Column Types

MygramDB supports filtering on indexed filter columns:

- **Integer**: `status=1`, `priority=5`
- **String**: `category=tech`, `author=john`
- **Date/Time**: `created_at=2024-01-15T10:30:00`

**Note:** Only columns configured as filters in `config.yaml` can be used in FILTER clauses.

### Filter Performance

- **Bitmap indexes**: Very fast for low-cardinality columns (e.g., status, category)
- **Dictionary compression**: Efficient for string columns
- **Filtering order**: Filters are applied after text search intersection

---

## Sorting (SORT clause)

Sort search results using the `SORT` clause.

### Syntax

```
SEARCH <table> <query> SORT <column> [ASC|DESC]
```

**Note:** The `ORDER BY` syntax is not supported. Use `SORT` instead.

### Default Behavior

If `SORT` is not specified, results are sorted by **primary key in descending order** (newest first).

```
SEARCH threads golang
-- Equivalent to: SEARCH threads golang SORT id DESC
```

### Sorting by Primary Key

**Full syntax:**
```
SEARCH threads golang SORT id ASC
SEARCH threads golang SORT id DESC
```

**Shorthand syntax (recommended):**
```
SEARCH threads golang SORT ASC   -- Primary key ascending
SEARCH threads golang SORT DESC  -- Primary key descending
```

### Sorting by Filter Column

Sort by any indexed filter column:

```
SEARCH threads golang SORT created_at DESC LIMIT 10
SEARCH posts database SORT score ASC LIMIT 20
```

### Combining with Boolean Queries

```
SEARCH threads (golang OR python) AND tutorial SORT created_at DESC LIMIT 10
SEARCH posts ((mysql OR postgresql) AND database) NOT sqlite SORT score ASC
```

### Performance Considerations

**Sorting Algorithm:**
- **With LIMIT**: Uses `partial_sort` - O(N × log(K)) where K = LIMIT + OFFSET
- **Without LIMIT**: Uses full sort - O(N × log(N))
- **Memory**: In-place sorting, no additional memory allocation

**For large result sets (e.g., 1M documents with 800K matches):**
- **Always use LIMIT** whenever possible to leverage partial_sort optimization (~3x faster)
- Sorting by primary key is faster than filter columns for numeric keys
- Results are sorted **before** applying OFFSET/LIMIT (ensures correct pagination)

**Example Performance:**
- 800K results with LIMIT 100: ~3x faster with partial_sort
- Sorting happens in-place: no memory overhead

### Column Validation

- **Primary key**: Always valid
- **Filter columns**: Must exist in at least one document
- **Non-existent columns**: Logged as warning, treated as NULL values (sorted last)

---

## Pagination (LIMIT/OFFSET)

Control the number of results returned using `LIMIT` and `OFFSET`.

### LIMIT - Maximum Results

```
SEARCH <table> <query> LIMIT <n>
```

Example:
```
SEARCH articles tech LIMIT 10
```

**Default:** 100 (configurable via `api.default_limit` in config.yaml)
**Range:** 5-1000 (configurable via `kMinLimit` and `kMaxLimit`)

### OFFSET - Skip Results

```
SEARCH <table> <query> OFFSET <n>
```

Example:
```
SEARCH articles tech LIMIT 10 OFFSET 20
```

This returns results 21-30 (skips first 20).

### Pagination Examples

**Page 1 (first 10 results):**
```
SEARCH articles tech LIMIT 10 OFFSET 0
```

**Page 2 (results 11-20):**
```
SEARCH articles tech LIMIT 10 OFFSET 10
```

**Page 3 (results 21-30):**
```
SEARCH articles tech LIMIT 10 OFFSET 20
```

### Maximum Query Length

MygramDB rejects queries whose combined expression length (search text + AND/NOT terms + FILTER values) exceeds the configured limit.

- **Default:** 128 characters
- **Config:** `api.max_query_length` (`0` disables the guard)
- **Error:** `ERROR Query expression length (...) exceeds maximum allowed length...`

Keep boolean expressions compact or raise the limit in `config.yaml` if applications require longer filters.

### Complete Example with All Options

```
SEARCH threads (golang OR python) AND tutorial
  FILTER status = published
  SORT created_at DESC
  LIMIT 10
  OFFSET 20
```

This query:
1. Finds documents with "tutorial" AND ("golang" OR "python")
2. Filters to only published documents
3. Sorts by creation date (newest first)
4. Returns results 21-30 (page 3 with 10 results per page)

### Pagination Performance

- **LIMIT optimization**: Enables partial_sort (much faster for large result sets)
- **OFFSET cost**: O(N) where N = OFFSET (results are still generated, just not returned)
- **Best practice**: Use LIMIT with ORDER BY for consistent pagination
- **Deep pagination**: Large OFFSET values (e.g., 10000+) can be slow

---

## Error Handling

### Invalid Queries

The following will return errors:

**Empty parentheses:**
```
SEARCH threads ()
ERROR Invalid query: empty expression in parentheses
```

**Unclosed parentheses:**
```
SEARCH threads (golang AND python
ERROR Invalid query: unclosed parentheses
```

**Extra closing parentheses:**
```
SEARCH threads golang AND python)
ERROR Invalid query: unexpected closing parenthesis
```

**Operator without operands:**
```
SEARCH threads AND
ERROR Invalid query: operator without operands
```

**Trailing operator:**
```
SEARCH threads golang AND
ERROR Invalid query: trailing operator
```

**Unclosed quotes:**
```
SEARCH threads "golang tutorial
ERROR Invalid query: unclosed quote
```

### Invalid Filters

**Non-existent table:**
```
SEARCH nonexistent tech
ERROR Table not found: nonexistent
```

**Invalid filter column:**
```
SEARCH articles tech FILTER invalid_column=1
ERROR Filter column not found: invalid_column
```

### Invalid Sorting

**Non-existent column:**
```
SEARCH articles tech ORDER BY nonexistent DESC
WARNING Column 'nonexistent' not found in documents, treating as NULL
```

Note: Non-existent columns generate a warning but don't error (treated as NULL).

---

## Performance Tips

### 1. Place Restrictive Terms First

```
-- Good: Specific term first
SEARCH articles "machine learning" AND tutorial

-- Less optimal: Generic term first
SEARCH articles tutorial AND "machine learning"
```

### 2. Use Parentheses for Clarity

```
-- Explicit and readable
SEARCH threads (golang OR python) AND (web OR api)

-- Harder to understand
SEARCH threads golang OR python AND web OR api
```

### 3. Avoid Leading NOT Operators

```
-- Good: Positive term first
SEARCH articles tech NOT old

-- Less optimal: Leading NOT
SEARCH articles NOT old
```

Leading NOT requires scanning all documents before exclusion.

### 4. Combine Filters and Text Search

```
-- Good: Filter narrows results early
SEARCH articles tech FILTER category = ai FILTER status = 1

-- Works but less efficient
SEARCH articles tech AND ai AND published
```

Filters on indexed columns are faster than text search on those terms.

### 5. Always Use LIMIT for Large Result Sets

```
-- Good: Uses partial_sort optimization
SEARCH articles tech SORT created_at DESC LIMIT 10

-- Slower: Full sort of all results
SEARCH articles tech SORT created_at DESC
```

### 6. Minimize Deep Pagination

```
-- Efficient
SEARCH articles tech LIMIT 10 OFFSET 0

-- Less efficient (large OFFSET)
SEARCH articles tech LIMIT 10 OFFSET 10000
```

Consider alternative pagination strategies for deep results (e.g., cursor-based).

---

## COUNT Command

All boolean query syntax works with `COUNT` as well:

```
COUNT <table> <query_expression> [FILTER ...]
```

Examples:
```
COUNT threads (golang OR python) AND tutorial
COUNT articles tech FILTER status = 1 FILTER category = ai
COUNT posts database AND (mysql OR postgresql) NOT sqlite
```

**Note:** COUNT does not support SORT, LIMIT, or OFFSET (not needed for counting).

---

## Implementation Details

### Grammar (BNF)

Queries are parsed into an Abstract Syntax Tree (AST) with proper operator precedence:

```bnf
query     → or_expr
or_expr   → and_expr (OR and_expr)*
and_expr  → not_expr (AND not_expr)*
not_expr  → NOT not_expr | primary
primary   → TERM | '(' or_expr ')'
```

### Performance Characteristics

- **AND operations**: Efficient intersection using sorted posting lists
- **OR operations**: Efficient union using set operations
- **NOT operations**: Complement against all documents (potentially expensive)
- **Parentheses**: No performance overhead; only affects parsing

### N-gram Tokenization

MygramDB uses n-gram tokenization for indexing and search:

- **Default n-gram size**: 2 (bigrams) - configurable per table
- **CJK text**: Separate n-gram size for kanji/kana (configurable)
- **Unicode normalization**: NFKC normalization, width conversion, optional lowercasing

---

## Snapshot Synchronization Commands

### SYNC Command

Manually trigger snapshot synchronization from MySQL to MygramDB.

**Syntax:**
```
SYNC <table_name>
```

**Example:**
```
SYNC articles
```

**Response:**
```
OK SYNC STARTED table=articles job_id=1
```

**See [SYNC Command Guide](sync_command.md) for detailed usage.**

### SYNC STATUS Command

Check the progress and status of SYNC operations.

**Syntax:**
```
SYNC STATUS
```

**Example:**
```
SYNC STATUS
```

**Response Examples:**
```
table=articles status=IN_PROGRESS progress=10000/25000 rows (40.0%) rate=5000 rows/s
table=articles status=COMPLETED rows=25000 time=5.2s gtid=uuid:123 replication=STARTED
status=IDLE message="No sync operation performed"
```

**See [SYNC Command Guide](sync_command.md) for detailed field descriptions.**

---

## See Also

- [SYNC Command Guide](sync_command.md) - Manual snapshot synchronization
- [Protocol Reference](protocol.md) - All commands and protocol details
- [Configuration Guide](configuration.md) - Configure filters, n-gram sizes, and limits
- [Performance Tuning](performance.md) - Advanced optimization techniques
- [README](../../README.md) - Project overview and quick start
