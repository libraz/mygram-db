# Query Syntax

MygramDB supports a rich boolean query syntax for complex text search operations.

## Basic Syntax

### Simple Term Search
```
SEARCH <table> <term>
```
Example:
```
SEARCH threads golang
```

### AND Search
```
SEARCH <table> term1 AND term2 AND term3
```
Example:
```
SEARCH threads golang AND tutorial
```

### OR Search
```
SEARCH <table> term1 OR term2 OR term3
```
Example:
```
SEARCH threads golang OR python OR rust
```

### NOT Search
```
SEARCH <table> term1 NOT term2
```
Example:
```
SEARCH threads tutorial NOT beginner
```

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

### Nested Expressions
You can nest multiple levels of parentheses:

```
SEARCH <table> ((term1 OR term2) AND term3) OR term4
```

Example:
```
SEARCH threads ((golang OR python) AND web) OR rust
```

### Complex Query Examples

#### 1. Find Go or Python tutorials, but not beginner content
```
SEARCH threads (golang OR python) AND tutorial AND NOT beginner
```

#### 2. Find database content about MySQL or PostgreSQL, excluding SQLite
```
SEARCH posts database AND (mysql OR postgresql) AND NOT sqlite
```

#### 3. Find machine learning content in Python or R, excluding TensorFlow
```
SEARCH articles "machine learning" AND (python OR R) AND NOT tensorflow
```

## Operator Precedence

When no parentheses are used, operators have the following precedence (highest to lowest):

1. **NOT** (highest)
2. **AND** (medium)
3. **OR** (lowest)

### Examples

**Query:** `a OR b AND c`
**Parsed as:** `a OR (b AND c)`

**Query:** `NOT a AND b`
**Parsed as:** `(NOT a) AND b`

**Query:** `a AND b OR c AND d`
**Parsed as:** `(a AND b) OR (c AND d)`

## Quoted Phrases

Use double quotes or single quotes for exact phrase matching:

```
SEARCH <table> "hello world"
SEARCH <table> 'machine learning'
```

Example with boolean operators:
```
SEARCH threads "web framework" AND (golang OR python)
```

## Filter Conditions

Boolean queries can be combined with filter conditions:

```
SEARCH <table> (golang OR python) AND tutorial FILTER status = published LIMIT 10
```

## ORDER BY Clause

Results can be sorted using the ORDER BY clause:

```
SEARCH <table> <term> ORDER BY <column> [ASC|DESC]
```

### Sorting Options

#### By Primary Key (Default)
If ORDER BY is not specified, results are sorted by primary key in descending order:

```
SEARCH threads golang
-- Equivalent to: SEARCH threads golang ORDER DESC
```

**Primary key sorting options:**

Full syntax:
```
SEARCH threads golang ORDER BY <primary_key> ASC
SEARCH threads golang ORDER BY <primary_key> DESC
```

**Shorthand syntax (recommended):**
```
SEARCH threads golang ORDER BY ASC   -- Primary key ascending
SEARCH threads golang ORDER BY DESC  -- Primary key descending
SEARCH threads golang ORDER ASC      -- Even shorter (BY is optional)
SEARCH threads golang ORDER DESC     -- Even shorter (BY is optional)
```

#### By Filter Column
Sort by any indexed filter column:

```
SEARCH threads golang ORDER BY created_at DESC LIMIT 10
SEARCH posts database ORDER BY score ASC LIMIT 20
```

### Combined Examples

**Complex boolean query with ORDER BY:**

The parser automatically handles parentheses and OR operators - no quotes needed!

```
SEARCH threads (golang OR python) AND tutorial ORDER DESC LIMIT 10
SEARCH posts ((mysql OR postgresql) AND database) NOT sqlite ORDER BY score ASC
SEARCH articles (python OR R) AND "machine learning" ORDER BY created_at DESC LIMIT 20
```

The parser tracks parentheses depth, so keywords inside `()` are treated as part of the search expression.

**With filters:**
```
SEARCH threads (golang OR python) AND tutorial FILTER status = published ORDER BY created_at DESC LIMIT 10
SEARCH posts ((mysql OR postgresql) AND "hello world") FILTER category = database ORDER BY score ASC
```

**Note:**
- ORDER BY works seamlessly with all query types: simple, AND/NOT, OR, and nested parentheses
- The parser automatically tracks parentheses depth
- Quoted phrases (`"hello world"`) can be mixed with parentheses without any escaping issues
- No special quoting or escaping required!

### Performance Considerations

**Sorting Algorithm:**
- **With LIMIT**: Uses `partial_sort` - O(N × log(K)) where K = LIMIT + OFFSET
- **Without LIMIT**: Uses full sort - O(N × log(N))
- **Memory**: In-place sorting, no additional memory allocation

**For large result sets (e.g., 1M documents with 800K matches):**
- Use LIMIT whenever possible to leverage partial_sort optimization
- Sorting by primary key is faster than filter columns for numeric keys
- Results are sorted BEFORE applying OFFSET/LIMIT (correct pagination)

**Example Performance:**
- 800K results with LIMIT 100: ~3x faster with partial_sort
- Sorting happens in-place: no memory overhead
- Sample-based column validation (first 100 docs): O(1) overhead

### Column Validation

- **Primary key**: Always valid
- **Filter columns**: Must exist in at least one document
- **Non-existent columns**: Logged as warning, treated as NULL values

## Error Handling

### Invalid Queries

The following will return errors:

- **Empty parentheses:** `()`
- **Unclosed parentheses:** `(golang AND python`
- **Extra closing parentheses:** `golang AND python)`
- **Operator without operands:** `AND`
- **Trailing operator:** `golang AND`
- **Unclosed quotes:** `"golang tutorial`

## COUNT Command

All boolean query syntax works with COUNT as well:

```
COUNT <table> (golang OR python) AND tutorial
```

## Implementation Details

Queries are parsed into an Abstract Syntax Tree (AST) with proper operator precedence. The AST is then evaluated against the n-gram index to return matching documents.

### Grammar (BNF)

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
- **NOT operations**: Complement against all documents (potentially expensive for large datasets)
- **Parentheses**: No performance overhead; only affects parsing

## Tips for Optimal Queries

1. **Place restrictive terms first** in AND operations
2. **Use parentheses** to make complex queries more readable
3. **Avoid leading NOT** operators when possible (e.g., prefer `term AND NOT exclude` over `NOT exclude`)
4. **Use filters** to narrow down results before full-text search when possible

## See Also

- [README.md](../../README.md) - Project overview
- [Configuration Guide](../../examples/config.yaml) - Server configuration
