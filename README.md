# SQL Parser (QueryParser)

QueryParser is a lightweight, thread-safe helper built on top of [sqlglot](https://github.com/tobymao/sqlglot) that answers two questions about SQL statements:

1. **Which columns are referenced, and what tables might they come from?**
2. **How do tables join together, including non-trivial join predicates?**

It is designed for schema exploration, automated lineage capture, and static analysis workflows where you need structured metadata without executing SQL. Snowflake SQL is supported by default, but any dialect supported by sqlglot can be passed via the `dialect` parameter.

## Installation

```bash
pip install sqlglot
```

Clone/download this repository and ensure it is on your `PYTHONPATH`. The simplest option is to place the `QueryParser` folder in the same directory as the script or notebook that uses it:

```
your_project/
├── my_script.py
└── QueryParser/
    ├── __init__.py
    ├── QueryParser.py
    └── Column.py
```

With this layout you can import it with `from QueryParser import QueryParser` without worrying about the path.

## QueryParser at a Glance

```python
from QueryParser import QueryParser

sql = """
WITH recent AS (
    SELECT account_id, balance FROM core.accounts WHERE updated_at >= DATEADD('day', -7, CURRENT_DATE)
)
SELECT
    r.account_id,
    b.balance_bucket
FROM recent r
JOIN analytics.balance_lookup b
    ON r.balance + 10 = b.bucket_start
"""

parser = QueryParser(sql, dialect="snowflake")
```

### `source_columns()` / `feature_columns()`

- `source_columns()` returns `Column` dataclass instances for every unique column reference.
- `feature_columns()` returns the same data as dictionaries.

```python
>>> parser.feature_columns()
[
    {"name": "account_id", "potential_tables": ["core.accounts"]},
    {"name": "balance", "potential_tables": ["core.accounts"]},
    {"name": "balance_bucket", "potential_tables": ["analytics.balance_lookup"]},
    {"name": "bucket_start", "potential_tables": ["analytics.balance_lookup"]},
]
```

### `joins()`

Returns dictionaries describing each join encountered. Fields:

| Field           | Description                                                                                       |
| --------------- | ------------------------------------------------------------------------------------------------- |
| `join_type`     | Standardized join type (`INNER JOIN`, `LEFT JOIN`, etc.).                                         |
| `column_left`   | `Column` object describing the left-side column in the predicate.                                 |
| `column_right`  | `Column` object describing the right-side column.                                                 |
| `complex_left`  | Optional string with raw SQL when the left expression is more than a bare column.                 |
| `complex_right` | Optional string with raw SQL when the right expression is more than a bare column.                |

Example entry for `r.balance + 10 = b.bucket_start`:

```python
{
    "join_type": "INNER JOIN",
    "column_left": Column(col_name="balance", potential_tables=["core.accounts"]),
    "column_right": Column(col_name="bucket_start", potential_tables=["analytics.balance_lookup"]),
    "complex_left": "r.balance + 10"
}
```

### `column_lineage(alias: Optional[str])`

Returns the captured lineage map per alias. With no argument it returns every alias; passing an alias filters the result:

```python
>>> parser.column_lineage("r")
{"r": {"account_id": ["core.accounts"], "balance": ["core.accounts"]}}
```

### `filters()` / `get_filters()`

Surfaces every `WHERE`/`HAVING` comparator in the query. Each entry includes the raw SQL snippet, the clause type, operator, and the columns (with lineage) referenced inside the predicate:

```python
>>> parser.filters()
[
    {
        "query": "updated_at >= DATEADD(DAY, -7, CURRENT_DATE)",
        "filter_type": "WHERE",
        "operator": ">=",
        "columns": [
            Column(name="updated_at", potential_tables=["core.accounts"], lineage=None)
        ],
    }
]
```

`get_filters()` is an alias for `filters()` to match external call-sites.

### `source_tables()`

Returns the ordered list of unique tables referenced by the query (including tables surfaced through CTEs and subqueries):

```python
>>> parser.source_tables()
["core.accounts", "analytics.balance_lookup"]
```

### Thread Safety

All public QueryParser methods acquire an internal re-entrant lock before populating caches, so you can safely share a single parser instance across threads when building multi-processing pipelines or serving metadata via APIs.

## Example End-to-End Usage

```python
from QueryParser import QueryParser

sql = """
SELECT
    t1.A,
    B
FROM EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A t1
LEFT JOIN EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B t2
    ON t1.ID = t2.ID
"""

parser = QueryParser(sql)

print("Columns:")
for column in parser.feature_columns():
    print(" -", column)

print("\nJoins:")
for join in parser.joins():
    print(f" - {join['join_type']}: {join['column_left']} = {join['column_right']}")
```

Output:

```
Columns:
 - {'name': 'A', 'potential_tables': ['EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A']}
 - {'name': 'B', 'potential_tables': ['EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A', 'EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B']}
 - {'name': 'ID', 'potential_tables': ['EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A']}
 - {'name': 'ID', 'potential_tables': ['EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B']}

Joins:
 - LEFT JOIN: Column(name='ID', potential_tables=['EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A'], lineage=None) = Column(name='ID', potential_tables=['EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B'], lineage=None)
```

## Testing

The regression suite covers:

- Basic selects, wildcard projections, and aliasing.
- CTE chains with up to six nested levels.
- Subqueries, unions, and derived tables.
- Join syntax variants, including `USING`, `OR` predicates, inequalities, and calculated expressions.

Run it with:

```bash
python parser_tester.py
```

## Project Structure

| File               | Purpose                                                                 |
| ------------------ | ----------------------------------------------------------------------- |
| `QueryParser.py`   | Main parser module; exposes column / join features and lineage helpers. |
| `Column.py`        | Dataclass definition of a class used by the parser.                     |
| `parser_tester.py` | End-to-end regression suite using Python's `unittest`.                  |

## Notes & Tips

- QueryParser defaults to the Snowflake dialect. Pass `dialect="postgres"` (or any other sqlglot-supported dialect) when constructing the parser if needed.
- When `joins()` detects expressions, `column_left`/`column_right` still report best-effort lineage while the `complex_*` keys preserve the raw SQL so downstream systems can decide how to handle them.
