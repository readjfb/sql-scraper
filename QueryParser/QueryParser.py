from __future__ import annotations

from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

from .Column import Column


class QueryParser:
    """Parse SQL (Snowflake by default) and expose simplified metadata features."""

    def __init__(self, query: str, dialect: str = "snowflake") -> None:
        if not query or not query.strip():
            raise ValueError("A SQL string is required")
        self.query = query
        self._dialect = dialect
        self._expression = sqlglot.parse_one(query, read=self._dialect)
        self._source_tables: List[str] = []
        self._alias_column_lineage: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        self._alias_display_names: Dict[str, str] = {}
        (
            self._cte_context,
            self._cte_definitions,
            self._cte_column_lineage,
        ) = self._collect_cte_sources(self._expression)
        self._alias_to_table, self._source_tables = self._collect_tables()
        self._select_context_cache: Dict[int, dict] = {}
        self._source_columns: Optional[List[Column]] = None
        self._select_columns: Optional[List[dict]] = None
        self._joins: Optional[List[dict]] = None
        self._filters: Optional[List[dict]] = None
        self._lock = RLock()

    def source_columns(self) -> List[Column]:
        """
        Return Column objects for every unique column reference that can be
        resolved from the query, including inferred table lineage.
        """
        with self._lock:
            if self._source_columns is None:
                self._source_columns = self._extract_source_columns()
            return self._source_columns

    def select_columns(self) -> List[dict]:
        """Return columns that surface in the SELECT list with direct/derived flags."""
        with self._lock:
            if self._select_columns is None:
                self._select_columns = self._extract_select_columns()
            return self._select_columns

    def joins(self) -> List[dict]:
        """
        Return structured join metadata for every SELECT in the query.
        Each join dict contains join_type, column_left/right Columns, and
        optional complex_left/right SQL strings when expressions are involved.
        """
        with self._lock:
            if self._joins is None:
                self._joins = self._extract_joins()
            return self._joins

    def source_tables(self) -> List[str]:
        """Return ordered list of unique source tables referenced by the query."""
        with self._lock:
            return list(self._source_tables)

    def filters(self) -> List[dict]:
        """
        Return structured WHERE/HAVING metadata for every SELECT in the query.
        """
        with self._lock:
            if self._filters is None:
                self._filters = self._extract_filters()
            return self._filters

    def get_filters(self) -> List[dict]:
        """Alias for ``filters`` to align with external call sites."""
        return self.filters()

    def feature_columns(self) -> List[dict]:
        """
        Return Feature 1 in dict form:
        [{"name": column_name, "potential_tables": ["SCHEMA.TABLE", ...]}, ...]
        """
        return [column.as_dict() for column in self.source_columns()]

    def column_lineage(
        self, alias: Optional[str] = None
    ) -> Dict[str, Dict[str, List[str]]]:
        """
        Return lineage metadata for one alias or all aliases. The return value
        maps alias -> {column_name: [tables]}.
        """

        if alias:
            lineage = self._alias_column_lineage.get(alias.lower())
            if not lineage:
                return {}
            display = self._alias_display_names.get(alias.lower(), alias)
            return {display: self._format_lineage_map(lineage)}

        return {
            self._alias_display_names.get(key, key): self._format_lineage_map(lineage)
            for key, lineage in self._alias_column_lineage.items()
            if lineage
        }

    def _collect_tables(self) -> tuple[Dict[str, List[str]], List[str]]:
        """Build alias-to-table map and ordered list of unique source tables."""
        alias_map: Dict[str, List[str]] = {}
        tables: List[str] = []
        seen: set[str] = set()

        def register_sources(sources: List[str]) -> None:
            for source in sources:
                if source and source not in seen:
                    tables.append(source)
                    seen.add(source)

        for alias, cte_sources in self._cte_definitions.items():
            register_sources(cte_sources)
            columns = self._cte_column_lineage.get(alias.lower()) or {}
            self._bind_alias(alias_map, alias, cte_sources, columns)

        for table in self._expression.find_all(exp.Table):
            sources = self._sources_for_table_reference(table, self._cte_context)
            if not sources:
                continue
            register_sources(sources)
            self._bind_alias(alias_map, table.name, sources)
            if table.alias:
                self._bind_alias(alias_map, table.alias, sources)

        for subquery in self._expression.find_all(exp.Subquery):
            alias = subquery.alias
            if not alias:
                continue
            sources = self._tables_for_subexpression(subquery.this, self._cte_context)
            register_sources(sources)
            columns = self._columns_for_subexpression(
                subquery.this, self._cte_context, self._cte_column_lineage
            )
            self._bind_alias(alias_map, alias, sources, columns)

        return alias_map, tables

    def _normalize_table_name(self, table: exp.Table) -> str:
        """Return fully qualified table name if catalog/schema exist."""
        catalog = self._identifier_name(table.args.get("catalog"))
        db = self._identifier_name(table.args.get("db"))
        name = table.name

        parts = [part for part in (catalog, db, name) if part]
        return ".".join(parts) if parts else name

    def _bind_alias(
        self,
        alias_map: Dict[str, List[str]],
        key: Optional[str],
        value: List[str],
        column_lineage: Optional[Dict[str, dict]] = None,
    ) -> None:
        """Register alias variations and optionally attach per-column lineage."""
        if not key:
            return
        normalized_value = list(value)
        normalized_lineage: Dict[str, Dict[str, List[str]]] = {}
        for column, entry in (column_lineage or {}).items():
            if isinstance(entry, dict):
                name = entry.get("name") or column
                tables = list(entry.get("tables") or [])
                lineage_entries = self._normalize_lineage_entries(entry.get("lineage"))
            else:
                name = column
                tables = list(entry or [])
                lineage_entries = []
            normalized_lineage[column.lower()] = {"name": name, "tables": tables}
            if lineage_entries:
                normalized_lineage[column.lower()]["lineage"] = lineage_entries

        for variant in [key, key.lower(), key.upper()]:
            alias_map[variant] = normalized_value
            lower = variant.lower()
            self._alias_display_names.setdefault(lower, variant)
            if normalized_lineage:
                self._alias_column_lineage[lower] = normalized_lineage

    def _identifier_name(self, identifier: Optional[exp.Expression]) -> Optional[str]:
        """Normalize sqlglot identifiers into plain strings."""
        if not identifier:
            return None
        if isinstance(identifier, exp.Identifier):
            return identifier.this
        if isinstance(identifier, str):
            return identifier
        return identifier.sql(dialect=self._dialect)

    def _extract_source_columns(self) -> List[Column]:
        """Walk the AST collecting every unique column/table pairing."""
        result: List[Column] = []
        seen: set[tuple[str, tuple[str, ...]]] = set()

        for column in self._expression.find_all(exp.Column):
            column_name = column.name
            if not column_name:
                continue

            select = column.find_ancestor(exp.Select)
            select_context = self._select_context(select)

            if column_name == "*":
                expanded = self._expand_star_column(column, select_context)
                for expanded_column in expanded:
                    key = (
                        expanded_column.col_name.lower(),
                        tuple(expanded_column.potential_tables),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    result.append(expanded_column)
                continue

            potential_tables = self._resolve_column_sources(
                column.table,
                select_context,
                column_name,
            )
            lineage_entries = self._column_lineage_for_reference(
                column.table,
                select_context,
                column_name,
            )
            if lineage_entries:
                normalized_entries = self._lineage_leaf_entries(lineage_entries)
                for entry in normalized_entries:
                    name = entry.get("name")
                    tables = entry.get("tables", [])
                    if not name or not tables:
                        continue
                    key = (name.lower(), tuple(tables))
                    if key in seen:
                        continue
                    seen.add(key)
                    result.append(Column(col_name=name, potential_tables=list(tables)))
                continue
            key = (column_name.lower(), tuple(potential_tables))
            if key in seen:
                continue

            seen.add(key)
            result.append(
                Column(col_name=column_name, potential_tables=potential_tables)
            )

        physical_columns = [column for column in result if not column.lineage]
        return self._filter_redundant_ambiguity(physical_columns)

    def _extract_select_columns(self) -> List[dict]:
        """Collect source columns used by output projections."""
        entries: Dict[tuple[str, tuple[str, ...]], dict] = {}
        for select in self._final_selects(self._expression):
            context = self._select_context(select)
            for projection in select.expressions or []:
                for column, direct in self._projection_columns(projection, context):
                    if column is None:
                        continue
                    key = (column.col_name.lower(), tuple(column.potential_tables))
                    existing = entries.get(key)
                    if existing:
                        existing["direct"] = existing["direct"] or direct
                    else:
                        entries[key] = {"column": column, "direct": direct}
        return list(entries.values())

    def _final_selects(self, expression: Optional[exp.Expression]) -> List[exp.Select]:
        """Return SELECT nodes that produce the query output."""
        if expression is None:
            return []
        if isinstance(expression, exp.With):
            return self._final_selects(expression.this)
        if isinstance(expression, exp.Subquery):
            return self._final_selects(expression.this)
        if isinstance(expression, exp.Paren):
            return self._final_selects(expression.this)
        if isinstance(expression, exp.SetOperation):
            left = self._final_selects(expression.this)
            right = self._final_selects(expression.args.get("expression"))
            return left + right
        return [expression] if isinstance(expression, exp.Select) else []

    def _projection_columns(
        self, projection: exp.Expression, context: dict
    ) -> List[tuple[Column, bool]]:
        """Return (Column, direct) pairs feeding a SELECT projection."""
        target = projection.this if isinstance(projection, exp.Alias) else projection
        if isinstance(target, exp.Column):
            return self._column_projection(target, context)

        dependencies = self._projection_lineage(projection, context)
        return self._lineage_columns(dependencies, allow_direct=False)

    def _column_projection(
        self, column: exp.Column, context: dict
    ) -> List[tuple[Column, bool]]:
        """Handle projection columns, expanding stars and lineage."""
        if column.name == "*":
            return self._star_projection(column, context)

        lineage_entries = self._column_lineage_for_reference(
            column.table, context, column.name
        )
        if lineage_entries:
            return self._lineage_columns(lineage_entries, allow_direct=True)

        column_obj = self._column_from_expression(column, context)
        return [(column_obj, True)] if column_obj else []

    def _star_projection(
        self, column: exp.Column, context: dict
    ) -> List[tuple[Column, bool]]:
        """Return column mappings for ``*`` projections."""
        columns: List[tuple[Column, bool]] = []
        for relation in self._relations_for_qualifier(context, column.table):
            for entry in (relation.get("columns") or {}).values():
                columns.extend(
                    self._lineage_columns([entry], allow_direct=True)
                )
        return columns

    def _lineage_columns(
        self, entries: Optional[List[Any]], allow_direct: bool
    ) -> List[tuple[Column, bool]]:
        """Convert lineage-style entries into Column objects with direct flags."""
        leaves = self._lineage_leaf_entries(entries)
        columns: List[Column] = []
        for leaf in leaves:
            name = leaf.get("name")
            tables = list(leaf.get("tables") or [])
            if not name or not tables:
                continue
            columns.append(Column(col_name=name, potential_tables=tables))

        direct = allow_direct and len(columns) == 1
        return [(column, direct) for column in columns]

    def _expand_star_column(
        self, column: exp.Column, select_context: dict
    ) -> List[Column]:
        """Expand ``*`` projections using the cached relation lineage."""

        targets = self._relations_for_qualifier(select_context, column.table)

        expanded: List[Column] = []
        for relation in targets:
            for entry in (relation.get("columns") or {}).values():
                name = entry.get("name")
                if not name:
                    continue
                tables = list(entry.get("tables") or [])
                lineage_leaves = self._lineage_leaf_entries(entry.get("lineage"))
                if lineage_leaves:
                    for component in lineage_leaves:
                        component_name = component.get("name")
                        component_tables = component.get("tables", [])
                        if not component_name or not component_tables:
                            continue
                        expanded.append(
                            Column(
                                col_name=component_name,
                                potential_tables=list(component_tables),
                            )
                        )
                    continue
                if not tables:
                    continue
                expanded.append(Column(col_name=name, potential_tables=tables))
        return expanded

    def _resolve_column_sources(
        self,
        qualifier: Optional[str],
        select_context: Optional[dict] = None,
        column_name: Optional[str] = None,
    ) -> List[str]:
        """Resolve a column's potential tables given the surrounding context."""
        select_context = select_context or {}
        default_tables = select_context.get("default_tables", self._source_tables[:])
        relations = select_context.get("relations", [])
        column_key = column_name.lower() if column_name else None

        if qualifier:
            qualifier_lower = qualifier.lower()
            for relation in self._relations_for_qualifier(select_context, qualifier):
                columns = relation.get("columns") or {}
                entry = columns.get(column_key) if column_key else None
                if entry:
                    return list(entry.get("tables", []))
                return list(relation.get("tables", []))

            alias_variants = [qualifier, qualifier_lower, qualifier.upper()]
            for alias in alias_variants:
                resolved = self._alias_to_table.get(alias)
                if resolved:
                    lineage = self._alias_column_lineage.get(alias.lower(), {})
                    if column_key and column_key in lineage:
                        return list(lineage[column_key].get("tables", []))
                    return list(resolved)
            return [qualifier]

        matches: List[str] = []
        matched = False
        if column_key:
            for relation in relations:
                columns = relation.get("columns") or {}
                entry = columns.get(column_key)
                if entry:
                    matches.extend(entry.get("tables", []))
                    matched = True

        if matched and matches:
            return self._merge_sources([], matches)

        if default_tables:
            return list(default_tables)
        return []

    def _extract_joins(self) -> List[dict]:
        """Collect joins from every SELECT statement."""
        joins: List[dict] = []
        for select in self._expression.find_all(exp.Select):
            joins.extend(self._extract_joins_for_select(select))
        return joins

    def _extract_filters(self) -> List[dict]:
        """Collect WHERE and HAVING clauses from every SELECT statement."""
        filters: List[dict] = []
        for select in self._expression.find_all(exp.Select):
            context = self._select_context(select)
            where = select.args.get("where")
            if where and where.this:
                filters.extend(
                    self._filters_from_condition(where.this, "WHERE", context)
                )
            having = select.args.get("having")
            if having and having.this:
                filters.extend(
                    self._filters_from_condition(having.this, "HAVING", context)
                )
        return filters

    def _extract_joins_for_select(self, select: exp.Select) -> List[dict]:
        """Return joins for a single SELECT statement."""
        joins: List[dict] = []
        from_clause = select.args.get("from")
        if not from_clause:
            return joins

        left_sources = self._relation_sources(from_clause.this)
        for join in select.args.get("joins") or []:
            join_type = self._format_join_type(join)
            right_sources = self._relation_sources(join.args.get("this"))
            condition_pairs = self._columns_from_condition(
                join.args.get("on"), left_sources, right_sources
            )
            condition_pairs.extend(
                self._columns_from_using(
                    join.args.get("using"), left_sources, right_sources
                )
            )

            for pair in condition_pairs:
                left_column = pair.get("column_left")
                right_column = pair.get("column_right")
                if left_column and right_column:
                    join_entry = {
                        "join_type": join_type,
                        "column_left": left_column,
                        "column_right": right_column,
                    }
                    if pair.get("complex_left"):
                        join_entry["complex_left"] = pair["complex_left"]
                    if pair.get("complex_right"):
                        join_entry["complex_right"] = pair["complex_right"]
                    joins.append(join_entry)

            left_sources = self._merge_sources(left_sources, right_sources)

        return joins

    def _format_join_type(self, join: exp.Join) -> str:
        """Normalize sqlglot join metadata into uppercase strings."""
        parts = [str(part).upper() for part in (join.side, join.kind) if part]
        if not parts:
            parts = ["INNER"]
        parts.append("JOIN")
        return " ".join(parts)

    def _filters_from_condition(
        self,
        condition: exp.Expression,
        filter_type: str,
        select_context: dict,
    ) -> List[dict]:
        """Return normalized filter metadata for a WHERE/HAVING clause."""
        filters: List[dict] = []
        comparators = self._flatten_conditions(condition)
        if not comparators:
            return filters

        for comparator in comparators:
            columns = self._filter_columns(comparator, select_context)
            entry = {
                "query": comparator.sql(dialect=self._dialect),
                "filter_type": filter_type,
                "operator": self._format_filter_operator(comparator),
                "columns": columns,
            }
            filters.append(entry)

        return filters

    def _filter_columns(
        self, comparator: exp.Expression, select_context: dict
    ) -> List[Column]:
        """Return unique Column objects involved in a comparator expression."""
        columns: List[Column] = []
        seen: set[tuple[str, tuple[str, ...]]] = set()
        for column_expr in comparator.find_all(exp.Column):
            column = self._column_with_lineage(column_expr, select_context)
            if column is None:
                continue
            key = (column.col_name.lower(), tuple(column.potential_tables))
            if key in seen:
                continue
            seen.add(key)
            columns.append(column)
        return columns

    def _format_filter_operator(self, comparator: exp.Expression) -> str:
        """Return a human-readable operator string for a comparator expression."""
        operator_map = {
            exp.EQ: "=",
            exp.NEQ: "!=",
            exp.LT: "<",
            exp.LTE: "<=",
            exp.GT: ">",
            exp.GTE: ">=",
        }
        for expression_type, symbol in operator_map.items():
            if isinstance(comparator, expression_type):
                return symbol
        return comparator.key.upper()

    def _column_from_expression(
        self,
        expression: Optional[exp.Expression],
        select_context: Optional[dict] = None,
    ) -> Optional[Column]:
        """Build a Column object from a sqlglot expression if possible."""
        if not isinstance(expression, exp.Column):
            return None

        column_name = expression.name
        if not column_name:
            return None

        potential_tables = self._resolve_column_sources(
            expression.table, select_context, column_name
        )
        return Column(col_name=column_name, potential_tables=potential_tables)

    def _column_with_lineage(
        self, column: exp.Column, select_context: Optional[dict]
    ) -> Optional[Column]:
        """Build a Column with lineage information for a filter expression."""
        column_obj = self._column_from_expression(column, select_context)
        if column_obj is None:
            return None

        lineage_entries = self._column_lineage_for_reference(
            column.table, select_context, column.name
        )
        lineage_columns = self._build_lineage_columns(lineage_entries)
        column_obj.lineage = lineage_columns or None
        return column_obj

    def _format_lineage_map(
        self, lineage: Dict[str, Dict[str, List[str]]]
    ) -> Dict[str, List[str]]:
        """Flatten internal lineage cache into alias -> [tables] mappings."""
        return {
            entry.get("name") or column_key: list(entry.get("tables", []))
            for column_key, entry in lineage.items()
        }

    def _collect_cte_sources(
        self,
        expression: exp.Expression,
        outer_context: Optional[Dict[str, List[str]]] = None,
        outer_columns: Optional[Dict[str, Dict[str, Dict[str, List[str]]]]] = None,
    ) -> tuple[
        Dict[str, List[str]],
        Dict[str, List[str]],
        Dict[str, Dict[str, Dict[str, List[str]]]],
    ]:
        """Collect table + column lineage produced by WITH clauses."""
        context: Dict[str, List[str]] = dict(outer_context or {})
        definitions: Dict[str, List[str]] = {}
        column_definitions: Dict[str, Dict[str, Dict[str, List[str]]]] = dict(
            outer_columns or {}
        )
        with_expression = expression.args.get("with")
        if not with_expression:
            return context, definitions, column_definitions

        for cte in with_expression.expressions:
            alias = cte.alias
            if not alias:
                continue

            cte_sources = self._tables_for_subexpression(cte.this, context)
            cte_columns = self._columns_for_subexpression(
                cte.this, context, column_definitions
            )
            definitions[alias] = cte_sources
            context[alias.lower()] = cte_sources
            column_definitions[alias.lower()] = cte_columns

        return context, definitions, column_definitions

    def _tables_for_subexpression(
        self,
        expression: exp.Expression,
        outer_context: Optional[Dict[str, List[str]]] = None,
    ) -> List[str]:
        """Return ordered tables referenced inside ``expression``."""

        context, _, _ = self._collect_cte_sources(expression, dict(outer_context or {}))
        tables: List[str] = []
        seen: set[str] = set()

        for table in expression.find_all(exp.Table):
            sources = self._sources_for_table_reference(table, context)
            for source in sources:
                if source and source not in seen:
                    tables.append(source)
                    seen.add(source)

        return tables

    def _columns_for_subexpression(
        self,
        expression: exp.Expression,
        table_context: Optional[Dict[str, List[str]]] = None,
        column_context: Optional[Dict[str, Dict[str, Dict[str, List[str]]]]] = None,
    ) -> Dict[str, Dict[str, List[str]]]:
        """Return a mapping of column alias -> {name,tables} for ``expression``."""
        table_context = table_context or {}
        column_context = column_context or {}

        target = expression.this if isinstance(expression, exp.Subquery) else expression
        if not isinstance(target, exp.Select):
            return {}

        context = self._build_select_context(target, table_context, column_context)
        lineage: Dict[str, Dict[str, List[str]]] = {}

        for projection in target.expressions or []:
            output_name = self._output_column_name(projection)
            if not output_name:
                continue
            if output_name == "*" and isinstance(projection, exp.Column):
                expanded = self._expand_star_column(projection, context)
                for expanded_column in expanded:
                    lineage[expanded_column.col_name.lower()] = {
                        "name": expanded_column.col_name,
                        "tables": expanded_column.potential_tables,
                    }
                continue

            tables = self._tables_for_projection(projection, context)
            projection_lineage = self._projection_lineage(projection, context)
            if tables:
                entry = {
                    "name": output_name,
                    "tables": sorted(set(tables)),
                }
                if projection_lineage:
                    entry["lineage"] = projection_lineage
                lineage[output_name.lower()] = entry

        return lineage

    def _sources_for_table_reference(
        self, table: exp.Table, cte_context: Optional[Dict[str, List[str]]] = None
    ) -> List[str]:
        """Resolve a Table node to its canonical fully qualified names."""
        cte_context = cte_context or {}
        name = table.name
        name_lower = name.lower() if name else ""
        sources = cte_context.get(name_lower)
        if sources:
            return list(sources)

        normalized = self._normalize_table_name(table)
        return [normalized] if normalized else []

    def _relation_sources(self, relation: Optional[exp.Expression]) -> List[str]:
        """Return source tables for arbitrary relation types (table/alias/subquery)."""
        if relation is None:
            return []

        if isinstance(relation, exp.Table):
            return self._sources_for_table_reference(relation, self._cte_context)

        if isinstance(relation, exp.Subquery):
            if relation.alias:
                alias_sources = self._alias_to_table.get(
                    relation.alias
                ) or self._alias_to_table.get(relation.alias.lower())
                if alias_sources:
                    return alias_sources
            return self._tables_for_subexpression(relation.this, self._cte_context)

        if isinstance(relation, exp.Identifier):
            alias_sources = self._alias_to_table.get(relation.this)
            if alias_sources:
                return alias_sources

        return []

    def _columns_from_condition(
        self,
        condition: Optional[exp.Expression],
        left_sources: List[str],
        right_sources: List[str],
    ) -> List[dict]:
        """Extract column pair metadata for ON clauses, correcting orientation."""
        result: List[dict] = []
        left_source_set = set(left_sources)
        right_source_set = set(right_sources)

        for comparator in self._flatten_conditions(condition):
            left_column, complex_left = self._extract_join_operand(
                comparator.args.get("this")
            )
            right_column, complex_right = self._extract_join_operand(
                comparator.args.get("expression")
            )

            if left_column and right_column:
                left_from_left = bool(
                    left_source_set.intersection(left_column.potential_tables)
                )
                right_from_left = bool(
                    left_source_set.intersection(right_column.potential_tables)
                )
                left_from_right = bool(
                    right_source_set.intersection(left_column.potential_tables)
                )
                right_from_right = bool(
                    right_source_set.intersection(right_column.potential_tables)
                )

                if not left_from_left and right_from_left:
                    left_column, right_column = right_column, left_column
                    complex_left, complex_right = complex_right, complex_left
                elif not right_from_right and left_from_right:
                    left_column, right_column = right_column, left_column
                    complex_left, complex_right = complex_right, complex_left

            result.append(
                {
                    "column_left": left_column
                    or Column(col_name="", potential_tables=[]),
                    "column_right": right_column
                    or Column(col_name="", potential_tables=[]),
                    "complex_left": complex_left,
                    "complex_right": complex_right,
                }
            )

        return result

    def _flatten_conditions(
        self, condition: Optional[exp.Expression]
    ) -> List[exp.Expression]:
        """Flatten nested logical trees while preserving arbitrary predicates."""
        if condition is None:
            return []

        if isinstance(condition, exp.Paren):
            return self._flatten_conditions(condition.this)

        logical_types = (exp.And, exp.Or)
        if isinstance(condition, logical_types):
            left_conditions = self._flatten_conditions(condition.args.get("this"))
            right_conditions = self._flatten_conditions(
                condition.args.get("expression")
            )
            return left_conditions + right_conditions

        return [condition]

    def _columns_from_using(
        self,
        using_columns: Optional[List[exp.Expression]],
        left_sources: List[str],
        right_sources: List[str],
    ) -> List[dict]:
        """Create join column pairs coming from ``USING`` clauses."""
        if not using_columns:
            return []

        pairs: List[dict] = []
        for identifier in using_columns:
            column_name = self._identifier_name(identifier)
            if not column_name:
                continue

            left_column = Column(
                col_name=column_name, potential_tables=list(left_sources)
            )
            right_column = Column(
                col_name=column_name, potential_tables=list(right_sources)
            )
            pairs.append(
                {
                    "column_left": left_column,
                    "column_right": right_column,
                }
            )

        return pairs

    def _extract_join_operand(
        self,
        expression: Optional[exp.Expression],
        select_context: Optional[dict] = None,
    ) -> Tuple[Optional[Column], Optional[str]]:
        """Return (Column, complex_sql) for a join comparator operand."""
        if expression is None:
            return None, None

        if isinstance(expression, exp.Column):
            return self._column_from_expression(expression, select_context), None

        complex_sql = expression.sql(dialect=self._dialect)
        nested_column = next(expression.find_all(exp.Column), None)
        column = (
            self._column_from_expression(nested_column, select_context)
            if nested_column
            else None
        )
        if column is None:
            column = Column(col_name=complex_sql, potential_tables=[])
        return column, complex_sql

    def _merge_sources(self, left: List[str], right: List[str]) -> List[str]:
        """Append ``right`` onto ``left`` while keeping order + uniqueness."""
        merged = list(left)
        seen = set(left)
        for source in right:
            if source not in seen:
                merged.append(source)
                seen.add(source)
        return merged

    def _relations_for_qualifier(
        self, select_context: dict, qualifier: Optional[str]
    ) -> List[dict]:
        """Return relations that match the qualifier, or all if qualifier missing."""
        relations = select_context.get("relations", [])
        if not qualifier:
            return relations
        qualifier_lower = qualifier.lower()
        return [
            relation
            for relation in relations
            if (relation.get("alias") or "").lower() == qualifier_lower
        ]

    def _filter_redundant_ambiguity(self, columns: List[Column]) -> List[Column]:
        """Drop ambiguous columns when concrete provenance already exists."""
        grouped: Dict[str, List[Column]] = {}
        for column in columns:
            grouped.setdefault(column.col_name.lower(), []).append(column)

        filtered: List[Column] = []
        for name, entries in grouped.items():
            unambiguous_tables = {
                column.potential_tables[0]
                for column in entries
                if len(column.potential_tables) == 1
            }

            for column in entries:
                if len(column.potential_tables) <= 1:
                    filtered.append(column)
                    continue

                intersection = [
                    table
                    for table in column.potential_tables
                    if table in unambiguous_tables
                ]
                if intersection:
                    column.potential_tables = sorted(set(intersection))

                if (
                    len(column.potential_tables) > 1
                    and column.potential_tables
                    and all(
                        table in unambiguous_tables for table in column.potential_tables
                    )
                ):
                    continue

                filtered.append(column)

        unique: List[Column] = []
        seen_pairs: set[tuple[str, tuple[str, ...]]] = set()
        for column in filtered:
            key = (column.col_name.lower(), tuple(column.potential_tables))
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            unique.append(column)

        return unique

    def _select_context(
        self,
        select: Optional[exp.Select],
        table_context: Optional[Dict[str, List[str]]] = None,
        column_context: Optional[Dict[str, Dict[str, Dict[str, List[str]]]]] = None,
    ) -> dict:
        """Return cached relation metadata for ``select`` to avoid recomputation."""
        if select is None:
            return {"relations": [], "default_tables": self._source_tables[:]}

        cache_key = id(select)
        if cache_key in self._select_context_cache:
            return self._select_context_cache[cache_key]

        context = self._build_select_context(
            select,
            table_context or self._cte_context,
            column_context or self._cte_column_lineage,
        )
        self._select_context_cache[cache_key] = context
        return context

    def _build_select_context(
        self,
        select: exp.Select,
        table_context: Dict[str, List[str]],
        column_context: Dict[str, Dict[str, Dict[str, List[str]]]],
    ) -> dict:
        """Construct descriptors for FROM + JOIN relations in a SELECT."""
        relations: List[dict] = []
        default_tables: List[str] = []

        from_clause = select.args.get("from")
        if from_clause:
            descriptor = self._relation_descriptor(
                from_clause.this, table_context, column_context
            )
            if descriptor:
                relations.append(descriptor)
                default_tables = self._merge_sources(
                    default_tables, descriptor["tables"]
                )

        for join in select.args.get("joins") or []:
            descriptor = self._relation_descriptor(
                join.args.get("this"), table_context, column_context
            )
            if descriptor:
                relations.append(descriptor)
                default_tables = self._merge_sources(
                    default_tables, descriptor["tables"]
                )

        projection_lineage: Dict[str, Dict[str, Any]] = {}
        projection_context = {"relations": relations, "default_tables": default_tables}
        for projection in select.expressions or []:
            output_name = self._output_column_name(projection)
            if not output_name:
                continue
            tables = self._tables_for_projection(projection, projection_context)
            projection_dependencies = self._projection_lineage(
                projection, projection_context
            )
            if projection_dependencies:
                projection_lineage[output_name.lower()] = {
                    "lineage": projection_dependencies
                }

        return {
            "relations": relations,
            "default_tables": default_tables,
            "projection_lineage": projection_lineage,
        }

    def _relation_descriptor(
        self,
        relation: Optional[exp.Expression],
        table_context: Dict[str, List[str]],
        column_context: Dict[str, Dict[str, Dict[str, List[str]]]],
    ) -> Optional[dict]:
        """Return alias/tables/columns for a relation expression."""
        if relation is None:
            return None

        if isinstance(relation, exp.Table):
            alias_token = relation.args.get("alias")
            alias = relation.alias or relation.name
            tables = self._sources_for_table_reference(relation, table_context)
            column_lineage = {}
            if alias_token and alias:
                column_lineage = column_context.get(alias.lower(), column_lineage)
            has_qualifier = bool(
                self._identifier_name(relation.args.get("db"))
                or self._identifier_name(relation.args.get("catalog"))
            )
            if relation.name and not has_qualifier:
                column_lineage = column_context.get(
                    relation.name.lower(), column_lineage
                )
            return {
                "alias": alias,
                "tables": tables,
                "columns": column_lineage,
            }

        if isinstance(relation, exp.Subquery):
            alias = relation.alias
            tables = self._tables_for_subexpression(relation.this, table_context)
            columns = self._columns_for_subexpression(
                relation.this, table_context, column_context
            )
            return {
                "alias": alias,
                "tables": tables,
                "columns": columns,
            }

        if isinstance(relation, exp.Identifier):
            alias = relation.this
            tables = table_context.get(alias.lower(), [])
            columns = column_context.get(alias.lower(), {})
            return {
                "alias": alias,
                "tables": tables,
                "columns": columns,
            }

        return None

    def _output_column_name(self, projection: exp.Expression) -> Optional[str]:
        """Best-effort name extraction for SELECT projection nodes."""
        if hasattr(projection, "alias") and projection.alias:
            return projection.alias
        if isinstance(projection, exp.Alias):
            return projection.alias_or_name
        if isinstance(projection, exp.Column):
            return projection.name
        if isinstance(projection, exp.Identifier):
            return projection.name
        return (
            projection.alias_or_name if hasattr(projection, "alias_or_name") else None
        )

    def _tables_for_projection(
        self, projection: exp.Expression, context: dict
    ) -> List[str]:
        """Resolve source tables feeding a projection expression."""
        sources: List[str] = []
        relations = context.get("relations", [])
        default_tables = context.get("default_tables", [])
        column_names = set()
        for column in projection.find_all(exp.Column):
            column_names.add(column.name.lower())
            tables = self._resolve_column_sources(
                column.table,
                context,
                column.name,
            )
            sources = self._merge_sources(sources, tables)

        if sources:
            return sources

        if len(relations) == 1 and column_names:
            relation_columns = relations[0].get("columns") or {}
            matches = []
            for name in column_names:
                entry = relation_columns.get(name)
                if entry:
                    matches.extend(entry.get("tables", []))
            if matches:
                return self._merge_sources([], matches)

        return list(default_tables)

    def _projection_lineage(self, projection: exp.Expression, context: dict) -> List[dict]:
        """Return normalized column lineage entries for a projection expression."""
        dependencies: List[dict] = []
        seen: set[tuple[str, tuple[str, ...]]] = set()

        for column in projection.find_all(exp.Column):
            column_name = column.name
            if not column_name:
                continue
            tables = self._resolve_column_sources(
                column.table,
                context,
                column.name,
            )
            key = (column_name.lower(), tuple(tables))
            if key in seen:
                continue
            seen.add(key)
            entry: Dict[str, Any] = {
                "name": column_name,
                "tables": list(tables),
            }
            nested_lineage = self._column_lineage_for_reference(
                column.table,
                context,
                column_name,
            )
            if nested_lineage:
                entry["lineage"] = nested_lineage
            dependencies.append(entry)

        return dependencies

    def _column_lineage_for_reference(
        self,
        qualifier: Optional[str],
        select_context: Optional[dict],
        column_name: Optional[str],
    ) -> List[dict]:
        """Return normalized lineage entries for a column reference if known."""
        if not column_name:
            return []
        select_context = select_context or {}
        column_key = column_name.lower()
        matches: List[dict] = []
        seen: set[tuple[str, tuple[str, ...]]] = set()

        def extend_from_entry(entry: Optional[dict]) -> None:
            if not entry:
                return
            normalized = self._normalize_lineage_entries(entry.get("lineage"))
            for component in normalized:
                name = component.get("name")
                if not name:
                    continue
                tables = tuple(component.get("tables") or [])
                key = (name.lower(), tables)
                if key in seen:
                    continue
                seen.add(key)
                matches.append(component)

        if qualifier:
            for relation in self._relations_for_qualifier(select_context, qualifier):
                columns = relation.get("columns") or {}
                extend_from_entry(columns.get(column_key))

            alias_lineage = self._alias_column_lineage.get(qualifier.lower())
            if alias_lineage:
                extend_from_entry(alias_lineage.get(column_key))
            return matches

        for relation in select_context.get("relations", []):
            columns = relation.get("columns") or {}
            extend_from_entry(columns.get(column_key))
        projection_lineage = select_context.get("projection_lineage", {})
        extend_from_entry(projection_lineage.get(column_key))
        return matches

    def _build_lineage_columns(
        self, entries: Optional[List[dict]]
    ) -> List[Column]:
        """Convert normalized lineage metadata into Column objects."""
        lineage_columns: List[Column] = []
        for entry in entries or []:
            name = entry.get("name")
            if not name:
                continue
            tables = list(entry.get("tables") or [])
            nested_entries = entry.get("lineage")
            nested_columns = self._build_lineage_columns(nested_entries)
            lineage_columns.append(
                Column(
                    col_name=name,
                    potential_tables=tables,
                    lineage=nested_columns or None,
                )
            )
        return lineage_columns

    def _normalize_lineage_entries(
        self, entries: Optional[List[Any]]
    ) -> List[Dict[str, Any]]:
        """Ensure lineage entries are stored using primitive dict structures."""
        normalized: List[Dict[str, Any]] = []
        for entry in entries or []:
            if isinstance(entry, Column):
                name = entry.col_name
                tables = list(entry.potential_tables)
                nested = (
                    self._normalize_lineage_entries(entry.lineage)
                    if entry.lineage
                    else []
                )
            elif isinstance(entry, dict):
                name = entry.get("name")
                tables = list(entry.get("tables") or [])
                nested = self._normalize_lineage_entries(entry.get("lineage"))
            else:
                continue
            if not name:
                if nested:
                    normalized.extend(nested)
                continue
            normalized_entry: Dict[str, Any] = {"name": name, "tables": tables}
            if nested:
                normalized_entry["lineage"] = nested
            normalized.append(normalized_entry)
        return normalized

    def _lineage_leaf_entries(
        self, entries: Optional[List[Any]]
    ) -> List[Dict[str, Any]]:
        """Return only terminal lineage entries with concrete table mappings."""
        leaves: List[Dict[str, Any]] = []
        for entry in self._normalize_lineage_entries(entries):
            nested = entry.get("lineage")
            if nested:
                leaves.extend(self._lineage_leaf_entries(nested))
                continue
            name = entry.get("name")
            tables = list(entry.get("tables") or [])
            if not name or not tables:
                continue
            leaves.append({"name": name, "tables": tables})
        return leaves
