from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Column:
    # Name of the column
    col_name: str

    # List of names of potential tables that the column is sourced from.
    # If there is ambiguity in exactly which table a column comes from, include
    # all potential tables.
    potential_tables: List[str] = field(default_factory=list)

    # Lineage is a list of columns that a column is sourced from. In cases of
    # columns selected directly from source tables, lineage should be None
    lineage: Optional[List["Column"]] = None

    def as_dict(self) -> dict:
        """
        Dict Representation of the class
        """
        return {
            "name": self.col_name,
            "potential_tables": list(self.potential_tables),
        }

    def __repr__(self) -> str:
        return (
            f"Column(name='{self.col_name}', "
            f"potential_tables={self.potential_tables}, "
            f"lineage={self.lineage})"
        )

    def lineage_table_sets(self) -> dict:
        """
        Return aggregated lineage provenance split into known vs potential tables.

        known_tables includes leaf columns that resolve to a single table.
        potential_tables includes table names from leaf columns with ambiguity.
        Both lists are de-duplicated in traversal order and set to None when empty.
        """
        known_tables: List[str] = []
        potential_tables: List[str] = []
        seen_known: set[str] = set()
        seen_potential: set[str] = set()

        stack: List[Column] = [self]
        while stack:
            column = stack.pop()
            if column.lineage:
                stack.extend(column.lineage)
                continue

            tables = list(column.potential_tables or [])
            if not tables:
                continue

            if len(tables) == 1:
                table = tables[0]
                if table not in seen_known:
                    known_tables.append(table)
                    seen_known.add(table)
            else:
                for table in tables:
                    if table not in seen_potential:
                        potential_tables.append(table)
                        seen_potential.add(table)

        return {
            "known_tables": known_tables or None,
            "potential_tables": potential_tables or None,
        }

    def lineage_column_sets(self) -> dict:
        """
        Return leaf Columns split into known vs potential sources.

        known_columns are leaves with a single potential table (unambiguous).
        potential_columns are leaves with multiple potential tables (ambiguous).
        Returns None instead of empty lists for each bucket.
        """
        known_columns: List[Column] = []
        potential_columns: List[Column] = []
        seen_known: set[tuple[str, tuple[str, ...]]] = set()
        seen_potential: set[tuple[str, tuple[str, ...]]] = set()

        stack: List[Column] = [self]
        while stack:
            column = stack.pop()
            if column.lineage:
                stack.extend(column.lineage)
                continue
            tables = list(column.potential_tables or [])
            name = column.col_name
            if not name or not tables:
                continue

            key = (name.lower(), tuple(tables))
            target = known_columns if len(tables) == 1 else potential_columns
            seen = seen_known if len(tables) == 1 else seen_potential
            if key in seen:
                continue
            seen.add(key)
            target.append(Column(col_name=name, potential_tables=tables))

        return {
            "known_columns": known_columns or None,
            "potential_columns": potential_columns or None,
        }
