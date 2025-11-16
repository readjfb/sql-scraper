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
