from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ColumnRule:
    name: str
    dtype: Literal["number", "date", "email"] | str = "text"
    required: bool = True
    allow_missing: bool = True


@dataclass(frozen=True)
class DatasetSchema:
    expected_columns: list[str]
    column_rules: list[ColumnRule]


@dataclass(frozen=True)
class ValidationIssue:
    severity: Literal["error", "warning"]
    code: str
    message: str
    column: str | None = None
    row_number: int | None = None
    value: str | None = None
