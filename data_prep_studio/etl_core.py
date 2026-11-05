from __future__ import annotations

import csv
import io
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests


DEFAULT_MISSING_TOKENS = ["", "na", "n/a", "null", "none", "nan"]
SUPPORTED_TYPES = ("text", "integer", "decimal", "date", "boolean")


class ETLError(Exception):
    def __init__(self, message: str, hint: str = "Check the input and try again.") -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint


@dataclass
class LoadResult:
    df: pd.DataFrame
    warnings: list[str] = field(default_factory=list)
    kind: str = "file"


@dataclass
class ColumnSpec:
    source: str
    name: str
    detected: str
    dtype: str
    keep: bool = True


@dataclass
class SchemaResult:
    typed: pd.DataFrame
    invalid: pd.DataFrame
    missing: pd.DataFrame
    warnings: list[str] = field(default_factory=list)


@dataclass
class StepLog:
    step: str
    message: str
    rows_before: int
    rows_after: int
    removed: int = 0


@dataclass
class JobResult:
    success: bool
    output: pd.DataFrame | None
    rejected: pd.DataFrame
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    step_log: list[StepLog] = field(default_factory=list)
    source_missing: dict[str, int] = field(default_factory=dict)


@dataclass
class AuthConfig:
    kind: str = "none"
    token: str = ""
    header_name: str = ""
    header_value: str = ""
    username: str = ""
    password: str = ""

    def headers(self) -> dict[str, str]:
        if self.kind == "bearer":
            if not self.token:
                raise ETLError("Enter an API token before connecting.", "Add the token in the authentication settings.")
            return {"Authorization": f"Bearer {self.token}"}
        if self.kind == "api_key":
            if not self.header_name or not self.header_value:
                raise ETLError("Enter both the API-key header name and value.", "Complete the authentication settings.")
            return {self.header_name: self.header_value}
        return {}


@dataclass
class PushConfig:
    url: str
    auth: AuthConfig = field(default_factory=AuthConfig)
    mapping: dict[str, str] = field(default_factory=dict)
    wrapper_key: str = ""
    batch_size: int = 100
    method: str = "POST"
    timeout: int = 15


@dataclass
class PushResult:
    success: bool
    message: str
    records_accepted: int = 0
    requests_total: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)
    stopped_early: bool = False


def _is_missing(value: object, tokens: set[str]) -> bool:
    return pd.isna(value) or (isinstance(value, str) and value.strip().casefold() in tokens)


def _json_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _flatten(value: dict[str, Any], prefix: str = "") -> dict[str, object]:
    flat: dict[str, object] = {}
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            flat.update(_flatten(item, name))
        else:
            flat[name] = _json_value(item)
    return flat


def _records_from_json(value: Any, records_path: str = "") -> list[dict[str, Any]]:
    if records_path:
        current = value
        for part in records_path.split("."):
            if not isinstance(current, dict) or part not in current:
                raise ETLError(f"Records path '{records_path}' was not found in the JSON response.",
                               "Check the response shape or update the records path.")
            current = current[part]
        value = current
    elif isinstance(value, dict):
        for key in ("data", "items", "records", "rows", "results"):
            if key in value and isinstance(value[key], list):
                value = value[key]
                break
        else:
            if isinstance(value.get("data"), dict):
                nested = value["data"]
                for key in ("items", "records", "rows", "results"):
                    if isinstance(nested.get(key), list):
                        value = nested[key]
                        break
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list) or not value or not all(isinstance(row, dict) for row in value):
        raise ETLError("The JSON data must contain a non-empty list of records.",
                       "Choose a records path that points to an array of objects.")
    return value


def _frame_from_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    flattened = [_flatten(row) for row in records]
    columns = list(dict.fromkeys(key for row in flattened for key in row))
    return pd.DataFrame([{key: row.get(key, "") for key in columns} for row in flattened]).fillna("")


def load_file(filename: str, data: bytes, records_path: str = "") -> LoadResult:
    suffix = filename.rsplit(".", 1)[-1].casefold() if "." in filename else ""
    if not data:
        raise ETLError("The uploaded file is empty.", "Choose a file that contains a header and at least one row.")
    if suffix not in {"csv", "json", "jsonl", "ndjson", "xlsx"}:
        raise ETLError("These file types aren't supported.", "Use CSV, JSON, JSON Lines, or an Excel .xlsx file.")

    warnings: list[str] = []
    try:
        if suffix == "csv":
            text = None
            for encoding in ("utf-8-sig", "cp1252"):
                try:
                    text = data.decode(encoding)
                    if encoding != "utf-8-sig":
                        warnings.append(f"File text was read using {encoding} encoding.")
                    break
                except UnicodeDecodeError:
                    continue
            if text is None:
                raise ETLError("The CSV encoding could not be read.", "Save the file as UTF-8 and upload it again.")
            sample = text[:8192]
            try:
                delimiter = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
            except csv.Error:
                delimiter = ","
            rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
            if len(rows) < 2:
                raise ETLError("The CSV file has a header but no data rows.", "Add at least one data row.")
            headers = []
            seen: dict[str, int] = {}
            for index, header in enumerate(rows[0], 1):
                base = header.strip() or f"column_{index}"
                seen[base] = seen.get(base, 0) + 1
                headers.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
            records = []
            for line_number, row in enumerate(rows[1:], 2):
                if not row:
                    continue
                if len(row) != len(headers):
                    raise ETLError(f"CSV row {line_number} has {len(row)} values; expected {len(headers)}.",
                                   "Check for unescaped delimiters or missing values in that row.")
                records.append(row)
            if not records:
                raise ETLError("The CSV file has a header but no data rows.", "Add at least one data row.")
            df = pd.DataFrame(records, columns=headers, dtype=object)
        elif suffix in {"jsonl", "ndjson"}:
            try:
                records = [json.loads(line) for line in data.decode("utf-8-sig").splitlines() if line.strip()]
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ETLError("The JSON Lines file could not be parsed.", "Check that each line contains one valid JSON object.") from exc
            df = _frame_from_records(_records_from_json(records, records_path))
        elif suffix == "json":
            try:
                value = json.loads(data.decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ETLError("The JSON file could not be parsed.", "Check the JSON syntax and try again.") from exc
            df = _frame_from_records(_records_from_json(value, records_path))
        else:
            try:
                sheets = pd.ExcelFile(io.BytesIO(data))
                df = pd.read_excel(sheets, sheet_name=0, dtype=object).fillna("")
            except Exception as exc:
                raise ETLError("The Excel workbook could not be opened.", "Upload a valid .xlsx workbook.") from exc
            if len(df) == 0:
                raise ETLError("The Excel sheet has no data rows.", "Choose a sheet with at least one data row.")
            df.columns = [str(column).strip() or f"column_{i + 1}" for i, column in enumerate(df.columns)]
            df = df.map(_json_value)
        df = df.map(lambda value: value if isinstance(value, str) else _json_value(value))
        if df.empty or len(df.columns) == 0:
            raise ETLError("The file doesn't contain tabular data.", "Check the selected file and try again.")
        return LoadResult(df=df.reset_index(drop=True), warnings=warnings)
    except ETLError:
        raise
    except (ValueError, csv.Error, pd.errors.ParserError) as exc:
        raise ETLError("The file could not be read as a table.", "Check its format and make sure it has consistent columns.") from exc


def list_excel_sheets(data: bytes) -> list[str]:
    try:
        return pd.ExcelFile(io.BytesIO(data)).sheet_names
    except Exception as exc:
        raise ETLError("The Excel workbook could not be opened.", "Upload a valid .xlsx workbook.") from exc


def _numeric_candidate(value: object) -> str:
    return re.sub(r"[$€£,\s]", "", str(value)).strip()


def detect_type(series: pd.Series, missing_tokens: list[str] | set[str] | None = None,
                dayfirst: bool = False) -> str:
    tokens = {token.casefold() for token in (missing_tokens or DEFAULT_MISSING_TOKENS)}
    values = [str(value).strip() for value in series.tolist() if not _is_missing(value, tokens)]
    if not values:
        return "text"
    lowered = {value.casefold() for value in values}
    if lowered <= {"true", "false", "yes", "no", "y", "n"}:
        return "boolean"
    if all(re.fullmatch(r"0\d+", value) for value in values):
        return "text"
    if any(len(_numeric_candidate(value).lstrip("+-").split(".")[0]) > 18 for value in values):
        return "text"
    nums = pd.to_numeric(pd.Series([_numeric_candidate(value) for value in values]), errors="coerce")
    numeric_ratio = float(nums.notna().mean())
    if numeric_ratio >= 0.9:
        if (nums.dropna() % 1 == 0).all():
            return "integer"
        return "decimal"
    dates = pd.to_datetime(pd.Series(values), errors="coerce", format="mixed", dayfirst=dayfirst)
    if float(dates.notna().mean()) >= 0.8:
        return "date"
    return "text"


def infer_schema(df: pd.DataFrame, missing_tokens: list[str] | None = None,
                 dayfirst: bool = False) -> list[ColumnSpec]:
    tokens = missing_tokens or DEFAULT_MISSING_TOKENS
    return [ColumnSpec(str(column), str(column), detect_type(df[column], tokens, dayfirst),
                       detect_type(df[column], tokens, dayfirst), True) for column in df.columns]


def convert_series(series: pd.Series, dtype: str, missing_tokens: list[str] | set[str] | None = None,
                   dayfirst: bool = False) -> tuple[pd.Series, pd.Series, pd.Series]:
    tokens = {token.casefold() for token in (missing_tokens or DEFAULT_MISSING_TOKENS)}
    missing = series.map(lambda value: _is_missing(value, tokens)).astype(bool)
    raw = series.astype("string").str.strip()
    invalid = pd.Series(False, index=series.index, dtype=bool)
    if dtype == "text":
        converted = raw.mask(missing, pd.NA).astype("string")
    elif dtype in {"integer", "decimal"}:
        normalized = raw.str.replace(r"[$€£,\s]", "", regex=True)
        numbers = pd.to_numeric(normalized.mask(missing), errors="coerce")
        invalid = (~missing & numbers.isna()).astype(bool)
        if dtype == "integer":
            invalid |= (~missing & numbers.notna() & (numbers % 1 != 0))
            converted = numbers.mask(invalid).round().astype("Int64")
        else:
            converted = numbers.mask(invalid).astype("Float64")
    elif dtype == "date":
        converted = pd.to_datetime(raw.mask(missing), errors="coerce", format="mixed", dayfirst=dayfirst)
        invalid = (~missing & converted.isna()).astype(bool)
    elif dtype == "boolean":
        normalized = raw.str.casefold()
        truth = {"true": True, "yes": True, "y": True, "1": True,
                 "false": False, "no": False, "n": False, "0": False}
        converted = normalized.map(truth).astype("boolean")
        invalid = (~missing & converted.isna()).astype(bool)
    else:
        raise ETLError(f"Unknown field type '{dtype}'.", "Choose text, integer, decimal, date, or boolean.")
    return converted, invalid, missing


def apply_schema(df: pd.DataFrame, specs: list[ColumnSpec], missing_tokens: list[str] | None = None,
                 dayfirst: bool = False) -> SchemaResult:
    kept = [spec for spec in specs if spec.keep]
    names = [spec.name.strip() for spec in kept]
    if any(not name for name in names):
        raise ETLError("Field names can't be empty.", "Enter a name for each field you want to keep.")
    if len(set(name.casefold() for name in names)) != len(names):
        raise ETLError("Two or more fields have the same name.", "Give every kept field a unique name.")
    typed = pd.DataFrame(index=df.index)
    invalid = pd.DataFrame(index=df.index)
    missing = pd.DataFrame(index=df.index)
    for spec in kept:
        if spec.source not in df:
            raise ETLError(f"Source field '{spec.source}' was not found.", "Review the uploaded file and field settings.")
        converted, bad, empty = convert_series(df[spec.source], spec.dtype, missing_tokens, dayfirst)
        typed[spec.name] = converted
        invalid[spec.name] = bad
        missing[spec.name] = empty
    return SchemaResult(typed, invalid, missing)


def issues_table(df: pd.DataFrame, specs: list[ColumnSpec], result: SchemaResult) -> tuple[pd.DataFrame, int]:
    rows: list[dict[str, Any]] = []
    for spec in specs:
        if not spec.keep or spec.source not in df:
            continue
        invalid_mask = result.invalid[spec.name]
        for index in df.index[invalid_mask]:
            rows.append({"field": spec.source, "row": int(index) + 2, "value": str(df.at[index, spec.source]),
                         "problem": f"Cannot read value as {spec.dtype}."})
    return pd.DataFrame(rows, columns=["field", "row", "value", "problem"]), int(result.invalid.any(axis=1).sum())


def profile_table(df: pd.DataFrame, specs: list[ColumnSpec], result: SchemaResult) -> pd.DataFrame:
    rows = []
    for spec in specs:
        if spec.source not in df:
            continue
        series = df[spec.source]
        rows.append({"Field": spec.source, "Type": spec.dtype, "Missing": int(result.missing[spec.name].sum()),
                     "Invalid": int(result.invalid[spec.name].sum()),
                     "Extra spaces": int(series.map(lambda value: isinstance(value, str) and value != value.strip()).sum()),
                     "Unique": int(series.nunique(dropna=True))})
    return pd.DataFrame(rows)


def make_step(kind: str, **parameters: Any) -> dict[str, Any]:
    return {"kind": kind, **parameters}


def describe_step(step: dict[str, Any]) -> str:
    kind = step.get("kind")
    if kind == "filter":
        return f"{step.get('mode', 'keep').title()} rows where {step.get('column')} {step.get('op')} {step.get('value', '')}"
    if kind == "select":
        return "Select and reorder fields: " + ", ".join(step.get("columns", []))
    if kind == "text":
        return f"Standardise text in {', '.join(step.get('columns', []))}: {', '.join(step.get('ops', []))}"
    if kind == "date_format":
        return f"Format {', '.join(step.get('columns', []))} as {step.get('format', '%Y-%m-%d')}"
    if kind == "missing":
        return f"{step.get('action', 'handle')} missing values in {', '.join(step.get('columns', [])) or 'all fields'}"
    if kind == "dedupe":
        return f"Remove duplicates using {', '.join(step.get('columns', [])) or 'all fields'} (keep {step.get('keep', 'first')})"
    if kind == "calc":
        if step.get("mode") == "concat":
            return f"Create {step.get('name')} by joining {', '.join(step.get('columns', []))}"
        return f"Create {step.get('name')} from {step.get('left')} {step.get('op')} {step.get('right')}"
    return f"Unknown step: {kind}"


def validate_steps(columns: list[str], steps: list[dict[str, Any]]) -> list[str]:
    current = list(columns)
    problems = []
    for index, step in enumerate(steps, 1):
        kind = step.get("kind")
        refs = []
        if kind == "filter":
            refs = [step.get("column")]
        elif kind in {"select", "text", "date_format", "missing", "dedupe"}:
            refs = step.get("columns", []) if kind != "filter" else []
        elif kind == "calc":
            refs = [step.get("left")] if step.get("mode") == "arithmetic" else step.get("columns", [])
            if step.get("mode") == "arithmetic" and step.get("right_kind") == "column":
                refs.append(step.get("right"))
        for name in refs:
            if name and name not in current:
                problems.append(f"Step {index}: field '{name}' isn't available at this point.")
        if kind == "select":
            current = list(step.get("columns", []))
        elif kind == "calc" and step.get("name"):
            current.append(step["name"])
    return problems


def _coerce_filter_value(series: pd.Series, value: str) -> object:
    if pd.api.types.is_numeric_dtype(series.dtype):
        try:
            return float(_numeric_candidate(value))
        except ValueError as exc:
            raise ETLError(f"'{value}' isn't a number.", "Enter a number that matches the selected field.") from exc
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            raise ETLError(f"'{value}' isn't a valid date.", "Enter a date such as 2024-12-31.")
        return parsed
    if pd.api.types.is_bool_dtype(series.dtype):
        return str(value).casefold() in {"true", "yes", "y", "1"}
    return value.casefold()


def _run_step(df: pd.DataFrame, step: dict[str, Any]) -> tuple[pd.DataFrame, pd.Series, str]:
    kind = step.get("kind")
    before = len(df)
    keep = pd.Series(True, index=df.index, dtype=bool)
    if kind == "filter":
        column, op, value = step.get("column"), step.get("op"), str(step.get("value", ""))
        if column not in df:
            raise ETLError(f"Field '{column}' isn't available.")
        series = df[column]
        if op == "is_missing":
            matches = series.isna() | series.astype("string").str.strip().eq("").fillna(False)
        elif op == "equals" and pd.api.types.is_bool_dtype(series.dtype):
            target = str(value).casefold() in {"true", "yes", "y", "1"}
            matches = series.eq(target).fillna(False)
        elif op in {"contains", "equals"} and not pd.api.types.is_numeric_dtype(series.dtype):
            text = series.astype("string").str.casefold()
            matches = text.str.contains(value.casefold(), regex=False, na=False) if op == "contains" else text.eq(value.casefold()).fillna(False)
        else:
            if op in {"gt", "gte", "lt", "lte"} and not (
                pd.api.types.is_numeric_dtype(series.dtype) or pd.api.types.is_datetime64_any_dtype(series.dtype)
            ):
                raise ETLError(f"Field '{column}' must be a number or date for this comparison.")
            target = _coerce_filter_value(series, value)
            if op == "equals":
                matches = series.eq(target).fillna(False)
            elif op == "gt":
                matches = series.gt(target).fillna(False)
            elif op == "gte":
                matches = series.ge(target).fillna(False)
            elif op == "lt":
                matches = series.lt(target).fillna(False)
            elif op == "lte":
                matches = series.le(target).fillna(False)
            else:
                raise ETLError(f"Unknown filter operation '{op}'.")
        keep = matches if step.get("mode", "keep") == "keep" else ~matches
        out = df.loc[keep].copy()
    elif kind == "select":
        columns = step.get("columns", [])
        if any(column not in df for column in columns):
            raise ETLError("One or more selected fields are unavailable.")
        out = df.loc[:, columns].copy()
    elif kind == "text":
        out = df.copy()
        for column in step.get("columns", []):
            if column not in out:
                raise ETLError(f"Field '{column}' isn't available.")
            values = out[column].astype("string")
            for operation in step.get("ops", []):
                if operation == "trim":
                    values = values.str.strip()
                elif operation == "collapse":
                    values = values.str.replace(r"\s+", " ", regex=True)
                elif operation == "lower":
                    values = values.str.lower()
                elif operation == "upper":
                    values = values.str.upper()
                elif operation == "title":
                    values = values.str.title()
                else:
                    raise ETLError(f"Unknown text operation '{operation}'.")
            out[column] = values
    elif kind == "date_format":
        out = df.copy()
        fmt = step.get("format", "%Y-%m-%d")
        for column in step.get("columns", []):
            if column not in out or not pd.api.types.is_datetime64_any_dtype(out[column].dtype):
                raise ETLError(f"Field '{column}' must be a date before formatting.")
            try:
                out[column] = out[column].dt.strftime(fmt).astype("string")
            except (ValueError, TypeError) as exc:
                raise ETLError(f"Date format '{fmt}' isn't valid.", "Use a Python date format such as %Y-%m-%d.") from exc
    elif kind == "missing":
        columns = step.get("columns") or list(df.columns)
        out = df.copy()
        action = step.get("action")
        if action == "drop_rows":
            keep = ~out[columns].isna().any(axis=1) if columns else pd.Series(True, index=out.index)
            out = out.loc[keep].copy()
        elif action == "fill_value":
            for column in columns:
                value = step.get("value", "")
                if pd.api.types.is_numeric_dtype(out[column].dtype):
                    try:
                        value = float(_numeric_candidate(value))
                        if pd.api.types.is_integer_dtype(out[column].dtype) and value % 1:
                            raise ValueError
                        value = int(value) if value % 1 == 0 else value
                    except ValueError as exc:
                        raise ETLError(f"'{step.get('value')}' isn't a valid value for {column}.",
                                       "Enter a value that matches the field type.") from exc
                out[column] = out[column].fillna(value)
        elif action in {"fill_mean", "fill_median", "fill_mode"}:
            for column in columns:
                if action != "fill_mode" and not pd.api.types.is_numeric_dtype(out[column].dtype):
                    raise ETLError(f"{action.replace('_', ' ').title()} can only be used with number fields.")
                if action == "fill_mean":
                    value = out[column].mean()
                elif action == "fill_median":
                    value = out[column].median()
                else:
                    modes = out[column].mode(dropna=True)
                    value = modes.iloc[0] if not modes.empty else pd.NA
                out[column] = out[column].fillna(value)
        else:
            raise ETLError(f"Unknown missing-value action '{action}'.")
    elif kind == "dedupe":
        columns = step.get("columns") or list(df.columns)
        keep = ~df.duplicated(subset=columns, keep=step.get("keep", "first"))
        out = df.loc[keep].copy()
    elif kind == "calc":
        name = str(step.get("name", "")).strip()
        if not name or (name in df.columns and name not in step.get("replace", [])):
            raise ETLError("Enter a new field name that doesn't already exist.")
        out = df.copy()
        if step.get("mode") == "concat":
            columns = step.get("columns", [])
            if any(column not in df for column in columns):
                raise ETLError("One or more fields for the calculated value are unavailable.")
            separator = str(step.get("separator", ""))
            out[name] = df[columns].astype("string").fillna("").agg(separator.join, axis=1)
        else:
            left = step.get("left")
            right = step.get("right")
            decimals = step.get("decimals")
            if left not in df:
                raise ETLError(f"Field '{left}' isn't available.")
            if not pd.api.types.is_numeric_dtype(df[left].dtype):
                raise ETLError(f"Field '{left}' must be numeric for arithmetic.")
            a = pd.to_numeric(df[left], errors="coerce")
            if step.get("right_kind") == "column":
                if right not in df or not pd.api.types.is_numeric_dtype(df[right].dtype):
                    raise ETLError(f"Field '{right}' must be numeric for arithmetic.")
                b = pd.to_numeric(df[right], errors="coerce")
            else:
                try:
                    raw_right = float(step.get("right", 0))
                    b = int(raw_right) if decimals is None and raw_right.is_integer() else raw_right
                except (TypeError, ValueError) as exc:
                    raise ETLError("The calculated number isn't valid.", "Enter a numeric value.") from exc
            op = step.get("op")
            if op == "+":
                values = a + b
            elif op == "-":
                values = a - b
            elif op == "*":
                values = a * b
            elif op == "/":
                zero = (b == 0) if isinstance(b, (int, float)) else b.eq(0)
                if isinstance(zero, bool):
                    zero = pd.Series(zero, index=df.index)
                keep = ~zero.fillna(False)
                values = a / b
            else:
                raise ETLError(f"Unknown arithmetic operation '{op}'.")
            if decimals is not None:
                values = values.round(int(decimals))
            out[name] = values
        out = out.loc[keep].copy()
    else:
        raise ETLError(f"Unknown transformation '{kind}'.")
    removed = before - len(out)
    if kind == "filter" and step.get("mode") == "remove":
        removed = before - len(out)
    return out, keep, f"{describe_step(step)}; {removed} rows removed."


def run_job(df: pd.DataFrame, specs: list[ColumnSpec], steps: list[dict[str, Any]],
            missing_tokens: list[str] | None = None, dayfirst: bool = False,
            invalid_handling: str = "reject") -> JobResult:
    try:
        initial = apply_schema(df.copy(deep=True), specs, missing_tokens, dayfirst)
        source_missing = {
            spec.source: int(initial.missing[spec.name].sum())
            for spec in specs if spec.keep and spec.name in initial.missing
        }
        bad_rows = initial.invalid.any(axis=1) if len(initial.invalid.columns) else pd.Series(False, index=df.index)
        rejected_records = []
        if invalid_handling == "reject":
            for index in df.index[bad_rows]:
                bad_fields = [name for name in initial.invalid.columns if bool(initial.invalid.at[index, name])]
                record: dict[str, Any] = {"source_row": int(index) + 1,
                                          "problem": "Unreadable values in " + ", ".join(bad_fields)}
                record.update({f"source: {column}": df.at[index, column] for column in df.columns})
                rejected_records.append(record)
            current = initial.typed.loc[~bad_rows].copy()
        elif invalid_handling == "blank":
            current = initial.typed.copy()
            if bad_rows.any():
                initial.warnings.append(f"{int(bad_rows.sum())} row(s) contain unreadable values; invalid cells were left blank.")
        else:
            raise ETLError("Unknown unreadable-value handling option.", "Choose set aside or leave blank.")
        source_rows = pd.Series(df.index + 1, index=df.index)
        row_numbers = source_rows.loc[current.index].copy()
        rejected = pd.DataFrame(rejected_records)
        logs: list[StepLog] = []
        validation = validate_steps(list(current.columns), steps)
        if validation:
            return JobResult(False, None, rejected, validation, initial.warnings, logs, source_missing)
        for index, step in enumerate(steps, 1):
            before = len(current)
            prior_index = current.index
            try:
                updated, keep_mask, message = _run_step(current, step)
            except ETLError as exc:
                return JobResult(False, None, rejected, [f"Step {index}: {exc.message}"], initial.warnings, logs, source_missing)
            if step.get("kind") == "calc" and step.get("op") == "/" and step.get("right_kind") == "column":
                dropped = prior_index.difference(updated.index)
                for row_index in dropped:
                    rec: dict[str, Any] = {"source_row": int(row_numbers.loc[row_index]),
                                           "problem": "Division by zero in calculated field"}
                    rec.update({f"source: {column}": df.iloc[int(row_numbers.loc[row_index]) - 2][column] for column in df.columns})
                    rejected = pd.concat([rejected, pd.DataFrame([rec])], ignore_index=True)
            current = updated
            row_numbers = row_numbers.loc[current.index]
            logs.append(StepLog(describe_step(step), message, before, len(current), before - len(current)))
        return JobResult(True, current.reset_index(drop=True), rejected.reset_index(drop=True),
                         [], initial.warnings, logs, source_missing)
    except ETLError as exc:
        return JobResult(False, None, pd.DataFrame(), [exc.message], [])


def compare_table(result: JobResult) -> pd.DataFrame:
    output = result.output if result.output is not None else pd.DataFrame()
    rejected_source_columns = [column for column in result.rejected if column.startswith("source: ")]
    source_empty: dict[str, int] = dict(result.source_missing)
    for column in rejected_source_columns:
        source_empty.setdefault(column.removeprefix("source: "), 0)
    for column in output.columns:
        source_empty.setdefault(column, int(output[column].isna().sum()))
    return pd.DataFrame([
        {"Field": column, "Empty in source": source_empty.get(column, 0),
         "Empty in output": int(output[column].isna().sum()) if column in output else 0}
        for column in dict.fromkeys([*source_empty, *output.columns])
    ])


def to_csv_bytes(df: pd.DataFrame, excel_friendly: bool = False) -> bytes:
    text = df.to_csv(index=False, lineterminator="\n")
    return (("\ufeff" if excel_friendly else "") + text).encode("utf-8")


def _json_safe(value: object) -> object:
    if value is None or value is pd.NA or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.date().isoformat() if isinstance(value, (pd.Timestamp, datetime)) else value.isoformat()
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def to_json_bytes(df: pd.DataFrame) -> bytes:
    records = [{str(column): _json_safe(value) for column, value in row.items()}
               for row in df.to_dict(orient="records")]
    return json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")


def recipe_to_dict(specs: list[ColumnSpec], steps: list[dict[str, Any]],
                   missing_tokens: list[str], dayfirst: bool, invalid_handling: str) -> dict[str, Any]:
    return {"version": 1, "schema": [asdict(spec) for spec in specs], "steps": steps,
            "missing_tokens": missing_tokens, "dayfirst": dayfirst,
            "invalid_handling": invalid_handling}


def recipe_from_dict(data: dict[str, Any], detected_specs: list[ColumnSpec]) -> tuple[list[ColumnSpec], list[dict[str, Any]], dict[str, Any], list[str]]:
    if not isinstance(data, dict) or set(data) == {"nonsense"}:
        raise ETLError("This recipe doesn't contain valid settings.", "Choose a recipe exported by Data Prep Studio.")
    if data.get("version") != 1:
        raise ETLError("This recipe version isn't supported.", "Export a new recipe with this version of Data Prep Studio.")
    if not isinstance(data.get("schema"), list) or not isinstance(data.get("steps"), list):
        raise ETLError("This recipe is missing required settings.", "Choose a complete recipe JSON file.")
    found = {str(item.get("source")): item for item in data["schema"] if isinstance(item, dict)}
    specs = []
    for detected in detected_specs:
        saved = found.get(detected.source)
        if saved is None:
            specs.append(detected)
        else:
            dtype = saved.get("dtype", detected.dtype)
            if dtype not in SUPPORTED_TYPES:
                raise ETLError(f"Recipe type '{dtype}' isn't supported.")
            specs.append(ColumnSpec(detected.source, str(saved.get("name", detected.name)),
                                   detected.detected, dtype, bool(saved.get("keep", True))))
    steps = data["steps"]
    if not all(isinstance(step, dict) and isinstance(step.get("kind"), str) for step in steps):
        raise ETLError("The recipe contains an invalid transformation.")
    opts = {"missing_tokens": data.get("missing_tokens", DEFAULT_MISSING_TOKENS),
            "dayfirst": bool(data.get("dayfirst", False)),
            "invalid_handling": data.get("invalid_handling", "reject")}
    return specs, steps, opts, []


def parse_kv_lines(text: str, separator: str = ":", label: str = "value") -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        if separator not in line:
            raise ETLError(f"Each {label} line must contain '{separator}'.",
                           f"Use one {label} per line in the form name{separator} value.")
        key, value = line.split(separator, 1)
        if not key.strip() or not value.strip():
            raise ETLError(f"Each {label} line needs a name and a value.")
        result[key.strip()] = value.strip()
    return result


def _auth_for_request(auth: AuthConfig) -> tuple[dict[str, str], tuple[str, str] | None]:
    headers = auth.headers()
    basic = None
    if auth.kind == "basic":
        if not auth.username:
            raise ETLError("Enter a username and password before connecting.", "Complete the authentication settings.")
        basic = (auth.username, auth.password)
    return headers, basic


def _check_url(url: str) -> None:
    if not url.startswith(("http://", "https://")):
        raise ETLError("Enter a valid HTTP or HTTPS URL.", "Check the URL and try again.")


def fetch_api(url: str, auth: AuthConfig | None = None, records_path: str = "",
              timeout: int = 15) -> LoadResult:
    _check_url(url)
    auth = auth or AuthConfig()
    headers, basic = _auth_for_request(auth)
    try:
        response = requests.get(url, headers=headers, auth=basic, timeout=timeout)
    except requests.RequestException as exc:
        raise ETLError("Could not connect to the API.", "Check the URL, network connection, and service availability.") from exc
    if not response.ok:
        raise ETLError(f"API request failed with HTTP {response.status_code}.",
                       "Check the URL and credentials, then try again.")
    try:
        data = response.json()
    except requests.JSONDecodeError as exc:
        raise ETLError("The API response wasn't valid JSON.", "Choose an endpoint that returns JSON records.") from exc
    records = _records_from_json(data, records_path)
    return LoadResult(_frame_from_records(records), kind="api")


def _records_for_push(df: pd.DataFrame, mapping: dict[str, str]) -> list[dict[str, Any]]:
    if not mapping or not any(str(target).strip() for target in mapping.values()):
        raise ETLError("Select at least one field to send.", "Enable a field and give it an output name.")
    targets = [target for target in mapping.values() if str(target).strip()]
    if len(targets) != len(set(targets)):
        raise ETLError("Two fields use the same output name.", "Give each sent field a unique output name.")
    if any(source not in df.columns for source in mapping):
        raise ETLError("A mapped field isn't in the output data.")
    return [
        {str(target): _json_safe(row[source]) for source, target in mapping.items() if str(target).strip()}
        for row in df.to_dict(orient="records")
    ]


def preview_payload(df: pd.DataFrame, config: PushConfig) -> object:
    records = _records_for_push(df, config.mapping)
    batch = records[:max(1, config.batch_size)]
    payload: object = batch[0] if config.batch_size == 1 and batch else batch
    return {config.wrapper_key: payload} if config.wrapper_key else payload


def push_records(df: pd.DataFrame, config: PushConfig) -> PushResult:
    _check_url(config.url)
    if config.method.upper() != "POST":
        raise ETLError("Only POST requests are supported for sending records.")
    if config.batch_size < 1:
        raise ETLError("Batch size must be at least one.")
    records = _records_for_push(df, config.mapping)
    headers, basic = _auth_for_request(config.auth)
    headers["Content-Type"] = "application/json"
    accepted = 0
    failures: list[dict[str, Any]] = []
    failed_in_row = 0
    requests_total = 0
    for start in range(0, len(records), config.batch_size):
        batch = records[start:start + config.batch_size]
        body: object = batch[0] if config.batch_size == 1 and batch else batch
        if config.wrapper_key:
            body = {config.wrapper_key: body}
        requests_total += 1
        try:
            response = requests.post(config.url, json=body, headers=headers, auth=basic, timeout=config.timeout)
            if not response.ok:
                try:
                    detail = response.json()
                    problem = str(detail.get("error", detail)) if isinstance(detail, dict) else str(detail)
                except requests.RequestException:
                    problem = response.text[:300]
                failures.append({"status": response.status_code, "problem": problem})
                failed_in_row += 1
            else:
                accepted += len(batch)
                failed_in_row = 0
        except requests.RequestException:
            failures.append({"status": "no response", "problem": "The API could not be reached."})
            failed_in_row += 1
        if failed_in_row >= 3:
            break
    stopped = failed_in_row >= 3
    success = not failures
    message = (f"accepted {accepted} record(s)." if success else
               f"accepted {accepted} record(s); {len(failures)} request(s) failed.")
    return PushResult(success, message, accepted, requests_total, failures, stopped)
