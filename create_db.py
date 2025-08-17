#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate a Supabase (PostgreSQL) CREATE TABLE statement from a 3-row CSV/TSV.

Expected file layout (exactly three logical rows):
  Row 1: Human-readable descriptions (ignored except inline comments)
  Row 2: Type labels (e.g., "String", "DateTime", "Enum", "Employee list", "Enum (multi select)", etc.)
  Row 3: Column names (snake_case preferred). Empty cell => column ignored

Key rules:
- Adds `id uuid primary key default gen_random_uuid()` (independent from any sheet columns).
- Type mapping:
    Enum                   -> <column_name>           (assumes enum type exists with same name)
    Enum (multi select)    -> <column_name>[]         (array of that enum)
    Employee list          -> jsonb
    Employee list (ordered array) -> jsonb
    DateTime               -> timestamptz
    Date                   -> date
    Time                   -> time
    Currency               -> numeric(12,2)
    Decimal                -> numeric
    String/Text/Phone/Adress -> text
- Unknown types => text (emits a warning comment).
- No email/phone checks.

Usage:
    python generate_supabase_schema.py --table <table> --csv <path> [--delimiter ,|\\t|;|\\|] [--encoding utf-8-sig]
"""

from __future__ import annotations
import argparse
import csv
import io
import re
import sys
from typing import Dict, List, Tuple, Optional

# -----------------------------
# Type mapping & helpers
# -----------------------------

SIMPLE_TYPE_MAP: Dict[str, str] = {
    "string": "text",
    "adress": "text",  # keep misspelling per source; treat as text
    "text": "text",
    "phone": "text",
    "datetime": "timestamptz",
    "date": "date",
    "time": "time",
    "boolean": "boolean",
    "currency": "numeric(12,2)",
    "decimal": "numeric",
    "employee list": "jsonb",
    "employee list (ordered array)": "jsonb",
}

ENUM_LABELS = {"enum", "enum (multi select)"}
KNOWN_LABELS = set(SIMPLE_TYPE_MAP.keys()) | ENUM_LABELS

def normalize_sheet_type(label: str) -> str:
    if label is None:
        return ""
    return re.sub(r"\s+", " ", label.strip().lower())

def is_valid_identifier(name: str) -> bool:
    return bool(re.match(r"^[a-z_][a-z0-9_]*$", name or ""))

def quote_ident(ident: str) -> str:
    if is_valid_identifier(ident):
        return ident
    return '"' + ident.replace('"', '""') + '"'

def map_type(sheet_type_label: str, column_name: str) -> Tuple[str, List[str]]:
    warnings: List[str] = []
    key = normalize_sheet_type(sheet_type_label)

    if key == "enum":
        if not column_name:
            warnings.append("Enum column without a name; skipped.")
            return ("", warnings)
        return (quote_ident(column_name), warnings)

    if key == "enum (multi select)":
        if not column_name:
            warnings.append("Enum (multi select) column without a name; skipped.")
            return ("", warnings)
        return (f"{quote_ident(column_name)}[]", warnings)

    if key in SIMPLE_TYPE_MAP:
        return (SIMPLE_TYPE_MAP[key], warnings)

    # Fallback
    warnings.append(
        f"Unrecognized type '{sheet_type_label}' for column '{column_name}', defaulting to text."
    )
    return ("text", warnings)

# -----------------------------
# Delimiter detection
# -----------------------------

POSSIBLE_DELIMS = [",", "\t", ";", "|"]

def sniff_delimiter(sample: str) -> Optional[str]:
    # Try Python's Sniffer first
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample, delimiters="".join(POSSIBLE_DELIMS))
        if dialect and dialect.delimiter in POSSIBLE_DELIMS:
            return dialect.delimiter
    except Exception:
        pass
    # Fallback: choose the delimiter that yields the most columns on the first non-empty line
    lines = [ln for ln in sample.splitlines() if ln.strip()]
    if not lines:
        return None
    best_delim = None
    best_count = 0
    first_line = lines[0]
    for d in POSSIBLE_DELIMS:
        count = len(list(csv.reader([first_line], delimiter=d))[0])
        if count > best_count:
            best_count = count
            best_delim = d
    return best_delim

# -----------------------------
# CSV ingestion
# -----------------------------

def read_three_row_csv(path: str, delimiter: Optional[str], encoding: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Read a 3-row CSV/TSV robustly:
      - Auto-detect delimiter if not provided
      - Preserve empty trailing cells
      - If row2 doesn't look like types but row3 does, swap them
    """
    with open(path, "rb") as fb:
        raw = fb.read()

    # Decode with specified encoding (default utf-8-sig to strip BOM if present)
    text = raw.decode(encoding, errors="strict")

    # Determine delimiter if needed
    used_delim = delimiter or sniff_delimiter(text)
    if not used_delim:
        raise ValueError(
            "Could not detect a delimiter. Try providing --delimiter (e.g., --delimiter '\\t')."
        )

    reader = csv.reader(io.StringIO(text), delimiter=used_delim)
    rows: List[List[str]] = [row for row in reader]

    if len(rows) < 3:
        raise ValueError(f"Expected a file with at least 3 rows, got {len(rows)}.")

    # Only take first three logical rows
    desc = rows[0]
    type_labels = rows[1]
    colnames = rows[2]

    # Normalize lengths to the max number of columns
    max_len = max(len(desc), len(type_labels), len(colnames))
    desc += [""] * (max_len - len(desc))
    type_labels += [""] * (max_len - len(type_labels))
    colnames += [""] * (max_len - len(colnames))

    # Heuristic swap if row2/row3 were flipped
    def looks_like_types(cells: List[str]) -> bool:
        non_empty = [c for c in cells if c.strip()]
        if not non_empty:
            return False
        normalized = [normalize_sheet_type(c) for c in non_empty]
        hits = sum(1 for n in normalized if n in KNOWN_LABELS or n.startswith("enum"))
        return hits / len(non_empty) >= 0.55

    if not looks_like_types(type_labels) and looks_like_types(colnames):
        type_labels, colnames = colnames, type_labels

    return (desc, type_labels, colnames)

# -----------------------------
# DDL generation
# -----------------------------

def generate_create_table_sql(table_name: str, descriptions: List[str], type_labels: List[str], colnames: List[str]) -> str:
    """
    Generate a CREATE TABLE DDL. Adds:
      - id uuid primary key default gen_random_uuid()
    Ignores columns with missing column name.
    Ensures no trailing comma even when inline comments are present.
    """
    table_ident = quote_ident(table_name)
    ddl_lines: List[str] = []

    ddl_lines.append(f"-- Generated by generate_supabase_schema.py")
    ddl_lines.append(f"-- Table: {table_ident}")
    ddl_lines.append(f"create table if not exists {table_ident} (")
    ddl_lines.append(f"  -- Surrogate primary key for internal use")
    ddl_lines.append(f"  id uuid primary key default gen_random_uuid(),")

    warnings: List[str] = []
    cols: List[Tuple[str, str]] = []  # (definition_sql, comment_text)

    # Build normalized column list first
    for desc, typelab, col in zip(descriptions, type_labels, colnames):
        col = (col or "").strip()
        typelab = (typelab or "").strip()
        if not col:
            continue  # ignore unnamed columns

        pg_type, w = map_type(typelab, col)
        warnings.extend(w)
        if not pg_type:
            continue

        col_ident = quote_ident(col)
        def_sql = f"{col_ident} {pg_type}"
        comment_text = re.sub(r"\s+", " ", desc.strip()) if desc.strip() else ""
        cols.append((def_sql, comment_text))

    # Emit columns with commas only between items
    for i, (def_sql, comment_text) in enumerate(cols):
        is_last = (i == len(cols) - 1)
        line = f"  {def_sql}"
        if not is_last:
            line += ","
        if comment_text:
            line += f"  -- {comment_text}"
        ddl_lines.append(line)

    ddl_lines.append(");")

    if warnings:
        ddl_lines.append("")
        ddl_lines.append("-- Warnings during generation:")
        for w in warnings:
            ddl_lines.append(f"--   {w}")

    return "\n".join(ddl_lines)

# -----------------------------
# CLI
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate Supabase CREATE TABLE DDL from a 3-row CSV/TSV.")
    ap.add_argument("--table", required=True, help="Destination table name (e.g., leads)")
    ap.add_argument("--csv", required=True, help="Path to the 3-row CSV/TSV source")
    ap.add_argument("--delimiter", default=None, help="Force delimiter: ',', '\\t', ';', or '|'")
    ap.add_argument("--encoding", default="utf-8-sig", help="File encoding (default utf-8-sig)")
    args = ap.parse_args()

    try:
        desc, types, cols = read_three_row_csv(args.csv, args.delimiter, args.encoding)
        ddl = generate_create_table_sql(args.table, desc, types, cols)
        sys.stdout.write(ddl + "\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
