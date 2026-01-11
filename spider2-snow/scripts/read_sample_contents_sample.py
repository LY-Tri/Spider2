#!/usr/bin/env python3
"""
Read a few sample rows from the GITHUB_REPOS.GITHUB_REPOS SAMPLE_CONTENTS parquet export.

Examples:
  python spider2-snow/scripts/read_sample_contents_sample.py
  python spider2-snow/scripts/read_sample_contents_sample.py --n 3
  python spider2-snow/scripts/read_sample_contents_sample.py --path spider2-snow/resource/data/GITHUB_REPOS/GITHUB_REPOS
  python spider2-snow/scripts/read_sample_contents_sample.py --path spider2-snow/resource/data/GITHUB_REPOS/GITHUB_REPOS/SAMPLE_CONTENTS_0_2_0.snappy.parquet --columns repo_name,path,content
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


def repo_root() -> Path:
    # This file lives at: spider2-snow/scripts/<this_file>.py
    return Path(__file__).resolve().parents[2]


def default_sample_contents_path() -> Path:
    return (
        repo_root()
        / "spider2-snow"
        / "resource"
        / "data"
        / "GITHUB_REPOS"
        / "GITHUB_REPOS"
        / "SAMPLE_CONTENTS_0_2_0.snappy.parquet"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--path",
        type=Path,
        default=default_sample_contents_path(),
        help="Parquet file OR directory containing parquet files.",
    )
    p.add_argument("--n", type=int, default=5, help="Number of rows to show.")
    p.add_argument(
        "--columns",
        type=str,
        default="",
        help="Comma-separated list of columns to read (optional).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    target = (repo_root() / args.path).resolve() if not args.path.is_absolute() else args.path

    try:
        import pyarrow.dataset as ds
    except Exception as e:  # pragma: no cover
        print(
            "Missing dependency: pyarrow.\n"
            "Install it with:\n"
            "  pip install -U pyarrow pandas\n",
            file=sys.stderr,
        )
        print(f"Import error: {e}", file=sys.stderr)
        return 2

    if not target.exists():
        print(f"Path not found: {target}", file=sys.stderr)
        return 2

    columns = [c.strip() for c in args.columns.split(",") if c.strip()] or None

    dataset = ds.dataset(str(target), format="parquet")
    print("== Dataset ==")
    print(f"path:   {target}")
    print(f"schema: {dataset.schema}")
    print()

    table = dataset.head(args.n, columns=columns)

    # Pretty-print via pandas when available.
    try:
        import pandas as pd  # noqa: F401

        df = table.to_pandas()
        print("== Sample rows ==")
        with __import__("pandas").option_context("display.max_columns", 200, "display.width", 200):
            print(df)
    except Exception:
        print("== Sample rows (pyarrow) ==")
        print(table.to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

