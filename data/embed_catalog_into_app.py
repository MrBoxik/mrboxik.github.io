#!/usr/bin/env python3
"""
Embed data/maprunner_data.csv into app.js as EMBEDDED_OBJECTIVES_CSV.

Run from repo root:
  python data/embed_catalog_into_app.py
"""

from __future__ import annotations

import json
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
APP_JS = ROOT / "app.js"
CSV_PATH = ROOT / "data" / "maprunner_data.csv"


def main() -> int:
    if not APP_JS.exists():
        print(f"Missing file: {APP_JS}")
        return 1
    if not CSV_PATH.exists():
        print(f"Missing file: {CSV_PATH}")
        return 1

    app_text = APP_JS.read_text(encoding="utf-8")
    csv_text = CSV_PATH.read_text(encoding="utf-8")
    encoded = json.dumps(csv_text)

    start = app_text.find("const EMBEDDED_OBJECTIVES_CSV =")
    end = app_text.find("\n\nconst state = {", start + 1)
    if start < 0 or end < 0:
        print("Could not find EMBEDDED_OBJECTIVES_CSV block in app.js")
        return 1

    replacement = f"const EMBEDDED_OBJECTIVES_CSV = {encoded};"
    out_text = app_text[:start] + replacement + app_text[end:]

    APP_JS.write_text(out_text, encoding="utf-8")
    print(f"Embedded {len(csv_text)} bytes from {CSV_PATH.name} into {APP_JS.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
