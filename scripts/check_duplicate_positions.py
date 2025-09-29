from __future__ import annotations
import json
import os
from collections import Counter


def main() -> None:
    path = os.path.join("app", "data", "database_context.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tables = data.get("tables") or []
    keys = []
    for t in tables:
        name = (t.get("name") or t.get("table_name") or "").strip().lower()
        schema = (t.get("schema") or t.get("schema_name") or "dbo").strip().lower()
        keys.append(f"{schema}.{name}")

    dup = [k for k, c in Counter(keys).items() if c > 1]
    if dup:
        print("Duplicados:", dup)
    else:
        print("No hay duplicados por full_name")


if __name__ == "__main__":
    main()
