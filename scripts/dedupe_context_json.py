from __future__ import annotations
import json, os, time
from collections import defaultdict


def key_for(tbl: dict) -> str:
    name = str(tbl.get("name") or tbl.get("table_name") or "").strip().lower()
    schema = str(tbl.get("schema") or tbl.get("schema_name") or "dbo").strip().lower()
    return f"{schema}.{name}"


def score_table(tbl: dict) -> int:
    desc = str(tbl.get("description") or "")
    cols = tbl.get("columns") or []
    col_desc_len = sum(len(str(c.get("description") or "")) for c in cols)
    return len(desc) + col_desc_len + len(cols)


def main() -> None:
    path = os.path.join("app", "data", "database_context.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tables = data.get("tables") or []
    buckets = defaultdict(list)
    for t in tables:
        buckets[key_for(t)].append(t)

    kept, removed = [], []
    for k, group in buckets.items():
        if len(group) == 1:
            kept.append(group[0])
        else:
            best = sorted(group, key=score_table, reverse=True)[0]
            kept.append(best)
            for t in group:
                if t is not best:
                    removed.append(k)

    backup = os.path.join("app", "data", f"database_context.backup.{time.strftime('%Y%m%d-%H%M%S')}.json")
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    data["tables"] = kept
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Respaldo: {backup}")
    print(f"Tablas originales: {len(tables)} -> deduplicadas: {len(kept)}")
    if removed:
        print("Claves removidas:", sorted(set(removed)))


if __name__ == "__main__":
    main()
