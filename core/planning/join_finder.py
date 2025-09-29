from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple

from core.planning.sql_validator import SchemaCatalog, TableInfo, ColumnInfo


@dataclass(frozen=True)
class JoinEdge:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    score: float


def _name_tokens(name: str) -> List[str]:
    import re, unicodedata
    def strip_acc(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in s if not unicodedata.combining(ch))
    name = strip_acc(name.lower())
    parts = re.split(r"[._\W]+", name)
    return [p for p in parts if p]


def _common_id_candidates(left: TableInfo, right: TableInfo) -> List[Tuple[str, str]]:
    # heurística simple: cita_id ↔ id_cita ↔ id
    ltoks = _name_tokens(left.name)
    rtoks = _name_tokens(right.name)
    candidates: List[Tuple[str, str]] = []

    # 1) PK ↔ *_id con nombre de la otra tabla
    for pk in left.pk_columns or []:
        pk_l = pk.lower()
        for rcol in right.columns.values():
            nrm = rcol.name.lower()
            if any(t in nrm for t in ltoks) and ("id" in nrm or nrm.endswith("_id") or nrm.startswith("id_")):
                candidates.append((pk_l, rcol.name))
    for pk in right.pk_columns or []:
        pk_r = pk.lower()
        for lcol in left.columns.values():
            nrm = lcol.name.lower()
            if any(t in nrm for t in rtoks) and ("id" in nrm or nrm.endswith("_id") or nrm.startswith("id_")):
                candidates.append((lcol.name, pk_r))

    # 2) *_id en ambos con tokens compatibles
    for lcol in left.columns.values():
        ln = lcol.name.lower()
        if "id" in ln:
            for rcol in right.columns.values():
                rn = rcol.name.lower()
                if "id" in rn and (any(t in ln for t in rtoks) or any(t in rn for t in ltoks)):
                    candidates.append((lcol.name, rcol.name))

    # 3) Misma columna exacta (ej. usuario_id en ambos)
    for lcol in left.columns.values():
        rn = lcol.name.lower()
        if rn in right.columns:
            candidates.append((lcol.name, lcol.name))

    # dedup
    uniq = []
    seen = set()
    for a, b in candidates:
        k = (a.lower(), b.lower())
        if k not in seen:
            seen.add(k)
            uniq.append((a, b))
    return uniq


def suggest_joins(catalog: SchemaCatalog, from_table: str, to_table: str) -> List[JoinEdge]:
    """Devuelve join candidates (no ejecuta nada; heurísticas basadas en nombres)."""
    fti = catalog.get_table(from_table)
    tti = catalog.get_table(to_table)
    if not fti or not tti:
        return []

    edges: List[JoinEdge] = []
    for lcol, rcol in _common_id_candidates(fti, tti):
        score = 1.0
        if lcol.lower() in (x.lower() for x in fti.pk_columns):
            score += 0.5
        if rcol.lower() in (x.lower() for x in tti.pk_columns):
            score += 0.5
        edges.append(JoinEdge(fti.full_name, lcol, tti.full_name, rcol, score))

    edges.sort(key=lambda e: e.score, reverse=True)
    return edges


def pick_best_join(catalog: SchemaCatalog, from_table: str, to_table: str) -> Optional[JoinEdge]:
    joins = suggest_joins(catalog, from_table, to_table)
    return joins[0] if joins else None
