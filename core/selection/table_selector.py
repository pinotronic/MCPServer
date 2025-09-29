from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Protocol

@dataclass(frozen=True)
class TableSnapshot:
    full_name: str
    name: str
    schema: str
    columns: List[str]

@dataclass(frozen=True)
class SemanticHit:
    table_hint: str
    score: float
    metadata: Dict[str, str] = field(default_factory=dict)

class SemanticRetriever(Protocol):
    def search(self, query: str, n_results: int = 5, dialect: Optional[str] = None, table: Optional[str] = None) -> List[Dict[str, object]]:
        ...

@dataclass
class TableCandidate:
    table: TableSnapshot
    score: float
    reasons: List[str] = field(default_factory=list)
    signals: Dict[str, float] = field(default_factory=dict)

@dataclass
class TableSelectionResult:
    candidates: List[TableCandidate]
    chosen: Optional[TableCandidate]
    reasons: List[str] = field(default_factory=list)

@dataclass
class TableSelectorConfig:
    w_name_exact: float = 3.0
    w_name_token: float = 1.5
    w_column_token: float = 0.9
    w_time_column_boost: float = 1.2
    w_status_column_boost: float = 0.8
    w_semantic_hit: float = 2.0
    w_schema_prefix_boost: float = 0.3
    min_score: float = 1.2
    top_k: int = 5
    max_semantic_hits: int = 6
    domain_synonyms: Dict[str, List[str]] = field(default_factory=lambda: {
        "cita": ["cita", "citas", "appointment", "appointments", "turno", "turnos", "agenda", "agendamiento"],
        "usuario": ["usuario", "usuarios", "user", "users"],
        "persona": ["persona", "personas", "patient", "paciente", "pacientes", "client", "cliente", "clientes"],
        "fecha": ["fecha", "fechas", "date", "datetime", "created_at", "updated_at"],
        "estado": ["estado", "estatus", "status"],
    })
    time_column_hints: List[str] = field(default_factory=lambda: [
        "fecha", "fecharegistro", "fechacreacion", "fechaprogramada", "created_at", "createdon", "createddate",
        "datetime", "timestamp", "fecha_cita", "fechacita"
    ])
    status_column_hints: List[str] = field(default_factory=lambda: [
        "estado", "estatus", "status"
    ])

def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _normalize_text(text: str) -> str:
    text = text.strip().lower()
    text = _strip_accents(text)
    text = re.sub(r"\s+", " ", text)
    return text

def _split_ident(ident: str) -> List[str]:
    ident = _normalize_text(ident)
    parts = re.split(r"[._]", ident)
    tokens: List[str] = []
    for p in parts:
        camel_cut = re.sub(r"([a-z])([A-Z])", r"\1 \2", p)
        camel_cut = re.sub(r"([A-Z])([A-Z][a-z])", r"\1 \2", camel_cut)
        for t in re.split(r"[^a-z0-9]+", camel_cut.lower()):
            if t:
                tokens.append(t)
    return tokens

def _tokenize_question(text: str, cfg: TableSelectorConfig) -> List[str]:
    norm = _normalize_text(text)
    base = [t for t in re.findall(r"[a-z0-9_]+", norm) if t]
    expanded = list(base)
    for key, syns in cfg.domain_synonyms.items():
        if any(key == tok or key in tok for tok in base):
            for s in syns:
                if s not in expanded:
                    expanded.append(s)
    return expanded

class TableSelector:
    def __init__(self, config: Optional[TableSelectorConfig] = None) -> None:
        self._cfg = config or TableSelectorConfig()

    def select(
        self,
        question: str,
        tables: Iterable[TableSnapshot],
        *,
        entities: Optional[object] = None,
        retriever: Optional[SemanticRetriever] = None,
        dialect: Optional[str] = None
    ) -> TableSelectionResult:
        q_tokens = _tokenize_question(question, self._cfg)

        scored: List[TableCandidate] = []
        for t in tables:
            cand = self._score_table(q_tokens, t, entities)
            if cand.score > 0:
                scored.append(cand)

        reasons_global: List[str] = []
        if retriever is not None:
            sem_hits = retriever.search(query=question, n_results=self._cfg.max_semantic_hits, dialect=dialect, table=None) or []
            mapped = self._map_semantic_hits(sem_hits)
            if mapped:
                reasons_global.append("sem:aportes_aplicados")
                self._apply_semantic_boost(scored, mapped)

        scored.sort(key=lambda c: c.score, reverse=True)
        top = scored[: self._cfg.top_k]
        chosen = top[0] if top and top[0].score >= self._cfg.min_score else None

        if not top:
            reasons_global.append("sin_candidatos")
        elif chosen is None:
            reasons_global.append("sin_eleccion_min_score")

        return TableSelectionResult(candidates=top, chosen=chosen, reasons=reasons_global)

    def _score_table(self, q_tokens: List[str], table: TableSnapshot, entities: Optional[object]) -> TableCandidate:
        cfg = self._cfg
        name_tokens = _split_ident(table.name)
        schema_tokens = _split_ident(table.schema) if table.schema else []
        column_tokens = set()
        for c in table.columns:
            for tok in _split_ident(c):
                column_tokens.add(tok)

        score = 0.0
        reasons: List[str] = []
        signals: Dict[str, float] = {}

        name_hits_exact = [qt for qt in q_tokens if qt in name_tokens]
        if name_hits_exact:
            w = cfg.w_name_exact * len(name_hits_exact)
            score += w
            signals["name_exact_hits"] = float(len(name_hits_exact))
            reasons.append(f"name_exact:{','.join(sorted(set(name_hits_exact)))}")

        partial_hits = 0
        for qt in q_tokens:
            if qt in name_tokens:
                continue
            if any(self._is_partial_match(qt, nt) for nt in name_tokens):
                partial_hits += 1
        if partial_hits:
            w = cfg.w_name_token * partial_hits
            score += w
            signals["name_partial_hits"] = float(partial_hits)
            reasons.append(f"name_partial:{partial_hits}")

        col_hits = len([qt for qt in q_tokens if qt in column_tokens])
        if col_hits:
            w = cfg.w_column_token * col_hits
            score += w
            signals["column_hits"] = float(col_hits)
            reasons.append(f"column_hits:{col_hits}")

        if schema_tokens and any(qt in schema_tokens for qt in q_tokens):
            score += cfg.w_schema_prefix_boost
            signals["schema_boost"] = cfg.w_schema_prefix_boost
            reasons.append("schema_match")

        has_time_need = False
        if entities is not None:
            has_time_need = bool(getattr(entities, "date_ranges", None)) or \
                            bool(getattr(entities, "flags", {}).get("has_time_filter", False))  # type: ignore[attr-defined]

        if has_time_need and self._has_any_hint(column_tokens, cfg.time_column_hints):
            score += cfg.w_time_column_boost
            signals["time_boost"] = cfg.w_time_column_boost
            reasons.append("time_column_hint")

        wants_status = any(tok in ("estado", "estatus", "status") for tok in q_tokens)
        if wants_status and self._has_any_hint(column_tokens, cfg.status_column_hints):
            score += cfg.w_status_column_boost
            signals["status_boost"] = cfg.w_status_column_boost
            reasons.append("status_column_hint")

        return TableCandidate(table=table, score=round(score, 4), reasons=reasons, signals=signals)

    @staticmethod
    def _is_partial_match(query_tok: str, target_tok: str) -> bool:
        if len(query_tok) <= 2 or len(target_tok) <= 2:
            return False
        return target_tok.startswith(query_tok) or target_tok.endswith(query_tok)

    @staticmethod
    def _has_any_hint(column_tokens: Iterable[str], hints: Iterable[str]) -> bool:
        colset = set(column_tokens)
        for h in hints:
            if h in colset:
                return True
        return False

    @staticmethod
    def _map_semantic_hits(raw_hits: List[Dict[str, object]]) -> Dict[str, float]:
        table_scores: Dict[str, float] = {}
        for h in raw_hits:
            table_hint = ""
            score_val = 0.0
            meta = h.get("metadata") or h.get("metadatas") or {}
            if isinstance(meta, list):
                meta = meta[0] if meta else {}
            if isinstance(meta, dict):
                table_hint = str(meta.get("table") or meta.get("TABLE") or "").strip()
            if not table_hint:
                table_hint = str(h.get("id") or h.get("table") or "").strip()
            try:
                score_val = float(h.get("score") or h.get("distance") or 0.0)
                if h.get("distance") is not None:
                    score_val = max(0.0, 1.0 - min(score_val, 1.0))
            except Exception:
                score_val = 0.0
            if not table_hint:
                continue
            key = table_hint.lower()
            table_scores[key] = max(table_scores.get(key, 0.0), score_val)
        return table_scores

    def _apply_semantic_boost(self, candidates: List[TableCandidate], table_scores: Dict[str, float]) -> None:
        w = self._cfg.w_semantic_hit
        idx: Dict[str, TableCandidate] = {c.table.full_name.lower(): c for c in candidates}
        short_idx: Dict[str, TableCandidate] = {c.table.name.lower(): c for c in candidates}
        for hint, sem_sc in table_scores.items():
            target = idx.get(hint) or short_idx.get(hint) or idx.get(self._normalize_table_hint(hint)) or short_idx.get(self._normalize_table_hint(hint))
            if not target:
                continue
            boost = w * max(0.2, min(sem_sc, 1.0))
            target.score = round(target.score + boost, 4)
            target.signals["semantic_boost"] = target.signals.get("semantic_boost", 0.0) + boost
            target.reasons.append(f"sem_hit:{hint}:{round(boost,3)}")

    @staticmethod
    def _normalize_table_hint(h: str) -> str:
        h = _normalize_text(h)
        h = h.replace(" ", "")
        return h
