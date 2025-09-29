from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Iterable, List, Optional, Tuple

# Si quieres usar ExtractedEntities, no forzamos import en runtime:
# from core.extraction.entities import ExtractedEntities


class ColumnRole(Enum):
    DATE = auto()
    STATUS = auto()
    ID = auto()


@dataclass(frozen=True)
class ColumnSnapshot:
    name: str
    type: Optional[str] = None
    is_pk: bool = False
    is_fk: bool = False
    nullable: Optional[bool] = None
    description: str = ""


@dataclass(frozen=True)
class TableProfile:
    full_name: str
    name: str
    schema: str
    columns: List[ColumnSnapshot]


@dataclass
class ColumnChoice:
    role: ColumnRole
    column: Optional[ColumnSnapshot]
    score: float
    reasons: List[str] = field(default_factory=list)


@dataclass
class ColumnSelectionResult:
    table_full_name: str
    choices: Dict[ColumnRole, ColumnChoice]
    reasons: List[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class ColumnSelectorConfig:
    w_name_hint: float = 1.4
    w_type_hint: float = 1.2
    w_entity_need: float = 1.0
    w_pk_bonus: float = 1.5
    w_name_pattern_id: float = 1.2
    w_question_token_hit: float = 0.6

    min_accept_score: float = 1.0

    date_hints: List[str] = field(default_factory=lambda: [
        "fecha", "fecharegistro", "fechacreacion", "fechaprogramada", "fechacita",
        "created_at", "createdon", "createddate", "datetime", "timestamp"
    ])
    status_hints: List[str] = field(default_factory=lambda: [
        "estado", "estatus", "status"
    ])
    id_name_patterns: List[str] = field(default_factory=lambda: [
        r"(^|_)id($|_)", r".+_id$", r"^id_.+", r".+id$"
    ])
    date_type_tokens: List[str] = field(default_factory=lambda: [
        "date", "datetime", "timestamp", "time"
    ])

    # tokens en la pregunta que sugieren roles
    question_date_tokens: List[str] = field(default_factory=lambda: [
        "fecha", "fechas", "dia", "dias", "mes", "meses", "ano", "anio", "año", "anos", "anios", "años"
    ])
    question_status_tokens: List[str] = field(default_factory=lambda: [
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


def _tokenize_question(text: str) -> List[str]:
    norm = _normalize_text(text)
    return [t for t in re.findall(r"[a-z0-9_]+", norm) if t]


def _any_token_in(tokens: Iterable[str], candidates: Iterable[str]) -> bool:
    cset = set(candidates)
    return any(t in cset for t in tokens)


class ColumnSelector:
    def __init__(self, config: Optional[ColumnSelectorConfig] = None) -> None:
        self._cfg = config or ColumnSelectorConfig()

    def select(
        self,
        table: TableProfile,
        *,
        question: Optional[str] = None,
        entities: Optional[object] = None  # ExtractedEntities recomendado
    ) -> ColumnSelectionResult:
        q_tokens = _tokenize_question(question or "")
        date_choice = self._pick_date_column(table, q_tokens, entities)
        status_choice = self._pick_status_column(table, q_tokens, entities)
        id_choice = self._pick_id_column(table, q_tokens)

        choices = {
            ColumnRole.DATE: date_choice,
            ColumnRole.STATUS: status_choice,
            ColumnRole.ID: id_choice,
        }

        accepted = [c for c in choices.values() if c.column is not None]
        if accepted:
            conf = min(0.99, sum(c.score for c in accepted) / (len(accepted) * (self._cfg.w_name_hint + self._cfg.w_type_hint + self._cfg.w_entity_need + self._cfg.w_pk_bonus)))
            conf = round(max(conf, 0.55), 3)
        else:
            conf = 0.0

        reasons = []
        if not accepted:
            reasons.append("no_hay_columnas_elegidas")
        else:
            reasons.append(f"roles_elegidos:{','.join([r.role.name for r in accepted])}")

        return ColumnSelectionResult(
            table_full_name=table.full_name,
            choices=choices,
            reasons=reasons,
            confidence=conf
        )

    def _pick_date_column(self, table: TableProfile, q_tokens: List[str], entities: Optional[object]) -> ColumnChoice:
        cfg = self._cfg
        best: Tuple[Optional[ColumnSnapshot], float, List[str]] = (None, 0.0, [])
        need_time = False

        if entities is not None:
            try:
                if getattr(entities, "date_ranges", None):
                    need_time = True
            except Exception:
                pass

        if not need_time and _any_token_in(q_tokens, cfg.question_date_tokens):
            need_time = True

        for col in table.columns:
            score = 0.0
            rs: List[str] = []
            name_tokens = _split_ident(col.name)

            if any(h in name_tokens for h in cfg.date_hints):
                score += cfg.w_name_hint
                rs.append("hint_nombre_fecha")

            if col.type:
                tnorm = _normalize_text(col.type)
                if any(tok in tnorm for tok in cfg.date_type_tokens):
                    score += cfg.w_type_hint
                    rs.append(f"hint_tipo:{tnorm}")

            if need_time:
                score += cfg.w_entity_need
                rs.append("necesidad_temporal")

            if _any_token_in(q_tokens, name_tokens):
                score += cfg.w_question_token_hit
                rs.append("match_pregunta_nombre_col")

            if score > best[1]:
                best = (col, score, rs)

        chosen, sc, reasons = best
        if sc < cfg.min_accept_score:
            return ColumnChoice(role=ColumnRole.DATE, column=None, score=sc, reasons=reasons + ["debajo_umbral"])
        return ColumnChoice(role=ColumnRole.DATE, column=chosen, score=round(sc, 3), reasons=reasons)

    def _pick_status_column(self, table: TableProfile, q_tokens: List[str], entities: Optional[object]) -> ColumnChoice:
        cfg = self._cfg
        best: Tuple[Optional[ColumnSnapshot], float, List[str]] = (None, 0.0, [])
        need_status = False

        if entities is not None:
            try:
                statuses = list(getattr(entities, "statuses", []) or [])
                if statuses:
                    need_status = True
            except Exception:
                pass

        if not need_status and _any_token_in(q_tokens, cfg.status_hints):
            need_status = True

        for col in table.columns:
            score = 0.0
            rs: List[str] = []
            name_tokens = _split_ident(col.name)

            if any(h in name_tokens for h in cfg.status_hints):
                score += cfg.w_name_hint
                rs.append("hint_nombre_status")

            if need_status:
                score += cfg.w_entity_need
                rs.append("necesidad_status")

            if _any_token_in(q_tokens, name_tokens):
                score += cfg.w_question_token_hit
                rs.append("match_pregunta_nombre_col")

            if score > best[1]:
                best = (col, score, rs)

        chosen, sc, reasons = best
        if sc < cfg.min_accept_score:
            return ColumnChoice(role=ColumnRole.STATUS, column=None, score=sc, reasons=reasons + ["debajo_umbral"])
        return ColumnChoice(role=ColumnRole.STATUS, column=chosen, score=round(sc, 3), reasons=reasons)

    def _pick_id_column(self, table: TableProfile, q_tokens: List[str]) -> ColumnChoice:
        cfg = self._cfg
        best: Tuple[Optional[ColumnSnapshot], float, List[str]] = (None, 0.0, [])

        table_tokens = _split_ident(table.name)

        for col in table.columns:
            score = 0.0
            rs: List[str] = []
            name_norm = _normalize_text(col.name)
            name_tokens = _split_ident(col.name)

            if col.is_pk:
                score += cfg.w_pk_bonus
                rs.append("pk")

            if any(re.search(rx, name_norm) for rx in cfg.id_name_patterns):
                score += cfg.w_name_pattern_id
                rs.append("patron_nombre_id")

            # bonus si el nombre de la tabla aparece en el nombre de la columna (ej: cita_id, id_cita)
            if any(tok in name_tokens for tok in table_tokens):
                score += cfg.w_question_token_hit
                rs.append("token_tabla_en_columna")

            if _any_token_in(q_tokens, name_tokens):
                score += cfg.w_question_token_hit
                rs.append("match_pregunta_nombre_col")

            if score > best[1]:
                best = (col, score, rs)

        chosen, sc, reasons = best
        if sc < cfg.min_accept_score:
            return ColumnChoice(role=ColumnRole.ID, column=None, score=sc, reasons=reasons + ["debajo_umbral"])
        return ColumnChoice(role=ColumnRole.ID, column=chosen, score=round(sc, 3), reasons=reasons)


# ---------- Adaptadores opcionales ----------

def profile_from_snapshot(
    *,
    full_name: str,
    name: str,
    schema: str,
    columns: Iterable[str],
    column_types: Optional[Dict[str, str]] = None,
    primary_keys: Optional[Iterable[str]] = None,
    foreign_keys: Optional[Iterable[str]] = None,
    descriptions: Optional[Dict[str, str]] = None
) -> TableProfile:
    """
    Construye un TableProfile enriquecido a partir de listas simples.
    Útil si vienes del TableSelector.TableSnapshot.
    """
    pkset = { _normalize_text(x) for x in (primary_keys or []) }
    fkset = { _normalize_text(x) for x in (foreign_keys or []) }
    ctype = { _normalize_text(k): v for k, v in (column_types or {}).items() }
    cdesc = { _normalize_text(k): v for k, v in (descriptions or {}).items() }

    col_objs: List[ColumnSnapshot] = []
    for cname in columns:
        key = _normalize_text(cname)
        col_objs.append(ColumnSnapshot(
            name=cname,
            type=ctype.get(key),
            is_pk=key in pkset,
            is_fk=key in fkset,
            nullable=None,
            description=cdesc.get(key, "")
        ))

    return TableProfile(
        full_name=full_name,
        name=name,
        schema=schema,
        columns=col_objs
    )
