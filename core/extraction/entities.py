from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple


class DateGranularity(Enum):
    DAY = auto()
    MONTH = auto()
    QUARTER = auto()
    YEAR = auto()
    RANGE = auto()
    UNKNOWN = auto()


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date  # Convención: fin EXCLUSIVO [start, end)
    label: str = ""


@dataclass(frozen=True)
class ExtractedEntities:
    normalized_question: str
    date_ranges: List[DateRange]
    date_granularity: DateGranularity
    statuses: List[str]
    limit: Optional[int]
    order_hint: Optional[str]  # "asc" | "desc" | None
    reasons: List[str] = field(default_factory=list)


# ---------- Normalización y tablas auxiliares ----------

_MONTHS: Dict[str, int] = {
    # español completo
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    # abreviaturas (sin acentos)
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7,
    "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12,
    # inglés básico por robustez
    "january": 1, "february": 2, "march": 3, "april": 4, "may_en": 5, "june": 6, "july": 7,
    "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb_en": 2, "mar_en": 3, "apr": 4, "may_abbr": 5, "jun_en": 6, "jul_en": 7,
    "aug": 8, "sep_en": 9, "oct_en": 10, "nov_en": 11, "dec": 12,
}
# Nota: en inglés, “may” y abrevs cortas se pisan con palabras comunes.
# Para evitar falsos positivos, usamos claves alternativas con sufijos (_en/_abbr) que
# sólo activamos si detectamos que el token estaba en inglés explícitamente.


# Estados comunes (normalizados a minúsculas, sin acentos)
_STATUS_VOCAB = {
    "programada", "pendiente", "confirmada", "completada", "realizada",
    "cancelada", "rechazada", "reprogramada", "no_show", "ausente", "en_proceso"
}

# Patrones de límite/orden
_LIMIT_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"\btop\s*(\d{1,5})\b", re.IGNORECASE), "desc"),
    (re.compile(r"\bprimer(?:os|as)?\s*(\d{1,5})\b", re.IGNORECASE), "asc"),
    (re.compile(r"\bultim(?:os|as)?\s*(\d{1,5})\b", re.IGNORECASE), "desc"),
]


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _normalize(text: str) -> str:
    text = text.strip().lower()
    text = _strip_accents(text)
    # normalizaciones útiles
    text = re.sub(r"\s+", " ", text)
    return text


# ---------- Utilidades de fechas ----------

def _is_leap_year(y: int) -> bool:
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def _last_day_of_month(y: int, m: int) -> int:
    if m in (1, 3, 5, 7, 8, 10, 12):
        return 31
    if m in (4, 6, 9, 11):
        return 30
    return 29 if _is_leap_year(y) else 28


def _next_month(y: int, m: int) -> Tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _quarter_bounds(y: int, q: int) -> Tuple[date, date]:
    if q == 1:
        return date(y, 1, 1), date(y, 4, 1)
    if q == 2:
        return date(y, 4, 1), date(y, 7, 1)
    if q == 3:
        return date(y, 7, 1), date(y, 10, 1)
    return date(y, 10, 1), date(y + 1, 1, 1)


def _safe_date(y: int, m: int, d: int) -> date:
    maxd = _last_day_of_month(y, m)
    d = min(max(d, 1), maxd)
    return date(y, m, d)


# ---------- Parsers de fechas en texto ----------

def _extract_years(text: str) -> List[DateRange]:
    ranges: List[DateRange] = []
    for m in re.finditer(r"\b(19|20)\d{2}\b", text):
        y = int(m.group(0))
        start = date(y, 1, 1)
        end = date(y + 1, 1, 1)
        ranges.append(DateRange(start=start, end=end, label=str(y)))
    return ranges


def _extract_month_year(text: str) -> List[DateRange]:
    # patrones: "enero 2025", "ene 2025", "mayo del 2026", "sep de 2024"
    ranges: List[DateRange] = []
    month_names = "|".join(sorted({
        *[k for k, v in _MONTHS.items() if v and "_" not in k and len(k) >= 3 and k.isalpha()],
        # incluimos abrevs esp
        "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "set", "oct", "nov", "dic"
    }, key=len, reverse=True))
    rx = re.compile(rf"\b({month_names})\s*(?:de|del)?\s*((?:19|20)\d{{2}})\b", re.IGNORECASE)
    for m in rx.finditer(text):
        raw_mon = m.group(1)
        y = int(m.group(2))
        mon_key = raw_mon
        mon_key = mon_key.replace("may", "mayo")  # desambiguar may esp vs may inglés
        mon_key = mon_key.strip().lower()
        mon_key = _strip_accents(mon_key)
        mon = _MONTHS.get(mon_key, 0)
        if mon == 0:
            continue
        start = date(y, mon, 1)
        ny, nm = _next_month(y, mon)
        end = date(ny, nm, 1)
        ranges.append(DateRange(start=start, end=end, label=f"{raw_mon} {y}"))
    return ranges


def _extract_quarters(text: str) -> List[DateRange]:
    ranges: List[DateRange] = []
    # Q1 2025 / T1 2025 / 1er trimestre 2025 / primer trimestre de 2025 / trimestre 3 de 2024
    rx1 = re.compile(r"\b(?:q|t)\s*([1-4])\s*((?:19|20)\d{2})\b", re.IGNORECASE)
    rx2 = re.compile(r"\b(?:([1-4])(?:er|o)?\s+trimestre)\s*(?:de)?\s*((?:19|20)\d{2})\b", re.IGNORECASE)
    rx3 = re.compile(r"\btrimestre\s*([1-4])\s*(?:de)?\s*((?:19|20)\d{2})\b", re.IGNORECASE)

    for rx in (rx1, rx2, rx3):
        for m in rx.finditer(text):
            q = int(m.group(1))
            y = int(m.group(2))
            start, end = _quarter_bounds(y, q)
            ranges.append(DateRange(start=start, end=end, label=f"Q{q} {y}"))
    return ranges


def _parse_iso_date(token: str) -> Optional[date]:
    m = re.fullmatch(r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})", token)
    if not m:
        return None
    y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return _safe_date(y, mm, dd)


def _parse_dd_mm_yyyy(token: str) -> Optional[date]:
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})", token)
    if not m:
        return None
    dd, mm, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return _safe_date(y, mm, dd)


def _parse_d_de_month_de_y(s: str) -> Optional[date]:
    # ej: "1 de enero de 2025" | "10 enero 2024"
    m = re.fullmatch(r"(\d{1,2})\s*(?:de\s*)?([a-zA-Z]+)\s*(?:de\s*)?((?:19|20)\d{2})", s)
    if not m:
        return None
    dd = int(m.group(1))
    mon_key = _strip_accents(m.group(2).lower())
    mon_key = mon_key.replace("may", "mayo")
    y = int(m.group(3))
    mon = _MONTHS.get(mon_key, 0)
    if mon == 0:
        return None
    return _safe_date(y, mon, dd)


def _extract_explicit_dates(text: str) -> List[date]:
    dates: List[date] = []
    for tok in re.findall(r"\b(?:19|20)\d{2}-\d{1,2}-\d{1,2}\b", text):
        d = _parse_iso_date(tok)
        if d:
            dates.append(d)
    for tok in re.findall(r"\b\d{1,2}/\d{1,2}/(?:19|20)\d{2}\b", text):
        d = _parse_dd_mm_yyyy(tok)
        if d:
            dates.append(d)
    # "1 de enero de 2025" / "1 enero 2025"
    for m in re.finditer(r"\b\d{1,2}\s*(?:de\s*)?[a-zA-Z]+\s*(?:de\s*)?(?:19|20)\d{2}\b", text):
        d = _parse_d_de_month_de_y(m.group(0))
        if d:
            dates.append(d)
    return dates


def _extract_between_ranges(text: str) -> List[DateRange]:
    ranges: List[DateRange] = []

    # patrones: "entre X y Y" / "del X al Y" / "desde X hasta Y"
    rx = re.compile(
        r"\b(?:entre|del|desde)\s+(.+?)\s+(?:y|al|hasta)\s+(.+?)\b",
        re.IGNORECASE
    )

    for m in rx.finditer(text):
        left_raw = m.group(1).strip()
        right_raw = m.group(2).strip()

        # intenta varios formatos
        candidates_left = [
            _parse_iso_date(left_raw),
            _parse_dd_mm_yyyy(left_raw),
            _parse_d_de_month_de_y(left_raw),
        ]
        candidates_right = [
            _parse_iso_date(right_raw),
            _parse_dd_mm_yyyy(right_raw),
            _parse_d_de_month_de_y(right_raw),
        ]
        left = next((d for d in candidates_left if d is not None), None)
        right = next((d for d in candidates_right if d is not None), None)

        if left and right:
            end = right + timedelta(days=1)  # fin exclusivo
            label = f"{left_raw} — {right_raw}"
            if end <= left:
                # intercambia si vinieron invertidas
                left, end = right, left + timedelta(days=1)
            ranges.append(DateRange(start=left, end=end, label=label))

    return ranges


def _extract_relative_periods(text: str, today: date) -> Tuple[List[DateRange], Optional[DateGranularity], List[str]]:
    reasons: List[str] = []
    ranges: List[DateRange] = []
    gran: Optional[DateGranularity] = None

    # hoy, ayer, mañana
    if re.search(r"\bhoy\b", text):
        start = today
        end = today + timedelta(days=1)
        ranges.append(DateRange(start=start, end=end, label="hoy"))
        gran = DateGranularity.DAY
        reasons.append("rel:hoy")
    if re.search(r"\bayer\b", text):
        d = today - timedelta(days=1)
        ranges.append(DateRange(start=d, end=today, label="ayer"))
        gran = DateGranularity.DAY
        reasons.append("rel:ayer")
    if re.search(r"\bmanana\b", text):
        d = today + timedelta(days=1)
        ranges.append(DateRange(start=d, end=d + timedelta(days=1), label="manana"))
        gran = DateGranularity.DAY
        reasons.append("rel:manana")

    # este año, año pasado, próximo año
    if re.search(r"\beste (a|an)o\b", text):
        y = today.year
        ranges.append(DateRange(start=date(y, 1, 1), end=date(y + 1, 1, 1), label=f"este ano {y}"))
        gran = DateGranularity.YEAR
        reasons.append("rel:este_ano")
    if re.search(r"\bel (a|an)o pasado\b", text):
        y = today.year - 1
        ranges.append(DateRange(start=date(y, 1, 1), end=date(y + 1, 1, 1), label=f"ano pasado {y}"))
        gran = DateGranularity.YEAR
        reasons.append("rel:ano_pasado")
    if re.search(r"\bel proximo (a|an)o\b", text):
        y = today.year + 1
        ranges.append(DateRange(start=date(y, 1, 1), end=date(y + 1, 1, 1), label=f"proximo ano {y}"))
        gran = DateGranularity.YEAR
        reasons.append("rel:proximo_ano")

    # este mes, mes pasado, próximo mes
    if re.search(r"\beste mes\b", text):
        y, m = today.year, today.month
        ny, nm = _next_month(y, m)
        ranges.append(DateRange(start=date(y, m, 1), end=date(ny, nm, 1), label="este mes"))
        gran = DateGranularity.MONTH
        reasons.append("rel:este_mes")
    if re.search(r"\bel mes pasado\b", text):
        y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
        ny, nm = _next_month(y, m)
        ranges.append(DateRange(start=date(y, m, 1), end=date(ny, nm, 1), label="mes pasado"))
        gran = DateGranularity.MONTH
        reasons.append("rel:mes_pasado")
    if re.search(r"\bel proximo mes\b", text):
        y, m = _next_month(today.year, today.month)
        ny, nm = _next_month(y, m)
        ranges.append(DateRange(start=date(y, m, 1), end=date(ny, nm, 1), label="proximo mes"))
        gran = DateGranularity.MONTH
        reasons.append("rel:proximo_mes")

    # trimestres relativos (este trimestre, trimestre pasado)
    if re.search(r"\beste trimestre\b", text):
        q = ((today.month - 1) // 3) + 1
        start, end = _quarter_bounds(today.year, q)
        ranges.append(DateRange(start=start, end=end, label=f"este trimestre Q{q}"))
        gran = DateGranularity.QUARTER
        reasons.append("rel:este_trimestre")
    if re.search(r"\bel trimestre pasado\b", text):
        q = ((today.month - 1) // 3) + 1
        y = today.year
        if q == 1:
            y -= 1
            q = 4
        else:
            q -= 1
        start, end = _quarter_bounds(y, q)
        ranges.append(DateRange(start=start, end=end, label=f"trimestre pasado Q{q} {y}"))
        gran = DateGranularity.QUARTER
        reasons.append("rel:trimestre_pasado")

    return ranges, gran, reasons


# ---------- Estados / límites / orden ----------

def _extract_statuses(text: str) -> List[str]:
    found: List[str] = []
    # Estado por patrón clave-valor
    for m in re.finditer(r"\bestad(?:o|us)\s*[:=]\s*([a-z0-9_]+)\b", text):
        found.append(m.group(1))

    # Vocabulario suelto (programada, cancelada, etc.)
    tokens = re.findall(r"[a-z0-9_]+", text)
    for t in tokens:
        if t in _STATUS_VOCAB and t not in found:
            found.append(t)

    # Normalización simple (no duplicados)
    return list(dict.fromkeys(found))


def _extract_limit_and_order(text: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    for rx, order in _LIMIT_PATTERNS:
        m = rx.search(text)
        if m:
            try:
                n = int(m.group(1))
                if n > 0:
                    # razón textual
                    return n, order, f"limit:{n}:{order}"
            except Exception:
                continue
    return None, None, None


# ---------- API principal ----------

def extract_entities(question: str, today_value: Optional[date] = None) -> ExtractedEntities:
    """
    Extrae entidades útiles para el planner:
      - rangos de fechas [start, end) con fin exclusivo
      - granularidad temporal inferida
      - estados/estatus
      - límite y pista de orden (asc/desc)

    El texto se normaliza y se aceptan expresiones:
      - Años: "2025"
      - Mes + Año: "enero 2025", "sep 2024"
      - Trimestres: "Q1 2025", "primer trimestre 2024"
      - Rangos: "entre 2024-05-01 y 2024-06-30", "del 1 de enero de 2025 al 10 de enero de 2025"
      - Relativos: "hoy", "este mes", "año pasado", "trimestre pasado"
      - Fechas sueltas: "2025-08-15", "01/02/2025", "1 de enero de 2025"

    Si se detectan múltiples rangos, se devuelven todos y la granularidad pasa a RANGE.
    """
    if not question or not question.strip():
        return ExtractedEntities(
            normalized_question="",
            date_ranges=[],
            date_granularity=DateGranularity.UNKNOWN,
            statuses=[],
            limit=None,
            order_hint=None,
            reasons=["entrada_vacia"]
        )

    norm = _normalize(question)
    reasons: List[str] = []

    # Límite y orden
    limit, order, limit_reason = _extract_limit_and_order(norm)
    if limit_reason:
        reasons.append(limit_reason)

    # Estados
    statuses = _extract_statuses(norm)
    if statuses:
        reasons.append(f"statuses:{','.join(statuses)}")

    # Fechas relativas (dependen de 'today')
    today = today_value or date.today()
    rel_ranges, rel_gran, rel_reasons = _extract_relative_periods(norm, today)
    reasons.extend(rel_reasons)

    # Rangos explícitos "entre/del/desde ... hasta/al ..."
    between_ranges = _extract_between_ranges(norm)
    if between_ranges:
        reasons.append("rangos:entre_del_hasta")
    # Mes + año
    month_year = _extract_month_year(norm)
    if month_year:
        reasons.append("mes_anio")
    # Trimestres
    quarters = _extract_quarters(norm)
    if quarters:
        reasons.append("trimestres")
    # Años sueltos
    years = _extract_years(norm)
    if years:
        reasons.append("anios")
    # Fechas sueltas (si ocurre una sola fecha, la tratamos como día)
    explicit_dates = _extract_explicit_dates(norm)
    day_ranges: List[DateRange] = []
    for d in explicit_dates:
        day_ranges.append(DateRange(start=d, end=d + timedelta(days=1), label=d.isoformat()))
    if day_ranges:
        reasons.append("fechas_sueltas")

    # Merge de todos los rangos detectados
    all_ranges: List[DateRange] = []
    all_ranges.extend(between_ranges)
    all_ranges.extend(month_year)
    all_ranges.extend(quarters)
    all_ranges.extend(years)
    all_ranges.extend(day_ranges)
    all_ranges.extend(rel_ranges)

    # Deduplicación básica por (start,end)
    uniq: Dict[Tuple[date, date], DateRange] = {}
    for r in all_ranges:
        key = (r.start, r.end)
        if key not in uniq:
            uniq[key] = r
    merged = list(uniq.values())

    # Granularidad final
    if len(merged) == 0:
        gran = DateGranularity.UNKNOWN
    elif len(merged) > 1:
        gran = DateGranularity.RANGE
    else:
        # una sola ventana: infiere granularidad por tamaño
        r = merged[0]
        span = (r.end - r.start).days
        if span <= 1:
            gran = DateGranularity.DAY
        elif span <= 32:
            gran = DateGranularity.MONTH
        elif span <= 93:
            gran = DateGranularity.QUARTER
        elif span <= 370:
            gran = DateGranularity.YEAR
        else:
            gran = DateGranularity.RANGE

    return ExtractedEntities(
        normalized_question=norm,
        date_ranges=merged,
        date_granularity=gran,
        statuses=statuses,
        limit=limit,
        order_hint=order,
        reasons=reasons
    )
