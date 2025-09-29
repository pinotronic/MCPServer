from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Tuple


class Intent(Enum):
    """Tipos de intención soportados por el pipeline."""
    COUNT = auto()        # ¿Cuántas...?
    LIST = auto()         # Listados / "muéstrame..."
    AGGREGATE = auto()    # Suma, promedio, min, max, etc.
    DESCRIBE = auto()     # Esquema: tablas, columnas, estructura
    UNKNOWN = auto()      # No se pudo determinar con reglas locales


@dataclass(frozen=True)
class DetectionResult:
    """Resultado de la detección de intención."""
    intent: Intent
    confidence: float
    normalized_question: str
    reasons: List[str] = field(default_factory=list)
    flags: Dict[str, bool] = field(default_factory=dict)


@dataclass
class IntentDetectionConfig:
    """
    Configuración de patrones y pesos por intención.
    Permite ajustar reglas sin modificar la lógica del detector.
    """
    # Patrones (regex) por intención con pesos asociados.
    # Mayor peso = mayor contribución al score de esa intención.
    patterns: Dict[Intent, List[Tuple[re.Pattern[str], float]]]

    # Umbral mínimo para considerar una intención como "confiable".
    min_confidence: float = 0.6

    # Penalización a aplicar si múltiples intenciones tienen scores cercanos
    # (reduce la confianza final para ser conservadores).
    tie_penalty: float = 0.1


def _compile_patterns() -> Dict[Intent, List[Tuple[re.Pattern[str], float]]]:
    """
    Compila patrones en español/inglés comunes para consultas de negocio.
    Se trabaja sobre texto en minúsculas y sin acentos.
    """
    def cp(rx: str, w: float) -> Tuple[re.Pattern[str], float]:
        return (re.compile(rx, flags=re.IGNORECASE), w)

    return {
        Intent.COUNT: [
            cp(r"\bcuantas?\b", 1.0),
            cp(r"\bnumero de\b", 0.9),
            cp(r"\bcantidad de\b", 0.9),
            cp(r"\btotal(?:es)? de\b", 1.0),
            cp(r"\bconteo\b", 1.0),
            cp(r"\bcount\b", 1.0),
            cp(r"\bhow many\b", 1.0),
        ],
        Intent.LIST: [
            cp(r"\blistar?\b", 0.9),
            cp(r"\blista\b", 0.8),
            cp(r"\bmostrar?\b", 0.7),
            cp(r"\bmu(?:e|é)strame\b", 0.9),
            cp(r"\bdame\b", 0.6),
            cp(r"\bver\b", 0.5),
            cp(r"\bconsulta(r)?\b", 0.6),
            cp(r"\blist\b", 0.8),
            cp(r"\bselect\b", 0.6),
            cp(r"\bdistinct\b", 0.6),
        ],
        Intent.AGGREGATE: [
            cp(r"\bpromedio\b", 1.0),
            cp(r"\bmedia\b", 0.9),
            cp(r"\bavg\b", 1.0),
            cp(r"\bsuma(?:toria)?\b", 1.0),
            cp(r"\bsum\b", 1.0),
            cp(r"\bmax(?:imo)?\b", 0.9),
            cp(r"\bmin(?:imo)?\b", 0.9),
            cp(r"\bmediana\b", 0.9),
            cp(r"\bpercentil(?:es)?\b", 0.9),
            cp(r"\bgroup by\b", 0.9),
            cp(r"\bagrup(?:ar|ados?)\b", 0.9),
        ],
        Intent.DESCRIBE: [
            cp(r"\bcolumnas?\b", 1.0),
            cp(r"\bcampos?\b", 1.0),
            cp(r"\bestructura\b", 1.0),
            cp(r"\besquema\b", 1.0),
            cp(r"\bdescribe(r)?\b", 1.0),
            cp(r"\bmetadata\b", 0.9),
            cp(r"\bque tablas?\b", 0.9),
            cp(r"\bcuales tablas\b", 0.9),
            cp(r"\ben que tablas\b", 0.9),
            cp(r"\bddl\b", 0.8),
        ],
    }


def _normalize(text: str) -> str:
    """
    Normaliza texto a minúsculas sin acentos ni caracteres de control.
    Esto hace robustas las coincidencias regex multiplataforma.
    """
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    # Normalizaciones adicionales comunes en español
    text = text.replace("qué", "que").replace("cúantos", "cuantos").replace("cuánta", "cuanta")
    return text


def _basic_flags(normalized: str) -> Dict[str, bool]:
    """
    Señales útiles para etapas posteriores (planner/selector):
    - has_time_filter: parece haber un criterio temporal (años, meses, expresiones).
    - has_grouping: sugiere agregación por 'group by' o 'agrupados'.
    """
    time_rx = [
        r"\b(19|20)\d{2}\b",                            # años tipo 2025
        r"\bhoy\b|\bayer\b|\bmanana\b",                 # expresiones simples
        r"\beste (a|an)o\b|\beste mes\b|\bel mes pasado\b|\bel (a|an)o pasado\b",
        r"\ben (ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b",
        r"\ben (enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\b",
        r"\btrimestre\b|\bcuatrimestre\b|\bsemestre\b|\btrimestre \d\b|\bq[1-4]\b",
    ]
    grp_rx = [r"\bgroup by\b", r"\bagrup(?:ar|ados?)\b"]

    has_time = any(re.search(rx, normalized) for rx in time_rx)
    has_group = any(re.search(rx, normalized) for rx in grp_rx)
    return {"has_time_filter": has_time, "has_grouping": has_group}


class IntentDetector:
    """
    Detector de intención basado en reglas ponderadas.
    - Independiente de LLM/BD.
    - Extensible vía IntentDetectionConfig.
    - Determinista y explicable (reasons).
    """

    def __init__(self, config: IntentDetectionConfig | None = None) -> None:
        self._config = config or IntentDetectionConfig(patterns=_compile_patterns())

    def detect(self, question: str) -> DetectionResult:
        if not question or not question.strip():
            return DetectionResult(
                intent=Intent.UNKNOWN,
                confidence=0.0,
                normalized_question="",
                reasons=["entrada_vacia"],
                flags={}
            )

        norm = _normalize(question)
        scores: Dict[Intent, float] = {i: 0.0 for i in Intent}
        reasons: List[str] = []

        # Reglas específicas de alta confianza (short-circuit si aplica)
        if norm.startswith("cuantas ") or norm.startswith("cuantos ") or norm.startswith("how many "):
            scores[Intent.COUNT] += 1.2
            reasons.append("regla_inicio_cuantas/how_many")

        # Acumula puntuaciones por patrones
        for intent, plist in self._config.patterns.items():
            for pattern, weight in plist:
                if pattern.search(norm):
                    scores[intent] += weight
                    reasons.append(f"match:{intent.name}:{pattern.pattern}")

        # Selección de mejor intención
        best_intent, best_score = self._best(scores)

        # Penaliza confianza si hay empate cercano
        confidence = self._confidence(best_intent, best_score, scores)

        # Normaliza: si nadie supera umbral, cae a UNKNOWN salvo heurística
        if confidence < self._config.min_confidence:
            # Heurística útil: si no hay señales fuertes, un listado es la intención por defecto
            if best_intent in (Intent.LIST, Intent.COUNT, Intent.AGGREGATE, Intent.DESCRIBE):
                # Reducimos confianza pero mantenemos la mejor hipótesis
                confidence = max(confidence, 0.51)
            else:
                best_intent = Intent.LIST
                confidence = 0.51
                reasons.append("fallback:list_por_defecto")

        flags = _basic_flags(norm)

        return DetectionResult(
            intent=best_intent,
            confidence=round(confidence, 3),
            normalized_question=norm,
            reasons=reasons,
            flags=flags
        )

    @staticmethod
    def _best(scores: Dict[Intent, float]) -> Tuple[Intent, float]:
        # Excluye UNKNOWN de la competencia si otros tienen score
        filtered = {k: v for k, v in scores.items() if k is not Intent.UNKNOWN}
        if not filtered:
            return Intent.UNKNOWN, 0.0
        best_intent = max(filtered, key=lambda k: filtered[k])
        return best_intent, filtered[best_intent]

    def _confidence(self, winner: Intent, winner_score: float, scores: Dict[Intent, float]) -> float:
        # Confianza proporcional al margen entre el ganador y el segundo
        second = 0.0
        for intent, sc in scores.items():
            if intent is winner or intent is Intent.UNKNOWN:
                continue
            if sc > second:
                second = sc

        # margen relativo
        if winner_score <= 0.0:
            return 0.0

        margin = max(winner_score - second, 0.0)
        base = min(winner_score / (winner_score + second + 1e-6), 1.0)

        # Ajusta con margen para no “inflar” confianza en empates
        conf = base
        if margin < 0.2:
            conf = max(0.5, base - self._config.tie_penalty)
        elif margin < 0.5:
            conf = max(0.55, base - (self._config.tie_penalty * 0.5))
        else:
            conf = min(0.99, base + 0.1)

        return float(conf)
