from __future__ import annotations
from typing import Any, Dict, Optional


class IterativeAnalysisService:
    """
    Implementación mínima: reusa el pipeline determinista una sola vez.
    En el futuro puedes iterar: reformular pregunta, ampliar tablas, etc.
    """

    def __init__(self, query_processor: Any) -> None:
        self._qp = query_processor

    async def analyze_and_respond(
        self,
        original_question: str,
        llm_provider: Optional[str] = None,
        max_iterations: int = 1
    ) -> Dict[str, Any]:
        # Iteración única por ahora (estables y explicables)
        return await self._qp.answer_one_shot(original_question, dialect=None)
