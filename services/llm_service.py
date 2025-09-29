from __future__ import annotations

import json
import re
from typing import Any, Optional, List, Dict

import httpx

from utils.config_loader import ConfigLoader


class LLMService:
    async def propose_sql(self, question: str, db_context: dict) -> Dict[str, Any]:
        raise NotImplementedError()

    async def summarize_answer(self, question: str, rows: List[Dict[str, Any]]) -> str:
        raise NotImplementedError()


def _strip_md_fences(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _first_json_in_text(text: str) -> Dict[str, Any]:
    text = _strip_md_fences(text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {"sql": "SELECT 1 AS resultado", "needs_retry": True, "reason": "No JSON encontrado"}
    frag = text[start : end + 1]
    try:
        return json.loads(frag)
    except Exception:
        return {"sql": "SELECT 1 AS resultado", "needs_retry": True, "reason": "JSON inválido"}


def _build_sql_prompt(question: str, db_ctx: dict, config: ConfigLoader) -> str:
    tpl = config.read_prompt("sql_prompt.txt") if hasattr(config, "read_prompt") else ""
    mini = db_ctx.get("mini_schema") or "(sin schema declarado)"
    if tpl:
        return tpl.replace("{{SCHEMA}}", mini).replace("{{QUESTION}}", question)

    return (
        "Eres un generador de SQL Server en SOLO LECTURA.\n"
        "Objetivo: generar UNA consulta SQL que responda la pregunta del usuario.\n\n"
        "Reglas OBLIGATORIAS:\n"
        "1) SOLO SELECT/CTE; prohibido INSERT/UPDATE/DELETE/DDL.\n"
        "2) Usa SOLO las tablas/columnas del esquema permitido (abajo). Prefiere esquema.tabla.\n"
        "3) Si no existe la entidad/columna requerida, devuelve JSON con needs_retry=true y reason.\n"
        '4) Devuelve ÚNICAMENTE un JSON válido: {"sql":"...","needs_retry":false,"reason":"..."}\n\n'
        f"Esquema permitido (recortado):\n{mini}\n\n"
        f"Pregunta del usuario:\n{question}"
    )


def _build_summary_prompt(question: str, rows: List[Dict[str, Any]], config: ConfigLoader) -> str:
    tpl = config.read_prompt("summary_prompt.txt") if hasattr(config, "read_prompt") else ""
    head = rows[:5]
    if tpl:
        return tpl.replace("{{QUESTION}}", question).replace("{{SAMPLE}}", json.dumps(head, ensure_ascii=False))

    return (
        "Resume en español, breve y claro, lo más relevante de los datos.\n"
        "- Indica cantidad de filas.\n"
        "- No inventes datos.\n"
        "- Si no hay filas, dilo explícitamente.\n\n"
        f"Pregunta:\n{question}\n\n"
        f"Muestras (hasta 5 filas):\n{json.dumps(head, ensure_ascii=False)}"
    )


class OpenAIService(LLMService):
    def __init__(self, config: ConfigLoader) -> None:
        self._config = config
        self._base = config.settings.get("LLM_OPENAI_BASE", "https://api.openai.com/v1")
        self._model = config.settings.get("LLM_OPENAI_MODEL", "gpt-4o-mini")
        self._api_key = config.read_key("openai.key")
        self._timeout = float(config.settings.get("REQUEST_TIMEOUT_SECONDS", 45))

    async def propose_sql(self, question: str, db_context: dict) -> Dict[str, Any]:
        prompt = _build_sql_prompt(question, db_context, self._config)
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
        }
        headers = {"Authorization": f"Bearer {self._api_key}"}
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(f"{self._base}/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
        return _first_json_in_text(content)

    async def summarize_answer(self, question: str, rows: List[Dict[str, Any]]) -> str:
        prompt = _build_summary_prompt(question, rows, self._config)
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
        }
        headers = {"Authorization": f"Bearer {self._api_key}"}
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(f"{self._base}/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()


class DeepSeekService(LLMService):
    def __init__(self, config: ConfigLoader) -> None:
        self._config = config
        self._base = config.settings.get("LLM_DEEPSEEK_BASE", "https://api.deepseek.com")
        self._model = config.settings.get("LLM_DEEPSEEK_MODEL", "deepseek-reasoner")
        self._api_key = config.read_key("deepseek.key")
        self._timeout = float(config.settings.get("REQUEST_TIMEOUT_SECONDS", 45))

    async def propose_sql(self, question: str, db_context: dict) -> Dict[str, Any]:
        prompt = _build_sql_prompt(question, db_context, self._config)
        payload = {"model": self._model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1}
        headers = {"Authorization": f"Bearer {self._api_key}"}
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(f"{self._base}/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
        return _first_json_in_text(content)

    async def summarize_answer(self, question: str, rows: List[Dict[str, Any]]) -> str:
        prompt = _build_summary_prompt(question, rows, self._config)
        payload = {"model": self._model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
        headers = {"Authorization": f"Bearer {self._api_key}"}
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(f"{self._base}/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()


class RuleBasedLLMService(LLMService):
    async def propose_sql(self, question: str, db_context: dict) -> Dict[str, Any]:
        q = (question or "").lower()

        if "total" in q and ("solicitudes" in q or "tramites" in q) and ("2025" in q):
            sql = (
                "SELECT COUNT(*) AS total "
                "FROM dbo.solicitud "
                "WHERE YEAR(fecha_creacion)=2025"
            )
            return {"sql": sql, "needs_retry": False, "reason": "Total de solicitudes en 2025"}

        if ("por mes" in q or "mensual" in q) and ("solicitudes" in q or "tramites" in q) and ("2025" in q):
            sql = (
                "SELECT FORMAT(fecha_creacion,'yyyy-MM') AS mes, COUNT(*) AS total "
                "FROM dbo.solicitud "
                "WHERE YEAR(fecha_creacion)=2025 "
                "GROUP BY FORMAT(fecha_creacion,'yyyy-MM') "
                "ORDER BY mes"
            )
            return {"sql": sql, "needs_retry": False, "reason": "Solicitudes por mes 2025"}

        if ("documentos" in q or "archivos" in q) and ("pendiente" in q or "no validados" in q or "sin validar" in q):
            sql = (
                "SELECT s.folio, d.nombre AS documento, sd.valido, sd.fecha_subida "
                "FROM dbo.solicitud_documento sd "
                "JOIN dbo.documento d ON d.id=sd.documento_id "
                "JOIN dbo.solicitud s ON s.id=sd.solicitud_id "
                "WHERE ISNULL(sd.valido,0)=0"
            )
            return {"sql": sql, "needs_retry": False, "reason": "Documentos no validados"}

        if ("seguimiento" in q or "supervisor" in q) and ("comentarios" in q or "observaciones" in q):
            sql = (
                "SELECT TOP 50 s.folio, seg.supervisor_id, seg.fecha, seg.comentario, seg.autorizado "
                "FROM dbo.seguimiento seg "
                "JOIN dbo.solicitud s ON s.id=seg.solicitud_id "
                "ORDER BY seg.fecha DESC"
            )
            return {"sql": sql, "needs_retry": False, "reason": "Seguimientos recientes"}

        if ("resoluciones" in q or "autorizaciones" in q) and ("2025" in q):
            sql = (
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN aprobada=1 THEN 1 ELSE 0 END) AS aprobadas "
                "FROM dbo.resolucion "
                "WHERE YEAR(fecha_resolucion)=2025"
            )
            return {"sql": sql, "needs_retry": False, "reason": "Resoluciones 2025"}

        if "total" in q and "citas" in q and "2025" in q:
            sql = "SELECT COUNT(*) AS total FROM dbo.cita WHERE YEAR(fecha)=2025"
            return {"sql": sql, "needs_retry": False, "reason": "Total de citas 2025"}

        return {"sql": "SELECT 1 AS resultado", "needs_retry": True, "reason": "Sin regla aplicable"}

    async def summarize_answer(self, question: str, rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return "No encontré datos para responder tu pregunta."
        cols = ", ".join(list(rows[0].keys()))
        return f"Pregunta: {question}. Filas: {len(rows)}. Columnas: {cols}."
