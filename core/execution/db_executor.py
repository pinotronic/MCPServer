from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Union


# ---------------------------
# Tipos de salida del ejecutor
# ---------------------------

@dataclass(frozen=True)
class QueryResult:
    rows: List[Dict[str, Any]]
    columns: List[str]
    rowcount: int
    elapsed_ms: float
    meta: Dict[str, Any]
    warnings: List[str]


# ---------------------------
# Contrato del gateway de BD
# ---------------------------

class DatabaseGateway(Protocol):
    async def fetch_all(self, sql: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        ...


# ---------------------------
# Adaptador para tu servicio
# ---------------------------

class ServiceDBGateway:
    """
    Adaptador fino para envolver tu DatabaseService existente.
    Si tu servicio ya acepta params en fetch_all(sql, params), se usan.
    Si NO los acepta, lanza TypeError cuando se intenten usar params.
    """
    def __init__(self, service: Any) -> None:
        self._svc = service

    async def fetch_all(self, sql: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        try:
            # Intento con firma (sql, params)
            return await self._svc.fetch_all(sql, params)  # type: ignore[call-arg]
        except TypeError:
            if params:
                raise TypeError(
                    "El DatabaseService actual no soporta parámetros. "
                    "Actualiza la firma a fetch_all(sql, params=None) para evitar concatenar SQL."
                )
            # Fallback solo si no hay params
            return await self._svc.fetch_all(sql)


# ---------------------------
# Ejecutor principal
# ---------------------------

class DBExecutor:
    """
    Ejecuta un plan SQL seguro (solo SELECT/WITH) contra un DatabaseGateway.
    - sqlserver: usa placeholders '?' y plan.params_seq
    - sqlite/postgres/otros: usa ':nombre' y plan.params_named
    Devuelve filas como lista de dicts + métricas.
    """

    _DANGEROUS_RX = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|MERGE|EXEC|EXECUTE|CALL|CREATE|GRANT|REVOKE)\b",
        re.IGNORECASE
    )
    _SELECT_RX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)

    def __init__(self, db: DatabaseGateway, dialect: str) -> None:
        self._db = db
        self._dialect = (dialect or "").strip().lower()

    async def execute(self, plan: Any) -> QueryResult:
        """
        Espera un objeto con atributos:
          - dialect: str
          - sql: str
          - params_seq: List[Any] (sqlserver)
          - params_named: Dict[str, Any] (otros)
          - meta: Dict[str, Any]
          - warnings: List[str]
        """
        self._guard_select(plan.sql)

        sql = str(plan.sql)
        params: Optional[Union[List[Any], Dict[str, Any]]] = None

        if (plan.dialect or "").lower() == "sqlserver":
            params = list(getattr(plan, "params_seq", []) or [])
        else:
            params = dict(getattr(plan, "params_named", {}) or {})

        t0 = time.perf_counter()
        rows = await self._db.fetch_all(sql, params)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        rows = self._ensure_dict_rows(rows)
        columns = list(rows[0].keys()) if rows else []
        rowcount = len(rows)

        warnings = list(getattr(plan, "warnings", []) or [])
        meta = dict(getattr(plan, "meta", {}) or {})

        return QueryResult(
            rows=rows,
            columns=columns,
            rowcount=rowcount,
            elapsed_ms=round(elapsed_ms, 3),
            meta=meta,
            warnings=warnings
        )

    # ---------------------------
    # Helpers internos
    # ---------------------------

    def _guard_select(self, sql: str) -> None:
        if self._DANGEROUS_RX.search(sql or ""):
            raise ValueError("SQL potencialmente peligroso detectado. Solo se permiten consultas SELECT/WITH.")
        if not self._SELECT_RX.search(sql or ""):
            raise ValueError("El SQL no parece ser una consulta SELECT/WITH.")

    @staticmethod
    def _ensure_dict_rows(rows: Any) -> List[Dict[str, Any]]:
        """
        Normaliza el resultado a List[Dict]. Si vienen tuplas u objetos con ._asdict(),
        intenta convertirlos. Si ya son dicts, los retorna tal cual.
        """
        if not rows:
            return []
        if isinstance(rows, list):
            if isinstance(rows[0], dict):
                return rows
            try:
                # Caso namedtuple-like
                return [r._asdict() for r in rows]  # type: ignore[attr-defined]
            except Exception:
                pass
            # Intento genérico: si es secuencia, no sabemos nombres → indexa por posición
            try:
                out: List[Dict[str, Any]] = []
                for r in rows:
                    if isinstance(r, (list, tuple)):
                        out.append({str(i): v for i, v in enumerate(r)})
                    else:
                        out.append({"value": r})
                return out
            except Exception:
                return [{"value": x} for x in rows]
        # Último recurso: envolver en lista
        return [{"value": rows}]
