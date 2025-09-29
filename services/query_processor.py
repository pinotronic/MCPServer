from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional

# Core (pipeline)
from core.intent.detector import IntentDetector, Intent
from core.extraction.entities import extract_entities
from core.selection.table_selector import TableSelector, TableSnapshot, SemanticRetriever
from core.selection.column_selector import ColumnSelector, profile_from_snapshot, ColumnRole, TableProfile
from core.planning.sql_planner import SqlPlanner, SqlPlan
from core.planning.sql_validator import SqlValidator, build_catalog_from_schema_provider, ValidationResult
from core.execution.db_executor import DBExecutor, ServiceDBGateway, QueryResult
from core.answering.formatter import AnswerFormatter, AnswerPayload
from services.database.base import DatabaseService

logger = logging.getLogger("mcp.query")


class QueryProcessor:
    """
    Orquestador determinista del flujo NL → SQL → Respuesta.

    Dependencias mínimas:
      - schema_provider: expone .schema.tables con full_name, schema, name, columns[*]
      - db_service: implementa fetch_all(sql, params?) (se envuelve con ServiceDBGateway)
      - retriever (opcional): cumple SemanticRetriever (Chroma/otro)
      - config (opcional): para futuras extensiones
      - llm (opcional): no se usa en este flujo determinista; puedes inyectarlo para fallback
    """

    def __init__(
        self,
        *,
        schema_provider: Any,
        db_service: Any,
        retriever: Optional[SemanticRetriever] = None,
        config: Optional[Any] = None,
        llm: Optional[Any] = None,
    ) -> None:
        self._schema_provider = schema_provider
        self._db_service = db_service
        self._retriever = retriever
        self._config = config
        self._llm = llm

        # Componentes core (puedes inyectarlos si quieres probar/mokear)
        self._detector = IntentDetector()
        self._table_selector = TableSelector()
        self._column_selector = ColumnSelector()
        self._planner = SqlPlanner()
        self._validator = SqlValidator()
        self._formatter = AnswerFormatter()

    # ---------------------------------------------------------------------
    #   API pública (principal)
    # ---------------------------------------------------------------------

    async def answer_one_shot(self, question: str, dialect: Optional[str] = None) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo y devuelve un dict listo para JSON.
        """
        try:
            intent = self._detector.detect(question).intent
            entities = extract_entities(question)
            dialect_eff = self._detect_dialect(dialect)

            # 1) Seleccionar tabla
            snapshots = self._snapshots_from_provider()
            sel_res = self._table_selector.select(
                question=question,
                tables=snapshots,
                entities=entities,
                retriever=self._retriever,
                dialect=dialect_eff
            )
            if not sel_res.chosen:
                return self._make_error(
                    code="no_table_candidate",
                    message="No se encontró una tabla candidata para la consulta.",
                    extra={"candidates": [c.table.full_name for c in sel_res.candidates], "reasons": sel_res.reasons}
                )

            chosen = sel_res.chosen.table

            # 2) Seleccionar columnas clave
            profile = profile_from_snapshot(
                full_name=chosen.full_name,
                name=chosen.name,
                schema=chosen.schema,
                columns=chosen.columns,
                column_types=self._column_types_for(chosen.full_name),
                primary_keys=self._primary_keys_for(chosen.full_name),
                foreign_keys=None,
                descriptions=None,
            )
            col_sel = self._column_selector.select(
                table=profile,
                question=question,
                entities=entities
            )

            # 3) Planear SQL
            plan = self._planner.build(
                intent=intent,
                dialect=dialect_eff,
                table=profile,
                columns=col_sel,
                entities=entities,
                question=question
            )

            # 4) Validar SQL contra catálogo
            catalog = build_catalog_from_schema_provider(self._schema_provider)
            validation = self._validator.validate(plan=plan, catalog=catalog, table=profile, columns=col_sel)

            if not validation.ok:
                # Errores: devolvemos payload con issues; no ejecutamos SQL
                return self._format_payload(intent, plan, None, validation)

            # 5) Ejecutar SQL
            executor = DBExecutor(ServiceDBGateway(self._db_service), dialect=plan.dialect)
            result = await executor.execute(plan)

            # 6) Formatear respuesta
            return self._format_payload(intent, plan, result, validation)

        except Exception as ex:
            logger.exception("Fallo en answer_one_shot: %s", ex)
            return self._make_error("internal_error", f"Error interno: {ex}")

    # ---------------------------------------------------------------------
    #   Helpers de orquestación
    # ---------------------------------------------------------------------

    def _detect_dialect(self, explicit: Optional[str]) -> str:
        if explicit:
            return explicit.strip().lower()

        # Intenta desde provider.schema.dialect
        try:
            d = getattr(getattr(self._schema_provider, "schema", None), "dialect", None)
            if d:
                return str(d).strip().lower()
        except Exception:
            pass

        # Intenta desde config (si tuvieses algo como config.db_engine)
        try:
            eng = getattr(self._config, "db_engine", None)
            if isinstance(eng, str) and eng.lower() in ("sqlite", "sqlserver", "postgres"):
                return "sqlite" if eng.lower() == "sqlite" else ("sqlserver" if eng.lower() == "sqlserver" else "postgres")
        except Exception:
            pass

        return "sqlserver"  # por defecto en tu entorno

    def _snapshots_from_provider(self) -> List[TableSnapshot]:
        out: List[TableSnapshot] = []
        try:
            tables = list(getattr(getattr(self._schema_provider, "schema", None), "tables", []) or [])
        except Exception:
            tables = []

        for t in tables:
            try:
                full = str(getattr(t, "full_name", "") or "").strip()
                schema = str(getattr(t, "schema", "") or "").strip().lower()
                name = str(getattr(t, "name", "") or "").strip().lower() or (full.split(".")[-1].lower() if full else "")
                cols = [str(getattr(c, "name", "") or "").strip().lower() for c in (getattr(t, "columns", []) or []) if getattr(c, "name", None)]
                if full and name:
                    out.append(TableSnapshot(full_name=full.lower(), name=name, schema=schema, columns=cols))
            except Exception:
                continue
        return out

    def _column_types_for(self, full_table_name: str) -> Dict[str, str]:
        """
        Devuelve mapa nombre_columna -> tipo (si el provider lo expone).
        """
        types: Dict[str, str] = {}
        try:
            schema = getattr(self._schema_provider, "schema", None)
            for t in getattr(schema, "tables", []) or []:
                if str(getattr(t, "full_name", "")).strip().lower() == full_table_name.lower():
                    for c in getattr(t, "columns", []) or []:
                        cname = str(getattr(c, "name", "") or "").strip().lower()
                        ctype = str(getattr(c, "type", "") or "")
                        if cname:
                            types[cname] = ctype
                    break
        except Exception:
            pass
        return types

    def _primary_keys_for(self, full_table_name: str) -> List[str]:
        pks: List[str] = []
        try:
            schema = getattr(self._schema_provider, "schema", None)
            for t in getattr(schema, "tables", []) or []:
                if str(getattr(t, "full_name", "")).strip().lower() == full_table_name.lower():
                    for c in getattr(t, "columns", []) or []:
                        if bool(getattr(c, "pk", False)):
                            cname = str(getattr(c, "name", "") or "").strip().lower()
                            if cname:
                                pks.append(cname)
                    break
        except Exception:
            pass
        return pks

    def _format_payload(
        self,
        intent: Intent,
        plan: SqlPlan,
        result: Optional[QueryResult],
        validation: Optional[ValidationResult]
    ) -> Dict[str, Any]:
        formatter = self._formatter

        if result is None:
            # No ejecutamos por errores; fabricamos un QueryResult vacío para tiempos/shape
            empty = QueryResult(rows=[], columns=[], rowcount=0, elapsed_ms=0.0, meta=plan.meta, warnings=plan.warnings)
            payload = formatter.format(intent=intent, plan=plan, result=empty, validation=validation)
        else:
            payload = formatter.format(intent=intent, plan=plan, result=result, validation=validation)

        # AnswerPayload → dict para JSON
        return {
            "status": payload.status,
            "message": payload.message,
            "data": payload.data,
            "trace": payload.trace,
            "warnings": payload.warnings,
        }

    def _make_error(self, code: str, message: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "status": "error",
            "message": message,
            "data": {},
            "trace": {"code": code},
            "warnings": [],
        }
        if extra:
            out["trace"].update(extra)
        return out
