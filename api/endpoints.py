from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from models.request_models import (
    QueryRequest,
    IterativeAnalysisRequest,
    HealthCheckRequest,
    SchemaInfoRequest,
    DirectSQLRequest,
)
from models.response_models import StandardResponse
from services.query_processor import QueryProcessor

logger = logging.getLogger("mcp.api")


class Dependencies:
    """
    Adaptador de dependencias desde el contenedor 'services' de tu app.
    Se asume que 'services' expone: config, db, query_processor, iterative.
    """

    def __init__(self, services: Any) -> None:
        self._services = services

    def config(self):
        cfg = getattr(self._services, "config", None)
        if not cfg:
            raise HTTPException(status_code=503, detail="Config no inicializada")
        return cfg

    def db(self):
        db = getattr(self._services, "db", None)
        if not db:
            raise HTTPException(status_code=503, detail="DB no inicializada")
        return db

    def qp(self) -> QueryProcessor:
        qp = getattr(self._services, "query_processor", None)
        if not qp:
            raise HTTPException(status_code=503, detail="QueryProcessor no inicializado")
        return qp

    def iterative(self):
        it = getattr(self._services, "iterative", None)
        if not it:
            raise HTTPException(status_code=503, detail="IterativeAnalysisService no inicializado")
        return it


def get_router(services: Any) -> APIRouter:
    """
    Crea el router principal de la API usando el contenedor de servicios.
    """
    deps = Dependencies(services)
    router = APIRouter(prefix="/api", tags=["MCP"])

    @router.get("/health")
    async def health(_: HealthCheckRequest = Depends()) -> StandardResponse:
        return StandardResponse(
            status="success",
            message="OK",
            timestamp=datetime.utcnow(),
            data={"service": "mcp", "version": "1.0.0"}
        )

    @router.post("/schema")
    async def schema_info(_: SchemaInfoRequest, db=Depends(deps.db)) -> StandardResponse:
        # reusa tu DatabaseService actual
        schema = await db.get_schema_overview()
        return StandardResponse(
            status="success",
            message="Esquema de base de datos",
            timestamp=datetime.utcnow(),
            data=schema
        )

    @router.post("/query")
    async def one_shot(req: QueryRequest, qp: QueryProcessor = Depends(deps.qp)) -> StandardResponse:
        """
        Orquesta el pipeline NL→SQL→Respuesta a través de QueryProcessor.
        Detecta automáticamente el dialecto si no se especifica en el request.
        """
        try:
            # Si tu QueryRequest trae 'dialect', úsalo; si no, None para autodetección.
            dialect = getattr(req, "dialect", None)
            result = await qp.answer_one_shot(req.question, dialect=dialect)
            return StandardResponse(
                status=result.get("status", "success"),
                message=result.get("message", "OK"),
                timestamp=datetime.utcnow(),
                data={
                    "result": result.get("data"),
                    "trace": result.get("trace"),
                    "warnings": result.get("warnings", []),
                }
            )
        except Exception as ex:
            logger.exception("Fallo en /query: %s", ex)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Fallo al procesar la consulta: {ex}"
            )

    @router.post("/iterative")
    async def iterative(req: IterativeAnalysisRequest, it=Depends(deps.iterative)) -> StandardResponse:
        """
        Mantén tu endpoint iterativo; puedes actualizar su implementación interna
        para que use el nuevo QueryProcessor si quieres.
        """
        import traceback
        try:
            result = await it.analyze_and_respond(
                original_question=req.question,
                llm_provider=getattr(req, "llm_provider", None),
                max_iterations=req.max_iterations
            )
            return StandardResponse(
                status="success",
                message="Respuesta final",
                timestamp=datetime.utcnow(),
                data=result
            )
        except Exception as ex:
            tb = traceback.format_exc()
            logger.exception("Fallo en /iterative: %s", ex)
            return StandardResponse(
                status="error",
                message=f"Error interno: {ex}",
                timestamp=datetime.utcnow(),
                data=tb
            )

    @router.post("/sql")
    async def direct_sql(req: DirectSQLRequest, db=Depends(deps.db)) -> StandardResponse:
        """
        Ejecución directa (solo para desarrollo). En producción,
        prefiere el pipeline validado del /query.
        """
        try:
            rows = await db.fetch_all(req.sql)  # si soportas params, amplía firma a (sql, params)
            return StandardResponse(
                status="success",
                message="Consulta ejecutada",
                timestamp=datetime.utcnow(),
                data={"rows": rows}
            )
        except Exception as ex:
            logger.exception("Fallo en /sql: %s", ex)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error SQL: {ex}"
            )

    @router.get("/db/context")
    async def db_context(cfg=Depends(deps.config)) -> StandardResponse:
        """
        Expone el contexto adaptado que arma tu ConfigLoader (whitelist/aliases).
        """
        return StandardResponse(
            status="success",
            message="Contexto de base de datos (adaptado)",
            timestamp=datetime.utcnow(),
            data=getattr(cfg, "db_context", {})
        )

    return router
