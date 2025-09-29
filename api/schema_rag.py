from __future__ import annotations
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from models.response_models import StandardResponse
from services.schema_provider import SchemaProvider
from services.knowledge_retriever import KnowledgeRetriever

# Solo para type-checkers; NO se ejecuta en runtime, evita el ciclo de imports.
if TYPE_CHECKING:
    from main import AppServices  # type: ignore

class SearchRequest(BaseModel):
    query: str = Field(..., description="Búsqueda semántica")
    n_results: int = Field(5, ge=1, le=20)
    dialect: Optional[str] = None
    table: Optional[str] = None

def get_schema_rag_router(services: "AppServices | Any") -> APIRouter:
    router = APIRouter(tags=["Schema/RAG"])

    def provider_dep() -> SchemaProvider:
        sp = getattr(services, "schema_provider", None)
        if sp is None:
            sp = SchemaProvider(path="database_context.json")
            sp.load()
            services.schema_provider = sp
        return sp

    def retriever_dep() -> KnowledgeRetriever:
        kr = getattr(services, "knowledge", None)
        if kr is None:
            # Import local para evitar dependencias en import-time
            from services.chroma_repository import ChromaRepository
            repo = getattr(services, "chroma", None) or ChromaRepository(path="./chroma_data", collection_name="schema_docs")
            services.chroma = repo
            kr = KnowledgeRetriever(repo)
            services.knowledge = kr
        return kr

    @router.get("/schema/tables")
    def list_tables(provider: SchemaProvider = Depends(provider_dep)) -> StandardResponse:
        return StandardResponse(
            status="success",
            message="Listado de tablas",
            timestamp=datetime.utcnow(),
            data=provider.list_tables()
        )

    @router.get("/schema/table/{table_name}")
    def get_table(table_name: str, provider: SchemaProvider = Depends(provider_dep)) -> StandardResponse:
        t = provider.get_table(table_name)
        if not t:
            raise HTTPException(status_code=404, detail=f"Tabla no encontrada: {table_name}")
        payload = {
            "table": t.full_name,
            "schema": t.schema,
            "dialect": provider.schema.dialect,
            "description": t.description,
            "columns": [
                {
                    "name": c.name,
                    "type": c.type,
                    "nullable": c.nullable,
                    "pk": c.pk,
                    "identity": c.identity,
                    "description": c.description
                } for c in t.columns
            ],
        }
        return StandardResponse(
            status="success",
            message="Detalle de tabla",
            timestamp=datetime.utcnow(),
            data=payload
        )

    @router.get("/schema/columns/{table_name}")
    def list_columns(table_name: str, provider: SchemaProvider = Depends(provider_dep)) -> StandardResponse:
        cols = provider.list_columns(table_name)
        if not cols:
            raise HTTPException(status_code=404, detail=f"No hay columnas o tabla no encontrada: {table_name}")
        return StandardResponse(
            status="success",
            message="Columnas de la tabla",
            timestamp=datetime.utcnow(),
            data={"table": table_name, "columns": cols}
        )

    @router.post("/rag/search")
    def rag_search(body: SearchRequest, retriever: KnowledgeRetriever = Depends(retriever_dep)) -> StandardResponse:
        hits = retriever.search(query=body.query, n_results=body.n_results, dialect=body.dialect, table=body.table)
        return StandardResponse(
            status="success",
            message="Resultados de búsqueda semántica",
            timestamp=datetime.utcnow(),
            data=hits
        )

    return router
