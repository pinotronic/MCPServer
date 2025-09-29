from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.endpoints import get_router

from utils.config_loader import ConfigLoader

from services.database.base import DatabaseService
from services.database.sqlite_service import SQLiteDatabaseService
from services.database.mssql_service import SqlServerDatabaseService

from services.schema_provider import SchemaProvider
from services.chroma_repository import ChromaRepository
from services.knowledge_retriever import KnowledgeRetriever

from services.query_processor import QueryProcessor
from services.iterative_analysis_service import IterativeAnalysisService


# ---------------------------------
# Logging
# ---------------------------------
logger = logging.getLogger("mcp")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)


# ---------------------------------
# Contenedor de servicios
# ---------------------------------
class AppServices:
    def __init__(self) -> None:
        self.config: Optional[ConfigLoader] = None
        self.db: Optional[DatabaseService] = None
        self.schema_provider: Optional[SchemaProvider] = None
        self.retriever: Optional[KnowledgeRetriever] = None
        self.query_processor: Optional[QueryProcessor] = None
        self.iterative: Optional[IterativeAnalysisService] = None
        self.llm: Optional[object] = None  # reservado por si integras LLM luego


services = AppServices()


# ---------------------------------
# Ciclo de vida (startup/shutdown)
# ---------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Config
    services.config = ConfigLoader()

    # DB
    engine = (services.config.db_engine or "").strip().lower()
    if engine == "sqlite":
        services.db = SQLiteDatabaseService(services.config)
    else:
        services.db = SqlServerDatabaseService(services.config)

    await services.db.connect()
    logger.info("Base de datos conectada (%s)", engine if engine else "sqlserver")

    # SchemaProvider (carga el esquema del JSON)
    services.schema_provider = SchemaProvider(path="data/database_context.json")
    services.schema_provider.load()
    logger.info("SchemaProvider cargado (data/database_context.json)")

    # RAG / Chroma (opcional pero recomendado)
    repo = ChromaRepository(path="data/chroma_data", collection_name="schema_docs")
    services.retriever = KnowledgeRetriever(repo)
    logger.info("Repositorio Chroma inicializado (data/chroma_data)")

    # QueryProcessor (pipeline NL → SQL → Respuesta)
    services.query_processor = QueryProcessor(
        schema_provider=services.schema_provider,
        db_service=services.db,
        retriever=services.retriever,
        config=services.config,
        llm=services.llm
    )

    # Servicio iterativo (por ahora reutiliza el pipeline determinista)
    services.iterative = IterativeAnalysisService(services.query_processor)

    logger.info("Servicios inicializados")
    try:
        yield
    finally:
        try:
            await services.db.disconnect()  # type: ignore[union-attr]
            logger.info("Base de datos desconectada")
        except Exception as ex:
            logger.warning("Fallo al desconectar la BD: %s", ex)
        logger.info("Servicios detenidos")


# ---------------------------------
# FastAPI app
# ---------------------------------
app = FastAPI(title="MCP Server", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Router principal (usa el contenedor de servicios)
app.include_router(get_router(services))


# ---------------------------------
# Ejecución directa (dev)
# ---------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
