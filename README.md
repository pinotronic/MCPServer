# MCP Server (NL → SQL)

Servidor FastAPI que convierte preguntas en lenguaje natural en consultas SQL seguras y devuelve respuestas con trazas auditables.

## Estructura
- `app/api`: Endpoints FastAPI
- `app/core`: Pipeline (intent, entities, selección, planning, ejecución, respuesta)
- `app/services`: Integraciones (BD, RAG/Chroma, schema provider)
- `app/data`: `database_context.json` (esquema) y `chroma_data/` (persistencia vectorial)
- `app/scripts`: utilidades (dedupe, diagnóstico, run_dev.ps1)

## Requisitos
- Python 3.11+
- Windows: ODBC Driver SQL Server si usas MSSQL
- `requirements.txt` con: `fastapi`, `uvicorn`, `chromadb==1.0.*`, `aiosqlite`, `pyodbc`, etc.

## Desarrollo
```powershell
.\app\scripts\run_dev.ps1 -Port 8000 -Reingest
