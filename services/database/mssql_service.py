from __future__ import annotations
import asyncio
from typing import Any, Dict, List, Optional, Union

import pyodbc  # asegúrate de tenerlo en requirements

from services.database.base import DatabaseService


class SqlServerDatabaseService(DatabaseService):
    def __init__(self, config: Any) -> None:
        self._cfg = config
        self._conn: Optional[pyodbc.Connection] = None

    async def connect(self) -> None:
        # Construir cadena de conexión desde la configuración
        if hasattr(self._cfg, 'settings'):
            settings = self._cfg.settings
            server = settings.get('DB_SERVER', 'localhost')
            port = settings.get('DB_PORT', '1433')
            database = settings.get('DB_DATABASE', '')
            user = settings.get('DB_USER', '')
            password = settings.get('DB_PASSWORD', '')
            encrypt = settings.get('DB_ENCRYPT', 'no')
            trust_cert = settings.get('DB_TRUSTSERVERCERT', 'no')
            
            # Construir cadena de conexión
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"Encrypt={encrypt};"
                f"TrustServerCertificate={trust_cert};"
            )
        else:
            # Fallback al método anterior
            conn_str = getattr(self._cfg, "mssql_conn_str", None)
            if not conn_str:
                raise RuntimeError("Configuración de SQL Server no encontrada")
        
        self._conn = await asyncio.to_thread(pyodbc.connect, conn_str)

    async def disconnect(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    async def fetch_all(self, sql: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        if not self._conn:
            raise RuntimeError("SQL Server no conectado")

        def run_query() -> List[Dict[str, Any]]:
            with self._conn.cursor() as cur:
                if isinstance(params, list):
                    cur.execute(sql, *params)
                elif isinstance(params, dict) and params:
                    # Pocas veces usaremos dict en SQL Server; intentamos orden por aparición de ':name'
                    import re
                    names = re.findall(r":([a-zA-Z_][a-zA-Z0-9_]*)", sql)
                    seq = [params[n] for n in names]
                    q = re.sub(r":[a-zA-Z_][a-zA-Z0-9_]*", "?", sql)
                    cur.execute(q, *seq)
                else:
                    cur.execute(sql)
                cols = [d[0] for d in cur.description] if cur.description else []
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        return await asyncio.to_thread(run_query)

    async def get_schema_overview(self) -> Dict[str, Any]:
        if not self._conn:
            raise RuntimeError("SQL Server no conectado")

        def run_overview() -> Dict[str, Any]:
            out: Dict[str, Any] = {"tables": []}
            with self._conn.cursor() as cur:
                cur.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)
                tables = [(r[0], r[1]) for r in cur.fetchall()]
                for sch, tab in tables:
                    cur.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
                               COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsComputed') AS IS_COMPUTED
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
                        ORDER BY ORDINAL_POSITION
                    """, sch, tab)
                    cols = cur.fetchall()
                    out["tables"].append({
                        "schema": sch, "table": tab,
                        "columns": [{"name": c[0], "type": c[1], "nullable": c[2], "identity": c[3], "computed": c[4]} for c in cols]
                    })
            return out

        return await asyncio.to_thread(run_overview)
