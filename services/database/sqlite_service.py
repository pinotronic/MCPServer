from __future__ import annotations
import aiosqlite
from typing import Any, Dict, List, Optional, Union

from services.database.base import DatabaseService


class SQLiteDatabaseService(DatabaseService):
    def __init__(self, config: Any) -> None:
        self._cfg = config
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        # Intenta obtener la ruta desde múltiples fuentes
        db_path = None
        if hasattr(self._cfg, 'settings') and 'SQLITE_PATH' in self._cfg.settings:
            db_path = self._cfg.settings['SQLITE_PATH']
        elif hasattr(self._cfg, 'sqlite_path'):
            db_path = self._cfg.sqlite_path
        else:
            db_path = "data/app.db"
        
        # Crear directorio si no existe
        from pathlib import Path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self._conn = await aiosqlite.connect(db_path)
        self._conn.row_factory = aiosqlite.Row

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def fetch_all(self, sql: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        if not self._conn:
            raise RuntimeError("SQLite no conectado")
        async with self._conn.execute(sql, params or {}) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_schema_overview(self) -> Dict[str, Any]:
        if not self._conn:
            raise RuntimeError("SQLite no conectado")
        out: Dict[str, Any] = {"tables": []}
        async with self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name") as cur:
            tables = [r[0] for r in await cur.fetchall()]
        for t in tables:
            async with self._conn.execute(f"PRAGMA table_info('{t}')") as cur:
                cols = await cur.fetchall()
                out["tables"].append({
                    "table": t,
                    "columns": [{"cid": c[0], "name": c[1], "type": c[2], "notnull": c[3], "dflt_value": c[4], "pk": c[5]} for c in cols]
                })
        return out
