from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

# Si usas el adaptador que te compartí:
# utils/db_context_adapter.py con la función to_whitelist_format
try:
    from utils.db_context_adapter import to_whitelist_format
except Exception:
    def to_whitelist_format(db_ctx_raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fallback mínimo si no está presente el adaptador:
        - Si viene con "whitelist", lo respeta.
        - Si viene con "schemas"/"tables"/"columns", intenta aplanar.
        """
        if not isinstance(db_ctx_raw, dict):
            return {"engine": "", "database": "", "whitelist": {}, "aliases": {}}
        if "whitelist" in db_ctx_raw and isinstance(db_ctx_raw.get("whitelist"), dict):
            engine = db_ctx_raw.get("engine") or ""
            database = db_ctx_raw.get("database") or db_ctx_raw.get("database_name") or ""
            aliases = db_ctx_raw.get("aliases") or {}
            return {
                "engine": engine,
                "database": database,
                "whitelist": {
                    k: [str(x) for x in (v if isinstance(v, list) else [v])]
                    for k, v in db_ctx_raw["whitelist"].items()
                },
                "aliases": aliases if isinstance(aliases, dict) else {},
            }
        whitelist: Dict[str, list[str]] = {}
        schemas = db_ctx_raw.get("schemas") or []
        for sch in schemas:
            schema_name = (sch or {}).get("schema_name") or (sch or {}).get("schema") or "dbo"
            for t in (sch or {}).get("tables") or []:
                table_name = (t or {}).get("table_name")
                if not table_name:
                    continue
                fq = f"{schema_name}.{table_name}"
                cols: list[str] = []
                for c in (t or {}).get("columns") or []:
                    name = (c or {}).get("name")
                    if name:
                        cols.append(str(name))
                whitelist[fq] = cols
        engine = db_ctx_raw.get("engine") or ""
        database = db_ctx_raw.get("database") or db_ctx_raw.get("database_name") or ""
        return {"engine": engine, "database": database, "whitelist": whitelist, "aliases": {}}


class ConfigLoader:
    """
    Carga y expone:
      - settings desde config.txt (key=value)
      - contexto de BD desde database_context.json (adaptado a {whitelist, aliases})
      - prompts desde /prompts
      - llaves desde /secrets

    Convenciones:
      - base_dir es la raíz del proyecto (donde viven config.txt y database_context.json).
      - secrets/ contiene archivos .key (p.ej. openai.key, deepseek.key).
      - prompts/ contiene sql_prompt.txt y summary_prompt.txt (opcionales).
    """

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self.base_dir: Path = Path(base_dir) if base_dir else Path(__file__).resolve().parents[1]
        self.secrets_dir: Path = self.base_dir / "secrets"
        self.prompts_dir: Path = self.base_dir / "prompts"

        self._settings: Dict[str, Any] = {}
        self._db_context_raw: Dict[str, Any] = {}
        self._db_context_adapted: Dict[str, Any] = {}

        self._load_all()

    # -----------------------
    # Propiedades públicas
    # -----------------------

    @property
    def settings(self) -> Dict[str, Any]:
        return self._settings

    @property
    def db_context(self) -> Dict[str, Any]:
        return self._db_context_adapted

    @property
    def db_engine(self) -> str:
        # Prioriza config.txt; si no, toma del contexto de BD
        eng = (self._settings.get("DB_ENGINE") or self._db_context_adapted.get("engine") or "sqlite")
        return str(eng).lower()

    # -----------------------
    # Lectura de llaves y prompts
    # -----------------------

    def read_key(self, filename: str) -> str:
        """
        Lee un archivo dentro de /secrets y devuelve su contenido limpio.
        Ej: read_key("openai.key")
        """
        path = self.secrets_dir / filename
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8").strip()

    def read_prompt(self, filename: str) -> str:
        """
        Lee un prompt desde /prompts. Si no existe, devuelve cadena vacía.
        Ej: read_prompt("sql_prompt.txt")
        """
        path = self.prompts_dir / filename
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")

    # -----------------------
    # Utilidades
    # -----------------------

    def get_setting(self, key: str, default: Any = None) -> Any:
        return self._settings.get(key, default)

    def refresh(self) -> None:
        """ Vuelve a cargar config.txt y database_context.json. """
        self._load_all()

    # -----------------------
    # Carga interna
    # -----------------------

    def _load_all(self) -> None:
        self._settings = self._load_kv_file(self.base_dir / "config.txt")
        self._db_context_raw = self._load_json_file(self.base_dir / "database_context.json")
        self._db_context_adapted = to_whitelist_format(self._db_context_raw)

    @staticmethod
    def _load_kv_file(path: Path) -> Dict[str, Any]:
        """
        Lee un archivo key=value (sin comillas) y devuelve dict.
        Ignora líneas vacías o que empiezan con '#'.
        """
        if not path.exists():
            return {}
        result: Dict[str, Any] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                key = k.strip()
                val = v.strip()
                result[key] = val
        return result

    @staticmethod
    def _load_json_file(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
