import json
from pathlib import Path
from typing import Any, Dict


class ContextLoader:
    def __init__(self, context_path: str = "database_context.json"):
        self.context_path = Path(context_path)

    def load_context(self) -> Dict[str, Any]:
        if not self.context_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo {self.context_path}")
        with open(self.context_path, "r", encoding="utf-8") as f:
            return json.load(f)
