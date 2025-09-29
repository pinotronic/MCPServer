from __future__ import annotations
from typing import Any, Dict, List, Optional
from .chroma_repository import ChromaRepository

class KnowledgeRetriever:
    def __init__(self, repo: ChromaRepository) -> None:
        self._repo = repo

    def search(self, query: str, n_results: int = 5, dialect: Optional[str] = None, table: Optional[str] = None) -> List[Dict[str, Any]]:
        where: Dict[str, Any] = {}
        if dialect:
            where["dialect"] = dialect
        if table:
            where["table"] = table
        
        # Si where está vacío, pasar None en lugar de diccionario vacío
        where_filter = where if where else None
        
        res = self._repo.query(query_text=query, n_results=n_results, where=where_filter)
        ids = res.get("ids", [[]])[0]
        docs = res.get("documents", [[]])[0]
        metas = res.get("metadatas", [[]])[0]
        out: List[Dict[str, Any]] = []
        for i, docid in enumerate(ids):
            out.append({
                "id": docid,
                "text": docs[i] if i < len(docs) else "",
                "metadata": metas[i] if i < len(metas) else {}
            })
        return out
