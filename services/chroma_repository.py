from __future__ import annotations
from typing import Any, Dict, List, Optional
from chromadb import PersistentClient
from chromadb.utils import embedding_functions

class ChromaRepository:
    def __init__(self, path: str = "./chroma_data", collection_name: str = "schema_docs") -> None:
        self._client = PersistentClient(path=path)
        self._collection = self._client.get_or_create_collection(name=collection_name)
        # Usa DefaultEmbeddingFunction por simplicidad; puedes inyectar otro modelo
        self._embed_fn = embedding_functions.DefaultEmbeddingFunction()

    def upsert_documents(self, docs: List[Dict[str, Any]]) -> None:
        if not docs:
            return

        # 1) Normaliza y deduplica por ID (minúsculas), “último gana” con preferencia por texto más largo
        unique: dict[str, Dict[str, Any]] = {}
        for d in docs:
            raw_id = str(d["id"]).strip().lower()
            text = str(d["text"])
            meta = d.get("metadata", {}) or {}
            if raw_id not in unique:
                unique[raw_id] = {"id": raw_id, "text": text, "metadata": meta}
            else:
                if len(text) > len(unique[raw_id]["text"]):
                    unique[raw_id] = {"id": raw_id, "text": text, "metadata": meta}

        items = list(unique.values())

        # 2) Verificación defensiva: IDs repetidos no deben existir ahora
        assert len(items) == len(set(x["id"] for x in items)), "Aún hay IDs duplicados tras deduplicación local."

        # 3) Upsert por lotes para evitar requests muy grandes
        def batches(seq, size: int = 256):
            for i in range(0, len(seq), size):
                yield seq[i:i + size]

        if self._embed_fn is None:
            raise RuntimeError(
                "No hay embedding_function configurada en ChromaRepository. "
                "Instala un backend de embeddings o habilita DefaultEmbeddingFunction."
            )

        for batch in batches(items, 256):
            ids = [x["id"] for x in batch]
            texts = [x["text"] for x in batch]
            metas = [x["metadata"] for x in batch]
            self._collection.upsert(ids=ids, documents=texts, metadatas=metas)

        try:
            self._client.persist()
        except Exception:
            pass


    def delete_by_ids(self, ids: List[str]) -> None:
        if not ids:
            return
        self._collection.delete(ids=ids)

    def query(self, query_text: str, n_results: int = 5, where: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Solo pasar el filtro where si tiene contenido
        if where:
            return self._collection.query(query_texts=[query_text], n_results=n_results, where=where)
        else:
            return self._collection.query(query_texts=[query_text], n_results=n_results)

