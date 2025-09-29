from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import json
import os

@dataclass(frozen=True)
class ColumnDef:
    name: str
    type: str
    nullable: bool
    pk: bool = False
    identity: bool = False
    description: str = ""

@dataclass(frozen=True)
class TableDef:
    name: str
    schema: str
    full_name: str
    columns: List[ColumnDef] = field(default_factory=list)
    description: str = ""

@dataclass(frozen=True)
class DatabaseSchema:
    dialect: str
    tables: List[TableDef]

class SchemaProvider:
    def __init__(self, path: str) -> None:
        self._path = path
        self._schema: Optional[DatabaseSchema] = None

    def load(self) -> None:
        if not os.path.exists(self._path):
            raise FileNotFoundError(f"No se encontró el archivo: {self._path}")
        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)

        dialect = str(data.get("dialect") or data.get("engine") or "").strip()
        raw_tables = data.get("tables") or []

        # Procesar esquemas del nuevo formato si existe
        if "schemas" in data:
            schemas = data.get("schemas", [])
            if schemas:
                raw_tables = schemas[0].get("tables", [])

        tables: List[TableDef] = []
        for t in raw_tables:
            t_name = str(t.get("name") or t.get("table_name") or "").strip()
            schema_name = str(t.get("schema") or t.get("schema_name") or "dbo").strip()
            full_name = f"{schema_name}.{t_name}" if schema_name and t_name else t_name
            description = str(t.get("description") or "")
            cols: List[ColumnDef] = []
            for c in (t.get("columns") or []):
                cols.append(ColumnDef(
                    name=str(c.get("name") or c.get("column_name") or "").strip(),
                    type=str(c.get("type") or c.get("data_type") or "").strip(),
                    nullable=bool(c.get("nullable", True) if c.get("nullable") is not None else c.get("is_nullable", True)),
                    pk=bool(c.get("pk", False) or c.get("is_primary_key", False)),
                    identity=bool(c.get("identity", False) or c.get("is_identity", False)),
                    description=str(c.get("description") or "")
                ))
            
            # Crear TableDef con información adicional
            table_def = TableDef(
                name=t_name,
                schema=schema_name,
                full_name=full_name,
                columns=cols,
                description=description
            )
            
            # Agregar campos adicionales para el contexto semántico
            if hasattr(table_def, '__dict__'):
                table_def.__dict__['business_context'] = t.get('business_context', '')
                table_def.__dict__['synonyms'] = t.get('synonyms', [])
                table_def.__dict__['related_concepts'] = t.get('related_concepts', [])
            
            tables.append(table_def)
        self._schema = DatabaseSchema(dialect=dialect, tables=tables)

    @property
    def schema(self) -> DatabaseSchema:
        if self._schema is None:
            self.load()
        assert self._schema is not None
        return self._schema

    def list_tables(self) -> List[str]:
        return [t.full_name for t in self.schema.tables]

    def get_table(self, full_or_short_name: str) -> Optional[TableDef]:
        target = full_or_short_name.lower()
        for t in self.schema.tables:
            if t.full_name.lower() == target or t.name.lower() == target:
                return t
        return None

    def list_columns(self, full_or_short_name: str) -> List[str]:
        t = self.get_table(full_or_short_name)
        if not t:
            return []
        return [c.name for c in t.columns]

    def to_documents(self) -> List[Dict[str, Any]]:
        docs: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        for t in self.schema.tables:
            # ID canónico y estable en minúsculas para evitar duplicados por casing
            doc_id = t.full_name.lower().strip()

            # Si el JSON trae entradas duplicadas de la misma tabla, nos quedamos con la "mejor":
            # criterio simple: si ya existe, preferimos la que tenga descripción más larga.
            if doc_id in seen_ids:
                # Buscar el existente y comparar longitud de texto
                for existing in docs:
                    if existing["id"] == doc_id:
                        # Construir el texto de la tabla actual para comparar
                        lines_new = []
                        lines_new.append(f"Tabla: {t.full_name}")
                        if t.description:
                            lines_new.append(f"Descripción: {t.description}")
                        lines_new.append("Columnas:")
                        for c in t.columns:
                            col_line = f"- {c.name}: {c.type}"
                            extras = []
                            if c.pk:
                                extras.append("PK")
                            if c.identity:
                                extras.append("IDENTITY")
                            if not c.nullable:
                                extras.append("NOT NULL")
                            if c.description:
                                extras.append(f"desc={c.description}")
                            if extras:
                                col_line += f" ({', '.join(extras)})"
                            lines_new.append(col_line)
                        text_new = "\n".join(lines_new)

                        # Si el nuevo texto es más informativo, reemplazamos
                        if len(text_new) > len(existing["text"]):
                            existing["text"] = text_new
                            existing["metadata"] = {
                                "kind": "table",
                                "table": t.full_name,   # mantenemos el nombre tal cual (case original) como metadata
                                "schema": t.schema,
                                "dialect": self.schema.dialect
                            }
                        break
                continue  # ya manejado el duplicado, seguimos con la siguiente tabla

            # Construye el documento por primera vez
            lines = []
            lines.append(f"Tabla: {t.full_name}")
            if t.description:
                lines.append(f"Descripción: {t.description}")
            
            # Agregar contexto de negocio si existe
            business_context = getattr(t, '__dict__', {}).get('business_context', '')
            if business_context:
                lines.append(f"Contexto de negocio: {business_context}")
            
            # Agregar sinónimos para mejorar las búsquedas semánticas
            synonyms = getattr(t, '__dict__', {}).get('synonyms', [])
            if synonyms:
                lines.append(f"También conocido como: {', '.join(synonyms)}")
            
            # Agregar conceptos relacionados
            related_concepts = getattr(t, '__dict__', {}).get('related_concepts', [])
            if related_concepts:
                lines.append(f"Conceptos relacionados: {', '.join(related_concepts)}")
            
            lines.append("Columnas:")
            for c in t.columns:
                col_line = f"- {c.name}: {c.type}"
                extras = []
                if c.pk:
                    extras.append("PK")
                if c.identity:
                    extras.append("IDENTITY")
                if not c.nullable:
                    extras.append("NOT NULL")
                if c.description:
                    extras.append(f"desc={c.description}")
                if extras:
                    col_line += f" ({', '.join(extras)})"
                lines.append(col_line)
            text = "\n".join(lines)

            # Metadata enriquecida
            metadata = {
                "kind": "table",
                "table": t.full_name,
                "schema": t.schema,
                "dialect": self.schema.dialect
            }
            
            # Agregar metadatos semánticos (convertir listas a strings para ChromaDB)
            if business_context:
                metadata["business_context"] = business_context
            if synonyms:
                metadata["synonyms"] = ", ".join(synonyms)  # Convertir lista a string
            if related_concepts:
                metadata["related_concepts"] = ", ".join(related_concepts)  # Convertir lista a string

            docs.append({
                "id": doc_id,  # ¡minúsculas!
                "text": text,
                "metadata": metadata
            })
            seen_ids.add(doc_id)

        return docs
