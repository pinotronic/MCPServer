from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from core.planning.sql_planner import SqlPlan
from core.selection.column_selector import ColumnSelectionResult, ColumnRole, TableProfile


# =========================
#   Modelos de validación
# =========================

@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: Optional[str] = None
    pk: bool = False
    nullable: Optional[bool] = None
    description: str = ""


@dataclass(frozen=True)
class TableInfo:
    full_name: str          # schema.name
    schema: str
    name: str
    columns: Dict[str, ColumnInfo]  # key en minúsculas
    pk_columns: List[str] = field(default_factory=list)


@dataclass
class SchemaCatalog:
    """
    Catálogo en memoria derivado del SchemaProvider.
    Keys en minúsculas para robustez: "dbo.cita"
    """
    tables: Dict[str, TableInfo] = field(default_factory=dict)

    def has_table(self, full_name: str) -> bool:
        return self._key(full_name) in self.tables

    def get_table(self, full_name: str) -> Optional[TableInfo]:
        return self.tables.get(self._key(full_name))

    def has_column(self, full_name: str, column: str) -> bool:
        t = self.get_table(full_name)
        return bool(t and self._colkey(column) in t.columns)

    def get_column(self, full_name: str, column: str) -> Optional[ColumnInfo]:
        t = self.get_table(full_name)
        if not t:
            return None
        return t.columns.get(self._colkey(column))

    @staticmethod
    def _key(full_name: str) -> str:
        return full_name.strip().lower()

    @staticmethod
    def _colkey(col: str) -> str:
        return col.strip().lower()


@dataclass
class ValidationIssue:
    level: str             # "error" | "warning" | "info"
    code: str              # código breve p.ej. "table_not_found"
    message: str           # mensaje legible
    details: Dict[str, str] = field(default_factory=dict)


@dataclass
class ValidationResult:
    ok: bool
    issues: List[ValidationIssue] = field(default_factory=list)

    def add_error(self, code: str, message: str, **details: str) -> None:
        self.issues.append(ValidationIssue(level="error", code=code, message=message, details=details))

    def add_warning(self, code: str, message: str, **details: str) -> None:
        self.issues.append(ValidationIssue(level="warning", code=code, message=message, details=details))

    def add_info(self, code: str, message: str, **details: str) -> None:
        self.issues.append(ValidationIssue(level="info", code=code, message=message, details=details))

    def finalize(self) -> "ValidationResult":
        self.ok = not any(i.level == "error" for i in self.issues)
        return self


# =========================
#   Builder de catálogo
# =========================

def build_catalog_from_schema_provider(provider: object) -> SchemaCatalog:
    """
    Adapta un SchemaProvider existente a SchemaCatalog.
    Se espera que provider.schema.tables sea una lista de objetos con:
      - full_name, schema, name
      - columns: lista de objetos con: name, type, pk, nullable, description
    """
    cat = SchemaCatalog()
    try:
        schema = provider.schema  # type: ignore[attr-defined]
        tables = getattr(schema, "tables", [])
    except Exception:
        return cat

    for t in tables:
        try:
            full = str(getattr(t, "full_name", "")).strip()
            schema_name = str(getattr(t, "schema", "")).strip()
            name = str(getattr(t, "name", "")).strip() or full.split(".")[-1]
            cols_src = list(getattr(t, "columns", []) or [])
        except Exception:
            continue

        cols: Dict[str, ColumnInfo] = {}
        pk_cols: List[str] = []
        for c in cols_src:
            try:
                cname = str(getattr(c, "name", "")).strip()
                ctype = getattr(c, "type", None)
                cpk = bool(getattr(c, "pk", False))
                cnull = getattr(c, "nullable", None)
                cdesc = getattr(c, "description", "") or ""
            except Exception:
                continue
            if not cname:
                continue
            info = ColumnInfo(name=cname, type=ctype, pk=cpk, nullable=cnull, description=cdesc)
            cols[cname.lower()] = info
            if cpk:
                pk_cols.append(cname)

        if not full:
            # reconstruye si hiciera falta
            full = f"{schema_name}.{name}" if schema_name else name

        table_info = TableInfo(
            full_name=full,
            schema=schema_name or full.split(".")[0] if "." in full else "",
            name=name or full.split(".")[-1],
            columns=cols,
            pk_columns=pk_cols
        )
        cat.tables[full.lower()] = table_info
    return cat


# =========================
#    Validador de planes
# =========================

class SqlValidator:
    """
    Valida de forma determinista que un SqlPlan:
     - Referencie una tabla existente
     - Use columnas existentes (roles DATE/STATUS/ID)
     - Tenga parámetros consistentes con el SQL final
     - No contenga operaciones peligrosas (DML/DDL/EXEC)
     - Opcionalmente, reporte warnings útiles (filtro de fecha ausente, etc.)
    """

    _DANGEROUS_RX = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|MERGE|EXEC|EXECUTE|CALL|CREATE|GRANT|REVOKE)\b",
        re.IGNORECASE
    )
    _SELECT_RX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
    _NAMED_PARAM_RX = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")

    def __init__(self) -> None:
        pass

    def validate(
        self,
        *,
        plan: SqlPlan,
        catalog: SchemaCatalog,
        table: TableProfile,
        columns: ColumnSelectionResult
    ) -> ValidationResult:
        vr = ValidationResult(ok=False, issues=[])

        # 1) Seguridad básica: sólo SELECT/WITH
        if self._DANGEROUS_RX.search(plan.sql):
            vr.add_error("dangerous_sql", "Se detectaron palabras clave potencialmente peligrosas en el SQL.")
        if not self._SELECT_RX.search(plan.sql):
            vr.add_error("not_select", "El plan SQL no parece ser una consulta SELECT/WITH.")

        # 2) Tabla existente
        if not catalog.has_table(table.full_name):
            vr.add_error("table_not_found", "La tabla no existe en el catálogo.", table=table.full_name)
        else:
            vr.add_info("table_ok", "Tabla encontrada en el catálogo.", table=table.full_name)

        # 3) Columnas por rol (date/status/id)
        self._validate_role_column(vr, catalog, table, columns, ColumnRole.DATE, "date_column")
        self._validate_role_column(vr, catalog, table, columns, ColumnRole.STATUS, "status_column")
        self._validate_role_column(vr, catalog, table, columns, ColumnRole.ID, "id_column")

        # 4) Parámetros consistentes con SQL
        self._validate_params(vr, plan)

        # 5) Warnings de utilidad
        self._warn_useful(vr, plan, columns)

        return vr.finalize()

    # ------------------------
    #    Helpers de validación
    # ------------------------

    def _validate_role_column(
        self,
        vr: ValidationResult,
        catalog: SchemaCatalog,
        table: TableProfile,
        columns: ColumnSelectionResult,
        role: ColumnRole,
        meta_key: str
    ) -> None:
        choice = columns.choices.get(role)
        col = choice.column if choice else None
        if col is None:
            vr.add_warning(f"{role.name.lower()}_missing", f"No se eligió columna para el rol {role.name}.", table=table.full_name)
            return
        if not catalog.has_column(table.full_name, col.name):
            vr.add_error(
                f"{role.name.lower()}_invalid",
                f"La columna '{col.name}' para el rol {role.name} no existe en la tabla.",
                table=table.full_name, column=col.name
            )
        else:
            # Info útil: tipo/PK
            info = catalog.get_column(table.full_name, col.name)
            details = {"table": table.full_name, "column": col.name}
            if info and info.type:
                details["type"] = str(info.type)
            if info and info.pk:
                details["pk"] = "true"
            vr.add_info(f"{role.name.lower()}_ok", f"Columna para {role.name} verificada.", **details)

    def _validate_params(self, vr: ValidationResult, plan: SqlPlan) -> None:
        sql = plan.sql

        # SQL Server (params posicionales "?")
        if plan.dialect == "sqlserver":
            qmarks = sql.count("?")
            if qmarks != len(plan.params_seq):
                vr.add_error(
                    "params_mismatch",
                    "Cantidad de marcadores '?' no coincide con los parámetros provistos.",
                    placeholders=str(qmarks),
                    params=str(len(plan.params_seq))
                )
            else:
                vr.add_info("params_ok", "Parámetros posicionales consistentes.", placeholders=str(qmarks))

        else:
            # Named params estilo ":name"
            used_names = set(self._NAMED_PARAM_RX.findall(sql))
            provided_names = set(plan.params_named.keys()) if plan.params_named else set()

            missing = used_names - provided_names
            extra = provided_names - used_names

            if missing:
                vr.add_error("params_missing", "Faltan valores para parámetros nombrados.", missing=",".join(sorted(missing)))
            if extra:
                vr.add_warning("params_extra", "Se proporcionaron parámetros no utilizados en el SQL.", extra=",".join(sorted(extra)))
            if not missing:
                vr.add_info("params_ok", "Parámetros nombrados consistentes.", count=str(len(used_names)))

    def _warn_useful(self, vr: ValidationResult, plan: SqlPlan, columns: ColumnSelectionResult) -> None:
        # Si el plan es COUNT y no hay filtros, sugerir que quizá falte un rango temporal
        if " COUNT(" in plan.sql.upper() and not plan.params_named and not plan.params_seq:
            vr.add_warning("no_filters", "Consulta COUNT sin filtros; verifique si se requiere un rango de fechas o estado.")

        # Si faltó columna de fecha pero había rangos en entidades, SqlPlanner ya añadió warning;
        # aquí solo propagamos si vino en meta/warnings.
        for w in plan.warnings:
            vr.add_warning("planner_warn", w)


# =========================
#     Utilidades varias
# =========================

def make_table_profile_from_catalog(cat: SchemaCatalog, full_name: str) -> Optional[TableProfile]:
    """
    Convierte un TableInfo en TableProfile (para reutilizar adaptadores ya existentes).
    """
    ti = cat.get_table(full_name)
    if not ti:
        return None

    from core.selection.column_selector import ColumnSnapshot, TableProfile as _TP

    cols = [
        ColumnSnapshot(
            name=c.name,
            type=c.type,
            is_pk=c.pk,
            is_fk=False,
            nullable=c.nullable,
            description=c.description
        )
        for c in ti.columns.values()
    ]
    return _TP(full_name=ti.full_name, name=ti.name, schema=ti.schema, columns=cols)
