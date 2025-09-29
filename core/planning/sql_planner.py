from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from core.intent.detector import Intent
from core.selection.column_selector import ColumnSelectionResult, ColumnRole, TableProfile
from core.extraction.entities import ExtractedEntities, DateRange


# ---------------------------
# Tipos de salida del planner
# ---------------------------

@dataclass(frozen=True)
class SqlPlan:
    dialect: str
    sql: str
    params_named: Dict[str, object]
    params_seq: List[object]
    meta: Dict[str, object] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------
# Configuración del planner
# ---------------------------

@dataclass
class SqlPlannerConfig:
    default_list_limit: int = 100
    max_select_columns: int = 12
    prefer_order_by_date: bool = True
    preferred_date_names: Tuple[str, ...] = (
        "fechacita", "fecha_cita", "fechaprogramada", "fecha", "created_at", "createdon", "createddate", "datetime", "timestamp"
    )
    id_fallback_names: Tuple[str, ...] = ("cita_id", "id_cita", "id", "citaid")


# ---------------------------
# Utilidades de quoting/params
# ---------------------------

def _quote_ident(dialect: str, ident: str) -> str:
    if dialect == "sqlserver":
        return "[" + ident.replace("]", "]]") + "]"
    if dialect == "postgres":
        return '"' + ident.replace('"', '""') + '"'
    return '"' + ident.replace('"', '""') + '"'  # sqlite y por defecto


def _quote_table(dialect: str, schema: str, table: str) -> str:
    if schema:
        return f"{_quote_ident(dialect, schema)}.{_quote_ident(dialect, table)}"
    return _quote_ident(dialect, table)


_named_param_rx = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")


def _named_to_qmark(sql_named: str, params: Dict[str, object]) -> Tuple[str, List[object]]:
    params_seq: List[object] = []

    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in params:
            raise KeyError(f"Parámetro no provisto: {name}")
        params_seq.append(params[name])
        return "?"

    sql_q = _named_param_rx.sub(repl, sql_named)
    return sql_q, params_seq


# ---------------------------
# Planner principal
# ---------------------------

class SqlPlanner:
    def __init__(self, config: Optional[SqlPlannerConfig] = None) -> None:
        self._cfg = config or SqlPlannerConfig()

    def build(
        self,
        *,
        intent: Intent,
        dialect: str,
        table: TableProfile,
        columns: ColumnSelectionResult,
        entities: ExtractedEntities,
        question: str = "",
        select_columns: Optional[List[str]] = None
    ) -> SqlPlan:
        if intent == Intent.COUNT:
            return self._build_count(dialect, table, columns, entities, question)
        if intent == Intent.LIST:
            return self._build_list(dialect, table, columns, entities, question, select_columns)
        if intent == Intent.AGGREGATE:
            raise ValueError("AGGREGATE aún no implementado en SqlPlanner")
        if intent == Intent.DESCRIBE:
            return self._build_describe(dialect, table)
        return self._build_list(dialect, table, columns, entities, question, select_columns)

    # ---------------------------
    # COUNT
    # ---------------------------

    def _build_count(
        self,
        dialect: str,
        table: TableProfile,
        columns: ColumnSelectionResult,
        entities: ExtractedEntities,
        question: str
    ) -> SqlPlan:
        date_col = self._get_role(columns, ColumnRole.DATE)
        status_col = self._get_role(columns, ColumnRole.STATUS)

        tbl_sql = _quote_table(dialect, table.schema, table.name)

        where_sql, params, warns = self._compose_where(dialect, date_col, status_col, entities)

        sql_named = f"SELECT COUNT(*) AS total FROM {tbl_sql}"
        if where_sql:
            sql_named += f" WHERE {where_sql}"

        final_sql, params_seq = self._finalize_sql(dialect, sql_named, params)

        meta = {
            "table": table.full_name,
            "intent": "COUNT",
            "date_column": date_col.name if date_col else None,
            "status_column": status_col.name if status_col else None,
            "question": question,
            "filters": list(params.keys())
        }
        return SqlPlan(dialect=dialect, sql=final_sql, params_named=params, params_seq=params_seq, meta=meta, warnings=warns)

    # ---------------------------
    # LIST
    # ---------------------------

    def _build_list(
        self,
        dialect: str,
        table: TableProfile,
        columns: ColumnSelectionResult,
        entities: ExtractedEntities,
        question: str,
        select_columns: Optional[List[str]]
    ) -> SqlPlan:
        date_col = self._get_role(columns, ColumnRole.DATE)
        status_col = self._get_role(columns, ColumnRole.STATUS)
        id_col = self._get_role(columns, ColumnRole.ID)

        tbl_sql = _quote_table(dialect, table.schema, table.name)

        if select_columns and len(select_columns) > 0:
            select_list = ", ".join(_quote_ident(dialect, c) for c in select_columns)
        else:
            names = [c.name for c in table.columns][: self._cfg.max_select_columns]
            select_list = ", ".join(_quote_ident(dialect, n) for n in names)

        where_sql, params, warns = self._compose_where(dialect, date_col, status_col, entities)

        order_col = None
        order_dir = "ASC"
        if date_col and self._cfg.prefer_order_by_date:
            order_col = date_col.name
            order_dir = "DESC"
        elif id_col:
            order_col = id_col.name
            order_dir = "DESC"

        limit = entities.limit or self._cfg.default_list_limit

        if dialect == "sqlserver":
            sql_named = f"SELECT TOP {int(limit)} {select_list} FROM {tbl_sql}"
            if where_sql:
                sql_named += f" WHERE {where_sql}"
            if order_col:
                sql_named += f" ORDER BY {_quote_ident(dialect, order_col)} {order_dir}"
        else:
            sql_named = f"SELECT {select_list} FROM {tbl_sql}"
            if where_sql:
                sql_named += f" WHERE {where_sql}"
            if order_col:
                sql_named += f" ORDER BY {_quote_ident(dialect, order_col)} {order_dir}"
            sql_named += " LIMIT :limit_rows"
            params["limit_rows"] = int(limit)

        final_sql, params_seq = self._finalize_sql(dialect, sql_named, params)

        meta = {
            "table": table.full_name,
            "intent": "LIST",
            "date_column": date_col.name if date_col else None,
            "status_column": status_col.name if status_col else None,
            "id_column": id_col.name if id_col else None,
            "question": question,
            "limit": limit,
            "order_by": order_col,
            "order_dir": order_dir,
            "filters": list(params.keys())
        }
        return SqlPlan(dialect=dialect, sql=final_sql, params_named=params, params_seq=params_seq, meta=meta, warnings=warns)

    # ---------------------------
    # DESCRIBE (estructura simple)
    # ---------------------------

    def _build_describe(self, dialect: str, table: TableProfile) -> SqlPlan:
        tbl_sql = _quote_table(dialect, table.schema, table.name)
        if dialect == "sqlite":
            sql_named = f"PRAGMA table_info({_quote_ident(dialect, table.name)})"
            params: Dict[str, object] = {}
        elif dialect == "sqlserver":
            sql_named = (
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table "
                "ORDER BY ORDINAL_POSITION"
            )
            params = {"schema": table.schema, "table": table.name}
        else:
            sql_named = f"SELECT * FROM {tbl_sql} LIMIT 0"
            params = {}
        final_sql, params_seq = self._finalize_sql(dialect, sql_named, params)
        meta = {"table": table.full_name, "intent": "DESCRIBE"}
        return SqlPlan(dialect=dialect, sql=final_sql, params_named=params, params_seq=params_seq, meta=meta, warnings=[])

    # ---------------------------
    # WHERE builder
    # ---------------------------

    def _compose_where(
        self,
        dialect: str,
        date_col: Optional[object],
        status_col: Optional[object],
        entities: ExtractedEntities
    ) -> Tuple[str, Dict[str, object], List[str]]:
        parts: List[str] = []
        params: Dict[str, object] = {}
        warns: List[str] = []

        # Fechas
        if entities.date_ranges:
            if date_col is None:
                warns.append("rango_fecha_solicitado_sin_columna_fecha")
            else:
                date_exprs: List[str] = []
                for i, r in enumerate(entities.date_ranges):
                    start_key = f"start_{i}"
                    end_key = f"end_{i}"
                    date_exprs.append(
                        f"{_quote_ident(dialect, date_col.name)} >= :{start_key} AND {_quote_ident(dialect, date_col.name)} < :{end_key}"
                    )
                    params[start_key] = r.start.isoformat()
                    params[end_key] = r.end.isoformat()
                parts.append("(" + " OR ".join(date_exprs) + ")")

        # Estados
        if entities.statuses and status_col is not None:
            if len(entities.statuses) == 1:
                key = "status_0"
                parts.append(f"{_quote_ident(dialect, status_col.name)} = :{key}")
                params[key] = entities.statuses[0]
            else:
                keys: List[str] = []
                for i, st in enumerate(entities.statuses):
                    k = f"status_{i}"
                    params[k] = st
                    keys.append(f":{k}")
                parts.append(f"{_quote_ident(dialect, status_col.name)} IN ({', '.join(keys)})")

        where_sql = " AND ".join(parts)
        return where_sql, params, warns

    # ---------------------------
    # Helpers
    # ---------------------------

    @staticmethod
    def _get_role(columns: ColumnSelectionResult, role: ColumnRole) -> Optional[object]:
        choice = columns.choices.get(role)
        return choice.column if choice else None

    def _finalize_sql(self, dialect: str, sql_named: str, params: Dict[str, object]) -> Tuple[str, List[object]]:
        if dialect == "sqlserver":
            sql_q, seq = _named_to_qmark(sql_named, params)
            return sql_q, seq
        return sql_named, []


# ---------------------------
# Utilidad para construir TableProfile básico desde snapshot simple
# ---------------------------

def profile_from_simple(
    full_name: str,
    schema: str,
    name: str,
    columns: List[str]
) -> TableProfile:
    from core.selection.column_selector import ColumnSnapshot
    col_objs = [ColumnSnapshot(c) for c in columns]
    return TableProfile(full_name=full_name, name=name, schema=schema, columns=col_objs)
