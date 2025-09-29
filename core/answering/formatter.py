from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.intent.detector import Intent
from core.execution.db_executor import QueryResult
from core.planning.sql_planner import SqlPlan
from core.planning.sql_validator import ValidationResult


@dataclass
class AnswerFormatterConfig:
    include_trace: bool = True
    max_preview_rows: int = 100
    show_columns: bool = True
    round_ms: int = 2


@dataclass
class AnswerPayload:
    status: str
    message: str
    data: Dict[str, Any] = field(default_factory=dict)
    trace: Optional[Dict[str, Any]] = None
    warnings: List[str] = field(default_factory=list)


class AnswerFormatter:
    def __init__(self, config: Optional[AnswerFormatterConfig] = None) -> None:
        self._cfg = config or AnswerFormatterConfig()

    def format(
        self,
        *,
        intent: Intent,
        plan: SqlPlan,
        result: QueryResult,
        validation: Optional[ValidationResult] = None
    ) -> AnswerPayload:
        status = "success"
        warnings: List[str] = []
        if result.warnings:
            warnings.extend(result.warnings)
        if plan.warnings:
            warnings.extend(plan.warnings)

        if validation:
            for issue in validation.issues:
                if issue.level == "error":
                    status = "error"
                    warnings.append(f"{issue.code}:{issue.message}")
                elif issue.level == "warning":
                    warnings.append(f"{issue.code}:{issue.message}")

        if intent == Intent.COUNT:
            payload = self._format_count(plan, result)
            message = "Conteo obtenido"
        elif intent == Intent.DESCRIBE:
            payload = self._format_describe(plan, result)
            message = "Estructura obtenida"
        else:
            payload = self._format_list(plan, result)
            message = "Datos obtenidos"

        trace = self._build_trace(plan, result, validation) if self._cfg.include_trace else None

        return AnswerPayload(
            status=status,
            message=message,
            data=payload,
            trace=trace,
            warnings=warnings
        )

    # -----------------------
    # Formateadores por tipo
    # -----------------------

    def _format_count(self, plan: SqlPlan, result: QueryResult) -> Dict[str, Any]:
        total = None
        if result.rows:
            row0 = result.rows[0]
            # soporta "total" o primera columna si el alias no llegó
            if "total" in row0:
                total = row0["total"]
            elif result.columns:
                total = row0.get(result.columns[0])
        if total is None:
            total = result.rowcount

        return {
            "total": total,
            "table": plan.meta.get("table"),
            "period": self._human_period(plan),
            "elapsed_ms": round(result.elapsed_ms, self._cfg.round_ms),
        }

    def _format_list(self, plan: SqlPlan, result: QueryResult) -> Dict[str, Any]:
        limit = plan.meta.get("limit")
        preview = result.rows[: self._cfg.max_preview_rows]
        truncated = len(result.rows) > self._cfg.max_preview_rows

        out: Dict[str, Any] = {
            "table": plan.meta.get("table"),
            "rows": preview,
            "rowcount": result.rowcount,
            "elapsed_ms": round(result.elapsed_ms, self._cfg.round_ms),
            "truncated": truncated,
        }
        if self._cfg.show_columns:
            out["columns"] = result.columns
        if limit is not None:
            out["limit"] = limit
        if plan.meta.get("order_by"):
            out["order"] = {
                "by": plan.meta.get("order_by"),
                "dir": plan.meta.get("order_dir", "DESC"),
            }
        return out

    def _format_describe(self, plan: SqlPlan, result: QueryResult) -> Dict[str, Any]:
        return {
            "table": plan.meta.get("table"),
            "columns_preview": result.rows[: self._cfg.max_preview_rows],
            "elapsed_ms": round(result.elapsed_ms, self._cfg.round_ms),
        }

    # -----------------------
    # Traza / debug
    # -----------------------

    def _build_trace(
        self,
        plan: SqlPlan,
        result: QueryResult,
        validation: Optional[ValidationResult]
    ) -> Dict[str, Any]:
        trace: Dict[str, Any] = {
            "dialect": plan.dialect,
            "table": plan.meta.get("table"),
            "intent": plan.meta.get("intent"),
            "sql": plan.sql,
            "params_named": plan.params_named,
            "params_seq": plan.params_seq,
            "filters": plan.meta.get("filters", []),
            "elapsed_ms": round(result.elapsed_ms, self._cfg.round_ms),
        }
        if validation:
            trace["validation"] = [
                {"level": i.level, "code": i.code, "message": i.message, "details": i.details}
                for i in validation.issues
            ]
        return trace

    # -----------------------
    # Utilidades
    # -----------------------

    @staticmethod
    def _human_period(plan: SqlPlan) -> Optional[str]:
        try:
            # Si el planner puso etiqueta de periodo en meta, úsala
            # si no, devuélvelo vacío; la UI puede mostrar filtros del trace
            return plan.meta.get("period")
        except Exception:
            return None
