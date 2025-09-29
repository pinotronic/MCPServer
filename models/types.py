from __future__ import annotations
from enum import Enum


class Dialect(str, Enum):
    SQLSERVER = "sqlserver"
    SQLITE = "sqlite"
    POSTGRES = "postgres"
