from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class DatabaseService(ABC):
    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def disconnect(self) -> None: ...
    @abstractmethod
    async def fetch_all(self, sql: str, params: Optional[Union[List[Any], Dict[str, Any]]] = None) -> List[Dict[str, Any]]: ...
    @abstractmethod
    async def get_schema_overview(self) -> Dict[str, Any]: ...
