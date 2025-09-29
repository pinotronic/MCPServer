from pydantic import BaseModel
from datetime import datetime
from typing import Any, Optional

class StandardResponse(BaseModel):
    status: str
    message: str
    timestamp: datetime
    data: Optional[Any] = None
