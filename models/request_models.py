from pydantic import BaseModel, Field
from typing import Optional

class QueryRequest(BaseModel):
    question: str = Field(min_length=5, max_length=2000)
    llm_provider: str = Field(default="deepseek")

class IterativeAnalysisRequest(BaseModel):
    question: str = Field(min_length=5, max_length=2000)
    llm_provider: str = Field(default="deepseek")
    max_iterations: int = Field(default=5, ge=1, le=10)

class DirectSQLRequest(BaseModel):
    sql: str = Field(min_length=6, max_length=10000)

class HealthCheckRequest(BaseModel):
    pass

class SchemaInfoRequest(BaseModel):
    pass
