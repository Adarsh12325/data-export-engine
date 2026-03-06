from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID
import enum

class ExportFormat(str, enum.Enum):
    CSV = "csv"
    JSON = "json"
    XML = "xml"
    PARQUET = "parquet"

class ColumnMapping(BaseModel):
    source: str
    target: str

class CreateExportRequest(BaseModel):
    format: ExportFormat
    columns: List[ColumnMapping]
    compression: Optional[str] = None

class ExportJob(BaseModel):
    export_id: UUID
    status: str
    format: ExportFormat
    columns: List[ColumnMapping]
    compression: Optional[str]

class BenchmarkResult(BaseModel):
    format: ExportFormat
    duration_seconds: float
    file_size_bytes: int
    peak_memory_mb: float

class BenchmarkResponse(BaseModel):
    datasetRowCount: int
    results: List[BenchmarkResult]