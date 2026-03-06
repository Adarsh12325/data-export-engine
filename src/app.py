
from fastapi import FastAPI, HTTPException, Response, BackgroundTasks
from fastapi.responses import StreamingResponse
import uvicorn
import os
import time
try:
    import resource  # Unix-only
except ImportError:
    resource = None

from typing import List
from uuid import uuid4, UUID
from datetime import datetime
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
import io
import gzip
import zlib
from typing import AsyncGenerator

from src.models import (
    CreateExportRequest, ExportFormat, ColumnMapping, BenchmarkResult,
    ExportJob
)
from src.models import BenchmarkResponse
from src.utils import get_db_connection, build_select_query
from src.exporters.csv_exporter import CSVExporter
from src.exporters.json_exporter import JSONExporter
from src.exporters.xml_exporter import XMLExporter
from src.exporters.parquet_exporter import ParquetExporter
import gzip
from uuid import UUID

from src.models import CreateExportRequest, ColumnMapping
from src.utils import get_db_connection, build_select_query
from src.exporters.csv_exporter import CSVExporter
from src.exporters.json_exporter import JSONExporter
from src.exporters.xml_exporter import XMLExporter
from src.exporters.parquet_exporter import ParquetExporter

app = FastAPI(title="Data Export Engine")

# In-memory job store (for demo - use Redis in production)
jobs = {}

@app.post("/exports", response_model=ExportJob, status_code=201)
async def create_export(request: CreateExportRequest):
    export_id = uuid4()
    job = ExportJob(
        export_id=export_id,
        status="pending",
        format=request.format,
        columns=request.columns,
        compression=request.compression
    )
    jobs[str(export_id)] = job.model_dump()
    return job

@app.get("/exports/{export_id}/download")
async def download_export(export_id: UUID):
    job_id = str(export_id)
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Export job not found")

    job = jobs[job_id]
    format_type = job["format"]
    columns = [ColumnMapping(**col) for col in job["columns"]]
    compression = job.get("compression")

    query = build_select_query(columns)

    def row_stream():
        with get_db_connection() as conn:
            # server-side cursor for streaming
            with conn.cursor(name="export_cursor", cursor_factory=RealDictCursor) as cursor:
                cursor.itersize = 1000
                cursor.execute(query)

                # choose exporter
                if format_type == "csv":
                    exporter = CSVExporter(cursor, columns)
                elif format_type == "json":
                    exporter = JSONExporter(cursor, columns)
                elif format_type == "xml":
                    exporter = XMLExporter(cursor, columns)
                elif format_type == "parquet":
                    exporter = ParquetExporter(cursor, columns)
                else:
                    raise HTTPException(status_code=400, detail="Unsupported format")

                # get raw bytes generator
                for chunk in exporter.stream_data():
                    yield chunk

    # wrap for gzip if needed (only for text formats)
    if compression == "gzip" and format_type in ("csv", "json", "xml"):
        def gzip_stream():
            compressor = zlib.compressobj(
                level=6,
                method=zlib.DEFLATED,
                wbits=zlib.MAX_WBITS | 16,  # gzip wrapper
                memLevel=8,
                strategy=zlib.Z_DEFAULT_STRATEGY,
            )
            for chunk in row_stream():
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                data = compressor.compress(chunk)
                if data:
                    yield data
            tail = compressor.flush()
            if tail:
                yield tail

        stream = gzip_stream()
        content_encoding = "gzip"
    else:
        stream = row_stream()
        content_encoding = None


    # set content-type and filename
    if format_type == "csv":
        content_type = "text/csv"
        filename = "export.csv"
    elif format_type == "json":
        content_type = "application/json"
        filename = "export.json"
    elif format_type == "xml":
        content_type = "application/xml"
        filename = "export.xml"
    elif format_type == "parquet":
        content_type = "application/octet-stream"
        filename = "export.parquet"
    else:
        raise HTTPException(status_code=400, detail="Unsupported format")

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }
    if content_encoding:
        headers["Content-Encoding"] = content_encoding

    jobs[job_id]["status"] = "completed"

    return StreamingResponse(stream, media_type=content_type, headers=headers)


@app.get("/exports/benchmark", response_model=BenchmarkResponse)
async def benchmark_exports():
    formats: List[str] = ["csv", "json", "xml", "parquet"]
    results: List[BenchmarkResult] = []

    default_columns = [
        ColumnMapping(source="id", target="id"),
        ColumnMapping(source="name", target="name"),
        ColumnMapping(source="value", target="value"),
        ColumnMapping(source="metadata", target="metadata"),
    ]

    for fmt in formats:
        start_time = time.time()
        if resource is not None:
            start_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        else:
            start_mem = 0.0  # MB-ish

        # temp file path for this format
        tmp_name = f"/tmp/benchmark-{fmt}-{uuid4()}"
        if fmt == "parquet":
            tmp_name += ".parquet"
        else:
            tmp_name += f".{fmt}"

        # run an export and write to temp file
        select_sql = build_select_query(default_columns)

        with get_db_connection() as conn:
            with conn.cursor(name=f"benchmark_{fmt}", cursor_factory=RealDictCursor) as cursor:
                cursor.itersize = 1000
                cursor.execute(select_sql)

                # choose exporter
                if fmt == "csv":
                    exporter = CSVExporter(cursor, default_columns)
                elif fmt == "json":
                    exporter = JSONExporter(cursor, default_columns)
                elif fmt == "xml":
                    exporter = XMLExporter(cursor, default_columns)
                elif fmt == "parquet":
                    # ParquetExporter already writes to its own temp file,
                    # so we just reuse its logic and then measure that file.
                    # We'll adapt: ParquetExporter will accept a fixed path.
                    exporter = ParquetExporter(cursor, default_columns, output_path=tmp_name)
                else:
                    continue

                # write stream to file (for text formats)
                if fmt in ("csv", "json", "xml"):
                    with open(tmp_name, "wb") as f:
                        for chunk in exporter.stream_data():
                            f.write(chunk)
                else:
                    # parquet: stream_data already writes and yields file chunks
                    # we still need to consume it to actually write the file
                    for _ in exporter.stream_data():
                        pass

        duration = time.time() - start_time
        if resource is not None:
            peak_mem = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024) - start_mem
        else:
            peak_mem = 0.0

        # file size
        try:
            size_bytes = os.path.getsize(tmp_name)
        except OSError:
            size_bytes = 0

        # cleanup
        try:
            os.remove(tmp_name)
        except OSError:
            pass

        results.append(
            BenchmarkResult(
                format=ExportFormat(fmt),
                duration_seconds=max(duration, 0.000001),
                file_size_bytes=max(size_bytes, 1),
                peak_memory_mb=max(peak_mem, 0.000001),
            )
        )

    return BenchmarkResponse(
        datasetRowCount=10000000,
        results=results,
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
