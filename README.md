# Polyglot Data Export Engine: Streaming 10M Rows to CSV, JSON, XML, and Parquet Formats

A FastAPI-based **data export service** that streams a large PostgreSQL dataset (10M rows) into multiple formats — **CSV, JSON, XML, Parquet** — with optional **gzip compression** and a **benchmark** endpoint to compare performance.

---

## 1. Features

- Export data from PostgreSQL in:
  - **CSV** (streamed, header row, proper quoting)
  - **JSON** (single large JSON array, streamed)
  - **XML** (`<records><record>...</record></records>`, streamed)
  - **Parquet** (via `pyarrow`, written in batches)
- Optional **gzip compression** for text formats (CSV, JSON, XML)
- Handles **large datasets** (10,000,000 rows) efficiently with:
  - Server-side cursors
  - Chunked streaming
  - Minimal memory usage
- `/exports/benchmark` endpoint:
  - Runs all 4 formats
  - Measures duration, file size, and peak memory
- Tests:
  - **Unit tests** for job creation and error handling
  - **Integration tests** for all formats (CSV, JSON, XML, Parquet)

---

## 2. Project Structure

```text
data-export-engine/
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
├─ README.md
├─ .env
├─ src/
│  ├─ __init__.py
│  ├─ app.py                # FastAPI app and routes
│  ├─ models.py             # Pydantic models (CreateExportRequest, ExportJob, BenchmarkResult, BenchmarkResponse, etc.)
│  ├─ utils.py              # DB connection helper, SQL builder
│  ├─ exporters/
│  │  ├─ __init__.py
│  │  ├─ csv_exporter.py
│  │  ├─ json_exporter.py
│  │  ├─ xml_exporter.py
│  │  ├─ parquet_exporter.py
├─ tests/
│  ├─ __init__.py
│  ├─ conftest.py           # Adds project root to sys.path
│  ├─ test_exports_unit.py
│  ├─ test_exports_integration.py
```

## 3. Architecture
#### 3.1 High-level flow
* A client creates an export job via POST /exports.

* The API stores a lightweight job description in an in-memory jobs dict.

* The client immediately calls GET /exports/{export_id}/download.

***The API:***

* Builds a SELECT query based on requested columns.

* Uses get_db_connection() to connect to PostgreSQL.

* Uses a server-side cursor (cursor(name=..., cursor_factory=RealDictCursor)) to stream rows.

* Wraps the cursor with a format-specific exporter (CSV/JSON/XML/Parquet).

* Optionally wraps the stream with a gzip compressor for text formats.

* Returns a StreamingResponse to the client.

#### 3.2 Database
PostgreSQL container seeded with 10M rows into a records table, with columns:

* id (int)

* name (text)

* value (numeric / decimal)

* metadata (JSONB with nested structure)

Connection string is read from DATABASE_URL env var, e.g.:

```
DATABASE_URL=postgresql://user:password@db:5432/exports_db
```

#### 3.3 Exporters
Each exporter takes a DB cursor (yielding dict rows) and a list of ColumnMapping objects (source DB column, target output field):

***CSVExporter***

* Writes a header row (targets).

For each row:

* Reads row[col.source]

* Normalizes values (e.g. datetimes to ISO, others to string)

* Writes via csv.writer into a StringIO

* Yields encoded bytes chunk-by-chunk.

***JSONExporter***

* Streams a single JSON array:

* Yields initial b"["

For each row:

* Builds a dict {col.target: normalized_value}

* Uses orjson.dumps() to serialize.

* Prepends commas between items.

* Yields final b"]".

* Normalizes Decimal to float and datetimes to ISO strings.

***XMLExporter***

* Streams well-formed XML:

* Yields XML declaration and <records> root.

For each row:

* Writes <record> and child tags per column using col.target as tag name.

* Escapes text with xml.sax.saxutils.escape.

* Yields closing </records>.

***ParquetExporter***

* Batches rows into a Python dict ({column_name: [values...]}).

* Uses pyarrow.Table.from_pydict and pyarrow.parquet.ParquetWriter to write to a temporary file under /tmp.

* Streams that file in 1 MB chunks.

* Deletes the temp file after streaming.

* Accepts an optional output_path (used by the benchmark endpoint).

#### 3.4 Gzip compression
* For CSV/JSON/XML, if compression="gzip" is set in the export job:

* Wraps the raw stream with a gzip_stream() using zlib.compressobj configured for gzip.

* Yields compressed chunks and final flush.

* Adds Content-Encoding: gzip header.

* Parquet is not gzipped (file format already supports compression internally).

## 4. Setup and Running
#### 4.1 Requirements
* Docker + Docker Compose

* Python 3.11+ (only needed if you want to run tests on host)

#### 4.2 Environment
Create a .env file in project root (or use defaults from docker-compose.yml):

```
DATABASE_URL=postgresql://user:password@db:5432/exports_db
PORT=8080
````

#### 4.3 Run with Docker Compose
From project root:

```
docker-compose down
docker-compose up --build
```

Wait until:

* db container is up and seeded (10M rows).

* app container logs: Application startup complete and Uvicorn listening on 0.0.0.0:8080.

To verify DB row count:

```
docker-compose exec db psql -U user -d exports_db -c "SELECT COUNT(*) FROM records;"
```

## 5. API Documentation
FastAPI auto-docs are available at:

* Swagger UI: http://localhost:8080/docs

* OpenAPI JSON: http://localhost:8080/openapi.json

#### 5.1 Models (conceptual)
ExportFormat: enum of "csv", "json", "xml", "parquet".

ColumnMapping:

* source (str): DB column name.

* target (str): Output field/tag name.

CreateExportRequest:

* format: ExportFormat

* columns: list[ColumnMapping]

* compression: optional "gzip" or None

ExportJob:

* export_id: UUID

* status: "pending" or "completed"

* format, columns, compression as above.

BenchmarkResult:

* format: ExportFormat

* duration_seconds: float

* file_size_bytes: int

* peak_memory_mb: float

BenchmarkResponse:

* datasetRowCount: int

* results: list[BenchmarkResult]

## 5.2 Endpoints
#### 5.2.1 Create Export Job
POST /exports

Request body (application/json):

```
{
  "format": "csv",
  "columns": [
    { "source": "id", "target": "id" },
    { "source": "name", "target": "name" },
    { "source": "value", "target": "value" },
    { "source": "metadata", "target": "metadata" }
  ],
  "compression": "gzip"
}
```
* format: "csv" | "json" | "xml" | "parquet"

* compression (optional): "gzip" for CSV/JSON/XML; ignored for Parquet.

Response 201 Created:

```
{
  "export_id": "d203c98a-b243-491b-ab30-3bf24483ab85",
  "status": "pending",
  "format": "csv",
  "columns": [
    { "source": "id", "target": "id" },
    { "source": "name", "target": "name" },
    { "source": "value", "target": "value" },
    { "source": "metadata", "target": "metadata" }
  ],
  "compression": "gzip"
}
```
#### 5.2.2 Download Export
GET /exports/{export_id}/download

Path parameter:

* export_id: UUID returned by POST /exports.

Behavior:

* Streams the export in the requested format.

Headers:

* Content-Type:

** text/csv for CSV

** application/json for JSON

** application/xml for XML

** application/octet-stream for Parquet
 
* Content-Disposition: attachment; filename="export.<ext>"

* Content-Encoding: gzip if compression="gzip" for CSV/JSON/XML.

Examples (PowerShell):

Create job:

```
$body = @{
    format = "csv"
    columns = @(
        @{ source = "id"; target = "id" },
        @{ source = "name"; target = "name" },
        @{ source = "value"; target = "value" },
        @{ source = "metadata"; target = "metadata" }
    )
} | ConvertTo-Json -Depth 5

$job = Invoke-RestMethod -Uri "http://localhost:8080/exports" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body

$exportId = $job.export_id

curl "http://localhost:8080/exports/$exportId/download" -o export.csv
```

For JSON + gzip:

```
$body = @{
    format = "json"
    columns = @(
        @{ source = "id"; target = "id" },
        @{ source = "name"; target = "name" },
        @{ source = "value"; target = "value" },
        @{ source = "metadata"; target = "metadata" }
    )
    compression = "gzip"
} | ConvertTo-Json -Depth 5

$job = Invoke-RestMethod -Uri "http://localhost:8080/exports" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body

$exportId = $job.export_id

curl "http://localhost:8080/exports/$exportId/download" -o export.json.gz
```
#### 5.2.3 Benchmark Exports
GET /exports/benchmark

Response (application/json):

```
{
  "datasetRowCount": 10000000,
  "results": [
    {
      "format": "csv",
      "duration_seconds": 127.53,
      "file_size_bytes": 1470335307,
      "peak_memory_mb": 1.39
    },
    {
      "format": "json",
      "duration_seconds": 10.12,
      "file_size_bytes": 1200000000,
      "peak_memory_mb": 1.5
    },
    {
      "format": "xml",
      "duration_seconds": 200.34,
      "file_size_bytes": 2000000000,
      "peak_memory_mb": 2.0
    },
    {
      "format": "parquet",
      "duration_seconds": 30.78,
      "file_size_bytes": 300000000,
      "peak_memory_mb": 1.2
    }
  ]
}
```
Values above are illustrative; actual numbers are measured at runtime.

## 6. Testing
#### 6.1 Unit tests
Run on host:

```
pytest -q
```
tests/test_exports_unit.py:

* test_create_export_job_csv

* test_create_export_job_invalid_format

* test_download_not_found

#### 6.2 Integration tests
Integration tests in tests/test_exports_integration.py hit the real DB and streaming logic.

You can:

* Run them on host by setting DATABASE_URL to your Docker Postgres:

```
$env:DATABASE_URL = "postgresql://user:password@localhost:5432/exports_db"
pytest -q
```
Or mark/skip them depending on environment (e.g., via a pytest marker).

Tests cover:

* CSV header + rows content.

JSON gzip correctness and structure.

* XML structure.

Parquet returns binary data.

## 7. Notes and Limitations
* The job store is in-memory (a simple dict) for demo purposes. In production, use Redis or another persistent store.

* /exports/{export_id}/download assumes the job is executed on demand; there is no background queue.

* Parquet exporter uses all fields as strings for simplicity; schema can be refined to use typed columns (int, float, struct) if needed.

* Gzip compression is applied manually for streaming responses using zlib and the Content-Encoding: gzip header.

## 8. Future improvements
* Replace in-memory jobs store with Redis / database.

* Add authentication/authorization.

* Improve Parquet schema to preserve numeric and nested types.

* Add pagination/filters on the dataset.

* Add Prometheus metrics for per-format export performance.
