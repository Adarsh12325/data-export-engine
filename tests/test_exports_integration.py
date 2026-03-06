# tests/test_exports_integration.py
import os
import pytest
from email.mime import text
import gzip
import json
from random import sample
from fastapi.testclient import TestClient

from src.app import app

client = TestClient(app)

pytestmark = pytest.mark.skipif(
    os.getenv("DATABASE_URL") is None or "db:5432" not in os.getenv("DATABASE_URL", ""),
    reason="Integration tests require DATABASE_URL pointing to Docker Postgres"
)


def _create_job(format_: str, compression: str | None = None):
    body = {
        "format": format_,
        "columns": [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
            {"source": "value", "target": "value"},
            {"source": "metadata", "target": "metadata"},
        ],
    }
    if compression:
        body["compression"] = compression

    resp = client.post("/exports", json=body)
    assert resp.status_code == 201
    data = resp.json()
    return data["export_id"]


def test_csv_download_streams_some_data():
    export_id = _create_job("csv")

    resp = client.get(f"/exports/{export_id}/download")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")

    content = resp.content[:4096]
    text = content.decode("utf-8", errors="ignore")
    lines = text.splitlines()
    assert len(lines) >= 2
    assert lines[0].startswith("id,name,value,metadata")



def test_json_download_gzip():
    export_id = _create_job("json", compression="gzip")

    resp = client.get(f"/exports/{export_id}/download")
    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "gzip"

    compressed = resp.content  # full body in tests
    decompressed = gzip.decompress(compressed)
    data = json.loads(decompressed.decode("utf-8"))
    assert isinstance(data, list)
    assert len(data) > 0
    first = data[0]
    assert {"id", "name", "value", "metadata"} <= set(first.keys())



def test_xml_download():
    export_id = _create_job("xml")

    resp = client.get(f"/exports/{export_id}/download")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/xml")

    sample = resp.content[:4096]
    text = sample.decode("utf-8", errors="ignore")
    assert "<records>" in text
    assert "<record>" in text
    assert "<id>" in text



def test_parquet_download():
    export_id = _create_job("parquet")

    resp = client.get(f"/exports/{export_id}/download")
    assert resp.status_code == 200
    assert "application/octet-stream" in resp.headers["content-type"]

    assert len(resp.content) > 0

