# tests/test_exports_unit.py
import json
from fastapi.testclient import TestClient

from src.app import app

client = TestClient(app)


def test_create_export_job_csv():
    payload = {
        "format": "csv",
        "columns": [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
            {"source": "value", "target": "value"},
            {"source": "metadata", "target": "metadata"},
        ],
    }

    resp = client.post("/exports", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert "export_id" in data
    assert data["format"] == "csv"
    assert data["status"] == "pending"


def test_create_export_job_invalid_format():
    payload = {
        "format": "invalid",
        "columns": [
            {"source": "id", "target": "id"},
        ],
    }

    resp = client.post("/exports", json=payload)
    assert resp.status_code == 422  # pydantic validation error


def test_download_not_found():
    resp = client.get("/exports/00000000-0000-0000-0000-000000000000/download")
    assert resp.status_code == 404
