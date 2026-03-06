import os
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal
from datetime import datetime, date

class ParquetExporter:
    def __init__(self, cursor, columns, output_path=None):
        self.cursor = cursor
        self.columns = columns
        self.content_type = "application/octet-stream"
        self.output_path = output_path  # may be None

    def _normalize(self, v):
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    def stream_data(self):
        # use provided path (benchmark) or generate one (download)
        tmp_name = self.output_path or f"/tmp/export-{uuid.uuid4()}.parquet"

        fields = [pa.field(col.target, pa.string()) for col in self.columns]
        schema = pa.schema(fields)

        writer = None
        batch_size = 10_000
        batch = {col.target: [] for col in self.columns}

        for row in self.cursor:
            for col in self.columns:
                val = self._normalize(row.get(col.source))
                batch[col.target].append(str(val) if val is not None else None)

            if len(batch[self.columns[0].target]) >= batch_size:
                table = pa.Table.from_pydict(batch, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(tmp_name, schema)
                writer.write_table(table)
                batch = {col.target: [] for col in self.columns}

        if batch[self.columns[0].target]:
            table = pa.Table.from_pydict(batch, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(tmp_name, schema)
            writer.write_table(table)

        if writer is not None:
            writer.close()

        with open(tmp_name, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk

        try:
            # only remove if exporter created it, but safe to try always
            os.remove(tmp_name)
        except OSError:
            pass
