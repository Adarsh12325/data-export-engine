import orjson
from decimal import Decimal
from datetime import datetime, date

class JSONExporter:
    def __init__(self, cursor, columns):
        self.cursor = cursor        # RealDictCursor (rows are dicts)
        self.columns = columns      # list[ColumnMapping]
        self.content_type = "application/json"

    def _normalize(self, v):
        # Make sure everything is JSON-serializable
        if isinstance(v, Decimal):
            return float(v)  # or str(v) if you prefer exact string
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    def stream_data(self):
        first = True
        yield b"["
        for row in self.cursor:
            obj = {}
            for col in self.columns:
                obj[col.target] = self._normalize(row.get(col.source))
            data = orjson.dumps(obj)
            if first:
                first = False
                yield data
            else:
                yield b"," + data
        yield b"]"
