from xml.sax.saxutils import escape
from datetime import datetime, date
from decimal import Decimal

class XMLExporter:
    def __init__(self, cursor, columns):
        self.cursor = cursor        # RealDictCursor (rows are dicts)
        self.columns = columns      # list[ColumnMapping]
        self.content_type = "application/xml"

    def _to_text(self, v):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, (dict, list)):
            # nested JSON -> XML: we’ll just stringify; later you can expand if needed
            return str(v)
        return "" if v is None else str(v)

    def stream_data(self):
        # XML declaration + root open
        yield b'<?xml version="1.0" encoding="UTF-8"?>\n<records>\n'
        for row in self.cursor:
            parts = ["  <record>\n"]
            for col in self.columns:
                raw = row.get(col.source)
                text = escape(self._to_text(raw))
                parts.append(f"    <{col.target}>{text}</{col.target}>\n")
            parts.append("  </record>\n")
            chunk = "".join(parts).encode("utf-8")
            yield chunk
        # close root
        yield b"</records>\n"
