import csv
import io

class CSVExporter:
    def __init__(self, cursor, columns):
        self.cursor = cursor
        self.columns = columns
        self.content_type = "text/csv"

    def stream_data(self):
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

        # header
        headers = [col.target for col in self.columns]
        writer.writerow(headers)
        output.seek(0)
        yield output.getvalue().encode("utf-8")
        output.seek(0)
        output.truncate(0)

        # rows
        for row in self.cursor:
            row_data = []
            for col in self.columns:
                value = row[col.source]
                if hasattr(value, "isoformat"):
                    row_data.append(value.isoformat())
                else:
                    row_data.append(str(value))
            writer.writerow(row_data)
            output.seek(0)
            data = output.getvalue().encode("utf-8")
            if data:
                yield data
            output.seek(0)
            output.truncate(0)
