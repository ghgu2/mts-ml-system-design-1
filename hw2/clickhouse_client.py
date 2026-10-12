"""Small HTTP client; no host-side Python dependencies are required."""

import base64
import json
import os
from decimal import Decimal
import urllib.error
import urllib.parse
import urllib.request


class ClickHouse:
    def __init__(self):
        self.url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
        credentials = (os.getenv("CLICKHOUSE_USER", "hw2") + ":" +
                       os.getenv("CLICKHOUSE_PASSWORD", "hw2-local")).encode()
        self.authorization = "Basic " + base64.b64encode(credentials).decode("ascii")

    def query(self, sql: str, *, params: dict | None = None,
              query_id: str | None = None, output_format: str | None = None):
        sql = sql.strip().rstrip(";")
        if output_format:
            sql += "\nFORMAT " + output_format
        options = {"wait_end_of_query": "1"}
        if query_id:
            options["query_id"] = query_id
        for key, value in (params or {}).items():
            options["param_" + key] = str(value)
        request = urllib.request.Request(
            self.url + "/?" + urllib.parse.urlencode(options),
            data=sql.encode("utf-8"),
            headers={"Authorization": self.authorization},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = response.read().decode("utf-8")
                summary = json.loads(response.headers.get("X-ClickHouse-Summary", "{}"))
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP {error.code}: {detail}") from error
        return body, summary

    def rows(self, sql: str, **kwargs) -> list[dict]:
        body, _ = self.query(sql, output_format="JSONEachRow", **kwargs)
        return [json.loads(line, parse_float=Decimal) for line in body.splitlines() if line.strip()]
