import json
import logging
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from google.cloud import bigquery

from main import run_sync


PORT = int(os.getenv("PORT", "8080"))


def _array_element_expr(field_name: str, field: bigquery.SchemaField) -> str:
    column_ref = f"`{field_name}`"
    if field.mode == "REPEATED":
        column_ref = f"{column_ref}[SAFE_OFFSET(0)]"
    return column_ref


def _cast_expr(field_name: str, target: bigquery.SchemaField, source: bigquery.SchemaField) -> str:
    element_expr = _array_element_expr(field_name, source)
    if source.field_type == "RECORD" and target.field_type == "STRING":
        cast_expr = f"TO_JSON_STRING({element_expr})"
    else:
        cast_expr = f"SAFE_CAST({element_expr} AS {target.field_type})"
    return f"{cast_expr} AS `{field_name}`"


class SyncRequestHandler(BaseHTTPRequestHandler):
    server_version = "RunnSyncHTTP/1.0"

    def log_message(self, format, *args):  # noqa: A003 - matches BaseHTTPRequestHandler signature
        logging.info("%s - - %s", self.address_string(), format % args)

    def _write_response(self, status, body=b"", content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - inherited naming
        parsed = urlparse(self.path)
        if parsed.path in {"/", ""}:
            self._write_response(
                HTTPStatus.OK,
                b"Runn to BigQuery sync service",
                content_type="text/plain; charset=utf-8",
            )
            return
        if parsed.path in {"/health", "/healthz"}:
            self._write_response(
                HTTPStatus.OK,
                b"ok",
                content_type="text/plain; charset=utf-8",
            )
            return

        self._write_response(
            HTTPStatus.NOT_FOUND,
            b"Not found",
            content_type="text/plain; charset=utf-8",
        )

    def do_POST(self):  # noqa: N802 - inherited naming
        parsed = urlparse(self.path)
        if parsed.path != "/sync":
            self._write_response(
                HTTPStatus.NOT_FOUND,
                b"Not found",
                content_type="text/plain; charset=utf-8",
            )
            return

        try:
            result = run_sync()
            body = json.dumps({"status": "ok", **result}).encode("utf-8")
            self._write_response(HTTPStatus.OK, body)
        except Exception as exc:  # pragma: no cover - defensive logging
            logging.exception("Error while executing sync")
            body = json.dumps(
                {"status": "error", "message": str(exc)}
            ).encode("utf-8")
            self._write_response(HTTPStatus.INTERNAL_SERVER_ERROR, body)


def main():
    logging.basicConfig(level=logging.INFO)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), SyncRequestHandler)
    logging.info("Starting HTTP server on port %s", PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - manual shutdown
        pass
    finally:
        server.server_close()
        logging.info("Server stopped")


if __name__ == "__main__":
    main()
