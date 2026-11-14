"""Shared constants, validation, and consistent JSON errors."""
import hashlib
import logging
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

CHUNK_SIZE = 1_000_000
REPLICATION_FACTOR = 2
log = logging.getLogger(__name__)


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_id(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError:
        raise HTTPException(400, "Invalid UUID") from None


def validate_metadata(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or len(value) > 100:
        raise HTTPException(400, "Metadata must be an object with at most 100 string pairs")
    if any(not isinstance(k, str) or not isinstance(v, str) or not k or
           len(k) > 128 or len(v) > 2048 for k, v in value.items()):
        raise HTTPException(400, "Metadata keys: 1–128 characters; string values: at most 2048")
    return value


def install_errors(app: FastAPI) -> None:
    original_openapi = app.openapi

    def openapi_with_validation_errors():
        schema = original_openapi()
        for path in schema["paths"].values():
            for operation in path.values():
                if not isinstance(operation, dict):
                    continue
                responses = operation.get("responses", {})
                if "422" in responses:
                    responses.pop("422")
                    responses["400"] = {
                        "description": "Invalid request",
                        "content": {"application/json": {"schema": {
                            "type": "object", "required": ["error"], "properties": {
                                "error": {"type": "object", "required": ["message"], "properties": {
                                    "message": {"type": "string"}, "details": {"type": "string"}}}
                            }}}}}
        return schema

    app.openapi = openapi_with_validation_errors

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return JSONResponse({"error": {"message": str(exc.detail)}}, status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return JSONResponse({"error": {"message": "Invalid request", "details": str(exc)}}, status_code=400)

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, exc):
        log.exception("Database operation failed")
        return JSONResponse({"error": {"message": "Metadata service unavailable"}}, status_code=503)

    @app.exception_handler(OSError)
    async def storage_error(request, exc):
        log.exception("Local storage operation failed")
        return JSONResponse({"error": {"message": "Local storage unavailable"}}, status_code=503)

    @app.exception_handler(Exception)
    async def unexpected_error(request, exc):
        log.exception("Unhandled request error")
        return JSONResponse({"error": {"message": "Internal server error"}}, status_code=500)
