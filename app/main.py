from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from functools import lru_cache

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse
from google.api_core import exceptions as gcloud_exceptions
from google.cloud import bigquery

from .config import Settings, get_settings
from .schemas import (
    AppendRequest,
    AppendResponse,
    DeleteMonthsRequest,
    DeleteResponse,
    ExportRequest,
    LoginRequest,
    LoginResponse,
    MonthListResponse,
    TableImportResponse,
    TableInfoRequest,
    TableInfoResponse,
    TablePrefixResponse,
)
from .services.bigquery_service import BigQueryService, BigQueryServiceError


TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
ALLOWED_EXCEL_CONTENT_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel"
}

app = FastAPI(title="Bayer Data Admin API", version="1.0.0")


@lru_cache
def _service_singleton() -> BigQueryService:
    return BigQueryService(get_settings())


def get_app_settings() -> Settings:
    return get_settings()


def get_bigquery_service() -> BigQueryService:
    return _service_singleton()


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail if exc.detail else "Unexpected error"
    payload = detail if isinstance(detail, dict) else {"error": detail}
    return JSONResponse(status_code=exc.status_code, content=payload)


@app.exception_handler(BigQueryServiceError)
async def bigquery_exception_handler(_: Request, exc: BigQueryServiceError) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"error": str(exc)})


@app.post("/api/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    settings: Settings = Depends(get_app_settings),
) -> LoginResponse:
    if payload.password != settings.app_password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Contraseña inválida")

    return LoginResponse(authenticated=True)


@app.get("/api/instar/meses", response_model=MonthListResponse)
async def list_instar_months(service: BigQueryService = Depends(get_bigquery_service)) -> MonthListResponse:
    months = service.fetch_instar_months()
    return MonthListResponse(months=months)


@app.get("/api/admedia/meses", response_model=MonthListResponse)
async def list_admedia_months(service: BigQueryService = Depends(get_bigquery_service)) -> MonthListResponse:
    months = service.fetch_admedia_months()
    return MonthListResponse(months=months)


@app.delete("/api/instar", response_model=DeleteResponse)
async def delete_instar_months(
    payload: DeleteMonthsRequest,
    service: BigQueryService = Depends(get_bigquery_service),
) -> DeleteResponse:
    deleted = service.delete_instar_months(payload.months)
    return DeleteResponse(deleted=deleted, months=payload.months)


@app.delete("/api/admedia", response_model=DeleteResponse)
async def delete_admedia_months(
    payload: DeleteMonthsRequest,
    service: BigQueryService = Depends(get_bigquery_service),
) -> DeleteResponse:
    deleted = service.delete_admedia_months(payload.months)
    return DeleteResponse(deleted=deleted, months=payload.months)


@app.post("/api/instar/append", response_model=AppendResponse)
async def append_instar(
    payload: AppendRequest,
    service: BigQueryService = Depends(get_bigquery_service),
) -> AppendResponse:
    config = service.instar_config()
    rows_appended = service.append_rows(
        source_table=payload.source_table,
        destination_table=payload.destination_table,
        month_column=config.month_column,
        months=payload.months or [],
        dataset_type="instar",
    )
    return AppendResponse(
        rows_appended=rows_appended,
        source_table=payload.source_table,
        destination_table=payload.destination_table,
        months=payload.months,
    )


@app.post("/api/admedia/append", response_model=AppendResponse)
async def append_admedia(
    payload: AppendRequest,
    service: BigQueryService = Depends(get_bigquery_service),
) -> AppendResponse:
    config = service.admedia_config()
    rows_appended = service.append_rows(
        source_table=payload.source_table,
        destination_table=payload.destination_table,
        month_column=config.month_column,
        months=payload.months or [],
        dataset_type="admedia",
    )
    return AppendResponse(
        rows_appended=rows_appended,
        source_table=payload.source_table,
        destination_table=payload.destination_table,
        months=payload.months,
    )




def _build_csv_payload(fieldnames: list[str], rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key) for key in fieldnames})
    return buffer.getvalue()


@app.get("/api/table/prefix", response_model=TablePrefixResponse)
async def table_prefix(
    service: BigQueryService = Depends(get_bigquery_service),
) -> TablePrefixResponse:
    config = service.instar_config()
    prefix = f"{config.project_id}.{config.dataset}."
    return TablePrefixResponse(prefix=prefix)


@app.post("/api/table/info", response_model=TableInfoResponse)
async def table_info(
    payload: TableInfoRequest,
    service: BigQueryService = Depends(get_bigquery_service),
) -> TableInfoResponse:
    info = service.get_table_info(payload.table)
    return TableInfoResponse(table=info.table, row_count=info.row_count)


@app.post("/api/instar/import", response_model=TableImportResponse)
async def import_instar(
    table_name: str = Form(...),
    file: UploadFile = File(...),
    service: BigQueryService = Depends(get_bigquery_service),
) -> TableImportResponse:
    sanitized_name = table_name.strip()
    if not sanitized_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ingresa el nombre de la tabla temporal")
    if not TABLE_NAME_PATTERN.match(sanitized_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Usa solo letras, numeros o guiones bajos para el nombre de la tabla",
        )

    if file.content_type not in ALLOWED_EXCEL_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sube un archivo Excel (.xlsx)",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El archivo esta vacio")

    try:
        dataframe = pd.read_excel(io.BytesIO(content))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No se pudo leer el Excel: {exc}",
        ) from exc

    if dataframe.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El archivo no contiene filas")

    dataframe.columns = [str(col).strip() for col in dataframe.columns]

    config = service.instar_config()
    schema = service.get_table_schema(config.fq_table)
    expected_columns = [field.name for field in schema]

    missing = [column for column in expected_columns if column not in dataframe.columns]
    extra = [column for column in dataframe.columns if column not in expected_columns]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Faltan columnas requeridas: {', '.join(missing)}",
        )
    if extra:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Columnas no reconocidas en el archivo: {', '.join(extra)}",
        )

    dataframe = dataframe[expected_columns]

    for field in schema:
        column = field.name
        field_type = field.field_type.upper()
        if field_type in {"INTEGER", "INT64"}:
            numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
            if not numeric_series.dropna().apply(lambda value: float(value).is_integer()).all():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"La columna {column} contiene valores no enteros",
                )
            dataframe[column] = numeric_series.astype("Int64")
        elif field_type in {"FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
        elif field_type in {"BOOL", "BOOLEAN"}:
            normalized = dataframe[column].map(
                lambda value: None if pd.isna(value) else str(value).strip().lower()
            )
            mapped = normalized.map(
                {
                    "true": True,
                    "t": True,
                    "1": True,
                    "false": False,
                    "f": False,
                    "0": False,
                    None: None,
                }
            )
            if (normalized.notna() & mapped.isna()).any():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"La columna {column} contiene valores booleanos no reconocidos",
                )
            dataframe[column] = mapped
        elif field_type in {"TIMESTAMP", "DATETIME", "DATE"}:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

    dataframe = dataframe.where(pd.notnull(dataframe), None)

    client = service.get_client()
    temp_table = f"{config.project_id}.{config.dataset}.{sanitized_name}"

    try:
        client.get_table(temp_table)
    except gcloud_exceptions.NotFound:
        pass
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ya existe una tabla con ese nombre en el dataset",
        )

    load_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = client.load_table_from_dataframe(dataframe, temp_table, job_config=load_config)
        load_job.result()
    except gcloud_exceptions.GoogleAPICallError as exc:
        message = getattr(exc, "message", None) or str(exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error al cargar el archivo en BigQuery: {message}",
        ) from exc

    rows_appended = 0
    try:
        rows_appended = service.append_rows(
            source_table=temp_table,
            destination_table=config.fq_table,
            month_column=config.month_column,
            months=[],
            dataset_type="instar",
        )
    except BigQueryServiceError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    finally:
        try:
            client.delete_table(temp_table, not_found_ok=True)
        except gcloud_exceptions.GoogleAPICallError:
            pass

    return TableImportResponse(table_name=config.fq_table, temp_table=temp_table, rows_imported=rows_appended)


@app.post("/api/export")
async def export_data(
    payload: ExportRequest,
    service: BigQueryService = Depends(get_bigquery_service),
) -> StreamingResponse:
    if not payload.include_all_columns and not payload.columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Seleccioná columnas o exportá todas",
        )

    config = service.instar_config() if payload.source == "instar" else service.admedia_config()
    selected_columns = None if payload.include_all_columns else payload.columns

    fieldnames, rows = service.export_rows(
        config,
        payload.months,
        dataset_type=payload.source,
        selected_columns=selected_columns,
    )

    csv_payload = _build_csv_payload(fieldnames, rows)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{payload.source}_export_{timestamp}.csv"

    return StreamingResponse(
        iter([csv_payload]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
