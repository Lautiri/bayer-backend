from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from google.api_core import exceptions as gcloud_exceptions
from google.cloud import bigquery
from google.oauth2 import service_account

from ..config import Settings
from ..utils.months import (
    normalize_admedia_months,
    normalize_instar_months,
    sort_admedia_months,
    sort_instar_months,
)


@dataclass(frozen=True)
class TableConfig:
    project_id: str
    dataset: str
    table: str
    month_column: str

    @property
    def fq_table(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"

@dataclass(frozen=True)
class TableInfo:
    table: str
    row_count: int



class BigQueryServiceError(Exception):
    """Base error for BigQuery service failures."""


class BigQueryService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._client: Optional[bigquery.Client] = None

    def _resolve_project_id(self, specific: Optional[str]) -> str:
        project_id = specific or self._settings.bigquery_project_id
        if not project_id:
            raise BigQueryServiceError(
                "BigQuery project id is not configured. Set GCP_PROJECT_ID or dataset-specific project env vars."
            )
        return project_id

    def _build_table_config(
        self,
        *,
        project_id: Optional[str],
        dataset: str,
        table: str,
        month_column: str,
    ) -> TableConfig:
        resolved_project = self._resolve_project_id(project_id)
        return TableConfig(
            project_id=resolved_project,
            dataset=dataset,
            table=table,
            month_column=month_column,
        )

    def _get_client(self) -> bigquery.Client:
        if self._client is not None:
            return self._client

        credentials = None
        project_id = self._settings.bigquery_project_id

        if self._settings.bigquery_credentials_path:
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    self._settings.bigquery_credentials_path
                )
                project_id = project_id or credentials.project_id
            except OSError as exc:
                raise BigQueryServiceError(
                    f"Unable to load service account credentials: {exc}"
                ) from exc

        if not project_id:
            raise BigQueryServiceError(
                "BigQuery project id is not configured. Set GCP_PROJECT_ID or BIGQUERY_CREDENTIALS_PATH."
            )

        client_kwargs = {"project": project_id}
        if credentials:
            client_kwargs["credentials"] = credentials
        if self._settings.bigquery_location:
            client_kwargs["location"] = self._settings.bigquery_location

        self._client = bigquery.Client(**client_kwargs)
        return self._client

    def _fetch_distinct_months(self, config: TableConfig) -> List[str]:
        client = self._get_client()
        query = (
            f"SELECT DISTINCT {config.month_column} AS month_value "
            f"FROM `{config.fq_table}` "
            f"WHERE {config.month_column} IS NOT NULL"
        )
        try:
            job = client.query(query)
            iterator = job.result()
            return [row["month_value"] for row in iterator if row["month_value"] is not None]
        except gcloud_exceptions.GoogleAPICallError as exc:
            raise BigQueryServiceError(
                f"Error fetching months for {config.fq_table}: {exc.message}"
            ) from exc

    def fetch_instar_months(self) -> List[str]:
        config = self._build_table_config(
            project_id=self._settings.instar_project_id,
            dataset=self._settings.instar_dataset,
            table=self._settings.instar_table,
            month_column=self._settings.instar_month_column,
        )
        raw_values = self._fetch_distinct_months(config)
        filtered = [value for value in raw_values if isinstance(value, str)]
        return sort_instar_months(filtered)

    def fetch_admedia_months(self) -> List[str]:
        config = self._build_table_config(
            project_id=self._settings.admedia_project_id,
            dataset=self._settings.admedia_dataset,
            table=self._settings.admedia_table,
            month_column=self._settings.admedia_month_column,
        )
        raw_values = self._fetch_distinct_months(config)
        normalized = normalize_admedia_months([str(value) for value in raw_values])
        return sort_admedia_months(normalized)

    def _delete_months(self, config: TableConfig, months: Sequence[str]) -> int:
        if not months:
            return 0

        client = self._get_client()
        query = (
            f"DELETE FROM `{config.fq_table}` "
            f"WHERE {config.month_column} IN UNNEST(@months)"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("months", "STRING", list(months))]
        )
        try:
            job = client.query(query, job_config=job_config)
            job.result()
            return int(job.num_dml_affected_rows or 0)
        except gcloud_exceptions.GoogleAPICallError as exc:
            raise BigQueryServiceError(
                f"Error deleting months from {config.fq_table}: {exc.message}"
            ) from exc

    def delete_instar_months(self, months: Sequence[str]) -> int:
        config = self._build_table_config(
            project_id=self._settings.instar_project_id,
            dataset=self._settings.instar_dataset,
            table=self._settings.instar_table,
            month_column=self._settings.instar_month_column,
        )
        normalized = normalize_instar_months(months)
        return self._delete_months(config, normalized)

    def delete_admedia_months(self, months: Sequence[str]) -> int:
        config = self._build_table_config(
            project_id=self._settings.admedia_project_id,
            dataset=self._settings.admedia_dataset,
            table=self._settings.admedia_table,
            month_column=self._settings.admedia_month_column,
        )
        normalized = normalize_admedia_months(months)
        return self._delete_months(config, normalized)

    _TABLE_PATTERN = re.compile(r"^[A-Za-z0-9\-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_\$]+$")

    def _validate_table_reference(self, table: str) -> str:
        candidate = table.strip()
        if not self._TABLE_PATTERN.match(candidate):
            raise BigQueryServiceError(
                "Table names must be fully-qualified (project.dataset.table) and contain only alphanumeric, '_', '-' or '$' characters."
            )
        return candidate

    def append_rows(
        self,
        *,
        source_table: str,
        destination_table: str,
        month_column: str,
        months: Optional[Sequence[str]] = None,
        dataset_type: str,
    ) -> int:
        source = self._validate_table_reference(source_table)
        destination = self._validate_table_reference(destination_table)

        if months:
            if dataset_type == "admedia":
                normalized_months = normalize_admedia_months(months)
            else:
                normalized_months = normalize_instar_months(months)
        else:
            normalized_months = []

        client = self._get_client()
        where_clause = ""
        query_parameters = []
        if normalized_months:
            where_clause = f" WHERE {month_column} IN UNNEST(@months)"
            query_parameters.append(
                bigquery.ArrayQueryParameter("months", "STRING", list(normalized_months))
            )

        query = (
            f"INSERT INTO `{destination}` "
            f"SELECT * FROM `{source}`{where_clause}"
        )

        job_config = bigquery.QueryJobConfig()
        if query_parameters:
            job_config.query_parameters = query_parameters

        try:
            job = client.query(query, job_config=job_config)
            job.result()
            return int(job.num_dml_affected_rows or 0)
        except gcloud_exceptions.GoogleAPICallError as exc:
            message = getattr(exc, "message", None) or str(exc)
            raise BigQueryServiceError(
                f"Error appending data from {source} to {destination}: {message}"
            ) from exc

    def list_columns(self, table: str) -> List[str]:
        candidate = self._validate_table_reference(table)
        client = self._get_client()
        try:
            table_obj = client.get_table(candidate)
            return [field.name for field in table_obj.schema]
        except gcloud_exceptions.GoogleAPICallError as exc:
            message = getattr(exc, "message", None) or str(exc)
            raise BigQueryServiceError(
                f"Error fetching schema for {candidate}: {message}"
            ) from exc

    def get_table_schema(self, table: str) -> List[bigquery.SchemaField]:
        candidate = self._validate_table_reference(table)
        client = self._get_client()
        try:
            table_obj = client.get_table(candidate)
            return list(table_obj.schema)
        except gcloud_exceptions.GoogleAPICallError as exc:
            message = getattr(exc, "message", None) or str(exc)
            raise BigQueryServiceError(
                f"Error obteniendo el esquema de {candidate}: {message}"
            ) from exc

    def get_client(self) -> bigquery.Client:
        return self._get_client()

    def get_table_info(self, table: str) -> TableInfo:
        candidate = self._validate_table_reference(table)
        client = self._get_client()
        try:
            table_obj = client.get_table(candidate)
        except gcloud_exceptions.NotFound:
            raise BigQueryServiceError(f"La tabla {candidate} no existe o no es accesible.")
        except gcloud_exceptions.GoogleAPICallError as exc:
            message = getattr(exc, "message", None) or str(exc)
            raise BigQueryServiceError(
                f"Error consultando la tabla {candidate}: {message}"
            ) from exc

        return TableInfo(table=candidate, row_count=int(table_obj.num_rows or 0))

    def export_rows(
        self,
        config: TableConfig,
        months: Sequence[str],
        *,
        dataset_type: str,
        selected_columns: Optional[Sequence[str]] = None,
    ) -> Tuple[List[str], List[dict]]:
        if not months:
            raise BigQueryServiceError("Seleccioná al menos un mes para exportar.")

        client = self._get_client()

        if dataset_type == "admedia":
            normalized_months = normalize_admedia_months(months)
        else:
            normalized_months = normalize_instar_months(months)

        available_columns = self.list_columns(config.fq_table)

        if selected_columns:
            invalid = [col for col in selected_columns if col not in available_columns]
            if invalid:
                raise BigQueryServiceError(
                    f"Columnas inválidas solicitadas: {', '.join(invalid)}"
                )
            column_clause = ", ".join(f"`{col}`" for col in selected_columns)
            fieldnames = list(selected_columns)
        else:
            column_clause = "*"
            fieldnames = available_columns

        query = (
            f"SELECT {column_clause} FROM `{config.fq_table}` "
            f"WHERE {config.month_column} IN UNNEST(@months) "
            f"ORDER BY {config.month_column}"
        )

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("months", "STRING", list(normalized_months))
            ]
        )

        try:
            job = client.query(query, job_config=job_config)
            iterator = job.result()
            rows = [dict(row.items()) for row in iterator]
            return fieldnames, rows
        except gcloud_exceptions.GoogleAPICallError as exc:
            raise BigQueryServiceError(
                f"Error exporting datos desde {config.fq_table}: {exc.message}"
            ) from exc

    def instar_config(self) -> TableConfig:
        return self._build_table_config(
            project_id=self._settings.instar_project_id,
            dataset=self._settings.instar_dataset,
            table=self._settings.instar_table,
            month_column=self._settings.instar_month_column,
        )

    def admedia_config(self) -> TableConfig:
        return self._build_table_config(
            project_id=self._settings.admedia_project_id,
            dataset=self._settings.admedia_dataset,
            table=self._settings.admedia_table,
            month_column=self._settings.admedia_month_column,
        )
