from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    app_password: str = Field(default="bayern2025", validation_alias="APP_PASSWORD")
    private_key: str = Field(default=None, validation_alias="PRIVATE_KEY")
    client_email: str = Field(default=None, validation_alias="CLIENT_EMAIL")
    private_key_id: str = Field(default=None, validation_alias="PRIVATE_KEY_ID")
    client_id: str = Field(default=None, validation_alias="CLIENT_ID")
    project_id: str = Field(default=None, validation_alias="PROJECT_ID")

    bigquery_project_id: Optional[str] = Field(default=None, validation_alias="GCP_PROJECT_ID")
    bigquery_location: Optional[str] = Field(default=None, validation_alias="BIGQUERY_LOCATION")
    bigquery_credentials_path: Optional[str] = Field(
        default=None, validation_alias="BIGQUERY_CREDENTIALS_PATH"
    )

    instar_project_id: Optional[str] = Field(default=None, validation_alias="INSTAR_PROJECT_ID")
    instar_dataset: str = Field(default="bayer", validation_alias="INSTAR_DATASET")
    instar_table: str = Field(default="instar_historico", validation_alias="INSTAR_TABLE")
    instar_month_column: str = Field(
        default="Mes_Anio", validation_alias="INSTAR_MONTH_COLUMN"
    )

    admedia_project_id: Optional[str] = Field(default=None, validation_alias="ADMEDIA_PROJECT_ID")
    admedia_dataset: str = Field(default="bayer", validation_alias="ADMEDIA_DATASET")
    admedia_table: str = Field(default="admedia_historico", validation_alias="ADMEDIA_TABLE")
    admedia_month_column: str = Field(
        default="Mes", validation_alias="ADMEDIA_MONTH_COLUMN"
    )

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
