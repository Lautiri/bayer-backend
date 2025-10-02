from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    password: str = Field(..., min_length=1, description="Application password")


class LoginResponse(BaseModel):
    authenticated: bool


class MonthListResponse(BaseModel):
    months: List[str]


class DeleteMonthsRequest(BaseModel):
    months: List[str] = Field(..., min_items=1, description="Months to delete")


class DeleteResponse(BaseModel):
    deleted: int
    months: List[str]


class AppendRequest(BaseModel):
    source_table: str = Field(..., description="Fully-qualified BigQuery source table")
    destination_table: str = Field(..., description="Fully-qualified BigQuery destination table")
    months: Optional[List[str]] = Field(default=None, description="Optional list of months to append")


class AppendResponse(BaseModel):
    rows_appended: int
    source_table: str
    destination_table: str
    months: Optional[List[str]]


class TableInfoRequest(BaseModel):
    table: str = Field(..., description="Fully-qualified BigQuery table")


class TableInfoResponse(BaseModel):
    table: str
    row_count: int = Field(..., ge=0, description="Number of rows reported by BigQuery")


class TablePrefixResponse(BaseModel):
    prefix: str = Field(..., description="Fully-qualified project and dataset prefix ending with a dot")



class TableImportResponse(BaseModel):
    table_name: str
    temp_table: str
    rows_imported: int

class ExportRequest(BaseModel):
    source: Literal["instar", "admedia"]
    months: List[str] = Field(..., min_items=1, max_items=3, description="Months to export (max 3)")
    include_all_columns: bool = True
    columns: Optional[List[str]] = Field(default=None, description="Subset of columns if include_all_columns is False")




