from dataclasses import dataclass
from google.cloud import bigquery
from typing import List, Tuple
from enum import Enum
import pandas as pd


class BigQueryDataType(str, Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    GEOGRAPHY = "GEOGRAPHY"
    BYTES = "BYTES"
    RECORD = "RECORD"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    ANY = "ANY"
    JSON = "JSON"
    BIGNUMERIC = "BIGNUMERIC"
    TIMEZONE = "TIMEZONE"
    INT64 = "INT64"
    FLOAT64 = "FLOAT64"
    BOOL = "BOOL"


class BigQueryDataMode(str, Enum):
    REPEATED = "REPEATED"


@dataclass
class ColumnSchema:
    name: str
    field_type: BigQueryDataType
    mode: BigQueryDataMode


@dataclass
class TableSchema:
    table_name: str
    columns: list[ColumnSchema]

    @property
    def columns_names(self) -> list:
        return [column.name for column in self.columns]

    def get_unsupported_fields(self) -> List[str]:
        """Returns a list of unsupported fields"""
        unsupported_fields = []
        for column in self.columns:
            if column.field_type == BigQueryDataType.RECORD:
                unsupported_fields.append(column.name)
        return unsupported_fields

    def get_column(self, column_name: str) -> ColumnSchema:
        return next(column for column in self.columns if column.name == column_name)

    def get_common_column_names(self, other) -> List[str]:
        """Returns a list of common columns"""
        return list(set(self.columns_names).intersection(other.columns_names))

    def get_common_columns(self, other) -> List[ColumnSchema]:
        """Returns a mapping of common columns and their field types"""
        common_columns_names = self.get_common_column_names(other)
        return [
            column for column in self.columns if column.name in common_columns_names
        ]

    def get_query_cast_schema_as_string(
        self, prefix="", column_name_suffix=""
    ) -> List[str]:
        """Returns SQL to query table schema as string, need to support arrays and strucs so leverage array_to_string and struct_to_string"""
        query_parts = []
        for column in self.columns:
            if (
                column.field_type == BigQueryDataType.STRING
                and column.mode != BigQueryDataMode.REPEATED
            ):
                query_parts.append(f"{prefix}{column.name}{column_name_suffix}")

            elif (
                column.field_type == BigQueryDataType.ARRAY
                or column.mode == BigQueryDataMode.REPEATED
            ):
                query_parts.append(
                    f"array_to_string((select array_agg(distinct x order by x asc) FROM UNNEST({prefix}{column.name}{column_name_suffix}) AS x), ',')"
                )

            elif column.field_type == BigQueryDataType.RECORD:
                # For now we don't support nested structs
                pass

            elif column.field_type == BigQueryDataType.STRUCT:
                # For now we don't support nested structs
                pass

            else:
                query_parts.append(
                    f"cast({prefix}{column.name}{column_name_suffix} as string)"
                )
        return query_parts

    def to_dataframe(self) -> pd.DataFrame:
        """Returns a dataframe of the table schema"""
        return pd.DataFrame(
            [
                [column.name, column.field_type, column.mode]
                for column in self.columns
            ],
            columns=["name", "field_type", "mode"],
        )

    @classmethod
    def from_bq_table(cls, table: bigquery.Table):
        columns = []
        for field in table.schema:
            columns.append(
                ColumnSchema(
                    name=field.name, field_type=field.field_type, mode=field.mode
                )
            )
        return TableSchema(table_name=table.table_id, columns=columns)