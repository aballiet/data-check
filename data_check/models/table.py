from dataclasses import dataclass
from typing import List, Mapping
from enum import Enum

class BigQueryDataType(str, Enum):
    STRING = 'STRING'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    NUMERIC = 'NUMERIC'
    BOOLEAN = 'BOOLEAN'
    TIMESTAMP = 'TIMESTAMP'
    DATE = 'DATE'
    TIME = 'TIME'
    DATETIME = 'DATETIME'
    GEOGRAPHY = 'GEOGRAPHY'
    BYTES = 'BYTES'
    RECORD = 'RECORD'
    ARRAY = 'ARRAY'
    STRUCT = 'STRUCT'
    ANY = 'ANY'
    JSON = 'JSON'
    BIGNUMERIC = 'BIGNUMERIC'
    TIMEZONE = 'TIMEZONE'
    INT64 = 'INT64'
    FLOAT64 = 'FLOAT64'
    BOOL = 'BOOL'

@dataclass
class column_schema():
    name: str
    field_type: BigQueryDataType

@dataclass
class table_schema():
    table_name: str
    columns: list[column_schema]

    @property
    def columns_names(self) -> list:
        return [column.name for column in self.columns]

    def get_common_column_names(self, other) -> List[str]:
        """Returns a list of common columns"""
        return list(set(self.columns_names).intersection(other.columns_names))

    def get_common_columns(self, other) -> List[column_schema]:
        """Returns a mapping of common columns and their field types"""
        common_columns_names = self.get_common_column_names(other)
        return [column for column in self.columns if column.name in common_columns_names]

    def get_query_cast_schema_as_string(self, prefix="") -> List[str]:
        """Returns SQL to query table schema as string, need to support arrays and strucs so leverage array_to_string and struct_to_string"""
        query_parts = []
        for column in self.columns:
            if column.field_type == BigQueryDataType.ARRAY:
                query_parts.append(f"array_to_string({prefix}{column.name}, ',')")
            elif column.field_type == BigQueryDataType.STRUCT:
                query_parts.append(f"struct_to_string({prefix}{column.name}, ',')")
            else:
                query_parts.append(f"cast({prefix}{column.name} as string)")
        return query_parts