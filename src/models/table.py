from dataclasses import dataclass

@dataclass
class column_schema():
    name: str
    field_type: str

@dataclass
class table_schema():
    table_name: str
    columns: list[column_schema]

    @property
    def columns_names(self) -> list:
        return [column.name for column in self.columns]
