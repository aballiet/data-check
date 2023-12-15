from abc import ABC, abstractmethod

import pandas as pd
from models.table import TableSchema
from sqlglot.expressions import Select


class QueryClient(ABC):
    ###### ABSTRACT METHODS ######
    @abstractmethod
    def get_credentials(self):
        pass

    @abstractmethod
    def init_client(self):
        pass

    @abstractmethod
    def get_table(self, table: str):
        pass

    @abstractmethod
    def run_query_to_dataframe(self, query: Select) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_table_schema_from_table(self, table: str) -> TableSchema:
        """Get the schema from an existing table or view"""
        pass

    @abstractmethod
    def get_table_schema_from_sql(self, query: Select) -> TableSchema:
        """Get the schema of a table from a query"""
        pass
