from abc import ABC, abstractmethod
from models.table import TableSchema
import pandas as pd

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
    def run_query_to_dataframe(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_table(self, table: str, columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_table_schema_from_table(self, table: str) -> TableSchema:
        """Get the schema from an existing table or view"""
        pass

    @abstractmethod
    def get_table_schema_from_sql(self, query: str) -> TableSchema:
        """Get the schema of a table from a query"""
        pass
