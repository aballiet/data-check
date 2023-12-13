from abc import ABC, abstractmethod
from typing import Tuple
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
    def get_table(self, table: str): # TODO : remove bigquery.Table
        pass

    @abstractmethod
    def run_query_to_dataframe(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_table(self, table: str, columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_tables_schemas(self, table1: str, table2: str) -> Tuple[TableSchema, TableSchema]:
        """Get the schema of a table"""
        pass