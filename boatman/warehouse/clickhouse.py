import sys
from clickhouse_driver import Client
import logging
from .warehouse import Warehouse
from ..config.data_type import DataType

class ClickHouse(Warehouse):
    clickhouse_client:Client

    def connect(self):
        self.clickhoose_client = Client(
            host=self.conf_dict['host'],
            port=self.conf_dict.get('port', 9000),
            user=self.conf_dict['user'],
            password=self.conf_dict['password'],
        )
        logging.info("connecting to ClickHouse")
        logging.info("Running sample query")
        
        result = self.clickhoose_client.execute("SELECT 1")
        logging.info(f"Result = {result}")
        return True
  
    # @abstractmethod
    def create_schema(self, schema: str):
        """ Create schema or namespace if does not exist"""
        return

    # @abstractmethod
    def create_table(self, schema: str, table: str):
        """ Create table if does not exist"""
        return

    # @abstractmethod
    def describe_table(self, chema: str, table: str):
        return

    # @abstractmethod
    def add_column(self, schema: str, table: str, column: str, column_type: DataType):
        return

    # @abstractmethod
    def insert_df(self, schema: str, table: str, df):
        return
