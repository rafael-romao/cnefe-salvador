import pandas as pd
import psycopg2
import logging
from pathlib import Path
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Any, Dict, Iterable, List
from sqlalchemy import create_engine

log = logging.getLogger(__name__)


class LocalToPostgres(BaseOperator):
    @apply_defaults
    def __init__(self,
                 data_dir_path,
                 raw_path,
                 file_name=None,
                 table_name=None,
                 format=None, 
                 *args,  **kwargs):
        super(LocalToPostgres, self).__init__(*args, **kwargs)

        self.conn_string = 'postgresql+psycopg2://postgres:postgres@data_warehouse:5432/case_gb_romao'
        self.data_dir_path = data_dir_path
        self.raw_path = raw_path
        self.file_name = file_name
        self.table_name = table_name
        self.format = format        
        self.conn = self._client()

    def execute(self, context):
        df = self._read_local()
        write_to_db = self._write_to_db(df)
        self.log.info(f'Result from write on db: {write_to_db}')
        self.conn.close()
        return None
    
    
    def _client(self):
        self.log.info("Getting the client")
        db = create_engine(self.conn_string)
        conn = db.connect()
        return conn

    def _read_local(self)-> pd.DataFrame:
        self.log.info("Getting the dataframe")
        final_path = f'{self.data_dir_path}{self.raw_path}'
        if self.format == 'csv':
            df = pd.read_csv(f'{final_path}/{self.file_name}')
        if self.format == 'xlsx':
            df = self._union_xls_files()
        return df
    
    def _write_to_db(self, df: pd.DataFrame):
        self.log.info("Writing on PostgreSQL")
        db_result = df.to_sql(self.table_name,
                              self.conn,
                              if_exists='replace',
                              index=False)
        
        return db_result

    def _union_xls_files(self) -> pd.DataFrame:
        self.log.info("Unioning the xls files into Dataframe")
        landing_path = f'{self.data_dir_path}{self.raw_path}'
        excel_list = [excel.name for excel in Path(landing_path).glob('*.xlsx')]
        excel_df_list = []
        for excel_file in excel_list:
            excel_final_path = f'{landing_path}/{excel_file}'
            excel_df = pd.read_excel(excel_final_path)
            excel_df_list.append(excel_df)
        df = pd.concat(excel_df_list)
        df.rename(columns=
            { 
            "ID_MARCA": "marca_id", 
            "MARCA": "marca_name",
            "ID_LINHA":"linha_id",
            "LINHA":"linha_name",
            "DATA_VENDA": "venda_date",
            "QTD_VENDA": "venda_quantity"
            },
            inplace=True
        )
        return df


