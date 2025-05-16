import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import psycopg2
from Configuration.ETLConfig import *
import logging

# Logging Configuration
logging.basicConfig(
    filename="Logs/ETLLogs.log",
    filemode="a",  # a for appending and w for override
    format='%(asctime)s-%(levelname)s-%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}")
pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}")


class CommonUtilities:
    def read_files_and_write_to_stage(file_path, file_type, table_name, db_engine):
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path, xpath='.//item')
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        df.to_sql(table_name, db_engine, if_exists='replace', index=False)
