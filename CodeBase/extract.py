import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import psycopg2
from Configuration.ETLConfig import *
from Utilities.ReadFiles import read_files_and_write_to_stage

mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}")
pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}")


class DataExtraction:

    def extract_sales_data(self,file_path, file_type, table_name, db_engine):
        print("Sales Data extraction started...")
        read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
        print("Sales Data Extraction Completed")

    def extract_product_data(self,file_path, file_type, table_name, db_engine):
        print("Product Data extraction started...")
        read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
        print("Product Data Extraction Completed")

    def extract_supplier_data(self,file_path, file_type, table_name, db_engine):
        print("Supplier Data extraction started...")
        read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
        print("Supplier Data Extraction Completed")

    def extract_inventory_data(self,file_path, file_type, table_name, db_engine):
        print("Inventory Data extraction started...")
        read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
        print("Inventory Data Extraction Completed")

    def extract_department_data(self):
        print("department data extraction completed")
        query = """select * from department"""
        df = pd.read_sql(query, pg_engine)
        df.to_sql("staging_departments", mysql_engine, if_exists='replace', index=False)
        print("department data extraction completed")


if __name__ == "__main__":
    extRef = DataExtraction()
    extRef.extract_sales_data("Source_Systems/sales_data_Linux_remote.csv", "csv", "staging_sales", mysql_engine)
    extRef.extract_product_data("Source_Systems/product_data.csv", "csv", "staging_product", mysql_engine)
    extRef.extract_supplier_data("Source_Systems/supplier_data.json", "json", "staging_supplier", mysql_engine)
    extRef.extract_inventory_data("Source_Systems/inventory_data.xml", "xml", "staging_inventory", mysql_engine)
    extRef.extract_department_data()
