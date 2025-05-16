import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import psycopg2
from Configuration.ETLConfig import *
from Utilities.ReadFiles import *

mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}")
pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}")


class DataExtraction:

    def extract_sales_data(self,file_path, file_type, table_name, db_engine):
        logger.info("Sales Data extraction started...")
        try:
            CommonUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
            logger.info("Sales Data extraction completed...")
        except Exception as e:
            logger.error("Error occurred while sales data extraction", e, exc_info=True)

    def extract_product_data(self, file_path, file_type, table_name, db_engine):
        logger.info("Product Data extraction started...")
        try:
            CommonUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
            logger.info("Product Data extraction completed...")
        except Exception as e:
            logger.error("Error occurred while Product data extraction", e, exc_info=True)



    def extract_supplier_data(self,file_path, file_type, table_name, db_engine):
        logger.info("Supplier Data extraction started...")
        try:
            CommonUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
            logger.info("Supplier Data extraction completed...")
        except Exception as e:
            logger.error("Error occurred while supplier data extraction", e, exc_info=True)


    def extract_inventory_data(self,file_path, file_type, table_name, db_engine):
        logger.info("Inventory Data extraction started...")
        try:
            CommonUtilities.read_files_and_write_to_stage(file_path, file_type, table_name, db_engine)
            logger.info("Inventory Data extraction completed...")
        except Exception as e:
            logger.error("Error occurred while inventory data extraction", e, exc_info=True)


    def extract_stores_data(self):
        logger.info("stores data extraction started")
        try:
            query = """select * from stores"""
            df = pd.read_sql(query, pg_engine)
            df.to_sql("staging_stores", mysql_engine, if_exists='replace', index=False)
            logger.info("stores data extraction completed")
        except Exception as e:
            logger.error("Error Occurred while stores data extraction", e, exc_info=True)


if __name__ == "__main__":
    extRef = DataExtraction()
    extRef.extract_sales_data("Source_Systems/sales_data_Linux_remote.csv", "csv", "staging_sales", mysql_engine)
    extRef.extract_product_data("Source_Systems/product_data.csv", "csv", "staging_product", mysql_engine)
    extRef.extract_supplier_data("Source_Systems/supplier_data.json", "json", "staging_supplier", mysql_engine)
    extRef.extract_inventory_data("Source_Systems/inventory_data.xml", "xml", "staging_inventory", mysql_engine)
    extRef.extract_stores_data()
