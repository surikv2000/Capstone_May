import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import psycopg2
from Configuration.ETLConfig import *
from Utilities.ReadFiles import *

mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}")
#pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}")


class DataTransformation:

    def transform_filter_sales_data(self):
        logger.info("Filter Transformation started...")
        try:
            query = """select * from staging_sales where sale_date>='2024-09-10'"""
            df = pd.read_sql(query,mysql_engine)
            df.to_sql("filtered_sales_data", mysql_engine, if_exists='replace', index=False)
            logger.info("Filter Transformation completed...")
        except Exception as e:
            logger.error("Error occurred while filter transformation", e, exc_info=True)

    def transform_router_sales_data_Low_region(self):
        logger.info("Router Transformation for low region started...")
        try:
            query = """select * from filtered_sales_data where region ='Low'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("low_sales", mysql_engine, if_exists='replace', index=False)
            logger.info("Router for low region Transformation completed...")
        except Exception as e:
            logger.error("Error occurred while Router transformation", e, exc_info=True)

    def transform_router_sales_data_High_region(self):
        logger.info("Router Transformation for high region started...")
        try:
            query = """select * from filtered_sales_data where region ='High'"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("high_sales", mysql_engine, if_exists='replace', index=False)
            logger.info("Router for High region Transformation completed...")
        except Exception as e:
            logger.error("Error occurred while Router transformation", e, exc_info=True)


    def transform_aggregator_sales_data(self):
        logger.info("Aggregator Transformation for sales data started...")
        try:
            query = """select product_id,month(sale_date) as month ,year(sale_date) as year ,sum(quantity*price) as total_sales from filtered_sales_data 
                        group by product_id,month(sale_date),year(sale_date)"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("monthly_sales_summary_source", mysql_engine, if_exists='replace', index=False)
            logger.info("Aggregator transformation for sales data completed...")
        except Exception as e:
            logger.error("Error occurred while Aggregator transformation", e, exc_info=True)

    def transform_Joiner_sales_product_stores(self):
        logger.info("Joiner Transformation for sales data started...")
        try:
            query = """select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price 
                       as sales_amount,fs.sale_date,p.product_id,p.product_name,
                        s.store_id,s.store_name
                        from filtered_sales_data as fs 
                        inner join staging_product as p on fs.product_id = p.product_id
                        inner join staging_stores as s on fs.store_id = s.store_id"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("sales_with_details", mysql_engine, if_exists='replace', index=False)
            logger.info("Joiner transformation for sales data completed...")
        except Exception as e:
            logger.error("Error occurred while Joiner transformation", e, exc_info=True)

    def transform_aggregator_inventory_level(self):
        logger.info("Aggregator Transformation for inventory data started...")
        try:
            query = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by 
            store_id"""
            df = pd.read_sql(query, mysql_engine)
            df.to_sql("aggregated_inventory_level", mysql_engine, if_exists='replace', index=False)
            logger.info("Aggregator transformation for inventory data completed...")
        except Exception as e:
            logger.error("Error occurred while Aggregator transformation", e, exc_info=True)


if __name__=="__main__":
    trfRef = DataTransformation()
    trfRef.transform_filter_sales_data()
    trfRef.transform_router_sales_data_High_region()
    trfRef.transform_router_sales_data_Low_region()
    trfRef.transform_aggregator_sales_data()
    trfRef.transform_Joiner_sales_product_stores()
    trfRef.transform_aggregator_inventory_level()



