import pandas as pd
from sqlalchemy import create_engine, text
import cx_Oracle
import pytest
import psycopg2
from Configuration.ETLConfig import *
from Utilities.ReadFiles import *

mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{msql_port}/{mysql_database}")
#pg_engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}")


class DataLoading:

    def load_fact_sales_table(self):
        logger.info("Loading for fact_sales started...")
        query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)
                        select sales_id,product_id,store_id,quantity,sales_amount,sale_date from sales_with_details""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("Fact_sales table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Fact_sales table loading completed")
        except Exception as e:
            logger.error(f"Error occurred while loading fact_sales table {e}", exc_info=True)

    def load_fact_inventory_table(self):
        logger.info("Loading for Fact_inventory started...")
        query = text("""insert into fact_inventory(product_id,store_id,quantity_on_hand,last_updated) 
                        select product_id,store_id,quantity_on_hand,last_updated from staging_inventory""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("Fact_inventory table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("Fact_inventory table loading completed")
        except Exception as e:
            logger.error(f"Error occurred while loading fact_inventory table {e}", exc_info=True)

    def load_monthly_sales_summary_table(self):
        logger.info("Loading for monthly_sales_summary started...")
        query = text("""insert into monthly_sales_summary(product_id,month,year,total_sales) 
                        select product_id,month,year,total_sales from monthly_sales_summary_source""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("monthly_sales_summary table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("monthly_sales_summary table loading completed")
        except Exception as e:
            logger.error(f"Error occurred while loading monthly_sales_summary table {e}", exc_info=True)

    def load_inventory_level_by_store_table(self):
        logger.info("Loading for inventory_level_by_store started...")
        query = text("""insert into inventory_levels_by_store(store_id,total_inventory) 
                        select store_id,total_inventory from aggregated_inventory_level""")
        try:
            with mysql_engine.connect() as conn:
                logger.info("inventory_level_by_store table loading started...")
                logger.info(query)
                conn.execute(query)
                conn.commit()
                logger.info("inventory_level_by_store table loading completed")
        except Exception as e:
            logger.error(f"Error occurred while loading inventory_level_by_store table {e}", exc_info=True)



if __name__=="__main__":
    loadRef = DataLoading()
    loadRef.load_fact_sales_table()
    loadRef.load_fact_inventory_table()
    loadRef.load_inventory_level_by_store_table()
    loadRef.load_inventory_level_by_store_table()




