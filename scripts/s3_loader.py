from s3_extractor import S3Extractor
from s3_cleaner import S3Cleaner
from s3_transformer import S3Transformer
import urllib
from sqlalchemy import create_engine, text
import os
import pandas as pd
import pymysql
import dotenv
#-------------------------------------

class SpartaDBInserter:
    def __init__(self, server="localhost", database="Sparta", username="root", password="", driver="mysql+pymysql"):
        """
        Initialise SQLAlchemy engine for MySQL.
        """
        # MySQL connection string
        conn_str = f"{driver}://{username}:{password}@{server}:3306/{database}"
        self.engine = create_engine(conn_str)

        # Test connection
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"Connected to MySQL database '{database}' on server '{server}'")
        except Exception as e:
            print(f"Connection failed: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        """
        Insert DataFrame into MySQL (auto IDs handled by MySQL).
        """
        df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
        print(f"Inserted {len(df)} rows into {table_name}")

    def insert_dataframe_with_id(self, df: pd.DataFrame, table_name: str):
        """
        Insert DataFrame with explicit IDs into MySQL.
        """
        df = df.where(pd.notnull(df), None)  # Replace NaN with None
        with self.engine.begin() as conn:
            df.to_sql(table_name, con=conn, if_exists="append", index=False, method="multi")
        print(f"Inserted {len(df)} rows into '{table_name}' with explicit IDs")
    

# Extract
extractor = S3Extractor()
dfs = extractor.get_csvs_to_dfs()

# Clean
cleaner = S3Cleaner()
clean_dfs = cleaner.clean_dfs(dfs)

# Transform
transformer = S3Transformer()
transform_dfs = transformer.transform_to_tables(clean_dfs)

# Insert into MySQL
inserter = SpartaDBInserter(
    server="",
    database="",
    username="",
    password=""
)

for table_name, df in transform_dfs.items():
    inserter.insert_dataframe(df, table_name)
