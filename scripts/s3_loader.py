import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env
load_dotenv()

class S3Loader:
    """
    Class for loading DataFrames into SQL (MSSQL or MySQL)
    """
    
    def __init__(self):
        """
        Initialise SQLAlchemy engine based on environment variables.
        """
        # Read from .env
        server_type = os.getenv("DB_SERVER_TYPE").lower()
        server = os.getenv("DB_SERVER")
        port = int(os.getenv("DB_PORT"))
        database = os.getenv("DB_NAME")
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        driver = os.getenv("DB_DRIVER")

        if server_type == "mssql":
            driver_encoded = urllib.parse.quote_plus(driver)
            conn_str = f"mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver={driver_encoded}"
        elif server_type == "mysql":
            conn_str = f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}"
        else:
            raise ValueError(f"Unsupported DB_SERVER_TYPE: {server_type}")

        self.engine = create_engine(conn_str)

        # Test connection
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"Connected to {server_type.upper()} database '{database}' on server '{server}'")
        except Exception as e:
            print(f"Connection failed: {e}")


    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        """
        Insert a DataFrame into SQL safely, handling identity columns and NULLs.

        - Automatically drops IDENTITY columns for tables where they exist.
        - Keeps all required PK/FK columns intact.
        - Replaces NaN with None for SQL Server.
        
        Identity columns are automatically determined from the database schema.
        """
        from sqlalchemy import inspect

        # Get table column info
        inspector = inspect(self.engine)
        columns_info = inspector.get_columns(table_name)

        # Identify IDENTITY columns
        identity_cols = [col['name'] for col in columns_info if col.get('autoincrement', False) or col.get('default') == "next value for"]


        df_to_insert = df.drop(columns=identity_cols, errors="ignore")  # drop other identities


        # Replace NaN with None
        df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)

        # Insert into database
        df_to_insert.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)

        print(f"Inserted {len(df_to_insert)} rows into '{table_name}' (dropped identity columns: {identity_cols})")