from s3_extractor import S3Extractor
from s3_cleaner import S3Cleaner
from s3_transformer import S3Transformer
import urllib
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

class S3Loader:
    
    """
    Class for loading DataFrames(tabel formatted) from S3Transformer into MY_DATABASE.
    """
    
    def load_tables_to_db(self, tables):
        
        load_dotenv()

        # Database connection
        driver = os.getenv("DB_DRIVER")
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
            "TrustServerCertificate=yes;"
        )
        
        params = urllib.parse.quote_plus(conn_str)

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        # Reset tables (delete in correct order)
        reset_order = ["Score", "Student", "Cohort", "Course", "Trainer", "Skill", "Week"]

        with engine.begin() as conn:
            for table in reset_order:
                print(f"Clearing table: {table}")
                conn.execute(text(f"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DELETE FROM dbo.{table};"))


        # Insert data back
        insert_order = ["Course", "Trainer", "Skill", "Week", "Cohort", "Student", "Score"]

        with engine.begin() as conn:
            for table_name in insert_order:
                if table_name in tables:
                    print(f"Inserting into table: {table_name}")
                    tables[table_name].to_sql(table_name, conn, if_exists="append", index=False)
                    

        # Load Tables
        extractor = S3Extractor()
        dfs = extractor.get_csvs_to_dfs()

        cleaner = S3Cleaner()
        clean_dfs = cleaner.clean_dfs(dfs)

        transformer = S3Transformer()
        tables = transformer.transform_to_tables(clean_dfs)