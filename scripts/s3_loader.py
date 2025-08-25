from scripts.s3_cleaner import transform_dfs_for_db
from scripts.s3_extractor import S3DataFrames
import urllib
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
# Load dataframes from S3
handler = S3DataFrames()
dfs = handler.get_csvs()
tables = transform_dfs_for_db(dfs)

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