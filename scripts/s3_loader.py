from s3_extractor import S3Extractor
from s3_cleaner import S3Cleaner
from s3_transformer import S3Transformer
import urllib
from sqlalchemy import create_engine, text
import os
import pandas as pd
import dotenv
#-------------------------------------

class S3Loader:
    def __init__(self, server="localhost", database="Sparta", username=None, password=None, driver="ODBC+Driver+17+for+SQL+Server"):
        """
        Initialize SQLAlchemy engine for SQL Server.
        """
        
        if username and password:
            conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
        else:
            # Trusted connection (Windows Auth)
            conn_str = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
        
        self.engine = create_engine(conn_str, fast_executemany=True)

           
           # Test connection
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            print(f"Connected to database '{database}' on server '{server}'")
        except Exception as e:
            print(f"Connection failed: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        """
        Insert letting SQL insert ids
        """
        df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
        print(f" Inserted {len(df)} rows into {table_name}")


    def insert_dataframe_with_id(self, df: pd.DataFrame, table_name: str):
        """
        Insert into SQL using our ids
        """
        df = df.where(pd.notnull(df), None)  # Replace NaN with None

        with self.engine.begin() as conn:  # begin a transaction
            # Enable explicit identity insert
            conn.execute(text(f"SET IDENTITY_INSERT {table_name} ON"))

            # Insert the DataFrame using the same connection
            df.to_sql(table_name, con=conn, if_exists="append", index=False, method='multi')

            # Disable explicit identity insert
            conn.execute(text(f"SET IDENTITY_INSERT {table_name} OFF"))

        print(f" Inserted {len(df)} rows into '{table_name}' with explicit IDs")
    

# Extract
extractor = S3Extractor()
dfs = extractor.get_csvs_to_dfs()

# Clean
cleaner = S3Cleaner()
clean_dfs = cleaner.clean_dfs(dfs)

# Transfrom
transform = S3Transformer()
transform_dfs = transform.transform_to_tables(clean_dfs)

# print(transform_dfs)

yourdriver   = os.getenv("DB_DRIVER")
yourserver   = os.getenv("DB_SERVER")
user     = os.getenv("DB_USER")
urpassword = os.getenv("DB_PASSWORD")


inserter = S3Loader(
    driver=yourdriver,
    server=yourserver, 
    database="Sparta",
    username=user,
    password=urpassword
)

# # Independent tables
inserter.insert_dataframe_with_id(transform_dfs['course'], "Course")
inserter.insert_dataframe_with_id(transform_dfs['trainer'], "Trainer")
inserter.insert_dataframe_with_id(transform_dfs['week'], "Week")
inserter.insert_dataframe_with_id(transform_dfs['skill'], "Skill")
inserter.insert_dataframe_with_id(transform_dfs['weakness'], "Weakness")
inserter.insert_dataframe_with_id(transform_dfs['strength'], "Strength")
inserter.insert_dataframe_with_id(transform_dfs['university'], "University")
inserter.insert_dataframe_with_id(transform_dfs['talent_member'], "TalentMember")
inserter.insert_dataframe_with_id(transform_dfs['tech_skill'], "TechSkill")
inserter.insert_dataframe_with_id(transform_dfs['address'], "Address")

# # Dependent tables
inserter.insert_dataframe_with_id(transform_dfs['candidate'], "Candidate")
inserter.insert_dataframe_with_id(transform_dfs['candidate_university'], "CandidateUniversity")
inserter.insert_dataframe_with_id(transform_dfs['invitation'], "Invitation")
inserter.insert_dataframe_with_id(transform_dfs['cohort'], "Cohort")
inserter.insert_dataframe_with_id(transform_dfs['student'], "Student")

# # Junction + fact tables
inserter.insert_dataframe_with_id(transform_dfs['interview'], "Interview")
inserter.insert_dataframe_with_id(transform_dfs['candidate_tech_skill'], "CandidateTechSkill")
inserter.insert_dataframe_with_id(transform_dfs['candidate_weakness'], "CandidateWeakness")
inserter.insert_dataframe_with_id(transform_dfs['candidate_strength'], "CandidateStrength")
inserter.insert_dataframe_with_id(transform_dfs['score'], "Score")
inserter.insert_dataframe_with_id(transform_dfs['sparta_day'], "SpartaDay")

