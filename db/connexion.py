import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def init_connexion():
    load_dotenv()
    driver = os.getenv("DB_DRIVER")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    ensure_database(server, username, password, database, driver)


    return f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"


def ensure_database(server, username, password, database, driver):
    master_engine = create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/master?driver={driver}&TrustServerCertificate=yes",
        isolation_level="AUTOCOMMIT"
    )
    with master_engine.connect() as conn:
        conn.execute(
            text(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{database}') "
                 f"CREATE DATABASE {database}")
        )
