from sqlalchemy import create_engine
from db.connexion import init_connexion
from db.base import Base
from db import models

if __name__ == "__main__":

    engine = create_engine(init_connexion())
    # create tables
    Base.metadata.create_all(bind=engine)