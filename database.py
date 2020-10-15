"""
database.py : config orm
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# SQLALCHEMY_DATABASE_URL = "postgresql://mag:password@localhost/magasin"
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:eisti0001@localhost/dbmovie"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,echo=True #view sql in log
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
  