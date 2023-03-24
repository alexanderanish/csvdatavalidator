#create a sql alchemy engine to be import and used by pandas
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from fastapi import FastAPI, HTTPException
from typing import Optional


SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL
)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# Define the upload status table schema
class UploadStatus(Base):
    __tablename__ = "upload_status"
    id = Column(Integer, primary_key=True, index=True)
    tablename = Column(String, index=True)
    filename = Column(String, index=True)
    row_count = Column(Integer)
    error_rows = Column(String, nullable=True)
    status = Column(String, index=True, default="pending")
    status_id = Column(Integer, index=True, default=1)
    mapping_id = Column(Integer, index=True, nullable=True)
    columns = Column(String, nullable=True)
    module = Column(String, nullable=True)
    load_id = Column(String, index=True, nullable=True)



