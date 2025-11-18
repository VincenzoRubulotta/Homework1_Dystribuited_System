from sqlalchemy import Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    email = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255))
    surname = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=func.now())