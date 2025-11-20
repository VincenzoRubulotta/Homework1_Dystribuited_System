from sqlalchemy import Column, Integer, String, BigInteger, DateTime, JSON, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB 

Base = declarative_base()

class UserInterest(Base):
    __tablename__ = "user_interests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String, nullable=False, index=True) 
    airport_icao = Column(String(4), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    
    __table_args__ = (
        UniqueConstraint('user_email', 'airport_icao', name='_user_airport_uc'),
    )

    def __repr__(self):
        return f"<UserInterest(email='{self.user_email}', airport='{self.airport_icao}')>"


class FlightData(Base):
    __tablename__ = "flight_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    icao24 = Column(String, nullable=False, index=True) 
    flight_type = Column(String(10), nullable=False) 
    airport_icao_ref = Column(String(4), nullable=False, index=True) 
    callsign = Column(String, nullable=True) 
    last_seen_time = Column(BigInteger, nullable=False) 
    raw_data = Column(JSON, nullable=False) 
    
    stored_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (
        UniqueConstraint('icao24', 'last_seen_time', name='_icao_time_uc'),
    )

    def __repr__(self):
        return f"<FlightData(icao24='{self.icao24}', type='{self.flight_type}')>"