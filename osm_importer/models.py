from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, DECIMAL, ARRAY, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

Base = declarative_base()


class Country(Base):
    __tablename__ = 'countries'

    id = Column(Integer, primary_key=True)
    osm_id = Column(BigInteger, unique=True, nullable=False, index=True)
    name_fr = Column(String(255))
    name_en = Column(String(255))
    name_local = Column(String(255), nullable=False)
    display_name = Column(String(255), nullable=False)
    country_code_alpha2 = Column(String(2), unique=True, index=True)
    country_code_alpha3 = Column(String(3), unique=True, index=True)
    center_lat = Column(DECIMAL(10, 8))
    center_lng = Column(DECIMAL(11, 8))
    boundaries = Column(Text)  # GeoJSON
    continent = Column(String(50))
    region = Column(String(100))
    timezones = Column(ARRAY(Text))
    currency_code = Column(String(3))
    official_languages = Column(ARRAY(Text))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    cities = relationship("City", back_populates="country")


class City(Base):
    __tablename__ = 'cities'

    id = Column(Integer, primary_key=True)
    osm_id = Column(BigInteger, unique=True, nullable=False, index=True)
    country_id = Column(Integer, ForeignKey('countries.id'), index=True)
    name_fr = Column(String(255))
    name_en = Column(String(255))
    name_local = Column(String(255), nullable=False)
    display_name = Column(String(255), nullable=False)
    center_lat = Column(DECIMAL(10, 8))
    center_lng = Column(DECIMAL(11, 8))
    region_state = Column(String(255))
    timezone = Column(String(50))
    place_type = Column(String(50))
    country_code_from_tags = Column(String(2), index=True)  # Nouveau champ
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    country = relationship("Country", back_populates="cities")