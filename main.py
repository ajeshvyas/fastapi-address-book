from fastapi import FastAPI, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy import create_engine
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from sqlalchemy import Column, Integer, Float, String

app = FastAPI()

SQLALCHEMY_DATABASE_URL = "sqlite:///./addresses.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
Base = declarative_base()
class Address(Base):
    __tablename__ = "addresses"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

Base.metadata.create_all(bind=engine)

def get_db():
    db = Session(bind=engine)
    try:
        yield db
    finally:
        db.close()

class AddressCreate(BaseModel):
    name: str
    latitude: float
    longitude: float

    @validator('latitude')
    def validate_latitude(cls, value):
        if value is not None and (value < -90 or value > 90):
            raise ValueError('Latitude value must be between -90 to 90')
        return value
    
    @validator('longitude')
    def validate_longitude(cls, value):
        if value is not None and (value < -180 or value > 180):
            raise ValueError('Longitude value must be between -180 to 180')
        return value

class AddressUpdate(BaseModel):
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @validator('latitude')
    def validate_latitude(cls, value):
        if value is not None and (value < -90 or value > 90):
            raise ValueError('Latitude value must be between -90 to 90')
        return value
    
    @validator('longitude')
    def validate_longitude(cls, value):
        if value is not None and (value < -180 or value > 180):
            raise ValueError('Longitude value must be between -180 to 180')
        return value

@app.get("/addresses/nearby/")
def get_addresses_within_distance(latitude: float, longitude: float, distance: float, db: Session = Depends(get_db)):
    addresses = db.query(Address).all()
    distance += 1
    polygon = Polygon([
        (latitude + distance, longitude + distance),
        (latitude - distance, longitude + distance),
        (latitude - distance, longitude - distance),
        (latitude + distance, longitude - distance),
    ])
    filtered_addresses = []
    for address in addresses:
        point = Point(address.latitude, address.longitude)
        if polygon.contains(point):
            filtered_addresses.append(address)
    return filtered_addresses


@app.get("/addresses/all/")
async def list_addresses(db: Session = Depends(get_db)):
    db_addresses = db.query(Address).all()
    return db_addresses

@app.post("/address/")
async def create_address(address: AddressCreate, db: Session = Depends(get_db)):
    db_address = Address(name=address.name, latitude=address.latitude, longitude=address.longitude)
    db.add(db_address)
    db.commit()
    db.refresh(db_address)
    return db_address

@app.get("/address/{address_id}")
async def read_address(address_id: int, db: Session = Depends(get_db)):
    db_address = db.query(Address).filter(Address.id == address_id).first()
    if db_address is None:
        raise HTTPException(status_code=404, detail="Address not found")
    return db_address

@app.put("/address/{address_id}")
async def update_address(address_id: int, address: AddressUpdate, db: Session = Depends(get_db)):
    db_address = db.query(Address).filter(Address.id == address_id).first()
    if db_address is None:
        raise HTTPException(status_code=404, detail="Address not found")
    if address.name is not None:
        db_address.name = address.name
    if address.latitude is not None:
        db_address.latitude = address.latitude
    if address.longitude is not None:
        db_address.longitude = address.longitude
    db.commit()
    db.refresh(db_address)
    return db_address

@app.delete("/address/{address_id}")
async def delete_address(address_id: int, db: Session = Depends(get_db)):
    db_address = db.query(Address).filter(Address.id == address_id).first()
    if db_address is None:
        raise HTTPException(status_code=404, detail="Address not found")
    db.delete(db_address)
    db.commit()
    return "Address deleted !"