from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session
from database import get_db, UserDB, InstrumentDB

def get_user_by_api_key(db: Session, api_key: str):
    return db.query(UserDB).filter(UserDB.api_key == api_key).first()

def get_instrument_by_ticker(db: Session, ticker: str):
    return db.query(InstrumentDB).filter(InstrumentDB.ticker == ticker).first()

def authenticate_user(api_key: str, db: Session = Depends(get_db)):
    user = get_user_by_api_key(db, api_key)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return user