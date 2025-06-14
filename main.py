import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
from datetime import datetime

# Import all modules
from database import Base, engine, get_db, UserDB, InstrumentDB, MarketOrderDB, LimitOrderDB
from schemas import (
    User, UserCreate, Instrument, InstrumentCreate,
    MarketOrder, MarketOrderCreate, LimitOrder, LimitOrderCreate,
    OrderMessage
)
from dependencies import authenticate_user, get_user_by_api_key, get_instrument_by_ticker
from messaging import rabbitmq, order_publisher

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Stock Exchange API", version="1.0.0")


@app.on_event("startup")
async def startup_event():
    # Initialize RabbitMQ connection
    rabbitmq.connect()


@app.on_event("shutdown")
async def shutdown_event():
    # Clean up RabbitMQ connection
    rabbitmq.disconnect()


# Basic routes
@app.get("/")
async def root():
    return {"message": "Stock Exchange API", "status": "running"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

###

@app.post("/api/v1/public/register", response_model=User)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    existing_user = get_user_by_api_key(db, user.api_key)
    if existing_user:
        raise HTTPException(status_code=400, detail="API key already exists")

    db_user = UserDB(**user.dict())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return User(
        id=UUID(db_user.id),
        name=db_user.name,
        role=db_user.role,
        api_key=db_user.api_key
    )

@app.get("/api/v1/public/instrument")
async def get_instruments():
    pass




###


# User routes
@app.post("/users", response_model=User)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    # Check if API key already exists
    existing_user = get_user_by_api_key(db, user.api_key)
    if existing_user:
        raise HTTPException(status_code=400, detail="API key already exists")

    db_user = UserDB(**user.dict())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return User(
        id=UUID(db_user.id),
        name=db_user.name,
        role=db_user.role,
        api_key=db_user.api_key
    )


@app.get("/users/{user_id}", response_model=User)
def get_user(user_id: str, db: Session = Depends(get_db)):
    db_user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    return User(
        id=UUID(db_user.id),
        name=db_user.name,
        role=db_user.role,
        api_key=db_user.api_key
    )


# Instrument routes
@app.post("/instruments", response_model=Instrument)
def create_instrument(instrument: InstrumentCreate, db: Session = Depends(get_db)):
    # Check if ticker already exists
    existing_instrument = get_instrument_by_ticker(db, instrument.ticker)
    if existing_instrument:
        raise HTTPException(status_code=400, detail="Ticker already exists")

    db_instrument = InstrumentDB(**instrument.dict())
    db.add(db_instrument)
    db.commit()
    db.refresh(db_instrument)
    return Instrument(
        id=db_instrument.id,
        name=db_instrument.name,
        ticker=db_instrument.ticker
    )


@app.get("/instruments", response_model=List[Instrument])
def list_instruments(db: Session = Depends(get_db)):
    instruments = db.query(InstrumentDB).all()
    return [
        Instrument(id=inst.id, name=inst.name, ticker=inst.ticker)
        for inst in instruments
    ]


@app.get("/instruments/{ticker}", response_model=Instrument)
def get_instrument(ticker: str, db: Session = Depends(get_db)):
    db_instrument = get_instrument_by_ticker(db, ticker)
    if not db_instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")
    return Instrument(
        id=db_instrument.id,
        name=db_instrument.name,
        ticker=db_instrument.ticker
    )


# Order routes
@app.post("/orders/market", response_model=MarketOrder)
async def create_market_order(
        order: MarketOrderCreate,
        api_key: str,
        user: UserDB = Depends(authenticate_user),
        db: Session = Depends(get_db)
):
    # Verify instrument exists
    instrument = get_instrument_by_ticker(db, order.ticker)
    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")

    # Create order in database
    db_order = MarketOrderDB(
        user_id=user.id,
        instrument_id=instrument.id,
        direction=order.direction,
        qty=order.qty
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)

    # Publish to RabbitMQ for async processing
    order_message = OrderMessage(
        order_id=db_order.id,
        order_type="market",
        user_id=db_order.user_id,
        ticker=instrument.ticker,
        direction=db_order.direction,
        qty=db_order.qty,
        timestamp=db_order.timestamp
    )

    order_publisher.publish_order(order_message)

    return MarketOrder(
        id=UUID(db_order.id),
        status=db_order.status,
        user_id=UUID(db_order.user_id),
        direction=db_order.direction,
        ticker=instrument.ticker,
        qty=db_order.qty,
        timestamp=db_order.timestamp
    )


@app.post("/orders/limit", response_model=LimitOrder)
async def create_limit_order(
        order: LimitOrderCreate,
        api_key: str,
        user: UserDB = Depends(authenticate_user),
        db: Session = Depends(get_db)
):
    # Verify instrument exists
    instrument = get_instrument_by_ticker(db, order.ticker)
    if not instrument:
        raise HTTPException(status_code=404, detail="Instrument not found")

    # Create order in database
    db_order = LimitOrderDB(
        user_id=user.id,
        instrument_id=instrument.id,
        direction=order.direction,
        qty=order.qty,
        price=order.price
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)

    # Publish to RabbitMQ for async processing
    order_message = OrderMessage(
        order_id=db_order.id,
        order_type="limit",
        user_id=db_order.user_id,
        ticker=instrument.ticker,
        direction=db_order.direction,
        qty=db_order.qty,
        price=db_order.price,
        timestamp=db_order.timestamp
    )

    order_publisher.publish_order(order_message)

    return LimitOrder(
        id=UUID(db_order.id),
        status=db_order.status,
        user_id=UUID(db_order.user_id),
        direction=db_order.direction,
        ticker=instrument.ticker,
        qty=db_order.qty,
        price=db_order.price,
        timestamp=db_order.timestamp
    )


@app.get("/orders/market/{order_id}", response_model=MarketOrder)
def get_market_order(order_id: str, db: Session = Depends(get_db)):
    db_order = db.query(MarketOrderDB).filter(MarketOrderDB.id == order_id).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")

    return MarketOrder(
        id=UUID(db_order.id),
        status=db_order.status,
        user_id=UUID(db_order.user_id),
        direction=db_order.direction,
        ticker=db_order.instrument.ticker,
        qty=db_order.qty,
        timestamp=db_order.timestamp
    )


@app.get("/orders/limit/{order_id}", response_model=LimitOrder)
def get_limit_order(order_id: str, db: Session = Depends(get_db)):
    db_order = db.query(LimitOrderDB).filter(LimitOrderDB.id == order_id).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")

    return LimitOrder(
        id=UUID(db_order.id),
        status=db_order.status,
        user_id=UUID(db_order.user_id),
        direction=db_order.direction,
        ticker=db_order.instrument.ticker,
        qty=db_order.qty,
        price=db_order.price,
        timestamp=db_order.timestamp
    )


# Get user's orders
@app.get("/users/{user_id}/orders/market", response_model=List[MarketOrder])
def get_user_market_orders(user_id: str, db: Session = Depends(get_db)):
    db_orders = db.query(MarketOrderDB).filter(MarketOrderDB.user_id == user_id).all()
    return [
        MarketOrder(
            id=UUID(order.id),
            status=order.status,
            user_id=UUID(order.user_id),
            direction=order.direction,
            ticker=order.instrument.ticker,
            qty=order.qty,
            timestamp=order.timestamp
        )
        for order in db_orders
    ]


@app.get("/users/{user_id}/orders/limit", response_model=List[LimitOrder])
def get_user_limit_orders(user_id: str, db: Session = Depends(get_db)):
    db_orders = db.query(LimitOrderDB).filter(LimitOrderDB.user_id == user_id).all()
    return [
        LimitOrder(
            id=UUID(order.id),
            status=order.status,
            user_id=UUID(order.user_id),
            direction=order.direction,
            ticker=order.instrument.ticker,
            qty=order.qty,
            price=order.price,
            timestamp=order.timestamp
        )
        for order in db_orders
    ]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)