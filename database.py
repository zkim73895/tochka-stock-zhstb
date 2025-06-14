from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from uuid import uuid4

# Database configuration
SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_exchange.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Import enums (will be defined in schemas.py)
from schemas import Direction, OrderStatus, UserRole


# SQLAlchemy Models
class UserDB(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    name = Column(String, nullable=False)
    role = Column(SQLEnum(UserRole), default=UserRole.USER)
    api_key = Column(String, nullable=False, unique=True)

    market_orders = relationship("MarketOrderDB", back_populates="user")
    limit_orders = relationship("LimitOrderDB", back_populates="user")


class InstrumentDB(Base):
    __tablename__ = "instruments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    ticker = Column(String, nullable=False, unique=True)

    market_orders = relationship("MarketOrderDB", back_populates="instrument")
    limit_orders = relationship("LimitOrderDB", back_populates="instrument")


class MarketOrderDB(Base):
    __tablename__ = "market_orders"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    status = Column(SQLEnum(OrderStatus), default=OrderStatus.NEW)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), nullable=False)
    direction = Column(SQLEnum(Direction), nullable=False)
    qty = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

    user = relationship("UserDB", back_populates="market_orders")
    instrument = relationship("InstrumentDB", back_populates="market_orders")


class LimitOrderDB(Base):
    __tablename__ = "limit_orders"

    id = Column(String, primary_key=True, default=lambda: str(uuid4()))
    status = Column(SQLEnum(OrderStatus), default=OrderStatus.NEW)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    instrument_id = Column(Integer, ForeignKey("instruments.id"), nullable=False)
    direction = Column(SQLEnum(Direction), nullable=False)
    qty = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

    user = relationship("UserDB", back_populates="limit_orders")
    instrument = relationship("InstrumentDB", back_populates="limit_orders")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
