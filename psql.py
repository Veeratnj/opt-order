from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy import text

# Load env vars
load_dotenv()

# --- PostgreSQL Connection Setup ---
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL,
                       pool_size=100,         # default is 5
                    max_overflow=100,      # default is 10
                    pool_timeout=50,
                    pool_pre_ping=True
                       )
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

print(SessionLocal)


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)
    age = Column(Integer)
    gender = Column(String(10))

    active_strategies = relationship("UserActiveStrategy", back_populates="user")


class StockDetails(Base):
    __tablename__ = 'stock_details'
    id = Column(Integer, primary_key=True, index=True)
    stock_name = Column(String, nullable=False)
    token = Column(String, unique=True, nullable=False)
    ltp = Column(Integer, nullable=False)
    last_update = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Corrected: This establishes the reverse relationship with UserActiveStrategy
    active_strategies = relationship("UserActiveStrategy", back_populates="stock_details")


class Strategy(Base):
    __tablename__ = 'strategy'
    id = Column(Integer, primary_key=True, index=True)
    strategy_name = Column(String, nullable=False)
    uuid = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(Integer, nullable=False, default=0)
    is_deleted = Column(Boolean, nullable=False, default=False)
    deleted_at = Column(DateTime, nullable=True, default=None)
    deleted_by = Column(Integer, nullable=True, default=None)

    active_strategies = relationship("UserActiveStrategy", back_populates="strategy")


class UserActiveStrategy(Base):
    __tablename__ = 'user_active_strategy'
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('user.id'))
    strategy_id = Column(String, ForeignKey('strategy.uuid'))
    stock_token = Column(String, ForeignKey('stock_details.token'), nullable=False)
    trade_count = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, nullable=False, default=True)
    is_started = Column(Boolean, nullable=False, default=False)
    deactivated_at = Column(DateTime, nullable=True, default=None)
    deactivated_by = Column(Integer, nullable=True, default=None)

    # Relationships
    user = relationship("User", back_populates="active_strategies")
    strategy = relationship("Strategy", back_populates="active_strategies")
    stock_details = relationship("StockDetails", back_populates="active_strategies")
    order_managers = relationship("OrderManager", back_populates="user_active_strategy")


class OrderManager(Base):
    __tablename__ = 'order_manager'
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, unique=True, nullable=False)
    completed_order_count = Column(Integer, nullable=False, default=0)
    buy_count = Column(Integer, nullable=False, default=0)
    sell_count = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # New foreign key
    user_active_strategy_id = Column(Integer, ForeignKey('user_active_strategy.id'), nullable=False)

    # Relationships
    user_active_strategy = relationship("UserActiveStrategy", back_populates="order_managers")


class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String, ForeignKey('order_manager.order_id'))
    stock_token = Column(String, ForeignKey('stock_details.token'))
    trade_type = Column(String, nullable=False)  # 'buy' or 'sell'
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    total_price = Column(Float, nullable=False)
    trade_entry_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    trade_exit_time = Column(DateTime, nullable=False, default=datetime.utcnow)


    order_manager = relationship("OrderManager")
    stock_details = relationship("StockDetails")





from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

def execute_queryv1(raw_sql, params=None):
    session = SessionLocal()
    try:
        result = session.execute(raw_sql, params or {})

        if result.returns_rows:
            # Get column names from result
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result.fetchall()]
            session.close()
            return data
        else:
            session.commit()
            session.close()
            return {"status": "success", "message": "Query executed successfully."}

    except SQLAlchemyError as e:
        session.rollback()
        session.close()
        print(f"SQLAlchemyError: {str(e)}")
        return {"status": "error", "message": str(e)}



def execute_query(raw_sql, params=None):
    # print('error')
    session = SessionLocal()
    try:
        # Wrap raw_sql in `text()` if it is a string
        raw_sql = text(raw_sql) if isinstance(raw_sql, str) else raw_sql
        result = session.execute(raw_sql, params or {})
        # print(result)
        if result.returns_rows:
            # Convert result to a list of dictionaries
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result.fetchall()]
            session.close()
            # print('hi')
            return data
        else:
            session.commit()
            session.close()
            return {"status": "success", "message": "Query executed successfully."}
    except SQLAlchemyError as e:
        session.rollback()
        session.close()
        # logging.error(f"SQLAlchemyError: {str(e)}", exc_info=True)
        raise
    finally:
        session.close()
  