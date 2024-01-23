"""
Core database runner for SQLAlchemy.
"""

from .settings import server_settings
from .logger import log

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Enum, DateTime, BigInteger, PickleType

log.info("Starting database engine.")

# Create the engine and sessionmaker.
engine = create_engine(
    server_settings.sqlalchemy_database_uri,
    # Required for async and SQLite
    connect_args={"check_same_thread": False}
)

log.info("Creating database session.")

SessionMaker = sessionmaker(bind=engine, autocommit=False, autoflush=False)

def yield_session() -> SessionMaker:
    """
    Yields a new databse session.
    """

    session = SessionMaker()
    try:
        yield session
    finally:
        session.close()


def get_session() -> SessionMaker:
    """
    Returns a new database session. Unlike yield_session, it is
    your responsibility to close the session.
    """

    return SessionMaker()

Base = declarative_base()

