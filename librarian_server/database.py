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
session = SessionMaker()

Base = declarative_base()

def query(model: Base, **kwargs):
    """
    Query the database for a model.

    Parameters
    ----------
    model : Base
        The model to query.
    kwargs
        The query parameters.

    Returns
    -------
    Base
        The query result.
    """

    return session.query(model).filter_by(**kwargs)
