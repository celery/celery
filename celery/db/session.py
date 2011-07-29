from collections import defaultdict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

ResultModelBase = declarative_base()

_SETUP = defaultdict(lambda: False)
_ENGINES = {}
_MAKERS = {}


def get_engine(dburi, **kwargs):
    if dburi not in _ENGINES:
        _ENGINES[dburi] = create_engine(dburi, **kwargs)
    return _ENGINES[dburi]


def create_session(dburi, **kwargs):
    engine = get_engine(dburi, **kwargs)
    if dburi not in _MAKERS:
        _MAKERS[dburi] = sessionmaker(bind=engine)
    return engine, _MAKERS[dburi]


def setup_results(engine):
    if not _SETUP["results"]:
        ResultModelBase.metadata.create_all(engine)
        _SETUP["results"] = True


def ResultSession(dburi, **kwargs):
    engine, session = create_session(dburi, **kwargs)
    setup_results(engine)
    return session()
