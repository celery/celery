# -*- coding: utf-8 -*-
"""
    celery.backends.database.session
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    SQLAlchemy sessions.

"""
from __future__ import absolute_import

from collections import defaultdict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

ResultModelBase = declarative_base()

_SETUP = defaultdict(lambda: False)
_ENGINES = {}
_SESSIONS = {}


def get_engine(dburi, **kwargs):
    if dburi not in _ENGINES:
        _ENGINES[dburi] = create_engine(dburi, **kwargs)
    return _ENGINES[dburi]


def create_session(dburi, short_lived_sessions=False, **kwargs):
    engine = get_engine(dburi, **kwargs)
    if short_lived_sessions or dburi not in _SESSIONS:
        _SESSIONS[dburi] = sessionmaker(bind=engine)
    return engine, _SESSIONS[dburi]


def setup_results(engine):
    if not _SETUP['results']:
        ResultModelBase.metadata.create_all(engine)
        _SETUP['results'] = True


def ResultSession(dburi, **kwargs):
    engine, session = create_session(dburi, **kwargs)
    setup_results(engine)
    return session()
