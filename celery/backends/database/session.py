# -*- coding: utf-8 -*-
"""
    celery.backends.database.session
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    SQLAlchemy sessions.

"""
from __future__ import absolute_import

from collections import defaultdict
from multiprocessing.util import register_after_fork

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

ResultModelBase = declarative_base()

_SETUP = defaultdict(lambda: False)
_ENGINES = {}
_SESSIONS = {}

__all__ = ['ResultSession', 'get_engine', 'create_session']


class _after_fork(object):
    registered = False

    def __call__(self):
        self.registered = False  # child must reregister
        for engine in list(_ENGINES.values()):
            engine.dispose()
        _ENGINES.clear()
        _SESSIONS.clear()
after_fork = _after_fork()


def get_engine(dburi, **kwargs):
    try:
        return _ENGINES[dburi]
    except KeyError:
        engine = _ENGINES[dburi] = create_engine(dburi, **kwargs)
        after_fork.registered = True
        register_after_fork(after_fork, after_fork)
        return engine


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
