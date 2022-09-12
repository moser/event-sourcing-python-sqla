import sqlalchemy as sa
from sqlalchemy import orm
from . import config

_ENGINE = None


def get_engine():
    print(config.get().get_sqla_engine_params())
    global _ENGINE
    if _ENGINE:
        return _ENGINE
    _ENGINE = sa.create_engine(**config.get().get_sqla_engine_params())
    return _ENGINE


def get_session():
    return orm.Session(get_engine())
