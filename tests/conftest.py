import os
import pathlib
import sqlalchemy as sa
import pytest

TEST_DB_NAME = "es_test"


def pytest_configure(config):
    """Sets env vars as early as possible, so that early uses of the config
    are still covered"""
    os.environ[
        "BANK_ACCOUNTS_DB_DSN"
    ] = f"postgresql://postgres:docker@localhost:45432/{TEST_DB_NAME}"


def create_clean_db(db_name):
    from nextversion import config

    engine = sa.create_engine(config.get().get_db_dsn("postgres"))
    conn = engine.connect()
    test_db_exists = (
        conn.execute(
            sa.text("select count(*) from pg_database where datname = :db_name"),
            dict(db_name=db_name),
        ).scalar()
        > 0
    )
    if test_db_exists:
        conn.execute("ROLLBACK")
        conn.execute(f"DROP DATABASE {db_name}")
    conn.execute("ROLLBACK")
    conn.execute(f"CREATE DATABASE {db_name}")


def migrate_db():
    from alembic import command
    from alembic.config import Config

    print("Migrating DB")
    alembic_cfg = Config(str(pathlib.Path(__file__).parent.parent / "alembic.ini"))
    command.upgrade(alembic_cfg, "head")
    print("Migrated DB")


@pytest.fixture(scope="session", autouse=True)
def clean_db():
    create_clean_db(TEST_DB_NAME)
    migrate_db()


@pytest.fixture
def db_session(clean_db):
    from nextversion import _db

    session = _db.get_session()

    orig_get_session = _db.get_session
    _db.get_session = lambda *_, **__: session

    commit_fn = session.commit
    close_fn = session.close
    rollback_fn = session.rollback
    session.commit = session.flush
    session.close = session.flush
    session.rollback = lambda: None
    yield session
    session.commit = commit_fn
    session.close = close_fn
    session.rollback = rollback_fn
    session.rollback()
    session.close()

    _db.get_session = orig_get_session


@pytest.fixture
def event_store(db_session):
    from nextversion import eventstore
    from nextversion.domain import EVENT_CLASSES
    from nextversion._consumer_db_orm import events_table

    return eventstore.DBEventStore(db_session, events_table, EVENT_CLASSES)


@pytest.fixture
def uow(event_store):
    from nextversion import uow

    return uow.UoW(event_store)
