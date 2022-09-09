import sqlalchemy as sa
import pytest


TEST_DB_NAME = "test_events"
PROJECTIONS_DB_NAME = "test_projections"


def _get_db_dsn(db_name):
    return f"postgresql://postgres:docker@localhost:45432/{db_name}"


def create_clean_db(db_name):
    engine = sa.create_engine(_get_db_dsn("postgres"))
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


@pytest.fixture(scope="session", autouse=True)
def clean_db():
    create_clean_db(TEST_DB_NAME)
    create_clean_db(PROJECTIONS_DB_NAME)


@pytest.fixture(scope="session")
def events_db_engine():
    return sa.create_engine(_get_db_dsn(TEST_DB_NAME), echo=True)


@pytest.fixture(scope="session")
def projections_db_engine():
    return sa.create_engine(_get_db_dsn(PROJECTIONS_DB_NAME))
