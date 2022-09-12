from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    from nextversion import config
    from nextversion import _consumer_db_orm
    from nextversion import _db
    from nextversion import projections

    connectable = _db.get_engine()

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=_consumer_db_orm.Base.metadata
        )

        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
