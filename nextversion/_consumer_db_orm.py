from sqlalchemy import orm
from . import eventstore

mapper_registry = orm.registry()
Base = mapper_registry.generate_base()
events_table = eventstore.DBEventStore.create_table("events", Base.metadata)
