from sqlalchemy import orm

mapper_registry = orm.registry()
Base = mapper_registry.generate_base()
