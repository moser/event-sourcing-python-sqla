import asyncio, uuid
from fastapi import FastAPI
from typing import TypeVar, Generic

import strawberry
from strawberry.fastapi import GraphQLRouter
from .gql.tools import force_run_in_executor, check_schema
from . import services
from .projections import BankAccountReader

app = FastAPI()


from functools import wraps, partial

PAGE_SIZE = 10


def get_injectable_reader(cls, session):
    # mock point for tests
    return cls(session)


def inject_readers(func):
    import inspect
    import sqlalchemy as sa
    from nextversion import _db
    from nextversion import projections

    injectable = [projections.BankAccountReader]

    to_inject = {}
    annot = func.__annotations__
    for key in list(annot.keys()):
        if annot[key] in injectable:
            to_inject[key] = annot[key]

    @wraps(func)
    def wrapper(*args, **kwargs):
        session = _db.get_session()
        for key, cls in to_inject.items():
            kwargs[key] = get_injectable_reader(cls, session)
        result = func(*args, **kwargs)
        session.close()
        return result

    wrapper.__signature__ = inspect.Signature(
        parameters=[
            param
            for param in inspect.signature(func).parameters.values()
            if param.name not in to_inject
        ],
        return_annotation=inspect.signature(func).return_annotation,
    )
    return wrapper


def query(func):
    return strawberry.field(force_run_in_executor(inject_readers(func)))


def get_injectable_service(cls, uow):
    # mock point for tests
    return cls(uow)


def inject_services(func):
    import inspect
    import sqlalchemy as sa
    from nextversion import eventstore
    from nextversion.domain import EVENT_CLASSES
    from nextversion.uow import UoW
    from nextversion import _db
    from nextversion import _consumer_db_orm

    injectable = [services.BankAccountService]

    to_inject = {}
    annot = func.__annotations__
    for key in list(annot.keys()):
        if annot[key] in injectable:
            to_inject[key] = annot[key]

    @wraps(func)
    def wrapper(*args, **kwargs):
        session = _db.get_session()
        event_store = eventstore.DBEventStore(
            session, _consumer_db_orm.events_table, EVENT_CLASSES
        )
        uow = UoW(event_store)
        for key, cls in to_inject.items():
            kwargs[key] = get_injectable_service(cls, uow)
        result = func(*args, **kwargs)
        session.close()
        return result

    wrapper.__signature__ = inspect.Signature(
        parameters=[
            param
            for param in inspect.signature(func).parameters.values()
            if param.name not in to_inject
        ],
        return_annotation=inspect.signature(func).return_annotation,
    )
    return wrapper


@force_run_in_executor
def _run_consumers():
    from .consumer_groups import PROJECTIONS

    PROJECTIONS.run_once()


def run_consumers_after(func):
    import inspect

    @wraps(func)
    def wrapper(*args, info: strawberry.types.Info, **kwargs):
        result = func(*args, **kwargs)
        # info.context["background_tasks"].add_task(_run_consumers)
        return result

    wrapper.__signature__ = inspect.Signature(
        parameters=[param for param in inspect.signature(func).parameters.values()]
        + [inspect.Parameter(name="info", kind=inspect.Parameter.KEYWORD_ONLY)],
        return_annotation=inspect.signature(func).return_annotation,
    )
    return wrapper


def mutation(func):
    return strawberry.mutation(
        force_run_in_executor(inject_services(run_consumers_after(func)))
    )


T = TypeVar("T")


@strawberry.type
class Page(Generic[T]):
    items: list[T]
    page: int
    overall: int


@strawberry.type
class BankAccount:
    id: uuid.UUID
    name: str


@strawberry.type
class Query:
    @query
    def bankAccounts(self, page: int, reader: BankAccountReader) -> Page[BankAccount]:
        return Page(
            items=reader.all(page=page, page_size=PAGE_SIZE),
            page=page,
            overall=reader.count(),
        )


@strawberry.type
class IdResult:
    id: uuid.UUID
    new_logical_time: int


@strawberry.type
class Mutation:
    @mutation
    def create_bank_account(
        self,
        name: str,
        bank_account_service: services.BankAccountService,
    ) -> IdResult:
        result = bank_account_service.create_account(name)
        return result


schema = strawberry.Schema(Query, mutation=Mutation)

check_schema(schema, strict=True)

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")
