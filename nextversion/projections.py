import functools, dataclasses
import datetime as dt
from typing import Optional, ClassVar, Type, Tuple, Union, Iterable, TypeVar, Generic
import uuid
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.dialects import postgresql as _pgsql
from . import eventstore
from . import domain
from . import consumers

from ._consumer_db_orm import Base, mapper_registry


class Projection(consumers.Consumer):
    def _handle(self, recorded_event: eventstore.RecordedEvent):
        self._apply(recorded_event.event, recorded_event)

    def _apply(self, event: domain.Event, recorded_event: eventstore.RecordedEvent):
        raise NotImplementedError


T = TypeVar("T", bound=Projection)


class ProjectionReader(Generic[T]):
    Projection: ClassVar[Type[T]]

    def __init__(self, session):
        self.projection = self.Projection(session)
        self.session = session

    @property
    def current_logical_time(self) -> int:
        self.session.refresh(self.projection.housekeeping)
        return self.projection.housekeeping.last_seen_logical_time


class BankAccount(Base):
    __tablename__ = "bank_accounts"
    id = sa.Column(_pgsql.UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String())


class BankAccountProjection(Projection):
    consumer_key = "bank_account_projection"
    EVENT_CLASSES = (domain.BankAccountCreated,)

    @functools.singledispatchmethod
    def _apply(self, event: domain.Event, recorded_event: eventstore.RecordedEvent):
        raise NotImplementedError

    @_apply.register
    def _apply_task_created(
        self, event: domain.BankAccountCreated, recorded_event: eventstore.RecordedEvent
    ):
        self.session.add(
            BankAccount(
                id=event.aggregate_id,
                name=event.name,
            )
        )


BankAccountProjectionHousekeeping = BankAccountProjection.create_housekeeping_class(
    mapper_registry
)


class BankAccountReader(ProjectionReader[BankAccountProjection]):
    Projection = BankAccountProjection

    def get(self, id: uuid.UUID) -> Optional[BankAccount]:
        return self.session.get(BankAccount, id)

    def all(self, page=1, page_size=20, order_by="name"):
        return (
            self.session.query(BankAccount)
            .order_by(getattr(BankAccount, order_by))
            .limit(page_size)
            .offset((page - 1) * page_size)
        )

    def count(self):
        return self.session.query(BankAccount).count()
