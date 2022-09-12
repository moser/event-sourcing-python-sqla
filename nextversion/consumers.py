import functools, dataclasses
import datetime as dt
from typing import Optional, ClassVar, Type, Tuple, Union, Iterable
import uuid
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.dialects import postgresql as _pgsql
from . import eventstore
from . import domain

from ._consumer_db_orm import mapper_registry


class Consumer:
    consumer_key: ClassVar[str]
    EVENT_CLASSES: ClassVar[Tuple[Type[domain.Event]]] = ()
    _housekeeping_class: ClassVar[Optional[Type]] = None

    @classmethod
    def create_housekeeping_class(cls, mapper_registry):
        tbl = sa.Table(
            f"{cls.consumer_key}_housekeeping",
            mapper_registry.metadata,
            # this is a single entry table, default + unique ensure this
            sa.Column(
                "pk",
                sa.Integer(),
                primary_key=True,
                autoincrement=False,
                server_default="1",
            ),
            sa.Column("last_seen_logical_time", sa.Integer()),
            sa.Column("last_seen_observed_at", sa.DateTime()),
            sa.Column("observed_event_count", sa.Integer()),
        )

        housekeeping_class = type(f"Housekeeping_{cls.consumer_key}", (), {})

        mapper_registry.map_imperatively(housekeeping_class, tbl)
        cls._housekeeping_class = housekeeping_class
        return housekeeping_class

    def __init__(self, session):
        if not self._housekeeping_class:
            raise RuntimeError(
                "Please call `create_housekeeping_class` on the consumer class to finish it's configuration"
            )
        self.session = session
        self.housekeeping = self.session.get(self._housekeeping_class, 1)
        if not self.housekeeping:
            self.housekeeping = self._housekeeping_class(
                last_seen_logical_time=-1,
                last_seen_observed_at=dt.datetime(1970, 1, 1),
                observed_event_count=0,
            )
            self.session.add(self.housekeeping)

    def logical_time_seen(self, logical_time: int):
        self.session.refresh(self.housekeeping)
        return self.housekeeping.last_seen_logical_time >= logical_time

    def consume(self, event_store: eventstore.EventStore):
        for recorded_event in event_store.get_events_starting_at(
            logical_time=self.housekeeping.last_seen_logical_time,
            types=self.EVENT_CLASSES,
        ):
            self._handle(recorded_event)
            self.housekeeping.last_seen_logical_time = recorded_event.logical_time
            self.housekeeping.observed_event_count += 1
        self.session.commit()

    def _handle(self, recorded_event: eventstore.RecordedEvent):
        raise NotImplementedError
