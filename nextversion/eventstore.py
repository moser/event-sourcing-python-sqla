import uuid, dataclasses, contextlib, datetime
from typing import Generic, TypeVar, Type, Iterable, Protocol, Union
from .domain import Event
from . import encoding
import sqlalchemy as sa


@dataclasses.dataclass
class RecordedEvent:
    logical_time: int
    observed_at: datetime.datetime
    event: Event


class EventStore(Protocol):
    def get_events(self, aggregate_id: uuid.UUID, types: list[Type]) -> Iterable[Event]:
        ...

    def write(self, event: Event):
        ...

    def get_events_starting_at(
        self, logical_time: int, type: Iterable[Type[Event]]
    ) -> Iterable[RecordedEvent]:
        ...


class DBEventStore(EventStore):
    @classmethod
    def create_table(cls, name, metadata):
        return sa.Table(
            name,
            metadata,
            sa.Column(
                "logical_time",
                sa.Integer,
                sa.Sequence(f"{name}_logical_time"),
                unique=True,
                nullable=False,
            ),
            sa.Column("observed_at", sa.DateTime, server_default=sa.func.now()),
            sa.Column("aggregate_id", sa.String, nullable=False, index=True),
            sa.Column("sequence_id", sa.Integer, nullable=False),
            sa.Column("event_id", sa.String, nullable=False),
            sa.Column("event_type", sa.String, nullable=False, index=True),
            sa.Column("content", sa.String, nullable=False),
        )

    def __init__(self, session, table, event_classes: dict[str, Type[Event]]):
        self.event_classes = event_classes
        self.session = session
        self.connection = session.connection()
        self.tbl = table
        self.qry = self.tbl.select()

    @property
    def current_logical_time(self) -> int:
        return self.connection.execute(
            sa.select(sa.func.max(self.tbl.c.logical_time))
        ).scalar()

    @contextlib.contextmanager
    def transaction(self):
        # with self.connection.begin() as transaction:
        try:
            yield
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def _from_row(self, row) -> Event:
        cls = self.event_classes[row.event_type]
        return cls(
            event_id=uuid.UUID(row.event_id),
            aggregate_id=uuid.UUID(row.aggregate_id),
            sequence_id=row.sequence_id,
            **encoding.parse_json(row.content),
        )

    def get_events(
        self, aggregate_id: uuid.UUID, types: Iterable[Type[Event]]
    ) -> Iterable[Event]:
        conditions = [
            self.tbl.c.aggregate_id == str(aggregate_id),
        ]
        if types:
            conditions.append(
                self.tbl.c.event_type.in_([cls.__name__ for cls in types])
            )
        for row in self.connection.execute(
            self.qry.where(sa.and_(*conditions)).order_by(self.tbl.c.sequence_id)
        ):
            yield self._from_row(row)

    def write(self, event: Event) -> int:
        content = dataclasses.asdict(event)
        del content["aggregate_id"]
        del content["sequence_id"]
        del content["event_id"]

        current_sequence_id = self.connection.execute(
            sa.select(sa.func.max(self.tbl.c.sequence_id)).where(
                self.tbl.c.aggregate_id == str(event.aggregate_id)
            )
        ).scalar()
        if current_sequence_id is None:
            current_sequence_id = -1
        if current_sequence_id != (event.sequence_id - 1):
            raise RuntimeError("Wrong seq id")

        result = self.connection.execute(
            self.tbl.insert().returning(self.tbl.c.logical_time),
            aggregate_id=str(event.aggregate_id),
            sequence_id=event.sequence_id,
            event_id=str(event.event_id),
            event_type=event.__class__.__name__,
            content=encoding.dump_json(content),
        ).scalar()
        return result

    def get_events_starting_at(
        self, logical_time: int, types: list[Type]
    ) -> Iterable[RecordedEvent]:
        for row in self.connection.execute(
            self.qry.where(
                sa.and_(
                    self.tbl.c.logical_time > logical_time,
                    self.tbl.c.event_type.in_([cls.__name__ for cls in types]),
                )
            ).order_by(self.tbl.c.logical_time)
        ):
            yield RecordedEvent(
                event=self._from_row(row),
                logical_time=row.logical_time,
                observed_at=row.observed_at,
            )

    def acquire_lock(self, object_type: str, pk: Union[str, int, uuid.UUID]):
        self.connection.execute(
            sa.sql.text(
                "SELECT pg_advisory_xact_lock(hashtext(:object_type), hashtext(:pk))"
            ),
            object_type=object_type,
            pk=str(pk),
        )
