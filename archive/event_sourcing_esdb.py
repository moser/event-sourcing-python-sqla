import itertools as it
import tempfile
import pickle
import collections
import pytest
import functools
import dataclasses
import uuid
from typing import Type, Iterable, Protocol, Optional
import sqlalchemy as sa


class Event(Protocol):
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID


@dataclasses.dataclass(frozen=True)
class TaskCreated(Event):
    title: str
    description: str
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskTitleChanged(Event):
    title: str
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskCompleted(Event):
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskAssigned(Event):
    assignee_id: uuid.UUID
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskUnassigned(Event):
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


class Aggregate:
    aggregate_id: uuid.UUID
    _events: list[Event]

    def __init__(self):
        self._events = []

    def apply(self, event: Event):
        self._apply(event)
        self.validate()
        self._events.append(event)

    def _apply(self, event: Event):
        raise NotImplementedError

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    @property
    def current_sequence_id(self) -> int:
        if not self._events:
            return 0
        return max(evt.sequence_id for evt in self.events)

    @property
    def next_event_attrs(self):
        return dict(
            sequence_id=self.current_sequence_id + 1, aggregate_id=self.aggregate_id
        )

    def validate(self):
        raise NotImplementedError

    def _validate_types(self, *attrs: str):
        types = self.__class__.__annotations__.copy()
        types["aggregate_id"] = uuid.UUID
        for attr in attrs:
            assert attr in types, f"No annotation for {attr}"
            if not isinstance(getattr(self, attr), types[attr]):
                raise TypeError(f"Wrong type for attribute {attr}")

    @classmethod
    def reconstruct(cls, events: Iterable[Event]):
        obj = cls()
        for event in events:
            obj.apply(event)
        return obj


class Task(Aggregate):
    title: str
    description: str
    completed: bool
    assignee_id: Optional[uuid.UUID]

    def __init__(self):
        super().__init__()

    def validate(self):
        self._validate_types("aggregate_id", "title", "description")
        assert len(self.title) >= 3

    @classmethod
    def create(cls, title: str, description: str) -> "Task":
        obj = cls()
        obj.apply(
            TaskCreated(
                sequence_id=0,
                aggregate_id=uuid.uuid4(),
                title=title,
                description=description,
            )
        )
        return obj

    @functools.singledispatchmethod
    def _apply(self, event: Event):
        raise NotImplementedError

    @_apply.register
    def _apply_task_created(self, event: TaskCreated):
        self.aggregate_id = event.aggregate_id
        self.title = event.title
        self.description = event.description
        self.completed = False
        self.assignee_id = None

    def update_title(self, title: str):
        self.apply(TaskTitleChanged(**self.next_event_attrs, title=title))

    @_apply.register
    def _apply_task_title_updated(self, event: TaskTitleChanged):
        self.title = event.title

    def complete(self):
        self.apply(TaskCompleted(**self.next_event_attrs))

    @_apply.register
    def _apply_task_completed(self, event: TaskCompleted):
        self.completed = True

    def assign(self, assignee_id: uuid.UUID):
        self.apply(TaskAssigned(**self.next_event_attrs, assignee_id=assignee_id))

    @_apply.register
    def _apply_task_assigned(self, event: TaskAssigned):
        self.assignee_id = event.assignee_id

    def unassign(self):
        self.apply(TaskUnassigned(**self.next_event_attrs))

    @_apply.register
    def _apply_task_unassigned(self, event: TaskUnassigned):
        self.assignee_id = None


def test_it():
    task = Task.create(title="Do it", description="Long text of what to do.")
    assert task.title == "Do it"

    task.update_title(title="Do it later")
    assert task.title == "Do it later"

    with pytest.raises(TypeError):
        task.update_title(title=123)


class TaskRepo:
    def __init__(self, uow: "UoW"):
        self._uow = uow
        self._reset_transaction()

    def add(self, task: Task):
        # assert aggregate_id not there
        assert not self.exists(task.aggregate_id)
        self._identity_map[task.aggregate_id] = task
        self._committed_sequence_ids[task.aggregate_id] = -1

    def get(self, aggregate_id: uuid.UUID) -> Task:
        if aggregate_id in self._identity_map:
            return self._identity_map[aggregate_id]
        obj = Task.reconstruct(self._get_events_for(aggregate_id))
        obj.validate()
        self._identity_map[aggregate_id] = obj
        self._committed_sequence_ids[aggregate_id] = obj.current_sequence_id
        return obj

    def exists(self, aggregate_id: uuid.UUID) -> Task:
        for event in self._get_events_for(aggregate_id):
            return True

    def _get_events_for(self, aggregate_id: uuid.UUID) -> Iterable[Event]:
        if aggregate_id in self._identity_map:
            yield from self._identity_map[aggregate_id].events
        else:
            yield from self._uow.get_events(aggregate_id, TaskCreated, TaskTitleChanged)

    def get_committable_events(self) -> Iterable[Event]:
        for obj in self._identity_map.values():
            committed_sequence_id = self._committed_sequence_ids[obj.aggregate_id]
            for event in obj.events:
                if event.sequence_id > committed_sequence_id:
                    yield event

    def _reset_transaction(self):
        self._identity_map = {}
        self._committed_sequence_ids = {}


class EventStore(Protocol):
    @property
    def all(self) -> Iterable[Event]:
        ...

    def get_events(self, aggregate_id: uuid.UUID, *types: Type) -> Iterable[Event]:
        ...

    def write(self, event: Event):
        ...

    def get_events_starting_at(self, target_index: int) -> Iterable[Event]:
        ...


class ESDBEventStore(EventStore):
    CLASSES = {
        cls.__name__: cls
        for cls in [
            TaskCreated,
            TaskTitleChanged,
            TaskCompleted,
            TaskAssigned,
            TaskUnassigned,
        ]
    }

    def __init__(self, stream_prefix: str):
        from esdbclient import EsdbClient

        self._stream_prefix = stream_prefix
        self.client = EsdbClient(uri="localhost:2113")

    def _from_db(self, event) -> Event:
        import json

        cls = self.CLASSES[event.type]
        content = json.loads(event.data.decode())
        for key in ["aggregate_id", "event_id"]:
            content[key] = uuid.UUID(content[key])
        return cls(**content)

    def get_events(self, aggregate_id: uuid.UUID, *types: Type) -> Iterable[Event]:
        import esdbclient

        try:
            response = self.client.read_stream_events(
                stream_name=self._get_stream_name(aggregate_id)
            )
            for event in response:
                yield self._from_db(event)
        except esdbclient.exceptions.StreamNotFound:
            pass

    def _get_stream_name(self, aggregate_id: uuid.UUID) -> str:
        return f"{self._stream_prefix}__{aggregate_id}"

    def _prepare_db_event(self, event: Event):
        import json
        from esdbclient import NewEvent

        content = dataclasses.asdict(event)
        for key in content:
            if isinstance(content[key], uuid.UUID):
                content[key] = str(content[key])

        return NewEvent(
            type=event.__class__.__name__,
            data=json.dumps(content).encode(),
            metadata=b"{}",
        )

    def write(self, event: Event):
        stream_name = self._get_stream_name(event.aggregate_id)
        self.client.append_events(
            stream_name=stream_name,
            expected_position=event.sequence_id - 1 if event.sequence_id > 0 else None,
            events=[self._prepare_db_event(event)],
        )

    def get_events_starting_at(self, target_index: int) -> Iterable[Event]:
        def events():
            for event in self.client.read_all_events(target_index):
                if event.stream_name.startswith(self._stream_prefix):
                    yield self._from_db(event)

        return events(), self.client.get_commit_position()


class UoW:
    def __init__(self, event_store: EventStore):
        self._event_store = event_store
        self.tasks = TaskRepo(self)
        self.repos = [self.tasks]

    def get_events(self, aggregate_id: uuid.UUID, *types: Type) -> Iterable[Event]:
        return self._event_store.get_events(aggregate_id, *types)

    def rollback(self):
        for repo in self.repos:
            repo._reset_transaction()


class ESDBUoW(UoW):
    event_store: ESDBEventStore

    def commit(self):
        for repo in self.repos:
            for event in repo.get_committable_events():
                self._event_store.write(event)
            repo._reset_transaction()


def test_uow_esdb():
    random_prefix = str(uuid.uuid4())
    subject = ESDBUoW(ESDBEventStore(random_prefix))

    task = Task.create(title="Do it", description="Long text of what to do.")
    task_id = task.aggregate_id
    subject.tasks.add(task)
    subject.commit()

    task = subject.tasks.get(task.aggregate_id)
    task.update_title(title="Something else")
    subject.commit()

    del task
    del subject

    # Just reading from disk now
    other_uow = ESDBUoW(ESDBEventStore(random_prefix))
    task2 = other_uow.tasks.get(task_id)
    assert task2.aggregate_id == task_id
    assert len(task2.events) == 2
    assert task2.title == "Something else"


class SQLiteReadModel:
    def __init__(self, fname):
        self._fname = fname
        self.connection = sa.create_engine(f"sqlite:///{fname}", echo=True).connect()
        self.connection.execute(
            "create table if not exists task_titles (task_id varchar, title varchar)"
        )
        self.connection.execute(
            "create table if not exists last_event_seen (event_id varchar, event_index integer)"
        )

    def _update_last_position(self, event_id: uuid.UUID, index: int):
        self.connection.execute(sa.sql.text("delete from last_event_seen"))
        self.connection.execute(
            sa.sql.text("insert into last_event_seen values (:event_id, :index)"),
            event_id=str(event_id),
            index=index,
        )

    def _get_last_position(self):
        row = self.connection.execute(
            "select event_id, event_index from last_event_seen"
        ).fetchone()
        if not row:
            return None, 0
        return row.event_id, row.event_index

    def update_from(self, event_store: EventStore):
        # TODO this should be in an transaction
        last_event_id, last_event_index = self._get_last_position()
        events, new_last_event_index = event_store.get_events_starting_at(
            last_event_index
        )
        if last_event_id is not None:
            first_event = next(events)
            assert str(first_event.event_id) == last_event_id
        any_events = False
        for event in events:
            any_events = True
            self._apply(event)
        if any_events:
            self._update_last_position(
                event_id=event.event_id, index=new_last_event_index
            )

    def _apply(self, event: Event):
        raise NotImplementedError


class TaskTitleReadModel(SQLiteReadModel):
    """Just interested in tasks and their titles"""

    def get_task_titles(self):
        for row in self.connection.execute(
            "select task_id, title from task_titles order by title"
        ):
            yield (row.task_id, row.title)

    @functools.singledispatchmethod
    def _apply(self, event: Event):
        # we are not interested in the event
        pass

    @_apply.register
    def _apply_task_created(self, event: TaskCreated):
        self.connection.execute(
            sa.sql.text("insert into task_titles values (:id, :title)"),
            id=str(event.aggregate_id),
            title=event.title,
        )

    @_apply.register
    def _apply_task_title_updated(self, event: TaskTitleChanged):
        self.connection.execute(
            sa.sql.text("update task_titles set title = :title where task_id = :id"),
            id=str(event.aggregate_id),
            title=event.title,
        )


def test_read_model_with_esdb():
    random_prefix = str(uuid.uuid4())
    with tempfile.NamedTemporaryFile() as tmp_db:
        event_store = ESDBEventStore(random_prefix)
        uow = ESDBUoW(event_store)
        task = Task.create(title="Do it", description="Long text of what to do.")
        task.update_title("Foo")
        uow.tasks.add(task)
        uow.commit()

        readmodel = TaskTitleReadModel(tmp_db.name)
        readmodel.update_from(event_store)
        assert list(readmodel.get_task_titles()) == [(str(task.aggregate_id), "Foo")]

        task2 = Task.create(title="Do it", description="Long text.")
        uow.tasks.add(task2)
        uow.commit()

        readmodel.update_from(event_store)
        assert list(readmodel.get_task_titles()) == [
            (str(task2.aggregate_id), "Do it"),
            (str(task.aggregate_id), "Foo"),
        ]

        task2 = Task.create(title="Do it", description="Long text.")
        for n in range(200):
            task2.update_title(f"asdkf {n}")
        uow.tasks.add(task2)
        uow.commit()

        readmodel.update_from(event_store)
        assert len(list(readmodel.get_task_titles())) == 3
