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
    event_id: uuid.UUID
    aggregate_id: uuid.UUID


@dataclasses.dataclass(frozen=True)
class TaskCreated(Event):
    title: str
    description: str
    aggregate_id: uuid.UUID
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskTitleChanged(Event):
    title: str
    aggregate_id: uuid.UUID
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskCompleted(Event):
    aggregate_id: uuid.UUID
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskAssigned(Event):
    assignee_id: uuid.UUID
    aggregate_id: uuid.UUID
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass(frozen=True)
class TaskUnassigned(Event):
    aggregate_id: uuid.UUID
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
            TaskCreated(aggregate_id=uuid.uuid4(), title=title, description=description)
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
        self.apply(TaskTitleChanged(aggregate_id=self.aggregate_id, title=title))

    @_apply.register
    def _apply_task_title_updated(self, event: TaskTitleChanged):
        self.title = event.title

    def complete(self):
        self.apply(TaskCompleted(aggregate_id=self.aggregate_id))

    @_apply.register
    def _apply_task_completed(self, event: TaskCompleted):
        self.completed = True

    def assign(self, assignee_id: uuid.UUID):
        self.apply(
            TaskAssigned(aggregate_id=self.aggregate_id, assignee_id=assignee_id)
        )

    @_apply.register
    def _apply_task_assigned(self, event: TaskAssigned):
        self.assignee_id = event.assignee_id

    def unassign(self):
        self.apply(TaskUnassigned(aggregate_id=self.aggregate_id))

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

    def get(self, aggregate_id: uuid.UUID) -> Task:
        if aggregate_id in self._identity_map:
            return self._identity_map[aggregate_id]
        obj = Task.reconstruct(self._get_events_for(aggregate_id))
        obj.validate()
        self._identity_map[aggregate_id] = obj
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
            yield from obj.events

    def get_committable_aggregates(self) -> Iterable[Aggregate]:
        for obj in self._identity_map.values():
            yield obj

    def _reset_transaction(self):
        self._identity_map = {}


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


class FileEventStore:
    def __init__(self, fname: str):
        self._fname = fname

    @property
    def all(self):
        with open(self._fname, "rb") as fp:
            try:
                while True:
                    yield pickle.load(fp)
            except EOFError:
                pass

    def get_events(self, aggregate_id: uuid.UUID, *types: Type) -> Iterable[Event]:
        for event in self.all:
            if event.aggregate_id == aggregate_id and isinstance(event, types):
                yield event

    def write(self, event: Event):
        with open(self._fname, "ab") as fp:
            pickle.dump(event, fp)

    def get_events_starting_at(self, target_index: int) -> Iterable[Event]:
        def events():
            for index, event in enumerate(self.all):
                if index >= target_index:
                    yield event

        return events(), sum(1 for _ in self.all) - 1


class SQLiteEventStore:
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

    def __init__(self, fname):
        self._fname = fname
        self.connection = sa.create_engine(f"sqlite:///{fname}", echo=True).connect()
        self.metadata = sa.MetaData()
        self.tbl = sa.Table(
            "events",
            self.metadata,
            sa.Column("logical_time", sa.Integer, primary_key=True),
            sa.Column("aggregate_id", sa.String, nullable=False),
            sa.Column("event_id", sa.String, nullable=False),
            sa.Column("event_type", sa.String, nullable=False),
            sa.Column("content", sa.String, nullable=False),
        )
        self.metadata.create_all(self.connection, checkfirst=True)
        self.qry = self.tbl.select().order_by(self.tbl.c.logical_time)

    def _from_row(self, row) -> Event:
        import json

        cls = self.CLASSES[row.event_type]
        return cls(
            event_id=uuid.UUID(row.event_id),
            aggregate_id=uuid.UUID(row.aggregate_id),
            **json.loads(row.content),
        )

    @property
    def all(self):
        for row in self.connection.execute(self.qry):
            yield self._from_row(row)

    def get_events(self, aggregate_id: uuid.UUID, *types: Type) -> Iterable[Event]:
        for row in self.connection.execute(
            self.qry.where(
                sa.and_(
                    self.tbl.c.aggregate_id == str(aggregate_id),
                    self.tbl.c.event_type.in_([cls.__name__ for cls in types]),
                )
            )
        ):
            yield self._from_row(row)

    def write(self, event: Event):
        import json

        content = dataclasses.asdict(event)
        del content["event_id"]
        del content["aggregate_id"]

        self.connection.execute(
            self.tbl.insert(),
            aggregate_id=str(event.aggregate_id),
            event_id=str(event.event_id),
            event_type=event.__class__.__name__,
            content=json.dumps(content),
        )

    def get_events_starting_at(self, target_index: int) -> Iterable[Event]:
        def events():
            for row in self.connection.execute(self.qry.offset(target_index)):
                yield self._from_row(row)

        return (
            events(),
            self.connection.execute(self.tbl.select(sa.func.count())).scalar(),
        )


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

    def write(self, event: Event):
        raise NotImplementedError

    def _get_stream_name(self, aggregate_id: uuid.UUID) -> str:
        return f"{self._stream_prefix}__{aggregate_id}"

    def write_if_necessary(self, event: Event, position: int):
        import json
        from esdbclient import NewEvent

        stream_name = self._get_stream_name(event.aggregate_id)
        current_pos = self.client.get_stream_position(stream_name=stream_name)
        if current_pos is not None and current_pos >= position:
            return
        content = dataclasses.asdict(event)
        for key in content:
            if isinstance(content[key], uuid.UUID):
                content[key] = str(content[key])
        self.client.append_events(
            stream_name=stream_name,
            expected_position=position - 1 if current_pos is not None else None,
            events=[
                NewEvent(
                    type=event.__class__.__name__,
                    data=json.dumps(content).encode(),
                    metadata=b"{}",
                )
            ],
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

    def commit(self):
        committed_event_ids = {event.event_id for event in self._event_store.all}
        for repo in self.repos:
            for event in repo.get_committable_events():
                if event.event_id not in committed_event_ids:
                    self._event_store.write(event)
            repo._reset_transaction()


class ESDBUoW(UoW):
    event_store: ESDBEventStore

    def commit(self):
        for repo in self.repos:
            for obj in repo.get_committable_aggregates():
                for idx, event in enumerate(obj.events):
                    self._event_store.write_if_necessary(event, idx)
            repo._reset_transaction()


def test_uow():
    with tempfile.NamedTemporaryFile() as tmp:
        subject = UoW(FileEventStore(tmp.name))

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
        other_uow = UoW(FileEventStore(tmp.name))
        task2 = other_uow.tasks.get(task_id)
        assert task2.aggregate_id == task_id
        assert len(task2.events) == 2
        assert task2.title == "Something else"


def test_uow_sqlite():
    with tempfile.NamedTemporaryFile() as tmp:
        subject = UoW(SQLiteEventStore(tmp.name))

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
        other_uow = UoW(SQLiteEventStore(tmp.name))
        task2 = other_uow.tasks.get(task_id)
        assert task2.aggregate_id == task_id
        assert len(task2.events) == 2
        assert task2.title == "Something else"


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


def test_uow_rollback():
    with tempfile.NamedTemporaryFile() as tmp:
        subject = UoW(FileEventStore(tmp.name))

        task = Task.create(title="Do it", description="Long text of what to do.")
        task_id = task.aggregate_id
        subject.tasks.add(task)

        subject.commit()

        task.update_title(title="Something else")
        subject.rollback()

        task = subject.tasks.get(task_id)
        assert task.aggregate_id == task_id
        assert task.title == "Do it"
        assert len(task.events) == 1

        del task
        del subject

        # Just reading from disk now
        other_uow = UoW(FileEventStore(tmp.name))
        task2 = other_uow.tasks.get(task_id)
        assert task2.aggregate_id == task_id
        assert task2.title == "Do it"
        assert len(task2.events) == 1


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


def test_read_model():
    with tempfile.NamedTemporaryFile() as tmp_events, tempfile.NamedTemporaryFile() as tmp_db:
        event_store = FileEventStore(tmp_events.name)
        uow = UoW(event_store)
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

        for _ in range(500):
            task2 = Task.create(title="Do it", description="Long text.")
            uow.tasks.add(task2)
        uow.commit()

        # readmodel.update_from(event_store)
        # assert len(list(readmodel.get_task_titles())) == 10002
