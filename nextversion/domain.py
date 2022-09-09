import functools
import dataclasses
import uuid
from typing import Type, Iterable, Protocol, Optional


class Event(Protocol):
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID


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


@dataclasses.dataclass(frozen=True)
class TaskCreated(Event):
    title: str
    description: str
    project_id: uuid.UUID
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


TASK_EVENT_TYPES = [
    TaskCreated,
    TaskTitleChanged,
    TaskCompleted,
    TaskAssigned,
    TaskUnassigned,
]


class Task(Aggregate):
    title: str
    description: str
    completed: bool
    project_id: uuid.UUID
    assignee_id: Optional[uuid.UUID]

    def __init__(self):
        super().__init__()

    def validate(self):
        self._validate_types(
            "aggregate_id", "title", "description", "completed", "project_id"
        )
        assert len(self.title) >= 3

    @classmethod
    def create(cls, title: str, description: str, project_id: uuid.UUID) -> "Task":
        obj = cls()
        obj.apply(
            TaskCreated(
                sequence_id=0,
                aggregate_id=uuid.uuid4(),
                title=title,
                description=description,
                project_id=project_id,
            )
        )
        return obj

    @functools.singledispatchmethod
    def _apply(self, event: Event):
        raise NotImplementedError

    @_apply.register
    def _apply_task_created(self, event: TaskCreated):
        self.aggregate_id = event.aggregate_id
        self.project_id = event.project_id
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


@dataclasses.dataclass(frozen=True)
class ProjectCreated(Event):
    name: str
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


PROJECT_EVENT_TYPES = [ProjectCreated]


class Project(Aggregate):
    name: str

    def validate(self):
        self._validate_types("aggregate_id", "name")

    @classmethod
    def create(cls, name: str) -> "Project":
        obj = cls()
        obj.apply(
            ProjectCreated(
                sequence_id=0,
                aggregate_id=uuid.uuid4(),
                name=name,
            )
        )
        return obj

    @functools.singledispatchmethod
    def _apply(self, event: Event):
        raise NotImplementedError

    @_apply.register
    def _apply_project_created(self, event: ProjectCreated):
        self.aggregate_id = event.aggregate_id
        self.name = event.name


EVENT_TYPES = [*TASK_EVENT_TYPES]
