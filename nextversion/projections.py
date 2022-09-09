import functools, dataclasses
import datetime as dt
from typing import Optional, ClassVar, Type, Tuple, Union, Iterable
import uuid
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.dialects import postgresql as _pgsql
from . import eventstore
from . import domain


def create_housekeeping_class(mapper_registry, projection_key: str):
    tbl = sa.Table(
        f"{projection_key}_housekeeping",
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

    cls = type(f"Housekeeping_{projection_key}", (), {})

    mapper_registry.map_imperatively(cls, tbl)
    return cls


class Projection:
    EVENT_CLASSES: ClassVar[Tuple[Type[domain.Event]]] = ()

    def __init__(self, engine, housekeeping_class):
        self.engine = engine
        Base.metadata.create_all(engine)
        self.session = orm.Session(engine)
        self.housekeeping = self.session.get(housekeeping_class, 1)
        if not self.housekeeping:
            self.housekeeping = housekeeping_class(
                last_seen_logical_time=-1,
                last_seen_observed_at=dt.datetime(1970, 1, 1),
                observed_event_count=0,
            )
            self.session.add(self.housekeeping)

    def logical_time_seen(self, logical_time: int):
        self.session.refresh(self.housekeeping)
        return self.housekeeping.last_seen_logical_time >= logical_time

    def update_from_events(self, event_store: eventstore.EventStore):
        for recorded_event in event_store.get_events_starting_at(
            logical_time=self.housekeeping.last_seen_logical_time,
            types=self.EVENT_CLASSES,
        ):
            self._apply(recorded_event.event, recorded_event)
            self.housekeeping.last_seen_logical_time = recorded_event.logical_time
            self.housekeeping.observed_event_count += 1
        self.session.commit()

    def _apply(self, event: domain.Event, recorded_event: eventstore.RecordedEvent):
        raise NotImplementedError


mapper_registry = orm.registry()
Base = mapper_registry.generate_base()


class TaskStatsForProject(Base):
    __tablename__ = "task_stats_for_project"
    project_id = sa.Column(_pgsql.UUID(as_uuid=True), primary_key=True)
    completed_task_count = sa.Column(sa.Integer)
    task_count = sa.Column(sa.Integer)
    unassigned_task_count = sa.Column(sa.Integer)


class Task(Base):
    __tablename__ = "tasks"
    id = sa.Column(_pgsql.UUID(as_uuid=True), primary_key=True)
    title = sa.Column(sa.String(500))
    completed = sa.Column(sa.Boolean)
    project_id = sa.Column(
        _pgsql.UUID(as_uuid=True), sa.ForeignKey(TaskStatsForProject.project_id)
    )
    project_stats = orm.relationship(TaskStatsForProject)

    assignee_id = sa.Column(_pgsql.UUID(as_uuid=True))


TaskHousekeeping = create_housekeeping_class(mapper_registry, "tasks")


class TaskProjection(Projection):
    EVENT_CLASSES = (
        domain.TaskCreated,
        domain.TaskTitleChanged,
        domain.TaskCompleted,
        domain.TaskAssigned,
        domain.TaskUnassigned,
    )

    def __init__(self, engine):
        super().__init__(engine=engine, housekeeping_class=TaskHousekeeping)

    def get(self, task_id: uuid.UUID) -> Optional[Task]:
        return self.session.get(Task, task_id)

    def all(self, page=1, page_size=20, order_by="title"):
        return (
            self.session.query(Task)
            .order_by(getattr(Task, order_by))
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

    def get_project_task_stats(
        self, project_id: uuid.UUID
    ) -> Optional[TaskStatsForProject]:
        return self.session.get(TaskStatsForProject, project_id)

    def all_project_task_stats(
        self,
    ) -> Iterable[TaskStatsForProject]:
        return (
            self.session.query(TaskStatsForProject)
            .order_by(TaskStatsForProject.project_id)
            .all()
        )

    @functools.singledispatchmethod
    def _apply(self, event: domain.Event, recorded_event: eventstore.RecordedEvent):
        raise NotImplementedError

    @_apply.register
    def _apply_task_created(
        self, event: domain.TaskCreated, recorded_event: eventstore.RecordedEvent
    ):
        project_stats = self.session.get(TaskStatsForProject, event.project_id)
        if not project_stats:
            project_stats = TaskStatsForProject(
                project_id=event.project_id,
                completed_task_count=0,
                task_count=0,
                unassigned_task_count=0,
            )
        project_stats.task_count += 1
        project_stats.unassigned_task_count += 1
        self.session.add(
            Task(
                id=event.aggregate_id,
                title=event.title,
                completed=False,
                project_stats=project_stats,
            )
        )

    @_apply.register
    def _apply_task_title_updated(
        self, event: domain.TaskTitleChanged, recorded_event: eventstore.RecordedEvent
    ):
        task = self.session.get(Task, event.aggregate_id)
        task.title = event.title

    @_apply.register
    def _apply_task_completed(
        self, event: domain.TaskCompleted, recorded_event: eventstore.RecordedEvent
    ):
        task = self.session.get(Task, event.aggregate_id)
        task.completed = True
        task.project_stats.completed_task_count += 1

    @_apply.register
    def _apply_task_assigned(
        self, event: domain.TaskAssigned, recorded_event: eventstore.RecordedEvent
    ):
        task = self.session.get(Task, event.aggregate_id)
        task.assignee_id = event.assignee_id
        task.project_stats.unassigned_task_count -= 1

    @_apply.register
    def _apply_task_unassigned(
        self, event: domain.TaskUnassigned, recorded_event: eventstore.RecordedEvent
    ):
        task = self.session.get(Task, event.aggregate_id)
        task.assignee_id = None
        task.project_stats.unassigned_task_count += 1


class TaskHistory(Base):
    __tablename__ = "task_history"
    id = sa.Column(sa.Integer, sa.Sequence("task_history_pk"), primary_key=True)
    task_id = sa.Column(_pgsql.UUID(as_uuid=True))
    observed_at = sa.Column(sa.DateTime())
    change = sa.Column(_pgsql.JSON)


TaskHistoryHousekeeping = create_housekeeping_class(mapper_registry, "task_history")


@dataclasses.dataclass
class TaskHistoryEntry:
    observed_at: dt.datetime
    change_type: str
    value: Optional[Union[str, dict]]


class TaskHistoryProjection(Projection):
    EVENT_CLASSES = (domain.TaskTitleChanged, domain.TaskCompleted)

    def __init__(self, engine):
        super().__init__(engine=engine, housekeeping_class=TaskHistoryHousekeeping)

    def get(self, task_id: uuid.UUID) -> Iterable[TaskHistoryEntry]:
        for entry in (
            self.session.query(TaskHistory)
            .where(TaskHistory.task_id == task_id)
            .order_by(TaskHistory.observed_at)
        ):
            yield TaskHistoryEntry(
                observed_at=entry.observed_at,
                change_type=entry.change["type"],
                value=entry.change.get("value"),
            )

    @functools.singledispatchmethod
    def _apply(self, event: domain.Event, recorded_event: eventstore.RecordedEvent):
        raise NotImplementedError

    @_apply.register
    def _apply_task_title_updated(
        self, event: domain.TaskTitleChanged, recorded_event: eventstore.RecordedEvent
    ):
        self.session.add(
            TaskHistory(
                task_id=event.aggregate_id,
                observed_at=recorded_event.observed_at,
                change=dict(type="ChangeTitle", value=event.title),
            )
        )

    @_apply.register
    def _apply_task_completed(
        self, event: domain.TaskCompleted, recorded_event: eventstore.RecordedEvent
    ):
        self.session.add(
            TaskHistory(
                task_id=event.aggregate_id,
                observed_at=recorded_event.observed_at,
                change=dict(type="Completed"),
            )
        )
