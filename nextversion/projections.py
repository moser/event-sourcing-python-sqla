import functools, dataclasses
import datetime as dt
from typing import Optional, ClassVar, Type, Tuple, Union, Iterable
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


class TaskProjection(Projection):
    consumer_key = "tasks"
    EVENT_CLASSES = (
        domain.TaskCreated,
        domain.TaskTitleChanged,
        domain.TaskCompleted,
        domain.TaskAssigned,
        domain.TaskUnassigned,
    )

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


TaskHousekeeping = TaskProjection.create_housekeeping_class(mapper_registry)


class TaskHistory(Base):
    __tablename__ = "task_history"
    id = sa.Column(sa.Integer, sa.Sequence("task_history_pk"), primary_key=True)
    task_id = sa.Column(_pgsql.UUID(as_uuid=True))
    observed_at = sa.Column(sa.DateTime())
    change = sa.Column(_pgsql.JSON)


@dataclasses.dataclass
class TaskHistoryEntry:
    observed_at: dt.datetime
    change_type: str
    value: Optional[Union[str, dict]]


class TaskHistoryProjection(Projection):
    consumer_key = "task_history"
    EVENT_CLASSES = (domain.TaskTitleChanged, domain.TaskCompleted)

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


TaskHistoryHousekeeping = TaskHistoryProjection.create_housekeeping_class(
    mapper_registry
)
