import uuid, textwrap
from nextversion.eventstore import DBEventStore
from nextversion.uow import UoW
from nextversion import domain
from nextversion import projections


def test_task_projection(events_db_engine, projections_db_engine):
    event_store = DBEventStore(events_db_engine)
    uow = UoW(event_store)

    project = domain.Project.create(name="aaa")
    uow.projects.add(project)
    task = domain.Task.create(
        title="Do it",
        description="Long text of what to do.",
        project_id=project.aggregate_id,
    )
    task_id = task.aggregate_id
    uow.tasks.add(task)
    uow.commit()

    task = uow.tasks.get(task.aggregate_id)
    task.update_title(title="Something else")
    task.assign(assignee_id=uuid.uuid4())
    uow.commit()

    uow.tasks.add(
        domain.Task.create(
            title="Do it again",
            description="Long text of what to do.",
            project_id=project.aggregate_id,
        )
    )
    uow.tasks.add(
        domain.Task.create(
            title="Buy groceries",
            description="Long text of what to do.",
            project_id=project.aggregate_id,
        )
    )
    uow.commit()

    tasks = projections.TaskProjection(engine=projections_db_engine)
    tasks.consume(event_store)

    task = uow.tasks.get(task.aggregate_id)
    task.complete()
    uow.tasks.add(
        domain.Task.create(
            title="Buy more groceries", description="", project_id=uuid.uuid4()
        )
    )
    uow.commit()

    tasks.consume(event_store)

    assert "\n".join(
        [str((t.title, t.completed)) for t in tasks.all()]
    ) == textwrap.dedent(
        """\
        ('Buy groceries', False)
        ('Buy more groceries', False)
        ('Do it again', False)
        ('Something else', True)"""
    )

    history = projections.TaskHistoryProjection(engine=projections_db_engine)
    history.consume(event_store)

    assert "\n".join(
        [str((t.change_type, t.value)) for t in history.get(task_id)]
    ) == textwrap.dedent(
        """\
        ('ChangeTitle', 'Something else')
        ('Completed', None)"""
    )
