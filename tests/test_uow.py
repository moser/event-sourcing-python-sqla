import uuid
from nextversion.eventstore import DBEventStore
from nextversion.uow import UoW
from nextversion import domain


def test_uow(events_db_engine):
    event_store = DBEventStore(events_db_engine)
    subject = UoW(event_store)
    project = domain.Project.create(name="aaa")
    subject.projects.add(project)
    task = domain.Task.create(
        title="Do it",
        description="Long text of what to do.",
        project_id=project.aggregate_id,
    )
    task_id = task.aggregate_id
    subject.tasks.add(task)
    subject.commit()

    task = subject.tasks.get(task_id)
    task.update_title(title="Something else")
    task.assign(assignee_id=uuid.uuid4())
    subject.commit()

    del task
    del subject

    # Just reading from DB
    other_uow = UoW(DBEventStore(events_db_engine))
    task2 = other_uow.tasks.get(task_id)
    assert task2.aggregate_id == task_id
    print(task2.events)
    assert len(task2.events) == 3
    assert task2.title == "Something else"
