import sys
import sqlalchemy as sa

RO_USER = "readonly"
DB_NAME = "events"
PROJECTIONS_DB_NAME = "projections"


def _get_db_dsn(db_name, username="postgres"):
    return f"postgresql://{username}:docker@localhost:45432/{db_name}"


def db_setup():
    engine = sa.create_engine(_get_db_dsn("postgres"))
    conn = engine.connect()

    def create_clean_db(db_name):
        test_db_exists = (
            conn.execute(
                sa.text("select count(*) from pg_database where datname = :db_name"),
                dict(db_name=db_name),
            ).scalar()
            > 0
        )
        if test_db_exists:
            conn.execute("ROLLBACK")
            conn.execute(f"DROP DATABASE {db_name}")
        conn.execute("ROLLBACK")
        conn.execute(f"CREATE DATABASE {db_name}")

    create_clean_db(DB_NAME)
    create_clean_db(PROJECTIONS_DB_NAME)

    engine = sa.create_engine(_get_db_dsn(PROJECTIONS_DB_NAME))
    conn = engine.connect()
    conn.execute(f"DROP ROLE IF EXISTS {RO_USER}")
    conn.execute(f"CREATE ROLE {RO_USER} LOGIN PASSWORD 'docker'")
    conn.execute(f"GRANT CONNECT ON DATABASE {PROJECTIONS_DB_NAME} TO {RO_USER}")
    conn.execute(f"GRANT USAGE ON SCHEMA public TO {RO_USER}")
    conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {RO_USER}")
    conn.execute(f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {RO_USER}")
    conn.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {RO_USER}"
    )


def print_help():
    print("Usage")
    print("python -m nextversion.main <job>")
    print("Jobs: demo, db_setup, run_consumers")


def demo():
    import time, uuid
    from nextversion import uow, domain, eventstore, projections

    time_factor = 2

    unit = uow.UoW(eventstore.DBEventStore(sa.create_engine(_get_db_dsn(DB_NAME))))

    print("Creating a project")
    unit.projects.add(project := domain.Project.create(name="Project 1"))
    unit.commit()

    time.sleep(1 * time_factor)

    print("Creating a task")
    unit.tasks.add(
        task := domain.Task.create(
            project_id=project.aggregate_id, title="Buy groceries", description=""
        )
    )
    task.update_title("Quick, work on this")
    unit.commit()

    time.sleep(1 * time_factor)

    print("Creating an assinged task")
    unit.tasks.add(
        task := domain.Task.create(
            project_id=project.aggregate_id, title="Buy groceries", description=""
        )
    )
    task.assign(uuid.uuid4())
    unit.commit()

    time.sleep(1 * time_factor)

    print("Completing a task")
    task = unit.tasks.get(task.aggregate_id)
    task.complete()
    ltime = unit.commit()
    print("Completed a task", ltime)

    projections_engine = sa.create_engine(
        _get_db_dsn(PROJECTIONS_DB_NAME, username=RO_USER)
    )
    tasks = projections.TaskProjection(projections_engine)
    while not tasks.logical_time_seen(ltime):
        print("Waiting for consistency")
        time.sleep(0.5)
    print("Consistency reached")
    print(tasks.get(task.aggregate_id))


def run_consumers():
    import time
    from nextversion import eventstore, projections, consumers

    eventstore = eventstore.DBEventStore(sa.create_engine(_get_db_dsn(DB_NAME)))
    projections_engine = sa.create_engine(_get_db_dsn(PROJECTIONS_DB_NAME))

    tasks = projections.TaskProjection(projections_engine)
    history = projections.TaskHistoryProjection(projections_engine)
    send_emails = consumers.SendAssignmentNotificationConsumer(projections_engine)
    all_consumers = [tasks, history, send_emails]
    while True:
        print("\n" * 3)
        print("ltime", eventstore.current_logical_time)
        for consumer in all_consumers:
            consumer.consume(eventstore)

        for x in tasks.all_project_task_stats():
            print(x.project_id, x.task_count, x.completed_task_count)
        time.sleep(0.2)


def main():
    if len(sys.argv) < 2:
        print_help()
        return

    job = sys.argv[1]
    if job == "run_consumers":
        run_consumers()
    elif job == "demo":
        demo()
    elif job == "db_setup":
        db_setup()
    else:
        print_help()


if __name__ == "__main__":
    main()
