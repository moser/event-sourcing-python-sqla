import logging
from . import consumers as _consumers
from . import projections as _projections
from . import eventstore as _eventstore
from . import domain as _domain
from . import _consumer_db_orm


class ConsumerGroup:
    def __init__(self, consumers: list[_consumers.Consumer]):
        self._consumers = consumers

    def run_once(self):
        from . import _db

        session = _db.get_session()
        event_store = _eventstore.DBEventStore(
            session, _consumer_db_orm.events_table, _domain.EVENT_CLASSES
        )

        for consumer in self._consumers:
            try:
                consumer(session).consume(event_store)
            except Exception:
                logging.exception("Error while updating consumer")
        session.close()

    def run_forever(self):
        import time

        while True:
            try:
                self.run_once()
            except Exception:
                pass
            time.sleep(0.1)


PROJECTIONS = ConsumerGroup([_projections.BankAccountProjection])
