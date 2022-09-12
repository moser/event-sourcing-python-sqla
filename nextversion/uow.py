import uuid
from typing import Generic, TypeVar, Type, Iterable
from . import domain as _domain
from .eventstore import EventStore

AggregateType = TypeVar("A", bound=_domain.Aggregate)


class Repo(Generic[AggregateType]):
    def __init__(self, uow: "UoW"):
        self._uow = uow
        self._reset_transaction()

    def _reset_transaction(self):
        self._identity_map = {}
        self._committed_sequence_ids = {}

    def add(self, aggregate: AggregateType):
        # assert aggregate_id not there
        assert not self.exists(aggregate.aggregate_id)
        self._identity_map[aggregate.aggregate_id] = aggregate
        self._committed_sequence_ids[aggregate.aggregate_id] = -1

    def get(self, aggregate_id: uuid.UUID) -> AggregateType:
        if aggregate_id in self._identity_map:
            return self._identity_map[aggregate_id]
        aggregate = self._get_aggregate_class().reconstruct(
            self._get_events_for(aggregate_id)
        )
        aggregate.validate()
        self._identity_map[aggregate_id] = aggregate
        self._committed_sequence_ids[aggregate_id] = aggregate.current_sequence_id
        return aggregate

    def exists(self, aggregate_id: uuid.UUID) -> bool:
        for event in self._get_events_for(aggregate_id):
            return True

    def _get_aggregate_class(self) -> Type[AggregateType]:
        raise NotImplementedError

    def _get_events_for(self, aggregate_id: uuid.UUID) -> Iterable[_domain.Event]:
        if aggregate_id in self._identity_map:
            yield from self._identity_map[aggregate_id].events
        else:
            yield from self._uow.get_events(
                aggregate_id, self._get_aggregate_class().event_classes
            )

    def get_committable_events(self) -> Iterable[_domain.Event]:
        for aggregate in self._identity_map.values():
            committed_sequence_id = self._committed_sequence_ids[aggregate.aggregate_id]
            for event in aggregate.events:
                if event.sequence_id > committed_sequence_id:
                    yield event


class BankAccountRepo(Repo):
    def _get_aggregate_class(self):
        return _domain.BankAccount


class UoW:
    def __init__(self, event_store: EventStore):
        self._event_store = event_store
        self.bank_accounts = BankAccountRepo(self)
        self.repos = [self.bank_accounts]

    def get_events(
        self, aggregate_id: uuid.UUID, types: Iterable[Type]
    ) -> Iterable[_domain.Event]:
        return self._event_store.get_events(aggregate_id, types)

    def rollback(self):
        for repo in self.repos:
            repo._reset_transaction()

    def commit(self):
        logical_time = -1
        events = []
        locks = set()
        for repo in self.repos:
            for event in repo.get_committable_events():
                events.append(event)
                locks.add((repo._get_aggregate_class().__name__, event.aggregate_id))
            repo._reset_transaction()
        with self._event_store.transaction():
            # acquire locks to make sure the global logical_time on the event
            # store is consistent with the sequence_ids etc
            # https://softwaremill.com/implementing-event-sourcing-using-a-relational-database/
            # TODO add more locks iff required by projections
            for object_type, pk in sorted(locks):
                self._event_store.acquire_lock(object_type, pk)
            for event in events:
                logical_time = self._event_store.write(event)
        return logical_time
