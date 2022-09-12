import functools
import dataclasses
import uuid
from typing import Type, Iterable, Protocol, Optional, ClassVar, Tuple


class Event(Protocol):
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID


class Aggregate:
    aggregate_id: uuid.UUID
    _events: list[Event]
    event_classes: ClassVar[Tuple[Type[Event]]] = ()

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
class BankAccountCreated(Event):
    name: str
    aggregate_id: uuid.UUID
    sequence_id: int
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


class BankAccount(Aggregate):
    name: str
    event_classes: ClassVar[Tuple[Type[Event]]] = (BankAccountCreated,)

    def validate(self):
        self._validate_types("aggregate_id", "name")
        assert len(self.name) >= 3

    @classmethod
    def create(cls, name: str):
        obj = cls()
        obj.apply(
            BankAccountCreated(name=name, aggregate_id=uuid.uuid4(), sequence_id=0)
        )
        return obj

    @functools.singledispatchmethod
    def _apply(self, event: Event):
        raise NotImplementedError

    @_apply.register
    def _apply_created(self, event: BankAccountCreated):
        self.aggregate_id = event.aggregate_id
        self.name = event.name


EVENT_CLASSES = {
    cls.__name__: cls for agg in [BankAccount] for cls in agg.event_classes
}
