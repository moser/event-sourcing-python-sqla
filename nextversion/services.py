import dataclasses, uuid
from . import domain
from .uow import UoW


@dataclasses.dataclass
class IdResult:
    id: uuid.UUID
    new_logical_time: int


class BankAccountService:
    def __init__(self, uow: UoW):
        self.uow = uow

    def create_account(self, name: str):
        account = domain.BankAccount.create(name=name)
        self.uow.bank_accounts.add(account)
        ltime = self.uow.commit()
        return IdResult(id=account.aggregate_id, new_logical_time=ltime)
