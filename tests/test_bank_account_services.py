from nextversion.services import BankAccountService


def test_create_account(uow):
    subject = BankAccountService(uow)
    result = subject.create_account("Some person")
    print(result)
    assert result

    account = uow.bank_accounts.get(result.id)
    assert account.name == "Some person"
    assert list(uow.get_events(result.id, []))
