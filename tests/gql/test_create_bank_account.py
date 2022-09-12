import uuid
import pytest
from nextversion.entrypoints import schema
from nextversion import services


def test_e2e(event_loop):
    result = event_loop.run_until_complete(
        schema.execute(
            """
        mutation x {
          createBankAccount(name: "aaa") {
            id
            newLogicalTime
          }
        }
        """
        )
    )
    assert result.errors is None
    assert result.data["createBankAccount"]["newLogicalTime"] >= 1


def test_interaction(event_loop, inject_mock_services):
    mocked = inject_mock_services[services.BankAccountService]
    mocked.create_account.return_value = services.IdResult(
        id=uuid.uuid4(), new_logical_time=123
    )
    result = event_loop.run_until_complete(
        schema.execute(
            """
        mutation x {
          createBankAccount(name: "aaa") {
            id
            newLogicalTime
          }
        }
        """
        )
    )
    mocked.create_account.assert_called_with("aaa")
