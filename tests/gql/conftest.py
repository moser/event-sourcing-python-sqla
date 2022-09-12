import pytest
from unittest import mock


@pytest.fixture
def inject_mock_services():
    from nextversion import services

    with mock.patch("nextversion.entrypoints.get_injectable_service") as service_getter:
        by_cls = {
            services.BankAccountService: mock.Mock(spec=services.BankAccountService)
        }
        service_getter.side_effect = lambda cls, uow: by_cls[cls]
        yield by_cls
