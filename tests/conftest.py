import pytest

from tests.fakes.broker import FakeBroker, install


@pytest.fixture
def broker(monkeypatch) -> FakeBroker:
    fake = FakeBroker()
    install(monkeypatch, fake)
    return fake
