import pytest


# use active livy
def pytest_addoption(parser):
    parser.addoption('--use_active_livy', action="store_true")
    parser.addoption('--keep_records', action="store_true")


@pytest.fixture
def use_active_livy(request):
    return request.config.getoption("--use_active_livy")


@pytest.fixture
def keep_records(request):
    return request.config.getoption("--keep_records")
