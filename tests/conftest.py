import pytest


# use active livy
def pytest_addoption(parser):
	parser.addoption('--keep_records', action="store_true")


@pytest.fixture
def keep_records(request):
	return request.config.getoption("--keep_records")

