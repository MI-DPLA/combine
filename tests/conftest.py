import pytest

def pytest_addoption(parser):	
	parser.addoption('--use_active_livy', action="store_true")

@pytest.fixture
def use_active_livy(request):
	return request.config.getoption("--use_active_livy")

