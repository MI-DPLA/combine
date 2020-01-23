# logging
import logging
import os
import sys

import django
import pytest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()

from django.contrib.auth.models import User


# use active livy
def pytest_addoption(parser):
    parser.addoption('--keep_records', action="store_true")


@pytest.fixture
def keep_records(request):
    return request.config.getoption("--keep_records")


# global variables object "VO"
class Vars():
    '''
    Object to capture and store variables used across tests
    '''

    def __init__(self):
        # debug
        self.ping = 'pong'

        # combine user
        self.user = User.objects.filter(username='combine').first()


_VO = Vars()


@pytest.fixture
def VO(request):
    return _VO
