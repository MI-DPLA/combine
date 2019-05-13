
import django
from lxml import etree
import os
import pytest
import shutil
import sys
import time
import uuid

# logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings

# import core
from core.models import *


#############################################################################
# Background Tasks
#############################################################################










#############################################################################
# DEBUG
#############################################################################
@pytest.mark.run(order=1)
def test_goobertronic(VO):

	logger.warn(VO.ping)
	assert True