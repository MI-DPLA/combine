
import django
import os
import pytest
import sys
import time

# logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# setup django environment
# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings

# import core
from core.models import *


# globals
livy_session = None


class TestLivySessionStart(object):


	def test_livy_start_session(self):

		'''
		Test Livy session can be started
		'''

		# get global livy session handle
		global livy_session

		# start livy session
		livy_session = LivySession()
		livy_session.start_session()

		# poll until session idle, limit to 60 seconds
		for x in range(0,60):

			# pause
			time.sleep(1)
			
			# refresh session
			livy_session.refresh_from_livy()
			logger.info(livy_session.status)
			
			# check status
			if livy_session.status != 'idle':
				continue
			else:
				break
		
		assert livy_session.status == 'idle'



class TestSparkJobHarvest(object):


	def test_static_harvest(self):

		'''
		Test static harvest of XML records from disk
		'''

		assert True






class TestLivySessionStop(object):


	def test_livy_stop_session(self):

		'''
		Test Livy session can be stopped
		'''

		# get global livy session handle
		global livy_session

		# attempt stop
		livy_session.stop_session()

		# poll until session idle, limit to 60 seconds
		for x in range(0,60):

			# pause
			time.sleep(1)
			
			# refresh session
			livy_session.refresh_from_livy()
			logger.info(livy_session.status)
			
			# check status
			if livy_session.status != 'gone':
				continue
			else:
				livy_session.delete()
				break

		# aseert
		assert livy_session.status == 'gone'






