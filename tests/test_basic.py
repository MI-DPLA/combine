
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



# global variables object "VO"
class Vars(object):

	'''
	Object to capture variables used across tests
	'''

	def __init__(self):

		# combine user
		self.user = User.objects.filter(username='combine').first()

VO = Vars()


#############################################################################
# Tests Setup
#############################################################################
def test_livy_start_session(use_active_livy):

	'''
	Test Livy session can be started
	'''

	# get vars
	global VO

	# if use active livy
	if use_active_livy:
		VO.livy_session = LivySession.get_active_session()

	# create livy session
	else:
		# start livy session
		VO.livy_session = LivySession()
		VO.livy_session.start_session()

		# poll until session idle, limit to 60 seconds
		for x in range(0,60):

			# pause
			time.sleep(1)
			
			# refresh session
			VO.livy_session.refresh_from_livy()
			logger.info(VO.livy_session.status)
			
			# check status
			if VO.livy_session.status != 'idle':
				continue
			else:
				break

	# assert
	assert VO.livy_session.status == 'idle'


def test_organization_create():

	'''
	Test creation of organization
	'''

	# get vars
	global VO

	# instantiate and save
	VO.org = Organization(
		name='test_org',
		description='',
		publish_id='test_org_pub_id'
	)
	VO.org.save()
	assert type(VO.org.id) == int


def test_record_group_create():

	'''
	Test creation of record group
	'''

	# get vars
	global VO

	# instantiate and save
	VO.rg = RecordGroup(
		organization=VO.org,
		name='test_record_group',
		description='',
		publish_set_id='test_record_group_pub_id'
	)
	VO.rg.save()
	assert type(VO.rg.id) == int



#############################################################################
# Test Harvest
#############################################################################
def test_static_harvest():

	'''
	Test static harvest of XML records from disk
	'''

	# get vars
	global VO

	# build payload dictionary
	payload_dict = {
		'type':'location',
		'payload_dir':'/home/combine/test/feeding_america_cookbooks',
		'xpath_document_root':'/mods:mods',
		'xpath_record_id':''
	}

	# initiate job
	cjob = HarvestStaticXMLJob(			
		job_name='test_static_harvest',
		job_note='',
		user=VO.user,
		record_group=VO.rg,
		index_mapper='GenericMapper',
		payload_dict=payload_dict
	)

	# start job and update status
	job_status = cjob.start_job()

	# if job_status is absent, report job status as failed
	if job_status == False:
		cjob.job.status = 'failed'
		cjob.job.save()

	# poll until complete
	for x in range(0,120):

		# pause
		time.sleep(1)
		
		# refresh session
		cjob.job.update_status()
		
		# check status
		if cjob.job.status != 'available':
			continue
		else:
			break

	# save static harvest job to VO
	VO.static_harvest_cjob = cjob

	# assert
	assert VO.static_harvest_cjob.job.status == 'available'



#############################################################################
# Test Transform
#############################################################################
def test_static_transform():

	'''
	Test static harvest of XML records from disk
	'''

	# get vars
	global VO

	# initiate job
	cjob = TransformJob(
		job_name='test_static_transform_job',
		job_note='',
		user=VO.user,
		record_group=VO.rg,
		input_job=VO.static_harvest_cjob.job,
		transformation=Transformation.objects.get(pk=1),
		index_mapper='GenericMapper'
	)
	
	# start job and update status
	job_status = cjob.start_job()

	# if job_status is absent, report job status as failed
	if job_status == False:
		cjob.job.status = 'failed'
		cjob.job.save()

	# poll until complete
	for x in range(0,120):

		# pause
		time.sleep(1)
		
		# refresh session
		cjob.job.update_status()
		
		# check status
		if cjob.job.status != 'available':
			continue
		else:
			break

	# save static harvest job to VO
	VO.static_transform_cjob = cjob

	# assert
	assert VO.static_transform_cjob.job.status == 'available'



#############################################################################
# Tests Setup
#############################################################################
def test_org_delete(keep_records):

	'''
	Test removal of organization with cascading deletes
	'''

	# get vars
	global VO

	# assert delete of org and children
	if not keep_records:
		assert VO.org.delete()[0] > 0
	else:
		assert True


def test_livy_stop_session(use_active_livy):

	'''
	Test Livy session can be stopped
	'''

	# get vars
	global VO

	if use_active_livy:
		assert True
	
	# stop livy session used for testing
	else:
		# attempt stop
		VO.livy_session.stop_session()

		# poll until session idle, limit to 60 seconds
		for x in range(0,60):

			# pause
			time.sleep(1)
			
			# refresh session
			VO.livy_session.refresh_from_livy()
			logger.info(VO.livy_session.status)
			
			# check status
			if VO.livy_session.status != 'gone':
				continue
			else:
				VO.livy_session.delete()
				break

		# assert
		assert VO.livy_session.status == 'gone'






