
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
	Object to capture and store variables used across tests
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

	# if use active livy
	if use_active_livy:
		VO.livy_session = LivySession.get_active_session()
		VO.livy_session.refresh_from_livy()

	# create livy session
	else:
		# start livy session
		VO.livy_session = LivySession()
		VO.livy_session.start_session()

		# poll until session idle, limit to 60 seconds
		for x in range(0,240):

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

	# instantiate and save
	VO.org = Organization(
		name='test_org_%s' % uuid.uuid4().hex,
		description=''
	)
	VO.org.save()
	assert type(VO.org.id) == int


def test_record_group_create():

	'''
	Test creation of record group
	'''

	# instantiate and save
	VO.rg = RecordGroup(
		organization=VO.org,
		name='test_record_group_%s' % uuid.uuid4().hex,
		description='',
		publish_set_id='test_record_group_pub_id'
	)
	VO.rg.save()
	assert type(VO.rg.id) == int



#############################################################################
# Test Harvest
#############################################################################
def prepare_records():

	'''
	Unzip 250 MODS records to temp location, feed to test_static_harvest()
	'''

	# parse file
	xml_tree = etree.parse('tests/data/mods_250.xml')
	xml_root = xml_tree.getroot()
	
	# get namespaces
	nsmap = {}
	for ns in xml_root.xpath('//namespace::*'):
		if ns[0]:
			nsmap[ns[0]] = ns[1]

	# find mods records
	mods_roots = xml_root.xpath('//mods:mods', namespaces=nsmap)

	# create temp dir
	payload_dir = '/tmp/%s' % uuid.uuid4().hex
	os.makedirs(payload_dir)

	# write MODS to temp dir
	for mods in mods_roots:
		with open(os.path.join(payload_dir, '%s.xml' % uuid.uuid4().hex), 'w') as f:
			f.write(etree.tostring(mods).decode('utf-8'))

	# return payload dir
	return payload_dir


def test_static_harvest():

	'''
	Test static harvest of XML records from disk
	'''

	# prepare test data
	payload_dir = prepare_records()

	# build payload dictionary
	payload_dict = {
		'type':'location',
		'payload_dir':payload_dir,
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
	for x in range(0,240):

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

	# remove payload_dir
	shutil.rmtree(payload_dir)

	# assert job is done and available via livy
	assert VO.static_harvest_cjob.job.status == 'available'

	# assert record count is 250
	dcount = VO.static_harvest_cjob.get_detailed_job_record_count()
	assert dcount['records'] == 250
	assert dcount['errors'] == 0

	# assert no indexing failures
	assert len(VO.static_harvest_cjob.get_indexing_failures()) == 0



#############################################################################
# Test Transform
#############################################################################
def prepare_transform():

	'''
	Create temporary transformation scenario based on tests/data/mods_transform.xsl
	'''

	with open('tests/data/mods_transform.xsl','r') as f:
		xsl_string = f.read()
	trans = Transformation(
		name='temp_mods_transformation',
		payload=xsl_string,
		transformation_type='xslt',
		filepath='will_be_updated'
	)	
	trans.save()

	# return transformation
	return trans


def test_static_transform():

	'''
	Test static harvest of XML records from disk
	'''

	# prepare and capture temporary transformation scenario
	VO.transformation_scenario = prepare_transform()

	# initiate job
	cjob = TransformJob(
		job_name='test_static_transform_job',
		job_note='',
		user=VO.user,
		record_group=VO.rg,
		input_job=VO.static_harvest_cjob.job,
		transformation=VO.transformation_scenario,
		index_mapper='GenericMapper'
	)
	
	# start job and update status
	job_status = cjob.start_job()

	# if job_status is absent, report job status as failed
	if job_status == False:
		cjob.job.status = 'failed'
		cjob.job.save()

	# poll until complete
	for x in range(0,240):

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

	# assert job is done and available via livy
	assert VO.static_transform_cjob.job.status == 'available'

	# assert record count is 250
	dcount = VO.static_transform_cjob.get_detailed_job_record_count()
	assert dcount['records'] == 250
	assert dcount['errors'] == 0

	# assert no indexing failures
	assert len(VO.static_transform_cjob.get_indexing_failures()) == 0

	# remove transformation
	assert VO.transformation_scenario.delete()[0] > 0



#############################################################################
# Test Validation Scenarios
#############################################################################
def test_add_schematron_validation_scenario():

	'''
	Add schematron validation
	'''

	# get schematron validation from test data
	with open('tests/data/schematron_validation.sch','r') as f:
		sch_payload = f.read()

	# init new validation scenario
	schematron_validation_scenario = ValidationScenario(
		name='temp_vs_%s' % str(uuid.uuid4()),
		payload=sch_payload,
		validation_type='sch',
		default_run=False
	)
	schematron_validation_scenario.save()

	# pin to VO
	VO.schematron_validation_scenario = schematron_validation_scenario

	# assert creation
	assert type(VO.schematron_validation_scenario.id) == int


def test_add_python_validation_scenario():

	'''
	Add python code snippet validation
	'''

	# get python validation from test data
	with open('tests/data/python_validation.py','r') as f:
		py_payload = f.read()

	# init new validation scenario
	python_validation_scenario = ValidationScenario(
		name='temp_vs_%s' % str(uuid.uuid4()),
		payload=py_payload,
		validation_type='python',
		default_run=False
	)
	python_validation_scenario.save()

	# pin to VO
	VO.python_validation_scenario = python_validation_scenario

	# assert creation
	assert type(VO.python_validation_scenario.id) == int


def test_schematron_validation():

	# get target records
	VO.harvest_record = VO.static_harvest_cjob.job.get_records().first()
	VO.transform_record = VO.static_transform_cjob.job.get_records().first()

	# validate harvest record with schematron
	'''
	expecting failure count of 2
	'''
	vs_results = VO.schematron_validation_scenario.validate_record(VO.harvest_record)
	assert vs_results['parsed']['fail_count'] == 2

	# validate transform record with schematron
	'''
	expecting failure count of 1
	'''
	vs_results = VO.schematron_validation_scenario.validate_record(VO.transform_record)
	assert vs_results['parsed']['fail_count'] == 1


def test_python_validation():

	# validate harvest record with python
	'''
	expecting failure count of 1
	'''
	vs_results = VO.python_validation_scenario.validate_record(VO.harvest_record)
	print(vs_results)
	assert vs_results['parsed']['fail_count'] == 1

	# validate transform record with python
	'''
	expecting failure count of 1
	'''
	vs_results = VO.python_validation_scenario.validate_record(VO.transform_record)
	print(vs_results)
	assert vs_results['parsed']['fail_count'] == 1



#############################################################################
# Test Duplicate/Merge Job
#############################################################################
def test_duplicate():

	'''
	Duplicate Transform job, applying newly created validation scenarios
	'''

	# initiate job
	cjob = MergeJob(
		job_name='test_merge_job_with_validation',
		job_note='',
		user=VO.user,
		record_group=VO.rg,
		input_jobs=[VO.static_transform_cjob.job],
		index_mapper='GenericMapper',
		validation_scenarios=[VO.schematron_validation_scenario.id, VO.python_validation_scenario.id]
	)
	
	# start job and update status
	job_status = cjob.start_job()

	# if job_status is absent, report job status as failed
	if job_status == False:
		cjob.job.status = 'failed'
		cjob.job.save()

	# poll until complete
	for x in range(0,240):

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
	VO.merge_cjob = cjob

	# assert job is done and available via livy
	assert VO.merge_cjob.job.status == 'available'

	# assert record count is 250
	dcount = VO.merge_cjob.get_detailed_job_record_count()
	assert dcount['records'] == 250
	assert dcount['errors'] == 0

	# assert validation scenarios applied
	job_validation_scenarios = VO.merge_cjob.job.jobvalidation_set.all()
	assert job_validation_scenarios.count() == 2

	# loop through validation scenarios and confirm that both show 250 failures
	for jv in job_validation_scenarios:
		assert jv.get_record_validation_failures().count() == 250

	# assert no indexing failures
	assert len(VO.merge_cjob.get_indexing_failures()) == 0



#############################################################################
# Tests Teardown
#############################################################################
def test_org_delete(keep_records):

	'''
	Test removal of organization with cascading deletes
	'''

	# assert delete of org and children
	if not keep_records:
		assert VO.org.delete()[0] > 0
	else:
		assert True


def test_validation_scenario_teardown():

	assert VO.schematron_validation_scenario.delete()[0] > 0
	assert VO.python_validation_scenario.delete()[0] > 0


def test_livy_stop_session(use_active_livy):

	'''
	Test Livy session can be stopped
	'''

	if use_active_livy:
		assert True
	
	# stop livy session used for testing
	else:
		# attempt stop
		VO.livy_session.stop_session()

		# poll until session idle, limit to 60 seconds
		for x in range(0,240):

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






