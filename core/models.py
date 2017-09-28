# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import hashlib
import inspect
import json
import logging
import os
import requests
import shutil
import subprocess
import textwrap
import time

# django imports
from django.apps import AppConfig
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth import signals
from django.db import models
from django.dispatch import receiver
from django.utils.encoding import python_2_unicode_compatible

# Livy
from livy.client import HttpClient

# import cyavro
import cyavro

# impot pandas
import pandas as pd

# Get an instance of a logger
logger = logging.getLogger(__name__)



##################################
# Django ORM
##################################

class LivySession(models.Model):

	name = models.CharField(max_length=128)
	session_id = models.IntegerField()
	session_url = models.CharField(max_length=128)
	status = models.CharField(max_length=30, null=True)
	session_timestamp = models.CharField(max_length=128)
	appId = models.CharField(max_length=128, null=True)
	driverLogUrl = models.CharField(max_length=255, null=True)
	sparkUiUrl = models.CharField(max_length=255, null=True)
	active = models.BooleanField(default=0)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'Livy session: %s, status: %s' % (self.name, self.status)


	def refresh_from_livy(self):

		'''
		ping Livy for session status and update DB
		'''

		logger.debug('querying Livy for session status')

		# query Livy for session status
		livy_response = LivyClient().session_status(self.session_id)

		# parse response and set self values
		logger.debug(livy_response.status_code)
		response = livy_response.json()
		logger.debug(response)
		headers = livy_response.headers
		logger.debug(headers)

		# if status_code 404, set as gone
		if livy_response.status_code == 404:
			
			logger.debug('session not found, setting status to gone')
			self.status = 'gone'
			# update
			self.save()

		elif livy_response.status_code == 200:
			
			# update Livy information
			logger.debug('session found, updating status')
			
			# update status
			self.status = response['state']
			if self.status in ['starting','idle','busy']:
				self.active = True
			
			self.session_timestamp = headers['Date']
			
			# update Spark/YARN information, if available
			if 'appId' in response.keys():
				self.appId = response['appId']
			if 'appInfo' in response.keys():
				if 'driverLogUrl' in response['appInfo']:
					self.driverLogUrl = response['appInfo']['driverLogUrl']
				if 'sparkUiUrl' in response['appInfo']:
					self.sparkUiUrl = response['appInfo']['sparkUiUrl']
			# update
			self.save()

		else:
			
			logger.debug('error retrieving information about Livy session')


	def stop_session(self):

		'''
		Stop Livy session with Livy HttpClient
		'''

		# stop session
		LivyClient.stop_session(self.session_id)

		# update from Livy
		self.refresh_from_livy()


	@staticmethod
	def get_active_session():

		'''
		Convenience method to return single active livy session,
		or multiple if multiple exist
		'''

		active_livy_sessions = LivySession.objects.filter(active=True)

		if active_livy_sessions.count() == 1:
			return active_livy_sessions.first()

		elif active_livy_sessions.count() == 0:
			logger.debug('no active livy sessions found, returning False')
			return False

		elif active_livy_sessions.count() > 1:
			logger.debug('multiple active livy sessions found, returning as list')
			return active_livy_sessions



class Organization(models.Model):

	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255)
	publish_id = models.CharField(max_length=255)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'Organization: %s' % self.name
models.BooleanField(default=0)



class RecordGroup(models.Model):

	organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255, null=True, default=None)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'Record Group: %s' % self.name



class Job(models.Model):

	record_group = models.ForeignKey(RecordGroup, on_delete=models.CASCADE)
	user = models.ForeignKey(User, on_delete=models.CASCADE)
	name = models.CharField(max_length=128, null=True)
	spark_code = models.CharField(max_length=32000, null=True)
	job_id = models.IntegerField(null=True, default=None)
	status = models.CharField(max_length=30, null=True)
	finished = models.BooleanField(default=0)
	url = models.CharField(max_length=255, null=True)
	headers = models.CharField(max_length=255, null=True)
	response = models.TextField(null=True, default=None)
	job_output = models.TextField(null=True, default=None)
	record_count = models.IntegerField(null=True, default=0)
	published = models.BooleanField(default=0)
	job_details = models.TextField(null=True, default=None)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return '%s, Job #%s, from Record Group: %s' % (self.name, self.id, self.record_group.name)


	def refresh_from_livy(self):

		# query Livy for statement status
		livy_response = LivyClient().job_status(self.url)
		
		# if status_code 404, set as gone
		if livy_response.status_code == 400:
			
			logger.debug(livy_response.json())
			logger.debug('Livy session likely not active, setting status to gone')
			self.status = 'gone'
			# update
			self.save()

		# if status_code 404, set as gone
		if livy_response.status_code == 404:
			
			logger.debug('job/statement not found, setting status to gone')
			self.status = 'gone'
			# update
			self.save()

		elif livy_response.status_code == 200:

			# parse response
			response = livy_response.json()
			headers = livy_response.headers
			
			# update Livy information
			logger.debug('job/statement found, updating status')
			self.status = response['state']

			# if state is available, assume finished
			if self.status == 'available':
				self.finished = True

			# update
			self.save()

		else:
			
			logger.debug('error retrieving information about Livy job/statement')
			logger.debug(livy_response.status_code)
			logger.debug(livy_response.json())


	def get_output_as_dataframe(self):

		'''
		method to use cyavro and return job_output as dataframe
		'''

		# confirm there is output to work with
		if not self.job_output:
			logger.debug('job does not have output, returning False')
			return False

		# Filesystem
		if self.job_output.startswith('file://'):
			
			output_dir = self.job_output.split('file://')[-1]

			###########################################################################
			# cyavro shim
			###########################################################################
			'''
			cyavro currently will fail on avro files written by ingestion3:
			https://github.com/maxpoint/cyavro/issues/27

			This shim removes any files of length identical to known values.
			These files represent "empty" avro files, in that they only contain the schema.
			The file length varies slightly depending on what subset of the dataframe we 
			write to disk.
			'''
			files = [f for f in os.listdir(output_dir) if f.startswith('part-r')]
			for f in files:
				if os.path.getsize(os.path.join(output_dir,f)) in [1375, 520]:
					logger.debug('detected empty avro and removing: %s' % f)
					os.remove(os.path.join(output_dir,f))
			###########################################################################
			# end cyavro shim
			###########################################################################

			# open avro files as dataframe with cyavro and return
			stime = time.time()
			df = cyavro.read_avro_path_as_dataframe(output_dir)
			logger.debug('cyavro read time: %s' % (time.time() - stime))
			return df

		# HDFS
		elif self.job_output.startswith('hdfs://'):
			logger.debug('HDFS record counting not yet implemented')
			return False

		else:
			raise Exception('could not parse dataframe from job output: %s' % self.job_output)


	def update_record_count(self):

		'''
		count records from self.job_output, and update DB
		'''
		
		# get dataframe
		try:
			
			df = self.get_output_as_dataframe()

			# count and save records to DB
			self.record_count = df.document.count()
			self.save()

		except:
			
			logger.debug('could not load job output as dataframe')



class JobInput(models.Model):

	'''
	Provides a one-to-many relationship for a job and potential multiple input jobs
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	input_job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='input_job')



class OAIEndpoint(models.Model):

	name = models.CharField(max_length=255)
	endpoint = models.CharField(max_length=255)
	verb = models.CharField(max_length=128)
	metadataPrefix = models.CharField(max_length=128)
	scope_type = models.CharField(max_length=128) # expecting one of setList, whiteList, blackList
	scope_value = models.CharField(max_length=1024)


	def __str__(self):
		return 'OAI endpoint: %s' % self.name


class Transformation(models.Model):

	name = models.CharField(max_length=255)
	payload = models.TextField()
	transformation_type = models.CharField(max_length=255, choices=[('xslt','XSLT Stylesheet'),('python','Python Code Snippet')])
	

	def __str__(self):
		return 'Transformation: %s, transformation type: %s' % (self.name, self.transformation_type)



class Record(object):

	pass


##################################
# Signals Handlers
##################################

@receiver(signals.user_logged_in)
def user_login_handle_livy_sessions(sender, user, **kwargs):

	'''
	When user logs in, handle check for pre-existing sessions or creating
	'''

	# if superuser, skip
	if user.is_superuser:
		logger.debug("superuser detected, not initiating Livy session")
		return False

	# else, continune with user sessions
	else:
		logger.debug('Checking for pre-existing user sessions')

		# get "active" user sessions
		livy_sessions = LivySession.objects.filter(status__in=['starting','running','idle'])
		logger.debug(livy_sessions)

		# none found
		if livy_sessions.count() == 0:
			logger.debug('no Livy sessions found, creating')
			livy_session = LivySession().save()

		# if sessions present
		elif livy_sessions.count() == 1:
			logger.debug('single, active Livy session found, using')

		elif livy_sessions.count() > 1:
			logger.debug('multiple Livy sessions found, sending to sessions page to select one')


@receiver(models.signals.pre_save, sender=LivySession)
def create_livy_session(sender, instance, **kwargs):

	'''
	Before saving a LivySession instance, check if brand new, or updating status
		- if not self.id, assume new and create new session with POST
		- if self.id, assume checking status, only issue GET and update fields
	'''

	# not instance.id, assume new
	if not instance.id:

		logger.debug('creating new Livy session')

		# create livy session, get response
		livy_response = LivyClient().create_session()

		# parse response and set instance values
		response = livy_response.json()
		headers = livy_response.headers

		instance.name = 'Livy Session, sessionId %s' % (response['id'])
		instance.session_id = int(response['id'])
		instance.session_url = headers['Location']
		instance.status = response['state']
		instance.session_timestamp = headers['Date']
		instance.active = True


@receiver(models.signals.pre_delete, sender=Job)
def delete_job_output_pre_delete(sender, instance, **kwargs):

	logger.debug('removing job_output for job id %s' % instance.id)

	# if file://
	if instance.job_output and instance.job_output.startswith('file://'):

		try:
			output_dir = instance.job_output.split('file://')[-1]
			shutil.rmtree(output_dir)
		except:
			logger.debug('could not remove job output directory at: %s' % instance.job_output)



##################################
# Apahce Livy
##################################

class LivyClient(object):

	'''
	Client used for HTTP requests made to Livy server.
	On init, pull Livy information and credentials from localsettings.py.
	
	This Class uses a combination of raw HTTP requests to Livy server, and the built-in
	python-api HttpClient.
		- raw requests are helpful for starting sessions, and getting session status
		- HttpClient useful for submitting jobs, closing session

	Sets class attributes from Django settings
	'''

	server_host = settings.LIVY_HOST 
	server_port = settings.LIVY_PORT 
	default_session_config = settings.LIVY_DEFAULT_SESSION_CONFIG


	@classmethod
	def http_request(self, http_method, url, data=None, headers={'Content-Type':'application/json'}, files=None, stream=False):

		'''
		Make HTTP request to Livy serer.

		Args:
			verb (str): HTTP verb to use for request, e.g. POST, GET, etc.
			url (str): expecting path only, as host is provided by settings
			data (str,file): payload of data to send for request
			headers (dict): optional dictionary of headers passed directly to requests.request, defaults to JSON content-type request
			files (dict): optional dictionary of files passed directly to requests.request
			stream (bool): passed directly to requests.request for stream parameter
		'''

		# prepare data as JSON string
		if type(data) != str:
			data = json.dumps(data)

		# build request
		session = requests.Session()
		request = requests.Request(http_method, "http://%s:%s/%s" % (self.server_host, self.server_port, url.lstrip('/')), data=data, headers=headers, files=files)
		prepped_request = request.prepare() # or, with session, session.prepare_request(request)
		response = session.send(
			prepped_request,
			stream=stream,
		)
		return response


	@classmethod
	def get_sessions(self):

		'''
		Return current Livy sessions

		Returns:
			Livy server response (dict)
		'''

		livy_sessions = self.http_request('GET','sessions')
		return livy_sessions


	@classmethod
	def create_session(self, config=None):

		'''
		Initialize Livy/Spark session.

		Args:
			config (dict): optional configuration for Livy session, defaults to settings.LIVY_DEFAULT_SESSION_CONFIG

		Returns:
			Livy server response (dict)
		'''

		# if optional session config provided, use, otherwise use default session config from localsettings
		if config:
			data = config
		else:
			data = self.default_session_config

		# issue POST request to create new Livy session
		return self.http_request('POST', 'sessions', data=data)


	@classmethod
	def session_status(self, session_id):

		'''
		Return status of Livy session based on session id

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		return self.http_request('GET','sessions/%s' % session_id)


	@classmethod
	def stop_session(self, session_id):

		'''
		Assume session id's are unique, change state of session DB based on session id only
			- as opposed to passing session row, which while convenient, would limit this method to 
			only stopping sessions with a LivySession row in the DB

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		# remove session
		return self.http_request('DELETE','sessions/%s' % session_id)


	@classmethod
	def get_jobs(self, session_id, python_code):

		'''
		Get all jobs (statements) for a session

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		# statement
		jobs = self.http_request('GET', 'sessions/%s/statements' % session_id)
		return job


	@classmethod
	def job_status(self, job_url):

		'''
		Get status of job (statement) for a session

		Args:
			job_url (str/int): full URL for statement in Livy session

		Returns:
			Livy server response (dict)
		'''

		# statement
		statement = self.http_request('GET', job_url)
		return statement


	@classmethod
	def submit_job(self, session_id, python_code):

		'''
		Submit job via HTTP request to /statements

		Args:
			session_id (str/int): Livy session id
			python_code (str): 

		Returns:
			Livy server response (dict)
		'''

		logger.debug(python_code)
		
		# statement
		job = self.http_request('POST', 'sessions/%s/statements' % session_id, data=json.dumps(python_code))
		logger.debug(job.json())
		logger.debug(job.headers)
		return job
		


##################################
# Job Factories
##################################

class CombineJob(object):


	def __init__(self, user):

		self.user = user
		self.livy_session = self._get_active_livy_session()


	def _get_active_livy_session(self):

		'''
		Method to determine active livy session if present, or create if does not exist
		'''

		# check for single, active livy session from LivyClient
		livy_sessions = LivySession.objects.filter(active=True)

		# if single session, confirm active or starting
		if livy_sessions.count() == 1:
			
			livy_session = livy_sessions.first()
			logger.debug('single livy session found, confirming running')
			livy_session_status = LivyClient().session_status(livy_session.session_id)
			
			if livy_session_status.status_code == 200:
				status = livy_session_status.json()['state']
				if status in ['starting','idle','busy']:
					# return livy session
					return livy_session

		# if none found, start and return
		elif livy_sessions.count() == 0:
			logger.debug('no active livy sessions found, starting one')
			
			# begin session
			livy_session = LivySession()
			livy_session.save()

			# poll livy session state until idle
			while True:

				# update from Livy
				livy_session.refresh_from_livy()
				if livy_session.status in ['idle','busy']:
					return livy_session

				else:
					time.sleep(3)

		# if more than one found, for now, raise Exception as only expecting one
		if livy_sessions.count() > 1:
			raise Exception('more than one active Livy session found')


	def submit_job(self, job_code, job_output):

		# submit job
		submit = LivyClient().submit_job(self.livy_session.session_id, job_code)
		response = submit.json()
		headers = submit.headers

		# update job in DB
		self.job.spark_code = job_code
		self.job.job_id = int(response['id'])
		self.job.status = response['state']
		self.job.url = headers['Location']
		self.job.headers = headers
		self.job.job_output = job_output
		self.job.save()

		# # save job_output to JobOutput instance
		# job_output_instance = JobOutput(
		# 	job=self.job,
		# 	output=job_output
		# )
		# job_output_instance.save()


	def get_job(self, job_id):

		'''
		Retrieve job information from DB to perform other tasks

		Args:
			job_id (int): Job ID
		'''

		self.job = Job.objects.filter(id=job_id).first()


	def count_records(self):

		'''
		For job pinned to this CombineJob instance,
		count records from self.job_output (HDFS location of avro files)
		'''

		# prepare code
		job_code = {'code': 'spark.read.format("com.databricks.spark.avro")\
		.load("%(harvest_path)s")\
		.select("record.*").where("record is not null")\
		.count()' % {'harvest_path':self.job.job_output}}

		# submit job
		response = LivyClient().submit_job(self.livy_session.session_id, job_code)
		logger.debug(response.json())
		logger.debug(response.headers)

		# poll until complete
		while True:
			count_check = LivyClient().http_request('GET', response.headers['Location'])
			if count_check.json()['state'] == 'available':
				record_count = int(count_check.json()['output']['data']['text/plain'])
				logger.debug('record count complete: %s' % record_count)
				return record_count
			else:
				time.sleep(.25)



class HarvestJob(CombineJob):


	def __init__(self, user, record_group, oai_endpoint, overrides=None):

		'''
		Initialize Job.

		Unlike other jobs, harvests do not require input from the output of another job

		Args:
			user (User or core.models.CombineUser): user that will issue job
			record_group (core.models.RecordGroup): record group instance that will be used for harvest
			oai_endpoint (core.models.OAIEndpoint): OAI endpoint to be used for OAI harvest
			overrides (dict): optional dictionary of overrides to OAI endpoint

		Returns:

		'''

		# perform CombineJob initialization
		super().__init__(user=user)

		self.record_group = record_group
		self.organization = self.record_group.organization
		self.oai_endpoint = oai_endpoint
		self.overrides = overrides

		# create Job entry in DB
		'''
		record_group = models.ForeignKey(RecordGroup, on_delete=models.CASCADE)
		name = models.CharField(max_length=128)
		spark_code = models.CharField(max_length=32000)
		status = models.CharField(max_length=30, null=True)
		url = models.CharField(max_length=255)
		headers = models.CharField(max_length=255)
		job_input = models.CharField(max_length=255)
		job_output = models.CharField(max_length=255, null=True)
		'''
		self.job = Job(
			record_group = self.record_group,
			user = self.user,
			name = 'OAI Harvest',
			spark_code = None,
			job_id = None,
			status = 'initializing',
			url = None,
			headers = None,
			job_output = None
		)
		self.job.save()


	@staticmethod
	def spark_function(**kwargs):

		'''
		Harvest records, select non-null, and write to avro files

		expecting kwargs from self.start_job()
		'''
		df = spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", kwargs['endpoint'])\
		.option("verb", kwargs['verb'])\
		.option("metadataPrefix", kwargs['metadataPrefix'])\
		.option(kwargs['scope_type'], kwargs['scope_value'])\
		.load()
		
		# select only records
		records = df.select("record.*").where("record is not null")
		
		# write them to avro files
		records.write.format("com.databricks.spark.avro").save(kwargs['harvest_save_path'])


	def start_job(self):

		'''
		Construct python code that will be sent to Livy for harvest job
		'''

		# construct harvest path
		harvest_save_path = '%s/organizations/%s/record_group/%s/jobs/harvest/%s' % (settings.AVRO_STORAGE.rstrip('/'), self.organization.id, self.record_group.id, self.job.id)

		# create shallow copy of oai_endpoint and mix in overrides
		harvest_vars = self.oai_endpoint.__dict__.copy()
		harvest_vars.update(self.overrides)

		# prepare job code
		job_code = {
			'code':'%(spark_function)s\nspark_function(endpoint="%(endpoint)s", verb="%(verb)s", metadataPrefix="%(metadataPrefix)s", scope_type="%(scope_type)s", scope_value="%(scope_value)s", harvest_save_path="%(harvest_save_path)s")' % 
			{
				'spark_function': textwrap.dedent(inspect.getsource(self.spark_function)).replace('@staticmethod\n',''),
				'endpoint':harvest_vars['endpoint'],
				'verb':harvest_vars['verb'],
				'metadataPrefix':harvest_vars['metadataPrefix'],
				'scope_type':harvest_vars['scope_type'],
				'scope_value':harvest_vars['scope_value'],
				'harvest_save_path':harvest_save_path
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job(job_code, harvest_save_path)



class TransformJob(CombineJob):
	
	'''
	Apply an XSLT transformation to a record group
	'''

	def __init__(self, user, record_group, input_job, transformation):

		# perform CombineJob initialization
		super().__init__(user=user)

		self.record_group = record_group
		self.organization = self.record_group.organization
		self.input_job = input_job
		self.transformation = transformation

		# create Job entry in DB
		self.job = Job(
			record_group = self.record_group,
			user = self.user,
			name = 'Transform',
			spark_code = None,
			job_id = None,
			status = 'initializing',
			url = None,
			headers = None,
			job_output = None,
			job_details = json.dumps(
				{'transformation':
					{
						'name':self.transformation.name,
						'type':self.transformation.transformation_type,
						'id':self.transformation.id
					}
				})
		)
		self.job.save()

		# save input job to JobInput table
		job_input_instance = JobInput(job=self.job, input_job=input_job)
		job_input_instance.save()


	@staticmethod
	def spark_function(**kwargs):

		# imports
		from lxml import etree
		from pyspark.sql import Row

		# read output from input_job
		df = spark.read.format('com.databricks.spark.avro').load(kwargs['job_input'])

		# get string of xslt
		with open('/home/combine/data/combine/xslt/WSUDOR_mods_to_DPLA_mods.xsl','r') as f:
			xslt = f.read().encode('utf-8')

		# define function for transformation
		def transform_xml(record_id, xml, xslt):

			# attempt transformation and save out put to 'document'
			try:
				xslt_root = etree.fromstring(xslt)
				transform = etree.XSLT(xslt_root)
				xml_root = etree.fromstring(xml)
				mods_root = xml_root.find('{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.loc.gov/mods/v3}mods')
				result_tree = transform(mods_root)
				result = etree.tostring(result_tree)
				return Row(
					id=record_id,
					document=result.decode('utf-8'),
					error=''
				)

			# catch transformation exception and save exception to 'error'
			except Exception as e:
				return Row(
					id=record_id,
					document='',
					error=str(e)
				)

		# transform via rdd.map
		transformed = df.rdd.map(lambda row: transform_xml(row.id, row.document, xslt))

		# write them to avro files
		transformed.toDF().write.format("com.databricks.spark.avro").save(kwargs['transform_save_path'])


	def start_job(self):

		'''
		Construct python code that will be sent to Livy for harvest job
		'''

		# construct harvest path
		transform_save_path = '%s/organizations/%s/record_group/%s/jobs/transform/%s' % (settings.AVRO_STORAGE.rstrip('/'), self.organization.id, self.record_group.id, self.job.id)

		# prepare job code
		job_code = {
			'code':'%(spark_function)s\nspark_function(transform_save_path="%(transform_save_path)s",job_input="%(job_input)s")' % 
			{
				'spark_function': textwrap.dedent(inspect.getsource(self.spark_function)).replace('@staticmethod\n',''),
				'job_input':self.input_job.job_output,
				'transform_save_path':transform_save_path
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job(job_code, transform_save_path)



class MergeJob(CombineJob):
	
	'''
	Details to figure, but the actual merge will be straightfoward:
	merged = pd.concat([j1.get_output_as_dataframe(),j2.get_output_as_dataframe()])
	'''

	pass
	







