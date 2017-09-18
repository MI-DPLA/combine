# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import hashlib
import json
import logging
import requests
import subprocess
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



class RecordGroup(models.Model):

	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255)
	status = models.CharField(max_length=30, null=True, default=None)


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
	response = models.CharField(max_length=32000, null=True, default=None)
	job_input = models.CharField(max_length=255, null=True)
	job_output = models.CharField(max_length=255, null=True)
	record_count = models.IntegerField(null=True, default=0)


	def __str__(self):
		return '%s, from Record Group: %s' % (self.name, self.record_group.name)


	def refresh_from_livy(self):

		# query Livy for statement status
		livy_response = LivyClient().job_status(self.url)

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



class OAIEndpoint(models.Model):

	name = models.CharField(max_length=255)
	endpoint = models.CharField(max_length=255)
	verb = models.CharField(max_length=128)
	metadataPrefix = models.CharField(max_length=128)
	scope_type = models.CharField(max_length=128) # expecting one of setList, whiteList, blackList
	scope_value = models.CharField(max_length=1024)


	def __str__(self):
		return 'OAI endpoint: %s' % self.name



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

				# livy_session_status = LivyClient().session_status(livy_session.session_id)
				# if livy_session_status.status_code == 200:
				# 	status = livy_session_status.json()['state']
				# 	if status in ['idle','busy']:
						
				# 		# return livy session
				# 		return livy_session

				else:
					time.sleep(3)

		# if more than one found, for now, raise Exception as only expecting one
		if livy_sessions.count() > 1:
			raise Exception('more than one active Livy session found')


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
			job_input = 'oai',
			job_output = None
		)
		self.job.save()


	def start_job(self):

		'''
		Construct python code that will be sent to Livy for harvest job
		'''

		# construct harvest path
		# harvest_save_path = '/user/combine/record_group/%s/jobs/harvest/%s' % (self.record_group.id, self.job.id)
		harvest_save_path = '%s/record_group/%s/jobs/harvest/%s' % (settings.AVRO_STORAGE.rstrip('/'), self.record_group.id, self.job.id)

		# create shallow copy of oai_endpoint and mix in overrides
		harvest_vars = self.oai_endpoint.__dict__.copy()
		harvest_vars.update(self.overrides)

		# prepare job code
		job_code = {'code': 'spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", "%(endpoint)s")\
		.option("verb", "%(verb)s")\
		.option("metadataPrefix", "%(metadataPrefix)s")\
		.option("%(scope_type)s", "%(scope_value)s")\
		.load()\
		.write.format("com.databricks.spark.avro").save("%(harvest_save_path)s")' % 
			{
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
		submit = LivyClient().submit_job(self.livy_session.session_id, job_code)
		response = submit.json()
		headers = submit.headers

		# update job in DB
		self.job.spark_code = job_code
		self.job.job_id = int(response['id'])
		self.job.status = response['state']
		self.job.url = headers['Location']
		self.job.headers = headers
		self.job.job_output = harvest_save_path
		self.job.save()














