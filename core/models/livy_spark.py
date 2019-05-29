# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import datetime
import dateutil
import json
import logging
import polling
import requests

# django imports
from django.conf import settings
from django.db import models, transaction

# import mongo dependencies
from core.mongo import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class LivySession(models.Model):

	'''
	Model to manage Livy sessions.
	'''

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


	def refresh_from_livy(self, save=True):

		'''
		Method to ping Livy for session status and update DB

		Args:
			None

		Returns:
			None
				- updates attributes of self
		'''

		logger.debug('querying Livy for session status')

		# query Livy for session status
		livy_response = LivyClient().session_status(self.session_id)

		# if response
		if livy_response != False:

			# parse response and set self values
			response = livy_response.json()
			headers = livy_response.headers

			# if status_code 404, set as gone
			if livy_response.status_code == 404:

				logger.debug('session not found, setting status to gone')
				self.status = 'gone'

				# update
				if save:
					self.save()

				# return self
				return self.status

			elif livy_response.status_code == 200:

				# update Livy information
				logger.debug('session found, updating status')

				# update status
				self.status = response['state']
				if self.status in ['starting','idle','busy']:
					self.active = True

				self.session_timestamp = headers['Date']

				# gather information about registered application in spark cluster
				try:
					spark_app_id = SparkAppAPIClient.get_application_id(self, self.session_id)
					self.appId = spark_app_id
				except:
					pass

				# update
				if save:
					self.save()

				# return self
				return self.status

			else:

				logger.debug('error: livy request http code: %s' % livy_response.status_code)
				logger.debug('error: livy request content: %s' % livy_response.content)
				return False

		# else, do nothing
		else:

			logger.debug('error communicating with Livy, doing nothing')
			return False


	def start_session(self):

		'''
		Method to start Livy session with Livy HttpClient

		Args:
			None

		Returns:
			None
		'''

		# get default config from localsettings
		livy_session_config = settings.LIVY_DEFAULT_SESSION_CONFIG

		# confirm SPARK_APPLICATION_ROOT_PORT is open
		# increment if need be, attempting 100 port increment, setting to livy_session_config
		spark_ui_port = settings.SPARK_APPLICATION_ROOT_PORT
		for x in range(0,100):
			try:
				r = requests.get('http://localhost:%s' % spark_ui_port)
				logger.debug('port %s in use, incrementing...' % spark_ui_port)
				spark_ui_port += 1
			except requests.ConnectionError:
				logger.debug('port %s open, using for LivySession' % spark_ui_port)
				livy_session_config['conf']['spark_ui_port'] = spark_ui_port
				break

		# create livy session, get response
		livy_response = LivyClient().create_session(config=livy_session_config)

		# parse response and set instance values
		response = livy_response.json()
		logger.debug(response)
		headers = livy_response.headers

		self.name = 'Livy Session, sessionId %s' % (response['id'])
		self.session_id = int(response['id'])
		self.session_url = headers['Location']
		self.status = response['state']
		self.session_timestamp = headers['Date']
		self.active = True
		self.sparkUiUrl = '%s:%s' % (settings.SPARK_HOST, spark_ui_port)

		# update db
		self.save()


	def stop_session(self):

		'''
		Method to stop Livy session with Livy HttpClient

		Args:
			None

		Returns:
			None
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

		Args:
			None

		Returns:
			(LivySession): active Livy session instance
		'''

		active_livy_sessions = LivySession.objects.filter(active=True)

		if active_livy_sessions.count() == 1:
			return active_livy_sessions.first()

		elif active_livy_sessions.count() == 0:
			return False

		elif active_livy_sessions.count() > 1:
			return active_livy_sessions


	def get_log_lines(self, size=10):

		'''
		Method to return last 10 log lines
		'''

		log_response = LivyClient.get_log_lines(
			self.session_id,
			size=size)

		return log_response.json()


	@staticmethod
	def ensure_active_session_id(session_id):

		'''
		Method to ensure passed session id:
			- matches current active Livy session saved to DB
			- in a state to recieve additional jobs

		Args:
			session_id (int): session id to check

		Returns:
			(int): passed sessionid if active and ready,
				or new session_id if started
		'''

		# if session_id is None, start a new session
		if session_id == None:

			logger.debug('active livy session not found, starting new one')

			# start and poll for new one
			new_ls = LivySession()
			new_ls.start_session()

			logger.debug('polling for Livy session to start...')
			results = polling.poll(lambda: new_ls.refresh_from_livy() == 'idle', step=5, timeout=120)

			# pass new session id and continue to livy job submission
			session_id = new_ls.session_id

			# return
			return session_id

		# if passed session id matches and status is idle or busy
		elif session_id != None:

			# retrieve active livy session and refresh
			active_ls = LivySession.get_active_session()
			active_ls.refresh_from_livy(save=False)

			if session_id == active_ls.session_id:

				if active_ls.status in ['idle','busy']:
					logger.debug('active livy session found, state is %s, ready to receieve new jobs, submitting livy job' % active_ls.status)

				else:
					logger.debug('active livy session is found, state %s, but stale, restarting livy session' % active_ls.status)

					# destroy active livy session
					active_ls.stop_session()
					active_ls.delete()

					# start and poll for new one
					new_ls = LivySession()
					new_ls.start_session()

					logger.debug('polling for Livy session to start...')
					results = polling.poll(lambda: new_ls.refresh_from_livy() == 'idle', step=5, timeout=120)

					# pass new session id and continue to livy job submission
					session_id = new_ls.session_id

				# return
				return session_id

			else:
				logger.debug('requested livy session id does not match active livy session id')
				return None


	def restart_session(self):

		'''
		Method to restart Livy session
		'''

		# stop and destroy self
		self.stop_session()
		self.delete()

		# start and poll for new one
		new_ls = LivySession()
		new_ls.start_session()

		# poll until ready
		def livy_session_ready(response):
			return response in ['idle','gone']

		logger.debug('polling for Livy session to start...')
		results = polling.poll(lambda: new_ls.refresh_from_livy(), check_success=livy_session_ready, step=5, timeout=120)

		return new_ls


	def session_port(self):

		'''
		Method to return port from sparkUiURL
		'''

		spark_url = getattr(self, 'sparkUiUrl', None)
		if spark_url is not None:
			return spark_url.split(':')[-1]
		else:
			return False



class LivyClient(object):

	'''
	Client used for HTTP requests made to Livy server.
	On init, pull Livy information and credentials from settings.

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
	def http_request(self,
			http_method,
			url,
			data=None,
			params=None,
			headers={'Content-Type':'application/json'},
			files=None,
			stream=False
		):

		'''
		Make HTTP request to Livy serer.

		Args:
			verb (str): HTTP verb to use for request, e.g. POST, GET, etc.
			url (str): expecting path only, as host is provided by settings
			data (str,file): payload of data to send for request
			headers (dict): optional dictionary of headers passed directly to requests.request,
				defaults to JSON content-type request
			files (dict): optional dictionary of files passed directly to requests.request
			stream (bool): passed directly to requests.request for stream parameter
		'''

		# prepare data as JSON string
		if type(data) != str:
			data = json.dumps(data)

		# build session
		session = requests.Session()

		# build request
		request = requests.Request(http_method, "http://%s:%s/%s" % (
			self.server_host,
			self.server_port,
			url.lstrip('/')),
			data=data,
			params=params,
			headers=headers,
			files=files)
		prepped_request = request.prepare() # or, with session, session.prepare_request(request)

		# send request
		try:
			response = session.send(
				prepped_request,
				stream=stream,
			)

			# return
			return response

		except requests.ConnectionError as e:
			logger.debug("LivyClient: error sending http request to Livy")
			logger.debug(str(e))
			return False


	@classmethod
	def get_sessions(self):

		'''
		Return current Livy sessions

		Args:
			None

		Returns:
			(dict): Livy server response
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
			(dict): Livy server response
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
			(dict): Livy server response
		'''

		return self.http_request('GET','sessions/%s' % session_id)


	@classmethod
	def stop_session(self, session_id):

		'''
		Assume session id's are unique, change state of session DB based on session id only
			- as opposed to passing session row, which while convenient, would limit this method to
			only stopping sessions with a LivySession row in the DB

		QUESTION: Should this attempt to kill all Jobs in spark app upon closing?

		Args:
			session_id (str/int): Livy session id

		Returns:
			(dict): Livy server response
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
			(dict): Livy server response
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
			(dict): Livy server response
		'''

		# statement
		statement = self.http_request('GET', job_url)
		return statement


	@classmethod
	def submit_job(self,
		session_id,
		python_code,
		ensure_livy_session=True):

		'''
		Submit job via HTTP request to /statements

		Args:
			session_id (str/int): Livy session id
			python_code (str):

		Returns:
			(dict): Livy server response
		'''

		# if ensure_livy_session
		if ensure_livy_session:
			logger.debug('ensuring Livy session')
			session_id = LivySession.ensure_active_session_id(session_id)

		if session_id != None:

			# submit statement
			job = self.http_request('POST', 'sessions/%s/statements' % session_id, data=json.dumps(python_code))
			return job

		else:
			return False


	@classmethod
	def stop_job(self, job_url):

		'''
		Stop job via HTTP request to /statements

		Args:
			job_url (str/int): full URL for statement in Livy session

		Returns:
			(dict): Livy server response
		'''

		# statement
		statement = self.http_request('POST', '%s/cancel' % job_url)
		return statement


	@classmethod
	def get_log_lines(self, session_id, size=10):

		'''
		Return lines from Livy log

		Args:
			session_id (str/int): Livy session id
			size (int): Max number of lines

		Returns:
			(list): Log lines
		'''

		return self.http_request('GET','sessions/%s/log' % session_id, params={'size':size})



class SparkAppAPIClient(object):

	'''
	Client to communicate with Spark Application created by Livy Session

	TODO:
		- the Spark Application port can change (https://github.com/MI-DPLA/combine/issues/243)
			- SPARK_APPLICATION_API_BASE is based on 4040 for SPARK_APPLICATION_ROOT_PORT
			- increment from 4040, consider looping through until valid app found?
	'''

	@classmethod
	def http_request(self,
			livy_session,
			http_method,
			url,
			data=None,
			params=None,
			headers={'Content-Type':'application/json'},
			files=None,
			stream=False
		):

		'''
		Make HTTP request to Spark Application API

		Args:
			livy_session (core.models.LivySession): instance of LivySession that contains sparkUiUrl
			verb (str): HTTP verb to use for request, e.g. POST, GET, etc.
			url (str): expecting path only, as host is provided by settings
			data (str,file): payload of data to send for request
			headers (dict): optional dictionary of headers passed directly to requests.request,
				defaults to JSON content-type request
			files (dict): optional dictionary of files passed directly to requests.request
			stream (bool): passed directly to requests.request for stream parameter
		'''

		# prepare data as JSON string
		if type(data) != str:
			data = json.dumps(data)

		# build request
		session = requests.Session()
		request = requests.Request(http_method, "http://%s%s" % ("%s" % livy_session.sparkUiUrl, url),
			data=data,
			params=params,
			headers=headers,
			files=files)
		prepped_request = request.prepare()
		response = session.send(
			prepped_request,
			stream=stream,
		)
		return response


	@classmethod
	def get_application_id(self, livy_session, livy_session_id):

		'''
		Attempt to retrieve application ID based on Livy Session ID

		Args:
			None

		Returns:
			(dict): Spark Application API response
		'''

		# get list of applications
		applications = self.http_request(livy_session, 'GET','/api/v1/applications').json()

		# loop through and look for Livy session
		for app in applications:
			if app['name'] == 'livy-session-%s' % livy_session_id:
				logger.debug('found application matching Livy session id: %s' % app['id'])
				return app['id']


	@classmethod
	def get_spark_jobs_by_jobGroup(self, livy_session, spark_app_id, jobGroup, parse_dates=False, calc_duration=True):

		'''
		Method to retrieve all Jobs from application, then filter by jobGroup
		'''

		# get all jobs from application
		jobs = self.http_request(livy_session, 'GET','/api/v1/applications/%s/jobs' % spark_app_id).json()

		# loop through and filter
		filtered_jobs = [ job for job in jobs if 'jobGroup' in job.keys() and job['jobGroup'] == str(jobGroup) ]

		# convert to datetimes
		if parse_dates:
			for job in filtered_jobs:
				job['submissionTime'] = dateutil.parser.parse(job['submissionTime'])
				if 'completionTime' in job.keys():
					job['completionTime'] = dateutil.parser.parse(job['completionTime'])

		# calc duration if flagged
		if calc_duration:
			for job in filtered_jobs:

				# prepare dates
				if not parse_dates:
					st = dateutil.parser.parse(job['submissionTime'])
					if 'completionTime' in job.keys():
						ct = dateutil.parser.parse(job['completionTime'])
					else:
						ct = datetime.datetime.now()
				else:
					st = job['submissionTime']
					if 'completionTime' in job.keys():
						ct = job['completionTime']
					else:
						ct = datetime.datetime.now()

				# calc and append
				job['duration'] = (ct.replace(tzinfo=None) - st.replace(tzinfo=None)).seconds
				m, s = divmod(job['duration'], 60)
				h, m = divmod(m, 60)
				job['duration_s'] = "%d:%02d:%02d" % (h, m, s)

		return filtered_jobs


	@classmethod
	def kill_job(self, livy_session, job_id):

		'''
		Method to send kill command via GET request to Spark Job id
		'''

		kill_request = self.http_request(livy_session, 'GET','/jobs/job/kill/', params={'id':job_id})
		if kill_request.status_code == 200:
			return True
		else:
			return False