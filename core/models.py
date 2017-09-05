# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import json
import logging
import requests
import subprocess

# django imports
from django.conf import settings
from django.db import models
from django.utils.encoding import python_2_unicode_compatible


# Get an instance of a logger
logger = logging.getLogger(__name__)


##################################
# Django ORM
##################################

class RecordGroup(models.Model):

	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255)
	status = models.CharField(max_length=30, null=True)

	def __str__(self):
		return 'Record Group: %s' % self.name


class Job(models.Model):

	record_group = models.ForeignKey(RecordGroup, on_delete=models.CASCADE)
	name = models.CharField(max_length=128)
	spark_code = models.CharField(max_length=32000)
	status = models.CharField(max_length=30, null=True)
	url = models.CharField(max_length=255)
	headers = models.CharField(max_length=255)
	job_input = models.CharField(max_length=255)
	job_output = models.CharField(max_length=255, null=True)

	def __str__(self):
		return 'Job: %s, from Record Group: %s' % (self.name, self.record_group.name)


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
# Apahce Livy
##################################

class LivyClient(object):

	'''
	Client used for HTTP requests made to Livy server.
	On init, pull Livy information and credentials from localsettings.py.
	Handle exceptions.

	Sets class attributes from Django settings
	'''

	server_host = settings.LIVY_HOST 
	server_port = settings.LIVY_PORT 
	default_session_config = settings.LIVY_DEFAULT_SESSION_CONFIG


	def get_livy_server_pid(self):

		'''
		Return PID of Livy server.
		This is instrumental in determining if a saved session or statement URL in Livy is current or stale.

		Args:
			None

		Returns:
			livy server pid (int)
		'''
		try:
			return int(subprocess.check_output(['lsof','-i',':%s' % self.server_port]).decode('utf-8').split('\n')[1].split('    ')[1].split(' ')[0].strip())
		except:
			logger.debug('Could not determine PID of Livy server, returning False')
			return False

		
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
		return livy_sessions.json()


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
		livy_session = self.http_request('POST', 'sessions', data=data)
		logger.debug(livy_session.json())

		return livy_session.json()


	@classmethod
	def session_status(self, session_id):

		'''
		Return status of Livy session based on session id

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		livy_session_status = self.http_request('GET','sessions/%s' % session_id)
		return livy_session_status.json()



















