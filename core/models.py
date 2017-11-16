# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import datetime
import hashlib
import inspect
import json
import logging
from lxml import etree
import os
import requests
import shutil
import subprocess
from sqlalchemy import create_engine
import re
import textwrap
import time
import uuid
import xmltodict

# django imports
from django.apps import AppConfig
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth import signals
from django.db import models
from django.dispatch import receiver
from django.utils.encoding import python_2_unicode_compatible
from django.utils.html import format_html

# Livy
from livy.client import HttpClient

# import elasticsearch and handles
from core.es import es_handle
from elasticsearch_dsl import Search, A, Q

# Get an instance of a logger
logger = logging.getLogger(__name__)



####################################################################
# Django ORM                                                       #
####################################################################

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


	def refresh_from_livy(self):

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
			logger.debug('no active livy sessions found, returning False')
			return False

		elif active_livy_sessions.count() > 1:
			logger.debug('multiple active livy sessions found, returning as list')
			return active_livy_sessions



class Organization(models.Model):

	'''
	Model to manage Organizations in Combine.
	Organizations contain Record Groups, and are the highest level of organization in Combine.
	'''

	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255)
	publish_id = models.CharField(max_length=255)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'Organization: %s' % self.name



class RecordGroup(models.Model):

	'''
	Model to manage Record Groups in Combine.
	Record Groups are members of Organizations, and contain Jobs
	'''

	organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255, null=True, default=None)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)
	publish_set_id = models.CharField(max_length=128)


	def __str__(self):
		return 'Record Group: %s' % self.name



class Job(models.Model):

	'''
	Model to manage jobs in Combine.
	Jobs are members of Record Groups, and contain Records.

	A Job can be considered a "stage" of records in Combine as they move through Harvest, Transformations, Merges, and 
	eventually Publishing.
	'''

	record_group = models.ForeignKey(RecordGroup, on_delete=models.CASCADE)
	job_type = models.CharField(max_length=128, null=True)
	user = models.ForeignKey(User, on_delete=models.CASCADE)
	name = models.CharField(max_length=128, null=True)
	spark_code = models.TextField(null=True, default=None)
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
	note = models.TextField(null=True, default=None)
	elapsed = models.IntegerField(null=True, default=0)


	def __str__(self):
		return '%s, Job #%s, from Record Group: %s' % (self.name, self.id, self.record_group.name)


	def update_status(self):

		'''
		Method to udpate job information based on status from Livy

		Args:
			None

		Returns:
			None
				- updates status, record_count, elapsed (soon)
		'''

		if self.status in ['init','waiting','pending','starting','running','available'] and self.url != None:
			self.refresh_from_livy(save=False)

		# udpate record count if not already calculated
		if self.record_count == 0:

			# if finished, count
			if self.finished:
				self.update_record_count(save=False)

		# update elapsed
		if not self.finished and self.status in ['starting','running']:
			self.elapsed = self.calc_elapsed()

		# finally, save
		self.save()


	def calc_elapsed(self):

		'''
		Method to calculate how long a job has elapsed based on time since timestamp.

		Args:
			None

		Returns:
			(int): elapsed time in seconds
		'''

		return (datetime.datetime.now() - self.timestamp.replace(tzinfo=None)).seconds


	def elapsed_as_string(self):

		'''
		Method to return elapsed as string for Django templates
		'''

		m, s = divmod(self.elapsed, 60)
		h, m = divmod(m, 60)
		return "%d:%02d:%02d" % (h, m, s)


	def refresh_from_livy(self, save=True):

		'''
		Update job status from Livy.

		Args:
			None

		Returns:
			None
				- sets attriutes of self
		'''

		# query Livy for statement status
		livy_response = LivyClient().job_status(self.url)
		
		# if status_code 404, set as gone
		if livy_response.status_code == 400:
			
			logger.debug(livy_response.json())
			logger.debug('Livy session likely not active, setting status to gone')
			self.status = 'gone'
			
			# update
			if save:
				self.save()

		# if status_code 404, set as gone
		if livy_response.status_code == 404:
			
			logger.debug('job/statement not found, setting status to gone')
			self.status = 'gone'
			
			# update
			if save:
				self.save()

		elif livy_response.status_code == 200:

			# parse response
			response = livy_response.json()
			headers = livy_response.headers
			
			# update Livy information
			self.status = response['state']
			logger.debug('job/statement found, updating status to %s' % self.status)

			# if state is available, assume finished
			if self.status == 'available':
				self.finished = True

			# update
			if save:
				self.save()

		else:
			
			logger.debug('error retrieving information about Livy job/statement')
			logger.debug(livy_response.status_code)
			logger.debug(livy_response.json())


	def get_records(self):

		'''
		Retrieve records associated with this job, if the document field is not blank.

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet)
		'''

		return Record.objects.filter(job=self).exclude(document='').all()


	def get_errors(self):

		'''
		Retrieve records associated with this job if the error field is not blank.

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet)
		'''

		return Record.objects.filter(job=self).exclude(error='').all()


	def update_record_count(self, save=True):

		'''
		Get record count from DB, save to self

		Args:
			None

		Returns:
			None
		'''
		
		self.record_count = self.get_records().count()
		
		# if save, save
		if save:
			self.save()


	def job_output_as_filesystem(self):

		'''
		Not entirely removing the possibility of storing jobs on HDFS, this method returns self.job_output as
		filesystem location and strips any righthand slash

		Args:
			None

		Returns:
			(str): location of job output
		'''

		return self.job_output.replace('file://','').rstrip('/')


	def get_output_files(self):

		'''
		Convenience method to return full path of all avro files in job output

		Args:
			None

		Returns:
			(list): list of strings of avro files locations on disk
		'''

		output_dir = self.job_output_as_filesystem()
		return [ os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.avro') ]


	def index_results_save_path(self):

		'''
		Return index save path

		Args:
			None

		Returns:
			(str): location of saved indexing results
		'''
		
		# index results save path
		return '%s/organizations/%s/record_group/%s/jobs/indexing/%s' % (settings.BINARY_STORAGE.rstrip('/'), self.record_group.organization.id, self.record_group.id, self.id)



class JobInput(models.Model):

	'''
	Model to manage input jobs for other jobs.
	Provides a one-to-many relationship for a job and potential multiple input jobs
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	input_job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='input_job')



class JobPublish(models.Model):

	'''
	Model to manage published jobs.
	Provides a one-to-one relationship for a record group and published job
	'''

	record_group = models.ForeignKey(RecordGroup)
	job = models.ForeignKey(Job, on_delete=models.CASCADE)

	def __str__(self):
		return 'Published Set #%s, "%s" - from Job %s, Record Group %s - ' % (self.id, self.record_group.publish_set_id, self.job.name, self.record_group.name)



class OAIEndpoint(models.Model):

	'''
	Model to manage user added OAI endpoints
	'''

	name = models.CharField(max_length=255)
	endpoint = models.CharField(max_length=255)
	verb = models.CharField(max_length=128)
	metadataPrefix = models.CharField(max_length=128)
	scope_type = models.CharField(max_length=128) # expecting one of setList, whiteList, blackList
	scope_value = models.CharField(max_length=1024)


	def __str__(self):
		return 'OAI endpoint: %s' % self.name


	def as_dict(self):

		'''
		Return model attributes as dictionary

		Args:
			None

		Returns:
			(dict): attributes for model instance
		'''

		d = self.__dict__
		d.pop('_state', None)
		return d



class Transformation(models.Model):

	'''
	Model to handle "transformation scenarios".  Envisioned to faciliate more than just XSL transformations, but
	currently, only XSLT is handled downstream
	'''

	name = models.CharField(max_length=255)
	payload = models.TextField()
	transformation_type = models.CharField(max_length=255, choices=[('xslt','XSLT Stylesheet'),('python','Python Code Snippet')])
	filepath = models.CharField(max_length=1024, null=True, default=None)
	

	def __str__(self):
		return 'Transformation: %s, transformation type: %s' % (self.name, self.transformation_type)



class OAITransaction(models.Model):

	'''
	Model to manage transactions from OAI server, including all requests and resumption tokens when needed.

	Improvement: expire resumption tokens after some time.
	'''

	verb = models.CharField(max_length=255)
	start = models.IntegerField(null=True, default=None)
	chunk_size = models.IntegerField(null=True, default=None)
	publish_set_id = models.CharField(max_length=255, null=True, default=None)
	token = models.CharField(max_length=1024, db_index=True)
	args = models.CharField(max_length=1024)
	

	def __str__(self):
		return 'OAI Transaction: %s, resumption token: %s' % (self.id, self.token)



class Record(models.Model):

	'''
	Model to manage individual records.
	Records are the lowest level of granularity in Combine.  They are members of Jobs.
	
	NOTE: This DB model is not managed by Django for performance reasons.  The SQL for table creation is included in 
	combine/core/inc/combine_tables.sql
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	record_id = models.CharField(max_length=1024, null=True, default=None)
	oai_id = models.CharField(max_length=1024, null=True, default=None)
	document = models.TextField(null=True, default=None)
	error = models.TextField(null=True, default=None)
	unique = models.BooleanField(default=1)
	oai_set = models.CharField(max_length=255, null=True, default=None)


	# this model is managed outside of Django
	class Meta:
		managed = False


	def __str__(self):
		return 'Record: #%s, record_id: %s, job_id: %s, job_type: %s' % (self.id, self.record_id, self.job.id, self.job.job_type)


	def get_record_stages(self, input_record_only=False):

		'''
		Method to return all upstream and downstreams stages of this record

		Args:
			input_record_only (bool): If True, return only immediate record that served as input for this record.

		Returns:
			(list): ordered list of Record instances from first created (e.g. Harvest), to last (e.g. Publish).
			This record is included in the list.
		'''

		record_stages = []

		def get_upstream(record, input_record_only):

			# check for upstream job
			upstream_job_query = record.job.jobinput_set

			# if upstream jobs found, continue
			if upstream_job_query.count() > 0:

				logger.debug('upstream jobs found, checking for record_id')

				# loop through upstream jobs, look for record id
				for upstream_job in upstream_job_query.all():
					upstream_record_query = Record.objects.filter(
						job=upstream_job.input_job).filter(record_id=self.record_id)

					# if count found, save record to record_stages and re-run
					if upstream_record_query.count() > 0:
						upstream_record = upstream_record_query.first()
						record_stages.insert(0, upstream_record)
						if not input_record_only:
							get_upstream(upstream_record, input_record_only)


		def get_downstream(record):

			# check for downstream job
			downstream_job_query = JobInput.objects.filter(input_job=record.job)

			# if downstream jobs found, continue
			if downstream_job_query.count() > 0:

				logger.debug('downstream jobs found, checking for record_id')

				# loop through downstream jobs
				for downstream_job in downstream_job_query.all():

					downstream_record_query = Record.objects.filter(
						job=downstream_job.job).filter(record_id=self.record_id)

					# if count found, save record to record_stages and re-run
					if downstream_record_query.count() > 0:
						downstream_record = downstream_record_query.first()
						record_stages.append(downstream_record)
						get_downstream(downstream_record)

		# run
		get_upstream(self, input_record_only)
		if not input_record_only:
			record_stages.append(self)
			get_downstream(self)
		
		# return		
		return record_stages


	def derive_dpla_identifier(self):

		'''
		Method to attempt to derive DPLA identifier based on unique string for service hub, and md5 hash of OAI 
		identifier.  Experiemental.

		Args:
			None

		Returns:
			(str): Derived DPLA identifier
		'''

		pre_hash_dpla_id = '%s%s' % (settings.SERVICE_HUB_PREFIX, self.oai_id)
		return hashlib.md5(pre_hash_dpla_id.encode('utf-8')).hexdigest()


	def get_es_doc(self):

		'''
		Return indexed ElasticSearch document as dictionary

		Args:
			None

		Returns:
			(dict): ES document
		'''

		# init search
		s = Search(using=es_handle, index='j%s' % self.job_id)
		s = s.query('match', _id=self.record_id)

		# execute search and capture as dictionary
		sr = s.execute()
		sr_dict = sr.to_dict()

		# return
		try:
			return sr_dict['hits']['hits'][0]['_source']
		except:
			return {}


	def parse_document_xml(self):

		'''
		Parse self.document as XML node with etree

		Args:
			None

		Returns:
			(lxml.etree._Element)
		'''

		return etree.fromstring(self.document.encode('utf-8'))



class IndexMappingFailure(models.Model):

	'''
	Model for accessing and updating indexing failures.
	
	NOTE: This DB model is not managed by Django for performance reasons.  The SQL for table creation is included in 
	combine/core/inc/combine_tables.sql
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	record_id = models.CharField(max_length=1024, null=True, default=None)
	mapping_error = models.TextField(null=True, default=None)


	# this model is managed outside of Django
	class Meta:
		managed = False


	def __str__(self):
		return 'Index Mapping Failure: #%s, record_id: %s, job_id: %s' % (self.id, self.record_id, self.job.id)


	@property
	def record(self):

		'''
		Property for one-off access to record the indexing failure stemmed from

		Returns:
			(core.models.Record): Record instance that relates to this indexing failure
		'''

		return Record.objects.filter(job=self.job, record_id=self.record_id).first()


	def get_record(self):

		'''
		Method to return target record, for performance purposes if accessed multiple times

		Returns:
			(core.models.Record): Record instance that relates to this indexing failure
		'''

		return Record.objects.filter(job=self.job, record_id=self.record_id).first()



####################################################################
# Signals Handlers                                                 # 
####################################################################

@receiver(signals.user_logged_in)
def user_login_handle_livy_sessions(sender, user, **kwargs):

	'''
	When user logs in, handle check for pre-existing sessions or creating

	Args:
		sender (auth.models.User): class
		user (auth.models.User): instance
		kwargs: not used
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

	Args:
		sender (auth.models.LivySession): class
		user (auth.models.LivySession): instance
		kwargs: not used
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


@receiver(models.signals.post_save, sender=Job)
def save_job(sender, instance, created, **kwargs):

	'''
	After job is saved, update job output

	Args:
		sender (auth.models.Job): class
		user (auth.models.Job): instance
		created (bool): indicates if newly created, or just save/update
		kwargs: not used
	'''

	# if the record was just created, then update job output (ensures this only runs once)
	if created:
		# set output based on job type
		logger.debug('setting job output for job')
		instance.job_output = '%s/organizations/%s/record_group/%s/jobs/%s/%s' % (
			settings.BINARY_STORAGE.rstrip('/'),
			instance.record_group.organization.id,
			instance.record_group.id,
			instance.job_type,
			instance.id)
		instance.save()


@receiver(models.signals.pre_delete, sender=Job)
def delete_job_output_pre_delete(sender, instance, **kwargs):

	'''
	When jobs are removed, some actions are performed:
		- if job is queued or running, stop
		- if Publish job, remove symlinks
		- remove avro files from disk
		- delete ES indexes (if present)

	Args:
		sender (auth.models.Job): class
		user (auth.models.Job): instance
		kwargs: not used
	'''

	logger.debug('removing job_output for job id %s' % instance.id)

	# check if job running or queued, attempt to stop
	try:
		instance.refresh_from_livy()
		if instance.status in ['waiting','running']:
			# attempt to stop job
			livy_response = LivyClient().stop_job(instance.url)
			logger.debug(livy_response)

	except Exception as e:
		logger.debug('could not stop job in livy')
		logger.debug(str(e))


	# if publish job, remove symlinks to global /published
	if instance.job_type == 'PublishJob':

		logger.debug('Publish job detected, removing symlinks and removing record set from ES index')

		# open cjob
		cjob = CombineJob.get_combine_job(instance.id)

		# loop through published symlinks and look for filename hash similarity
		published_dir = os.path.join(settings.BINARY_STORAGE.split('file://')[-1].rstrip('/'), 'published')
		job_output_filename_hash = cjob.get_job_output_filename_hash()
		try:
			for f in os.listdir(published_dir):
				# if hash is part of filename, remove
				if job_output_filename_hash in f:
					os.remove(os.path.join(published_dir, f))
		except:
			logger.debug('could not delete symlinks from /published directory')

		# attempting to delete from ES
		try:
			del_dsl = {
				'query':{
					'match':{
						'publish_set_id':instance.record_group.publish_set_id
					}
				}
			}
			if es_handle.indices.exists('published'):
				r = es_handle.delete_by_query(
					index='published',
					doc_type='record',
					body=del_dsl
				)
			else:
				logger.debug('published index not found in ES, skipping removal of records')
		except Exception as e:
			logger.debug('could not remove published records from ES index')
			logger.debug(str(e))


	# remove avro files from disk
	# if file://
	if instance.job_output and instance.job_output.startswith('file://'):

		try:
			output_dir = instance.job_output.split('file://')[-1]
			shutil.rmtree(output_dir)
		except:
			logger.debug('could not remove job output directory at: %s' % instance.job_output)


	# remove ES index if exists
	try:
		if es_handle.indices.exists('j%s' % instance.id):
			logger.debug('removing ES index: j%s' % instance.id)
			es_handle.indices.delete('j%s' % instance.id)
	except:
		logger.debug('could not remove ES index: j%s' % instance.id)


	# attempt to delete indexing results avro files
	try:
		indexing_dir = ('%s/organizations/%s/record_group/%s/jobs/indexing/%s' % (
			settings.BINARY_STORAGE.rstrip('/'),
			instance.record_group.organization.id,
			instance.record_group.id,
			instance.id)).split('file://')[-1]
		shutil.rmtree(indexing_dir)
	except:
		logger.debug('could not remove indexing results')


@receiver(models.signals.pre_save, sender=Transformation)
def save_transformation_to_disk(sender, instance, **kwargs):

	'''
	When users enter a payload for a transformation, write to disk for use in Spark context

	Args:
		sender (auth.models.Transformation): class
		user (auth.models.Transformation): instance
		kwargs: not used
	'''

	# check that transformation directory exists
	transformations_dir = '%s/transformations' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]
	if not os.path.exists(transformations_dir):
		os.mkdir(transformations_dir)

	# if previously written to disk, remove
	if instance.filepath:
		try:
			os.remove(instance.filepath)
		except:
			logger.debug('could not remove transformation file: %s' % instance.filepath)

	# write XSLT type transformation to disk
	if instance.transformation_type == 'xslt':
		filename = uuid.uuid4().hex

		filepath = '%s/%s.xsl' % (transformations_dir, filename)
		with open(filepath, 'w') as f:
			f.write(instance.payload)

		# update filepath
		instance.filepath = filepath

	else:
		logger.debug('currently only xslt style transformations accepted')



####################################################################
# Apahce livy 													   #
####################################################################

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
			url, data=None,
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

		# build request
		session = requests.Session()
		request = requests.Request(http_method, "http://%s:%s/%s" % (
			self.server_host,
			self.server_port,
			url.lstrip('/')),
			data=data,
			headers=headers,
			files=files)
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
	def submit_job(self, session_id, python_code):

		'''
		Submit job via HTTP request to /statements

		Args:
			session_id (str/int): Livy session id
			python_code (str): 

		Returns:
			(dict): Livy server response
		'''

		logger.debug(python_code)
		
		# statement
		job = self.http_request('POST', 'sessions/%s/statements' % session_id, data=json.dumps(python_code))
		logger.debug(job.json())
		logger.debug(job.headers)
		return job


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
		


####################################################################
# Combine Models 												   #
####################################################################

class PublishedRecords(object):

	'''
	Model to manage the aggregation and retrieval of published records.
	'''

	def __init__(self):

		self.record_group = 0

		# get published jobs
		self.publish_links = JobPublish.objects.all()

		# get set IDs from record group of published jobs
		self.sets = { publish_link.record_group.publish_set_id:publish_link.job for publish_link in self.publish_links }

		# get iterable queryset of records
		self.records = Record.objects.filter(job__job_type = 'PublishJob')

		# set record count
		self.record_count = self.records.count()


	def get_record(self, id):

		'''
		Return single, published record by record.oai_id

		Args:
			id (str): OAI identifier, not internal record_id (pre OAI identifier generation)

		Returns:
			(core.model.Record): single Record instance
		'''

		record_query = self.records.filter(oai_id = id)

		# if one, return
		if record_query.count() == 1:
			return record_query.first()

		else:
			raise Exception('multiple records found for id %s - this is not allowed for published records' % id)



class CombineJob(object):

	'''
	Class to aggregate methods useful for managing and inspecting jobs.  

	Additionally, some methods and workflows for loading a job, inspecting job.job_type, and loading as appropriate
	Combine job.

	Note: There is overlap with the core.models.Job class, but this not being a Django model, allows for a bit 
	more flexibility with __init__.
	'''

	def __init__(self, user=None, job_id=None, parse_job_output=True):

		self.user = user
		self.livy_session = self._get_active_livy_session()
		self.df = None
		self.job_id = job_id

		# if job_id provided, attempt to retrieve and parse output
		if self.job_id:

			# retrieve job
			self.get_job(self.job_id)


	def default_job_name(self):

		'''
		Method to provide default job name based on class type and date

		Args:
			None

		Returns:
			(str): formatted, default job name
		'''

		return '%s @ %s' % (type(self).__name__, datetime.datetime.now().isoformat())


	@staticmethod
	def get_combine_job(job_id):

		'''
		Method to retrieve job, and load as appropriate Combine Job type.

		Args:
			job_id (int): Job ID in DB

		Returns:
			([
				core.models.HarvestJob,
				core.models.TransformJob,
				core.models.MergeJob,
				core.models.PublishJob
			])
		'''

		# get job from db
		j = Job.objects.get(pk=job_id)

		# using job_type, return instance of approriate job type
		return globals()[j.job_type](job_id=job_id)


	def _get_active_livy_session(self):

		'''
		Method to retrieve active livy session

		Args:
			None

		Returns:
			(core.models.LivySession)
		'''

		# check for single, active livy session from LivyClient
		livy_sessions = LivySession.objects.filter(active=True)

		# if single session, confirm active or starting
		if livy_sessions.count() == 1:
			
			livy_session = livy_sessions.first()
			logger.debug('single livy session found, confirming running')

			try:
				livy_session_status = LivyClient().session_status(livy_session.session_id)
				if livy_session_status.status_code == 200:
					status = livy_session_status.json()['state']
					if status in ['starting','idle','busy']:
						# return livy session
						return livy_session
					
			except:
				logger.debug('could not confirm session status')

		elif livy_sessions.count() == 0:
			logger.debug('no active livy sessions found')
			return False


	def start_job(self):

		'''
		Starts job, sends to prepare_job() for child classes

		Args:
			None

		Returns:
			None
		'''

		# if active livy session
		if self.livy_session:
			self.prepare_job()

		else:
			logger.debug('could not submit livy job, not active livy session found')
			return False


	def submit_job_to_livy(self, job_code, job_output):

		'''
		Using LivyClient, submit actual job code to Spark.  For the most part, Combine Jobs have the heavy lifting of 
		their Spark code in core.models.spark.jobs, but this spark code is enough to fire those.

		Args:
			job_code (str): String of python code to submit to Spark
			job_output (str): location for job output (NOTE: No longer used)

		Returns:
			None
				- sets attributes to self
		'''

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
		self.job.save()


	def get_job(self, job_id):

		'''
		Retrieve Job from DB

		Args:
			job_id (int): Job ID

		Returns:
			(core.models.Job)
		'''

		self.job = Job.objects.filter(id=job_id).first()


	def get_record(self, id, is_oai=False):

		'''
		Convenience method to return single record from job.

		Args:
			id (str): string of record ID
			is_oai (bool): If True, use provided ID to search record.oai_id. Defaults to False
		'''

		if is_oai:
			record_query = Record.objects.filter(job=self.job).filter(oai_id=id)
		else:
			record_query = Record.objects.filter(job=self.job).filter(record_id=id)

		# if only one found
		if record_query.count() == 1:
			return record_query.first()

		# else, return all results
		else:
			return record_query


	def count_indexed_fields(self):

		'''
		Count instances of fields across all documents in a job's index

		Args:
			None

		Returns:
			(dict):
				total_docs: count of total docs
				field_counts (dict): dictionary of fields with counts, uniqueness across index, etc.
		'''

		if es_handle.indices.exists(index='j%s' % self.job_id) and es_handle.search(index='j%s' % self.job_id)['hits']['total'] > 0:

			# get mappings for job index
			es_r = es_handle.indices.get(index='j%s' % self.job_id)
			index_mappings = es_r['j%s' % self.job_id]['mappings']['record']['properties']

			# sort alphabetically that influences results list
			field_names = list(index_mappings.keys())
			field_names.sort()

			# init search
			s = Search(using=es_handle, index='j%s' % self.job_id)

			# return no results, only aggs
			s = s[0]

			# add agg buckets for each field to count total and unique instances
			for field_name in field_names:
				s.aggs.bucket('%s_instances' % field_name, A('filter', Q('exists', field=field_name)))
				s.aggs.bucket('%s_distinct' % field_name, A('cardinality', field='%s.keyword' % field_name))

			# execute search and capture as dictionary
			sr = s.execute()
			sr_dict = sr.to_dict()

			# calc fields percentage and return as list
			field_count = [ 
				{
					'field_name':field,
					'instances':sr_dict['aggregations']['%s_instances' % field]['doc_count'],
					'distinct':sr_dict['aggregations']['%s_distinct' % field]['value'],
					'distinct_ratio':round((sr_dict['aggregations']['%s_distinct' % field]['value'] /\
					 sr_dict['aggregations']['%s_instances' % field]['doc_count']), 4),
					'percentage_of_total_records':round((sr_dict['aggregations']['%s_instances' % field]['doc_count'] /\
					 sr_dict['hits']['total']), 4)
				}
				for field in field_names
			]

			# return
			return {
				'total_docs':sr_dict['hits']['total'],
				'fields':field_count
			}

		else:

			return False


	def field_analysis(self, field_name):

		'''
		For a given field, return all values for that field across a job's index

		Args:
			field_name (str): field name

		Returns:
			(dict): dictionary of values for a field
		'''

		# init search
		s = Search(using=es_handle, index='j%s' % self.job_id)

		# add agg bucket for field values
		s.aggs.bucket(field_name, A('terms', field='%s.keyword' % field_name, size=1000000))

		# return zero
		s = s[0]

		# execute and return aggs
		sr = s.execute()
		return sr.aggs[field_name]['buckets']


	def field_analysis_dt(self, field_name):

		'''
		Helper method to return field analysis as expected for DataTables Ajax data source

		Args:
			field_name (str): field name

		Returns:
			(dict): dictionary of values for a field in DT format
			{
				data: [
					[
						field_name,
						count
					],
					...
				]
			}
		'''

		# get buckets
		buckets = self.field_analysis(field_name)

		# prepare as DT data
		dt_dict = {
			'data': [ [f['key'],f['doc_count']] for f in buckets ]
		}

		# return
		return dt_dict


	def get_indexing_failures(self):

		'''
		Retrieve failures for job indexing process

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet): from IndexMappingFailure model
		'''

		# load indexing failures for this job from DB
		index_failures = IndexMappingFailure.objects.filter(job=self.job)
		return index_failures


	def get_total_input_job_record_count(self):

		'''
		Calc record count sum from all input jobs

		Args:
			None

		Returns:
			(int): count of records
		'''

		if self.job.jobinput_set.count() > 0:
			total_input_record_count = sum([input_job.input_job.record_count for input_job in self.job.jobinput_set.all()])
			return total_input_record_count
		else:
			return None


	def get_detailed_job_record_count(self):

		'''
		Return details of record counts for input jobs, successes, and errors

		Args:
			None

		Returns:
			(dict): Dictionary of record counts
		'''

		r_count_dict = {}

		# get counts
		r_count_dict['records'] = self.job.get_records().count()
		r_count_dict['errors'] = self.job.get_errors().count()

		# include input jobs
		total_input_records = self.get_total_input_job_record_count()
		r_count_dict['input_jobs'] = {
			'total_input_records': total_input_records,
			'jobs':self.job.jobinput_set.all()
		}
		
		# calc error percentags
		if r_count_dict['errors'] != 0:
			r_count_dict['error_percentage'] = round((float(r_count_dict['errors']) / float(total_input_records)), 4)
		else:
			r_count_dict['error_percentage'] = 0.0
		
		# return
		return r_count_dict


	def get_job_output_filename_hash(self):

		'''
		When avro files are saved to disk from Spark, they are given a unique hash for the outputted filenames.
		This method reads the avro files from a Job's output, and extracts this unique hash for use elsewhere.

		Args:
			None

		Returns:
			(str): hash shared by all avro files within a job's output
		'''

		# get list of avro files
		job_output_dir = self.job.job_output.split('file://')[-1]

		try:
			avros = [f for f in os.listdir(job_output_dir) if f.endswith('.avro')]

			if len(avros) > 0:
				job_output_filename_hash = re.match(r'part-r-[0-9]+-(.+?)\.avro', avros[0]).group(1)
				logger.debug('job output filename hash: %s' % job_output_filename_hash)
				return job_output_filename_hash

			elif len(avros) == 0:
				logger.debug('no avro files found in job output directory')
				return False
		except:
			logger.debug('could not load job output to determine filename hash')
			return False
		


class HarvestJob(CombineJob):

	'''
	Harvest records via OAI-PMH endpoint
	Note: Unlike downstream jobs, Harvest does not require an input job
	'''

	def __init__(self,
		job_name=None,
		job_note=None,
		user=None,
		record_group=None,
		oai_endpoint=None,
		overrides=None,
		job_id=None,
		index_mapper=None):

		'''
		Args:
			job_name (str): Name for job
			user (auth.models.User): user that will issue job
			record_group (core.models.RecordGroup): record group instance that will be used for harvest
			oai_endpoint (core.models.OAIEndpoint): OAI endpoint to be used for OAI harvest
			overrides (dict): optional dictionary of overrides to OAI endpoint
			job_id (int): Not set on init, but acquired through self.job.save()
			index_mapper (str): String of index mapper clsas from core.spark.es

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.job_note = job_note
			self.record_group = record_group		
			self.organization = self.record_group.organization
			self.oai_endpoint = oai_endpoint
			self.overrides = overrides
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB and save
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				note = self.job_note,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None
			)
			self.job.save()


	def prepare_job(self):

		'''
		Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

		Args:
			None

		Returns:
			None
				- submits job to Livy
		'''

		# create shallow copy of oai_endpoint and mix in overrides
		harvest_vars = self.oai_endpoint.__dict__.copy()
		harvest_vars.update(self.overrides)

		# prepare job code
		job_code = {
			'code':'from jobs import HarvestSpark\nHarvestSpark.spark_function(spark, endpoint="%(endpoint)s", verb="%(verb)s", metadataPrefix="%(metadataPrefix)s", scope_type="%(scope_type)s", scope_value="%(scope_value)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'endpoint':harvest_vars['endpoint'],
				'verb':harvest_vars['verb'],
				'metadataPrefix':harvest_vars['metadataPrefix'],
				'scope_type':harvest_vars['scope_type'],
				'scope_value':harvest_vars['scope_value'],
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		'''
		return harvest job specific errors
		NOTE: Currently, we are not saving errors from OAI harveset, and so, cannot retrieve...
		'''

		return None



class TransformJob(CombineJob):
	
	'''
	Apply an XSLT transformation to a record group
	'''

	def __init__(self,
		job_name=None,
		job_note=None,
		user=None,
		record_group=None,
		input_job=None,
		transformation=None,
		job_id=None,
		index_mapper=None):

		'''
		Args:
			job_name (str): Name for job
			user (auth.models.User): user that will issue job
			record_group (core.models.RecordGroup): record group instance that will be used for harvest
			input_job (core.models.Job): Job that provides input records for this job's work
			transformation (core.models.Transformation): Transformation scenario to use for transforming records
			job_id (int): Not set on init, but acquired through self.job.save()
			index_mapper (str): String of index mapper clsas from core.spark.es

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.job_note = job_note
			self.record_group = record_group
			self.organization = self.record_group.organization
			self.input_job = input_job
			self.transformation = transformation
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				note = self.job_note,
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
			job_input_link = JobInput(job=self.job, input_job=self.input_job)
			job_input_link.save()


	def prepare_job(self):

		'''
		Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

		Args:
			None

		Returns:
			None
				- submits job to Livy
		'''

		# prepare job code
		job_code = {
			'code':'from jobs import TransformSpark\nTransformSpark.spark_function(spark, transform_filepath="%(transform_filepath)s", input_job_id="%(input_job_id)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'transform_filepath':self.transformation.filepath,				
				'input_job_id':self.input_job.id,
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		'''
		Return errors from Job

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet)
		'''

		return self.job.get_errors()



class MergeJob(CombineJob):
	
	'''
	Merge multiple jobs into a single job
	Note: Merge jobs merge only successful documents from an input job, not the errors
	'''

	def __init__(self,
		job_name=None,
		job_note=None,
		user=None,
		record_group=None,
		input_jobs=None,
		job_id=None,
		index_mapper=None):

		'''
		Args:
			job_name (str): Name for job
			user (auth.models.User): user that will issue job
			record_group (core.models.RecordGroup): record group instance that will be used for harvest
			input_jobs (core.models.Job): Job(s) that provides input records for this job's work
			job_id (int): Not set on init, but acquired through self.job.save()
			index_mapper (str): String of index mapper clsas from core.spark.es

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.job_note = job_note
			self.record_group = record_group
			self.organization = self.record_group.organization
			self.input_jobs = input_jobs
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				note = self.job_note,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None,
				job_details = json.dumps(
					{'publish':
						{
							'publish_job_id':str(self.input_jobs),
						}
					})
			)
			self.job.save()

			# save input job to JobInput table
			for input_job in self.input_jobs:
				job_input_link = JobInput(job=self.job, input_job=input_job)
				job_input_link.save()


	def prepare_job(self):

		'''
		Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

		Args:
			None

		Returns:
			None
				- submits job to Livy
		'''

		# prepare job code
		job_code = {
			'code':'from jobs import MergeSpark\nMergeSpark.spark_function(spark, sc, input_jobs_ids="%(input_jobs_ids)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'input_jobs_ids':str([ input_job.id for input_job in self.input_jobs ]),
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		'''
		Not current implemented from Merge jobs, as primarily just copying of successful records
		'''

		pass



class PublishJob(CombineJob):
	
	'''
	Copy record output from job as published job set
	'''

	def __init__(self,
		job_name=None,
		job_note=None,
		user=None,
		record_group=None,
		input_job=None,
		job_id=None,
		index_mapper=None):

		'''
		Args:
			job_name (str): Name for job
			user (auth.models.User): user that will issue job
			record_group (core.models.RecordGroup): record group instance that will be used for harvest
			input_job (core.models.Job): Job that provides input records for this job's work
			job_id (int): Not set on init, but acquired through self.job.save()
			index_mapper (str): String of index mapper clsas from core.spark.es

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.job_note = job_note
			self.record_group = record_group
			self.organization = self.record_group.organization
			self.input_job = input_job
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				note = self.job_note,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None,
				job_details = json.dumps(
					{'publish':
						{
							'publish_job_id':self.input_job.id,
						}
					})
			)
			self.job.save()

			# save input job to JobInput table
			job_input_link = JobInput(job=self.job, input_job=self.input_job)
			job_input_link.save()

			# save publishing link from job to record_group
			job_publish_link = JobPublish(record_group=self.record_group, job=self.job)
			job_publish_link.save()


	def prepare_job(self):

		'''
		Prepare limited python code that is serialized and sent to Livy, triggering spark jobs from core.spark.jobs

		Args:
			None

		Returns:
			None
				- submits job to Livy
		'''

		# prepare job code
		job_code = {
			'code':'from jobs import PublishSpark\nPublishSpark.spark_function(spark, input_job_id="%(input_job_id)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'input_job_id':self.input_job.id,
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		'''
		Not implemented for Publish jobs, primarily just copying and indexing records
		'''

		pass




