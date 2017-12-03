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

# pandas
import pandas as pd

# django imports
from django.apps import AppConfig
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth import signals
from django.db import models
from django.http import HttpResponse, JsonResponse
from django.dispatch import receiver
from django.utils.encoding import python_2_unicode_compatible
from django.utils.html import format_html
from django.views import View

# Livy
from livy.client import HttpClient

# import elasticsearch and handles
from core.es import es_handle
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.utils import AttrList

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



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

		if self.status in ['initializing','waiting','pending','starting','running','available'] and self.url != None:
			self.refresh_from_livy(save=False)

		# udpate record count if not already calculated
		if self.record_count == 0:

			# if finished, count
			if self.finished:
				self.update_record_count(save=False)

		# update elapsed		
		self.elapsed = self.calc_elapsed()

		# finally, save
		self.save()


	def calc_elapsed(self):

		'''
		Method to calculate how long a job has been running/ran.

		Args:
			None

		Returns:
			(int): elapsed time in seconds
		'''

		# if job_track exists, calc elapsed
		if self.jobtrack_set.count() > 0: 

			# get start time
			job_track = self.jobtrack_set.first()

			# if not finished, determined elapsed until now
			if not self.finished:
				return (datetime.datetime.now() - job_track.start_timestamp.replace(tzinfo=None)).seconds

			# else, if finished, calc time between job_track start and finish
			else:
				return (job_track.finish_timestamp - job_track.start_timestamp).seconds

		# else, return zero
		else:
			return 0


	def elapsed_as_string(self):

		'''
		Method to return elapsed as string for Django templates
		'''

		m, s = divmod(self.elapsed, 60)
		h, m = divmod(m, 60)
		return "%d:%02d:%02d" % (h, m, s)


	def calc_records_per_second(self):

		'''
		Method to calculcate records per second, if total known.
		If running, use current elapsed, if finished, use total elapsed.

		Args:
			None

		Returns:
			(float): records per second, rounded to one dec.
		'''

		try:
			if self.record_count > 0:

				if not self.finished:
					elapsed = self.calc_elapsed()
				else:
					elapsed = self.elapsed
				return round((float(self.record_count) / float(elapsed)),1)

			else:
				return None
		except:
			return None


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

		stime = time.time()

		records = self.record_set.filter(success=1)

		logger.debug('get_records elapsed: %s' % (time.time() - stime))

		# return
		return records

	def get_errors(self):

		'''
		Retrieve records associated with this job if the error field is not blank.

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet)
		'''

		stime = time.time()

		errors = self.record_set.filter(success=0)

		logger.debug('get_errors elapsed: %s' % (time.time() - stime))

		# return
		return errors


	def update_record_count(self, save=True):

		'''
		Get record count from DB, save to self

		Args:
			None

		Returns:
			None
		'''
		
		self.record_count = self.record_set.count()
		
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


	def dpla_mapping(self):

		'''
		Method to return DPLA mapping for this job

		Args:
			None

		Returns:
			(core.models.DPLAJobMap, None): Instance of DPLAJobMap if exists, else None
		'''

		if self.dplajobmap_set.count() == 1:
			return self.dplajobmap_set.first()
		else:
			return False



class JobTrack(models.Model):

	'''
	Model to record information about jobs from Spark context, as not to interfere with model `Job` transactions
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	start_timestamp = models.DateTimeField(null=True, auto_now_add=True)
	finish_timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'JobTrack: job_id #%s' % self.job_id


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
	success = models.BooleanField(default=1)


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
		Return indexed ElasticSearch document as dictionary.
		Search is limited by ES index (Job associated) and record_id

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


class DPLAJobMap(models.Model):

	'''
	#Experiemental#

	Model to map parsed fields from ES index to DPLA record fields.  Values for each DPLA field correspond to an ES
	field name for the associated Job.

	Note: This mapping is meant to serve for preview/QA purposes only, this is currently not a final mapping
	to the DPLA JSON model.

	Inspiration for DPLA fields are taken from here:
	https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/model/DplaMapData.scala
	'''

	# associate mapping with Job
	job = models.ForeignKey(Job, on_delete=models.CASCADE)

	# DPLA fields
	# thumbnails and access
	isShownAt = models.CharField(max_length=255, null=True, default=None)
	preview = models.CharField(max_length=255, null=True, default=None)

	# descriptive metadata
	contributor = models.CharField(max_length=255, null=True, default=None)
	creator = models.CharField(max_length=255, null=True, default=None)
	date = models.CharField(max_length=255, null=True, default=None)
	description = models.CharField(max_length=255, null=True, default=None)
	extent = models.CharField(max_length=255, null=True, default=None)
	format = models.CharField(max_length=255, null=True, default=None)
	genre = models.CharField(max_length=255, null=True, default=None)
	identifier = models.CharField(max_length=255, null=True, default=None)
	language = models.CharField(max_length=255, null=True, default=None)
	place = models.CharField(max_length=255, null=True, default=None)
	publisher = models.CharField(max_length=255, null=True, default=None)
	relation = models.CharField(max_length=255, null=True, default=None)
	rights = models.CharField(max_length=255, null=True, default=None)
	subject = models.CharField(max_length=255, null=True, default=None)
	temporal = models.CharField(max_length=255, null=True, default=None)
	title = models.CharField(max_length=255, null=True, default=None)


	def __str__(self):

		# count mapped fields
		mapped_fields = self.mapped_fields()
		
		return 'DPLA Preview Mapping - job_id: %s, mapped fields: %s' % (self.job.id, len(mapped_fields))


	def all_fields(self):

		'''
		Return list of all potential field mappings
		'''

		all_fields = [ field.name for field in self._meta.get_fields() if field.name not in ['id','job'] ]
		all_fields.sort()
		return all_fields


	def mapped_fields(self):

		'''
		Return dictionary of fields with associated mapping

		Args:
			None

		Returns:
			(dict): dictionary of instance mappings
		'''

		mapped_fields = { 
				field.name: getattr(self, field.name) for field in self._meta.get_fields() 
				if field.name not in ['id','job'] and type(getattr(self, field.name)) == str
			}
		return mapped_fields


	def inverted_mapped_fields(self):

		'''
		Convenience method to invert mapping, using ES field name as key for DPLA field

		Args:
			None

		Returns:
			(dict): dictionary of inverted model instance mapping
		'''
		
		# get mapped fields as dict
		mapped_fields = self.mapped_fields()

		# invert and return
		return {v: k for k, v in mapped_fields.items()}



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

		# create DPLAJobMap instance and save
		djm = DPLAJobMap(
			job = instance
		)
		djm.save()


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


class ESIndex(object):

	'''
	Model to aggregate methods useful for accessing and analyzing ElasticSearch indices
	'''

	def __init__(self, es_index):

		self.es_index = es_index


	def get_index_fields(self):

		'''
		Get list of all fields for index

		Args:
			None

		Returns:
			(list): list of field names
		'''

		if es_handle.indices.exists(index=self.es_index) and es_handle.search(index=self.es_index)['hits']['total'] > 0:

			# get mappings for job index
			es_r = es_handle.indices.get(index=self.es_index)
			self.index_mappings = es_r[self.es_index]['mappings']['record']['properties']

			# sort alphabetically that influences results list
			field_names = list(self.index_mappings.keys())
			field_names.sort()

			return field_names


	def calc_field_metrics(self,
			sr_dict,
			field_name,
			one_per_doc_offset=settings.ONE_PER_DOC_OFFSET
		):

		'''
		Calculate metrics for a given field.

		Args:
			sr_dict (dict): ElasticSearch search results dictionary
			field_name (str): Field name to analyze metrics for
			one_per_doc_offset (float): Offset from 1.0 that is used to guess if field is unique for all documents

		Returns:
			(dict): Dictionary of metrics for given field
		'''
		
		if sr_dict['aggregations']['%s_doc_instances' % field_name]['doc_count'] > 0:
				
			# add that don't require calculation
			field_dict = {
				'field_name':field_name,
				'doc_instances':sr_dict['aggregations']['%s_doc_instances' % field_name]['doc_count'],
				'val_instances':sr_dict['aggregations']['%s_val_instances' % field_name]['value'],
				'distinct':sr_dict['aggregations']['%s_distinct' % field_name]['value']
			}

			# documents without
			field_dict['doc_missing'] = sr_dict['hits']['total'] - field_dict['doc_instances']

			# distinct ratio
			if field_dict['val_instances'] > 0:
				field_dict['distinct_ratio'] = round((field_dict['distinct'] / field_dict['val_instances']), 4)
			else:
				field_dict['distinct_ratio'] = 0.0

			# percentage of total documents with instance of this field
			field_dict['percentage_of_total_records'] = round(
				(field_dict['doc_instances'] / sr_dict['hits']['total']), 4)

			# one, distinct value for this field, for this document
			if field_dict['distinct_ratio'] > (1.0 - one_per_doc_offset) \
			 and field_dict['distinct_ratio'] < (1.0 + one_per_doc_offset) \
			 and len(set([field_dict['doc_instances'], field_dict['val_instances'], sr_dict['hits']['total']])) == 1:
				field_dict['one_distinct_per_doc'] = True
			else:
				field_dict['one_distinct_per_doc'] = False

			# return 
			return field_dict

		# if no instances of field in results, return False
		else:
			return False


	def count_indexed_fields(self,
			cardinality_precision_threshold=settings.CARDINALITY_PRECISION_THRESHOLD
		):

		'''
		Calculate metrics of fields across all document in a job's index:
			- *_doc_instances = how many documents the field exists for
			- *_val_instances = count of total values for that field, across all documents
			- *_distinct = count of distinct values for that field, across all documents

		Note: distinct counts rely on cardinality aggregations from ElasticSearch, but these are not 100 percent
		accurate according to ES documentation:
		https://www.elastic.co/guide/en/elasticsearch/guide/current/_approximate_aggregations.html

		Args:
			cardinality_precision_threshold (int, 0:40-000): Cardinality precision threshold (see note above)

		Returns:
			(dict):
				total_docs: count of total docs
				field_counts (dict): dictionary of fields with counts, uniqueness across index, etc.
		'''

		if es_handle.indices.exists(index=self.es_index) and es_handle.search(index=self.es_index)['hits']['total'] > 0:

			# get field mappings for index
			field_names = self.get_index_fields()

			# init search
			s = Search(using=es_handle, index=self.es_index)

			# return no results, only aggs
			s = s[0]

			# add agg buckets for each field to count total and unique instances
			for field_name in field_names:
				s.aggs.bucket('%s_doc_instances' % field_name, A('filter', Q('exists', field=field_name)))
				s.aggs.bucket('%s_val_instances' % field_name, A('value_count', field='%s.keyword' % field_name))
				s.aggs.bucket('%s_distinct' % field_name, A(
						'cardinality',
						field='%s.keyword' % field_name,
						precision_threshold = cardinality_precision_threshold
					))

			# execute search and capture as dictionary
			sr = s.execute()
			sr_dict = sr.to_dict()

			# calc field percentages and return as list
			'''
			Because this also acts on the `published` ES index, which might contain mappings for fields that no longer
			exist, filter out fields with zero instances.
			'''
			field_count = []
			for field_name in field_names:

					# get metrics and append if field metrics found
					field_metrics = self.calc_field_metrics(sr_dict, field_name)
					if field_metrics:
						field_count.append(field_metrics)

			# return
			return {
				'total_docs':sr_dict['hits']['total'],
				'fields':field_count
			}

		else:
			return False


	def field_analysis(self,
			field_name,
			cardinality_precision_threshold=settings.CARDINALITY_PRECISION_THRESHOLD,
			metrics_only=False
		):

		'''
		For a given field, return all values for that field across a job's index

		Note: distinct counts rely on cardinality aggregations from ElasticSearch, but these are not 100 percent
		accurate according to ES documentation:
		https://www.elastic.co/guide/en/elasticsearch/guide/current/_approximate_aggregations.html

		Args:
			field_name (str): field name
			cardinality_precision_threshold (int, 0:40,000): Cardinality precision threshold (see note above)
			metrics_only (bool): If True, return only field metrics and not values

		Returns:
			(dict): dictionary of values for a field
		'''

		# init search
		s = Search(using=es_handle, index=self.es_index)

		# add aggs buckets for field metrics
		s.aggs.bucket('%s_doc_instances' % field_name, A('filter', Q('exists', field=field_name)))
		s.aggs.bucket('%s_val_instances' % field_name, A('value_count', field='%s.keyword' % field_name))
		s.aggs.bucket('%s_distinct' % field_name, A(
				'cardinality',
				field='%s.keyword' % field_name,
				precision_threshold = cardinality_precision_threshold
			))

		# add agg bucket for field values
		if not metrics_only:
			s.aggs.bucket(field_name, A('terms', field='%s.keyword' % field_name, size=1000000))

		# return zero
		s = s[0]

		# execute and return aggs
		sr = s.execute()

		# get metrics
		field_metrics = self.calc_field_metrics(sr.to_dict(), field_name)

		# prepare and return
		if not metrics_only:
			values = sr.aggs[field_name]['buckets']
		else:
			values = None

		return {
			'metrics':field_metrics,
			'values':values
		}


	# def field_analysis_dt(self, field_name):

	# 	'''
	# 	Helper method to return field analysis as expected for DataTables Ajax data source

	# 	Args:
	# 		field_name (str): field name

	# 	Returns:
	# 		(dict): dictionary of values for a field in DT format
	# 		{
	# 			data: [
	# 				[
	# 					field_name,
	# 					count
	# 				],
	# 				...
	# 			]
	# 		}
	# 	'''

	# 	# get buckets
	# 	buckets = self.field_analysis(field_name)

	# 	# prepare as DT data
	# 	dt_dict = {
	# 		'data': [ [f['key'],f['doc_count']] for f in buckets ]
	# 	}

	# 	# return
	# 	return dt_dict



class PublishedRecords(object):

	'''
	Model to manage the aggregation and retrieval of published records.
	'''

	def __init__(self):

		self.record_group = 0

		# get published jobs
		self.publish_links = JobPublish.objects.all()

		# get set IDs from record group of published jobs
		# self.sets = { publish_link.record_group.publish_set_id:publish_link.job for publish_link in self.publish_links }
		sets = {}
		for publish_link in self.publish_links:
			publish_set_id = publish_link.record_group.publish_set_id
			
			# if set not seen, add as list
			if publish_set_id not in sets.keys():
				sets[publish_set_id] = []

			# add publish job
			sets[publish_set_id].append(publish_link.job)	
		self.sets = sets

		# get iterable queryset of records
		self.records = Record.objects.filter(job__job_type = 'PublishJob')

		# set record count
		self.record_count = self.records.count()

		# setup ESIndex instance
		self.esi = ESIndex('published')


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


	def count_indexed_fields(self):

		'''
		Wrapper for ESIndex.count_indexed_fields
		'''

		# return count
		return self.esi.count_indexed_fields()


	def field_analysis(self, field_name):

		'''
		Wrapper for ESIndex.field_analysis
		'''

		# return field analysis
		return self.esi.field_analysis(field_name)


	# def field_analysis_dt(self, field_name):

	# 	'''
	# 	Wrapper for ESIndex.field_analysis_dt
	# 	'''

	# 	# return field analysis
	# 	return self.esi.field_analysis_dt(field_name)



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

		# setup ESIndex instance
		self.esi = ESIndex('j%s' % self.job_id)

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
		Wrapper for ESIndex.count_indexed_fields
		'''

		# return count
		return self.esi.count_indexed_fields()


	def field_analysis(self, field_name):

		'''
		Wrapper for ESIndex.field_analysis
		'''

		# return field analysis
		return self.esi.field_analysis(field_name)


	# def field_analysis_dt(self, field_name):

	# 	'''
	# 	Wrapper for ESIndex.field_analysis_dt
	# 	'''

	# 	# return field analysis
	# 	return self.esi.field_analysis_dt(field_name)


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

		# DEBUG
		stime = time.time()

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

		# DEBUG
		logger.debug('detailed job record count elapsed: %s' % (time.time() - stime))
		
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
			'code':'from jobs import TransformSpark\nTransformSpark.spark_function(spark, transformation_id="%(transformation_id)s", input_job_id="%(input_job_id)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'transformation_id':self.transformation.id,				
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


####################################################################
# ElasticSearch DataTables connector 							   #
####################################################################

class DTElasticSearch(View):

	'''
	Model to query ElasticSearch and return DataTables ready JSON.
	This model is a Django Class-based view.
	This model is located in core.models, as it still may function seperate from a Django view.

	NOTE: Consider breaking aggregation search to own class, very different approach
	'''

	def __init__(self,
			fields=None,
			es_index=None,
			DTinput={
				'draw':None,
				'start':0,
				'length':10
			}):

		'''
		Args:
			fields (list): list of fields to return from ES index
			es_index (str): ES index
			DTinput (dict): DataTables formatted GET parameters as dictionary

		Returns:
			None
				- sets parameters
		'''

		logger.debug('initiating DTElasticSearch connector')

		# fields to retrieve from index
		self.fields = fields

		# ES index
		self.es_index = es_index

		# dictionary INPUT DataTables ajax
		self.DTinput = DTinput

		# placeholder for query to build
		self.query = None

		# request
		self.request = None

		# dictionary OUTPUT to DataTables
		# self.DToutput = DTResponse().__dict__
		self.DToutput = {
			'draw': None,
			'recordsTotal': None,
			'recordsFiltered': None,
			'data': []
		}
		self.DToutput['draw'] = DTinput['draw']


	def filter(self):

		'''
		Filter based on DTinput paramters

		Args:
			None

		Returns:
			None
				- modifies self.query
		'''

		##################################################################################
		# filtering applied before DataTables input
		##################################################################################
		filter_type = self.request.GET.get('filter_type', None)

		# equals filtering
		if filter_type == 'equals':
			logger.debug('equals type filtering')

			# get fields for filtering
			filter_field = self.request.GET.get('filter_field', None)
			filter_value = self.request.GET.get('filter_value', None)
			
			# determine if including or excluding
			matches = self.request.GET.get('matches', None)
			if matches and matches.lower() == 'true':
				matches = True
			else:
				matches = False

			# filter query
			logger.debug('filtering by field:value: %s:%s' % (filter_field, filter_value))

			if matches:
				# filter where filter_field == filter_value
				logger.debug('filtering to matches')
				self.query = self.query.filter(Q('term', **{'%s.keyword' % filter_field : filter_value}))
			else:
				# filter where filter_field == filter_value AND filter_field exists
				logger.debug('filtering to non-matches')
				self.query = self.query.exclude(Q('term', **{'%s.keyword' % filter_field : filter_value}))
				self.query = self.query.filter(Q('exists', field=filter_field))

		# exists filtering
		elif filter_type == 'exists':
			logger.debug('exists type filtering')

			# get field for filtering
			filter_field = self.request.GET.get('filter_field', None)

			# determine if including or excluding
			exists = self.request.GET.get('exists', None)
			if exists and exists.lower() == 'true':
				exists = True
			else:
				exists = False

			# filter query
			if exists:
				logger.debug('filtering to exists')
				self.query = self.query.filter(Q('exists', field=filter_field))
			else:
				logger.debug('filtering to non-exists')
				self.query = self.query.exclude(Q('exists', field=filter_field))


		##################################################################################
		# filtering applied by DataTables input
		##################################################################################
		# search_string = self.DTinput['search']['value']
		# if search_string != '':
		# 	self.query = self.query.where(
		# 		(self.peewee_model.title.contains(search_string)) |
		# 		(self.peewee_model.abstract.contains(search_string)) |
		# 		(self.peewee_model.identifier.contains(search_string))
		# 	)
		##################################################################################


	def sort(self):
		
		'''
		Sort based on DTinput parameters.

		Note: Sorting is different for the different types of requests made to DTElasticSearch.

		Args:
			None

		Returns:
			None
				- modifies self.query_results
		'''

		# get sort params from DTinput
		# https://bitbucket.org/pigletto/django-datatables-view/src/216fd0db6044d2eef43034f5e905ceff68bdfeaa/django_datatables_view/base_datatable_view.py?at=master&fileviewer=file-view-default#base_datatable_view.py-68:114
		sorting_cols = 0
		sort_key = 'order[{0}][column]'.format(sorting_cols)
		while sort_key in self.DTinput:
			sorting_cols += 1
			sort_key = 'order[{0}][column]'.format(sorting_cols)

		for i in range(sorting_cols):
			# sorting column
			sort_dir = 'asc'
			sort_col = int(self.DTinput.get('order[{0}][column]'.format(i)))
			# sorting order
			sort_dir = self.DTinput.get('order[{0}][dir]'.format(i))

			logger.debug('detected sort: %s / %s' % (sort_col, sort_dir))
		
		# field per doc (ES Search Results)
		if self.search_type == 'fields_per_doc':
			
			# determine if field is sortable
			if sort_col < len(self.fields):

				# if combine_db_id, do not add keyword
				if self.fields[sort_col] == 'combine_db_id':
					sort_field_string = self.fields[sort_col]
				# else, add .keyword
				else:
					sort_field_string = "%s.keyword" % self.fields[sort_col]

				if sort_dir == 'desc':
					sort_field_string = "-%s" % sort_field_string
				logger.debug("sortable field, sorting by %s, %s" % (sort_field_string, sort_dir))			
			else:
				logger.debug("cannot sort by column %s" % sort_col)

			# apply sorting to query
			self.query = self.query.sort(sort_field_string)

		# value per field (DataFrame)
		if self.search_type == 'values_per_field':

			if sort_col < len(self.query_results.columns):
				asc = True
				if sort_dir == 'desc':
					asc = False
				self.query_results = self.query_results.sort_values(self.query_results.columns[sort_col], ascending=asc)


	def paginate(self):

		'''
		Paginate based on DTinput paramters

		Args:
			None

		Returns:
			None
				- modifies self.query
		'''
		
		# using offset (start) and limit (length)
		start = int(self.DTinput['start'])
		length = int(self.DTinput['length'])

		if self.search_type == 'fields_per_doc':
			self.query = self.query[start : (start + length)]

		if self.search_type == 'values_per_field':
			self.query_results = self.query_results[start : (start + length)]


	def to_json(self):

		'''
		Return DToutput as JSON

		Returns:
			(json)
		'''

		return json.dumps(self.DToutput)


	def get(self, request, es_index, search_type):

		'''
		Django Class-based view, GET request.
		Route to appropriate response builder (e.g. fields_per_doc, values_per_field)

		Args:
			request (django.request): request object
			es_index (str): ES index
		'''

		# save parameters to self
		self.request = request
		self.es_index = es_index
		self.DTinput = self.request.GET

		# time respond build
		stime = time.time()

		# return fields per document
		if search_type == 'fields_per_doc':
			self.fields_per_doc()

		# aggregate-based search, count of values per field
		if search_type == 'values_per_field':
			self.values_per_field()

		# end time
		logger.debug('DTElasticSearch calc time: %s' % (time.time()-stime))

		# for all search types, build and return response
		return JsonResponse(self.DToutput)


	def fields_per_doc(self):

		'''
		Perform search to get all fields, for all docs.
		Loops through self.fields, returns rows per ES document with values (or None) for those fields.
		Helpful for high-level understanding of documents for a given query.

		Note: can be used outside of Django context, but must set self.fields first
		'''

		# set search type
		self.search_type = 'fields_per_doc'

		# get field names
		if self.request:
			field_names = self.request.GET.getlist('field_names')
			self.fields = field_names

		# initiate es query
		self.query = Search(using=es_handle, index=self.es_index)

		# get total document count, pre-filtering
		self.DToutput['recordsTotal'] = self.query.count()

		# apply filtering to ES query
		self.filter()

		# apply sorting to ES query
		self.sort()

		# self.sort()
		self.paginate()

		# get document count, post-filtering
		self.DToutput['recordsFiltered'] = self.query.count()

		# execute and retrieve search
		self.query_results = self.query.execute()

		# loop through hits
		for hit in self.query_results.hits:

			# get combine record
			record = Record.objects.get(pk=int(hit.combine_db_id))

			# loop through rows, add to list while handling data types
			row_data = []
			for field in self.fields:
				field_value = getattr(hit, field, None)

				# handle ES lists
				if type(field_value) == AttrList:
					row_data.append(str(field_value))

				# all else, append
				else:
					row_data.append(field_value)

			# place record's org_id, record_group_id, and job_id in front
			row_data = [
					record.job.record_group.organization.id,
					record.job.record_group.id,
					record.job.id
					] + row_data

			# add list to object
			self.DToutput['data'].append(row_data)


	def values_per_field(self):

		'''
		Perform aggregation-based search to get count of values for single field.
		Helpful for understanding breakdown of a particular field's values and usage across documents.

		Note: can be used outside of Django context, but must set self.fields first
		'''

		# set search type
		self.search_type = 'values_per_field'

		# get single field
		if self.request:
			self.fields = self.request.GET.getlist('field_names')
			self.field = self.fields[0]
		else:
			self.field = self.fields[0] # expects only one for this search type, take first

		# initiate es query
		self.query = Search(using=es_handle, index=self.es_index)

		# add agg bucket for field values
		self.query.aggs.bucket(self.field, A('terms', field='%s.keyword' % self.field, size=1000000))

		# return zero
		self.query = self.query[0]

		# apply filtering to ES query
		self.filter()

		# execute search and convert to dataframe
		sr = self.query.execute()
		self.query_results = pd.DataFrame([ val.to_dict() for val in sr.aggs[self.field]['buckets'] ])

		# rearrange columns
		cols = self.query_results.columns.tolist()
		cols = cols[-1:] + cols[:-1]
		self.query_results = self.query_results[cols]

		# get total document count, pre-filtering
		self.DToutput['recordsTotal'] = len(self.query_results)

		# get document count, post-filtering
		self.DToutput['recordsFiltered'] = len(self.query_results)

		# apply sorting to DataFrame
		'''
		Think through if sorting on ES query or resulting Dataframe is better option.
		Might have to be DataFrame, as sorting is not allowed for aggregations in ES when they are string type:
		https://discuss.elastic.co/t/ordering-terms-aggregation-based-on-pipeline-metric/31839/2
		'''
		self.sort()

		# paginate
		self.paginate()

		# loop through field values
		'''
		example row from ES:
		{'doc_count': 3, 'key': 'Frock Coats'}
		'''
		for index, row in self.query_results.iterrows():

			# iterate through columns and place in list
			row_data = [row.key, row.doc_count]

			# add list to object
			self.DToutput['data'].append(row_data)
		























