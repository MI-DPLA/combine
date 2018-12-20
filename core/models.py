# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import ast
import binascii
from collections import OrderedDict
import datetime
import dateutil
import difflib
import django
import gc
import gzip
import hashlib
import inspect
import io
import json
from json import JSONDecodeError
import jsonschema
import logging
from lxml import etree, isoschematron
import os
import pdb
import polling
import requests
import shutil
import sickle
import subprocess
from sqlalchemy import create_engine
import re
import tarfile
import textwrap
import time
from types import ModuleType
import urllib.parse
import uuid
from xmlrpc import client as xmlrpc_client
import xmltodict
import zipfile

# pyjxslt
import pyjxslt

# pandas
import pandas as pd

# django imports
from django.apps import AppConfig
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth import signals
from django.contrib.sessions.backends.db import SessionStore
from django.contrib.sessions.models import Session
from django.core import serializers
from django.core.exceptions import ObjectDoesNotExist
from django.core.urlresolvers import reverse
from django.db import connection, models, transaction
from django.db.models import Count
from django.http import HttpResponse, JsonResponse
from django.http.request import QueryDict
from django.dispatch import receiver
from django.utils.encoding import python_2_unicode_compatible
from django.utils.html import format_html
from django.utils.datastructures import MultiValueDict
from django.views import View

# import xml2kvp
from core.xml2kvp import XML2kvp

# import background tasks
from core import tasks

# Livy
from livy.client import HttpClient

# import elasticsearch and handles
from core.es import es_handle
import elasticsearch as es
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search, A, Q
from elasticsearch_dsl.utils import AttrList

# sxsdiff
from sxsdiff import DiffCalculator
from sxsdiff.generators.github import GitHubStyledGenerator

# import mongo dependencies
from core.mongo import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

# import ElasticSearch BaseMapper and PythonUDFRecord
from core.spark.utils import PythonUDFRecord

# AWS
import boto3

# celery
from celery.result import AsyncResult
from celery.task.control import revoke

# toposort
from toposort import toposort, toposort_flatten, CircularDependencyError


####################################################################
# Django ORM 													   #
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
		self.sparkUiUrl = '%s:%s' % (settings.APP_HOST, spark_ui_port)

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

		# retrieve active livy session and refresh
		active_ls = LivySession.get_active_session()
		active_ls.refresh_from_livy(save=False)

		# if passed session id matches and status is idle or busy
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



class Organization(models.Model):

	'''
	Model to manage Organizations in Combine.
	Organizations contain Record Groups, and are the highest level of organization in Combine.
	'''

	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255, blank=True)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)
	for_analysis = models.BooleanField(default=0)


	def __str__(self):
		return 'Organization: %s' % self.name


	def total_record_count(self):

		'''
		Method to determine total records under this Org
		'''

		total_record_count = 0

		# loop through record groups
		for rg in self.recordgroup_set.all():

			# loop through jobs
			for job in rg.job_set.all():

				total_record_count += job.record_count

		# return
		return total_record_count



class RecordGroup(models.Model):

	'''
	Model to manage Record Groups in Combine.
	Record Groups are members of Organizations, and contain Jobs
	'''

	organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255, null=True, default=None, blank=True)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)
	# publish_set_id = models.CharField(max_length=128, null=True, default=None, blank=True)
	for_analysis = models.BooleanField(default=0)


	def __str__(self):
		return 'Record Group: %s' % self.name


	def get_jobs_lineage(self):

		'''
		Method to generate structured data outlining the lineage of jobs for this Record Group.
		Will use Combine DB ID as node identifiers.

		Args:
			None

		Returns:
			(dict): lineage dictionary of nodes (jobs) and edges (input jobs as edges)
		'''

		# debug
		stime = time.time()

		# create record group lineage dictionary
		ld = {'edges':[], 'nodes':[]}

		# get all jobs
		record_group_jobs = self.job_set.order_by('-id').all()

		# loop through jobs
		for job in record_group_jobs:
				job_ld = job.get_lineage()
				ld['edges'].extend(job_ld['edges'])
				ld['nodes'].extend(job_ld['nodes'])

		# filter for unique
		ld['nodes'] = list({node['id']:node for node in ld['nodes']}.values())
		ld['edges'] = list({edge['id']:edge for edge in ld['edges']}.values())

		# sort by id
		ld['nodes'].sort(key=lambda x: x['id'])
		ld['edges'].sort(key=lambda x: x['id'])

		# return
		logger.debug('lineage calc time elapsed: %s' % (time.time()-stime))
		return ld


	def published_jobs(self):

		# get published jobs for rg
		return self.job_set.filter(published=True)


	def is_published(self):

		'''
		Method to determine if a Job has been published for this RecordGroup

		Args:
			None

		Returns:
			(bool): if a job has been published for this RecordGroup, return True, else False
		'''

		# get jobs for rg
		published = self.published_jobs()

		# return True/False
		if published.count() == 0:
			return False
		else:
			return True


	def total_record_count(self):

		'''
		Method to count total records under this RG
		'''

		total_record_count = 0

		# loop through jobs
		for job in self.job_set.all():

			total_record_count += job.record_count

		# return
		return total_record_count



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
	publish_set_id = models.CharField(max_length=255, null=True, default=None, blank=True)
	job_details = models.TextField(null=True, default=None)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)
	note = models.TextField(null=True, default=None)
	elapsed = models.IntegerField(null=True, default=0)
	deleted = models.BooleanField(default=0)


	def __str__(self):
		return '%s, Job #%s' % (self.name, self.id)


	def job_type_family(self):

		'''
		Method to return high-level job type from Harvest, Transform, Merge, Publish

		Args:
			None

		Returns:
			(str, ['HarvestJob', 'TransformJob', 'MergeJob', 'PublishJob']): String of high-level job type
		'''

		# get class hierarchy of job
		class_tree = inspect.getmro(globals()[self.job_type])

		# handle Harvest determination
		if HarvestJob in class_tree:
			return class_tree[-3].__name__

		# else, return job_type untouched
		else:
			return self.job_type


	def update_status(self):

		'''
		Method to udpate job information based on status from Livy.
		Jobs marked as deleted are not updated.

		Args:
			None

		Returns:
			None
				- updates status, record_count, elapsed (soon)
		'''

		# if not deleted
		if not self.deleted:

			# if job in various status, and not finished, ping livy
			if self.status in ['initializing','waiting','pending','starting','running','available','gone']\
			and self.url != None\
			and not self.finished:

				logger.debug('pinging Livy for Job status: %s' % self)
				self.refresh_from_livy(save=False)

			# udpate record count if not already calculated
			if self.record_count == 0:

				# if finished, count
				if self.finished:

					# update record count
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
				try:
					return (job_track.finish_timestamp - job_track.start_timestamp).seconds
				except Exception as e:
					logger.debug('error with calculating elapsed: %s' % str(e))
					return 0

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

		# if status_code 400 or 404, set as gone
		if livy_response.status_code in [400,404]:

			self.status = 'available'
			self.finished = True

			# update
			if save:
				self.save()

		elif livy_response.status_code == 200:

			# set response
			self.response = livy_response.content

			# parse response
			response = livy_response.json()
			headers = livy_response.headers

			# update Livy information
			self.status = response['state']

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


	def get_spark_jobs(self):

		'''
		Attempt to retrieve associated jobs from Spark Application API
		'''

		# get active livy session, and refresh, which contains spark_app_id as appId
		ls = LivySession.get_active_session()

		if ls and type(ls) == LivySession:

			# if appId not set, attempt to retrieve
			if not ls.appId:
				ls.refresh_from_livy()

			# get list of Jobs, filter by jobGroup for this Combine Job
			try:
				filtered_jobs = SparkAppAPIClient.get_spark_jobs_by_jobGroup(ls, ls.appId, self.id)
			except:
				logger.warning('trouble retrieving Jobs from Spark App API')
				filtered_jobs = []
			if len(filtered_jobs) > 0:
				return filtered_jobs
			else:
				return None

		else:
			return False


	def has_spark_failures(self):

		'''
		Look for failure in spark jobs associated with this Combine Job
		'''

		# get spark jobs
		spark_jobs = self.get_spark_jobs()

		if spark_jobs:
			failed = [ job for job in spark_jobs if job['status'] == 'FAILED' ]
			if len(failed) > 0:
				return failed
			else:
				return False
		else:
			return None


	def get_records(self, success=True):

		'''
		Retrieve records associated with this job from Mongo

		Args:
			success (boolean): filter records on success column by this arg
				- passing None will return unfiltered (success and failures)

		Returns:
			(django.db.models.query.QuerySet)
		'''

		if success == None:
			records = Record.objects(job_id=self.id)

		else:
			records = Record.objects(job_id=self.id, success=success)

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

		errors = Record.objects(job_id=self.id, success=False)

		# return
		return errors


	def update_record_count(self, save=True):

		'''
		Get record count from Mongo from Record table, filtering by job_id

		Args:
			save (bool): Save instance on calculation

		Returns:
			None
		'''

		# det detailed record count
		drc = self.get_detailed_job_record_count(force_recount=True)

		# update Job record count
		self.record_count = drc['records'] + drc['errors']

		# if job has single input ID, and that is still None, set to record count
		if self.jobinput_set.count() == 1:
			ji = self.jobinput_set.first()
			if ji.passed_records == None:
				ji.passed_records = self.record_count
				ji.save()

		# if save, save
		if save:
			self.save()


	def get_total_input_job_record_count(self):

		'''
		Calc record count sum from all input jobs, factoring in whether record input validity was all, valid, or invalid

		Args:
			None

		Returns:
			(int): count of records
		'''

		if self.jobinput_set.count() > 0:

			# init dict
			input_jobs_dict = {
				'total_input_record_count':0
			}

			# loop through input jobs
			for input_job in self.jobinput_set.all():

				# bump count
				if input_job.passed_records != None:
					input_jobs_dict['total_input_record_count'] += input_job.passed_records

			# return
			return input_jobs_dict
		else:
			return None


	def get_detailed_job_record_count(self, force_recount=False):

		'''
		Return details of record counts for input jobs, successes, and errors

		Args:
			force_recount (bool): If True, force recount from db

		Returns:
			(dict): Dictionary of record counts
		'''

		# debug
		stime = time.time()

		if 'detailed_record_count' in self.job_details_dict.keys() and not force_recount:
			logger.debug('total detailed record count retrieve elapsed: %s' % (time.time()-stime))
			return self.job_details_dict['detailed_record_count']

		else:

			r_count_dict = {}

			# get counts
			r_count_dict['records'] = self.get_records().count()
			r_count_dict['errors'] = self.get_errors().count()

			# include input jobs
			r_count_dict['input_jobs'] = self.get_total_input_job_record_count()

			# calc success percentages, based on records ratio to job record count (which includes both success and error)
			if r_count_dict['records'] != 0:
				r_count_dict['success_percentage'] = round((float(r_count_dict['records']) / float(r_count_dict['records'])), 4)
			else:
				r_count_dict['success_percentage'] = 0.0

			# saving to job_details
			self.update_job_details({'detailed_record_count':r_count_dict})

			# return
			logger.debug('total detailed record count calc elapsed: %s' % (time.time()-stime))
			return r_count_dict


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
		return '%s/organizations/%s/record_group/%s/jobs/indexing/%s' % (
			settings.BINARY_STORAGE.rstrip('/'), self.record_group.organization.id, self.record_group.id, self.id)


	def get_lineage(self):

		'''
		Method to retrieve lineage of self.
			- creates nodes and edges dictionary of all "upstream" Jobs
		'''

		# lineage dict
		ld = {'nodes':[],'edges':[]}

		# get validation results for self
		validation_results = self.validation_results()

		# prepare node dictionary
		node_dict = {
				'id':self.id,
				'name':self.name,
				'record_group_id':None,
				'org_id':None,
				'job_type':self.job_type,
				'job_status':self.status,
				'is_valid':validation_results['verdict'],
				'deleted':self.deleted
			}

		# if not Analysis job, add org and record group
		if self.job_type != 'AnalysisJob':
			node_dict['record_group_id'] = self.record_group.id
			node_dict['org_id'] = self.record_group.organization.id

		# add self to lineage dictionary
		ld['nodes'].append(node_dict)

		# update lineage dictionary recursively
		self._get_parent_jobs(self, ld)

		# return
		return ld


	def _get_parent_jobs(self, job, ld):

		'''
		Method to recursively find parent jobs and add to lineage dictionary

		Args:
			job (core.models.Job): job to derive all upstream jobs from
			ld (dict): lineage dictionary

		Returns:
			(dict): lineage dictionary, updated with upstream parents
		'''

		# get parent job(s)
		parent_job_links = job.jobinput_set.all() # reverse many to one through JobInput model

		# if parent jobs found
		if parent_job_links.count() > 0:

			# loop through
			for link in parent_job_links:

				# get parent job proper
				pj = link.input_job

				# add as node, if not already added to nodes list
				if pj.id not in [ node['id'] for node in ld['nodes'] ]:

					# get validation results and add to node
					validation_results = pj.validation_results()

					# prepare node dictionary
					node_dict = {
						'id':pj.id,
						'name':pj.name,
						'record_group_id':None,
						'org_id':None,
						'job_type':pj.job_type,
						'job_status':self.status,
						'is_valid':validation_results['verdict'],
						'deleted':pj.deleted
						}

					# if not Analysis job, add org and record group
					if pj.job_type != 'AnalysisJob':
						node_dict['record_group_id'] = pj.record_group.id
						node_dict['org_id'] = pj.record_group.organization.id

					# if from another Record Group, note in node
					if pj.record_group != self.record_group:
						node_dict['external_record_group'] = True

					# append to nodes
					ld['nodes'].append(node_dict)

				# set edge directionality
				from_node = pj.id
				to_node = job.id

				# add edge
				edge_id = '%s_to_%s' % (from_node, to_node)
				if edge_id not in [ edge['id'] for edge in ld['edges'] ]:

					if 'input_filters' in self.job_details_dict:

						# check for job specific filters to use for edge
						if 'job_specific' in self.job_details_dict['input_filters'].keys() and str(from_node) in self.job_details_dict['input_filters']['job_specific'].keys():

							logger.debug('found job type specifics for input job: %s, applying to edge' % from_node)

							# get job_spec_dict
							job_spec_dict = self.job_details_dict['input_filters']['job_specific'][str(from_node)]

							# prepare edge dictionary
							try:
								edge_dict = {
									'id':edge_id,
									'from':from_node,
									'to':to_node,
									'input_validity_valve':job_spec_dict['input_validity_valve'],
									'input_numerical_valve':job_spec_dict['input_numerical_valve'],
									'filter_dupe_record_ids':job_spec_dict['filter_dupe_record_ids'],
									'total_records_passed':link.passed_records
								}
								# add es query flag
								if job_spec_dict['input_es_query_valve']:
									edge_dict['input_es_query_valve'] = True
								else:
									edge_dict['input_es_query_valve'] = False

							except:
								edge_dict = {
									'id':edge_id,
									'from':from_node,
									'to':to_node,
									'input_validity_valve':'unknown',
									'input_numerical_valve':None,
									'filter_dupe_record_ids':False,
									'input_es_query_valve':False,
									'total_records_passed':link.passed_records
								}

						# else, use global input job filters
						else:

							# prepare edge dictionary
							try:
								edge_dict = {
									'id':edge_id,
									'from':from_node,
									'to':to_node,
									'input_validity_valve':self.job_details_dict['input_filters']['input_validity_valve'],
									'input_numerical_valve':self.job_details_dict['input_filters']['input_numerical_valve'],
									'filter_dupe_record_ids':self.job_details_dict['input_filters']['filter_dupe_record_ids'],
									'total_records_passed':link.passed_records
								}
								# add es query flag
								if self.job_details_dict['input_filters']['input_es_query_valve']:
									edge_dict['input_es_query_valve'] = True
								else:
									edge_dict['input_es_query_valve'] = False

							except:
								edge_dict = {
									'id':edge_id,
									'from':from_node,
									'to':to_node,
									'input_validity_valve':'unknown',
									'input_numerical_valve':None,
									'filter_dupe_record_ids':False,
									'input_es_query_valve':False,
									'total_records_passed':link.passed_records
								}

					else:

						logger.debug('no input filters were found for job: %s' % self.id)
						edge_dict = {
							'id':edge_id,
							'from':from_node,
							'to':to_node,
							'input_validity_valve':'unknown',
							'input_numerical_valve':None,
							'filter_dupe_record_ids':False,
							'input_es_query_valve':False,
							'total_records_passed':link.passed_records
						}

					ld['edges'].append(edge_dict)

				# recurse
				self._get_parent_jobs(pj, ld)


	@staticmethod
	def get_all_jobs_lineage(
		organization=None,
		record_group=None,
		jobs_query_set=None,
		exclude_analysis_jobs=True):

		'''
		Static method to get lineage for all Jobs
			- used for all jobs and input select views

		Args:
			organization(core.models.Organization): Organization to filter results by
			record_group(core.models.RecordGroup): RecordGroup to filter results by
			jobs_query_set(django.db.models.query.QuerySet): optional pre-constructed Job model QuerySet

		Returns:
			(dict): lineage dictionary of Jobs
		'''

		# if Job QuerySet provided, use
		if jobs_query_set:
			jobs = jobs_query_set

		# else, construct Job QuerySet
		else:
			# get all jobs
			jobs = Job.objects.all()

			# if Org provided, filter
			if organization:
				jobs = jobs.filter(record_group__organization=organization)

			# if RecordGroup provided, filter
			if record_group:
				jobs = jobs.filter(record_group=record_group)

			# if excluding analysis jobs
			if exclude_analysis_jobs:
				jobs = jobs.exclude(job_type='AnalysisJob')

		# create record group lineage dictionary
		ld = {'edges':[], 'nodes':[]}

		# loop through jobs
		for job in jobs:
				job_ld = job.get_lineage()
				ld['edges'].extend(job_ld['edges'])
				ld['nodes'].extend(job_ld['nodes'])

		# filter for unique
		ld['nodes'] = list({node['id']:node for node in ld['nodes']}.values())
		ld['edges'] = list({edge['id']:edge for edge in ld['edges']}.values())

		# sort by id
		ld['nodes'].sort(key=lambda x: x['id'])
		ld['edges'].sort(key=lambda x: x['id'])

		# return
		return ld


	def validation_results(self, force_recount=False):

		'''
		Method to return boolean whether job passes all/any validation tests run

		Args:
			None

		Returns:
			(dict):
				verdict (boolean): True if all tests passed, or no tests performed, False is any fail
				failure_count (int): Total number of distinct Records with 1+ validation failures
				validation_scenarios (list): QuerySet of associated JobValidation
		'''

		# return dict
		results = {
			'verdict':True,
			'passed_count':self.record_count,
			'failure_count':0,
			'validation_scenarios':[]
		}

		# if not finished, don't bother counting at all
		if not self.finished:
			return results

		# no validation tests run, return True
		if self.jobvalidation_set.count() == 0:
			return results

		# check if already calculated, and use
		elif 'validation_results' in self.job_details_dict.keys() and not force_recount:
			return self.job_details_dict['validation_results']

		# validation tests run, loop through
		else:

			# determine total number of distinct Records with 1+ validation failures
			results['failure_count'] = Record.objects(job_id=self.id, valid=False).count()

			# if failures found
			if results['failure_count'] > 0:
				# set result to False
				results['verdict'] = False
				# subtract failures from passed
				results['passed_count'] -= results['failure_count']

			# add validation scenario ids
			for jv in self.jobvalidation_set.all():
				results['validation_scenarios'].append(jv.validation_scenario_id)

			# save to job_details
			logger.debug("saving validation results for %s to job_details" % self)
			self.update_job_details({'validation_results':results})

			# return
			return results


	def get_dpla_bulk_data_matches(self):

		'''
		Method to update counts and return overview of results of DPLA Bulk Data matching
		'''

		if self.finished:

			# check job_details for dbdm key in job_details, indicating bulk data check
			if 'dbdm' in self.job_details_dict.keys() and 'dbdd' in self.job_details_dict['dbdm'].keys() and self.job_details_dict['dbdm']['dbdd'] != None:

				# get dbdm
				dbdm = self.job_details_dict.get('dbdm', False)

				try:

					# retrieve DBDD
					dbdd = DPLABulkDataDownload.objects.get(pk=dbdm['dbdd'])

					# get misses and matches, counting if not yet done
					if dbdm['matches'] == None and dbdm['misses'] == None:

						# matches
						dbdm['matches'] = self.get_records().filter(dbdm=True).count()

						# misses
						dbdm['misses'] = self.get_records().filter(dbdm=False).count()

						# update job details
						self.update_job_details({'dbdm':dbdm})

					# return dict
					return {
						'dbdd':dbdd,
						'matches':dbdm['matches'],
						'misses': dbdm['misses']
					}

				except Exception as e:
					logger.debug('Job claims to have dbdd, but encountered error while retrieving: %s' % str(e))
					return {}

			else:
				logger.debug('DPLA Bulk comparison not run, or no matches found.')
				return False

		else:
			return False


	def drop_es_index(self, clear_mapped_field_analysis=True):

		'''
		Method to drop associated ES index
		'''

		# remove ES index if exists
		try:
			if es_handle.indices.exists('j%s' % self.id):
				logger.debug('removing ES index: j%s' % self.id)
				es_handle.indices.delete('j%s' % self.id)
				logger.debug('ES index remove')
		except:
			logger.debug('could not remove ES index: j%s' % self.id)


		# remove saved mapped_field_analysis in job_details, if exists
		if clear_mapped_field_analysis:
			job_details = self.job_details_dict
			if 'mapped_field_analysis' in job_details.keys():
				job_details.pop('mapped_field_analysis')
				self.job_details = json.dumps(job_details)
				with transaction.atomic():
					self.save()


	def get_fm_config_json(self, as_dict=False):

		'''
		Method to return used Field Mapper Configuration as JSON
		'''

		try:

			# get field mapper config as dictionary
			fm_dict = self.job_details_dict['field_mapper_config']

			# return as JSON
			if as_dict:
				return fm_dict
			else:
				return json.dumps(fm_dict)

		except Exception as e:
			logger.debug('error retrieving fm_config_json: %s' % str(e))
			return False


	@property
	def job_details_dict(self):

		'''
		Property to return job_details json as dictionary
		'''

		if self.job_details:
			return json.loads(self.job_details)
		else:
			return {}


	def update_job_details(self, update_dict, save=True):

		'''
		Method to update job_details by providing a dictionary to update with, optiontally saving

		Args:
			update_dict (dict): dictionary of key/value pairs to update job_details JSON with
			save (bool): if True, save Job instance
		'''

		# parse job details
		try:
			if self.job_details:
				job_details = json.loads(self.job_details)
			elif not self.job_details:
				job_details = {}
		except:
			logger.debug('could not parse job details')
			raise Exception('could not parse job details')

		# update details with update_dict
		job_details.update(update_dict)

		# if saving
		if save:
			self.job_details = json.dumps(job_details)
			self.save()

		# return
		return job_details


	def publish(self, publish_set_id=None):

		'''
		Method to publish Job
			- remove 'published_field_counts' doc from combine.misc Mongo collection

		Args:
			publish_set_id (str): identifier to group published Records
		'''

		# debug
		logger.debug('publishing job #%s, with publish_set_id %s' % (self.id, publish_set_id))

		# remove previously saved published field counts
		mc_handle.combine.misc.delete_one({'_id':'published_field_counts'})

		# mongo db command
		result = mc_handle.combine.record.update_many({'job_id':self.id},{'$set':{'published':True, 'publish_set_id':publish_set_id}}, upsert=False)
		logger.debug('Matched %s, marked as published %s' % (result.matched_count, result.modified_count))

		# set self as published
		self.refresh_from_db()
		self.publish_set_id = publish_set_id
		self.published = True
		self.save()

		# add to job details
		self.update_job_details({
				'published':{
					'status':True,
					'publish_set_id':self.publish_set_id
				}
			})

		# return
		return True


	def unpublish(self):

		'''
		Method to unpublish Job
			- remove 'published_field_counts' doc from combine.misc Mongo collection
		'''

		# debug
		logger.debug('unpublishing job #%s' % (self.id))

		# remove previously saved published field counts
		mc_handle.combine.misc.delete_one({'_id':'published_field_counts'})

		# mongo db command
		result = mc_handle.combine.record.update_many({'job_id':self.id},{'$set':{'published':False, 'publish_set_id':None}}, upsert=False)
		logger.debug('Matched %s, marked as unpublished %s' % (result.matched_count, result.modified_count))

		# set self as publish
		self.refresh_from_db()
		self.publish_set_id = None
		self.published = False
		self.save()

		# add to job details
		self.update_job_details({
				'published':{
					'status':False,
					'publish_set_id':None
				}
			})

		# return
		return True


	def remove_records_from_db(self):

		'''
		Method to remove records from DB, fired as pre_delete signal
		'''

		logger.debug('removing records from db')
		mc_handle.combine.record.delete_many({'job_id':self.id})
		logger.debug('removed records from db')
		return True


	def remove_validations_from_db(self):

		'''
		Method to remove validations from DB, fired as pre_delete signal
			- usually handled by signals, but method left as convenience
		'''

		logger.debug('removing validations from db')
		mc_handle.combine.record_validation.delete_many({'job_id':self.id})
		logger.debug('removed validations from db')
		return True


	def remove_mapping_failures_from_db(self):

		'''
		Method to remove mapping failures from DB, fired as pre_delete signal
		'''

		logger.debug('removing mapping failures from db')
		mc_handle.combine.index_mapping_failure.delete_many({'job_id':self.id})
		logger.debug('removed mapping failures from db')
		return True


	def remove_validation_jobs(self, validation_scenarios=None):

		'''
		Method to remove validation jobs that match validation scenarios provided
			- NOTE: only one validation job should exist per validation scenario per Job
		'''

		# if validation scenarios provided
		if validation_scenarios != None:

			for vs_id in validation_scenarios:

					logger.debug('removing JobValidations for Job %s, using vs_id %s' % (self, vs_id))

					# attempt to retrieve JobValidation
					jvs = JobValidation.objects.filter(validation_scenario=ValidationScenario.objects.get(pk=int(vs_id)), job=self)

					# if any found, delete all
					if jvs.count() > 0:
						for jv in jvs:
							jv.delete()

		# loop through and delete all
		else:
			for jv in self.jobvalidation_set.all():
				jv.delete()

		# return
		return True


	def get_downstream_jobs(self,
		include_self=True,
		topographic_sort=True,
		depth=None):

		'''
		Method to retrieve downstream jobs, ordered by order required for re-running

		Args:
			include_self (bool): Boolean to include self as first in returned set
			topographic_sort (bool): Boolean to topographically sort returned set
			depth (None, int): None or int depth to recurse
		'''

		def _job_recurse(job_node, rec_depth):

			# bump recurse levels
			rec_depth += 1

			# get children
			downstream_jobs = JobInput.objects.filter(input_job=job_node)

			# if children, re-run
			if downstream_jobs.count() > 0:

				# recurse
				for downstream_job in downstream_jobs:

					# add to sets
					job_set.add(downstream_job.job)

					# recurse
					if depth == None or (depth != None and rec_depth < depth):
						if downstream_job.job not in visited:
							_job_recurse(downstream_job.job, rec_depth)
							visited.add(downstream_job.job)

		# set lists and seed
		visited = set()
		if include_self:
			job_set = {self}
		else:
			job_set = set()

		# recurse
		_job_recurse(self, 0)

		# return topographically sorted
		if topographic_sort:
			return Job._topographic_sort_jobs(job_set)
		else:
			return list(job_set)


	def get_upstream_jobs(self, topographic_sort=True):

		'''
		Method to retrieve upstream jobs
			- placheholder for now
		'''

		pass


	@staticmethod
	def _topographic_sort_jobs(job_set):

		'''
		Method to topographically sort set of Jobs,
		using toposort (https://bitbucket.org/ericvsmith/toposort)

			- informed by cartesian JobInput links that exist between all Jobs in job_set

		Args:
			job_set (set): set of unordered jobs
		'''

		# if single job, return
		if len(job_set) == 1:
			return job_set

		# else, topographically sort
		else:

			# get all lineage edges given Job set
			lineage_edges = JobInput.objects.filter(job__in=job_set, input_job__in=job_set)

			# loop through and build graph
			edge_hash = {}
			for edge in lineage_edges:

				# if not in hash, add
				if edge.job not in edge_hash:
					edge_hash[edge.job] = set()

				# append required input job
				edge_hash[edge.job].add(edge.input_job)

			# topographically sort and return
			topo_sorted_jobs = list(toposort_flatten(edge_hash, sort=False))
			return topo_sorted_jobs


	def stop_job(self, cancel_livy_statement=True, kill_spark_jobs=True):

		'''
		Stop running Job in Livy/Spark
			- cancel Livy statement
				- unnecessary if running Spark app, but helpful if queued by Livy
			- issue "kill" commands to Spark app
				- Livy statement cancels do not stop Spark jobs, but we can kill them manually
		'''

		logger.debug('Stopping Job: %s' % self)

		# get active livy session
		ls = LivySession.get_active_session()

		# if active session
		if ls:

			# send cancel to Livy
			if cancel_livy_statement:
				try:
					livy_response = LivyClient().stop_job(self.url).json()
					logger.debug('Livy statement cancel result: %s' % livy_response)
				except:
					logger.debug('error canceling Livy statement: %s' % self.url)

			# retrieve list of spark jobs, killing two most recent
			# note: repeate x3 if job turnover so quick enough that kill is missed
			if kill_spark_jobs:
				try:
					for x in range(0,3):
						sjs = self.get_spark_jobs()
						sj = sjs[0]
						logger.debug('Killing Spark Job: #%s, %s' % (sj['jobId'],sj.get('description','DESCRIPTION NOT FOUND')))
						kill_result = SparkAppAPIClient.kill_job(ls, sj['jobId'])
						logger.debug('kill result: %s' % kill_result)
				except:
					logger.debug('error killing Spark jobs')

		else:
			logger.debug('active Livy session not found, unable to cancel Livy statement or kill Spark application jobs')


	def add_input_job(self, input_job, job_spec_input_filters=None):

		'''
		Method to add input Job to self
			- add JobInput instance
			- modify self.job_details_dict.input_job_ids

		Args:
			input_job (int,str,core.models.Job): Input Job to add
			job_spec_input_filters (dict): dictionary of Job specific input filters
		'''

		if self.job_type_family() not in ['HarvestJob']:

			# handle types
			if type(input_job) in [int,str]:
				input_job = Job.objects.get(pk=int(input_job))

			# check that inverse relationship does not exist,
			#  resulting in circular dependency
			if JobInput.objects.filter(job=input_job, input_job=self).count() > 0:
				logger.debug('cannot add %s as Input Job, would result in circular dependency' % input_job)
				return False

			# create JobInput instance
			jv = JobInput(job=self, input_job=input_job)
			jv.save()

			# add input_job.id to input_job_ids
			input_job_ids = self.job_details_dict['input_job_ids']
			input_job_ids.append(input_job.id)
			self.update_job_details({'input_job_ids':input_job_ids})

			# if job spec input filteres provided, add
			if job_spec_input_filters != None:
				input_filters = self.job_details_dict['input_filters']
				input_filters['job_specific'][str(input_job.id)] = job_spec_input_filters
				self.update_job_details({'input_filters':input_filters})

			# return
			logger.debug('%s added as input job' % input_job)
			return True

		else:

			logger.debug('cannot add Input Job to Job type: %s' % self.job_type_family())
			return False


	def remove_input_job(self, input_job):

		'''
		Method to remove input Job from self
			- remove JobInput instance
			- modify self.job_details_dict.input_job_ids

		Args:
			input_job (int,str,core.models.Job): Input Job to remove
		'''

		if self.job_type_family() not in ['HarvestJob']:

			# handle types
			if type(input_job) in [int,str]:
				input_job = Job.objects.get(pk=int(input_job))

			# create JobInput instance
			jv = JobInput.objects.filter(job=self, input_job=input_job)
			jv.delete()

			# add input_job.id to input_job_ids
			input_job_ids = self.job_details_dict['input_job_ids']
			try:
				input_job_ids.remove(input_job.id)
			except:
				logger.debug('input job_id not found in input_job_ids: %s' % input_job.id)
			self.update_job_details({'input_job_ids':input_job_ids})

			# check for job in job spec filters, remove if found
			if str(input_job.id) in self.job_details_dict['input_filters']['job_specific'].keys():
				input_filters = self.job_details_dict['input_filters']
				input_filters['job_specific'].pop(str(input_job.id))
				self.update_job_details({'input_filters':input_filters})

			# return
			logger.debug('%s removed as input job' % input_job)
			return True

		else:

			logger.debug('cannot add Input Job to Job type: %s' % self.job_type_family())
			return False


	def remove_as_input_job(self):

		'''
		Remove self from other Job's job_details that see it as input_job
		'''

		logger.debug('removing self from child jobs input_job_ids')

		# get all JobInputs where self is input job
		immediate_children = self.get_downstream_jobs(include_self=False, depth=1)

		# loop through children and remove from job_details.input_job_ids
		for child in immediate_children:
			input_job_ids = child.job_details_dict['input_job_ids']
			input_job_ids.remove(self.id)
			child.update_job_details({'input_job_ids':input_job_ids})



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
	passed_records = models.IntegerField(null=True, default=None)


	def __str__(self):
		return 'JobInputLink: input job #%s for job #%s' % (self.input_job.id, self.job.id)



class OAIEndpoint(models.Model):

	'''
	Model to manage user added OAI endpoints
	'''

	name = models.CharField(max_length=255)
	endpoint = models.CharField(max_length=255)
	verb = models.CharField(max_length=128, null=True, default='ListRecords')
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
	Model to handle "transformation scenarios".	Envisioned to faciliate more than just XSL transformations, but
	currently, only XSLT is handled downstream
	'''

	name = models.CharField(max_length=255)
	payload = models.TextField()
	transformation_type = models.CharField(
		max_length=255,
		choices=[
			('xslt','XSLT Stylesheet'),
			('python','Python Code Snippet'),
			('openrefine','Open Refine Actions')
		]
	)
	filepath = models.CharField(max_length=1024, null=True, default=None, blank=True)
	use_as_include = models.BooleanField(default=False)


	def __str__(self):
		return 'Transformation: %s, transformation type: %s' % (self.name, self.transformation_type)


	def transform_record(self, row):

		'''
		Method to test transformation against a single record.

		Note: The code for self._transform_xslt() and self._transform_python() are similar,
		to staticmethods found in core.spark.jobs.py.	However, because those are running on spark workers,
		in a spark context, it makes it difficult to define once, but use in multiple places.	As such, these
		transformations are recreated here.

		Args:
			row (core.models.Record): Record instance, called "row" here to mirror spark job iterating over DataFrame
		'''

		logger.debug('transforming single record: %s' % row)

		# run appropriate validation based on transformation type
		if self.transformation_type == 'xslt':
			result = self._transform_xslt(row)
		if self.transformation_type == 'python':
			result = self._transform_python(row)
		if self.transformation_type == 'openrefine':
			result = self._transform_openrefine(row)

		# return result
		return result


	def _transform_xslt(self, row):

		try:

			# attempt to parse xslt prior to submitting to pyjxslt
			try:
				parsed_xml = etree.fromstring(self.payload.encode('utf-8'))
			except Exception as e:
				return str(e)

			# transform with pyjxslt gateway
			gw = pyjxslt.Gateway(6767)
			gw.add_transform('xslt_transform', self.payload)
			result = gw.transform('xslt_transform', row.document)
			gw.drop_transform('xslt_transform')

			# return
			return result

		except Exception as e:
			return str(e)


	def _transform_python(self, row):

		try:

			logger.debug('python transformation running')

			# prepare row as parsed document with PythonUDFRecord class
			prtb = PythonUDFRecord(row)

			# get python function from Transformation Scenario
			temp_pyts = ModuleType('temp_pyts')
			exec(self.payload, temp_pyts.__dict__)

			# run transformation
			trans_result = temp_pyts.python_record_transformation(prtb)

			# check that trans_result is a list
			if type(trans_result) != list:
				raise Exception('Python transformation should return a list, but got type %s' % type(trans_result))

			# convert any possible byte responses to string
			if trans_result[2] == True:
				if type(trans_result[0]) == bytes:
					trans_result[0] = trans_result[0].decode('utf-8')
				return trans_result[0]
			if trans_result[2] == False:
				if type(trans_result[1]) == bytes:
					trans_result[1] = trans_result[1].decode('utf-8')
				return trans_result[1]

		except Exception as e:
			return str(e)


	def _transform_openrefine(self, row):

		try:

			# parse or_actions
			or_actions = json.loads(self.payload)

			# load record as prtb
			prtb = PythonUDFRecord(row)

			# loop through actions
			for event in or_actions:

				# handle core/mass-edit
				if event['op'] == 'core/mass-edit':

					# get xpath
					xpath = XML2kvp.k_to_xpath(event['columnName'])
					logger.debug("using xpath value: %s" % xpath)

					# find elements for potential edits
					eles = prtb.xml.xpath(xpath, namespaces=prtb.nsmap)

					# loop through elements
					for ele in eles:

						# loop through edits
						for edit in event['edits']:

							# check if element text in from, change
							if ele.text in edit['from']:
								ele.text = edit['to']

				# handle jython
				if event['op'] == 'core/text-transform' and event['expression'].startswith('jython:'):

					# fire up temp module
					temp_pyts = ModuleType('temp_pyts')

					# parse code
					code = event['expression'].split('jython:')[1]

					# wrap in function and write to temp module
					code = 'def temp_func(value):\n%s' % textwrap.indent(code, prefix='		')
					exec(code, temp_pyts.__dict__)

					# get xpath
					xpath = XML2kvp.k_to_xpath(event['columnName'])
					logger.debug("using xpath value: %s" % xpath)

					# find elements for potential edits
					eles = prtb.xml.xpath(xpath, namespaces=prtb.nsmap)

					# loop through elements
					for ele in eles:
						ele.text = temp_pyts.temp_func(ele.text)

			# re-serialize as trans_result
			return etree.tostring(prtb.xml).decode('utf-8')

		except Exception as e:
			# set trans_result tuple
			return str(e)


	def _rewrite_xsl_http_includes(self):

		'''
		Method to check XSL payloads for external HTTP includes,
		if found, download and rewrite

			- do not save self (instance), firing during pre-save signal
		'''

		if self.transformation_type == 'xslt':

			logger.debug('XSLT transformation, checking for external HTTP includes')

			# rewrite flag
			rewrite = False

			# output dir
			transformations_dir = '%s/transformations' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]

			# parse payload
			xsl = etree.fromstring(self.payload.encode('utf-8'))

			# handle empty, global namespace
			_nsmap = xsl.nsmap.copy()
			try:
				global_ns = _nsmap.pop(None)
				_nsmap['global_ns'] = ns0
			except:
				pass

			# xpath query for xsl:include
			includes = xsl.xpath('//xsl:include', namespaces=_nsmap)

			# loop through includes and check for HTTP hrefs
			for i in includes:

				# get href
				href = i.attrib.get('href',False)

				# check for http
				if href:
					if href.lower().startswith('http'):

						logger.debug('external HTTP href found for xsl:include: %s' % href)

						# set flag for rewrite
						rewrite = True

						# download and save to transformations directory on filesystem
						r = requests.get(href)
						filepath = '%s/%s' % (transformations_dir, href.split('/')[-1])
						with open(filepath, 'wb') as f:
							f.write(r.content)

						# rewrite href and add note
						i.attrib['href'] = filepath

			# rewrite if need be
			if rewrite:
				logger.debug('rewriting XSL payload')
				self.payload = etree.tostring(xsl, encoding='utf-8', xml_declaration=True).decode('utf-8')



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



class Record(mongoengine.Document):

	# fields
	combine_id = mongoengine.StringField()
	document = mongoengine.StringField()
	error = mongoengine.StringField()
	fingerprint = mongoengine.IntField()
	job_id = mongoengine.IntField()
	oai_set = mongoengine.StringField()
	publish_set_id = mongoengine.StringField()
	published = mongoengine.BooleanField(default=False)
	record_id = mongoengine.StringField()
	success = mongoengine.BooleanField(default=True)
	transformed = mongoengine.BooleanField(default=False)
	unique = mongoengine.BooleanField(default=True)
	unique_published = mongoengine.BooleanField(default=True)
	valid = mongoengine.BooleanField(default=True)
	dbdm = mongoengine.BooleanField(default=False)
	orig_id = mongoengine.StringField()

	# meta
	meta = {
		'index_options': {},
        'index_background': False,
        'auto_create_index': False,
        'index_drop_dups': False,
		'indexes': [
			{'fields': ['job_id']},
			{'fields': ['record_id']},
			{'fields': ['combine_id']},
			{'fields': ['success']},
			{'fields': ['valid']},
			{'fields': ['published']},
			{'fields': ['publish_set_id']},
			{'fields': ['dbdm']}
		]
	}

	# cached attributes
	_job = None


	def __str__(self):
		return 'Record: %s, record_id: %s, Job: %s' % (self.id, self.record_id, self.job.name)


	# _id shim property
	@property
	def _id(self):
		return self.id


	# define job property
	@property
	def job(self):

		'''
		Method to retrieve Job from Django ORM via job_id
		'''
		if self._job is None:
			try:
				job = Job.objects.get(pk=self.job_id)
			except:
				job = False
			self._job = job
		return self._job


	def get_record_stages(self, input_record_only=False, remove_duplicates=True):

		'''
		Method to return all upstream and downstreams stages of this record

		Args:
			input_record_only (bool): If True, return only immediate record that served as input for this record.
			remove_duplicates (bool): Removes duplicates - handy for flat list of stages,
			but use False to create lineage

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

				logger.debug('upstream jobs found, checking for combine_id')

				# loop through upstream jobs, look for record id
				for upstream_job in upstream_job_query.all():
					upstream_record_query = Record.objects.filter(
							job_id=upstream_job.input_job.id,
							combine_id=self.combine_id
						)

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

				logger.debug('downstream jobs found, checking for combine_id')

				# loop through downstream jobs
				for downstream_job in downstream_job_query.all():

					downstream_record_query = Record.objects.filter(
						job_id=downstream_job.job.id,
						combine_id=self.combine_id
					)

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

		# remove duplicate
		if remove_duplicates:
			record_stages = list(OrderedDict.fromkeys(record_stages))

		# return
		return record_stages


	def get_es_doc(self, drop_combine_fields=False):

		'''
		Return indexed ElasticSearch document as dictionary.
		Search is limited by ES index (Job associated) and combine_id

		Args:
			None

		Returns:
			(dict): ES document
		'''

		# init search
		s = Search(using=es_handle, index='j%s' % self.job_id)
		s = s.query('match', _id=str(self.id))

		# drop combine fields if flagged
		if drop_combine_fields:
			s = s.source(exclude=['combine_id','db_id','fingerprint','publish_set_id','record_id'])

		# execute search and capture as dictionary
		try:
			sr = s.execute()
			sr_dict = sr.to_dict()
		except NotFoundError:
			logger.debug('mapped fields for record not found in ElasticSearch')
			return {}

		# return
		try:
			return sr_dict['hits']['hits'][0]['_source']
		except:
			return {}


	@property
	def mapped_fields(self):

		'''
		Return mapped fields as property
		'''

		return self.get_es_doc()


	@property
	def document_mapped_fields(self):

		'''
		Method to return mapped fields, dropping internal Combine fields
		'''

		return self.get_es_doc(drop_combine_fields=True)


	def get_dpla_mapped_fields(self):

		'''
		Method to return DPLA specific mapped fields from Record's mapped fields
		'''

		# get mapped fields and return filtered
		return {f:v for f,v in self.get_es_doc().items() if f.startswith('dpla_')}


	def parse_document_xml(self):

		'''
		Parse self.document as XML node with etree

		Args:
			None

		Returns:
			(tuple): ((bool) result of XML parsing, (lxml.etree._Element) parsed document)
		'''
		try:
			return (True, etree.fromstring(self.document.encode('utf-8')))
		except Exception as e:
			logger.debug(str(e))
			return (False, str(e))


	def dpla_api_record_match(self, search_string=None):

		'''
		Method to query DPLA API for match against mapped fields
			- querying is an ranked list of fields to consecutively search
			- this method is recursive such that a preformatted search string can be fed back into it

		Args:
			search_string(str): Optional search_string override

		Returns:
			(dict): If match found, return dictionary of DPLA API response
		'''

		# check for DPLA_API_KEY, else return None
		if settings.DPLA_API_KEY:

			# check for any mapped DPLA fields, skipping altogether if none
			mapped_dpla_fields = self.get_dpla_mapped_fields()
			if len(mapped_dpla_fields) > 0:

				# attempt search if mapped fields present and search_string not provided
				if not search_string:

					# ranked search fields
					opinionated_search_fields = [
						('dpla_isShownAt', 'isShownAt'),
						('dpla_title', 'sourceResource.title'),
						('dpla_description', 'sourceResource.description')
					]

					# loop through ranked search fields
					for local_mapped_field, target_dpla_field in opinionated_search_fields:

						# if local_mapped_field in keys
						if local_mapped_field in mapped_dpla_fields.keys():

							# get value for mapped field
							field_value = mapped_dpla_fields[local_mapped_field]

							# if list, loop through and attempt searches
							if type(field_value) == list:

								for val in field_value:
									search_string = urllib.parse.urlencode({target_dpla_field:'"%s"' % val})
									match_results = self.dpla_api_record_match(search_string=search_string)

							# else if string, perform search
							else:
								search_string = urllib.parse.urlencode({target_dpla_field:'"%s"' % field_value})
								match_results = self.dpla_api_record_match(search_string=search_string)


					# parse results
					# count instances of isShownAt, a single one is good enough
					if 'isShownAt' in self.dpla_api_matches.keys() and len(self.dpla_api_matches['isShownAt']) == 1:
						self.dpla_api_doc = self.dpla_api_matches['isShownAt'][0]['hit']

					# otherwise, count all, and if only one, use
					else:
						matches = []
						for field,field_matches in self.dpla_api_matches.items():
							matches.extend(field_matches)

						if len(matches) == 1:
							self.dpla_api_doc = matches[0]['hit']

						else:
							self.dpla_api_doc = None

					# return
					return self.dpla_api_doc

				else:
					# prepare search query
					api_q = requests.get(
						'https://api.dp.la/v2/items?%s&api_key=%s' % (search_string, settings.DPLA_API_KEY))

					# attempt to parse response as JSON
					try:
						api_r = api_q.json()
					except:
						logger.debug('DPLA API call unsuccessful: code: %s, response: %s' % (api_q.status_code, api_q.content))
						self.dpla_api_doc = None
						return self.dpla_api_doc

					# if count present
					if type(api_r) == dict:
						if 'count' in api_r.keys():

							# response
							if api_r['count'] >= 1:

								# add matches to matches
								field,value = search_string.split('=')
								value = urllib.parse.unquote(value)

								# check for matches attr
								if not hasattr(self, "dpla_api_matches"):
									self.dpla_api_matches = {}

								# add mapped field used for searching
								if field not in self.dpla_api_matches.keys():
									self.dpla_api_matches[field] = []

								# add matches for values searched
								for doc in api_r['docs']:
									self.dpla_api_matches[field].append({
											"search_term":value,
											"hit":doc
										})

							else:
								if not hasattr(self, "dpla_api_matches"):
									self.dpla_api_matches = {}
						else:
							logger.debug('non-JSON response from DPLA API: %s' % api_r)
							if not hasattr(self, "dpla_api_matches"):
									self.dpla_api_matches = {}

					else:
						logger.debug(api_r)

		# return None by default
		self.dpla_api_doc = None
		return self.dpla_api_doc


	def get_validation_errors(self):

		'''
		Return validation errors associated with this record
		'''

		vfs = RecordValidation.objects.filter(record_id=self.id)
		return vfs


	def document_pretty_print(self):

		'''
		Method to return document as pretty printed (indented) XML
		'''

		# return as pretty printed string
		parsed_doc = self.parse_document_xml()
		if parsed_doc[0]:
			return etree.tostring(parsed_doc[1], pretty_print=True)
		else:
			raise Exception(parsed_doc[1])


	def get_lineage_url_paths(self):

		'''
		get paths of Record, Record Group, and Organzation
		'''

		record_lineage_urls = {
			'record':{
					'name':self.record_id,
					'path':reverse('record', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id, 'job_id':self.job.id, 'record_id':self.id})
				},
			'job':{
					'name':self.job.name,
					'path':reverse('job_details', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id, 'job_id':self.job.id})
				},
			'record_group':{
					'name':self.job.record_group.name,
					'path':reverse('record_group', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id})
				},
			'organization':{
					'name':self.job.record_group.organization.name,
					'path':reverse('organization', kwargs={'org_id':self.job.record_group.organization.id})
				}
		}

		return record_lineage_urls


	def get_dpla_bulk_data_match(self):

		'''
		Method to return single DPLA Bulk Data Match
		'''

		return DPLABulkDataMatch.objects.filter(record=self)


	def get_input_record_diff(self, output='all', combined_as_html=False):

		'''
		Method to return a string diff of this record versus the input record
			- this is primarily helpful for Records from Transform Jobs
			- use self.get_record_stages(input_record_only=True)[0]

		Returns:
			(str|list): results of Record documents diff, line-by-line
		'''

		# check if Record has input Record
		irq = self.get_record_stages(input_record_only=True)
		if len(irq) == 1:
			logger.debug('single, input Record found: %s' % irq[0])

			# get input record
			ir = irq[0]

			# check if fingerprints the same
			if self.fingerprint != ir.fingerprint:

				logger.debug('fingerprint mismatch, returning diffs')
				return self.get_record_diff(
						input_record=ir,
						output=output,
						combined_as_html=combined_as_html
					)

			# else, return None
			else:
				logger.debug('fingerprint match, returning None')
				return None

		else:
			return False


	def get_record_diff(self,
			input_record=None,
			xml_string=None,
			output='all',
			combined_as_html=False,
			reverse_direction=False
		):

		'''
		Method to return diff of document XML strings

		Args;
			input_record (core.models.Record): use another Record instance to compare diff
			xml_string (str): provide XML string to provide diff on

		Returns:
			(dict): {
				'combined_gen' : generator of diflibb
				'side_by_side_html' : html output of sxsdiff lib
			}

		'''

		if input_record:
			input_xml_string = input_record.document

		elif xml_string:
			input_xml_string = xml_string

		else:
			logger.debug('input record or XML string required, returning false')
			return False

		# prepare input / result
		docs = [input_xml_string, self.document]
		if reverse_direction:
			docs.reverse()

		# include combine generator in output
		if output in ['all','combined_gen']:

			# get generator of differences
			combined_gen = difflib.unified_diff(
				docs[0].splitlines(),
				docs[1].splitlines()
			)

			# return as HTML
			if combined_as_html:
				combined_gen = self._return_combined_diff_gen_as_html(combined_gen)

		else:
			combined_gen = None

		# include side_by_side html in output
		if output in ['all','side_by_side_html']:

			sxsdiff_result = DiffCalculator().run(docs[0], docs[1])
			sio = io.StringIO()
			GitHubStyledGenerator(file=sio).run(sxsdiff_result)
			sio.seek(0)
			side_by_side_html = sio.read()

		else:
			side_by_side_html = None

		return {
			'combined_gen':combined_gen,
			'side_by_side_html':side_by_side_html
		}


	def _return_combined_diff_gen_as_html(self, combined_gen):

		'''
		Small method to return combined diff generated as pre-compiled HTML
		'''

		html = '<pre><code>'
		for line in combined_gen:
			if line.startswith('-'):
				html += '<span style="background-color:#ffeef0;">'
			elif line.startswith('+'):
				html += '<span style="background-color:#e6ffed;">'
			else:
				html += '<span>'
			html += line.replace('<','&lt;').replace('>','&gt;')
			html += '</span><br>'
		html += '</code></pre>'

		return html


	def calc_fingerprint(self, update_db=False):

		'''
		Generate fingerprint hash with binascii.crc32()
		'''

		fingerprint = binascii.crc32(self.document.encode('utf-8'))

		if update_db:
			self.fingerprint = fingerprint
			self.save()

		return fingerprint


	def map_fields_for_es(self, mapper):

		'''
		Method for testing how a Record will map given an instance
		of a mapper from core.spark.es
		'''

		stime = time.time()
		mapped_fields = mapper.map_record(record_string=self.document)
		logger.debug('mapping elapsed: %s' % (time.time()-stime))
		return mapped_fields



class IndexMappingFailure(mongoengine.Document):

	db_id = mongoengine.StringField()
	record_id = mongoengine.StringField()
	job_id = mongoengine.IntField()
	mapping_error = mongoengine.StringField()

	# meta
	meta = {
		'index_options': {},
		'index_background': False,
		'auto_create_index': False,
		'index_drop_dups': False,
		'indexes': [
			{'fields': ['job_id']},
			{'fields': ['db_id']},
		]
	}

	def __str__(self):
		return 'Index Mapping Failure: #%s' % (self.id)


	# cache
	_job = None
	_record = None


	# define job property
	@property
	def job(self):

		'''
		Method to retrieve Job from Django ORM via job_id
		'''

		if self._job is None:
			job = Job.objects.get(pk=self.job_id)
			self._job = job
		return self._job


	# convenience method
	@property
	def record(self):

		'''
		Method to retrieve Record from Django ORM via job_id
		'''

		if self._record is None:
			record = Record.objects.get(id=self.db_id)
			self._record = record
		return self._record



class ValidationScenario(models.Model):

	'''
	Model to handle validation scenarios used to validate records.
	'''

	name = models.CharField(max_length=255)
	payload = models.TextField()
	validation_type = models.CharField(
		max_length=255,
		choices=[
			('sch','Schematron'),
			('python','Python Code Snippet'),
			('es_query','ElasticSearch DSL Query'),
			('xsd','XML Schema')
		]
	)
	filepath = models.CharField(max_length=1024, null=True, default=None, blank=True)
	default_run = models.BooleanField(default=1)


	def __str__(self):
		return 'ValidationScenario: %s, validation type: %s, default run: %s' % (
			self.name, self.validation_type, self.default_run)


	def validate_record(self, row):

		'''
		Method to test validation against a single record.

		Note: The code for self._validate_schematron() and self._validate_python() are similar, if not identical,
		to staticmethods found in core.spark.record_validation.py.	However, because those are running on spark workers,
		in a spark context, it makes it difficult to define once, but use in multiple places.	As such, these
		validations are effectively defined twice.

		Args:
			row (core.models.Record): Record instance, called "row" here to mirror spark job iterating over DataFrame
		'''

		# run appropriate validation based on type
		if self.validation_type == 'sch':
			result = self._validate_schematron(row)
		if self.validation_type == 'python':
			result = self._validate_python(row)
		if self.validation_type == 'es_query':
			result = self._validate_es_query(row)
		if self.validation_type == 'xsd':
			result = self._validate_xsd(row)

		# return result
		return result


	def _validate_schematron(self, row):

		# parse schematron
		sct_doc = etree.parse(self.filepath)
		validator = isoschematron.Schematron(sct_doc, store_report=True)

		# get document xml
		record_xml = etree.fromstring(row.document.encode('utf-8'))

		# validate
		is_valid = validator.validate(record_xml)

		# prepare results_dict
		results_dict = {
			'fail_count':0,
			'passed':[],
			'failed':[]
		}

		# temporarily add all tests to successes
		sct_root = sct_doc.getroot()
		nsmap = sct_root.nsmap

		# if schematron namespace logged as None, fix
		try:
			schematron_ns = nsmap.pop(None)
			nsmap['schematron'] = schematron_ns
		except:
			pass

		# get all assertions
		assertions = sct_root.xpath('//schematron:assert', namespaces=nsmap)
		for a in assertions:
			results_dict['passed'].append(a.text)

		# record total tests
		results_dict['total_tests'] = len(results_dict['passed'])

		# if not valid, parse failed
		if not is_valid:

			# get failed
			report_root = validator.validation_report.getroot()
			fails = report_root.findall('svrl:failed-assert', namespaces=report_root.nsmap)

			# log count
			results_dict['fail_count'] = len(fails)

			# loop through fails
			for fail in fails:

				# get fail test name
				fail_text_elem = fail.find('svrl:text', namespaces=fail.nsmap)

				# if in successes, remove
				if fail_text_elem.text in results_dict['passed']:
					results_dict['passed'].remove(fail_text_elem.text)

				# append to failed
				results_dict['failed'].append(fail_text_elem.text)

		# return
		return {
			'parsed':results_dict,
			'raw':etree.tostring(validator.validation_report).decode('utf-8')
		}


	def _validate_python(self, row):

		# parse user defined functions from validation scenario payload
		temp_pyvs = ModuleType('temp_pyvs')
		exec(self.payload, temp_pyvs.__dict__)

		# get defined functions
		pyvs_funcs = []
		test_labeled_attrs = [ attr for attr in dir(temp_pyvs) if attr.lower().startswith('test') ]
		for attr in test_labeled_attrs:
			attr = getattr(temp_pyvs, attr)
			if inspect.isfunction(attr):
				pyvs_funcs.append(attr)

		# instantiate prvb
		prvb = PythonUDFRecord(row)

		# prepare results_dict
		results_dict = {
			'fail_count':0,
			'passed':[],
			'failed':[]
		}

		# record total tests
		results_dict['total_tests'] = len(pyvs_funcs)

		# loop through functions
		for func in pyvs_funcs:

			# get func test message
			signature = inspect.signature(func)
			t_msg = signature.parameters['test_message'].default

			# attempt to run user-defined validation function
			try:

				# run test
				test_result = func(prvb)

				# if fail, append
				if test_result != True:
					results_dict['fail_count'] += 1
					# if custom message override provided, use
					if test_result != False:
						results_dict['failed'].append(test_result)
					# else, default to test message
					else:
						results_dict['failed'].append(t_msg)

				# if success, append to passed
				else:
					results_dict['passed'].append(t_msg)

			# if problem, report as failure with Exception string
			except Exception as e:
				results_dict['fail_count'] += 1
				results_dict['failed'].append("test '%s' had exception: %s" % (func.__name__, str(e)))

		# return
		return {
			'parsed':results_dict,
			'raw':json.dumps(results_dict)
		}


	def _validate_es_query(self, row):

		'''
		Method to test ElasticSearch DSL query validation against row
			- NOTE: unlike the schematron and python validations, which run as
			python UDF functions in spark, the mechanics are slightly different here
			where this will run with Hadoop ES queries and unions in Spark

		Proposed structure:
		[
				{
					"test_name":"record has mods_subject_topic",
					"matches":"valid",
					"es_query":{
						"query":{
							"exists":{
								"field":"mods_subject_topic"
							}
						}
					}
				},
				{
					"test_name":"record does not have subject of Fiction",
					"matches":"invalid",
					"es_query":{
						"query":{
							"match":{
								"mods_subject_topic.keyword":"Fiction"
							}
						}
					}
				}
			]
		'''

		# parse es validation payload
		es_payload = json.loads(self.payload)

		# prepare results_dict
		results_dict = {
			'fail_count':0,
			'passed':[],
			'failed':[],
			'total_tests':len(es_payload)
		}

		# loop through tests in ES validation
		for t in es_payload:

			# get row's cjob
			cjob = CombineJob.get_combine_job(row.job.id)

			# init query with es_handle and es index
			query = Search(using=es_handle, index=cjob.esi.es_index)

			# update query with search body
			query = query.update_from_dict(t['es_query'])

			# add row to query
			query = query.query("term", db_id=str(row.id))

			# debug
			logger.debug(query.to_dict())

			# execute query
			query_results = query.execute()

			# if hits.total > 0, assume a hit and call success
			if t['matches'] == 'valid':
				if query_results.hits.total > 0:
					results_dict['passed'].append(t['test_name'])
				else:
					results_dict['failed'].append(t['test_name'])
					results_dict['fail_count'] += 1
			elif t['matches'] == 'invalid':
				if query_results.hits.total == 0:
					results_dict['passed'].append(t['test_name'])
				else:
					results_dict['failed'].append(t['test_name'])
					results_dict['fail_count'] += 1

		# return
		return {
			'parsed':results_dict,
			'raw':json.dumps(results_dict)
		}


	def _validate_xsd(self, row):

		# prepare results_dict
		results_dict = {
			'total_tests':1,
			'fail_count':0,
			'passed':[],
			'failed':[]
		}

		# parse xsd
		xmlschema_doc = etree.parse(self.filepath)
		xmlschema = etree.XMLSchema(xmlschema_doc)

		# get document xml
		record_xml = etree.fromstring(row.document.encode('utf-8'))

		# validate
		try:
			xmlschema.assertValid(record_xml)
			is_valid = True
			validation_msg = 'Document is valid'
			results_dict['passed'].append(validation_msg)

		except etree.DocumentInvalid as e:
			is_valid = False
			validation_msg = str(e)
			results_dict['failed'].append(validation_msg)
			results_dict['fail_count'] += 1

		# return
		return {
			'parsed':results_dict,
			'raw':validation_msg
		}



class JobValidation(models.Model):

	'''
	Model to record one-to-many relationship between jobs and validation scenarios run against its records
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	validation_scenario = models.ForeignKey(ValidationScenario, on_delete=models.CASCADE)
	failure_count = models.IntegerField(null=True, default=None)

	def __str__(self):
		return 'JobValidation: #%s, Job: #%s, ValidationScenario: #%s, failure count: %s' % (
			self.id, self.job.id, self.validation_scenario.id, self.failure_count)


	def get_record_validation_failures(self):

		'''
		Method to return records, for this job, with validation errors

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet): RecordValidation queryset of records from self.job and self.validation_scenario
		'''

		rvfs = RecordValidation.objects\
			.filter(validation_scenario_id=self.validation_scenario.id)\
			.filter(job_id=self.job.id)
		return rvfs


	def validation_failure_count(self, force_recount=False):

		'''
		Method to count, set, and return failure count for this job validation
			- set self.failure_count if not set

		Args:
			None

		Returns:
			(int): count of records that did not pass validation (Note: each record may have failed 1+ assertions)
				- sets self.failure_count and saves model
		'''

		if (self.failure_count is None and self.job.finished) or force_recount:
			logger.debug("calculating failure count for validation job: %s" % self)
			rvfs = self.get_record_validation_failures()
			self.failure_count = rvfs.count()
			self.save()

		# return count
		return self.failure_count


	def delete_record_validation_failures(self):

		'''
		Method to delete record validations associated with this validation job
		'''

		rvfs = RecordValidation.objects\
			.filter(validation_scenario_id=self.validation_scenario.id)\
			.filter(job_id=self.job.id)
		del_results = rvfs.delete()
		logger.debug('%s validations removed' % del_results)
		return del_results



class RecordValidation(mongoengine.Document):

	# fields
	record_id = mongoengine.ReferenceField(Record, reverse_delete_rule=mongoengine.CASCADE)
	record_identifier = mongoengine.StringField()
	job_id = mongoengine.IntField()
	validation_scenario_id = mongoengine.IntField()
	validation_scenario_name = mongoengine.StringField()
	valid = mongoengine.BooleanField(default=True)
	results_payload = mongoengine.StringField()
	fail_count = mongoengine.IntField()

	# meta
	meta = {
		'index_options': {},
        'index_background': False,
        'auto_create_index': False,
        'index_drop_dups': False,
		'indexes': [
			{'fields': ['record_id']},
			{'fields': ['job_id']},
			{'fields': ['validation_scenario_id']}
		]
	}

	# cache
	_validation_scenario = None
	_job = None

	# define Validation Scenario property
	@property
	def validation_scenario(self):

		'''
		Method to retrieve Job from Django ORM via job_id
		'''
		if self._validation_scenario is None:
			validation_scenario = ValidationScenario.objects.get(pk=self.validation_scenario_id)
			self._validation_scenario = validation_scenario
		return self._validation_scenario


	# define job property
	@property
	def job(self):

		'''
		Method to retrieve Job from Django ORM via job_id
		'''
		if self._job is None:
			job = Job.objects.get(pk=self.job_id)
			self._job = job
		return self._job


	# convenience method
	@property
	def record(self):
		return self.record_id


	# failed tests as property
	@property
	def failed(self):
		return json.loads(self.results_payload)['failed']



class FieldMapper(models.Model):

	'''
	Model to handle different Field Mappers
	'''

	name = models.CharField(max_length=128)
	payload = models.TextField(null=True, default=None, blank=True)
	config_json = models.TextField(null=True, default=None, blank=True)
	field_mapper_type = models.CharField(
		max_length=255,
		choices=[
			('xml2kvp','XML to Key/Value Pair (XML2kvp)'),
			('xslt','XSL Stylesheet'),
			('python','Python Code Snippet')]
	)


	def __str__(self):
		return '%s, FieldMapper: #%s' % (self.name, self.id)


	@property
	def config(self):

		if self.config_json:
			return json.loads(self.config_json)
		else:
			return None


	def validate_config_json(self, config_json=None):

		# if config_json not provided, assume use self
		if not config_json:
			config_json = self.config_json

		# load config_json as dictionary
		config_dict = json.loads(config_json)

		# validate against XML2kvp schema
		jsonschema.validate(config_dict, XML2kvp.schema)



class RecordIdentifierTransformationScenario(models.Model):

	'''
	Model to manage transformation scenarios for Record's record_ids (RITS)
	'''

	name = models.CharField(max_length=255)
	transformation_type = models.CharField(
		max_length=255,
		choices=[('regex','Regular Expression'),('python','Python Code Snippet'),('xpath','XPath Expression')]
	)
	transformation_target = models.CharField(
		max_length=255,
		choices=[('record_id','Record Identifier'),('document','Record Document')]
	)
	regex_match_payload = models.CharField(null=True, default=None, max_length=4096, blank=True)
	regex_replace_payload = models.CharField(null=True, default=None, max_length=4096, blank=True)
	python_payload = models.TextField(null=True, default=None, blank=True)
	xpath_payload = models.CharField(null=True, default=None, max_length=4096, blank=True)

	def __str__(self):
		return '%s, RITS: #%s' % (self.name, self.id)



class DPLABulkDataDownload(models.Model):

	'''
	Model to handle the management of DPLA bulk data downloads
	'''

	s3_key = models.CharField(max_length=255)
	downloaded_timestamp = models.DateTimeField(null=True, auto_now_add=True)
	filepath = models.CharField(max_length=255, null=True, default=None)
	es_index = models.CharField(max_length=255, null=True, default=None)
	uploaded_timestamp = models.DateTimeField(null=True, default=None, auto_now_add=False)
	status = models.CharField(
		max_length=255,
		choices=[
			('init','Initiating'),
			('downloading','Downloading'),
			('indexing','Indexing'),
			('finished','Downloaded and Indexed')
		],
		default='init'
	)

	def __str__(self):
		return '%s, DPLABulkDataDownload: #%s' % (self.s3_key, self.id)


	# name shim
	@property
	def name(self):
		return self.s3_key



class CombineBackgroundTask(models.Model):

	'''
	Model for long running, background tasks
	'''

	name = models.CharField(max_length=255, null=True, default=None)
	task_type = models.CharField(
		max_length=255,
		choices=[
			('job_delete','Job Deletion'),
			('record_group_delete','Record Group Deletion'),
			('org_delete','Organization Deletion'),
			('validation_report','Validation Report Generation'),
			('export_mapped_fields','Export Mapped Fields'),
			('export_documents','Export Documents'),
			('job_reindex','Job Reindex Records'),
			('job_new_validations','Job New Validations'),
			('job_remove_validation','Job Remove Validation')
		],
		default=None,
		null=True
	)
	celery_task_id = models.CharField(max_length=128, null=True, default=None)
	celery_task_output = models.TextField(null=True, default=None)
	task_params_json = models.TextField(null=True, default=None)
	task_output_json = models.TextField(null=True, default=None)
	start_timestamp = models.DateTimeField(null=True, auto_now_add=True)
	finish_timestamp = models.DateTimeField(null=True, default=None, auto_now_add=False)
	completed = models.BooleanField(default=False)


	def __str__(self):
		return 'CombineBackgroundTask: %s, ID #%s, Celery Task ID #%s' % (self.name, self.id, self.celery_task_id)


	def update(self):

		'''
		Method to update completed status, and affix task to instance
		'''

		# get async task from Redis
		try:

			self.celery_task = AsyncResult(self.celery_task_id)
			self.celery_status = self.celery_task.status

			if not self.completed:

				# if ready (finished)
				if self.celery_task.ready():

					# set completed
					self.completed = True

					# update timestamp
					self.finish_timestamp = datetime.datetime.now()

					# handle result type
					if type(self.celery_task.result) == Exception:
						result = str(self.celery_task.result)
					else:
						result = self.celery_task.result

					# save json of async task output
					task_output = {
						'result':result,
						'status':self.celery_task.status,
						'task_id':self.celery_task.task_id,
						'traceback':self.celery_task.traceback
					}
					self.celery_task_output = json.dumps(task_output)

					# save
					self.save()

		except Exception as e:
			self.celery_task = None
			self.celery_status = 'STOPPED'
			self.completed = True
			self.save()
			logger.debug(str(e))


	def calc_elapsed_as_string(self):

		# determine time elapsed in seconds

		# completed, with timestamp
		if self.completed and self.finish_timestamp != None:
			# use finish timestamp
			seconds_elapsed = (self.finish_timestamp.replace(tzinfo=None) - self.start_timestamp.replace(tzinfo=None)).seconds

		# marked as completed, but not timestamp, set to zero
		elif self.completed:
			seconds_elapsed = 0

		# else, calc until now
		else:
			seconds_elapsed = (datetime.datetime.now() - self.start_timestamp.replace(tzinfo=None)).seconds

		# return as string
		m, s = divmod(seconds_elapsed, 60)
		h, m = divmod(m, 60)

		return "%d:%02d:%02d" % (h, m, s)


	@property
	def task_params(self):

		'''
		Property to return JSON params as dict
		'''

		if self.task_params_json:
			return json.loads(self.task_params_json)
		else:
			return {}


	@property
	def task_output(self):

		'''
		Property to return JSON output as dict
		'''

		if self.task_output_json:
			return json.loads(self.task_output_json)
		else:
			return {}


	def cancel(self):

		'''
		Method to cancel background task
		'''

		# attempt to stop any Spark jobs
		if 'job_id' in self.task_params.keys():
			job_id = self.task_params['job_id']
			logger.debug('attempt to kill spark jobs related to Job: %s' % job_id)
			j = Job.objects.get(pk=int(job_id))
			j.stop_job(cancel_livy_statement=False, kill_spark_jobs=True)

		# revoke celery task
		if self.celery_task_id:
			revoke(self.celery_task_id, terminate=True)

		# update status
		self.refresh_from_db()
		self.completed = True
		self.save()



class StateIO(mongoengine.Document):

	'''
	Model to facilitate the recording of State Exports and Imports in Combine
		- flexible to both, defined by stateio_type
		- identifiers and manifests may be null
	'''


	name = mongoengine.StringField()
	stateio_type = mongoengine.StringField(
		required=True,
		choices=[('export','Export'),('import','Import'),('generic','Generic')],
		default='generic'
	)
	status = mongoengine.StringField(
		default='initializing'
	)
	export_id = mongoengine.StringField()
	export_manifest = mongoengine.DictField()
	export_path = mongoengine.StringField()
	import_id = mongoengine.StringField()
	import_manifest = mongoengine.DictField()
	import_path = mongoengine.StringField()
	bg_task_id = mongoengine.IntField()
	finished = mongoengine.BooleanField(default=False)


	# meta
	meta = {
		'index_options': {},
        'index_drop_dups': False,
		'indexes': []
	}

	# custom save
	def save(self, *args, **kwargs):

		if self.name == None:
			self.name = 'StateIO %s @ %s' % (self.stateio_type.title(), datetime.datetime.now().strftime('%b. %d, %Y, %-I:%M:%S %p'))
		return super(StateIO, self).save(*args, **kwargs)


	def __str__(self):
		return 'StateIO: %s, %s, %s' % (self.id, self.name, self.stateio_type)


	# _id shim property
	@property
	def _id(self):
		return self.id


	# timestamp property
	@property
	def timestamp(self):
		return self.id.generation_time if self.id else None


	# pre_delete method
	@classmethod
	def pre_delete(cls, sender, document, **kwargs):

		logger.debug('preparing to delete %s' % document)

		try:

			# if export
			if document.stateio_type == 'export':

				# check for export_path and delete
				logger.debug('removing export_path: %s' % document.export_path)
				if os.path.isfile(document.export_path):
					logger.debug('export is filetype, removing')
					os.remove(document.export_path)
				elif os.path.isdir(document.export_path):
					logger.debug('export is dir, removing')
					shutil.rmtree(document.export_path)
				else:
					logger.debug('Could not remove %s' % document.export_path)

			# if import
			elif document.stateio_type == 'import':

				# check for export_path and delete
				logger.debug('removing import_path: %s' % document.import_manifest['import_path'])
				if os.path.isdir(document.import_path):
					logger.debug('removing import dir')
					shutil.rmtree(document.import_path)
				else:
					logger.debug('Could not remove %s' % document.import_path)

		except:
			logger.warning('Could not remove import/export directory')


	def export_all(self, skip_dict=None):

		'''
		Method to export all exportable objects in Combine

		Args:

			skip_dict (dict): Dictionary of model instances to skip
				- currently only supporting Organizations to skip, primarily for debugging purposes

		Todo:
			- currently only filtering org_ids from skip_dict, but could expand to skip
			any model type
		'''

		# if not yet initialized with id, do that now
		if not self.id:
			self.save

		# select all Orgs
		logger.debug('retrieving all Organizations')
		org_ids = [ org.id for org in Organization.objects.filter(for_analysis=False) ]

		# capture all config scenarios
		conf_model_instances = []

		# hash for conversion
		config_prefix_hash = {
			ValidationScenario:'validations',
			Transformation:'transformations',
			OAIEndpoint:'oai_endpoints',
			RecordIdentifierTransformationScenario:'rits',
			FieldMapper:'field_mapper_configs',
			DPLABulkDataDownload:'dbdd'
		}

		# loop through types
		for conf_model in [
				ValidationScenario,
				Transformation,
				OAIEndpoint,
				RecordIdentifierTransformationScenario,
				FieldMapper,
				DPLABulkDataDownload
			]:

			logger.debug('retrieving all config scenarios for: %s' % conf_model.__name__)
			conf_model_instances.extend([ '%s|%s' % (config_prefix_hash[conf_model], conf_instance.id) for conf_instance in conf_model.objects.all() ])


		# using skip_dict to filter orgs
		if 'orgs' in skip_dict:
			logger.debug('found orgs to skip in skip_dict')
			for org_id in skip_dict['orgs']:
				org_ids.remove(org_id)

		# fire StateIOClient with id
		sioc = StateIOClient()
		sioc.export_state(
			orgs=org_ids,
			config_scenarios=conf_model_instances,
			export_name=self.name,
			stateio_id=self.id
		)


	@property
	def bg_task(self):

		'''
		Method to return CombineBackgroundTask instance
		'''

		if self.bg_task_id != None:
			# run query
			ct_task_query = CombineBackgroundTask.objects.filter(id=int(self.bg_task_id))
			if ct_task_query.count() == 1:
				return ct_task_query.first()
			else:
				return False
		else:
			return False



####################################################################
# Signals Handlers												   #
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
		logger.debug('Checking for pre-existing livy sessions')

		# get "active" user sessions
		livy_sessions = LivySession.objects.filter(status__in=['starting','running','idle'])
		logger.debug(livy_sessions)

		# none found
		if livy_sessions.count() == 0:
			logger.debug('no Livy sessions found, creating')
			livy_session = models.LivySession()
			livy_session.start_session()

		# if sessions present
		elif livy_sessions.count() == 1:
			logger.debug('single, active Livy session found, using')

		elif livy_sessions.count() > 1:
			logger.debug('multiple Livy sessions found, sending to sessions page to select one')


@receiver(models.signals.pre_delete, sender=Organization)
def delete_org_pre_delete(sender, instance, **kwargs):

	# mark child record groups as deleted
	logger.debug('marking all child Record Groups as deleting')
	for record_group in instance.recordgroup_set.all():

		record_group.name = "%s (DELETING)" % record_group.name
		record_group.save()

		# mark child jobs as deleted
		logger.debug('marking all child Jobs as deleting')
		for job in record_group.job_set.all():

			job.name = "%s (DELETING)" % job.name
			job.deleted = True
			job.status = 'deleting'
			job.save()


@receiver(models.signals.pre_delete, sender=RecordGroup)
def delete_record_group_pre_delete(sender, instance, **kwargs):

	# mark child jobs as deleted
	logger.debug('marking all child Jobs as deleting')
	for job in instance.job_set.all():

		job.name = "%s (DELETING)" % job.name
		job.deleted = True
		job.status = 'deleting'
		job.save()


@receiver(models.signals.post_save, sender=Job)
def save_job_post_save(sender, instance, created, **kwargs):

	'''
	After job is saved, update job output

	Args:
		sender (auth.models.Job): class
		user (auth.models.Job): instance
		created (bool): indicates if newly created, or just save/update
		kwargs: not used
	'''

	# if the record was just created, then update job output (ensures this only runs once)
	if created and instance.job_type != 'AnalysisJob':

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
def delete_job_pre_delete(sender, instance, **kwargs):

	'''
	When jobs are removed, some actions are performed:
		- if job is queued or running, stop
		- remove avro files from disk
		- delete ES indexes (if present)
		- delete from Mongo

	Args:
		sender (auth.models.Job): class
		user (auth.models.Job): instance
		kwargs: not used
	'''

	logger.debug('pre-delete sequence for Job: #%s' % instance.id)

	# stop Job
	instance.stop_job()

	# remove avro files from disk
	if instance.job_output and instance.job_output.startswith('file://'):

		try:
			output_dir = instance.job_output.split('file://')[-1]
			shutil.rmtree(output_dir)

		except:
			logger.debug('could not remove job output directory at: %s' % instance.job_output)

	# remove ES index if exists
	instance.drop_es_index()

	# remove Records from Mongo
	instance.remove_records_from_db()

	# remove Validations from Mongo
	instance.remove_validations_from_db()

	# remove Validations from Mongo
	instance.remove_mapping_failures_from_db()

	# remove Job as input for other Jobs
	instance.remove_as_input_job()


@receiver(models.signals.pre_delete, sender=JobValidation)
def delete_job_validation_pre_delete(sender, instance, **kwargs):

	'''
	Signal to remove RecordValidations from DB if JobValidation removed
	'''

	del_results = instance.delete_record_validation_failures()


@receiver(models.signals.post_delete, sender=Job)
def delete_job_post_delete(sender, instance, **kwargs):

	logger.debug('job %s was deleted successfully' % instance)


@receiver(models.signals.pre_save, sender=Transformation)
def save_transformation_to_disk(sender, instance, **kwargs):

	'''
	Pre-save work for Transformations

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

	# fire transformation method to rewrite external HTTP includes for XSLT
	if instance.transformation_type == 'xslt':
		instance._rewrite_xsl_http_includes()

	# write XSLT type transformation to disk
	if instance.transformation_type == 'xslt':
		filename = uuid.uuid4().hex

		filepath = '%s/%s.xsl' % (transformations_dir, filename)
		with open(filepath, 'w') as f:
			f.write(instance.payload)

		# update filepath
		instance.filepath = filepath


@receiver(models.signals.pre_save, sender=ValidationScenario)
def save_validation_scenario_to_disk(sender, instance, **kwargs):

	'''
	When users enter a payload for a validation scenario, write to disk for use in Spark context

	Args:
		sender (auth.models.ValidationScenario): class
		user (auth.models.ValidationScenario): instance
		kwargs: not used
	'''

	# check that transformation directory exists
	validations_dir = '%s/validation' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]
	if not os.path.exists(validations_dir):
		os.mkdir(validations_dir)

	# if previously written to disk, remove
	if instance.filepath:
		try:
			os.remove(instance.filepath)
		except:
			logger.debug('could not remove validation scenario file: %s' % instance.filepath)

	# write Schematron type validation to disk
	if instance.validation_type == 'sch':
		filename = 'file_%s.sch' % uuid.uuid4().hex
	if instance.validation_type == 'python':
		filename = 'file_%s.py' % uuid.uuid4().hex
	if instance.validation_type == 'es_query':
		filename = 'file_%s.json' % uuid.uuid4().hex
	if instance.validation_type == 'xsd':
		filename = 'file_%s.xsd' % uuid.uuid4().hex

	filepath = '%s/%s' % (validations_dir, filename)
	with open(filepath, 'w') as f:
		f.write(instance.payload)

	# update filepath
	instance.filepath = filepath


@receiver(models.signals.pre_delete, sender=DPLABulkDataDownload)
def delete_dbdd_pre_delete(sender, instance, **kwargs):

	# remove download from disk
	if os.path.exists(instance.filepath):
		logger.debug('removing %s from disk' % instance.filepath)
		os.remove(instance.filepath)

	# remove ES index if exists
	try:
		if es_handle.indices.exists(instance.es_index):
			logger.debug('removing ES index: %s' % instance.es_index)
			es_handle.indices.delete(instance.es_index)
	except:
		logger.debug('could not remove ES index: %s' % instance.es_index)


@receiver(models.signals.post_init, sender=CombineBackgroundTask)
def background_task_post_init(sender, instance, **kwargs):

	# if exists already, update status
	if instance.id:
		instance.update()


@receiver(models.signals.pre_delete, sender=CombineBackgroundTask)
def background_task_pre_delete_django_tasks(sender, instance, **kwargs):

	# if export dir exists in task_output, delete as well
	if instance.task_output != {} and 'export_dir' in instance.task_output.keys():
		try:
			logger.debug('removing task export dir: %s' % instance.task_output['export_dir'])
			shutil.rmtree(instance.task_output['export_dir'])
		except:
			logger.debug('could not parse task output as JSON')


# MongoEngine signals
mongoengine.signals.pre_delete.connect(StateIO.pre_delete, sender=StateIO)


####################################################################
# Apahce Livy and Spark Clients										 #
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
		- the Spark Application port can change (https://github.com/WSULib/combine/issues/243)
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


####################################################################
# Combine Models 												   #
####################################################################

class ESIndex(object):

	'''
	Model to aggregate methods useful for accessing and analyzing ElasticSearch indices
	'''

	def __init__(self, es_index):

		# convert single index to list
		if type(es_index) == str:
			self.es_index = [es_index]
		else:
			self.es_index = es_index

		# also, save as string
		self.es_index_str = str(self.es_index)


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

			# loop through indices and build field names
			field_names = []
			for index,index_properties in es_r.items():
				fields = index_properties['mappings']['record']['properties']
				# get fields as list and extend list
				field_names.extend(list(fields.keys()))
			# get unique list
			field_names = list(set(field_names))

			# remove uninteresting fields
			field_names = [ field for field in field_names if field not in [
					'db_id',
					'combine_id',
					'xml2kvp_meta',
					'fingerprint']
				]

			# sort alphabetically that influences results list
			field_names.sort()

			return field_names


	def _calc_field_metrics(self,
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
			cardinality_precision_threshold=settings.CARDINALITY_PRECISION_THRESHOLD,
			job_record_count=None
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
			job_record_count (int): optional pre-count of records

		Returns:
			(dict):
				total_docs: count of total docs
				field_counts (dict): dictionary of fields with counts, uniqueness across index, etc.
		'''

		if es_handle.indices.exists(index=self.es_index) and es_handle.search(index=self.es_index)['hits']['total'] > 0:

			# DEBUG
			stime = time.time()

			# get field mappings for index
			field_names = self.get_index_fields()

			# loop through fields and query ES
			field_count = []
			for field_name in field_names:

				logger.debug('analyzing mapped field %s' % field_name)

				# init search
				s = Search(using=es_handle, index=self.es_index)

				# return no results, only aggs
				s = s[0]

				# add agg buckets for each field to count total and unique instances
				# for field_name in field_names:
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

				# get metrics and append if field metrics found
				field_metrics = self._calc_field_metrics(sr_dict, field_name)
				if field_metrics:
					field_count.append(field_metrics)

			# DEBUG
			logger.debug('count indexed fields elapsed: %s' % (time.time()-stime))

			# prepare dictionary for return
			return_dict = {
				'total_docs':sr_dict['hits']['total'],
				'fields':field_count
			}

			# if job record count provided, include percentage of indexed records to that count
			if job_record_count:
				indexed_percentage = round((float(return_dict['total_docs']) / float(job_record_count)), 4)
				return_dict['indexed_percentage'] = indexed_percentage

			# return
			return return_dict

		else:
			return False


	def field_analysis(self,
			field_name,
			cardinality_precision_threshold=settings.CARDINALITY_PRECISION_THRESHOLD,
			metrics_only=False,
			terms_limit=10000
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
			s.aggs.bucket(field_name, A('terms', field='%s.keyword' % field_name, size=terms_limit))

		# return zero
		s = s[0]

		# execute and return aggs
		sr = s.execute()

		# get metrics
		field_metrics = self._calc_field_metrics(sr.to_dict(), field_name)

		# prepare and return
		if not metrics_only:
			values = sr.aggs[field_name]['buckets']
		else:
			values = None

		return {
			'metrics':field_metrics,
			'values':values
		}


	def query(self, query_body):

		'''
		Method to run query against Job's ES index
		'''

		# init query
		query = Search(using=es_handle, index=self.es_index)

		# update with query_body
		if type(query_body) == dict:
			query = query.update_from_dict(query_body)
		elif type(query_body) == str:
			query = query.update_from_dict(json.loads(query_body))

		# execute and return
		results = query.execute()
		return results



class PublishedRecords(object):

	'''
	Model to manage the aggregation and retrieval of published records.
	'''

	def __init__(self):

		self.record_group = 0

		# get published jobs
		self.published_jobs = Job.objects.filter(published=True)

		# get set IDs from record group of published jobs
		sets = {}
		for job in self.published_jobs:

			if job.publish_set_id:

				# if set not seen, add as list
				if job.publish_set_id not in sets.keys():
					sets[job.publish_set_id] = []

				# add publish job
				sets[job.publish_set_id].append(job)
		self.sets = sets

		# establish ElasticSearchIndex (esi) instance
		self.esi = ESIndex([ 'j%s' % job.id for job in self.published_jobs ])


	@property
	def records(self):

		'''
		Property to return QuerySet of all published records
		'''

		return Record.objects.filter(published=True)


	def get_record(self, record_id):

		'''
		Return single, published record by record.record_id

		Args:
			record_id (str): Record's record_id

		Returns:
			(core.model.Record): single Record instance
		'''

		record_query = self.records.filter(record_id = record_id)

		# if one, return
		if record_query.count() == 1:
			return record_query.first()

		else:
			logger.debug('multiple records found for id %s - this is not allowed for published records' % id)
			return False


	def count_indexed_fields(self, force_recount=False):

		'''
		Wrapper for ESIndex.count_indexed_fields
			- stores results in Mongo to avoid re-calcing everytime
				- stored as misc/published_field_counts
			- checks Mongo for stored metrics, if not found, calcs and stores
			- when Jobs are published, this Mongo entry is removed forcing a re-calc

		Args:
			force_recount (boolean): force recount and update to stored doc in Mongo
		'''

		# check for stored field counts
		published_field_counts = mc_handle.combine.misc.find_one('published_field_counts')

		# if present, return and use
		if published_field_counts and not force_recount:
			logger.debug('saved published field counts found, using')
			return published_field_counts

		# else, calculate, store, and return
		else:

			logger.debug('calculating published field counts, saving, and returning')

			# calc
			published_field_counts = self.esi.count_indexed_fields()

			# if published_field_counts
			if published_field_counts:

				# add id and replace (upsert if necessary)
				published_field_counts['_id'] = 'published_field_counts'
				doc = mc_handle.combine.misc.replace_one(
					{'_id':'published_field_counts'},
					published_field_counts,
					upsert=True)

			# return
			return published_field_counts


	@staticmethod
	def get_publish_set_ids():

		'''
		Static method to return unique, not Null publish set ids

		Args:
			None

		Returns:
			(list): list of publish set ids
		'''

		publish_set_ids = Job.objects.exclude(publish_set_id=None).values('publish_set_id').distinct()
		return publish_set_ids



class CombineJob(object):

	'''
	Class to aggregate methods useful for managing and inspecting jobs.

	Additionally, some methods and workflows for loading a job, inspecting job.job_type, and loading as appropriate
	Combine job.

	Note: There is overlap with the core.models.Job class, but this not being a Django model, allows for a bit
	more flexibility with __init__.
	'''

	def __init__(self,
		user=None,
		job_id=None,
		parse_job_output=True):

		self.user = user
		self.livy_session = LivySession.get_active_session()
		self.df = None
		self.job_id = job_id

		# setup ESIndex instance
		self.esi = ESIndex('j%s' % self.job_id)

		# if job_id provided, attempt to retrieve and parse output
		if self.job_id:

			# retrieve job
			self.get_job(self.job_id)


	def __repr__(self):
		return '<Combine Job: #%s, %s, status %s>' % (self.job.id, self.job.job_type, self.job.status)


	def default_job_name(self):

		'''
		Method to provide default job name based on class type and date

		Args:
			None

		Returns:
			(str): formatted, default job name
		'''

		return '%s @ %s' % (type(self).__name__, datetime.datetime.now().strftime('%b. %d, %Y, %-I:%M:%S %p'))


	@staticmethod
	def init_combine_job(
		user=None,
		record_group=None,
		job_type_class=None,
		job_params={},
		job_details = {},
		**kwargs):

		'''
		Static method to initiate a CombineJob

		Args:
			user (django.auth.User): Instance of User
			record_group (core.models.RecordGroup): Record Group for Job to be run in
			job_type_class (CombineJob subclass): Type of Job to run
			job_params (dict, QueryDict): parameters for Job
				- accepts dictionary or Django QueryDict
				- is converted to Django MultiValueDict
			job_details (dict): optional, pre-loaded job_details dict

		Returns:
			- inititates core.models.Job instance
			- initiates job_details
				- parses *shared* parameters across all Job types
			- passes unsaved job instance and initiated job_details dictionary to job_type_class

		'''

		# prepare job_details
		job_details = {}

		# user
		if user != None:
			job_details['user_id'] = user.id

		# convert python dictionary or Django request object to Django MultiValueDict
		job_params = MultiValueDict(job_params)

		# init job_details by parsing job params shared across job types
		job_details = CombineJob._parse_shared_job_params(job_details, job_params, kwargs)

		# capture and mix in job type specific params
		job_details = job_type_class.parse_job_type_params(job_details, job_params, kwargs)

		# init job_type_class with record group and parsed job_details dict
		cjob = job_type_class(
			user=user,
			record_group=record_group,
			job_details=job_details)

		# return
		return cjob



	@staticmethod
	def _parse_shared_job_params(job_details, job_params, kwargs):

		'''
		Method to parse job parameters shared across all Job types

		Args:
			job_details (dict): dictionary to add parsed parameters to
			job_params (django.utils.datastructures.MultiValueDict): parameters provided for job
		'''

		# parse job name
		job_details['job_name'] = job_params.get('job_name')
		if job_details['job_name'] == '':
			job_details['job_name'] = None

		# get job note
		job_details['job_note'] = job_params.get('job_note')
		if job_details['job_note'] == '':
			job_details['job_note'] = None

		# get field mapper configurations
		job_details['field_mapper'] = job_params.get('field_mapper', None)
		if job_details['field_mapper'] != None and job_details['field_mapper'] != 'default':
			job_details['field_mapper'] = int(job_details['field_mapper'])
		fm_config_json = job_params.get('fm_config_json', None)
		if fm_config_json != None:
			job_details['field_mapper_config'] = json.loads(fm_config_json)
		else:
			job_details['field_mapper_config'] = json.loads(XML2kvp().config_json)

		# finish input filters
		job_details['input_filters'] = CombineJob._parse_input_filters(job_params)

		# get requested validation scenarios and convert to int
		job_details['validation_scenarios'] = [int(vs_id) for vs_id in job_params.getlist('validation_scenario', []) ]

		# handle requested record_id transform
		job_details['rits'] = job_params.get('rits', None)
		if job_details['rits'] == '':
			job_details['rits'] = None
		if job_details['rits'] != None:
			job_details['rits'] = int(job_details['rits'])

		# handle DPLA bulk data compare
		job_details['dbdm'] = {
			'dbdd':None,
			'dbdd_s3_key':None,
			'matches':None,
			'misses':None
		}
		job_details['dbdm']['dbdd'] = job_params.get('dbdd', None)
		if job_details['dbdm']['dbdd'] == '':
			job_details['dbdm']['dbdd'] = None
		if job_details['dbdm']['dbdd'] != None:
			# set as int
			job_details['dbdm']['dbdd'] = int(job_details['dbdm']['dbdd'])
			# get dbdd instance
			dbdd = DPLABulkDataDownload.objects.get(pk=job_details['dbdm']['dbdd'])
			# set s3_key
			job_details['dbdm']['dbdd_s3_key'] = dbdd.s3_key

		# debug
		logger.debug(job_details)

		# return
		return job_details


	@staticmethod
	def _parse_input_filters(job_params):

		'''
		Method to handle parsing input filters, including Job specific filters
		TODO: refactor duplicative parsing for global and job specific

		Args:
			job_params (dict): parameters passed

		Returns:
			(dict): dictionary to be set as job_details['input_filters']
		'''

		# capture input filters
		input_filters = {
			'job_specific':{}
		}

		# validity valve
		input_filters['input_validity_valve'] = job_params.get('input_validity_valve', 'all')

		# numerical valve
		input_numerical_valve = job_params.get('input_numerical_valve', None)
		if input_numerical_valve in ('', None):
			input_filters['input_numerical_valve'] = None
		else:
			input_filters['input_numerical_valve'] = int(input_numerical_valve)

		# es query valve
		input_es_query_valve = job_params.get('input_es_query_valve', None)
		if input_es_query_valve in ('', None):
			input_es_query_valve = None
		input_filters['input_es_query_valve'] = input_es_query_valve

		# duplicates valve
		filter_dupe_record_ids = job_params.get('filter_dupe_record_ids', 'true')
		if filter_dupe_record_ids == 'true':
			input_filters['filter_dupe_record_ids'] = True
		else:
			input_filters['filter_dupe_record_ids'] = False

		# check for any Job specific filters, and append
		job_specs = job_params.getlist('job_spec_json')
		for job_spec_json in job_specs:

			# parse JSON and make similar to global input filters structure
			job_spec_form_dict = { f['name'].lstrip('job_spec_'):f['value'] for f in json.loads(job_spec_json) }

			# prepare job_spec
			job_spec_dict = {}

			# validity valve
			job_spec_dict['input_validity_valve'] = job_spec_form_dict.get('input_validity_valve', 'all')

			# numerical valve
			input_numerical_valve = job_spec_form_dict.get('input_numerical_valve', None)
			if input_numerical_valve in ('', None):
				job_spec_dict['input_numerical_valve'] = None
			else:
				job_spec_dict['input_numerical_valve'] = int(input_numerical_valve)

			# es query valve
			input_es_query_valve = job_spec_form_dict.get('input_es_query_valve', None)
			if input_es_query_valve in ('', None):
				input_es_query_valve = None
			job_spec_dict['input_es_query_valve'] = input_es_query_valve

			# duplicates valve
			filter_dupe_record_ids = job_spec_form_dict.get('filter_dupe_record_ids', 'true')
			if filter_dupe_record_ids == 'true':
				job_spec_dict['filter_dupe_record_ids'] = True
			else:
				job_spec_dict['filter_dupe_record_ids'] = False

			# finally, append to job_specific dictionary for input_filters
			input_filters['job_specific'][job_spec_form_dict['input_job_id']] = job_spec_dict

		# finish input filters
		return input_filters


	def write_validation_job_links(self, job_details):

		'''
		Method to write links for all Validation Scenarios run
		'''

		# write validation links
		if len(job_details['validation_scenarios']) > 0:
			for vs_id in job_details['validation_scenarios']:
				val_job = JobValidation(
					job=self.job,
					validation_scenario=ValidationScenario.objects.get(pk=int(vs_id))
				)
				val_job.save()


	def write_input_job_links(self, job_details):

		'''
		Method to write links for all input Jobs used
		'''

		# get input_jobs
		input_jobs = [ Job.objects.get(pk=int(job_id)) for job_id in job_details['input_job_ids'] ]

		# save input jobs to JobInput table
		for input_job in input_jobs:
			job_input_link = JobInput(
				job=self.job,
				input_job=input_job
			)
			job_input_link.save()


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
		try:
			j = Job.objects.get(pk=job_id)
		except ObjectDoesNotExist:
			logger.debug('Job #%s was not found, returning False' % job_id)
			return False

		# using job_type, return instance of approriate job type
		return globals()[j.job_type](job_id=job_id)


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
			logger.debug('could not start job, active livy session not found')
			return False


	def submit_job_to_livy(self, job_code):

		'''
		Using LivyClient, submit actual job code to Spark.	For the most part, Combine Jobs have the heavy lifting of
		their Spark code in core.models.spark.jobs, but this spark code is enough to fire those.

		Args:
			job_code (str): String of python code to submit to Spark

		Returns:
			None
				- sets attributes to self
		'''

		# if livy session provided
		if self.livy_session != None:

			# submit job
			submit = LivyClient().submit_job(self.livy_session.session_id, job_code)
			response = submit.json()
			headers = submit.headers

			# update job in DB
			self.job.response = json.dumps(response)
			self.job.spark_code = job_code
			self.job.job_id = int(response['id'])
			self.job.status = response['state']
			self.job.url = headers['Location']
			self.job.headers = headers
			self.job.save()

		else:

			logger.debug('active livy session not found')

			# update job in DB
			self.job.spark_code = job_code
			self.job.status = 'failed'
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


	def get_record(self, id, record_field='record_id'):

		'''
		Convenience method to return single record from job.

		Args:
			id (str): string of record ID
			record_field (str): field from Record to filter on, defaults to 'record_id'
		'''

		# query for record
		record_query = Record.objects.filter(job=self.job).filter(**{record_field:id})

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
		return self.esi.count_indexed_fields(job_record_count=self.job.record_count)


	def field_analysis(self, field_name):

		'''
		Wrapper for ESIndex.field_analysis
		'''

		# return field analysis
		return self.esi.field_analysis(field_name)


	def get_indexing_failures(self):

		'''
		Retrieve failures for job indexing process

		Args:
			None

		Returns:
			(django.db.models.query.QuerySet): from IndexMappingFailure model
		'''

		# load indexing failures for this job from DB
		index_failures = IndexMappingFailure.objects.filter(job_id=self.job.id)
		return index_failures


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
				job_output_filename_hash = re.match(r'part-[0-9]+-(.+?)\.avro', avros[0]).group(1)
				logger.debug('job output filename hash: %s' % job_output_filename_hash)
				return job_output_filename_hash

			elif len(avros) == 0:
				logger.debug('no avro files found in job output directory')
				return False
		except:
			logger.debug('could not load job output to determine filename hash')
			return False


	def reindex_bg_task(self, fm_config_json=None):

		'''
		Method to reindex job as bg task

		Args:
			fm_config_json (dict|str): XML2kvp field mapper configurations, JSON or dictionary
				- if None, saved configurations for Job will be used
				- pass JSON to bg task for serialization
		'''

		# handle fm_config
		if not fm_config_json:
			fm_config_json = self.job.get_fm_config_json()
		else:
			if type(fm_config_json) == dict:
				fm_config_json = json.dumps(fm_config_json)

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Re-Map and Index Job: %s' % self.job.name,
			task_type = 'job_reindex',
			task_params_json = json.dumps({
				'job_id':self.job.id,
				'fm_config_json':fm_config_json
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.job_reindex.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		return ct


	def new_validations_bg_task(self, validation_scenarios):

		'''
		Method to run new validations for Job

		Args:
			validation_scenarios (list): List of Validation Scenarios ids
		'''

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'New Validations for Job: %s' % self.job.name,
			task_type = 'job_new_validations',
			task_params_json = json.dumps({
				'job_id':self.job.id,
				'validation_scenarios':validation_scenarios
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.job_new_validations.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		return ct


	def remove_validation_bg_task(self, jv_id):

		'''
		Method to remove validations from Job based on Validation Job id
		'''

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Remove Validation %s for Job: %s' % (jv_id, self.job.name),
			task_type = 'job_remove_validation',
			task_params_json = json.dumps({
				'job_id':self.job.id,
				'jv_id':jv_id
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.job_remove_validation.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		return ct


	def publish_bg_task(self, publish_set_id=None):

		'''
		Method to remove validations from Job based on Validation Job id
		'''

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Publish Job: %s' % (self.job.name),
			task_type = 'job_publish',
			task_params_json = json.dumps({
				'job_id':self.job.id,
				'publish_set_id':publish_set_id
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.job_publish.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		# return ct
		return ct


	def unpublish_bg_task(self):

		'''
		Method to remove validations from Job based on Validation Job id
		'''

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Unpublish Job: %s' % (self.job.name),
			task_type = 'job_unpublish',
			task_params_json = json.dumps({
				'job_id':self.job.id
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.job_unpublish.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		# return
		return ct


	def dbdm_bg_task(self, dbdd_id):

		'''
		Method to run DPLA Bulk Data Match as bg task
		'''

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Run DPLA Bulk Data Match for Job: %s' % (self.job.name),
			task_type = 'job_dbdm',
			task_params_json = json.dumps({
				'job_id':self.job.id,
				'dbdd_id':dbdd_id
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.job_dbdm.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		return ct


	def rerun(self, rerun_downstream=True, set_gui_status=True):

		'''
		Method to re-run job, and if flagged, all downstream Jobs in lineage
		'''

		# get lineage
		rerun_jobs = self.job.get_downstream_jobs()

		# if not running downstream, select only this job
		if not rerun_downstream:
			rerun_jobs = [self.job]

		# loop through jobs
		for re_job in rerun_jobs:

			logger.debug('re-running job: %s' % re_job)

			# optionally, update status for GUI representation
			if set_gui_status:

				re_job.timestamp = datetime.datetime.now()
				re_job.status = 'initializing'
				re_job.record_count = 0
				re_job.finished = False
				re_job.elapsed = 0
				re_job.deleted = True
				re_job.save()

			# drop records
			re_job.remove_records_from_db()

			# drop es index
			re_job.drop_es_index()

			# remove previously run validations
			re_job.remove_validation_jobs()
			re_job.remove_validations_from_db()

			# remove mapping failures
			re_job.remove_mapping_failures_from_db()

			# where Job has Input Job, reset passed records
			parent_input_jobs = JobInput.objects.filter(job_id = re_job.id)
			for ji in parent_input_jobs:
				ji.passed_records = None
				ji.save()

			# where Job is input for another, reset passed_records
			as_input_jobs = JobInput.objects.filter(input_job_id = re_job.id)
			for ji in as_input_jobs:
				ji.passed_records = None
				ji.save()

			# get combine job
			re_cjob = CombineJob.get_combine_job(re_job.id)

			# write Validation links
			re_cjob.write_validation_job_links(re_cjob.job.job_details_dict)

			# remove old JobTrack instance
			JobTrack.objects.filter(job=self.job).delete()

			# re-submit to Livy
			if re_cjob.job.spark_code != None:
				re_cjob.submit_job_to_livy(eval(re_cjob.job.spark_code))
			else:
				logger.debug('Spark code not set for Job, attempt to re-prepare...')
				re_cjob.job.spark_code = re_cjob.prepare_job(return_job_code=True)
				re_cjob.job.save()
				re_cjob.submit_job_to_livy(eval(re_cjob.job.spark_code))

			# set as undeleted
			re_cjob.job.deleted = False
			re_cjob.job.save()


	def clone(self, rerun=True, clone_downstream=True, skip_clones=[]):

		'''
		Method to clone Job

		Args:
			rerun (bool): If True, Job(s) are automatically rerun after cloning
			clone_downstream (bool): If True, downstream Jobs are cloned as well
		'''

		if clone_downstream:
			to_clone = self.job.get_downstream_jobs()
		else:
			to_clone = [self.job]

		# loop through jobs to clone
		clones = {} # dictionary of original:clone
		clones_ids = {} # dictionary of original.id:clone.id
		for job in to_clone:

			# confirm that job is itself not a clone made during lineage from another Job
			if job in skip_clones:
				continue

			# establish clone handle
			clone = CombineJob.get_combine_job(job.id)

			# drop PK
			clone.job.pk = None

			# update name
			clone.job.name = "%s (CLONE)" % job.name

			# save, cloning in ORM and generating new Job ID which is needed for clone.prepare_job()
			clone.job.save()
			logger.debug('Cloned Job #%s --> #%s' % (job.id, clone.job.id))

			# update spark_code
			clone.job.spark_code = clone.prepare_job(return_job_code=True)
			clone.job.save()

			# save to clones and clones_ids dictionary
			clones[job] = clone.job
			clones_ids[job.id] = clone.job.id

			# recreate JobInput links
			for ji in job.jobinput_set.all():
				logger.debug('cloning input job link: %s' % ji.input_job)
				ji.pk = None

				# if input job was parent clone, rewrite input jobs links
				if ji.input_job in clones.keys():

					# alter job.job_details
					update_dict = {}

					# handle input_job_ids
					update_dict['input_job_ids'] = [ clones[ji.input_job].id if job_id == ji.input_job.id else job_id for job_id in clone.job.job_details_dict['input_job_ids'] ]

					# handle record input filters
					update_dict['input_filters'] = clone.job.job_details_dict['input_filters']
					for job_id in update_dict['input_filters']['job_specific'].keys():

						if int(job_id) in clones_ids.keys():
							job_spec_dict = update_dict['input_filters']['job_specific'].pop(job_id)
							update_dict['input_filters']['job_specific'][str(clones_ids[ji.input_job.id])] = job_spec_dict

					# update
					clone.job.update_job_details(update_dict)

					# rewrite JobInput.input_job
					ji.input_job = clones[ji.input_job]

				ji.job = clone.job
				ji.save()

			# recreate JobValidation links
			for jv in job.jobvalidation_set.all():
				logger.debug('cloning validation link: %s' % jv.validation_scenario.name)
				jv.pk = None
				jv.job = clone.job
				jv.save()

			# rerun clone
			if rerun:
				# reopen and run
				clone = CombineJob.get_combine_job(clone.job.id)
				clone.rerun(rerun_downstream=False)

		# return
		return clones



class HarvestJob(CombineJob):

	'''
	Harvest records to Combine.

	This class represents a high-level "Harvest" job type, with more specific harvest types extending this class.
	In saved and associated core.models.Job instance, job_type will be "HarvestJob".

	Note: Unlike downstream jobs, Harvest does not require an input job
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:
			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- fires parent CombineJob init
				- captures args specific to Harvest jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			# if job name not provided, provide default
			if not job_details['job_name']:
				job_details['job_name'] = self.default_job_name()

			# create Job entry in DB and save
			self.job = Job(
				record_group = record_group,
				job_type = type(self).__name__, # selects this level of class inheritance hierarchy
				user = user,
				name = job_details['job_name'],
				note = job_details['job_note'],
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_details = json.dumps(job_details)
			)
			self.job.save()



class HarvestOAIJob(HarvestJob):

	'''
	Harvest records from OAI-PMH endpoint
	Extends core.models.HarvestJob
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:

			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- fires parent HarvestJob init
		'''

		# perform HarvestJob initialization
		super().__init__(
			user=user,
			job_id=job_id,
			record_group=record_group,
			job_details=job_details)

		# if job_id not provided, assume new Job
		if not job_id:

			# write job details
			self.job.update_job_details(job_details)

			# write validation links
			self.write_validation_job_links(job_details)


	@staticmethod
	def parse_job_type_params(job_details, job_params, kwargs):

		'''
		Method to parse job type specific parameters
		'''

		# retrieve endpoint params
		oai_params = OAIEndpoint.objects.get(pk=int(job_params.get('oai_endpoint_id'))).__dict__.copy()

		# drop _state
		oai_params.pop('_state')

		# retrieve overrides
		overrides = { override:job_params.get(override) for override in ['verb','metadataPrefix','scope_type','scope_value'] if job_params.get(override) != '' }

		# mix in overrides
		for param,value in overrides.items():
			oai_params[param] = value

		# get include <header> toggle
		include_oai_record_header = job_params.get('include_oai_record_header', False);
		if include_oai_record_header == 'true':
			oai_params['include_oai_record_header'] = True
		elif include_oai_record_header == False:
			oai_params['include_oai_record_header'] = False

		# save to job_details
		job_details['oai_params'] = oai_params

		return job_details


	def prepare_job(self, return_job_code=False):

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
			'code':'from jobs import HarvestOAISpark\nHarvestOAISpark(spark, job_id="%(job_id)s").spark_function()' %
			{
				'job_id':self.job.id
			}
		}

		# return job code if requested
		if return_job_code:
			return job_code

		# submit job
		self.submit_job_to_livy(job_code)


	def get_job_errors(self):

		'''
		return harvest job specific errors
		NOTE: Currently, we are not saving errors from OAI harveset, and so, cannot retrieve...
		'''

		return None



class HarvestStaticXMLJob(HarvestJob):

	'''
	Harvest records from static XML files
	Extends core.models.HarvestJob
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:
			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- fires parent HarvestJob init
		'''

		# perform HarvestJob initialization
		super().__init__(
			user=user,
			job_id=job_id,
			record_group=record_group,
			job_details=job_details)


		# if job_id not provided, assume new Job
		if not job_id:

			# write job details
			self.job.update_job_details(job_details)

			# write validation links
			self.write_validation_job_links(job_details)


	@staticmethod
	def parse_job_type_params(job_details, job_params, kwargs):

		'''
		Method to parse job type specific parameters

		Args:
			job_details (dict): in-process job_details dictionary
			job_params (dict): original parameters passed to Job
			kwargs (dict): optional, named args for Jobs
		'''

		# use location on disk
		# When a location on disk is provided, set payload_dir as the location provided
		if job_params.get('static_filepath') != '':
			job_details['type'] = 'location'
			job_details['payload_dir'] = job_params.get('static_filepath')

		# use upload
		# When a payload is uploaded, create payload_dir and set
		else:
			job_details['type'] = 'upload'

			# get static file payload
			payload_file = kwargs['files']['static_payload']

			# grab content type
			job_details['content_type'] = payload_file.content_type

			# create payload dir
			job_details['payload_dir'] = '%s/static_uploads/%s' % (settings.BINARY_STORAGE.split('file://')[-1], str(uuid.uuid4()))
			os.makedirs(job_details['payload_dir'])

			# establish payload filename
			if kwargs['hash_payload_filename']:
				job_details['payload_filename'] = hashlib.md5(payload_file.name.encode('utf-8')).hexdigest()
			else:
				job_details['payload_filename'] = payload_file.name

			# write temporary Django file to disk
			with open(os.path.join(job_details['payload_dir'], job_details['payload_filename']), 'wb') as f:
				f.write(payload_file.read())
				payload_file.close()

			# handle zip files
			if job_details['content_type'] in ['application/zip','application/x-zip-compressed']:
				logger.debug('handling zip file upload')
				zip_filepath = os.path.join(job_details['payload_dir'], job_details['payload_filename'])
				zip_ref = zipfile.ZipFile(zip_filepath, 'r')
				zip_ref.extractall(job_details['payload_dir'])
				zip_ref.close()
				os.remove(zip_filepath)

		# include other information for finding, parsing, and preparing identifiers
		job_details['xpath_document_root'] = job_params.get('xpath_document_root', None)
		job_details['document_element_root'] = job_params.get('document_element_root', None)
		job_details['additional_namespace_decs'] = job_params.get('additional_namespace_decs', None).replace("'",'"')
		job_details['xpath_record_id'] = job_params.get('xpath_record_id', None)

		return job_details


	def prepare_job(self, return_job_code=False):

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
			'code':'from jobs import HarvestStaticXMLSpark\nHarvestStaticXMLSpark(spark, job_id="%(job_id)s").spark_function()' %
			{
				'job_id':self.job.id
			}
		}

		# return job code if requested
		if return_job_code:
			return job_code

		# submit job
		self.submit_job_to_livy(job_code)


	def get_job_errors(self):

		'''
		Currently not implemented for HarvestStaticXMLJob
		'''

		return None



class HarvestTabularDataJob(HarvestJob):

	'''
	Harvest records from tabular data
	Extends core.models.HarvestJob
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:
			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- fires parent HarvestJob init
		'''

		# perform HarvestJob initialization
		super().__init__(
			user=user,
			job_id=job_id,
			record_group=record_group,
			job_details=job_details)


		# if job_id not provided, assume new Job
		if not job_id:

			# write job details
			self.job.update_job_details(job_details)

			# write validation links
			self.write_validation_job_links(job_details)


	@staticmethod
	def parse_job_type_params(job_details, job_params, kwargs):

		'''
		Method to parse job type specific parameters

		Args:
			job_details (dict): in-process job_details dictionary
			job_params (dict): original parameters passed to Job
			kwargs (dict): optional, named args for Jobs
		'''

		# use location on disk
		if job_params.get('static_filepath') != '':
			job_details['payload_filepath'] = job_params.get('static_filepath')

		# use upload
		else:
			# get static file payload
			payload_file = kwargs['files']['static_payload']

			# grab content type
			job_details['content_type'] = payload_file.content_type

			# create payload dir
			job_details['payload_dir'] = '%s/static_uploads/%s' % (settings.BINARY_STORAGE.split('file://')[-1], str(uuid.uuid4()))
			os.makedirs(job_details['payload_dir'])

			# establish payload filename
			if kwargs['hash_payload_filename']:
				job_details['payload_filename'] = hashlib.md5(payload_file.name.encode('utf-8')).hexdigest()
			else:
				job_details['payload_filename'] = payload_file.name

			# filepath
			job_details['payload_filepath'] = os.path.join(job_details['payload_dir'], job_details['payload_filename'])

			# write temporary Django file to disk
			with open(job_details['payload_filepath'], 'wb') as f:
				f.write(payload_file.read())
				payload_file.close()

		# get fm_config_json
		job_details['fm_harvest_config_json'] = job_params.get('fm_harvest_config_json')

		return job_details


	def prepare_job(self, return_job_code=False):

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
			'code':'from jobs import HarvestTabularDataSpark\nHarvestTabularDataSpark(spark, job_id="%(job_id)s").spark_function()' %
			{
				'job_id':self.job.id
			}
		}

		# return job code if requested
		if return_job_code:
			return job_code

		# submit job
		self.submit_job_to_livy(job_code)


	def get_job_errors(self):

		'''
		Currently not implemented for HarvestStaticXMLJob
		'''

		return None



class TransformJob(CombineJob):

	'''
	Apply an XSLT transformation to a Job
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:
			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			# if job name not provided, provide default
			if not job_details['job_name']:
				job_details['job_name'] = self.default_job_name()

			# create Job entry in DB and save
			self.job = Job(
				record_group = record_group,
				job_type = type(self).__name__, # selects this level of class inheritance hierarchy
				user = user,
				name = job_details['job_name'],
				note = job_details['job_note'],
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_details = json.dumps(job_details)
			)
			self.job.save()

			# write job details
			self.job.update_job_details(job_details)

			# write validation links
			self.write_validation_job_links(job_details)

			# write validation links
			self.write_input_job_links(job_details)


	@staticmethod
	def parse_job_type_params(job_details, job_params, kwargs):

		'''
		Method to parse job type specific parameters

		Args:
			job_details (dict): in-process job_details dictionary
			job_params (dict): original parameters passed to Job
			kwargs (dict): optional, named args for Jobs
		'''

		# retrieve input jobs
		job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id') ]

		# retrieve transformation, add details to job details

		# reconstitute json and init job_details
		sel_trans = json.loads(job_params['sel_trans_json'])
		job_details['transformation'] = {
			'scenarios_json':job_params['sel_trans_json'],
			'scenarios':[]
		}

		# loop through and add with name and type
		for trans in sel_trans:
			transformation = Transformation.objects.get(pk=int(trans['trans_id']))
			job_details['transformation']['scenarios'].append({
				'name':transformation.name,
				'type':transformation.transformation_type,
				'type_human':transformation.get_transformation_type_display(),
				'id':transformation.id,
				'index':trans['index']
			})

		return job_details


	def prepare_job(self, return_job_code=False):

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
			'code':'from jobs import TransformSpark\nTransformSpark(spark, job_id="%(job_id)s").spark_function()' %
			{
				'job_id':self.job.id
			}
		}

		# return job code if requested
		if return_job_code:
			return job_code

		# submit job
		self.submit_job_to_livy(job_code)


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
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:
			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			# if job name not provided, provide default
			if not job_details['job_name']:
				job_details['job_name'] = self.default_job_name()

			# create Job entry in DB and save
			self.job = Job(
				record_group = record_group,
				job_type = type(self).__name__, # selects this level of class inheritance hierarchy
				user = user,
				name = job_details['job_name'],
				note = job_details['job_note'],
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_details = json.dumps(job_details)
			)
			self.job.save()

			# write job details
			self.job.update_job_details(job_details)

			# write validation links
			self.write_validation_job_links(job_details)

			# write validation links
			self.write_input_job_links(job_details)


	@staticmethod
	def parse_job_type_params(job_details, job_params, kwargs):

		'''
		Method to parse job type specific parameters

		Args:
			job_details (dict): in-process job_details dictionary
			job_params (dict): original parameters passed to Job
			kwargs (dict): optional, named args for Jobs
		'''

		# retrieve input jobs
		job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id') ]

		return job_details


	def prepare_job(self, return_job_code=False):

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
			'code':'from jobs import MergeSpark\nMergeSpark(spark, job_id="%(job_id)s").spark_function()' %
			{
				'job_id':self.job.id
			}
		}

		# return job code if requested
		if return_job_code:
			return job_code

		# submit job
		self.submit_job_to_livy(job_code)


	def get_job_errors(self):

		'''
		Not current implemented from Merge jobs, as primarily just copying of successful records
		'''

		pass



class AnalysisJob(CombineJob):

	'''
	Analysis job
		- Analysis job are unique in name and some functionality, but closely mirror Merge Jobs in execution
		- Though Analysis jobs are very similar to most typical workflow jobs, they do not naturally
		belong to an Organization and Record Group like others.	As such, they dynamically create their own Org and
		Record Group, configured in localsettings.py, that is hidden from most other views.
	'''

	def __init__(self,
		user=None,
		job_id=None,
		record_group=None,
		job_details=None):

		'''
		Args:
			user (django.auth.User): user account
			job_id (int): Job ID
			record_group (core.models.RecordGroup): RecordGroup instance that Job falls under
			job_details (dict): dictionary for all Job parameters

		Returns:
			None
				- sets multiple attributes for self.job
				- sets in motion the output of spark jobs from core.spark.jobs
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			# if job name not provided, provide default
			if not job_details['job_name']:
				job_details['job_name'] = self.default_job_name()

			# get Record Group for Analysis jobs via AnalysisJob.get_analysis_hierarchy()
			analysis_hierarchy = self.get_analysis_hierarchy()

			# create Job entry in DB and save
			self.job = Job(
				record_group = analysis_hierarchy['record_group'],
				job_type = type(self).__name__, # selects this level of class inheritance hierarchy
				user = user,
				name = job_details['job_name'],
				note = job_details['job_note'],
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_details = json.dumps(job_details)
			)
			self.job.save()

			# write job details
			self.job.update_job_details(job_details)

			# write validation links
			self.write_validation_job_links(job_details)

			# write validation links
			self.write_input_job_links(job_details)


	@staticmethod
	def get_analysis_hierarchy():

		'''
		Method to return organization and record_group for Analysis jobs
			- if do not exist, or name has changed, also create
			- reads from settings.ANALYSIS_JOBS_HIERARCHY for unique names for Organization and Record Group
		'''

		# get Organization and Record Group name from settings
		org_name = settings.ANALYSIS_JOBS_HIERARCHY['organization']
		record_group_name = settings.ANALYSIS_JOBS_HIERARCHY['record_group']

		# check of Analysis jobs aggregating Organization exists
		analysis_org_search = Organization.objects.filter(name=org_name)
		if analysis_org_search.count() == 0:
			logger.debug('creating Organization with name %s' % org_name)
			analysis_org = Organization(
				name = org_name,
				description = 'For the explicit use of aggregating Analysis jobs',
				for_analysis = True
			)
			analysis_org.save()

		# if one found, use
		elif analysis_org_search.count() == 1:
			analysis_org = analysis_org_search.first()

		else:
			raise Exception('multiple Organizations found for explicit purpose of aggregating Analysis jobs')

		# check of Analysis jobs aggregating Record Group exists
		analysis_record_group_search = RecordGroup.objects.filter(name=record_group_name)
		if analysis_record_group_search.count() == 0:
			logger.debug('creating RecordGroup with name %s' % record_group_name)
			analysis_record_group = RecordGroup(
				organization = analysis_org,
				name = record_group_name,
				description = 'For the explicit use of aggregating Analysis jobs',
				for_analysis = True
			)
			analysis_record_group.save()

		# if one found, use
		elif analysis_record_group_search.count() == 1:
			analysis_record_group = analysis_record_group_search.first()

		else:
			raise Exception('multiple Record Groups found for explicit purpose of aggregating Analysis jobs')

		# return Org and Record Group
		return {
			'organization':analysis_org,
			'record_group':analysis_record_group
		}



	@staticmethod
	def parse_job_type_params(job_details, job_params, kwargs):

		'''
		Method to parse job type specific parameters

		Args:
			job_details (dict): in-process job_details dictionary
			job_params (dict): original parameters passed to Job
			kwargs (dict): optional, named args for Jobs
		'''

		# retrieve input job
		job_details['input_job_ids'] = [int(job_id) for job_id in job_params.getlist('input_job_id') ]

		return job_details


	def prepare_job(self, return_job_code=False):

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
			'code':'from jobs import MergeSpark\nMergeSpark(spark, job_id="%(job_id)s").spark_function()' %
			{
				'job_id':self.job.id
			}
		}

		# return job code if requested
		if return_job_code:
			return job_code

		# submit job
		self.submit_job_to_livy(job_code)


	def get_job_errors(self):

		'''
		Not current implemented from Analyze jobs, as primarily just copying of successful records
		'''

		pass



####################################################################
# ElasticSearch DataTables connectors 								 #
####################################################################

class DTElasticFieldSearch(View):

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

		logger.debug('initiating DTElasticFieldSearch connector')

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

		# filtering applied before DataTables input
		filter_type = self.request.GET.get('filter_type', None)
		filter_field = self.request.GET.get('filter_field', None)
		filter_value = self.request.GET.get('filter_value', None)

		# equals filtering
		if filter_type == 'equals':
			logger.debug('equals type filtering')

			# determine if including or excluding
			matches = self.request.GET.get('matches', None)
			if matches and matches.lower() == 'true':
				matches = True
			else:
				matches = False

			# filter query
			logger.debug('filtering by field:value: %s:%s' % (filter_field, filter_value))

			if matches:
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

		# further filter by DT provided keyword
		if self.DTinput['search[value]'] != '':
			logger.debug('general type filtering')
			self.query = self.query.query('match', _all=self.DTinput['search[value]'])


	def sort(self):

		'''
		Sort based on DTinput parameters.

		Note: Sorting is different for the different types of requests made to DTElasticFieldSearch.

		Args:
			None

		Returns:
			None
				- modifies self.query_results
		'''

		# get sort params from DTinput
		sorting_cols = 0
		sort_key = 'order[%s][column]' % (sorting_cols)
		while sort_key in self.DTinput:
			sorting_cols += 1
			sort_key = 'order[%s][column]' % (sorting_cols)

		for i in range(sorting_cols):
			# sorting column
			sort_dir = 'asc'
			sort_col = int(self.DTinput.get('order[%s][column]' % (i)))
			# sorting order
			sort_dir = self.DTinput.get('order[%s][dir]' % (i))

			logger.debug('detected sort: %s / %s' % (sort_col, sort_dir))

		# field per doc (ES Search Results)
		if self.search_type == 'fields_per_doc':

			# determine if field is sortable
			if sort_col < len(self.fields):

				# if db_id, do not add keyword
				if self.fields[sort_col] == 'db_id':
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

		# save request
		self.request = request

		# handle es index
		esi = ESIndex(ast.literal_eval(es_index))
		self.es_index = esi.es_index

		# save DT params
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
		logger.debug('DTElasticFieldSearch calc time: %s' % (time.time()-stime))

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
			record = Record.objects.get(id=hit.db_id)

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


	def values_per_field(self, terms_limit=10000):

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
		self.query.aggs.bucket(self.field, A('terms', field='%s.keyword' % self.field, size=terms_limit))

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
		for index, row in self.query_results.iterrows():

			# iterate through columns and place in list
			row_data = [row.key, row.doc_count]

			# add list to object
			self.DToutput['data'].append(row_data)



class DTElasticGenericSearch(View):

	'''
	Model to query ElasticSearch and return DataTables ready JSON.
	This model is a Django Class-based view.
	This model is located in core.models, as it still may function seperate from a Django view.
	'''

	def __init__(self,
			fields=['db_id','combine_id','record_id'],
			es_index='j*',
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

		logger.debug('initiating DTElasticGenericSearch connector')

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

		logger.debug('DTElasticGenericSearch: filtering')

		# get search string if present
		search_term = self.request.GET.get('search[value]')

		if search_term != '':
			logger.debug('searching ES for: %s' % search_term)
			self.query = self.query.query('match', _all="'%s'" % search_term.replace("'","\'"))


	def sort(self):

		'''
		Sort based on DTinput parameters.

		Note: Sorting is different for the different types of requests made to DTElasticFieldSearch.

		Args:
			None

		Returns:
			None
				- modifies self.query_results
		'''

		# if using deep paging, will need to implement some sorting to search_after
		self.query = self.query.sort('record_id.keyword','db_id')


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
		self.query = self.query[start : (start + length)]

		# use search_after for "deep paging"
		'''
		This will require capturing current sorts from the DT table, and applying last
		value here
		'''
		# self.query = self.query.extra(search_after=['036182a450f31181cf678197523e2023',1182966])


	def to_json(self):

		'''
		Return DToutput as JSON

		Returns:
			(json)
		'''

		return json.dumps(self.DToutput)


	def get(self, request):

		'''
		Django Class-based view, GET request.

		Args:
			request (django.request): request object
			es_index (str): ES index
		'''

		# save parameters to self
		self.request = request
		self.DTinput = self.request.GET

		# time respond build
		stime = time.time()

		# execute search
		self.search()

		# end time
		logger.debug('DTElasticGenericSearch: response time %s' % (time.time()-stime))

		# for all search types, build and return response
		return JsonResponse(self.DToutput)


	def search(self):

		'''
		Execute search
		'''

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

			try:
				# get combine record
				record = Record.objects.get(pk=int(hit.db_id))

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

				# add record lineage in front
				row_data = self._prepare_record_hierarchy_links(record, row_data)

				# add list to object
				self.DToutput['data'].append(row_data)
			except Exception as e:
				logger.debug("error retrieving DB record based on id %s, from index %s: %s" % (hit.db_id, hit.meta.index, str(e)))


	def _prepare_record_hierarchy_links(self, record, row_data):

		'''
		Method to prepare links based on the hierarchy of the Record
		'''

		urls = record.get_lineage_url_paths()

		to_append = [
			'<a href="%s">%s</a>' % (urls['organization']['path'], urls['organization']['name']),
			'<a href="%s">%s</a>' % (urls['record_group']['path'], urls['record_group']['name']),
			'<a href="%s"><span class="%s">%s</span></a>' % (urls['job']['path'], record.job.job_type_family(), urls['job']['name']),
			urls['record']['path'],
		]

		return to_append + row_data



####################################################################
# Published Records Test Clients									 #
####################################################################

class CombineOAIClient(object):

	'''
	This class provides a client to test the built-in OAI server for Combine
	'''

	def __init__(self):

		# initiate sickle instance
		self.sickle = sickle.Sickle(settings.COMBINE_OAI_ENDPOINT)

		# set default metadata prefix
		# NOTE: Currently Combine's OAI server does not support this, a nonfunctional default is provided
		self.metadata_prefix = None

		# save results from identify
		self.identify = self.sickle.Identify()


	def get_records(self, oai_set=None):

		'''
		Method to return generator of records

		Args:
			oai_set ([str, sickle.models.Set]): optional OAI set, string or instance of Sickle Set to filter records
		'''

		# if oai_set is provided, filter records to set
		if oai_set:
			if type(oai_set) == sickle.models.Set:
				set_name = oai_set.setName
			elif type(oai_set) == str:
				set_name = oai_set

			# return records filtered by set
			return self.sickle.ListRecords(set=set_name, metadataPrefix=self.metadata_prefix)

		# no filter
		return self.sickle.ListRecords(metadataPrefix=self.metadata_prefix)


	def get_identifiers(self, oai_set=None):

		'''
		Method to return generator of identifiers

		Args:
			oai_set ([str, sickle.models.Set]): optional OAI set, string or instance of Sickle Set to filter records
		'''

		# if oai_set is provided, filter record identifiers to set
		if oai_set:
			if type(oai_set) == sickle.models.Set:
				set_name = oai_set.setName
			elif type(oai_set) == str:
				set_name = oai_set

			# return record identifiers filtered by set
			return self.sickle.ListIdentifiers(set=set_name, metadataPrefix=self.metadata_prefix)

		# no filter
		return self.sickle.ListIdentifiers(metadataPrefix=self.metadata_prefix)


	def get_sets(self):

		'''
		Method to return generator of all published sets
		'''

		return self.sickle.ListSets()


	def get_record(self, oai_record_id):

		'''
		Method to return a single record
		'''

		return sickle.GetRecord(identifier = oai_record_id, metadataPrefix = self.metadata_prefix)


####################################################################
# Identifier Transformation Scenario								 #
####################################################################

class RITSClient(object):

	'''
	class to handle the record_id transformation scenarios
	'''

	def __init__(self, query_dict):

		logger.debug('initializaing RITS')

		self.qd = query_dict

		# parse data
		self.target = self.qd.get('record_id_transform_target', None)
		logger.debug('target is %s' % self.target)

		# parse regex
		if self.qd.get('record_id_transform_type', None) == 'regex':

			# set type
			self.transform_type = 'regex'

			logger.debug('parsing as %s type transformation' % self.transform_type)

			# get args
			self.regex_match = self.qd.get('regex_match_payload', None)
			self.regex_replace = self.qd.get('regex_replace_payload', None)

		# parse python
		if self.qd.get('record_id_transform_type', None) == 'python':

			# set type
			self.transform_type = 'python'

			logger.debug('parsing as %s type transformation' % self.transform_type)

			# get args
			self.python_payload = self.qd.get('python_payload', None)

		# parse xpath
		if self.qd.get('record_id_transform_type', None) == 'xpath':

			# set type
			self.transform_type = 'xpath'

			logger.debug('parsing as %s type transformation' % self.transform_type)

			# get args
			self.xpath_payload = self.qd.get('xpath_payload', None)

		# capture test data if
		self.test_input = self.qd.get('test_transform_input', None)


	def test_user_input(self):

		'''
		method to test record_id transformation based on user input
		'''

		# handle regex
		if self.transform_type == 'regex':
			trans_result = re.sub(self.regex_match, self.regex_replace, self.test_input)


		# handle python
		if self.transform_type == 'python':

			if self.target == 'record_id':
				sr = PythonUDFRecord(None, non_row_input = True, record_id = self.test_input)
			if self.target == 'document':
				sr = PythonUDFRecord(None, non_row_input = True, document = self.test_input)

			# parse user supplied python code
			temp_mod = ModuleType('temp_mod')
			exec(self.python_payload, temp_mod.__dict__)

			try:
				trans_result = temp_mod.transform_identifier(sr)
			except Exception as e:
				trans_result = str(e)


		# handle xpath
		if self.transform_type == 'xpath':

			if self.target == 'record_id':
				trans_result = 'XPath only works for Record Document'

			if self.target == 'document':
				sr = PythonUDFRecord(None, non_row_input=True, document = self.test_input)

				# attempt xpath
				xpath_results = sr.xml.xpath(self.xpath_payload, namespaces = sr.nsmap)
				n = xpath_results[0]
				trans_result = n.text


		# return dict
		r_dict = {
			'results':trans_result,
			'success':True
		}
		return r_dict


	def params_as_json(self):

		'''
		Method to generate the required parameters to include in Spark job
		'''

		return json.dumps(self.__dict__)



####################################################################
# DPLA Service Hub and Bulk Data 								   #
####################################################################

class DPLABulkDataClient(object):

	'''
	Client to faciliate browsing, downloading, and indexing of bulk DPLA data

	Args:
		filepath (str): optional filepath for downloaded bulk data on disk
	'''

	def __init__(self):

		self.service_hub_prefix = settings.SERVICE_HUB_PREFIX
		self.combine_oai_identifier = settings.COMBINE_OAI_IDENTIFIER
		self.bulk_dir = '%s/bulk' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]

		# ES
		self.es_handle = es_handle

		# S3
		self.s3 = boto3.resource('s3')

		# DPLA bucket
		self.dpla_bucket = self.s3.Bucket(settings.DPLA_S3_BUCKET)

		# boto3 client
		self.boto_client = boto3.client('s3')



	def download_bulk_data(self, object_key, filepath):

		'''
		Method to bulk download a service hub's data from DPLA's S3 bucket
		'''

		# create bulk directory if not already present
		if not os.path.exists(self.bulk_dir):
			os.mkdir(self.bulk_dir)

		# download
		s3 = boto3.resource('s3')
		download_results = self.dpla_bucket.download_file(object_key, filepath)

		# return
		return download_results


	def get_bulk_reader(self, filepath, compressed=True):

		'''
		Return instance of BulkDataJSONReader
		'''

		return BulkDataJSONReader(filepath, compressed=compressed)


	def get_sample_record(self, filepath):

		return self.get_bulk_reader(filepath).get_next_record()


	def index_to_es(self, object_key, filepath, limit=False):

		'''
		Use streaming bulk indexer:
		http://elasticsearch-py.readthedocs.io/en/master/helpers.html
		'''

		stime = time.time()

		##	prepare index
		index_name = hashlib.md5(object_key.encode('utf-8')).hexdigest()
		logger.debug('indexing to %s' % index_name)

		# if exists, delete
		if es_handle.indices.exists(index_name):
			es_handle.indices.delete(index_name)
		# set mapping
		mapping = {
			'mappings':{
				'item':{
					'date_detection':False
				}
			}
		}
		# create index
		self.es_handle.indices.create(index_name, body=json.dumps(mapping))

		# get instance of bulk reader
		bulk_reader = self.get_bulk_reader(filepath)

		# index using streaming
		for i in es.helpers.streaming_bulk(self.es_handle, bulk_reader.es_doc_generator(bulk_reader.get_record_generator(limit=limit, attr='record'), index_name=index_name), chunk_size=500):
			continue

		logger.debug("index to ES elapsed: %s" % (time.time() - stime))

		# return
		return index_name


	def retrieve_keys(self):

		'''
		Method to retrieve and parse key structure from S3 bucket

		Note: boto3 only returns 1000 objects from a list_objects
			- as such, need to add delimiters and prefixes to walk keys
			- OR, use bucket.objects.all() --> iterator
		'''

		stime = time.time()

		# get and return list of all keys
		keys = []
		for obj in self.dpla_bucket.objects.all():
			key = {
				'key':obj.key,
				'year':obj.key.split('/')[0],
				'month':obj.key.split('/')[1],
				'size':self._sizeof_fmt(int(obj.size))
			}
			keys.append(key)

		# return
		logger.debug('retrieved %s keys in %s' % (len(keys), time.time()-stime))
		return keys


	def _sizeof_fmt(self, num, suffix='B'):

		'''
		https://stackoverflow.com/a/1094933/1196358
		'''

		for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
			if abs(num) < 1024.0:
				return "%3.1f%s%s" % (num, unit, suffix)
			num /= 1024.0
		return "%.1f%s%s" % (num, 'Yi', suffix)


	def download_and_index_bulk_data(self, object_key):

		'''
		Method to init background tasks of downloading and indexing bulk data
		'''

		# get object
		obj = self.s3.Object(self.dpla_bucket.name, object_key)

		# init DPLABulkDataDownload (dbdd) instance
		dbdd = DPLABulkDataDownload()

		# set key
		dbdd.s3_key = object_key

		# set filepath
		dbdd.filepath = '%s/%s' % (self.bulk_dir, object_key.replace('/','_'))

		# set bulk data timestamp (when it was uploaded to S3 from DPLA)
		dbdd.uploaded_timestamp = obj.last_modified

		# save
		dbdd.save()

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Download and Index DPLA Bulk Data: %s' % dbdd.s3_key,
			task_type = 'download_and_index_bulk_data',
			task_params_json = json.dumps({
				'dbdd_id':dbdd.id
			})
		)
		ct.save()

		# run celery task
		bg_task = tasks.download_and_index_bulk_data.delay(dbdd.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()



class BulkDataJSONReader(object):

	'''
	Class to handle the reading of DPLA bulk data
	'''

	def __init__(self, input_file, compressed=True):

		self.input_file = input_file
		self.compressed = compressed

		# not compressed
		if not self.compressed:
			self.file_handle = open(self.input_file,'rb')

		# compressed
		if self.compressed:
			self.file_handle = gzip.open(self.input_file, 'rb')

		# bump file handle
		next(self.file_handle)
		self.records_gen = self.file_handle


	def get_next_record(self):

		r_string = next(self.file_handle).decode('utf-8').lstrip(',')
		return DPLARecord(r_string)


	def get_record_generator(self, limit=False, attr=None):

		i = 0
		while True:
			i += 1
			try:
				# if attr provided, return attribute of record
				if attr:
					yield getattr(self.get_next_record(), attr)
				# else, return whole record
				else:
					yield self.get_next_record()
				if limit and i >= limit:
					break
			except JSONDecodeError:
				break


	def es_doc_generator(self, rec_gen, index_name=str(uuid.uuid4())):

		'''
		Create generator for explicit purpose of indexing to ES
			- pops _id and _rev from _source
			- writes custom _index
		'''

		for r in rec_gen:

			# pop values
			for f in ['_id','_rev','originalRecord']:
				try:
					r['_source'].pop(f)
				except:
					pass

			# write new index
			r['_index'] = index_name

			# yield
			yield r



class DPLARecord(object):

	'''
	Small class to model a parsed DPLA JSON record
	'''

	def __init__(self, record):

		'''
		Expecting dictionary or json of record
		'''

		if type(record) in [dict, OrderedDict]:
			self.record = record
		elif type(record) == str:
			self.record = json.loads(record)

		# capture convenience values
		self.pre_hash_record_id = self.record['_id']
		self.dpla_id = self.record['_source']['id']
		self.dpla_url = self.record['_source']['@id']
		self.dpla_es_index = self.record['_index']
		try:
			self.original_metadata = self.record['_source']['originalRecord']['metadata']
		except:
			self.original_metadata = False
		self.metadata_string = str(self.original_metadata)



####################################################################
# OpenRefine Actions Client 									   #
####################################################################

class OpenRefineActionsClient(object):

	'''
	This class / client is to handle the transformation of Record documents (XML)
	using the history of actions JSON output from OpenRefine.
	'''

	def __init__(self, or_actions=None):

		'''
		Args:
			or_actions_json (str|dict): raw json or dictionary
		'''

		# handle or_actions
		if type(or_actions) == str:
			logger.debug('parsing or_actions as JSON string')
			self.or_actions_json = or_actions
			self.or_actions = json.loads(or_actions)
		elif type(or_actions) == dict:
			logger.debug('parsing or_actions as dictionary')
			self.or_actions_json = json.dumps(or_actions)
			self.or_actions = or_actions
		else:
			logger.debug('not parsing or_actions, storing as-is')
			self.or_actions = or_actions



####################################################################
# Supervisor RPC Server Client   								   #
####################################################################

class SupervisorRPCClient(object):

	def __init__(self):

		self.server = xmlrpc_client.ServerProxy('http://localhost:9001/RPC2')


	def get_server_state(self):

		return self.server.supervisor.getState()


	def list_processes(self):

		return self.server.supervisor.getAllProcessInfo()


	def check_process(self, process_name):

		return self.server.supervisor.getProcessInfo(process_name)


	def start_process(self, process_name):

		return self.server.supervisor.startProcess(process_name)


	def stop_process(self, process_name):

		return self.server.supervisor.stopProcess(process_name)


	def restart_process(self, process_name):

		'''
		RPC throws Fault 70 if not running, catch when stopping
		'''

		# attempt to stop
		try:
			self.stop_process(process_name)
		except Exception as e:
			logger.debug(str(e))

		# start process
		return self.start_process(process_name)


	def stdout_log_tail(self, process_name, offset=0, length=10000):

		return self.server.supervisor.tailProcessStdoutLog(process_name, offset, length)[0]


	def stderr_log_tail(self, process_name, offset=0, length=10000):

		return self.server.supervisor.tailProcessStderrLog(process_name, offset, length)[0]



####################################################################
# Global Message Client         								   #
####################################################################

class GlobalMessageClient(object):

	'''
	Client to handle CRUD for global messages

	Message dictionary structure {
		html (str): string of HTML content to display
		class (str): class of Bootstrap styling, [success, warning, danger, info]
		id (uuid4): unique id for removing
	}
	'''

	def __init__(self, session=None):

		# use session if provided
		if type(session) == SessionStore:
			self.session = session

		# if session_key provided, use
		elif type(session) == str:
			self.session = SessionStore(session_key=session)

		# else, set to session
		else:
			self.session = session


	def load_most_recent_session(self):

		'''
		Method to retrieve most recent session
		'''

		s = Session.objects.order_by('expire_date').last()
		self.__init__(s.session_key)
		return self


	def add_gm(self, gm_dict):

		'''
		Method to add message
		'''

		# check for 'gms' key in session, create if not present
		if 'gms' not in self.session:
			self.session['gms'] = []

		# create unique id and add
		if 'id' not in gm_dict:
			gm_dict['id'] = uuid.uuid4().hex

		# append gm dictionary
		self.session['gms'].append(gm_dict)

		# save
		self.session.save()


	def delete_gm(self, gm_id):

		'''
		Method to remove message
		'''

		if 'gms' not in self.session:
			logger.debug('no global messages found, returning False')
			return False

		else:

			logger.debug('removing gm: %s' % gm_id)

			# grab total gms
			pre_len = len(self.session['gms'])

			# loop through messages to find and remove
			self.session['gms'][:] = [gm for gm in self.session['gms'] if gm.get('id') != gm_id]
			self.session.save()

			# return results
			return pre_len - len(self.session['gms'])


	def clear(self):

		'''
		Method to clear all messages
		'''

		self.session.clear()
		self.session.save()



####################################################################
# State Export/Import Client       								   #
####################################################################

class StateIOClient(object):

	'''
	Client to facilitate export and import of states in Combine, including:
		- Organizations, Record Groups, and Jobs
		- Records, Validations, and Mapped Fields associated with Jobs
		- configuration Scenarios
			- Transformation
			- Validation
			- RITS
			- DPLA Bulk Data Downloads (?)
	'''


	# model translation for serializable strings for models
	model_translation = {
		'jobs':Job,
		'record_groups':RecordGroup,
		'orgs':Organization,
		'dbdd':DPLABulkDataDownload,
		'oai_endpoints':OAIEndpoint,
		'rits':RecordIdentifierTransformationScenario,
		'transformations':Transformation,
		'validations':ValidationScenario
	}


	def __init__(self):

		# ensure working directories exist
		self._confirm_working_dirs()


	def _confirm_working_dirs(self):

		'''
		Method to ensure working dirs exist, and are writable
		'''

		# check for exports directory
		if not os.path.isdir(settings.STATEIO_EXPORT_DIR):
			logger.debug('creating StateIO working directory: %s' % settings.STATEIO_EXPORT_DIR)
			os.makedirs(settings.STATEIO_EXPORT_DIR)

		# check for imports directory
		if not os.path.isdir(settings.STATEIO_IMPORT_DIR):
			logger.debug('creating StateIO working directory: %s' % settings.STATEIO_IMPORT_DIR)
			os.makedirs(settings.STATEIO_IMPORT_DIR)


	def export_state(self,
		jobs=[],
		record_groups=[],
		orgs=[],
		config_scenarios=[],
		export_name=None,
		compress=True,
		compression_format='zip',
		stateio_id=None):

		'''
		Method to export state of passed Organization, Record Group, and/or Jobs

		Args:
			jobs (list): List of Jobs to export, accept instance or id
			record_groups (list): List of Record Groups to export, accept instance or id
			orgs (list): List of Organizations to export, accept instance or id
			config_scenarios (list): List of configuration-related model *instances*
				- sorted based on type(instance) and added to self.export_dict
			compress (bool): If True, compress to zip file and delete directory
			compression_format (str): Possible values here: https://docs.python.org/3.5/library/shutil.html#shutil.make_archive
			stateio (int): ID of pre-existing, associated StateIO instance
		'''

		# DEBUG
		stime = time.time()

		# location of export created, or imported
		self.export_path = None

		# dictionary used to aggregate components for export
		self.export_dict = {

			# job related
			'jobs':set(),
			'record_groups':set(),
			'orgs':set(),
			'job_inputs':set(),
			'job_validations':set(),

			# config scenarios
			'validations':set(),
			'transformations':set(),
			'oai_endpoints':set(),
			'rits':set(),
			'field_mapper_configs':set(),
			'dbdd':set()

		}

		# save compression flag
		self.compress = compress
		self.compression_format = compression_format

		# set export_roots with model instances
		self.export_roots = {
			'jobs':[ Job.objects.get(pk=int(job)) if type(job) in [int,str] else job for job in jobs ],
			'record_groups':[ RecordGroup.objects.get(pk=int(rg)) if type(rg) in [int,str] else rg for rg in record_groups ],
			'orgs':[ Organization.objects.get(pk=int(org)) if type(org) in [int,str] else org for org in orgs ],
		}
		# set export_root_ids with model ids
		self.export_roots_ids = {
			'jobs':[ job.id for job in self.export_roots['jobs'] ],
			'record_groups': [ record_group.id for record_group in self.export_roots['record_groups'] ],
			'orgs': [ org.id for org in self.export_roots['orgs'] ]
		}

		# unique hash for export, used for filepath and manifest
		export_id = uuid.uuid4().hex

		# prepare tmp location for export
		self.export_path = '%s/%s' % (settings.STATEIO_EXPORT_DIR, export_id)
		os.mkdir(self.export_path)

		# init export manifest dictionary

		self.export_manifest = {
			'combine_version':getattr(settings,'COMBINE_VERSION', None),
			'export_id':export_id,
			'export_path':self.export_path,
			'export_name':export_name,
			'export_root_ids':self.export_roots_ids,
			'jobs':[]
		}

		# init/update associated StateIO instance
		sio_dict = {
			'stateio_type':'export',
			'name':self.export_manifest['export_name'],
			'export_id':self.export_manifest['export_id'],
			'export_path':self.export_manifest['export_path'],
			'export_manifest':self.export_manifest,
			'status':'running'
		}
		if stateio_id == None:
			logger.debug('initializing StateIO object')
			self.stateio = StateIO(**sio_dict)
		else:
			logger.debug('retrieving and updating StateIO object')
			self.stateio = StateIO.objects.get(id=stateio_id)
			self.stateio.update(**sio_dict)
			self.stateio.reload()
		# save
		self.stateio.save()

		# generate unique set of Jobs
		self.export_job_set = self._get_unique_roots_job_set()

		# if jobs present from export_roots
		if len(self.export_job_set) > 0:

			# based on Jobs identified from export_roots, collect all connected Jobs
			self._collect_related_jobs()

			# based on network of Jobs, collection associated components
			self._collect_related_components()

		# handle non-Job related configuration scenarios passed
		if len(config_scenarios) > 0:
			self._sort_discrete_config_scenarios(config_scenarios)

		# serialize for export
		self.package_export()

		# update associated StateIO instance
		self.stateio.update(**{
			'export_manifest':self.export_manifest,
			'export_path':self.export_manifest['export_path'],
			'status':'finished',
			'finished':True
		})
		self.stateio.reload()
		self.stateio.save()

		# debug
		logger.debug("total time for state export: %s" % (time.time() - stime))


	def _get_unique_roots_job_set(self):

		'''
		Method to get unique set of Jobs from self.export_roots
			- export_roots may contain Organizations, Record Groups, and Jobs
		'''

		# extend self.export_roots['record_groups'] with Record Groups from Orgs
		for org in self.export_roots['orgs']:
			self.export_roots['record_groups'].extend(org.recordgroup_set.all())

		# extend self.export_roots['jobs'] with Jobs from Record Groups
		for rg in self.export_roots['record_groups']:
			self.export_roots['jobs'].extend(rg.job_set.all())

		# ensure all Jobs unique and return
		self.export_roots['jobs'] = list(set(self.export_roots['jobs']))
		return self.export_roots['jobs']


	def _collect_related_jobs(self, include_upstream=True):

		'''
		Method to collect all connected Jobs based on Jobs identified
		in export_roots['jobs'].

		Topographically sorts all Jobs, to determine order.

		First:
			- Connected Jobs from lineage
				- upstream, downstream

		Second:
			- Related scenarios (mostly, to facilitate re-running once re-imported)
				- OAI endpoint
				- transformation
				- validation
				- RITS <------- waiting
				- DBDD <------- waiting
				- field mapper configurations <------- waiting, might need to store ID in job_details?
			- ElasticSearch index for each Job
			- Records from Mongo
			- Validations from Mongo

		TODO:
			- break pieces into sub-methods

		Args:
			include_upstream (bool): Boolean to also collect "upstream" Jobs
				- defaults to True, to safely ensure that Jobs exported/imported have all
				supporting infrastructure

		Returns:
			None
				- sets final list of Jobs to export at self.export_dict['jobs'],
				topographically sorted and unique
		'''

		############################
		# JOBS
		############################

		# loop through Jobs identified from roots and queue
		for job in self.export_roots['jobs']:

			# add job
			self.export_dict['jobs'].add(job)

			# get downstream
			downstream_jobs = job.get_downstream_jobs()
			self.export_dict['jobs'].update(downstream_jobs)

			if include_upstream:

				# get upstream for all Jobs currently queued in export_dict
				upstream_jobs = []
				for job in self.export_dict['jobs']:

					# get lineage
					# TODO: replace with new upstream lineage method that's coming
					lineage = job.get_lineage()

					# loop through nodes and add to set
					for lineage_job_dict in lineage['nodes']:
						upstream_jobs.append(Job.objects.get(pk=int(lineage_job_dict['id'])))

				# update set with upstream
				self.export_dict['jobs'].update(upstream_jobs)


		# topopgraphically sort all queued export jobs, and write to manifest
		self.export_dict['jobs'] = Job._topographic_sort_jobs(self.export_dict['jobs'])
		self.export_manifest['jobs'] = [ job.id for job in self.export_dict['jobs'] ]


	def _collect_related_components(self):

		'''
		Method to collect related components based on self.export_dict['jobs'],
		and items from self.

		All operate over self.export_dict['jobs'], updating other sections of
		self.export_dict

		TODO:
			- these would benefit for more error and existence checking
				- even if dependencies are not found, exports should continue
		'''

		###################################
		# ORGANIZATIONS and RECORD GROUPS
		###################################

		# extend self.export_dict['orgs'] and self.export_dict['record_groups']
		for job in self.export_dict['jobs']:
			self.export_dict['orgs'].add(job.record_group.organization)
			self.export_dict['record_groups'].add(job.record_group)


		############################
		# JOBS: Job Input
		############################

		# get all related Job Inputs
		job_inputs = JobInput.objects.filter(
			job__in=self.export_dict['jobs'],
			input_job__in=self.export_dict['jobs'])

		# write to serialize set
		self.export_dict['job_inputs'].update(job_inputs)


		############################
		# JOBS: Job Validation
		############################

		# get all related Job Inputs
		job_validations = JobValidation.objects.filter(job__in=self.export_dict['jobs'])

		# write to serialize set
		self.export_dict['job_validations'].update(job_validations)


		############################
		# TRANSFORMATION SCENARIOS
		############################

		# loop through Jobs, looking for Transformation Scenarios
		for job in self.export_dict['jobs']:

			# check job details for transformation used
			if 'transformation' in job.job_details_dict.keys():

				try:
					for trans in job.job_details_dict['transformation']['scenarios']:
						self.export_dict['transformations'].add(Transformation.objects.get(pk=int(trans['id'])))
				except Exception as e:
					logger.warning('Could not export Transformations for job %s: %s' % (job, str(e)))


		############################
		# VALIDATION SCENARIOS
		############################

		# loop through Jobs, looking for Validations applied Scenarios
		for job in self.export_dict['jobs']:

			# check for JobValidation instances
			jvs = JobValidation.objects.filter(job=job)

			# loop through and add to set
			for jv in jvs:
				self.export_dict['validations'].add(jv.validation_scenario)


		############################
		# OAI ENDPOINTS
		############################

		# loop through Jobs, looking for OAI endpoints that need exporting
		for job in self.export_dict['jobs']:

			if job.job_type == 'HarvestOAIJob':

				try:
					# read OAI endpoint from params
					self.export_dict['oai_endpoints'].add(OAIEndpoint.objects.get(pk=job.job_details_dict['oai_params']['id']))
				except Exception as e:
					logger.warning('Could not export OAIEndpoint for job %s: %s' % (job, str(e)))


		############################
		# RITS
		############################

		# loop through Jobs, looking for RITS Scenarios
		for job in self.export_dict['jobs']:

			# check job details for rits used
			if 'rits' in job.job_details_dict.keys() and job.job_details_dict['rits'] != None:
				try:
					self.export_dict['rits'].add(RecordIdentifierTransformationScenario.objects.get(pk=(job.job_details_dict['rits'])))
				except Exception as e:
					logger.warning('Could not export Record Identifier Transformation Scenario for job %s: %s' % (job, str(e)))


		############################
		# DBDD
		############################

		# loop through Jobs, looking for DBDD used
		for job in self.export_dict['jobs']:

			# check job details for DBDD used
			if 'dbdm' in job.job_details_dict.keys() and job.job_details_dict['dbdm']['dbdd'] != None:

				logger.debug('attempting to export dbdd_id %s for %s' % (job.job_details_dict['dbdm']['dbdd'], job))

				try:
					# get dbdd
					dbdd = DPLABulkDataDownload.objects.get(pk=(job.job_details_dict['dbdm']['dbdd']))

					# add to export_dict
					self.export_dict['dbdd'].add(dbdd)

					# export DBDD index from ElaticSearch
					# prepare dbdd export dir
					dbdd_export_path = '%s/dbdd' % self.export_path
					if not os.path.isdir(dbdd_export_path):
						os.mkdir(dbdd_export_path)

					# build command list
					cmd = [
						"elasticdump",
						"--input=http://localhost:9200/%s" % dbdd.es_index,
						"--output=%(dbdd_export_path)s/dbdd%(dbdd_id)s.json" % {'dbdd_export_path':dbdd_export_path, 'dbdd_id':dbdd.id},
						"--type=data",
						"--ignore-errors",
						"--noRefresh"
					]

					logger.debug("elasticdump cmd: %s" % cmd)

					# run cmd
					os.system(" ".join(cmd))

				except Exception as e:
					logger.debug('could not export DBDD for job %s: %s' % (job, str(e)))


		############################
		# JOB RECORDS (Mongo)
		############################

		# prepare records export dir
		record_exports_path = '%s/record_exports' % self.export_path
		os.mkdir(record_exports_path)

		# loop through jobs and export
		for job in self.export_dict['jobs']:

			# prepare command
			cmd = 'mongoexport --db combine --collection record --out %(record_exports_path)s/j%(job_id)s_mongo_records.json --type=json -v --query \'{"job_id":%(job_id)s}\'' % {
				'job_id':job.id,
				'record_exports_path':record_exports_path
			}

			logger.debug("mongoexport cmd: %s" % cmd)

			# run
			os.system(cmd)


		############################
		# JOB VALIDATIONS (Mongo)
		############################

		# prepare records export dir
		validation_exports_path = '%s/validation_exports' % self.export_path
		os.mkdir(validation_exports_path)

		# loop through jobs and export
		for job in self.export_dict['jobs']:

			# prepare command
			cmd = 'mongoexport --db combine --collection record_validation --out %(validation_exports_path)s/j%(job_id)s_mongo_validations.json --type=json -v --query \'{"job_id":%(job_id)s}\'' % {
				'job_id':job.id,
				'validation_exports_path':validation_exports_path
			}

			logger.debug("mongoexport cmd: %s" % cmd)

			# run
			os.system(cmd)


		############################
		# JOB MAPPED FIELDS (ES)
		############################
		'''
		Consider: elasticdump
		'''

		# prepare records export dir
		es_export_path = '%s/mapped_fields_exports' % self.export_path
		os.mkdir(es_export_path)

		# loop through jobs and export
		for job in self.export_dict['jobs']:

			# build command list
			cmd = [
				"elasticdump",
				"--input=http://localhost:9200/j%s" % job.id,
				"--output=%(es_export_path)s/j%(job_id)s_mapped_fields.json" % {'es_export_path':es_export_path, 'job_id':job.id},
				"--type=data",
				"--sourceOnly",
				"--ignore-errors",
				"--noRefresh"
			]

			logger.debug("elasticdump cmd: %s" % cmd)

			# run cmd
			os.system(" ".join(cmd))


	def _sort_discrete_config_scenarios(self, config_scenarios):

		'''
		Method to sort and add configuration scenarios to list of model instances
		in self.export_dict for eventual serialization

		Sorting to these:
			'validations':set(),
			'transformations':set(),
			'oai_endpoints':set(),
			'rits':set(),
			'field_mapper_configs':set(),
			'dbdd':set()

		Args:
			config_scenarios (list): List of model instances, or specially prefixed ids

		'''

		logger.debug('sorting passed discrete configuration scenarios')

		# establish sort hash
		sorting_hash = {
			ValidationScenario:'validations',
			Transformation:'transformations',
			OAIEndpoint:'oai_endpoints',
			RecordIdentifierTransformationScenario:'rits',
			FieldMapper:'field_mapper_configs',
			DPLABulkDataDownload:'dbdd'
		}

		# invert sorting_hash for id prefixes
		id_prefix_hash = { v:k for k,v in sorting_hash.items() }

		# loop through passed model instances
		for config_scenario in config_scenarios:

			logger.debug('adding to export_dict for serialization: %s' % config_scenario)

			# if prefixed string passed, retrieve model instance
			if type(config_scenario) == str and '|' in config_scenario:
				model_type, model_id = config_scenario.split('|')
				config_scenario = id_prefix_hash[model_type].objects.get(pk=int(model_id))

			# slot to export dict through hash
			self.export_dict[sorting_hash[type(config_scenario)]].add(config_scenario)


	def package_export(self):

		'''
		Method to serialize model instances, and combine with serializations already on disk

			- for each model instance, serialize to JSON
			- write as JSON lines (?) to file, save to disk
			- likely, thinking through now, a directory that was started at export_state()
				- this can be where Records and ES go as well, all tar.gz'ed at the end

			- OR, consider sql dumping?
				- will have organized list of model instances, across multiple tables
					- command line:
					mysqldump -ucombine -p combine core_job --skip-create-options --skip-add-drop-table --no-create-info --where="id IN (4,5)" > /tmp/sqldump.sql
					- SQL statement, which could be issued from python

		'''

		# serialize Django model instances
		with open('%s/django_objects.json' % self.export_path,'w') as f:

			# combine all model instances, across model types
			to_serialize = []
			for k in self.export_dict.keys():
				to_serialize.extend(self.export_dict[k])

			# write as single JSON file
			f.write(serializers.serialize('json', to_serialize))

		# finalize export_manifest
		self._finalize_export_manifest()

		# write export_manifest
		with open('%s/export_manifest.json' % self.export_path,'w') as f:
			f.write(json.dumps(self.export_manifest))

		# if compressing, zip up directory, and remove originals after archive created
		if self.compress:

			logger.debug("compressiong exported state at %s" % self.export_path)

			# establish output archive file
			export_filename = '%s/%s' % (settings.STATEIO_EXPORT_DIR, self.export_manifest['export_id'])

			# use shutil to zip up
			compress_stime = time.time()
			new_export_path = shutil.make_archive(
				export_filename,
				self.compression_format,
				settings.STATEIO_EXPORT_DIR,
				self.export_manifest['export_id']
			)
			logger.debug('archive %s created in %ss' % (new_export_path, (time.time() - compress_stime)))

			# remove originals
			shutil.rmtree(self.export_path)

			# update export path and manifeset
			self.export_path = new_export_path
			self.export_manifest['export_path'] = self.export_path

		# return export_path
		return self.export_path


	def _finalize_export_manifest(self):

		'''
		Method to finalize export_manifest before writing to export
			- loop through self.export_dict for export types that are human meaningful
		'''

		# establish section in export_manifest
		self.export_manifest['exports'] = {
			'jobs':[],
			'record_groups':[],
			'orgs':[],
			'dbdd':[],
			'oai_endpoints':[],
			'rits':[],
			'transformations':[],
			'validations':[]
		}

		# loop through export Django model instance exports
		export_count = 0
		for export_type in self.export_manifest['exports'].keys():

			# loop through exports for type
			for e in self.export_dict[export_type]:

				logger.debug('writing %s to export_manifest' % e)

				# bump counter
				export_count += 1

				# write
				self.export_manifest['exports'][export_type].append({
					'name':e.name,
					'id':e.id
				})

		# write count to exports
		self.export_manifest['exports']['count'] = export_count


	def import_state(self,
		export_path,
		import_name=None,
		load_only=False,
		import_records=True,
		stateio_id=None):

		'''
		Import exported state

		Args:
			export_path (str): location on disk of unzipped export directory
			import_name (str): Human name for import task
			load_only (bool): If True, will only parse export but will not import anything
			import_records (bool): If True, will import Mongo and ElasticSearch records
			stateio_id (int): ID of pre-existing StateIO object

		Returns:


		'''

		#debug
		import_stime = time.time()

		# mint new import id
		self.import_id = uuid.uuid4().hex

		# init import_manifest
		self.import_manifest = {
			'combine_version':getattr(settings,'COMBINE_VERSION', None),
			'import_id':self.import_id,
			'import_name':import_name,
			'export_path':export_path,
			'pk_hash':{
				'jobs':{},
				'record_groups':{},
				'orgs':{},
				'transformations':{},
				'validations':{},
				'oai_endpoints':{},
				'rits':{},
				'field_mapper_configs':{},
				'dbdd':{}
			}
		}

		# init/update associated StateIO instance
		update_dict = {
			'stateio_type':'import',
			'name':self.import_manifest['import_name'],
			'import_id':self.import_manifest['import_id'],
			'export_path':self.import_manifest['export_path'],
			'import_manifest':{ k:v for k,v in self.import_manifest.items() if k not in ['pk_hash', 'export_manifest'] },
			'status':'running'
		}
		if stateio_id == None:
			logger.debug('initializing StateIO object')
			self.stateio = StateIO(**update_dict)
		else:
			logger.debug('retrieving and updating StateIO object')
			self.stateio = StateIO.objects.get(id=stateio_id)
			self.stateio.update(**update_dict)
			self.stateio.reload()
		# save
		self.stateio.save()

		# load state, deserializing and export_manifest
		self._load_state(export_path)

		# if not load only, continue
		if not load_only:

			# load configuration dependencies
			self._import_config_instances()

			# if Jobs present
			if len(self.export_manifest['jobs']) > 0:

				# load structural hierarchy
				self._import_hierarchy()

				# load model instances
				self._import_job_related_instances()

				if import_records:
					# load Mongo and ES DB records
					self._import_db_records()

			# update import_manifest
			self._finalize_import_manifest()

		# update associated StateIO instance
		self.stateio.update(**{
			'export_path':self.export_path,
			'export_manifest':self.export_manifest,
			'import_manifest':{ k:v for k,v in self.import_manifest.items() if k not in ['pk_hash', 'export_manifest'] },
			'import_path':self.import_path,
			'status':'finished',
			'finished':True
		})
		self.stateio.reload()
		self.stateio.save()

		logger.debug('state %s imported in %ss' % (self.import_id, (time.time()-import_stime)))


	def _load_state(self, export_path):

		'''
		Method to load state data in preparation for import

		Args:
			export_path (str): location on disk of unzipped export directory

		Returns:
			None
		'''

		# set export path
		self.export_path = export_path

		# unpack if compressed, move to imports
		self._prepare_files()

		# parse export_manifest
		with open('%s/export_manifest.json' % self.import_path, 'r') as f:
			self.export_manifest = json.loads(f.read())
			self.import_manifest['export_manifest'] = self.export_manifest

		# deserialize django objects
		self.deser_django_objects = []
		with open('%s/django_objects.json' % self.import_path, 'r') as f:
			django_objects_json = f.read()
		for obj in serializers.deserialize('json', django_objects_json):
			self.deser_django_objects.append(obj)


	def _prepare_files(self):

		'''
		Method to handle unpacking of exported state, and prepare self.import_path
		'''

		# create import dir based on self.import_id
		self.import_path = '%s/%s' % (settings.STATEIO_IMPORT_DIR, self.import_id)
		self.import_manifest['import_path'] = self.import_path

		# handle URL
		if self.export_path.startswith('http'):
			logger.debug('exported state determined to be URL, downloading and processing')
			raise Exception('not yet handling remote URL locations for importing states')

		# handle archives
		elif os.path.isfile(self.export_path) and self.export_path.endswith(('.zip', '.tar', '.tar.gz')):
			logger.debug('exported state determined to be archive, decompressing')
			shutil.unpack_archive(self.export_path, self.import_path)

		# handle dir
		elif os.path.isdir(self.export_path):
			logger.debug('exported state is directory, copying to import directory')
			os.system('cp -r %s %s' % (self.export_path, self.import_path))

		# else, raise Exception
		else:
			raise Exception('cannot handle export_path %s, aborting' % self.export_path)

		# next, look for export_manifest.json, indicating base directory
		import_base_dir = False
		for root, dirs, files in os.walk(self.import_path):
			if 'export_manifest.json' in files:
				import_base_dir = root
		# if not found, raise Exception
		if not import_base_dir:
			raise Exception('could not find export_manfiest.json, aborting')

		# if import_base_dir != self.import_path, move everything to self.import_path
		if import_base_dir != self.import_path:

			# mv everything to import dir
			os.system('mv %s/* %s' % (import_base_dir, self.import_path))

			# remove now empty base_dir
			shutil.rmtree(import_base_dir)

		logger.debug('confirmed import path at %s' % self.import_path)


	def _import_config_instances(self):

		'''
		Method to import configurations and scenarios instances before all else
		'''

		#################################
		# TRANSFORMATION SCENARIOS
		#################################
		# loop through and create
		for ts in self._get_django_model_type(Transformation):
			logger.debug('rehydrating %s' % ts)

			# check for identical name and payload
			ts_match = Transformation.objects.filter(name=ts.object.name, payload=ts.object.payload).order_by('id')

			# matching scenario found
			if ts_match.count() > 0:
				logger.debug('found identical Transformation, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['transformations'][ts.object.id] = ts_match.first().id

			# not found, creating
			else:
				logger.debug('Transformation not found, creating')
				ts_orig_id = ts.object.id
				ts.object.id = None
				ts.save()
				self.import_manifest['pk_hash']['transformations'][ts_orig_id] = ts.object.id


		#################################
		# VALIDATION SCENARIOS
		#################################
		# loop through and create
		for vs in self._get_django_model_type(ValidationScenario):
			logger.debug('rehydrating %s' % vs)

			# check for identical name and payload
			vs_match = ValidationScenario.objects.filter(name=vs.object.name, payload=vs.object.payload).order_by('id')

			# matching scenario found
			if vs_match.count() > 0:
				logger.debug('found identical ValidationScenario, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['validations'][vs.object.id] = vs_match.first().id

			# not found, creating
			else:
				logger.debug('ValidationScenario not found, creating')
				vs_orig_id = vs.object.id
				vs.object.id = None
				vs.save()
				self.import_manifest['pk_hash']['validations'][vs_orig_id] = vs.object.id


		#################################
		# OAI ENDPOINTS
		#################################

		# loop through and create
		for oai in self._get_django_model_type(OAIEndpoint):
			logger.debug('rehydrating %s' % oai)

			# check for identical name and payload
			oai_match = OAIEndpoint.objects.filter(name=oai.object.name, endpoint=oai.object.endpoint).order_by('id')

			# matching scenario found
			if oai_match.count() > 0:
				logger.debug('found identical OAIEndpoint, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['oai_endpoints'][oai.object.id] = oai_match.first().id

			# not found, creating
			else:
				logger.debug('OAIEndpoint not found, creating')
				oai_orig_id = oai.object.id
				oai.object.id = None
				oai.save()
				self.import_manifest['pk_hash']['oai_endpoints'][oai_orig_id] = oai.object.id


		#################################
		# FIELD MAPPER CONFIGS
		#################################

		# loop through and create
		for scenario in self._get_django_model_type(FieldMapper):
			logger.debug('rehydrating %s' % scenario)

			# check for identical name and payload
			scenario_match = FieldMapper.objects.filter(
				name=scenario.object.name,
				payload=scenario.object.payload,
				config_json=scenario.object.config_json,
				field_mapper_type=scenario.object.field_mapper_type
			).order_by('id')

			# matching scenario found
			if scenario_match.count() > 0:
				logger.debug('found identical FieldMapper, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['field_mapper_configs'][scenario.object.id] = scenario_match.first().id

			# not found, creating
			else:
				logger.debug('FieldMapper not found, creating')
				orig_id = scenario.object.id
				scenario.object.id = None
				scenario.save()
				self.import_manifest['pk_hash']['field_mapper_configs'][orig_id] = scenario.object.id


		#################################
		# RITS
		#################################

		# loop through and create
		for scenario in self._get_django_model_type(RecordIdentifierTransformationScenario):
			logger.debug('rehydrating %s' % scenario)

			# check for identical name and payload
			scenario_match = RecordIdentifierTransformationScenario.objects.filter(
				name=scenario.object.name,
				transformation_type=scenario.object.transformation_type,
				transformation_target=scenario.object.transformation_target,
				regex_match_payload=scenario.object.regex_match_payload,
				regex_replace_payload=scenario.object.regex_replace_payload,
				python_payload=scenario.object.python_payload,
				xpath_payload=scenario.object.xpath_payload,
			).order_by('id')

			# matching scenario found
			if scenario_match.count() > 0:
				logger.debug('found identical RITS, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['rits'][scenario.object.id] = scenario_match.first().id

			# not found, creating
			else:
				logger.debug('RITS not found, creating')
				orig_id = scenario.object.id
				scenario.object.id = None
				scenario.save()
				self.import_manifest['pk_hash']['rits'][orig_id] = scenario.object.id


		###########################################
		# DBDD
		###########################################

		'''
		Requires import to ElasticSearch
			- but because not modifying, can use elasticdump to import
		'''

		# loop through and create
		for scenario in self._get_django_model_type(DPLABulkDataDownload):
			logger.debug('rehydrating %s' % scenario)

			# check for identical name and payload
			scenario_match = DPLABulkDataDownload.objects.filter(
				s3_key=scenario.object.s3_key,
				es_index=scenario.object.es_index,
			).order_by('id')

			# matching scenario found
			if scenario_match.count() > 0:
				logger.debug('found identical DPLA Bulk Data Download, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['dbdd'][scenario.object.id] = scenario_match.first().id

			# not found, creating
			else:
				logger.debug('DPLA Bulk Data Download not found, creating')
				orig_id = scenario.object.id
				scenario.object.id = None
				# drop filepath
				scenario.object.filepath = None
				scenario.save()
				self.import_manifest['pk_hash']['dbdd'][orig_id] = scenario.object.id

				# re-hydrating es index

				# get dbdd
				dbdd = DPLABulkDataDownload.objects.get(pk=(scenario.object.id))

				# import DBDD index to ElaticSearch
				dbdd_export_path = '%s/dbdd/dbdd%s.json' % (self.import_path, orig_id)

				# build command list
				cmd = [
					"elasticdump",
					"--input=%(dbdd_export_path)s" % {'dbdd_export_path':dbdd_export_path},
					"--output=http://localhost:9200/%s" % dbdd.es_index,
					"--ignore-errors",
					"--noRefresh"
				]

				logger.debug("elasticdump cmd: %s" % cmd)

				# run cmd
				os.system(" ".join(cmd))


	def _import_hierarchy(self):

		'''
		Import Organizations and Record Groups

		Note: If same name is found, but duplicates exist, will be aligned with first instance
		sorted by id
		'''

		# loop through deserialized Organizations
		for org in self._get_django_model_type(Organization):
			logger.debug('rehydrating %s' % org)

			# get org_orig_id
			org_orig_id = org.object.id

			# check Org name
			org_match = Organization.objects.filter(name=org.object.name).order_by('id')

			# matching Organization found
			if org_match.count() > 0:
				logger.debug('found same Organization name, skipping creation, adding to hash')
				self.import_manifest['pk_hash']['orgs'][org.object.id] = org_match.first().id

			# not found, creating
			else:
				logger.debug('Organization not found, creating')
				org.object.id = None
				org.save()
				self.import_manifest['pk_hash']['orgs'][org_orig_id] = org.object.id


			# loop through deserialized Record Groups for this Org
			for rg in self._get_django_model_type(RecordGroup):

				if rg.object.organization_id == org_orig_id:

					logger.debug('rehydrating %s' % rg)

					# checking parent org exists, and contains record group with same name
					rg_match = RecordGroup.objects\
						.filter(name=rg.object.name, organization__name=self._get_django_model_instance(org.object.id, Organization).object.name).order_by('id')

					# matching Record Group found
					if rg_match.count() > 0:
						logger.debug('found Organization/Record Group name combination, skipping creation, adding to hash')
						self.import_manifest['pk_hash']['record_groups'][rg.object.id] = rg_match.first().id

					# not found, creating
					else:
						logger.debug('Record Group not found, creating')
						rg_orig_id = rg.object.id
						rg.object.id = None
						# update org id
						org_orig_id = rg.object.organization_id
						rg.object.organization = None
						rg.object.organization_id = self.import_manifest['pk_hash']['orgs'][org_orig_id]
						rg.save()
						self.import_manifest['pk_hash']['record_groups'][rg_orig_id] = rg.object.id


	def _import_job_related_instances(self):

		'''
		Method to import Organization, Record Group, and Job model instances
		'''

		############################
		# DJANGO OBJECTS: JOBS
		############################
		# loop through ORDERED job ids, and rehydrate, capturing new PK in pk_hash
		for job_id in self.export_manifest['jobs']:

			# get deserialized Job
			job = self._get_django_model_instance(job_id, Job)
			logger.debug('rehydrating %s' % job.object.name)

			# run rg id through pk_hash
			rg_orig_id = job.object.record_group_id
			job.object.record_group_id = self.import_manifest['pk_hash']['record_groups'][rg_orig_id]

			# drop pk, save, and note new id
			job_orig_id = job.object.id
			job.object.id = None
			job.save()

			# update pk_hash
			self.import_manifest['pk_hash']['jobs'][job_orig_id] = job.object.id

			# Job, update job_details
			self._update_job_details(job.object)


		#############################
		# DJANGO OBJECTS: JOB INPUTS
		#############################
		# loop through and create
		for ji in self._get_django_model_type(JobInput):
			logger.debug('rehydrating %s' % ji)
			ji.object.id = None
			ji.object.job_id = self.import_manifest['pk_hash']['jobs'][ji.object.job_id]
			ji.object.input_job_id = self.import_manifest['pk_hash']['jobs'][ji.object.input_job_id]
			ji.save()


		#################################
		# DJANGO OBJECTS: JOB VALIDATIONS
		#################################
		# loop through and create
		for jv in self._get_django_model_type(JobValidation):
			logger.debug('rehydrating %s' % jv)

			# update validation_scenario_id
			vs_orig_id = jv.object.validation_scenario_id
			jv.object.validation_scenario_id = self.import_manifest['pk_hash']['validations'][vs_orig_id]

			# update job_id
			jv.object.id = None
			jv.object.job_id = self.import_manifest['pk_hash']['jobs'][jv.object.job_id]
			jv.save()


	def _import_db_records(self):

		'''
		Method to import DB records from:
			- Mongo
			- ElasticSearch
		'''

		# generate spark code
		self.spark_code = 'from jobs import CombineStateIOImport\nCombineStateIOImport(spark, import_path="%(import_path)s", import_manifest=%(import_manifest)s).spark_function()' % {
			'import_path':'file://%s' % self.import_path,
			'import_manifest':self.import_manifest
		}

		# submit to livy and poll
		submit = LivyClient().submit_job(LivySession.get_active_session().session_id, {'code':self.spark_code})
		self.spark_results = polling.poll(lambda: LivyClient().job_status(submit.headers['Location']).json()['state'] == 'available', step=5, poll_forever=True)

		return self.spark_results


	def _finalize_import_manifest(self):

		'''
		Method to finalize import_manifest before writing to export
			- loop through self.export_dict for export types that are human meaningful
		'''

		# establish section in export_manifest
		self.import_manifest['imports'] = {
			'jobs':[],
			'record_groups':[],
			'orgs':[],
			'dbdd':[],
			'oai_endpoints':[],
			'rits':[],
			'transformations':[],
			'validations':[]
		}

		# loop through deserialized objects
		import_count = 0
		for import_type in self.import_manifest['imports'].keys():

			# invert pk_hash for type
			inv_pk_hash = { v:k for k,v in self.import_manifest['pk_hash'][import_type].items() }

			# loop through imports for type
			for obj in self._get_django_model_type(self.model_translation[import_type]):

				# confirm that id has changed, indicating newly created and not mapped from pre-existing
				if obj.object.id in inv_pk_hash.keys() and obj.object.id != inv_pk_hash[obj.object.id]:

					logger.debug('writing %s to import_manifest' % obj)

					# bump count
					import_count += 1

					# write name, and UPDATED id of imported object
					self.import_manifest['imports'][import_type].append({
						'name':obj.object.name,
						'id':obj.object.id
					})

		# calc total number of imports
		self.import_manifest['imports']['count'] = import_count

		# write pk_hash as json string (int keys are not valid in Mongo)
		self.import_manifest['pk_hash_json'] = json.dumps(self.import_manifest['pk_hash'])


	def _update_job_details(self, job):

		'''
		Handle the various ways in which Job.job_details must
		be updated in light of changing model instance PKs.

			- Generic
				- validation_scenarios: list of integers
				- input_job_ids: list of integers
				- input_filters.job_specific: dictionary with string of job_id as key

			- OAI Harvest
				- oai_params

			- Transformation
				- transformation

		Args:
			job (core.models.Job): newly minted Job instance to update

		Returns:
			None
		'''

		logger.debug('updating job_details for %s' % job)

		# pk_hash
		pk_hash = self.import_manifest['pk_hash']

		# update dictionary
		update_dict = {}

		# update validation_scenarios
		if 'validation_scenarios' in job.job_details_dict.keys():
			try:
				validation_scenarios = job.job_details_dict['validation_scenarios']
				validation_scenarios_updated = [ pk_hash['validations'].get(vs_id, vs_id) for vs_id in validation_scenarios ]
				update_dict['validation_scenarios'] = validation_scenarios_updated
			except:
				logger.debug('error with updating job_details: validation_scenarios')

		# update input_job_ids
		if 'input_job_ids' in job.job_details_dict.keys():
			try:
				input_job_ids = job.job_details_dict['input_job_ids']
				input_job_ids_updated = [ pk_hash['jobs'].get(job_id, job_id) for job_id in input_job_ids ]
				update_dict['input_job_ids'] = input_job_ids_updated
			except:
				logger.debug('error with updating job_details: input_job_ids')

		# update input_filters.job_specific
		if 'input_filters' in job.job_details_dict.keys():
			try:
				input_filters = job.job_details_dict['input_filters']
				job_specific_updated = { str(pk_hash['jobs'].get(int(k), int(k))):v for k,v in input_filters['job_specific'].items() }
				input_filters['job_specific'] = job_specific_updated
				update_dict['input_filters'] = input_filters
			except:
				logger.debug('error with updating job_details: input_filters')

		# update rits
		if 'rits' in job.job_details_dict.keys():
			try:
				orig_rits_id = job.job_details_dict['rits']
				update_dict['rits'] = pk_hash['rits'][orig_rits_id]
			except:
				logger.debug('error with updating job_details: rits')

		# update dbdd
		if 'dbdm' in job.job_details_dict.keys():
			try:
				dbdm = job.job_details_dict['dbdm']
				dbdd_orig_id = dbdm['dbdd']
				dbdm['dbdd'] = pk_hash['dbdd'][dbdd_orig_id]
				update_dict['dbdm'] = dbdm
			except:
				logger.debug('error with updating job_details: dbdd')

		# if OAIHarvest, update oai_params
		if job.job_type == 'HarvestOAIJob':
			try:
				logger.debug('HarvestOAIJob encountered, updating job details')
				oai_params = job.job_details_dict['oai_params']
				oai_params['id'] = pk_hash['oai_endpoints'].get(oai_params['id'], oai_params['id'])
				update_dict['oai_params'] = oai_params
			except:
				logger.debug('error with updating job_details: oai_params')

		# if Transform, update transformation
		if job.job_type == 'TransformJob':
			try:
				logger.debug('TransformJob encountered, updating job details')
				transformation = job.job_details_dict['transformation']
				transformation['id'] = pk_hash['transformations'].get(transformation['id'], transformation['id'])
				update_dict['transformation'] = transformation
			except:
				logger.debug('error with updating job_details: transformation')

		# update job details
		job.update_job_details(update_dict)

		# update spark code
		cjob = CombineJob.get_combine_job(job.id)
		cjob.job.spark_code = cjob.prepare_job(return_job_code=True)
		cjob.job.save()


	def _get_django_model_instance(self, instance_id, instance_type, instances=None):

		'''
		Method to retrieve model instance from deserialized "bag",
		by instance type and id.

		Could be more performant, but length of deserialized objects
		makes it relatively negligable.

		Args:
			instance_id (int): model instance PK
			intsance_type (django.models.model): model instance type

		Returns:
			(core.models.model): Model instance based on matching id and type
		'''

		# if instances not provided, assumed self.deser_django_objects
		if instances == None:
			instances = self.deser_django_objects

		# loop through deserialized objects
		for obj in instances:
			if obj.object.id == instance_id and type(obj.object) == instance_type:
				logger.debug('deserialized object found: %s' % obj.object)
				return obj


	def _get_django_model_type(self, instance_type, instances=None):

		'''
		Method to retrieve types from deserialized objects

		Args:
			instance_type (core.models.model): Model type to return

		Return:
			(list): List of model instances that match type
		'''

		# if instances not provided, assumed self.deser_django_objects
		if instances == None:
			instances = self.deser_django_objects

		# return list
		return [ obj for obj in instances if type(obj.object) == instance_type ]


	@staticmethod
	def export_state_bg_task(
		export_name=None,
		jobs=[],
		record_groups=[],
		orgs=[],
		config_scenarios=[]):

		'''
		Method to init export state as bg task
		'''

		# init StateIO
		stateio = StateIO(
			name=export_name,
			stateio_type='export',
			status='initializing')
		stateio.save()

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Export State',
			task_type = 'stateio_export',
			task_params_json = json.dumps({
				'jobs':jobs,
				'record_groups':record_groups,
				'orgs':orgs,
				'config_scenarios':config_scenarios,
				'export_name':stateio.name,
				'stateio_id':str(stateio.id),
			})
		)
		ct.save()
		logger.debug(ct)

		# add ct.id to stateio
		stateio.bg_task_id = ct.id
		stateio.save()

		# run celery task
		bg_task = tasks.stateio_export.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		return ct


	@staticmethod
	def import_state_bg_task(
		import_name=None,
		export_path=None):

		'''
		Method to init state import as bg task
		'''

		# init StateIO
		stateio = StateIO(
			name=import_name,
			stateio_type='import',
			status='initializing')
		stateio.save()

		# initiate Combine BG Task
		ct = CombineBackgroundTask(
			name = 'Import State',
			task_type = 'stateio_import',
			task_params_json = json.dumps({
				'import_name':stateio.name,
				'export_path':export_path,
				'stateio_id':str(stateio.id),
			})
		)
		ct.save()
		logger.debug(ct)

		# add ct.id to stateio
		stateio.bg_task_id = ct.id
		stateio.save()

		# run celery task
		bg_task = tasks.stateio_import.delay(ct.id)
		logger.debug('firing bg task: %s' % bg_task)
		ct.celery_task_id = bg_task.task_id
		ct.save()

		return ct























