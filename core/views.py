# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import datetime
import hashlib
import json
import jsonschema
import logging
from lxml import etree, isoschematron
import os
import pdb
import re
import requests
import textwrap
import time
from types import ModuleType
from urllib.parse import urlencode
import uuid

# django
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core import serializers
from django.core.files.uploadedfile import InMemoryUploadedFile, TemporaryUploadedFile
from django.core.urlresolvers import reverse
from django.db.models import Q
from django.forms.models import model_to_dict
from django.http import HttpResponse, JsonResponse, FileResponse
from django.shortcuts import render, redirect
from django.views import View

# Django Background Tasks
from background_task.models_completed import CompletedTask
from background_task.models import Task

# import models
from core import models, forms
from core.es import es_handle

# import oai server
from core.oai import OAIProvider

# import background tasks
from core import tasks

# django-datatables-view
from django_datatables_view.base_datatable_view import BaseDatatableView

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



# breadcrumb parser
def breadcrumb_parser(request):
	
	'''
	Rudimentary breadcrumbs parser
	'''

	crumbs = []

	# livy/spark
	regex_match = re.match(r'(.+?/livy_sessions)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>Livy/Spark</span>", reverse('livy_sessions')))

	# configurations
	regex_match = re.match(r'(.+?/configuration)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>Configuration</span>", reverse('configuration')))

	# search
	regex_match = re.match(r'(.+?/search)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>Search</span>", reverse('search')))

	# configurations/test_validation_scenario
	regex_match = re.match(r'(.+?/configuration/test_validation_scenario)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>Test Validation Scenario</span>", reverse('test_validation_scenario')))

	# all jobs
	regex_match = re.match(r'(.+?/jobs/all)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>All Jobs</span>", reverse('all_jobs')))

	# analysis
	regex_match = re.match(r'(.+?/analysis)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>Analysis</span>", reverse('analysis')))

	# field analysis
	regex_match = re.match(r'(.+?/analysis/es/index/j([0-9]+)/field_analysis.*)', request.path)
	if regex_match:

		# get job
		j = models.Job.objects.get(pk=int(regex_match.group(2)))

		# get field for analysis
		field_name = request.GET.get('field_name', None)

		# append crumbs
		if j.record_group.organization.for_analysis:
			logger.debug("breadcrumbs: org is for analysis, skipping")
		else:
			crumbs.append(("<span class='font-weight-bold'>Organzation</span> - <code>%s</code>" % j.record_group.organization.name, reverse('organization', kwargs={'org_id':j.record_group.organization.id})))
		if j.record_group.for_analysis:
			logger.debug("breadcrumbs: rg is for analysis, skipping")
		else:
			crumbs.append(("<span class='font-weight-bold'>RecordGroup</span> - <code>%s</code>" % j.record_group.name, reverse('record_group', kwargs={'org_id':j.record_group.organization.id, 'record_group_id':j.record_group.id})))
		crumbs.append(("<span class='font-weight-bold'>Job</span> - <code>%s</code>" % j.name, reverse('job_details', kwargs={'org_id':j.record_group.organization.id, 'record_group_id':j.record_group.id, 'job_id':j.id})))
		crumbs.append(("<span class='font-weight-bold'>Field Analysis - <code>%s</code></span>" % field_name, '%s?%s' % (regex_match.group(1), request.META['QUERY_STRING'])))

	# published
	pub_m = re.match(r'(.+?/published)', request.path)
	if pub_m:
		crumbs.append(("<span class='font-weight-bold'>Published</span>", reverse('published')))

	# organization
	pub_m = re.match(r'(.+?/organization/.*)', request.path)
	if pub_m:
		crumbs.append(("<span class='font-weight-bold'>Organizations</span>", reverse('organizations')))

	# org
	org_m = re.match(r'(.+?/organization/([0-9]+))', request.path)
	if org_m:
		org = models.Organization.objects.get(pk=int(org_m.group(2)))
		if org.for_analysis:
			logger.debug("breadcrumbs: org is for analysis, converting breadcrumbs")
			crumbs.append(("<span class='font-weight-bold'>Analysis</span>", reverse('analysis')))
		else:
			crumbs.append(("<span class='font-weight-bold'>Organzation</span> - <code>%s</code>" % org.name, org_m.group(1)))

	# record_group
	rg_m = re.match(r'(.+?/record_group/([0-9]+))', request.path)
	if rg_m:
		rg = models.RecordGroup.objects.get(pk=int(rg_m.group(2)))
		if rg.for_analysis:
			logger.debug("breadcrumbs: rg is for analysis, converting breadcrumbs")
		else:
			crumbs.append(("<span class='font-weight-bold'>RecordGroup</span> - <code>%s</code>" % rg.name, rg_m.group(1)))

	# job
	j_m = re.match(r'(.+?/job/([0-9]+))', request.path)
	if j_m:
		j = models.Job.objects.get(pk=int(j_m.group(2)))
		if j.record_group.for_analysis:
			crumbs.append(("<span class='font-weight-bold'>Analysis</span> - %s" % j.name, j_m.group(1)))
		else:
			crumbs.append(("<span class='font-weight-bold'>Job</span> - <code>%s</code>" % j.name, j_m.group(1)))

	# record
	r_m = re.match(r'(.+?/record/([0-9]+))', request.path)
	if r_m:
		r = models.Record.objects.get(pk=int(r_m.group(2)))
		crumbs.append(("<span class='font-weight-bold'>Record</span> - <code>%s</code>" % r.record_id, r_m.group(1)))

	# background tasks
	regex_match = re.match(r'(.+?/background_tasks)', request.path)
	if regex_match:
		crumbs.append(("<span class='font-weight-bold'>Background Tasks</span>", reverse('bg_tasks')))

	# background task
	regex_match = re.match(r'(.+?/background_tasks/task/([0-9]+))', request.path)
	if regex_match:
		bg_task = models.CombineBackgroundTask.objects.get(pk=int(regex_match.group(2)))
		crumbs.append(("<span class='font-weight-bold'>Task - <code>%s</code></span>" % (bg_task.name), reverse('bg_tasks')))

	# return
	return crumbs



####################################################################
# Index 														   #
####################################################################

@login_required
def index(request):

	# get username
	username = request.user.username

	# get all organizations
	orgs = models.Organization.objects.exclude(for_analysis=True).all()

	# get record count
	record_count = models.Record.objects.all().count()

	# get published records count
	pr = models.PublishedRecords()
	published_record_count = pr.records.count()

	# get job count
	job_count = models.Job.objects.all().count()

	return render(request, 'core/index.html', {
		'username':username,
		'orgs':orgs,
		'record_count':"{:,}".format(record_count),
		'published_record_count':"{:,}".format(published_record_count),
		'job_count':"{:,}".format(job_count)
		})



####################################################################
# User Livy Sessions 											   #
####################################################################

@login_required
def livy_sessions(request):
	
	# single Livy session
	logger.debug("checking or active Livy session")
	livy_session = models.LivySession.get_active_session()

	# if session found, refresh
	if livy_session:
		livy_session.refresh_from_livy()

	# return
	return render(request, 'core/livy_sessions.html', {
		'livy_session':livy_session,
		'breadcrumbs':breadcrumb_parser(request)
	})


@login_required
def livy_session_start(request):
	
	logger.debug('Checking for pre-existing livy sessions')

	# get "active" livy sessions
	livy_sessions = models.LivySession.objects.filter(status__in=['starting','running','idle'])
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

	# redirect
	return redirect('livy_sessions')


@login_required
def livy_session_stop(request, session_id):
	
	logger.debug('stopping Livy session by Combine ID: %s' % session_id)

	livy_session = models.LivySession.objects.filter(id=session_id).first()
	
	# attempt to stop with Livy
	models.LivyClient.stop_session(livy_session.session_id)

	# remove from DB
	livy_session.delete()

	# redirect
	return redirect('livy_sessions')



####################################################################
# Organizations 												   #
####################################################################

def organizations(request):

	'''
	View all Organizations
	'''
	
	# show organizations
	if request.method == 'GET':

		logger.debug('retrieving organizations')
		
		# get all organizations
		orgs = models.Organization.objects.exclude(for_analysis=True).all()

		# render page
		return render(request, 'core/organizations.html', {
				'orgs':orgs,
				'breadcrumbs':breadcrumb_parser(request)
			})


	# create new organization
	if request.method == 'POST':

		# create new org
		logger.debug(request.POST)
		f = forms.OrganizationForm(request.POST)
		new_org = f.save()

		return redirect('organization', org_id=new_org.id)


def organization(request, org_id):

	'''
	Details for Organization
	'''

	# get organization
	org = models.Organization.objects.get(pk=org_id)

	# get record groups for this organization
	record_groups = models.RecordGroup.objects.filter(organization=org).exclude(for_analysis=True)

	# render page
	return render(request, 'core/organization.html', {
			'org':org,
			'record_groups':record_groups,
			'breadcrumbs':breadcrumb_parser(request)
		})


def organization_delete(request, org_id):

	'''
	Delete Organization
	Note: Through cascade deletes, would remove:
		- RecordGroup
			- Job
				- Record
	'''

	# get organization
	org = models.Organization.objects.get(pk=org_id)

	# set job status to deleting
	org.name = "%s (DELETING)" % org.name
	org.save()
	
	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Delete Organization: %s' % org.name,
		task_type = 'job_delete',
		task_params_json = json.dumps({
			'model':'Job',
			'job_id':org.id
		})
	)
	ct.save()
	bg_task = tasks.delete_model_instance(
		'Organization',
		org.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	return redirect('organizations')



####################################################################
# Record Groups 												   #
####################################################################

def record_group_new(request, org_id):

	'''
	Create new Record Group
	'''

	# create new organization
	if request.method == 'POST':

		# create new record group
		logger.debug(request.POST)
		f = forms.RecordGroupForm(request.POST)
		new_rg = f.save()

		# redirect to organization page
		return redirect('record_group', org_id=org_id, record_group_id=new_rg.id)



def record_group_delete(request, org_id, record_group_id):

	'''
	Create new Record Group
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.get(pk=record_group_id)

	# set job status to deleting
	record_group.name = "%s (DELETING)" % record_group.name
	record_group.save()
	
	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Delete RecordGroup: %s' % record_group.name,
		task_type = 'job_delete',
		task_params_json = json.dumps({
			'model':'Job',
			'job_id':record_group.id
		})
	)
	ct.save()
	bg_task = tasks.delete_model_instance(
		'RecordGroup',
		record_group.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	# redirect to organization page
	return redirect('organization', org_id=org_id)



def record_group(request, org_id, record_group_id):

	'''
	View information about a single record group, including any and all jobs run

	Args:
		record_group_id (str/int): PK for RecordGroup table
	'''
	
	logger.debug('retrieving record group ID: %s' % record_group_id)

	# retrieve record group
	record_group = models.RecordGroup.objects.filter(id=record_group_id).first()

	# get all jobs associated with record group
	jobs = models.Job.objects.filter(record_group=record_group_id)

	# get record group job lineage
	job_lineage = record_group.get_jobs_lineage()

	# get all currently applied publish set ids
	publish_set_ids = models.PublishedRecords.get_publish_set_ids()

	# loop through jobs
	for job in jobs:

		# update status
		job.update_status()

	# get all record groups for this organization
	record_groups = models.RecordGroup.objects.filter(organization=org_id).exclude(id=record_group_id).exclude(for_analysis=True)

	# render page
	return render(request, 'core/record_group.html', {
			'record_group':record_group,
			'jobs':jobs,
			'job_lineage_json':json.dumps(job_lineage),
			'publish_set_ids':publish_set_ids,
			'record_groups':record_groups,
			'breadcrumbs':breadcrumb_parser(request)
		})


def record_group_update_publish_set_id(request, org_id, record_group_id):

	if request.method == 'POST':

		# get record group
		record_group = models.RecordGroup.objects.get(pk=int(record_group_id))

		logger.debug(request.POST)

		# update RecordGroup publish set id
		if request.POST.get('new_publish_set_id') != '':
			record_group.publish_set_id = request.POST.get('new_publish_set_id')
			record_group.save()
		elif request.POST.get('existing_publish_set_id') != '':
			record_group.publish_set_id = request.POST.get('existing_publish_set_id')
			record_group.save()
		else:
			logger.debug('publish_set_id not set, skipping')

		# redirect to
		# return redirect('record_group', org_id=org_id, record_group_id=record_group.id)
		return redirect(request.META.get('HTTP_REFERER'))



####################################################################
# Jobs 															   #
####################################################################

@login_required
def all_jobs(request):

	# get all the record groups.
	record_groups = models.RecordGroup.objects.exclude(for_analysis=True)

	'''
	View to show all jobs, across all Organizations, RecordGroups, and Job types

	GET Args:
		include_analysis: if true, include Analysis type jobs
	'''

	# capture include_analysis GET param if present
	include_analysis = request.GET.get('include_analysis', False)
	
	# get all jobs associated with record group
	if include_analysis:
		jobs = models.Job.objects.all()
	else:
		jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

	# get job lineage for all jobs
	if include_analysis:
		ld = models.Job.get_all_jobs_lineage(directionality='downstream', exclude_analysis_jobs=False)
	else:
		ld = models.Job.get_all_jobs_lineage(directionality='downstream', exclude_analysis_jobs=True)

	# loop through jobs and update status
	for job in jobs:
		job.update_status()

	# render page
	return render(request, 'core/all_jobs.html', {
			'jobs':jobs,
			'record_groups':record_groups,
			'job_lineage_json':json.dumps(ld),
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def job_delete(request, org_id, record_group_id, job_id):

	logger.debug('deleting job by id: %s' % job_id)

	# get job
	job = models.Job.objects.get(pk=job_id)

	# set job status to deleting
	job.name = "%s (DELETING)" % job.name
	job.deleted = True
	job.status = 'deleting'
	job.save()
	
	# remove via background tasks
	# bg_task = tasks.delete_model_instance('Job', job.id)
	# logger.debug('job scheduled for delete as background task: %s' % bg_task.task_hash)

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Delete Job: %s' % job.name,
		task_type = 'job_delete',
		task_params_json = json.dumps({
			'model':'Job',
			'job_id':job.id
		})
	)
	ct.save()
	bg_task = tasks.delete_model_instance(
		'Job',
		job.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	# redirect
	return redirect(request.META.get('HTTP_REFERER'))


@login_required
def delete_jobs(request):

	logger.debug('deleting jobs')
	
	job_ids = request.POST.getlist('job_ids[]')
	logger.debug(job_ids)

	# loop through job_ids
	for job_id in job_ids:

		logger.debug('deleting job by ids: %s' % job_id)
		
		# get job
		job = models.Job.objects.get(pk=int(job_id))

		# set job status to deleting
		job.name = "%s (DELETING)" % job.name
		job.deleted = True
		job.status = 'deleting'
		job.save()

		# initiate Combine BG Task
		ct = models.CombineBackgroundTask(
			name = 'Delete Job: #%s' % job.name,
			task_type = 'job_delete',
			task_params_json = json.dumps({
				'model':'Job',
				'job_id':job.id
			})
		)
		ct.save()
		bg_task = tasks.delete_model_instance(
			'Job',
			job.id,
			verbose_name=ct.verbose_name,
			creator=ct
		)

	# return
	return JsonResponse({'results':True})


@login_required
def move_jobs(request):

	logger.debug('moving jobs')

	stime = time.time()

	job_ids = request.POST.getlist('job_ids[]')
	record_group_id = request.POST.getlist('record_group_id')[0]

	# loop through job_ids
	for job_id in job_ids:

		logger.debug('moving job by ids: %s' % job_id)
		
		cjob = models.CombineJob.get_combine_job(job_id)
		new_record_group = models.RecordGroup.objects.get(pk=record_group_id)
		cjob.job.record_group = new_record_group
		cjob.job.save()

		logger.debug('job has been moved ids: %s' % job_id)

	# redirect
	return JsonResponse({'results':True})


@login_required
def job_details(request, org_id, record_group_id, job_id):
	
	logger.debug('details for job id: %s' % job_id)

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# detailed record count
	record_count_details = cjob.get_detailed_job_record_count()

	# field analysis
	field_counts = cjob.count_indexed_fields()

	# get job lineage
	job_lineage = cjob.job.get_lineage()

	# get dpla_bulk_data_match
	dpla_bulk_data_matches = cjob.job.get_dpla_bulk_data_matches()

	# check if limiting to one, pre-existing record
	q = request.GET.get('q', None)

	# retrieve field mapper config json used
	if type(cjob) != models.PublishJob:
		try:
			job_details = json.loads(cjob.job.job_details)
			job_fm_config_json = job_details['fm_config_json']
		except:
			job_fm_config_json = json.dumps({'error':'job field mapping configuration json could not be found'})
	else:
		job_fm_config_json = json.dumps({'info':'PublishJob: mapped fields were copied from input Job'})

	# return
	return render(request, 'core/job_details.html', {
			'cjob':cjob,
			'record_count_details':record_count_details,
			'field_counts':field_counts,
			'job_lineage_json':json.dumps(job_lineage),
			'dpla_bulk_data_matches':dpla_bulk_data_matches,
			'q':q,
			'job_fm_config_json':job_fm_config_json,
			'es_index':cjob.esi.es_index,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def job_errors(request, org_id, record_group_id, job_id):
	
	logger.debug('retrieving errors for job id: %s' % job_id)

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	job_errors = cjob.get_job_errors()
	
	# return
	return render(request, 'core/job_errors.html', {
			'cjob':cjob,
			'job_errors':job_errors,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def job_update_note(request, org_id, record_group_id, job_id):
	
	if request.method == 'POST':

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# update job note
		cjob.job.note = job_note
		cjob.job.save()

		# redirect
		return redirect(request.META.get('HTTP_REFERER'))


@login_required
def job_update_name(request, org_id, record_group_id, job_id):
	
	if request.method == 'POST':

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# get job note
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# update job note
		cjob.job.name = job_name
		cjob.job.save()

		# redirect
		return redirect(request.META.get('HTTP_REFERER'))


@login_required
def job_dpla_field_map(request, org_id, record_group_id, job_id):
	
	if request.method == 'POST':

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# get DPLAJobMap
		djm = cjob.job.dpla_mapping

		# get fields
		dpla_field = request.POST.get('dpla_field')
		es_field = request.POST.get('es_field')

		# if dpla none, get current dpla field for this es field, then set to None
		if dpla_field == '':
			if es_field in djm.inverted_mapped_fields().keys():
				current_dpla_field = djm.inverted_mapped_fields()[es_field]
				logger.debug('unsetting %s' % current_dpla_field)
				dpla_field = current_dpla_field
				es_field = None
		
		# update DPLAJobMap and redirect
		setattr(djm, dpla_field, es_field)
		djm.save()
		return redirect(request.META.get('HTTP_REFERER'))


@login_required
def job_harvest_oai(request, org_id, record_group_id):

	'''
	Create a new OAI Harvest Job
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.filter(id=record_group_id).first()
	
	# if GET, prepare form
	if request.method == 'GET':
		
		# retrieve all OAI endoints
		oai_endpoints = models.OAIEndpoint.objects.all()

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# get record identifier transformation scenarios
		rits = models.RecordIdentifierTransformationScenario.objects.all()

		# get field mappers
		field_mappers = models.FieldMapper.objects.all()
		
		# get all bulk downloads
		bulk_downloads = models.DPLABulkDataDownload.objects.all()

		# render page
		return render(request, 'core/job_harvest_oai.html', {
				'record_group':record_group,
				'oai_endpoints':oai_endpoints,
				'validation_scenarios':validation_scenarios,
				'rits':rits,
				'field_mappers':field_mappers,
				'xml2kvp_handle':models.XML2kvp(),
				'bulk_downloads':bulk_downloads,
				'breadcrumbs':breadcrumb_parser(request)
			})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('beginning oai harvest for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# retrieve OAIEndpoint
		oai_endpoint = models.OAIEndpoint.objects.get(pk=int(request.POST['oai_endpoint_id']))

		# add overrides if set
		overrides = { override:request.POST[override]
			for override in ['verb','metadataPrefix','scope_type','scope_value'] if request.POST[override] != '' }
		logger.debug(overrides)

		# get field mapper configurations
		field_mapper = request.POST.get('field_mapper')
		fm_config_json = request.POST.get('fm_config_json')

		# get requested validation scenarios
		validation_scenarios = request.POST.getlist('validation_scenario', [])

		# handle requested record_id transform
		rits = request.POST.get('rits', None)
		if rits == '':
			rits = None

		# handle requested record_id transform
		dbdd = request.POST.get('dbdd', None)
		if dbdd == '':
			dbdd = None

		# initiate job
		cjob = models.HarvestOAIJob(
			job_name=job_name,
			job_note=job_note,
			user=request.user,
			record_group=record_group,
			oai_endpoint=oai_endpoint,
			overrides=overrides,
			field_mapper=field_mapper,
			fm_config_json=fm_config_json,
			validation_scenarios=validation_scenarios,
			rits=rits,
			dbdd=dbdd
		)
		
		# start job and update status
		job_status = cjob.start_job()

		# if job_status is absent, report job status as failed
		if job_status == False:
			cjob.job.status = 'failed'
			cjob.job.save()

		return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_harvest_static_xml(request, org_id, record_group_id, hash_payload_filename=False):

	'''
	Create a new static XML Harvest Job
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.filter(id=record_group_id).first()
	
	# get validation scenarios
	validation_scenarios = models.ValidationScenario.objects.all()

	# get field mappers
	field_mappers = models.FieldMapper.objects.all()

	# get record identifier transformation scenarios
	rits = models.RecordIdentifierTransformationScenario.objects.all()

	# get all bulk downloads
	bulk_downloads = models.DPLABulkDataDownload.objects.all()
	
	# if GET, prepare form
	if request.method == 'GET':
		
		# render page
		return render(request, 'core/job_harvest_static_xml.html', {
				'record_group':record_group,
				'validation_scenarios':validation_scenarios,
				'rits':rits,
				'field_mappers':field_mappers,
				'xml2kvp_handle':models.XML2kvp(),
				'bulk_downloads':bulk_downloads,
				'breadcrumbs':breadcrumb_parser(request)
			})


	# if POST, submit job
	if request.method == 'POST':

		logger.debug('beginning static xml harvest for Record Group: %s' % record_group.name)

		'''
		When determining between user supplied file, and location on disk, favor location
		'''
		# establish payload dictionary
		payload_dict = {}

		# use location on disk
		# When a location on disk is provided, set payload_dir as the location provided
		if request.POST.get('static_filepath') != '':
			payload_dict['type'] = 'location'
			payload_dict['payload_dir'] = request.POST.get('static_filepath')

		# use upload
		# When a payload is uploaded, create payload_dir and set
		else:
			payload_dict['type'] = 'upload'

			# get static file payload
			payload_file = request.FILES['static_payload']

			# grab content type
			payload_dict['content_type'] = payload_file.content_type

			# create payload dir
			payload_dict['payload_dir'] = '/tmp/combine/%s' % str(uuid.uuid4())
			os.makedirs(payload_dict['payload_dir'])

			# establish payload filename
			if hash_payload_filename:
				payload_dict['payload_filename'] = hashlib.md5(payload_file.name.encode('utf-8')).hexdigest()
			else:
				payload_dict['payload_filename'] = payload_file.name
			
			with open(os.path.join(payload_dict['payload_dir'], payload_dict['payload_filename']), 'wb') as f:
				f.write(payload_file.read())
				payload_file.close()

		# include other information for finding, parsing, and preparing identifiers
		payload_dict['xpath_document_root'] = request.POST.get('xpath_document_root', None)
		payload_dict['document_element_root'] = request.POST.get('document_element_root', None)
		payload_dict['additional_namespace_decs'] = request.POST.get('additional_namespace_decs', None).replace("'",'"')
		payload_dict['xpath_record_id'] = request.POST.get('xpath_record_id', None)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# get field mapper configurations
		field_mapper = request.POST.get('field_mapper')
		fm_config_json = request.POST.get('fm_config_json')

		# get requested validation scenarios
		validation_scenarios = request.POST.getlist('validation_scenario', [])

		# handle requested record_id transform
		rits = request.POST.get('rits', None)
		if rits == '':
			rits = None

		# handle requested record_id transform
		dbdd = request.POST.get('dbdd', None)
		if dbdd == '':
			dbdd = None

		# initiate job
		cjob = models.HarvestStaticXMLJob(
			job_name=job_name,
			job_note=job_note,
			user=request.user,
			record_group=record_group,
			field_mapper=field_mapper,
			fm_config_json=fm_config_json,
			payload_dict=payload_dict,
			validation_scenarios=validation_scenarios,
			rits=rits,
			dbdd=dbdd
		)
		
		# start job and update status
		job_status = cjob.start_job()

		# if job_status is absent, report job status as failed
		if job_status == False:
			cjob.job.status = 'failed'
			cjob.job.save()

		return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_transform(request, org_id, record_group_id):

	'''
	Create a new Transform Job
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.filter(id=record_group_id).first()
	
	# if GET, prepare form
	if request.method == 'GET':
		
		# get scope of input jobs and retrieve
		input_job_scope = request.GET.get('scope', None)

		# if all jobs, retrieve all jobs
		if input_job_scope == 'all_jobs':
			input_jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

		# else, limit to RecordGroup
		else:
			input_jobs = record_group.job_set.all()

		# get all transformation scenarios
		transformations = models.Transformation.objects.all()

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# get field mappers
		field_mappers = models.FieldMapper.objects.all()

		# get record identifier transformation scenarios
		rits = models.RecordIdentifierTransformationScenario.objects.all()

		# get job lineage for all jobs (filtered to input jobs scope)
		ld = models.Job.get_all_jobs_lineage(directionality='downstream', jobs_query_set=input_jobs)

		# get all bulk downloads
		bulk_downloads = models.DPLABulkDataDownload.objects.all()

		# render page
		return render(request, 'core/job_transform.html', {
				'job_select_type':'single',
				'record_group':record_group,
				'input_jobs':input_jobs,
				'input_job_scope':input_job_scope,
				'transformations':transformations,
				'validation_scenarios':validation_scenarios,
				'rits':rits,
				'field_mappers':field_mappers,
				'xml2kvp_handle':models.XML2kvp(),
				'job_lineage_json':json.dumps(ld),
				'bulk_downloads':bulk_downloads,
				'breadcrumbs':breadcrumb_parser(request)
			})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('beginning transform for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# retrieve input job
		input_job = models.Job.objects.get(pk=int(request.POST['input_job_id']))
		logger.debug('using job as input: %s' % input_job)

		# retrieve transformation
		transformation = models.Transformation.objects.get(pk=int(request.POST['transformation_id']))
		logger.debug('using transformation: %s' % transformation)

		# get field mapper configurations
		field_mapper = request.POST.get('field_mapper')
		fm_config_json = request.POST.get('fm_config_json')

		# get requested validation scenarios
		validation_scenarios = request.POST.getlist('validation_scenario', [])

		# handle requested record_id transform
		rits = request.POST.get('rits', None)
		if rits == '':
			rits = None

		# capture input record validity valve
		# input_validity_valve = request.POST.get('input_validity_valve', None)

		# capture input filters
		input_filters = {
			'input_validity_valve':request.POST.get('input_validity_valve', 'all')
		}
		input_numerical_valve = request.POST.get('input_numerical_valve', None)
		if input_numerical_valve == '':
			input_numerical_valve = None
		else:
			input_numerical_valve = int(input_numerical_valve)
		input_filters['input_numerical_valve'] = input_numerical_valve
		# es query valve
		input_es_query_valve = request.POST.get('input_es_query_valve', None)
		if input_es_query_valve == '':
			input_es_query_valve = None
		input_filters['input_es_query_valve'] = input_es_query_valve

		# handle requested record_id transform
		dbdd = request.POST.get('dbdd', None)
		if dbdd == '':
			dbdd = None

		# initiate job
		cjob = models.TransformJob(
			job_name=job_name,
			job_note=job_note,
			user=request.user,
			record_group=record_group,
			input_job=input_job,
			transformation=transformation,
			field_mapper=field_mapper,
			fm_config_json=fm_config_json,
			validation_scenarios=validation_scenarios,
			rits=rits,
			input_filters=input_filters,
			dbdd=dbdd
		)
		
		# start job and update status
		job_status = cjob.start_job()

		# if job_status is absent, report job status as failed
		if job_status == False:
			cjob.job.status = 'failed'
			cjob.job.save()

		return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_merge(request, org_id, record_group_id):

	'''
	Merge multiple jobs into a single job
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.get(pk=record_group_id)
	
	# if GET, prepare form
	if request.method == 'GET':

		# get scope of input jobs and retrieve
		input_job_scope = request.GET.get('scope', None)

		# if all jobs, retrieve all jobs
		if input_job_scope == 'all_jobs':
			input_jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

		# else, limit to RecordGroup
		else:
			input_jobs = record_group.job_set.all()

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# get record identifier transformation scenarios
		rits = models.RecordIdentifierTransformationScenario.objects.all()

		# get field mappers
		field_mappers = models.FieldMapper.objects.all()

		# get job lineage for all jobs (filtered to input jobs scope)
		ld = models.Job.get_all_jobs_lineage(directionality='downstream', jobs_query_set=input_jobs)

		# get all bulk downloads
		bulk_downloads = models.DPLABulkDataDownload.objects.all()

		# render page
		return render(request, 'core/job_merge.html', {
				'job_select_type':'multiple',
				'record_group':record_group,
				'input_jobs':input_jobs,
				'input_job_scope':input_job_scope,
				'validation_scenarios':validation_scenarios,
				'rits':rits,
				'field_mappers':field_mappers,
				'xml2kvp_handle':models.XML2kvp(),
				'job_lineage_json':json.dumps(ld),
				'bulk_downloads':bulk_downloads,
				'breadcrumbs':breadcrumb_parser(request)
			})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('Merging jobs for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# retrieve jobs to merge
		input_jobs = [ models.Job.objects.get(pk=int(job)) for job in request.POST.getlist('input_job_id') ]
		logger.debug('merging jobs: %s' % input_jobs)

		# get field mapper configurations
		field_mapper = request.POST.get('field_mapper')
		fm_config_json = request.POST.get('fm_config_json')

		# get requested validation scenarios
		validation_scenarios = request.POST.getlist('validation_scenario', [])

		# handle requested record_id transform
		rits = request.POST.get('rits', None)
		if rits == '':
			rits = None

		# capture input filters
		input_filters = {
			'input_validity_valve':request.POST.get('input_validity_valve', 'all')
		}
		# numerical valve
		input_numerical_valve = request.POST.get('input_numerical_valve', None)
		if input_numerical_valve == '':
			input_numerical_valve = None
		else:
			input_numerical_valve = int(input_numerical_valve)
		input_filters['input_numerical_valve'] = input_numerical_valve
		# es query valve
		input_es_query_valve = request.POST.get('input_es_query_valve', None)
		if input_es_query_valve == '':
			input_es_query_valve = None
		input_filters['input_es_query_valve'] = input_es_query_valve

		# handle requested record_id transform
		dbdd = request.POST.get('dbdd', None)
		if dbdd == '':
			dbdd = None

		# initiate job
		cjob = models.MergeJob(
			job_name=job_name,
			job_note=job_note,
			user=request.user,
			record_group=record_group,
			input_jobs=input_jobs,
			field_mapper=field_mapper,
			fm_config_json=fm_config_json,
			validation_scenarios=validation_scenarios,
			rits=rits,
			input_filters=input_filters,
			dbdd=dbdd
		)
		
		# start job and update status
		job_status = cjob.start_job()

		# if job_status is absent, report job status as failed
		if job_status == False:
			cjob.job.status = 'failed'
			cjob.job.save()

		return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


@login_required
def job_publish(request, org_id, record_group_id):

	'''
	Publish a single job for a Record Group
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.get(pk=record_group_id)
	
	# if GET, prepare form
	if request.method == 'GET':
		
		# get scope of input jobs and retrieve
		input_job_scope = request.GET.get('scope', None)

		# if all jobs, retrieve all jobs
		if input_job_scope == 'all_jobs':
			input_jobs = models.Job.objects.exclude(job_type='AnalysisJob').all()

		# else, limit to RecordGroup
		else:
			input_jobs = record_group.job_set.all()

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# get job lineage for all jobs (filtered to input jobs scope)
		ld = models.Job.get_all_jobs_lineage(directionality='downstream', jobs_query_set=input_jobs)

		# get all currently applied publish set ids
		publish_set_ids = models.PublishedRecords.get_publish_set_ids()

		# get all bulk downloads
		bulk_downloads = models.DPLABulkDataDownload.objects.all()

		# render page
		return render(request, 'core/job_publish.html', {
				'job_select_type':'single',
				'record_group':record_group,
				'input_jobs':input_jobs,
				'input_job_scope':input_job_scope,
				'validation_scenarios':validation_scenarios,
				'job_lineage_json':json.dumps(ld),
				'publish_set_ids':publish_set_ids,
				'bulk_downloads':bulk_downloads,
				'breadcrumbs':breadcrumb_parser(request)
			})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('Publishing job for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# retrieve input job
		input_job = models.Job.objects.get(pk=int(request.POST['input_job_id']))
		logger.debug('publishing job: %s' % input_job)

		# update RecordGroup publish set id
		'''
		priority:
			1) new, user input publish_set_id
			2) pre-existing publish_set_id
		'''
		if request.POST.get('new_publish_set_id') != '':
			record_group.publish_set_id = request.POST.get('new_publish_set_id')
			record_group.save()
		elif request.POST.get('existing_publish_set_id') != '':
			record_group.publish_set_id = request.POST.get('existing_publish_set_id')
			record_group.save()
		else:
			logger.debug('publish_set_id not set, skipping')

		# initiate job
		cjob = models.PublishJob(
			job_name=job_name,
			job_note=job_note,
			user=request.user,
			record_group=record_group,
			input_job=input_job
		)
		
		# start job and update status
		job_status = cjob.start_job()

		# if job_status is absent, report job status as failed
		if job_status == False:
			cjob.job.status = 'failed'
			cjob.job.save()

		return redirect('record_group', org_id=org_id, record_group_id=record_group.id)


def job_lineage_json(request, org_id, record_group_id, job_id):

	'''
	Return job lineage as JSON
	'''

	# get job
	job = models.Job.objects.get(pk=int(job_id))

	# get lineage
	job_lineage = job.get_lineage()

	return JsonResponse({
		'job_id_list':[ node['id'] for node in job_lineage['nodes'] ],
		'nodes':job_lineage['nodes'],
		'edges':job_lineage['edges']
		})



####################################################################
# Job Validation Report       									   #
####################################################################

@login_required
def job_reports_create_validation(request, org_id, record_group_id, job_id):

	'''
	Generate job report based on validation results
	'''

	# retrieve job
	cjob = models.CombineJob.get_combine_job(int(job_id))

	# if GET, prepare form
	if request.method == 'GET':

		# field analysis
		field_counts = cjob.count_indexed_fields()

		# render page
		return render(request, 'core/job_reports_create_validation.html', {
				'cjob':cjob,
				'field_counts':field_counts,
				'breadcrumbs':breadcrumb_parser(request)
			})

	# if POST, generate report
	if request.method == 'POST':

		# get job name for Combine Task
		report_name = request.POST.get('report_name')
		if report_name == '':
			report_name = 'j_%s_validation_report' % cjob.job.id
			combine_task_name = "Validation Report: %s" % cjob.job.name
		else:
			combine_task_name = "Validation Report: %s" % report_name

		# handle POST params and save as Combine task params
		task_params = {
			'job_id':cjob.job.id,
			'report_name':report_name,
			'report_format':request.POST.get('report_format'),
			'validation_scenarios':request.POST.getlist('validation_scenario', []),
			'mapped_field_include':request.POST.getlist('mapped_field_include', [])
		}

		# initiate Combine BG Task
		ct = models.CombineBackgroundTask(
			name = combine_task_name,
			task_type = 'validation_report',
			task_params_json = json.dumps(task_params)
		)
		ct.save()

		# run actual background task, passing CombineTask (ct) id (must be JSON serializable),
		# and setting creator and verbose_name params
		bt = tasks.create_validation_report(
			ct.id,
			verbose_name = ct.verbose_name,
			creator = ct
		)

		# redirect to Background Tasks
		return redirect('bg_tasks')
		

@login_required
def job_update(request, org_id, record_group_id, job_id):

	'''
	Update Job in one of several ways:
		- re-map and index
		- run new / different validations
	'''

	# retrieve job
	cjob = models.CombineJob.get_combine_job(int(job_id))
	
	# if GET, prepare form
	if request.method == 'GET':

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# get field mappers
		field_mappers = models.FieldMapper.objects.all()
		orig_fm_config_json = cjob.job.get_fm_config_json()

		# get uptdate type from GET params
		update_type = request.GET.get('update_type', None)

		# render page
		return render(request, 'core/job_update.html', {
				'cjob':cjob,
				'update_type':update_type,
				'validation_scenarios':validation_scenarios,
				'field_mappers':field_mappers,
				'xml2kvp_handle':models.XML2kvp(),
				'orig_fm_config_json':orig_fm_config_json,
				'breadcrumbs':breadcrumb_parser(request)
			})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('updating job')
		logger.debug(request.POST)

		# retrieve job
		cjob = models.CombineJob.get_combine_job(int(job_id))

		# get update type
		update_type = request.POST.get('update_type', None)
		logger.debug('running job update: %s' % update_type)

		# handle re-index
		if update_type == 'reindex':

			# get preferred metadata index mapper
			field_mapper = request.POST.get('field_mapper')
			fm_config_json = request.POST.get('fm_config_json')

			# initiate Combine BG Task
			ct = models.CombineBackgroundTask(
				name = 'Re-Map and Index Job: %s' % cjob.job.name,
				task_type = 'job_reindex',
				task_params_json = json.dumps({
					'job_id':cjob.job.id,
					'field_mapper':field_mapper,
					'fm_config_json':fm_config_json
				})
			)
			ct.save()
			bg_task = tasks.job_reindex(
				ct.id,
				verbose_name=ct.verbose_name,
				creator=ct
			)

			return redirect('bg_tasks')

		# handle new validations
		if update_type == 'validations':

			# get requested validation scenarios
			validation_scenarios = request.POST.getlist('validation_scenario', [])

			# initiate Combine BG Task
			ct = models.CombineBackgroundTask(
				name = 'New Validations for Job: %s' % cjob.job.name,
				task_type = 'job_new_validations',
				task_params_json = json.dumps({
					'job_id':cjob.job.id,
					'validation_scenarios':validation_scenarios
				})
			)
			ct.save()
			bg_task = tasks.job_new_validations(
				ct.id,
				verbose_name=ct.verbose_name,
				creator=ct
			)

			return redirect('bg_tasks')

		# handle validation removal
		if update_type == 'remove_validation':

			# get validation scenario to remove
			jv_id = request.POST.get('jv_id', False)

			# initiate Combine BG Task
			ct = models.CombineBackgroundTask(
				name = 'Remove Validation %s for Job: %s' % (jv_id, cjob.job.name),
				task_type = 'job_remove_validation',
				task_params_json = json.dumps({
					'job_id':cjob.job.id,
					'jv_id':jv_id
				})
			)
			ct.save()
			bg_task = tasks.job_remove_validation(
				ct.id,
				verbose_name=ct.verbose_name,
				creator=ct
			)

			return redirect('bg_tasks')



####################################################################
# Job Validation Report       									   #
####################################################################

def document_download(request):

	'''
	Args (GET params):
		file_location: location on disk for file
		file_download_name: desired download name
		content_type: ContentType Headers
	'''

	# known download format params
	download_format_hash = {
		'excel':{
			'extension':'.xlsx',
			'content_type':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
		},
		'csv':{
			'extension':'.csv',
			'content_type':'text/plain'
		}
	}

	# get params
	download_format = request.GET.get('download_format', None)
	filepath = request.GET.get('filepath', None)
	name = request.GET.get('name', 'download')
	content_type = request.GET.get('content_type', 'text/plain')
	preview = request.GET.get('preview', False)

	# if known download format, use hash and overwrite provided or defaults
	if download_format and download_format in download_format_hash.keys():

		format_params = download_format_hash[download_format]
		name = '%s%s' % (name, format_params['extension'])
		content_type = format_params['content_type']

	# NEW
	response = FileResponse(open(filepath, 'rb'))
	if not preview:
			response['Content-Disposition'] = 'attachment; filename="%s"' % name
	return response



####################################################################
# Jobs QA	                   									   #
####################################################################

@login_required
def field_analysis(request, es_index):

	# get field name
	field_name = request.GET.get('field_name')
	
	# get ESIndex
	esi = models.ESIndex(es_index)

	# get analysis for field
	field_metrics = esi.field_analysis(field_name, metrics_only=True)

	# return
	return render(request, 'core/field_analysis.html', {
			'esi':esi,
			'field_name':field_name,
			'field_metrics':field_metrics,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def job_indexing_failures(request, org_id, record_group_id, job_id):

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# return
	return render(request, 'core/job_indexing_failures.html', {
			'cjob':cjob,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def remove_job_indexing_failures(request, org_id, record_group_id, job_id):

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# remove indexing failures
	to_delete = models.IndexMappingFailure.objects.filter(job=cjob.job)
	delete_results = to_delete._raw_delete(to_delete.db)
	logger.debug(delete_results)

	# return
	return redirect(request.META.get('HTTP_REFERER'))


@login_required
def field_analysis_docs(request, es_index, filter_type):

	'''

	Table of documents that match a filtered ES query.

	Args:
		es_index (str): string ES index name
		filter_type (str): what kind of filtering to impose on documents returned
	'''

	# regardless of filtering type, get field name
	field_name = request.GET.get('field_name')

	# get ESIndex
	esi = models.ESIndex(es_index)

	# begin construction of DT GET params with 'fields_names'
	dt_get_params = [
		('field_names', 'db_id'), # get DB ID
		('field_names', 'combine_id'), # get Combine ID
		('field_names', 'record_id'), # get ID from ES index document
		('field_names', field_name), # add field to returned fields
		('filter_field', field_name),
		('filter_type', filter_type)
	]

	# analysis scenario dict
	analysis_scenario = {
		'exists':None,
		'matches':None,
		'value':None
	}

	# field existence
	if filter_type == 'exists':

		# if check exists, get expected GET params
		exists = request.GET.get('exists')
		dt_get_params.append(('exists', exists))

		# update analysis scenario dict
		analysis_scenario['exists'] = exists

	# field equals
	if filter_type == 'equals':

		# if check equals, get expected GET params
		matches = request.GET.get('matches')
		dt_get_params.append(('matches', matches))

		value = request.GET.get('value', None) # default None if checking non-matches to value
		if value:
			dt_get_params.append(('filter_value', value))

		# update analysis scenario dict
		analysis_scenario['matches'] = matches
		analysis_scenario['value'] = value


	# construct DT Ajax GET parameters string from tuples
	dt_get_params_string = urlencode(dt_get_params)

	# return
	return render(request, 'core/field_analysis_docs.html', {
			'esi':esi,
			'field_name':field_name,
			'filter_type':filter_type,
			'analysis_scenario':analysis_scenario,
			'msg':None,
			'dt_get_params_string':dt_get_params_string,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def job_validation_scenario_failures(request, org_id, record_group_id, job_id, job_validation_id):

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# get job validation instance
	jv = models.JobValidation.objects.get(pk=int(job_validation_id))

	# return
	return render(request, 'core/job_validation_scenario_failures.html', {
			'cjob':cjob,
			'jv':jv,
			'breadcrumbs':breadcrumb_parser(request)
		})


####################################################################
# Records 														   #
####################################################################

def record(request, org_id, record_group_id, job_id, record_id):

	'''
	Single Record page
	'''
	
	# get single record based on Combine record DB id
	record = models.Record.objects.get(pk=int(record_id))

	# build ancestry in both directions
	record_stages = record.get_record_stages()

	# get details depending on job type
	logger.debug('Job type is %s, retrieving details' % record.job.job_type)
	try:
		job_details = json.loads(record.job.job_details)

		# TransformJob
		if record.job.job_type == 'TransformJob':

			# get transformation
			transformation = models.Transformation.objects.get(pk=job_details['transformation']['id'])
			job_details['transformation'] = transformation

			# get isolated input record
			job_details['input_record'] = record.get_record_stages(input_record_only=True)[0]

	except:
		logger.debug('could not load job details')
		job_details = {}

	# attempt to retrieve pre-existing DPLA document
	dpla_api_doc = record.dpla_api_record_match()
	if dpla_api_doc is not None:
		dpla_api_json = json.dumps(dpla_api_doc, indent=4, sort_keys=True)
	else:
		dpla_api_json = None

	# retrieve diffs, if any, from input record
	# request only combined diff at this point
	record_diff_dict = record.get_input_record_diff(output='combined_gen', combined_as_html=True)

	# retrieve field mapper config json used
	try:
		job_fm_config_json = job_details['fm_config_json']
	except:
		job_fm_config_json = json.dumps({'error':'job field mapping configuration json could not be found'})

	# return
	return render(request, 'core/record.html', {
		'record_id':record_id,
		'record':record,
		'record_stages':record_stages,
		'job_details':job_details,
		'dpla_api_doc':dpla_api_doc,
		'dpla_api_json':dpla_api_json,
		'record_diff_dict':record_diff_dict,
		'job_fm_config_json':job_fm_config_json,
		'breadcrumbs':breadcrumb_parser(request)
	})


def record_document(request, org_id, record_group_id, job_id, record_id):

	'''
	View document for record
	'''

	# get record
	record = models.Record.objects.get(pk=int(record_id))

	# return document as XML
	return HttpResponse(record.document, content_type='text/xml')


def record_indexed_document(request, org_id, record_group_id, job_id, record_id):

	'''
	View indexed, ES document for record
	'''

	# get record
	record = models.Record.objects.get(pk=int(record_id))

	# return ES document as JSON
	return JsonResponse(record.get_es_doc())
	


def record_error(request, org_id, record_group_id, job_id, record_id):

	'''
	View document for record
	'''

	# get record
	record = models.Record.objects.get(pk=int(record_id))

	# return document as XML
	return HttpResponse("<pre>%s</pre>" % record.error)


def record_validation_scenario(request, org_id, record_group_id, job_id, record_id, job_validation_id):

	'''
	Re-run validation test for single record

	Returns:
		results of validation
	'''

	# get record
	record = models.Record.objects.get(pk=int(record_id))

	# get validation scenario
	vs = models.ValidationScenario.objects.get(pk=int(job_validation_id))

	# schematron type validation
	if vs.validation_type == 'sch':

		vs_result = vs.validate_record(record)

		# return
		return HttpResponse(vs_result['raw'], content_type='text/xml')

	# python type validation
	if vs.validation_type == 'python':

		vs_result = vs.validate_record(record)

		# return
		return JsonResponse(vs_result['parsed'], safe=False)


def record_combined_diff_html(request, org_id, record_group_id, job_id, record_id):

	'''
	Return combined diff of Record against Input Record
	'''

	# get record
	record = models.Record.objects.get(pk=int(record_id))

	# get side_by_side diff as HTML
	diff_dict = record.get_input_record_diff(output='combined_gen', combined_as_html=True)

	if diff_dict:

		# get combined output as html from output
		html = diff_dict['combined_gen']

		# return document as HTML
		return HttpResponse(html, content_type='text/html')
	
	else:
		return HttpResponse("Record was not altered during Transformation.", content_type='text/html')


def record_side_by_side_diff_html(request, org_id, record_group_id, job_id, record_id):

	'''
	Return side_by_side diff of Record against Input Record
		- uses sxsdiff (https://github.com/timonwong/sxsdiff)
		- if embed == true, strip some uncessary HTML and return
	'''

	# get record
	record = models.Record.objects.get(pk=int(record_id))

	# check for embed flag
	embed = request.GET.get('embed', False)

	# get side_by_side diff as HTML
	diff_dict = record.get_input_record_diff(output='side_by_side_html')

	if diff_dict:

		# get side_by_side html from output
		html = diff_dict['side_by_side_html']

		# if embed flag set, alter CSS
		# these are defaulted in sxsdiff library, currently
		# easier to pinpoint and remove these than fork library and alter
		html = html.replace('<div class="container">', '<div>')
		html = html.replace('padding-left:30px;', '/*padding-left:30px;*/')
		html = html.replace('padding-right:30px;', '/*padding-right:30px;*/')

		# return document as HTML
		return HttpResponse(html, content_type='text/html')
	
	else:
		return HttpResponse("Record was not altered during Transformation.", content_type='text/html')



####################################################################
# Configuration 												   #
####################################################################

@login_required
def configuration(request):

	# get all transformations
	transformations = models.Transformation.objects.all()

	# get all OAI endpoints
	oai_endpoints = models.OAIEndpoint.objects.all()

	# get all validation scenarios
	validation_scenarios = models.ValidationScenario.objects.all()

	# get record identifier transformation scenarios
	rits = models.RecordIdentifierTransformationScenario.objects.all()

	# get all bulk downloads
	bulk_downloads = models.DPLABulkDataDownload.objects.all()

	# get field mappers
	field_mappers = models.FieldMapper.objects.all()

	# return
	return render(request, 'core/configuration.html', {
			'transformations':transformations,
			'oai_endpoints':oai_endpoints,
			'validation_scenarios':validation_scenarios,
			'rits':rits,
			'field_mappers':field_mappers,
			'bulk_downloads':bulk_downloads,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def oai_endpoint_payload(request, oai_endpoint_id):

	'''
	Return JSON of saved OAI endpoint information
	'''

	# retrieve OAIEndpoint
	oai_endpoint = models.OAIEndpoint.objects.get(pk=oai_endpoint_id)

	# pop state
	oai_endpoint.__dict__.pop('_state')

	# return as json
	return JsonResponse(oai_endpoint.__dict__)


def transformation_scenario_payload(request, trans_id):

	'''
	View payload for transformation scenario
	'''

	# get transformation
	transformation = models.Transformation.objects.get(pk=int(trans_id))

	# return transformation as XML
	if transformation.transformation_type == 'xslt':
		return HttpResponse(transformation.payload, content_type='text/xml')

	# return transformation as Python
	if transformation.transformation_type == 'python':
		return HttpResponse(transformation.payload, content_type='text/plain')

	# return transformation as Python
	if transformation.transformation_type == 'openrefine':
		return HttpResponse(transformation.payload, content_type='text/plain')



def test_transformation_scenario(request):

	'''
	View to live test transformation scenarios
	'''

	# If GET, serve transformation test screen
	if request.method == 'GET':

		# get validation scenarios
		transformation_scenarios = models.Transformation.objects.all()

		# check if limiting to one, pre-existing record
		q = request.GET.get('q', None)

		# check for pre-requested transformation scenario
		tsid = request.GET.get('transformation_scenario', None)

		# return
		return render(request, 'core/test_transformation_scenario.html', {
			'q':q,
			'tsid':tsid,
			'transformation_scenarios':transformation_scenarios,
			'breadcrumbs':breadcrumb_parser(request)
		})

	# If POST, provide raw result of validation test
	if request.method == 'POST':

		logger.debug('running test transformation and returning')

		# get response type
		response_type = request.POST.get('response_type', False)

		# get record
		record = models.Record.objects.get(pk=int(request.POST.get('db_id')))

		try:
			
			# init new transformation scenario
			trans = models.Transformation(
				name='temp_trans_%s' % str(uuid.uuid4()),
				payload=request.POST.get('trans_payload'),
				transformation_type=request.POST.get('trans_type')
			)
			trans.save()

			# validate with record
			trans_results = trans.transform_record(record)			

			# delete temporary trans
			trans.delete()

			# if raw transformation results
			if response_type == 'transformed_doc':
				return HttpResponse(trans_results, content_type="text/xml")

			# get diff of original record as combined results
			elif response_type == 'combined_html':

				# get combined diff as HTML
				diff_dict = record.get_record_diff(xml_string=trans_results, output='combined_gen', combined_as_html=True, reverse_direction=True)
				if diff_dict:
					diff_html = diff_dict['combined_gen']

				return HttpResponse(diff_html, content_type="text/xml")

			# get diff of original record as side_by_side
			elif response_type == 'side_by_side_html':

				# get side_by_side diff as HTML
				diff_dict = record.get_record_diff(xml_string=trans_results, output='side_by_side_html', reverse_direction=True)
				if diff_dict:
					diff_html = diff_dict['side_by_side_html']

					# strip some CSS
					diff_html = diff_html.replace('<div class="container">', '<div>')
					diff_html = diff_html.replace('padding-left:30px;', '/*padding-left:30px;*/')
					diff_html = diff_html.replace('padding-right:30px;', '/*padding-right:30px;*/')


				return HttpResponse(diff_html, content_type="text/xml")
			
		except Exception as e:

			logger.debug('test validation scenario was unsucessful, deleting temporary vs')
			trans.delete()

			return HttpResponse(str(e), content_type="text/plain")


def validation_scenario_payload(request, vs_id):

	'''
	View payload for validation scenario
	'''

	# get transformation
	vs = models.ValidationScenario.objects.get(pk=int(vs_id))

	if vs.validation_type == 'sch':
		# return document as XML
		return HttpResponse(vs.payload, content_type='text/xml')

	else:
		return HttpResponse(vs.payload, content_type='text/plain')


def test_validation_scenario(request):

	'''
	View to live test validation scenario
	'''

	# If GET, serve validation test screen
	if request.method == 'GET':

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# check if limiting to one, pre-existing record
		q = request.GET.get('q', None)

		# check for pre-requested transformation scenario
		vsid = request.GET.get('validation_scenario', None)

		# return
		return render(request, 'core/test_validation_scenario.html', {
			'q':q,
			'vsid':vsid,
			'validation_scenarios':validation_scenarios,
			'breadcrumbs':breadcrumb_parser(request)
		})

	# If POST, provide raw result of validation test
	if request.method == 'POST':

		logger.debug('running test validation and returning')
		logger.debug(request.POST)

		# get record
		record = models.Record.objects.get(pk=int(request.POST.get('db_id')))

		try:
			# init new validation scenario
			vs = models.ValidationScenario(
				name='temp_vs_%s' % str(uuid.uuid4()),
				payload=request.POST.get('vs_payload'),
				validation_type=request.POST.get('vs_type'),
				default_run=False
			)
			vs.save()

			# validate with record
			vs_results = vs.validate_record(record)

			# delete vs
			vs.delete()

			if request.POST.get('vs_results_format') == 'raw':
				return HttpResponse(vs_results['raw'], content_type="text/plain")
			elif request.POST.get('vs_results_format') == 'parsed':
				return JsonResponse(vs_results['parsed'])
			else:
				raise Exception('validation results format not recognized')

		except Exception as e:

			logger.debug('test validation scenario was unsucessful, deleting temporary vs')
			vs.delete()

			return HttpResponse(str(e), content_type="text/plain")


def rits_payload(request, rits_id):

	'''
	View payload for record identifier transformation scenario
	'''

	# get transformation
	rt = models.RecordIdentifierTransformationScenario.objects.get(pk=int(rits_id))

	# return as json package
	return JsonResponse(model_to_dict(rt))


def test_rits(request):

	'''
	View to live test record identifier transformation scenarios
	'''

	# If GET, serve validation test screen
	if request.method == 'GET':

		# check if limiting to one, pre-existing record
		q = request.GET.get('q', None)

		# get record identifier transformation scenarios
		rits = models.RecordIdentifierTransformationScenario.objects.all()

		# return
		return render(request, 'core/test_rits.html', {
			'q':q,
			'rits':rits,
			'breadcrumbs':breadcrumb_parser(request)
		})

	# If POST, provide raw result of validation test
	if request.method == 'POST':

		logger.debug('testing record identifier transformation')
		logger.debug(request.POST)

		try:

			# make POST data mutable
			request.POST._mutable = True

			# get record
			if request.POST.get('db_id', False):
				record = models.Record.objects.get(pk=int(request.POST.get('db_id')))
			else:
				return JsonResponse({'results':'Please select a record from the table above!','success':False})

			# determine testing type
			if request.POST['record_id_transform_target'] == 'record_id':
				logger.debug('configuring test for record_id')
				request.POST['test_transform_input'] = record.record_id
			elif request.POST['record_id_transform_target'] == 'document':
				logger.debug('configuring test for record_id')
				request.POST['test_transform_input'] = record.document

			# instantiate rits and return test
			rits = models.RITSClient(request.POST)
			return JsonResponse(rits.test_user_input())

		except Exception as e:
			return JsonResponse({'results':str(e), 'success':False})


def field_mapper_payload(request, fm_id):

	'''
	View payload for field mapper
	'''

	# get transformation
	fm = models.FieldMapper.objects.get(pk=int(fm_id))

	# get type
	doc_type = request.GET.get('type',None)

	if fm.field_mapper_type == 'xml2kvp':

		if not doc_type:
			return HttpResponse(fm.config_json, content_type='application/json')

		elif doc_type and doc_type == 'config':
			return HttpResponse(fm.config_json, content_type='application/json')

		elif doc_type and doc_type == 'payload':
			return HttpResponse(fm.payload, content_type='application/json')


def field_mapper_update(request):

	'''
	Create and save JSON to FieldMapper instance, or update pre-existing
	'''

	logger.debug(request.POST)

	# get update type
	update_type = request.POST.get('update_type')

	# handle new FieldMapper creation
	if update_type == 'new':
		logger.debug('creating new FieldMapper instance')

		fm = models.FieldMapper(
			name=request.POST.get('fm_name'),
			config_json=request.POST.get('fm_config_json'),
			field_mapper_type='xml2kvp'
		)

		# validate fm_config before creating
		try:
		    fm.validate_config_json()
		    fm.save()
		    return JsonResponse({'results':True,'msg':'New Field Mapper configurations were <strong>saved</strong> as: <strong>%s</strong>' % request.POST.get('fm_name')}, status=201)
		except jsonschema.ValidationError as e:
		    return JsonResponse({'results':False,'msg':'Could not <strong>create</strong> <strong>%s</strong>, the following error was had: %s' % (fm.name, str(e))}, status=409)

	# handle update
	if update_type == 'update':
		logger.debug('updating pre-existing FieldMapper instance')

		# get fm instance
		fm = models.FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

		# update and save
		fm.config_json = request.POST.get('fm_config_json')
		
		# validate fm_config before updating
		try:
		    fm.validate_config_json()
		    fm.save()
		    return JsonResponse({'results':True,'msg':'Field Mapper configurations for <strong>%s</strong> were <strong>updated</strong>' % fm.name}, status=200)
		except jsonschema.ValidationError as e:
		    return JsonResponse({'results':False,'msg':'Could not <strong>update</strong> <strong>%s</strong>, the following error was had: %s' % (fm.name, str(e))}, status=409)

	# handle delete
	if update_type == 'delete':
		logger.debug('deleting pre-existing FieldMapper instance')

		# get fm instance
		fm = models.FieldMapper.objects.get(pk=int(request.POST.get('fm_id')))

		# delete
		fm.delete()
		return JsonResponse({'results':True,'msg':'Field Mapper configurations for <strong>%s</strong> were <strong>deleted</strong>' % fm.name}, status=200)


def test_field_mapper(request):

	'''
	View to live test field mapper configurations
	'''
	
	if request.method == 'GET':

		# get field mapper
		field_mappers = models.FieldMapper.objects.all()

		# check if limiting to one, pre-existing record
		q = request.GET.get('q', None)

		# check for pre-requested transformation scenario
		fmid = request.GET.get('fmid', None)

		# return
		return render(request, 'core/test_field_mapper.html', {
			'q':q,
			'fmid':fmid,
			'field_mappers':field_mappers,
			'xml2kvp_handle':models.XML2kvp(),
			'breadcrumbs':breadcrumb_parser(request)
		})

	# If POST, provide mapping of record
	if request.method == 'POST':

		logger.debug('running test field mapping')
		logger.debug(request.POST)

		# get record
		record = models.Record.objects.get(pk=int(request.POST.get('db_id')))

		# get field mapper info
		field_mapper = request.POST.get('field_mapper')
		fm_config_json = request.POST.get('fm_config_json')

		try:
		
			# parse record with XML2kvp
			fm_config = json.loads(fm_config_json)
			kvp_dict = models.XML2kvp.xml_to_kvp(record.document, **fm_config)

			# return as JSON
			return JsonResponse(kvp_dict)

		except Exception as e:

			logger.debug('field mapper was unsucessful')
			return JsonResponse({'error':str(e)})
			

@login_required
def dpla_bulk_data_download(request):

	'''
	View to support the downloading of DPLA bulk data
	'''

	if request.method == 'GET':

		# if S3 credentials set
		if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY and settings.AWS_ACCESS_KEY_ID != None and settings.AWS_SECRET_ACCESS_KEY != None:

			# get DPLABulkDataClient and keys from DPLA bulk download
			dbdc = models.DPLABulkDataClient()
			bulk_data_keys = dbdc.retrieve_keys()

		else:
			bulk_data_keys = False

		# return
		return render(request, 'core/dpla_bulk_data_download.html', {
			'bulk_data_keys':bulk_data_keys,
			'breadcrumbs':breadcrumb_parser(request)
		})

	if request.method == 'POST':

		logger.debug('initiating bulk data download')

		# get DPLABulkDataClient
		dbdc = models.DPLABulkDataClient()

		# initiate download
		dbdc.download_and_index_bulk_data(request.POST.get('object_key', None))

		# return to configuration screen
		return redirect('configuration')


####################################################################
# Published 													   #
####################################################################

@login_required
def published(request):

	'''
	Published records
	'''
	
	# get instance of Published model
	published = models.PublishedRecords()

	# isolate field_counts for templated tabled
	field_counts = published.count_indexed_fields()

	return render(request, 'core/published.html', {
			'published':published,
			'field_counts':field_counts,
			'es_index':published.esi.es_index,
			'breadcrumbs':breadcrumb_parser(request)
		})



####################################################################
# OAI Server 													   #
####################################################################

def oai(request):

	'''
	Parse GET parameters, send to OAIProvider instance from oai.py
	Return XML results
	'''

	# get OAIProvider instance
	op = OAIProvider(request.GET)

	# return XML
	return HttpResponse(op.generate_response(), content_type='text/xml')



####################################################################
# Global Search													   #
####################################################################

def search(request):

	'''
	Global search of Records
	'''

	# if search term present, use
	q = request.GET.get('q', None)
	if q:
		search_params = json.dumps({'q':q})
		logger.debug(search_params)
	else:
		search_params = None

	return render(request, 'core/search.html', {
			'search_string':q,
			'search_params':search_params,
			'breadcrumbs':breadcrumb_parser(request),
			'page_title':' | Search'
		})



####################################################################
# Export                   										   #
####################################################################

def job_export_mapped_fields(request, org_id, record_group_id, job_id):

	logger.debug('exporting mapped fields from Job')

	# retrieve job
	cjob = models.CombineJob.get_combine_job(int(job_id))

	# check for Kibana check
	kibana_style = request.POST.get('kibana_style', False)
	if kibana_style:
		kibana_style = True

	# get archive type
	archive_type = request.POST.get('archive_type')

	# get selected fields if present
	mapped_field_include = request.POST.getlist('mapped_field_include',False)

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Export Mapped Fields for Job: %s' % cjob.job.name,
		task_type = 'export_mapped_fields',
		task_params_json = json.dumps({
			'job_id':cjob.job.id,
			'kibana_style':kibana_style,
			'archive_type':archive_type,
			'mapped_field_include':mapped_field_include
		})
	)
	ct.save()
	bg_task = tasks.export_mapped_fields(
		ct.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	return redirect('bg_tasks')


def job_export_documents(request, org_id, record_group_id, job_id):

	logger.debug('exporting documents from Job')

	# retrieve job
	cjob = models.CombineJob.get_combine_job(int(job_id))

	# get records per file
	records_per_file = request.POST.get('records_per_file', False)
	if records_per_file in ['',False]:
		records_per_file = 500

	# get archive type
	archive_type = request.POST.get('archive_type')

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Export Documents for Job: %s' % cjob.job.name,
		task_type = 'export_documents',
		task_params_json = json.dumps({
			'job_id':cjob.job.id,
			'records_per_file':int(records_per_file),
			'archive_type':archive_type
		})
	)
	ct.save()
	bg_task = tasks.export_documents(
		ct.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	return redirect('bg_tasks')


def published_export_mapped_fields(request):

	logger.debug('exporting mapped fields from Published')

	# get instance of Published model
	published = models.PublishedRecords()

	# check for Kibana check
	kibana_style = request.POST.get('kibana_style', False)
	if kibana_style:
		kibana_style = True

	# get selected fields if present
	mapped_field_include = request.POST.getlist('mapped_field_include',False)

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Export Mapped Fields for Published Records',
		task_type = 'export_mapped_fields',
		task_params_json = json.dumps({
			'published':True,
			'kibana_style':kibana_style,
			'mapped_field_include':mapped_field_include
		})
	)
	ct.save()
	bg_task = tasks.export_mapped_fields(
		ct.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	return redirect('bg_tasks')


def published_export_documents(request):

	logger.debug('exporting documents from Job')

	# get instance of Published model
	published = models.PublishedRecords()

	# get records per file
	records_per_file = request.POST.get('records_per_file', False)
	if records_per_file in ['',False]:
		records_per_file = 500

	# get archive type
	archive_type = request.POST.get('archive_type')

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = 'Export Documents for Published Records',
		task_type = 'export_documents',
		task_params_json = json.dumps({
			'published':True,
			'records_per_file':int(records_per_file),
			'archive_type':archive_type
		})
	)
	ct.save()
	bg_task = tasks.export_documents(
		ct.id,
		verbose_name=ct.verbose_name,
		creator=ct
	)

	return redirect('bg_tasks')



####################################################################
# Analysis  													   #
####################################################################

def analysis(request):

	'''
	Analysis home
	'''

	# get all jobs associated with record group
	analysis_jobs = models.Job.objects.filter(job_type='AnalysisJob')

	# get analysis jobs hierarchy
	analysis_hierarchy = models.AnalysisJob.get_analysis_hierarchy()

	# get analysis jobs lineage
	analysis_job_lineage = models.Job.get_all_jobs_lineage(
			organization = analysis_hierarchy['organization'],
			record_group = analysis_hierarchy['record_group'],
			exclude_analysis_jobs = False
		)

	# loop through jobs
	for job in analysis_jobs:
		# update status
		job.update_status()

	# render page
	return render(request, 'core/analysis.html', {
			'jobs':analysis_jobs,
			'job_lineage_json':json.dumps(analysis_job_lineage),
			'for_analysis':True,
			'breadcrumbs':breadcrumb_parser(request)
		})


@login_required
def job_analysis(request):

	'''
	Run new analysis job
	'''

	# if GET, prepare form
	if request.method == 'GET':

		# check if published analysis
		analysis_type = request.GET.get('type', None)

		# retrieve all jobs
		input_jobs = models.Job.objects.all()

		# get validation scenarios
		validation_scenarios = models.ValidationScenario.objects.all()

		# get field mappers
		field_mappers = models.FieldMapper.objects.all()

		# get record identifier transformation scenarios
		rits = models.RecordIdentifierTransformationScenario.objects.all()

		# get job lineage for all jobs (filtered to input jobs scope)
		ld = models.Job.get_all_jobs_lineage(directionality='downstream', jobs_query_set=input_jobs)

		# get all bulk downloads
		bulk_downloads = models.DPLABulkDataDownload.objects.all()

		# render page
		return render(request, 'core/job_analysis.html', {
				'job_select_type':'multiple',
				'input_jobs':input_jobs,
				'validation_scenarios':validation_scenarios,
				'rits':rits,
				'field_mappers':field_mappers,
				'xml2kvp_handle':models.XML2kvp(),
				'analysis_type':analysis_type,
				'bulk_downloads':bulk_downloads,
				'job_lineage_json':json.dumps(ld)
			})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('Running new analysis job')

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# get job note
		job_note = request.POST.get('job_note')
		if job_note == '':
			job_note = None

		# retrieve jobs to merge
		input_jobs = [ models.Job.objects.get(pk=int(job)) for job in request.POST.getlist('input_job_id') ]
		logger.debug('analyzing jobs: %s' % input_jobs)

		# get field mapper configurations
		field_mapper = request.POST.get('field_mapper')
		fm_config_json = request.POST.get('fm_config_json')

		# get requested validation scenarios
		validation_scenarios = request.POST.getlist('validation_scenario', [])

		# handle requested record_id transform
		rits = request.POST.get('rits', None)
		if rits == '':
			rits = None

		# capture input filters
		input_filters = {
			'input_validity_valve':request.POST.get('input_validity_valve', 'all')
		}
		input_numerical_valve = request.POST.get('input_numerical_valve', None)
		if input_numerical_valve == '':
			input_numerical_valve = None
		else:
			input_numerical_valve = int(input_numerical_valve)
		input_filters['input_numerical_valve'] = input_numerical_valve
		# es query valve
		input_es_query_valve = request.POST.get('input_es_query_valve', None)
		if input_es_query_valve == '':
			input_es_query_valve = None
		input_filters['input_es_query_valve'] = input_es_query_valve

		# handle requested record_id transform
		dbdd = request.POST.get('dbdd', None)
		if dbdd == '':
			dbdd = None

		# initiate job
		cjob = models.AnalysisJob(
			job_name=job_name,
			job_note=job_note,
			user=request.user,
			input_jobs=input_jobs,
			field_mapper=field_mapper,
			fm_config_json=fm_config_json,
			validation_scenarios=validation_scenarios,
			rits=rits,
			input_filters=input_filters,
			dbdd=dbdd
		)
		
		# start job and update status
		job_status = cjob.start_job()

		# if job_status is absent, report job status as failed
		if job_status == False:
			cjob.job.status = 'failed'
			cjob.job.save()

		return redirect('analysis')



####################################################################
# Background Tasks												   #
####################################################################

def bg_tasks(request):

	logger.debug('retrieving background tasks')

	# update all tasks not marked as complete
	nc_tasks = models.CombineBackgroundTask.objects.filter(completed=False)
	for task in nc_tasks:
		task.update()

	return render(request, 'core/bg_tasks.html', {
		'breadcrumbs':breadcrumb_parser(request)
		})


def bg_tasks_delete_all(request):

	logger.debug('deleting all background tasks')

	# delete all Combine Background Tasks
	cts = models.CombineBackgroundTask.objects.all()
	for ct in cts:
		ct.delete()

	# delete all Django Background Tasks
	running = Task.objects.all()
	for task in running:
		task.delete()

	completed = CompletedTask.objects.all()
	for task in completed:
		task.delete()

	return redirect('bg_tasks')


def bg_task(request, task_id):

	# get task
	ct = models.CombineBackgroundTask.objects.get(pk=int(task_id))
	logger.debug('retrieving task: %s' % ct)

	# include job if mentioned in task params
	if 'job_id' in ct.task_params:
		cjob = models.CombineJob.get_combine_job(ct.task_params['job_id'])
	else:
		cjob = None

	return render(request, 'core/bg_task.html', {
			'ct':ct,
			'cjob':cjob,
			'breadcrumbs':breadcrumb_parser(request)
		})


def bg_task_delete(request, task_id):

	# get task
	ct = models.CombineBackgroundTask.objects.get(pk=int(task_id))
	logger.debug('deleting task: %s' % ct)

	ct.delete()

	return redirect('bg_tasks')



####################################################################
# Datatables endpoints 											   #
# https://bitbucket.org/pigletto/django-datatables-view/overview   #
####################################################################

class DTRecordsJson(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for Records table in Job Details
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'combine_id',
			'record_id',
			'job',
			'oai_set',
			'unique',
			'document',
			'error',
			'valid'
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		# order_columns = ['number', 'user', 'state', '', '']
		order_columns = [
			'id',
			'combine_id',
			'record_id',
			'job',
			'oai_set',
			'unique',
			'document',
			'error',
			'valid'
		]

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			
			# if job present, filter by job
			if 'job_id' in self.kwargs.keys():
				# get job
				job = models.Job.objects.get(pk=self.kwargs['job_id'])
				# return filtered queryset
				return models.Record.objects.filter(job=job)

			# else, return all records
			else:
				return models.Record.objects


		def render_column(self, row, column):

			# construct record link
			record_link = reverse(record, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':row.id
					})

			# handle db_id
			if column == 'id':
				return '<a href="%s"><code>%s</code></a>' % (record_link, row.id)

			# handle combine_id
			if column == 'combine_id':
				return '<a href="%s"><code>%s</code></a>' % (record_link, row.combine_id)

			# handle record_id
			if column == 'record_id':
				return '<a href="%s"><code>%s</code></a>' % (record_link, row.record_id)

			# handle document
			elif column == 'document':
				# attempt to parse as XML and return if valid or not
				try:
					xml = etree.fromstring(row.document.encode('utf-8'))
					return '<a target="_blank" href="%s">Valid XML</a>' % (reverse(record_document, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':row.id
					}))
				except:
					return '<span style="color: red;">Invalid XML</span>'

			# handle associated job
			elif column == 'job':
				return '<a href="%s"><code>%s</code></a>' % (reverse(job_details, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id
					}), row.job.name)

			# handle unique
			elif column == 'unique':
				if row.unique:
					return '<span style="color:green;">Unique in Job</span>'
				else:
					return '<span style="color:red;">Duplicate in Job</span>'

			# handle validation_results
			elif column == 'valid':
				if row.valid:
					return '<span style="color:green;">Valid</span>'
				else:
					return '<span style="color:red;">Invalid</span>'

			else:
				return super(DTRecordsJson, self).render_column(row, column)


		def filter_queryset(self, qs):
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				try:
					int(search)
					qs = qs.filter(Q(id=search))
				except:
					qs = qs.filter(Q(id__contains=search) | Q(combine_id__contains=search) | Q(record_id__contains=search))

			# return
			return qs



class DTPublishedJson(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for Published records
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'record_id',
			'job__record_group',
			'job__record_group__publish_set_id', # note syntax for Django FKs
			'oai_set',
			'unique_published',
			'document'
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		order_columns = [
			'id',
			'record_id',
			'job__record_group',
			'job__record_group__publish_set_id', # note syntax for Django FKs
			'oai_set',
			'unique_published',
			'document'
		]

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering

			# get PublishedRecords instance
			pr = models.PublishedRecords()
			
			# return queryset
			return pr.records


		def render_column(self, row, column):
			
			# handle document metadata

			if column == 'record_id':
				return '<a href="%s">%s</a>' % (reverse(record, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':row.id
					}), row.record_id)

			if column == 'job__record_group':
				return '<a href="%s">%s</a>' % (reverse(record_group, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id
					}), row.job.record_group.name)

			if column == 'document':
				# attempt to parse as XML and return if valid or not
				try:
					xml = etree.fromstring(row.document.encode('utf-8'))
					return '<a target="_blank" href="%s">Valid XML</a>' % (reverse(record_document, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':row.id
					}))
				except:
					return '<span style="color: red;">Invalid XML</span>'

			# handle associated job
			if column == 'job__record_group__publish_set_id':
				return row.job.record_group.publish_set_id

			# handle associated job
			if column == 'unique_published':
				if row.unique_published:
					return '<span style="color:green;">True</span>'
				else:
					return '<span style="color:red;">False</span>'

			else:
				return super(DTPublishedJson, self).render_column(row, column)


		def filter_queryset(self, qs):
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)

			if search:

				# determine if search is integer
				try:
					int_qs = int(search)
				except:
					int_qs = False

				# if integer
				if int_qs:
					qs = qs.filter(
						Q(id=search)
					)
				else:
					# very slow to include the job's publish set id - removing from search
					qs = qs.filter(
						Q(record_id__contains=search) |
						Q(document__contains=search)
					)


			return qs



class DTIndexingFailuresJson(BaseDatatableView):

		'''
		Databales JSON response for Indexing Failures
		'''

		# define the columns that will be returned
		columns = ['id', 'combine_id', 'record_id', 'mapping_error']

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		# order_columns = ['number', 'user', 'state', '', '']
		order_columns = ['id', 'combine_id', 'record_id', 'mapping_error']

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			
			# get job
			job = models.Job.objects.get(pk=self.kwargs['job_id'])

			# return filtered queryset
			return models.IndexMappingFailure.objects.filter(job=job)


		def render_column(self, row, column):

			# determine record link
			target_record = row.record
			record_link = reverse(record, kwargs={
					'org_id':target_record.job.record_group.organization.id,
					'record_group_id':target_record.job.record_group.id,
					'job_id':target_record.job.id,
					'record_id':target_record.id
				})

			if column == 'id':
				return '<a href="%s">%s</a>' % (record_link, target_record.id)
			
			if column == 'combine_id':
				return '<a href="%s">%s</a>' % (record_link, target_record.combine_id)

			if column == 'record_id':
				return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

			# handle associated job
			if column == 'job':
				return row.job.name

			else:
				return super(DTIndexingFailuresJson, self).render_column(row, column)


		def filter_queryset(self, qs):

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				logger.debug('looking for: %s' % search)
				qs = qs.filter(Q(id = search) | Q(combine_id = search) | Q(record_id = search) | Q(mapping_error__contains = search))

			return qs



class DTJobValidationScenarioFailuresJson(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'record_id',
			'results_payload',
			'fail_count'
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		# order_columns = ['number', 'user', 'state', '', '']
		order_columns = [
			'id',
			'record_id',
			'results_payload',
			'fail_count'
		]

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			
			# get job
			jv = models.JobValidation.objects.get(pk=self.kwargs['job_validation_id'])

			# return filtered queryset
			return jv.get_record_validation_failures()


		def render_column(self, row, column):

			# determine record link
			target_record = row.record
			record_link = "%s#validation_tab" % reverse(record, kwargs={
					'org_id':target_record.job.record_group.organization.id,
					'record_group_id':target_record.job.record_group.id,
					'job_id':target_record.job.id,
					'record_id':target_record.id
				})

			# handle record id
			if column == 'id':
				# get target record from row
				target_record = row.record
				return '<a href="%s">%s</a>' % (record_link, target_record.id)

			# handle record record_id
			elif column == 'record_id':
				# get target record from row
				target_record = row.record
				return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

			# handle results_payload
			elif column == 'results_payload':
				rp = json.loads(row.results_payload)['failed']
				return ', '.join(rp)

			# handle all else
			else:
				return super(DTJobValidationScenarioFailuresJson, self).render_column(row, column)


		def filter_queryset(self, qs):
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				qs = qs.filter(Q(record__record_id__contains=search)|Q(results_payload__contains=search))

			# return
			return qs



class DTDPLABulkDataMatches(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'record_id'
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		# order_columns = ['number', 'user', 'state', '', '']
		order_columns = [
			'id',
			'record_id'
		]

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			
			# get job
			job = models.Job.objects.get(pk=self.kwargs['job_id'])

			# get DPLA misses / matches
			dpla_bulk_data_matches = job.get_dpla_bulk_data_matches()

			# return queryset
			if self.kwargs['match_type'] == 'matches':
				return dpla_bulk_data_matches['matches']
			elif self.kwargs['match_type'] == 'misses':
				return dpla_bulk_data_matches['misses']


		def render_column(self, row, column):

			# determine record link
			target_record = row.record
			record_link = reverse(record, kwargs={
					'org_id':target_record.job.record_group.organization.id,
					'record_group_id':target_record.job.record_group.id,
					'job_id':target_record.job.id,
					'record_id':target_record.id
				})

			# handle record id
			if column == 'id':
				# get target record from row
				target_record = row
				return '<a href="%s" target="_blank">%s</a>' % (record_link, target_record.id)

			# handle record record_id
			elif column == 'record_id':
				# get target record from row
				target_record = row
				return '<a href="%s" target="_blank">%s</a>' % (record_link, target_record.record_id)

			# handle all else
			else:
				return super(DTDPLABulkDataMatches, self).render_column(row, column)


		def get_context_data(self, *args, **kwargs):
			stime = time.time()
			try:
				self.initialize(*args, **kwargs)

				qs = self.get_initial_queryset()

				# number of records before filtering
				total_records = qs.count()

				qs = self.filter_queryset(qs)

				# number of records after filtering
				total_display_records = qs.count()

				qs = self.ordering(qs)
				qs = self.paging(qs)

				# prepare output data
				if self.pre_camel_case_notation:
					aaData = self.prepare_results(qs)

					ret = {'sEcho': int(self._querydict.get('sEcho', 0)),
						   'iTotalRecords': total_records,
						   'iTotalDisplayRecords': total_display_records,
						   'aaData': aaData
						   }
				else:
					data = self.prepare_results(qs)

					ret = {'draw': int(self._querydict.get('draw', 0)),
						   'recordsTotal': total_records,
						   'recordsFiltered': total_display_records,
						   'data': data
						   }
				logger.debug('context data total %s' % (time.time() - stime))
				return ret
			except Exception as e:
				return self.handle_exception(e)



class JobRecordDiffs(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for Records that were
		transformed during a Transformation Job
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'record_id',
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		order_columns = [
			'id',
			'record_id'
		]

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			
			# get job
			job = models.Job.objects.get(pk=self.kwargs['job_id'])
			job_records = job.get_records()

			# filter for records that were transformed
			return job_records.filter(transformed=True)


		def render_column(self, row, column):

			# record link
			record_link = reverse(record, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':row.id
					})

			# handle db_id
			if column == 'id':
				return '<a href="%s"><code>%s</code></a>' % (record_link, row.id)

			# handle record_id
			if column == 'record_id':
				return '<a href="%s"><code>%s</code></a>' % (record_link, row.record_id)

			else:
				return super(JobRecordDiffs, self).render_column(row, column)


		def filter_queryset(self, qs):
			
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				qs = qs.filter(Q(id__contains=search) | Q(record_id__contains=search) | Q(document__contains=search))

			# return
			return qs


class CombineBackgroundTasksDT(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for Records table in Job Details
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'start_timestamp',
			'name',
			'task_type',
			'verbose_name',
			'completed',
			'duration',
			'actions'
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		# order_columns = ['number', 'user', 'state', '', '']
		order_columns = [
			'id',
			'start_timestamp',
			'name',
			'task_type',
			'verbose_name',
			'completed',
			'duration',
			'actions'
		]

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			return models.CombineBackgroundTask.objects


		def render_column(self, row, column):

			if column == 'task_type':
				return row.get_task_type_display()

			elif column == 'verbose_name':
				return '<code>%s</code>' % row.verbose_name

			elif column == 'completed':
				if row.completed:
					return "<span style='color:green;'>Finished</span>"
				else:
					return "<span style='color:orange;'>Running</span>"

			elif column == 'duration':
				return row.calc_elapsed_as_string()
				

			elif column == 'actions':
				return '<a href="%s"><button type="button" class="btn btn-success btn-sm">Results</button></a> <a href="%s"><button type="button" class="btn btn-outline-danger btn-sm" onclick="return confirm(\'Are you sure you want to remove this task?\');">Delete</button></a>' % (
					reverse(bg_task, kwargs={'task_id':row.id}),
					reverse(bg_task_delete, kwargs={'task_id':row.id})
				)

			else:
				return super(CombineBackgroundTasksDT, self).render_column(row, column)


		def filter_queryset(self, qs):
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				qs = qs.filter(Q(id__contains=search) | Q(name__contains=search) | Q(verbose_name__contains=search))

			# return
			return qs

