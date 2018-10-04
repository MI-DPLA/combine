# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import ast
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
from django.db import connection
from django.db.models import Q
from django.db.models.query import QuerySet
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

# import mongo dependencies
from core.mongo import *

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
	r_m = re.match(r'(.+?/record/([0-9a-z]+))', request.path)
	if r_m:
		r = models.Record.objects.get(id=r_m.group(2))
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
def system(request):
	
	# single Livy session
	logger.debug("checking or active Livy session")
	livy_session = models.LivySession.get_active_session()

	# if session found, refresh
	if type(livy_session) == models.LivySession:

		# refresh
		livy_session.refresh_from_livy()
		
		# create and append to list
		livy_sessions = [livy_session]

	elif type(livy_session) == QuerySet:
		
		# loop and refresh
		for s in livy_session:
			s.refresh_from_livy()
		
		# set as list
		livy_sessions = livy_session

	else:
		livy_sessions = livy_session

	# get status of background jobs
	sp = models.SupervisorRPCClient()
	bgtasks_proc = sp.check_process('combine_background_tasks')

	# return
	return render(request, 'core/system.html', {
		'livy_session':livy_session,
		'livy_sessions':livy_sessions,
		'bgtasks_proc':bgtasks_proc,
		'breadcrumbs':breadcrumb_parser(request)
	})


@login_required
def livy_session_start(request):
	
	logger.debug('Checking for pre-existing livy sessions')

	# get active livy sessions
	active_ls = models.LivySession.get_active_session()

	# none found
	if not active_ls:
		logger.debug('active livy session not found, starting')
		livy_session = models.LivySession()
		livy_session.start_session()

	elif type(active_ls) == models.LivySession and request.GET.get('restart') == 'true':
		logger.debug('single, active session found, and restart flag passed, restarting')

		# restart
		new_ls = active_ls.restart_session()

	# redirect
	return redirect('system')


@login_required
def livy_session_stop(request, session_id):
	
	logger.debug('stopping Livy session by Combine ID: %s' % session_id)

	livy_session = models.LivySession.objects.filter(id=session_id).first()
	
	# attempt to stop with Livy
	models.LivyClient.stop_session(livy_session.session_id)

	# remove from DB
	livy_session.delete()

	# redirect
	return redirect('system')


@login_required
def bgtasks_proc_action(request, proc_action):
	
	logger.debug('performing %s on bgtasks_proc' % proc_action)

	# get supervisor handle
	sp = models.SupervisorRPCClient()

	# fire action
	actions = {
		'start':sp.start_process,
		'restart':sp.restart_process,
		'stop':sp.stop_process
	}
	results = actions[proc_action]('combine_background_tasks') 
	logger.debug(results)

	# redirect
	return redirect('system')


@login_required
def bgtasks_proc_stderr_log(request):

	# get supervisor handle
	sp = models.SupervisorRPCClient()

	log_tail = sp.stderr_log_tail('combine_background_tasks')

	# redirect
	return HttpResponse(log_tail, content_type='text/plain')



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


@login_required
def record_group(request, org_id, record_group_id):

	'''
	View information about a single record group, including any and all jobs run

	Args:
		record_group_id (str/int): PK for RecordGroup table
	'''	
	
	logger.debug('retrieving record group ID: %s' % record_group_id)

	# retrieve record group
	record_group = models.RecordGroup.objects.get(pk=int(record_group_id))

	# get all jobs associated with record group
	jobs = models.Job.objects.filter(record_group=record_group_id)	

	# get all currently applied publish set ids
	publish_set_ids = models.PublishedRecords.get_publish_set_ids()	

	# loop through jobs
	for job in jobs:

		# update status
		job.update_status()

	# get record group job lineage	
	job_lineage = record_group.get_jobs_lineage()

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
def stop_jobs(request):

	logger.debug('stopping jobs')
	
	job_ids = request.POST.getlist('job_ids[]')
	logger.debug(job_ids)

	# get downstream toggle
	downstream_toggle = request.POST.get('downstream_stop_toggle', False);
	if downstream_toggle == 'true':
		downstream_toggle = True
	elif downstream_toggle == 'false':
		downstream_toggle = False
	
	# set of jobs to rerun
	job_stop_set = set()

	# loop through job_ids
	for job_id in job_ids:		
		
		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# if including downstream
		if downstream_toggle:

			# add rerun lineage for this job to set
			job_stop_set.update(cjob.job.get_downstream_lineage())

		# else, just job
		else:

			job_stop_set.add(cjob.job)

	# sort and run
	ordered_job_delete_set = sorted(list(job_stop_set), key=lambda j: j.id)

	# # loop through and update visible elements of Job for front-end
	for job in ordered_job_delete_set:

		logger.debug('stopping Job: %s' % job)		

		# stop job
		job.stop_job()

	# set gms
	gmc = models.GlobalMessageClient(request.session)
	gmc.add_gm({
		'html':'<p><strong>Stopped Job(s):</strong><br>%s</p>' %  ('<br>'.join([j.name for j in ordered_job_delete_set ])),
		'class':'danger'
	})

	# return
	return JsonResponse({'results':True})


@login_required
def delete_jobs(request):

	logger.debug('deleting jobs')
	
	job_ids = request.POST.getlist('job_ids[]')
	logger.debug(job_ids)

	# get downstream toggle
	downstream_toggle = request.POST.get('downstream_delete_toggle', False);
	if downstream_toggle == 'true':
		downstream_toggle = True
	elif downstream_toggle == 'false':
		downstream_toggle = False
	
	# set of jobs to rerun
	job_delete_set = set()

	# loop through job_ids
	for job_id in job_ids:		
		
		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# if including downstream
		if downstream_toggle:

			# add rerun lineage for this job to set
			job_delete_set.update(cjob.job.get_downstream_lineage())

		# else, just job
		else:

			job_delete_set.add(cjob.job)

	# sort and run
	ordered_job_delete_set = sorted(list(job_delete_set), key=lambda j: j.id)

	# # loop through and update visible elements of Job for front-end
	for job in ordered_job_delete_set:

		logger.debug('deleting Job: %s' % job)		

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

	# set gms
	gmc = models.GlobalMessageClient(request.session)
	gmc.add_gm({
		'html':'<p><strong>Deleting Job(s):</strong><br>%s</p><p>Refresh this page to update status of removing Jobs. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button></p>' %  ('<br>'.join([j.name for j in ordered_job_delete_set ])),
		'class':'danger'
	})

	# return
	return JsonResponse({'results':True})


@login_required
def move_jobs(request):

	logger.debug('moving jobs')	

	job_ids = request.POST.getlist('job_ids[]')
	record_group_id = request.POST.getlist('record_group_id')[0]

	# get downstream toggle
	downstream_toggle = request.POST.get('downstream_move_toggle', False);
	if downstream_toggle == 'true':
		downstream_toggle = True
	elif downstream_toggle == 'false':
		downstream_toggle = False
	
	# set of jobs to rerun
	job_move_set = set()

	# loop through job_ids
	for job_id in job_ids:		
		
		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# if including downstream
		if downstream_toggle:

			# add rerun lineage for this job to set
			job_move_set.update(cjob.job.get_downstream_lineage())

		# else, just job
		else:

			job_move_set.add(cjob.job)

	# sort and run
	ordered_job_move_set = sorted(list(job_move_set), key=lambda j: j.id)

	# loop through jobs
	for job in ordered_job_move_set:

		logger.debug('moving Job: %s' % job)		
		
		new_record_group = models.RecordGroup.objects.get(pk=record_group_id)
		job.record_group = new_record_group
		job.save()

		logger.debug('Job %s has been moved' % job)

	# redirect
	return JsonResponse({'results':True})


@login_required
def job_details(request, org_id, record_group_id, job_id):
	
	logger.debug('details for job id: %s' % job_id)

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# update status
	cjob.job.update_status()

	# detailed record count	
	record_count_details = cjob.job.get_detailed_job_record_count()	

	# get job lineage
	job_lineage = cjob.job.get_lineage()

	# get dpla_bulk_data_match
	dpla_bulk_data_matches = cjob.job.get_dpla_bulk_data_matches()

	# check if limiting to one, pre-existing record
	q = request.GET.get('q', None)

	# job details and job type specific augment
	job_details = cjob.job.job_details_dict	

	# mapped field analysis, generate if not part of job_details
	if 'mapped_field_analysis' in job_details.keys():
		field_counts = job_details['mapped_field_analysis']
	else:
		if cjob.job.finished:
			field_counts = cjob.count_indexed_fields()
			cjob.job.update_job_details({'mapped_field_analysis':field_counts}, save=True)
		else:
			logger.debug('job not finished, not setting')
			field_counts = {}	

	# OAI Harvest
	if type(cjob) == models.HarvestOAIJob:
		pass

	# Static Harvest
	elif type(cjob) == models.HarvestStaticXMLJob:
		pass

	# Transform 
	elif type(cjob) == models.TransformJob:
		pass

	# Merge/Duplicate 
	elif type(cjob) == models.MergeJob:
		pass

	# Analysis
	elif type(cjob) == models.AnalysisJob:
		pass

	# get published records, primarily for published sets
	pr = models.PublishedRecords()

	# return
	return render(request, 'core/job_details.html', {
			'cjob':cjob,
			'record_count_details':record_count_details,
			'field_counts':field_counts,
			'job_lineage_json':json.dumps(job_lineage),
			'dpla_bulk_data_matches':dpla_bulk_data_matches,
			'q':q,			
			'job_details':job_details,
			'pr':pr,
			'es_index_str':cjob.esi.es_index_str,
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
def job_publish(request, org_id, record_group_id, job_id):

	# get preferred metadata index mapper
	publish_set_id = request.GET.get('publish_set_id', None)

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# init publish
	bg_task = cjob.publish_bg_task(publish_set_id=publish_set_id)

	# set gms	
	gmc = models.GlobalMessageClient(request.session)
	gmc.add_gm({
		'html':'<p><strong>Publishing Job:</strong><br>%s<br><br><strong>Publish Set ID:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Published Records</button></a></p>' %  (cjob.job.name, publish_set_id, reverse('published')),
		'class':'success'
	})

	return redirect('record_group',
		org_id=cjob.job.record_group.organization.id,
		record_group_id=cjob.job.record_group.id)	


@login_required
def job_unpublish(request, org_id, record_group_id, job_id):

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# init unpublish
	bg_task = cjob.unpublish_bg_task()

	# set gms	
	gmc = models.GlobalMessageClient(request.session)
	gmc.add_gm({
		'html':'<p><strong>Unpublishing Job:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Published Records</button></a></p>' %  (cjob.job.name, reverse('published')),
		'class':'success'
	})

	return redirect('record_group',
		org_id=cjob.job.record_group.organization.id,
		record_group_id=cjob.job.record_group.id)


@login_required
def rerun_jobs(request):

	logger.debug('re-running jobs')
	
	# get job ids
	job_ids = request.POST.getlist('job_ids[]')

	# get downstream toggle
	downstream_toggle = request.POST.get('downstream_rerun_toggle', False);
	if downstream_toggle == 'true':
		downstream_toggle = True
	elif downstream_toggle == 'false':
		downstream_toggle = False
	
	# set of jobs to rerun
	job_rerun_set = set()

	# loop through job_ids
	for job_id in job_ids:		
		
		# get CombineJob
		cjob = models.CombineJob.get_combine_job(job_id)

		# if including downstream
		if downstream_toggle:

			# add rerun lineage for this job to set
			job_rerun_set.update(cjob.job.get_downstream_lineage())

		# else, just job
		else:

			job_rerun_set.add(cjob.job)

	# sort and run
	ordered_job_rerun_set = sorted(list(job_rerun_set), key=lambda j: j.id)

	# # loop through and update visible elements of Job for front-end
	for re_job in ordered_job_rerun_set:

		re_job.timestamp = datetime.datetime.now()
		re_job.status = 'initializing'
		re_job.record_count = 0
		re_job.finished = False
		re_job.elapsed = 0
		re_job.deleted = True
		re_job.save()

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = "Rerun Jobs Prep",
		task_type = 'rerun_jobs_prep',
		task_params_json = json.dumps({
			'ordered_job_rerun_set':[j.id for j in ordered_job_rerun_set]
		})
	)
	ct.save()

	# run actual background task, passing CombineTask (ct) id (must be JSON serializable),
	# and setting creator and verbose_name params
	bt = tasks.rerun_jobs_prep(
		ct.id,
		verbose_name = ct.verbose_name,
		creator = ct
	)

	# set gms
	gmc = models.GlobalMessageClient(request.session)
	gmc.add_gm({
		'html':'<strong>Preparing to Rerun Job(s):</strong><br>%s<br><br>Refresh this page to update status of Jobs rerunning. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button>' %  '<br>'.join([str(j.name) for j in ordered_job_rerun_set]),
		'class':'success'
	})

	# return, as requested via Ajax which will reload page
	return JsonResponse({'results':True})


@login_required
def clone_jobs(request):

	logger.debug('cloning jobs')
	
	job_ids = request.POST.getlist('job_ids[]')
	
	# get downstream toggle
	downstream_toggle = request.POST.get('downstream_clone_toggle', False);
	if downstream_toggle == 'true':
		downstream_toggle = True
	elif downstream_toggle == 'false':
		downstream_toggle = False

	# get rerun toggle
	rerun_on_clone = request.POST.get('rerun_on_clone', False);
	if rerun_on_clone == 'true':
		rerun_on_clone = True
	elif rerun_on_clone == 'false':
		rerun_on_clone = False
	
	# set of jobs to rerun
	job_clone_set = set()

	# loop through job_ids and add
	for job_id in job_ids:
		cjob = models.CombineJob.get_combine_job(job_id)
		job_clone_set.add(cjob.job)

	# sort and run
	ordered_job_clone_set = sorted(list(job_clone_set), key=lambda j: j.id)

	# initiate Combine BG Task
	ct = models.CombineBackgroundTask(
		name = "Clone Jobs",
		task_type = 'clone_jobs',
		task_params_json = json.dumps({
			'ordered_job_clone_set':[j.id for j in ordered_job_clone_set],
			'downstream_toggle':downstream_toggle,
			'rerun_on_clone':rerun_on_clone
		})
	)
	ct.save()

	# run actual background task, passing CombineTask (ct) id (must be JSON serializable),
	# and setting creator and verbose_name params
	bt = tasks.clone_jobs(
		ct.id,
		verbose_name = ct.verbose_name,
		creator = ct
	)

	# set gms
	gmc = models.GlobalMessageClient(request.session)
	gmc.add_gm({
		'html':'<strong>Cloning Job(s):</strong><br>%s<br><br>Including downstream? <strong>%s</strong><br><br>Refresh this page to update status of Jobs cloning. <button class="btn-sm btn-outline-primary" onclick="location.reload();">Refresh</button>' %  ('<br>'.join([str(j.name) for j in ordered_job_clone_set]), downstream_toggle),
		'class':'success'
	})

	# return, as requested via Ajax which will reload page
	return JsonResponse({'results':True})


@login_required
def job_parameters(request, org_id, record_group_id, job_id):
	
	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# if GET, return JSON
	if request.method == 'GET':

		# return
		return JsonResponse(cjob.job.job_details_dict)


	# if POST, udpate
	if request.method == 'POST':

		# get job_details as JSON
		job_details_json = request.POST.get('job_details_json', None)

		if job_details_json != None:

			cjob.job.job_details = job_details_json
			cjob.job.save()

		return JsonResponse({"msg":"Job Parameters updated!"})



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

		cjob = models.CombineJob.init_combine_job(
			user = request.user,
			record_group = record_group,
			job_type_class = models.HarvestOAIJob,
			job_params = request.POST
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

		cjob = models.CombineJob.init_combine_job(
			user = request.user,
			record_group = record_group,
			job_type_class = models.HarvestStaticXMLJob,
			job_params = request.POST,
			files = request.FILES,
			hash_payload_filename = hash_payload_filename
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
		transformations = models.Transformation.objects.filter(use_as_include=False)

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

		cjob = models.CombineJob.init_combine_job(
			user = request.user,
			record_group = record_group,
			job_type_class = models.TransformJob,
			job_params = request.POST)

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

		cjob = models.CombineJob.init_combine_job(
			user = request.user,
			record_group = record_group,
			job_type_class = models.MergeJob,
			job_params = request.POST)

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

		# mapped field analysis, generate if not part of job_details		
		if 'mapped_field_analysis' in cjob.job.job_details_dict.keys():
			field_counts = cjob.job.job_details_dict['mapped_field_analysis']
		else:
			if cjob.job.finished:
				field_counts = cjob.count_indexed_fields()
				cjob.job.update_job_details({'mapped_field_analysis':field_counts}, save=True)
			else:
				logger.debug('job not finished, not setting')
				field_counts = {}

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
			'compression_type':request.POST.get('compression_type'),
			'validation_scenarios':request.POST.getlist('validation_scenario', []),
			'mapped_field_include':request.POST.getlist('mapped_field_include', [])
		}

		# cast to int
		task_params['validation_scenarios'] = [int(vs_id) for vs_id in task_params['validation_scenarios']]

		# remove select, reserved fields if in mapped field request
		task_params['mapped_field_include'] = [ f for f in task_params['mapped_field_include'] if f not in ['record_id','db_id','oid','_id']] 

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

		# get all bulk downloads
		bulk_downloads = models.DPLABulkDataDownload.objects.all()

		# get uptdate type from GET params
		update_type = request.GET.get('update_type', None)		

		# render page
		return render(request, 'core/job_update.html', {
				'cjob':cjob,
				'update_type':update_type,
				'validation_scenarios':validation_scenarios,
				'field_mappers':field_mappers,
				'bulk_downloads':bulk_downloads,
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
			fm_config_json = request.POST.get('fm_config_json')

			# init re-index
			ct = cjob.reindex_bg_task(fm_config_json=fm_config_json)

			# set gms
			gmc = models.GlobalMessageClient(request.session)
			gmc.add_gm({
				'html':'<p><strong>Re-Indexing Job:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (cjob.job.name, reverse('bg_tasks')),
				'class':'success'
			})

			return redirect('job_details',
				org_id=cjob.job.record_group.organization.id,
				record_group_id=cjob.job.record_group.id,
				job_id=cjob.job.id)

		# handle new validations
		if update_type == 'validations':

			# get requested validation scenarios
			validation_scenarios = request.POST.getlist('validation_scenario', [])

			# get validations
			validations = models.ValidationScenario.objects.filter(id__in=[ int(vs_id) for vs_id in validation_scenarios ])

			# init bg task
			bg_task = cjob.new_validations_bg_task(validation_scenarios)

			# set gms
			gmc = models.GlobalMessageClient(request.session)
			gmc.add_gm({
				'html':'<p><strong>Running New Validations for Job:</strong><br>%s<br><br><strong>Validation Scenarios:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (cjob.job.name, '<br>'.join([vs.name for vs in validations]), reverse('bg_tasks')),
				'class':'success'
			})

			return redirect('job_details',
				org_id=cjob.job.record_group.organization.id,
				record_group_id=cjob.job.record_group.id,
				job_id=cjob.job.id)

		# handle validation removal
		if update_type == 'remove_validation':

			# get validation scenario to remove
			jv_id = request.POST.get('jv_id', False)

			# initiate Combine BG Task
			bg_task = cjob.remove_validation_bg_task(jv_id)

			# set gms
			vs = models.JobValidation.objects.get(pk=int(jv_id)).validation_scenario
			gmc = models.GlobalMessageClient(request.session)
			gmc.add_gm({
				'html':'<p><strong>Removing Validation for Job:</strong><br>%s<br><br><strong>Validation Scenario:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (cjob.job.name, vs.name, reverse('bg_tasks')),
				'class':'success'
			})

			return redirect('job_details',
				org_id=cjob.job.record_group.organization.id,
				record_group_id=cjob.job.record_group.id,
				job_id=cjob.job.id)

		# handle validation removal
		if update_type == 'dbdm':

			# get validation scenario to remove
			dbdd_id = request.POST.get('dbdd', False)

			# initiate Combine BG Task
			bg_task = cjob.dbdm_bg_task(dbdd_id)

			# set gms
			dbdd = models.DPLABulkDataDownload.objects.get(pk=int(dbdd_id))
			gmc = models.GlobalMessageClient(request.session)
			gmc.add_gm({
				'html':'<p><strong>Running DPLA Bulk Data comparison for Job:</strong><br>%s<br><br><strong>Bulk Data S3 key:</strong><br>%s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (cjob.job.name, dbdd.s3_key, reverse('bg_tasks')),
				'class':'success'
			})

			return redirect('job_details',
				org_id=cjob.job.record_group.organization.id,
				record_group_id=cjob.job.record_group.id,
				job_id=cjob.job.id)



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
		},
		'tsv':{
			'extension':'.tsv',
			'content_type':'text/plain'
		},
		'json':{
			'extension':'.json',
			'content_type':'text/plain'
		},
		'zip':{
			'extension':'.zip',
			'content_type':'application/zip'
		},
		'targz':{
			'extension':'.tar.gz',
			'content_type':'application/gzip'
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

	# generate response
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
	
	# get ESIndex, evaluating stringified list
	esi = models.ESIndex(ast.literal_eval(es_index))

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
	esi = models.ESIndex(ast.literal_eval(es_index))

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
	
	# get record
	record = models.Record.objects.get(id=record_id)

	# build ancestry in both directions
	record_stages = record.get_record_stages()

	# get details depending on job type
	logger.debug('Job type is %s, retrieving details' % record.job.job_type)
	try:
		job_details = record.job.job_details_dict

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

	# attempt to get document as pretty print
	try:
		pretty_document = record.document_pretty_print()
		pretty_format_msg = False
	except Exception as e:
		pretty_document = record.document
		pretty_format_msg = str(e)

	# return
	return render(request, 'core/record.html', {
		'record_id':record_id,
		'record':record,
		'record_stages':record_stages,
		'job_details':job_details,
		'dpla_api_doc':dpla_api_doc,
		'dpla_api_json':dpla_api_json,
		'record_diff_dict':record_diff_dict,
		'pretty_document':pretty_document,
		'pretty_format_msg':pretty_format_msg,
		'job_fm_config_json':job_fm_config_json,
		'breadcrumbs':breadcrumb_parser(request)
	})


def record_document(request, org_id, record_group_id, job_id, record_id):

	'''
	View document for record
	'''

	# get record
	record = models.Record.objects.get(id=record_id)

	# return document as XML
	return HttpResponse(record.document, content_type='text/xml')


def record_indexed_document(request, org_id, record_group_id, job_id, record_id):

	'''
	View indexed, ES document for record
	'''

	# get record
	record = models.Record.objects.get(id=record_id)

	# return ES document as JSON
	return JsonResponse(record.get_es_doc())
	


def record_error(request, org_id, record_group_id, job_id, record_id):

	'''
	View document for record
	'''

	# get record
	record = models.Record.objects.get(id=record_id)

	# return document as XML
	return HttpResponse("<pre>%s</pre>" % record.error)


def record_validation_scenario(request, org_id, record_group_id, job_id, record_id, job_validation_id):

	'''
	Re-run validation test for single record

	Returns:
		results of validation
	'''

	# get record
	record = models.Record.objects.get(id=record_id)

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
	record = models.Record.objects.get(id=record_id)

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
	record = models.Record.objects.get(id=record_id)

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
	transformations = models.Transformation.objects.filter(use_as_include=False)

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
		transformation_scenarios = models.Transformation.objects.filter(use_as_include=False)

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
		record = models.Record.objects.get(id=request.POST.get('db_id'))

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

		# get record
		record = models.Record.objects.get(id=request.POST.get('db_id'))

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
				record = models.Record.objects.get(id=request.POST.get('db_id'))
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
		record = models.Record.objects.get(id=request.POST.get('db_id'))

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

		# OLD ######################################################################
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

	# get field counts
	if published.records.count() > 0:
		# get count of fields for all published job indices
		field_counts = published.count_indexed_fields()
	else:
		field_counts = {}

	return render(request, 'core/published.html', {
			'published':published,
			'field_counts':field_counts,
			'es_index_str':published.esi.es_index_str,
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

def export_documents(request, export_source, job_id=None):

	# get records per file
	records_per_file = request.POST.get('records_per_file', False)
	if records_per_file in ['',False]:
		records_per_file = 500

	# get archive type
	archive_type = request.POST.get('archive_type')

	# export for single job
	if export_source == 'job':

		logger.debug('exporting documents from Job')

		# retrieve job
		cjob = models.CombineJob.get_combine_job(int(job_id))

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

		# fire bg_task
		bg_task = tasks.export_documents(
			ct.id,
			verbose_name=ct.verbose_name,
			creator=ct
		)

		# set gm
		gmc = models.GlobalMessageClient(request.session)
		target = "Job:</strong><br>%s" % cjob.job.name
		gmc.add_gm({
			'html':'<p><strong>Exporting Documents for %s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (target, reverse('bg_tasks')),
			'class':'success'
		})

		return redirect('job_details',
			org_id=cjob.job.record_group.organization.id,
			record_group_id=cjob.job.record_group.id,
			job_id=cjob.job.id)

	# export for published
	if export_source == 'published':

		logger.debug('exporting documents from Job')

		# get instance of Published model
		published = models.PublishedRecords()

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

		# fire bg_task
		bg_task = tasks.export_documents(
			ct.id,
			verbose_name=ct.verbose_name,
			creator=ct
		)

		# set gm
		gmc = models.GlobalMessageClient(request.session)
		target = ":</strong><br>Published Records"
		gmc.add_gm({
			'html':'<p><strong>Exporting Documents for %s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (target, reverse('bg_tasks')),
			'class':'success'
		})

		return redirect('published')


def export_mapped_fields(request, export_source, job_id=None):

	# get mapped fields export type
	mapped_fields_export_type = request.POST.get('mapped_fields_export_type')

	# check for Kibana check
	kibana_style = request.POST.get('kibana_style', False)
	if kibana_style:
		kibana_style = True

	# get archive type
	archive_type = request.POST.get('archive_type')

	# get selected fields if present
	mapped_field_include = request.POST.getlist('mapped_field_include',False)

	# export for single job
	if export_source == 'job':

		logger.debug('exporting mapped fields from Job')

		# retrieve job
		cjob = models.CombineJob.get_combine_job(int(job_id))

		# initiate Combine BG Task
		ct = models.CombineBackgroundTask(
			name = 'Export Mapped Fields for Job: %s' % cjob.job.name,
			task_type = 'export_mapped_fields',
			task_params_json = json.dumps({
				'job_id':cjob.job.id,
				'mapped_fields_export_type':mapped_fields_export_type,
				'kibana_style':kibana_style,
				'archive_type':archive_type,
				'mapped_field_include':mapped_field_include
			})
		)
		ct.save()

		# fire bg task
		bg_task = tasks.export_mapped_fields(
			ct.id,
			verbose_name=ct.verbose_name,
			creator=ct
		)

		# set gm
		gmc = models.GlobalMessageClient(request.session)
		target = "Job:</strong><br>%s" % cjob.job.name
		gmc.add_gm({
			'html':'<p><strong>Exporting Mapped Fields for %s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (target, reverse('bg_tasks')),
			'class':'success'
		})

		return redirect('job_details',
			org_id=cjob.job.record_group.organization.id,
			record_group_id=cjob.job.record_group.id,
			job_id=cjob.job.id)

	# export for published
	if export_source == 'published':

		logger.debug('exporting mapped fields from published records')

		# get instance of Published model
		published = models.PublishedRecords()

		# initiate Combine BG Task
		ct = models.CombineBackgroundTask(
			name = 'Export Mapped Fields for Published Records',
			task_type = 'export_mapped_fields',
			task_params_json = json.dumps({
				'published':True,
				'mapped_fields_export_type':mapped_fields_export_type,
				'kibana_style':kibana_style,
				'archive_type':archive_type,
				'mapped_field_include':mapped_field_include
			})
		)
		ct.save()

		# fire bg task
		bg_task = tasks.export_mapped_fields(
			ct.id,
			verbose_name=ct.verbose_name,
			creator=ct
		)

		# set gm
		gmc = models.GlobalMessageClient(request.session)
		target = ":</strong><br>Published Records"
		gmc.add_gm({
			'html':'<p><strong>Exporting Mapped Fields for %s</p><p><a href="%s"><button type="button" class="btn btn-outline-primary btn-sm">View Background Tasks</button></a></p>' %  (target, reverse('bg_tasks')),
			'class':'success'
		})

		return redirect('published')



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

		# retrieve jobs (limiting if needed)
		input_jobs = models.Job.objects.all()

		# limit if analysis_type set		
		analysis_type = request.GET.get('type', None)
		if analysis_type == 'published':
			input_jobs = input_jobs.filter(published=True)

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

		cjob = models.CombineJob.init_combine_job(
			user = request.user,
			record_group = record_group,
			job_type_class = models.AnalysisJob,
			job_params = request.POST)

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
			'_id',			
			'record_id',
			'job_id',
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
			'_id',			
			'record_id',
			'job_id',
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

				# get jobself.kwargs['job_id']
				job = models.Job.objects.get(pk=self.kwargs['job_id'])

				# return filtered queryset
				return job.get_records(success=None)

			# else, return all records
			else:
				return models.Record.objects


		def render_column(self, row, column):

			# construct record link
			record_link = reverse(record, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':str(row.id)
					})

			# handle db_id
			if column == '_id':
				return '<a href="%s"><code>%s</code></a>' % (record_link, str(row.id))

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
						'job_id':row.job.id, 'record_id':str(row.id)
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
				# sniff out ObjectId if present
				if len(search) == 24:
					try:
						oid = ObjectId(search)					
						qs = qs.filter(mongoengine.Q(id=oid))
					except:
						logger.debug('recieved 24 chars, but not ObjectId')
				else:
					qs = qs.filter(mongoengine.Q(record_id=search))

			# return
			return qs



class DTPublishedJson(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for Published records
		'''

		# define the columns that will be returned
		columns = [
			'_id',
			'record_id',
			'job_id',
			'publish_set_id', 
			# 'oai_set',
			# 'unique_published',
			'document'
		]

		# define column names that will be used in sorting
		# order is important and should be same as order of columns
		# displayed by datatables. For non sortable columns use empty
		# value like ''
		order_columns = [
			'_id',
			'record_id',
			'job_id',
			'publish_set_id', 
			# 'oai_set',
			# 'unique_published',
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

			if column == '_id':
				return '<a href="%s">%s</a>' % (reverse(record, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':str(row.id)
					}), str(row.id))

			if column == 'record_id':
				return '<a href="%s">%s</a>' % (reverse(record, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':str(row.id)
					}), row.record_id)

			if column == 'job_id':
				return '<a href="%s">%s</a>' % (reverse(job_details, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id
					}), row.job.name)

			if column == 'document':
				# attempt to parse as XML and return if valid or not
				try:
					xml = etree.fromstring(row.document.encode('utf-8'))
					return '<a target="_blank" href="%s">Valid XML</a>' % (reverse(record_document, kwargs={
						'org_id':row.job.record_group.organization.id,
						'record_group_id':row.job.record_group.id,
						'job_id':row.job.id, 'record_id':str(row.id)
					}))
				except:
					return '<span style="color: red;">Invalid XML</span>'

			# # handle associated job
			# if column == 'unique_published':
			# 	if row.unique_published:
			# 		return '<span style="color:green;">True</span>'
			# 	else:
			# 		return '<span style="color:red;">False</span>'

			else:
				return super(DTPublishedJson, self).render_column(row, column)


		def filter_queryset(self, qs):
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				# sniff out ObjectId if present
				if len(search) == 24:
					try:
						oid = ObjectId(search)					
						qs = qs.filter(mongoengine.Q(id=oid))
					except:
						logger.debug('recieved 24 chars, but not ObjectId')
				else:
					qs = qs.filter(mongoengine.Q(record_id=search) | mongoengine.Q(publish_set_id=search))

			# return
			return qs



class DTIndexingFailuresJson(BaseDatatableView):

		'''
		Databales JSON response for Indexing Failures
		'''

		# define the columns that will be returned
		columns = ['_id', 'record_id', 'mapping_error']

		# define column names that will be used in sorting		
		order_columns = ['_id', 'record_id', 'mapping_error']

		# set max limit of records returned, this is used to protect our site if someone tries to attack our site
		# and make it return huge amount of data
		max_display_length = 1000


		def get_initial_queryset(self):
			
			# return queryset used as base for futher sorting/filtering
			
			# get job
			job = models.Job.objects.get(pk=self.kwargs['job_id'])

			# return filtered queryset
			return models.IndexMappingFailure.objects(job_id=job.id)


		def render_column(self, row, column):

			# determine record link
			target_record = row.record
			record_link = reverse(record, kwargs={
					'org_id':target_record.job.record_group.organization.id,
					'record_group_id':target_record.job.record_group.id,
					'job_id':target_record.job.id,
					'record_id':target_record.id
				})

			if column == '_id':
				return '<a href="%s">%s</a>' % (record_link, target_record.id)

			if column == 'record_id':
				return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

			# handle associated job
			if column == 'job':
				return row.job.name

			else:
				return super(DTIndexingFailuresJson, self).render_column(row, column)


		# def filter_queryset(self, qs):

		# 	# handle search
		# 	search = self.request.GET.get(u'search[value]', None)
		# 	if search:
		# 		logger.debug('looking for: %s' % search)
		# 		qs = qs.filter(Q(id = search) | Q(combine_id = search) | Q(record_id = search) | Q(mapping_error__contains = search))

		# 	return qs



class DTJobValidationScenarioFailuresJson(BaseDatatableView):

		'''
		Prepare and return Datatables JSON for RecordValidation failures from Job, per Validation Scenario
		'''

		# define the columns that will be returned
		columns = [
			'id',
			'record',
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
			'record',
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
			elif column == 'record':
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
				# sniff out ObjectId if present
				if len(search) == 24:
					try:
						oid = ObjectId(search)					
						qs = qs.filter(mongoengine.Q(record_id=oid))
					except:
						logger.debug('recieved 24 chars, but not ObjectId')
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
			
			# get job and records
			job = models.Job.objects.get(pk=self.kwargs['job_id'])

			# return queryset filtered for match/miss
			if self.kwargs['match_type'] == 'matches':
				return job.get_records().filter(dbdm=True)
			elif self.kwargs['match_type'] == 'misses':
				return job.get_records().filter(dbdm=False)


		def render_column(self, row, column):

			# determine record link
			target_record = row
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
				return '<a href="%s">%s</a>' % (record_link, target_record.id)

			# handle record record_id
			elif column == 'record_id':
				# get target record from row
				target_record = row
				return '<a href="%s">%s</a>' % (record_link, target_record.record_id)

			# handle all else
			else:
				return super(DTDPLABulkDataMatches, self).render_column(row, column)


		def filter_queryset(self, qs):
			# use parameters passed in GET request to filter queryset

			# handle search
			search = self.request.GET.get(u'search[value]', None)
			if search:
				# sniff out ObjectId if present
				if len(search) == 24:
					try:
						oid = ObjectId(search)					
						qs = qs.filter(mongoengine.Q(id=oid))
					except:
						logger.debug('recieved 24 chars, but not ObjectId')
				else:
					qs = qs.filter(mongoengine.Q(record_id=search))

			# return
			return qs



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
				return '<a href="%s"><button type="button" class="btn btn-success btn-sm">Results <i class="la la-info-circle"></i></button></a> <a href="%s"><button type="button" class="btn btn-outline-danger btn-sm" onclick="return confirm(\'Are you sure you want to remove this task?\');">Delete <i class="la la-close"></i></button></a>' % (
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



####################################################################
# Global Messages												   #
####################################################################

@login_required
def gm_delete(request):
	
	if request.method == 'POST':

		# get gm_id
		gm_id = request.POST.get('gm_id')

		# init GlobalMessageClient
		gmc = models.GlobalMessageClient(request.session)

		# delete by id
		results = gmc.delete_gm(gm_id)

		# redirect
		return JsonResponse({			
			'gm_id':gm_id,
			'num_removed':results	
		})


















