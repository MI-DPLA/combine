# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core import serializers
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

# import models
from core import models, forms
from core.es import es_handle

# import oai server
from core.oai import OAIProvider

# import cyavro
import cyavro

# elasticsearch-dsl
from elasticsearch_dsl import Search, Q, A

import json
import logging
import os
import re
import requests
import textwrap
import time


# Get an instance of a logger
logger = logging.getLogger(__name__)


# breadcrumb parser
def breadcrumb_parser(path):
	
	'''
	return parsed URL based on the pattern:
	organization / record_group / job 
	'''

	crumbs = []

	# org
	m = re.match(r'(.+?/organization/([0-9]+))',path)
	if m:
		org = models.Organization.objects.get(pk=int(m.group(2)))
		crumbs.append(("%s (Organization)" % org.name,m.group(1)))

	# record_group
	m = re.match(r'(.+?/record_group/([0-9]+))',path)
	if m:
		rg = models.RecordGroup.objects.get(pk=int(m.group(2)))
		crumbs.append(("%s (Record Group)" % rg.name,m.group(1)))

	# job
	m = re.match(r'(.+?/job/([0-9]+))',path)
	if m:
		j = models.Job.objects.get(pk=int(m.group(2)))
		crumbs.append(("%s (Job)" % j.name,m.group(1)))

	# return
	logger.debug(crumbs)
	return crumbs


##################################
# User Livy Sessions
##################################

@login_required
def livy_sessions(request):
	
	logger.debug('retrieving Livy sessions')
	
	# query db
	livy_sessions = models.LivySession.objects.all()

	# refresh sessions
	for livy_session in livy_sessions:
		livy_session.refresh_from_livy()

		# check user session status, and set active flag for session
		if livy_session.status in ['starting','idle','busy']:
			livy_session.active = True
		else:
			livy_session.active = False
		livy_session.save()
	
	# return
	return render(request, 'core/livy_sessions.html', {'livy_sessions':livy_sessions})


@login_required
def livy_session_start(request):
	
	logger.debug('Checking for pre-existing user sessions')

	# get "active" user sessions
	livy_sessions = models.LivySession.objects.filter(status__in=['starting','running','idle'])
	logger.debug(livy_sessions)

	# none found
	if livy_sessions.count() == 0:
		logger.debug('no Livy sessions found, creating')
		livy_session = models.LivySession().save()

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



##################################
# Organizations
##################################

def organizations(request):

	'''
	View all Organizations
	'''
	
	# show organizations
	if request.method == 'GET':

		logger.debug('retrieving organizations')
		
		# get all organizations
		orgs = models.Organization.objects.all()

		# get Organization form
		organization_form = forms.OrganizationForm()

		# render page
		return render(request, 'core/organizations.html', {'orgs':orgs, 'organization_form':organization_form})


	# create new organization
	if request.method == 'POST':

		# create new org
		logger.debug(request.POST)
		f = forms.OrganizationForm(request.POST)
		f.save()	

		return redirect('organizations')


def organization(request, org_id):

	'''
	Details for Organization
	'''

	# get organization
	org = models.Organization.objects.get(pk=org_id)

	# get record groups for this organization
	record_groups = models.RecordGroup.objects.filter(organization=org)

	# get RecordGroup form
	record_group_form = forms.RecordGroupForm()
	
	# render page
	return render(request, 'core/organization.html', {'org':org, 'record_groups':record_groups, 'record_group_form':record_group_form, 'breadcrumbs':breadcrumb_parser(request.path)})



##################################
# Record Groups
##################################


def record_group_new(request, org_id):

	'''
	Create new Record Group
	'''

	# create new organization
	if request.method == 'POST':

		# create new record group
		logger.debug(request.POST)
		f = forms.RecordGroupForm(request.POST)
		f.save()

		# redirect to organization page
		return redirect('organization', org_id=org_id)



def record_group(request, org_id, record_group_id):

	'''
	View information about a single record group, including any and all jobs run

	Args:
		record_group_id (str/int): PK for RecordGroup table
	'''
	
	logger.debug('retrieving record group ID: %s' % record_group_id)

	# retrieve current livy session
	livy_session = models.LivySession.objects.filter(active=True).first()

	# retrieve record group
	record_group = models.RecordGroup.objects.filter(id=record_group_id).first()

	# get all jobs associated with record group
	record_group_jobs = models.Job.objects.filter(record_group=record_group_id)

	# loop through jobs and update status
	for job in record_group_jobs:

		# if job is pending, starting, or running, attempt to update status
		if job.status in ['init','waiting','pending','starting','running','available'] and job.url != None:
			job.refresh_from_livy()

		# udpate record count if not already calculated
		if job.record_count == 0:

			# if finished, count
			if job.finished:
				logger.debug('updating record count for job #%s' % job.id)
				job.update_record_count()

	# render page 
	return render(request, 'core/record_group.html', {'livy_session':livy_session, 'record_group':record_group, 'record_group_jobs':record_group_jobs, 'breadcrumbs':breadcrumb_parser(request.path)})



##################################
# Jobs
##################################

@login_required
def job_delete(request, org_id, record_group_id, job_id):
	
	logger.debug('deleting job by id: %s' % job_id)

	job = models.Job.objects.filter(id=job_id).first()
	
	# remove from DB
	job.delete()

	# redirect
	return redirect('record_group', org_id=org_id, record_group_id=record_group_id)


@login_required
def job_details(request, org_id, record_group_id, job_id):
	
	logger.debug('details for job id: %s' % job_id)

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# field analysis
	field_counts = cjob.count_indexed_fields()
	
	# return
	return render(request, 'core/job_details.html', {'cjob':cjob, 'field_counts':field_counts, 'breadcrumbs':breadcrumb_parser(request.path)})


@login_required
def job_errors(request, org_id, record_group_id, job_id):
	
	logger.debug('retrieving errors for job id: %s' % job_id)

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	job_errors = cjob.get_job_errors()
	
	# return
	return render(request, 'core/job_errors.html', {'cjob':cjob, 'job_errors':job_errors, 'breadcrumbs':breadcrumb_parser(request.path)})


@login_required
def job_input_select(request):
	
	logger.debug('loading job selection view')

	jobs = models.Job.objects.all()
	
	# return
	return render(request, 'core/job_input_select.html', {'jobs':jobs})


@login_required
def job_harvest(request, org_id, record_group_id):

	'''
	Create a new Harvest Job
	'''

	# retrieve record group
	record_group = models.RecordGroup.objects.filter(id=record_group_id).first()
	
	# if GET, prepare form
	if request.method == 'GET':
		
		# retrieve all OAI endoints
		oai_endpoints = models.OAIEndpoint.objects.all()

		# render page
		return render(request, 'core/job_harvest.html', {'record_group':record_group, 'oai_endpoints':oai_endpoints, 'breadcrumbs':breadcrumb_parser(request.path)})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('beginning harvest for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# retrieve OAIEndpoint
		oai_endpoint = models.OAIEndpoint.objects.get(pk=int(request.POST['oai_endpoint_id']))

		# add overrides if set
		overrides = { override:request.POST[override] for override in ['verb','metadataPrefix','scope_type','scope_value'] if request.POST[override] != '' }
		logger.debug(overrides)

		# get preferred metadata index mapper
		index_mapper = request.POST.get('index_mapper')

		# initiate job
		job = models.HarvestJob(
			job_name=job_name,
			user=request.user,
			record_group=record_group,
			oai_endpoint=oai_endpoint,
			overrides=overrides,
			index_mapper=index_mapper
		)
		
		# start job
		job.start_job()

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
		
		# retrieve all jobs
		jobs = record_group.job_set.all()	

		# get all transformation scenarios
		transformations = models.Transformation.objects.all()	

		# render page
		return render(request, 'core/job_transform.html', {'job_select_type':'single', 'record_group':record_group, 'jobs':jobs, 'transformations':transformations, 'breadcrumbs':breadcrumb_parser(request.path)})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('beginning transform for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# retrieve input job
		input_job = models.Job.objects.get(pk=int(request.POST['input_job_id']))
		logger.debug('using job as input: %s' % input_job)

		# retrieve transformation
		transformation = models.Transformation.objects.get(pk=int(request.POST['transformation_id']))
		logger.debug('using transformation: %s' % transformation)

		# get preferred metadata index mapper
		index_mapper = request.POST.get('index_mapper')

		# initiate job
		job = models.TransformJob(
			job_name=job_name,
			user=request.user,
			record_group=record_group,
			input_job=input_job,
			transformation=transformation,
			index_mapper=index_mapper
		)
		
		# start job
		job.start_job()

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
		
		# retrieve all jobs for this record group
		jobs = models.Job.objects.all()

		# render page
		return render(request, 'core/job_merge.html', {'job_select_type':'multiple', 'record_group':record_group, 'jobs':jobs, 'breadcrumbs':breadcrumb_parser(request.path)})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('Merging jobs for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# retrieve jobs to merge
		input_jobs = [ models.Job.objects.get(pk=int(job)) for job in request.POST.getlist('input_job_id') ]		
		logger.debug('merging jobs: %s' % input_jobs)

		# get preferred metadata index mapper
		index_mapper = request.POST.get('index_mapper')

		# initiate job
		job = models.MergeJob(
			job_name=job_name,
			user=request.user,
			record_group=record_group,
			input_jobs=input_jobs,
			index_mapper=index_mapper
		)
		
		# # start job
		job.start_job()

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
		
		# retrieve all jobs for this record group
		jobs = record_group.job_set.all()

		# render page
		return render(request, 'core/job_publish.html', {'job_select_type':'single', 'record_group':record_group, 'jobs':jobs, 'breadcrumbs':breadcrumb_parser(request.path)})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('Publishing job for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# get job name
		job_name = request.POST.get('job_name')
		if job_name == '':
			job_name = None

		# retrieve input job
		input_job = models.Job.objects.get(pk=int(request.POST['input_job_id']))
		logger.debug('publishing job: %s' % input_job)

		# get preferred metadata index mapper
		index_mapper = request.POST.get('index_mapper')

		# initiate job
		job = models.PublishJob(
			job_name=job_name,
			user=request.user,
			record_group=record_group,
			input_job=input_job,
			index_mapper=index_mapper
		)
		
		# start job
		job.start_job()

		return redirect('record_group', org_id=org_id, record_group_id=record_group.id)



##################################
# Jobs QA
##################################
@login_required
def field_analysis(request, org_id, record_group_id, job_id):

	# get field name
	field_name = request.GET.get('field_name')
	logger.debug('field analysis for field "%s", job id: %s' % (field_name, job_id))
	
	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# get analysis for field
	field_analysis_results = cjob.field_analysis(field_name)

	# return
	return render(request, 'core/field_analysis.html', {'field_name':field_name,'field_analysis_results':field_analysis_results, 'breadcrumbs':breadcrumb_parser(request.path)})


@login_required
def job_indexing_failures(request, org_id, record_group_id, job_id):

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(job_id)

	# get indexing failures
	indexing_failures = cjob.get_indexing_failures()

	# return
	return render(request, 'core/job_indexing_failures.html', {'indexing_failures':indexing_failures, 'breadcrumbs':breadcrumb_parser(request.path)})



##################################
# Transformations
##################################
@login_required
def configuration(request):

	# get all transformations
	transformations = models.Transformation.objects.all()

	# get all OAI endpoints
	oai_endpoints = models.OAIEndpoint.objects.all()

	# return
	return render(request, 'core/configuration.html', {'transformations':transformations, 'oai_endpoints':oai_endpoints})



##################################
# Pbulished
##################################
@login_required
def published(request):

	'''
	Published records
	'''
	
	# get instance of Published model
	published = models.Published()

	return render(request, 'core/published.html', {'published':published})



##################################
# OAI Server
##################################
@login_required
def oai(request):

	'''
	Parse GET parameters, send to OAIProvider instance from oai.py
	Return XML results
	'''

	# debug args
	args = {
		'verb':'Identify'
	}

	# get OAIProvider instance
	op = OAIProvider(args)

	# return XML
	return HttpResponse(op.generate_response(), content_type='text/xml')



##################################
# Index
##################################
@login_required
def index(request):
	username = request.user.username
	logger.info('Welcome to Combine, %s' % username)
	return render(request, 'core/index.html', {'username':username})





