# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core import serializers
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

# import models
from core import models

import json
import logging
import requests
import textwrap
import time


# Get an instance of a logger
logger = logging.getLogger(__name__)


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
# Record Groups
##################################

def record_groups(request):

	'''
	View all record groups
	'''
	
	logger.debug('retrieving record groups')
	
	record_groups = models.RecordGroup.objects.all()
	logger.debug("found %s record groups" % record_groups.count())

	# render page
	return render(request, 'core/record_groups.html', {'settings':settings, 'record_groups':record_groups})


def record_group(request, record_group_id):

	'''
	View information about a single record group, including any and all jobs run for this group

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

		# 	# if job is available, and record_count is 0, attempt count
		# 	if job.status == 'available' and job.record_count == 0:
		# 		combine_job = models.CombineJob(request.user)
		# 		combine_job.get_job(job.id)
		# 		job.record_count = combine_job.count_records()
		# 		job.save()

		# # if job is gone, but finished is True and record count is 0, attempt count
		# if job.status == 'gone' and job.finished == True and job.record_count == 0:
		# 	combine_job = models.CombineJob(request.user)
		# 	combine_job.get_job(job.id)
		# 	job.record_count = combine_job.count_records()
		# 	job.save()

	'''
	TODO: ping each URL and get status for job, update in DB
		- create LivyClient method for updating job status from Livy
	'''

	# render page 
	return render(request, 'core/record_group.html', {'settings':settings, 'livy_session':livy_session, 'record_group':record_group, 'record_group_jobs':record_group_jobs})


##################################
# Jobs
##################################

@login_required
def job_delete(request, record_group_id, job_id):
	
	logger.debug('deleting job by id: %s' % job_id)

	job = models.Job.objects.filter(id=job_id).first()
	
	# remove from DB
	job.delete()

	# redirect
	return redirect('record_group', record_group_id=record_group_id)


@login_required
def job_harvest(request, record_group_id):

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
		return render(request, 'core/job_harvest.html', {'record_group':record_group, 'oai_endpoints':oai_endpoints})

	# if POST, submit job
	if request.method == 'POST':

		logger.debug('beggining harvest for Record Group: %s' % record_group.name)

		# debug form
		logger.debug(request.POST)

		# retrieve OAIEndpoint
		oai_endpoint = models.OAIEndpoint.objects.get(pk=int(request.POST['oai_endpoint_id']))

		# add overrides if set
		overrides = { override:request.POST[override] for override in ['verb','metadataPrefix','scope_type','scope_value'] if request.POST[override] != '' }
		logger.debug(overrides)

		# initiate and submit job
		job = models.HarvestJob(request.user, record_group, oai_endpoint, overrides)
		job.start_job()

		return redirect('record_group', record_group_id=record_group.id)


##################################
# Index
##################################
@login_required
def index(request):
	username = request.user.username
	logger.info('Welcome to Combine, %s' % username)
	return render(request, 'core/index.html', {'username':username})





