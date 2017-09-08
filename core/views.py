# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core import serializers
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

# import models
from core import models

import json
import logging
import requests
import textwrap
import time


# Get an instance of a logger
logger = logging.getLogger(__name__)

# instantiate Livy handle
'''
Consider reworking this: as this LivyClient instance is tethered to the running Django instance.
	- init with every view?  
	- have LivyClient.http_request() a @classmethod that can be used anytime?
Ignoring for now to focus on other moving parts
'''
livy = models.LivyClient()



##################################
# Livy Sessions
##################################
def livy_sessions(request):
	
	logger.debug('retrieving current Livy sessions')
	
	# make request
	livy_response = livy.get_sessions()
	return JsonResponse(livy_response)


def livy_session_create(request):
	
	logger.debug('creating Livy session')
	
	# make request
	livy_response = livy.create_session()
	return JsonResponse(livy_response)


def livy_session_status(request, session_id):
	
	logger.debug('retreiving Livy session status')
	
	# make request
	livy_response = livy.session_status(session_id)
	return JsonResponse(livy_response)



##################################
# Record Groups
##################################
def record_groups(request):

	'''
	View all record groups
	'''
	
	logger.debug('retrieving record groups')
	
	record_groups = serializers.serialize("json", models.RecordGroup.objects.all())
	return HttpResponse(record_groups, content_type='application/json')


def record_group(request, record_group_id):

	'''
	View information about a single record group, including any and all jobs run for this group

	Args:
		record_group_id (str/int): PK for RecordGroup table
	'''
	
	logger.debug('retrieving record group by PK: %s' % record_group_id)
	
	# retrieve record group
	rg = models.RecordGroup.objects.filter(id=record_group_id)

	# get all jobs associated with record group
	rg_jobs = models.Job.objects.filter(record_group=record_group_id)

	# generate response
	return_dict = {
		'record_group':json.loads(serializers.serialize("json",rg)),
		'associated_jobs':json.loads(serializers.serialize("json",rg_jobs))
	}

	return JsonResponse(return_dict)


	




##################################
# Index
##################################
@login_required
def index(request):
	logger.info('Welcome to Combine.')
	return render(request, 'core/index.html', None)





