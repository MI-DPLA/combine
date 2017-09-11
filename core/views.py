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
	
	logger.debug('retrieving Livy sessions for user')
	
	# query db
	user_sessions = models.LivySession.objects.filter(user=request.user)

	# refresh sessions
	for user_session in user_sessions:
		user_session.refresh_from_livy()

		# check user session status, and set active flag for session
		if user_session.status in ['starting','idle']:
			user_session.active = True
		else:
			user_session.active = False
		user_session.save()
	
	# return
	return render(request, 'core/user_sessions.html', {'user_sessions':user_sessions})


@login_required
def livy_session_delete(request, session_id):
	
	logger.debug('deleting Livy session by Combine ID: %s' % session_id)

	user_session = models.LivySession.objects.filter(id=session_id).first()
	
	# attempt to stop with Livy
	models.LivyClient.stop_session(user_session.session_id)

	# remove from DB
	user_session.delete()

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
	return render(request, 'core/record_groups.html', {'record_groups':record_groups})


# def record_group(request, record_group_id):

# 	'''
# 	View information about a single record group, including any and all jobs run for this group

# 	Args:
# 		record_group_id (str/int): PK for RecordGroup table
# 	'''
	
# 	logger.debug('retrieving record group by PK: %s' % record_group_id)
	
# 	# retrieve record group
# 	rg = models.RecordGroup.objects.filter(id=record_group_id)

# 	# get all jobs associated with record group
# 	rg_jobs = models.Job.objects.filter(record_group=record_group_id)

# 	# generate response
# 	return_dict = {
# 		'record_group':json.loads(serializers.serialize("json",rg)),
# 		'associated_jobs':json.loads(serializers.serialize("json",rg_jobs))
# 	}

# 	return JsonResponse(return_dict)


	




##################################
# Index
##################################
@login_required
def index(request):
	username = request.user.username
	logger.info('Welcome to Combine, %s' % username)
	return render(request, 'core/index.html', {'username':username})





