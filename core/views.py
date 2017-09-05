# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
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



# Livy Sessions
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












# Default Index View
def index(request):
	logger.info('Welcome to Combine.')
	return render(request, 'core/index.html', None)





