# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render

import json
import logging
import requests
import textwrap
import time


# Get an instance of a logger
logger = logging.getLogger(__name__)


def index(request):
	logger.info('Welcome to Combine. %s' % u'\U0001F69C')
	return render(request, 'core/index.html', None)


def test_create_session(request):
	
	'''
	proof-of-concept: harvest OAI set with Ingestion3, via Django
	'''

	# start session
	host = 'http://%s:%s' % (settings.LIVY_HOST, settings.LIVY_PORT)
	logger.debug(host)
	headers = {'Content-Type': 'application/json'}
	r = requests.post(host + '/sessions', data=json.dumps(settings.LIVY_SESSION_CONFIG), headers=headers)
	logger.debug(r.json())

	# poll until session started
	session_url = host + r.headers['location']
	for x in range(0,30):
		r = requests.get(session_url, headers=headers).json()
		logger.debug(r)
		if r['state'] == 'idle':
			logger.debug('session is idle and ready!')
			break
		time.sleep(.5)

	# close session
	r = requests.delete(session_url, headers=headers).json()
	logger.debug(r)
	return HttpResponse('session started, determined idle, and terminated')



