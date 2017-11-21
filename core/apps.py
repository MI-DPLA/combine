# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import logging

# combine
from django.apps import AppConfig

# pyjxslt
import pyjxslt


# Get an instance of a logger
logger = logging.getLogger(__name__)

class CoreConfig(AppConfig):
	
	name = 'core'

	def ready(self):

		'''
		ready() method fires once, when application is loaded and ready
		https://docs.djangoproject.com/en/dev/ref/applications/#django.apps.AppConfig.ready

		This fires any functions defined here that are needed when Combine starts.

		Args:
			(django.apps.AppConfig): instance of 'Core' application config

		Returns:
			None
		'''

		# logger.debug('Core application ready method preperations firing')

		# load pyjxslt transformations
		# load_pyjxslt_transformations()

		pass


# def load_pyjxslt_transformations():

# 	'''
# 	Load all XSLT transformations to pyjxslt when Combine starts

# 	Args:
# 		None:

# 	Returns:
# 		None
# 			- loads xslt transformation with transform key as transformation scenario DB id
# 	'''
	
# 	from core.models import Transformation
# 	logger.debug('loading XSLT transformations for pyjxslt')

# 	# get all xslt type transformations
# 	xslt_transformations = Transformation.objects.filter(transformation_type = 'xslt')

# 	# get pyjxslt gateway
# 	gw = pyjxslt.Gateway(6767)

# 	# loop through and add to pyjxslt
# 	for t in xslt_transformations:
# 		with open(t.filepath, 'r') as f:
# 			xslt_string = f.read()
# 			gw.add_transform('xslt_%s' % str(t.id), xslt_string)


